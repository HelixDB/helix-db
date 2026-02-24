use std::path::{Path, PathBuf};
use std::process::Command;

use eyre::Result;

use crate::{
    output::{Operation, Step},
    project::ProjectContext,
    utils::helixc_utils::{
        analyze_source, collect_hx_files, generate_content, generate_rust_code, parse_content,
    },
};

pub async fn run(output_dir: Option<String>, path: Option<String>) -> Result<()> {
    let op = Operation::new("Compiling", "queries");

    // Load project context from the specified path (helix.toml directory) or find it automatically
    let project = match &path {
        Some(helix_toml_dir) => {
            let dir_path = PathBuf::from(helix_toml_dir);
            ProjectContext::find_and_load(Some(&dir_path))?
        }
        None => ProjectContext::find_and_load(None)?,
    };

    let queries_project_dir = project.root.join(&project.config.project.queries);
    if queries_project_dir.join("Cargo.toml").exists() {
        let mut compile_step = Step::with_messages(
            "Compiling enterprise queries",
            "Enterprise queries compiled",
        );
        compile_step.start();
        let output_bin = run_enterprise_compile(&queries_project_dir, output_dir.as_deref())?;
        compile_step.done_with_info(&output_bin.display().to_string());
        op.success();
        return Ok(());
    }

    // Collect all .hx files for validation from the queries directory
    let mut parse_step = Step::with_messages("Parsing queries", "Queries parsed");
    parse_step.start();
    let hx_files = collect_hx_files(&project.root, &project.config.project.queries)?;

    // Generate content and validate using helix-db parsing logic
    let content = generate_content(&hx_files)?;
    let source = parse_content(&content)?;

    // Check if schema is empty before analyzing
    if source.schema.is_empty() {
        parse_step.fail();
        op.failure();
        let error = crate::errors::CliError::new("no schema definitions found in project")
            .with_context("searched all .hx files in the queries directory but found no N:: (node) or E:: (edge) definitions")
            .with_hint("add at least one schema definition like 'N::User { name: String }' to your .hx files");
        return Err(eyre::eyre!("{}", error.render()));
    }

    let num_queries = source.queries.len();
    parse_step.done_with_info(&format!("{} queries", num_queries));

    // Run static analysis to catch validation errors
    let mut analyze_step = Step::with_messages("Analyzing", "Analysis complete");
    analyze_step.start();
    let generated_source = analyze_source(source, &content.files)?;
    analyze_step.done();

    // Generate Rust code
    let mut codegen_step = Step::with_messages("Generating Rust code", "Rust code generated");
    codegen_step.start();
    let output_dir = output_dir
        .map(|dir| PathBuf::from(&dir))
        .unwrap_or(project.root);
    generate_rust_code(generated_source, &output_dir)?;
    codegen_step.done();

    op.success();
    Ok(())
}

fn run_enterprise_compile(
    queries_project_dir: &Path,
    output_path: Option<&str>,
) -> Result<PathBuf> {
    let manifest_path = queries_project_dir.join("Cargo.toml");
    if !manifest_path.exists() {
        return Err(eyre::eyre!(
            "Enterprise queries Cargo.toml not found at {}",
            manifest_path.display()
        ));
    }

    let cargo_output = Command::new("cargo")
        .arg("run")
        .arg("--manifest-path")
        .arg(&manifest_path)
        .current_dir(queries_project_dir)
        .output()
        .map_err(|e| eyre::eyre!("Failed to run cargo in enterprise queries project: {}", e))?;

    if !cargo_output.status.success() {
        let stdout = String::from_utf8_lossy(&cargo_output.stdout);
        let stderr = String::from_utf8_lossy(&cargo_output.stderr);
        return Err(eyre::eyre!(
            "Enterprise query project compilation failed:\n{}\n{}",
            stderr,
            stdout
        ));
    }

    let generated_bin = queries_project_dir.join("queries.bin");
    if !generated_bin.exists() {
        return Err(eyre::eyre!(
            "Enterprise query project did not generate queries.bin at {}",
            generated_bin.display()
        ));
    }

    let target_path = resolve_enterprise_output_path(output_path, &generated_bin);
    if target_path != generated_bin {
        if let Some(parent) = target_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::copy(&generated_bin, &target_path).map_err(|e| {
            eyre::eyre!(
                "Failed to copy generated queries.bin from {} to {}: {}",
                generated_bin.display(),
                target_path.display(),
                e
            )
        })?;
    }

    Ok(target_path)
}

fn resolve_enterprise_output_path(output_path: Option<&str>, generated_bin: &Path) -> PathBuf {
    let Some(raw_output) = output_path else {
        return generated_bin.to_path_buf();
    };

    let output = PathBuf::from(raw_output);
    if output.exists() {
        if output.is_dir() {
            return output.join("queries.bin");
        }
        return output;
    }

    if output.extension().is_some() {
        output
    } else {
        output.join("queries.bin")
    }
}
