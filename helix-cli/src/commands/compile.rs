use std::path::PathBuf;

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
