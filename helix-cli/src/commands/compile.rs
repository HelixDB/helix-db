use std::path::PathBuf;

use eyre::Result;

use crate::{
    project::ProjectContext,
    utils::{
        helixc_utils::{
            analyze_source, collect_hx_files, generate_content, generate_rust_code, parse_content,
        },
        print_status, print_success,
    },
};

pub async fn run(output_dir: Option<String>, path: Option<String>) -> Result<()> {
    println!("Checking Helix queries...");
    print_status("VALIDATE", "Parsing and validating Helix queries");

    // Load project context from the specified path (helix.toml directory) or find it automatically
    let project = match &path {
        Some(helix_toml_dir) => {
            let dir_path = PathBuf::from(helix_toml_dir);
            ProjectContext::find_and_load(Some(&dir_path))?
        }
        None => ProjectContext::find_and_load(None)?,
    };

    // Collect all .hx files for validation from the queries directory
    let hx_files = collect_hx_files(&project.root, &project.config.project.queries)?;

    // Generate content and validate using helix-db parsing logic
    let content = generate_content(&hx_files)?;
    let source = parse_content(&content)?;

    // Check if schema is empty before analyzing
    if source.schema.is_empty() {
        let error = crate::errors::CliError::new("no schema definitions found in project")
            .with_context("searched all .hx files in the queries directory but found no N:: (node) or E:: (edge) definitions")
            .with_hint("add at least one schema definition like 'N::User { name: String }' to your .hx files");
        return Err(eyre::eyre!("{}", error.render()));
    }

    // Run static analysis to catch validation errors
    let generated_source = analyze_source(source)?;

    // Generate Rust code
    let output_dir = output_dir
        .map(|dir| PathBuf::from(&dir))
        .unwrap_or(project.root);
    generate_rust_code(generated_source, &output_dir)?;

    print_success("Compilation completed successfully");
    Ok(())
}
