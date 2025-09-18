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
    print_status("VALIDATE", "Parsing and validating Helix queries...");

    let project = ProjectContext::find_and_load(None)?;

    // Collect all .hx files for validation
    let hx_files = collect_hx_files(
        &path
            .map(|dir| PathBuf::from(&dir))
            .unwrap_or(project.root.clone()),
    )?;

    // Generate content and validate using helix-db parsing logic
    let content = generate_content(&hx_files)?;
    let source = parse_content(&content)?;

    // Run static analysis to catch validation errors
    let generated_source = analyze_source(source)?;

    // Generate Rust code
    let output_dir = output_dir
        .map(|dir| PathBuf::from(&dir))
        .unwrap_or(project.root);
    generate_rust_code(generated_source, &output_dir)?;

    print_success("All queries and schema are valid");
    Ok(())
}
