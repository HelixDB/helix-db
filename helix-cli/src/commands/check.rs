use crate::project::ProjectContext;
use crate::utils::helixc_utils::{
    analyze_source, collect_hx_files, generate_content, parse_content,
};
use crate::utils::{print_error, print_status, print_success};
use eyre::Result;
use std::path::Path;

pub async fn run(instance: Option<String>) -> Result<()> {
    // Load project context
    let project = ProjectContext::find_and_load(None)?;

    match instance {
        Some(instance_name) => check_instance(&project, &instance_name).await,
        None => check_all_instances(&project).await,
    }
}

async fn check_instance(project: &ProjectContext, instance_name: &str) -> Result<()> {
    print_status("CHECK", &format!("Checking instance '{instance_name}'"));

    // Validate instance exists in config
    let _instance_config = project.config.get_instance(instance_name)?;

    // Check project files
    check_project_files(&project.root)?;

    // Validate queries and schema syntax
    validate_project_syntax(project)?;

    print_success(&format!(
        "Instance '{instance_name}' configuration is valid"
    ));
    Ok(())
}

async fn check_all_instances(project: &ProjectContext) -> Result<()> {
    print_status("CHECK", "Checking all instances");

    // Check project files
    check_project_files(&project.root)?;

    // Validate queries and schema syntax
    validate_project_syntax(project)?;

    // Check each instance
    for instance_name in project.config.list_instances() {
        print_status("CHECK", &format!("Validating instance '{instance_name}'"));
        let _instance_config = project.config.get_instance(instance_name)?;
    }

    print_success("All instances are valid");
    Ok(())
}

fn check_project_files(project_root: &Path) -> Result<()> {
    let schema_path = project_root.join("schema.hx");
    let queries_path = project_root.join("queries.hx");

    if !schema_path.exists() {
        print_error("schema.hx not found");
        return Err(eyre::eyre!("Missing schema.hx file"));
    }

    if !queries_path.exists() {
        print_error("queries.hx not found");
        return Err(eyre::eyre!("Missing queries.hx file"));
    }

    Ok(())
}

/// Validate project syntax by parsing queries and schema (similar to build.rs but without generating files)
fn validate_project_syntax(project: &ProjectContext) -> Result<()> {
    print_status("VALIDATE", "Parsing and validating Helix queries...");

    // Collect all .hx files for validation
    let hx_files = collect_hx_files(&project.root)?;

    // Generate content and validate using helix-db parsing logic
    let content = generate_content(&hx_files)?;
    let source = parse_content(&content)?;

    // Run static analysis to catch validation errors
    analyze_source(source)?;

    print_success("All queries and schema are valid");
    Ok(())
}
