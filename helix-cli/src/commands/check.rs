use crate::project::ProjectContext;
use crate::utils::helixc_utils::{
    analyze_source, collect_hx_files, generate_content, parse_content,
};
use crate::utils::{print_status, print_success};
use eyre::Result;

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

    // Validate queries and schema syntax
    validate_project_syntax(project)?;

    print_success(&format!(
        "Instance '{instance_name}' configuration is valid"
    ));
    Ok(())
}

async fn check_all_instances(project: &ProjectContext) -> Result<()> {
    print_status("CHECK", "Checking all instances");

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

/// Validate project syntax by parsing queries and schema (similar to build.rs but without generating files)
fn validate_project_syntax(project: &ProjectContext) -> Result<()> {
    print_status("VALIDATE", "Parsing and validating Helix queries");

    // Collect all .hx files for validation
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
    analyze_source(source, &content.files)?;

    print_success("All queries and schema are valid");
    Ok(())
}
