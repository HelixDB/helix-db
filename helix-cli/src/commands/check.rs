//! Check command - validates project configuration, queries, and generated Rust code.

use crate::commands::build;
use crate::github_issue::{GitHubIssueBuilder, filter_errors_only};
use crate::metrics_sender::MetricsSender;
use crate::project::ProjectContext;
use crate::utils::helixc_utils::{
    analyze_source, collect_hx_contents, collect_hx_files, generate_content, parse_content,
};
use crate::utils::{print_confirm, print_error, print_status, print_success, print_warning};
use eyre::Result;
use helix_db::helixc::parser::types::FieldType;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::Instant;

/// Output from running cargo check.
struct CargoCheckOutput {
    success: bool,
    #[allow(dead_code)] // May be useful for debugging
    full_output: String,
    errors_only: String,
}

pub async fn run(instance: Option<String>, metrics_sender: &MetricsSender) -> Result<()> {
    // Load project context
    let project = ProjectContext::find_and_load(None)?;

    match instance {
        Some(instance_name) => check_instance(&project, &instance_name, metrics_sender).await,
        None => check_all_instances(&project, metrics_sender).await,
    }
}

async fn check_instance(
    project: &ProjectContext,
    instance_name: &str,
    metrics_sender: &MetricsSender,
) -> Result<()> {
    let start_time = Instant::now();

    print_status("CHECK", &format!("Checking instance '{instance_name}'"));

    // Validate instance exists in config
    let _instance_config = project.config.get_instance(instance_name)?;

    // Step 1: Validate syntax first (quick check)
    print_status("SYNTAX", "Validating query syntax...");
    validate_project_syntax(project)?;
    print_success("Syntax validation passed");

    // Step 1.5: Validate vector data types
    print_status("VECTORS", "Validating vector data types...");
    validate_vector_data_types(project)?;

    // Step 2: Ensure helix repo is cached (reuse from build.rs)
    build::ensure_helix_repo_cached().await?;

    // Step 3: Prepare instance workspace (reuse from build.rs)
    build::prepare_instance_workspace(project, instance_name).await?;

    // Step 4: Compile project - generate queries.rs (reuse from build.rs)
    let metrics_data = build::compile_project(project, instance_name).await?;

    // Step 5: Copy generated files to helix-repo-copy for cargo check
    let instance_workspace = project.instance_workspace(instance_name);
    let generated_src = instance_workspace.join("helix-container/src");
    let cargo_check_src = instance_workspace.join("helix-repo-copy/helix-container/src");

    // Copy queries.rs and config.hx.json
    fs::copy(
        generated_src.join("queries.rs"),
        cargo_check_src.join("queries.rs"),
    )?;
    fs::copy(
        generated_src.join("config.hx.json"),
        cargo_check_src.join("config.hx.json"),
    )?;

    // Step 6: Run cargo check
    print_status("CARGO", "Running cargo check on generated code...");
    let helix_container_dir = instance_workspace.join("helix-repo-copy/helix-container");
    let cargo_output = run_cargo_check(&helix_container_dir)?;

    let compile_time = start_time.elapsed().as_secs() as u32;

    if !cargo_output.success {
        // Send failure telemetry
        metrics_sender.send_compile_event(
            instance_name.to_string(),
            metrics_data.queries_string,
            metrics_data.num_of_queries,
            compile_time,
            false,
            Some(cargo_output.errors_only.clone()),
        );

        // Read generated Rust for issue
        let generated_rust = fs::read_to_string(cargo_check_src.join("queries.rs"))
            .unwrap_or_else(|_| String::from("[Could not read generated code]"));

        // Handle failure - print errors and offer GitHub issue
        handle_cargo_check_failure(&cargo_output, &generated_rust, project)?;

        return Err(eyre::eyre!("Cargo check failed on generated Rust code"));
    }

    print_success("Cargo check passed");
    print_success(&format!(
        "Instance '{}' check completed successfully",
        instance_name
    ));
    Ok(())
}

async fn check_all_instances(
    project: &ProjectContext,
    metrics_sender: &MetricsSender,
) -> Result<()> {
    print_status("CHECK", "Checking all instances");

    let instances: Vec<String> = project
        .config
        .list_instances()
        .into_iter()
        .map(String::from)
        .collect();

    if instances.is_empty() {
        return Err(eyre::eyre!(
            "No instances found in helix.toml. Add at least one instance to check."
        ));
    }

    // Validate vector data types once for all instances
    print_status("VECTORS", "Validating vector data types...");
    validate_vector_data_types(project)?;

    // Check each instance
    for instance_name in &instances {
        check_instance(project, instance_name, metrics_sender).await?;
    }

    print_success("All instances checked successfully");
    Ok(())
}

/// Validate vector data types and warn about F64 usage
fn validate_vector_data_types(project: &ProjectContext) -> Result<()> {
    // Collect all .hx files for validation
    let hx_files = collect_hx_files(&project.root, &project.config.project.queries)?;

    // Generate content and parse
    let content = generate_content(&hx_files)?;
    let source = parse_content(&content)?;

    let mut found_f64_vectors = false;
    let mut f64_vector_names = Vec::new();

    // Check all vector schemas for F64 usage
    for schema in source.get_schemas_in_order() {
        for vector_schema in &schema.vector_schemas {
            for field in &vector_schema.fields {
                if contains_f64_type(&field.field_type) {
                    found_f64_vectors = true;
                    f64_vector_names.push(format!("V::{}.{}", vector_schema.name, field.name));
                }
            }
        }
    }

    if found_f64_vectors {
        print_warning("Found F64 data types in vector fields");
        println!();
        println!("  Vector fields using F64:");
        for vector_name in &f64_vector_names {
            println!("    • {}", vector_name);
        }
        println!();
        println!("  ⚠️  F64 vectors are deprecated.");
        println!(
            "     For vectors, use [F32] instead of [F64] for better performance and compatibility."
        );
        println!("     F32 provides sufficient precision for most vector similarity use cases.");
        return Err(eyre::eyre!(
            "Vectors with F64 data types are deprecated. Use F32 instead."
        ));
    } else {
        print_success("Vector data types validation passed");
    }

    Ok(())
}

/// Recursively check if a FieldType contains F64
fn contains_f64_type(field_type: &FieldType) -> bool {
    match field_type {
        FieldType::F64 => true,
        FieldType::Array(inner) => contains_f64_type(inner),
        FieldType::Object(obj) => obj.values().any(contains_f64_type),
        _ => false,
    }
}

/// Validate project syntax by parsing queries and schema (similar to build.rs but without generating files)
fn validate_project_syntax(project: &ProjectContext) -> Result<()> {
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

    Ok(())
}

/// Run cargo check on the generated code.
fn run_cargo_check(helix_container_dir: &Path) -> Result<CargoCheckOutput> {
    let output = Command::new("cargo")
        .arg("check")
        .arg("--color=never") // Disable color codes for cleaner output
        .current_dir(helix_container_dir)
        .output()
        .map_err(|e| eyre::eyre!("Failed to run cargo check: {}", e))?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    // stderr contains the actual errors, stdout has JSON if using message-format
    let full_output = format!("{}\n{}", stderr, stdout);

    let errors_only = filter_errors_only(&full_output);

    Ok(CargoCheckOutput {
        success: output.status.success(),
        full_output,
        errors_only,
    })
}

/// Handle cargo check failure - print errors and offer GitHub issue creation.
fn handle_cargo_check_failure(
    cargo_output: &CargoCheckOutput,
    generated_rust: &str,
    project: &ProjectContext,
) -> Result<()> {
    print_error("Cargo check failed on generated Rust code");
    println!();
    println!("This may indicate a bug in the Helix code generator.");
    println!();

    // Offer to create GitHub issue
    print_warning("You can report this issue to help improve Helix.");
    println!();

    let should_create =
        print_confirm("Would you like to create a GitHub issue with diagnostic information?")?;

    if !should_create {
        return Ok(());
    }

    // Collect .hx content
    let hx_content = collect_hx_contents(&project.root, &project.config.project.queries)
        .unwrap_or_else(|_| String::from("[Could not read .hx files]"));

    // Build and open GitHub issue
    let issue = GitHubIssueBuilder::new(cargo_output.errors_only.clone())
        .with_hx_content(hx_content)
        .with_generated_rust(generated_rust.to_string());

    print_status("BROWSER", "Opening GitHub issue page...");
    println!("Please review the content before submitting.");

    issue.open_in_browser()?;

    print_success("GitHub issue page opened in your browser");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ContainerRuntime, HelixConfig, ProjectConfig};
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_validate_vector_data_types_with_f64() {
        let temp_dir = tempdir().unwrap();
        let project_root = temp_dir.path();

        // Create a helix.toml config
        let config = HelixConfig {
            project: ProjectConfig {
                name: "test_project".to_string(),
                queries: std::path::PathBuf::from("./db/"),
                container_runtime: ContainerRuntime::Docker,
            },
            local: std::collections::HashMap::new(),
            cloud: std::collections::HashMap::new(),
        };

        // Create project context
        let project = ProjectContext {
            root: project_root.to_path_buf(),
            config,
            helix_dir: project_root.join(".helix"),
        };

        // Create db directory
        let db_dir = project_root.join("db");
        fs::create_dir_all(&db_dir).unwrap();

        // Create a .hx file with F64 vector fields
        let schema_content = r#"
V::Document {
    content: String,
    embedding: [F64],
    scores: [F64]
}

QUERY test() =>
    d <- V<Document>
    RETURN d
"#;

        fs::write(db_dir.join("schema.hx"), schema_content).unwrap();

        // Test validation - should detect F64 usage
        let result = validate_vector_data_types(&project);
        assert!(
            result.is_ok(),
            "Validation should succeed but warn about F64"
        );
    }

    #[test]
    fn test_validate_vector_data_types_with_f32() {
        let temp_dir = tempdir().unwrap();
        let project_root = temp_dir.path();

        // Create a helix.toml config
        let config = HelixConfig {
            project: ProjectConfig {
                name: "test_project".to_string(),
                queries: std::path::PathBuf::from("./db/"),
                container_runtime: ContainerRuntime::Docker,
            },
            local: std::collections::HashMap::new(),
            cloud: std::collections::HashMap::new(),
        };

        // Create project context
        let project = ProjectContext {
            root: project_root.to_path_buf(),
            config,
            helix_dir: project_root.join(".helix"),
        };

        // Create db directory
        let db_dir = project_root.join("db");
        fs::create_dir_all(&db_dir).unwrap();

        // Create a .hx file with F32 vector fields (correct)
        let schema_content = r#"
V::Document {
    content: String,
    embedding: [F32],
    scores: [F32]
}

QUERY test() =>
    d <- V<Document>
    RETURN d
"#;

        fs::write(db_dir.join("schema.hx"), schema_content).unwrap();

        // Test validation - should pass without warnings
        let result = validate_vector_data_types(&project);
        assert!(result.is_ok(), "Validation should succeed with F32");
    }

    #[test]
    fn test_contains_f64_type() {
        use helix_db::helixc::parser::types::FieldType;

        // Test direct F64
        assert!(contains_f64_type(&FieldType::F64));

        // Test F32 (should be false)
        assert!(!contains_f64_type(&FieldType::F32));

        // Test Array of F64
        assert!(contains_f64_type(&FieldType::Array(Box::new(
            FieldType::F64
        ))));

        // Test Array of F32 (should be false)
        assert!(!contains_f64_type(&FieldType::Array(Box::new(
            FieldType::F32
        ))));

        // Test nested object with F64
        let mut obj = std::collections::HashMap::new();
        obj.insert("score".to_string(), FieldType::F64);
        assert!(contains_f64_type(&FieldType::Object(obj)));

        // Test other types
        assert!(!contains_f64_type(&FieldType::String));
        assert!(!contains_f64_type(&FieldType::Boolean));
    }
}
