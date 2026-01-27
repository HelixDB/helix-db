//! Check command - validates project configuration, queries, and generated Rust code.

use crate::commands::build;
use crate::github_issue::{GitHubIssueBuilder, filter_errors_only};
use crate::metrics_sender::MetricsSender;
use crate::output::{Operation, Step};
use crate::project::ProjectContext;
use crate::utils::helixc_utils::{
    analyze_source, collect_hx_contents, collect_hx_files, generate_content, parse_content,
};
use crate::utils::{print_confirm, print_error, print_warning};
use eyre::Result;
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

    let op = Operation::new("Checking", instance_name);

    // Validate instance exists in config
    let _instance_config = project.config.get_instance(instance_name)?;

    // Step 1: Validate syntax first (quick check)
    let mut syntax_step = Step::with_messages("Validating syntax", "Syntax validated");
    syntax_step.start();
    validate_project_syntax(project)?;
    syntax_step.done();

    // Step 2: Ensure helix repo is cached (reuse from build.rs)
    let mut repo_step = Step::with_messages("Syncing repository", "Repository synced");
    repo_step.start();
    build::ensure_helix_repo_cached().await?;
    repo_step.done();

    // Step 3: Prepare instance workspace (reuse from build.rs)
    build::prepare_instance_workspace(project, instance_name).await?;

    // Step 4: Compile project - generate queries.rs (reuse from build.rs)
    let mut compile_step = Step::with_messages("Compiling queries", "Queries compiled");
    compile_step.start();
    let metrics_data = build::compile_project(project, instance_name).await?;
    compile_step.done_with_info(&format!("{} queries", metrics_data.num_of_queries));

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
    let mut cargo_step = Step::with_messages("Running cargo check", "Cargo check passed");
    cargo_step.start();
    Step::verbose_substep("Running cargo check on generated code...");
    let helix_container_dir = instance_workspace.join("helix-repo-copy/helix-container");
    let cargo_output = run_cargo_check(&helix_container_dir)?;

    let compile_time = start_time.elapsed().as_secs() as u32;

    if !cargo_output.success {
        cargo_step.fail();
        op.failure();

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

    cargo_step.done();
    op.success();
    Ok(())
}

async fn check_all_instances(
    project: &ProjectContext,
    metrics_sender: &MetricsSender,
) -> Result<()> {
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

    // Check each instance
    for instance_name in &instances {
        check_instance(project, instance_name, metrics_sender).await?;
    }

    crate::output::success("All instances checked successfully");
    Ok(())
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

    crate::output::info("Opening GitHub issue page...");
    println!("Please review the content before submitting.");

    issue.open_in_browser()?;

    crate::output::success("GitHub issue page opened in your browser");

    Ok(())
}
