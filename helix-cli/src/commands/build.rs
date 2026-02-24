use crate::config::InstanceInfo;
use crate::docker::{DockerBuildError, DockerManager};
use crate::github_issue::{GitHubIssueBuilder, filter_errors_only};
use crate::metrics_sender::MetricsSender;
use crate::output::{Operation, Step};
use crate::project::{ProjectContext, get_helix_repo_cache};
use crate::prompts;
use crate::utils::{
    copy_dir_recursive_excluding, diagnostic_source,
    helixc_utils::{collect_hx_contents, collect_hx_files},
    print_confirm, print_error, print_warning,
};
use eyre::{Result, eyre};
use std::process::Command;
use std::sync::OnceLock;
use std::time::Instant;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct MetricsData {
    pub queries_string: String,
    pub num_of_queries: u32,
}
use helix_db::{
    helix_engine::traversal_core::config::Config,
    helixc::{
        analyzer::{analyze, diagnostic::DiagnosticSeverity},
        generator::Source as GeneratedSource,
        parser::{
            HelixParser,
            types::{Content, HxFile, Source},
        },
    },
};
use std::{fmt::Write, fs};

// Development flag - set to true when working on V2 locally
const DEV_MODE: bool = cfg!(debug_assertions);
const HELIX_REPO_URL: &str = "https://github.com/helixdb/helix-db.git";

// Get the cargo workspace root at compile time
const CARGO_MANIFEST_DIR: &str = env!("CARGO_MANIFEST_DIR");

static REPO_CACHE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn repo_cache_lock() -> &'static Mutex<()> {
    REPO_CACHE_LOCK.get_or_init(|| Mutex::new(()))
}

pub async fn run(
    instance_name: Option<String>,
    bin: Option<String>,
    metrics_sender: &MetricsSender,
) -> Result<MetricsData> {
    // Load project context
    let project = ProjectContext::find_and_load(None)?;

    // Get instance name - prompt if not provided
    let instance_name = match instance_name {
        Some(name) => name,
        None if prompts::is_interactive() => {
            let instances = project.config.list_instances_with_types();
            prompts::intro(
                "helix build",
                Some(
                    "This will build your selected instance based on the configuration in helix.toml.",
                ),
            )?;
            prompts::select_instance(&instances)?
        }
        None => {
            let instances = project.config.list_instances();
            return Err(eyre::eyre!(
                "No instance specified. Available instances: {}",
                instances
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
    };

    // Start the build operation
    let op = Operation::new("Building", &instance_name);

    // Run the build steps
    let result = run_build_steps(
        &op,
        &project,
        &instance_name,
        bin.as_deref(),
        metrics_sender,
    )
    .await;

    match &result {
        Ok(_) => op.success(),
        Err(_) => op.failure(),
    }

    result
}

/// Run the build steps without creating an Operation (for use by other commands like push)
pub async fn run_build_steps(
    _op: &Operation,
    project: &ProjectContext,
    instance_name: &str,
    bin: Option<&str>,
    metrics_sender: &MetricsSender,
) -> Result<MetricsData> {
    let start_time = Instant::now();

    // Get instance config
    let instance_config = project.config.get_instance(instance_name)?;

    // Step 1: Repository sync
    let mut repo_step = Step::with_messages("Syncing repository", "Repository synced");
    repo_step.start();
    ensure_helix_repo_cached().await?;
    repo_step.done();

    // Step 2: Prepare workspace (verbose only shows details)
    prepare_instance_workspace(project, instance_name).await?;

    // Step 3: Compile project queries
    let mut compile_step = Step::with_messages("Compiling queries", "Queries compiled");
    compile_step.start();
    let compile_result = compile_project(project, instance_name).await;

    // Collect metrics data
    let compile_time = start_time.elapsed().as_secs() as u32;
    let success = compile_result.is_ok();
    let error_messages = compile_result.as_ref().err().map(|e| e.to_string());

    // Get metrics data from compilation result or use defaults
    let metrics_data = match &compile_result {
        Ok(data) => data.clone(),
        Err(_) => MetricsData {
            queries_string: String::new(),
            num_of_queries: 0,
        },
    };

    // Send compile metrics
    metrics_sender.send_compile_event(
        instance_name.to_string(),
        metrics_data.queries_string.clone(),
        metrics_data.num_of_queries,
        compile_time,
        success,
        error_messages,
    );

    // Propagate compilation error if any (fail step on error)
    match &compile_result {
        Ok(data) => {
            compile_step.done_with_info(&format!("{} queries", data.num_of_queries));
        }
        Err(_) => {
            compile_step.fail();
            return Err(compile_result.unwrap_err());
        }
    }

    // Binary output or Docker build
    if let Some(binary_output) = bin {
        let mut cargo_step = Step::with_messages("Building binary", "Binary built");
        cargo_step.start();
        match build_binary_using_cargo(project, instance_name, binary_output) {
            Ok(()) => cargo_step.done(),
            Err(e) => {
                cargo_step.fail();
                return Err(e);
            }
        }
    } else if instance_config.should_build_docker_image() {
        // Generate Docker files
        generate_docker_files(project, instance_name, instance_config.clone()).await?;
        let runtime = project.config.project.container_runtime;
        DockerManager::check_runtime_available(runtime)?;
        let docker = DockerManager::new(project);

        let mut docker_step = Step::with_messages("Building Docker image", "Docker image built");
        docker_step.start();

        match docker.build_image(instance_name, instance_config.docker_build_target()) {
            Ok(()) => {
                docker_step.done();
            }
            Err(e) => {
                docker_step.fail();
                // Check if this is a Rust compilation error
                if let Some(DockerBuildError::RustCompilation { output, .. }) =
                    e.downcast_ref::<DockerBuildError>()
                {
                    handle_docker_rust_compilation_failure(output, project)?;
                }
                return Err(e);
            }
        }
    }

    Ok(metrics_data.clone())
}

pub(crate) async fn ensure_helix_repo_cached() -> Result<()> {
    let _lock = repo_cache_lock().lock().await;
    let repo_cache = get_helix_repo_cache()?;

    if needs_cache_recreation(&repo_cache)? {
        recreate_helix_cache(&repo_cache).await?;
    } else if repo_cache.exists() {
        update_helix_cache(&repo_cache).await?;
    } else {
        create_helix_cache(&repo_cache).await?;
    }

    Ok(())
}

fn needs_cache_recreation(repo_cache: &std::path::Path) -> Result<bool> {
    if !repo_cache.exists() {
        return Ok(false);
    }

    let is_git_repo = repo_cache.join(".git").exists();

    match (DEV_MODE, is_git_repo) {
        (true, true) => {
            Step::verbose_substep("Cache is git repo but DEV_MODE requires copy - recreating...");
            Ok(true)
        }
        (false, false) => {
            Step::verbose_substep(
                "Cache is copy but production mode requires git repo - recreating...",
            );
            Ok(true)
        }
        _ => Ok(false),
    }
}

async fn recreate_helix_cache(repo_cache: &std::path::Path) -> Result<()> {
    std::fs::remove_dir_all(repo_cache)?;
    create_helix_cache(repo_cache).await
}

async fn create_helix_cache(repo_cache: &std::path::Path) -> Result<()> {
    Step::verbose_substep("Caching Helix repository (first time setup)...");

    if DEV_MODE {
        create_dev_cache(repo_cache)?;
    } else {
        create_git_cache(repo_cache)?;
    }

    Ok(())
}

async fn update_helix_cache(repo_cache: &std::path::Path) -> Result<()> {
    Step::verbose_substep("Updating Helix repository cache...");

    if DEV_MODE {
        update_dev_cache(repo_cache)?;
    } else {
        update_git_cache(repo_cache)?;
    }

    Ok(())
}

fn create_dev_cache(repo_cache: &std::path::Path) -> Result<()> {
    let workspace_root = std::path::Path::new(CARGO_MANIFEST_DIR)
        .parent() // helix-cli -> helix-db
        .ok_or_else(|| eyre::eyre!("Cannot determine workspace root"))?;

    Step::verbose_substep("Development mode: copying local workspace...");
    copy_dir_recursive_excluding(workspace_root, repo_cache)
}

fn create_git_cache(repo_cache: &std::path::Path) -> Result<()> {
    let output = std::process::Command::new("git")
        .args(["clone", HELIX_REPO_URL, &repo_cache.to_string_lossy()])
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let error = crate::errors::CliError::new("failed to clone Helix repository")
            .with_context(stderr.to_string())
            .with_hint("ensure git is installed and you have internet connectivity");
        return Err(eyre::eyre!("{}", error.render()));
    }

    Ok(())
}

fn update_dev_cache(repo_cache: &std::path::Path) -> Result<()> {
    let workspace_root = std::path::Path::new(CARGO_MANIFEST_DIR)
        .parent()
        .ok_or_else(|| eyre::eyre!("Cannot determine workspace root"))?;

    // Remove old cache and copy fresh
    if repo_cache.exists() {
        std::fs::remove_dir_all(repo_cache)?;
    }
    copy_dir_recursive_excluding(workspace_root, repo_cache)
}

fn update_git_cache(repo_cache: &std::path::Path) -> Result<()> {
    let output = std::process::Command::new("git")
        .args(["pull"])
        .current_dir(repo_cache)
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(eyre::eyre!(
            "Failed to update Helix repository:\n{}",
            stderr
        ));
    }

    Ok(())
}

pub(crate) async fn prepare_instance_workspace(
    project: &ProjectContext,
    instance_name: &str,
) -> Result<()> {
    Step::verbose_substep(&format!("Preparing workspace for '{instance_name}'"));

    // Ensure instance directories exist
    project.ensure_instance_dirs(instance_name)?;

    // Copy cached repo to instance workspace for Docker build context
    let _lock = repo_cache_lock().lock().await;
    let repo_cache = get_helix_repo_cache()?;
    let instance_workspace = project.instance_workspace(instance_name);
    let repo_copy_path = instance_workspace.join("helix-repo-copy");

    // Remove existing copy if it exists
    if repo_copy_path.exists() {
        std::fs::remove_dir_all(&repo_copy_path)?;
    }

    // Copy cached repo to instance workspace
    copy_dir_recursive_excluding(&repo_cache, &repo_copy_path)?;

    Step::verbose_substep(&format!(
        "Copied cached repo to {}",
        repo_copy_path.display()
    ));

    Ok(())
}

pub(crate) async fn compile_project(
    project: &ProjectContext,
    instance_name: &str,
) -> Result<MetricsData> {
    // Create helix-container directory in instance workspace for generated files
    let instance_workspace = project.instance_workspace(instance_name);
    let helix_container_dir = instance_workspace.join("helix-container");
    let src_dir = helix_container_dir.join("src");

    // Create the directories
    fs::create_dir_all(&src_dir)?;

    // Generate config.hx.json from helix.toml
    let instance = project.config.get_instance(instance_name)?;
    let legacy_config_json = instance.to_legacy_json();
    let legacy_config_str = serde_json::to_string_pretty(&legacy_config_json)?;
    fs::write(src_dir.join("config.hx.json"), legacy_config_str)?;

    Step::verbose_substep("Generating Rust code from Helix queries...");

    // Collect all .hx files for compilation
    let hx_files = collect_hx_files(&project.root, &project.config.project.queries)?;

    // Generate content and compile using helix-db compilation logic
    let (analyzed_source, metrics_data) = compile_helix_files(&hx_files, &src_dir)?;

    // Write the generated Rust code to queries.rs
    let mut generated_rust_code = String::new();
    write!(&mut generated_rust_code, "{analyzed_source}")?;
    fs::write(src_dir.join("queries.rs"), generated_rust_code)?;

    Ok(metrics_data)
}

async fn generate_docker_files(
    project: &ProjectContext,
    instance_name: &str,
    instance_config: InstanceInfo<'_>,
) -> Result<()> {
    if !instance_config.should_build_docker_image() {
        // Cloud instances don't need Docker files
        return Ok(());
    }

    let docker = DockerManager::new(project);

    Step::verbose_substep(&format!(
        "{} configuration generated",
        docker.runtime.label()
    ));

    // Generate Dockerfile
    let dockerfile_content = docker.generate_dockerfile(instance_name, instance_config.clone())?;
    let dockerfile_path = project.dockerfile_path(instance_name);
    fs::write(&dockerfile_path, dockerfile_content)?;

    // Generate docker-compose.yml
    let compose_content =
        docker.generate_docker_compose(instance_name, instance_config.clone(), None)?;
    let compose_path = project.docker_compose_path(instance_name);
    fs::write(&compose_path, compose_content)?;

    Ok(())
}

fn compile_helix_files(
    files: &[std::fs::DirEntry],
    instance_src_dir: &std::path::Path,
) -> Result<(GeneratedSource, MetricsData)> {
    Step::verbose_substep("Parsing Helix files...");

    // Generate content from the files
    let content = generate_content(files)?;

    // Parse the content
    Step::verbose_substep("Analyzing Helix files...");
    let source = parse_content(&content)?;

    // Extract metrics data during parsing
    let query_names: Vec<String> = source.queries.iter().map(|q| q.name.clone()).collect();
    let metrics_data = MetricsData {
        queries_string: query_names.join("\n"),
        num_of_queries: query_names.len() as u32,
    };

    // Run static analysis
    let mut analyzed_source = analyze_source(source, &content.files)?;

    // Read and set the config from the instance workspace
    analyzed_source.config = read_config(instance_src_dir)?;

    Ok((analyzed_source, metrics_data))
}

/// Generates a Content object from a vector of DirEntry objects
/// Returns a Content object with the files and source
pub(crate) fn generate_content(files: &[std::fs::DirEntry]) -> Result<Content> {
    let files: Vec<HxFile> = files
        .iter()
        .map(|file| {
            let name = file
                .path()
                .canonicalize()
                .unwrap_or_else(|_| file.path())
                .to_string_lossy()
                .into_owned();
            let content = fs::read_to_string(file.path())
                .map_err(|e| eyre::eyre!("Failed to read file {name}: {e}"))?;
            Ok(HxFile { name, content })
        })
        .collect::<Result<Vec<_>>>()?;

    let content = files
        .iter()
        .map(|file| file.content.clone())
        .collect::<Vec<String>>()
        .join("\n");

    Ok(Content {
        content,
        files,
        source: Source::default(),
    })
}

/// Uses the helix parser to parse the content into a Source object
fn parse_content(content: &Content) -> Result<Source> {
    let source = HelixParser::parse_source(content).map_err(|e| eyre::eyre!("Parse error: {e}"))?;
    Ok(source)
}

/// Runs the static analyzer on the parsed source to catch errors and generate diagnostics if any.
/// Otherwise returns the generated source object which is an IR used to transpile the queries to rust.
fn analyze_source(source: Source, files: &[HxFile]) -> Result<GeneratedSource> {
    if source.schema.is_empty() {
        let error = crate::errors::CliError::new("no schema definitions found in project")
            .with_hint("add at least one schema definition like 'N::User { name: String }' to your .hx files");
        return Err(eyre::eyre!("{}", error.render()));
    }

    let (diagnostics, generated_source) =
        analyze(&source).map_err(|e| eyre::eyre!("Analysis error: {}", e))?;

    let (error_diagnostics, warning_diagnostics): (Vec<_>, Vec<_>) = diagnostics
        .into_iter()
        .partition(|d| matches!(d.severity, DiagnosticSeverity::Error));

    if !warning_diagnostics.is_empty() {
        let mut warning_msg = String::new();
        for diag in &warning_diagnostics {
            let filepath = diag.filepath.clone().unwrap_or("queries.hx".to_string());
            let snippet_src = diagnostic_source(&filepath, files, &generated_source.src);
            warning_msg.push_str(&diag.render(snippet_src.as_ref(), &filepath));
            warning_msg.push('\n');
        }
        eprintln!(
            "Compilation produced {} warning(s):\n{warning_msg}",
            warning_diagnostics.len()
        );
    }

    if !error_diagnostics.is_empty() {
        let mut error_msg = String::new();
        for diag in error_diagnostics {
            let filepath = diag.filepath.clone().unwrap_or("queries.hx".to_string());
            let snippet_src = diagnostic_source(&filepath, files, &source.source);
            error_msg.push_str(&diag.render(snippet_src.as_ref(), &filepath));
            error_msg.push('\n');
        }
        return Err(eyre::eyre!("Compilation failed:\n{error_msg}"));
    }

    Ok(generated_source)
}

/// Read the config.hx.json file from the instance workspace
fn read_config(instance_src_dir: &std::path::Path) -> Result<Config> {
    let config_path = instance_src_dir.join("config.hx.json");

    if !config_path.exists() {
        return Err(eyre::eyre!(
            "config.hx.json not found in instance workspace"
        ));
    }

    let config =
        Config::from_file(config_path).map_err(|e| eyre::eyre!("Failed to load config: {e}"))?;
    Ok(config)
}

/// Handle Rust compilation failure during Docker build - print errors and offer GitHub issue creation.
fn handle_docker_rust_compilation_failure(
    docker_output: &str,
    project: &ProjectContext,
) -> Result<()> {
    print_error("Rust compilation failed during Docker build");
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

    // Filter to get just cargo errors from the Docker output
    let cargo_errors = filter_errors_only(docker_output);

    // Collect .hx content
    let hx_content = collect_hx_contents(&project.root, &project.config.project.queries)
        .unwrap_or_else(|_| String::from("[Could not read .hx files]"));

    // Build and open GitHub issue (no generated Rust available from Docker build)
    let issue = GitHubIssueBuilder::new(cargo_errors).with_hx_content(hx_content);

    crate::output::info("Opening GitHub issue page...");
    println!("Please review the content before submitting.");

    issue.open_in_browser()?;

    crate::output::success("GitHub issue page opened in your browser");

    Ok(())
}

fn build_binary_using_cargo(
    project: &ProjectContext,
    instance_name: &str,
    binary_output: &str,
) -> Result<()> {
    let binary_output_path = std::path::Path::new(binary_output);
    std::fs::create_dir_all(binary_output_path)?;

    // <path-to-.helix>/<instance_name>/helix-repo-copy/helix-container/
    let current_dir = project
        .helix_dir
        .join(instance_name)
        .join("helix-repo-copy")
        .join("helix-container");

    let status = Command::new("cargo")
        .arg("build")
        .arg("--target-dir")
        .arg(binary_output_path.as_os_str())
        .current_dir(current_dir)
        .status()?;

    if !status.success() {
        return Err(eyre!(
            "Cargo build failed with exit code: {:?}",
            status.code()
        ));
    }
    Ok(())
}
