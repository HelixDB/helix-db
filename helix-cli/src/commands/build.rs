use crate::config::InstanceInfo;
use crate::docker::DockerManager;
use crate::metrics_sender::MetricsSender;
use crate::project::{ProjectContext, get_helix_repo_cache};
use crate::utils::{copy_dir_recursive_excluding, print_status, print_success};
use eyre::Result;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct MetricsData {
    pub queries_string: String,
    pub num_of_queries: u32,
}
use helix_db::{
    helix_engine::traversal_core::config::Config,
    helixc::{
        analyzer::analyze,
        generator::Source as GeneratedSource,
        parser::{
            HelixParser,
            types::{Content, HxFile, Source},
        },
    },
};
use std::fmt::Write;
use std::fs;

// Development flag - set to true when working on V2 locally
const DEV_MODE: bool = cfg!(debug_assertions);
const HELIX_REPO_URL: &str = "https://github.com/helixdb/helix-db.git";

// Get the cargo workspace root at compile time
const CARGO_MANIFEST_DIR: &str = env!("CARGO_MANIFEST_DIR");

pub async fn run(instance_name: String, metrics_sender: &MetricsSender) -> Result<MetricsData> {
    let start_time = Instant::now();

    // Load project context
    let project = ProjectContext::find_and_load(None)?;

    // Get instance config
    let instance_config = project.config.get_instance(&instance_name)?;

    print_status("BUILD", &format!("Building instance '{instance_name}'"));

    // Ensure Helix repo is cached
    ensure_helix_repo_cached().await?;

    // Prepare instance workspace
    prepare_instance_workspace(&project, &instance_name).await?;

    // Compile project queries into the workspace
    let compile_result = compile_project(&project, &instance_name).await;

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
        instance_name.clone(),
        metrics_data.queries_string.clone(),
        metrics_data.num_of_queries,
        compile_time,
        success,
        error_messages,
    );

    // Propagate compilation error if any
    compile_result?;

    // Generate Docker files
    generate_docker_files(&project, &instance_name, instance_config.clone()).await?;

    // For local instances, build Docker image
    if instance_config.should_build_docker_image() {
        let docker = DockerManager::new(&project);
        DockerManager::check_docker_available()?;
        docker.build_image(&instance_name, instance_config.docker_build_target())?;
    }

    print_success(&format!("Instance '{instance_name}' built successfully"));

    Ok(metrics_data.clone())
}

async fn ensure_helix_repo_cached() -> Result<()> {
    let repo_cache = get_helix_repo_cache()?;

    if !repo_cache.exists() {
        print_status("CACHE", "Caching Helix repository (first time setup)...");

        if DEV_MODE {
            // Development mode: copy from current workspace
            let workspace_root = std::path::Path::new(CARGO_MANIFEST_DIR)
                .parent() // helix-cli -> helix-db
                .ok_or_else(|| eyre::eyre!("Cannot determine workspace root"))?;

            print_status("DEV", "Development mode: copying local workspace...");
            copy_dir_recursive_excluding(workspace_root, &repo_cache, &["target"])?;
        } else {
            // Production mode: clone from GitHub
            let output = std::process::Command::new("git")
                .args(["clone", HELIX_REPO_URL, &repo_cache.to_string_lossy()])
                .output()?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(eyre::eyre!("Failed to clone Helix repository:\n{stderr}"));
            }
        }

        print_success("Helix repository cached successfully");
    } else {
        // Update existing repository
        print_status("UPDATE", "Updating Helix repository cache...");

        if DEV_MODE {
            // Development mode: re-copy from current workspace
            let workspace_root = std::path::Path::new(CARGO_MANIFEST_DIR)
                .parent()
                .ok_or_else(|| eyre::eyre!("Cannot determine workspace root"))?;

            // Remove old cache and copy fresh
            if repo_cache.exists() {
                std::fs::remove_dir_all(&repo_cache)?;
            }
            copy_dir_recursive_excluding(workspace_root, &repo_cache, &["target"])?;
        } else {
            // Production mode: git pull
            let output = std::process::Command::new("git")
                .args(["pull"])
                .current_dir(&repo_cache)
                .output()?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(eyre::eyre!(
                    "Failed to update Helix repository:\n{}",
                    stderr
                ));
            }
        }

        print_success("Helix repository updated");
    }

    Ok(())
}

async fn prepare_instance_workspace(project: &ProjectContext, instance_name: &str) -> Result<()> {
    print_status(
        "PREPARE",
        &format!("Preparing workspace for '{instance_name}'"),
    );

    // Ensure instance directories exist
    project.ensure_instance_dirs(instance_name)?;

    // Copy cached repo to instance workspace for Docker build context
    let repo_cache = get_helix_repo_cache()?;
    let instance_workspace = project.instance_workspace(instance_name);
    let repo_copy_path = instance_workspace.join("helix-repo-copy");

    // Remove existing copy if it exists
    if repo_copy_path.exists() {
        std::fs::remove_dir_all(&repo_copy_path)?;
    }

    // Copy cached repo to instance workspace
    copy_dir_recursive_excluding(&repo_cache, &repo_copy_path, &["target"])?;

    print_status(
        "COPY",
        &format!("Copied cached repo to {}", repo_copy_path.display()),
    );

    Ok(())
}

async fn compile_project(project: &ProjectContext, instance_name: &str) -> Result<MetricsData> {
    print_status("COMPILE", "Compiling Helix queries...");

    // Read project files
    let schema_path = project.root.join("schema.hx");
    let queries_path = project.root.join("queries.hx");

    if !schema_path.exists() {
        return Err(eyre::eyre!(
            "schema.hx not found. Run 'helix init' to create a project."
        ));
    }

    if !queries_path.exists() {
        return Err(eyre::eyre!(
            "queries.hx not found. Run 'helix init' to create a project."
        ));
    }

    // Create helix-container directory in instance workspace for generated files
    let instance_workspace = project.instance_workspace(instance_name);
    let helix_container_dir = instance_workspace.join("helix-container");
    let src_dir = helix_container_dir.join("src");

    // Create the directories
    fs::create_dir_all(&src_dir)?;

    // Copy schema file to helix-container/src
    fs::copy(&schema_path, src_dir.join("schema.hx"))?;

    // Generate config.hx.json from helix.toml
    let instance = project.config.get_instance(instance_name)?;
    let legacy_config_json = instance.to_legacy_json();
    let legacy_config_str = serde_json::to_string_pretty(&legacy_config_json)?;
    fs::write(src_dir.join("config.hx.json"), legacy_config_str)?;

    // Read and compile the .hx files using the same logic as the original CLI
    print_status("CODEGEN", "Generating Rust code from Helix queries...");

    // Collect all .hx files for compilation
    let hx_files = collect_hx_files(&project.root)?;

    // Generate content and compile using helix-db compilation logic
    let (analyzed_source, metrics_data) = compile_helix_files(&hx_files, &src_dir)?;

    // Write the generated Rust code to queries.rs
    let mut generated_rust_code = String::new();
    write!(&mut generated_rust_code, "{analyzed_source}")?;
    fs::write(src_dir.join("queries.rs"), generated_rust_code)?;

    print_success("Helix queries compiled to Rust files");
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

    print_status("DOCKER", "Generating Docker configuration...");

    let docker = DockerManager::new(project);

    // Generate Dockerfile
    let dockerfile_content = docker.generate_dockerfile(instance_name, instance_config.clone())?;
    let dockerfile_path = project.dockerfile_path(instance_name);
    fs::write(&dockerfile_path, dockerfile_content)?;

    // Generate docker-compose.yml
    let compose_content = docker.generate_docker_compose(instance_name, instance_config.clone())?;
    let compose_path = project.docker_compose_path(instance_name);
    fs::write(&compose_path, compose_content)?;

    print_success("Docker configuration generated");
    Ok(())
}

pub(crate) fn collect_hx_files(project_root: &std::path::Path) -> Result<Vec<std::fs::DirEntry>> {
    let files: Vec<std::fs::DirEntry> = std::fs::read_dir(project_root)?
        .filter_map(|entry| entry.ok())
        .filter(|file| file.file_name().to_string_lossy().ends_with(".hx"))
        .collect();

    let has_schema = files.iter().any(|file| file.file_name() == "schema.hx");
    if !has_schema {
        return Err(eyre::eyre!("No schema.hx file found"));
    }

    let has_queries = files.iter().any(|file| file.file_name() != "schema.hx");
    if !has_queries {
        return Err(eyre::eyre!("No query files (.hx) found"));
    }

    Ok(files)
}

fn compile_helix_files(
    files: &[std::fs::DirEntry],
    instance_src_dir: &std::path::Path,
) -> Result<(GeneratedSource, MetricsData)> {
    print_status("PARSE", "Parsing Helix files...");

    // Generate content from the files
    let content = generate_content(files)?;

    // Parse the content
    print_status("ANALYZE", "Analyzing Helix files...");
    let source = parse_content(&content)?;

    // Extract metrics data during parsing
    let query_names: Vec<String> = source.queries.iter().map(|q| q.name.clone()).collect();
    let metrics_data = MetricsData {
        queries_string: query_names.join("\n"),
        num_of_queries: query_names.len() as u32,
    };

    // Run static analysis
    let mut analyzed_source = analyze_source(source)?;

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
            let name = file.path().to_string_lossy().into_owned();
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
fn analyze_source(source: Source) -> Result<GeneratedSource> {
    if source.schema.is_empty() {
        return Err(eyre::eyre!("No schema definitions provided"));
    }

    let (diagnostics, generated_source) = analyze(&source);
    if !diagnostics.is_empty() {
        let mut error_msg = String::new();
        for diag in diagnostics {
            let filepath = diag.filepath.clone().unwrap_or("queries.hx".to_string());
            error_msg.push_str(&diag.render(&source.source, &filepath));
            error_msg.push('\n');
        }
        return Err(eyre::eyre!("Compilation failed:\n{error_msg}"));
    }

    Ok(generated_source)
}

/// Read the config.hx.json file from the instance workspace
fn read_config(instance_src_dir: &std::path::Path) -> Result<Config> {
    let config_path = instance_src_dir.join("config.hx.json");
    let schema_path = instance_src_dir.join("schema.hx");

    if !config_path.exists() {
        return Err(eyre::eyre!(
            "config.hx.json not found in instance workspace"
        ));
    }

    if !schema_path.exists() {
        return Err(eyre::eyre!("schema.hx not found in instance workspace"));
    }

    let config = Config::from_files(config_path, schema_path)
        .map_err(|e| eyre::eyre!("Failed to load config: {e}"))?;
    Ok(config)
}
