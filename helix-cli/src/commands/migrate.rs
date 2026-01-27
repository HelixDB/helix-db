use crate::config::{
    BuildMode, ContainerRuntime, DbConfig, GraphConfig, HelixConfig, LocalInstanceConfig,
    ProjectConfig, VectorConfig,
};
use crate::errors::{CliError, project_error};
use crate::output;
use crate::utils::print_instructions;
use color_eyre::owo_colors::OwoColorize;
use eyre::Result;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
struct V1Config {
    vector_config: V1VectorConfig,
    graph_config: V1GraphConfig,
    db_max_size_gb: u32,
    mcp: bool,
    bm25: bool,
}

#[derive(Debug, Clone)]
struct V1VectorConfig {
    m: u32,
    ef_construction: u32,
    ef_search: u32,
    db_max_size: u32,
}

#[derive(Debug, Clone)]
struct V1GraphConfig {
    secondary_indices: Vec<String>,
}

#[derive(Debug)]
#[allow(unused)]
struct MigrationContext {
    project_dir: PathBuf,
    project_name: String,
    v1_config: V1Config,
    hx_files: Vec<PathBuf>,
    queries_dir: String,
    instance_name: String,
    port: u16,
    dry_run: bool,
    no_backup: bool,
}

pub async fn run(
    path: Option<String>,
    queries_dir: String,
    instance_name: String,
    port: u16,
    dry_run: bool,
    no_backup: bool,
) -> Result<()> {
    let project_dir = match path {
        Some(p) => PathBuf::from(p),
        None => env::current_dir()?,
    };

    output::info("Detecting v1 project structure");

    // Step 1: Detect and validate v1 project
    let v1_config = detect_and_parse_v1_config(&project_dir)?;
    let hx_files = find_hx_files(&project_dir)?;

    let project_name = project_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("helix-project")
        .to_string();

    output::success(&format!(
        "Found v1 project '{}' with {} .hx files",
        project_name,
        hx_files.len()
    ));

    // Step 2: Check if v2 project already exists
    let helix_toml_path = project_dir.join("helix.toml");
    if helix_toml_path.exists() {
        return Err(project_error("helix.toml already exists in this directory")
            .with_hint("This appears to be a v2 project already. Migration not needed.")
            .into());
    }

    let migration_ctx = MigrationContext {
        project_dir,
        project_name,
        v1_config,
        hx_files,
        queries_dir,
        instance_name,
        port,
        dry_run,
        no_backup,
    };

    if dry_run {
        output::info("Showing planned migration changes");
        show_migration_plan(&migration_ctx)?;
        return Ok(());
    }

    // Step 3: Perform migration
    output::info("Starting migration to v2 format");

    // Create backup if requested
    if !no_backup {
        create_backup(&migration_ctx)?;
    }

    // Migrate home directory structure
    migrate_home_directory(&migration_ctx)?;

    // Create queries directory and move files
    migrate_file_structure(&migration_ctx)?;

    // Create v2 config
    create_v2_config(&migration_ctx)?;

    output::success(&format!(
        "Successfully migrated project to v2 format with instance '{}'",
        migration_ctx.instance_name
    ));

    // Provide enhanced guidance for both local and cloud users
    provide_post_migration_guidance(&migration_ctx)?;

    Ok(())
}

fn detect_and_parse_v1_config(project_dir: &Path) -> Result<V1Config> {
    let config_path = project_dir.join("config.hx.json");

    if !config_path.exists() {
        return Err(CliError::new("No config.hx.json file found")
            .with_hint("This doesn't appear to be a v1 Helix project")
            .into());
    }

    let config_content = fs::read_to_string(&config_path).map_err(|e| {
        CliError::new("Failed to read config.hx.json").with_caused_by(e.to_string())
    })?;

    let json: JsonValue = serde_json::from_str(&config_content).map_err(|e| {
        CliError::new("Failed to parse config.hx.json").with_caused_by(e.to_string())
    })?;

    // Parse vector_config
    let vector_config_json = json
        .get("vector_config")
        .ok_or_else(|| CliError::new("Missing vector_config in config.hx.json"))?;

    let vector_config = V1VectorConfig {
        m: vector_config_json
            .get("m")
            .and_then(|v| v.as_u64())
            .unwrap_or(16) as u32,
        ef_construction: vector_config_json
            .get("ef_construction")
            .and_then(|v| v.as_u64())
            .unwrap_or(128) as u32,
        ef_search: vector_config_json
            .get("ef_search")
            .and_then(|v| v.as_u64())
            .unwrap_or(768) as u32,
        db_max_size: vector_config_json
            .get("db_max_size")
            .and_then(|v| v.as_u64())
            .unwrap_or(20) as u32,
    };

    // Parse graph_config
    let graph_config_json = json
        .get("graph_config")
        .ok_or_else(|| CliError::new("Missing graph_config in config.hx.json"))?;

    let secondary_indices = graph_config_json
        .get("secondary_indices")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect()
        })
        .unwrap_or_default();

    let graph_config = V1GraphConfig { secondary_indices };

    // Parse other config fields
    let db_max_size_gb = json
        .get("db_max_size_gb")
        .and_then(|v| v.as_u64())
        .unwrap_or(vector_config.db_max_size as u64) as u32;

    let mcp = json.get("mcp").and_then(|v| v.as_bool()).unwrap_or(true);

    let bm25 = json.get("bm25").and_then(|v| v.as_bool()).unwrap_or(true);

    Ok(V1Config {
        vector_config,
        graph_config,
        db_max_size_gb,
        mcp,
        bm25,
    })
}

fn find_hx_files(project_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut hx_files = Vec::new();

    // Check for schema.hx
    let schema_path = project_dir.join("schema.hx");
    if !schema_path.exists() {
        return Err(CliError::new("No schema.hx file found")
            .with_hint("This doesn't appear to be a v1 Helix project")
            .into());
    }
    hx_files.push(schema_path);

    // Find all other .hx files
    for entry in fs::read_dir(project_dir).map_err(|e| {
        CliError::new("Failed to read project directory").with_caused_by(e.to_string())
    })? {
        let entry = entry?;
        let path = entry.path();

        if let Some(extension) = path.extension()
            && extension == "hx"
            && path.file_name() != Some("schema.hx".as_ref())
        {
            hx_files.push(path);
        }
    }

    if hx_files.len() == 1 {
        return Err(
            CliError::new("No query files (.hx) found besides schema.hx")
                .with_hint("This doesn't appear to be a complete v1 project")
                .into(),
        );
    }

    Ok(hx_files)
}

fn show_migration_plan(ctx: &MigrationContext) -> Result<()> {
    println!();
    println!(
        "{}",
        format!("📋 Migration Plan for '{}':", ctx.project_name)
            .bold()
            .underline()
    );
    println!(
        "  {}: {}",
        "Project directory".bright_white().bold(),
        ctx.project_dir.display()
    );
    println!();

    println!("{}", "📁 File Structure Changes:".bold().underline());
    println!(
        "  {}: {}",
        "Create directory".bright_white().bold(),
        ctx.queries_dir
    );
    println!(
        "  {}: .helix/v1-backup/",
        "Create directory".bright_white().bold()
    );
    for hx_file in &ctx.hx_files {
        let file_name = hx_file.file_name().unwrap().to_string_lossy();
        let dest_path = PathBuf::from(&ctx.queries_dir).join(&*file_name);
        println!(
            "  {}: {} → {}",
            "Move file".bright_white().bold(),
            file_name,
            dest_path.display()
        );
    }
    println!("  {}: helix.toml", "Create file".bright_white().bold());

    if !ctx.no_backup {
        println!(
            "  {}: .helix/v1-backup/config.hx.json",
            "Create backup".bright_white().bold()
        );
    } else {
        println!("  {}: config.hx.json", "Remove file".bright_white().bold());
    }

    println!();
    println!("{}", "🏠 Home Directory Migration:".bold().underline());
    let home_dir =
        dirs::home_dir().ok_or_else(|| CliError::new("Could not find home directory"))?;
    let v1_helix_dir = home_dir.join(".helix");
    if v1_helix_dir.exists() {
        let v2_marker = v1_helix_dir.join(".v2");
        if v2_marker.exists() {
            println!(
                "  {}: ~/.helix directory already migrated to v2",
                "Already migrated".bright_white().bold()
            );
        } else {
            println!(
                "  {}: ~/.helix → ~/.helix-v1-backup",
                "Create backup".bright_white().bold()
            );
            if v1_helix_dir.join("dockerdev").exists() {
                println!(
                    "  {}: Stop/remove helix-dockerdev containers and images",
                    "Clean up Docker".bright_white().bold()
                );
            }
            println!(
                "  {}: Remove all except ~/.helix/credentials and ~/.helix/repo",
                "Clean directory".bright_white().bold()
            );
            if v1_helix_dir.join("credentials").exists() {
                println!(
                    "  {}: ~/.helix/credentials",
                    "Preserve file".bright_white().bold()
                );
            }
            if v1_helix_dir.join("repo").exists() {
                println!(
                    "  {}: ~/.helix/repo",
                    "Preserve directory".bright_white().bold()
                );
            }
            println!(
                "  {}: Create ~/.helix/.v2 marker file",
                "Mark migrated".bright_white().bold()
            );
        }
    } else {
        println!(
            "  {}: ~/.helix directory not found",
            "No action needed".bright_white().bold()
        );
    }

    println!();
    println!("{}", "⚙️  Configuration Migration:".bold().underline());
    println!(
        "  {}: {}",
        "Instance name".bright_white().bold(),
        ctx.instance_name
    );
    println!("  {}: {}", "Instance port".bright_white().bold(), ctx.port);
    println!(
        "  {}: m={}, ef_construction={}, ef_search={}",
        "Vector config".bright_white().bold(),
        ctx.v1_config.vector_config.m,
        ctx.v1_config.vector_config.ef_construction,
        ctx.v1_config.vector_config.ef_search
    );
    println!(
        "  {}: {}GB",
        "Database max size".bright_white().bold(),
        ctx.v1_config.db_max_size_gb
    );
    println!(
        "  {}: {}",
        "MCP enabled".bright_white().bold(),
        ctx.v1_config.mcp
    );
    println!(
        "  {}: {}",
        "BM25 enabled".bright_white().bold(),
        ctx.v1_config.bm25
    );
    println!(
        "  {}: {}",
        "Secondary indices".bright_white().bold(),
        ctx.v1_config.graph_config.secondary_indices.len()
    );

    println!();
    println!("  To perform the migration, run the same command without --dry-run");

    Ok(())
}

fn create_backup(ctx: &MigrationContext) -> Result<()> {
    output::info("Creating backup of v1 files");

    // Create .helix/v1-backup directory
    let backup_dir = ctx.project_dir.join(".helix/v1-backup");
    fs::create_dir_all(&backup_dir).map_err(|e| {
        CliError::new("Failed to create backup directory").with_caused_by(e.to_string())
    })?;

    let backup_path = backup_dir.join("config.hx.json");
    let original_path = ctx.project_dir.join("config.hx.json");

    fs::copy(&original_path, &backup_path)
        .map_err(|e| CliError::new("Failed to create backup").with_caused_by(e.to_string()))?;

    output::success("Created backup: .helix/v1-backup/config.hx.json");
    Ok(())
}

fn migrate_file_structure(ctx: &MigrationContext) -> Result<()> {
    output::info("Migrating file structure");

    // Create queries directory
    let queries_dir_path = ctx.project_dir.join(&ctx.queries_dir);
    fs::create_dir_all(&queries_dir_path).map_err(|e| {
        CliError::new("Failed to create queries directory").with_caused_by(e.to_string())
    })?;

    // Move .hx files
    for hx_file in &ctx.hx_files {
        let file_name = hx_file.file_name().unwrap();
        let dest_path = queries_dir_path.join(file_name);

        fs::rename(hx_file, &dest_path).map_err(|e| {
            CliError::new(format!("Failed to move {}", hx_file.display()))
                .with_caused_by(e.to_string())
        })?;

        output::info(&format!(
            "Moved {} to {}",
            file_name.to_string_lossy(),
            PathBuf::from(&ctx.queries_dir).display()
        ));
    }

    // Remove or backup config.hx.json
    let config_path = ctx.project_dir.join("config.hx.json");
    fs::remove_file(&config_path).map_err(|e| {
        CliError::new("Failed to remove config.hx.json").with_caused_by(e.to_string())
    })?;

    Ok(())
}

fn create_v2_config(ctx: &MigrationContext) -> Result<()> {
    output::info("Creating helix.toml configuration");

    // Create vector config
    let vector_config = VectorConfig {
        m: ctx.v1_config.vector_config.m,
        ef_construction: ctx.v1_config.vector_config.ef_construction,
        ef_search: ctx.v1_config.vector_config.ef_search,
        db_max_size_gb: ctx.v1_config.db_max_size_gb,
    };

    // Create graph config
    let graph_config = GraphConfig {
        secondary_indices: ctx.v1_config.graph_config.secondary_indices.clone(),
    };

    // Create db config
    let db_config = DbConfig {
        vector_config,
        graph_config,
        mcp: ctx.v1_config.mcp,
        bm25: ctx.v1_config.bm25,
        schema: None,
        embedding_model: Some("text-embedding-ada-002".to_string()),
        graphvis_node_label: None,
    };

    // Create local instance config
    let local_config = LocalInstanceConfig {
        port: Some(ctx.port),
        build_mode: BuildMode::Dev,
        db_config,
    };

    // Create local instances map
    let mut local = HashMap::new();
    local.insert(ctx.instance_name.clone(), local_config);

    // Create project config
    let project_config = ProjectConfig {
        name: ctx.project_name.clone(),
        queries: PathBuf::from(&ctx.queries_dir),
        container_runtime: ContainerRuntime::Docker,
    };

    // Create final helix config
    let helix_config = HelixConfig {
        project: project_config,
        local,
        cloud: HashMap::new(),
    };

    // Save to file
    let config_path = ctx.project_dir.join("helix.toml");
    helix_config
        .save_to_file(&config_path)
        .map_err(|e| CliError::new("Failed to create helix.toml").with_caused_by(e.to_string()))?;

    output::success("Created helix.toml configuration");
    Ok(())
}

fn provide_post_migration_guidance(ctx: &MigrationContext) -> Result<()> {
    // Check if user has Helix Cloud credentials
    let has_cloud_credentials = check_cloud_credentials();

    print_instructions(
        "Next steps:",
        &[
            &format!(
                "Run 'helix check {}' to validate your configuration",
                ctx.instance_name
            ),
            &format!(
                "Run 'helix push {}' to start your instance",
                ctx.instance_name
            ),
        ],
    );

    if has_cloud_credentials {
        output::info("You're authenticated with Helix Cloud");
        output::info("The CLI v2 has enhanced cloud features with better instance management");
        print_instructions(
            "To set up cloud instances:",
            &[
                "Run 'helix add cloud --name production' to add a production instance",
                "Run 'helix add cloud --name staging' to add a staging instance",
                "Run 'helix build production' to build for your cloud instance",
                "Run 'helix push production' to deploy to Helix Cloud",
            ],
        );
    } else {
        output::info("Ready for Helix Cloud?");
        output::info("Take your project to production with managed infrastructure");
        print_instructions(
            "To get started with Helix Cloud:",
            &[
                "Run 'helix auth login' to authenticate with Helix Cloud",
                "Run 'helix add cloud --name production' to add a cloud instance",
                "Run 'helix push production' to deploy to the cloud",
            ],
        );
    }

    Ok(())
}

fn migrate_home_directory(_ctx: &MigrationContext) -> Result<()> {
    output::info("Migrating ~/.helix directory");

    let home_dir =
        dirs::home_dir().ok_or_else(|| CliError::new("Could not find home directory"))?;

    let v1_helix_dir = home_dir.join(".helix");

    if !v1_helix_dir.exists() {
        output::info("No ~/.helix directory found, skipping home migration");
        return Ok(());
    }

    // Check if already migrated
    let v2_marker = v1_helix_dir.join(".v2");
    if v2_marker.exists() {
        output::info("~/.helix directory already migrated to v2, skipping home migration");
        return Ok(());
    }

    // Create backup of the entire .helix directory
    let backup_dir = home_dir.join(".helix-v1-backup");
    if backup_dir.exists() {
        fs::remove_dir_all(&backup_dir).map_err(|e| {
            CliError::new("Failed to remove existing backup directory")
                .with_caused_by(e.to_string())
        })?;
    }

    // Use the utility function to copy the directory without exclusions
    crate::utils::copy_dir_recursively(&v1_helix_dir, &backup_dir).map_err(|e| {
        CliError::new("Failed to backup ~/.helix directory").with_caused_by(e.to_string())
    })?;

    output::success("Created backup: ~/.helix-v1-backup");

    // Clean up dockerdev containers/images if present
    let dockerdev_dir = v1_helix_dir.join("dockerdev");
    if dockerdev_dir.exists() {
        cleanup_dockerdev()?;
    }

    // Remove everything except credentials and repo
    let credentials_path = v1_helix_dir.join("credentials");
    let repo_path = v1_helix_dir.join("repo");

    // Temporarily move credentials and repo out of the way
    let temp_credentials = if credentials_path.exists() {
        let temp_path = home_dir.join(".helix-credentials-temp");
        fs::rename(&credentials_path, &temp_path).map_err(|e| {
            CliError::new("Failed to backup credentials").with_caused_by(e.to_string())
        })?;
        Some(temp_path)
    } else {
        None
    };

    let temp_repo = if repo_path.exists() {
        let temp_path = home_dir.join(".helix-repo-temp");
        fs::rename(&repo_path, &temp_path)
            .map_err(|e| CliError::new("Failed to backup repo").with_caused_by(e.to_string()))?;
        Some(temp_path)
    } else {
        None
    };

    // Remove the entire .helix directory
    fs::remove_dir_all(&v1_helix_dir).map_err(|e| {
        CliError::new("Failed to remove ~/.helix directory").with_caused_by(e.to_string())
    })?;

    // Recreate .helix directory
    fs::create_dir_all(&v1_helix_dir).map_err(|e| {
        CliError::new("Failed to recreate ~/.helix directory").with_caused_by(e.to_string())
    })?;

    // Restore credentials and repo
    if let Some(temp_creds) = temp_credentials {
        fs::rename(&temp_creds, &credentials_path).map_err(|e| {
            CliError::new("Failed to restore credentials").with_caused_by(e.to_string())
        })?;
        output::info("Preserved ~/.helix/credentials");
    }

    if let Some(temp_repo) = temp_repo {
        fs::rename(&temp_repo, &repo_path)
            .map_err(|e| CliError::new("Failed to restore repo").with_caused_by(e.to_string()))?;
        output::info("Preserved ~/.helix/repo");
    }

    // Create .v2 marker file to indicate migration is complete
    fs::write(&v2_marker, "").map_err(|e| {
        CliError::new("Failed to create v2 marker file").with_caused_by(e.to_string())
    })?;

    output::success("Cleaned up ~/.helix directory, preserving credentials and repo");
    Ok(())
}

fn cleanup_dockerdev() -> Result<()> {
    output::info("Cleaning up Docker dev containers and images");

    // Stop and remove the container
    let container_name = "helix-dockerdev";

    // Try to stop the container (ignore errors if not running)
    let _ = std::process::Command::new("docker")
        .args(["stop", container_name])
        .output();

    // Try to remove the container (ignore errors if doesn't exist)
    let _ = std::process::Command::new("docker")
        .args(["rm", container_name])
        .output();

    // Try to remove any helix-related images
    let output = std::process::Command::new("docker")
        .args([
            "images",
            "--format",
            "{{.Repository}}:{{.Tag}}",
            "--filter",
            "reference=helix*",
        ])
        .output();

    if let Ok(output) = output {
        let images = String::from_utf8_lossy(&output.stdout);
        for image in images.lines().filter(|line| !line.is_empty()) {
            let _ = std::process::Command::new("docker")
                .args(["rmi", image])
                .output();
        }
    }

    // Try to remove helix volumes
    let output = std::process::Command::new("docker")
        .args([
            "volume",
            "ls",
            "--format",
            "{{.Name}}",
            "--filter",
            "name=helix",
        ])
        .output();

    if let Ok(output) = output {
        let volumes = String::from_utf8_lossy(&output.stdout);
        for volume in volumes.lines().filter(|line| !line.is_empty()) {
            let _ = std::process::Command::new("docker")
                .args(["volume", "rm", volume])
                .output();
        }
    }

    output::info("Cleaned up Docker dev environment");
    Ok(())
}

fn check_cloud_credentials() -> bool {
    let home = match dirs::home_dir() {
        Some(dir) => dir,
        None => return false,
    };

    let credentials_path = home.join(".helix").join("credentials");
    credentials_path.exists()
}
