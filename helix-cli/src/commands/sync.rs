use crate::commands::auth::require_auth;
use crate::commands::integrations::helix::CLOUD_AUTHORITY;
use crate::config::{InstanceInfo, WorkspaceConfig};
use crate::output::{Operation, Step};
use crate::project::ProjectContext;
use crate::prompts;
use crate::utils::print_warning;
use eyre::{Result, eyre};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize)]
struct SyncResponse {
    helix_toml: Option<String>,
    hx_files: HashMap<String, String>,
}

#[derive(Deserialize)]
pub struct CliWorkspace {
    pub id: String,
    pub name: String,
    #[allow(dead_code)]
    pub url_slug: String,
}

#[derive(Deserialize)]
pub struct CliCluster {
    pub cluster_id: String,
    pub cluster_name: String,
    pub project_name: String,
}

pub async fn run(instance_name: Option<String>) -> Result<()> {
    // Try to load project context
    let project = ProjectContext::find_and_load(None).ok();

    // Resolve instance name
    let instance_name = match instance_name {
        Some(name) => name,
        None if prompts::is_interactive() => {
            if let Some(ref project) = project {
                let instances = project.config.list_instances_with_types();
                if !instances.is_empty() {
                    prompts::intro(
                        "helix sync",
                        Some("Sync .hx source files and config from a deployed instance."),
                    )?;
                    prompts::select_instance(&instances)?
                } else {
                    // No instances in helix.toml, fall through to workspace flow
                    return run_workspace_sync_flow().await;
                }
            } else {
                // No helix.toml found, use workspace flow
                return run_workspace_sync_flow().await;
            }
        }
        None => {
            return Err(eyre!(
                "No instance specified. Run 'helix sync <instance>' or run interactively in a project directory."
            ));
        }
    };

    let project = project
        .ok_or_else(|| eyre!("No helix.toml found. Run 'helix init' to create a project first."))?;

    // Get instance config
    let instance_config = project.config.get_instance(&instance_name)?;

    if instance_config.is_local() {
        pull_from_local_instance(&project, &instance_name).await
    } else {
        pull_from_cloud_instance(&project, &instance_name, instance_config).await
    }
}

/// Interactive flow when no project/instance is available: prompt workspace → cluster selection.
async fn run_workspace_sync_flow() -> Result<()> {
    prompts::intro(
        "helix sync",
        Some("No helix.toml found. Select a workspace and cluster to sync from."),
    )?;

    let credentials = require_auth().await?;
    let client = reqwest::Client::new();
    let base_url = format!("https://{}", *CLOUD_AUTHORITY);

    // Load or prompt for workspace
    let mut workspace_config = WorkspaceConfig::load()?;

    let workspace_id = if workspace_config.has_workspace_id() {
        workspace_config.workspace_id.clone().unwrap()
    } else {
        // Fetch workspaces
        let workspaces: Vec<CliWorkspace> = client
            .get(format!("{}/api/cli/workspaces", base_url))
            .header("x-api-key", &credentials.helix_admin_key)
            .send()
            .await
            .map_err(|e| eyre!("Failed to fetch workspaces: {}", e))?
            .error_for_status()
            .map_err(|e| eyre!("Failed to fetch workspaces: {}", e))?
            .json()
            .await
            .map_err(|e| eyre!("Failed to parse workspaces response: {}", e))?;

        if workspaces.is_empty() {
            return Err(eyre!(
                "No workspaces found. Create a workspace at https://app.helixdb.cloud first."
            ));
        }

        let selected = prompts::select_workspace(&workspaces)?;

        // Save workspace selection
        workspace_config.workspace_id = Some(selected.clone());
        workspace_config.save()?;

        selected
    };

    // Fetch clusters for workspace
    let clusters: Vec<CliCluster> = client
        .get(format!(
            "{}/api/cli/workspaces/{}/clusters",
            base_url, workspace_id
        ))
        .header("x-api-key", &credentials.helix_admin_key)
        .send()
        .await
        .map_err(|e| eyre!("Failed to fetch clusters: {}", e))?
        .error_for_status()
        .map_err(|e| eyre!("Failed to fetch clusters: {}", e))?
        .json()
        .await
        .map_err(|e| eyre!("Failed to parse clusters response: {}", e))?;

    if clusters.is_empty() {
        return Err(eyre!(
            "No clusters found in this workspace. Deploy a cluster first with 'helix push'."
        ));
    }

    let cluster_id = prompts::select_cluster(&clusters)?;

    // Sync from the selected cluster (no project context needed)
    sync_from_cluster_id(&credentials.helix_admin_key, &cluster_id).await
}

/// Sync directly from a cluster ID without a project context.
async fn sync_from_cluster_id(api_key: &str, cluster_id: &str) -> Result<()> {
    let op = Operation::new("Syncing", cluster_id);

    let client = reqwest::Client::new();
    let sync_url = format!(
        "https://{}/api/clusters/{}/sync",
        *CLOUD_AUTHORITY, cluster_id
    );

    let mut sync_step = Step::with_messages("Fetching source files", "Source files fetched");
    sync_step.start();

    let response = match client
        .get(&sync_url)
        .header("x-api-key", api_key)
        .header("x-cluster-id", cluster_id)
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(e) => {
            sync_step.fail();
            op.failure();
            return Err(eyre!("Failed to connect to Helix Cloud: {}", e));
        }
    };

    match response.status() {
        reqwest::StatusCode::OK => {}
        reqwest::StatusCode::NOT_FOUND => {
            sync_step.fail();
            op.failure();
            return Err(eyre!(
                "No source files found for cluster '{}'. Make sure you have deployed at least once with `helix push`.",
                cluster_id
            ));
        }
        reqwest::StatusCode::UNAUTHORIZED => {
            sync_step.fail();
            op.failure();
            return Err(eyre!(
                "Authentication failed. Run 'helix auth login' to re-authenticate."
            ));
        }
        reqwest::StatusCode::FORBIDDEN => {
            sync_step.fail();
            op.failure();
            return Err(eyre!(
                "Access denied to cluster '{}'. Make sure you have permission to access this cluster.",
                cluster_id
            ));
        }
        status => {
            let error_text = response.text().await.unwrap_or_default();
            sync_step.fail();
            op.failure();
            return Err(eyre!("Sync failed ({}): {}", status, error_text));
        }
    }

    let sync_response: SyncResponse = match response.json().await {
        Ok(resp) => resp,
        Err(e) => {
            sync_step.fail();
            op.failure();
            return Err(eyre!("Failed to parse sync response: {}", e));
        }
    };

    sync_step.done();

    // Write files to current directory
    let cwd = std::env::current_dir()?;
    let queries_dir = if let Some(ref remote_toml) = sync_response.helix_toml {
        if let Ok(remote_config) = toml::from_str::<crate::config::HelixConfig>(remote_toml) {
            cwd.join(&remote_config.project.queries)
        } else {
            cwd.join("db")
        }
    } else {
        cwd.join("db")
    };

    if !queries_dir.exists() {
        std::fs::create_dir_all(&queries_dir)?;
    }

    let mut write_step = Step::with_messages("Writing source files", "Source files written");
    write_step.start();

    let mut files_written = 0;
    for (filename, content) in &sync_response.hx_files {
        let file_path = queries_dir.join(filename);
        if let Some(parent) = file_path.parent()
            && !parent.exists()
        {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&file_path, content)
            .map_err(|e| eyre!("Failed to write {}: {}", filename, e))?;
        files_written += 1;
        Step::verbose_substep(&format!("  Wrote {}", filename));
    }

    if let Some(ref remote_toml) = sync_response.helix_toml {
        let helix_toml_path = cwd.join("helix.toml");
        std::fs::write(&helix_toml_path, remote_toml)
            .map_err(|e| eyre!("Failed to write helix.toml: {}", e))?;
        files_written += 1;
        Step::verbose_substep("  Wrote helix.toml");
    }

    write_step.done_with_info(&format!("{} files", files_written));
    op.success();

    println!();
    crate::output::info(&format!(
        "Synced {} files from cluster '{}'",
        files_written, cluster_id
    ));
    crate::output::info(&format!("Files saved to: {}", queries_dir.display()));

    Ok(())
}

async fn pull_from_local_instance(project: &ProjectContext, instance_name: &str) -> Result<()> {
    let op = Operation::new("Syncing", instance_name);

    // For local instances, we'd need to extract the .hx files from the running container
    // or from the compiled workspace

    let workspace = project.instance_workspace(instance_name);
    let container_dir = workspace.join("helix-container");

    if !container_dir.exists() {
        op.failure();
        return Err(eyre!(
            "Instance '{instance_name}' has not been built yet. Run 'helix build {instance_name}' first."
        ));
    }

    // TODO: Implement extraction of .hx files from compiled container
    // This would reverse-engineer the queries from the compiled Rust code
    // or maintain source files alongside compiled versions

    print_warning("Local instance query extraction not yet implemented");
    println!("  Local instances compile queries into Rust code.");
    println!("  Query extraction from compiled code is not currently supported.");

    Ok(())
}

async fn pull_from_cloud_instance(
    project: &ProjectContext,
    instance_name: &str,
    instance_config: InstanceInfo<'_>,
) -> Result<()> {
    let op = Operation::new("Syncing", instance_name);

    // Verify this is a Helix Cloud instance
    let cluster_id = match &instance_config {
        InstanceInfo::Helix(config) => &config.cluster_id,
        InstanceInfo::FlyIo(_) => {
            op.failure();
            return Err(eyre!(
                "Sync is only supported for Helix Cloud instances, not Fly.io deployments"
            ));
        }
        InstanceInfo::Ecr(_) => {
            op.failure();
            return Err(eyre!(
                "Sync is only supported for Helix Cloud instances, not ECR deployments"
            ));
        }
        InstanceInfo::Local(_) => {
            op.failure();
            return Err(eyre!("Sync is only supported for cloud instances"));
        }
    };

    // Check auth
    let credentials = require_auth().await?;

    Step::verbose_substep(&format!("Downloading from cluster: {cluster_id}"));

    // Make API request to sync endpoint
    let client = reqwest::Client::new();
    let sync_url = format!(
        "https://{}/api/clusters/{}/sync",
        *CLOUD_AUTHORITY, cluster_id
    );

    let mut sync_step = Step::with_messages("Fetching source files", "Source files fetched");
    sync_step.start();

    let response = match client
        .get(&sync_url)
        .header("x-api-key", &credentials.helix_admin_key)
        .header("x-cluster-id", cluster_id)
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(e) => {
            sync_step.fail();
            op.failure();
            return Err(eyre!("Failed to connect to Helix Cloud: {}", e));
        }
    };

    // Handle response status
    match response.status() {
        reqwest::StatusCode::OK => {}
        reqwest::StatusCode::NOT_FOUND => {
            sync_step.fail();
            op.failure();
            return Err(eyre!(
                "No source files found for cluster '{}'. Make sure you have deployed at least once with `helix push`.",
                cluster_id
            ));
        }
        reqwest::StatusCode::UNAUTHORIZED => {
            sync_step.fail();
            op.failure();
            return Err(eyre!(
                "Authentication failed. Run 'helix auth login' to re-authenticate."
            ));
        }
        reqwest::StatusCode::FORBIDDEN => {
            sync_step.fail();
            op.failure();
            return Err(eyre!(
                "Access denied to cluster '{}'. Make sure you have permission to access this cluster.",
                cluster_id
            ));
        }
        status => {
            let error_text = response.text().await.unwrap_or_default();
            sync_step.fail();
            op.failure();
            return Err(eyre!("Sync failed ({}): {}", status, error_text));
        }
    }

    // Parse response
    let sync_response: SyncResponse = match response.json().await {
        Ok(resp) => resp,
        Err(e) => {
            sync_step.fail();
            op.failure();
            return Err(eyre!("Failed to parse sync response: {}", e));
        }
    };

    sync_step.done();

    // Get the queries directory from project config
    let queries_dir = project.root.join(&project.config.project.queries);

    // Create queries directory if it doesn't exist
    if !queries_dir.exists() {
        std::fs::create_dir_all(&queries_dir)?;
    }

    // Collect files that differ from local
    let mut differing_files: Vec<String> = Vec::new();
    for (filename, content) in &sync_response.hx_files {
        let file_path = queries_dir.join(filename);
        if file_path.exists()
            && let Ok(local_content) = std::fs::read_to_string(&file_path)
            && local_content != *content
        {
            differing_files.push(filename.clone());
        }
    }

    // Check if helix.toml would change
    let helix_toml_path = project.root.join("helix.toml");
    let helix_toml_differs = if let Some(ref remote_toml) = sync_response.helix_toml {
        if helix_toml_path.exists() {
            // Parse remote and see if merge would change anything
            if let (Ok(remote_config), Ok(local_content)) = (
                toml::from_str::<crate::config::HelixConfig>(remote_toml),
                std::fs::read_to_string(&helix_toml_path),
            ) {
                if let Ok(local_config) =
                    toml::from_str::<crate::config::HelixConfig>(&local_content)
                {
                    // Check if project section or the cloud instance entry differs
                    let project_differs = toml::to_string(&remote_config.project).ok()
                        != toml::to_string(&local_config.project).ok();
                    let cloud_differs = remote_config.cloud.iter().any(|(k, v)| {
                        local_config
                            .cloud
                            .get(k)
                            .map(|lv| toml::to_string(lv).ok() != toml::to_string(v).ok())
                            .unwrap_or(true)
                    });
                    project_differs || cloud_differs
                } else {
                    true
                }
            } else {
                false
            }
        } else {
            true // new file
        }
    } else {
        false
    };

    if helix_toml_differs {
        differing_files.push("helix.toml".to_string());
    }

    // Prompt for confirmation if files differ
    if !differing_files.is_empty() {
        println!();
        println!("The following files differ from remote:");
        for f in &differing_files {
            println!("  - {}", f);
        }
        println!();
        if !crate::prompts::confirm(&format!(
            "Overwrite {} files that differ from remote?",
            differing_files.len()
        ))? {
            op.failure();
            return Err(eyre!("Sync aborted by user"));
        }
    }

    // Write .hx files
    let mut write_step = Step::with_messages("Writing source files", "Source files written");
    write_step.start();

    let mut files_written = 0;
    for (filename, content) in &sync_response.hx_files {
        let file_path = queries_dir.join(filename);

        // Create parent directories if needed
        if let Some(parent) = file_path.parent()
            && !parent.exists()
        {
            std::fs::create_dir_all(parent)?;
        }

        std::fs::write(&file_path, content)
            .map_err(|e| eyre!("Failed to write {}: {}", filename, e))?;

        files_written += 1;
        Step::verbose_substep(&format!("  Wrote {}", filename));
    }

    // Merge helix.toml if remote provided one
    if let Some(ref remote_toml) = sync_response.helix_toml
        && let Ok(remote_config) = toml::from_str::<crate::config::HelixConfig>(remote_toml)
    {
        let mut merged = if helix_toml_path.exists() {
            let local_content = std::fs::read_to_string(&helix_toml_path)
                .map_err(|e| eyre!("Failed to read helix.toml: {}", e))?;
            toml::from_str::<crate::config::HelixConfig>(&local_content)
                .map_err(|e| eyre!("Failed to parse local helix.toml: {}", e))?
        } else {
            crate::config::HelixConfig {
                project: remote_config.project.clone(),
                local: HashMap::new(),
                cloud: HashMap::new(),
            }
        };

        // Update project section
        merged.project = remote_config.project;

        // Merge cloud instance entries (insert/update only those from remote)
        for (name, cloud_config) in remote_config.cloud {
            merged.cloud.insert(name, cloud_config);
        }

        let merged_toml = toml::to_string_pretty(&merged)
            .map_err(|e| eyre!("Failed to serialize merged helix.toml: {}", e))?;
        std::fs::write(&helix_toml_path, &merged_toml)
            .map_err(|e| eyre!("Failed to write helix.toml: {}", e))?;

        files_written += 1;
        Step::verbose_substep("  Wrote helix.toml (merged)");
    }

    write_step.done_with_info(&format!("{} files", files_written));

    op.success();

    // Print summary
    println!();
    crate::output::info(&format!(
        "Synced {} files from cluster '{}'",
        files_written, cluster_id
    ));
    crate::output::info(&format!("Files saved to: {}", queries_dir.display()));

    // List files that were synced
    if !sync_response.hx_files.is_empty() {
        println!();
        println!("Files synced:");
        for filename in sync_response.hx_files.keys() {
            println!("  - {}", filename);
        }
    }

    Ok(())
}
