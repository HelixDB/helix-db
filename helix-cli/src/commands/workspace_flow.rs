use crate::commands::auth::Credentials;
use crate::commands::integrations::helix::cloud_base_url;
use crate::config::{AvailabilityMode, BuildMode, WorkspaceConfig};
use crate::prompts;
use eyre::{Result, eyre};
use serde::Deserialize;

// ============================================================================
// Result types
// ============================================================================

pub struct StandardClusterResult {
    pub cluster_id: String,
    pub instance_name: String,
    pub build_mode: BuildMode,
}

pub struct EnterpriseClusterResult {
    pub cluster_id: String,
    pub instance_name: String,
    pub availability_mode: AvailabilityMode,
    pub gateway_node_type: String,
    pub db_node_type: String,
    pub min_instances: u64,
    pub max_instances: u64,
}

pub enum ClusterResult {
    Standard(StandardClusterResult),
    Enterprise(EnterpriseClusterResult),
}

pub struct WorkspaceProjectClusterFlowResult {
    pub cluster: ClusterResult,
    pub resolved_project_name: String,
    pub resolved_project_id: String,
}

struct ResolvedProject {
    id: String,
    name: String,
}

// ============================================================================
// API response types
// ============================================================================

#[derive(Deserialize)]
struct CliWorkspace {
    id: String,
    name: String,
    #[allow(dead_code)]
    url_slug: String,
    #[serde(default = "default_workspace_type")]
    workspace_type: String,
}

fn default_workspace_type() -> String {
    "organization".to_string()
}

struct SelectedWorkspace {
    id: String,
    workspace_type: String,
}

#[derive(Deserialize)]
struct CliBillingResponse {
    has_billing: bool,
    #[allow(dead_code)]
    workspace_type: String,
    #[allow(dead_code)]
    plan: String,
}

#[derive(Deserialize)]
struct CliProject {
    id: String,
    name: String,
}

#[derive(Deserialize)]
struct CreateProjectResponse {
    id: String,
    #[allow(dead_code)]
    name: String,
}

#[derive(Deserialize)]
struct CreateClusterResponse {
    cluster_id: String,
}

// ============================================================================
// Main flow
// ============================================================================

/// Run the workspace → project → cluster selection/creation flow.
/// Returns a ClusterResult describing the created cluster.
pub async fn run_workspace_project_cluster_flow(
    project_name: &str,
    project_id_hint: Option<&str>,
    credentials: &Credentials,
    preferred_cluster_name: Option<&str>,
) -> Result<WorkspaceProjectClusterFlowResult> {
    let client = reqwest::Client::new();
    let base_url = cloud_base_url();

    // Step 1: Workspace selection
    let workspace = select_or_load_workspace(&client, &base_url, credentials).await?;

    // Step 2: Billing check
    check_billing(&client, &base_url, credentials, &workspace.id).await?;

    // Step 3: Project matching
    let resolved_project = match_or_create_project(
        &client,
        &base_url,
        credentials,
        &workspace.id,
        project_name,
        project_id_hint,
    )
    .await?;

    // Step 4: Cluster type selection
    let cluster_type = if workspace.workspace_type == "enterprise" {
        prompts::select_cluster_type()?
    } else {
        crate::output::info("Selected workspace is not enterprise; creating a standard cluster.");
        "standard"
    };

    // Step 5/6: Configure and create cluster
    match cluster_type {
        "enterprise" => Ok(WorkspaceProjectClusterFlowResult {
            cluster: create_enterprise_cluster_flow(
                &client,
                &base_url,
                credentials,
                &resolved_project.id,
                preferred_cluster_name,
            )
            .await?,
            resolved_project_name: resolved_project.name,
            resolved_project_id: resolved_project.id,
        }),
        _ => Ok(WorkspaceProjectClusterFlowResult {
            cluster: create_standard_cluster_flow(
                &client,
                &base_url,
                credentials,
                &resolved_project.id,
                preferred_cluster_name,
            )
            .await?,
            resolved_project_name: resolved_project.name,
            resolved_project_id: resolved_project.id,
        }),
    }
}

async fn select_or_load_workspace(
    client: &reqwest::Client,
    base_url: &str,
    credentials: &Credentials,
) -> Result<SelectedWorkspace> {
    let mut workspace_config = WorkspaceConfig::load()?;

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
        .map_err(|e| eyre!("Failed to parse workspaces: {}", e))?;

    if workspaces.is_empty() {
        return Err(eyre!(
            "No workspaces found. Go to the dashboard to create a workspace first."
        ));
    }

    if let Some(cached_workspace_id) = workspace_config.workspace_id.clone() {
        if let Some(workspace) = workspaces.iter().find(|w| w.id == cached_workspace_id) {
            return Ok(SelectedWorkspace {
                id: workspace.id.clone(),
                workspace_type: workspace.workspace_type.clone(),
            });
        }

        crate::output::warning(
            "Saved workspace selection is no longer available. Please select a workspace again.",
        );
        workspace_config.workspace_id = None;
        workspace_config.save()?;
    }

    // Convert for prompt
    let ws_for_prompt: Vec<crate::commands::sync::CliWorkspace> = workspaces
        .iter()
        .map(|w| crate::commands::sync::CliWorkspace {
            id: w.id.clone(),
            name: w.name.clone(),
            url_slug: w.url_slug.clone(),
        })
        .collect();

    let selected = prompts::select_workspace(&ws_for_prompt)?;

    // Save selection
    workspace_config.workspace_id = Some(selected.clone());
    workspace_config.save()?;

    let selected_workspace = workspaces
        .iter()
        .find(|w| w.id == selected)
        .ok_or_else(|| eyre!("Selected workspace was not found in response"))?;

    Ok(SelectedWorkspace {
        id: selected_workspace.id.clone(),
        workspace_type: selected_workspace.workspace_type.clone(),
    })
}

async fn check_billing(
    client: &reqwest::Client,
    base_url: &str,
    credentials: &Credentials,
    workspace_id: &str,
) -> Result<CliBillingResponse> {
    let billing: CliBillingResponse = client
        .get(format!(
            "{}/api/cli/workspaces/{}/billing",
            base_url, workspace_id
        ))
        .header("x-api-key", &credentials.helix_admin_key)
        .send()
        .await
        .map_err(|e| eyre!("Failed to check billing: {}", e))?
        .error_for_status()
        .map_err(|e| eyre!("Failed to check billing: {}", e))?
        .json()
        .await
        .map_err(|e| eyre!("Failed to parse billing response: {}", e))?;

    if !billing.has_billing {
        return Err(eyre!(
            "No active billing found for this workspace. Go to the dashboard to set up billing first."
        ));
    }

    Ok(billing)
}

async fn match_or_create_project(
    client: &reqwest::Client,
    base_url: &str,
    credentials: &Credentials,
    workspace_id: &str,
    project_name: &str,
    project_id_hint: Option<&str>,
) -> Result<ResolvedProject> {
    // Fetch projects
    let projects: Vec<CliProject> = client
        .get(format!(
            "{}/api/cli/workspaces/{}/projects",
            base_url, workspace_id
        ))
        .header("x-api-key", &credentials.helix_admin_key)
        .send()
        .await
        .map_err(|e| eyre!("Failed to fetch projects: {}", e))?
        .error_for_status()
        .map_err(|e| eyre!("Failed to fetch projects: {}", e))?
        .json()
        .await
        .map_err(|e| eyre!("Failed to parse projects: {}", e))?;

    // Try to find matching project by ID first (canonical identity)
    if let Some(expected_project_id) = project_id_hint
        && let Some(existing) = projects.iter().find(|p| p.id == expected_project_id)
    {
        crate::output::info(&format!(
            "Using project '{}' from your selected workspace.",
            existing.name
        ));

        return Ok(ResolvedProject {
            id: existing.id.clone(),
            name: existing.name.clone(),
        });
    }

    // Fallback to matching by display name
    if let Some(existing) = projects.iter().find(|p| p.name == project_name) {
        crate::output::info(&format!(
            "Using existing project '{}' from your selected workspace.",
            existing.name
        ));

        return Ok(ResolvedProject {
            id: existing.id.clone(),
            name: existing.name.clone(),
        });
    }

    match prompts::select_missing_project_choice(project_name)? {
        prompts::MissingProjectChoice::ChooseExisting => {
            if projects.is_empty() {
                return Err(eyre!(
                    "No projects exist in this workspace yet. Create one to continue."
                ));
            }

            let project_choices: Vec<(String, String)> = projects
                .iter()
                .map(|p| (p.id.clone(), p.name.clone()))
                .collect();
            let selected_project_id = prompts::select_project(&project_choices)?;
            let selected_project = projects
                .iter()
                .find(|p| p.id == selected_project_id)
                .ok_or_else(|| eyre!("Selected project was not found in response"))?;

            crate::output::info(&format!(
                "Using existing project '{}' from your selected workspace.",
                selected_project.name
            ));

            Ok(ResolvedProject {
                id: selected_project.id.clone(),
                name: selected_project.name.clone(),
            })
        }
        prompts::MissingProjectChoice::Create => {
            let chosen_name = prompts::input_project_name(project_name)?;
            let project_id =
                create_project(client, base_url, credentials, workspace_id, &chosen_name).await?;

            Ok(ResolvedProject {
                id: project_id,
                name: chosen_name,
            })
        }
    }
}

async fn create_project(
    client: &reqwest::Client,
    base_url: &str,
    credentials: &Credentials,
    workspace_id: &str,
    name: &str,
) -> Result<String> {
    let resp: CreateProjectResponse = client
        .post(format!(
            "{}/api/cli/workspaces/{}/projects",
            base_url, workspace_id
        ))
        .header("x-api-key", &credentials.helix_admin_key)
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({ "name": name }))
        .send()
        .await
        .map_err(|e| eyre!("Failed to create project: {}", e))?
        .error_for_status()
        .map_err(|e| eyre!("Failed to create project: {}", e))?
        .json()
        .await
        .map_err(|e| eyre!("Failed to parse create project response: {}", e))?;

    crate::output::success(&format!("Project '{}' created", name));
    Ok(resp.id)
}

async fn create_standard_cluster_flow(
    client: &reqwest::Client,
    base_url: &str,
    credentials: &Credentials,
    project_id: &str,
    preferred_cluster_name: Option<&str>,
) -> Result<ClusterResult> {
    let cluster_name = if let Some(name) = preferred_cluster_name {
        name.to_string()
    } else if prompts::is_interactive() {
        prompts::input_cluster_name("prod")?
    } else {
        "prod".to_string()
    };

    let build_mode = if prompts::is_interactive() {
        prompts::select_build_mode()?
    } else {
        BuildMode::Release
    };

    let build_mode_str = match build_mode {
        BuildMode::Dev => "dev",
        BuildMode::Release => "release",
        BuildMode::Debug => "dev",
    };

    let resp: CreateClusterResponse = client
        .post(format!(
            "{}/api/cli/projects/{}/clusters",
            base_url, project_id
        ))
        .header("x-api-key", &credentials.helix_admin_key)
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({
            "cluster_name": cluster_name,
            "build_mode": build_mode_str,
        }))
        .send()
        .await
        .map_err(|e| eyre!("Failed to create cluster: {}", e))?
        .error_for_status()
        .map_err(|e| eyre!("Failed to create cluster: {}", e))?
        .json()
        .await
        .map_err(|e| eyre!("Failed to parse create cluster response: {}", e))?;

    crate::output::success(&format!(
        "Cluster '{}' created (ID: {})",
        cluster_name, resp.cluster_id
    ));

    Ok(ClusterResult::Standard(StandardClusterResult {
        cluster_id: resp.cluster_id,
        instance_name: cluster_name,
        build_mode,
    }))
}

async fn create_enterprise_cluster_flow(
    client: &reqwest::Client,
    base_url: &str,
    credentials: &Credentials,
    project_id: &str,
    preferred_cluster_name: Option<&str>,
) -> Result<ClusterResult> {
    let cluster_name = if let Some(name) = preferred_cluster_name {
        name.to_string()
    } else if prompts::is_interactive() {
        prompts::input_cluster_name("prod")?
    } else {
        "prod".to_string()
    };

    let availability_mode = if prompts::is_interactive() {
        prompts::select_availability_mode()?
    } else {
        AvailabilityMode::Dev
    };
    let is_ha = availability_mode == AvailabilityMode::Ha;

    let gateway_node_type = if prompts::is_interactive() {
        prompts::select_gateway_node_type(is_ha)?
    } else if is_ha {
        "GW-40".to_string()
    } else {
        "GW-20".to_string()
    };

    let db_node_type = if prompts::is_interactive() {
        prompts::select_db_node_type(is_ha)?
    } else if is_ha {
        "HLX-160".to_string()
    } else {
        "HLX-40".to_string()
    };

    let (min_instances, max_instances) = if is_ha && prompts::is_interactive() {
        let min = prompts::input_min_instances()?;
        let max = prompts::input_max_instances(min)?;
        (min, max)
    } else if is_ha {
        (3, 3)
    } else {
        (1, 1)
    };

    // Show summary
    println!();
    crate::output::info(&format!("Cluster: {}", cluster_name));
    crate::output::info(&format!("Mode: {}", availability_mode));
    crate::output::info(&format!("Gateway: {}", gateway_node_type));
    crate::output::info(&format!("DB: {}", db_node_type));
    if is_ha {
        crate::output::info(&format!("Instances: {} - {}", min_instances, max_instances));
    }
    println!();

    if prompts::is_interactive() && !prompts::confirm("Create this enterprise cluster?")? {
        return Err(eyre!("Cluster creation cancelled"));
    }

    let availability_mode_str = match availability_mode {
        AvailabilityMode::Dev => "dev",
        AvailabilityMode::Ha => "ha",
    };

    let resp: CreateClusterResponse = client
        .post(format!(
            "{}/api/cli/projects/{}/enterprise-clusters",
            base_url, project_id
        ))
        .header("x-api-key", &credentials.helix_admin_key)
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({
            "cluster_name": cluster_name,
            "availability_mode": availability_mode_str,
            "gateway_node_type": gateway_node_type,
            "db_node_type": db_node_type,
            "min_instances": min_instances,
            "max_instances": max_instances,
        }))
        .send()
        .await
        .map_err(|e| eyre!("Failed to create enterprise cluster: {}", e))?
        .error_for_status()
        .map_err(|e| eyre!("Failed to create enterprise cluster: {}", e))?
        .json()
        .await
        .map_err(|e| eyre!("Failed to parse response: {}", e))?;

    crate::output::success(&format!(
        "Enterprise cluster '{}' created (ID: {})",
        cluster_name, resp.cluster_id
    ));

    Ok(ClusterResult::Enterprise(EnterpriseClusterResult {
        cluster_id: resp.cluster_id,
        instance_name: cluster_name,
        availability_mode,
        gateway_node_type,
        db_node_type,
        min_instances,
        max_instances,
    }))
}
