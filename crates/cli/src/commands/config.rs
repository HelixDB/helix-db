use crate::commands::auth::require_auth;
use crate::config::WorkspaceConfig;
use crate::enterprise_cloud::{
    cloud_base_url, fetch_indexes_for_cluster, fetch_projects, fetch_workspaces,
    find_project_by_id, find_project_by_name, find_workspace_by_id, find_workspace_by_slug,
    list_clusters_for_context, resolve_enterprise_cluster, CliClusterIndexes, CliEnterpriseCluster,
    CliIndexSnapshot,
};
use crate::project::ProjectContext;
use crate::prompts;
use crate::{
    ClusterConfigAction, ConfigAction, ConfigOutputFormat, ProjectConfigAction,
    WorkspaceConfigAction,
};
use color_eyre::owo_colors::OwoColorize;
use eyre::{eyre, Result, WrapErr};
use serde::Serialize;

pub async fn run(action: Option<ConfigAction>) -> Result<()> {
    match action {
        Some(ConfigAction::Workspace { action }) => run_workspace(Some(action)).await,
        Some(ConfigAction::Project { action }) => run_project(Some(action)).await,
        Some(ConfigAction::Cluster { action }) => run_cluster(Some(action)).await,
        None if prompts::is_interactive() => interactive_config().await,
        None => Err(eyre!(
            "Specify a config command: 'helix workspace', 'helix project', or 'helix cluster'"
        )),
    }
}

pub async fn run_workspace(action: Option<WorkspaceConfigAction>) -> Result<()> {
    match action {
        Some(WorkspaceConfigAction::List { format }) => workspace_list(format).await,
        Some(WorkspaceConfigAction::Show { format }) => workspace_show(format).await,
        Some(WorkspaceConfigAction::Switch { workspace, id }) => {
            workspace_switch(&workspace, id).await
        }
        None if prompts::is_interactive() => workspace_select().await,
        None => Err(eyre!(
            "Specify a workspace command: 'helix workspace list', 'helix workspace show', or 'helix workspace switch <workspace>'"
        )),
    }
}

pub async fn run_project(action: Option<ProjectConfigAction>) -> Result<()> {
    match action {
        Some(ProjectConfigAction::List {
            workspace_id,
            format,
        }) => project_list(workspace_id, format).await,
        Some(ProjectConfigAction::Show { format }) => project_show(format).await,
        Some(ProjectConfigAction::Switch { project, id }) => project_switch(&project, id).await,
        None if prompts::is_interactive() => project_select().await,
        None => Err(eyre!(
            "Specify a project command: 'helix project list', 'helix project show', or 'helix project switch <project>'"
        )),
    }
}

pub async fn run_cluster(action: Option<ClusterConfigAction>) -> Result<()> {
    match action {
        Some(ClusterConfigAction::List {
            workspace_id,
            project_id,
            format,
        }) => cluster_list(workspace_id, project_id, format).await,
        Some(ClusterConfigAction::Indexes { cluster_id, format }) => {
            list_indexes_for_cluster(cluster_id, format).await
        }
        None if prompts::is_interactive() => cluster_select().await,
        None => Err(eyre!(
            "Specify a cluster command: 'helix cluster list' or 'helix cluster indexes'"
        )),
    }
}

async fn interactive_config() -> Result<()> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum ConfigTarget {
        Workspace,
        Project,
        Cluster,
    }

    let target = cliclack::select("What would you like to configure?")
        .item(
            ConfigTarget::Workspace,
            "Workspace",
            "Choose active Enterprise Cloud workspace",
        )
        .item(
            ConfigTarget::Project,
            "Project",
            "Link this project to Enterprise Cloud",
        )
        .item(
            ConfigTarget::Cluster,
            "Cluster",
            "Inspect Enterprise Cloud clusters",
        )
        .interact()?;

    match target {
        ConfigTarget::Workspace => workspace_select().await,
        ConfigTarget::Project => project_select().await,
        ConfigTarget::Cluster => cluster_select().await,
    }
}

fn print_json<T: Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

async fn workspace_list(format: ConfigOutputFormat) -> Result<()> {
    let credentials = require_auth().await?;
    let client = reqwest::Client::new();
    let workspaces =
        fetch_workspaces(&client, &cloud_base_url(), &credentials.helix_admin_key).await?;
    if format == ConfigOutputFormat::Json {
        return print_json(&workspaces);
    }
    println!("{}", "Workspaces".bold());
    for workspace in workspaces {
        println!("  {} ({})", workspace.name, workspace.url_slug);
    }
    Ok(())
}

async fn workspace_show(format: ConfigOutputFormat) -> Result<()> {
    let config = WorkspaceConfig::load()?;
    if format == ConfigOutputFormat::Json {
        return print_json(&config);
    }
    match config.workspace_id {
        Some(id) => println!("Selected workspace: {id}"),
        None => println!("No workspace selected"),
    }
    Ok(())
}

async fn workspace_switch(selector: &str, use_id: bool) -> Result<()> {
    let credentials = require_auth().await?;
    let client = reqwest::Client::new();
    let workspaces =
        fetch_workspaces(&client, &cloud_base_url(), &credentials.helix_admin_key).await?;
    let selected = if use_id {
        find_workspace_by_id(&workspaces, selector)
    } else {
        find_workspace_by_slug(&workspaces, selector)
    }
    .ok_or_else(|| eyre!("Workspace '{selector}' was not found"))?;

    let config = WorkspaceConfig {
        workspace_id: Some(selected.id.clone()),
    };
    config.save()?;
    crate::output::success(&format!("Selected workspace '{}'", selected.name));
    Ok(())
}

async fn workspace_select() -> Result<()> {
    let credentials = require_auth().await?;
    let client = reqwest::Client::new();
    let workspaces =
        fetch_workspaces(&client, &cloud_base_url(), &credentials.helix_admin_key).await?;
    let items: Vec<(String, String, String)> = workspaces
        .iter()
        .map(|workspace| {
            (
                workspace.id.clone(),
                workspace.name.clone(),
                workspace.url_slug.clone(),
            )
        })
        .collect();
    let selected_id = prompts::select_workspace(&items)?;
    let selected = workspaces
        .iter()
        .find(|workspace| workspace.id == selected_id)
        .ok_or_else(|| eyre!("Selected workspace was not found"))?;
    WorkspaceConfig {
        workspace_id: Some(selected.id.clone()),
    }
    .save()?;
    crate::output::success(&format!("Selected workspace '{}'", selected.name));
    Ok(())
}

async fn project_list(workspace_id: Option<String>, format: ConfigOutputFormat) -> Result<()> {
    let credentials = require_auth().await?;
    let client = reqwest::Client::new();
    let workspace_id = workspace_id
        .or_else(|| {
            WorkspaceConfig::load()
                .ok()
                .and_then(|config| config.workspace_id)
        })
        .ok_or_else(|| {
            eyre!("No workspace selected. Run 'helix config workspace switch <workspace>'.")
        })?;
    let projects = fetch_projects(
        &client,
        &cloud_base_url(),
        &credentials.helix_admin_key,
        &workspace_id,
    )
    .await?;
    if format == ConfigOutputFormat::Json {
        return print_json(&projects);
    }
    println!("{}", "Projects".bold());
    for project in projects {
        println!("  {} ({})", project.name, project.id);
    }
    Ok(())
}

async fn project_show(format: ConfigOutputFormat) -> Result<()> {
    let project = ProjectContext::find_and_load(None)?;
    if format == ConfigOutputFormat::Json {
        return print_json(&project.config.project);
    }
    println!("Project: {}", project.config.project.name);
    if let Some(id) = &project.config.project.id {
        println!("ID: {id}");
    }
    if let Some(workspace_id) = &project.config.project.workspace_id {
        println!("Workspace ID: {workspace_id}");
    }
    Ok(())
}

async fn project_switch(selector: &str, use_id: bool) -> Result<()> {
    let credentials = require_auth().await?;
    let client = reqwest::Client::new();
    let workspace_id = WorkspaceConfig::load()?.workspace_id.ok_or_else(|| {
        eyre!("No workspace selected. Run 'helix config workspace switch <workspace>'.")
    })?;
    let projects = fetch_projects(
        &client,
        &cloud_base_url(),
        &credentials.helix_admin_key,
        &workspace_id,
    )
    .await?;
    let selected = if use_id {
        find_project_by_id(&projects, selector)
    } else {
        find_project_by_name(&projects, selector)
    }
    .ok_or_else(|| eyre!("Project '{selector}' was not found"))?;

    let mut project = ProjectContext::find_and_load(None)?;
    project.config.project.id = Some(selected.id.clone());
    project.config.project.workspace_id = Some(workspace_id);
    project.config.project.name = selected.name.clone();
    project
        .config
        .save_to_file(&project.root.join("helix.toml"))?;
    crate::output::success(&format!("Linked project '{}'", selected.name));
    Ok(())
}

async fn project_select() -> Result<()> {
    let credentials = require_auth().await?;
    let client = reqwest::Client::new();
    let workspace_id = WorkspaceConfig::load()?.workspace_id.ok_or_else(|| {
        eyre!(
            "No workspace selected. Run 'helix workspace' or 'helix workspace switch <workspace>'."
        )
    })?;
    let projects = fetch_projects(
        &client,
        &cloud_base_url(),
        &credentials.helix_admin_key,
        &workspace_id,
    )
    .await?;
    let items: Vec<(String, String)> = projects
        .iter()
        .map(|project| (project.id.clone(), project.name.clone()))
        .collect();
    let selected_id = prompts::select_project(&items)?;
    let selected = projects
        .iter()
        .find(|project| project.id == selected_id)
        .ok_or_else(|| eyre!("Selected project was not found"))?;

    let mut project = ProjectContext::find_and_load(None)?;
    project.config.project.id = Some(selected.id.clone());
    project.config.project.workspace_id = Some(workspace_id);
    project.config.project.name = selected.name.clone();
    project
        .config
        .save_to_file(&project.root.join("helix.toml"))?;
    crate::output::success(&format!("Linked project '{}'", selected.name));
    Ok(())
}

async fn cluster_list(
    workspace_id: Option<String>,
    project_id: Option<String>,
    format: ConfigOutputFormat,
) -> Result<()> {
    let credentials = require_auth().await?;
    let client = reqwest::Client::new();
    let workspace_id = workspace_id.or_else(|| {
        WorkspaceConfig::load()
            .ok()
            .and_then(|config| config.workspace_id)
    });
    let clusters = list_clusters_for_context(
        &client,
        &cloud_base_url(),
        &credentials.helix_admin_key,
        project_id.as_deref(),
        workspace_id.as_deref(),
    )
    .await?;

    if format == ConfigOutputFormat::Json {
        return print_json(&clusters);
    }
    print_enterprise_clusters(&clusters);
    Ok(())
}

async fn list_indexes_for_cluster(
    cluster_id: Option<String>,
    format: ConfigOutputFormat,
) -> Result<()> {
    let cluster_id = resolve_cluster_id_for_indexes(cluster_id)?;
    let credentials = require_auth().await?;
    let client = reqwest::Client::new();
    let (indexes, raw) = fetch_indexes_for_cluster(
        &client,
        &cloud_base_url(),
        &credentials.helix_admin_key,
        cluster_id.as_str(),
    )
    .await?;

    if format == ConfigOutputFormat::Json {
        // Print the raw API body so JSON output always mirrors the API exactly.
        return print_json(&raw);
    }

    print_cluster_indexes(&cluster_id, &indexes, &raw);
    Ok(())
}

fn resolve_cluster_id_for_indexes(cluster_id: Option<String>) -> Result<String> {
    if let Some(cluster_id) = cluster_id {
        let cluster_id = cluster_id.trim();
        if !cluster_id.is_empty() {
            return Ok(cluster_id.to_string());
        }
    }

    let project = ProjectContext::find_and_load(None).wrap_err(
        "Provide --cluster-id, or run inside a Helix project with an Enterprise instance.",
    )?;

    let mut enterprise_instances = project
        .config
        .enterprise
        .keys()
        .map(|name| (name.clone(), "Enterprise".to_string()))
        .collect::<Vec<_>>();
    enterprise_instances.sort_by(|a, b| a.0.cmp(&b.0));

    let instance_name = match enterprise_instances.len() {
        0 => return Err(eyre!("No Enterprise instances found in helix.toml")),
        1 => enterprise_instances[0].0.clone(),
        _ if prompts::is_interactive() => prompts::select_instance(
            &enterprise_instances,
            "List indexes for which Enterprise instance?",
        )?,
        _ => {
            let available = enterprise_instances
                .into_iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>()
                .join(", ");
            return Err(eyre!(
                "No Enterprise instance specified. Available Enterprise instances: {available}. Pass --cluster-id to select one."
            ));
        }
    };

    project
        .config
        .enterprise
        .get(&instance_name)
        .map(|config| config.cluster_id.clone())
        .ok_or_else(|| eyre!("Enterprise instance '{instance_name}' was not found"))
}

fn print_cluster_indexes(cluster_id: &str, indexes: &CliClusterIndexes, raw: &serde_json::Value) {
    // If we couldn't recognize any backend in the response, fall back to the raw
    // JSON so the user always sees what the API actually returned.
    let Some(canonical) = indexes.canonical_backend() else {
        println!("{}", "Cluster indexes".bold());
        println!("  Cluster: {cluster_id}");
        println!(
            "  {}",
            "Unrecognized response shape; raw API response:".yellow()
        );
        if let Ok(pretty) = serde_json::to_string_pretty(raw) {
            println!("{pretty}");
        }
        return;
    };

    let mode = indexes.mode.as_deref().unwrap_or("unknown");
    let backend_count = indexes.readable_backends.len().max(indexes.backends.len());
    let writer = if canonical.pod.is_empty() {
        "unknown"
    } else {
        canonical.pod.as_str()
    };

    println!(
        "{} (mode: {mode}, {backend_count} backend(s), writer {writer})",
        "Cluster indexes".bold()
    );
    println!("  Cluster: {cluster_id}");

    let snap = &canonical.snapshot;
    print_snapshot_counts("Node", snap, true);
    print_snapshot_counts("Edge", snap, false);
    println!(
        "  {}",
        "Run with --format json for the full per-index listing.".dimmed()
    );

    // Surface any errors reported by the cluster.
    if !indexes.errors.is_empty() {
        println!("  {}", "Errors:".red());
        for err in &indexes.errors {
            println!("    {err}");
        }
    }

    // Warn if any readable backend diverges from the writer's index counts
    // (a replication-lag signal). Counts are sufficient; no full diff.
    let writer_sig = snapshot_signature(snap);
    let diverging: Vec<&str> = indexes
        .backends
        .iter()
        .filter(|b| b.pod != canonical.pod && snapshot_signature(&b.snapshot) != writer_sig)
        .map(|b| b.pod.as_str())
        .collect();
    if !diverging.is_empty() {
        println!(
            "  {} backend(s) diverge from the writer (replication lag?): {}",
            "Warning:".yellow(),
            diverging.join(", ")
        );
    }
}

/// Prints a one-line per-category count summary for the node or edge half of a
/// snapshot.
fn print_snapshot_counts(label: &str, snap: &CliIndexSnapshot, node: bool) {
    let (label_index, equality, range, range_desc, text, vector) = if node {
        (
            snap.node_label_index,
            snap.node_equality_indexes.len(),
            snap.node_range_indexes.len(),
            snap.node_range_desc_indexes.len(),
            snap.node_text_indexes.len(),
            snap.node_vector_indexes.len(),
        )
    } else {
        (
            snap.edge_label_index,
            snap.edge_equality_indexes.len(),
            snap.edge_range_indexes.len(),
            snap.edge_range_desc_indexes.len(),
            snap.edge_text_indexes.len(),
            snap.edge_vector_indexes.len(),
        )
    };
    let label_index = if label_index { "enabled" } else { "disabled" };
    println!(
        "  {label}:  label index {label_index} | equality {equality} | text {text} | vector {vector} | range {range} | range_desc {range_desc}"
    );
}

/// A cheap comparable signature of a snapshot's per-category counts, used to
/// detect backend divergence without a full diff. Counts (not contents) are
/// compared deliberately: backends return the same indexes in differing orders,
/// so a content/Vec comparison would report spurious divergence.
fn snapshot_signature(snap: &CliIndexSnapshot) -> Vec<usize> {
    vec![
        snap.node_label_index as usize,
        snap.edge_label_index as usize,
        snap.node_equality_indexes.len(),
        snap.node_range_indexes.len(),
        snap.node_range_desc_indexes.len(),
        snap.node_text_indexes.len(),
        snap.node_vector_indexes.len(),
        snap.edge_equality_indexes.len(),
        snap.edge_range_indexes.len(),
        snap.edge_range_desc_indexes.len(),
        snap.edge_text_indexes.len(),
        snap.edge_vector_indexes.len(),
    ]
}

fn print_enterprise_clusters(clusters: &[CliEnterpriseCluster]) {
    println!("{}", "Enterprise clusters".bold());
    for cluster in clusters {
        println!("  {} ({})", cluster.name, cluster.cluster_id);
        if let Some(gateway_url) = &cluster.gateway_url {
            println!("    gateway: {gateway_url}");
        }
    }
}

async fn cluster_select() -> Result<()> {
    let credentials = require_auth().await?;
    let client = reqwest::Client::new();
    let project_context = ProjectContext::find_and_load(None).ok();
    let project_id = project_context
        .as_ref()
        .and_then(|project| project.config.project.id.clone());
    let workspace_id = if project_id.is_some() {
        None
    } else {
        Some(WorkspaceConfig::load()?.workspace_id.ok_or_else(|| {
            eyre!("No workspace selected. Run 'helix workspace' or 'helix workspace switch <workspace>'.")
        })?)
    };
    let clusters = list_clusters_for_context(
        &client,
        &cloud_base_url(),
        &credentials.helix_admin_key,
        project_id.as_deref(),
        workspace_id.as_deref(),
    )
    .await?;

    let items: Vec<(String, String, String)> = clusters
        .iter()
        .map(|cluster| {
            let hint = cluster
                .project_name
                .as_deref()
                .unwrap_or("Enterprise cluster")
                .to_string();
            (cluster.cluster_id.clone(), cluster.name.clone(), hint)
        })
        .collect();
    let selected_id = prompts::select_cluster(&items)?;
    let selected = clusters
        .iter()
        .find(|cluster| cluster.cluster_id == selected_id)
        .ok_or_else(|| eyre!("Selected Enterprise cluster was not found"))?;

    println!("{}", "Enterprise cluster".bold());
    println!("  Name: {}", selected.name);
    println!("  ID: {}", selected.cluster_id);
    if let Some(project_name) = &selected.project_name {
        println!("  Project: {project_name}");
    }
    if let Some(gateway_url) = &selected.gateway_url {
        println!("  Gateway: {gateway_url}");
    }
    Ok(())
}

/// An Enterprise cluster resolved into the fields needed to write an
/// `[enterprise.<name>]` block in helix.toml.
pub struct EnterpriseTarget {
    pub cluster_id: String,
    pub project_id: Option<String>,
    pub workspace_id: Option<String>,
    pub gateway_url: Option<String>,
}

/// Resolve the cluster (and its owning project/workspace + gateway URL) for
/// `helix init cloud` / `helix add cloud`.
///
/// When `cluster_id` is `None`, the user picks one from the cluster list (only in
/// an interactive terminal). When it is `Some`, the cloud API is queried to fill in
/// the owning project/workspace. In both cases an explicit `gateway_url_override`
/// wins over the cluster's own gateway URL.
pub async fn resolve_enterprise_target(
    cluster_id: Option<String>,
    gateway_url_override: Option<String>,
    project_ctx: Option<String>,
    workspace_ctx: Option<String>,
) -> Result<EnterpriseTarget> {
    let credentials = require_auth().await?;
    let client = reqwest::Client::new();
    let base_url = cloud_base_url();
    let api_key = &credentials.helix_admin_key;

    match cluster_id {
        Some(cluster_id) => {
            let resolved = resolve_enterprise_cluster(
                &client,
                &base_url,
                api_key,
                &cluster_id,
                project_ctx.as_deref(),
                workspace_ctx.as_deref(),
            )
            .await?;
            Ok(EnterpriseTarget {
                cluster_id,
                project_id: Some(resolved.project_id),
                workspace_id: resolved.workspace_id,
                gateway_url: gateway_url_override.or(resolved.cluster.gateway_url),
            })
        }
        None => {
            if !prompts::is_interactive() {
                return Err(eyre!(
                    "Provide --cluster-id, or run interactively to pick a cluster from the list."
                ));
            }
            let workspace_id = workspace_ctx.or_else(|| {
                WorkspaceConfig::load()
                    .ok()
                    .and_then(|config| config.workspace_id)
            });
            let clusters = list_clusters_for_context(
                &client,
                &base_url,
                api_key,
                project_ctx.as_deref(),
                workspace_id.as_deref(),
            )
            .await?;
            let items: Vec<(String, String, String)> = clusters
                .iter()
                .map(|cluster| {
                    let hint = cluster
                        .project_name
                        .as_deref()
                        .unwrap_or("Enterprise cluster")
                        .to_string();
                    (cluster.cluster_id.clone(), cluster.name.clone(), hint)
                })
                .collect();
            let selected_id = prompts::select_cluster(&items)?;
            let cluster = clusters
                .into_iter()
                .find(|cluster| cluster.cluster_id == selected_id)
                .ok_or_else(|| eyre!("Selected Enterprise cluster was not found"))?;
            Ok(EnterpriseTarget {
                cluster_id: cluster.cluster_id,
                project_id: cluster.project_id.or(project_ctx),
                workspace_id,
                gateway_url: gateway_url_override.or(cluster.gateway_url),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enterprise_cloud::CliClusterBackend;
    use serde_json::json;

    #[test]
    fn explicit_cluster_id_is_trimmed() {
        assert_eq!(
            resolve_cluster_id_for_indexes(Some(" cluster-1 ".into())).unwrap(),
            "cluster-1"
        );
    }

    #[test]
    fn cluster_index_rendering_covers_unknown_canonical_and_divergent_shapes() {
        print_cluster_indexes(
            "cluster-1",
            &CliClusterIndexes::default(),
            &json!({"new":true}),
        );

        let writer_snapshot = CliIndexSnapshot {
            node_label_index: true,
            edge_label_index: true,
            node_equality_indexes: vec![("User".into(), "email".into())],
            node_range_indexes: vec![("User".into(), "age".into())],
            node_range_desc_indexes: vec![("User".into(), "score".into())],
            node_text_indexes: vec![("User".into(), "bio".into())],
            node_vector_indexes: vec![("User".into(), "embedding".into())],
            edge_equality_indexes: vec![("Knows".into(), "kind".into())],
            edge_range_indexes: vec![("Knows".into(), "since".into())],
            edge_range_desc_indexes: vec![("Knows".into(), "rank".into())],
            edge_text_indexes: vec![("Knows".into(), "note".into())],
            edge_vector_indexes: vec![("Knows".into(), "embedding".into())],
            ..CliIndexSnapshot::default()
        };
        let indexes = CliClusterIndexes {
            mode: Some("standard".into()),
            readable_backends: vec!["writer-0".into(), "reader-0".into()],
            backends: vec![
                CliClusterBackend {
                    pod: "writer-0".into(),
                    snapshot: writer_snapshot.clone(),
                },
                CliClusterBackend {
                    pod: "reader-0".into(),
                    snapshot: CliIndexSnapshot::default(),
                },
            ],
            writer_backend: Some(json!({"pod":"writer-0"})),
            errors: vec![json!({"error":"lagging"})],
            ..CliClusterIndexes::default()
        };
        assert_ne!(
            snapshot_signature(&writer_snapshot),
            snapshot_signature(&CliIndexSnapshot::default())
        );
        print_cluster_indexes("cluster-1", &indexes, &json!({}));
    }

    #[test]
    fn enterprise_cluster_human_output_handles_optional_fields() {
        let clusters: Vec<CliEnterpriseCluster> = serde_json::from_value(json!([
            {"cluster_id":"cluster-1","name":"Primary","gateway_url":"https://gateway"},
            {"cluster_id":"cluster-2","name":"Secondary"}
        ]))
        .unwrap();
        print_enterprise_clusters(&clusters);
    }
}
