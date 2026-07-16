use crate::commands::auth::require_auth;
use crate::commands::enterprise_deploy::{
    compile_enterprise_queries, deploy_enterprise_by_cluster_id, enterprise_queries_dir,
    should_descend_enterprise_source_dir, should_include_enterprise_source_file,
};
use crate::config::{HelixConfig, InstanceInfo, DEFAULT_QUERY_AUTH_ENV, DEFAULT_QUERY_AUTH_HEADER};
use crate::enterprise_cloud::{
    cloud_base_url, resolve_enterprise_cluster, ResolvedEnterpriseCluster,
};
use crate::output::{Operation, Step};
use crate::project::ProjectContext;
use crate::prompts;
use color_eyre::owo_colors::OwoColorize;
use eyre::{eyre, Result};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const CLOCK_SKEW_WINDOW_MS: i64 = 5_000;

#[derive(Clone, Debug, Deserialize, Default)]
struct SyncFileMetadata {
    #[serde(default)]
    sha256: Option<String>,
    #[serde(default)]
    last_modified_ms: Option<i64>,
}

#[derive(Deserialize, Default)]
struct EnterpriseSyncResponse {
    #[serde(default)]
    source_files: HashMap<String, String>,
    #[serde(default)]
    file_metadata: HashMap<String, SyncFileMetadata>,
    #[serde(default)]
    helix_toml: Option<String>,
}

#[derive(Clone, Debug)]
struct ManifestEntry {
    sha256: String,
    last_modified_ms: Option<i64>,
    content: String,
}

#[derive(Clone, Debug, Default)]
struct ManifestDiff {
    local_only: Vec<String>,
    remote_only: Vec<String>,
    changed: Vec<String>,
}

impl ManifestDiff {
    fn all_files(&self) -> Vec<String> {
        let mut files = Vec::new();
        files.extend(self.local_only.iter().cloned());
        files.extend(self.remote_only.iter().cloned());
        files.extend(self.changed.iter().cloned());
        files.sort();
        files.dedup();
        files
    }

    fn is_empty(&self) -> bool {
        self.local_only.is_empty() && self.remote_only.is_empty() && self.changed.is_empty()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DivergenceAuthority {
    LocalNewer,
    RemoteNewer,
    TieOrUnknown,
}

#[derive(Clone, Debug)]
enum SnapshotComparison {
    BothEmpty,
    LocalOnly,
    RemoteOnly,
    InSync,
    Diverged {
        authority: DivergenceAuthority,
        diff: ManifestDiff,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SyncDirection {
    Pull,
    Push,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SyncActionPlan {
    to_create: Vec<String>,
    to_change: Vec<String>,
    to_delete: Vec<String>,
}

#[derive(Clone, Copy)]
enum TieResolutionAction {
    NoOp,
    Pull,
    Push,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SyncReconciliationOutcome {
    Unchanged,
    Pulled,
    Pushed,
}

pub async fn run(instance: Option<String>, assume_yes: bool, dry_run: bool) -> Result<()> {
    let project = ProjectContext::find_and_load(None)?;
    let instance_name = resolve_enterprise_instance_name(instance, &project)?;
    let credentials = require_auth().await?;

    sync_enterprise_instance(
        &project,
        &credentials.helix_admin_key,
        &instance_name,
        assume_yes,
        dry_run,
    )
    .await
}

fn resolve_enterprise_instance_name(
    instance_name: Option<String>,
    project: &ProjectContext,
) -> Result<String> {
    if let Some(instance_name) = instance_name {
        return Ok(instance_name);
    }

    let enterprise_instances: Vec<(String, String)> = project
        .config
        .enterprise
        .keys()
        .map(|name| (name.clone(), "Enterprise".to_string()))
        .collect();

    if prompts::is_interactive() {
        return prompts::select_instance(&enterprise_instances, "Sync which Enterprise instance?");
    }

    let available = enterprise_instances
        .into_iter()
        .map(|(name, _)| name)
        .collect::<Vec<_>>()
        .join(", ");
    if available.is_empty() {
        Err(eyre!("No Enterprise instances found in helix.toml"))
    } else {
        Err(eyre!(
            "No Enterprise instance specified. Available Enterprise instances: {available}"
        ))
    }
}

async fn sync_enterprise_instance(
    project: &ProjectContext,
    api_key: &str,
    instance_name: &str,
    assume_yes: bool,
    dry_run: bool,
) -> Result<()> {
    let config = match project.config.get_instance(instance_name)? {
        InstanceInfo::Enterprise(config) => config.clone(),
        InstanceInfo::Local(_) => {
            return Err(eyre!(
                "Sync is only supported for Enterprise instances; local v2 instances are managed with 'helix start' and query files."
            ));
        }
    };

    let client = reqwest::Client::new();
    let base_url = cloud_base_url();
    let remote_cluster = resolve_enterprise_cluster(
        &client,
        &base_url,
        api_key,
        &config.cluster_id,
        config.project_id.as_deref(),
        config.workspace_id.as_deref(),
    )
    .await
    .ok();

    let mut fetch_step = Step::with_messages(
        "Fetching enterprise cloud changes",
        "Enterprise cloud changes fetched",
    );
    fetch_step.start();
    let sync_response = match fetch_enterprise_sync_response_with_remote_empty_fallback(
        &client,
        api_key,
        &config.cluster_id,
    )
    .await
    {
        Ok(response) => {
            fetch_step.done();
            response
        }
        Err(error) => {
            fetch_step.fail();
            return Err(error);
        }
    };

    let remote_config = sync_response.helix_toml.as_deref().and_then(|remote_toml| {
        parse_and_sanitize_remote_config(remote_toml, "enterprise cluster sync")
    });
    let target_queries_relative = remote_config
        .as_ref()
        .map(|config| config.project.queries.clone())
        .unwrap_or_else(|| project.config.project.queries.clone());
    let cluster_name = remote_cluster
        .as_ref()
        .map(|remote| remote.cluster.name.as_str())
        .unwrap_or(instance_name);

    let sync_outcome = reconcile_enterprise_cluster_snapshot(
        project,
        &config.cluster_id,
        cluster_name,
        &target_queries_relative,
        &sync_response,
        assume_yes,
        dry_run,
    )
    .await?;

    // Dry runs only report what would change; never touch helix.toml or local files.
    if dry_run {
        return Ok(());
    }

    if let SyncReconciliationOutcome::Pulled = sync_outcome
        && project.config.project.queries != target_queries_relative
    {
        update_project_queries_path_in_helix_toml(&project.root, &target_queries_relative)?;
        Step::verbose_substep(&format!(
            "  Updated project queries path to {}",
            target_queries_relative.display()
        ));
    }

    if let Some(remote_cluster) = remote_cluster {
        refresh_enterprise_metadata(&project.root, instance_name, &remote_cluster)?;
        Step::verbose_substep("  Wrote helix.toml (canonical cloud metadata)");
    } else {
        crate::output::warning(&format!(
            "Synced Enterprise instance '{instance_name}', but cloud metadata could not be refreshed."
        ));
    }

    Ok(())
}

fn refresh_enterprise_metadata(
    project_root: &Path,
    instance_name: &str,
    remote: &ResolvedEnterpriseCluster,
) -> Result<()> {
    let helix_toml_path = project_root.join("helix.toml");
    let mut config = HelixConfig::from_file(&helix_toml_path)
        .map_err(|e| eyre!("Failed to load helix.toml for metadata refresh: {e}"))?;

    config.project.id = Some(remote.project_id.clone());
    config.project.name = remote.project_name.clone();
    if remote.workspace_id.is_some() {
        config.project.workspace_id = remote.workspace_id.clone();
    }

    let instance = config
        .enterprise
        .get_mut(instance_name)
        .ok_or_else(|| eyre!("Enterprise instance '{instance_name}' not found"))?;
    instance.cluster_id = remote.cluster.cluster_id.clone();
    instance.workspace_id = remote
        .workspace_id
        .clone()
        .or_else(|| instance.workspace_id.clone());
    instance.project_id = Some(remote.project_id.clone());
    instance.gateway_url = remote
        .cluster
        .gateway_url
        .clone()
        .or_else(|| instance.gateway_url.clone());
    instance.query_auth_header = remote
        .cluster
        .query_auth_header
        .clone()
        .unwrap_or_else(|| DEFAULT_QUERY_AUTH_HEADER.to_string());
    instance.query_auth_env = remote
        .cluster
        .query_auth_env
        .clone()
        .unwrap_or_else(|| DEFAULT_QUERY_AUTH_ENV.to_string());
    instance.availability_mode = remote
        .cluster
        .availability_mode
        .clone()
        .or_else(|| instance.availability_mode.clone());
    instance.gateway_node_type = remote
        .cluster
        .gateway_node_type
        .clone()
        .or_else(|| instance.gateway_node_type.clone());
    instance.db_node_type = remote
        .cluster
        .db_node_type
        .clone()
        .or_else(|| instance.db_node_type.clone());
    if let Some(min_instances) = remote.cluster.compatibility_min_instances() {
        instance.min_instances = min_instances;
    }
    if let Some(max_instances) = remote.cluster.compatibility_max_instances() {
        instance.max_instances = max_instances;
    }

    config
        .save_to_file(&helix_toml_path)
        .map_err(|e| eyre!("Failed to write helix.toml after metadata refresh: {e}"))?;
    Ok(())
}

async fn fetch_enterprise_sync_response_with_remote_empty_fallback(
    client: &reqwest::Client,
    api_key: &str,
    cluster_id: &str,
) -> Result<EnterpriseSyncResponse> {
    let sync_url = format!(
        "{}/api/cli/enterprise-clusters/{}/sync",
        cloud_base_url(),
        cluster_id
    );
    let response = client
        .get(&sync_url)
        .header("x-api-key", api_key)
        .send()
        .await
        .map_err(|e| eyre!("Failed to connect to Helix Cloud: {e}"))?;

    match response.status() {
        reqwest::StatusCode::OK => response
            .json::<EnterpriseSyncResponse>()
            .await
            .map_err(|e| eyre!("Failed to parse enterprise sync response: {e}")),
        reqwest::StatusCode::NOT_FOUND => {
            crate::output::warning(&format!(
                "No remote enterprise source files found for cluster '{cluster_id}'. Treating cloud changes as empty."
            ));
            Ok(EnterpriseSyncResponse::default())
        }
        reqwest::StatusCode::UNAUTHORIZED => Err(eyre!(
            "Authentication failed. Run 'helix auth login' to re-authenticate."
        )),
        reqwest::StatusCode::FORBIDDEN => Err(eyre!(
            "Access denied to enterprise cluster '{cluster_id}'. Make sure you have permission to access this cluster."
        )),
        status => {
            let error_text = response.text().await.unwrap_or_default();
            Err(eyre!("Enterprise sync failed ({status}): {error_text}"))
        }
    }
}

async fn reconcile_enterprise_cluster_snapshot(
    project: &ProjectContext,
    cluster_id: &str,
    cluster_name: &str,
    target_queries_relative: &Path,
    sync_response: &EnterpriseSyncResponse,
    assume_yes: bool,
    dry_run: bool,
) -> Result<SyncReconciliationOutcome> {
    let op = Operation::new("Syncing", cluster_name);
    let current_queries_dir = project.root.join(&project.config.project.queries);
    let target_queries_dir = project.root.join(target_queries_relative);
    let local_manifest = collect_local_enterprise_manifest(&current_queries_dir)?;
    let remote_manifest = build_remote_enterprise_manifest(sync_response);
    let comparison = compare_manifests(&local_manifest, &remote_manifest);

    if dry_run {
        print_dry_run_summary(&comparison, &local_manifest, &remote_manifest);
        op.success();
        return Ok(SyncReconciliationOutcome::Unchanged);
    }
    let apply_pull = || -> Result<()> {
        pull_remote_enterprise_snapshot_into_local(
            &current_queries_dir,
            &target_queries_dir,
            &local_manifest,
            &remote_manifest,
        )?;
        let query_json_path = compile_enterprise_queries(&target_queries_dir)?;
        Step::verbose_substep(&format!("  Regenerated {}", query_json_path.display()));
        Ok(())
    };
    let mut outcome = SyncReconciliationOutcome::Unchanged;

    match comparison {
        SnapshotComparison::BothEmpty | SnapshotComparison::InSync => {
            crate::output::info("Local and enterprise cloud changes are already in sync.");
        }
        SnapshotComparison::LocalOnly => {
            if let Err(error) = validate_local_enterprise_queries_for_push(project) {
                op.failure();
                return Err(eyre!(
                    "enterprise query project failed validation. Fix errors before pushing to cloud.\n\n{error}"
                ));
            }

            match confirm_sync_action(
                assume_yes,
                "your enterprise cluster has no source snapshot. Push your local query project to cloud now?",
            )? {
                true => {
                    let diff = compute_manifest_diff(&local_manifest, &remote_manifest);
                    print_plan_for_direction(&diff, SyncDirection::Push);
                    push_local_enterprise_snapshot_to_cluster(project, cluster_id, cluster_name)
                        .await?;
                    outcome = SyncReconciliationOutcome::Pushed;
                }
                false => crate::output::info("Left local and cloud changes unchanged."),
            }
        }
        SnapshotComparison::RemoteOnly => {
            match confirm_sync_action(
                assume_yes,
                "Local enterprise source is empty while cloud has files. Pull cloud files to local?",
            )? {
                true => {
                    let diff = compute_manifest_diff(&local_manifest, &remote_manifest);
                    print_plan_for_direction(&diff, SyncDirection::Pull);
                    apply_pull()?;
                    outcome = SyncReconciliationOutcome::Pulled;
                }
                false => crate::output::info("Left local and cloud changes unchanged."),
            }
        }
        SnapshotComparison::Diverged { authority, diff } => match authority {
            DivergenceAuthority::LocalNewer => {
                let push_allowed = match validate_local_enterprise_queries_for_push(project) {
                    Ok(()) => true,
                    Err(error) => {
                        crate::output::warning(
                            "Local enterprise queries failed validation, so pushing local files is unavailable.",
                        );
                        crate::output::warning(&error.to_string());
                        false
                    }
                };

                if push_allowed {
                    if confirm_sync_action(
                        assume_yes,
                        "Local enterprise changes are newer. Push your local query project to cloud?",
                    )? {
                        print_plan_for_direction(&diff, SyncDirection::Push);
                        push_local_enterprise_snapshot_to_cluster(
                            project,
                            cluster_id,
                            cluster_name,
                        )
                        .await?;
                        outcome = SyncReconciliationOutcome::Pushed;
                    } else if confirm_sync_action(
                        false,
                        "Overwrite local enterprise files with cloud changes instead?",
                    )? {
                        print_plan_for_direction(&diff, SyncDirection::Pull);
                        apply_pull()?;
                        outcome = SyncReconciliationOutcome::Pulled;
                    } else {
                        crate::output::info("Left local and cloud changes unchanged.");
                    }
                } else if assume_yes || !prompts::is_interactive() {
                    crate::output::info(
                        "Local push skipped because enterprise query project failed validation.",
                    );
                    crate::output::info("Left local and cloud changes unchanged.");
                } else if confirm_sync_action(
                    false,
                    "Overwrite local enterprise files with cloud changes instead?",
                )? {
                    print_plan_for_direction(&diff, SyncDirection::Pull);
                    apply_pull()?;
                    outcome = SyncReconciliationOutcome::Pulled;
                } else {
                    crate::output::info("Left local and cloud changes unchanged.");
                }
            }
            DivergenceAuthority::RemoteNewer => {
                match confirm_sync_action(
                    assume_yes,
                    "Enterprise cloud changes are newer. Pull cloud files to local?",
                )? {
                    true => {
                        print_plan_for_direction(&diff, SyncDirection::Pull);
                        apply_pull()?;
                        outcome = SyncReconciliationOutcome::Pulled;
                    }
                    false => crate::output::info("Left local and cloud changes unchanged."),
                }
            }
            DivergenceAuthority::TieOrUnknown => {
                let allow_push = match validate_local_enterprise_queries_for_push(project) {
                    Ok(()) => true,
                    Err(error) => {
                        crate::output::warning(
                            "Local enterprise queries failed validation, so pushing local files is unavailable.",
                        );
                        crate::output::warning(&error.to_string());
                        false
                    }
                };

                match resolve_tie_action(assume_yes, allow_push)? {
                    TieResolutionAction::NoOp => {
                        crate::output::info("Left local and cloud changes unchanged.");
                    }
                    TieResolutionAction::Pull => {
                        print_plan_for_direction(&diff, SyncDirection::Pull);
                        apply_pull()?;
                        outcome = SyncReconciliationOutcome::Pulled;
                    }
                    TieResolutionAction::Push => {
                        print_plan_for_direction(&diff, SyncDirection::Push);
                        push_local_enterprise_snapshot_to_cluster(
                            project,
                            cluster_id,
                            cluster_name,
                        )
                        .await?;
                        outcome = SyncReconciliationOutcome::Pushed;
                    }
                }
            }
        },
    }

    if outcome != SyncReconciliationOutcome::Unchanged {
        crate::output::success("Enterprise sync reconciliation applied.");
    }

    op.success();
    Ok(outcome)
}

fn validate_local_enterprise_queries_for_push(project: &ProjectContext) -> Result<()> {
    compile_enterprise_queries(&enterprise_queries_dir(project)).map(|_| ())
}

async fn push_local_enterprise_snapshot_to_cluster(
    project: &ProjectContext,
    cluster_id: &str,
    cluster_name: &str,
) -> Result<()> {
    let refreshed_project = ProjectContext::find_and_load(Some(&project.root))
        .map_err(|e| eyre!("Failed to reload project context: {e}"))?;
    deploy_enterprise_by_cluster_id(&refreshed_project, cluster_id, cluster_name).await
}

fn compute_sha256(content: &str) -> String {
    format!("{:x}", Sha256::digest(content.as_bytes()))
}

fn system_time_to_ms(timestamp: SystemTime) -> Option<i64> {
    timestamp
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
}

fn collect_local_enterprise_manifest(queries_dir: &Path) -> Result<HashMap<String, ManifestEntry>> {
    fn walk(dir: &Path, root: &Path, manifest: &mut HashMap<String, ManifestEntry>) -> Result<()> {
        for entry in fs::read_dir(dir)
            .map_err(|e| eyre!("Failed to read directory {}: {}", dir.display(), e))?
        {
            let entry = entry.map_err(|e| eyre!("Failed to read directory entry: {e}"))?;
            let path = entry.path();
            let relative = path
                .strip_prefix(root)
                .map_err(|_| eyre!("Failed to compute relative path for {}", path.display()))?;

            if path.is_dir() {
                if should_descend_enterprise_source_dir(relative) {
                    walk(&path, root, manifest)?;
                }
                continue;
            }

            if !should_include_enterprise_source_file(relative) {
                continue;
            }

            let relative_path = relative.to_string_lossy().replace('\\', "/");
            let content = match fs::read_to_string(&path) {
                Ok(content) => content,
                Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
                    Step::verbose_substep(&format!(
                        "  Skipping non-utf8 source file during sync: {relative_path}"
                    ));
                    continue;
                }
                Err(e) => {
                    return Err(eyre!(
                        "Failed to read local source file {}: {e}",
                        path.display()
                    ));
                }
            };
            let last_modified_ms = entry
                .metadata()
                .ok()
                .and_then(|metadata| metadata.modified().ok())
                .and_then(system_time_to_ms);

            manifest.insert(
                relative_path,
                ManifestEntry {
                    sha256: compute_sha256(&content),
                    last_modified_ms,
                    content,
                },
            );
        }

        Ok(())
    }

    let mut manifest = HashMap::new();
    if !queries_dir.exists() {
        return Ok(manifest);
    }
    walk(queries_dir, queries_dir, &mut manifest)?;
    Ok(manifest)
}

fn build_remote_enterprise_manifest(
    sync_response: &EnterpriseSyncResponse,
) -> HashMap<String, ManifestEntry> {
    let mut manifest = HashMap::new();

    for (raw_path, content) in &sync_response.source_files {
        let safe_path = match sanitize_relative_path(Path::new(raw_path)) {
            Ok(path) => path,
            Err(e) => {
                crate::output::warning(&format!(
                    "Skipping remote enterprise file '{raw_path}' due to unsafe path: {e}"
                ));
                continue;
            }
        };
        let normalized_path = safe_path.to_string_lossy().replace('\\', "/");
        if !should_include_enterprise_source_file(Path::new(&normalized_path)) {
            continue;
        }
        let metadata = sync_response
            .file_metadata
            .get(raw_path)
            .or_else(|| sync_response.file_metadata.get(&normalized_path));

        manifest.insert(
            normalized_path,
            ManifestEntry {
                sha256: metadata
                    .and_then(|entry| entry.sha256.clone())
                    .unwrap_or_else(|| compute_sha256(content)),
                last_modified_ms: metadata.and_then(|entry| entry.last_modified_ms),
                content: content.clone(),
            },
        );
    }

    manifest
}

fn compute_manifest_diff(
    local: &HashMap<String, ManifestEntry>,
    remote: &HashMap<String, ManifestEntry>,
) -> ManifestDiff {
    let mut diff = ManifestDiff::default();
    let mut all_paths = BTreeSet::new();
    all_paths.extend(local.keys().cloned());
    all_paths.extend(remote.keys().cloned());

    for path in all_paths {
        match (local.get(&path), remote.get(&path)) {
            (Some(_), None) => diff.local_only.push(path),
            (None, Some(_)) => diff.remote_only.push(path),
            (Some(local_entry), Some(remote_entry)) => {
                if local_entry.sha256 != remote_entry.sha256 {
                    diff.changed.push(path);
                }
            }
            (None, None) => {}
        }
    }

    diff
}

fn newest_timestamp_for_paths(
    manifest: &HashMap<String, ManifestEntry>,
    paths: &[String],
) -> Option<i64> {
    paths
        .iter()
        .filter_map(|path| manifest.get(path).and_then(|entry| entry.last_modified_ms))
        .max()
}

fn compare_manifests(
    local: &HashMap<String, ManifestEntry>,
    remote: &HashMap<String, ManifestEntry>,
) -> SnapshotComparison {
    if local.is_empty() && remote.is_empty() {
        return SnapshotComparison::BothEmpty;
    }
    if !local.is_empty() && remote.is_empty() {
        return SnapshotComparison::LocalOnly;
    }
    if local.is_empty() && !remote.is_empty() {
        return SnapshotComparison::RemoteOnly;
    }

    let diff = compute_manifest_diff(local, remote);
    if diff.is_empty() {
        return SnapshotComparison::InSync;
    }

    let differing_paths = diff.all_files();
    let local_latest = newest_timestamp_for_paths(local, &differing_paths);
    let remote_latest = newest_timestamp_for_paths(remote, &differing_paths);
    let authority = match (local_latest, remote_latest) {
        (Some(local_ms), Some(remote_ms)) => {
            let delta = local_ms - remote_ms;
            if delta.abs() <= CLOCK_SKEW_WINDOW_MS {
                DivergenceAuthority::TieOrUnknown
            } else if delta > 0 {
                DivergenceAuthority::LocalNewer
            } else {
                DivergenceAuthority::RemoteNewer
            }
        }
        _ => DivergenceAuthority::TieOrUnknown,
    };

    SnapshotComparison::Diverged { authority, diff }
}

fn sanitize_relative_path(relative_path: &Path) -> Result<PathBuf> {
    if relative_path.is_absolute() {
        return Err(eyre!("Refusing absolute path: {}", relative_path.display()));
    }

    let mut sanitized = PathBuf::new();
    for component in relative_path.components() {
        match component {
            Component::Normal(part) => sanitized.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(eyre!(
                    "Refusing unsafe relative path: {}",
                    relative_path.display()
                ));
            }
        }
    }

    if sanitized.as_os_str().is_empty() {
        return Err(eyre!("Refusing empty path: {}", relative_path.display()));
    }

    Ok(sanitized)
}

fn safe_join_relative(base_dir: &Path, relative_path: &str) -> Result<PathBuf> {
    Ok(base_dir.join(sanitize_relative_path(Path::new(relative_path))?))
}

fn parse_and_sanitize_remote_config(remote_toml: &str, source: &str) -> Option<HelixConfig> {
    let mut remote_config = match toml::from_str::<HelixConfig>(remote_toml) {
        Ok(config) => config,
        Err(e) => {
            crate::output::warning(&format!(
                "Ignoring remote helix.toml from {source}: failed to parse ({e})"
            ));
            return None;
        }
    };

    match sanitize_relative_path(&remote_config.project.queries) {
        Ok(queries_relative) => {
            remote_config.project.queries = queries_relative;
        }
        Err(e) => {
            crate::output::warning(&format!(
                "Ignoring unsafe remote project.queries '{}' from {source}: {e}. Using current project queries path.",
                remote_config.project.queries.display()
            ));
            return None;
        }
    }

    Some(remote_config)
}

fn update_project_queries_path_in_helix_toml(
    project_root: &Path,
    queries_path: &Path,
) -> Result<()> {
    let helix_toml_path = project_root.join("helix.toml");
    let mut config = HelixConfig::from_file(&helix_toml_path)
        .map_err(|e| eyre!("Failed to load helix.toml for queries path update: {e}"))?;

    config.project.queries = sanitize_relative_path(queries_path)?;
    config
        .save_to_file(&helix_toml_path)
        .map_err(|e| eyre!("Failed to update queries path in helix.toml: {e}"))?;

    Ok(())
}

fn pull_remote_enterprise_snapshot_into_local(
    current_queries_dir: &Path,
    target_queries_dir: &Path,
    local_manifest: &HashMap<String, ManifestEntry>,
    remote_manifest: &HashMap<String, ManifestEntry>,
) -> Result<()> {
    if current_queries_dir == target_queries_dir {
        fs::create_dir_all(target_queries_dir)?;
        for (relative_path, remote_entry) in remote_manifest {
            let destination = safe_join_relative(target_queries_dir, relative_path)?;
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&destination, &remote_entry.content)
                .map_err(|e| eyre!("Failed to write {relative_path}: {e}"))?;
        }

        for local_only_path in local_manifest
            .keys()
            .filter(|path| !remote_manifest.contains_key(*path))
        {
            let local_path = safe_join_relative(current_queries_dir, local_only_path)?;
            if local_path.exists() {
                fs::remove_file(&local_path).map_err(|e| {
                    eyre!("Failed to remove local enterprise file {local_only_path}: {e}")
                })?;
                Step::verbose_substep(&format!("  Removed {local_only_path}"));
            }
        }

        return Ok(());
    }

    let target_manifest = collect_local_enterprise_manifest(target_queries_dir)?;
    fs::create_dir_all(target_queries_dir)?;

    for (relative_path, remote_entry) in remote_manifest {
        let destination = safe_join_relative(target_queries_dir, relative_path)?;
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&destination, &remote_entry.content)
            .map_err(|e| eyre!("Failed to write {relative_path}: {e}"))?;
    }

    for relative_path in local_manifest.keys() {
        let local_path = safe_join_relative(current_queries_dir, relative_path)?;
        if local_path.exists() {
            fs::remove_file(&local_path).map_err(|e| {
                eyre!("Failed to remove local enterprise file {relative_path}: {e}")
            })?;
            Step::verbose_substep(&format!("  Removed {relative_path}"));
        }
    }

    for relative_path in target_manifest
        .keys()
        .filter(|path| !remote_manifest.contains_key(*path))
    {
        let local_path = safe_join_relative(target_queries_dir, relative_path)?;
        if local_path.exists() {
            fs::remove_file(&local_path).map_err(|e| {
                eyre!("Failed to remove local enterprise file {relative_path}: {e}")
            })?;
            Step::verbose_substep(&format!("  Removed {relative_path}"));
        }
    }

    Ok(())
}

fn confirm_sync_action(assume_yes: bool, prompt: &str) -> Result<bool> {
    if assume_yes {
        crate::output::info("Proceeding because --yes was provided.");
        return Ok(true);
    }
    if !prompts::is_interactive() {
        return Err(eyre!(
            "Sync requires confirmation. Re-run with '--yes' in non-interactive mode."
        ));
    }

    prompts::confirm(prompt)
}

fn resolve_tie_action(assume_yes: bool, allow_push: bool) -> Result<TieResolutionAction> {
    if assume_yes || !prompts::is_interactive() {
        crate::output::warning(
            "Local and cloud changes appear near-simultaneous. Leaving files unchanged by default.",
        );
        return Ok(TieResolutionAction::NoOp);
    }

    let mut select = cliclack::select(
        "Local and cloud changes happened at nearly the same time. Choose a sync action",
    )
    .item("noop", "Keep unchanged", "Safe default")
    .item("pull", "Pull cloud", "Overwrite local from cloud");
    if allow_push {
        select = select.item("push", "Push local", "Push local changes to cloud");
    }
    let selection: &'static str = select.interact()?;

    Ok(match selection {
        "pull" => TieResolutionAction::Pull,
        "push" => TieResolutionAction::Push,
        _ => TieResolutionAction::NoOp,
    })
}

fn build_sync_action_plan(diff: &ManifestDiff, direction: SyncDirection) -> SyncActionPlan {
    let (mut to_create, mut to_delete) = match direction {
        SyncDirection::Pull => (diff.remote_only.clone(), diff.local_only.clone()),
        SyncDirection::Push => (diff.local_only.clone(), diff.remote_only.clone()),
    };
    let mut to_change = diff.changed.clone();

    to_create.sort();
    to_change.sort();
    to_delete.sort();

    SyncActionPlan {
        to_create,
        to_change,
        to_delete,
    }
}

fn styled_plan_marker(marker: &str) -> String {
    match marker {
        "+" => marker.green().bold().to_string(),
        "-" => marker.red().bold().to_string(),
        "=" => marker.yellow().bold().to_string(),
        _ => marker.bold().to_string(),
    }
}

fn print_plan_section(marker: &str, files: &[String]) {
    for file in files {
        println!("  {} {file}", styled_plan_marker(marker));
    }
}

fn print_sync_action_plan(direction: SyncDirection, plan: &SyncActionPlan) {
    let target = match direction {
        SyncDirection::Pull => "Local",
        SyncDirection::Push => "Cloud",
    };

    let mut printed_any = false;
    if !plan.to_delete.is_empty() {
        println!();
        println!("{target} files to be deleted ({})", plan.to_delete.len());
        print_plan_section("-", &plan.to_delete);
        printed_any = true;
    }
    if !plan.to_change.is_empty() {
        println!();
        println!("{target} files to be changed ({})", plan.to_change.len());
        print_plan_section("=", &plan.to_change);
        printed_any = true;
    }
    if !plan.to_create.is_empty() {
        println!();
        println!("{target} files to be created ({})", plan.to_create.len());
        print_plan_section("+", &plan.to_create);
        printed_any = true;
    }
    if !printed_any {
        crate::output::info("No file changes to apply.");
    }
}

fn print_plan_for_direction(diff: &ManifestDiff, direction: SyncDirection) {
    let plan = build_sync_action_plan(diff, direction);
    print_sync_action_plan(direction, &plan);
}

/// Print what a real `helix sync` would do for the current divergence, without
/// applying anything. Used by `--dry-run`.
fn print_dry_run_summary(
    comparison: &SnapshotComparison,
    local_manifest: &HashMap<String, ManifestEntry>,
    remote_manifest: &HashMap<String, ManifestEntry>,
) {
    match comparison {
        SnapshotComparison::BothEmpty | SnapshotComparison::InSync => {
            crate::output::info("Local and enterprise cloud changes are already in sync.");
        }
        SnapshotComparison::LocalOnly => {
            crate::output::info(
                "Cloud has no source snapshot; sync would push your local query project.",
            );
            let diff = compute_manifest_diff(local_manifest, remote_manifest);
            print_plan_for_direction(&diff, SyncDirection::Push);
        }
        SnapshotComparison::RemoteOnly => {
            crate::output::info("Local source is empty; sync would pull cloud files.");
            let diff = compute_manifest_diff(local_manifest, remote_manifest);
            print_plan_for_direction(&diff, SyncDirection::Pull);
        }
        SnapshotComparison::Diverged { authority, diff } => match authority {
            DivergenceAuthority::LocalNewer => {
                crate::output::info(
                    "Local changes are newer; sync would offer to push (pull available as an alternative).",
                );
                print_plan_for_direction(diff, SyncDirection::Push);
            }
            DivergenceAuthority::RemoteNewer => {
                crate::output::info("Cloud changes are newer; sync would offer to pull.");
                print_plan_for_direction(diff, SyncDirection::Pull);
            }
            DivergenceAuthority::TieOrUnknown => {
                crate::output::info(
                    "Local and cloud have diverged; sync would ask which side wins.",
                );
                crate::output::info("If you push:");
                print_plan_for_direction(diff, SyncDirection::Push);
                crate::output::info("If you pull:");
                print_plan_for_direction(diff, SyncDirection::Pull);
            }
        },
    }
    crate::output::info("Dry run: no changes were made.");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enterprise_cloud::CliProjectClusters;

    fn manifest_entry(hash: &str, last_modified_ms: Option<i64>) -> ManifestEntry {
        ManifestEntry {
            sha256: hash.to_string(),
            last_modified_ms,
            content: String::new(),
        }
    }

    #[test]
    fn compare_manifests_prefers_local_when_local_is_newer() {
        let local = HashMap::from([(
            "src/main.rs".to_string(),
            manifest_entry("local", Some(10_000)),
        )]);
        let remote = HashMap::from([(
            "src/main.rs".to_string(),
            manifest_entry("remote", Some(1_000)),
        )]);

        let comparison = compare_manifests(&local, &remote);

        assert!(matches!(
            comparison,
            SnapshotComparison::Diverged {
                authority: DivergenceAuthority::LocalNewer,
                ..
            }
        ));
    }

    #[test]
    fn build_remote_enterprise_manifest_normalizes_paths_and_uses_metadata() {
        let mut source_files = HashMap::new();
        source_files.insert("Cargo.toml".to_string(), "[package]\n".to_string());
        source_files.insert("src\\main.rs".to_string(), "fn main() {}\n".to_string());
        source_files.insert("queries.json".to_string(), "ignore".to_string());
        source_files.insert("../escape.rs".to_string(), "ignore".to_string());
        source_files.insert("README.md".to_string(), "ignore".to_string());

        let file_metadata = HashMap::from([(
            "src/main.rs".to_string(),
            SyncFileMetadata {
                sha256: Some("remote-sha".to_string()),
                last_modified_ms: Some(42),
            },
        )]);
        let response = EnterpriseSyncResponse {
            source_files,
            file_metadata,
            helix_toml: None,
        };

        let manifest = build_remote_enterprise_manifest(&response);

        assert_eq!(manifest.len(), 2);
        assert!(manifest.contains_key("Cargo.toml"));
        assert!(manifest.contains_key("src/main.rs"));
        assert_eq!(manifest["src/main.rs"].sha256, "remote-sha");
        assert!(!manifest.contains_key("queries.json"));
        assert!(!manifest.contains_key("README.md"));
        assert!(!manifest.contains_key("../escape.rs"));
    }

    #[test]
    fn enterprise_cluster_counts_accept_role_based_values() {
        let response: CliProjectClusters = serde_json::from_value(serde_json::json!({
            "project_id": "project-1",
            "project_name": "demo",
            "enterprise": [{
                "cluster_id": "cluster-1",
                "cluster_name": "enterprise-a",
                "availability_mode": "ha",
                "gateway_node_type": "GW-40",
                "db_node_type": "HLX-160",
                "min_gateway_count": 6,
                "max_gateway_count": 6,
                "min_hyperscale_count": 3,
                "max_hyperscale_count": 3
            }]
        }))
        .unwrap();

        let cluster = &response.enterprise[0];
        assert_eq!(cluster.resolved_gateway_count(), Some(6));
        assert_eq!(cluster.resolved_hyperscale_count(), Some(3));
        assert_eq!(cluster.compatibility_min_instances(), Some(3));
        assert_eq!(cluster.compatibility_max_instances(), Some(6));
    }

    #[test]
    fn compare_manifests_covers_empty_sync_remote_and_tie_states() {
        let empty = HashMap::new();
        assert!(matches!(
            compare_manifests(&empty, &empty),
            SnapshotComparison::BothEmpty
        ));

        let same = HashMap::from([("src/main.rs".to_string(), manifest_entry("same", Some(1)))]);
        assert!(matches!(
            compare_manifests(&same, &same),
            SnapshotComparison::InSync
        ));
        assert!(matches!(
            compare_manifests(&empty, &same),
            SnapshotComparison::RemoteOnly
        ));
        assert!(matches!(
            compare_manifests(&same, &empty),
            SnapshotComparison::LocalOnly
        ));

        let local = HashMap::from([(
            "src/main.rs".to_string(),
            manifest_entry("local", Some(1_000)),
        )]);
        let remote = HashMap::from([(
            "src/main.rs".to_string(),
            manifest_entry("remote", Some(10_000)),
        )]);
        assert!(matches!(
            compare_manifests(&local, &remote),
            SnapshotComparison::Diverged {
                authority: DivergenceAuthority::RemoteNewer,
                ..
            }
        ));

        let remote = HashMap::from([("src/main.rs".to_string(), manifest_entry("remote", None))]);
        assert!(matches!(
            compare_manifests(&local, &remote),
            SnapshotComparison::Diverged {
                authority: DivergenceAuthority::TieOrUnknown,
                ..
            }
        ));
    }

    #[test]
    fn manifest_diff_and_action_plan_cover_add_update_delete() {
        let local = HashMap::from([
            ("added.rs".to_string(), manifest_entry("a", Some(1))),
            ("changed.rs".to_string(), manifest_entry("local", Some(1))),
        ]);
        let remote = HashMap::from([
            ("removed.rs".to_string(), manifest_entry("r", Some(1))),
            ("changed.rs".to_string(), manifest_entry("remote", Some(1))),
        ]);
        let diff = compute_manifest_diff(&local, &remote);
        assert_eq!(diff.local_only, vec!["added.rs"]);
        assert_eq!(diff.remote_only, vec!["removed.rs"]);
        assert_eq!(diff.changed, vec!["changed.rs"]);

        let push = build_sync_action_plan(&diff, SyncDirection::Push);
        assert_eq!(push.to_create, vec!["added.rs"]);
        assert_eq!(push.to_change, vec!["changed.rs"]);
        assert_eq!(push.to_delete, vec!["removed.rs"]);
        let pull = build_sync_action_plan(&diff, SyncDirection::Pull);
        assert_eq!(pull.to_create, vec!["removed.rs"]);
        assert_eq!(pull.to_delete, vec!["added.rs"]);
    }

    #[test]
    fn remote_paths_cannot_escape_the_query_directory() {
        assert_eq!(
            sanitize_relative_path(Path::new("src/main.rs")).unwrap(),
            Path::new("src/main.rs")
        );
        assert!(sanitize_relative_path(Path::new("../escape.rs")).is_err());
        assert!(sanitize_relative_path(Path::new("/absolute.rs")).is_err());
        assert!(sanitize_relative_path(Path::new("src/../../escape.rs")).is_err());
    }
}
