use crate::commands::auth::require_auth;
use crate::commands::integrations::helix::HelixManager;
use crate::commands::integrations::helix::cloud_base_url;
use crate::config::{
    AvailabilityMode, BuildMode, CloudConfig, CloudInstanceConfig, DbConfig,
    EnterpriseInstanceConfig, HelixConfig, InstanceInfo, WorkspaceConfig,
};
use crate::output::{Operation, Step};
use crate::project::ProjectContext;
use crate::prompts;
use crate::utils::helixc_utils::{
    analyze_source, collect_hx_files, generate_content, parse_content,
};
use crate::utils::print_warning;
use color_eyre::owo_colors::OwoColorize;
use eyre::{Result, eyre};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Deserialize, Default)]
struct SyncResponse {
    #[serde(default)]
    helix_toml: Option<String>,
    #[serde(default)]
    hx_files: HashMap<String, String>,
    #[serde(default)]
    file_metadata: HashMap<String, SyncFileMetadata>,
}

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

#[derive(Deserialize)]
struct CliEnterpriseCluster {
    pub cluster_id: String,
    pub cluster_name: String,
    #[allow(dead_code)]
    pub project_id: String,
    pub project_name: String,
    #[allow(dead_code)]
    pub availability_mode: String,
}

#[derive(Deserialize)]
struct CliWorkspaceClusters {
    standard: Vec<CliCluster>,
    enterprise: Vec<CliEnterpriseCluster>,
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
    #[allow(dead_code)]
    pub project_id: String,
    pub project_name: String,
}

#[derive(Clone, Deserialize)]
struct CliProject {
    id: String,
    name: String,
}

#[derive(Deserialize)]
struct CreateProjectResponse {
    id: String,
}

#[derive(Deserialize)]
struct CliProjectClusters {
    project_id: String,
    project_name: String,
    standard: Vec<CliProjectStandardCluster>,
    enterprise: Vec<CliProjectEnterpriseCluster>,
}

#[derive(Deserialize)]
struct CliProjectStandardCluster {
    cluster_id: String,
    cluster_name: String,
    build_mode: String,
    #[allow(dead_code)]
    max_memory_gb: u32,
    #[allow(dead_code)]
    max_vcpus: f32,
}

#[derive(Deserialize)]
struct CliProjectEnterpriseCluster {
    cluster_id: String,
    cluster_name: String,
    availability_mode: String,
    gateway_node_type: String,
    db_node_type: String,
    min_instances: u64,
    max_instances: u64,
}

#[derive(Deserialize)]
struct CliClusterProject {
    #[allow(dead_code)]
    cluster_id: String,
    project_id: String,
    #[allow(dead_code)]
    project_name: String,
    #[allow(dead_code)]
    workspace_id: String,
}

const DEFAULT_QUERIES_DIR: &str = "db";
const CLOCK_SKEW_WINDOW_MS: i64 = 5_000;

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

fn compute_sha256(content: &str) -> String {
    format!("{:x}", Sha256::digest(content.as_bytes()))
}

fn system_time_to_ms(timestamp: SystemTime) -> Option<i64> {
    timestamp
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
}

fn collect_local_hx_manifest(queries_dir: &Path) -> Result<HashMap<String, ManifestEntry>> {
    fn walk(dir: &Path, root: &Path, manifest: &mut HashMap<String, ManifestEntry>) -> Result<()> {
        for entry in fs::read_dir(dir)
            .map_err(|e| eyre!("Failed to read directory {}: {}", dir.display(), e))?
        {
            let entry = entry.map_err(|e| eyre!("Failed to read directory entry: {}", e))?;
            let path = entry.path();

            if path.is_dir() {
                walk(&path, root, manifest)?;
                continue;
            }

            let is_hx = path.extension().map(|ext| ext == "hx").unwrap_or(false);
            if !is_hx {
                continue;
            }

            let relative_path = path
                .strip_prefix(root)
                .map_err(|_| eyre!("Failed to compute relative path for {}", path.display()))?
                .to_string_lossy()
                .replace('\\', "/");
            let content = fs::read_to_string(&path)
                .map_err(|e| eyre!("Failed to read local file {}: {}", path.display(), e))?;
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

fn build_remote_hx_manifest(sync_response: &SyncResponse) -> HashMap<String, ManifestEntry> {
    let mut manifest = HashMap::new();

    for (raw_path, content) in &sync_response.hx_files {
        let safe_path = match sanitize_relative_path(Path::new(raw_path)) {
            Ok(path) => path,
            Err(e) => {
                print_warning(&format!(
                    "Skipping remote file '{}' due to unsafe path: {}",
                    raw_path, e
                ));
                continue;
            }
        };
        let normalized_path = safe_path.to_string_lossy().replace('\\', "/");

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

fn should_descend_enterprise_source_dir(relative_path: &Path) -> bool {
    if relative_path.as_os_str().is_empty() {
        return true;
    }

    for component in relative_path.components() {
        if let Component::Normal(part) = component {
            if part == "target" || part == ".git" {
                return false;
            }
        }
    }

    true
}

fn should_include_enterprise_source_file(relative_path: &Path) -> bool {
    if relative_path.as_os_str().is_empty() {
        return false;
    }

    let normalized = relative_path.to_string_lossy().replace('\\', "/");
    if normalized == "queries.bin" {
        return false;
    }

    if !should_descend_enterprise_source_dir(relative_path) {
        return false;
    }

    matches!(
        normalized.as_str(),
        "Cargo.toml" | "Cargo.lock" | "build.rs" | "rust-toolchain" | "rust-toolchain.toml"
    ) || normalized.starts_with("src/")
        || (normalized.starts_with(".cargo/") && normalized.ends_with(".toml"))
}

fn collect_local_enterprise_manifest(queries_dir: &Path) -> Result<HashMap<String, ManifestEntry>> {
    fn walk(dir: &Path, root: &Path, manifest: &mut HashMap<String, ManifestEntry>) -> Result<()> {
        for entry in fs::read_dir(dir)
            .map_err(|e| eyre!("Failed to read directory {}: {}", dir.display(), e))?
        {
            let entry = entry.map_err(|e| eyre!("Failed to read directory entry: {}", e))?;
            let path = entry.path();
            let relative = path
                .strip_prefix(root)
                .map_err(|_| eyre!("Failed to compute relative path for {}", path.display()))?;

            if path.is_dir() {
                if !should_descend_enterprise_source_dir(relative) {
                    continue;
                }
                walk(&path, root, manifest)?;
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
                        "  Skipping non-utf8 source file during sync: {}",
                        relative_path
                    ));
                    continue;
                }
                Err(e) => {
                    return Err(eyre!(
                        "Failed to read local source file {}: {}",
                        path.display(),
                        e
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
                print_warning(&format!(
                    "Skipping remote enterprise file '{}' due to unsafe path: {}",
                    raw_path, e
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
        .map_err(|e| eyre!("Failed to connect to Helix Cloud: {}", e))?;

    match response.status() {
        reqwest::StatusCode::OK => response
            .json::<EnterpriseSyncResponse>()
            .await
            .map_err(|e| eyre!("Failed to parse enterprise sync response: {}", e)),
        reqwest::StatusCode::NOT_FOUND => {
            print_warning(&format!(
                "No remote enterprise source files found for cluster '{}'. Treating cloud changes as empty.",
                cluster_id
            ));
            Ok(EnterpriseSyncResponse::default())
        }
        reqwest::StatusCode::UNAUTHORIZED => Err(eyre!(
            "Authentication failed. Run 'helix auth login' to re-authenticate."
        )),
        reqwest::StatusCode::FORBIDDEN => Err(eyre!(
            "Access denied to enterprise cluster '{}'. Make sure you have permission to access this cluster.",
            cluster_id
        )),
        status => {
            let error_text = response.text().await.unwrap_or_default();
            Err(eyre!("Enterprise sync failed ({}): {}", status, error_text))
        }
    }
}

fn regenerate_enterprise_queries_bin(queries_dir: &Path) -> Result<PathBuf> {
    let manifest_path = queries_dir.join("Cargo.toml");
    if !manifest_path.exists() {
        return Err(eyre!(
            "Enterprise queries Cargo.toml not found at {}",
            manifest_path.display()
        ));
    }

    let compile_output = Command::new("cargo")
        .arg("run")
        .arg("--manifest-path")
        .arg(&manifest_path)
        .current_dir(queries_dir)
        .output()
        .map_err(|e| eyre!("Failed to run cargo for enterprise queries: {e}"))?;

    if !compile_output.status.success() {
        let stderr = String::from_utf8_lossy(&compile_output.stderr);
        let stdout = String::from_utf8_lossy(&compile_output.stdout);
        return Err(eyre!(
            "Enterprise query project compilation failed during sync:\n{}\n{}",
            stderr,
            stdout
        ));
    }

    let query_bin_path = queries_dir.join("queries.bin");
    if !query_bin_path.exists() {
        return Err(eyre!(
            "Enterprise query project did not generate queries.bin at {}",
            query_bin_path.display()
        ));
    }

    let metadata = fs::metadata(&query_bin_path)
        .map_err(|e| eyre!("Failed to read queries.bin metadata: {}", e))?;
    if metadata.len() == 0 {
        return Err(eyre!(
            "Generated queries.bin is empty ({})",
            query_bin_path.display()
        ));
    }

    Ok(query_bin_path)
}

fn validate_local_enterprise_queries_for_push(project: &ProjectContext) -> Result<()> {
    let queries_dir = project.root.join(&project.config.project.queries);
    regenerate_enterprise_queries_bin(&queries_dir).map(|_| ())
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
    let relative = relative_path;

    if relative.is_absolute() {
        return Err(eyre!("Refusing absolute path: {}", relative.display()));
    }

    let mut sanitized = PathBuf::new();
    for component in relative.components() {
        match component {
            Component::Normal(part) => sanitized.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(eyre!(
                    "Refusing unsafe relative path: {}",
                    relative.display()
                ));
            }
        }
    }

    if sanitized.as_os_str().is_empty() {
        return Err(eyre!("Refusing empty path: {}", relative.display()));
    }

    Ok(sanitized)
}

fn safe_join_relative(base_dir: &Path, relative_path: &str) -> Result<PathBuf> {
    Ok(base_dir.join(sanitize_relative_path(Path::new(relative_path))?))
}

fn parse_and_sanitize_remote_config(
    remote_toml: &str,
    source: &str,
) -> Option<crate::config::HelixConfig> {
    let mut remote_config = match toml::from_str::<crate::config::HelixConfig>(remote_toml) {
        Ok(config) => config,
        Err(e) => {
            print_warning(&format!(
                "Ignoring remote helix.toml from {}: failed to parse ({})",
                source, e
            ));
            return None;
        }
    };

    match sanitize_relative_path(&remote_config.project.queries) {
        Ok(queries_relative) => {
            remote_config.project.queries = queries_relative;
        }
        Err(e) => {
            print_warning(&format!(
                "Ignoring unsafe remote project.queries '{}' from {}: {}. Using '{}'.",
                remote_config.project.queries.display(),
                source,
                e,
                DEFAULT_QUERIES_DIR
            ));
            remote_config.project.queries = PathBuf::from(DEFAULT_QUERIES_DIR);
        }
    }

    Some(remote_config)
}

fn serialize_remote_config(
    remote_config: &crate::config::HelixConfig,
    source: &str,
) -> Option<String> {
    match toml::to_string_pretty(remote_config) {
        Ok(serialized) => Some(serialized),
        Err(e) => {
            print_warning(&format!(
                "Failed to serialize sanitized remote helix.toml from {}: {}",
                source, e
            ));
            None
        }
    }
}

fn resolve_remote_queries_dir(
    base_dir: &Path,
    remote_config: Option<&crate::config::HelixConfig>,
) -> PathBuf {
    let Some(remote_config) = remote_config else {
        return base_dir.join(DEFAULT_QUERIES_DIR);
    };

    match sanitize_relative_path(&remote_config.project.queries) {
        Ok(queries_relative) => base_dir.join(queries_relative),
        Err(e) => {
            print_warning(&format!(
                "Ignoring unsafe remote project.queries '{}': {}. Using '{}'.",
                remote_config.project.queries.display(),
                e,
                DEFAULT_QUERIES_DIR
            ));
            base_dir.join(DEFAULT_QUERIES_DIR)
        }
    }
}

async fn fetch_sync_response_with_remote_empty_fallback(
    client: &reqwest::Client,
    api_key: &str,
    cluster_id: &str,
) -> Result<SyncResponse> {
    let sync_url = format!("{}/api/cli/clusters/{}/sync", cloud_base_url(), cluster_id);
    let response = client
        .get(&sync_url)
        .header("x-api-key", api_key)
        .send()
        .await
        .map_err(|e| eyre!("Failed to connect to Helix Cloud: {}", e))?;

    match response.status() {
        reqwest::StatusCode::OK => {
            let parsed: SyncResponse = response
                .json()
                .await
                .map_err(|e| eyre!("Failed to parse sync response: {}", e))?;
            Ok(parsed)
        }
        reqwest::StatusCode::NOT_FOUND => {
            print_warning(&format!(
                "No remote source files found for cluster '{}'. Treating cloud changes as empty.",
                cluster_id
            ));
            Ok(SyncResponse::default())
        }
        reqwest::StatusCode::UNAUTHORIZED => Err(eyre!(
            "Authentication failed. Run 'helix auth login' to re-authenticate."
        )),
        reqwest::StatusCode::FORBIDDEN => Err(eyre!(
            "Access denied to cluster '{}'. Make sure you have permission to access this cluster.",
            cluster_id
        )),
        status => {
            let error_text = response.text().await.unwrap_or_default();
            Err(eyre!("Sync failed ({}): {}", status, error_text))
        }
    }
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

fn validate_local_hx_queries_for_push(project: &ProjectContext) -> Result<()> {
    let hx_files =
        collect_hx_files(&project.root, &project.config.project.queries).map_err(|e| {
            eyre!(
                "Local .hx queries failed validation. Fix errors before pushing to cloud.\n\n{}",
                e
            )
        })?;
    let content = generate_content(&hx_files).map_err(|e| {
        eyre!(
            "Local .hx queries failed validation. Fix errors before pushing to cloud.\n\n{}",
            e
        )
    })?;
    let source = parse_content(&content).map_err(|e| {
        eyre!(
            "Local .hx queries failed validation. Fix errors before pushing to cloud.\n\n{}",
            e
        )
    })?;
    analyze_source(source, &content.files).map_err(|e| {
        eyre!(
            "Local .hx queries failed validation. Fix errors before pushing to cloud.\n\n{}",
            e
        )
    })?;

    Ok(())
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
        println!("  {} {}", styled_plan_marker(marker), file);
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
        println!("{} files to be deleted ({})", target, plan.to_delete.len());
        print_plan_section("-", &plan.to_delete);
        printed_any = true;
    }

    if !plan.to_change.is_empty() {
        println!();
        println!("{} files to be changed ({})", target, plan.to_change.len());
        print_plan_section("=", &plan.to_change);
        printed_any = true;
    }

    if !plan.to_create.is_empty() {
        println!();
        println!("{} files to be created ({})", target, plan.to_create.len());
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

fn pull_remote_snapshot_into_local(
    project: &ProjectContext,
    local_manifest: &HashMap<String, ManifestEntry>,
    remote_manifest: &HashMap<String, ManifestEntry>,
) -> Result<()> {
    let queries_dir = project.root.join(&project.config.project.queries);
    fs::create_dir_all(&queries_dir)?;

    for (relative_path, remote_entry) in remote_manifest {
        let destination = safe_join_relative(&queries_dir, relative_path)?;
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&destination, &remote_entry.content)
            .map_err(|e| eyre!("Failed to write {}: {}", relative_path, e))?;
    }

    for local_only_path in local_manifest
        .keys()
        .filter(|path| !remote_manifest.contains_key(*path))
    {
        let local_path = safe_join_relative(&queries_dir, local_only_path)?;
        if local_path.exists() {
            fs::remove_file(&local_path)
                .map_err(|e| eyre!("Failed to remove local file {}: {}", local_only_path, e))?;
            Step::verbose_substep(&format!("  Removed {}", local_only_path));
        }
    }

    Ok(())
}

async fn push_local_snapshot_to_cluster(
    project: &ProjectContext,
    cluster_id: &str,
    cluster_name: &str,
) -> Result<()> {
    let refreshed_project = ProjectContext::find_and_load(Some(&project.root))
        .map_err(|e| eyre!("Failed to reload project context: {}", e))?;
    let helix = HelixManager::new(&refreshed_project);

    helix
        .deploy_by_cluster_id(None, cluster_id, cluster_name, None)
        .await
}

fn pull_remote_enterprise_snapshot_into_local(
    project: &ProjectContext,
    local_manifest: &HashMap<String, ManifestEntry>,
    remote_manifest: &HashMap<String, ManifestEntry>,
) -> Result<()> {
    let queries_dir = project.root.join(&project.config.project.queries);
    fs::create_dir_all(&queries_dir)?;

    for (relative_path, remote_entry) in remote_manifest {
        let destination = safe_join_relative(&queries_dir, relative_path)?;
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&destination, &remote_entry.content)
            .map_err(|e| eyre!("Failed to write {}: {}", relative_path, e))?;
    }

    for local_only_path in local_manifest
        .keys()
        .filter(|path| !remote_manifest.contains_key(*path))
    {
        let local_path = safe_join_relative(&queries_dir, local_only_path)?;
        if local_path.exists() {
            fs::remove_file(&local_path).map_err(|e| {
                eyre!(
                    "Failed to remove local enterprise file {}: {}",
                    local_only_path,
                    e
                )
            })?;
            Step::verbose_substep(&format!("  Removed {}", local_only_path));
        }
    }

    Ok(())
}

async fn push_local_enterprise_snapshot_to_cluster(
    project: &ProjectContext,
    cluster_id: &str,
    cluster_name: &str,
) -> Result<()> {
    let refreshed_project = ProjectContext::find_and_load(Some(&project.root))
        .map_err(|e| eyre!("Failed to reload project context: {}", e))?;
    let helix = HelixManager::new(&refreshed_project);

    helix
        .deploy_enterprise_by_cluster_id(None, cluster_id, cluster_name)
        .await
}

#[derive(Clone, Copy)]
enum TieResolutionAction {
    NoOp,
    Pull,
    Push,
}

fn resolve_tie_action(assume_yes: bool, allow_push: bool) -> Result<TieResolutionAction> {
    if assume_yes || !prompts::is_interactive() {
        print_warning(
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

async fn reconcile_standard_cluster_snapshot(
    project: &ProjectContext,
    api_key: &str,
    cluster_id: &str,
    cluster_name: &str,
    assume_yes: bool,
) -> Result<()> {
    let op = Operation::new("Syncing", cluster_name);
    let client = reqwest::Client::new();

    let mut fetch_step = Step::with_messages("Fetching cloud changes", "Cloud changes fetched");
    fetch_step.start();
    let sync_response =
        match fetch_sync_response_with_remote_empty_fallback(&client, api_key, cluster_id).await {
            Ok(response) => {
                fetch_step.done();
                response
            }
            Err(error) => {
                fetch_step.fail();
                return Err(error);
            }
        };

    let queries_dir = project.root.join(&project.config.project.queries);
    let local_manifest = collect_local_hx_manifest(&queries_dir)?;
    let remote_manifest = build_remote_hx_manifest(&sync_response);
    let comparison = compare_manifests(&local_manifest, &remote_manifest);

    let mut changed = false;

    match comparison {
        SnapshotComparison::BothEmpty | SnapshotComparison::InSync => {
            crate::output::info("Local and cloud changes are already in sync.");
        }
        SnapshotComparison::LocalOnly => {
            if let Err(error) = validate_local_hx_queries_for_push(project) {
                op.failure();
                return Err(eyre!(
                    "your Cloud cluster has no queries, but local .hx queries failed validation. Fix errors before pushing to cloud.\n\n{}",
                    error
                ));
            }

            if confirm_sync_action(
                assume_yes,
                "your Cloud cluster has no queries! Push your local files to cloud now?",
            )? {
                let diff = compute_manifest_diff(&local_manifest, &remote_manifest);
                print_plan_for_direction(&diff, SyncDirection::Push);
                push_local_snapshot_to_cluster(project, cluster_id, cluster_name).await?;
                changed = true;
            } else {
                crate::output::info("Left local and cloud changes unchanged.");
            }
        }
        SnapshotComparison::RemoteOnly => {
            if confirm_sync_action(
                assume_yes,
                "Local source is empty while cloud has files. Pull cloud files to local?",
            )? {
                let diff = compute_manifest_diff(&local_manifest, &remote_manifest);
                print_plan_for_direction(&diff, SyncDirection::Pull);
                pull_remote_snapshot_into_local(project, &local_manifest, &remote_manifest)?;
                changed = true;
            } else {
                crate::output::info("Left local and cloud changes unchanged.");
            }
        }
        SnapshotComparison::Diverged { authority, diff } => match authority {
            DivergenceAuthority::LocalNewer => {
                let push_allowed = match validate_local_hx_queries_for_push(project) {
                    Ok(()) => true,
                    Err(error) => {
                        print_warning(
                            "Local .hx queries failed validation, so pushing local files is unavailable.",
                        );
                        print_warning(&error.to_string());
                        false
                    }
                };

                if push_allowed {
                    if confirm_sync_action(
                        assume_yes,
                        "Local changes are newer. Push your local files to cloud?",
                    )? {
                        print_plan_for_direction(&diff, SyncDirection::Push);
                        push_local_snapshot_to_cluster(project, cluster_id, cluster_name).await?;
                        changed = true;
                    } else if confirm_sync_action(
                        false,
                        "Overwrite local files with cloud changes instead?",
                    )? {
                        print_plan_for_direction(&diff, SyncDirection::Pull);
                        pull_remote_snapshot_into_local(
                            project,
                            &local_manifest,
                            &remote_manifest,
                        )?;
                        changed = true;
                    } else {
                        crate::output::info("Left local and cloud changes unchanged.");
                    }
                } else if assume_yes || !prompts::is_interactive() {
                    crate::output::info(
                        "Local push skipped because .hx queries failed validation.",
                    );
                    crate::output::info("Left local and cloud changes unchanged.");
                } else if confirm_sync_action(
                    false,
                    "Overwrite local files with cloud changes instead?",
                )? {
                    print_plan_for_direction(&diff, SyncDirection::Pull);
                    pull_remote_snapshot_into_local(project, &local_manifest, &remote_manifest)?;
                    changed = true;
                } else {
                    crate::output::info("Left local and cloud changes unchanged.");
                }
            }
            DivergenceAuthority::RemoteNewer => {
                if confirm_sync_action(
                    assume_yes,
                    "Cloud changes are newer. Pull cloud files to local?",
                )? {
                    print_plan_for_direction(&diff, SyncDirection::Pull);
                    pull_remote_snapshot_into_local(project, &local_manifest, &remote_manifest)?;
                    changed = true;
                } else {
                    crate::output::info("Left local and cloud changes unchanged.");
                }
            }
            DivergenceAuthority::TieOrUnknown => {
                let allow_push = match validate_local_hx_queries_for_push(project) {
                    Ok(()) => true,
                    Err(error) => {
                        print_warning(
                            "Local .hx queries failed validation, so pushing local files is unavailable.",
                        );
                        print_warning(&error.to_string());
                        false
                    }
                };

                match resolve_tie_action(assume_yes, allow_push)? {
                    TieResolutionAction::NoOp => {
                        crate::output::info("Left local and cloud changes unchanged.");
                    }
                    TieResolutionAction::Pull => {
                        print_plan_for_direction(&diff, SyncDirection::Pull);
                        pull_remote_snapshot_into_local(
                            project,
                            &local_manifest,
                            &remote_manifest,
                        )?;
                        changed = true;
                    }
                    TieResolutionAction::Push => {
                        print_plan_for_direction(&diff, SyncDirection::Push);
                        push_local_snapshot_to_cluster(project, cluster_id, cluster_name).await?;
                        changed = true;
                    }
                }
            }
        },
    }

    if changed {
        crate::output::success("Sync reconciliation applied.");
    }

    op.success();
    Ok(())
}

async fn reconcile_enterprise_cluster_snapshot(
    project: &ProjectContext,
    api_key: &str,
    cluster_id: &str,
    cluster_name: &str,
    assume_yes: bool,
) -> Result<()> {
    let op = Operation::new("Syncing", cluster_name);
    let client = reqwest::Client::new();

    let mut fetch_step = Step::with_messages(
        "Fetching enterprise cloud changes",
        "Enterprise cloud changes fetched",
    );
    fetch_step.start();
    let sync_response = match fetch_enterprise_sync_response_with_remote_empty_fallback(
        &client, api_key, cluster_id,
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

    let queries_dir = project.root.join(&project.config.project.queries);
    let local_manifest = collect_local_enterprise_manifest(&queries_dir)?;
    let remote_manifest = build_remote_enterprise_manifest(&sync_response);
    let comparison = compare_manifests(&local_manifest, &remote_manifest);

    let apply_pull = || -> Result<()> {
        pull_remote_enterprise_snapshot_into_local(project, &local_manifest, &remote_manifest)?;
        let query_bin_path = regenerate_enterprise_queries_bin(&queries_dir)?;
        Step::verbose_substep(&format!("  Regenerated {}", query_bin_path.display()));
        Ok(())
    };

    let mut changed = false;

    match comparison {
        SnapshotComparison::BothEmpty | SnapshotComparison::InSync => {
            crate::output::info("Local and enterprise cloud changes are already in sync.");
        }
        SnapshotComparison::LocalOnly => {
            if let Err(error) = validate_local_enterprise_queries_for_push(project) {
                op.failure();
                return Err(eyre!(
                    "enterprise query project failed validation. Fix errors before pushing to cloud.\n\n{}",
                    error
                ));
            }

            if confirm_sync_action(
                assume_yes,
                "your enterprise cluster has no source snapshot. Push your local query project to cloud now?",
            )? {
                let diff = compute_manifest_diff(&local_manifest, &remote_manifest);
                print_plan_for_direction(&diff, SyncDirection::Push);
                push_local_enterprise_snapshot_to_cluster(project, cluster_id, cluster_name)
                    .await?;
                changed = true;
            } else {
                crate::output::info("Left local and cloud changes unchanged.");
            }
        }
        SnapshotComparison::RemoteOnly => {
            if confirm_sync_action(
                assume_yes,
                "Local enterprise source is empty while cloud has files. Pull cloud files to local?",
            )? {
                let diff = compute_manifest_diff(&local_manifest, &remote_manifest);
                print_plan_for_direction(&diff, SyncDirection::Pull);
                apply_pull()?;
                changed = true;
            } else {
                crate::output::info("Left local and cloud changes unchanged.");
            }
        }
        SnapshotComparison::Diverged { authority, diff } => match authority {
            DivergenceAuthority::LocalNewer => {
                let push_allowed = match validate_local_enterprise_queries_for_push(project) {
                    Ok(()) => true,
                    Err(error) => {
                        print_warning(
                            "Local enterprise queries failed validation, so pushing local files is unavailable.",
                        );
                        print_warning(&error.to_string());
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
                        changed = true;
                    } else if confirm_sync_action(
                        false,
                        "Overwrite local enterprise files with cloud changes instead?",
                    )? {
                        print_plan_for_direction(&diff, SyncDirection::Pull);
                        apply_pull()?;
                        changed = true;
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
                    changed = true;
                } else {
                    crate::output::info("Left local and cloud changes unchanged.");
                }
            }
            DivergenceAuthority::RemoteNewer => {
                if confirm_sync_action(
                    assume_yes,
                    "Enterprise cloud changes are newer. Pull cloud files to local?",
                )? {
                    print_plan_for_direction(&diff, SyncDirection::Pull);
                    apply_pull()?;
                    changed = true;
                } else {
                    crate::output::info("Left local and cloud changes unchanged.");
                }
            }
            DivergenceAuthority::TieOrUnknown => {
                let allow_push = match validate_local_enterprise_queries_for_push(project) {
                    Ok(()) => true,
                    Err(error) => {
                        print_warning(
                            "Local enterprise queries failed validation, so pushing local files is unavailable.",
                        );
                        print_warning(&error.to_string());
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
                        changed = true;
                    }
                    TieResolutionAction::Push => {
                        print_plan_for_direction(&diff, SyncDirection::Push);
                        push_local_enterprise_snapshot_to_cluster(
                            project,
                            cluster_id,
                            cluster_name,
                        )
                        .await?;
                        changed = true;
                    }
                }
            }
        },
    }

    if changed {
        crate::output::success("Enterprise sync reconciliation applied.");
    }

    op.success();
    Ok(())
}

async fn fetch_workspaces(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
) -> Result<Vec<CliWorkspace>> {
    let workspaces: Vec<CliWorkspace> = client
        .get(format!("{}/api/cli/workspaces", base_url))
        .header("x-api-key", api_key)
        .send()
        .await
        .map_err(|e| eyre!("Failed to fetch workspaces: {}", e))?
        .error_for_status()
        .map_err(|e| eyre!("Failed to fetch workspaces: {}", e))?
        .json()
        .await
        .map_err(|e| eyre!("Failed to parse workspaces response: {}", e))?;

    Ok(workspaces)
}

async fn resolve_workspace_id(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    workspace_config: &mut WorkspaceConfig,
) -> Result<String> {
    let workspaces = fetch_workspaces(client, base_url, api_key).await?;

    if workspaces.is_empty() {
        return Err(eyre!(
            "No workspaces found. Create a workspace in the dashboard first."
        ));
    }

    if let Some(cached_workspace_id) = workspace_config.workspace_id.clone() {
        if workspaces.iter().any(|w| w.id == cached_workspace_id) {
            return Ok(cached_workspace_id);
        }

        print_warning(
            "Saved workspace selection is no longer available. Please select a workspace again.",
        );
        workspace_config.workspace_id = None;
        workspace_config.save()?;
    }

    let selected = prompts::select_workspace(&workspaces)?;
    workspace_config.workspace_id = Some(selected.clone());
    workspace_config.save()?;
    Ok(selected)
}

fn update_project_identity_in_helix_toml(
    project_root: &Path,
    new_project_name: &str,
    project_id: &str,
) -> Result<()> {
    let helix_toml_path = project_root.join("helix.toml");
    let mut config = HelixConfig::from_file(&helix_toml_path).map_err(|e| {
        eyre!(
            "Failed to load helix.toml for project identity update: {}",
            e
        )
    })?;

    config.project.name = new_project_name.to_string();
    config.project.id = Some(project_id.to_string());
    config
        .save_to_file(&helix_toml_path)
        .map_err(|e| eyre!("Failed to update project identity in helix.toml: {}", e))?;

    Ok(())
}

async fn resolve_or_create_project_for_sync(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    workspace_id: &str,
    project_name_from_toml: &str,
    project_id_from_toml: Option<&str>,
) -> Result<CliProject> {
    let projects: Vec<CliProject> = client
        .get(format!(
            "{}/api/cli/workspaces/{}/projects",
            base_url, workspace_id
        ))
        .header("x-api-key", api_key)
        .send()
        .await
        .map_err(|e| eyre!("Failed to fetch projects: {}", e))?
        .error_for_status()
        .map_err(|e| eyre!("Failed to fetch projects: {}", e))?
        .json()
        .await
        .map_err(|e| eyre!("Failed to parse projects response: {}", e))?;

    if let Some(project_id_hint) = project_id_from_toml
        && let Some(existing) = projects.iter().find(|p| p.id == project_id_hint).cloned()
    {
        crate::output::info(&format!(
            "Using project '{}' from your selected workspace.",
            existing.name
        ));
        return Ok(existing);
    }

    if let Some(existing) = projects
        .iter()
        .find(|p| p.name == project_name_from_toml)
        .cloned()
    {
        crate::output::info(&format!(
            "Using project '{}' from your selected workspace.",
            existing.name
        ));
        return Ok(existing);
    }

    match prompts::select_missing_project_choice(project_name_from_toml)? {
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
                .into_iter()
                .find(|p| p.id == selected_project_id)
                .ok_or_else(|| eyre!("Selected project was not found in response"))?;

            crate::output::info(&format!(
                "Using existing project '{}' from your selected workspace.",
                selected_project.name
            ));

            Ok(selected_project)
        }
        prompts::MissingProjectChoice::Create => {
            let chosen_name = prompts::input_project_name(project_name_from_toml)?;
            let created: CreateProjectResponse = client
                .post(format!(
                    "{}/api/cli/workspaces/{}/projects",
                    base_url, workspace_id
                ))
                .header("x-api-key", api_key)
                .header("Content-Type", "application/json")
                .json(&serde_json::json!({ "name": chosen_name }))
                .send()
                .await
                .map_err(|e| eyre!("Failed to create project: {}", e))?
                .error_for_status()
                .map_err(|e| eyre!("Failed to create project: {}", e))?
                .json()
                .await
                .map_err(|e| eyre!("Failed to parse create project response: {}", e))?;

            Ok(CliProject {
                id: created.id,
                name: chosen_name,
            })
        }
    }
}

async fn fetch_project_clusters(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    project_id: &str,
) -> Result<CliProjectClusters> {
    let project_clusters: CliProjectClusters = client
        .get(format!(
            "{}/api/cli/projects/{}/clusters",
            base_url, project_id
        ))
        .header("x-api-key", api_key)
        .send()
        .await
        .map_err(|e| eyre!("Failed to fetch project clusters: {}", e))?
        .error_for_status()
        .map_err(|e| eyre!("Failed to fetch project clusters: {}", e))?
        .json()
        .await
        .map_err(|e| eyre!("Failed to parse project clusters response: {}", e))?;

    Ok(project_clusters)
}

async fn fetch_cluster_project(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    cluster_id: &str,
) -> Result<CliClusterProject> {
    let cluster_project: CliClusterProject = client
        .get(format!(
            "{}/api/cli/clusters/{}/project",
            base_url, cluster_id
        ))
        .header("x-api-key", api_key)
        .send()
        .await
        .map_err(|e| eyre!("Failed to fetch cluster project: {}", e))?
        .error_for_status()
        .map_err(|e| eyre!("Failed to fetch cluster project: {}", e))?
        .json()
        .await
        .map_err(|e| eyre!("Failed to parse cluster project response: {}", e))?;

    Ok(cluster_project)
}

async fn fetch_enterprise_cluster_project(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    cluster_id: &str,
) -> Result<CliClusterProject> {
    let cluster_project: CliClusterProject = client
        .get(format!(
            "{}/api/cli/enterprise-clusters/{}/project",
            base_url, cluster_id
        ))
        .header("x-api-key", api_key)
        .send()
        .await
        .map_err(|e| eyre!("Failed to fetch enterprise cluster project: {}", e))?
        .error_for_status()
        .map_err(|e| eyre!("Failed to fetch enterprise cluster project: {}", e))?
        .json()
        .await
        .map_err(|e| eyre!("Failed to parse enterprise cluster project response: {}", e))?;

    Ok(cluster_project)
}

fn build_mode_from_cloud(value: &str) -> BuildMode {
    match value {
        "dev" => BuildMode::Dev,
        "release" => BuildMode::Release,
        _ => BuildMode::Release,
    }
}

fn availability_mode_from_cloud(value: &str) -> AvailabilityMode {
    match value {
        "ha" => AvailabilityMode::Ha,
        _ => AvailabilityMode::Dev,
    }
}

fn insert_unique_cloud_instance_name(
    cloud: &mut HashMap<String, CloudConfig>,
    preferred_name: &str,
    cluster_id: &str,
    config: CloudConfig,
) {
    let mut name = preferred_name.to_string();
    if cloud.contains_key(&name) {
        let suffix = cluster_id.chars().take(8).collect::<String>();
        name = format!("{}-{}", preferred_name, suffix);
    }
    cloud.insert(name, config);
}

fn insert_unique_enterprise_instance_name(
    enterprise: &mut HashMap<String, EnterpriseInstanceConfig>,
    preferred_name: &str,
    cluster_id: &str,
    config: EnterpriseInstanceConfig,
) {
    let mut name = preferred_name.to_string();
    if enterprise.contains_key(&name) {
        let suffix = cluster_id.chars().take(8).collect::<String>();
        name = format!("{}-{}", preferred_name, suffix);
    }
    enterprise.insert(name, config);
}

fn reconcile_project_config_from_cloud(
    project: &ProjectContext,
    project_clusters: &CliProjectClusters,
) -> Result<()> {
    let helix_toml_path = project.root.join("helix.toml");
    let mut config = HelixConfig::from_file(&helix_toml_path)
        .map_err(|e| eyre!("Failed to load helix.toml: {}", e))?;

    config.project.name = project_clusters.project_name.clone();
    config.project.id = Some(project_clusters.project_id.clone());
    // Remove only Helix-managed cloud entries; preserve FlyIo, Ecr
    config
        .cloud
        .retain(|_name, entry| !matches!(entry, CloudConfig::Helix(_)));
    config.enterprise.clear();

    for cluster in &project_clusters.standard {
        let instance_config = CloudInstanceConfig {
            cluster_id: cluster.cluster_id.clone(),
            region: Some("us-east-1".to_string()),
            build_mode: build_mode_from_cloud(&cluster.build_mode),
            env_vars: HashMap::new(),
            db_config: DbConfig::default(),
        };

        insert_unique_cloud_instance_name(
            &mut config.cloud,
            &cluster.cluster_name,
            &cluster.cluster_id,
            CloudConfig::Helix(instance_config),
        );
    }

    for cluster in &project_clusters.enterprise {
        let instance_config = EnterpriseInstanceConfig {
            cluster_id: cluster.cluster_id.clone(),
            availability_mode: availability_mode_from_cloud(&cluster.availability_mode),
            gateway_node_type: cluster.gateway_node_type.clone(),
            db_node_type: cluster.db_node_type.clone(),
            min_instances: cluster.min_instances,
            max_instances: cluster.max_instances,
            db_config: DbConfig::default(),
        };

        insert_unique_enterprise_instance_name(
            &mut config.enterprise,
            &cluster.cluster_name,
            &cluster.cluster_id,
            instance_config,
        );
    }

    config
        .save_to_file(&helix_toml_path)
        .map_err(|e| eyre!("Failed to write helix.toml: {}", e))?;

    Ok(())
}

async fn sync_cluster_into_project(
    api_key: &str,
    cluster_id: &str,
    cluster_name: &str,
    project: &ProjectContext,
    assume_yes: bool,
) -> Result<()> {
    reconcile_standard_cluster_snapshot(project, api_key, cluster_id, cluster_name, assume_yes)
        .await
}

async fn run_project_sync_flow(project: &ProjectContext, assume_yes: bool) -> Result<()> {
    prompts::intro(
        "helix sync",
        Some(&format!(
            "Using project '{}' from helix.toml. Select a cluster to sync from.",
            project.config.project.name
        )),
    )?;

    let credentials = require_auth().await?;
    let client = reqwest::Client::new();
    let base_url = cloud_base_url();

    let mut workspace_config = WorkspaceConfig::load()?;
    let workspace_id = resolve_workspace_id(
        &client,
        &base_url,
        &credentials.helix_admin_key,
        &mut workspace_config,
    )
    .await?;

    let resolved_project = resolve_or_create_project_for_sync(
        &client,
        &base_url,
        &credentials.helix_admin_key,
        &workspace_id,
        &project.config.project.name,
        project.config.project.id.as_deref(),
    )
    .await?;

    if project.config.project.name != resolved_project.name
        || project.config.project.id.as_deref() != Some(resolved_project.id.as_str())
    {
        update_project_identity_in_helix_toml(
            &project.root,
            &resolved_project.name,
            &resolved_project.id,
        )?;
        crate::output::info(&format!(
            "Updated helix.toml project identity to '{}' ({})",
            resolved_project.name, resolved_project.id
        ));
    }

    let project_clusters = fetch_project_clusters(
        &client,
        &base_url,
        &credentials.helix_admin_key,
        &resolved_project.id,
    )
    .await?;

    if project_clusters.standard.is_empty() && project_clusters.enterprise.is_empty() {
        return Err(eyre!(
            "No clusters found in project '{}'. Create and deploy a cluster first.",
            resolved_project.name
        ));
    }

    reconcile_project_config_from_cloud(project, &project_clusters)?;
    crate::output::info(
        "Updated helix.toml with canonical project and cluster metadata from Helix Cloud.",
    );

    let standard_items: Vec<(String, String, String)> = project_clusters
        .standard
        .iter()
        .map(|cluster| {
            (
                cluster.cluster_id.clone(),
                cluster.cluster_name.clone(),
                project_clusters.project_name.clone(),
            )
        })
        .collect();

    let enterprise_items: Vec<(String, String, String)> = project_clusters
        .enterprise
        .iter()
        .map(|cluster| {
            (
                cluster.cluster_id.clone(),
                cluster.cluster_name.clone(),
                project_clusters.project_name.clone(),
            )
        })
        .collect();

    let (cluster_id, is_enterprise) =
        prompts::select_cluster_from_workspace(&standard_items, &enterprise_items)?;

    if is_enterprise {
        let cluster_name = project_clusters
            .enterprise
            .iter()
            .find(|cluster| cluster.cluster_id == cluster_id)
            .map(|cluster| cluster.cluster_name.as_str())
            .unwrap_or(cluster_id.as_str());

        reconcile_enterprise_cluster_snapshot(
            project,
            &credentials.helix_admin_key,
            &cluster_id,
            cluster_name,
            assume_yes,
        )
        .await?;
    } else {
        let cluster_name = project_clusters
            .standard
            .iter()
            .find(|cluster| cluster.cluster_id == cluster_id)
            .map(|cluster| cluster.cluster_name.as_str())
            .unwrap_or(cluster_id.as_str());

        sync_cluster_into_project(
            &credentials.helix_admin_key,
            &cluster_id,
            cluster_name,
            project,
            assume_yes,
        )
        .await?;
    }

    Ok(())
}

pub async fn run(instance_name: Option<String>, assume_yes: bool) -> Result<()> {
    // Try to load project context
    let project = ProjectContext::find_and_load(None).ok();

    if let Some(instance_name) = instance_name {
        let project = project.ok_or_else(|| {
            eyre!("No helix.toml found. Run 'helix init' to create a project first.")
        })?;

        let instance_config = project.config.get_instance(&instance_name)?;
        if instance_config.is_local() {
            return pull_from_local_instance(&project, &instance_name).await;
        }

        return pull_from_cloud_instance(&project, &instance_name, instance_config, assume_yes)
            .await;
    }

    if !prompts::is_interactive() {
        return Err(eyre!(
            "No instance specified. Run 'helix sync <instance>' or run interactively in a project directory."
        ));
    }

    if let Some(ref project) = project {
        run_project_sync_flow(project, assume_yes).await
    } else {
        run_workspace_sync_flow().await
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
    let base_url = cloud_base_url();

    // Load or prompt for workspace
    let mut workspace_config = WorkspaceConfig::load()?;

    let workspace_id = resolve_workspace_id(
        &client,
        &base_url,
        &credentials.helix_admin_key,
        &mut workspace_config,
    )
    .await?;

    // Fetch clusters for workspace (both standard and enterprise)
    let workspace_clusters: CliWorkspaceClusters = client
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

    if workspace_clusters.standard.is_empty() && workspace_clusters.enterprise.is_empty() {
        return Err(eyre!(
            "No clusters found in this workspace. Deploy a cluster first with 'helix push'."
        ));
    }

    // Build prompt data
    let standard_items: Vec<(String, String, String)> = workspace_clusters
        .standard
        .iter()
        .map(|c| {
            (
                c.cluster_id.clone(),
                c.cluster_name.clone(),
                c.project_name.clone(),
            )
        })
        .collect();
    let enterprise_items: Vec<(String, String, String)> = workspace_clusters
        .enterprise
        .iter()
        .map(|c| {
            (
                c.cluster_id.clone(),
                c.cluster_name.clone(),
                c.project_name.clone(),
            )
        })
        .collect();

    let (cluster_id, is_enterprise) =
        prompts::select_cluster_from_workspace(&standard_items, &enterprise_items)?;

    if is_enterprise {
        // Enterprise sync
        sync_enterprise_from_cluster_id(&credentials.helix_admin_key, &cluster_id).await
    } else {
        // Standard sync
        sync_from_cluster_id(&credentials.helix_admin_key, &cluster_id).await
    }
}

/// Sync directly from a cluster ID without a project context.
async fn sync_from_cluster_id(api_key: &str, cluster_id: &str) -> Result<()> {
    let op = Operation::new("Syncing", cluster_id);

    let client = reqwest::Client::new();

    let mut sync_step = Step::with_messages("Fetching source files", "Source files fetched");
    sync_step.start();

    let sync_response =
        match fetch_sync_response_with_remote_empty_fallback(&client, api_key, cluster_id).await {
            Ok(resp) => resp,
            Err(e) => {
                sync_step.fail();
                op.failure();
                return Err(e);
            }
        };

    sync_step.done();

    // Write files to current directory
    let cwd = std::env::current_dir()?;
    let remote_config = sync_response
        .helix_toml
        .as_deref()
        .and_then(|remote_toml| parse_and_sanitize_remote_config(remote_toml, "cluster sync"));
    let queries_dir = resolve_remote_queries_dir(&cwd, remote_config.as_ref());

    if !queries_dir.exists() {
        std::fs::create_dir_all(&queries_dir)?;
    }

    let mut write_step = Step::with_messages("Writing source files", "Source files written");
    write_step.start();

    let mut files_written = 0;
    for (filename, content) in &sync_response.hx_files {
        let file_path = safe_join_relative(&queries_dir, filename)?;
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

    if let Some(remote_config) = remote_config.as_ref()
        && let Some(remote_toml) = serialize_remote_config(remote_config, "cluster sync")
    {
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

/// Sync enterprise source files from a cluster by ID (no project context).
async fn sync_enterprise_from_cluster_id(api_key: &str, cluster_id: &str) -> Result<()> {
    let op = Operation::new("Syncing", cluster_id);

    let client = reqwest::Client::new();

    let mut sync_step = Step::with_messages(
        "Fetching enterprise source files",
        "Enterprise source files fetched",
    );
    sync_step.start();

    let sync_response = match fetch_enterprise_sync_response_with_remote_empty_fallback(
        &client, api_key, cluster_id,
    )
    .await
    {
        Ok(resp) => resp,
        Err(e) => {
            sync_step.fail();
            op.failure();
            return Err(e);
        }
    };

    sync_step.done();

    let cwd = std::env::current_dir()?;
    let remote_config = sync_response.helix_toml.as_deref().and_then(|remote_toml| {
        parse_and_sanitize_remote_config(remote_toml, "enterprise cluster sync")
    });
    let queries_dir = resolve_remote_queries_dir(&cwd, remote_config.as_ref());

    if !queries_dir.exists() {
        std::fs::create_dir_all(&queries_dir)?;
    }

    let local_manifest = collect_local_enterprise_manifest(&queries_dir)?;
    let remote_manifest = build_remote_enterprise_manifest(&sync_response);

    let mut write_step = Step::with_messages(
        "Writing enterprise source files",
        "Enterprise source files written",
    );
    write_step.start();

    let mut files_written = 0;
    for (filename, entry) in &remote_manifest {
        let file_path = safe_join_relative(&queries_dir, filename)?;
        if let Some(parent) = file_path.parent()
            && !parent.exists()
        {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&file_path, &entry.content)
            .map_err(|e| eyre!("Failed to write {}: {}", filename, e))?;
        files_written += 1;
        Step::verbose_substep(&format!("  Wrote {}", filename));
    }

    for local_only_path in local_manifest
        .keys()
        .filter(|path| !remote_manifest.contains_key(*path))
    {
        let local_path = safe_join_relative(&queries_dir, local_only_path)?;
        if local_path.exists() {
            std::fs::remove_file(&local_path)
                .map_err(|e| eyre!("Failed to remove {}: {}", local_only_path, e))?;
            Step::verbose_substep(&format!("  Removed {}", local_only_path));
        }
    }

    if let Some(remote_config) = remote_config.as_ref()
        && let Some(remote_toml) = serialize_remote_config(remote_config, "enterprise cluster sync")
    {
        let helix_toml_path = cwd.join("helix.toml");
        std::fs::write(&helix_toml_path, remote_toml)
            .map_err(|e| eyre!("Failed to write helix.toml: {}", e))?;
        files_written += 1;
        Step::verbose_substep("  Wrote helix.toml");
    }

    if queries_dir.join("Cargo.toml").exists() {
        let generated = regenerate_enterprise_queries_bin(&queries_dir)?;
        Step::verbose_substep(&format!("  Regenerated {}", generated.display()));
    }

    write_step.done_with_info(&format!("{} files", files_written));
    op.success();

    crate::output::info(&format!(
        "Synced {} source files from enterprise cluster '{}'",
        files_written, cluster_id
    ));

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
    assume_yes: bool,
) -> Result<()> {
    // Handle enterprise instances separately
    if let InstanceInfo::Enterprise(config) = &instance_config {
        return pull_from_enterprise_instance(project, instance_name, config, assume_yes).await;
    }

    // Verify this is a Helix Cloud instance
    let cluster_id = match &instance_config {
        InstanceInfo::Helix(config) => &config.cluster_id,
        InstanceInfo::FlyIo(_) => {
            return Err(eyre!(
                "Sync is only supported for Helix Cloud instances, not Fly.io deployments"
            ));
        }
        InstanceInfo::Ecr(_) => {
            return Err(eyre!(
                "Sync is only supported for Helix Cloud instances, not ECR deployments"
            ));
        }
        InstanceInfo::Local(_) | InstanceInfo::Enterprise(_) => {
            return Err(eyre!("Sync is only supported for cloud instances"));
        }
    };

    // Check auth
    let credentials = require_auth().await?;

    Step::verbose_substep(&format!("Reconciling against cluster: {cluster_id}"));

    reconcile_standard_cluster_snapshot(
        project,
        &credentials.helix_admin_key,
        cluster_id,
        instance_name,
        assume_yes,
    )
    .await?;

    let client = reqwest::Client::new();

    let cluster_project = fetch_cluster_project(
        &client,
        &cloud_base_url(),
        &credentials.helix_admin_key,
        cluster_id,
    )
    .await?;
    let project_clusters = fetch_project_clusters(
        &client,
        &cloud_base_url(),
        &credentials.helix_admin_key,
        &cluster_project.project_id,
    )
    .await?;

    reconcile_project_config_from_cloud(project, &project_clusters)?;
    Step::verbose_substep("  Wrote helix.toml (canonical cloud metadata)");

    Ok(())
}

async fn pull_from_enterprise_instance(
    project: &ProjectContext,
    instance_name: &str,
    config: &crate::config::EnterpriseInstanceConfig,
    assume_yes: bool,
) -> Result<()> {
    let credentials = require_auth().await?;
    reconcile_enterprise_cluster_snapshot(
        project,
        &credentials.helix_admin_key,
        &config.cluster_id,
        instance_name,
        assume_yes,
    )
    .await?;

    let client = reqwest::Client::new();
    let cluster_project = fetch_enterprise_cluster_project(
        &client,
        &cloud_base_url(),
        &credentials.helix_admin_key,
        &config.cluster_id,
    )
    .await?;
    let project_clusters = fetch_project_clusters(
        &client,
        &cloud_base_url(),
        &credentials.helix_admin_key,
        &cluster_project.project_id,
    )
    .await?;

    reconcile_project_config_from_cloud(project, &project_clusters)?;
    Step::verbose_substep("  Wrote helix.toml (canonical cloud metadata)");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest_entry(hash: &str, last_modified_ms: Option<i64>) -> ManifestEntry {
        ManifestEntry {
            sha256: hash.to_string(),
            last_modified_ms,
            content: String::new(),
        }
    }

    #[test]
    fn compare_manifests_both_empty() {
        let local = HashMap::new();
        let remote = HashMap::new();
        assert!(matches!(
            compare_manifests(&local, &remote),
            SnapshotComparison::BothEmpty
        ));
    }

    #[test]
    fn compare_manifests_local_only() {
        let mut local = HashMap::new();
        local.insert("schema.hx".to_string(), manifest_entry("a", Some(100)));
        let remote = HashMap::new();

        assert!(matches!(
            compare_manifests(&local, &remote),
            SnapshotComparison::LocalOnly
        ));
    }

    #[test]
    fn compare_manifests_remote_only() {
        let local = HashMap::new();
        let mut remote = HashMap::new();
        remote.insert("schema.hx".to_string(), manifest_entry("a", Some(100)));

        assert!(matches!(
            compare_manifests(&local, &remote),
            SnapshotComparison::RemoteOnly
        ));
    }

    #[test]
    fn compare_manifests_in_sync_when_hashes_match() {
        let mut local = HashMap::new();
        local.insert("schema.hx".to_string(), manifest_entry("same", Some(1000)));

        let mut remote = HashMap::new();
        remote.insert("schema.hx".to_string(), manifest_entry("same", Some(2000)));

        assert!(matches!(
            compare_manifests(&local, &remote),
            SnapshotComparison::InSync
        ));
    }

    #[test]
    fn compare_manifests_prefers_local_when_local_is_newer() {
        let mut local = HashMap::new();
        local.insert(
            "schema.hx".to_string(),
            manifest_entry("local", Some(10_000)),
        );

        let mut remote = HashMap::new();
        remote.insert(
            "schema.hx".to_string(),
            manifest_entry("remote", Some(1_000)),
        );

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
    fn compare_manifests_prefers_remote_when_remote_is_newer() {
        let mut local = HashMap::new();
        local.insert(
            "schema.hx".to_string(),
            manifest_entry("local", Some(1_000)),
        );

        let mut remote = HashMap::new();
        remote.insert(
            "schema.hx".to_string(),
            manifest_entry("remote", Some(10_000)),
        );

        let comparison = compare_manifests(&local, &remote);
        assert!(matches!(
            comparison,
            SnapshotComparison::Diverged {
                authority: DivergenceAuthority::RemoteNewer,
                ..
            }
        ));
    }

    #[test]
    fn compare_manifests_uses_tie_safety_window() {
        let mut local = HashMap::new();
        local.insert(
            "schema.hx".to_string(),
            manifest_entry("local", Some(10_000)),
        );

        let mut remote = HashMap::new();
        remote.insert(
            "schema.hx".to_string(),
            manifest_entry("remote", Some(10_000 + CLOCK_SKEW_WINDOW_MS - 1)),
        );

        let comparison = compare_manifests(&local, &remote);
        assert!(matches!(
            comparison,
            SnapshotComparison::Diverged {
                authority: DivergenceAuthority::TieOrUnknown,
                ..
            }
        ));
    }

    #[test]
    fn build_sync_action_plan_for_pull_maps_to_local_file_operations() {
        let diff = ManifestDiff {
            local_only: vec!["local-only.hx".to_string()],
            remote_only: vec!["remote-only.hx".to_string()],
            changed: vec!["changed.hx".to_string()],
        };

        let plan = build_sync_action_plan(&diff, SyncDirection::Pull);

        assert_eq!(plan.to_create, vec!["remote-only.hx".to_string()]);
        assert_eq!(plan.to_change, vec!["changed.hx".to_string()]);
        assert_eq!(plan.to_delete, vec!["local-only.hx".to_string()]);
    }

    #[test]
    fn build_sync_action_plan_for_push_maps_to_cloud_file_operations() {
        let diff = ManifestDiff {
            local_only: vec!["local-only.hx".to_string()],
            remote_only: vec!["remote-only.hx".to_string()],
            changed: vec!["changed.hx".to_string()],
        };

        let plan = build_sync_action_plan(&diff, SyncDirection::Push);

        assert_eq!(plan.to_create, vec!["local-only.hx".to_string()]);
        assert_eq!(plan.to_change, vec!["changed.hx".to_string()]);
        assert_eq!(plan.to_delete, vec!["remote-only.hx".to_string()]);
    }

    #[test]
    fn sanitize_relative_path_normalizes_curdir_components() {
        let sanitized = sanitize_relative_path(Path::new("./src/./main.rs"))
            .expect("valid relative path should sanitize");
        assert_eq!(sanitized, PathBuf::from("src/main.rs"));
    }

    #[test]
    fn sanitize_relative_path_rejects_parent_and_absolute_paths() {
        assert!(sanitize_relative_path(Path::new("../escape.rs")).is_err());
        assert!(sanitize_relative_path(Path::new("/etc/passwd")).is_err());
    }

    #[test]
    fn enterprise_source_allowlist_filters_non_project_files() {
        assert!(should_include_enterprise_source_file(Path::new("Cargo.toml")));
        assert!(should_include_enterprise_source_file(Path::new("src/main.rs")));
        assert!(should_include_enterprise_source_file(Path::new(
            ".cargo/config.toml"
        )));

        assert!(!should_include_enterprise_source_file(Path::new("queries.bin")));
        assert!(!should_include_enterprise_source_file(Path::new("README.md")));
        assert!(!should_include_enterprise_source_file(Path::new("target/tmp.rs")));
    }

    #[test]
    fn build_remote_enterprise_manifest_normalizes_paths_and_uses_metadata() {
        let mut source_files = HashMap::new();
        source_files.insert(
            "Cargo.toml".to_string(),
            "[package]\nname = \"queries\"\n".to_string(),
        );
        source_files.insert("src/main.rs".to_string(), "fn main() {}\n".to_string());
        source_files.insert(
            "src\\nested\\query.rs".to_string(),
            "pub fn query() {}\n".to_string(),
        );
        source_files.insert("queries.bin".to_string(), "ignore".to_string());
        source_files.insert("../escape.rs".to_string(), "ignore".to_string());
        source_files.insert("README.md".to_string(), "ignore".to_string());

        let mut file_metadata = HashMap::new();
        file_metadata.insert(
            "src/main.rs".to_string(),
            SyncFileMetadata {
                sha256: Some("remote-sha".to_string()),
                last_modified_ms: Some(42),
            },
        );

        let response = EnterpriseSyncResponse {
            source_files,
            file_metadata,
            helix_toml: None,
        };

        let manifest = build_remote_enterprise_manifest(&response);
        assert_eq!(manifest.len(), 3);
        assert!(manifest.contains_key("Cargo.toml"));
        assert!(manifest.contains_key("src/main.rs"));
        assert!(manifest.contains_key("src/nested/query.rs"));
        assert!(!manifest.contains_key("queries.bin"));
        assert!(!manifest.contains_key("README.md"));
        assert!(!manifest.contains_key("../escape.rs"));

        assert_eq!(manifest["src/main.rs"].sha256, "remote-sha");
        assert_eq!(manifest["src/main.rs"].last_modified_ms, Some(42));
        assert_eq!(
            manifest["Cargo.toml"].sha256,
            compute_sha256("[package]\nname = \"queries\"\n")
        );
    }
}
