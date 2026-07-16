use crate::commands::auth::require_auth;
use crate::config::{EnterpriseInstanceConfig, HelixConfig};
use crate::enterprise_cloud::cloud_base_url;
use crate::external_tools::{self, ExternalTool};
use crate::output;
use crate::project::ProjectContext;
use base64::prelude::{Engine as _, BASE64_STANDARD};
use eyre::{eyre, Result};
use serde_json::json;
use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};

const ENTERPRISE_SOURCE_MAX_FILES: usize = 2_000;
const ENTERPRISE_SOURCE_MAX_BYTES: usize = 20 * 1024 * 1024;
const ENTERPRISE_DEPLOY_REQUEST_MAX_BYTES: usize = 20 * 1024 * 1024;

pub(crate) async fn deploy_enterprise_by_cluster_id(
    project: &ProjectContext,
    cluster_id: &str,
    cluster_name_hint: &str,
) -> Result<()> {
    let Some((instance_name, config)) = project
        .config
        .enterprise
        .iter()
        .find(|(_, config)| config.cluster_id == cluster_id)
    else {
        return Err(eyre!(
            "Enterprise cluster '{}' is not configured in helix.toml. Run 'helix sync' to refresh cluster metadata, then retry syncing cluster '{}'.",
            cluster_id,
            cluster_name_hint
        ));
    };

    deploy_enterprise(project, instance_name, config).await
}

pub(crate) async fn deploy_enterprise(
    project: &ProjectContext,
    instance_name: &str,
    config: &EnterpriseInstanceConfig,
) -> Result<()> {
    let credentials = require_auth().await?;
    let queries_project_dir = enterprise_queries_dir(project);
    let query_json_path = compile_enterprise_queries(&queries_project_dir)?;
    let query_json_bytes = std::fs::read(&query_json_path).map_err(|e| {
        eyre!(
            "Failed to read generated queries.json ({}): {e}",
            query_json_path.display()
        )
    })?;

    if query_json_bytes.is_empty() {
        return Err(eyre!(
            "Generated queries.json is empty ({})",
            query_json_path.display()
        ));
    }

    let source_files = collect_enterprise_source_files(&queries_project_dir)?;
    if source_files.is_empty() {
        return Err(eyre!(
            "No source files found in enterprise queries project: {}",
            queries_project_dir.display()
        ));
    }

    let helix_toml_content = pruned_enterprise_config(project, instance_name, config)
        .and_then(|config| toml::to_string_pretty(&config).ok());
    let payload = json!({
        "queries_json_b64": BASE64_STANDARD.encode(&query_json_bytes),
        "queries_json_size_bytes": query_json_bytes.len(),
        "source_files": source_files,
        "instance_name": instance_name,
        "helix_toml": helix_toml_content,
    });
    let payload_bytes = serde_json::to_vec(&payload)
        .map_err(|e| eyre!("Failed to serialize enterprise deploy payload: {e}"))?;

    if payload_bytes.len() > ENTERPRISE_DEPLOY_REQUEST_MAX_BYTES {
        return Err(eyre!(
            "Enterprise deploy payload exceeds size limit ({} bytes > {} bytes). Trim your queries.json or source snapshot before deploy.",
            payload_bytes.len(),
            ENTERPRISE_DEPLOY_REQUEST_MAX_BYTES
        ));
    }

    let deploy_url = format!(
        "{}/api/cli/enterprise-clusters/{}/deploy",
        cloud_base_url(),
        config.cluster_id
    );
    let response = reqwest::Client::new()
        .post(&deploy_url)
        .header("x-api-key", &credentials.helix_admin_key)
        .header("Content-Type", "application/json")
        .body(payload_bytes)
        .send()
        .await
        .map_err(|e| eyre!("Enterprise deployment request failed: {e}"))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(eyre!("Enterprise deployment failed ({status}): {body}"));
    }

    let response_payload: serde_json::Value = response
        .json()
        .await
        .map_err(|e| eyre!("Failed to parse enterprise deploy response: {e}"))?;
    if let Some(s3_key) = response_payload
        .get("s3_key")
        .and_then(|value| value.as_str())
    {
        output::info(&format!("Uploaded queries.json to {s3_key}"));
    }

    output::success("Enterprise cluster deployed successfully");
    Ok(())
}

pub(crate) fn enterprise_queries_dir(project: &ProjectContext) -> PathBuf {
    project
        .root
        .join(&project.config.project.queries)
        .canonicalize()
        .unwrap_or_else(|_| project.root.join(&project.config.project.queries))
}

pub(crate) fn compile_enterprise_queries(queries_project_dir: &Path) -> Result<PathBuf> {
    let manifest_path = queries_project_dir.join("Cargo.toml");
    if !manifest_path.exists() {
        return Err(eyre!(
            "Enterprise queries project manifest not found: {}",
            manifest_path.display()
        ));
    }

    output::info("Compiling enterprise query project...");
    let compile_output = external_tools::command(ExternalTool::Cargo)
        .arg("run")
        .arg("--manifest-path")
        .arg(&manifest_path)
        .current_dir(queries_project_dir)
        .output()
        .map_err(|e| eyre!("Failed to run cargo for enterprise queries: {e}"))?;

    if !compile_output.status.success() {
        let stderr = String::from_utf8_lossy(&compile_output.stderr);
        let stdout = String::from_utf8_lossy(&compile_output.stdout);
        return Err(eyre!(
            "Enterprise query project compilation failed:\n{}\n{}",
            stderr,
            stdout
        ));
    }

    let query_json_path = queries_project_dir.join("queries.json");
    if !query_json_path.exists() {
        return Err(eyre!(
            "Enterprise query project did not generate queries.json at {}",
            query_json_path.display()
        ));
    }

    let metadata = std::fs::metadata(&query_json_path)
        .map_err(|e| eyre!("Failed to read queries.json metadata: {e}"))?;
    if metadata.len() == 0 {
        return Err(eyre!(
            "Generated queries.json is empty ({})",
            query_json_path.display()
        ));
    }

    Ok(query_json_path)
}

fn pruned_enterprise_config(
    project: &ProjectContext,
    instance_name: &str,
    config: &EnterpriseInstanceConfig,
) -> Option<HelixConfig> {
    let mut enterprise = HashMap::new();
    enterprise.insert(instance_name.to_string(), config.clone());

    Some(HelixConfig {
        project: project.config.project.clone(),
        local: HashMap::new(),
        enterprise,
    })
}

pub(crate) fn should_descend_enterprise_source_dir(relative_path: &Path) -> bool {
    for component in relative_path.components() {
        if let Component::Normal(part) = component
            && (part == "target" || part == ".git")
        {
            return false;
        }
    }

    true
}

pub(crate) fn should_include_enterprise_source_file(relative_path: &Path) -> bool {
    if relative_path.as_os_str().is_empty() {
        return false;
    }

    let normalized = relative_path.to_string_lossy().replace('\\', "/");
    if normalized == "queries.json" {
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

pub(crate) fn collect_enterprise_source_files(
    queries_project_dir: &Path,
) -> Result<HashMap<String, String>> {
    fn walk(dir: &Path, root: &Path, files: &mut HashMap<String, String>) -> Result<()> {
        for entry in std::fs::read_dir(dir)
            .map_err(|e| eyre!("Failed to read directory {}: {}", dir.display(), e))?
        {
            let entry = entry.map_err(|e| eyre!("Failed to read directory entry: {e}"))?;
            let path = entry.path();
            let relative = path.strip_prefix(root).map_err(|_| {
                eyre!(
                    "Failed to compute relative path for source file {}",
                    path.display()
                )
            })?;

            if path.is_dir() {
                if should_descend_enterprise_source_dir(relative) {
                    walk(&path, root, files)?;
                }
                continue;
            }

            if !should_include_enterprise_source_file(relative) {
                continue;
            }

            let normalized_relative = relative.to_string_lossy().replace('\\', "/");
            match std::fs::read_to_string(&path) {
                Ok(content) => {
                    files.insert(normalized_relative, content);
                    if files.len() > ENTERPRISE_SOURCE_MAX_FILES {
                        return Err(eyre!(
                            "Enterprise source snapshot exceeds file limit ({} files). Trim your query project before deploy.",
                            ENTERPRISE_SOURCE_MAX_FILES
                        ));
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
                    output::verbose(&format!(
                        "Skipping non-utf8 source file during enterprise deploy snapshot: {}",
                        path.display()
                    ));
                }
                Err(e) => {
                    return Err(eyre!("Failed to read source file {}: {e}", path.display()));
                }
            }
        }

        Ok(())
    }

    let mut files = HashMap::new();
    walk(queries_project_dir, queries_project_dir, &mut files)?;

    let total_bytes: usize = files.values().map(|content| content.len()).sum();
    if total_bytes > ENTERPRISE_SOURCE_MAX_BYTES {
        return Err(eyre!(
            "Enterprise source snapshot exceeds size limit ({} bytes > {} bytes). Trim your query project before deploy.",
            total_bytes,
            ENTERPRISE_SOURCE_MAX_BYTES
        ));
    }

    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn include_rules_allow_only_expected_enterprise_project_files() {
        assert!(should_include_enterprise_source_file(Path::new(
            "Cargo.toml"
        )));
        assert!(should_include_enterprise_source_file(Path::new(
            "Cargo.lock"
        )));
        assert!(should_include_enterprise_source_file(Path::new("build.rs")));
        assert!(should_include_enterprise_source_file(Path::new(
            "src/main.rs"
        )));
        assert!(should_include_enterprise_source_file(Path::new(
            ".cargo/config.toml"
        )));

        assert!(!should_include_enterprise_source_file(Path::new(
            "queries.json"
        )));
        assert!(!should_include_enterprise_source_file(Path::new(
            "README.md"
        )));
        assert!(!should_include_enterprise_source_file(Path::new(
            "target/debug/main"
        )));
        assert!(!should_include_enterprise_source_file(Path::new(
            ".git/config"
        )));
    }
}
