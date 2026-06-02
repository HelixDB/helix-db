use crate::errors::ConfigError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

pub const DEFAULT_LOCAL_PORT: u16 = 6969;
pub const DEFAULT_ENTERPRISE_DEV_IMAGE: &str = "ghcr.io/helixdb/enterprise-dev";
pub const DEFAULT_ENTERPRISE_DEV_TAG: &str = "latest";
pub const DEFAULT_QUERY_AUTH_HEADER: &str = "Authorization";
pub const DEFAULT_QUERY_AUTH_ENV: &str = "HELIX_API_KEY";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkspaceConfig {
    pub workspace_id: Option<String>,
}

impl WorkspaceConfig {
    pub fn config_path() -> Result<PathBuf, ConfigError> {
        let home = dirs::home_dir().ok_or(ConfigError::HomeDirNotFound)?;
        Ok(home.join(".helix").join("config"))
    }

    pub fn load() -> Result<Self, ConfigError> {
        let path = Self::config_path()?;
        if !path.exists() {
            return Ok(Self::default());
        }

        let content =
            fs::read_to_string(&path).map_err(|source| ConfigError::ReadWorkspaceConfig {
                path: path.clone(),
                source,
            })?;

        toml::from_str(&content)
            .map_err(|source| ConfigError::ParseWorkspaceConfig { path, source })
    }

    pub fn save(&self) -> Result<(), ConfigError> {
        let path = Self::config_path()?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| ConfigError::CreateWorkspaceDir {
                path: parent.to_path_buf(),
                source,
            })?;
        }

        let content = toml::to_string_pretty(self)
            .map_err(|source| ConfigError::SerializeWorkspaceConfig { source })?;
        fs::write(&path, content)
            .map_err(|source| ConfigError::WriteWorkspaceConfig { path, source })?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelixConfig {
    pub project: ProjectConfig,
    #[serde(default)]
    pub local: HashMap<String, LocalInstanceConfig>,
    #[serde(default)]
    pub enterprise: HashMap<String, EnterpriseInstanceConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    pub name: String,
    #[serde(default = "default_queries_path")]
    pub queries: PathBuf,
    #[serde(default = "default_container_runtime")]
    pub container_runtime: ContainerRuntime,
}

fn default_queries_path() -> PathBuf {
    PathBuf::from("db")
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ContainerRuntime {
    #[default]
    Docker,
    Podman,
    Native,
}

impl ContainerRuntime {
    pub const fn binary(&self) -> &'static str {
        match self {
            Self::Docker => "docker",
            Self::Podman => "podman",
            Self::Native => "native",
        }
    }

    pub const fn label(&self) -> &'static str {
        match self {
            Self::Docker => "Docker",
            Self::Podman => "Podman",
            Self::Native => "Native",
        }
    }

    pub const fn is_native(&self) -> bool {
        matches!(self, Self::Native)
    }
}

fn default_container_runtime() -> ContainerRuntime {
    ContainerRuntime::Docker
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalInstanceConfig {
    #[serde(default = "default_local_port")]
    pub port: u16,
    #[serde(default = "default_enterprise_dev_image")]
    pub image: String,
    #[serde(default = "default_enterprise_dev_tag")]
    pub tag: String,
    #[serde(default, skip_serializing_if = "is_default_local_storage")]
    pub storage: LocalStorageMode,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LocalStorageMode {
    #[default]
    Memory,
    Disk,
}

impl LocalStorageMode {
    pub const fn from_disk_flag(disk: bool) -> Self {
        if disk { Self::Disk } else { Self::Memory }
    }

    pub const fn is_disk(&self) -> bool {
        matches!(self, Self::Disk)
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Disk => "disk",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VectorConfig {
    #[serde(default = "default_m")]
    pub m: u32,
    #[serde(default = "default_ef_construction")]
    pub ef_construction: u32,
    #[serde(default = "default_ef_search")]
    pub ef_search: u32,
    #[serde(default = "default_db_max_size_gb")]
    pub db_max_size_gb: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct GraphConfig {
    #[serde(default)]
    pub secondary_indices: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DbConfig {
    #[serde(default, skip_serializing_if = "is_default_vector_config")]
    pub vector_config: VectorConfig,
    #[serde(default, skip_serializing_if = "is_default_graph_config")]
    pub graph_config: GraphConfig,
    #[serde(default = "default_true", skip_serializing_if = "is_true")]
    pub mcp: bool,
    #[serde(default = "default_true", skip_serializing_if = "is_true")]
    pub bm25: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(
        default = "default_embedding_model",
        skip_serializing_if = "is_default_embedding_model"
    )]
    pub embedding_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graphvis_node_label: Option<String>,
}

fn default_true() -> bool {
    true
}

fn default_m() -> u32 {
    16
}

fn default_ef_construction() -> u32 {
    128
}

fn default_ef_search() -> u32 {
    768
}

fn default_db_max_size_gb() -> u32 {
    20
}

fn default_embedding_model() -> Option<String> {
    Some("text-embedding-ada-002".to_string())
}

fn is_default_embedding_model(value: &Option<String>) -> bool {
    *value == default_embedding_model()
}

fn is_true(value: &bool) -> bool {
    *value
}

fn is_default_vector_config(value: &VectorConfig) -> bool {
    *value == VectorConfig::default()
}

fn is_default_graph_config(value: &GraphConfig) -> bool {
    *value == GraphConfig::default()
}

impl Default for VectorConfig {
    fn default() -> Self {
        Self {
            m: default_m(),
            ef_construction: default_ef_construction(),
            ef_search: default_ef_search(),
            db_max_size_gb: default_db_max_size_gb(),
        }
    }
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            vector_config: VectorConfig::default(),
            graph_config: GraphConfig::default(),
            mcp: true,
            bm25: true,
            schema: None,
            embedding_model: default_embedding_model(),
            graphvis_node_label: None,
        }
    }
}

fn default_local_port() -> u16 {
    DEFAULT_LOCAL_PORT
}

fn default_enterprise_dev_image() -> String {
    DEFAULT_ENTERPRISE_DEV_IMAGE.to_string()
}

fn default_enterprise_dev_tag() -> String {
    DEFAULT_ENTERPRISE_DEV_TAG.to_string()
}

fn is_default_local_storage(value: &LocalStorageMode) -> bool {
    *value == LocalStorageMode::Memory
}

impl Default for LocalInstanceConfig {
    fn default() -> Self {
        Self {
            port: DEFAULT_LOCAL_PORT,
            image: DEFAULT_ENTERPRISE_DEV_IMAGE.to_string(),
            tag: DEFAULT_ENTERPRISE_DEV_TAG.to_string(),
            storage: LocalStorageMode::Memory,
        }
    }
}

impl LocalInstanceConfig {
    pub fn image_ref(&self) -> String {
        format!("{}:{}", self.image, self.tag)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnterpriseInstanceConfig {
    pub cluster_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gateway_url: Option<String>,
    #[serde(default = "default_query_auth_header")]
    pub query_auth_header: String,
    #[serde(default = "default_query_auth_env")]
    pub query_auth_env: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub availability_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gateway_node_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub db_node_type: Option<String>,
    #[serde(default = "default_min_instances")]
    pub min_instances: u64,
    #[serde(default = "default_min_instances")]
    pub max_instances: u64,
    #[serde(flatten)]
    pub db_config: DbConfig,
}

fn default_min_instances() -> u64 {
    1
}

fn default_query_auth_header() -> String {
    DEFAULT_QUERY_AUTH_HEADER.to_string()
}

fn default_query_auth_env() -> String {
    DEFAULT_QUERY_AUTH_ENV.to_string()
}

#[derive(Debug, Clone, Copy)]
pub enum InstanceInfo<'a> {
    Local(&'a LocalInstanceConfig),
    Enterprise(&'a EnterpriseInstanceConfig),
}

impl InstanceInfo<'_> {
    pub fn is_local(&self) -> bool {
        matches!(self, Self::Local(_))
    }

    pub fn cluster_id(&self) -> Option<&str> {
        match self {
            Self::Local(_) => None,
            Self::Enterprise(config) => Some(&config.cluster_id),
        }
    }
}

impl HelixConfig {
    pub fn from_file(path: &Path) -> Result<Self, ConfigError> {
        let content = fs::read_to_string(path).map_err(|source| ConfigError::ReadHelixConfig {
            path: path.to_path_buf(),
            source,
        })?;

        let config: HelixConfig =
            toml::from_str(&content).map_err(|source| ConfigError::ParseHelixConfig {
                path: path.to_path_buf(),
                source,
            })?;

        config.validate(path)?;
        Ok(config)
    }

    pub fn save_to_file(&self, path: &Path) -> Result<(), ConfigError> {
        let content = toml::to_string_pretty(self)
            .map_err(|source| ConfigError::SerializeHelixConfig { source })?;
        fs::write(path, content).map_err(|source| ConfigError::WriteHelixConfig {
            path: path.to_path_buf(),
            source,
        })?;
        Ok(())
    }

    fn validate(&self, path: &Path) -> Result<(), ConfigError> {
        let relative_path = std::env::current_dir()
            .ok()
            .and_then(|cwd| path.strip_prefix(&cwd).ok())
            .map(Path::to_path_buf)
            .unwrap_or_else(|| path.to_path_buf());

        if self.project.name.trim().is_empty() {
            return Err(ConfigError::EmptyProjectName {
                path: relative_path,
            });
        }

        if self.local.is_empty() && self.enterprise.is_empty() {
            return Err(ConfigError::MissingInstances {
                path: relative_path,
            });
        }

        for name in self.local.keys().chain(self.enterprise.keys()) {
            if name.trim().is_empty() {
                return Err(ConfigError::EmptyInstanceName {
                    path: relative_path.clone(),
                });
            }
        }

        for (name, config) in &self.enterprise {
            if config.cluster_id.trim().is_empty() {
                return Err(ConfigError::MissingClusterId {
                    name: name.clone(),
                    path: relative_path.clone(),
                });
            }
        }

        Ok(())
    }

    pub fn get_instance(&self, name: &str) -> Result<InstanceInfo<'_>, ConfigError> {
        if let Some(config) = self.local.get(name) {
            return Ok(InstanceInfo::Local(config));
        }

        if let Some(config) = self.enterprise.get(name) {
            return Ok(InstanceInfo::Enterprise(config));
        }

        Err(ConfigError::InstanceNotFound {
            name: name.to_string(),
        })
    }

    pub fn list_instances(&self) -> Vec<&String> {
        let mut instances = Vec::new();
        instances.extend(self.local.keys());
        instances.extend(self.enterprise.keys());
        instances.sort();
        instances
    }

    pub fn list_instances_with_types(&self) -> Vec<(&String, &'static str)> {
        let mut instances = Vec::new();
        for name in self.local.keys() {
            instances.push((name, "local"));
        }
        for name in self.enterprise.keys() {
            instances.push((name, "Enterprise"));
        }
        instances.sort_by(|a, b| a.0.cmp(b.0));
        instances
    }

    pub fn default_config(project_name: &str) -> Self {
        let mut local = HashMap::new();
        local.insert("dev".to_string(), LocalInstanceConfig::default());

        Self {
            project: ProjectConfig {
                id: None,
                workspace_id: None,
                name: project_name.to_string(),
                queries: default_queries_path(),
                container_runtime: ContainerRuntime::Docker,
            },
            local,
            enterprise: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn old_enterprise_config_defaults_queries_and_runtime_fields() {
        let config: HelixConfig = toml::from_str(
            r#"
[project]
name = "demo"

[enterprise.production]
cluster_id = "cluster-123"
availability_mode = "ha"
gateway_node_type = "GW-40"
db_node_type = "HLX-160"
min_instances = 2
max_instances = 4
"#,
        )
        .expect("old enterprise config should deserialize");

        assert_eq!(config.project.queries, PathBuf::from("db"));
        let enterprise = config.enterprise.get("production").unwrap();
        assert_eq!(enterprise.availability_mode.as_deref(), Some("ha"));
        assert_eq!(enterprise.gateway_node_type.as_deref(), Some("GW-40"));
        assert_eq!(enterprise.db_node_type.as_deref(), Some("HLX-160"));
        assert_eq!(enterprise.min_instances, 2);
        assert_eq!(enterprise.max_instances, 4);
        assert_eq!(enterprise.db_config.vector_config.db_max_size_gb, 20);
    }

    #[test]
    fn old_local_config_defaults_to_memory_storage() {
        let config: HelixConfig = toml::from_str(
            r#"
[project]
name = "demo"

[local.dev]
port = 8080
image = "ghcr.io/helixdb/enterprise-dev"
tag = "latest"
"#,
        )
        .expect("old local config should deserialize");

        let local = config.local.get("dev").unwrap();
        assert_eq!(local.storage, LocalStorageMode::Memory);
    }

    #[test]
    fn local_config_can_use_disk_storage() {
        let config: HelixConfig = toml::from_str(
            r#"
[project]
name = "demo"

[local.dev]
storage = "disk"
"#,
        )
        .expect("disk local config should deserialize");

        let local = config.local.get("dev").unwrap();
        assert_eq!(local.storage, LocalStorageMode::Disk);
    }
}
