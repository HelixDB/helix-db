use eyre::{Result, eyre};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::commands::integrations::ecr::EcrConfig;
use crate::commands::integrations::fly::FlyInstanceConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelixConfig {
    pub project: ProjectConfig,
    #[serde(default)]
    pub local: HashMap<String, LocalInstanceConfig>,
    #[serde(default)]
    pub cloud: HashMap<String, CloudConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectConfig {
    pub name: String,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbConfig {
    #[serde(default, skip_serializing_if = "is_default_vector_config")]
    pub vector_config: VectorConfig,
    #[serde(default, skip_serializing_if = "is_default_graph_config")]
    pub graph_config: GraphConfig,
    #[serde(default = "default_true", skip_serializing_if = "is_true")]
    pub mcp: bool,
    #[serde(default = "default_true", skip_serializing_if = "is_true")]
    pub bm25: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalInstanceConfig {
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default = "default_debug_build_mode")]
    pub build_mode: BuildMode,
    #[serde(flatten)]
    pub db_config: DbConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudInstanceConfig {
    pub cluster_id: String,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default = "default_release_build_mode")]
    pub build_mode: BuildMode,
    #[serde(flatten)]
    pub db_config: DbConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CloudConfig {
    HelixCloud(CloudInstanceConfig),
    FlyIo(FlyInstanceConfig),
    Ecr(EcrConfig),
}

impl CloudConfig {
    pub fn get_cluster_id(&self) -> &str {
        match self {
            CloudConfig::HelixCloud(config) => &config.cluster_id,
            CloudConfig::FlyIo(config) => &config.cluster_id,
            CloudConfig::Ecr(_) => "ecr", // ECR doesn't use cluster_id
        }
    }
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum BuildMode {
    #[default]
    Debug,
    Release,
}

pub fn default_debug_build_mode() -> BuildMode {
    BuildMode::Debug
}

pub fn default_release_build_mode() -> BuildMode {
    BuildMode::Release
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
        VectorConfig {
            m: default_m(),
            ef_construction: default_ef_construction(),
            ef_search: default_ef_search(),
            db_max_size_gb: default_db_max_size_gb(),
        }
    }
}

impl Default for DbConfig {
    fn default() -> Self {
        DbConfig {
            vector_config: VectorConfig::default(),
            graph_config: GraphConfig::default(),
            mcp: true,
            bm25: true,
        }
    }
}

#[derive(Debug, Clone)]
pub enum InstanceInfo<'a> {
    Local(&'a LocalInstanceConfig),
    HelixCloud(&'a CloudInstanceConfig),
    FlyIo(&'a FlyInstanceConfig),
    Ecr(&'a EcrConfig),
}

impl<'a> InstanceInfo<'a> {
    pub fn build_mode(&self) -> BuildMode {
        match self {
            InstanceInfo::Local(config) => config.build_mode,
            InstanceInfo::HelixCloud(config) => config.build_mode,
            InstanceInfo::FlyIo(config) => config.build_mode,
            InstanceInfo::Ecr(config) => config.build_mode,
        }
    }

    pub fn port(&self) -> Option<u16> {
        match self {
            InstanceInfo::Local(config) => config.port,
            InstanceInfo::HelixCloud(_) => None,
            InstanceInfo::FlyIo(_) => None,
            InstanceInfo::Ecr(_) => None,
        }
    }

    pub fn cluster_id(&self) -> Option<&str> {
        match self {
            InstanceInfo::Local(_) => None,
            InstanceInfo::HelixCloud(config) => Some(&config.cluster_id),
            InstanceInfo::FlyIo(config) => Some(&config.cluster_id),
            InstanceInfo::Ecr(_) => None, // ECR doesn't use cluster_id
        }
    }

    pub fn db_config(&self) -> &DbConfig {
        match self {
            InstanceInfo::Local(config) => &config.db_config,
            InstanceInfo::HelixCloud(config) => &config.db_config,
            InstanceInfo::FlyIo(config) => &config.db_config,
            InstanceInfo::Ecr(config) => &config.db_config,
        }
    }

    pub fn is_local(&self) -> bool {
        matches!(self, InstanceInfo::Local(_))
    }

    pub fn should_build_docker_image(&self) -> bool {
        matches!(self, InstanceInfo::Local(_)) || matches!(self, InstanceInfo::FlyIo(_))
    }

    pub fn docker_build_target(&self) -> Option<&str> {
        match self {
            InstanceInfo::Local(_) => None,
            InstanceInfo::HelixCloud(_) => None,
            InstanceInfo::FlyIo(_) => Some("linux/amd64"),
            InstanceInfo::Ecr(_) => Some("linux/amd64"),
        }
    }

    /// Convert instance config to the legacy config.hx.json format
    pub fn to_legacy_json(&self) -> serde_json::Value {
        let db_config = self.db_config();

        serde_json::json!({
            "vector_config": {
                "m": db_config.vector_config.m,
                "ef_construction": db_config.vector_config.ef_construction,
                "ef_search": db_config.vector_config.ef_search,
                "db_max_size": db_config.vector_config.db_max_size_gb
            },
            "graph_config": {
                "secondary_indices": db_config.graph_config.secondary_indices
            },
            "db_max_size_gb": db_config.vector_config.db_max_size_gb,
            "mcp": db_config.mcp,
            "bm25": db_config.bm25
        })
    }
}

impl HelixConfig {
    pub fn from_file(path: &Path) -> Result<Self> {
        let content =
            fs::read_to_string(path).map_err(|e| eyre!("Failed to read helix.toml: {}", e))?;

        let config: HelixConfig =
            toml::from_str(&content).map_err(|e| eyre!("Failed to parse helix.toml: {}", e))?;

        config.validate()?;
        Ok(config)
    }

    pub fn save_to_file(&self, path: &Path) -> Result<()> {
        let content = toml::to_string_pretty(self)
            .map_err(|e| eyre!("Failed to serialize helix.toml: {}", e))?;

        fs::write(path, content).map_err(|e| eyre!("Failed to write helix.toml: {}", e))?;

        Ok(())
    }

    fn validate(&self) -> Result<()> {
        // Validate project config
        if self.project.name.is_empty() {
            return Err(eyre!("Project name cannot be empty"));
        }

        // Validate instances
        if self.local.is_empty() && self.cloud.is_empty() {
            return Err(eyre!("At least one instance must be defined"));
        }

        // Validate local instances
        for name in self.local.keys() {
            if name.is_empty() {
                return Err(eyre!("Instance name cannot be empty"));
            }
        }

        // Validate cloud instances
        for (name, cloud_config) in &self.cloud {
            if name.is_empty() {
                return Err(eyre!("Instance name cannot be empty"));
            }
            if cloud_config.get_cluster_id().is_empty() {
                return Err(eyre!(
                    "Cloud instance '{}' must have a non-empty cluster_id",
                    name
                ));
            }
        }

        Ok(())
    }

    pub fn get_instance(&self, name: &str) -> Result<InstanceInfo> {
        if let Some(local_config) = self.local.get(name) {
            return Ok(InstanceInfo::Local(local_config));
        }

        if let Some(cloud_config) = self.cloud.get(name) {
            match cloud_config {
                CloudConfig::HelixCloud(config) => {
                    return Ok(InstanceInfo::HelixCloud(config));
                }
                CloudConfig::FlyIo(config) => {
                    return Ok(InstanceInfo::FlyIo(config));
                }
                CloudConfig::Ecr(config) => {
                    return Ok(InstanceInfo::Ecr(config));
                }
            }
        }

        Err(eyre!("Instance '{}' not found in helix.toml", name))
    }

    pub fn list_instances(&self) -> Vec<&String> {
        let mut instances = Vec::new();
        instances.extend(self.local.keys());
        instances.extend(self.cloud.keys());
        instances
    }

    pub fn default_config(project_name: &str) -> Self {
        let mut local = HashMap::new();
        local.insert(
            "dev".to_string(),
            LocalInstanceConfig {
                port: Some(6969),
                build_mode: BuildMode::Debug,
                db_config: DbConfig::default(),
            },
        );

        HelixConfig {
            project: ProjectConfig {
                name: project_name.to_string(),
            },
            local,
            cloud: HashMap::new(),
        }
    }
}
