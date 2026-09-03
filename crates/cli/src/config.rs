use crate::errors::ConfigError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::{fmt, str::FromStr};

pub const DEFAULT_LOCAL_PORT: u16 = 6969;
pub const DEFAULT_LOCAL_IMAGE: &str = "ghcr.io/helixdb/helixdb";
pub const DEFAULT_LOCAL_IMAGE_TAG: &str = "v0.0.4";
pub const DEFAULT_S3_REGION: &str = "us-east-1";
pub const DEFAULT_S3_PREFIX: &str = "db/";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HelixConfig {
    pub project: ProjectConfig,
    #[serde(default)]
    pub local: HashMap<String, LocalInstanceConfig>,
    #[serde(default)]
    pub enterprise: HashMap<String, EnterpriseInstanceConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
}

impl ContainerRuntime {
    pub const fn binary(&self) -> &'static str {
        match self {
            Self::Docker => "docker",
            Self::Podman => "podman",
        }
    }

    pub const fn label(&self) -> &'static str {
        match self {
            Self::Docker => "Docker",
            Self::Podman => "Podman",
        }
    }
}

fn default_container_runtime() -> ContainerRuntime {
    ContainerRuntime::Docker
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalInstanceConfig {
    #[serde(default = "default_local_port")]
    pub port: u16,
    #[serde(default = "default_local_image")]
    pub image: String,
    #[serde(default = "default_local_image_tag")]
    pub tag: String,
    #[serde(default, skip_serializing_if = "is_default_local_storage")]
    pub storage: LocalStorageMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3: Option<S3StorageConfig>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LocalStorageMode {
    #[default]
    Memory,
    Disk,
    S3,
}

impl LocalStorageMode {
    pub const fn from_disk_flag(disk: bool) -> Self {
        if disk {
            Self::Disk
        } else {
            Self::Memory
        }
    }

    pub const fn is_disk(&self) -> bool {
        matches!(self, Self::Disk)
    }

    pub const fn is_s3(&self) -> bool {
        matches!(self, Self::S3)
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Disk => "disk",
            Self::S3 => "s3",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct S3StorageConfig {
    pub bucket: String,
    #[serde(default = "default_s3_prefix")]
    pub prefix: String,
    #[serde(default = "default_s3_region")]
    pub region: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub allow_http: bool,
}

impl S3StorageConfig {
    pub fn from_uri(
        uri: &str,
        region: Option<String>,
        endpoint_url: Option<String>,
        allow_http: bool,
    ) -> Result<Self, String> {
        let rest = uri
            .strip_prefix("s3://")
            .ok_or_else(|| "S3 storage URI must start with s3://".to_string())?;
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        let bucket = bucket.trim();
        if bucket.is_empty() {
            return Err("S3 storage URI must include a bucket name".to_string());
        }
        if let Some(endpoint_url) = endpoint_url.as_deref() {
            validate_s3_endpoint_url(endpoint_url)?;
        }

        Ok(Self {
            bucket: bucket.to_string(),
            prefix: normalize_s3_prefix(prefix),
            region: region.unwrap_or_else(default_s3_region),
            endpoint_url,
            allow_http,
        })
    }

    pub fn apply_overrides(&mut self, args: &crate::S3StorageArgs) -> Result<(), String> {
        if let Some(uri) = &args.storage_uri {
            let replacement = Self::from_uri(
                uri,
                Some(self.region.clone()),
                self.endpoint_url.clone(),
                self.allow_http,
            )?;
            self.bucket = replacement.bucket;
            self.prefix = replacement.prefix;
        }
        if let Some(region) = &args.s3_region {
            self.region = region.clone();
        }
        if let Some(endpoint_url) = &args.s3_endpoint_url {
            validate_s3_endpoint_url(endpoint_url)?;
            self.endpoint_url = Some(endpoint_url.clone());
        }
        if args.s3_allow_http {
            self.allow_http = true;
        }
        Ok(())
    }

    pub fn normalized_prefix(&self) -> String {
        normalize_s3_prefix(&self.prefix)
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

fn default_local_image() -> String {
    DEFAULT_LOCAL_IMAGE.to_string()
}

fn default_local_image_tag() -> String {
    DEFAULT_LOCAL_IMAGE_TAG.to_string()
}

fn default_s3_region() -> String {
    DEFAULT_S3_REGION.to_string()
}

fn default_s3_prefix() -> String {
    DEFAULT_S3_PREFIX.to_string()
}

fn normalize_s3_prefix(prefix: &str) -> String {
    let prefix = prefix.trim_start_matches('/');
    if prefix.is_empty() {
        return default_s3_prefix();
    }
    if prefix.ends_with('/') {
        prefix.to_string()
    } else {
        format!("{prefix}/")
    }
}

fn validate_s3_endpoint_url(endpoint_url: &str) -> Result<(), String> {
    if endpoint_url.starts_with("http://") || endpoint_url.starts_with("https://") {
        return Ok(());
    }
    Err("S3 endpoint URL must start with http:// or https://".to_string())
}

fn is_default_local_storage(value: &LocalStorageMode) -> bool {
    *value == LocalStorageMode::Memory
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl Default for LocalInstanceConfig {
    fn default() -> Self {
        Self {
            port: DEFAULT_LOCAL_PORT,
            image: DEFAULT_LOCAL_IMAGE.to_string(),
            tag: DEFAULT_LOCAL_IMAGE_TAG.to_string(),
            storage: LocalStorageMode::Memory,
            s3: None,
        }
    }
}

impl LocalInstanceConfig {
    pub fn image_ref(&self) -> String {
        format!("{}:{}", self.image, self.tag)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseReference {
    Cluster(String),
    Tenant(String),
}

impl DatabaseReference {
    pub fn id(&self) -> &str {
        match self {
            Self::Cluster(id) | Self::Tenant(id) => id,
        }
    }

    pub fn query_request(&self) -> serde_json::Value {
        match self {
            Self::Cluster(id) => serde_json::json!({"clusterId": id}),
            Self::Tenant(id) => serde_json::json!({"tenantId": id}),
        }
    }
}

impl fmt::Display for DatabaseReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cluster(id) => write!(formatter, "cluster:{id}"),
            Self::Tenant(id) => write!(formatter, "tenant:{id}"),
        }
    }
}

impl FromStr for DatabaseReference {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (kind, id) = value
            .split_once(':')
            .ok_or_else(|| "database must be cluster:<id> or tenant:<id>".to_owned())?;
        if id.is_empty()
            || id.len() > 128
            || !id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err("database ID is invalid".to_owned());
        }
        match kind {
            "cluster" => Ok(Self::Cluster(id.to_owned())),
            "tenant" => Ok(Self::Tenant(id.to_owned())),
            _ => Err("database must be cluster:<id> or tenant:<id>".to_owned()),
        }
    }
}

impl Serialize for DatabaseReference {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for DatabaseReference {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnterpriseInstanceConfig {
    pub database: DatabaseReference,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
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

    pub fn database(&self) -> Option<&DatabaseReference> {
        match self {
            Self::Local(_) => None,
            Self::Enterprise(config) => Some(&config.database),
        }
    }
}

impl HelixConfig {
    pub fn from_file(path: &Path) -> Result<Self, ConfigError> {
        Self::from_file_inner(path, true)
    }

    /// Like [`from_file`](Self::from_file), but tolerates a `helix.toml` that defines zero
    /// instances. Used by `helix add`, whose whole job is to add the first instance back —
    /// it would otherwise be locked out by the "at least one instance" check.
    pub fn from_file_allow_no_instances(path: &Path) -> Result<Self, ConfigError> {
        Self::from_file_inner(path, false)
    }

    fn from_file_inner(path: &Path, require_instances: bool) -> Result<Self, ConfigError> {
        let content = fs::read_to_string(path).map_err(|source| ConfigError::ReadHelixConfig {
            path: path.to_path_buf(),
            source,
        })?;

        let config: HelixConfig =
            toml::from_str(&content).map_err(|source| ConfigError::ParseHelixConfig {
                path: path.to_path_buf(),
                source,
            })?;

        config.validate(path, require_instances)?;
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

    fn validate(&self, path: &Path, require_instances: bool) -> Result<(), ConfigError> {
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

        if require_instances && self.local.is_empty() && self.enterprise.is_empty() {
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
            if !name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
            {
                return Err(ConfigError::InvalidInstanceName {
                    name: name.clone(),
                    path: relative_path.clone(),
                });
            }
        }

        for (name, config) in &self.local {
            match (&config.storage, &config.s3) {
                (LocalStorageMode::S3, None) => {
                    return Err(ConfigError::MissingS3Config {
                        name: name.clone(),
                        path: relative_path.clone(),
                    });
                }
                (LocalStorageMode::S3, Some(s3)) => {
                    if s3.bucket.trim().is_empty() {
                        return Err(ConfigError::MissingS3Bucket {
                            name: name.clone(),
                            path: relative_path.clone(),
                        });
                    }
                    if s3.prefix.trim().is_empty() {
                        return Err(ConfigError::MissingS3Prefix {
                            name: name.clone(),
                            path: relative_path.clone(),
                        });
                    }
                    if s3.region.trim().is_empty() {
                        return Err(ConfigError::MissingS3Region {
                            name: name.clone(),
                            path: relative_path.clone(),
                        });
                    }
                    if let Some(endpoint_url) = &s3.endpoint_url {
                        validate_s3_endpoint_url(endpoint_url).map_err(|message| {
                            ConfigError::InvalidS3Endpoint {
                                name: name.clone(),
                                path: relative_path.clone(),
                                message,
                            }
                        })?;
                    }
                }
                (_, Some(_)) => {
                    return Err(ConfigError::UnexpectedS3Config {
                        name: name.clone(),
                        path: relative_path.clone(),
                    });
                }
                _ => {}
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
    fn obsolete_enterprise_cloud_fields_are_rejected() {
        let error = toml::from_str::<HelixConfig>(
            r#"
[project]
name = "demo"

[enterprise.production]
cluster_id = "cluster-123"
"#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("unknown field"));

        let valid: HelixConfig = toml::from_str(
            r#"
[project]
name = "demo"

[enterprise.production]
database = "cluster:cluster-123"
"#,
        )
        .unwrap();
        assert_eq!(valid.project.queries, PathBuf::from("db"));
        assert_eq!(
            valid.enterprise["production"].database,
            DatabaseReference::Cluster("cluster-123".into())
        );

        let obsolete_query_auth = format!(
            "\n[project]\nname = \"demo\"\n{}_{} = \"{}_{}\"\n\n[local.dev]\n",
            "query", "auth_env", "HELIX", "API_KEY"
        );
        for obsolete in [
            obsolete_query_auth.as_str(),
            r#"
[project]
name = "demo"

[sync]
snapshot = "obsolete"

[local.dev]
"#,
        ] {
            assert!(toml::from_str::<HelixConfig>(obsolete).is_err());
        }
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
    fn local_config_defaults_to_published_standalone_image() {
        let config = LocalInstanceConfig::default();

        assert_eq!(config.image_ref(), "ghcr.io/helixdb/helixdb:v0.0.4");
    }

    #[test]
    fn zero_instance_config_rejected_by_default_but_allowed_leniently() {
        let config: HelixConfig = toml::from_str(
            r#"
[project]
name = "demo"
"#,
        )
        .expect("config with no instances should still deserialize");

        let path = Path::new("helix.toml");
        // Default validation (used by every command except `add`) rejects it.
        assert!(matches!(
            config.validate(path, true),
            Err(ConfigError::MissingInstances { .. })
        ));
        // Lenient validation (used by `helix add`) accepts it so the first
        // instance can be re-added after the last one was deleted.
        assert!(config.validate(path, false).is_ok());
    }

    #[test]
    fn instance_name_with_path_traversal_characters_is_rejected() {
        // Instance names are joined onto `.helix/` to build each instance's state
        // directory (see `ProjectContext::instance_workspace`), which `prune`/`delete`
        // then recursively remove. A name containing `..` or `/` must never reach
        // that join unsanitized, so it's rejected here at config-validation time
        // instead of at every path-building call site.
        let config: HelixConfig = toml::from_str(
            r#"
[project]
name = "demo"

[local."../../evil"]
"#,
        )
        .expect("TOML allows arbitrary quoted table keys");

        let path = Path::new("helix.toml");
        assert!(matches!(
            config.validate(path, true),
            Err(ConfigError::InvalidInstanceName { .. })
        ));
    }

    #[test]
    fn lenient_validation_still_enforces_other_checks() {
        let path = Path::new("helix.toml");

        // Empty project name is rejected even leniently.
        let empty_name: HelixConfig = toml::from_str(
            r#"
[project]
name = "  "
"#,
        )
        .unwrap();
        assert!(matches!(
            empty_name.validate(path, false),
            Err(ConfigError::EmptyProjectName { .. })
        ));

        // Invalid database references are rejected during deserialization.
        let no_database = toml::from_str::<HelixConfig>(
            r#"
[project]
name = "demo"

[enterprise.production]
database = "cluster:"
"#,
        );
        assert!(no_database.is_err());
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

    #[test]
    fn s3_uri_normalizes_bucket_prefix_and_defaults() {
        let s3 = S3StorageConfig::from_uri("s3://bucket/path/to/db", None, None, false).unwrap();

        assert_eq!(s3.bucket, "bucket");
        assert_eq!(s3.prefix, "path/to/db/");
        assert_eq!(s3.region, DEFAULT_S3_REGION);
        assert_eq!(s3.endpoint_url, None);
        assert!(!s3.allow_http);
    }

    #[test]
    fn local_config_can_use_s3_storage() {
        let config: HelixConfig = toml::from_str(
            r#"
[project]
name = "demo"

[local.dev]
storage = "s3"

[local.dev.s3]
bucket = "my-bucket"
prefix = "my-prefix/"
region = "eu-west-2"
endpoint_url = "https://s3.example.com"
"#,
        )
        .expect("s3 local config should deserialize");

        config
            .validate(Path::new("helix.toml"), true)
            .expect("s3 local config should validate");
        let local = config.local.get("dev").unwrap();
        assert_eq!(local.storage, LocalStorageMode::S3);
        let s3 = local.s3.as_ref().unwrap();
        assert_eq!(s3.bucket, "my-bucket");
        assert_eq!(s3.prefix, "my-prefix/");
        assert_eq!(s3.region, "eu-west-2");
        assert_eq!(s3.endpoint_url.as_deref(), Some("https://s3.example.com"));
    }

    #[test]
    fn s3_storage_requires_s3_config_table() {
        let config: HelixConfig = toml::from_str(
            r#"
[project]
name = "demo"

[local.dev]
storage = "s3"
"#,
        )
        .unwrap();

        assert!(matches!(
            config.validate(Path::new("helix.toml"), true),
            Err(ConfigError::MissingS3Config { .. })
        ));
    }

    #[test]
    fn s3_config_requires_s3_storage_mode() {
        let config: HelixConfig = toml::from_str(
            r#"
[project]
name = "demo"

[local.dev]

[local.dev.s3]
bucket = "my-bucket"
"#,
        )
        .unwrap();

        assert!(matches!(
            config.validate(Path::new("helix.toml"), true),
            Err(ConfigError::UnexpectedS3Config { .. })
        ));
    }

    #[test]
    fn s3_config_rejects_invalid_endpoint_url() {
        let config: HelixConfig = toml::from_str(
            r#"
[project]
name = "demo"

[local.dev]
storage = "s3"

[local.dev.s3]
bucket = "my-bucket"
endpoint_url = "localhost:9000"
"#,
        )
        .unwrap();

        assert!(matches!(
            config.validate(Path::new("helix.toml"), true),
            Err(ConfigError::InvalidS3Endpoint { .. })
        ));
    }
}
