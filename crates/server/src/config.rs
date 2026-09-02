use std::env;
use std::net::{AddrParseError, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use db::{DbConfig, HelixDbSource};
use object_store::aws::AmazonS3Builder;

/// Runtime configuration for the standalone server.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// HTTP listener address.
    pub http_addr: SocketAddr,
    /// gRPC listener address.
    pub grpc_addr: SocketAddr,
    /// Logical DB path inside the selected object store.
    pub db_path: String,
    /// Storage backend.
    pub storage: StorageConfig,
    /// Optional S3-compatible object store dedicated to WAL objects.
    pub wal_storage: Option<WalStorageConfig>,
}

impl ServerConfig {
    /// Load server configuration from environment variables.
    pub fn from_env() -> Result<Self, ServerConfigError> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    fn from_lookup(
        mut lookup: impl FnMut(&str) -> Option<String>,
    ) -> Result<Self, ServerConfigError> {
        let http_addr = parse_addr(
            lookup("HELIX_HTTP_ADDR")
                .or_else(|| lookup("HTTP_ADDR"))
                .unwrap_or_else(|| "0.0.0.0:8080".to_string()),
        )?;
        let grpc_addr = parse_addr(
            lookup("HELIX_GRPC_ADDR")
                .or_else(|| lookup("GRPC_ADDR"))
                .unwrap_or_else(|| "0.0.0.0:8081".to_string()),
        )?;
        let db_path = lookup("DB_PATH").unwrap_or_else(|| "db/".to_string());
        let storage = StorageConfig::from_lookup(&mut lookup)?;
        let wal_storage = WalStorageConfig::from_lookup(&storage, &mut lookup)?;

        Ok(Self {
            http_addr,
            grpc_addr,
            db_path,
            storage,
            wal_storage,
        })
    }

    /// Build the DB crate storage source.
    pub fn db_source(&self) -> HelixDbSource {
        match &self.storage {
            StorageConfig::Memory => HelixDbSource::InMemory {
                database: self.db_path.clone(),
            },
            StorageConfig::Disk { root } => HelixDbSource::Disk {
                root: root.clone(),
                database: self.db_path.clone(),
            },
            StorageConfig::S3 {
                bucket,
                region,
                endpoint,
                allow_http,
            } => HelixDbSource::ObjectStorage {
                database: self.db_path.clone(),
                bucket: bucket.clone(),
                region: region.clone(),
                endpoint: endpoint.clone(),
                allow_http: *allow_http,
            },
        }
    }

    /// Build DB runtime configuration, including the optional WAL object store.
    pub fn db_config(&self) -> Result<DbConfig, ServerConfigError> {
        let Some(wal_storage) = &self.wal_storage else {
            return Ok(DbConfig::new());
        };
        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(&wal_storage.bucket)
            .with_region(&wal_storage.region)
            .with_allow_http(wal_storage.allow_http);
        builder = wal_storage
            .endpoint
            .iter()
            .fold(builder, |builder, endpoint| builder.with_endpoint(endpoint));
        let wal_object_store: Arc<dyn object_store::ObjectStore> = Arc::new(
            builder
                .build()
                .map_err(|source| ServerConfigError::WalObjectStore { source })?,
        );
        Ok(DbConfig::new().with_wal_object_store(wal_object_store))
    }
}

/// Resolved S3-compatible storage settings for WAL objects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalStorageConfig {
    /// Bucket name.
    pub bucket: String,
    /// Region.
    pub region: String,
    /// Optional endpoint for S3-compatible storage or a WAL cache/proxy.
    pub endpoint: Option<String>,
    /// Whether HTTP endpoints are allowed.
    pub allow_http: bool,
}

impl WalStorageConfig {
    fn from_lookup(
        storage: &StorageConfig,
        lookup: &mut impl FnMut(&str) -> Option<String>,
    ) -> Result<Option<Self>, ServerConfigError> {
        let bucket = lookup("WAL_S3_BUCKET");
        let endpoint = lookup("WAL_AWS_ENDPOINT").or_else(|| lookup("WAL_AWS_ENDPOINT_URL_S3"));
        let region_override = lookup("WAL_S3_REGION");
        let allow_http_override = lookup("WAL_AWS_ALLOW_HTTP");

        if bucket.is_none() && endpoint.is_none() {
            if region_override.is_some() || allow_http_override.is_some() {
                return Err(ServerConfigError::WalOverridesWithoutStorage);
            }
            return Ok(None);
        }

        let bucket = match bucket {
            Some(bucket) => bucket,
            None => match storage {
                StorageConfig::S3 { bucket, .. } => bucket.clone(),
                StorageConfig::Memory | StorageConfig::Disk { .. } => {
                    return Err(ServerConfigError::WalEndpointWithoutBucket);
                }
            },
        };
        let region = match region_override {
            Some(region) => region,
            None => match storage {
                StorageConfig::S3 { region, .. } => region.clone(),
                StorageConfig::Memory | StorageConfig::Disk { .. } => resolved_s3_region(lookup),
            },
        };
        let endpoint = match endpoint {
            Some(endpoint) => Some(endpoint),
            None => match storage {
                StorageConfig::S3 { endpoint, .. } => endpoint.clone(),
                StorageConfig::Memory | StorageConfig::Disk { .. } => resolved_s3_endpoint(lookup),
            },
        };
        let allow_http = match allow_http_override {
            Some(value) => environment_flag(&value),
            None => match storage {
                StorageConfig::S3 { allow_http, .. } => *allow_http,
                StorageConfig::Memory | StorageConfig::Disk { .. } => lookup("AWS_ALLOW_HTTP")
                    .as_deref()
                    .map(environment_flag)
                    .unwrap_or(false),
            },
        };

        Ok(Some(Self {
            bucket,
            region,
            endpoint,
            allow_http,
        }))
    }
}

/// Supported storage backends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageConfig {
    /// In-memory object store.
    Memory,
    /// Local filesystem object store.
    Disk {
        /// Root directory containing the database object store.
        root: PathBuf,
    },
    /// S3-compatible object store.
    S3 {
        /// Bucket name.
        bucket: String,
        /// Region.
        region: String,
        /// Optional endpoint for S3-compatible local storage.
        endpoint: Option<String>,
        /// Whether HTTP endpoints are allowed.
        allow_http: bool,
    },
}

impl StorageConfig {
    fn from_lookup(
        lookup: &mut impl FnMut(&str) -> Option<String>,
    ) -> Result<Self, ServerConfigError> {
        let data_dir = lookup("HELIX_DATA_DIR");
        let bucket = lookup("S3_BUCKET");
        if data_dir.is_some() && bucket.is_some() {
            return Err(ServerConfigError::ConflictingStorageConfiguration);
        }
        if let Some(root) = data_dir {
            return Ok(Self::Disk {
                root: PathBuf::from(root),
            });
        }
        let Some(bucket) = bucket else {
            return Ok(Self::Memory);
        };
        let region = resolved_s3_region(lookup);
        let endpoint = resolved_s3_endpoint(lookup);
        let allow_http = lookup("AWS_ALLOW_HTTP")
            .as_deref()
            .map(environment_flag)
            .unwrap_or(false);

        Ok(Self::S3 {
            bucket,
            region,
            endpoint,
            allow_http,
        })
    }
}

fn resolved_s3_region(lookup: &mut impl FnMut(&str) -> Option<String>) -> String {
    lookup("S3_REGION")
        .or_else(|| lookup("AWS_REGION"))
        .or_else(|| lookup("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|| "us-east-1".to_string())
}

fn resolved_s3_endpoint(lookup: &mut impl FnMut(&str) -> Option<String>) -> Option<String> {
    lookup("AWS_ENDPOINT").or_else(|| lookup("AWS_ENDPOINT_URL_S3"))
}

fn environment_flag(value: &str) -> bool {
    value.eq_ignore_ascii_case("true") || value == "1"
}

fn parse_addr(value: String) -> Result<SocketAddr, ServerConfigError> {
    value
        .parse()
        .map_err(|source| ServerConfigError::Addr { value, source })
}

/// Server configuration errors.
#[derive(Debug, thiserror::Error)]
pub enum ServerConfigError {
    /// Listener address could not be parsed.
    #[error("invalid listener address `{value}`: {source}")]
    Addr {
        /// Raw address value.
        value: String,
        /// Parse error.
        source: AddrParseError,
    },
    /// Two mutually exclusive storage backends were configured.
    #[error("HELIX_DATA_DIR and S3_BUCKET cannot be set together")]
    ConflictingStorageConfiguration,
    /// WAL-only overrides were supplied without enabling separate WAL storage.
    #[error(
        "WAL_S3_REGION and WAL_AWS_ALLOW_HTTP require WAL_S3_BUCKET, WAL_AWS_ENDPOINT, or WAL_AWS_ENDPOINT_URL_S3"
    )]
    WalOverridesWithoutStorage,
    /// A WAL endpoint needs a bucket, either explicit or inherited from main S3 storage.
    #[error("a WAL endpoint requires WAL_S3_BUCKET or S3_BUCKET")]
    WalEndpointWithoutBucket,
    /// The configured WAL object store could not be built.
    #[error("invalid WAL object store configuration: {source}")]
    WalObjectStore {
        /// Object-store configuration error.
        #[source]
        source: object_store::Error,
    },
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn absent_environment_uses_memory_and_documented_addresses() {
        let config = ServerConfig::from_lookup(|_| None).unwrap();
        assert_eq!(
            config.http_addr,
            "0.0.0.0:8080".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            config.grpc_addr,
            "0.0.0.0:8081".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(config.db_path, "db/");
        assert_eq!(config.storage, StorageConfig::Memory);
        assert_eq!(config.wal_storage, None);
        assert!(config.db_config().unwrap().wal_object_store().is_none());
    }

    #[test]
    fn canonical_addresses_override_fallbacks_and_invalid_values_are_typed() {
        let values = BTreeMap::from([
            ("HELIX_HTTP_ADDR", "127.0.0.1:9000"),
            ("HTTP_ADDR", "127.0.0.1:9001"),
            ("GRPC_ADDR", "127.0.0.1:9002"),
            ("DB_PATH", "tenant/db"),
        ]);
        let config =
            ServerConfig::from_lookup(|name| values.get(name).map(|value| (*value).to_string()))
                .unwrap();
        assert_eq!(
            config.http_addr,
            "127.0.0.1:9000".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            config.grpc_addr,
            "127.0.0.1:9002".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(config.db_path, "tenant/db");

        let error = ServerConfig::from_lookup(|name| {
            (name == "HELIX_HTTP_ADDR").then(|| "not-an-address".to_string())
        })
        .unwrap_err();
        assert!(
            matches!(error, ServerConfigError::Addr { value, .. } if value == "not-an-address")
        );
    }

    #[test]
    fn s3_environment_uses_closed_fallback_order_and_boolean_policy() {
        let values = BTreeMap::from([
            ("S3_BUCKET", "launch-bucket"),
            ("AWS_REGION", "eu-west-2"),
            ("AWS_DEFAULT_REGION", "ignored"),
            ("AWS_ENDPOINT_URL_S3", "http://minio:9000"),
            ("AWS_ALLOW_HTTP", "TRUE"),
        ]);
        let config =
            ServerConfig::from_lookup(|name| values.get(name).map(|value| (*value).to_string()))
                .unwrap();
        assert_eq!(
            config.storage,
            StorageConfig::S3 {
                bucket: "launch-bucket".to_string(),
                region: "eu-west-2".to_string(),
                endpoint: Some("http://minio:9000".to_string()),
                allow_http: true,
            }
        );

        let default_region = ServerConfig::from_lookup(|name| {
            (name == "S3_BUCKET").then(|| "launch-bucket".to_string())
        })
        .unwrap();
        assert!(matches!(
            default_region.storage,
            StorageConfig::S3 {
                region,
                endpoint: None,
                allow_http: false,
                ..
            } if region == "us-east-1"
        ));
        assert_eq!(config.wal_storage, None);
    }

    #[test]
    fn wal_environment_uses_explicit_precedence() {
        let values = BTreeMap::from([
            ("S3_BUCKET", "main-bucket"),
            ("S3_REGION", "main-region"),
            ("AWS_ENDPOINT", "http://main-primary:9000"),
            ("AWS_ALLOW_HTTP", "true"),
            ("WAL_S3_BUCKET", "wal-bucket"),
            ("WAL_S3_REGION", "wal-region"),
            ("WAL_AWS_ENDPOINT", "http://wal-primary:9000"),
            ("WAL_AWS_ENDPOINT_URL_S3", "http://wal-fallback:9000"),
            ("WAL_AWS_ALLOW_HTTP", "false"),
        ]);
        let config =
            ServerConfig::from_lookup(|name| values.get(name).map(|value| (*value).to_string()))
                .unwrap();

        assert_eq!(
            config.wal_storage,
            Some(WalStorageConfig {
                bucket: "wal-bucket".to_string(),
                region: "wal-region".to_string(),
                endpoint: Some("http://wal-primary:9000".to_string()),
                allow_http: false,
            })
        );
        assert!(config.db_config().unwrap().wal_object_store().is_some());
    }

    #[test]
    fn wal_endpoint_only_inherits_main_s3_bucket_and_region() {
        let values = BTreeMap::from([
            ("S3_BUCKET", "main-bucket"),
            ("AWS_REGION", "eu-west-1"),
            ("WAL_AWS_ENDPOINT_URL_S3", "http://wal-cache:9000"),
        ]);
        let config =
            ServerConfig::from_lookup(|name| values.get(name).map(|value| (*value).to_string()))
                .unwrap();

        assert_eq!(
            config.wal_storage,
            Some(WalStorageConfig {
                bucket: "main-bucket".to_string(),
                region: "eu-west-1".to_string(),
                endpoint: Some("http://wal-cache:9000".to_string()),
                allow_http: false,
            })
        );
    }

    #[test]
    fn wal_bucket_inherits_main_endpoint_and_http_policy() {
        let values = BTreeMap::from([
            ("S3_BUCKET", "main-bucket"),
            ("AWS_DEFAULT_REGION", "ap-southeast-2"),
            ("AWS_ENDPOINT_URL_S3", "http://main-store:9000"),
            ("AWS_ALLOW_HTTP", "1"),
            ("WAL_S3_BUCKET", "wal-bucket"),
        ]);
        let config =
            ServerConfig::from_lookup(|name| values.get(name).map(|value| (*value).to_string()))
                .unwrap();

        assert_eq!(
            config.wal_storage,
            Some(WalStorageConfig {
                bucket: "wal-bucket".to_string(),
                region: "ap-southeast-2".to_string(),
                endpoint: Some("http://main-store:9000".to_string()),
                allow_http: true,
            })
        );
    }

    #[test]
    fn explicit_wal_bucket_can_back_memory_or_disk_storage() {
        for data_dir in [None, Some("/var/lib/helix")] {
            let config = ServerConfig::from_lookup(|name| match name {
                "HELIX_DATA_DIR" => data_dir.map(str::to_string),
                "WAL_S3_BUCKET" => Some("wal-bucket".to_string()),
                "AWS_DEFAULT_REGION" => Some("us-west-2".to_string()),
                "AWS_ENDPOINT_URL_S3" => Some("http://wal-store:9000".to_string()),
                "AWS_ALLOW_HTTP" => Some("TRUE".to_string()),
                _ => None,
            })
            .unwrap();

            assert_eq!(
                config.wal_storage,
                Some(WalStorageConfig {
                    bucket: "wal-bucket".to_string(),
                    region: "us-west-2".to_string(),
                    endpoint: Some("http://wal-store:9000".to_string()),
                    allow_http: true,
                })
            );
        }
    }

    #[test]
    fn wal_region_or_http_without_storage_is_rejected() {
        for variable in ["WAL_S3_REGION", "WAL_AWS_ALLOW_HTTP"] {
            let error = ServerConfig::from_lookup(|name| match name {
                "S3_BUCKET" => Some("main-bucket".to_string()),
                name if name == variable => Some("override".to_string()),
                _ => None,
            })
            .unwrap_err();
            assert!(matches!(
                error,
                ServerConfigError::WalOverridesWithoutStorage
            ));
        }
    }

    #[test]
    fn wal_endpoint_without_an_explicit_or_main_bucket_is_rejected() {
        let error = ServerConfig::from_lookup(|name| {
            (name == "WAL_AWS_ENDPOINT").then(|| "http://wal-cache:9000".to_string())
        })
        .unwrap_err();
        assert!(matches!(error, ServerConfigError::WalEndpointWithoutBucket));
    }

    #[test]
    fn data_directory_selects_disk_storage() {
        let values = BTreeMap::from([
            ("HELIX_DATA_DIR", "/var/lib/helix"),
            ("DB_PATH", "tenant/db"),
        ]);
        let config =
            ServerConfig::from_lookup(|name| values.get(name).map(|value| (*value).to_string()))
                .unwrap();
        assert_eq!(
            config.storage,
            StorageConfig::Disk {
                root: PathBuf::from("/var/lib/helix"),
            }
        );
        assert!(matches!(
            config.db_source(),
            HelixDbSource::Disk { root, database }
                if root == *"/var/lib/helix" && database == "tenant/db"
        ));
    }

    #[test]
    fn data_directory_and_s3_bucket_are_rejected_together() {
        let error = ServerConfig::from_lookup(|name| match name {
            "HELIX_DATA_DIR" => Some("/var/lib/helix".to_string()),
            "S3_BUCKET" => Some("bucket".to_string()),
            _ => None,
        })
        .unwrap_err();
        assert!(matches!(
            error,
            ServerConfigError::ConflictingStorageConfiguration
        ));
    }
}
