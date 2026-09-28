use std::env;
use std::net::{AddrParseError, SocketAddr};
use std::path::PathBuf;

use db::HelixDbSource;

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

        Ok(Self {
            http_addr,
            grpc_addr,
            db_path,
            storage,
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
        let region = lookup("S3_REGION")
            .or_else(|| lookup("AWS_REGION"))
            .or_else(|| lookup("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|| "us-east-1".to_string());
        let endpoint = lookup("AWS_ENDPOINT").or_else(|| lookup("AWS_ENDPOINT_URL_S3"));
        let allow_http = lookup("AWS_ALLOW_HTTP")
            .map(|value| value.eq_ignore_ascii_case("true") || value == "1")
            .unwrap_or(false);

        Ok(Self::S3 {
            bucket,
            region,
            endpoint,
            allow_http,
        })
    }
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
            ("AWS_ENDPOINT_URL_S3", "http://seaweedfs:8333"),
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
                endpoint: Some("http://seaweedfs:8333".to_string()),
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
