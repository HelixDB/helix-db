use std::env;
use std::net::{AddrParseError, SocketAddr};

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
        let http_addr = parse_addr_env("HELIX_HTTP_ADDR", "HTTP_ADDR", "0.0.0.0:8080")?;
        let grpc_addr = parse_addr_env("HELIX_GRPC_ADDR", "GRPC_ADDR", "0.0.0.0:8081")?;
        let db_path = env::var("DB_PATH").unwrap_or_else(|_| "db/".to_string());
        let storage = StorageConfig::from_env();

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
    fn from_env() -> Self {
        let Ok(bucket) = env::var("S3_BUCKET") else {
            return Self::Memory;
        };
        let region = env::var("S3_REGION")
            .or_else(|_| env::var("AWS_REGION"))
            .or_else(|_| env::var("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|_| "us-east-1".to_string());
        let endpoint = env::var("AWS_ENDPOINT")
            .or_else(|_| env::var("AWS_ENDPOINT_URL_S3"))
            .ok();
        let allow_http = env::var("AWS_ALLOW_HTTP")
            .map(|value| value.eq_ignore_ascii_case("true") || value == "1")
            .unwrap_or(false);

        Self::S3 {
            bucket,
            region,
            endpoint,
            allow_http,
        }
    }
}

fn parse_addr_env(
    primary: &'static str,
    fallback: &'static str,
    default: &'static str,
) -> Result<SocketAddr, ServerConfigError> {
    let value = env::var(primary)
        .or_else(|_| env::var(fallback))
        .unwrap_or_else(|_| default.to_string());
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_storage_is_default_without_s3_bucket() {
        // SAFETY: this test only mutates the process environment for one key and
        // does not depend on concurrent test execution for correctness.
        unsafe {
            env::remove_var("S3_BUCKET");
        }

        assert_eq!(StorageConfig::from_env(), StorageConfig::Memory);
    }
}
