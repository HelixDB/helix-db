use std::env;
use std::net::{AddrParseError, SocketAddr};
use std::num::{NonZeroUsize, ParseIntError};
use std::path::{Path, PathBuf};

use db::HelixDbSource;

/// Resident bytes for the hybrid SlateDB cache's memory tier when
/// `HELIX_DISK_CACHE_MEMORY_BYTES` is unset: the block plus metadata budget the
/// memory-only mode already uses, so enabling the disk tier does not change RSS.
const DEFAULT_CACHE_MEMORY_BYTES: NonZeroUsize = NonZeroUsize::new(
    (slatedb::db_cache::DEFAULT_BLOCK_CACHE_CAPACITY
        + slatedb::db_cache::DEFAULT_META_CACHE_CAPACITY) as usize,
)
.expect("SlateDB default cache capacities are nonzero");
/// Disk bytes shared by every disk tier when `HELIX_DISK_CACHE_BYTES` is unset.
/// The object-store tier receives half, which is SlateDB's own 16 GiB default.
const DEFAULT_CACHE_DISK_BYTES: NonZeroUsize =
    NonZeroUsize::new(32 * 1024 * 1024 * 1024).expect("default cache disk budget is nonzero");
/// Smallest disk budget that still leaves every tier several 4 MiB
/// object-store cache parts.
const MIN_CACHE_DISK_BYTES: usize = 64 * 1024 * 1024;

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
            StorageConfig::Disk { root, .. } => HelixDbSource::Disk {
                root: root.clone(),
                database: self.db_path.clone(),
            },
            StorageConfig::S3 {
                bucket,
                region,
                endpoint,
                allow_http,
                ..
            } => HelixDbSource::ObjectStorage {
                database: self.db_path.clone(),
                bucket: bucket.clone(),
                region: region.clone(),
                endpoint: endpoint.clone(),
                allow_http: *allow_http,
            },
        }
    }

    /// Build the DB runtime config for the selected storage and cache.
    ///
    /// Every backend without a hybrid cache keeps [`db::DbConfig::new`]'s
    /// bounded in-memory caches.
    ///
    /// # Examples
    ///
    /// ```
    /// use server::{CacheConfig, HybridCache, ServerConfig, StorageConfig};
    ///
    /// let directory = tempfile::tempdir().unwrap();
    /// let cache = HybridCache::try_new(
    ///     directory.path().join("cache"),
    ///     std::num::NonZeroUsize::new(64 * 1024 * 1024).unwrap(),
    ///     std::num::NonZeroUsize::new(1024 * 1024 * 1024).unwrap(),
    /// )
    /// .unwrap();
    /// let config = ServerConfig {
    ///     http_addr: "127.0.0.1:0".parse().unwrap(),
    ///     grpc_addr: "127.0.0.1:0".parse().unwrap(),
    ///     db_path: "db/".to_string(),
    ///     storage: StorageConfig::Disk {
    ///         root: directory.path().join("data"),
    ///         cache: CacheConfig::Hybrid(cache),
    ///     },
    /// };
    /// assert!(matches!(
    ///     config.db_config().cache().mode(),
    ///     db::config::CacheMode::Hybrid { .. }
    /// ));
    /// ```
    pub fn db_config(&self) -> db::DbConfig {
        match &self.storage {
            StorageConfig::Memory
            | StorageConfig::Disk {
                cache: CacheConfig::Memory,
                ..
            }
            | StorageConfig::S3 {
                cache: CacheConfig::Memory,
                ..
            } => db::DbConfig::new(),
            StorageConfig::Disk {
                cache: CacheConfig::Hybrid(cache),
                ..
            }
            | StorageConfig::S3 {
                cache: CacheConfig::Hybrid(cache),
                ..
            } => cache.db_config(),
        }
    }
}

/// Supported storage backends.
///
/// Only durable backends carry a [`CacheConfig`]: in-memory storage has no
/// remote reads for a disk cache to absorb.
#[derive(Debug, Clone, PartialEq)]
pub enum StorageConfig {
    /// In-memory object store.
    Memory,
    /// Local filesystem object store.
    Disk {
        /// Root directory containing the database object store.
        root: PathBuf,
        /// Local caches in front of the object store.
        cache: CacheConfig,
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
        /// Local caches in front of the object store.
        cache: CacheConfig,
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
                cache: CacheConfig::from_lookup(lookup)?,
            });
        }
        let Some(bucket) = bucket else {
            // Reject before any cache directory is created.
            return [
                "HELIX_DISK_CACHE_DIR",
                "HELIX_DISK_CACHE_MEMORY_BYTES",
                "HELIX_DISK_CACHE_BYTES",
            ]
            .into_iter()
            .find(|&variable| lookup(variable).is_some())
            .map_or(Ok(Self::Memory), |variable| {
                Err(ServerConfigError::CacheWithMemoryStorage { variable })
            });
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
            cache: CacheConfig::from_lookup(lookup)?,
        })
    }
}

/// Local caches placed in front of a durable storage backend.
#[derive(Debug, Clone, PartialEq)]
pub enum CacheConfig {
    /// Bounded in-memory SlateDB block/metadata and FTS split caches.
    Memory,
    /// Memory-plus-disk caches rooted in one local directory.
    Hybrid(HybridCache),
}

impl CacheConfig {
    fn from_lookup(
        lookup: &mut impl FnMut(&str) -> Option<String>,
    ) -> Result<Self, ServerConfigError> {
        let Some(root) = lookup("HELIX_DISK_CACHE_DIR") else {
            return ["HELIX_DISK_CACHE_MEMORY_BYTES", "HELIX_DISK_CACHE_BYTES"]
                .into_iter()
                .find(|&variable| lookup(variable).is_some())
                .map_or(Ok(Self::Memory), |variable| {
                    Err(ServerConfigError::CacheSizeWithoutDirectory { variable })
                });
        };
        let memory_bytes = parse_cache_bytes(lookup, "HELIX_DISK_CACHE_MEMORY_BYTES")?
            .unwrap_or(DEFAULT_CACHE_MEMORY_BYTES);
        let disk_bytes = parse_cache_bytes(lookup, "HELIX_DISK_CACHE_BYTES")?
            .unwrap_or(DEFAULT_CACHE_DISK_BYTES);
        HybridCache::try_new(root, memory_bytes, disk_bytes).map(Self::Hybrid)
    }
}

/// Validated memory-plus-disk caches rooted in one writable directory.
///
/// The directory holds three tiers, each bounded by a share of the disk budget:
///
/// | Subdirectory | Tier | Disk share |
/// | --- | --- | --- |
/// | `object-store/` | SlateDB object-store part cache | 1/2 |
/// | `slate/` | SlateDB block/metadata hybrid cache | 3/8 |
/// | `fts/` | Full-text split cache | the remaining ~1/8 |
///
/// The object-store tier also caches SSTs this server flushes or compacts, so
/// a single-process server reads its own writes back from local disk. Only one
/// server may use a cache directory at a time.
#[derive(Debug, Clone, PartialEq)]
pub struct HybridCache {
    root: PathBuf,
    mode: db::config::CacheMode,
}

impl HybridCache {
    /// Validate the cache budgets and prove `root` is a writable directory.
    ///
    /// `root` is created when missing. `memory_bytes` bounds the SlateDB
    /// block/metadata cache's memory tier; `disk_bytes` is split across the
    /// disk tiers and must be at least 64 MiB.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    ///
    /// use server::{HybridCache, ServerConfigError};
    ///
    /// let directory = tempfile::tempdir().unwrap();
    /// let root = directory.path().join("cache");
    /// let memory = NonZeroUsize::new(64 * 1024 * 1024).unwrap();
    /// let cache =
    ///     HybridCache::try_new(&root, memory, NonZeroUsize::new(1024 * 1024 * 1024).unwrap())
    ///         .unwrap();
    /// assert_eq!(cache.root(), root);
    /// assert!(root.is_dir());
    ///
    /// let too_small = HybridCache::try_new(&root, memory, NonZeroUsize::new(1024).unwrap());
    /// assert!(matches!(too_small, Err(ServerConfigError::CacheDiskTooSmall { .. })));
    /// ```
    pub fn try_new(
        root: impl Into<PathBuf>,
        memory_bytes: NonZeroUsize,
        disk_bytes: NonZeroUsize,
    ) -> Result<Self, ServerConfigError> {
        let root = root.into();
        if root.as_os_str().is_empty() {
            return Err(ServerConfigError::EmptyCacheDirectory);
        }
        if disk_bytes.get() < MIN_CACHE_DISK_BYTES {
            return Err(ServerConfigError::CacheDiskTooSmall {
                bytes: disk_bytes.get(),
                minimum: MIN_CACHE_DISK_BYTES,
            });
        }
        let object_store_bytes = disk_bytes.get() / 2;
        let slate_disk_bytes = disk_bytes.get() / 8 * 3;
        let fts_disk_bytes = disk_bytes.get() - object_store_bytes - slate_disk_bytes;
        let object_store_defaults = slatedb::config::ObjectStoreCacheOptions::default();
        let fts_defaults = db::config::FtsMemoryCacheConfig::default();
        let mode = db::config::CacheMode::Hybrid {
            slate_db: db::config::SlateHybridCacheConfig::try_new(
                memory_bytes.get(),
                root.join("slate"),
                slate_disk_bytes,
            )?,
            object_store: db::config::SlateObjectStoreCacheSettings::try_new(
                root.join("object-store"),
                Some(object_store_bytes),
                object_store_defaults.part_size_bytes,
                true,
                db::config::ObjectStoreWarmLevel::Off,
                object_store_defaults.scan_interval,
                object_store_defaults.max_open_file_handles,
            )?,
            slate_warm: db::config::SlateWarmConfig::default(),
            fts: Some(db::config::FtsHybridCacheConfig::try_new(
                fts_defaults.memory_bytes(),
                root.join("fts"),
                fts_disk_bytes,
                fts_defaults.warm().clone(),
                fts_defaults.generation_grace_period().as_secs(),
            )?),
        };
        let probe = root.join(".helix-cache-write-check");
        std::fs::create_dir_all(&root)
            .and_then(|()| std::fs::write(&probe, b""))
            .and_then(|()| std::fs::remove_file(&probe))
            .map_err(|source| ServerConfigError::CacheDirectory {
                path: root.clone(),
                source,
            })?;
        Ok(Self { root, mode })
    }

    /// Cache root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Build the DB runtime config with these caches and every other default.
    pub fn db_config(&self) -> db::DbConfig {
        let config = db::DbConfig::new();
        let cache = config.cache().clone().with_mode(self.mode.clone());
        config.with_cache(cache)
    }
}

fn parse_addr(value: String) -> Result<SocketAddr, ServerConfigError> {
    value
        .parse()
        .map_err(|source| ServerConfigError::Addr { value, source })
}

fn parse_cache_bytes(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    variable: &'static str,
) -> Result<Option<NonZeroUsize>, ServerConfigError> {
    lookup(variable)
        .map(|value| {
            value
                .parse()
                .map_err(|source| ServerConfigError::CacheBytes {
                    variable,
                    value,
                    source,
                })
        })
        .transpose()
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
    /// A disk cache setting was supplied for in-memory storage.
    #[error(
        "{variable} requires HELIX_DATA_DIR or S3_BUCKET; in-memory storage has no durable objects to cache on disk"
    )]
    CacheWithMemoryStorage {
        /// First cache variable found.
        variable: &'static str,
    },
    /// A cache size was supplied without a cache directory.
    #[error("{variable} requires HELIX_DISK_CACHE_DIR")]
    CacheSizeWithoutDirectory {
        /// Size variable found.
        variable: &'static str,
    },
    /// A cache size was not a positive integer byte count.
    #[error("invalid {variable} `{value}`: expected a positive byte count ({source})")]
    CacheBytes {
        /// Size variable.
        variable: &'static str,
        /// Raw value.
        value: String,
        /// Parse error.
        source: ParseIntError,
    },
    /// The disk budget cannot give every tier a usable share.
    #[error("HELIX_DISK_CACHE_BYTES must be at least {minimum} bytes, got {bytes}")]
    CacheDiskTooSmall {
        /// Requested disk budget.
        bytes: usize,
        /// Smallest accepted disk budget.
        minimum: usize,
    },
    /// The cache directory was empty.
    #[error("HELIX_DISK_CACHE_DIR must not be empty")]
    EmptyCacheDirectory,
    /// The cache directory could not be created or written.
    #[error("HELIX_DISK_CACHE_DIR `{}` is not a writable directory: {source}", .path.display())]
    CacheDirectory {
        /// Cache directory.
        path: PathBuf,
        /// Filesystem error.
        source: std::io::Error,
    },
    /// The DB crate rejected a derived cache tier.
    #[error("invalid cache configuration: {0}")]
    Cache(#[from] db::config::ConfigError),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    const MIB: usize = 1024 * 1024;

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
        assert_eq!(config.db_config().cache(), db::DbConfig::new().cache());
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
                cache: CacheConfig::Memory,
            }
        );
        assert_eq!(config.db_config().cache(), db::DbConfig::new().cache());

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
                cache: CacheConfig::Memory,
            }
        );
        assert!(matches!(
            config.db_source(),
            HelixDbSource::Disk { root, database }
                if root == *"/var/lib/helix" && database == "tenant/db"
        ));
        assert_eq!(config.db_config().cache(), db::DbConfig::new().cache());
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

    #[test]
    fn cache_directory_enables_hybrid_tiers_with_documented_defaults() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("nested").join("cache");
        let values = BTreeMap::from([
            ("S3_BUCKET", "bucket".to_string()),
            ("HELIX_DISK_CACHE_DIR", root.display().to_string()),
        ]);
        let config = ServerConfig::from_lookup(|name| values.get(name).cloned()).unwrap();

        let StorageConfig::S3 {
            cache: CacheConfig::Hybrid(cache),
            ..
        } = &config.storage
        else {
            panic!("S3 storage with a cache directory selects the hybrid cache");
        };
        assert_eq!(cache.root(), root);
        assert!(root.is_dir(), "startup creates the cache directory");
        assert_eq!(
            std::fs::read_dir(&root).unwrap().count(),
            0,
            "the write probe is removed"
        );
        let db::config::CacheMode::Hybrid {
            slate_db,
            object_store,
            slate_warm,
            fts: Some(fts),
        } = config.db_config().cache().mode().clone()
        else {
            panic!("hybrid cache builds hybrid SlateDB and FTS tiers");
        };
        assert_eq!(slate_db.memory_bytes(), 640 * MIB);
        assert_eq!(slate_db.disk().root(), root.join("slate"));
        assert_eq!(slate_db.disk().bytes(), 12 * 1024 * MIB);
        assert_eq!(object_store.root(), root.join("object-store"));
        assert_eq!(object_store.warm(), db::config::ObjectStoreWarmLevel::Off);
        let options = object_store.to_slate_options();
        assert_eq!(options.max_cache_size_bytes, Some(16 * 1024 * MIB));
        assert_eq!(
            options.max_cache_size_bytes,
            slatedb::config::ObjectStoreCacheOptions::default().max_cache_size_bytes,
            "the object-store tier keeps SlateDB's default capacity"
        );
        assert_eq!(options.part_size_bytes, 4 * MIB);
        assert!(options.cache_puts);
        assert_eq!(slate_warm, db::config::SlateWarmConfig::default());
        assert_eq!(fts.disk_root(), root.join("fts"));
        assert_eq!(fts.disk_bytes(), 4 * 1024 * 1024 * 1024);
        assert_eq!(
            fts.memory_bytes(),
            db::config::FtsMemoryCacheConfig::default().memory_bytes()
        );
        assert_eq!(
            config.db_config().cache().vector_memory(),
            db::DbConfig::new().cache().vector_memory()
        );
    }

    #[test]
    fn cache_size_overrides_split_the_whole_disk_budget_across_tiers() {
        let directory = tempfile::tempdir().unwrap();
        let disk_bytes = 1024 * MIB + 7;
        let values = BTreeMap::from([
            (
                "HELIX_DATA_DIR",
                directory.path().join("data").display().to_string(),
            ),
            (
                "HELIX_DISK_CACHE_DIR",
                directory.path().join("cache").display().to_string(),
            ),
            ("HELIX_DISK_CACHE_MEMORY_BYTES", (128 * MIB).to_string()),
            ("HELIX_DISK_CACHE_BYTES", disk_bytes.to_string()),
        ]);
        let config = ServerConfig::from_lookup(|name| values.get(name).cloned()).unwrap();
        assert!(matches!(
            config.storage,
            StorageConfig::Disk {
                cache: CacheConfig::Hybrid(_),
                ..
            }
        ));
        let db::config::CacheMode::Hybrid {
            slate_db,
            object_store,
            fts: Some(fts),
            ..
        } = config.db_config().cache().mode().clone()
        else {
            panic!("hybrid cache builds hybrid SlateDB and FTS tiers");
        };
        let object_store_bytes = object_store
            .to_slate_options()
            .max_cache_size_bytes
            .unwrap();
        assert_eq!(slate_db.memory_bytes(), 128 * MIB);
        assert_eq!(object_store_bytes, disk_bytes / 2);
        assert_eq!(slate_db.disk().bytes(), 384 * MIB);
        assert_eq!(
            object_store_bytes + slate_db.disk().bytes() + fts.disk_bytes() as usize,
            disk_bytes,
            "the remainder goes to the FTS tier"
        );

        let minimum = BTreeMap::from([
            ("S3_BUCKET", "bucket".to_string()),
            (
                "HELIX_DISK_CACHE_DIR",
                directory.path().join("minimum").display().to_string(),
            ),
            ("HELIX_DISK_CACHE_BYTES", MIN_CACHE_DISK_BYTES.to_string()),
        ]);
        assert!(ServerConfig::from_lookup(|name| minimum.get(name).cloned()).is_ok());
    }

    #[test]
    fn invalid_cache_sizes_name_the_variable() {
        let directory = tempfile::tempdir().unwrap();
        for (variable, value) in [
            ("HELIX_DISK_CACHE_MEMORY_BYTES", "0"),
            ("HELIX_DISK_CACHE_MEMORY_BYTES", "-1"),
            ("HELIX_DISK_CACHE_MEMORY_BYTES", "64MiB"),
            ("HELIX_DISK_CACHE_BYTES", "0"),
            ("HELIX_DISK_CACHE_BYTES", ""),
            ("HELIX_DISK_CACHE_BYTES", "99999999999999999999999"),
        ] {
            let values = BTreeMap::from([
                ("S3_BUCKET", "bucket".to_string()),
                (
                    "HELIX_DISK_CACHE_DIR",
                    directory.path().display().to_string(),
                ),
                (variable, value.to_string()),
            ]);
            let error = ServerConfig::from_lookup(|name| values.get(name).cloned()).unwrap_err();
            assert!(
                matches!(
                    &error,
                    ServerConfigError::CacheBytes { variable: found, value: raw, .. }
                        if *found == variable && raw == value
                ),
                "{variable}={value} produced {error:?}"
            );
            assert!(error
                .to_string()
                .starts_with(&format!("invalid {variable}")));
        }

        let values = BTreeMap::from([
            ("S3_BUCKET", "bucket".to_string()),
            (
                "HELIX_DISK_CACHE_DIR",
                directory.path().display().to_string(),
            ),
            (
                "HELIX_DISK_CACHE_BYTES",
                (MIN_CACHE_DISK_BYTES - 1).to_string(),
            ),
        ]);
        let error = ServerConfig::from_lookup(|name| values.get(name).cloned()).unwrap_err();
        assert!(matches!(
            error,
            ServerConfigError::CacheDiskTooSmall { bytes, minimum }
                if bytes == MIN_CACHE_DISK_BYTES - 1 && minimum == MIN_CACHE_DISK_BYTES
        ));
    }

    #[test]
    fn empty_or_unwritable_cache_directory_fails_startup() {
        let empty = BTreeMap::from([
            ("S3_BUCKET", "bucket".to_string()),
            ("HELIX_DISK_CACHE_DIR", String::new()),
        ]);
        assert!(matches!(
            ServerConfig::from_lookup(|name| empty.get(name).cloned()).unwrap_err(),
            ServerConfigError::EmptyCacheDirectory
        ));

        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("not-a-directory");
        std::fs::write(&file, b"occupied").unwrap();
        for root in [file.clone(), file.join("cache")] {
            let values = BTreeMap::from([
                ("HELIX_DATA_DIR", directory.path().display().to_string()),
                ("HELIX_DISK_CACHE_DIR", root.display().to_string()),
            ]);
            let error = ServerConfig::from_lookup(|name| values.get(name).cloned()).unwrap_err();
            assert!(
                matches!(&error, ServerConfigError::CacheDirectory { path, .. } if *path == root),
                "{} produced {error:?}",
                root.display()
            );
            assert!(error.to_string().contains("is not a writable directory"));
        }
    }

    #[test]
    fn cache_sizes_without_a_cache_directory_are_rejected() {
        for variable in ["HELIX_DISK_CACHE_MEMORY_BYTES", "HELIX_DISK_CACHE_BYTES"] {
            let values = BTreeMap::from([
                ("S3_BUCKET", "bucket".to_string()),
                (variable, (128 * MIB).to_string()),
            ]);
            let error = ServerConfig::from_lookup(|name| values.get(name).cloned()).unwrap_err();
            assert!(matches!(
                error,
                ServerConfigError::CacheSizeWithoutDirectory { variable: found }
                    if found == variable
            ));
            assert_eq!(
                error.to_string(),
                format!("{variable} requires HELIX_DISK_CACHE_DIR")
            );
        }
    }

    #[test]
    fn memory_storage_rejects_cache_settings_without_touching_the_filesystem() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("cache");
        for (variable, value) in [
            ("HELIX_DISK_CACHE_DIR", root.display().to_string()),
            ("HELIX_DISK_CACHE_MEMORY_BYTES", (128 * MIB).to_string()),
            ("HELIX_DISK_CACHE_BYTES", "not-a-number".to_string()),
        ] {
            let values = BTreeMap::from([(variable, value)]);
            let error = ServerConfig::from_lookup(|name| values.get(name).cloned()).unwrap_err();
            assert!(matches!(
                error,
                ServerConfigError::CacheWithMemoryStorage { variable: found } if found == variable
            ));
            assert!(error.to_string().contains("in-memory storage"));
        }
        assert!(
            !root.exists(),
            "memory storage never creates a cache directory"
        );
    }
}
