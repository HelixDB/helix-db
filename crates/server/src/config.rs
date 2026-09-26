use std::env;
use std::ffi::OsString;
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
/// Largest disk budget (1 TiB). Its 3/8 block-cache share keeps the block
/// cache within 32Ki partition files, each held open while the server runs.
const MAX_CACHE_DISK_BYTES: usize = 1024 * 1024 * 1024 * 1024;
/// Open files the server needs besides the two disk-cache tiers: listeners,
/// connections, WAL and SST reads, and full-text split files.
const OPEN_FILE_HEADROOM: u64 = 1024;

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
    ///
    /// Paths may be any bytes; every other variable must be UTF-8.
    pub fn from_env() -> Result<Self, ServerConfigError> {
        Self::from_lookup(|name| env::var_os(name))
    }

    fn from_lookup(
        mut lookup: impl FnMut(&str) -> Option<OsString>,
    ) -> Result<Self, ServerConfigError> {
        let http_addr = parse_addr(
            text(&mut lookup, &["HELIX_HTTP_ADDR", "HTTP_ADDR"])?
                .unwrap_or(("HELIX_HTTP_ADDR", "0.0.0.0:8080".to_string())),
        )?;
        let grpc_addr = parse_addr(
            text(&mut lookup, &["HELIX_GRPC_ADDR", "GRPC_ADDR"])?
                .unwrap_or(("HELIX_GRPC_ADDR", "0.0.0.0:8081".to_string())),
        )?;
        let db_path =
            text(&mut lookup, &["DB_PATH"])?.map_or_else(|| "db/".to_string(), |(_, path)| path);
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
    ///         cache: CacheConfig::Hybrid(Box::new(cache)),
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

    /// Minimum open files the hard limit must allow before storage opens, or
    /// `None` when only memory caches run and the process limit is left alone.
    ///
    /// # Examples
    ///
    /// ```
    /// use server::{ServerConfig, StorageConfig};
    ///
    /// let config = ServerConfig {
    ///     http_addr: "127.0.0.1:0".parse().unwrap(),
    ///     grpc_addr: "127.0.0.1:0".parse().unwrap(),
    ///     db_path: "db/".to_string(),
    ///     storage: StorageConfig::Memory,
    /// };
    /// assert_eq!(config.required_open_files(), None);
    /// ```
    pub fn required_open_files(&self) -> Option<u64> {
        match &self.storage {
            StorageConfig::Memory
            | StorageConfig::Disk {
                cache: CacheConfig::Memory,
                ..
            }
            | StorageConfig::S3 {
                cache: CacheConfig::Memory,
                ..
            } => None,
            StorageConfig::Disk {
                cache: CacheConfig::Hybrid(cache),
                ..
            }
            | StorageConfig::S3 {
                cache: CacheConfig::Hybrid(cache),
                ..
            } => Some(cache.required_open_files()),
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
        lookup: &mut impl FnMut(&str) -> Option<OsString>,
    ) -> Result<Self, ServerConfigError> {
        match (lookup("HELIX_DATA_DIR"), text(lookup, &["S3_BUCKET"])?) {
            (Some(_), Some(_)) => Err(ServerConfigError::ConflictingStorageConfiguration),
            (Some(root), None) => Ok(Self::Disk {
                root: PathBuf::from(root),
                cache: CacheConfig::from_lookup(lookup)?,
            }),
            // Reject before any cache directory is created.
            (None, None) => [
                "HELIX_DISK_CACHE_DIR",
                "HELIX_DISK_CACHE_MEMORY_BYTES",
                "HELIX_DISK_CACHE_BYTES",
            ]
            .into_iter()
            .find(|&variable| lookup(variable).is_some())
            .map_or(Ok(Self::Memory), |variable| {
                Err(ServerConfigError::CacheWithMemoryStorage { variable })
            }),
            (None, Some((_, bucket))) => Ok(Self::S3 {
                bucket,
                region: text(lookup, &["S3_REGION", "AWS_REGION", "AWS_DEFAULT_REGION"])?
                    .map_or_else(|| "us-east-1".to_string(), |(_, region)| region),
                endpoint: text(lookup, &["AWS_ENDPOINT", "AWS_ENDPOINT_URL_S3"])?
                    .map(|(_, endpoint)| endpoint),
                allow_http: text(lookup, &["AWS_ALLOW_HTTP"])?
                    .is_some_and(|(_, value)| value.eq_ignore_ascii_case("true") || value == "1"),
                cache: CacheConfig::from_lookup(lookup)?,
            }),
        }
    }
}

/// Local caches placed in front of a durable storage backend.
#[derive(Debug, Clone, PartialEq)]
pub enum CacheConfig {
    /// Bounded in-memory SlateDB block/metadata and FTS split caches.
    Memory,
    /// Memory-plus-disk caches rooted in one local directory.
    Hybrid(Box<HybridCache>),
}

impl CacheConfig {
    fn from_lookup(
        lookup: &mut impl FnMut(&str) -> Option<OsString>,
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
        HybridCache::try_new(PathBuf::from(root), memory_bytes, disk_bytes)
            .map(|cache| Self::Hybrid(Box::new(cache)))
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
    slate_db: db::config::SlateHybridCacheConfig,
    object_store: db::config::SlateObjectStoreCacheSettings,
    fts: db::config::FtsHybridCacheConfig,
}

impl HybridCache {
    /// Validate the cache budgets and prove every tier directory is writable.
    ///
    /// `root` and its tier subdirectories are created when missing.
    /// `memory_bytes` bounds the SlateDB block/metadata cache's memory tier;
    /// `disk_bytes` is split across the disk tiers and must be within
    /// 64 MiB..=1 TiB.
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
    /// assert!(root.join("slate").is_dir());
    ///
    /// let too_small = HybridCache::try_new(&root, memory, NonZeroUsize::new(1024).unwrap());
    /// assert!(matches!(too_small, Err(ServerConfigError::CacheDiskOutOfRange { .. })));
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
        if !(MIN_CACHE_DISK_BYTES..=MAX_CACHE_DISK_BYTES).contains(&disk_bytes.get()) {
            return Err(ServerConfigError::CacheDiskOutOfRange {
                bytes: disk_bytes.get(),
                minimum: MIN_CACHE_DISK_BYTES,
                maximum: MAX_CACHE_DISK_BYTES,
            });
        }
        let object_store_bytes = disk_bytes.get() / 2;
        let slate_disk_bytes = disk_bytes.get() / 8 * 3;
        let fts_disk_bytes = disk_bytes.get() - object_store_bytes - slate_disk_bytes;
        let object_store_defaults = slatedb::config::ObjectStoreCacheOptions::default();
        let fts_defaults = db::config::FtsMemoryCacheConfig::default();
        let cache = Self {
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
            fts: db::config::FtsHybridCacheConfig::try_new(
                fts_defaults.memory_bytes(),
                root.join("fts"),
                fts_disk_bytes,
                fts_defaults.warm().clone(),
                fts_defaults.generation_grace_period().as_secs(),
            )?,
            root,
        };
        [
            cache.root.as_path(),
            cache.slate_db.disk().root(),
            cache.object_store.root(),
            cache.fts.disk().root(),
        ]
        .into_iter()
        .try_for_each(|directory| {
            let probe = directory.join(".helix-cache-write-check");
            std::fs::create_dir_all(directory)
                .and_then(|()| std::fs::write(&probe, b""))
                .and_then(|()| std::fs::remove_file(&probe))
                .map_err(|source| ServerConfigError::CacheDirectory {
                    path: directory.to_path_buf(),
                    source,
                })
        })?;
        Ok(cache)
    }

    /// Cache root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Minimum open files a server with this cache needs: one per
    /// block-cache partition (at most 32Ki), the object-store tier's handle
    /// cache, and headroom for everything else. The full-text tier's open
    /// split files come on top, so the server raises its soft limit to the
    /// hard limit rather than to this floor.
    pub fn required_open_files(&self) -> u64 {
        (self.slate_db.disk_partitions() + self.object_store.max_open_file_handles()) as u64
            + OPEN_FILE_HEADROOM
    }

    /// Build the DB runtime config with these caches and every other default.
    pub fn db_config(&self) -> db::DbConfig {
        let config = db::DbConfig::new();
        let cache = config
            .cache()
            .clone()
            .with_mode(db::config::CacheMode::Hybrid {
                slate_db: self.slate_db.clone(),
                object_store: self.object_store.clone(),
                slate_warm: db::config::SlateWarmConfig::default(),
                fts: Some(self.fts.clone()),
            });
        config.with_cache(cache)
    }
}

/// Reads the first set variable among `variables`, which must be UTF-8.
fn text(
    lookup: &mut impl FnMut(&str) -> Option<OsString>,
    variables: &[&'static str],
) -> Result<Option<(&'static str, String)>, ServerConfigError> {
    variables
        .iter()
        .find_map(|&variable| {
            lookup(variable).map(|value| {
                value
                    .into_string()
                    .map(|value| (variable, value))
                    .map_err(|_| ServerConfigError::NotUnicode { variable })
            })
        })
        .transpose()
}

fn parse_addr((variable, value): (&'static str, String)) -> Result<SocketAddr, ServerConfigError> {
    value.parse().map_err(|source| ServerConfigError::Addr {
        variable,
        value,
        source,
    })
}

fn parse_cache_bytes(
    lookup: &mut impl FnMut(&str) -> Option<OsString>,
    variable: &'static str,
) -> Result<Option<NonZeroUsize>, ServerConfigError> {
    text(lookup, &[variable])?
        .map(|(_, value)| {
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

/// Server configuration errors. Every message names the variable to fix.
#[derive(Debug, thiserror::Error)]
pub enum ServerConfigError {
    /// Listener address could not be parsed.
    #[error("invalid {variable} `{value}`")]
    Addr {
        /// Address variable.
        variable: &'static str,
        /// Raw address value.
        value: String,
        /// Parse error.
        source: AddrParseError,
    },
    /// A variable that must be text was not valid UTF-8.
    #[error("{variable} is not valid UTF-8")]
    NotUnicode {
        /// Variable found.
        variable: &'static str,
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
    #[error("invalid {variable} `{value}`: expected a positive byte count")]
    CacheBytes {
        /// Size variable.
        variable: &'static str,
        /// Raw value.
        value: String,
        /// Parse error.
        source: ParseIntError,
    },
    /// The disk budget is too small to give every tier a usable share, or so
    /// large the block cache would need an unbounded number of open files.
    #[error("HELIX_DISK_CACHE_BYTES must be between {minimum} and {maximum} bytes, got {bytes}")]
    CacheDiskOutOfRange {
        /// Requested disk budget.
        bytes: usize,
        /// Smallest accepted disk budget.
        minimum: usize,
        /// Largest accepted disk budget.
        maximum: usize,
    },
    /// The cache directory was empty.
    #[error("HELIX_DISK_CACHE_DIR must not be empty")]
    EmptyCacheDirectory,
    /// The cache directory or one of its tier subdirectories could not be
    /// created or written.
    #[error("HELIX_DISK_CACHE_DIR: `{}` is not a writable directory", .path.display())]
    CacheDirectory {
        /// Unwritable directory.
        path: PathBuf,
        /// Filesystem error.
        source: std::io::Error,
    },
    /// The DB crate rejected a derived cache tier.
    #[error(
        "invalid HELIX_DISK_CACHE_DIR, HELIX_DISK_CACHE_BYTES or HELIX_DISK_CACHE_MEMORY_BYTES setting"
    )]
    Cache(#[from] db::config::ConfigError),
    /// The hard open-file limit is below the disk cache's minimum. The
    /// minimum does not fall steadily with the budget, so the remedy is a
    /// higher limit.
    #[error(
        "HELIX_DISK_CACHE_BYTES: the disk cache needs at least {required} open files but the hard open-file limit is {limit}; raise the hard limit (docker run --ulimit nofile=65536:65536)"
    )]
    OpenFileLimit {
        /// Open files the cache needs.
        required: u64,
        /// Hard open-file limit.
        limit: u64,
    },
    /// Raising the soft open-file limit failed.
    #[error(
        "HELIX_DISK_CACHE_BYTES: could not raise the soft open-file limit for a disk cache that needs at least {required} open files"
    )]
    RaiseOpenFileLimit {
        /// Open files the cache needs.
        required: u64,
        /// Operating-system error.
        source: std::io::Error,
    },
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
        assert_eq!(config.required_open_files(), None);
    }

    /// Set only in the child process [`from_env_reads_the_process_environment`]
    /// launches.
    const FROM_ENV_PROBE: &str = "HELIX_SERVER_FROM_ENV_PROBE";

    /// The environment is process-global, so instead of mutating it under
    /// concurrently running tests, this re-runs the test binary as a child
    /// whose environment is set from startup; the child checks `from_env`.
    #[test]
    fn from_env_reads_the_process_environment() {
        let status = std::process::Command::new(env::current_exe().unwrap())
            .args(["--exact", "config::tests::from_env_probe", "--nocapture"])
            .env(FROM_ENV_PROBE, "1")
            .env("HELIX_HTTP_ADDR", "127.0.0.1:9100")
            .env("HELIX_GRPC_ADDR", "127.0.0.1:9101")
            .env("DB_PATH", "env/db")
            .env("HELIX_DATA_DIR", "/var/lib/helix")
            .env_remove("S3_BUCKET")
            .env_remove("HELIX_DISK_CACHE_DIR")
            .env_remove("HELIX_DISK_CACHE_MEMORY_BYTES")
            .env_remove("HELIX_DISK_CACHE_BYTES")
            .status()
            .unwrap();
        assert!(status.success(), "the from_env probe process succeeds");
    }

    #[test]
    fn from_env_probe() {
        if env::var_os(FROM_ENV_PROBE).is_none() {
            return;
        }
        let config = ServerConfig::from_env().unwrap();
        assert_eq!(
            config.http_addr,
            "127.0.0.1:9100".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            config.grpc_addr,
            "127.0.0.1:9101".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(config.db_path, "env/db");
        assert_eq!(
            config.storage,
            StorageConfig::Disk {
                root: PathBuf::from("/var/lib/helix"),
                cache: CacheConfig::Memory,
            }
        );
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
            ServerConfig::from_lookup(|name| values.get(name).map(OsString::from)).unwrap();
        assert_eq!(
            config.http_addr,
            "127.0.0.1:9000".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            config.grpc_addr,
            "127.0.0.1:9002".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(config.db_path, "tenant/db");

        for variable in ["HELIX_HTTP_ADDR", "HTTP_ADDR", "GRPC_ADDR"] {
            let error = ServerConfig::from_lookup(|name| {
                (name == variable).then(|| OsString::from("not-an-address"))
            })
            .unwrap_err();
            assert!(matches!(
                &error,
                ServerConfigError::Addr { variable: found, value, .. }
                    if *found == variable && value == "not-an-address"
            ));
            assert!(error
                .to_string()
                .starts_with(&format!("invalid {variable}")));
        }
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
            ServerConfig::from_lookup(|name| values.get(name).map(OsString::from)).unwrap();
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
        assert_eq!(config.required_open_files(), None);

        let default_region = ServerConfig::from_lookup(|name| {
            (name == "S3_BUCKET").then(|| OsString::from("launch-bucket"))
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
            ServerConfig::from_lookup(|name| values.get(name).map(OsString::from)).unwrap();
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
        assert_eq!(config.required_open_files(), None);
    }

    #[test]
    fn data_directory_and_s3_bucket_are_rejected_together() {
        let error = ServerConfig::from_lookup(|name| match name {
            "HELIX_DATA_DIR" => Some("/var/lib/helix".into()),
            "S3_BUCKET" => Some("bucket".into()),
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
            ("S3_BUCKET", OsString::from("bucket")),
            ("HELIX_DISK_CACHE_DIR", root.clone().into_os_string()),
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
        let mut tiers = std::fs::read_dir(&root)
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                assert!(path.is_dir(), "{} is a tier directory", path.display());
                assert_eq!(
                    std::fs::read_dir(&path).unwrap().count(),
                    0,
                    "the write probe is removed from {}",
                    path.display()
                );
                path.file_name().unwrap().to_owned()
            })
            .collect::<Vec<_>>();
        tiers.sort();
        assert_eq!(tiers, ["fts", "object-store", "slate"]);

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
        assert_eq!(slate_db.disk_partitions(), 24_576);
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
        assert_eq!(options.max_open_file_handles, 1000);
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
        assert_eq!(config.required_open_files(), Some(24_576 + 1000 + 1024));
    }

    #[test]
    fn cache_size_overrides_split_the_whole_disk_budget_across_tiers() {
        let directory = tempfile::tempdir().unwrap();
        let disk_bytes = 1024 * MIB + 7;
        let values = BTreeMap::from([
            (
                "HELIX_DATA_DIR",
                directory.path().join("data").into_os_string(),
            ),
            (
                "HELIX_DISK_CACHE_DIR",
                directory.path().join("cache").into_os_string(),
            ),
            (
                "HELIX_DISK_CACHE_MEMORY_BYTES",
                (128 * MIB).to_string().into(),
            ),
            ("HELIX_DISK_CACHE_BYTES", disk_bytes.to_string().into()),
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
        assert_eq!(
            config.required_open_files(),
            Some(slate_db.disk_partitions() as u64 + 1000 + 1024)
        );

        for (label, bytes) in [
            ("minimum", MIN_CACHE_DISK_BYTES),
            ("maximum", MAX_CACHE_DISK_BYTES),
        ] {
            let values = BTreeMap::from([
                ("S3_BUCKET", OsString::from("bucket")),
                (
                    "HELIX_DISK_CACHE_DIR",
                    directory.path().join(label).into_os_string(),
                ),
                ("HELIX_DISK_CACHE_BYTES", bytes.to_string().into()),
            ]);
            let config = ServerConfig::from_lookup(|name| values.get(name).cloned()).unwrap();
            assert!(
                config
                    .required_open_files()
                    .is_some_and(|files| files <= 32_768 + 1000 + 1024),
                "the block cache stays within 32Ki partitions at the {label} budget"
            );
        }
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
                ("S3_BUCKET", OsString::from("bucket")),
                ("HELIX_DISK_CACHE_DIR", directory.path().into()),
                (variable, value.into()),
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

        for bytes in [MIN_CACHE_DISK_BYTES - 1, MAX_CACHE_DISK_BYTES + 1] {
            let values = BTreeMap::from([
                ("S3_BUCKET", OsString::from("bucket")),
                ("HELIX_DISK_CACHE_DIR", directory.path().into()),
                ("HELIX_DISK_CACHE_BYTES", bytes.to_string().into()),
            ]);
            let error = ServerConfig::from_lookup(|name| values.get(name).cloned()).unwrap_err();
            assert!(matches!(
                error,
                ServerConfigError::CacheDiskOutOfRange { bytes: found, minimum, maximum }
                    if found == bytes
                        && minimum == MIN_CACHE_DISK_BYTES
                        && maximum == MAX_CACHE_DISK_BYTES
            ));
            assert!(error.to_string().starts_with("HELIX_DISK_CACHE_BYTES"));
        }
    }

    #[test]
    fn empty_or_uncreatable_cache_directory_fails_startup() {
        let empty = BTreeMap::from([
            ("S3_BUCKET", OsString::from("bucket")),
            ("HELIX_DISK_CACHE_DIR", OsString::new()),
        ]);
        let error = ServerConfig::from_lookup(|name| empty.get(name).cloned()).unwrap_err();
        assert!(matches!(error, ServerConfigError::EmptyCacheDirectory));
        assert!(error.to_string().starts_with("HELIX_DISK_CACHE_DIR"));

        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("not-a-directory");
        std::fs::write(&file, b"occupied").unwrap();
        for root in [file.clone(), file.join("cache")] {
            let values = BTreeMap::from([
                ("HELIX_DATA_DIR", directory.path().as_os_str().to_owned()),
                ("HELIX_DISK_CACHE_DIR", root.clone().into_os_string()),
            ]);
            let error = ServerConfig::from_lookup(|name| values.get(name).cloned()).unwrap_err();
            assert!(
                matches!(&error, ServerConfigError::CacheDirectory { path, .. } if *path == root),
                "{} produced {error:?}",
                root.display()
            );
            assert!(error.to_string().starts_with("HELIX_DISK_CACHE_DIR"));
        }
    }

    /// Covers the write probe itself: the directories exist, so creating
    /// them succeeds and only the write fails. Root bypasses permissions.
    #[cfg(unix)]
    #[test]
    fn read_only_cache_or_tier_directory_fails_the_write_probe() {
        use std::os::unix::fs::PermissionsExt;

        if rustix::process::geteuid().is_root() {
            return;
        }
        for read_only in ["", "slate", "object-store", "fts"] {
            let directory = tempfile::tempdir().unwrap();
            let root = directory.path().join("cache");
            let target = root.join(read_only);
            std::fs::create_dir_all(&target).unwrap();
            std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o555)).unwrap();
            let values = BTreeMap::from([
                ("S3_BUCKET", OsString::from("bucket")),
                ("HELIX_DISK_CACHE_DIR", root.clone().into_os_string()),
            ]);

            let error = ServerConfig::from_lookup(|name| values.get(name).cloned()).unwrap_err();
            assert!(
                matches!(
                    &error,
                    ServerConfigError::CacheDirectory { path, source }
                        if *path == target
                            && source.kind() == std::io::ErrorKind::PermissionDenied
                ),
                "{} produced {error:?}",
                target.display()
            );
            assert!(error.to_string().starts_with("HELIX_DISK_CACHE_DIR"));
            std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
    }

    #[test]
    fn cache_sizes_without_a_cache_directory_are_rejected() {
        for variable in ["HELIX_DISK_CACHE_MEMORY_BYTES", "HELIX_DISK_CACHE_BYTES"] {
            let values = BTreeMap::from([
                ("S3_BUCKET", OsString::from("bucket")),
                (variable, (128 * MIB).to_string().into()),
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
            ("HELIX_DISK_CACHE_DIR", root.clone().into_os_string()),
            (
                "HELIX_DISK_CACHE_MEMORY_BYTES",
                (128 * MIB).to_string().into(),
            ),
            ("HELIX_DISK_CACHE_BYTES", "not-a-number".into()),
        ] {
            let values = BTreeMap::from([(variable, value)]);
            let error = ServerConfig::from_lookup(|name| values.get(name).cloned()).unwrap_err();
            assert!(matches!(
                error,
                ServerConfigError::CacheWithMemoryStorage { variable: found } if found == variable
            ));
            assert!(error.to_string().starts_with(variable));
        }
        assert!(
            !root.exists(),
            "memory storage never creates a cache directory"
        );
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_values_are_rejected_by_name_or_kept_as_paths() {
        use std::os::unix::ffi::OsStringExt;

        let latin1 = || OsString::from_vec(b"caf\xe9".to_vec());
        for (storage, variable) in [
            ("HELIX_DATA_DIR", "HELIX_DISK_CACHE_BYTES"),
            ("HELIX_DATA_DIR", "HELIX_DISK_CACHE_MEMORY_BYTES"),
            ("S3_BUCKET", "S3_BUCKET"),
            ("S3_BUCKET", "S3_REGION"),
            ("S3_BUCKET", "AWS_ALLOW_HTTP"),
            ("DB_PATH", "HELIX_HTTP_ADDR"),
        ] {
            let values = BTreeMap::from([
                (storage, OsString::from("value")),
                (
                    "HELIX_DISK_CACHE_DIR",
                    std::env::temp_dir().into_os_string(),
                ),
                (variable, latin1()),
            ]);
            let error = ServerConfig::from_lookup(|name| values.get(name).cloned()).unwrap_err();
            assert!(
                matches!(error, ServerConfigError::NotUnicode { variable: found } if found == variable),
                "{variable} produced {error:?}"
            );
            assert_eq!(error.to_string(), format!("{variable} is not valid UTF-8"));
        }

        let memory = BTreeMap::from([("HELIX_DISK_CACHE_DIR", latin1())]);
        assert!(matches!(
            ServerConfig::from_lookup(|name| memory.get(name).cloned()).unwrap_err(),
            ServerConfigError::CacheWithMemoryStorage {
                variable: "HELIX_DISK_CACHE_DIR"
            }
        ));
    }

    /// Linux filesystems accept any path bytes; macOS APFS requires UTF-8.
    #[cfg(target_os = "linux")]
    #[test]
    fn non_utf8_cache_directory_is_used_verbatim() {
        use std::os::unix::ffi::OsStringExt;

        let directory = tempfile::tempdir().unwrap();
        let mut root = directory.path().as_os_str().to_owned().into_vec();
        root.extend_from_slice(b"/caf\xe9");
        let root = PathBuf::from(OsString::from_vec(root));
        let values = BTreeMap::from([
            ("S3_BUCKET", OsString::from("bucket")),
            ("HELIX_DISK_CACHE_DIR", root.clone().into_os_string()),
        ]);
        let config = ServerConfig::from_lookup(|name| values.get(name).cloned()).unwrap();
        assert!(matches!(
            &config.storage,
            StorageConfig::S3 { cache: CacheConfig::Hybrid(cache), .. } if cache.root() == root
        ));
        assert!(root.join("slate").is_dir());
    }
}
