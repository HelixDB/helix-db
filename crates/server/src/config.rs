use std::env;
use std::ffi::OsString;
use std::net::{AddrParseError, SocketAddr};
use std::num::{NonZeroU64, NonZeroUsize, ParseIntError};
use std::path::{Path, PathBuf};

use db::HelixDbSource;

/// Resident bytes for the hybrid SlateDB cache's memory tier when
/// `HELIX_DISK_CACHE_MEMORY_BYTES` is unset: the block plus metadata budget the
/// memory-only mode already uses.
///
/// The disk tier still adds RSS outside this budget. Foyer indexes every
/// entry of its `slate/` share in memory, about 50-90 bytes each, and an
/// entry (a 4 KiB SlateDB block plus its header) takes one or two 4 KiB pages
/// on disk. Once the share fills that is roughly 2-9 MiB of RAM per GiB of
/// `HELIX_DISK_CACHE_BYTES`, and a restart briefly needs about twice as much
/// while it rebuilds the index from disk.
const DEFAULT_CACHE_MEMORY_BYTES: NonZeroUsize = NonZeroUsize::new(
    (slatedb::db_cache::DEFAULT_BLOCK_CACHE_CAPACITY
        + slatedb::db_cache::DEFAULT_META_CACHE_CAPACITY) as usize,
)
.expect("SlateDB default cache capacities are nonzero");
/// Disk bytes shared by every disk tier when `HELIX_DISK_CACHE_BYTES` is unset:
/// 4 GiB for object-store parts, 3 GiB for the block cache in 24,576
/// partition files, and 1 GiB for full-text splits. It stays small because a
/// default that does not fit the cache's filesystem fails startup.
const DEFAULT_CACHE_DISK_BYTES: NonZeroUsize =
    NonZeroUsize::new(8 * 1024 * 1024 * 1024).expect("default cache disk budget is nonzero");
/// Smallest disk budget: its 32 MiB object-store share still holds
/// [`OBJECT_STORE_MIN_PARTS`] parts of 128 KiB.
const MIN_CACHE_DISK_BYTES: usize = 64 * 1024 * 1024;
/// Largest disk budget (1 TiB). Its 3/8 block-cache share keeps the block
/// cache within 32Ki partition files, each held open while the server runs,
/// and its full index within roughly 2-9 GiB of RAM (see
/// [`DEFAULT_CACHE_MEMORY_BYTES`]).
const MAX_CACHE_DISK_BYTES: usize = 1024 * 1024 * 1024 * 1024;
/// Fewest parts the object-store tier is split into. A miss fetches and
/// caches a whole part, so parts shrink below SlateDB's 4 MiB default until
/// the tier holds this many; budgets from 2 GiB keep the default.
const OBJECT_STORE_MIN_PARTS: usize = 256;
/// Open files the server needs besides the two disk-cache tiers: listeners,
/// connections, WAL and SST reads, and full-text split files.
const OPEN_FILE_HEADROOM: u64 = 1024;
/// File in the cache root a running server holds an exclusive lock on.
const CACHE_LOCK_FILE: &str = ".helix-cache.lock";
/// Cache root for S3 storage when `HELIX_DISK_CACHE_DIR` is unset: the
/// directory the product image creates for its runtime user.
const DEFAULT_S3_CACHE_DIR: &str = "/var/cache/helix";
/// Seconds after its last use that a full-text split is still exempt from
/// eviction. The DB default of five minutes lets a burst of admissions keep
/// the tier over its share that long. One second, the shortest period, is
/// enough here: the cache lock keeps other servers off the tier, and this
/// server's own open splits hold leases that eviction already skips.
const FTS_EVICTION_GRACE_SECS: u64 = 1;

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
    /// Queued vector/text index policy: the defaults, with the strong search
    /// bounds from `HELIX_STRONG_VECTOR_SEARCH_MAX_PENDING_BYTES` and
    /// `HELIX_STRONG_TEXT_SEARCH_MAX_ANALYSIS_BYTES`.
    pub index_operation_queue: db::config::IndexOperationQueueTuning,
}

impl ServerConfig {
    /// Load server configuration from environment variables.
    ///
    /// Paths may be any bytes; every other variable must be UTF-8. S3 storage
    /// always runs a hybrid disk cache, rooted at `HELIX_DISK_CACHE_DIR` or
    /// else `/var/cache/helix`; with `HELIX_DATA_DIR` the cache is opt-in
    /// through `HELIX_DISK_CACHE_DIR`, and memory storage rejects every cache
    /// variable. Without `HELIX_DISK_CACHE_BYTES`, the default disk budget
    /// must fit the cache's filesystem. `HELIX_DISK_CACHE_WARM=off` stops the
    /// startup warm of the disk cache ([`HybridCache`]).
    /// `HELIX_STRONG_VECTOR_SEARCH_MAX_PENDING_BYTES` and
    /// `HELIX_STRONG_TEXT_SEARCH_MAX_ANALYSIS_BYTES`, each a positive byte
    /// count, replace the strong vector and text search bounds
    /// ([`db::config::IndexOperationQueueTuning::strong_vector_search_max_pending_bytes`],
    /// [`db::config::IndexOperationQueueTuning::strong_text_search_max_analysis_bytes`]).
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
        let tuning = db::config::IndexOperationQueueTuning::default();
        let tuning =
            parse_queue_bytes(&mut lookup, "HELIX_STRONG_VECTOR_SEARCH_MAX_PENDING_BYTES")?
                .map_or(tuning, |bytes| {
                    tuning.with_strong_vector_search_max_pending_bytes(bytes)
                });
        let index_operation_queue =
            parse_queue_bytes(&mut lookup, "HELIX_STRONG_TEXT_SEARCH_MAX_ANALYSIS_BYTES")?
                .map_or(tuning, |bytes| {
                    tuning.with_strong_text_search_max_analysis_bytes(bytes)
                });
        // Last, so every other variable is valid before a cache directory is
        // created.
        let storage = StorageConfig::from_lookup(&mut lookup)?;

        Ok(Self {
            http_addr,
            grpc_addr,
            db_path,
            storage,
            index_operation_queue,
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

    /// Build the DB runtime config for the selected storage, cache, and
    /// index queue policy.
    ///
    /// Memory storage, and disk storage without a hybrid cache, keep
    /// [`db::DbConfig::new`]'s bounded in-memory caches.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    ///
    /// use server::{CacheConfig, HybridCache, ServerConfig, StorageConfig};
    ///
    /// let directory = tempfile::tempdir().unwrap();
    /// let cache = |name: &str| {
    ///     HybridCache::try_new(
    ///         directory.path().join(name),
    ///         NonZeroUsize::new(64 * 1024 * 1024).unwrap(),
    ///         NonZeroUsize::new(1024 * 1024 * 1024).unwrap(),
    ///     )
    ///     .unwrap()
    /// };
    /// let caches_sst_writes = |storage| {
    ///     let config = ServerConfig {
    ///         http_addr: "127.0.0.1:0".parse().unwrap(),
    ///         grpc_addr: "127.0.0.1:0".parse().unwrap(),
    ///         db_path: "db/".to_string(),
    ///         storage,
    ///         index_operation_queue: db::config::IndexOperationQueueTuning::default(),
    ///     };
    ///     let db::config::CacheMode::Hybrid { object_store, .. } =
    ///         config.db_config().cache().mode().clone()
    ///     else {
    ///         panic!("a hybrid cache builds hybrid tiers");
    ///     };
    ///     object_store.to_slate_options().cache_puts
    /// };
    /// // SSTs written to a local data directory are not copied into the cache.
    /// assert!(!caches_sst_writes(StorageConfig::Disk {
    ///     root: directory.path().join("data"),
    ///     cache: CacheConfig::Hybrid(Box::new(cache("disk-cache"))),
    /// }));
    /// // S3 storage always has a disk cache, which also keeps the SSTs it writes.
    /// assert!(caches_sst_writes(StorageConfig::S3 {
    ///     bucket: "bucket".to_string(),
    ///     region: "us-east-1".to_string(),
    ///     endpoint: None,
    ///     allow_http: false,
    ///     cache: Box::new(cache("s3-cache")),
    /// }));
    /// ```
    pub fn db_config(&self) -> db::DbConfig {
        match &self.storage {
            StorageConfig::Memory
            | StorageConfig::Disk {
                cache: CacheConfig::Memory,
                ..
            } => db::DbConfig::new(),
            // SSTs in `HELIX_DATA_DIR` are already on local disk, so caching
            // them as they are written, or warming them at startup, would
            // only copy local files.
            StorageConfig::Disk {
                cache: CacheConfig::Hybrid(cache),
                ..
            } => cache.db_config(false),
            StorageConfig::S3 { cache, .. } => cache.db_config(true),
        }
        .with_index_operation_queue_tuning(self.index_operation_queue)
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
    ///     index_operation_queue: db::config::IndexOperationQueueTuning::default(),
    /// };
    /// assert_eq!(config.required_open_files(), None);
    /// ```
    pub fn required_open_files(&self) -> Option<u64> {
        self.hybrid_cache().map(HybridCache::required_open_files)
    }

    /// The hybrid disk cache in front of durable storage, if one is set.
    pub(crate) fn hybrid_cache(&self) -> Option<&HybridCache> {
        self.storage.hybrid_cache()
    }
}

/// Supported storage backends.
///
/// S3 storage always carries a [`HybridCache`]: without a local disk tier,
/// every read that misses the memory caches is a serial S3 GET, so a vector
/// index larger than the block cache stalls on one GET per graph hop. Local
/// disk storage makes the cache optional ([`CacheConfig`]), and in-memory
/// storage has no durable objects to cache.
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
        /// Memory-plus-disk caches in front of the object store.
        cache: Box<HybridCache>,
    },
}

impl StorageConfig {
    /// The hybrid disk cache in front of durable storage, if one is set.
    pub(crate) fn hybrid_cache(&self) -> Option<&HybridCache> {
        match self {
            Self::Memory
            | Self::Disk {
                cache: CacheConfig::Memory,
                ..
            } => None,
            Self::Disk {
                cache: CacheConfig::Hybrid(cache),
                ..
            }
            | Self::S3 { cache, .. } => Some(cache),
        }
    }

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
                "HELIX_DISK_CACHE_WARM",
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
                // Last, so every other variable is valid before the cache
                // directory is created.
                cache: {
                    let root = lookup("HELIX_DISK_CACHE_DIR")
                        .unwrap_or_else(|| DEFAULT_S3_CACHE_DIR.into());
                    Box::new(HybridCache::from_lookup(lookup, root)?)
                },
            }),
        }
    }
}

/// Local caches placed in front of local disk storage.
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
            return [
                "HELIX_DISK_CACHE_MEMORY_BYTES",
                "HELIX_DISK_CACHE_BYTES",
                "HELIX_DISK_CACHE_WARM",
            ]
            .into_iter()
            .find(|&variable| lookup(variable).is_some())
            .map_or(Ok(Self::Memory), |variable| {
                Err(ServerConfigError::CacheSettingWithoutDirectory { variable })
            });
        };
        HybridCache::from_lookup(lookup, root).map(|cache| Self::Hybrid(Box::new(cache)))
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
/// The full-text tier is filled by searches only; startup warms nothing into
/// it but trims it to its share. Each admission also evicts the least
/// recently used splits down to the share. A trim spares splits admitted or
/// recorded as used in the second before it, and splits held open by
/// searches and the full-text memory cache; those keep the tier over its
/// share until the next admission. After a restart the first search to use
/// a cached split hashes all of it before answering.
///
/// With S3 storage the object-store tier also caches SSTs this server flushes
/// or compacts, so it reads its own writes back from local disk. With
/// `HELIX_DATA_DIR` those SSTs are already local, so the tier caches only the
/// SSTs the server reads.
///
/// Unless `HELIX_DISK_CACHE_WARM` is `off`, the server warms two tiers in the
/// background once storage opens: `slate/` with the index, filter and stats
/// blocks of the newest SSTs, and, with S3 storage only, `object-store/` with
/// the search rows of every Active vector index, reading at most half that
/// tier. Neither delays startup or readiness; queries read through whatever is
/// not warm yet. With `off`, every tier fills only as queries read.
///
/// Only one server may use a cache directory at a time: while its storage is
/// open, a server holds an exclusive lock on `.helix-cache.lock` in the root.
#[derive(Debug, Clone, PartialEq)]
pub struct HybridCache {
    root: PathBuf,
    disk_bytes: usize,
    warm: DiskCacheWarm,
    slate_db: db::config::SlateHybridCacheConfig,
    object_store: db::config::SlateObjectStoreCacheSettings,
    fts: db::config::FtsHybridCacheConfig,
}

/// Whether the server warms its disk cache at startup
/// (`HELIX_DISK_CACHE_WARM`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiskCacheWarm {
    /// Warm in the background once storage opens; the default.
    On,
    /// Warm nothing; every tier fills as queries read.
    Off,
}

impl DiskCacheWarm {
    /// Reads `HELIX_DISK_CACHE_WARM`, `on` or `off` in any case, defaulting
    /// to [`Self::On`] when unset.
    fn from_lookup(
        lookup: &mut impl FnMut(&str) -> Option<OsString>,
    ) -> Result<Self, ServerConfigError> {
        let Some((_, value)) = text(lookup, &["HELIX_DISK_CACHE_WARM"])? else {
            return Ok(Self::On);
        };
        match value.trim().to_ascii_lowercase().as_str() {
            "on" => Ok(Self::On),
            "off" => Ok(Self::Off),
            _ => Err(ServerConfigError::CacheWarm { value }),
        }
    }
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
                // A power of two of at least 128 KiB, so a whole number of KiB
                // as SlateDB requires.
                (1 << (object_store_bytes / OBJECT_STORE_MIN_PARTS).ilog2())
                    .min(object_store_defaults.part_size_bytes),
                false,
                db::config::ObjectStoreWarmLevel::Off,
                object_store_defaults.scan_interval,
                object_store_defaults.max_open_file_handles,
            )?,
            // No startup warm: with a disk tier it would download every
            // active split whole, ignoring this tier's share, and re-hash
            // every cached split on each restart. Splits reach `fts/` only
            // once searches reuse them; startup and each admission evict
            // older splits down to the share.
            fts: db::config::FtsHybridCacheConfig::try_new(
                fts_defaults.memory_bytes(),
                root.join("fts"),
                fts_disk_bytes,
                db::config::FtsWarmConfig::Off,
                FTS_EVICTION_GRACE_SECS,
            )?,
            root,
            disk_bytes: disk_bytes.get(),
            warm: DiskCacheWarm::On,
        };
        [
            cache.root.as_path(),
            cache.slate_db.disk().root(),
            cache.object_store.root(),
            cache.fts.disk().root(),
        ]
        .into_iter()
        .try_for_each(|directory| {
            // Unique, so servers starting together never remove each
            // other's probe.
            let probe =
                directory.join(format!(".helix-cache-write-check-{}", uuid::Uuid::new_v4()));
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

    /// Reads the cache budgets and warm switch, defaulting each one that is
    /// unset, and validates a cache rooted at `root`. They are parsed before
    /// `root` is touched. A default disk budget must also fit the cache's
    /// filesystem ([`Self::fit_default_budget`]); a configured one only
    /// warns when it does not ([`Self::warn_on_disk_shortfall`]).
    fn from_lookup(
        lookup: &mut impl FnMut(&str) -> Option<OsString>,
        root: OsString,
    ) -> Result<Self, ServerConfigError> {
        let memory_bytes = parse_cache_bytes(lookup, "HELIX_DISK_CACHE_MEMORY_BYTES")?
            .unwrap_or(DEFAULT_CACHE_MEMORY_BYTES);
        let warm = DiskCacheWarm::from_lookup(lookup)?;
        match parse_cache_bytes(lookup, "HELIX_DISK_CACHE_BYTES")? {
            Some(disk_bytes) => Self::try_new(PathBuf::from(root), memory_bytes, disk_bytes),
            None => Self::try_new(PathBuf::from(root), memory_bytes, DEFAULT_CACHE_DISK_BYTES)
                .and_then(Self::fit_default_budget),
        }
        .map(|cache| Self { warm, ..cache })
    }

    /// Keeps a default disk budget only where the cache's filesystem has room
    /// for it: its free space plus what the cache already occupies (see
    /// [`Self::disk_shortfall`]). Nobody sized the default for this machine,
    /// and without a volume the cache shares the container runtime's
    /// filesystem, so a default that does not fit fails startup instead of
    /// filling that filesystem. Only Unix measures; elsewhere the default is
    /// kept.
    ///
    /// When free space alone falls short this walks the cache's files, which
    /// at the default budget number in the tens of thousands at most.
    fn fit_default_budget(self) -> Result<Self, ServerConfigError> {
        #[cfg(unix)]
        match self.disk_shortfall() {
            Ok(None) => {}
            Ok(Some(shortfall)) => {
                return Err(ServerConfigError::DefaultCacheDiskTooLarge {
                    path: self.root,
                    budget: self.disk_bytes,
                    shortfall,
                });
            }
            Err(source) => {
                return Err(ServerConfigError::DefaultCacheDiskUnmeasured {
                    path: self.root,
                    source,
                });
            }
        }
        Ok(self)
    }

    /// Cache root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Disk bytes shared by every disk tier (`HELIX_DISK_CACHE_BYTES`).
    pub(crate) const fn disk_bytes(&self) -> usize {
        self.disk_bytes
    }

    /// Whether the server warms this cache at startup (`HELIX_DISK_CACHE_WARM`).
    pub(crate) const fn warms_at_startup(&self) -> bool {
        matches!(self.warm, DiskCacheWarm::On)
    }

    /// This cache as `HELIX_DISK_CACHE_WARM=off` configures it.
    #[cfg(test)]
    pub(crate) fn without_startup_warm_for_tests(self) -> Self {
        Self {
            warm: DiskCacheWarm::Off,
            ..self
        }
    }

    /// Claims this cache directory for one server: takes the exclusive lock
    /// that keeps every other server off it until the returned file is
    /// dropped.
    ///
    /// The tiers assume one owner: a second server with a smaller budget
    /// would delete the block-cache partitions the first still has open. The
    /// lock is advisory and taken only on the server's open path, so a
    /// process embedding `db` may still open several databases on one cache.
    pub(crate) fn claim(&self) -> Result<std::fs::File, ServerConfigError> {
        std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.root.join(CACHE_LOCK_FILE))
            .and_then(|file| file.try_lock().map(|()| file).map_err(std::io::Error::from))
            // Contention converts to `WouldBlock`. Any other failure, in
            // opening the lock file or in locking it, leaves the directory
            // unlockable.
            .map_err(|source| {
                if source.kind() == std::io::ErrorKind::WouldBlock {
                    ServerConfigError::CacheDirectoryInUse {
                        path: self.root.clone(),
                    }
                } else {
                    ServerConfigError::CacheDirectoryLock {
                        path: self.root.clone(),
                        source,
                    }
                }
            })
    }

    /// Logs a warning when the disk budget does not fit the cache's
    /// filesystem. Only a warning: the budget is still enforced, and free
    /// space changes as the data and other files grow.
    ///
    /// It blocks, possibly on a walk of every cached file (see
    /// [`Self::disk_shortfall`]), so the server runs it on a blocking thread
    /// without delaying startup.
    #[cfg(unix)]
    pub(crate) fn warn_on_disk_shortfall(&self) {
        match self.disk_shortfall() {
            Ok(None) => {}
            Ok(Some(shortfall)) => tracing::warn!(
                root = %self.root.display(),
                budget_bytes = self.disk_bytes,
                shortfall_bytes = shortfall,
                "HELIX_DISK_CACHE_BYTES exceeds the free space left for the disk cache; it can fill the filesystem, and durable writes fail too if HELIX_DATA_DIR shares it"
            ),
            Err(error) => tracing::warn!(
                root = %self.root.display(),
                %error,
                "could not compare HELIX_DISK_CACHE_BYTES with the free space for the disk cache"
            ),
        }
    }

    /// Bytes by which the disk budget exceeds the room its filesystem leaves
    /// the cache, or `None` when it fits. The room is the free space plus the
    /// space the cache's files already occupy, so a full cache still fits
    /// after a restart.
    ///
    /// Only when free space alone falls short does this walk every file below
    /// the root, which at the largest budget is on the order of 10^5 files.
    #[cfg(unix)]
    pub(crate) fn disk_shortfall(&self) -> std::io::Result<Option<u64>> {
        let filesystem = rustix::fs::statvfs(&self.root)?;
        let free = filesystem.f_bavail.saturating_mul(filesystem.f_frsize);
        let Some(beyond_free) = (self.disk_bytes as u64)
            .checked_sub(free)
            .filter(|&bytes| bytes > 0)
        else {
            return Ok(None);
        };
        Ok(beyond_free
            .checked_sub(allocated_bytes(&self.root)?)
            .filter(|&shortfall| shortfall > 0))
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
    ///
    /// `remote_store` marks S3 storage. Only then does the object-store tier
    /// keep the SSTs the server flushes or compacts, and only then does
    /// startup warm vector search rows into it: in front of a local durable
    /// store both would copy files already on local disk.
    fn db_config(&self, remote_store: bool) -> db::DbConfig {
        let (slate_warm, vector_part_warm) = match (self.warm, remote_store) {
            (DiskCacheWarm::On, true) => (
                db::config::SlateWarmConfig::default(),
                db::config::VectorPartWarm::Background,
            ),
            (DiskCacheWarm::On, false) => (
                db::config::SlateWarmConfig::default(),
                db::config::VectorPartWarm::Off,
            ),
            (DiskCacheWarm::Off, _) => (
                db::config::SlateWarmConfig::Off,
                db::config::VectorPartWarm::Off,
            ),
        };
        let config = db::DbConfig::new();
        let cache = config
            .cache()
            .clone()
            .with_mode(db::config::CacheMode::Hybrid {
                slate_db: self.slate_db.clone(),
                object_store: self
                    .object_store
                    .clone()
                    .with_cache_puts(remote_store)
                    .with_vector_part_warm(vector_part_warm),
                slate_warm,
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

fn parse_queue_bytes(
    lookup: &mut impl FnMut(&str) -> Option<OsString>,
    variable: &'static str,
) -> Result<Option<NonZeroU64>, ServerConfigError> {
    text(lookup, &[variable])?
        .map(|(_, value)| {
            value
                .parse()
                .map_err(|source| ServerConfigError::IndexQueueBytes {
                    variable,
                    value,
                    source,
                })
        })
        .transpose()
}

/// Disk space the files below `directory` occupy, counting allocated blocks
/// rather than lengths because block-cache partitions are sparse.
///
/// The tiers keep evicting while this walks them, so a file or subdirectory
/// below `directory` that disappears meanwhile counts as empty instead of
/// failing the walk.
#[cfg(unix)]
fn allocated_bytes(directory: &Path) -> std::io::Result<u64> {
    use std::os::unix::fs::MetadataExt;

    std::fs::read_dir(directory)?.try_fold(0_u64, |total, entry| {
        let bytes = entry.and_then(|entry| {
            let metadata = entry.metadata()?;
            if metadata.is_dir() {
                allocated_bytes(&entry.path())
            } else {
                // `st_blocks` counts 512-byte units on every Unix.
                Ok(metadata.blocks() * 512)
            }
        });
        match bytes {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(total),
            bytes => Ok(total.saturating_add(bytes?)),
        }
    })
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
    /// A disk cache setting was supplied for `HELIX_DATA_DIR` storage
    /// without a cache directory.
    #[error("{variable} requires HELIX_DISK_CACHE_DIR with HELIX_DATA_DIR storage")]
    CacheSettingWithoutDirectory {
        /// Cache variable found.
        variable: &'static str,
    },
    /// `HELIX_DISK_CACHE_WARM` was neither `on` nor `off`.
    #[error("invalid HELIX_DISK_CACHE_WARM `{value}`: expected on or off")]
    CacheWarm {
        /// Raw value.
        value: String,
    },
    /// An index queue limit was not a positive integer byte count.
    #[error("invalid {variable} `{value}`: expected a positive byte count")]
    IndexQueueBytes {
        /// Limit variable.
        variable: &'static str,
        /// Raw value.
        value: String,
        /// Parse error.
        source: ParseIntError,
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
    /// created or written. S3 storage uses `/var/cache/helix` unless
    /// `HELIX_DISK_CACHE_DIR` is set, which another user or a read-only root
    /// filesystem cannot write, so the message gives both remedies.
    #[error(
        "HELIX_DISK_CACHE_DIR: `{}` is not a writable directory; mount a writable volume there or set HELIX_DISK_CACHE_DIR to a writable directory",
        .path.display()
    )]
    CacheDirectory {
        /// Unwritable directory.
        path: PathBuf,
        /// Filesystem error.
        source: std::io::Error,
    },
    /// The cache directory's lock file could not be created or locked, for
    /// example on a filesystem without `flock` support.
    #[error("HELIX_DISK_CACHE_DIR: could not lock `{}`", .path.display())]
    CacheDirectoryLock {
        /// Cache directory.
        path: PathBuf,
        /// Filesystem error.
        source: std::io::Error,
    },
    /// Another server holds the cache directory's lock.
    #[error(
        "HELIX_DISK_CACHE_DIR: `{}` is in use by another server; give each running server its own cache directory",
        .path.display()
    )]
    CacheDirectoryInUse {
        /// Locked cache directory.
        path: PathBuf,
    },
    /// `HELIX_DISK_CACHE_BYTES` is unset and its default exceeds the room the
    /// cache's filesystem leaves the cache.
    #[error(
        "HELIX_DISK_CACHE_BYTES is unset and its {budget}-byte default exceeds the space free for the disk cache at `{}` by {shortfall} bytes; set HELIX_DISK_CACHE_BYTES to a budget that fits, or mount a larger volume there",
        .path.display()
    )]
    DefaultCacheDiskTooLarge {
        /// Cache directory.
        path: PathBuf,
        /// Default disk budget.
        budget: usize,
        /// Bytes by which the budget exceeds the room.
        shortfall: u64,
    },
    /// `HELIX_DISK_CACHE_BYTES` is unset and the room for its default could
    /// not be measured.
    #[error(
        "HELIX_DISK_CACHE_BYTES is unset and the space free for its default at `{}` could not be measured; set HELIX_DISK_CACHE_BYTES",
        .path.display()
    )]
    DefaultCacheDiskUnmeasured {
        /// Cache directory.
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
        assert_eq!(
            config.db_config().index_operation_queue(),
            db::config::IndexOperationQueueTuning::default()
        );
        assert_eq!(config.required_open_files(), None);
    }

    /// Each strong search bound is a positive byte count, parsed before
    /// storage creates a cache directory, and both apply together.
    #[test]
    fn the_strong_search_bounds_are_positive_byte_counts() {
        type Tuning = db::config::IndexOperationQueueTuning;
        type WithBound = fn(Tuning, NonZeroU64) -> Tuning;
        const VECTOR: &str = "HELIX_STRONG_VECTOR_SEARCH_MAX_PENDING_BYTES";
        const TEXT: &str = "HELIX_STRONG_TEXT_SEARCH_MAX_ANALYSIS_BYTES";
        let bounds: [(&str, WithBound); 2] = [
            (VECTOR, Tuning::with_strong_vector_search_max_pending_bytes),
            (TEXT, Tuning::with_strong_text_search_max_analysis_bytes),
        ];
        for (variable, bounded) in bounds {
            for (raw, bytes) in [
                ("1", 1),
                ("2147483648", 1 << 31),
                ("18446744073709551615", u64::MAX),
            ] {
                let config = ServerConfig::from_lookup(|name| {
                    (name == variable).then(|| OsString::from(raw))
                })
                .unwrap();
                let expected = bounded(Tuning::default(), NonZeroU64::new(bytes).unwrap());
                assert_eq!(config.index_operation_queue, expected, "{variable}={raw}");
                assert_eq!(
                    config.db_config().index_operation_queue(),
                    expected,
                    "{variable}={raw}"
                );
            }

            for value in ["0", "-1", "", "512MiB", "18446744073709551616"] {
                let error = ServerConfig::from_lookup(|name| {
                    (name == variable).then(|| OsString::from(value))
                })
                .unwrap_err();
                assert!(
                    matches!(
                        &error,
                        ServerConfigError::IndexQueueBytes { variable: reported, value: raw, .. }
                            if *reported == variable && raw == value
                    ),
                    "{variable}={value:?} produced {error:?}"
                );
                assert_eq!(
                    error.to_string(),
                    format!("invalid {variable} `{value}`: expected a positive byte count")
                );
            }
            // Rejected before storage creates a cache directory.
            let error = ServerConfig::from_lookup(|name| match name {
                "S3_BUCKET" => Some(OsString::from("bucket")),
                "HELIX_DISK_CACHE_DIR" => Some(OsString::from("")),
                _ => (name == variable).then(|| OsString::from("0")),
            })
            .unwrap_err();
            assert!(
                matches!(error, ServerConfigError::IndexQueueBytes { .. }),
                "{variable}: {error:?}"
            );
        }

        let config = ServerConfig::from_lookup(|name| match name {
            VECTOR => Some(OsString::from("1048576")),
            TEXT => Some(OsString::from("2097152")),
            _ => None,
        })
        .unwrap();
        assert_eq!(
            config.index_operation_queue,
            Tuning::default()
                .with_strong_vector_search_max_pending_bytes(NonZeroU64::new(1 << 20).unwrap())
                .with_strong_text_search_max_analysis_bytes(NonZeroU64::new(2 << 20).unwrap())
        );
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
        let directory = tempfile::tempdir().unwrap();
        let cache_root = directory.path().join("cache");
        let values = BTreeMap::from([
            ("S3_BUCKET", OsString::from("launch-bucket")),
            ("AWS_REGION", "eu-west-2".into()),
            ("AWS_DEFAULT_REGION", "ignored".into()),
            ("AWS_ENDPOINT_URL_S3", "http://seaweedfs:8333".into()),
            ("AWS_ALLOW_HTTP", "TRUE".into()),
            ("HELIX_DISK_CACHE_DIR", cache_root.clone().into_os_string()),
            (
                "HELIX_DISK_CACHE_BYTES",
                MIN_CACHE_DISK_BYTES.to_string().into(),
            ),
        ]);
        let config = ServerConfig::from_lookup(|name| values.get(name).cloned()).unwrap();
        assert_eq!(
            config.storage,
            StorageConfig::S3 {
                bucket: "launch-bucket".to_string(),
                region: "eu-west-2".to_string(),
                endpoint: Some("http://seaweedfs:8333".to_string()),
                allow_http: true,
                cache: Box::new(
                    HybridCache::try_new(
                        &cache_root,
                        DEFAULT_CACHE_MEMORY_BYTES,
                        NonZeroUsize::new(MIN_CACHE_DISK_BYTES).unwrap()
                    )
                    .unwrap()
                ),
            }
        );

        let default_region = ServerConfig::from_lookup(|name| match name {
            "S3_BUCKET" => Some("launch-bucket".into()),
            "HELIX_DISK_CACHE_DIR" => Some(cache_root.clone().into_os_string()),
            "HELIX_DISK_CACHE_BYTES" => Some(MIN_CACHE_DISK_BYTES.to_string().into()),
            _ => None,
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

    /// The default disk budget is set explicitly, so the split it documents
    /// does not depend on the free space the default itself must fit.
    #[test]
    fn cache_directory_enables_hybrid_tiers_with_documented_defaults() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("nested").join("cache");
        let values = BTreeMap::from([
            ("S3_BUCKET", OsString::from("bucket")),
            ("HELIX_DISK_CACHE_DIR", root.clone().into_os_string()),
            (
                "HELIX_DISK_CACHE_BYTES",
                DEFAULT_CACHE_DISK_BYTES.to_string().into(),
            ),
        ]);
        let config = ServerConfig::from_lookup(|name| values.get(name).cloned()).unwrap();

        assert!(matches!(
            &config.storage,
            StorageConfig::S3 { cache, .. } if cache.root() == root
        ));
        // Each tier is a directory (`read_dir` succeeds) left empty by the
        // removed write probe.
        let mut tiers = std::fs::read_dir(&root)
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                (
                    path.file_name().unwrap().to_owned(),
                    std::fs::read_dir(&path).map(Iterator::count).ok(),
                )
            })
            .collect::<Vec<_>>();
        tiers.sort();
        assert_eq!(
            tiers,
            [
                (OsString::from("fts"), Some(0)),
                (OsString::from("object-store"), Some(0)),
                (OsString::from("slate"), Some(0)),
            ]
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
        assert_eq!(slate_db.disk().bytes(), 3 * 1024 * MIB);
        assert_eq!(slate_db.disk_block_bytes(), 128 * 1024);
        assert_eq!(slate_db.disk_partitions(), 24_576);
        assert_eq!(object_store.root(), root.join("object-store"));
        assert_eq!(object_store.warm(), db::config::ObjectStoreWarmLevel::Off);
        let options = object_store.to_slate_options();
        assert_eq!(options.max_cache_size_bytes, Some(4 * 1024 * MIB));
        assert_eq!(options.part_size_bytes, 4 * MIB);
        assert_eq!(options.max_open_file_handles, 1000);
        assert!(options.cache_puts);
        assert_eq!(
            object_store.vector_part_warm(),
            db::config::VectorPartWarm::Background
        );
        assert_eq!(slate_warm, db::config::SlateWarmConfig::default());
        assert_eq!(fts.disk_root(), root.join("fts"));
        assert_eq!(fts.disk_bytes(), 1024 * 1024 * 1024);
        assert_eq!(
            fts.warm_mode(),
            db::config::CacheWarmMode::Off,
            "startup never downloads splits into the full-text disk tier"
        );
        assert_eq!(
            fts.generation_grace_period(),
            std::time::Duration::from_secs(1),
            "only splits used in the last second may exceed the full-text share"
        );
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
        assert!(
            !object_store.to_slate_options().cache_puts,
            "SSTs written to HELIX_DATA_DIR are not copied into the cache"
        );
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
    fn small_budgets_split_the_object_store_tier_into_smaller_parts() {
        let directory = tempfile::tempdir().unwrap();
        for (disk_bytes, part_bytes) in [
            (MIN_CACHE_DISK_BYTES, 128 * 1024),
            (MIN_CACHE_DISK_BYTES * 3 / 2, 128 * 1024),
            (1024 * MIB, 2 * MIB),
            (2048 * MIB, 4 * MIB),
            (MAX_CACHE_DISK_BYTES, 4 * MIB),
        ] {
            let options = HybridCache::try_new(
                directory.path().join(disk_bytes.to_string()),
                NonZeroUsize::new(MIB).unwrap(),
                NonZeroUsize::new(disk_bytes).unwrap(),
            )
            .unwrap()
            .object_store
            .to_slate_options();
            assert_eq!(options.part_size_bytes, part_bytes, "{disk_bytes}");
            assert!(
                options.max_cache_size_bytes.unwrap() / part_bytes >= OBJECT_STORE_MIN_PARTS,
                "a {disk_bytes}-byte budget leaves the tier at least {OBJECT_STORE_MIN_PARTS} parts"
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
                "{root:?} produced {error:?}"
            );
            assert_eq!(
                error.to_string(),
                format!(
                    "HELIX_DISK_CACHE_DIR: `{}` is not a writable directory; mount a writable volume there or set HELIX_DISK_CACHE_DIR to a writable directory",
                    root.display()
                )
            );
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
                "{target:?} produced {error:?}"
            );
            assert!(error.to_string().starts_with("HELIX_DISK_CACHE_DIR"));
            std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
    }

    #[test]
    fn caches_validated_together_keep_their_own_write_probes() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("cache");
        let validate = || {
            HybridCache::try_new(
                &root,
                NonZeroUsize::new(MIB).unwrap(),
                NonZeroUsize::new(MIN_CACHE_DISK_BYTES).unwrap(),
            )
        };
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..50 {
                        validate().unwrap();
                    }
                });
            }
        });
        let mut entries = std::fs::read_dir(&root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        entries.sort();
        assert_eq!(
            entries,
            ["fts", "object-store", "slate"].map(OsString::from),
            "every probe was removed by the thread that wrote it"
        );
    }

    #[test]
    fn cache_lock_admits_one_holder_until_dropped() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("cache");
        let cache = HybridCache::try_new(
            &root,
            NonZeroUsize::new(MIB).unwrap(),
            NonZeroUsize::new(MIN_CACHE_DISK_BYTES).unwrap(),
        )
        .unwrap();

        let held = cache.claim().unwrap();
        let error = cache.claim().unwrap_err();
        assert!(matches!(
            &error,
            ServerConfigError::CacheDirectoryInUse { path } if *path == root
        ));
        assert!(error.to_string().starts_with("HELIX_DISK_CACHE_DIR"));
        drop(held);
        drop(cache.claim().unwrap());

        std::fs::remove_dir_all(&root).unwrap();
        let error = cache.claim().unwrap_err();
        assert!(matches!(
            &error,
            ServerConfigError::CacheDirectoryLock { path, source }
                if *path == root && source.kind() == std::io::ErrorKind::NotFound
        ));
        assert_eq!(
            error.to_string(),
            format!("HELIX_DISK_CACHE_DIR: could not lock `{}`", root.display())
        );
    }

    #[cfg(unix)]
    #[test]
    fn allocated_bytes_count_written_blocks_below_the_root_but_not_sparse_lengths() {
        use std::io::Write;
        use std::os::unix::fs::MetadataExt;

        let directory = tempfile::tempdir().unwrap();
        let nested = directory.path().join("object-store").join("db");
        std::fs::create_dir_all(&nested).unwrap();
        // Random bytes, so filesystems that compress (ZFS, btrfs) still
        // allocate blocks for them.
        let incompressible = (0..MIB / 16)
            .flat_map(|_| uuid::Uuid::new_v4().into_bytes())
            .collect::<Vec<_>>();
        let files = [
            directory.path().join("written"),
            nested.join("written"),
            directory.path().join("sparse"),
        ];
        for path in &files[..2] {
            let mut file = std::fs::File::create(path).unwrap();
            file.write_all(&incompressible).unwrap();
            file.sync_all().unwrap();
        }
        let sparse = std::fs::File::create(&files[2]).unwrap();
        sparse.set_len(64 * MIB as u64).unwrap();
        sparse.sync_all().unwrap();

        // Some filesystems account written blocks lazily, so compare with
        // each file's own block count, which only grows, around the walk.
        let blocks = |path: &PathBuf| std::fs::metadata(path).unwrap().blocks() * 512;
        let before = files.iter().map(blocks).sum::<u64>();
        let allocated = allocated_bytes(directory.path()).unwrap();
        let after = files.iter().map(blocks).sum::<u64>();
        assert!(
            (before..=after).contains(&allocated),
            "{allocated} is the sum of every file's blocks, {before}..={after}"
        );
        assert!(
            (blocks(&files[2]) + 1..64 * MIB as u64).contains(&allocated),
            "written blocks count, a 64 MiB sparse length does not: {allocated}"
        );
        assert!(allocated_bytes(&directory.path().join("missing")).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn disk_budget_shortfall_is_what_exceeds_free_space_plus_the_cache() {
        let directory = tempfile::tempdir().unwrap();
        let mut cache = HybridCache::try_new(
            directory.path().join("cache"),
            NonZeroUsize::new(MIB).unwrap(),
            NonZeroUsize::new(MIN_CACHE_DISK_BYTES).unwrap(),
        )
        .unwrap();
        assert_eq!(
            cache.disk_shortfall().unwrap(),
            None,
            "the minimum budget fits the test filesystem"
        );
        cache.warn_on_disk_shortfall();

        cache.disk_bytes = usize::MAX;
        let filesystem = rustix::fs::statvfs(cache.root()).unwrap();
        let shortfall = cache.disk_shortfall().unwrap().unwrap();
        assert!(
            shortfall >= usize::MAX as u64 - filesystem.f_blocks * filesystem.f_frsize,
            "no filesystem leaves room for the whole address space"
        );
        cache.warn_on_disk_shortfall();

        std::fs::remove_dir_all(cache.root()).unwrap();
        assert!(cache.disk_shortfall().is_err());
        cache.warn_on_disk_shortfall();
    }

    /// An unreadable tier makes the walk fail, which shows that a budget
    /// free space covers never walks. Root bypasses permissions.
    #[cfg(unix)]
    #[test]
    fn disk_budget_within_free_space_skips_walking_the_cache() {
        use std::os::unix::fs::PermissionsExt;

        if rustix::process::geteuid().is_root() {
            return;
        }
        let directory = tempfile::tempdir().unwrap();
        let mut cache = HybridCache::try_new(
            directory.path().join("cache"),
            NonZeroUsize::new(MIB).unwrap(),
            NonZeroUsize::new(MIN_CACHE_DISK_BYTES).unwrap(),
        )
        .unwrap();
        let unreadable = cache.root().join("slate");
        std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o000)).unwrap();

        let fits = cache.disk_shortfall();
        cache.disk_bytes = usize::MAX;
        let walked = cache.disk_shortfall();
        std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(fits.unwrap(), None);
        assert_eq!(
            walked.unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied
        );
    }

    /// A default budget fails startup where the cache's filesystem cannot
    /// hold it, and the message names the variable that sets another.
    #[cfg(unix)]
    #[test]
    fn default_disk_budget_must_fit_the_cache_filesystem() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("cache");
        let mut cache = HybridCache::try_new(
            &root,
            NonZeroUsize::new(MIB).unwrap(),
            NonZeroUsize::new(MIN_CACHE_DISK_BYTES).unwrap(),
        )
        .unwrap();
        assert_eq!(
            cache.clone().fit_default_budget().unwrap(),
            cache,
            "the minimum budget fits the test filesystem"
        );

        cache.disk_bytes = usize::MAX;
        let error = cache.clone().fit_default_budget().unwrap_err();
        assert!(
            matches!(
                &error,
                ServerConfigError::DefaultCacheDiskTooLarge { path, budget: usize::MAX, shortfall }
                    if *path == root && *shortfall > 0
            ),
            "{error:?}"
        );
        let message = error.to_string();
        assert!(
            message.starts_with(&format!(
                "HELIX_DISK_CACHE_BYTES is unset and its {}-byte default exceeds the space free for the disk cache at `{}` by ",
                usize::MAX,
                root.display()
            )) && message.ends_with(
                " bytes; set HELIX_DISK_CACHE_BYTES to a budget that fits, or mount a larger volume there"
            ),
            "{message}"
        );

        std::fs::remove_dir_all(&root).unwrap();
        let error = cache.fit_default_budget().unwrap_err();
        assert!(
            matches!(
                &error,
                ServerConfigError::DefaultCacheDiskUnmeasured { path, source }
                    if *path == root && source.kind() == std::io::ErrorKind::NotFound
            ),
            "{error:?}"
        );
        assert_eq!(
            error.to_string(),
            format!(
                "HELIX_DISK_CACHE_BYTES is unset and the space free for its default at `{}` could not be measured; set HELIX_DISK_CACHE_BYTES",
                root.display()
            )
        );
    }

    /// An unset `HELIX_DISK_CACHE_BYTES` takes the default where it fits and
    /// fails startup where it does not, while a set budget is kept whatever
    /// the free space. Which outcome the default gets depends on the test
    /// filesystem, so both are asserted without branching on it.
    #[cfg(unix)]
    #[test]
    fn unset_disk_budget_defaults_only_where_it_fits() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("cache");
        let default =
            HybridCache::try_new(&root, DEFAULT_CACHE_MEMORY_BYTES, DEFAULT_CACHE_DISK_BYTES)
                .unwrap();
        let fits = default.disk_shortfall().unwrap().is_none();
        let values = BTreeMap::from([
            ("S3_BUCKET", OsString::from("bucket")),
            ("HELIX_DISK_CACHE_DIR", root.clone().into_os_string()),
        ]);
        let unset = ServerConfig::from_lookup(|name| values.get(name).cloned());
        assert_eq!(
            unset.as_ref().ok().and_then(ServerConfig::hybrid_cache),
            fits.then_some(&default)
        );
        let too_large = ServerConfigError::DefaultCacheDiskTooLarge {
            path: root.clone(),
            budget: DEFAULT_CACHE_DISK_BYTES.get(),
            shortfall: 1,
        };
        assert_eq!(
            unset.as_ref().err().map(std::mem::discriminant),
            (!fits).then_some(std::mem::discriminant(&too_large))
        );

        // The 1 TiB maximum exceeds most test filesystems, yet only warns.
        let set = BTreeMap::from([
            ("S3_BUCKET", OsString::from("bucket")),
            ("HELIX_DISK_CACHE_DIR", root.clone().into_os_string()),
            (
                "HELIX_DISK_CACHE_BYTES",
                MAX_CACHE_DISK_BYTES.to_string().into(),
            ),
        ]);
        assert!(ServerConfig::from_lookup(|name| set.get(name).cloned()).is_ok());
    }

    /// `HELIX_DISK_CACHE_WARM` switches both startup warms, in any case and
    /// with surrounding whitespace, and unset means on. Only S3 storage warms
    /// vector rows into the object-store tier.
    #[test]
    fn disk_cache_warm_switches_both_startup_warms() {
        use db::config::{SlateWarmConfig, VectorPartWarm};

        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("cache").into_os_string();
        let warms = |storage: (&'static str, &str), warm: Option<&str>| {
            let mut values = BTreeMap::from([
                (storage.0, OsString::from(storage.1)),
                ("HELIX_DISK_CACHE_DIR", root.clone()),
                (
                    "HELIX_DISK_CACHE_BYTES",
                    MIN_CACHE_DISK_BYTES.to_string().into(),
                ),
            ]);
            values.extend(warm.map(|value| ("HELIX_DISK_CACHE_WARM", OsString::from(value))));
            let config = ServerConfig::from_lookup(|name| values.get(name).cloned())
                .unwrap()
                .db_config();
            let cache = config.cache();
            (
                cache
                    .slate_warm()
                    .cloned()
                    .expect("a disk cache has a SlateDB warm policy"),
                cache
                    .object_store_cache()
                    .expect("a disk cache has an object-store tier")
                    .vector_part_warm(),
            )
        };
        let s3 = ("S3_BUCKET", "bucket");
        let disk = ("HELIX_DATA_DIR", "/var/lib/helix");
        let on = SlateWarmConfig::default;
        for (storage, warm, expected) in [
            (s3, None, (on(), VectorPartWarm::Background)),
            (s3, Some("on"), (on(), VectorPartWarm::Background)),
            (s3, Some(" ON\n"), (on(), VectorPartWarm::Background)),
            (s3, Some("off"), (SlateWarmConfig::Off, VectorPartWarm::Off)),
            (s3, Some("Off"), (SlateWarmConfig::Off, VectorPartWarm::Off)),
            (disk, None, (on(), VectorPartWarm::Off)),
            (disk, Some("on"), (on(), VectorPartWarm::Off)),
            (
                disk,
                Some("off"),
                (SlateWarmConfig::Off, VectorPartWarm::Off),
            ),
        ] {
            assert_eq!(
                warms(storage, warm),
                expected,
                "{storage:?} with HELIX_DISK_CACHE_WARM={warm:?}"
            );
        }
    }

    /// Any other value fails startup naming the variable, before the cache
    /// directory is created.
    #[test]
    fn invalid_disk_cache_warm_names_the_variable() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("cache");
        for value in ["", "true", "1", "background", "blocking", "o n"] {
            let values = BTreeMap::from([
                ("S3_BUCKET", OsString::from("bucket")),
                ("HELIX_DISK_CACHE_DIR", root.clone().into_os_string()),
                ("HELIX_DISK_CACHE_WARM", OsString::from(value)),
            ]);
            let error = ServerConfig::from_lookup(|name| values.get(name).cloned()).unwrap_err();
            assert!(
                matches!(&error, ServerConfigError::CacheWarm { value: raw } if raw == value),
                "{value:?} produced {error:?}"
            );
            assert_eq!(
                error.to_string(),
                format!("invalid HELIX_DISK_CACHE_WARM `{value}`: expected on or off")
            );
        }
        assert!(
            !root.exists(),
            "the warm switch is read before the cache directory"
        );
    }

    #[test]
    fn disk_storage_rejects_cache_settings_without_a_cache_directory() {
        for (variable, value) in [
            ("HELIX_DISK_CACHE_MEMORY_BYTES", (128 * MIB).to_string()),
            ("HELIX_DISK_CACHE_BYTES", (128 * MIB).to_string()),
            ("HELIX_DISK_CACHE_WARM", "off".to_string()),
        ] {
            let values = BTreeMap::from([
                ("HELIX_DATA_DIR", OsString::from("/var/lib/helix")),
                (variable, value.into()),
            ]);
            let error = ServerConfig::from_lookup(|name| values.get(name).cloned()).unwrap_err();
            assert!(matches!(
                error,
                ServerConfigError::CacheSettingWithoutDirectory { variable: found }
                    if found == variable
            ));
            assert_eq!(
                error.to_string(),
                format!("{variable} requires HELIX_DISK_CACHE_DIR with HELIX_DATA_DIR storage")
            );
        }
    }

    /// S3 reads the budgets without `HELIX_DISK_CACHE_DIR`, and validates them
    /// before the default directory is touched.
    #[test]
    fn s3_reads_cache_sizes_without_a_cache_directory() {
        for (variable, value) in [
            ("HELIX_DISK_CACHE_MEMORY_BYTES", "0"),
            ("HELIX_DISK_CACHE_BYTES", "not-a-number"),
        ] {
            let values = BTreeMap::from([("S3_BUCKET", "bucket"), (variable, value)]);
            let error =
                ServerConfig::from_lookup(|name| values.get(name).map(OsString::from)).unwrap_err();
            assert!(
                matches!(
                    &error,
                    ServerConfigError::CacheBytes { variable: found, value: raw, .. }
                        if *found == variable && raw == value
                ),
                "{variable}={value} produced {error:?}"
            );
        }

        let values = BTreeMap::from([
            ("S3_BUCKET", "bucket".to_string()),
            (
                "HELIX_DISK_CACHE_BYTES",
                (MIN_CACHE_DISK_BYTES - 1).to_string(),
            ),
        ]);
        let error =
            ServerConfig::from_lookup(|name| values.get(name).map(OsString::from)).unwrap_err();
        assert!(matches!(
            error,
            ServerConfigError::CacheDiskOutOfRange { bytes, .. } if bytes == MIN_CACHE_DISK_BYTES - 1
        ));
    }

    /// Without `HELIX_DISK_CACHE_DIR`, S3 caches in the image's directory,
    /// which outside the image a non-root user cannot create, so startup
    /// fails naming the variable to set. The test never creates the
    /// directory: it skips root, and a machine that already has one.
    #[cfg(unix)]
    #[test]
    fn s3_defaults_the_cache_directory_and_names_the_variable_when_unwritable() {
        if rustix::process::geteuid().is_root() || Path::new(DEFAULT_S3_CACHE_DIR).exists() {
            return;
        }
        let values = BTreeMap::from([
            ("S3_BUCKET", "bucket".to_string()),
            ("HELIX_DISK_CACHE_BYTES", MIN_CACHE_DISK_BYTES.to_string()),
        ]);
        let error =
            ServerConfig::from_lookup(|name| values.get(name).map(OsString::from)).unwrap_err();
        assert!(
            matches!(
                &error,
                ServerConfigError::CacheDirectory { path, .. } if path == Path::new(DEFAULT_S3_CACHE_DIR)
            ),
            "{error:?}"
        );
        assert_eq!(
            error.to_string(),
            "HELIX_DISK_CACHE_DIR: `/var/cache/helix` is not a writable directory; mount a writable volume there or set HELIX_DISK_CACHE_DIR to a writable directory"
        );
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
            ("HELIX_DISK_CACHE_WARM", "off".into()),
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
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("cache");
        for (storage, variable) in [
            ("HELIX_DATA_DIR", "HELIX_DISK_CACHE_BYTES"),
            ("HELIX_DATA_DIR", "HELIX_DISK_CACHE_MEMORY_BYTES"),
            ("S3_BUCKET", "S3_BUCKET"),
            ("S3_BUCKET", "S3_REGION"),
            ("S3_BUCKET", "AWS_ALLOW_HTTP"),
            ("S3_BUCKET", "HELIX_DISK_CACHE_WARM"),
            ("HELIX_DATA_DIR", "HELIX_DISK_CACHE_WARM"),
            ("DB_PATH", "HELIX_HTTP_ADDR"),
        ] {
            let values = BTreeMap::from([
                (storage, OsString::from("value")),
                ("HELIX_DISK_CACHE_DIR", root.clone().into_os_string()),
                (variable, latin1()),
            ]);
            let error = ServerConfig::from_lookup(|name| values.get(name).cloned()).unwrap_err();
            assert!(
                matches!(error, ServerConfigError::NotUnicode { variable: found } if found == variable),
                "{variable} produced {error:?}"
            );
            assert_eq!(error.to_string(), format!("{variable} is not valid UTF-8"));
            // Every other variable is read before the cache directory.
            assert!(!root.exists(), "{variable} created the cache directory");
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
            (
                "HELIX_DISK_CACHE_BYTES",
                MIN_CACHE_DISK_BYTES.to_string().into(),
            ),
        ]);
        let config = ServerConfig::from_lookup(|name| values.get(name).cloned()).unwrap();
        assert!(matches!(
            &config.storage,
            StorageConfig::S3 { cache, .. } if cache.root() == root
        ));
        assert!(root.join("slate").is_dir());
    }
}
