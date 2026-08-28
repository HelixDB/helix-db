//! Validated runtime cache policy for SlateDB and vector search.
//!
//! These settings control process memory and startup behavior only. They are
//! never persisted into index catalogs or physical row formats.

use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use super::utils::{ConfigError, ConfigResult, DiskCacheConfig, NonEmptyPathBuf};

/// Startup cache warm behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CacheWarmMode {
    /// Complete best-effort warming before database open returns.
    Blocking,
    /// Return from open and warm in an owned background task.
    #[default]
    Background,
    /// Skip proactive warming while retaining demand caching.
    Off,
}

impl FromStr for CacheWarmMode {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "blocking" => Ok(Self::Blocking),
            "background" => Ok(Self::Background),
            "off" => Ok(Self::Off),
            other => Err(format!(
                "invalid cache warm mode '{other}', expected blocking, background, or off"
            )),
        }
    }
}

/// Object-store disk cache startup preload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ObjectStoreWarmLevel {
    /// Do not preload object-store cache files on startup.
    #[default]
    Off,
    /// Preload only L0 object-store cache files on startup.
    L0,
    /// Preload every discovered object-store cache file on startup.
    All,
}

impl ObjectStoreWarmLevel {
    /// Convert to the SlateDB preload level.
    pub const fn to_slate_preload(self) -> Option<slatedb::config::PreloadLevel> {
        match self {
            Self::Off => None,
            Self::L0 => Some(slatedb::config::PreloadLevel::L0Sst),
            Self::All => Some(slatedb::config::PreloadLevel::AllSst),
        }
    }
}

impl FromStr for ObjectStoreWarmLevel {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "off" => Ok(Self::Off),
            "l0" => Ok(Self::L0),
            "all" => Ok(Self::All),
            other => Err(format!(
                "invalid object-store warm level '{other}', expected off, l0, or all"
            )),
        }
    }
}

/// Checked settings for SlateDB's block/meta Foyer hybrid cache.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlateHybridCacheConfig {
    memory_bytes: NonZeroUsize,
    disk: DiskCacheConfig,
}

impl SlateHybridCacheConfig {
    /// Build SlateDB hybrid cache settings.
    ///
    /// ```
    /// # use db::config::SlateHybridCacheConfig;
    /// assert!(SlateHybridCacheConfig::try_new(64 * 1024 * 1024, "/tmp/slate", 1024).is_ok());
    /// assert!(SlateHybridCacheConfig::try_new(0, "/tmp/slate", 1024).is_err());
    /// ```
    pub fn try_new(
        memory_bytes: usize,
        disk_root: impl Into<PathBuf>,
        disk_bytes: usize,
    ) -> ConfigResult<Self> {
        Ok(Self {
            memory_bytes: NonZeroUsize::new(memory_bytes)
                .ok_or_else(|| ConfigError::new("Slate hybrid cache memory must be nonzero"))?,
            disk: DiskCacheConfig::try_new(disk_root, disk_bytes)?,
        })
    }

    /// Resident memory capacity in bytes.
    pub const fn memory_bytes(&self) -> usize {
        self.memory_bytes.get()
    }

    /// Disk tier settings.
    pub const fn disk(&self) -> &DiskCacheConfig {
        &self.disk
    }
}

/// Checked capacities for SlateDB's in-memory block and metadata caches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlateMemoryCacheConfig {
    block_bytes: NonZeroU64,
    metadata_bytes: NonZeroU64,
}

impl SlateMemoryCacheConfig {
    /// Build independently bounded block and metadata cache tiers.
    ///
    /// ```
    /// use db::config::SlateMemoryCacheConfig;
    ///
    /// let cache = SlateMemoryCacheConfig::try_new(48 * 1024 * 1024, 16 * 1024 * 1024)?;
    /// assert_eq!(cache.total_bytes(), 64 * 1024 * 1024);
    /// # Ok::<(), db::config::ConfigError>(())
    /// ```
    pub fn try_new(block_bytes: u64, metadata_bytes: u64) -> ConfigResult<Self> {
        Ok(Self {
            block_bytes: NonZeroU64::new(block_bytes)
                .ok_or_else(|| ConfigError::new("Slate block cache must be nonzero"))?,
            metadata_bytes: NonZeroU64::new(metadata_bytes)
                .ok_or_else(|| ConfigError::new("Slate metadata cache must be nonzero"))?,
        })
    }

    /// Resident block-cache capacity.
    pub const fn block_bytes(self) -> u64 {
        self.block_bytes.get()
    }

    /// Resident metadata-cache capacity.
    pub const fn metadata_bytes(self) -> u64 {
        self.metadata_bytes.get()
    }

    /// Combined resident capacity.
    pub const fn total_bytes(self) -> u64 {
        self.block_bytes
            .get()
            .saturating_add(self.metadata_bytes.get())
    }
}

const DEFAULT_SLATE_WARM_CONCURRENCY: usize = 4;
const DEFAULT_SLATE_WARM_SST_LIMIT: usize = 256;

/// Startup policy for SlateDB block/meta-cache warming.
///
/// Warming targets immutable SST index, filter, and stats entries only. Data
/// blocks remain demand-filled until Helix has a trustworthy hot-range signal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlateWarmConfig {
    /// Do not proactively warm SlateDB cache entries.
    Off,
    /// Warm after open returns.
    Background {
        /// Maximum concurrent SST warm operations.
        concurrency: NonZeroUsize,
        /// Maximum newest physical SSTs considered by one pass.
        startup_sst_limit: NonZeroUsize,
    },
    /// Warm before open returns.
    Blocking {
        /// Maximum concurrent SST warm operations.
        concurrency: NonZeroUsize,
        /// Maximum newest physical SSTs considered by one pass.
        startup_sst_limit: NonZeroUsize,
    },
}

impl SlateWarmConfig {
    /// Build a checked background SlateDB warm policy.
    pub fn background(concurrency: usize, startup_sst_limit: usize) -> ConfigResult<Self> {
        Ok(Self::Background {
            concurrency: NonZeroUsize::new(concurrency)
                .ok_or_else(|| ConfigError::new("Slate warm concurrency must be nonzero"))?,
            startup_sst_limit: NonZeroUsize::new(startup_sst_limit)
                .ok_or_else(|| ConfigError::new("Slate warm SST limit must be nonzero"))?,
        })
    }

    /// Build a checked blocking SlateDB warm policy.
    pub fn blocking(concurrency: usize, startup_sst_limit: usize) -> ConfigResult<Self> {
        Ok(Self::Blocking {
            concurrency: NonZeroUsize::new(concurrency)
                .ok_or_else(|| ConfigError::new("Slate warm concurrency must be nonzero"))?,
            startup_sst_limit: NonZeroUsize::new(startup_sst_limit)
                .ok_or_else(|| ConfigError::new("Slate warm SST limit must be nonzero"))?,
        })
    }

    /// Resolved warm mode.
    pub const fn mode(&self) -> CacheWarmMode {
        match self {
            Self::Off => CacheWarmMode::Off,
            Self::Background { .. } => CacheWarmMode::Background,
            Self::Blocking { .. } => CacheWarmMode::Blocking,
        }
    }

    /// Maximum concurrent SST operations. Off resolves to one for callers
    /// constructing a semaphore before checking the mode.
    pub const fn concurrency(&self) -> usize {
        match self {
            Self::Off => 1,
            Self::Background { concurrency, .. } | Self::Blocking { concurrency, .. } => {
                concurrency.get()
            }
        }
    }

    /// Maximum newest physical SSTs considered by one warm pass.
    pub const fn startup_sst_limit(&self) -> usize {
        match self {
            Self::Off => 0,
            Self::Background {
                startup_sst_limit, ..
            }
            | Self::Blocking {
                startup_sst_limit, ..
            } => startup_sst_limit.get(),
        }
    }
}

impl Default for SlateWarmConfig {
    fn default() -> Self {
        Self::background(DEFAULT_SLATE_WARM_CONCURRENCY, DEFAULT_SLATE_WARM_SST_LIMIT)
            .expect("default Slate warm policy is valid")
    }
}

/// Checked settings for SlateDB's object-store disk cache.
#[derive(Debug, Clone, PartialEq)]
pub struct SlateObjectStoreCacheSettings {
    root: NonEmptyPathBuf,
    max_cache_size_bytes: Option<NonZeroUsize>,
    part_size_bytes: NonZeroUsize,
    cache_puts: bool,
    warm: ObjectStoreWarmLevel,
    scan_interval: Option<Duration>,
    max_open_file_handles: NonZeroUsize,
}

impl SlateObjectStoreCacheSettings {
    /// Build object-store cache settings.
    pub fn try_new(
        root: impl Into<PathBuf>,
        max_cache_size_bytes: Option<usize>,
        part_size_bytes: usize,
        cache_puts: bool,
        warm: ObjectStoreWarmLevel,
        scan_interval: Option<Duration>,
        max_open_file_handles: usize,
    ) -> ConfigResult<Self> {
        Ok(Self {
            root: NonEmptyPathBuf::try_new(root)?,
            max_cache_size_bytes: max_cache_size_bytes
                .map(|bytes| {
                    NonZeroUsize::new(bytes).ok_or_else(|| {
                        ConfigError::new("object-store cache max size must be nonzero")
                    })
                })
                .transpose()?,
            part_size_bytes: NonZeroUsize::new(part_size_bytes)
                .ok_or_else(|| ConfigError::new("object-store cache part size must be nonzero"))?,
            cache_puts,
            warm,
            scan_interval,
            max_open_file_handles: NonZeroUsize::new(max_open_file_handles).ok_or_else(|| {
                ConfigError::new("object-store cache file-handle count must be nonzero")
            })?,
        })
    }

    /// Convert to the SlateDB API shape used by writer and reader opens.
    pub fn to_slate_options(&self) -> slatedb::config::ObjectStoreCacheOptions {
        slatedb::config::ObjectStoreCacheOptions {
            root_folder: Some(self.root.to_path_buf()),
            max_cache_size_bytes: self.max_cache_size_bytes.map(NonZeroUsize::get),
            part_size_bytes: self.part_size_bytes.get(),
            cache_puts: self.cache_puts,
            preload_disk_cache_on_startup: self.warm.to_slate_preload(),
            scan_interval: self.scan_interval,
            max_open_file_handles: self.max_open_file_handles.get(),
        }
    }

    /// Cache root directory.
    pub fn root(&self) -> &Path {
        self.root.as_path()
    }

    /// Startup preload level for the object-store cache.
    pub const fn warm(&self) -> ObjectStoreWarmLevel {
        self.warm
    }
}

/// SlateDB runtime settings.
#[derive(Debug, Clone)]
pub struct SlateRuntimeConfig {
    runtime_settings: slatedb::Settings,
}

impl SlateRuntimeConfig {
    /// Build default SlateDB runtime settings.
    pub fn new() -> Self {
        Self {
            runtime_settings: slatedb::Settings::default(),
        }
    }

    /// Build the writer settings passed to SlateDB.
    pub fn to_writer_settings(
        &self,
        object_store_cache: Option<&SlateObjectStoreCacheSettings>,
    ) -> slatedb::Settings {
        let mut settings = self.runtime_settings.clone();
        settings.object_store_cache_options = match object_store_cache {
            None => slatedb::config::ObjectStoreCacheOptions {
                root_folder: None,
                ..Default::default()
            },
            Some(cache) => cache.to_slate_options(),
        };
        settings
    }

    /// Build the reader options passed to SlateDB.
    pub fn to_reader_options(
        &self,
        object_store_cache: Option<&SlateObjectStoreCacheSettings>,
    ) -> slatedb::config::DbReaderOptions {
        slatedb::config::DbReaderOptions {
            wal_replay: self.runtime_settings.wal_replay,
            object_store_cache_options: match object_store_cache {
                None => slatedb::config::ObjectStoreCacheOptions {
                    root_folder: None,
                    ..Default::default()
                },
                Some(cache) => cache.to_slate_options(),
            },
            ..Default::default()
        }
    }

    /// Replace SlateDB runtime settings.
    pub fn with_runtime_settings(mut self, settings: slatedb::Settings) -> Self {
        self.runtime_settings = settings;
        self
    }
}

impl Default for SlateRuntimeConfig {
    fn default() -> Self {
        Self::new()
    }
}

const DEFAULT_FTS_MEMORY_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_FTS_GENERATION_GRACE_PERIOD_SECS: u64 = 300;
const DEFAULT_FTS_WARM_CONCURRENCY: usize = 4;

/// Startup warm behavior for the FTS split cache.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum FtsWarmConfig {
    /// Do not proactively open or hydrate FTS splits.
    #[default]
    Off,
    /// Warm FTS splits after open returns.
    Background {
        /// Maximum concurrent split opens/hydrations.
        concurrency: NonZeroUsize,
        /// Optional cap on active generations enumerated by one pass.
        startup_generation_limit: Option<NonZeroUsize>,
    },
    /// Warm FTS splits before open returns.
    Blocking {
        /// Maximum concurrent split opens/hydrations.
        concurrency: NonZeroUsize,
        /// Optional cap on active generations enumerated by one pass.
        startup_generation_limit: Option<NonZeroUsize>,
    },
}

impl FtsWarmConfig {
    /// Build a checked background warm policy.
    pub fn background(
        concurrency: usize,
        startup_generation_limit: Option<usize>,
    ) -> ConfigResult<Self> {
        Ok(Self::Background {
            concurrency: NonZeroUsize::new(concurrency)
                .ok_or_else(|| ConfigError::new("FTS warm concurrency must be nonzero"))?,
            startup_generation_limit: checked_optional_limit(
                startup_generation_limit,
                "FTS warm generation limit must be nonzero",
            )?,
        })
    }

    /// Build a checked blocking warm policy.
    pub fn blocking(
        concurrency: usize,
        startup_generation_limit: Option<usize>,
    ) -> ConfigResult<Self> {
        Ok(Self::Blocking {
            concurrency: NonZeroUsize::new(concurrency)
                .ok_or_else(|| ConfigError::new("FTS warm concurrency must be nonzero"))?,
            startup_generation_limit: checked_optional_limit(
                startup_generation_limit,
                "FTS warm generation limit must be nonzero",
            )?,
        })
    }

    /// Resolved warm mode.
    pub const fn mode(&self) -> CacheWarmMode {
        match self {
            Self::Off => CacheWarmMode::Off,
            Self::Background { .. } => CacheWarmMode::Background,
            Self::Blocking { .. } => CacheWarmMode::Blocking,
        }
    }

    /// Maximum concurrent warm operations.
    pub const fn concurrency(&self) -> usize {
        match self {
            Self::Off => 1,
            Self::Background { concurrency, .. } | Self::Blocking { concurrency, .. } => {
                concurrency.get()
            }
        }
    }

    /// Optional active-generation cap.
    pub const fn startup_generation_limit(&self) -> Option<usize> {
        match self {
            Self::Off => None,
            Self::Background {
                startup_generation_limit,
                ..
            }
            | Self::Blocking {
                startup_generation_limit,
                ..
            } => match startup_generation_limit {
                Some(limit) => Some(limit.get()),
                None => None,
            },
        }
    }
}

fn checked_optional_limit(
    limit: Option<usize>,
    message: &'static str,
) -> ConfigResult<Option<NonZeroUsize>> {
    limit
        .map(|value| NonZeroUsize::new(value).ok_or_else(|| ConfigError::new(message)))
        .transpose()
}

/// Checked in-memory FTS split-cache settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FtsMemoryCacheConfig {
    memory_bytes: NonZeroU64,
    warm: FtsWarmConfig,
    generation_grace_period: NonZeroU64,
}

impl FtsMemoryCacheConfig {
    /// Build checked in-memory FTS cache settings.
    pub fn try_new(
        memory_bytes: u64,
        warm: FtsWarmConfig,
        generation_grace_period_secs: u64,
    ) -> ConfigResult<Self> {
        Ok(Self {
            memory_bytes: NonZeroU64::new(memory_bytes)
                .ok_or_else(|| ConfigError::new("FTS split memory cache must be nonzero"))?,
            warm,
            generation_grace_period: NonZeroU64::new(generation_grace_period_secs)
                .ok_or_else(|| ConfigError::new("FTS cache grace period must be nonzero"))?,
        })
    }

    /// Resident-memory ceiling.
    pub const fn memory_bytes(&self) -> u64 {
        self.memory_bytes.get()
    }

    /// Startup warm policy.
    pub const fn warm(&self) -> &FtsWarmConfig {
        &self.warm
    }

    /// Resolved startup warm mode.
    pub const fn warm_mode(&self) -> CacheWarmMode {
        self.warm.mode()
    }

    /// Maximum concurrent split warm operations.
    pub const fn warm_concurrency(&self) -> usize {
        self.warm.concurrency()
    }

    /// Optional active-generation cap for one startup pass.
    pub const fn startup_generation_limit(&self) -> Option<usize> {
        self.warm.startup_generation_limit()
    }

    /// Minimum local-artifact age before budget cleanup may evict it.
    pub const fn generation_grace_period(&self) -> Duration {
        Duration::from_secs(self.generation_grace_period.get())
    }
}

impl Default for FtsMemoryCacheConfig {
    fn default() -> Self {
        Self::try_new(
            DEFAULT_FTS_MEMORY_BYTES,
            FtsWarmConfig::background(DEFAULT_FTS_WARM_CONCURRENCY, None)
                .expect("default FTS warm policy is valid"),
            DEFAULT_FTS_GENERATION_GRACE_PERIOD_SECS,
        )
        .expect("default FTS memory cache is valid")
    }
}

/// Checked memory-plus-disk FTS split-cache settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FtsHybridCacheConfig {
    memory_bytes: NonZeroU64,
    disk: DiskCacheConfig,
    warm: FtsWarmConfig,
    generation_grace_period: NonZeroU64,
}

impl FtsHybridCacheConfig {
    /// Build checked memory-plus-disk FTS split-cache settings.
    pub fn try_new(
        memory_bytes: u64,
        disk_root: impl Into<PathBuf>,
        disk_bytes: usize,
        warm: FtsWarmConfig,
        generation_grace_period_secs: u64,
    ) -> ConfigResult<Self> {
        Ok(Self {
            memory_bytes: NonZeroU64::new(memory_bytes)
                .ok_or_else(|| ConfigError::new("FTS split memory cache must be nonzero"))?,
            disk: DiskCacheConfig::try_new(disk_root, disk_bytes)?,
            warm,
            generation_grace_period: NonZeroU64::new(generation_grace_period_secs)
                .ok_or_else(|| ConfigError::new("FTS cache grace period must be nonzero"))?,
        })
    }

    /// Resident-memory ceiling.
    pub const fn memory_bytes(&self) -> u64 {
        self.memory_bytes.get()
    }

    /// Local-disk tier settings.
    pub const fn disk(&self) -> &DiskCacheConfig {
        &self.disk
    }

    /// Startup warm policy.
    pub const fn warm(&self) -> &FtsWarmConfig {
        &self.warm
    }

    /// Resolved startup warm mode.
    pub const fn warm_mode(&self) -> CacheWarmMode {
        self.warm.mode()
    }

    /// Maximum concurrent split warm operations.
    pub const fn warm_concurrency(&self) -> usize {
        self.warm.concurrency()
    }

    /// Optional active-generation cap for one startup pass.
    pub const fn startup_generation_limit(&self) -> Option<usize> {
        self.warm.startup_generation_limit()
    }

    /// Minimum local-artifact age before budget cleanup may evict it.
    pub const fn generation_grace_period(&self) -> Duration {
        Duration::from_secs(self.generation_grace_period.get())
    }

    /// Owned local-disk root for runtime construction.
    pub fn disk_root(&self) -> PathBuf {
        self.disk.root().to_path_buf()
    }

    /// Local-disk ceiling.
    pub const fn disk_bytes(&self) -> u64 {
        self.disk.bytes() as u64
    }
}

/// Default production-wide resident-vector admission budget (256 MiB).
pub const DEFAULT_VECTOR_MEMORY_BUDGET_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_VECTOR_MEMORY_POLL_INTERVAL_SECS: u64 = 5;
const DEFAULT_SIMHASHER_CACHE_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_SIMHASHER_CACHE_ENTRIES: usize = 64;

/// Resident memory budget for vector memory stores.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorMemoryBudget {
    bytes: Option<NonZeroU64>,
}

/// Checked retention limits for deterministic SimHash projection tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SimHasherCacheSettings {
    bytes: NonZeroUsize,
    entries: NonZeroUsize,
}

impl SimHasherCacheSettings {
    /// Builds runtime-only byte and entry caps for the SimHasher LRU.
    ///
    /// Both limits must be positive because a zero-capacity registry could not
    /// satisfy any vector operation. A candidate larger than `bytes` is later
    /// rejected before allocation.
    ///
    /// ```
    /// use db::config::SimHasherCacheSettings;
    ///
    /// let settings = SimHasherCacheSettings::try_new(32 * 1024 * 1024, 64)?;
    /// assert_eq!(settings.maximum_f32_dimension(), 131_072);
    /// # Ok::<(), db::config::ConfigError>(())
    /// ```
    pub fn try_new(bytes: usize, entries: usize) -> ConfigResult<Self> {
        Ok(Self {
            bytes: NonZeroUsize::new(bytes)
                .ok_or_else(|| ConfigError::new("SimHasher cache bytes must be nonzero"))?,
            entries: NonZeroUsize::new(entries)
                .ok_or_else(|| ConfigError::new("SimHasher cache entries must be nonzero"))?,
        })
    }

    /// Maximum bytes retained or reserved by the registry.
    pub const fn bytes(self) -> usize {
        self.bytes.get()
    }

    /// Maximum ready, failed, or constructing identities in the registry.
    pub const fn entries(self) -> usize {
        self.entries.get()
    }

    /// Largest f32 vector dimension whose 64 projections fit this byte cap.
    pub const fn maximum_f32_dimension(self) -> usize {
        self.bytes.get() / (64 * core::mem::size_of::<f32>())
    }
}

impl Default for SimHasherCacheSettings {
    fn default() -> Self {
        Self::try_new(
            DEFAULT_SIMHASHER_CACHE_BYTES,
            DEFAULT_SIMHASHER_CACHE_ENTRIES,
        )
        .expect("default SimHasher cache limits are nonzero")
    }
}

impl VectorMemoryBudget {
    /// Build a bounded budget, rejecting zero.
    pub fn bounded(bytes: u64) -> ConfigResult<Self> {
        Ok(Self {
            bytes: Some(
                NonZeroU64::new(bytes)
                    .ok_or_else(|| ConfigError::new("vector memory budget must be nonzero"))?,
            ),
        })
    }

    /// Builds the test-only unbounded policy used by exhaustive cache fixtures.
    #[cfg(test)]
    pub(crate) const fn unbounded_for_test() -> Self {
        Self { bytes: None }
    }

    /// Positive budget bytes, or `None` when unbounded.
    pub const fn bytes(self) -> Option<u64> {
        match self.bytes {
            Some(bytes) => Some(bytes.get()),
            None => None,
        }
    }
}

/// Checked settings for vector memory store pinning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorMemorySettings {
    budget: VectorMemoryBudget,
    hydration: VectorMemoryHydrationMode,
    simhasher_cache: SimHasherCacheSettings,
}

/// Startup and refresh behavior for vector memory stores.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorMemoryHydrationMode {
    /// Return from open immediately and hydrate vector memory stores in the background.
    Background {
        /// Background refresh interval.
        poll_interval_secs: NonZeroU64,
    },
    /// Hydrate once before open returns, then refresh in the background.
    BlockingThenBackground {
        /// Background refresh interval.
        poll_interval_secs: NonZeroU64,
    },
}

impl VectorMemoryHydrationMode {
    /// Build background vector-memory hydration.
    pub fn background(poll_interval_secs: u64) -> ConfigResult<Self> {
        Ok(Self::Background {
            poll_interval_secs: NonZeroU64::new(poll_interval_secs)
                .ok_or_else(|| ConfigError::new("vector memory poll interval must be nonzero"))?,
        })
    }

    /// Build blocking startup hydration followed by background refreshes.
    pub fn blocking_then_background(poll_interval_secs: u64) -> ConfigResult<Self> {
        Ok(Self::BlockingThenBackground {
            poll_interval_secs: NonZeroU64::new(poll_interval_secs)
                .ok_or_else(|| ConfigError::new("vector memory poll interval must be nonzero"))?,
        })
    }

    /// Background refresh interval in seconds.
    pub const fn poll_interval_secs(self) -> u64 {
        match self {
            Self::Background { poll_interval_secs }
            | Self::BlockingThenBackground { poll_interval_secs } => poll_interval_secs.get(),
        }
    }
}

impl VectorMemorySettings {
    /// Build vector memory settings.
    pub fn try_new(budget: VectorMemoryBudget, poll_interval_secs: u64) -> ConfigResult<Self> {
        Self::try_new_with_hydration(
            budget,
            VectorMemoryHydrationMode::background(poll_interval_secs)?,
        )
    }

    /// Build vector memory settings with explicit startup hydration behavior.
    pub const fn try_new_with_hydration(
        budget: VectorMemoryBudget,
        hydration: VectorMemoryHydrationMode,
    ) -> ConfigResult<Self> {
        Ok(Self {
            budget,
            hydration,
            simhasher_cache: SimHasherCacheSettings {
                bytes: NonZeroUsize::new(DEFAULT_SIMHASHER_CACHE_BYTES)
                    .expect("default SimHasher byte limit is nonzero"),
                entries: NonZeroUsize::new(DEFAULT_SIMHASHER_CACHE_ENTRIES)
                    .expect("default SimHasher entry limit is nonzero"),
            },
        })
    }

    /// Replaces only the runtime SimHasher retention limits.
    pub const fn with_simhasher_cache(mut self, simhasher_cache: SimHasherCacheSettings) -> Self {
        self.simhasher_cache = simhasher_cache;
        self
    }

    /// Resident memory budget.
    pub const fn budget(&self) -> VectorMemoryBudget {
        self.budget
    }

    /// Startup and refresh behavior.
    pub const fn hydration(&self) -> VectorMemoryHydrationMode {
        self.hydration
    }

    /// Background refresh interval in seconds.
    pub const fn poll_interval_secs(&self) -> u64 {
        self.hydration.poll_interval_secs()
    }

    /// Runtime-only SimHasher retention limits.
    pub const fn simhasher_cache(&self) -> SimHasherCacheSettings {
        self.simhasher_cache
    }
}

impl Default for VectorMemorySettings {
    fn default() -> Self {
        Self::try_new(
            VectorMemoryBudget {
                bytes: Some(
                    NonZeroU64::new(DEFAULT_VECTOR_MEMORY_BUDGET_BYTES)
                        .expect("default vector memory budget is nonzero"),
                ),
            },
            DEFAULT_VECTOR_MEMORY_POLL_INTERVAL_SECS,
        )
        .expect("default vector memory settings are valid")
    }
}

/// Runtime cache mode. The vector memory store is always enabled through
/// [`CacheConfig::vector_memory`]; this enum controls every other cache.
#[derive(Debug, Clone, PartialEq)]
pub enum CacheMode {
    /// Keep only vector memory stores enabled.
    VectorMemoryOnly,
    /// Use bounded in-memory SlateDB block/meta caches and an optional FTS cache.
    Memory {
        /// SlateDB block and metadata cache capacities.
        slate_db: SlateMemoryCacheConfig,
        /// SlateDB metadata warm policy.
        slate_warm: SlateWarmConfig,
        /// Optional shared FTS split cache.
        fts: Option<FtsMemoryCacheConfig>,
    },
    /// Use disk-backed cache tiers for SlateDB, object-store reads, and FTS.
    Hybrid {
        /// SlateDB Foyer hybrid-cache settings.
        slate_db: SlateHybridCacheConfig,
        /// SlateDB object-store disk-cache settings.
        object_store: SlateObjectStoreCacheSettings,
        /// SlateDB metadata warm policy.
        slate_warm: SlateWarmConfig,
        /// Optional shared FTS split cache.
        fts: Option<FtsHybridCacheConfig>,
    },
}

/// Database cache configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct CacheConfig {
    vector_memory: VectorMemorySettings,
    mode: CacheMode,
}

impl CacheConfig {
    /// Build a cache config with checked vector-memory settings and mode.
    ///
    /// ```
    /// # use db::config::{CacheConfig, CacheMode, VectorMemorySettings};
    /// let config = CacheConfig::new(VectorMemorySettings::default(), CacheMode::VectorMemoryOnly);
    /// assert!(matches!(config.mode(), CacheMode::VectorMemoryOnly));
    /// ```
    pub const fn new(vector_memory: VectorMemorySettings, mode: CacheMode) -> Self {
        Self {
            vector_memory,
            mode,
        }
    }

    /// Cache mode for all caches except vector memory.
    pub const fn mode(&self) -> &CacheMode {
        &self.mode
    }

    /// Vector memory settings. Vector memory is always enabled.
    pub const fn vector_memory(&self) -> &VectorMemorySettings {
        &self.vector_memory
    }

    /// Replace the vector memory settings.
    pub fn with_vector_memory(mut self, vector_memory: VectorMemorySettings) -> Self {
        self.vector_memory = vector_memory;
        self
    }

    /// Replace the non-vector cache mode.
    pub fn with_mode(mut self, mode: CacheMode) -> Self {
        self.mode = mode;
        self
    }

    /// SlateDB object-store cache settings, if the mode enables them.
    pub const fn object_store_cache(&self) -> Option<&SlateObjectStoreCacheSettings> {
        match &self.mode {
            CacheMode::Hybrid { object_store, .. } => Some(object_store),
            CacheMode::VectorMemoryOnly | CacheMode::Memory { .. } => None,
        }
    }

    /// SlateDB startup warm policy when a block/meta cache is enabled.
    pub const fn slate_warm(&self) -> Option<&SlateWarmConfig> {
        match &self.mode {
            CacheMode::VectorMemoryOnly => None,
            CacheMode::Memory { slate_warm, .. } | CacheMode::Hybrid { slate_warm, .. } => {
                Some(slate_warm)
            }
        }
    }

    /// FTS memory capacity, or zero when the shared FTS cache is disabled.
    pub const fn fts_memory_bytes(&self) -> u64 {
        match &self.mode {
            CacheMode::VectorMemoryOnly => 0,
            CacheMode::Memory { fts, .. } => match fts {
                Some(fts) => fts.memory_bytes(),
                None => 0,
            },
            CacheMode::Hybrid { fts, .. } => match fts {
                Some(fts) => fts.memory_bytes(),
                None => 0,
            },
        }
    }

    /// FTS disk root when the hybrid split cache is enabled.
    pub fn fts_disk_root(&self) -> Option<PathBuf> {
        match &self.mode {
            CacheMode::Hybrid { fts: Some(fts), .. } => Some(fts.disk_root()),
            CacheMode::VectorMemoryOnly
            | CacheMode::Memory { .. }
            | CacheMode::Hybrid { fts: None, .. } => None,
        }
    }

    /// FTS disk capacity, or zero without an enabled disk tier.
    pub const fn fts_disk_bytes(&self) -> u64 {
        match &self.mode {
            CacheMode::Hybrid { fts: Some(fts), .. } => fts.disk_bytes(),
            CacheMode::VectorMemoryOnly
            | CacheMode::Memory { .. }
            | CacheMode::Hybrid { fts: None, .. } => 0,
        }
    }

    /// FTS warm mode, resolving a disabled cache to off.
    pub const fn fts_warm_mode(&self) -> CacheWarmMode {
        match &self.mode {
            CacheMode::Memory { fts: Some(fts), .. } => fts.warm_mode(),
            CacheMode::Hybrid { fts: Some(fts), .. } => fts.warm_mode(),
            CacheMode::VectorMemoryOnly
            | CacheMode::Memory { fts: None, .. }
            | CacheMode::Hybrid { fts: None, .. } => CacheWarmMode::Off,
        }
    }

    /// Maximum FTS warm concurrency, resolving a disabled cache to one.
    pub const fn fts_warm_concurrency(&self) -> usize {
        match &self.mode {
            CacheMode::Memory { fts: Some(fts), .. } => fts.warm_concurrency(),
            CacheMode::Hybrid { fts: Some(fts), .. } => fts.warm_concurrency(),
            CacheMode::VectorMemoryOnly
            | CacheMode::Memory { fts: None, .. }
            | CacheMode::Hybrid { fts: None, .. } => 1,
        }
    }

    /// Optional active-generation cap for FTS startup warming.
    pub const fn fts_startup_generation_limit(&self) -> Option<usize> {
        match &self.mode {
            CacheMode::Memory { fts: Some(fts), .. } => fts.startup_generation_limit(),
            CacheMode::Hybrid { fts: Some(fts), .. } => fts.startup_generation_limit(),
            CacheMode::VectorMemoryOnly
            | CacheMode::Memory { fts: None, .. }
            | CacheMode::Hybrid { fts: None, .. } => None,
        }
    }

    /// FTS stale-artifact grace period.
    pub const fn fts_generation_grace_period(&self) -> Duration {
        match &self.mode {
            CacheMode::Memory { fts: Some(fts), .. } => fts.generation_grace_period(),
            CacheMode::Hybrid { fts: Some(fts), .. } => fts.generation_grace_period(),
            CacheMode::VectorMemoryOnly
            | CacheMode::Memory { fts: None, .. }
            | CacheMode::Hybrid { fts: None, .. } => {
                Duration::from_secs(DEFAULT_FTS_GENERATION_GRACE_PERIOD_SECS)
            }
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self::new(
            VectorMemorySettings::default(),
            CacheMode::Memory {
                slate_db: SlateMemoryCacheConfig::try_new(
                    slatedb::db_cache::DEFAULT_BLOCK_CACHE_CAPACITY,
                    slatedb::db_cache::DEFAULT_META_CACHE_CAPACITY,
                )
                .expect("default SlateDB cache capacities are nonzero"),
                slate_warm: SlateWarmConfig::default(),
                fts: Some(FtsMemoryCacheConfig::default()),
            },
        )
    }
}
