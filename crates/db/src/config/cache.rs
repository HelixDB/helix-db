//! Validated runtime cache policy for SlateDB and vector search.
//!
//! These settings control process memory and startup behavior only. They are
//! never persisted into index catalogs or physical row formats.

use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use super::utils::{ConfigError, ConfigResult, DiskCacheConfig, NonEmptyPathBuf};

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
    /// Use SlateDB's default in-memory block/meta cache.
    Memory,
    /// Use disk-backed cache tiers for SlateDB and object-store reads.
    Hybrid {
        slate_db: SlateHybridCacheConfig,
        object_store: SlateObjectStoreCacheSettings,
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
            CacheMode::VectorMemoryOnly | CacheMode::Memory => None,
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self::new(VectorMemorySettings::default(), CacheMode::Memory)
    }
}
