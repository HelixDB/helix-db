use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use super::cache::{
    CacheConfig, CacheMode, FtsMemoryCacheConfig, FtsWarmConfig, SimHasherCacheSettings,
    SlateMemoryCacheConfig, SlateRuntimeConfig, SlateWarmConfig, VectorMemoryBudget,
    VectorMemorySettings,
};
use super::index_lifecycle_throughput::IndexLifecycleThroughputTuning;
use super::migrations::MigrationTuning;
use super::search_index_backfill::SearchIndexBackfillLimits;
use super::secondary_index_lifecycle::SecondaryIndexLifecycleTuning;
use super::utils::{ConfigError, ConfigResult};
use crate::encoding::v2::values::adjacency::{
    EDGE_UPDATE_ADAPTIVE, EDGE_UPDATE_EAGER, EDGE_UPDATE_LAZY, ENCODING_TYPE_EFP,
    ENCODING_TYPE_NONE,
};
use crate::id_allocator::{DEFAULT_LEASE_REFILL_THRESHOLD, DEFAULT_LEASE_SIZE};

const MIB: u64 = 1024 * 1024;
const EMBEDDED_VECTOR_MEMORY_BYTES: u64 = 64 * MIB;
const EMBEDDED_SIMHASHER_MEMORY_BYTES: usize = 8 * MIB as usize;
const EMBEDDED_SIMHASHER_ENTRIES: usize = 16;
const EMBEDDED_FTS_MEMORY_BYTES: u64 = 16 * MIB;
const EMBEDDED_FTS_GENERATION_GRACE_PERIOD_SECS: u64 = 300;
const EMBEDDED_IN_MEMORY_SLATE_BLOCK_BYTES: u64 = 16 * MIB;
const EMBEDDED_IN_MEMORY_SLATE_METADATA_BYTES: u64 = 8 * MIB;
const EMBEDDED_DISK_SLATE_BLOCK_BYTES: u64 = 48 * MIB;
const EMBEDDED_DISK_SLATE_METADATA_BYTES: u64 = 16 * MIB;
const EMBEDDED_L0_SST_BYTES: usize = 16 * MIB as usize;
const EMBEDDED_MAX_UNFLUSHED_BYTES: usize = 64 * MIB as usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EmbeddedStorageProfile {
    InMemory,
    Disk,
}

impl EmbeddedStorageProfile {
    const fn flush_interval(self) -> Duration {
        match self {
            Self::InMemory => Duration::from_millis(1),
            Self::Disk => Duration::from_millis(3),
        }
    }

    const fn slate_cache(self) -> (u64, u64) {
        match self {
            Self::InMemory => (
                EMBEDDED_IN_MEMORY_SLATE_BLOCK_BYTES,
                EMBEDDED_IN_MEMORY_SLATE_METADATA_BYTES,
            ),
            Self::Disk => (
                EMBEDDED_DISK_SLATE_BLOCK_BYTES,
                EMBEDDED_DISK_SLATE_METADATA_BYTES,
            ),
        }
    }
}

/// Edge storage encoding selected for newly written edge lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EdgeEncoding {
    /// Store edge lists without compression.
    #[default]
    None,
    /// Store edge lists using EFP compression.
    Efp,
}

impl EdgeEncoding {
    /// Return the byte used by the edge encoding layer.
    pub const fn as_u8(self) -> u8 {
        match self {
            Self::None => ENCODING_TYPE_NONE,
            Self::Efp => ENCODING_TYPE_EFP,
        }
    }
}

impl TryFrom<u8> for EdgeEncoding {
    type Error = ConfigError;

    fn try_from(value: u8) -> ConfigResult<Self> {
        match value {
            ENCODING_TYPE_NONE | 0 => Ok(Self::None),
            ENCODING_TYPE_EFP | 1 => Ok(Self::Efp),
            other => Err(ConfigError::new(format!(
                "invalid edge encoding type {other}; expected none or efp"
            ))),
        }
    }
}

/// Edge update policy for maintaining adjacency rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EdgeUpdatePolicy {
    /// Always use read-modify-write.
    #[default]
    Eager,
    /// Always append deltas.
    Lazy,
    /// Choose eager/lazy by approximate vertex degree.
    Adaptive,
}

impl EdgeUpdatePolicy {
    /// Return the byte used by the edge update layer.
    pub const fn as_u8(self) -> u8 {
        match self {
            Self::Eager => EDGE_UPDATE_EAGER,
            Self::Lazy => EDGE_UPDATE_LAZY,
            Self::Adaptive => EDGE_UPDATE_ADAPTIVE,
        }
    }
}

impl TryFrom<u8> for EdgeUpdatePolicy {
    type Error = ConfigError;

    fn try_from(value: u8) -> ConfigResult<Self> {
        match value {
            EDGE_UPDATE_EAGER => Ok(Self::Eager),
            EDGE_UPDATE_LAZY => Ok(Self::Lazy),
            EDGE_UPDATE_ADAPTIVE => Ok(Self::Adaptive),
            other => Err(ConfigError::new(format!(
                "invalid edge update policy {other}; expected eager, lazy, or adaptive"
            ))),
        }
    }
}

/// Configuration options for HelixDb
#[derive(Debug, Clone, Default)]
pub struct OpenAttribution {
    /// Stable identity for a specific opened DB client instance.
    pub db_client_id: Option<String>,
    /// Correlation ID for a specific writer-open attempt.
    pub writer_open_id: Option<String>,
    /// Human-readable reason for opening storage.
    pub open_reason: Option<String>,
    /// Requested control-plane term, when present.
    pub requested_term: Option<u64>,
    /// Correlates a sequence of election actions within one gateway tick.
    pub election_tick_id: Option<String>,
    /// Correlates demote/fence/promote decisions from one control-plane action.
    pub fence_op_id: Option<String>,
    /// Identifies the gateway process issuing the control-plane action.
    pub gateway_instance_id: Option<String>,
}

/// Full runtime configuration for an opened Helix database.
#[derive(Debug, Clone)]
pub struct HelixConfig {
    /// User/runtime DB settings.
    db: DbConfig,
}

impl HelixConfig {
    /// Build a full runtime config from DB settings.
    pub const fn new(db: DbConfig) -> Self {
        Self { db }
    }

    /// Borrow the TOML/user-facing DB settings.
    pub const fn db(&self) -> &DbConfig {
        &self.db
    }
}

/// User/runtime DB settings.
///
/// Indexes are deliberately excluded. Dynamic index definitions live in runtime
/// metadata and are loaded into the DB runtime state when the DB opens.
#[derive(Debug, Clone)]
pub struct DbConfig {
    /// Default encoding type for edges (0 = none, 1 = EFP)
    default_encoding_type: EdgeEncoding,

    /// Whether to enable write-ahead logging
    pub(crate) enable_wal: bool,

    /// Maximum concurrent reads during traversal
    max_concurrent_reads: NonZeroUsize,

    /// Edge update policy (EDGE_UPDATE_EAGER, EDGE_UPDATE_LAZY, EDGE_UPDATE_ADAPTIVE)
    edge_update_policy: EdgeUpdatePolicy,

    /// Threshold for adaptive policy: vertices with approximate degree above this
    /// use lazy updates, below use eager updates
    pub(crate) high_degree_threshold: u64,

    /// Number of IDs to lease at a time for automatic ID allocation
    ///
    /// Higher values reduce storage operations but may "waste" IDs on restart.
    /// Default: 10,000
    id_lease_size: NonZeroU64,

    /// Remaining durable IDs at which a proactive lease refill begins.
    ///
    /// This is an absolute count and does not scale with `id_lease_size`.
    /// Default: 5,000
    id_lease_refill_threshold: u64,

    /// SlateDB runtime settings.
    slate: SlateRuntimeConfig,

    /// Runtime cache settings.
    cache: CacheConfig,

    /// Bounded secondary lifecycle worker policy.
    secondary_index_lifecycle: SecondaryIndexLifecycleTuning,

    /// Bounded text/vector lifecycle resource policy.
    search_index_backfill: SearchIndexBackfillLimits,

    /// Runtime-only scan and task throughput policy for index lifecycle work.
    index_lifecycle_throughput: IndexLifecycleThroughputTuning,

    /// Durable graph and catalog migration policy.
    migrations: MigrationTuning,

    /// Optional attribution fields for the current open attempt.
    pub(crate) open_attribution: Option<OpenAttribution>,
}

impl DbConfig {
    /// Create a new configuration with default tuning.
    pub fn new() -> Self {
        Self {
            default_encoding_type: EdgeEncoding::None,
            enable_wal: true,
            max_concurrent_reads: NonZeroUsize::new(64)
                .expect("default read concurrency is nonzero"),
            edge_update_policy: EdgeUpdatePolicy::Eager,
            high_degree_threshold: 1000, // Vertices with >1000 edges use lazy updates
            id_lease_size: NonZeroU64::new(DEFAULT_LEASE_SIZE)
                .expect("default lease size is nonzero"),
            id_lease_refill_threshold: DEFAULT_LEASE_REFILL_THRESHOLD,
            slate: SlateRuntimeConfig::new(),
            cache: CacheConfig::default(),
            secondary_index_lifecycle: SecondaryIndexLifecycleTuning::default(),
            search_index_backfill: SearchIndexBackfillLimits::default(),
            index_lifecycle_throughput: IndexLifecycleThroughputTuning::default(),
            migrations: MigrationTuning::default(),
            open_attribution: None,
        }
    }

    /// Build source-specific local defaults for an embedded handle.
    pub(crate) fn embedded_default(profile: EmbeddedStorageProfile) -> Self {
        let slate = slatedb::Settings {
            flush_interval: Some(profile.flush_interval()),
            l0_sst_size_bytes: EMBEDDED_L0_SST_BYTES,
            max_unflushed_bytes: EMBEDDED_MAX_UNFLUSHED_BYTES,
            ..slatedb::Settings::default()
        };

        let (block_bytes, metadata_bytes) = profile.slate_cache();
        let vector_memory = VectorMemorySettings::try_new(
            VectorMemoryBudget::bounded(EMBEDDED_VECTOR_MEMORY_BYTES)
                .expect("embedded vector cache capacity is nonzero"),
            5,
        )
        .expect("embedded vector cache settings are valid")
        .with_simhasher_cache(
            SimHasherCacheSettings::try_new(
                EMBEDDED_SIMHASHER_MEMORY_BYTES,
                EMBEDDED_SIMHASHER_ENTRIES,
            )
            .expect("embedded SimHasher cache limits are nonzero"),
        );
        let cache = CacheConfig::new(
            vector_memory,
            CacheMode::Memory {
                slate_db: SlateMemoryCacheConfig::try_new(block_bytes, metadata_bytes)
                    .expect("embedded SlateDB cache capacities are nonzero"),
                slate_warm: SlateWarmConfig::Off,
                fts: Some(
                    FtsMemoryCacheConfig::try_new(
                        EMBEDDED_FTS_MEMORY_BYTES,
                        FtsWarmConfig::Off,
                        EMBEDDED_FTS_GENERATION_GRACE_PERIOD_SECS,
                    )
                    .expect("embedded FTS cache settings are valid"),
                ),
            },
        );
        Self::new().with_slate_settings(slate).with_cache(cache)
    }

    /// Edge encoding used for newly written edge lists.
    pub const fn default_encoding_type(&self) -> EdgeEncoding {
        self.default_encoding_type
    }

    /// Raw edge encoding byte used by the storage layer.
    pub const fn default_encoding_type_byte(&self) -> u8 {
        self.default_encoding_type.as_u8()
    }

    /// Maximum concurrent reads during traversal.
    pub const fn max_concurrent_reads(&self) -> usize {
        self.max_concurrent_reads.get()
    }

    /// Edge update policy for adjacency maintenance.
    pub const fn edge_update_policy(&self) -> EdgeUpdatePolicy {
        self.edge_update_policy
    }

    /// Raw edge update policy byte used by the storage layer.
    pub const fn edge_update_policy_byte(&self) -> u8 {
        self.edge_update_policy.as_u8()
    }

    /// Number of IDs to lease at a time for automatic ID allocation.
    pub const fn id_lease_size(&self) -> u64 {
        self.id_lease_size.get()
    }

    /// Remaining durable IDs at which a proactive lease refill begins.
    pub const fn id_lease_refill_threshold(&self) -> u64 {
        self.id_lease_refill_threshold
    }

    /// SlateDB runtime settings.
    pub const fn slate(&self) -> &SlateRuntimeConfig {
        &self.slate
    }

    /// Runtime cache settings.
    pub const fn cache(&self) -> &CacheConfig {
        &self.cache
    }

    /// Secondary lifecycle worker mode, budgets, and scheduling intervals.
    pub const fn secondary_index_lifecycle(&self) -> SecondaryIndexLifecycleTuning {
        self.secondary_index_lifecycle
    }

    /// Returns the validated resource policy shared by text/vector lifecycle workers.
    ///
    /// Workers copy this immutable value before admitting a source batch so
    /// one build uses a consistent set of source, transaction, artifact, and
    /// compaction limits.
    pub const fn search_index_backfill(&self) -> SearchIndexBackfillLimits {
        self.search_index_backfill
    }

    /// Returns the runtime-only lifecycle scan and task throughput policy.
    pub const fn index_lifecycle_throughput(&self) -> IndexLifecycleThroughputTuning {
        self.index_lifecycle_throughput
    }

    /// Durable graph and catalog migration policy.
    pub const fn migrations(&self) -> MigrationTuning {
        self.migrations
    }

    /// Set the default encoding type for edges
    pub fn with_encoding_type(mut self, encoding_type: EdgeEncoding) -> Self {
        self.default_encoding_type = encoding_type;
        self
    }

    /// Set the default edge encoding from a legacy raw byte.
    pub fn try_with_encoding_type_u8(mut self, encoding_type: u8) -> ConfigResult<Self> {
        self.default_encoding_type = EdgeEncoding::try_from(encoding_type)?;
        Ok(self)
    }

    /// Enable or disable write-ahead logging
    pub fn with_wal(mut self, enable: bool) -> Self {
        self.enable_wal = enable;
        self
    }

    /// Set maximum concurrent reads during traversal
    pub fn with_max_concurrent_reads(mut self, max: usize) -> Self {
        self.max_concurrent_reads =
            NonZeroUsize::new(max).unwrap_or_else(|| NonZeroUsize::new(1).expect("one is nonzero"));
        self
    }

    /// Attach structured attribution fields to the next storage open.
    pub fn with_open_attribution(mut self, attribution: OpenAttribution) -> Self {
        self.open_attribution = Some(attribution);
        self
    }

    /// Set the edge update policy
    ///
    /// - `EDGE_UPDATE_EAGER`: Always use read-modify-write (default, safest)
    /// - `EDGE_UPDATE_LAZY`: Always append deltas (best for high-degree graphs)
    /// - `EDGE_UPDATE_ADAPTIVE`: Use Morris counter to choose per-node
    pub fn with_edge_update_policy(mut self, policy: EdgeUpdatePolicy) -> Self {
        self.edge_update_policy = policy;
        self
    }

    /// Set the edge update policy from a legacy raw byte.
    pub fn try_with_edge_update_policy_u8(mut self, policy: u8) -> ConfigResult<Self> {
        self.edge_update_policy = EdgeUpdatePolicy::try_from(policy)?;
        Ok(self)
    }

    /// Set the high-degree threshold for adaptive policy
    ///
    /// Vertices with approximate degree above this threshold will use
    /// lazy updates when in adaptive mode.
    pub fn with_high_degree_threshold(mut self, threshold: u64) -> Self {
        self.high_degree_threshold = threshold;
        self
    }

    /// Enable adaptive edge updates based on node degree
    ///
    /// This is a convenience method that sets:
    /// - `edge_update_policy` to `EDGE_UPDATE_ADAPTIVE`
    /// - `high_degree_threshold` to the specified value
    pub fn with_adaptive_updates(mut self, threshold: u64) -> Self {
        self.edge_update_policy = EdgeUpdatePolicy::Adaptive;
        self.high_degree_threshold = threshold;
        self
    }

    /// Set the ID lease size for automatic ID allocation
    ///
    /// Higher values reduce storage operations but may "waste" IDs when
    /// the database is closed before the lease is exhausted.
    ///
    /// Default: 10,000
    pub fn with_id_lease_size(mut self, size: NonZeroU64) -> Self {
        self.id_lease_size = size;
        self
    }

    /// Set the ID lease size from a raw integer, rejecting zero.
    pub fn try_with_id_lease_size(mut self, size: u64) -> ConfigResult<Self> {
        self.id_lease_size = NonZeroU64::new(size)
            .ok_or_else(|| ConfigError::new("id lease size must be nonzero"))?;
        Ok(self)
    }

    /// Set the absolute remaining-ID threshold for proactive lease refill.
    ///
    /// A threshold of zero starts renewal after the final ID in the current
    /// lease is claimed. Thresholds larger than the lease size are supported;
    /// the allocator reserves enough whole lease chunks to restore the
    /// requested headroom.
    pub fn with_id_lease_refill_threshold(mut self, remaining_ids: u64) -> Self {
        self.id_lease_refill_threshold = remaining_ids;
        self
    }
}

impl DbConfig {
    /// Set custom SlateDB runtime settings.
    pub fn with_slate_settings(mut self, settings: slatedb::Settings) -> Self {
        self.slate = self.slate.with_runtime_settings(settings);
        self
    }

    /// Set runtime cache configuration.
    pub fn with_cache(mut self, cache: CacheConfig) -> Self {
        self.cache = cache;
        self
    }

    /// Set the complete bounded secondary lifecycle worker policy.
    pub fn with_secondary_index_lifecycle_tuning(
        mut self,
        tuning: SecondaryIndexLifecycleTuning,
    ) -> Self {
        self.secondary_index_lifecycle = tuning;
        self
    }

    /// Replaces the complete bounded text/vector lifecycle resource policy.
    ///
    /// Construct `limits` through its validated ADTs, then pass the resulting
    /// configuration to database open; lifecycle workers do not accept loose
    /// integer overrides that could contradict one another.
    pub fn with_search_index_backfill_limits(mut self, limits: SearchIndexBackfillLimits) -> Self {
        self.search_index_backfill = limits;
        self
    }

    /// Replaces the complete runtime-only lifecycle throughput policy.
    pub fn with_index_lifecycle_throughput_tuning(
        mut self,
        tuning: IndexLifecycleThroughputTuning,
    ) -> Self {
        self.index_lifecycle_throughput = tuning;
        self
    }

    /// Replaces the complete durable migration policy.
    pub const fn with_migration_tuning(mut self, tuning: MigrationTuning) -> Self {
        self.migrations = tuning;
        self
    }
}

impl Default for DbConfig {
    fn default() -> Self {
        Self::new()
    }
}
