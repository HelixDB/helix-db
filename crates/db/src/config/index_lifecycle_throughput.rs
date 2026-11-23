//! Runtime-only throughput policy for index lifecycle work.
//!
//! This policy controls source-scan prefetching, worker concurrency and the
//! in-memory bulk build of vector indexes. It is never serialized into
//! lifecycle records or physical index rows.
//!
//! # Usage
//!
//! ```
//! use std::num::NonZeroU64;
//!
//! use db::config::{
//!     DbConfig, IndexLifecycleConcurrency, IndexLifecycleScanTuning,
//!     IndexLifecycleThroughputTuning, VectorBulkBuildMemory, VectorBulkBuildTuning,
//! };
//!
//! let scan = IndexLifecycleScanTuning::try_new(64 * 1024, 2)?;
//! let concurrency = IndexLifecycleConcurrency::try_new(2, 2, 1, 1)?;
//! // Build vector indexes of 10,000 vectors or more in memory, on eight
//! // threads, while they fit 4 GiB.
//! let four_gib = VectorBulkBuildMemory::Bytes(NonZeroU64::new(4 << 30).unwrap());
//! let bulk = VectorBulkBuildTuning::try_new(8, four_gib, 10_000)?;
//! let tuning = IndexLifecycleThroughputTuning::new(scan, concurrency)
//!     .with_vector_bulk_build(bulk);
//! let config = DbConfig::new().with_index_lifecycle_throughput_tuning(tuning);
//!
//! assert_eq!(config.index_lifecycle_throughput(), tuning);
//! assert_eq!(tuning.vector_bulk_build().memory(), four_gib);
//! # Ok::<(), db::config::IndexLifecycleThroughputTuningError>(())
//! ```

use std::num::{NonZeroU64, NonZeroUsize};

const DEFAULT_SCAN_READ_AHEAD_BYTES: usize = 256 * 1024;
const DEFAULT_SCAN_FETCH_TASKS: usize = 4;
// Secondary steps are mostly durable commits, so tasks overlap their WAL
// uploads: one tenant's first 122 empty builds take 24 s with eight and 54 s
// with two (`examples/index_build_throughput.rs`). More vector or text tasks
// did not help. Every family lane and the queued-publication lane (one task per
// vector and text task) fits in the total at once, so secondary work never
// holds the permits vector, text or publication need.
const DEFAULT_SECONDARY_TASKS: usize = 8;
const DEFAULT_VECTOR_TASKS: usize = 1;
const DEFAULT_TEXT_TASKS: usize = 1;
const DEFAULT_TOTAL_OPERATION_TASKS: usize =
    DEFAULT_SECONDARY_TASKS + 2 * (DEFAULT_VECTOR_TASKS + DEFAULT_TEXT_TASKS);
// Share of the memory ceiling an automatic bulk-build budget leaves free, for
// what no configured limit bounds: request buffers, thread stacks, and memory
// the allocator holds for the rest of the process. Serial vector builds in
// 3.3 and 6.7 GiB containers peaked at 2.8 and 3.0 GiB of anonymous memory, a
// few hundred MiB above the caches and write buffer their configuration
// allows.
const AUTOMATIC_BUDGET_HEADROOM_DIVISOR: u64 = 8;
// Below a few thousand vectors a serial build takes seconds, less than the
// extra source scan a bulk build starts with.
const DEFAULT_VECTOR_BULK_BUILD_MIN_VECTORS: u64 = 4096;

/// Invalid runtime throughput policy rejected before lifecycle work starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexLifecycleThroughputTuningError {
    /// One required positive setting was zero.
    Zero { setting: &'static str },
    /// A family or lane limit exceeded the shared task ceiling.
    ExceedsGlobal {
        setting: &'static str,
        limit: usize,
        global: usize,
    },
}

impl core::fmt::Display for IndexLifecycleThroughputTuningError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Zero { setting } => write!(formatter, "{setting} must be nonzero"),
            Self::ExceedsGlobal {
                setting,
                limit,
                global,
            } => write!(
                formatter,
                "{setting} limit {limit} exceeds global operation limit {global}"
            ),
        }
    }
}

impl std::error::Error for IndexLifecycleThroughputTuningError {}

/// Positive source-scan prefetch policy with cache admission permanently off.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IndexLifecycleScanTuning {
    read_ahead_bytes: NonZeroUsize,
    max_fetch_tasks: NonZeroUsize,
}

impl IndexLifecycleScanTuning {
    /// Constructs scan tuning, rejecting zero read-ahead and task counts.
    pub fn try_new(
        read_ahead_bytes: usize,
        max_fetch_tasks: usize,
    ) -> Result<Self, IndexLifecycleThroughputTuningError> {
        let read_ahead_bytes = NonZeroUsize::new(read_ahead_bytes).ok_or(
            IndexLifecycleThroughputTuningError::Zero {
                setting: "scan read-ahead bytes",
            },
        )?;
        let max_fetch_tasks = NonZeroUsize::new(max_fetch_tasks).ok_or(
            IndexLifecycleThroughputTuningError::Zero {
                setting: "scan fetch tasks",
            },
        )?;
        Ok(Self {
            read_ahead_bytes,
            max_fetch_tasks,
        })
    }

    /// Returns the requested read-ahead window in bytes.
    pub const fn read_ahead_bytes(self) -> NonZeroUsize {
        self.read_ahead_bytes
    }

    /// Returns the maximum parallel block-fetch tasks per tuned scan.
    pub const fn max_fetch_tasks(self) -> NonZeroUsize {
        self.max_fetch_tasks
    }

    /// Tuned lifecycle scans never admit fetched data blocks to the shared cache.
    pub const fn cache_admission_enabled(self) -> bool {
        false
    }

    pub(crate) fn scan_options(self) -> slatedb::config::ScanOptions {
        slatedb::config::ScanOptions::default()
            .with_read_ahead_bytes(self.read_ahead_bytes.get())
            .with_cache_blocks(false)
            .with_max_fetch_tasks(self.max_fetch_tasks.get())
    }
}

impl Default for IndexLifecycleScanTuning {
    fn default() -> Self {
        Self::try_new(DEFAULT_SCAN_READ_AHEAD_BYTES, DEFAULT_SCAN_FETCH_TASKS)
            .expect("default lifecycle scan tuning is positive")
    }
}

/// Validated global and family task ceilings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IndexLifecycleConcurrency {
    total_operation_tasks: NonZeroUsize,
    secondary_tasks: NonZeroUsize,
    vector_tasks: NonZeroUsize,
    text_tasks: NonZeroUsize,
}

impl IndexLifecycleConcurrency {
    /// Constructs concurrency limits and rejects zero or contradictory ceilings.
    pub fn try_new(
        total_operation_tasks: usize,
        secondary_tasks: usize,
        vector_tasks: usize,
        text_tasks: usize,
    ) -> Result<Self, IndexLifecycleThroughputTuningError> {
        let total_operation_tasks = positive("total operation tasks", total_operation_tasks)?;
        let secondary_tasks = positive("secondary tasks", secondary_tasks)?;
        let vector_tasks = positive("vector tasks", vector_tasks)?;
        let text_tasks = positive("text tasks", text_tasks)?;
        let global = total_operation_tasks.get();
        for (setting, limit) in [
            ("secondary tasks", secondary_tasks),
            ("vector tasks", vector_tasks),
            ("text tasks", text_tasks),
        ] {
            if limit.get() > global {
                return Err(IndexLifecycleThroughputTuningError::ExceedsGlobal {
                    setting,
                    limit: limit.get(),
                    global,
                });
            }
        }
        Ok(Self {
            total_operation_tasks,
            secondary_tasks,
            vector_tasks,
            text_tasks,
        })
    }

    pub const fn total_operation_tasks(self) -> NonZeroUsize {
        self.total_operation_tasks
    }

    pub const fn secondary_tasks(self) -> NonZeroUsize {
        self.secondary_tasks
    }

    pub const fn vector_tasks(self) -> NonZeroUsize {
        self.vector_tasks
    }

    pub const fn text_tasks(self) -> NonZeroUsize {
        self.text_tasks
    }

    /// Returns how many queue publication attempts may run at once: one per
    /// vector and per text task.
    pub(crate) const fn publication_tasks(self) -> NonZeroUsize {
        self.vector_tasks.saturating_add(self.text_tasks.get())
    }
}

impl Default for IndexLifecycleConcurrency {
    fn default() -> Self {
        Self::try_new(
            DEFAULT_TOTAL_OPERATION_TASKS,
            DEFAULT_SECONDARY_TASKS,
            DEFAULT_VECTOR_TASKS,
            DEFAULT_TEXT_TASKS,
        )
        .expect("default lifecycle concurrency is positive and globally bounded")
    }
}

/// Memory the bulk vector builds of one process may hold together.
///
/// # Examples
///
/// ```
/// use std::num::NonZeroU64;
///
/// use db::config::VectorBulkBuildMemory;
///
/// const GIB: u64 = 1 << 30;
/// // An 8 GiB container whose caches and write buffers may grow to 3 GiB
/// // keeps an eighth of the ceiling free and leaves bulk builds the rest.
/// let automatic = VectorBulkBuildMemory::Automatic;
/// assert_eq!(automatic.budget_bytes(Some(8 * GIB), 3 * GIB, None), 4 * GIB);
/// // A process that already holds more than its configuration bounds leaves
/// // less, and one at its ceiling leaves nothing.
/// assert_eq!(automatic.budget_bytes(Some(8 * GIB), 3 * GIB, Some(5 * GIB)), 2 * GIB);
/// assert_eq!(automatic.budget_bytes(Some(8 * GIB), 3 * GIB, Some(9 * GIB)), 0);
/// // Without a readable ceiling nothing is known to be free.
/// assert_eq!(automatic.budget_bytes(None, 3 * GIB, None), 0);
/// // An explicit budget is taken as given, and `Off` is no budget.
/// let explicit = VectorBulkBuildMemory::Bytes(NonZeroU64::new(GIB).unwrap());
/// assert_eq!(explicit.budget_bytes(None, 3 * GIB, Some(9 * GIB)), GIB);
/// assert_eq!(VectorBulkBuildMemory::Off.budget_bytes(Some(8 * GIB), 0, None), 0);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum VectorBulkBuildMemory {
    /// No bulk builds: every vector build inserts serially. The default,
    /// because a release from before bulk builds cannot open a database, as
    /// its writer, while a bulk build is in progress: turn bulk builds on
    /// only once every process that can become the writer runs a release
    /// that has them.
    #[default]
    Off,
    /// What the process's memory ceiling leaves beside the rest of the
    /// process; see [`Self::budget_bytes`].
    Automatic,
    /// Exactly this much, whatever else the process holds. The operator
    /// answers for the sum fitting the machine.
    Bytes(NonZeroU64),
}

impl VectorBulkBuildMemory {
    /// Bytes bulk builds may hold together right now.
    ///
    /// `ceiling` is the process's memory ceiling
    /// ([`super::memory_ceiling_bytes`]), `configured` the bytes its
    /// configuration lets caches and write buffers grow to
    /// ([`super::DbConfig::configured_memory_bytes`]), and `resident` the
    /// anonymous memory the rest of the process holds beside its bulk
    /// builds and what they left the allocator holding, where the platform
    /// reports it.
    ///
    /// An automatic budget is the ceiling less an eighth of it kept free and
    /// less the larger of `configured` and `resident`: the rest of the
    /// process is taken to hold whichever is more, because caches not yet
    /// full will grow to their configured size and a process already above it
    /// will not shrink. Without a ceiling the budget is zero.
    pub fn budget_bytes(self, ceiling: Option<u64>, configured: u64, resident: Option<u64>) -> u64 {
        match self {
            Self::Off => 0,
            Self::Bytes(bytes) => bytes.get(),
            Self::Automatic => ceiling.map_or(0, |ceiling| {
                ceiling
                    .saturating_sub(ceiling / AUTOMATIC_BUDGET_HEADROOM_DIVISOR)
                    .saturating_sub(configured.max(resident.unwrap_or(0)))
            }),
        }
    }
}

/// When a vector index build links its graph in memory instead of inserting
/// one vector at a time, and with how many threads.
///
/// A bulk build holds every source vector and the whole graph in memory, so
/// it runs only for a build that fits `memory` (shared by the builds of one
/// process) and holds at least `min_vectors` vectors; any other build inserts
/// serially.
///
/// The defaults are [`VectorBulkBuildMemory::Off`], half the cores, and 4,096
/// vectors: no build is a bulk build until the memory is set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VectorBulkBuildTuning {
    construction_threads: NonZeroUsize,
    memory: VectorBulkBuildMemory,
    min_vectors: u64,
}

impl VectorBulkBuildTuning {
    /// Constructs the policy, rejecting zero construction threads.
    pub fn try_new(
        construction_threads: usize,
        memory: VectorBulkBuildMemory,
        min_vectors: u64,
    ) -> Result<Self, IndexLifecycleThroughputTuningError> {
        Ok(Self {
            construction_threads: positive(
                "vector bulk construction threads",
                construction_threads,
            )?,
            memory,
            min_vectors,
        })
    }

    /// Bulk builds off: every vector build inserts serially.
    pub const fn disabled() -> Self {
        Self {
            construction_threads: NonZeroUsize::MIN,
            memory: VectorBulkBuildMemory::Off,
            min_vectors: 0,
        }
    }

    /// Replaces the memory bulk builds may hold together.
    pub const fn with_memory(mut self, memory: VectorBulkBuildMemory) -> Self {
        self.memory = memory;
        self
    }

    /// OS threads that link one graph. They are dedicated to the build, not
    /// taken from the async runtime.
    pub const fn construction_threads(self) -> NonZeroUsize {
        self.construction_threads
    }

    /// Memory the bulk builds of one process may hold together.
    pub const fn memory(self) -> VectorBulkBuildMemory {
        self.memory
    }

    /// Fewest source vectors a bulk build is started for.
    pub const fn min_vectors(self) -> u64 {
        self.min_vectors
    }
}

impl Default for VectorBulkBuildTuning {
    fn default() -> Self {
        Self {
            construction_threads: std::thread::available_parallelism()
                .ok()
                .and_then(|cores| NonZeroUsize::new(cores.get() / 2))
                .unwrap_or(NonZeroUsize::MIN),
            memory: VectorBulkBuildMemory::Off,
            min_vectors: DEFAULT_VECTOR_BULK_BUILD_MIN_VECTORS,
        }
    }
}

/// Complete runtime-only lifecycle throughput policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct IndexLifecycleThroughputTuning {
    scan: IndexLifecycleScanTuning,
    concurrency: IndexLifecycleConcurrency,
    vector_bulk_build: VectorBulkBuildTuning,
}

impl IndexLifecycleThroughputTuning {
    /// Combines scan and concurrency limits with the default bulk-build policy.
    pub fn new(scan: IndexLifecycleScanTuning, concurrency: IndexLifecycleConcurrency) -> Self {
        Self {
            scan,
            concurrency,
            vector_bulk_build: VectorBulkBuildTuning::default(),
        }
    }

    /// Replaces the vector bulk-build policy.
    pub const fn with_vector_bulk_build(
        mut self,
        vector_bulk_build: VectorBulkBuildTuning,
    ) -> Self {
        self.vector_bulk_build = vector_bulk_build;
        self
    }

    pub const fn vector_bulk_build(self) -> VectorBulkBuildTuning {
        self.vector_bulk_build
    }

    pub const fn scan(self) -> IndexLifecycleScanTuning {
        self.scan
    }

    pub const fn concurrency(self) -> IndexLifecycleConcurrency {
        self.concurrency
    }
}

fn positive(
    setting: &'static str,
    value: usize,
) -> Result<NonZeroUsize, IndexLifecycleThroughputTuningError> {
    NonZeroUsize::new(value).ok_or(IndexLifecycleThroughputTuningError::Zero { setting })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_runtime_throughput_contract() {
        let tuning = IndexLifecycleThroughputTuning::default();
        assert_eq!(tuning.scan().read_ahead_bytes().get(), 256 * 1024);
        assert_eq!(tuning.scan().max_fetch_tasks().get(), 4);
        assert!(!tuning.scan().cache_admission_enabled());
        assert_eq!(tuning.concurrency().total_operation_tasks().get(), 12);
        assert_eq!(tuning.concurrency().secondary_tasks().get(), 8);
        assert_eq!(tuning.concurrency().vector_tasks().get(), 1);
        assert_eq!(tuning.concurrency().text_tasks().get(), 1);
        let bulk = tuning.vector_bulk_build();
        assert_eq!(
            bulk.construction_threads().get(),
            (std::thread::available_parallelism().unwrap().get() / 2).max(1)
        );
        assert_eq!(bulk.memory(), VectorBulkBuildMemory::Off);
        assert_eq!(bulk.min_vectors(), 4096);
    }

    #[test]
    fn bulk_build_policy_rejects_zero_threads_and_can_be_disabled() {
        let one_gib = VectorBulkBuildMemory::Bytes(NonZeroU64::new(1 << 30).unwrap());
        assert_eq!(
            VectorBulkBuildTuning::try_new(0, one_gib, 1),
            Err(IndexLifecycleThroughputTuningError::Zero {
                setting: "vector bulk construction threads",
            })
        );
        assert_eq!(
            VectorBulkBuildTuning::disabled().memory(),
            VectorBulkBuildMemory::Off
        );
        let tuning = IndexLifecycleThroughputTuning::default()
            .with_vector_bulk_build(VectorBulkBuildTuning::disabled());
        assert_eq!(
            tuning.vector_bulk_build(),
            VectorBulkBuildTuning::disabled()
        );
        assert_eq!(
            VectorBulkBuildTuning::default()
                .with_memory(one_gib)
                .memory(),
            one_gib
        );
    }

    /// The automatic budget is what the ceiling leaves beside an eighth kept
    /// free and the larger of the configured and the resident memory, and
    /// never wraps below zero.
    #[test]
    fn an_automatic_budget_is_what_the_ceiling_leaves() {
        const GIB: u64 = 1 << 30;
        let automatic = VectorBulkBuildMemory::Automatic;
        assert_eq!(automatic.budget_bytes(Some(16 * GIB), 0, None), 14 * GIB);
        assert_eq!(
            automatic.budget_bytes(Some(16 * GIB), 4 * GIB, None),
            10 * GIB
        );
        // The larger of the two holdings counts, once.
        assert_eq!(
            automatic.budget_bytes(Some(16 * GIB), 4 * GIB, Some(3 * GIB)),
            10 * GIB
        );
        assert_eq!(
            automatic.budget_bytes(Some(16 * GIB), 4 * GIB, Some(6 * GIB)),
            8 * GIB
        );
        assert_eq!(
            automatic.budget_bytes(Some(16 * GIB), 14 * GIB, None),
            0,
            "the free eighth is kept before the budget"
        );
        assert_eq!(automatic.budget_bytes(Some(GIB), 2 * GIB, Some(3 * GIB)), 0);
        assert_eq!(automatic.budget_bytes(None, 0, Some(GIB)), 0);
        assert_eq!(
            VectorBulkBuildMemory::Off.budget_bytes(Some(16 * GIB), 0, None),
            0
        );
        assert_eq!(
            VectorBulkBuildMemory::Bytes(NonZeroU64::new(7).unwrap()).budget_bytes(None, GIB, None),
            7
        );
        assert_eq!(VectorBulkBuildMemory::default(), VectorBulkBuildMemory::Off);
    }

    #[test]
    fn legacy_equivalent_scan_and_serial_concurrency_remain_explicit() {
        let scan = IndexLifecycleScanTuning::try_new(1, 1).unwrap();
        let concurrency = IndexLifecycleConcurrency::try_new(1, 1, 1, 1).unwrap();
        let options = scan.scan_options();
        assert_eq!(options.read_ahead_bytes, 1);
        assert_eq!(options.max_fetch_tasks, 1);
        assert!(!options.cache_blocks);
        assert_eq!(concurrency.total_operation_tasks().get(), 1);
    }

    #[test]
    fn zero_and_family_limits_above_global_are_rejected() {
        assert!(matches!(
            IndexLifecycleScanTuning::try_new(0, 1),
            Err(IndexLifecycleThroughputTuningError::Zero { .. })
        ));
        assert!(matches!(
            IndexLifecycleConcurrency::try_new(2, 2, 1, 3),
            Err(IndexLifecycleThroughputTuningError::ExceedsGlobal {
                setting: "text tasks",
                ..
            })
        ));
        for zero_position in 0..4 {
            let mut limits = [1; 4];
            limits[zero_position] = 0;
            assert!(matches!(
                IndexLifecycleConcurrency::try_new(limits[0], limits[1], limits[2], limits[3]),
                Err(IndexLifecycleThroughputTuningError::Zero { .. })
            ));
        }
    }
}
