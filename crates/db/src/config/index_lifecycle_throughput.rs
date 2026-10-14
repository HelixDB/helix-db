//! Runtime-only throughput policy for index lifecycle work.
//!
//! This policy controls source-scan prefetching and worker concurrency. It is
//! never serialized into lifecycle records or physical index rows.
//!
//! # Usage
//!
//! ```
//! use db::config::{
//!     DbConfig, IndexLifecycleConcurrency, IndexLifecycleScanTuning,
//!     IndexLifecycleThroughputTuning,
//! };
//!
//! let scan = IndexLifecycleScanTuning::try_new(64 * 1024, 2)?;
//! let concurrency = IndexLifecycleConcurrency::try_new(2, 2, 1, 1)?;
//! let tuning = IndexLifecycleThroughputTuning::new(scan, concurrency);
//! let config = DbConfig::new().with_index_lifecycle_throughput_tuning(tuning);
//!
//! assert_eq!(config.index_lifecycle_throughput(), tuning);
//! # Ok::<(), db::config::IndexLifecycleThroughputTuningError>(())
//! ```

use std::num::NonZeroUsize;

const DEFAULT_SCAN_READ_AHEAD_BYTES: usize = 256 * 1024;
const DEFAULT_SCAN_FETCH_TASKS: usize = 4;
const DEFAULT_TOTAL_OPERATION_TASKS: usize = 2;
const DEFAULT_SECONDARY_TASKS: usize = 2;
const DEFAULT_VECTOR_TASKS: usize = 1;
const DEFAULT_TEXT_TASKS: usize = 1;

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

/// Complete runtime-only lifecycle throughput policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct IndexLifecycleThroughputTuning {
    scan: IndexLifecycleScanTuning,
    concurrency: IndexLifecycleConcurrency,
}

impl IndexLifecycleThroughputTuning {
    pub const fn new(
        scan: IndexLifecycleScanTuning,
        concurrency: IndexLifecycleConcurrency,
    ) -> Self {
        Self { scan, concurrency }
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
        assert_eq!(tuning.concurrency().total_operation_tasks().get(), 2);
        assert_eq!(tuning.concurrency().secondary_tasks().get(), 2);
        assert_eq!(tuning.concurrency().vector_tasks().get(), 1);
        assert_eq!(tuning.concurrency().text_tasks().get(), 1);
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
