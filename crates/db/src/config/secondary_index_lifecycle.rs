//! Validated resource and scheduling policy for secondary lifecycle work.
//!
//! [`SecondaryIndexLifecycleTuning`] is the single user-facing source for bounded
//! secondary outbox discovery, source backfill, catch-up, validation, and
//! cleanup execution. Every count, byte cap, and interval is positive by construction;
//! [`SecondaryIndexLifecycleWorkerMode`] represents manual versus background driving
//! without a parallel boolean. The database converts this policy into internal
//! transaction contracts when it runs one worker step.
//!
//! These values are runtime policy only. They are not serialized into physical
//! rows or canonical lifecycle records, so changing them does not change any
//! on-disk format.

use std::num::{NonZeroU64, NonZeroUsize};

const DEFAULT_BATCH_ROWS: usize = 1_024;
const DEFAULT_MAX_INPUT_BYTES: u64 = 8 * 1024 * 1024;
const DEFAULT_MAX_OUTPUT_OPERATIONS: u64 = 4_096;
const DEFAULT_MAX_OUTPUT_BYTES: u64 = 8 * 1024 * 1024;
const DEFAULT_RECONCILE_INPUT_BYTES: u64 = 1024 * 1024;
const DEFAULT_ACTIVE_INTERVAL_MILLIS: u64 = 10;
const DEFAULT_IDLE_INTERVAL_MILLIS: u64 = 1_000;

/// Whether secondary lifecycle work is scheduled automatically.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum SecondaryIndexLifecycleWorkerMode {
    /// Writer open starts a task and DDL wake-ups notify it.
    #[default]
    Enabled,
    /// Work advances only through the explicit one-step API.
    Disabled,
}

/// Positive maximum source entities admitted by one transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SecondaryIndexLifecycleBatchRows(NonZeroUsize);

impl SecondaryIndexLifecycleBatchRows {
    /// Creates a positive entity limit, returning `None` for zero.
    pub const fn new(rows: usize) -> Option<Self> {
        match NonZeroUsize::new(rows) {
            Some(rows) => Some(Self(rows)),
            None => None,
        }
    }

    /// Returns the positive entity limit.
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

/// Positive delay between passes while a runnable job was observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SecondaryIndexLifecycleActiveIntervalMillis(NonZeroU64);

impl SecondaryIndexLifecycleActiveIntervalMillis {
    /// Creates a positive active delay, returning `None` for zero.
    pub const fn new(milliseconds: u64) -> Option<Self> {
        match NonZeroU64::new(milliseconds) {
            Some(milliseconds) => Some(Self(milliseconds)),
            None => None,
        }
    }

    /// Returns the active delay in milliseconds.
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Positive polling delay after a complete job scan found no runnable work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SecondaryIndexLifecycleIdleIntervalMillis(NonZeroU64);

impl SecondaryIndexLifecycleIdleIntervalMillis {
    /// Creates a positive idle delay, returning `None` for zero.
    pub const fn new(milliseconds: u64) -> Option<Self> {
        match NonZeroU64::new(milliseconds) {
            Some(milliseconds) => Some(Self(milliseconds)),
            None => None,
        }
    }

    /// Returns the idle delay in milliseconds.
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Complete positive budgets and timing for secondary lifecycle execution.
///
/// Every lifecycle transaction stays within the batch budgets below. A step
/// that holds the scope's exclusive gate first drains the deltas the writes
/// it waited for recorded, in further transactions under the same budgets,
/// so however large that write window was, no transaction exceeds them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SecondaryIndexLifecycleTuning {
    worker_mode: SecondaryIndexLifecycleWorkerMode,
    batch_rows: SecondaryIndexLifecycleBatchRows,
    max_input_bytes: NonZeroU64,
    max_output_operations: NonZeroU64,
    max_output_bytes: NonZeroU64,
    reconcile_input_bytes: NonZeroU64,
    active_interval_millis: SecondaryIndexLifecycleActiveIntervalMillis,
    idle_interval_millis: SecondaryIndexLifecycleIdleIntervalMillis,
}

impl SecondaryIndexLifecycleTuning {
    /// Returns whether writer-open should start the background task.
    pub const fn worker_mode(self) -> SecondaryIndexLifecycleWorkerMode {
        self.worker_mode
    }

    /// Returns the maximum entities one lifecycle transaction admits, whether
    /// it scans sources, drains deltas, releases applied state, or cleans up.
    pub const fn batch_rows(self) -> SecondaryIndexLifecycleBatchRows {
        self.batch_rows
    }

    /// Returns the exact decoded-input cap for one lifecycle transaction.
    pub const fn max_input_bytes(self) -> NonZeroU64 {
        self.max_input_bytes
    }

    /// Returns the maximum puts/deletes staged by one lifecycle transaction.
    pub const fn max_output_operations(self) -> NonZeroU64 {
        self.max_output_operations
    }

    /// Returns the exact encoded-output cap for one lifecycle transaction and
    /// entity.
    pub const fn max_output_bytes(self) -> NonZeroU64 {
        self.max_output_bytes
    }

    /// Returns the operation and checkpoint bytes decoded by one reconciliation call.
    pub const fn reconcile_input_bytes(self) -> NonZeroU64 {
        self.reconcile_input_bytes
    }

    /// Returns the delay between passes while a runnable job exists.
    pub const fn active_interval_millis(self) -> SecondaryIndexLifecycleActiveIntervalMillis {
        self.active_interval_millis
    }

    /// Returns the polling delay after a complete scan finds no runnable job.
    pub const fn idle_interval_millis(self) -> SecondaryIndexLifecycleIdleIntervalMillis {
        self.idle_interval_millis
    }

    /// Replaces automatic/manual scheduling mode without changing budgets.
    pub const fn with_worker_mode(mut self, mode: SecondaryIndexLifecycleWorkerMode) -> Self {
        self.worker_mode = mode;
        self
    }

    /// Replaces the positive per-transaction entity cap.
    pub const fn with_batch_rows(mut self, rows: SecondaryIndexLifecycleBatchRows) -> Self {
        self.batch_rows = rows;
        self
    }

    /// Replaces the positive decoded-input cap.
    pub const fn with_max_input_bytes(mut self, bytes: NonZeroU64) -> Self {
        self.max_input_bytes = bytes;
        self
    }

    /// Replaces the positive output-operation cap.
    pub const fn with_max_output_operations(mut self, operations: NonZeroU64) -> Self {
        self.max_output_operations = operations;
        self
    }

    /// Replaces the positive transaction and single-entity output-byte cap.
    pub const fn with_max_output_bytes(mut self, bytes: NonZeroU64) -> Self {
        self.max_output_bytes = bytes;
        self
    }

    /// Replaces the positive reconciliation decoded-input cap.
    pub const fn with_reconcile_input_bytes(mut self, bytes: NonZeroU64) -> Self {
        self.reconcile_input_bytes = bytes;
        self
    }

    /// Replaces the positive delay between active passes.
    pub const fn with_active_interval_millis(
        mut self,
        interval: SecondaryIndexLifecycleActiveIntervalMillis,
    ) -> Self {
        self.active_interval_millis = interval;
        self
    }

    /// Replaces the positive delay after an exhausted scan.
    pub const fn with_idle_interval_millis(
        mut self,
        interval: SecondaryIndexLifecycleIdleIntervalMillis,
    ) -> Self {
        self.idle_interval_millis = interval;
        self
    }
}

impl Default for SecondaryIndexLifecycleTuning {
    fn default() -> Self {
        Self {
            worker_mode: SecondaryIndexLifecycleWorkerMode::Enabled,
            batch_rows: SecondaryIndexLifecycleBatchRows::new(DEFAULT_BATCH_ROWS)
                .expect("default batch rows are positive"),
            max_input_bytes: NonZeroU64::new(DEFAULT_MAX_INPUT_BYTES)
                .expect("default input bytes are positive"),
            max_output_operations: NonZeroU64::new(DEFAULT_MAX_OUTPUT_OPERATIONS)
                .expect("default output operations are positive"),
            max_output_bytes: NonZeroU64::new(DEFAULT_MAX_OUTPUT_BYTES)
                .expect("default output bytes are positive"),
            reconcile_input_bytes: NonZeroU64::new(DEFAULT_RECONCILE_INPUT_BYTES)
                .expect("default reconcile bytes are positive"),
            active_interval_millis: SecondaryIndexLifecycleActiveIntervalMillis::new(
                DEFAULT_ACTIVE_INTERVAL_MILLIS,
            )
            .expect("default active interval is positive"),
            idle_interval_millis: SecondaryIndexLifecycleIdleIntervalMillis::new(
                DEFAULT_IDLE_INTERVAL_MILLIS,
            )
            .expect("default idle interval is positive"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_limits_and_intervals_are_unrepresentable() {
        assert_eq!(SecondaryIndexLifecycleBatchRows::new(0), None);
        assert_eq!(SecondaryIndexLifecycleActiveIntervalMillis::new(0), None);
        assert_eq!(SecondaryIndexLifecycleIdleIntervalMillis::new(0), None);
    }

    #[test]
    fn builders_replace_one_policy_dimension_without_erasing_the_rest() {
        let defaults = SecondaryIndexLifecycleTuning::default();
        let updated = defaults
            .with_worker_mode(SecondaryIndexLifecycleWorkerMode::Disabled)
            .with_batch_rows(SecondaryIndexLifecycleBatchRows::new(3).unwrap())
            .with_max_output_operations(NonZeroU64::new(11).unwrap())
            .with_active_interval_millis(
                SecondaryIndexLifecycleActiveIntervalMillis::new(5).unwrap(),
            )
            .with_idle_interval_millis(SecondaryIndexLifecycleIdleIntervalMillis::new(7).unwrap());
        assert_eq!(
            updated.worker_mode(),
            SecondaryIndexLifecycleWorkerMode::Disabled
        );
        assert_eq!(updated.batch_rows().get(), 3);
        assert_eq!(updated.max_output_operations().get(), 11);
        assert_eq!(updated.active_interval_millis().get(), 5);
        assert_eq!(updated.idle_interval_millis().get(), 7);
        assert_eq!(updated.max_input_bytes(), defaults.max_input_bytes());
        assert_eq!(updated.max_output_bytes(), defaults.max_output_bytes());
    }
}
