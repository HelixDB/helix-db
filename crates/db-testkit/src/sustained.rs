//! Typed sustained-workload matrix and sequence-based replica-lag metrics.

use std::num::NonZeroU16;
use std::time::Duration;

use db::DatabaseSequence;
use serde::{Deserialize, Serialize};

use crate::{Result, TestkitError};

/// Required sustained cloud-launch workload classes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadClass {
    /// Stable concurrent reads without writes.
    ReadOnly,
    /// Concurrent serializable mutations.
    WriteOnly,
    /// Snapshot reads overlapping writes.
    ReadsDuringWrites,
    /// Index construction and catch-up under foreground traffic.
    BackfillUnderTraffic,
    /// Repeated create, drop, retry, and abort behavior.
    LifecycleChurn,
    /// Compaction, reconciliation, reclaim, and garbage collection.
    BackgroundMaintenance,
    /// Writer and reader restart with durable replay.
    RestartAndRecovery,
    /// Fair progress and isolation across at least four tenant scopes.
    MultiTenantContention,
}

impl WorkloadClass {
    /// Complete sustained workload domain.
    pub const ALL: [Self; 8] = [
        Self::ReadOnly,
        Self::WriteOnly,
        Self::ReadsDuringWrites,
        Self::BackfillUnderTraffic,
        Self::LifecycleChurn,
        Self::BackgroundMaintenance,
        Self::RestartAndRecovery,
        Self::MultiTenantContention,
    ];
}

/// One workload's explicit concurrency topology.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadConcurrency {
    readers: u16,
    writers: u16,
    workers: u16,
    tenants: NonZeroU16,
}

impl WorkloadConcurrency {
    /// Constructs a topology; reader, writer, and worker zeroes are valid when
    /// the workload class does not use that role.
    pub fn try_new(readers: u16, writers: u16, workers: u16, tenants: u16) -> Result<Self> {
        let Some(tenants) = NonZeroU16::new(tenants) else {
            return Err(TestkitError::ModelViolation(
                "sustained workload tenant count must be positive".to_string(),
            ));
        };
        Ok(Self {
            readers,
            writers,
            workers,
            tenants,
        })
    }

    /// Returns reader task concurrency.
    pub const fn readers(self) -> u16 {
        self.readers
    }

    /// Returns writer task concurrency.
    pub const fn writers(self) -> u16 {
        self.writers
    }

    /// Returns explicit background worker concurrency.
    pub const fn workers(self) -> u16 {
        self.workers
    }

    /// Returns independent tenant scopes.
    pub const fn tenants(self) -> NonZeroU16 {
        self.tenants
    }
}

/// One workload class paired with its reviewed concurrency defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadSpec {
    class: WorkloadClass,
    concurrency: WorkloadConcurrency,
}

impl WorkloadSpec {
    /// Returns the pre-launch default for one workload class.
    pub fn pre_launch(class: WorkloadClass) -> Self {
        let concurrency = match class {
            WorkloadClass::ReadOnly => WorkloadConcurrency::try_new(32, 0, 0, 1),
            WorkloadClass::WriteOnly => WorkloadConcurrency::try_new(0, 8, 0, 1),
            WorkloadClass::ReadsDuringWrites => WorkloadConcurrency::try_new(24, 8, 0, 1),
            WorkloadClass::BackfillUnderTraffic => WorkloadConcurrency::try_new(16, 4, 1, 1),
            WorkloadClass::LifecycleChurn => WorkloadConcurrency::try_new(16, 4, 1, 1),
            WorkloadClass::BackgroundMaintenance => WorkloadConcurrency::try_new(16, 4, 1, 1),
            WorkloadClass::RestartAndRecovery => WorkloadConcurrency::try_new(8, 2, 1, 1),
            WorkloadClass::MultiTenantContention => WorkloadConcurrency::try_new(16, 4, 1, 4),
        }
        .expect("frozen pre-launch workload topology has tenants");
        Self { class, concurrency }
    }

    /// Returns a short pull-request topology preserving every required role.
    pub fn pull_request(class: WorkloadClass) -> Self {
        let pre_launch = Self::pre_launch(class);
        let concurrency = WorkloadConcurrency::try_new(
            pre_launch.concurrency.readers.min(4),
            pre_launch.concurrency.writers.min(2),
            pre_launch.concurrency.workers.min(1),
            pre_launch.concurrency.tenants.get(),
        )
        .expect("pre-launch topology has tenants");
        Self { class, concurrency }
    }

    /// Returns the workload class.
    pub const fn class(self) -> WorkloadClass {
        self.class
    }

    /// Returns the explicit concurrency topology.
    pub const fn concurrency(self) -> WorkloadConcurrency {
        self.concurrency
    }
}

/// Configurable replica-lag acceptance boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaLagPolicy {
    maximum_duration: Duration,
    maximum_commits: u64,
}

impl ReplicaLagPolicy {
    /// Validates positive duration and sequence-progress limits.
    pub fn try_new(maximum_duration: Duration, maximum_commits: u64) -> Result<Self> {
        if maximum_duration.is_zero() {
            return Err(TestkitError::ModelViolation(
                "replica lag duration must be positive".to_string(),
            ));
        }
        if maximum_commits == 0 {
            return Err(TestkitError::ModelViolation(
                "replica lag commit limit must be positive".to_string(),
            ));
        }
        Ok(Self {
            maximum_duration,
            maximum_commits,
        })
    }

    /// Default launch contract: at most 30 seconds and 256 acknowledged commits.
    pub fn launch_default() -> Self {
        Self::try_new(Duration::from_secs(30), 256)
            .expect("frozen replica lag defaults are positive")
    }

    /// Returns the maximum elapsed convergence duration.
    pub const fn maximum_duration(self) -> Duration {
        self.maximum_duration
    }

    /// Returns the maximum measured sequence lag.
    pub const fn maximum_commits(self) -> u64 {
        self.maximum_commits
    }
}

/// Sequence-grounded metrics accumulated by a sustained adapter.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SustainedMetrics {
    /// Successful read requests.
    pub reads: u64,
    /// Acknowledged mutations.
    pub writes: u64,
    /// Typed serializable conflicts.
    pub conflicts: u64,
    /// Typed lifecycle races that require the caller to retry the request.
    pub retryable_failures: u64,
    /// Runtime restarts.
    pub restarts: u64,
    /// Maximum writer-to-reader storage-sequence lag.
    pub maximum_replica_lag_commits: u64,
}

impl SustainedMetrics {
    /// Records a reader's exact lag from the latest flushed writer sequence.
    pub fn observe_replica(&mut self, writer: DatabaseSequence, reader: DatabaseSequence) -> u64 {
        let lag = reader.lag_to(writer);
        self.maximum_replica_lag_commits = self.maximum_replica_lag_commits.max(lag);
        lag
    }

    /// Verifies measured sequence lag against a configured limit.
    pub fn validate_lag(self, policy: ReplicaLagPolicy) -> Result<()> {
        if self.maximum_replica_lag_commits > policy.maximum_commits {
            Err(TestkitError::ModelViolation(format!(
                "reader lag {} commits exceeds configured limit {}",
                self.maximum_replica_lag_commits, policy.maximum_commits
            )))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lag_policy_rejects_zeroes_and_metrics_use_sequence_progress() {
        assert!(ReplicaLagPolicy::try_new(Duration::ZERO, 1).is_err());
        assert!(ReplicaLagPolicy::try_new(Duration::from_secs(1), 0).is_err());
        let mut metrics = SustainedMetrics::default();
        assert_eq!(
            metrics.observe_replica(DatabaseSequence::new(10), DatabaseSequence::new(7)),
            3
        );
        assert!(metrics
            .validate_lag(ReplicaLagPolicy::try_new(Duration::from_secs(1), 2).unwrap())
            .is_err());
    }
}
