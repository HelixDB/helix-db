//! Stable crash-injection boundaries for durable index lifecycle transitions.
//!
//! The outbox recovery harness can request one named boundary through
//! `HELIX_INDEX_OUTBOX_FAILPOINT`. Setting
//! `HELIX_INDEX_OUTBOX_FAIL_ACTION=abort` terminates the process at that
//! boundary; otherwise the boundary returns an injected error. Tests use the
//! same typed enum through a one-shot in-process injector.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use crate::error::{HelixDbError, Result};

use super::IndexOperationId;

/// Every stable boundary surrounding an outbox durability transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexOutboxFailpoint {
    /// Immediately before a DDL enqueue transaction begins.
    DdlEnqueueBefore,
    /// Immediately after canonical and outbox rows are staged but before commit.
    DdlEnqueueAfterStaging,
    /// Immediately before an eligible pointer is claimed.
    ClaimBefore,
    /// Immediately after the claim commit is durable.
    ClaimAfter,
    /// Immediately before a claimed operation's bounded read.
    BatchReadBefore,
    /// Immediately after a claimed operation's bounded read.
    BatchReadAfter,
    /// Immediately before a family driver stages physical changes.
    PhysicalStagingBefore,
    /// Immediately after a family driver stages physical changes.
    PhysicalStagingAfter,
    /// Immediately before the next operation checkpoint is staged.
    CheckpointStagingBefore,
    /// Immediately after the next operation checkpoint is staged.
    CheckpointStagingAfter,
    /// Immediately before a claimed step transaction commits.
    CommitBefore,
    /// Immediately after a claimed step transaction commits.
    CommitAfter,
    /// Immediately before a completed build activates its canonical record.
    ActivationBefore,
    /// Immediately after activation state is staged.
    ActivationAfter,
    /// Immediately before a blocked or completed pointer is removed.
    QueueRemovalBefore,
    /// Immediately after pointer removal is staged.
    QueueRemovalAfter,
}

impl IndexOutboxFailpoint {
    /// Complete set used by crash-matrix tests and external harnesses.
    #[cfg(any(
        test,
        feature = "migration-parity",
        feature = "production-coverage",
        feature = "index-lifecycle-testing"
    ))]
    pub const ALL: [Self; 16] = [
        Self::DdlEnqueueBefore,
        Self::DdlEnqueueAfterStaging,
        Self::ClaimBefore,
        Self::ClaimAfter,
        Self::BatchReadBefore,
        Self::BatchReadAfter,
        Self::PhysicalStagingBefore,
        Self::PhysicalStagingAfter,
        Self::CheckpointStagingBefore,
        Self::CheckpointStagingAfter,
        Self::CommitBefore,
        Self::CommitAfter,
        Self::ActivationBefore,
        Self::ActivationAfter,
        Self::QueueRemovalBefore,
        Self::QueueRemovalAfter,
    ];

    /// Stable snake-case environment value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DdlEnqueueBefore => "ddl_enqueue_before",
            Self::DdlEnqueueAfterStaging => "ddl_enqueue_after_staging",
            Self::ClaimBefore => "claim_before",
            Self::ClaimAfter => "claim_after",
            Self::BatchReadBefore => "batch_read_before",
            Self::BatchReadAfter => "batch_read_after",
            Self::PhysicalStagingBefore => "physical_staging_before",
            Self::PhysicalStagingAfter => "physical_staging_after",
            Self::CheckpointStagingBefore => "checkpoint_staging_before",
            Self::CheckpointStagingAfter => "checkpoint_staging_after",
            Self::CommitBefore => "commit_before",
            Self::CommitAfter => "commit_after",
            Self::ActivationBefore => "activation_before",
            Self::ActivationAfter => "activation_after",
            Self::QueueRemovalBefore => "queue_removal_before",
            Self::QueueRemovalAfter => "queue_removal_after",
        }
    }

    /// Parses one stable environment value.
    #[cfg(any(
        test,
        feature = "production-coverage",
        feature = "index-lifecycle-testing"
    ))]
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|failpoint| failpoint.as_str() == value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InjectedFailpoint {
    failpoint: IndexOutboxFailpoint,
    operation_id: Option<IndexOperationId>,
}

static INJECTED_FAILPOINT: Mutex<Option<InjectedFailpoint>> = Mutex::new(None);
static FAILPOINT_TRIGGERED: AtomicBool = AtomicBool::new(false);

/// Installs one process-local failure for deterministic recovery tests.
#[cfg(any(
    test,
    feature = "production-coverage",
    feature = "index-lifecycle-testing"
))]
pub(crate) fn inject_once(failpoint: IndexOutboxFailpoint) -> Result<()> {
    inject(failpoint, None)
}

/// Installs one failure that only the named operation can consume.
#[cfg(any(
    test,
    feature = "production-coverage",
    feature = "index-lifecycle-testing"
))]
pub(crate) fn inject_for_operation_once(
    failpoint: IndexOutboxFailpoint,
    operation_id: IndexOperationId,
) -> Result<()> {
    inject(failpoint, Some(operation_id))
}

#[cfg(any(
    test,
    feature = "production-coverage",
    feature = "index-lifecycle-testing"
))]
fn inject(failpoint: IndexOutboxFailpoint, operation_id: Option<IndexOperationId>) -> Result<()> {
    let mut injected = INJECTED_FAILPOINT.lock().map_err(|_| {
        HelixDbError::InvariantViolation("index outbox failpoint mutex was poisoned".to_string())
    })?;
    *injected = Some(InjectedFailpoint {
        failpoint,
        operation_id,
    });
    FAILPOINT_TRIGGERED.store(false, Ordering::SeqCst);
    Ok(())
}

/// Reports whether the current one-shot test failure fired.
#[cfg(any(
    test,
    feature = "production-coverage",
    feature = "index-lifecycle-testing"
))]
pub(crate) fn was_triggered() -> bool {
    FAILPOINT_TRIGGERED.load(Ordering::SeqCst)
}

/// Trips a configured crash boundary without changing durable data itself.
pub(crate) fn trip(failpoint: IndexOutboxFailpoint) -> Result<()> {
    trip_for_operation_inner(failpoint, None)
}

/// Trips a configured boundary for one exact durable operation.
pub(crate) fn trip_for_operation(
    failpoint: IndexOutboxFailpoint,
    operation_id: IndexOperationId,
) -> Result<()> {
    trip_for_operation_inner(failpoint, Some(operation_id))
}

fn trip_for_operation_inner(
    failpoint: IndexOutboxFailpoint,
    operation_id: Option<IndexOperationId>,
) -> Result<()> {
    let mut injected = INJECTED_FAILPOINT.lock().map_err(|_| {
        HelixDbError::InvariantViolation("index outbox failpoint mutex was poisoned".to_string())
    })?;
    if injected.is_some_and(|injected| {
        injected.failpoint == failpoint
            && injected
                .operation_id
                .is_none_or(|expected| Some(expected) == operation_id)
    }) {
        *injected = None;
        FAILPOINT_TRIGGERED.store(true, Ordering::SeqCst);
        return Err(injected_error(failpoint));
    }
    drop(injected);

    if std::env::var("HELIX_INDEX_OUTBOX_FAILPOINT").as_deref() != Ok(failpoint.as_str()) {
        return Ok(());
    }
    if std::env::var("HELIX_INDEX_OUTBOX_FAIL_ACTION").as_deref() == Ok("abort") {
        std::process::abort();
    }
    Err(injected_error(failpoint))
}

fn injected_error(failpoint: IndexOutboxFailpoint) -> HelixDbError {
    HelixDbError::InvariantViolation(format!(
        "injected index outbox failpoint {}",
        failpoint.as_str()
    ))
}

#[cfg(feature = "production-coverage")]
#[path = "../../tests/production_support/index_lifecycle_failpoints.rs"]
pub(crate) mod production_contracts;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_names_round_trip_and_one_shot_injection_clears() {
        for failpoint in IndexOutboxFailpoint::ALL {
            assert_eq!(
                IndexOutboxFailpoint::parse(failpoint.as_str()),
                Some(failpoint)
            );
        }
        assert_eq!(IndexOutboxFailpoint::parse("unknown"), None);

        inject_once(IndexOutboxFailpoint::ClaimBefore).unwrap();
        assert!(trip(IndexOutboxFailpoint::ClaimBefore).is_err());
        assert!(was_triggered());
        assert!(trip(IndexOutboxFailpoint::ClaimBefore).is_ok());

        let expected = IndexOperationId::new_v4();
        let other = IndexOperationId::new_v4();
        inject_for_operation_once(IndexOutboxFailpoint::CommitBefore, expected).unwrap();
        assert!(trip_for_operation(IndexOutboxFailpoint::CommitBefore, other).is_ok());
        assert!(!was_triggered());
        assert!(trip(IndexOutboxFailpoint::CommitBefore).is_ok());
        assert!(!was_triggered());
        assert!(trip_for_operation(IndexOutboxFailpoint::CommitBefore, expected).is_err());
        assert!(was_triggered());
    }
}
