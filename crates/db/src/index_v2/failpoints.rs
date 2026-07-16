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

/// Every stable boundary surrounding an outbox durability transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexOutboxFailpoint {
    /// Immediately before an eligible pointer is claimed.
    ClaimBefore,
    /// Immediately after the claim commit is durable.
    ClaimAfter,
    /// Immediately before an eligible upload pointer is claimed.
    UploadClaimBefore,
    /// Immediately after the upload claim commit is durable.
    UploadClaimAfter,
    /// Immediately before a claimed upload invokes its reconciliation driver.
    UploadStepBefore,
    /// Immediately after the upload reconciliation driver returns.
    UploadStepAfter,
    /// Immediately before the next upload checkpoint is staged.
    UploadCheckpointBefore,
    /// Immediately after the next upload checkpoint is staged.
    UploadCheckpointAfter,
    /// Immediately before a claimed upload checkpoint commits.
    UploadCommitBefore,
    /// Immediately after a claimed upload checkpoint is durable.
    UploadCommitAfter,
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
    #[cfg(test)]
    pub const ALL: [Self; 22] = [
        Self::ClaimBefore,
        Self::ClaimAfter,
        Self::UploadClaimBefore,
        Self::UploadClaimAfter,
        Self::UploadStepBefore,
        Self::UploadStepAfter,
        Self::UploadCheckpointBefore,
        Self::UploadCheckpointAfter,
        Self::UploadCommitBefore,
        Self::UploadCommitAfter,
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
            Self::ClaimBefore => "claim_before",
            Self::ClaimAfter => "claim_after",
            Self::UploadClaimBefore => "upload_claim_before",
            Self::UploadClaimAfter => "upload_claim_after",
            Self::UploadStepBefore => "upload_step_before",
            Self::UploadStepAfter => "upload_step_after",
            Self::UploadCheckpointBefore => "upload_checkpoint_before",
            Self::UploadCheckpointAfter => "upload_checkpoint_after",
            Self::UploadCommitBefore => "upload_commit_before",
            Self::UploadCommitAfter => "upload_commit_after",
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
    #[cfg(test)]
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|failpoint| failpoint.as_str() == value)
    }
}

static INJECTED_FAILPOINT: Mutex<Option<IndexOutboxFailpoint>> = Mutex::new(None);
static FAILPOINT_TRIGGERED: AtomicBool = AtomicBool::new(false);

/// Installs one process-local failure for deterministic recovery tests.
#[cfg(any(test, feature = "production-coverage"))]
pub(crate) fn inject_once(failpoint: IndexOutboxFailpoint) -> Result<()> {
    let mut injected = INJECTED_FAILPOINT.lock().map_err(|_| {
        HelixDbError::InvariantViolation("index outbox failpoint mutex was poisoned".to_string())
    })?;
    *injected = Some(failpoint);
    FAILPOINT_TRIGGERED.store(false, Ordering::SeqCst);
    Ok(())
}

/// Reports whether the current one-shot test failure fired.
#[cfg(any(test, feature = "production-coverage"))]
pub(crate) fn was_triggered() -> bool {
    FAILPOINT_TRIGGERED.load(Ordering::SeqCst)
}

/// Trips a configured crash boundary without changing durable data itself.
pub(crate) fn trip(failpoint: IndexOutboxFailpoint) -> Result<()> {
    let mut injected = INJECTED_FAILPOINT.lock().map_err(|_| {
        HelixDbError::InvariantViolation("index outbox failpoint mutex was poisoned".to_string())
    })?;
    if *injected == Some(failpoint) {
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
    }
}
