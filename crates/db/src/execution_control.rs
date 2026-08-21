//! Monotonic, request-scoped execution cancellation and write commit control.

use std::future::Future;
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use crate::error::{HelixDbError, Result};

const PRE_COMMIT: u8 = 0;
const COMMIT_STARTED: u8 = 1;
const ABORT_CLAIMED: u8 = 2;

/// Observable state of one request-local durable commit gate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteCommitState {
    /// No durable commit boundary has been entered.
    PreCommit,
    /// At least one durable commit boundary has been entered.
    CommitStarted,
    /// Drain expiry proved that the request cannot enter a durable commit.
    AbortClaimed,
}

/// Result of atomically claiming a write for drain expiry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteAbortClaim {
    /// The abort won before durable commit began.
    AbortClaimed,
    /// Durable commit had already begun, so the write outcome is unknown.
    CommitStarted,
}

#[derive(Debug)]
struct WriteCommitGateInner {
    state: AtomicU8,
    abort_notify: tokio::sync::Notify,
}

/// In-memory, request-local gate between pre-commit work and durable commit.
///
/// The gate is deliberately neither a receipt nor persistent transaction
/// metadata. A transport retains a clone only while classifying an admitted
/// request during a bounded writer drain.
#[derive(Clone, Debug)]
pub struct WriteCommitGate {
    inner: Arc<WriteCommitGateInner>,
}

impl Default for WriteCommitGate {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteCommitGate {
    /// Creates a gate before any durable commit boundary has been entered.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(WriteCommitGateInner {
                state: AtomicU8::new(PRE_COMMIT),
                abort_notify: tokio::sync::Notify::new(),
            }),
        }
    }

    /// Atomically claims the request for drain abort unless commit has started.
    #[must_use]
    pub fn claim_abort(&self) -> WriteAbortClaim {
        match self.inner.state.compare_exchange(
            PRE_COMMIT,
            ABORT_CLAIMED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                self.inner.abort_notify.notify_waiters();
                WriteAbortClaim::AbortClaimed
            }
            Err(ABORT_CLAIMED) => WriteAbortClaim::AbortClaimed,
            Err(COMMIT_STARTED) => WriteAbortClaim::CommitStarted,
            Err(state) => unreachable!("invalid write commit gate state {state}"),
        }
    }

    /// Returns the current monotonic request state.
    #[must_use]
    pub fn state(&self) -> WriteCommitState {
        match self.inner.state.load(Ordering::Acquire) {
            PRE_COMMIT => WriteCommitState::PreCommit,
            COMMIT_STARTED => WriteCommitState::CommitStarted,
            ABORT_CLAIMED => WriteCommitState::AbortClaimed,
            state => unreachable!("invalid write commit gate state {state}"),
        }
    }

    fn claim_commit(&self) -> Result<()> {
        match self.inner.state.compare_exchange(
            PRE_COMMIT,
            COMMIT_STARTED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) | Err(COMMIT_STARTED) => Ok(()),
            Err(ABORT_CLAIMED) => Err(HelixDbError::WriteAbortedByDrain),
            Err(state) => unreachable!("invalid write commit gate state {state}"),
        }
    }

    async fn wait_for_abort(&self) {
        loop {
            let notified = self.inner.abort_notify.notified();
            if self.state() == WriteCommitState::AbortClaimed {
                return;
            }
            notified.await;
        }
    }
}

/// Cooperative execution control captured at a transport boundary.
///
/// The deadline is monotonic and immutable so every planner/interpreter layer
/// observes the same attempt budget. An absent deadline preserves legacy
/// callers. A write gate is installed only by transports that need bounded
/// drain classification.
#[derive(Clone, Debug, Default)]
pub struct ExecutionControl {
    deadline: Option<Instant>,
    write_commit_gate: Option<WriteCommitGate>,
}

impl ExecutionControl {
    /// Creates legacy execution control with no deadline or write gate.
    #[must_use]
    pub const fn unlimited() -> Self {
        Self {
            deadline: None,
            write_commit_gate: None,
        }
    }

    /// Creates execution control expiring after `timeout`.
    #[must_use]
    pub fn from_timeout(timeout: Duration) -> Self {
        Self {
            deadline: Some(Instant::now() + timeout),
            write_commit_gate: None,
        }
    }

    /// Attaches one request-local gate to every durable write commit boundary.
    #[must_use]
    pub fn with_write_commit_gate(mut self, gate: WriteCommitGate) -> Self {
        self.write_commit_gate = Some(gate);
        self
    }

    /// Returns an error once the request is aborted or its deadline elapses.
    pub fn check(&self) -> Result<()> {
        if self
            .write_commit_gate
            .as_ref()
            .is_some_and(|gate| gate.state() == WriteCommitState::AbortClaimed)
        {
            return Err(HelixDbError::WriteAbortedByDrain);
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(HelixDbError::QueryDeadlineExceeded);
        }
        Ok(())
    }

    /// Claims the request before invoking a durable storage commit.
    pub(crate) fn claim_write_commit(&self) -> Result<()> {
        match &self.write_commit_gate {
            Some(gate) => gate.claim_commit(),
            None => Ok(()),
        }
    }

    /// Runs cancellable pre-commit work until abort or execution expiry.
    ///
    /// Durable commit and required post-commit finalization must not be wrapped
    /// with this helper.
    pub(crate) async fn run<F, T>(&self, future: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        self.check()?;
        match (&self.write_commit_gate, self.deadline) {
            (None, None) => future.await,
            (None, Some(deadline)) => {
                match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), future)
                    .await
                {
                    Ok(result) => result,
                    Err(_) => Err(HelixDbError::QueryDeadlineExceeded),
                }
            }
            (Some(gate), None) => {
                tokio::select! {
                    result = future => result,
                    () = gate.wait_for_abort() => Err(HelixDbError::WriteAbortedByDrain),
                }
            }
            (Some(gate), Some(deadline)) => {
                tokio::select! {
                    result = future => result,
                    () = gate.wait_for_abort() => Err(HelixDbError::WriteAbortedByDrain),
                    () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                        Err(HelixDbError::QueryDeadlineExceeded)
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unlimited_control_preserves_legacy_execution() {
        assert!(ExecutionControl::unlimited().check().is_ok());
    }

    #[test]
    fn elapsed_deadline_returns_typed_database_error() {
        let error = ExecutionControl::from_timeout(Duration::ZERO)
            .check()
            .expect_err("zero timeout must already be elapsed");
        assert!(matches!(error, HelixDbError::QueryDeadlineExceeded));
    }

    #[test]
    fn commit_and_abort_claims_are_monotonic_and_mutually_exclusive() {
        let committed = WriteCommitGate::new();
        let control = ExecutionControl::unlimited().with_write_commit_gate(committed.clone());
        control
            .claim_write_commit()
            .expect("commit claims the gate");
        assert_eq!(committed.state(), WriteCommitState::CommitStarted);
        assert_eq!(committed.claim_abort(), WriteAbortClaim::CommitStarted);
        control
            .claim_write_commit()
            .expect("subsequent request commits share the terminal commit claim");

        let aborted = WriteCommitGate::new();
        let control = ExecutionControl::unlimited().with_write_commit_gate(aborted.clone());
        assert_eq!(aborted.claim_abort(), WriteAbortClaim::AbortClaimed);
        assert_eq!(aborted.claim_abort(), WriteAbortClaim::AbortClaimed);
        assert!(matches!(
            control.claim_write_commit(),
            Err(HelixDbError::WriteAbortedByDrain)
        ));
        assert_eq!(aborted.state(), WriteCommitState::AbortClaimed);
    }

    #[tokio::test]
    async fn cancellable_precommit_work_stops_at_deadline() {
        let control = ExecutionControl::from_timeout(Duration::from_millis(1));
        let error = control
            .run(async {
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok::<_, HelixDbError>(())
            })
            .await
            .expect_err("pending pre-commit work must be cancelled");
        assert!(matches!(error, HelixDbError::QueryDeadlineExceeded));
    }

    #[tokio::test]
    async fn abort_claim_wakes_precommit_execution() {
        let gate = WriteCommitGate::new();
        let control = ExecutionControl::unlimited().with_write_commit_gate(gate.clone());
        let execution = tokio::spawn(async move {
            control
                .run(async {
                    std::future::pending::<()>().await;
                    Ok::<_, HelixDbError>(())
                })
                .await
        });
        assert_eq!(gate.claim_abort(), WriteAbortClaim::AbortClaimed);
        let error = execution
            .await
            .expect("execution task joins")
            .expect_err("abort must stop pre-commit work");
        assert!(matches!(error, HelixDbError::WriteAbortedByDrain));
    }
}
