//! Monotonic, request-scoped execution cancellation.

use std::future::Future;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use crate::error::{HelixDbError, Result};

#[derive(Debug)]
struct ReaderRetirementCancellationInner {
    cancelled: AtomicBool,
    notify: tokio::sync::Notify,
}

/// Monotonic cancellation shared by every read admitted before retirement.
///
/// Cancellation is process-local and irreversible. The transport owns the
/// matching retirement operation identity and maps this signal to its typed
/// wire response.
///
/// # Examples
///
/// ```
/// use db::execution_control::ReaderRetirementCancellation;
///
/// let cancellation = ReaderRetirementCancellation::new();
/// assert!(!cancellation.is_cancelled());
/// cancellation.cancel();
/// assert!(cancellation.is_cancelled());
/// ```
#[derive(Clone, Debug)]
pub struct ReaderRetirementCancellation {
    inner: Arc<ReaderRetirementCancellationInner>,
}

impl Default for ReaderRetirementCancellation {
    fn default() -> Self {
        Self::new()
    }
}

impl ReaderRetirementCancellation {
    /// Creates an uncancelled reader-retirement signal.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ReaderRetirementCancellationInner {
                cancelled: AtomicBool::new(false),
                notify: tokio::sync::Notify::new(),
            }),
        }
    }

    /// Cancels every execution that observes this signal.
    pub fn cancel(&self) {
        if !self.inner.cancelled.swap(true, Ordering::AcqRel) {
            self.inner.notify.notify_waiters();
        }
    }

    /// Returns whether retirement cancellation has occurred.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    async fn wait_for_cancellation(&self) {
        loop {
            let notified = self.inner.notify.notified();
            if self.is_cancelled() {
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
/// callers.
#[derive(Clone, Debug, Default)]
pub struct ExecutionControl {
    deadline: Option<Instant>,
    reader_retirement: Option<ReaderRetirementCancellation>,
}

impl ExecutionControl {
    /// Creates execution control with no deadline.
    #[must_use]
    pub const fn unlimited() -> Self {
        Self {
            deadline: None,
            reader_retirement: None,
        }
    }

    /// Creates execution control expiring after `timeout`.
    #[must_use]
    pub fn from_timeout(timeout: Duration) -> Self {
        Self {
            deadline: Some(Instant::now() + timeout),
            reader_retirement: None,
        }
    }

    /// Attaches the process-local retirement signal for one admitted read.
    #[must_use]
    pub fn with_reader_retirement_cancellation(
        mut self,
        cancellation: ReaderRetirementCancellation,
    ) -> Self {
        self.reader_retirement = Some(cancellation);
        self
    }

    /// Returns an error once the request deadline elapses or retirement starts.
    pub fn check(&self) -> Result<()> {
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(HelixDbError::QueryDeadlineExceeded);
        }
        if self
            .reader_retirement
            .as_ref()
            .is_some_and(ReaderRetirementCancellation::is_cancelled)
        {
            return Err(HelixDbError::QueryCancelledByReaderRetirement);
        }
        Ok(())
    }

    /// Runs cancellable work until execution expiry.
    pub(crate) async fn run<F, T>(&self, future: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        self.check()?;
        let execution = async {
            let Some(deadline) = self.deadline else {
                return future.await;
            };
            match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), future).await {
                Ok(result) => result,
                Err(_) => Err(HelixDbError::QueryDeadlineExceeded),
            }
        };

        let Some(cancellation) = &self.reader_retirement else {
            return execution.await;
        };
        tokio::select! {
            biased;
            () = cancellation.wait_for_cancellation() => {
                if self.deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    Err(HelixDbError::QueryDeadlineExceeded)
                } else {
                    Err(HelixDbError::QueryCancelledByReaderRetirement)
                }
            }
            result = execution => result,
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

    #[tokio::test]
    async fn cancellable_work_stops_at_deadline() {
        let control = ExecutionControl::from_timeout(Duration::from_millis(1));
        let error = control
            .run(async {
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok::<_, HelixDbError>(())
            })
            .await
            .expect_err("pending work must be cancelled");
        assert!(matches!(error, HelixDbError::QueryDeadlineExceeded));
    }

    #[tokio::test]
    async fn reader_retirement_wakes_execution_with_typed_error() {
        let cancellation = ReaderRetirementCancellation::new();
        let control =
            ExecutionControl::unlimited().with_reader_retirement_cancellation(cancellation.clone());
        let execution = tokio::spawn(async move {
            control
                .run(async {
                    std::future::pending::<()>().await;
                    Ok::<_, HelixDbError>(())
                })
                .await
        });

        cancellation.cancel();
        cancellation.cancel();
        let error = execution
            .await
            .expect("execution task joins")
            .expect_err("retirement must stop admitted read work");
        assert!(matches!(
            error,
            HelixDbError::QueryCancelledByReaderRetirement
        ));
        assert!(cancellation.is_cancelled());
    }

    #[tokio::test]
    async fn elapsed_explicit_deadline_precedes_reader_retirement() {
        let cancellation = ReaderRetirementCancellation::new();
        cancellation.cancel();
        let control = ExecutionControl::from_timeout(Duration::ZERO)
            .with_reader_retirement_cancellation(cancellation);

        let error = control
            .run(async { Ok::<_, HelixDbError>(()) })
            .await
            .expect_err("elapsed deadline must remain terminal");
        assert!(matches!(error, HelixDbError::QueryDeadlineExceeded));
    }
}
