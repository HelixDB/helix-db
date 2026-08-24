//! Monotonic, request-scoped execution cancellation.

use std::future::Future;
use std::time::{Duration, Instant};

use crate::error::{HelixDbError, Result};

/// Cooperative execution control captured at a transport boundary.
///
/// The deadline is monotonic and immutable so every planner/interpreter layer
/// observes the same attempt budget. An absent deadline preserves legacy
/// callers.
#[derive(Clone, Copy, Debug, Default)]
pub struct ExecutionControl {
    deadline: Option<Instant>,
}

impl ExecutionControl {
    /// Creates execution control with no deadline.
    #[must_use]
    pub const fn unlimited() -> Self {
        Self { deadline: None }
    }

    /// Creates execution control expiring after `timeout`.
    #[must_use]
    pub fn from_timeout(timeout: Duration) -> Self {
        Self {
            deadline: Some(Instant::now() + timeout),
        }
    }

    /// Returns an error once the request deadline elapses.
    pub fn check(self) -> Result<()> {
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(HelixDbError::QueryDeadlineExceeded);
        }
        Ok(())
    }

    /// Runs cancellable work until execution expiry.
    pub(crate) async fn run<F, T>(self, future: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        self.check()?;
        let Some(deadline) = self.deadline else {
            return future.await;
        };
        match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), future).await {
            Ok(result) => result,
            Err(_) => Err(HelixDbError::QueryDeadlineExceeded),
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
}
