//! Search work that runs on the blocking pool.
//!
//! Decoding the pending backlog and scoring it exactly grow with the
//! unpublished work rather than with `k`, so they run on Tokio's blocking
//! pool instead of stalling the async workers every other request needs.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::*;

/// Lets blocking search work stop once nobody needs its result.
#[derive(Debug)]
pub(super) struct BlockingProbe {
    control: crate::execution_control::ExecutionControl,
    abandoned: Arc<AtomicBool>,
}

impl BlockingProbe {
    /// Fails once the request's deadline passes, reader retirement starts,
    /// or the request stopped awaiting the work.
    ///
    /// An abandoned result is never observed, so the error it returns only
    /// ends the work early.
    pub(super) fn check(&self) -> Result<()> {
        self.control.check()?;
        if self.abandoned.load(Ordering::Relaxed) {
            return Err(HelixDbError::QueryDeadlineExceeded);
        }
        Ok(())
    }
}

/// Marks the work abandoned when the awaiting request future is dropped.
struct AbandonOnDrop(Arc<AtomicBool>);

impl Drop for AbandonOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// Runs `work` on the blocking pool under the request's `control`.
///
/// The request may stop awaiting at any point: its deadline elapses, reader
/// retirement cancels it, or the client goes away and the request future is
/// dropped. Blocking work cannot be preempted, so `work` should call
/// [`BlockingProbe::check`] between bounded steps; it then stops at the next
/// one. Errors from `work` propagate unchanged, and a panic in it fails the
/// request with an invariant violation instead of unwinding an async worker.
pub(super) async fn run_blocking<T, Work>(
    control: &crate::execution_control::ExecutionControl,
    work: Work,
) -> Result<T>
where
    T: Send + 'static,
    Work: FnOnce(&BlockingProbe) -> Result<T> + Send + 'static,
{
    control.check()?;
    let abandoned = Arc::new(AtomicBool::new(false));
    let _abandon = AbandonOnDrop(Arc::clone(&abandoned));
    let probe = BlockingProbe {
        control: control.clone(),
        abandoned,
    };
    tokio::task::spawn_blocking(move || work(&probe))
        .await
        .map_err(|error| {
            HelixDbError::InvariantViolation(format!("blocking search work failed: {error}"))
        })?
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::execution_control::ExecutionControl;

    #[tokio::test]
    async fn blocking_work_returns_its_result_and_propagates_its_error() {
        let control = ExecutionControl::unlimited();
        assert_eq!(run_blocking(&control, |_| Ok(7)).await.unwrap(), 7);
        let error = run_blocking::<(), _>(&control, |_| {
            Err(HelixDbError::IndexCatalogCorruption("damaged".to_string()))
        })
        .await
        .expect_err("the work's error propagates");
        assert!(
            matches!(&error, HelixDbError::IndexCatalogCorruption(message) if message == "damaged"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn a_panic_in_blocking_work_fails_the_request() {
        let error =
            run_blocking::<(), _>(&ExecutionControl::unlimited(), |_| panic!("scoring bug"))
                .await
                .expect_err("a panic fails the request");
        assert!(
            matches!(error, HelixDbError::InvariantViolation(_)),
            "{error}"
        );
    }

    #[tokio::test]
    async fn an_expired_request_never_starts_blocking_work() {
        let started = Arc::new(AtomicBool::new(false));
        let work = {
            let started = Arc::clone(&started);
            move |_: &BlockingProbe| {
                started.store(true, Ordering::Relaxed);
                Ok(())
            }
        };
        let error = run_blocking(&ExecutionControl::from_timeout(Duration::ZERO), work)
            .await
            .expect_err("an expired request starts nothing");
        assert!(matches!(error, HelixDbError::QueryDeadlineExceeded));
        assert!(!started.load(Ordering::Relaxed));
    }

    /// Work whose request stops awaiting it, by deadline or by being
    /// dropped, sees the probe fail at its next check.
    #[tokio::test]
    async fn blocking_work_stops_once_its_request_stops_awaiting_it() {
        for deadline in [true, false] {
            let (entered, entered_signal) = std::sync::mpsc::channel();
            let (stopped, stopped_signal) = std::sync::mpsc::channel();
            let control = if deadline {
                ExecutionControl::from_timeout(Duration::from_millis(200))
            } else {
                ExecutionControl::unlimited()
            };
            let work = move |probe: &BlockingProbe| {
                entered.send(()).unwrap();
                let failed = std::iter::repeat_with(|| {
                    std::thread::sleep(Duration::from_millis(5));
                    probe.check()
                })
                .take(10_000)
                .find_map(Result::err);
                stopped.send(failed.is_some()).unwrap();
                // Like real search work, return the probe's error: the work
                // can finish before the deadline timer is polled, and the
                // request must still fail.
                failed.map_or(Ok(()), Err)
            };
            let request = {
                let control = control.clone();
                tokio::spawn(async move { control.run(run_blocking(&control, work)).await })
            };
            tokio::task::spawn_blocking(move || entered_signal.recv().unwrap())
                .await
                .unwrap();
            if deadline {
                let error = request.await.unwrap().expect_err("the deadline elapses");
                assert!(matches!(error, HelixDbError::QueryDeadlineExceeded));
            } else {
                request.abort();
                assert!(request.await.unwrap_err().is_cancelled());
            }
            let stopped = tokio::task::spawn_blocking(move || {
                stopped_signal
                    .recv_timeout(Duration::from_secs(30))
                    .unwrap()
            })
            .await
            .unwrap();
            assert!(
                stopped,
                "deadline {deadline}: the probe fails once abandoned"
            );
        }
    }
}
