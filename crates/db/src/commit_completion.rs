//! Finite commit completions survive request cancellation and drain at close.
//! A completion runs in its request's task; a request dropped before it finishes
//! hands the rest to a spawned task. Tracking owns no task handles or database
//! references, so task/runtime ownership remains acyclic. Sealing rejects new
//! commits before any work starts.
use crate::error::{HelixDbError, Result};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::watch;

#[derive(Debug, Clone, Copy)]
enum State {
    Open(usize),
    Sealed(usize),
}

pub(crate) struct Tracker {
    state: watch::Sender<State>,
}
impl Default for Tracker {
    fn default() -> Self {
        Self {
            state: watch::channel(State::Open(0)).0,
        }
    }
}

struct Completion {
    state: watch::Sender<State>,
}
impl Drop for Completion {
    fn drop(&mut self) {
        self.state.send_modify(|state| {
            let (State::Open(active) | State::Sealed(active)) = state;
            *active = active
                .checked_sub(1)
                .expect("each completion owns one live admission");
        });
    }
}

impl Tracker {
    /// Admit one finite completion that runs in the caller's task. Dropping it
    /// before it finishes does not abandon it: the rest runs as a spawned task.
    /// Its token is released on success, error or panic.
    pub(crate) fn run<F>(&self, future: F) -> Result<InPlace<F>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let accepted = self.state.send_if_modified(|state| {
            let State::Open(active) = state else {
                return false;
            };
            *active = active
                .checked_add(1)
                .expect("live commit count fits address space");
            true
        });
        if !accepted {
            return Err(HelixDbError::DatabaseClosed);
        }
        Ok(InPlace {
            future: Some(Box::pin(future)),
            completion: Some(Completion {
                state: self.state.clone(),
            }),
        })
    }

    /// Permanently reject new admissions before the shutdown owner is spawned.
    pub(crate) fn seal(&self) {
        self.state.send_modify(|state| {
            let (State::Open(active) | State::Sealed(active)) = *state;
            *state = State::Sealed(active);
        });
    }

    /// Permanently seal new admissions and wait for every started completion.
    /// Subscribe after sealing: watch retains the state, including an already
    /// completed final task. Multiple drains and cancellation/retry are safe.
    pub(crate) async fn seal_and_wait(&self) {
        self.seal();
        let mut state = self.state.subscribe();
        state
            .wait_for(|state| matches!(state, State::Sealed(0)))
            .await
            .expect("tracker retains the state sender");
    }
}

/// An admitted completion polled by its request, or detached when dropped.
pub(crate) struct InPlace<F: Future + Send + 'static>
where
    F::Output: Send + 'static,
{
    future: Option<Pin<Box<F>>>,
    completion: Option<Completion>,
}

impl<F> Future for InPlace<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<F::Output> {
        // Held outside `self` while polled, so a future that panics is dropped
        // by the unwind and never resumed.
        let mut future = self
            .future
            .take()
            .expect("an in-place completion is not polled after it finishes");
        let Poll::Ready(output) = future.as_mut().poll(context) else {
            self.future = Some(future);
            return Poll::Pending;
        };
        self.completion = None;
        Poll::Ready(output)
    }
}

impl<F> Drop for InPlace<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn drop(&mut self) {
        // A finished or panicked completion has nothing left to run, and
        // without a runtime the rest cannot run; each releases its token.
        let (Some(future), Some(completion)) = (self.future.take(), self.completion.take()) else {
            return;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        runtime.spawn(async move {
            future.await;
            drop(completion);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    #[tokio::test]
    async fn dropping_a_waiter_retains_work_and_sealing_rejects_new_work() {
        let tracker = Tracker::default();
        let (release, wait) = tokio::sync::oneshot::channel();
        let done = Arc::new(AtomicBool::new(false));
        let completed = Arc::clone(&done);
        let task = tracker
            .run(async move {
                wait.await.unwrap();
                completed.store(true, Ordering::Release);
            })
            .unwrap();
        drop(task);
        let mut drain = Box::pin(tracker.seal_and_wait());
        assert!(futures::poll!(&mut drain).is_pending());
        let ran = Arc::new(AtomicBool::new(false));
        let rejected = Arc::clone(&ran);
        assert!(matches!(
            tracker.run(async move {
                rejected.store(true, Ordering::Release);
            }),
            Err(HelixDbError::DatabaseClosed)
        ));
        assert!(!ran.load(Ordering::Acquire));
        // Dropping a shutdown waiter must not reopen admission or lose the task.
        drop(drain);
        assert!(tracker.seal_and_wait().now_or_never().is_none());
        release.send(()).unwrap();
        tracker.seal_and_wait().await;
        assert!(done.load(Ordering::Acquire));
        assert!(tracker.seal_and_wait().now_or_never().is_some());
    }

    #[tokio::test]
    async fn failure_and_panic_release_their_ownership() {
        let tracker = Tracker::default();
        let error = tracker
            .run(async { Err::<(), _>("storage failure") })
            .unwrap()
            .await;
        assert_eq!(error, Err("storage failure"));
        let panic = std::panic::AssertUnwindSafe(
            tracker
                .run(async {
                    panic!("completion panic");
                })
                .unwrap(),
        )
        .catch_unwind()
        .await;
        assert!(panic.is_err());
        assert!(tracker.seal_and_wait().now_or_never().is_some());
    }

    #[tokio::test]
    async fn a_completion_dropped_mid_way_finishes_detached() {
        let tracker = Tracker::default();
        let (release, wait) = tokio::sync::oneshot::channel();
        let done = Arc::new(AtomicBool::new(false));
        let completed = Arc::clone(&done);
        let mut completion = Box::pin(
            tracker
                .run(async move {
                    tokio::task::yield_now().await;
                    wait.await.unwrap();
                    completed.store(true, Ordering::Release);
                    17
                })
                .unwrap(),
        );
        assert!(futures::poll!(&mut completion).is_pending());
        drop(completion);
        let mut drain = Box::pin(tracker.seal_and_wait());
        assert!(futures::poll!(&mut drain).is_pending());
        release.send(()).unwrap();
        drain.await;
        assert!(done.load(Ordering::Acquire));
        // An undisturbed completion returns its output in place.
        let tracker = Tracker::default();
        assert_eq!(tracker.run(async { 17 }).unwrap().await, 17);
        assert!(tracker.seal_and_wait().now_or_never().is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn admission_racing_with_shutdown_never_loses_started_work() {
        let tracker = Arc::new(Tracker::default());
        let start = Arc::new(tokio::sync::Barrier::new(65));
        let completed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut attempts = Vec::new();
        for _ in 0..64 {
            let tracker = Arc::clone(&tracker);
            let start = Arc::clone(&start);
            let completed = Arc::clone(&completed);
            attempts.push(tokio::spawn(async move {
                start.wait().await;
                match tracker.run(async move {
                    tokio::task::yield_now().await;
                    completed.fetch_add(1, Ordering::AcqRel);
                }) {
                    Ok(task) => {
                        drop(task);
                        true
                    }
                    Err(HelixDbError::DatabaseClosed) => false,
                    Err(error) => panic!("unexpected admission error: {error}"),
                }
            }));
        }
        start.wait().await;
        tracker.seal_and_wait().await;
        let mut accepted = 0;
        for attempt in attempts {
            accepted += usize::from(attempt.await.unwrap());
        }
        assert_eq!(completed.load(Ordering::Acquire), accepted);
    }

    #[tokio::test]
    async fn database_close_drains_owned_completion_without_retaining_the_runtime() {
        let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
            database: "commit-close-owner".into(),
        })
        .await
        .unwrap();
        let runtime = Arc::downgrade(&db.inner);
        let owned = crate::HelixDB {
            inner: Arc::clone(&db.inner),
        };
        let (release, wait) = tokio::sync::oneshot::channel();
        let task = db
            .inner
            .commit_completions
            .run(async move {
                wait.await.unwrap();
                // The completion still has its original live storage authority.
                let key = crate::index_lifecycle::graph_mutation::GraphEntity::node(99)
                    .property_key(crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped);
                owned.inner_db().get(key).await.unwrap();
            })
            .unwrap();
        drop(task);
        let mut close = Box::pin(db.close());
        assert!(futures::poll!(&mut close).is_pending());
        assert!(matches!(
            db.inner.commit_completions.run(async {}),
            Err(HelixDbError::DatabaseClosed)
        ));
        release.send(()).unwrap();
        close.await.unwrap();
        drop(db);
        assert!(
            runtime.upgrade().is_none(),
            "completed task retained its runtime"
        );
    }

    #[tokio::test]
    async fn dropping_the_close_waiter_does_not_abandon_shutdown() {
        let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
            database: "commit-close-cancelled-waiter".into(),
        })
        .await
        .unwrap();
        let (release, wait) = tokio::sync::oneshot::channel();
        drop(
            db.inner
                .commit_completions
                .run(async move { wait.await.unwrap() })
                .unwrap(),
        );
        let mut close = Box::pin(db.close());
        assert!(futures::poll!(&mut close).is_pending());
        drop(close);
        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), db.close())
            .await
            .expect("dropping the close waiter must not abandon shutdown")
            .unwrap();
    }
}
