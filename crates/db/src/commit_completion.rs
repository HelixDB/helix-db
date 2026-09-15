//! Finite commit completion tasks survive request cancellation and drain at close.
//! Tracking owns no task handles or database references, so task/runtime ownership
//! remains acyclic. Sealing rejects new commits before any task is spawned.
use crate::error::{HelixDbError, Result};
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
    /// Spawn one admitted, finite completion. Dropping the returned handle does
    /// not abort the task. Its token is released on success, error or panic.
    pub(crate) fn spawn<F>(&self, future: F) -> Result<tokio::task::JoinHandle<F::Output>>
    where
        F: std::future::Future + Send + 'static,
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
        let completion = Completion {
            state: self.state.clone(),
        };
        Ok(tokio::spawn(async move {
            let result = future.await;
            drop(completion);
            result
        }))
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
            .spawn(async move {
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
            tracker.spawn(async move {
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
    async fn failure_panic_and_task_abort_release_their_ownership() {
        let tracker = Tracker::default();
        let error = tracker
            .spawn(async { Err::<(), _>("storage failure") })
            .unwrap()
            .await
            .unwrap();
        assert_eq!(error, Err("storage failure"));
        let panic = tracker
            .spawn(async {
                panic!("completion panic");
            })
            .unwrap()
            .await
            .unwrap_err();
        assert!(panic.is_panic());
        let task = tracker.spawn(std::future::pending::<()>()).unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
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
                match tracker.spawn(async move {
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
            .spawn(async move {
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
            db.inner.commit_completions.spawn(async {}),
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
                .spawn(async move { wait.await.unwrap() })
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
