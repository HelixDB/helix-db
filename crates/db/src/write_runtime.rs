//! The thread that applies a writer's commits.
//!
//! SlateDB applies every commit on one batch-writer task and wakes the
//! committing request from it. On the request runtime, tokio runs a woken task
//! next on the waking worker (its LIFO slot), so the request keeps the batch
//! writer's thread busy planning its next write while other commits queue
//! behind it. Bulk inserts lost 15% once planning stopped yielding between
//! requests. A runtime of its own keeps the batch writer applying commits.

use std::thread::JoinHandle;

use tokio::runtime::Handle;
use tokio::sync::{oneshot, watch};

use crate::error::{HelixDbError, Result};

/// A current-thread runtime, driven by a thread of its own, that runs one
/// writer's SlateDB batch writer.
///
/// SlateDB needs the runtime driven for every write, flush, close and
/// checkpoint, and marks a database closed before its final flush, so the
/// thread runs until the database itself is gone: until its status channel
/// ends, which happens once every handle to it, the batch writer's included,
/// has dropped. A database dropped without closing therefore keeps writing
/// for whoever still holds it, as it would on the request runtime.
pub(crate) struct WriteRuntime {
    handle: Handle,
    /// Hands the thread what it runs until: the database's status channel
    /// ending. Dropped unsent, as when the database fails to build, it stops
    /// the thread.
    open: oneshot::Sender<std::pin::Pin<Box<dyn Future<Output = ()> + Send>>>,
    thread: JoinHandle<()>,
}

impl WriteRuntime {
    /// Starts the runtime and its thread. Only timers are enabled: SlateDB's
    /// batch writer needs them, and an IO driver would cost a syscall per
    /// write.
    ///
    /// # Errors
    ///
    /// Fails when the runtime or its thread cannot be created.
    pub(crate) fn start() -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .map_err(|error| {
                HelixDbError::Config(format!(
                    "failed to build the SlateDB write runtime: {error}"
                ))
            })?;
        let handle = runtime.handle().clone();
        let (open, opened) = oneshot::channel();
        let thread = std::thread::Builder::new()
            .name("helix-slatedb-writer".to_string())
            .spawn(move || {
                // Tasks left on the runtime drop with it.
                runtime.block_on(async {
                    let Ok(closed) = opened.await else {
                        return;
                    };
                    closed.await;
                });
            })
            .map_err(|error| {
                HelixDbError::Config(format!(
                    "failed to start the SlateDB write runtime thread: {error}"
                ))
            })?;
        Ok(Self {
            handle,
            open,
            thread,
        })
    }

    /// The runtime to hand SlateDB's builder.
    pub(crate) fn handle(&self) -> Handle {
        self.handle.clone()
    }

    /// Keeps the thread running until `status`, the built database's status
    /// channel, ends, and returns the thread, which callers may detach.
    pub(crate) fn run_while_open<T: Send + Sync + 'static>(
        self,
        mut status: watch::Receiver<T>,
    ) -> JoinHandle<()> {
        // A send fails only if the thread already exited, which only a panic
        // could cause; it then reports itself through the join handle.
        let _ = self.open.send(Box::pin(
            async move { while status.changed().await.is_ok() {} },
        ));
        self.thread
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn the_thread_runs_tasks_until_its_status_channel_ends() {
        let runtime = WriteRuntime::start().expect("the runtime starts");
        let handle = runtime.handle();
        let (status, watched) = watch::channel(());
        let thread = runtime.run_while_open(watched);
        let name = handle
            .spawn(async { std::thread::current().name().map(str::to_string) })
            .await
            .expect("the task runs");
        assert_eq!(name.as_deref(), Some("helix-slatedb-writer"));
        handle
            .spawn(tokio::time::sleep(Duration::from_millis(1)))
            .await
            .expect("timers are enabled");
        status.send(()).expect("the thread still watches");
        assert!(
            handle.spawn(std::future::ready(())).await.is_ok(),
            "a status change keeps the thread running"
        );
        drop(status);
        tokio::task::spawn_blocking(move || thread.join())
            .await
            .expect("the join runs")
            .expect("the thread exits cleanly");
        assert!(
            handle.spawn(std::future::ready(())).await.is_err(),
            "the runtime stops with its thread"
        );
    }

    #[tokio::test]
    async fn a_runtime_never_given_a_database_stops() {
        let runtime = WriteRuntime::start().expect("the runtime starts");
        let handle = runtime.handle();
        let WriteRuntime { open, thread, .. } = runtime;
        drop(open);
        tokio::task::spawn_blocking(move || thread.join())
            .await
            .expect("the join runs")
            .expect("the thread exits cleanly");
        assert!(handle.spawn(std::future::ready(())).await.is_err());
    }
}
