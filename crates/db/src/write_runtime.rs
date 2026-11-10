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
use tokio::sync::oneshot;

use crate::error::{HelixDbError, Result};

/// A current-thread runtime, driven by a thread of its own, that runs one
/// writer's SlateDB batch writer.
///
/// SlateDB needs the runtime driven for every write, flush, close and
/// checkpoint, so it must outlive the database's close. Dropping it stops the
/// thread and joins it; a database still open then can no longer write.
#[derive(Debug)]
pub(crate) struct WriteRuntime {
    handle: Handle,
    /// Dropped to stop the thread.
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
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
        let (shutdown, stopped) = oneshot::channel::<()>();
        let thread = std::thread::Builder::new()
            .name("helix-slatedb-writer".to_string())
            .spawn(move || {
                // Ends once the sender drops; tasks left on the runtime drop
                // with it.
                runtime.block_on(async {
                    let _ = stopped.await;
                });
            })
            .map_err(|error| {
                HelixDbError::Config(format!(
                    "failed to start the SlateDB write runtime thread: {error}"
                ))
            })?;
        Ok(Self {
            handle,
            shutdown: Some(shutdown),
            thread: Some(thread),
        })
    }

    /// The runtime to hand SlateDB's builder.
    #[must_use]
    pub(crate) fn handle(&self) -> Handle {
        self.handle.clone()
    }
}

impl Drop for WriteRuntime {
    fn drop(&mut self) {
        drop(self.shutdown.take());
        let Some(thread) = self.thread.take() else {
            return;
        };
        if thread.join().is_err() {
            tracing::error!("the SlateDB write runtime thread panicked");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn tasks_run_on_the_runtime_thread_until_it_drops() {
        let runtime = WriteRuntime::start().expect("the runtime starts");
        let thread = runtime
            .handle()
            .spawn(async { std::thread::current().name().map(str::to_string) })
            .await
            .expect("the task runs");
        assert_eq!(thread.as_deref(), Some("helix-slatedb-writer"));
        let timer = runtime
            .handle()
            .spawn(async { tokio::time::sleep(std::time::Duration::from_millis(1)).await });
        timer.await.expect("timers are enabled");
        let handle = runtime.handle();
        drop(runtime);
        let stopped = handle.spawn(std::future::ready(()));
        assert!(
            stopped.await.is_err(),
            "a dropped runtime no longer runs tasks"
        );
    }
}
