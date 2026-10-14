//! Supervised background migration loop.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::config::{MigrationTuning, MigrationWorkerMode};
use crate::encoding::v2::keys::scope::DataScope;
use crate::HelixWriter;

const MAX_FAILURE_BACKOFF: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy)]
struct FailureBackoff {
    initial: Duration,
    next: Duration,
}

impl FailureBackoff {
    fn new(initial: Duration) -> Self {
        let initial = initial.min(MAX_FAILURE_BACKOFF);
        Self {
            initial,
            next: initial,
        }
    }

    fn next_delay(&mut self) -> Duration {
        let delay = self.next;
        self.next = self.next.saturating_mul(2).min(MAX_FAILURE_BACKOFF);
        delay
    }

    fn reset(&mut self) {
        self.next = self.initial;
    }
}

/// Supervisor retained by the database handle and joined before storage closes.
pub(crate) struct MigrationWorkerSupervisor {
    shutdown: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

impl MigrationWorkerSupervisor {
    /// Starts one immediately active worker when background scheduling is enabled.
    pub(crate) fn start_if_enabled(
        writer: Arc<HelixWriter>,
        tuning: MigrationTuning,
    ) -> Option<Self> {
        if tuning.worker_mode() == MigrationWorkerMode::Disabled {
            return None;
        }
        let (shutdown, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(run(writer, tuning, shutdown_rx));
        Some(Self { shutdown, handle })
    }

    /// Requests shutdown and joins the worker before its storage dependency closes.
    pub(crate) async fn stop(self) {
        let _ = self.shutdown.send(true);
        match self.handle.await {
            Ok(()) => {}
            Err(error) if error.is_cancelled() => {}
            Err(error) => {
                tracing::warn!(%error, "background migration worker failed during shutdown");
            }
        }
    }
}

async fn run(
    writer: Arc<HelixWriter>,
    tuning: MigrationTuning,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut failure_backoff =
        FailureBackoff::new(Duration::from_millis(tuning.idle_interval_millis().get()));
    loop {
        if *shutdown.borrow() {
            return;
        }

        let delay =
            match super::super::process_migration_once(&writer, DataScope::LegacyUnscoped, tuning)
                .await
            {
                Ok(true) => {
                    failure_backoff.reset();
                    Duration::from_millis(tuning.active_interval_millis().get())
                }
                Ok(false) => return,
                Err(error) => {
                    let delay = failure_backoff.next_delay();
                    tracing::warn!(
                        %error,
                        retry_delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                        "background migration batch failed; retrying with capped backoff"
                    );
                    delay
                }
            };

        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use slatedb::object_store::memory::InMemory;
    use slatedb::Db;

    use super::*;
    use crate::config::{MigrationActiveIntervalMillis, MigrationIdleIntervalMillis};
    use crate::migrations::{
        decode_json, ensure_migration_job, migration_completed, MigrationId, MigrationJob,
        MigrationJobKey, MigrationJobState, MigrationMode, MigrationStage,
    };

    async fn test_writer(name: &str) -> (Arc<Db>, Arc<HelixWriter>) {
        let db = Arc::new(
            Db::builder(name, Arc::new(InMemory::new()))
                .build()
                .await
                .expect("migration worker test database opens"),
        );
        let writer = Arc::new(HelixWriter::new(Arc::clone(&db), 64, 32));
        (db, writer)
    }

    #[tokio::test]
    async fn disabled_tuning_does_not_start_a_worker() {
        let (db, writer) = test_writer("disabled-background-migration-worker").await;
        let tuning = MigrationTuning::default().with_worker_mode(MigrationWorkerMode::Disabled);

        assert!(MigrationWorkerSupervisor::start_if_enabled(writer, tuning).is_none());

        db.close().await.expect("test database closes");
    }

    #[test]
    fn failure_backoff_doubles_resets_and_caps() {
        let mut backoff = FailureBackoff::new(Duration::from_secs(1));
        assert_eq!(backoff.next_delay(), Duration::from_secs(1));
        assert_eq!(backoff.next_delay(), Duration::from_secs(2));
        assert_eq!(backoff.next_delay(), Duration::from_secs(4));
        assert_eq!(backoff.next_delay(), Duration::from_secs(8));
        assert_eq!(backoff.next_delay(), Duration::from_secs(16));
        assert_eq!(backoff.next_delay(), Duration::from_secs(32));
        assert_eq!(backoff.next_delay(), MAX_FAILURE_BACKOFF);
        assert_eq!(backoff.next_delay(), MAX_FAILURE_BACKOFF);

        backoff.reset();
        assert_eq!(backoff.next_delay(), Duration::from_secs(1));

        let mut capped = FailureBackoff::new(Duration::from_secs(120));
        assert_eq!(capped.next_delay(), MAX_FAILURE_BACKOFF);
    }

    #[tokio::test]
    async fn worker_immediately_completes_runnable_cleanup_and_joins() {
        let (db, writer) = test_writer("active-background-migration-worker").await;
        ensure_migration_job(
            &db,
            DataScope::LegacyUnscoped,
            MigrationId::GraphFormatV1Cleanup,
            MigrationMode::Background,
        )
        .await
        .expect("background cleanup is enqueued");
        let tuning = MigrationTuning::default()
            .with_active_interval(
                MigrationActiveIntervalMillis::new(1).expect("active interval is positive"),
            )
            .with_idle_interval(
                MigrationIdleIntervalMillis::new(1).expect("idle interval is positive"),
            );
        let worker = MigrationWorkerSupervisor::start_if_enabled(writer, tuning)
            .expect("background tuning starts a worker");

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if migration_completed(
                    db.as_ref(),
                    DataScope::LegacyUnscoped,
                    MigrationId::GraphFormatV1Cleanup,
                )
                .await
                .expect("cleanup status loads")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("worker completes the empty cleanup promptly");

        tokio::time::timeout(Duration::from_secs(2), async {
            while !worker.handle.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("worker exits after all known background work completes");

        worker.stop().await;
        db.close().await.expect("test database closes");
    }

    #[tokio::test]
    async fn shutdown_interrupts_the_active_interval_and_joins() {
        let (db, writer) = test_writer("sleeping-background-migration-worker").await;
        ensure_migration_job(
            &db,
            DataScope::LegacyUnscoped,
            MigrationId::GraphFormatV1Cleanup,
            MigrationMode::Background,
        )
        .await
        .expect("background cleanup is enqueued");
        let tuning = MigrationTuning::default().with_active_interval(
            MigrationActiveIntervalMillis::new(60_000).expect("active interval is positive"),
        );
        let worker = MigrationWorkerSupervisor::start_if_enabled(writer, tuning)
            .expect("background tuning starts a worker");

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if migration_completed(
                    db.as_ref(),
                    DataScope::LegacyUnscoped,
                    MigrationId::GraphFormatV1Cleanup,
                )
                .await
                .expect("cleanup status loads")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("worker completes the empty cleanup promptly");

        tokio::time::timeout(Duration::from_secs(2), worker.stop())
            .await
            .expect("shutdown interrupts the active interval and joins");
        db.close().await.expect("test database closes");
    }

    #[tokio::test]
    async fn failed_checkpoint_is_durable_and_retried_after_idle() {
        let (db, writer) = test_writer("retrying-background-migration-worker").await;
        ensure_migration_job(
            &db,
            DataScope::LegacyUnscoped,
            MigrationId::GraphFormatV1Cleanup,
            MigrationMode::Background,
        )
        .await
        .expect("background cleanup is enqueued");
        let malformed_key = MigrationStage::LegacyEdgePairs.prefix(DataScope::LegacyUnscoped);
        db.put(&malformed_key, bytes::Bytes::from_static(b"malformed"))
            .await
            .expect("malformed source row is stored");
        let tuning = MigrationTuning::default()
            .with_active_interval(
                MigrationActiveIntervalMillis::new(1).expect("active interval is positive"),
            )
            .with_idle_interval(
                MigrationIdleIntervalMillis::new(100).expect("idle interval is positive"),
            );
        let worker = MigrationWorkerSupervisor::start_if_enabled(writer, tuning)
            .expect("background tuning starts a worker");
        let job_key =
            MigrationJobKey::new(DataScope::LegacyUnscoped, MigrationId::GraphFormatV1Cleanup);

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let stored = db
                    .get(job_key.as_ref())
                    .await
                    .expect("cleanup status loads")
                    .expect("cleanup job exists");
                let job = decode_json::<MigrationJob>(&stored).expect("cleanup job decodes");
                if matches!(job.state, MigrationJobState::Failed { .. }) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("failed checkpoint becomes durable before the retry interval");

        db.delete(&malformed_key)
            .await
            .expect("malformed source row is removed");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if migration_completed(
                    db.as_ref(),
                    DataScope::LegacyUnscoped,
                    MigrationId::GraphFormatV1Cleanup,
                )
                .await
                .expect("cleanup status loads")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("worker retries the failed checkpoint after the idle interval");

        worker.stop().await;
        db.close().await.expect("test database closes");
    }
}
