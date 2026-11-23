//! Supervised background migration loop.

use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::config::{MigrationTuning, MigrationWorkerMode};
use crate::encoding::v2::keys::scope::DataScope;
use crate::migrations::indexes::label_identity::{self, LabelCopySession};
use crate::HelixWriter;

const MAX_FAILURE_BACKOFF: Duration = Duration::from_secs(60);

/// When a step may next take one background migration.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum Retry {
    /// It has not failed since its last success: every step takes it.
    #[default]
    Ready,
    /// Its last attempt failed: no step takes it before `at`, which is `delay`
    /// after the step that made that attempt ended. Another failure waits twice as long, up to
    /// [`MAX_FAILURE_BACKOFF`]; a success makes it [`Retry::Ready`] again.
    At { at: Instant, delay: Duration },
}

/// Each background migration's own failure backoff, kept by the worker
/// between steps, so a migration that keeps failing slows only itself.
///
/// The flush backlog check that admits the paced migrations backs off on its
/// own too: while it does, no paced migration runs.
#[derive(Debug, Default)]
pub(crate) struct MigrationRetries {
    job: Retry,
    pacing: Retry,
    label_copy: Retry,
    label_cleanup: Retry,
    datetimes: Retry,
}

/// What one background step leaves the worker to do.
#[derive(Debug)]
#[cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "the step logs every failure; only stepping tests read a failure back"
    )
)]
pub(crate) enum BackgroundStep {
    /// A migration advanced, or was held back by the flush backlog: the worker
    /// steps again after its active interval. `failure` is the first failure
    /// of another migration in this step.
    Active {
        failure: Option<crate::error::HelixDbError>,
    },
    /// No migration advanced and at least one is backing off: the worker steps
    /// again at `at`, the earliest retry. `failure` is this step's first
    /// failure, or `None` if every unfinished migration was waiting.
    RetryAt {
        at: Instant,
        failure: Option<crate::error::HelixDbError>,
    },
    /// Every background migration has finished.
    Finished,
}

/// The background migrations one writer runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BackgroundMigrations {
    /// Every background migration: a writer authorized to migrate storage.
    All,
    /// Only the label identity copy and the deletion of the hash label rows
    /// it retires. A migrating writer started the copy, after which every
    /// writer files label rows under both keys, so a writer that runs no
    /// migration, such as a failover writer, still finishes both.
    LabelIdentity,
}

/// Supervisor retained by the database handle and joined before storage closes.
pub(crate) struct MigrationWorkerSupervisor {
    shutdown: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

impl MigrationWorkerSupervisor {
    /// Starts one immediately active worker when background scheduling is enabled.
    ///
    /// Jobs that write through the query path reach the database through
    /// `db`, upgraded once per step, so the worker never keeps it alive.
    pub(crate) fn start_if_enabled(
        writer: Arc<HelixWriter>,
        db: Weak<crate::HelixDBInner>,
        tuning: MigrationTuning,
        migrations: BackgroundMigrations,
    ) -> Option<Self> {
        if tuning.worker_mode() == MigrationWorkerMode::Disabled {
            return None;
        }
        let (shutdown, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(run(writer, db, tuning, migrations, shutdown_rx));
        Some(Self { shutdown, handle })
    }

    /// Joins a worker that stops on its own once every job has finished.
    #[cfg(test)]
    pub(crate) async fn join(self) {
        self.handle
            .await
            .expect("background migration worker does not panic");
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

/// Advances every pending background migration `migrations` names by one
/// bounded batch: the next runnable migration job, the label identity
/// copy, the deletion of the hash label rows it retires, and, once no migration
/// job has more to do, the datetime string conversion. `labels` carries the
/// copy's state and the step pacing between steps, `retries` each migration's
/// failure backoff.
///
/// The step reads `clock` twice: before it takes any migration, to tell which
/// are due, and after all have run, to time each failure's retry from then. A
/// retry delay so starts when the failed step ends, not when it started, so a
/// slow batch beside a failure does not eat into that failure's backoff.
///
/// A migration that failed is skipped until its retry time, and backs off
/// alone: a failing label step slows neither the migration jobs nor the
/// datetime conversion, nor they it. The step logs each failure with the
/// migration's name and retry delay, and a success clears that migration's
/// backoff. The one ordering is that the datetime conversion runs only once
/// the migration jobs report nothing more to do: jobs that fail or wait on
/// their backoff count as not done, so the conversion never rewrites rows an
/// unfinished job still moves.
///
/// The step reports [`BackgroundStep::Finished`] only when every migration has
/// finished; one backing off has not, and makes the step report its retry
/// time unless another advanced.
///
/// The cleanup follows the copy, so the step that publishes the copy starts
/// it. The label steps and the datetime conversion run only while the
/// writer's flush backlog is low (see [`LabelCopySession::admit_step`]), so
/// they never push foreground writes into storage backpressure; a step held
/// back reports more to do. The datetime conversion writes through the query
/// path, which `db` reaches, upgraded once per step; it runs only for
/// [`BackgroundMigrations::All`], and is done once `db` has been dropped.
pub(crate) async fn process_background_step(
    writer: &HelixWriter,
    db: &Weak<crate::HelixDBInner>,
    tuning: MigrationTuning,
    migrations: BackgroundMigrations,
    labels: &mut LabelCopySession,
    retries: &mut MigrationRetries,
    clock: impl Fn() -> Instant,
) -> BackgroundStep {
    let now = clock();
    let due = |retry: Retry| !matches!(retry, Retry::At { at, .. } if now < at);
    // `None` is a migration this step did not take: backing off, or the
    // conversion waiting on the jobs.
    let job = match migrations {
        BackgroundMigrations::All if due(retries.job) => Some(
            super::super::process_migration_once(writer, DataScope::LegacyUnscoped, tuning).await,
        ),
        BackgroundMigrations::All => None,
        BackgroundMigrations::LabelIdentity => Some(Ok(false)),
    };
    let copy_due = due(retries.label_copy);
    let cleanup_due = due(retries.label_cleanup);
    let datetimes_due = migrations == BackgroundMigrations::All
        && matches!(job, Some(Ok(false)))
        && due(retries.datetimes);
    let pacing = if (copy_due || cleanup_due || datetimes_due) && due(retries.pacing) {
        Some(labels.admit_step(writer.db()).await)
    } else {
        None
    };
    let admitted = matches!(pacing, Some(Ok(true)));
    let copy = if admitted && copy_due {
        Some(
            async {
                let step = label_identity::copy_hash_label_rows_once(writer, labels).await?;
                match step {
                    // The open left the copying store below the version that
                    // admits dropped-generation entries; a crash before this
                    // commit leaves it to the next open.
                    label_identity::LabelCopyStep::Published => {
                        crate::index_lifecycle::dropped::admit(
                            writer.db(),
                            crate::index_lifecycle::dropped::Admission::LabelCopyPublished,
                        )
                        .await?;
                    }
                    label_identity::LabelCopyStep::NotStarted
                    | label_identity::LabelCopyStep::Copied
                    | label_identity::LabelCopyStep::Conflicted
                    | label_identity::LabelCopyStep::AlreadyPublished => {}
                }
                Ok(step.advanced())
            }
            .await,
        )
    } else {
        None
    };
    let cleanup = if admitted && cleanup_due {
        Some(
            label_identity::delete_hash_label_rows_once(writer.db(), labels)
                .await
                .map(label_identity::LabelHashCleanup::advanced),
        )
    } else {
        None
    };
    // `db` is upgraded only for a conversion step, so the worker holds the
    // database for no longer than one.
    let datetimes = if admitted && datetimes_due {
        Some(match db.upgrade() {
            Some(inner) => {
                super::super::datetime_strings::process_once(&crate::HelixDB { inner }, tuning)
                    .await
            }
            None => Ok(false),
        })
    } else {
        None
    };
    let initial = Duration::from_millis(tuning.idle_interval_millis().get());
    let ended = clock();
    // Every outcome is folded, not just those before the first failure, so
    // each failure is logged and backs its own migration off. A backlog held
    // high has more to do: the paced migrations it held back.
    let (more, failure) = [
        ("migration job", &mut retries.job, job),
        (
            "step pacing",
            &mut retries.pacing,
            pacing.map(|admitted| admitted.map(|admitted| !admitted)),
        ),
        ("label identity copy", &mut retries.label_copy, copy),
        ("label hash cleanup", &mut retries.label_cleanup, cleanup),
        (
            "datetime string conversion",
            &mut retries.datetimes,
            datetimes,
        ),
    ]
    .into_iter()
    .fold(
        (false, None),
        |(more, failure), (migration, retry, outcome)| match outcome {
            None => (more, failure),
            Some(Ok(advanced)) => {
                *retry = Retry::Ready;
                (more | advanced, failure)
            }
            Some(Err(error)) => {
                // The first failure since a success waits `initial`, each further
                // one twice the last, none more than the cap.
                let delay = match *retry {
                    Retry::Ready => initial,
                    Retry::At { delay, .. } => delay.saturating_mul(2),
                }
                .min(MAX_FAILURE_BACKOFF);
                *retry = Retry::At {
                    at: ended + delay,
                    delay,
                };
                tracing::warn!(
                    migration,
                    %error,
                    retry_delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                    "background migration step failed; retrying it with capped backoff"
                );
                (more, failure.or(Some(error)))
            }
        },
    );
    let retry_at = [
        retries.job,
        retries.pacing,
        retries.label_copy,
        retries.label_cleanup,
        retries.datetimes,
    ]
    .into_iter()
    .filter_map(|retry| match retry {
        // Against the step's start: a migration it skipped as not yet due
        // still waits, even if its retry time passed while the step ran; the
        // worker then wakes at once and takes it.
        Retry::At { at, .. } if now < at => Some(at),
        // A retry time already passed is the conversion's, due but waiting on
        // the jobs, or a paced migration's, due but waiting on the pacing's
        // backoff: the worker waits for what it waits on, not for that.
        Retry::At { .. } | Retry::Ready => None,
    })
    .min();
    // A migration this step did not take is backing off, or is due but
    // waiting on the jobs, which either advanced or are backing off, or on
    // the pacing, which is backing off. So a step that saw no advance and no
    // pending retry finished every migration.
    match (more, retry_at) {
        (true, _) => BackgroundStep::Active { failure },
        (false, Some(at)) => BackgroundStep::RetryAt { at, failure },
        (false, None) => BackgroundStep::Finished,
    }
}

async fn run(
    writer: Arc<HelixWriter>,
    db: Weak<crate::HelixDBInner>,
    tuning: MigrationTuning,
    migrations: BackgroundMigrations,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut labels = LabelCopySession::new();
    let mut retries = MigrationRetries::default();
    loop {
        if *shutdown.borrow() {
            return;
        }

        // The step logged every failure; each backs off on its own.
        let wake = match process_background_step(
            &writer,
            &db,
            tuning,
            migrations,
            &mut labels,
            &mut retries,
            Instant::now,
        )
        .await
        {
            BackgroundStep::Active { .. } => {
                Instant::now() + Duration::from_millis(tuning.active_interval_millis().get())
            }
            BackgroundStep::RetryAt { at, .. } => at,
            BackgroundStep::Finished => return,
        };

        tokio::select! {
            _ = tokio::time::sleep_until(wake) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "label_and_datetime_tests.rs"]
mod label_and_datetime_tests;

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
        let writer = Arc::new(HelixWriter::new(Arc::clone(&db), 64, 32, Arc::default()));
        (db, writer)
    }

    #[tokio::test]
    async fn disabled_tuning_does_not_start_a_worker() {
        let (db, writer) = test_writer("disabled-background-migration-worker").await;
        let tuning = MigrationTuning::default().with_worker_mode(MigrationWorkerMode::Disabled);

        assert!(MigrationWorkerSupervisor::start_if_enabled(
            writer,
            Weak::new(),
            tuning,
            BackgroundMigrations::All
        )
        .is_none());

        db.close().await.expect("test database closes");
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
        let worker = MigrationWorkerSupervisor::start_if_enabled(
            writer,
            Weak::new(),
            tuning,
            BackgroundMigrations::All,
        )
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
        let worker = MigrationWorkerSupervisor::start_if_enabled(
            writer,
            Weak::new(),
            tuning,
            BackgroundMigrations::All,
        )
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
        let worker = MigrationWorkerSupervisor::start_if_enabled(
            writer,
            Weak::new(),
            tuning,
            BackgroundMigrations::All,
        )
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
