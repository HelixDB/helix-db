//! Parent-owned worker runtime for fair global index lifecycle dispatch.
//!
//! The ownership graph is deliberately acyclic:
//!
//! ```text
//! HelixDBInner -> IndexWorkerSupervisor -> JoinHandle
//!                                      -> shutdown/wake channels
//! spawned task -> Arc<slatedb::Db> + capability registry
//! ```
//!
//! The task never owns `HelixDBInner`. [`IndexWorkerSupervisor::stop`] cancels
//! and joins it before SlateDB closes. Repository and driver methods continue
//! to borrow `&Db`; only this spawned runtime retains the `Arc<Db>` needed for
//! its `'static` lifetime.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{watch, Notify};
use tokio::task::JoinHandle;

use crate::config::SearchIndexBatchLimits;
use crate::error::{HelixDbError, Result};

use super::outbox::{
    self, ClaimPermission, IndexOperationDriver, OperationPointerObservation,
    OperationQueuePageSize, SameEpochRecoveryProof,
};
use super::text::active_mutation::ActiveTextMutationRegistry;
use super::text::blob_gc::{self, BlobGcDriver, BlobGcRootStep};
#[cfg(test)]
use super::text::upload_queue::PreparedTextUploadStep;
use super::text::upload_queue::{self, TextUploadDriver, UploadPointerObservation};
use super::{ClaimSequence, IndexOperationFamily, WriterEpoch};

const DEFAULT_OPERATION_PAGE_SIZE: usize = 64;
const SUPERVISOR_RESTART_DELAY: Duration = Duration::from_millis(10);
const IDLE_DELAY: Duration = Duration::from_secs(24 * 60 * 60);

/// Installed runtime service for one physical index family.
#[derive(Clone)]
pub(crate) enum IndexFamilyCapability {
    /// No driver is installed, so persisted data cannot authorize physical work.
    #[cfg(test)]
    Unavailable,
    /// The physical driver is complete enough for internal lifecycle tests,
    /// but mutation/serving/coordinator contracts are not all installed.
    DriverReady {
        /// Physical driver installed by the family service.
        driver: Arc<dyn IndexOperationDriver>,
        /// Existing validated source/transaction limits passed to every step.
        limits: SearchIndexBatchLimits,
        /// Whether the parent-owned supervisor may claim this family's work.
        scheduling: IndexDriverScheduling,
    },
    /// Every DDL, mutation, serving, cleanup, and coordinator dependency is
    /// installed for this family.
    FullyReady {
        /// Physical driver installed by the family service.
        driver: Arc<dyn IndexOperationDriver>,
        /// Existing validated source/transaction limits passed to every step.
        limits: SearchIndexBatchLimits,
        /// Whether the parent-owned supervisor may claim this family's work.
        scheduling: IndexDriverScheduling,
    },
}

/// Complete runtime service set required before text work can be scheduled.
///
/// Keeping the operation, upload, and GC-root drivers in one variant prevents
/// a text family from being marked ready while one of its durable lanes is
/// silently unavailable.
#[derive(Clone)]
pub(crate) enum TextIndexCapability {
    /// No complete text lifecycle service is installed.
    Unavailable,
    /// Active mutations plus durable upload and GC-root recovery are installed,
    /// while build/drop work and public DDL remain unavailable.
    #[cfg(test)]
    MutationReady {
        /// Upload publication/reconciliation driver shared with Active requests.
        upload_driver: Arc<dyn TextUploadDriver>,
        /// Independently discoverable blob-GC root recovery driver.
        gc_driver: Arc<dyn BlobGcDriver>,
    },
    /// Internal text lifecycle dependencies are installed but public use is gated.
    DriverReady {
        /// Physical build/drop driver.
        driver: Arc<dyn outbox::IndexOperationDriver>,
        /// Upload publication/reconciliation driver.
        upload_driver: Arc<dyn TextUploadDriver>,
        /// Independently discoverable blob-GC root recovery driver.
        gc_driver: Arc<dyn BlobGcDriver>,
        /// Validated bounded family step limits.
        limits: SearchIndexBatchLimits,
        /// Whether the global worker may schedule every text lifecycle lane.
        scheduling: IndexDriverScheduling,
    },
    /// Text DDL, mutation, serving, cleanup, and coordinator contracts are complete.
    FullyReady {
        /// Physical build/drop driver.
        driver: Arc<dyn outbox::IndexOperationDriver>,
        /// Upload publication/reconciliation driver.
        upload_driver: Arc<dyn TextUploadDriver>,
        /// Independently discoverable blob-GC root recovery driver.
        gc_driver: Arc<dyn BlobGcDriver>,
        /// Validated bounded family step limits.
        limits: SearchIndexBatchLimits,
        /// Whether the global worker may schedule every text lifecycle lane.
        scheduling: IndexDriverScheduling,
    },
}

/// Runtime-only scheduling authority for an installed family driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IndexDriverScheduling {
    /// The global worker may discover, claim, and advance this family's work.
    Automatic,
    /// The driver remains installed for an explicit one-step caller only.
    ExplicitOnly,
}

/// Runtime-only family capability registry; persisted bytes never select it.
#[derive(Clone)]
pub(crate) struct IndexFamilyCapabilities {
    secondary: IndexFamilyCapability,
    vector: IndexFamilyCapability,
    text: TextIndexCapability,
}

impl IndexFamilyCapabilities {
    /// Fail-closed registry used until family services are explicitly installed.
    #[cfg(test)]
    pub(crate) const fn unavailable() -> Self {
        Self {
            secondary: IndexFamilyCapability::Unavailable,
            vector: IndexFamilyCapability::Unavailable,
            text: TextIndexCapability::Unavailable,
        }
    }

    /// Whether public DDL may enqueue work for this family.
    pub(crate) const fn public_ddl_ready(&self, family: IndexOperationFamily) -> bool {
        self.fully_ready(family)
    }

    /// Whether public serving may open physical rows for this family.
    #[cfg(test)]
    pub(crate) const fn public_serving_ready(&self, family: IndexOperationFamily) -> bool {
        self.fully_ready(family)
    }

    /// Whether the complete internal lifecycle driver is installed for a family.
    ///
    /// `DriverReady` deliberately remains distinct from public readiness:
    /// callers must install the reader and family-specific coordination
    /// dependencies before promoting a family to `FullyReady`.
    #[cfg(test)]
    pub(crate) const fn lifecycle_driver_ready(&self, family: IndexOperationFamily) -> bool {
        match family {
            IndexOperationFamily::Secondary => matches!(
                &self.secondary,
                IndexFamilyCapability::DriverReady { .. }
                    | IndexFamilyCapability::FullyReady { .. }
            ),
            IndexOperationFamily::Vector => matches!(
                &self.vector,
                IndexFamilyCapability::DriverReady { .. }
                    | IndexFamilyCapability::FullyReady { .. }
            ),
            IndexOperationFamily::Text => matches!(
                &self.text,
                TextIndexCapability::DriverReady { .. } | TextIndexCapability::FullyReady { .. }
            ),
        }
    }

    /// Returns whether every family dependency was installed atomically.
    const fn fully_ready(&self, family: IndexOperationFamily) -> bool {
        match family {
            IndexOperationFamily::Secondary => {
                matches!(&self.secondary, IndexFamilyCapability::FullyReady { .. })
            }
            IndexOperationFamily::Vector => {
                matches!(&self.vector, IndexFamilyCapability::FullyReady { .. })
            }
            IndexOperationFamily::Text => {
                matches!(&self.text, TextIndexCapability::FullyReady { .. })
            }
        }
    }

    /// Builds a registry from separately derived runtime capabilities.
    pub(crate) const fn new(
        secondary: IndexFamilyCapability,
        vector: IndexFamilyCapability,
        text: TextIndexCapability,
    ) -> Self {
        Self {
            secondary,
            vector,
            text,
        }
    }

    fn driver(
        &self,
        family: IndexOperationFamily,
    ) -> Option<(&Arc<dyn IndexOperationDriver>, SearchIndexBatchLimits)> {
        match family {
            IndexOperationFamily::Secondary => family_driver(&self.secondary),
            IndexOperationFamily::Vector => family_driver(&self.vector),
            IndexOperationFamily::Text => match &self.text {
                TextIndexCapability::DriverReady {
                    driver,
                    limits,
                    scheduling: IndexDriverScheduling::Automatic,
                    ..
                }
                | TextIndexCapability::FullyReady {
                    driver,
                    limits,
                    scheduling: IndexDriverScheduling::Automatic,
                    ..
                } => Some((driver, *limits)),
                TextIndexCapability::Unavailable
                | TextIndexCapability::DriverReady {
                    scheduling: IndexDriverScheduling::ExplicitOnly,
                    ..
                }
                | TextIndexCapability::FullyReady {
                    scheduling: IndexDriverScheduling::ExplicitOnly,
                    ..
                } => None,
                #[cfg(test)]
                TextIndexCapability::MutationReady { .. } => None,
            },
        }
    }

    /// Returns the upload driver only from a complete automatically scheduled text service.
    fn upload_driver(&self) -> Option<&Arc<dyn TextUploadDriver>> {
        match &self.text {
            TextIndexCapability::DriverReady {
                upload_driver,
                scheduling: IndexDriverScheduling::Automatic,
                ..
            }
            | TextIndexCapability::FullyReady {
                upload_driver,
                scheduling: IndexDriverScheduling::Automatic,
                ..
            } => Some(upload_driver),
            #[cfg(test)]
            TextIndexCapability::MutationReady { upload_driver, .. } => Some(upload_driver),
            TextIndexCapability::Unavailable
            | TextIndexCapability::DriverReady {
                scheduling: IndexDriverScheduling::ExplicitOnly,
                ..
            }
            | TextIndexCapability::FullyReady {
                scheduling: IndexDriverScheduling::ExplicitOnly,
                ..
            } => None,
        }
    }

    /// Returns the GC driver only from an automatically scheduled text service.
    fn gc_driver(&self) -> Option<&Arc<dyn BlobGcDriver>> {
        match &self.text {
            TextIndexCapability::DriverReady {
                gc_driver,
                scheduling: IndexDriverScheduling::Automatic,
                ..
            }
            | TextIndexCapability::FullyReady {
                gc_driver,
                scheduling: IndexDriverScheduling::Automatic,
                ..
            } => Some(gc_driver),
            #[cfg(test)]
            TextIndexCapability::MutationReady { gc_driver, .. } => Some(gc_driver),
            TextIndexCapability::Unavailable
            | TextIndexCapability::DriverReady {
                scheduling: IndexDriverScheduling::ExplicitOnly,
                ..
            }
            | TextIndexCapability::FullyReady {
                scheduling: IndexDriverScheduling::ExplicitOnly,
                ..
            } => None,
        }
    }
}

/// Selects one automatically scheduled non-text family driver.
fn family_driver(
    capability: &IndexFamilyCapability,
) -> Option<(
    &Arc<dyn outbox::IndexOperationDriver>,
    SearchIndexBatchLimits,
)> {
    match capability {
        IndexFamilyCapability::DriverReady {
            driver,
            limits,
            scheduling: IndexDriverScheduling::Automatic,
        }
        | IndexFamilyCapability::FullyReady {
            driver,
            limits,
            scheduling: IndexDriverScheduling::Automatic,
        } => Some((driver, *limits)),
        IndexFamilyCapability::DriverReady {
            scheduling: IndexDriverScheduling::ExplicitOnly,
            ..
        }
        | IndexFamilyCapability::FullyReady {
            scheduling: IndexDriverScheduling::ExplicitOnly,
            ..
        } => None,
        #[cfg(test)]
        IndexFamilyCapability::Unavailable => None,
    }
}

/// Round-robin lanes retain separate scheduling budgets as later text work is installed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkLane {
    Operation,
    Upload,
    BlobGcRoot,
}

impl WorkLane {
    const ALL: [Self; 3] = [Self::Operation, Self::Upload, Self::BlobGcRoot];
}

/// Supervisor retained by `HelixDBInner` and joined by the close protocol.
pub(crate) struct IndexWorkerSupervisor {
    writer_epoch: WriterEpoch,
    wake: Arc<Notify>,
    shutdown: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

impl IndexWorkerSupervisor {
    /// Starts one supervised global worker after writer fencing succeeds.
    pub(crate) fn start(
        db: Arc<slatedb::Db>,
        capabilities: IndexFamilyCapabilities,
        active_text_mutations: ActiveTextMutationRegistry,
    ) -> Self {
        let writer_epoch = WriterEpoch::new_v4();
        let wake = Arc::new(Notify::new());
        let (shutdown, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(supervise_worker(
            db,
            capabilities,
            active_text_mutations,
            writer_epoch,
            Arc::clone(&wake),
            shutdown_rx,
        ));
        Self {
            writer_epoch,
            wake,
            shutdown,
            handle,
        }
    }

    /// Writer epoch used to fence every claim emitted by this runtime.
    pub(crate) const fn writer_epoch(&self) -> WriterEpoch {
        self.writer_epoch
    }

    /// Wakes a sleeping full-cycle scan after a transaction enqueues work.
    pub(crate) fn wake(&self) {
        self.wake.notify_one();
    }

    /// Idempotently requests shutdown and joins before storage is closed.
    pub(crate) async fn stop(self) {
        let _ = self.shutdown.send(true);
        self.wake.notify_waiters();
        match self.handle.await {
            Ok(()) => {}
            Err(error) if error.is_cancelled() => {}
            Err(error) => {
                tracing::warn!(%error, "index outbox supervisor failed during shutdown");
            }
        }
    }
}

async fn supervise_worker(
    db: Arc<slatedb::Db>,
    capabilities: IndexFamilyCapabilities,
    active_text_mutations: ActiveTextMutationRegistry,
    writer_epoch: WriterEpoch,
    wake: Arc<Notify>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut same_epoch_proof = None;
    let mut next_claim_sequence = 1_u64;
    loop {
        match run_worker_cycle(
            WorkerCycleContext {
                db: db.as_ref(),
                capabilities: &capabilities,
                active_text_mutations: &active_text_mutations,
                writer_epoch,
                same_epoch_proof,
                wake: &wake,
            },
            &mut next_claim_sequence,
            &mut shutdown,
        )
        .await
        {
            Ok(WorkerCycleExit::Shutdown) => return,
            Err(error) => {
                tracing::warn!(
                    %error,
                    writer_epoch = %writer_epoch.as_uuid(),
                    "index outbox worker cycle failed; restarting after termination"
                );
                same_epoch_proof = Some(SameEpochRecoveryProof::after_join(writer_epoch));
                tokio::select! {
                    result = shutdown.changed() => {
                        if result.is_err() || *shutdown.borrow() {
                            return;
                        }
                    }
                    () = tokio::time::sleep(SUPERVISOR_RESTART_DELAY) => {}
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerCycleExit {
    Shutdown,
}

/// Immutable runtime dependencies shared by one supervised worker cycle.
struct WorkerCycleContext<'a> {
    db: &'a slatedb::Db,
    capabilities: &'a IndexFamilyCapabilities,
    active_text_mutations: &'a ActiveTextMutationRegistry,
    writer_epoch: WriterEpoch,
    same_epoch_proof: Option<SameEpochRecoveryProof>,
    wake: &'a Notify,
}

async fn run_worker_cycle(
    context: WorkerCycleContext<'_>,
    next_claim_sequence: &mut u64,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<WorkerCycleExit> {
    let WorkerCycleContext {
        db,
        capabilities,
        active_text_mutations,
        writer_epoch,
        same_epoch_proof,
        wake,
    } = context;
    let page_size = OperationQueuePageSize::new(DEFAULT_OPERATION_PAGE_SIZE)?;
    let upload_page_size = upload_queue::UploadQueuePageSize::new(DEFAULT_OPERATION_PAGE_SIZE)?;
    let root_page_size = NonZeroUsize::new(DEFAULT_OPERATION_PAGE_SIZE).ok_or(
        HelixDbError::InvariantViolation("blob-GC root page size must be positive".to_string()),
    )?;
    let mut operation_cursor = None;
    let mut upload_cursor = None;
    let mut root_cursor = None;
    let mut lane_index = 0_usize;
    let mut lanes_without_work = 0_usize;
    let mut earliest_delay = None::<u64>;

    loop {
        if *shutdown.borrow() {
            return Ok(WorkerCycleExit::Shutdown);
        }
        let lane = WorkLane::ALL[lane_index];
        lane_index = (lane_index + 1) % WorkLane::ALL.len();
        let outcome = match lane {
            WorkLane::Operation => {
                let page =
                    outbox::scan_operation_queue_page(db, operation_cursor, page_size).await?;
                operation_cursor = page.resume_after;
                let mut did_work = false;
                for operation_id in page.operation_ids {
                    let observation = outbox::observe_operation_pointer(
                        db,
                        operation_id,
                        writer_epoch,
                        now_unix_millis(),
                    )
                    .await?;
                    let (eligible, permission) = match observation {
                        OperationPointerObservation::Eligible(eligible) => {
                            (eligible, ClaimPermission::Normal)
                        }
                        OperationPointerObservation::ClaimedByCurrentWriter(eligible) => {
                            let Some(proof) = same_epoch_proof else {
                                continue;
                            };
                            (eligible, ClaimPermission::SameEpochRecovery(proof))
                        }
                        OperationPointerObservation::Delayed { delay_millis } => {
                            earliest_delay = Some(
                                earliest_delay
                                    .map_or(delay_millis, |current| current.min(delay_millis)),
                            );
                            continue;
                        }
                        OperationPointerObservation::WaitingOnChild
                        | OperationPointerObservation::StalePointerRemoved => continue,
                    };
                    let Some((driver, limits)) = capabilities.driver(eligible.record.family())
                    else {
                        continue;
                    };
                    let sequence = ClaimSequence::new(*next_claim_sequence)
                        .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?;
                    *next_claim_sequence = next_claim_sequence
                        .checked_add(1)
                        .ok_or(HelixDbError::IdentifierExhausted("index claim sequence"))?;
                    let Some(claimed) = outbox::claim_operation(
                        db,
                        &eligible,
                        writer_epoch,
                        sequence,
                        now_unix_millis(),
                        permission,
                    )
                    .await?
                    else {
                        continue;
                    };
                    outbox::execute_claimed_step(
                        db,
                        &claimed,
                        driver.as_ref(),
                        limits,
                        now_unix_millis(),
                    )
                    .await?;
                    did_work = true;
                    break;
                }
                if page.prefix_exhausted {
                    operation_cursor = None;
                }
                did_work
            }
            WorkLane::Upload => match capabilities.upload_driver() {
                None => {
                    upload_cursor = None;
                    false
                }
                Some(driver) => {
                    let page =
                        upload_queue::scan_upload_queue_page(db, upload_cursor, upload_page_size)
                            .await?;
                    upload_cursor = page.resume_after;
                    let mut did_work = false;
                    for intent_id in page.intent_ids {
                        let observation = upload_queue::observe_upload_pointer(
                            db,
                            intent_id,
                            active_text_mutations,
                            writer_epoch,
                            now_unix_millis(),
                        )
                        .await?;
                        let (eligible, permission) = match observation {
                            UploadPointerObservation::Eligible(eligible) => {
                                (eligible, ClaimPermission::Normal)
                            }
                            UploadPointerObservation::ClaimedByCurrentWriter(eligible) => {
                                let Some(proof) = same_epoch_proof else {
                                    continue;
                                };
                                (eligible, ClaimPermission::SameEpochRecovery(proof))
                            }
                            UploadPointerObservation::Delayed { delay_millis } => {
                                earliest_delay = Some(
                                    earliest_delay
                                        .map_or(delay_millis, |current| current.min(delay_millis)),
                                );
                                continue;
                            }
                            UploadPointerObservation::ActiveOwnerCurrentWriter
                            | UploadPointerObservation::StalePointerRemoved => continue,
                        };
                        let sequence = ClaimSequence::new(*next_claim_sequence)
                            .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?;
                        *next_claim_sequence = next_claim_sequence
                            .checked_add(1)
                            .ok_or(HelixDbError::IdentifierExhausted("index claim sequence"))?;
                        let Some(claimed) = upload_queue::claim_upload(
                            db,
                            &eligible,
                            active_text_mutations,
                            writer_epoch,
                            sequence,
                            now_unix_millis(),
                            permission,
                        )
                        .await?
                        else {
                            continue;
                        };
                        upload_queue::execute_claimed_upload_step(
                            db,
                            &claimed,
                            driver.as_ref(),
                            now_unix_millis(),
                        )
                        .await?;
                        did_work = true;
                        break;
                    }
                    if page.prefix_exhausted {
                        upload_cursor = None;
                    }
                    did_work
                }
            },
            WorkLane::BlobGcRoot => match capabilities.gc_driver() {
                None => {
                    root_cursor = None;
                    false
                }
                Some(driver) => {
                    let page = blob_gc::scan_root_page(db, root_cursor, root_page_size).await?;
                    root_cursor = page.resume_after;
                    let mut did_work = false;
                    for run_id in page.run_ids {
                        match driver
                            .execute_root_step(db, run_id, writer_epoch, now_unix_millis())
                            .await?
                        {
                            BlobGcRootStep::Progressed => {
                                did_work = true;
                                break;
                            }
                            BlobGcRootStep::Delayed { delay_millis } => {
                                earliest_delay = Some(
                                    earliest_delay
                                        .map_or(delay_millis, |current| current.min(delay_millis)),
                                );
                            }
                            BlobGcRootStep::Idle => {}
                        }
                    }
                    if page.prefix_exhausted {
                        root_cursor = None;
                    }
                    did_work
                }
            },
        };

        if outcome {
            lanes_without_work = 0;
            earliest_delay = None;
            continue;
        }
        lanes_without_work += 1;
        if lanes_without_work < WorkLane::ALL.len()
            || operation_cursor.is_some()
            || upload_cursor.is_some()
            || root_cursor.is_some()
        {
            continue;
        }

        lanes_without_work = 0;
        let delay = earliest_delay
            .take()
            .map(Duration::from_millis)
            .unwrap_or(IDLE_DELAY);
        tokio::select! {
            result = shutdown.changed() => {
                if result.is_err() || *shutdown.borrow() {
                    return Ok(WorkerCycleExit::Shutdown);
                }
            }
            () = wake.notified() => {}
            () = tokio::time::sleep(delay) => {}
        }
    }
}

fn now_unix_millis() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use sha2::{Digest, Sha256};
    use slatedb::object_store::memory::InMemory;
    use slatedb::DbTransaction;

    use super::*;
    use crate::config::{SearchIndexBackfillLimits, SecondaryIndexDefinition};
    use crate::encoding::v1::keys::index_v2 as index_keys;
    use crate::encoding::v1::keys::tenant::{DataScope, TenantId};
    use crate::encoding::v1::values::index_v2 as index_values;
    use crate::index_v2::blob_publication::BlobPublicationPermit;
    use crate::index_v2::outbox::{
        enqueue_operation, scan_operation_queue_page, ExpectedCanonicalRevision,
        IndexOperationStepResult,
    };
    use crate::index_v2::text::upload::{
        stage_prepared_upload, PreparedTextUploadIntent, PreparedUploadStageOutcome,
    };
    use crate::index_v2::{
        BlobPublicationPermitId, BuildOperationOutcome, IndexComponent, IndexElementKind,
        IndexGenerationId, IndexId, IndexIdentity, IndexIdentityFamily,
        IndexOperationExecutionState, IndexOperationId, IndexOperationOutcome,
        IndexOperationProgress, IndexOperationRecord, IndexOperationRevision, IndexRecordV2,
        IndexRevision, NoCursorProgress, OperationCounters, PhysicalGeneration,
        SecondaryBuildProgress, SecondaryBuildStage, ValidatedDynamicIndexDefinition,
        VectorBuildProgress, VectorBuildStage,
    };

    /// Text operation driver used only to exercise capability wiring.
    struct NoopTextOperationDriver;

    #[async_trait]
    impl IndexOperationDriver for NoopTextOperationDriver {
        fn family(&self) -> IndexOperationFamily {
            IndexOperationFamily::Text
        }

        async fn step(
            &self,
            _db: &slatedb::Db,
            _transaction: &DbTransaction,
            _scope: DataScope,
            _operation: &IndexOperationRecord,
            _limits: SearchIndexBatchLimits,
        ) -> Result<IndexOperationStepResult> {
            Ok(IndexOperationStepResult::TransientFailure)
        }
    }

    /// Upload driver used only to exercise capability wiring.
    struct NoopTextUploadDriver;

    #[async_trait]
    impl TextUploadDriver for NoopTextUploadDriver {
        async fn prepare_step(
            &self,
            _intent: &super::super::work::TextUploadIntentValue,
        ) -> Result<PreparedTextUploadStep> {
            Ok(PreparedTextUploadStep::transient_failure())
        }
    }

    /// Blob-GC driver used only to exercise complete capability wiring.
    struct NoopBlobGcDriver;

    #[async_trait]
    impl BlobGcDriver for NoopBlobGcDriver {
        async fn execute_root_step(
            &self,
            _db: &slatedb::Db,
            _run_id: crate::index_v2::BlobGcRunId,
            _writer_epoch: WriterEpoch,
            _now_unix_millis: u64,
        ) -> Result<BlobGcRootStep> {
            Ok(BlobGcRootStep::Idle)
        }
    }

    #[test]
    fn capabilities_default_fail_closed_for_every_family() {
        let capabilities = IndexFamilyCapabilities::unavailable();
        assert!(capabilities
            .driver(IndexOperationFamily::Secondary)
            .is_none());
        assert!(capabilities.driver(IndexOperationFamily::Vector).is_none());
        assert!(capabilities.driver(IndexOperationFamily::Text).is_none());
        assert!(!capabilities.public_serving_ready(IndexOperationFamily::Vector));
    }

    #[test]
    fn explicit_only_driver_is_installed_but_hidden_from_background_dispatch() {
        struct NoopDriver;

        #[async_trait]
        impl IndexOperationDriver for NoopDriver {
            fn family(&self) -> IndexOperationFamily {
                IndexOperationFamily::Secondary
            }

            async fn step(
                &self,
                _db: &slatedb::Db,
                _transaction: &DbTransaction,
                _scope: DataScope,
                _operation: &IndexOperationRecord,
                _limits: SearchIndexBatchLimits,
            ) -> Result<IndexOperationStepResult> {
                Ok(IndexOperationStepResult::TransientFailure)
            }
        }

        let capabilities = IndexFamilyCapabilities::new(
            IndexFamilyCapability::DriverReady {
                driver: Arc::new(NoopDriver),
                limits: SearchIndexBackfillLimits::default().batch(),
                scheduling: IndexDriverScheduling::ExplicitOnly,
            },
            IndexFamilyCapability::Unavailable,
            TextIndexCapability::Unavailable,
        );
        assert!(capabilities
            .driver(IndexOperationFamily::Secondary)
            .is_none());
        assert!(!capabilities.public_ddl_ready(IndexOperationFamily::Secondary));
        assert!(!capabilities.public_serving_ready(IndexOperationFamily::Secondary));
    }

    #[test]
    fn text_capability_bundles_every_driver_and_gates_every_surface() {
        let limits = SearchIndexBackfillLimits::default().batch();
        let mutation_ready = IndexFamilyCapabilities::new(
            IndexFamilyCapability::Unavailable,
            IndexFamilyCapability::Unavailable,
            TextIndexCapability::MutationReady {
                upload_driver: Arc::new(NoopTextUploadDriver),
                gc_driver: Arc::new(NoopBlobGcDriver),
            },
        );
        assert!(!mutation_ready.lifecycle_driver_ready(IndexOperationFamily::Text));
        assert!(mutation_ready.driver(IndexOperationFamily::Text).is_none());
        assert!(mutation_ready.upload_driver().is_some());
        assert!(mutation_ready.gc_driver().is_some());
        assert!(!mutation_ready.public_ddl_ready(IndexOperationFamily::Text));
        assert!(!mutation_ready.public_serving_ready(IndexOperationFamily::Text));

        let explicit = IndexFamilyCapabilities::new(
            IndexFamilyCapability::Unavailable,
            IndexFamilyCapability::Unavailable,
            TextIndexCapability::DriverReady {
                driver: Arc::new(NoopTextOperationDriver),
                upload_driver: Arc::new(NoopTextUploadDriver),
                gc_driver: Arc::new(NoopBlobGcDriver),
                limits,
                scheduling: IndexDriverScheduling::ExplicitOnly,
            },
        );
        assert!(explicit.lifecycle_driver_ready(IndexOperationFamily::Text));
        assert!(explicit.driver(IndexOperationFamily::Text).is_none());
        assert!(explicit.upload_driver().is_none());
        assert!(explicit.gc_driver().is_none());
        assert!(!explicit.public_ddl_ready(IndexOperationFamily::Text));
        assert!(!explicit.public_serving_ready(IndexOperationFamily::Text));

        let ready = IndexFamilyCapabilities::new(
            IndexFamilyCapability::Unavailable,
            IndexFamilyCapability::Unavailable,
            TextIndexCapability::FullyReady {
                driver: Arc::new(NoopTextOperationDriver),
                upload_driver: Arc::new(NoopTextUploadDriver),
                gc_driver: Arc::new(NoopBlobGcDriver),
                limits,
                scheduling: IndexDriverScheduling::Automatic,
            },
        );
        assert!(ready.lifecycle_driver_ready(IndexOperationFamily::Text));
        assert!(ready.driver(IndexOperationFamily::Text).is_some());
        assert!(ready.upload_driver().is_some());
        assert!(ready.gc_driver().is_some());
        assert!(ready.public_ddl_ready(IndexOperationFamily::Text));
        assert!(ready.public_serving_ready(IndexOperationFamily::Text));
    }

    #[test]
    fn round_robin_lane_order_is_stable_and_complete() {
        assert_eq!(
            WorkLane::ALL,
            [WorkLane::Operation, WorkLane::Upload, WorkLane::BlobGcRoot]
        );
    }

    #[tokio::test]
    async fn automatic_text_capability_dispatches_upload_lane_before_shutdown() {
        struct StopAfterUploadDriver(watch::Sender<bool>);

        #[async_trait]
        impl TextUploadDriver for StopAfterUploadDriver {
            async fn prepare_step(
                &self,
                _intent: &super::super::work::TextUploadIntentValue,
            ) -> Result<PreparedTextUploadStep> {
                let _ = self.0.send(true);
                Ok(PreparedTextUploadStep::publication_succeeded())
            }
        }

        let db = Arc::new(
            slatedb::Db::builder("outbox-upload-lane", Arc::new(InMemory::new()))
                .build()
                .await
                .unwrap(),
        );
        let intent_id = super::super::TextUploadIntentId::from_bytes([31; 16]).unwrap();
        let payload = b"worker-dispatched upload";
        let blob = super::super::work::BlobRef::new(
            Sha256::digest(payload).into(),
            u64::try_from(payload.len()).unwrap(),
        );
        let split = super::super::work::SplitRef::try_new(blob, 0, 0, 0, blob.size()).unwrap();
        let prepared = PreparedTextUploadIntent::try_new(
            intent_id,
            IndexId::initial(),
            IndexIdentity::new(
                IndexIdentityFamily::Text,
                IndexElementKind::Node,
                IndexComponent::try_new("label", "Document").unwrap(),
                IndexComponent::try_new("property", "body").unwrap(),
            ),
            IndexGenerationId::initial(),
            super::super::work::TextPartition::Unpartitioned,
            blob,
            BlobPublicationPermit::from_id(BlobPublicationPermitId::from_bytes([32; 16]).unwrap()),
            super::super::work::TextUploadOwner::Build {
                operation_id: IndexOperationId::from_bytes([33; 16]).unwrap(),
                expected_operation_revision: IndexOperationRevision::initial(),
            },
            super::super::work::TextUploadAttachment::ManifestSplit(split),
        )
        .unwrap();
        let transaction = db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert_eq!(
            stage_prepared_upload(&transaction, DataScope::LegacyUnscoped, &prepared)
                .await
                .unwrap(),
            PreparedUploadStageOutcome::Staged
        );
        transaction.commit().await.unwrap();

        let (shutdown, mut shutdown_rx) = watch::channel(false);
        let capabilities = IndexFamilyCapabilities::new(
            IndexFamilyCapability::Unavailable,
            IndexFamilyCapability::Unavailable,
            TextIndexCapability::DriverReady {
                driver: Arc::new(NoopTextOperationDriver),
                upload_driver: Arc::new(StopAfterUploadDriver(shutdown)),
                gc_driver: Arc::new(NoopBlobGcDriver),
                limits: SearchIndexBackfillLimits::default().batch(),
                scheduling: IndexDriverScheduling::Automatic,
            },
        );
        assert!(!capabilities.public_ddl_ready(IndexOperationFamily::Text));
        assert!(!capabilities.public_serving_ready(IndexOperationFamily::Text));
        let mut next_claim_sequence = 1;
        let active_text_mutations = ActiveTextMutationRegistry::new();
        assert_eq!(
            run_worker_cycle(
                WorkerCycleContext {
                    db: db.as_ref(),
                    capabilities: &capabilities,
                    active_text_mutations: &active_text_mutations,
                    writer_epoch: WriterEpoch::from_bytes([34; 16]).unwrap(),
                    same_epoch_proof: None,
                    wake: &Notify::new(),
                },
                &mut next_claim_sequence,
                &mut shutdown_rx,
            )
            .await
            .unwrap(),
            WorkerCycleExit::Shutdown
        );
        let uploaded = crate::index_v2::repository::load_upload_from_pointer(&*db, intent_id)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            uploaded.phase,
            super::super::work::TextUploadPhase::Uploaded
        ));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn driver_ready_text_capability_dispatches_root_lane_without_owner_pointer() {
        struct StopAfterBlobGcDriver {
            shutdown: watch::Sender<bool>,
            calls: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl BlobGcDriver for StopAfterBlobGcDriver {
            async fn execute_root_step(
                &self,
                _db: &slatedb::Db,
                _run_id: crate::index_v2::BlobGcRunId,
                _writer_epoch: WriterEpoch,
                _now_unix_millis: u64,
            ) -> Result<BlobGcRootStep> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let _ = self.shutdown.send(true);
                Ok(BlobGcRootStep::Idle)
            }
        }

        let db = Arc::new(
            slatedb::Db::builder("outbox-blob-gc-root-lane", Arc::new(InMemory::new()))
                .build()
                .await
                .unwrap(),
        );
        let run_id = crate::index_v2::BlobGcRunId::from_bytes([41; 16]).unwrap();
        let operation_id = IndexOperationId::from_bytes([42; 16]).unwrap();
        let root = super::super::work::BlobGcRunRootValue::try_new(
            run_id,
            super::super::work::BlobGcRunOwner::GenerationCleanup {
                scope: DataScope::LegacyUnscoped,
                operation_id,
                index_id: IndexId::initial(),
                generation: IndexGenerationId::initial(),
            },
            super::super::BlobGcRunRevision::initial(),
            0,
            None,
            super::super::work::BlobGcPhase::AwaitDeleteFences {
                member_cursor: None,
            },
            1,
        )
        .unwrap();
        db.put(
            index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes(),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcEntry(
                super::super::work::BlobGcEntryValue::RunRoot(root),
            )),
        )
        .await
        .unwrap();

        let (shutdown, mut shutdown_rx) = watch::channel(false);
        let calls = Arc::new(AtomicUsize::new(0));
        let capabilities = IndexFamilyCapabilities::new(
            IndexFamilyCapability::Unavailable,
            IndexFamilyCapability::Unavailable,
            TextIndexCapability::DriverReady {
                driver: Arc::new(NoopTextOperationDriver),
                upload_driver: Arc::new(NoopTextUploadDriver),
                gc_driver: Arc::new(StopAfterBlobGcDriver {
                    shutdown,
                    calls: Arc::clone(&calls),
                }),
                limits: SearchIndexBackfillLimits::default().batch(),
                scheduling: IndexDriverScheduling::Automatic,
            },
        );
        let mut next_claim_sequence = 1;
        let active_text_mutations = ActiveTextMutationRegistry::new();
        assert_eq!(
            run_worker_cycle(
                WorkerCycleContext {
                    db: db.as_ref(),
                    capabilities: &capabilities,
                    active_text_mutations: &active_text_mutations,
                    writer_epoch: WriterEpoch::from_bytes([43; 16]).unwrap(),
                    same_epoch_proof: None,
                    wake: &Notify::new(),
                },
                &mut next_claim_sequence,
                &mut shutdown_rx,
            )
            .await
            .unwrap(),
            WorkerCycleExit::Shutdown
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        db.close().await.unwrap();
    }

    #[test]
    fn wall_clock_conversion_cannot_produce_a_negative_deadline() {
        assert!(now_unix_millis() > 0);
    }

    #[tokio::test]
    async fn supervised_restart_recovers_same_epoch_claim_discovered_through_tenant_pointer() {
        struct RestartThenComplete {
            calls: AtomicUsize,
        }

        #[async_trait]
        impl IndexOperationDriver for RestartThenComplete {
            fn family(&self) -> IndexOperationFamily {
                IndexOperationFamily::Secondary
            }

            async fn step(
                &self,
                _db: &slatedb::Db,
                _transaction: &DbTransaction,
                _scope: crate::encoding::v1::keys::tenant::DataScope,
                _operation: &IndexOperationRecord,
                _limits: SearchIndexBatchLimits,
            ) -> Result<IndexOperationStepResult> {
                if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Ok(IndexOperationStepResult::Progressed(
                        IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                            VectorBuildStage::Activate(NoCursorProgress {
                                counters: OperationCounters::default(),
                            }),
                        )),
                    ));
                }
                Ok(IndexOperationStepResult::Completed(
                    IndexOperationOutcome::Build(BuildOperationOutcome::Succeeded),
                ))
            }
        }

        let db = Arc::new(
            slatedb::Db::builder(
                "outbox-supervised-tenant-restart",
                Arc::new(InMemory::new()),
            )
            .build()
            .await
            .unwrap(),
        );
        let definition = ValidatedDynamicIndexDefinition::try_from(
            SecondaryIndexDefinition::node_equality("TenantUser", "email").unwrap(),
        )
        .unwrap();
        let operation_id = IndexOperationId::from_bytes([9; 16]).unwrap();
        let index = IndexRecordV2::building(
            IndexId::initial(),
            definition,
            IndexRevision::initial(),
            PhysicalGeneration::Secondary {
                generation: IndexGenerationId::initial(),
            },
            operation_id,
        )
        .unwrap();
        let operation = IndexOperationRecord::try_new(
            operation_id,
            index.index_id(),
            index.identity().clone(),
            index.state().generation(),
            index.revision(),
            IndexOperationRevision::initial(),
            super::super::IndexOperationKind::Build,
            IndexOperationFamily::Secondary,
            IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                SecondaryBuildStage::Activate(NoCursorProgress {
                    counters: OperationCounters::default(),
                }),
            )),
            0,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .unwrap();
        let scope = DataScope::Tenant(TenantId::from_u128(42));
        enqueue_operation(
            db.as_ref(),
            scope,
            ExpectedCanonicalRevision::Absent,
            &index,
            &operation,
        )
        .await
        .unwrap();

        let driver = Arc::new(RestartThenComplete {
            calls: AtomicUsize::new(0),
        });
        let supervisor = IndexWorkerSupervisor::start(
            Arc::clone(&db),
            IndexFamilyCapabilities::new(
                IndexFamilyCapability::DriverReady {
                    driver: driver.clone(),
                    limits: SearchIndexBackfillLimits::default().batch(),
                    scheduling: IndexDriverScheduling::Automatic,
                },
                IndexFamilyCapability::Unavailable,
                TextIndexCapability::Unavailable,
            ),
            ActiveTextMutationRegistry::new(),
        );
        supervisor.wake();

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if scan_operation_queue_page(
                    db.as_ref(),
                    None,
                    OperationQueuePageSize::new(1).unwrap(),
                )
                .await
                .unwrap()
                .operation_ids
                .is_empty()
                    && driver.calls.load(Ordering::SeqCst) >= 2
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("supervisor recovers the exact same-epoch claim");

        supervisor.stop().await;
        db.close().await.unwrap();
    }
}
