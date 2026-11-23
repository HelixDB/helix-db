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

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{watch, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinError, JoinHandle, JoinSet};

use crate::config::{IndexLifecycleConcurrency, SearchIndexBatchLimits};
use crate::error::{HelixDbError, Result};

use super::outbox::{
    self, ClaimPermission, IndexOperationDriver, OperationPointerObservation,
    OperationQueuePageSize, SameEpochRecoveryProof,
};
use super::queue::publication::{NextTarget, PublicationOutcome, QueuePublisher};
use super::queue::QueueTarget;
use super::{
    ClaimContinuation, ClaimSequence, IndexOperationFamily, IndexOperationId, WriterEpoch,
};

const DEFAULT_OPERATION_PAGE_SIZE: usize = 64;
/// Longest one task runs consecutive steps of one operation under a single
/// claim.
///
/// Within a slice each step keeps its claim, so the next one needs no claim
/// transaction. The first step after the slice ends or after shutdown is
/// requested releases it, and so does a step whose driver parks the
/// operation ([`IndexOperationDriver::parked_until`]), with its own commit:
/// the dispatcher can then give the family's lane to another queued
/// operation, and shutdown or a failing cycle joins the task within about
/// one slice plus one step.
///
/// Steps commit without waiting for their WAL upload, so the task ends each
/// slice by uploading them. A build therefore runs at most one slice ahead of
/// durability: when object storage falls behind, the build slows down
/// instead of growing the WAL tail that every durable write and every gated
/// step waits behind. Measured on S3, a clean 1M-row range build took as
/// long as unbounded (about 20 s), and one under 1,000 writes/s about 7%
/// longer.
const CLAIM_SLICE: Duration = Duration::from_secs(1);
const SUPERVISOR_RESTART_DELAY: Duration = Duration::from_millis(10);
const IDLE_DELAY: Duration = Duration::from_secs(24 * 60 * 60);
/// Operation-family lanes plus the queued-publication lane.
const DISPATCH_LANES: usize = OPERATION_FAMILIES.len() + 1;

/// Installed runtime service for one physical index family.
#[derive(Clone)]
pub(crate) struct IndexFamilyCapability {
    /// Physical driver installed by the family service.
    driver: Arc<dyn IndexOperationDriver>,
    /// Existing validated source/transaction limits passed to every step.
    limits: SearchIndexBatchLimits,
    /// Whether the parent-owned supervisor may claim this family's work.
    scheduling: IndexDriverScheduling,
}

impl IndexFamilyCapability {
    pub(crate) const fn new(
        driver: Arc<dyn IndexOperationDriver>,
        limits: SearchIndexBatchLimits,
        scheduling: IndexDriverScheduling,
    ) -> Self {
        Self {
            driver,
            limits,
            scheduling,
        }
    }
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
    text: IndexFamilyCapability,
    text_compactor: Arc<dyn ActiveTextCompactionDriver>,
}

/// Runtime-only Active text compaction service installed by the text driver.
#[async_trait]
pub(crate) trait ActiveTextCompactionDriver: Send + Sync {
    /// Processes at most one durable manifest-page pointer.
    async fn compact_active_text_once(&self, db: &slatedb::Db) -> Result<bool>;
}

impl IndexFamilyCapabilities {
    /// Builds a registry from the three installed family drivers.
    pub(crate) const fn new(
        secondary: IndexFamilyCapability,
        vector: IndexFamilyCapability,
        text: IndexFamilyCapability,
        text_compactor: Arc<dyn ActiveTextCompactionDriver>,
    ) -> Self {
        Self {
            secondary,
            vector,
            text,
            text_compactor,
        }
    }

    fn driver(
        &self,
        family: IndexOperationFamily,
    ) -> Option<(&Arc<dyn IndexOperationDriver>, SearchIndexBatchLimits)> {
        family_driver(match family {
            IndexOperationFamily::Secondary => &self.secondary,
            IndexOperationFamily::Vector => &self.vector,
            IndexOperationFamily::Text => &self.text,
        })
    }

    fn automatic_text_compactor(&self) -> Option<&Arc<dyn ActiveTextCompactionDriver>> {
        match self.text.scheduling {
            IndexDriverScheduling::Automatic => Some(&self.text_compactor),
            IndexDriverScheduling::ExplicitOnly => None,
        }
    }

    /// Returns the installed secondary driver reserved for explicit stepping.
    pub(crate) fn explicit_secondary_driver(
        &self,
    ) -> Option<(&Arc<dyn IndexOperationDriver>, SearchIndexBatchLimits)> {
        match self.secondary.scheduling {
            IndexDriverScheduling::ExplicitOnly => {
                Some((&self.secondary.driver, self.secondary.limits))
            }
            IndexDriverScheduling::Automatic => None,
        }
    }

    /// Returns an installed driver regardless of background scheduling policy.
    #[cfg(feature = "index-lifecycle-testing")]
    pub(crate) fn explicit_driver(
        &self,
        family: IndexOperationFamily,
    ) -> Option<(&Arc<dyn IndexOperationDriver>, SearchIndexBatchLimits)> {
        let capability = match family {
            IndexOperationFamily::Secondary => &self.secondary,
            IndexOperationFamily::Vector => &self.vector,
            IndexOperationFamily::Text => &self.text,
        };
        Some((&capability.driver, capability.limits))
    }
}

/// Selects one automatically scheduled non-text family driver.
fn family_driver(
    capability: &IndexFamilyCapability,
) -> Option<(
    &Arc<dyn outbox::IndexOperationDriver>,
    SearchIndexBatchLimits,
)> {
    match capability.scheduling {
        IndexDriverScheduling::Automatic => Some((&capability.driver, capability.limits)),
        IndexDriverScheduling::ExplicitOnly => None,
    }
}

/// One checked sequence namespace shared by every claim source in a writer.
pub(crate) struct ClaimSequenceAllocator {
    next: AtomicU64,
}

impl ClaimSequenceAllocator {
    /// Starts a fresh writer epoch at the first valid claim sequence.
    pub(crate) const fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }

    /// Allocates one sequence without permitting wraparound.
    pub(crate) fn next(&self) -> Result<ClaimSequence> {
        let raw = self
            .next
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| HelixDbError::IdentifierExhausted("index claim sequence"))?;
        ClaimSequence::new(raw).map_err(|error| HelixDbError::InvariantViolation(error.to_string()))
    }
}

const OPERATION_FAMILIES: [IndexOperationFamily; 3] = [
    IndexOperationFamily::Secondary,
    IndexOperationFamily::Vector,
    IndexOperationFamily::Text,
];

/// Exact durable target currently owned by one spawned worker task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum InFlightTarget {
    Operation(IndexOperationId),
    TextCompaction,
    Publication(QueueTarget),
}

/// Global plus family/lane permits retained until one task is fully joined.
struct InFlightPermits {
    _global: OwnedSemaphorePermit,
    _lane: OwnedSemaphorePermit,
}

/// Runtime semaphores derived from one validated concurrency policy.
struct WorkerPermits {
    global: Arc<Semaphore>,
    secondary: Arc<Semaphore>,
    vector: Arc<Semaphore>,
    text: Arc<Semaphore>,
    publication: Arc<Semaphore>,
}

impl WorkerPermits {
    fn new(concurrency: IndexLifecycleConcurrency) -> Self {
        Self {
            global: Arc::new(Semaphore::new(concurrency.total_operation_tasks().get())),
            secondary: Arc::new(Semaphore::new(concurrency.secondary_tasks().get())),
            vector: Arc::new(Semaphore::new(concurrency.vector_tasks().get())),
            text: Arc::new(Semaphore::new(concurrency.text_tasks().get())),
            publication: Arc::new(Semaphore::new(concurrency.publication_tasks().get())),
        }
    }

    fn try_operation(&self, family: IndexOperationFamily) -> Option<InFlightPermits> {
        let lane = match family {
            IndexOperationFamily::Secondary => &self.secondary,
            IndexOperationFamily::Vector => &self.vector,
            IndexOperationFamily::Text => &self.text,
        };
        self.try_pair(lane)
    }

    fn try_pair(&self, lane: &Arc<Semaphore>) -> Option<InFlightPermits> {
        let global = Arc::clone(&self.global).try_acquire_owned().ok()?;
        let lane = Arc::clone(lane).try_acquire_owned().ok()?;
        Some(InFlightPermits {
            _global: global,
            _lane: lane,
        })
    }
}

/// An eligible operation the task claims before its step, so claims of
/// separate operations commit concurrently instead of one at a time in the
/// dispatch loop.
struct PendingClaim {
    eligible: outbox::EligibleOperation,
    writer_epoch: WriterEpoch,
    sequence: ClaimSequence,
    permission: ClaimPermission,
}

/// Typed work retained by the task set once its target is reserved.
enum InFlightTask {
    Operation {
        target: IndexOperationId,
        claim: Box<PendingClaim>,
        driver: Arc<dyn IndexOperationDriver>,
        limits: SearchIndexBatchLimits,
        /// Ends the claim slice early once shutdown is requested.
        shutdown: watch::Receiver<bool>,
        _permits: InFlightPermits,
        #[cfg(feature = "index-lifecycle-testing")]
        lifecycle_metrics: Arc<crate::index_lifecycle_testing::AutomaticLifecycleMetrics>,
    },
    TextCompaction {
        driver: Arc<dyn ActiveTextCompactionDriver>,
        _permits: InFlightPermits,
    },
    Publication {
        target: QueueTarget,
        publisher: Arc<QueuePublisher>,
        _permits: InFlightPermits,
    },
}

impl InFlightTask {
    const fn target(&self) -> InFlightTarget {
        match self {
            Self::Operation { target, .. } => InFlightTarget::Operation(*target),
            Self::TextCompaction { .. } => InFlightTarget::TextCompaction,
            Self::Publication { target, .. } => InFlightTarget::Publication(*target),
        }
    }

    async fn execute(self, db: Arc<slatedb::Db>) -> Result<InFlightCompletion> {
        let target = self.target();
        let did_work = match self {
            Self::Operation {
                claim,
                driver,
                limits,
                shutdown,
                _permits,
                #[cfg(feature = "index-lifecycle-testing")]
                lifecycle_metrics,
                ..
            } => 'operation: {
                let PendingClaim {
                    eligible,
                    writer_epoch,
                    sequence,
                    permission,
                } = *claim;
                let claimed = match outbox::claim_operation(
                    db.as_ref(),
                    &eligible,
                    writer_epoch,
                    sequence,
                    now_unix_millis(),
                    permission,
                )
                .await
                {
                    Ok(Some(claimed)) => claimed,
                    Err(error) if !error.is_transaction_conflict() => return Err(error),
                    // Another claim or step moved the operation first, or won a
                    // serializable conflict with this claim: nothing committed.
                    // It counts as work, because the operation may still be
                    // eligible: a task that did none lets the dispatcher idle
                    // (up to `IDLE_DELAY`) before the queue is scanned again.
                    lost => {
                        tracing::debug!(
                            operation_id = %eligible.record.operation_id().as_uuid(),
                            conflict = lost.is_err(),
                            "index operation claim lost; the queue offers it again"
                        );
                        #[cfg(feature = "index-lifecycle-testing")]
                        lifecycle_metrics.observe_worker_claim_conflict();
                        break 'operation true;
                    }
                };
                #[cfg(feature = "index-lifecycle-testing")]
                let _in_flight =
                    lifecycle_metrics.begin_worker_task(match claimed.record.family() {
                        IndexOperationFamily::Secondary => {
                            crate::index_lifecycle_testing::AutomaticLifecycleTaskKind::Secondary
                        }
                        IndexOperationFamily::Vector => {
                            crate::index_lifecycle_testing::AutomaticLifecycleTaskKind::Vector
                        }
                        IndexOperationFamily::Text => {
                            crate::index_lifecycle_testing::AutomaticLifecycleTaskKind::Text
                        }
                    });
                // The task target keeps every other task of this writer off
                // the operation for the whole slice; see [`CLAIM_SLICE`].
                let slice = std::time::Instant::now();
                let mut claimed = claimed;
                // Whether a step of this slice committed without waiting for
                // its WAL upload.
                let mut applied = false;
                loop {
                    // The dispatcher observed only the slice's first step.
                    #[cfg(test)]
                    crate::migrations::observe_legacy_text_migration_operation(&claimed.record)?;
                    let claim = if *shutdown.borrow() || slice.elapsed() >= CLAIM_SLICE {
                        ClaimContinuation::Release
                    } else {
                        ClaimContinuation::Retain
                    };
                    let executed = outbox::execute_claimed_step_with_evidence(
                        db.as_ref(),
                        &claimed,
                        driver.as_ref(),
                        limits,
                        now_unix_millis(),
                        claim,
                    )
                    .await?;
                    #[cfg(feature = "index-lifecycle-testing")]
                    lifecycle_metrics.observe_operation(executed.evidence);
                    applied |=
                        executed.evidence.outcome == outbox::CommittedOperationStep::Progressed;
                    let Some(retained) = executed.retained else {
                        // See [`CLAIM_SLICE`]: the slice ends once its steps
                        // are durable. A blocked or completed step waited for
                        // its upload, which made every earlier one durable;
                        // after any other last step, such as a progressed one
                        // or a retained claim found superseded, the slice
                        // uploads what its progressed steps applied.
                        if applied
                            && !matches!(
                                executed.evidence.outcome,
                                outbox::CommittedOperationStep::Blocked
                                    | outbox::CommittedOperationStep::Completed
                            )
                        {
                            db.flush().await?;
                        }
                        break;
                    };
                    claimed = retained;
                }
                true
            }
            Self::TextCompaction { driver, _permits } => {
                driver.compact_active_text_once(db.as_ref()).await?
            }
            Self::Publication {
                target,
                publisher,
                _permits,
            } => {
                let outcome = publisher.publish_once(target).await?;
                matches!(
                    outcome,
                    PublicationOutcome::Published { .. }
                        | PublicationOutcome::Discarded { .. }
                        | PublicationOutcome::Trimmed
                        | PublicationOutcome::Blocked
                )
            }
        };
        Ok(InFlightCompletion { target, did_work })
    }
}

struct InFlightCompletion {
    target: InFlightTarget,
    did_work: bool,
}

/// A bounded scan keeps its position for fairness. Work or a wake invalidates
/// its idle proof, so a partial suffix must wrap before it can report idle.
#[derive(Clone, Copy, Default)]
struct OperationScan {
    cursor: Option<IndexOperationId>,
    wrap_before_idle: bool,
}

enum DispatchAttempt {
    Scheduled(Box<InFlightTask>),
    Continue,
    Idle,
}

/// Owned dependencies for one supervised worker task.
struct WorkerSupervisorContext {
    db: Arc<slatedb::Db>,
    capabilities: IndexFamilyCapabilities,
    publisher: Option<Arc<QueuePublisher>>,
    sweep_interval: Duration,
    concurrency: IndexLifecycleConcurrency,
    claim_sequences: Arc<ClaimSequenceAllocator>,
    #[cfg(feature = "index-lifecycle-testing")]
    lifecycle_metrics: Arc<crate::index_lifecycle_testing::AutomaticLifecycleMetrics>,
    writer_epoch: WriterEpoch,
    wake: Arc<Notify>,
    shutdown: watch::Receiver<bool>,
}

/// Supervisor retained by `HelixDBInner` and joined by the close protocol.
pub(crate) struct IndexWorkerSupervisor {
    writer_epoch: WriterEpoch,
    wake: Arc<Notify>,
    shutdown: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

/// Cloneable lock-free notification capability for the lifecycle worker.
///
/// Created before the worker so the operation ledger can wake it too; a
/// handle whose worker never starts (a reader) wakes nothing.
#[derive(Debug, Clone, Default)]
pub(crate) struct IndexWorkerWakeHandle {
    wake: Arc<Notify>,
}

impl IndexWorkerWakeHandle {
    /// Wakes the worker, or its next wait if it is busy.
    pub(crate) fn wake(&self) {
        self.wake.notify_one();
    }

    /// Consumes a pending wake, returning whether there was one.
    #[cfg(test)]
    pub(crate) fn take_wake(&self) -> bool {
        futures::FutureExt::now_or_never(self.wake.notified()).is_some()
    }
}

impl IndexWorkerSupervisor {
    /// Starts one supervised global worker after writer fencing succeeds.
    #[allow(
        clippy::too_many_arguments,
        reason = "the supervisor owns every lifecycle and publication dependency"
    )]
    pub(crate) fn start(
        db: Arc<slatedb::Db>,
        capabilities: IndexFamilyCapabilities,
        publisher: Option<Arc<QueuePublisher>>,
        sweep_interval: Duration,
        concurrency: IndexLifecycleConcurrency,
        claim_sequences: Arc<ClaimSequenceAllocator>,
        wake: IndexWorkerWakeHandle,
        #[cfg(feature = "index-lifecycle-testing")] lifecycle_metrics: Arc<
            crate::index_lifecycle_testing::AutomaticLifecycleMetrics,
        >,
    ) -> Self {
        let writer_epoch = WriterEpoch::new_v4();
        let IndexWorkerWakeHandle { wake } = wake;
        let (shutdown, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(supervise_worker(WorkerSupervisorContext {
            db,
            capabilities,
            publisher,
            sweep_interval,
            concurrency,
            claim_sequences,
            #[cfg(feature = "index-lifecycle-testing")]
            lifecycle_metrics,
            writer_epoch,
            wake: Arc::clone(&wake),
            shutdown: shutdown_rx,
        }));
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

/// Advances at most one immediately eligible secondary operation.
///
/// The caller serializes invocations and proves that automatic secondary
/// scheduling is disabled. A synchronous return also proves any prior
/// explicit task in this writer epoch has joined, permitting recovery of its
/// durable claim.
pub(crate) async fn process_secondary_once(
    db: &slatedb::Db,
    capabilities: &IndexFamilyCapabilities,
    writer_epoch: WriterEpoch,
    claim_sequences: &ClaimSequenceAllocator,
) -> Result<bool> {
    let Some((driver, limits)) = capabilities.explicit_secondary_driver() else {
        return Err(HelixDbError::SecondaryLifecycleSteppingRequiresDisabledMode);
    };
    let driver = Arc::clone(driver);
    let page_size = OperationQueuePageSize::new(DEFAULT_OPERATION_PAGE_SIZE)?;
    let mut resume_after = None;
    let same_epoch_proof = SameEpochRecoveryProof::after_join(writer_epoch);

    loop {
        let page = outbox::scan_operation_queue_page(db, resume_after, page_size).await?;
        for operation_id in page.operation_ids {
            let Some((_scope, operation)) = outbox::read_queued_operation(db, operation_id).await?
            else {
                continue;
            };
            if operation.family() != IndexOperationFamily::Secondary {
                continue;
            }
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
                OperationPointerObservation::ClaimedByCurrentWriter(eligible) => (
                    eligible,
                    ClaimPermission::SameEpochRecovery(same_epoch_proof),
                ),
                OperationPointerObservation::Delayed { .. } => continue,
                OperationPointerObservation::StalePointerRemoved => return Ok(true),
            };
            let Some(claimed) = outbox::claim_operation(
                db,
                &eligible,
                writer_epoch,
                claim_sequences.next()?,
                now_unix_millis(),
                permission,
            )
            .await?
            else {
                continue;
            };
            outbox::execute_claimed_step(db, &claimed, driver.as_ref(), limits, now_unix_millis())
                .await?;
            return Ok(true);
        }
        if page.prefix_exhausted {
            return Ok(false);
        }
        resume_after = page.resume_after;
    }
}

async fn supervise_worker(context: WorkerSupervisorContext) {
    let mut shutdown = context.shutdown;
    let mut same_epoch_proof = None;
    loop {
        match run_worker_cycle(
            WorkerCycleContext {
                db: &context.db,
                capabilities: &context.capabilities,
                publisher: context.publisher.as_ref(),
                sweep_interval: context.sweep_interval,
                concurrency: context.concurrency,
                #[cfg(feature = "index-lifecycle-testing")]
                lifecycle_metrics: &context.lifecycle_metrics,
                writer_epoch: context.writer_epoch,
                same_epoch_proof,
                wake: &context.wake,
            },
            &context.claim_sequences,
            &mut shutdown,
        )
        .await
        {
            Ok(WorkerCycleExit::Shutdown) => return,
            Err(error) => {
                #[cfg(feature = "index-lifecycle-testing")]
                {
                    context.lifecycle_metrics.observe_worker_failure();
                    context.lifecycle_metrics.observe_worker_restart();
                }
                tracing::warn!(
                    %error,
                    writer_epoch = %context.writer_epoch.as_uuid(),
                    "index outbox worker cycle failed; restarting after termination"
                );
                same_epoch_proof = Some(SameEpochRecoveryProof::after_join(context.writer_epoch));
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
    db: &'a Arc<slatedb::Db>,
    capabilities: &'a IndexFamilyCapabilities,
    publisher: Option<&'a Arc<QueuePublisher>>,
    sweep_interval: Duration,
    concurrency: IndexLifecycleConcurrency,
    #[cfg(feature = "index-lifecycle-testing")]
    lifecycle_metrics: &'a Arc<crate::index_lifecycle_testing::AutomaticLifecycleMetrics>,
    writer_epoch: WriterEpoch,
    same_epoch_proof: Option<SameEpochRecoveryProof>,
    wake: &'a Notify,
}

async fn run_worker_cycle(
    context: WorkerCycleContext<'_>,
    claim_sequences: &ClaimSequenceAllocator,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<WorkerCycleExit> {
    #[cfg(feature = "index-lifecycle-testing")]
    let lifecycle_metrics = Arc::clone(context.lifecycle_metrics);
    let mut tasks = JoinSet::new();
    let mut targets = HashSet::new();
    let result =
        dispatch_worker_cycle(context, claim_sequences, shutdown, &mut tasks, &mut targets).await;
    drain_in_flight(
        &mut tasks,
        &mut targets,
        #[cfg(feature = "index-lifecycle-testing")]
        &lifecycle_metrics,
    )
    .await;
    result
}

async fn dispatch_worker_cycle(
    context: WorkerCycleContext<'_>,
    claim_sequences: &ClaimSequenceAllocator,
    shutdown: &mut watch::Receiver<bool>,
    tasks: &mut JoinSet<JoinedInFlightTask>,
    targets: &mut HashSet<InFlightTarget>,
) -> Result<WorkerCycleExit> {
    let WorkerCycleContext {
        db,
        capabilities,
        publisher,
        sweep_interval,
        concurrency,
        #[cfg(feature = "index-lifecycle-testing")]
        lifecycle_metrics,
        writer_epoch,
        same_epoch_proof,
        wake,
    } = context;
    let page_size = OperationQueuePageSize::new(DEFAULT_OPERATION_PAGE_SIZE)?;
    let permits = WorkerPermits::new(concurrency);
    let mut operation_scans = [OperationScan::default(); OPERATION_FAMILIES.len()];
    let mut operation_family_index = 0_usize;
    let mut lanes_without_work = [false; DISPATCH_LANES];
    let mut earliest_deadline = None::<tokio::time::Instant>;

    loop {
        while let Some(joined) = tasks.try_join_next() {
            if finish_joined_task(
                joined,
                targets,
                #[cfg(feature = "index-lifecycle-testing")]
                lifecycle_metrics,
            )? {
                lanes_without_work.fill(false);
                for scan in &mut operation_scans {
                    scan.wrap_before_idle = scan.cursor.is_some();
                }
                earliest_deadline = None;
            }
        }
        if *shutdown.borrow() {
            return Ok(WorkerCycleExit::Shutdown);
        }
        let lane = operation_family_index;
        operation_family_index = (operation_family_index + 1) % DISPATCH_LANES;
        // A completed sweep does not exclude new work arriving while another
        // lane continues scanning.
        let attempt = 'dispatch: {
            let Some(&family) = OPERATION_FAMILIES.get(lane) else {
                break 'dispatch schedule_publication_task(
                    publisher,
                    &permits,
                    targets,
                    &mut earliest_deadline,
                );
            };
            let attempt = schedule_operation_task(
                db,
                capabilities,
                &permits,
                targets,
                family,
                &mut operation_scans[lane],
                page_size,
                writer_epoch,
                same_epoch_proof,
                claim_sequences,
                shutdown,
                &mut earliest_deadline,
                #[cfg(feature = "index-lifecycle-testing")]
                lifecycle_metrics,
            )
            .await?;
            if family == IndexOperationFamily::Text && matches!(attempt, DispatchAttempt::Idle) {
                schedule_text_compaction_task(db, capabilities, &permits, targets).await?
            } else {
                attempt
            }
        };

        match attempt {
            DispatchAttempt::Scheduled(task) => {
                lanes_without_work.fill(false);
                let target = task.target();
                assert!(
                    targets.insert(target),
                    "one durable index target cannot execute twice"
                );
                let task_db = Arc::clone(db);
                tasks.spawn(async move {
                    JoinedInFlightTask {
                        target,
                        result: (*task).execute(task_db).await,
                    }
                });
                continue;
            }
            DispatchAttempt::Continue => continue,
            DispatchAttempt::Idle => {
                lanes_without_work[lane] = true;
            }
        }

        if !lanes_without_work.iter().all(|idle| *idle) {
            continue;
        }
        lanes_without_work.fill(false);
        for scan in &mut operation_scans {
            scan.wrap_before_idle = scan.cursor.is_some();
        }
        if wait_for_work(
            shutdown,
            tasks,
            targets,
            wake,
            &mut earliest_deadline,
            publisher.is_some_and(|publisher| publisher.has_outstanding_work()),
            sweep_interval,
            #[cfg(feature = "index-lifecycle-testing")]
            lifecycle_metrics,
        )
        .await?
        {
            return Ok(WorkerCycleExit::Shutdown);
        }
    }
}

/// Waits for a wake, a joined task, the earliest delay, or the recovery sweep.
///
/// Returns `true` when shutdown was requested. Outstanding queued work caps
/// the wait at the recovery sweep interval so a lost commit notification or a
/// restart delays publication by at most one sweep. Both timers apply while
/// tasks are in flight too: a long step never holds back a backed-off target.
#[allow(
    clippy::too_many_arguments,
    reason = "the idle wait observes every wake source of one worker cycle"
)]
async fn wait_for_work(
    shutdown: &mut watch::Receiver<bool>,
    tasks: &mut JoinSet<JoinedInFlightTask>,
    targets: &mut HashSet<InFlightTarget>,
    wake: &Notify,
    earliest_deadline: &mut Option<tokio::time::Instant>,
    queued_work: bool,
    sweep_interval: Duration,
    #[cfg(feature = "index-lifecycle-testing")] lifecycle_metrics: &Arc<
        crate::index_lifecycle_testing::AutomaticLifecycleMetrics,
    >,
) -> Result<bool> {
    let now = tokio::time::Instant::now();
    let mut delay = earliest_deadline
        .take()
        .map(|deadline| deadline.saturating_duration_since(now))
        .unwrap_or(IDLE_DELAY);
    if queued_work {
        delay = delay.min(sweep_interval);
    }
    if !tasks.is_empty() {
        tokio::select! {
            result = shutdown.changed() => {
                if result.is_err() || *shutdown.borrow() {
                    return Ok(true);
                }
            }
            joined = tasks.join_next() => {
                let Some(joined) = joined else {
                    return Err(HelixDbError::InvariantViolation(
                        "non-empty index task set returned no task".to_string(),
                    ));
                };
                if finish_joined_task(
                    joined,
                    targets,
                    #[cfg(feature = "index-lifecycle-testing")]
                    lifecycle_metrics,
                )? {
                    *earliest_deadline = None;
                }
            }
            () = wake.notified() => {}
            () = tokio::time::sleep_until(now + delay) => {}
        }
        return Ok(false);
    }
    tokio::select! {
        result = shutdown.changed() => {
            if result.is_err() || *shutdown.borrow() {
                return Ok(true);
            }
        }
        () = wake.notified() => {}
        () = tokio::time::sleep_until(now + delay) => {}
    }
    Ok(false)
}

fn schedule_publication_task(
    publisher: Option<&Arc<QueuePublisher>>,
    permits: &WorkerPermits,
    targets: &HashSet<InFlightTarget>,
    earliest_deadline: &mut Option<tokio::time::Instant>,
) -> DispatchAttempt {
    let Some(publisher) = publisher else {
        return DispatchAttempt::Idle;
    };
    let in_flight = targets
        .iter()
        .filter_map(|target| match target {
            InFlightTarget::Publication(target) => Some(*target),
            InFlightTarget::Operation(_) | InFlightTarget::TextCompaction => None,
        })
        .collect::<HashSet<_>>();
    let now = std::time::Instant::now();
    let target = match publisher.next_target(&in_flight, now) {
        NextTarget::Ready(target) => target,
        NextTarget::Delayed(not_before) => {
            let deadline = tokio::time::Instant::from_std(not_before);
            *earliest_deadline =
                Some(earliest_deadline.map_or(deadline, |current| current.min(deadline)));
            return DispatchAttempt::Idle;
        }
        NextTarget::Idle => return DispatchAttempt::Idle,
    };
    let Some(task_permits) = permits.try_pair(&permits.publication) else {
        return DispatchAttempt::Idle;
    };
    DispatchAttempt::Scheduled(Box::new(InFlightTask::Publication {
        target,
        publisher: Arc::clone(publisher),
        _permits: task_permits,
    }))
}

async fn schedule_text_compaction_task(
    db: &slatedb::Db,
    capabilities: &IndexFamilyCapabilities,
    permits: &WorkerPermits,
    targets: &HashSet<InFlightTarget>,
) -> Result<DispatchAttempt> {
    if targets.contains(&InFlightTarget::TextCompaction) {
        return Ok(DispatchAttempt::Idle);
    }
    let Some(driver) = capabilities.automatic_text_compactor() else {
        return Ok(DispatchAttempt::Idle);
    };
    let prefix = crate::encoding::v2::keys::GlobalKey::logical_prefix(
        crate::encoding::v2::keys::GlobalKind::TextCompactionPointer,
    );
    let mut pointers = db.scan_prefix(prefix, ..).await?;
    if pointers.next().await?.is_none() {
        return Ok(DispatchAttempt::Idle);
    }
    let Some(task_permits) = permits.try_operation(IndexOperationFamily::Text) else {
        return Ok(DispatchAttempt::Idle);
    };
    Ok(DispatchAttempt::Scheduled(Box::new(
        InFlightTask::TextCompaction {
            driver: Arc::clone(driver),
            _permits: task_permits,
        },
    )))
}

struct JoinedInFlightTask {
    target: InFlightTarget,
    result: Result<InFlightCompletion>,
}

fn finish_joined_task(
    joined: std::result::Result<JoinedInFlightTask, JoinError>,
    targets: &mut HashSet<InFlightTarget>,
    #[cfg(feature = "index-lifecycle-testing")] lifecycle_metrics: &Arc<
        crate::index_lifecycle_testing::AutomaticLifecycleMetrics,
    >,
) -> Result<bool> {
    let joined = joined.map_err(|error| {
        #[cfg(feature = "index-lifecycle-testing")]
        lifecycle_metrics.observe_worker_panic();
        HelixDbError::InvariantViolation(format!(
            "index lifecycle task panicked or was cancelled: {error}"
        ))
    })?;
    assert!(
        targets.remove(&joined.target),
        "joined index task retains one registered target"
    );
    let completion = joined.result?;
    assert_eq!(completion.target, joined.target);
    Ok(completion.did_work)
}

async fn drain_in_flight(
    tasks: &mut JoinSet<JoinedInFlightTask>,
    targets: &mut HashSet<InFlightTarget>,
    #[cfg(feature = "index-lifecycle-testing")] lifecycle_metrics: &Arc<
        crate::index_lifecycle_testing::AutomaticLifecycleMetrics,
    >,
) {
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(joined) => {
                targets.remove(&joined.target);
                if let Err(error) = joined.result {
                    tracing::warn!(%error, "peer index lifecycle task failed while draining");
                }
            }
            Err(error) => {
                #[cfg(feature = "index-lifecycle-testing")]
                lifecycle_metrics.observe_worker_panic();
                tracing::warn!(%error, "peer index lifecycle task panicked while draining");
            }
        }
    }
    targets.clear();
}

#[allow(clippy::too_many_arguments)]
async fn schedule_operation_task(
    db: &slatedb::Db,
    capabilities: &IndexFamilyCapabilities,
    permits: &WorkerPermits,
    targets: &HashSet<InFlightTarget>,
    family: IndexOperationFamily,
    scan: &mut OperationScan,
    page_size: OperationQueuePageSize,
    writer_epoch: WriterEpoch,
    same_epoch_proof: Option<SameEpochRecoveryProof>,
    claim_sequences: &ClaimSequenceAllocator,
    shutdown: &watch::Receiver<bool>,
    earliest_deadline: &mut Option<tokio::time::Instant>,
    #[cfg(feature = "index-lifecycle-testing")] lifecycle_metrics: &Arc<
        crate::index_lifecycle_testing::AutomaticLifecycleMetrics,
    >,
) -> Result<DispatchAttempt> {
    let Some((driver, limits)) = capabilities.driver(family) else {
        *scan = OperationScan::default();
        return Ok(DispatchAttempt::Idle);
    };
    let Some(task_permits) = permits.try_operation(family) else {
        return Ok(DispatchAttempt::Idle);
    };
    let driver = Arc::clone(driver);
    let page = outbox::scan_operation_queue_page(db, scan.cursor, page_size).await?;
    for operation_id in page.operation_ids {
        scan.cursor = Some(operation_id);
        if targets.contains(&InFlightTarget::Operation(operation_id)) {
            continue;
        }
        let observed_at = tokio::time::Instant::now();
        let observation =
            outbox::observe_operation_pointer(db, operation_id, writer_epoch, now_unix_millis())
                .await?;
        let (eligible, permission) = match observation {
            OperationPointerObservation::Eligible(eligible) => (eligible, ClaimPermission::Normal),
            OperationPointerObservation::ClaimedByCurrentWriter(eligible) => {
                let Some(proof) = same_epoch_proof else {
                    continue;
                };
                (eligible, ClaimPermission::SameEpochRecovery(proof))
            }
            OperationPointerObservation::Delayed { delay_millis } => {
                let deadline = observed_at + Duration::from_millis(delay_millis);
                *earliest_deadline =
                    Some(earliest_deadline.map_or(deadline, |current| current.min(deadline)));
                continue;
            }
            OperationPointerObservation::StalePointerRemoved => continue,
        };
        if eligible.record.family() != family {
            continue;
        }
        // A parked operation gets no step until its driver wakes the worker
        // or its time passes, whichever is first.
        let Some(until) = outbox::parked(driver.as_ref(), &eligible.record) else {
            scan.wrap_before_idle = true;
            return Ok(DispatchAttempt::Scheduled(Box::new(
                InFlightTask::Operation {
                    target: operation_id,
                    claim: Box::new(PendingClaim {
                        eligible,
                        writer_epoch,
                        sequence: claim_sequences.next()?,
                        permission,
                    }),
                    driver,
                    limits,
                    shutdown: shutdown.clone(),
                    _permits: task_permits,
                    #[cfg(feature = "index-lifecycle-testing")]
                    lifecycle_metrics: Arc::clone(lifecycle_metrics),
                },
            )));
        };
        let deadline = tokio::time::Instant::from_std(until);
        *earliest_deadline =
            Some(earliest_deadline.map_or(deadline, |current| current.min(deadline)));
    }
    if page.prefix_exhausted {
        scan.cursor = None;
        Ok(if std::mem::take(&mut scan.wrap_before_idle) {
            DispatchAttempt::Continue
        } else {
            DispatchAttempt::Idle
        })
    } else {
        scan.cursor = page.resume_after;
        Ok(DispatchAttempt::Continue)
    }
}

fn now_unix_millis() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, DbTransaction};

    use super::*;
    use crate::config::{SearchIndexBackfillLimits, SecondaryIndexDefinition};
    use crate::encoding::v2::keys::scope::DataScope;
    use crate::index_lifecycle::{
        BuildOperationOutcome, IndexGenerationId, IndexId, IndexOperationExecutionState,
        IndexOperationKind, IndexOperationOutcome, IndexOperationProgress, IndexOperationRecord,
        IndexOperationRevision, IndexOperationStatus, IndexRecordV2, IndexRevision,
        NoCursorProgress, OperationCounters, PhysicalGeneration, SecondaryBuildProgress,
        SecondaryBuildStage, ValidatedDynamicIndexDefinition,
    };
    use crate::search::vector::gated_wal::{GatedWalStore, WalUploads};

    /// A driver for a task whose claim is lost before any step.
    struct NeverSteps;

    #[async_trait]
    impl IndexOperationDriver for NeverSteps {
        fn family(&self) -> IndexOperationFamily {
            IndexOperationFamily::Secondary
        }

        async fn step(
            &self,
            _db: &Db,
            _transaction: &DbTransaction,
            _scope: DataScope,
            _operation: &IndexOperationRecord,
            _limits: SearchIndexBatchLimits,
        ) -> Result<outbox::IndexOperationStepExecution> {
            unreachable!("a lost claim never steps")
        }
    }

    fn fixture(ordinal: u8) -> (IndexRecordV2, IndexOperationRecord) {
        let definition = ValidatedDynamicIndexDefinition::try_from(
            SecondaryIndexDefinition::node_equality(format!("User{ordinal}"), "email").unwrap(),
        )
        .unwrap();
        let operation_id = IndexOperationId::from_bytes([ordinal; 16]).unwrap();
        let index = IndexRecordV2::building(
            IndexId::new(u64::from(ordinal)).unwrap(),
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
            IndexOperationKind::Build,
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
        (index, operation)
    }

    /// A task whose claim finds the operation already moved commits nothing
    /// and still reports work, so the dispatcher scans the queue again
    /// instead of counting it as an idle lane and sleeping for `IDLE_DELAY`.
    #[tokio::test]
    async fn a_lost_claim_counts_as_work() {
        let db = Arc::new(
            Db::builder("worker-lost-claim", Arc::new(InMemory::new()))
                .build()
                .await
                .unwrap(),
        );
        let (index, operation) = fixture(7);
        let operation_id = operation.operation_id();
        outbox::enqueue_operation(
            &db,
            DataScope::LegacyUnscoped,
            outbox::ExpectedCanonicalRevision::Absent,
            &index,
            &operation,
        )
        .await
        .unwrap();
        let epoch = WriterEpoch::from_bytes([4; 16]).unwrap();
        let OperationPointerObservation::Eligible(eligible) =
            outbox::observe_operation_pointer(&db, operation_id, epoch, 0)
                .await
                .unwrap()
        else {
            panic!("a queued operation is eligible");
        };
        // Another claim moves the operation after the dispatcher observed it.
        outbox::claim_operation(
            &db,
            &eligible,
            epoch,
            ClaimSequence::new(1).unwrap(),
            0,
            ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .expect("the first claim wins");
        let permits = WorkerPermits::new(IndexLifecycleConcurrency::default());
        let task = InFlightTask::Operation {
            target: operation_id,
            claim: Box::new(PendingClaim {
                eligible,
                writer_epoch: epoch,
                sequence: ClaimSequence::new(2).unwrap(),
                permission: ClaimPermission::Normal,
            }),
            driver: Arc::new(NeverSteps),
            limits: SearchIndexBackfillLimits::default().batch(),
            shutdown: watch::channel(false).1,
            _permits: permits
                .try_operation(IndexOperationFamily::Secondary)
                .expect("a secondary slot is free"),
            #[cfg(feature = "index-lifecycle-testing")]
            lifecycle_metrics: Arc::new(
                crate::index_lifecycle_testing::AutomaticLifecycleMetrics::new(),
            ),
        };
        let completion = task.execute(db).await.unwrap();
        assert!(completion.did_work);
    }

    #[tokio::test]
    async fn delayed_pages_finish_a_sweep() {
        for count in [64, 65] {
            let db = Db::builder("worker-delayed-pages", Arc::new(InMemory::new()))
                .with_settings(slatedb::Settings {
                    flush_interval: Some(Duration::from_millis(1)),
                    ..Default::default()
                })
                .build()
                .await
                .unwrap();
            let epoch = WriterEpoch::new_v4();
            for ordinal in 1..=count {
                let (index, operation) = fixture(ordinal);
                let delayed = operation
                    .claim(super::super::OperationClaim {
                        writer_epoch: epoch,
                        sequence: ClaimSequence::new(u64::from(ordinal)).unwrap(),
                    })
                    .unwrap()
                    .transient_failure(now_unix_millis() + 30_000)
                    .unwrap();
                let transaction = db
                    .begin(slatedb::IsolationLevel::SerializableSnapshot)
                    .await
                    .unwrap();
                use crate::encoding::v2::values;
                let scope = DataScope::LegacyUnscoped;
                transaction
                    .put(
                        outbox::scoped_index_key_for_identity(scope, index.identity()),
                        values::encode_index_record(&index),
                    )
                    .unwrap();
                transaction
                    .put(
                        outbox::scoped_operation_key(scope, delayed.operation_id()),
                        values::encode_operation_record(&delayed),
                    )
                    .unwrap();
                transaction
                    .put(
                        outbox::global_operation_key(delayed.operation_id()),
                        values::encode_metadata_value(
                            &super::super::IndexV2MetadataValue::OperationQueuePointer(
                                outbox::pointer_for(scope, &delayed),
                            ),
                        ),
                    )
                    .unwrap();
                transaction.commit().await.unwrap();
            }
            let capabilities = test_capabilities();
            let permits = WorkerPermits::new(IndexLifecycleConcurrency::default());
            let mut cursor = Default::default();
            let mut delay = None;
            for attempt in 0..2 {
                let result = schedule_operation_task(
                    &db,
                    &capabilities,
                    &permits,
                    &HashSet::new(),
                    IndexOperationFamily::Secondary,
                    &mut cursor,
                    OperationQueuePageSize::new(64).unwrap(),
                    epoch,
                    None,
                    &ClaimSequenceAllocator::new(),
                    &watch::channel(false).1,
                    &mut delay,
                    #[cfg(feature = "index-lifecycle-testing")]
                    &Arc::new(crate::index_lifecycle_testing::AutomaticLifecycleMetrics::new()),
                )
                .await
                .unwrap();
                assert!(
                    if attempt == 0 {
                        matches!(result, DispatchAttempt::Continue)
                    } else {
                        matches!(result, DispatchAttempt::Idle)
                    },
                    "a delayed full page continues, but its exhausted suffix is idle"
                );
            }
            assert!(delay.is_some_and(|deadline| {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                !remaining.is_zero() && remaining <= Duration::from_secs(30)
            }));
            db.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn a_partial_sweep_wraps_to_ready_work_before_idling() {
        let db = Db::open("worker-wrap", Arc::new(InMemory::new()))
            .await
            .unwrap();
        let epoch = WriterEpoch::new_v4();
        let capabilities = test_capabilities();
        let permits = WorkerPermits::new(IndexLifecycleConcurrency::default());
        let sequences = ClaimSequenceAllocator::new();
        let mut cursor = Default::default();
        let mut delay = None;
        let mut targets = HashSet::new();
        // Scheduling the later key leaves a partial scan. A new key can then
        // arrive behind it, and must be found before this lane reports idle.
        for ordinal in [100, 1] {
            let (index, operation) = fixture(ordinal);
            outbox::enqueue_operation(
                &db,
                DataScope::LegacyUnscoped,
                outbox::ExpectedCanonicalRevision::Absent,
                &index,
                &operation,
            )
            .await
            .unwrap();
            if ordinal == 1 {
                let result = schedule_operation_task(
                    &db,
                    &capabilities,
                    &permits,
                    &targets,
                    IndexOperationFamily::Secondary,
                    &mut cursor,
                    OperationQueuePageSize::new(64).unwrap(),
                    epoch,
                    None,
                    &sequences,
                    &watch::channel(false).1,
                    &mut delay,
                    #[cfg(feature = "index-lifecycle-testing")]
                    &Arc::new(crate::index_lifecycle_testing::AutomaticLifecycleMetrics::new()),
                )
                .await
                .unwrap();
                assert!(matches!(result, DispatchAttempt::Continue));
            }
            let result = schedule_operation_task(
                &db,
                &capabilities,
                &permits,
                &targets,
                IndexOperationFamily::Secondary,
                &mut cursor,
                OperationQueuePageSize::new(64).unwrap(),
                epoch,
                None,
                &sequences,
                &watch::channel(false).1,
                &mut delay,
                #[cfg(feature = "index-lifecycle-testing")]
                &Arc::new(crate::index_lifecycle_testing::AutomaticLifecycleMetrics::new()),
            )
            .await
            .unwrap();
            let DispatchAttempt::Scheduled(task) = result else {
                panic!("ready work behind the cursor must be scheduled before idling");
            };
            assert_eq!(
                task.target(),
                InFlightTarget::Operation(operation.operation_id())
            );
            targets.insert(task.target());
        }
        db.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn scan_time_does_not_restart_a_retry_delay() {
        for elapsed in [1, 4] {
            for in_flight in [false, true] {
                let (_shutdown, mut shutdown) = watch::channel(false);
                let mut tasks = JoinSet::new();
                if in_flight {
                    tasks.spawn(std::future::pending());
                }
                let mut targets = HashSet::new();
                let wake = Notify::new();
                let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
                let mut delay = Some(deadline);
                tokio::time::advance(Duration::from_secs(elapsed)).await;
                let before_wait = tokio::time::Instant::now();
                assert!(!wait_for_work(
                    &mut shutdown,
                    &mut tasks,
                    &mut targets,
                    &wake,
                    &mut delay,
                    false,
                    Duration::from_secs(1),
                    #[cfg(feature = "index-lifecycle-testing")]
                    &Arc::new(crate::index_lifecycle_testing::AutomaticLifecycleMetrics::new()),
                )
                .await
                .unwrap());
                assert_eq!(tokio::time::Instant::now(), deadline.max(before_wait));
                assert!(delay.is_none());
                tasks.shutdown().await;
            }
        }
    }

    fn test_capabilities() -> IndexFamilyCapabilities {
        let capability = IndexFamilyCapability::new(
            Arc::new(NeverSteps),
            SearchIndexBackfillLimits::default().batch(),
            IndexDriverScheduling::Automatic,
        );
        struct NoCompaction;
        #[async_trait]
        impl ActiveTextCompactionDriver for NoCompaction {
            async fn compact_active_text_once(&self, _db: &Db) -> Result<bool> {
                Ok(false)
            }
        }
        IndexFamilyCapabilities::new(
            capability.clone(),
            capability.clone(),
            capability,
            Arc::new(NoCompaction),
        )
    }

    /// Progresses one entity per step, sleeping `delay` first, and completes
    /// once `completes_at` entities are counted.
    struct CountingDriver {
        completes_at: u64,
        delay: Duration,
    }

    #[async_trait]
    impl IndexOperationDriver for CountingDriver {
        fn family(&self) -> IndexOperationFamily {
            IndexOperationFamily::Secondary
        }

        async fn step(
            &self,
            _db: &Db,
            _transaction: &DbTransaction,
            _scope: DataScope,
            operation: &IndexOperationRecord,
            _limits: SearchIndexBatchLimits,
        ) -> Result<outbox::IndexOperationStepExecution> {
            let entities = IndexOperationStatus::from_record(operation)
                .common()
                .progress
                .entities;
            if entities >= self.completes_at {
                return Ok(outbox::IndexOperationStepExecution::new(
                    outbox::IndexOperationStepResult::Completed(IndexOperationOutcome::Build(
                        BuildOperationOutcome::Succeeded,
                    )),
                ));
            }
            tokio::time::sleep(self.delay).await;
            Ok(outbox::IndexOperationStepExecution::new(
                outbox::IndexOperationStepResult::Progressed(activate_progress(entities + 1)),
            ))
        }
    }

    fn activate_progress(entities: u64) -> IndexOperationProgress {
        IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
            SecondaryBuildStage::Activate(NoCursorProgress {
                counters: OperationCounters {
                    entities,
                    ..OperationCounters::default()
                },
            }),
        ))
    }

    /// Enqueues one secondary build at activation with no counted entities.
    async fn enqueue(db: &Db, id_byte: u8) -> IndexOperationRecord {
        enqueue_index(db, id_byte, IndexId::initial(), "email").await
    }

    /// [`enqueue`] for the index `index_id` over `property`, so one database
    /// holds several builds.
    async fn enqueue_index(
        db: &Db,
        id_byte: u8,
        index_id: IndexId,
        property: &str,
    ) -> IndexOperationRecord {
        let definition = ValidatedDynamicIndexDefinition::try_from(
            SecondaryIndexDefinition::node_equality("User", property).unwrap(),
        )
        .unwrap();
        let operation_id = IndexOperationId::from_bytes([id_byte; 16]).unwrap();
        let index = IndexRecordV2::building(
            index_id,
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
            IndexOperationKind::Build,
            IndexOperationFamily::Secondary,
            activate_progress(0),
            0,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .unwrap();
        outbox::enqueue_operation(
            db,
            DataScope::LegacyUnscoped,
            outbox::ExpectedCanonicalRevision::Absent,
            &index,
            &operation,
        )
        .await
        .unwrap();
        operation
    }

    /// Runs one operation task for `operation` to its end and returns the
    /// operation's record afterwards.
    async fn run_task(
        db: &Arc<Db>,
        operation: &IndexOperationRecord,
        driver: impl IndexOperationDriver + 'static,
        shutdown: watch::Receiver<bool>,
    ) -> IndexOperationRecord {
        let epoch = WriterEpoch::from_bytes([5; 16]).unwrap();
        let OperationPointerObservation::Eligible(eligible) =
            outbox::observe_operation_pointer(db, operation.operation_id(), epoch, 0)
                .await
                .unwrap()
        else {
            panic!("a queued operation is eligible");
        };
        let permits = WorkerPermits::new(IndexLifecycleConcurrency::default());
        let task = InFlightTask::Operation {
            target: operation.operation_id(),
            claim: Box::new(PendingClaim {
                eligible,
                writer_epoch: epoch,
                sequence: ClaimSequence::new(1).unwrap(),
                permission: ClaimPermission::Normal,
            }),
            driver: Arc::new(driver),
            limits: SearchIndexBackfillLimits::default().batch(),
            shutdown,
            _permits: permits
                .try_operation(IndexOperationFamily::Secondary)
                .expect("a secondary slot is free"),
            #[cfg(feature = "index-lifecycle-testing")]
            lifecycle_metrics: Arc::new(
                crate::index_lifecycle_testing::AutomaticLifecycleMetrics::new(),
            ),
        };
        let completion = task.execute(Arc::clone(db)).await.unwrap();
        assert!(completion.did_work);
        outbox::read_operation(
            db.as_ref(),
            DataScope::LegacyUnscoped,
            operation.operation_id(),
        )
        .await
        .unwrap()
        .expect("the operation is retained")
    }

    /// Progresses like [`CountingDriver`], and once `superseded_at` entities
    /// are counted holds WAL uploads and advances the operation's revision
    /// without waiting for its upload, as a concurrent abort or claim does,
    /// so the slice's next step finds its claim superseded.
    struct SupersedingDriver {
        counting: CountingDriver,
        superseded_at: u64,
        db: Arc<Db>,
        store: Arc<GatedWalStore>,
    }

    #[async_trait]
    impl IndexOperationDriver for SupersedingDriver {
        fn family(&self) -> IndexOperationFamily {
            IndexOperationFamily::Secondary
        }

        async fn step(
            &self,
            db: &Db,
            transaction: &DbTransaction,
            scope: DataScope,
            operation: &IndexOperationRecord,
            limits: SearchIndexBatchLimits,
        ) -> Result<outbox::IndexOperationStepExecution> {
            self.counting
                .step(db, transaction, scope, operation, limits)
                .await
        }

        async fn after_commit(
            &self,
            scope: DataScope,
            _index: &IndexRecordV2,
            operation: &IndexOperationRecord,
            committed: outbox::CommittedOperationStep,
            _state: Option<outbox::CommittedStepState>,
        ) {
            let entities = IndexOperationStatus::from_record(operation)
                .common()
                .progress
                .entities
                + 1;
            if committed != outbox::CommittedOperationStep::Progressed
                || entities != self.superseded_at
            {
                return;
            }
            self.store.uploads.send_replace(WalUploads::Held);
            let current = outbox::read_operation(self.db.as_ref(), scope, operation.operation_id())
                .await
                .unwrap()
                .expect("the operation is retained");
            let superseding = current
                .progressed(activate_progress(entities), ClaimContinuation::Retain)
                .unwrap();
            // The record and its queue pointer, as the superseding write
            // leaves them.
            let mut batch = slatedb::WriteBatch::new();
            batch.put(
                outbox::scoped_operation_key(scope, operation.operation_id()),
                crate::encoding::v2::values::encode_operation_record(&superseding),
            );
            batch.put(
                outbox::global_operation_key(operation.operation_id()),
                crate::encoding::v2::values::encode_metadata_value(
                    &crate::index_lifecycle::IndexV2MetadataValue::OperationQueuePointer(
                        outbox::pointer_for(scope, &superseding),
                    ),
                ),
            );
            self.db
                .write_with_options(
                    batch,
                    &slatedb::config::WriteOptions {
                        await_durable: false,
                        ..slatedb::config::WriteOptions::default()
                    },
                )
                .await
                .unwrap();
        }
    }

    /// A slice whose retained step finds its claim superseded commits
    /// nothing more, yet still ends by uploading the steps it progressed:
    /// the task waits for that upload instead of letting the build run more
    /// than one slice ahead of it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_superseded_slice_still_uploads_its_progressed_steps() {
        let store = Arc::new(GatedWalStore::new());
        let db = Arc::new(
            Db::builder(
                "worker-superseded-slice",
                Arc::clone(&store) as Arc<dyn slatedb::object_store::ObjectStore>,
            )
            .build()
            .await
            .unwrap(),
        );
        let operation = enqueue(&db, 12).await;
        let task = tokio::spawn({
            let db = Arc::clone(&db);
            let driver = SupersedingDriver {
                counting: CountingDriver {
                    completes_at: 1_000,
                    delay: Duration::ZERO,
                },
                superseded_at: 5,
                db: Arc::clone(&db),
                store: Arc::clone(&store),
            };
            async move { run_task(&db, &operation, driver, watch::channel(false).1).await }
        });
        tokio::time::timeout(
            Duration::from_secs(10),
            store
                .uploads
                .subscribe()
                .wait_for(|uploads| *uploads == WalUploads::Held),
        )
        .await
        .expect("the build reaches the superseding step")
        .expect("the gate outlives its store");
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            !task.is_finished(),
            "the superseded slice waits for the upload of its progressed steps"
        );
        store.uploads.send_replace(WalUploads::Open);
        let superseded = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("the slice ends once its upload lands")
            .unwrap();
        assert_eq!(
            IndexOperationStatus::from_record(&superseded)
                .common()
                .progress
                .entities,
            5,
            "no step ran after the claim was superseded"
        );
    }

    /// One task runs every step of a short build under its single claim:
    /// the claim attempt counts one claim, not one per step.
    #[tokio::test]
    async fn a_task_runs_consecutive_steps_under_one_claim() {
        let db = Arc::new(
            Db::builder("worker-claim-slice", Arc::new(InMemory::new()))
                .build()
                .await
                .unwrap(),
        );
        let operation = enqueue(&db, 8).await;
        let finished = run_task(
            &db,
            &operation,
            CountingDriver {
                completes_at: 50,
                delay: Duration::ZERO,
            },
            watch::channel(false).1,
        )
        .await;
        assert!(matches!(
            finished.execution_state(),
            IndexOperationExecutionState::Completed(IndexOperationOutcome::Build(
                BuildOperationOutcome::Succeeded
            ))
        ));
        assert_eq!(finished.attempt(), 1, "fifty steps ran under one claim");
        // Fifty progressed steps and the completion, plus the claim.
        assert_eq!(finished.operation_revision().get(), 1 + 1 + 50 + 1);
    }

    /// The public status reports a build as running from its claim until
    /// it completes: the steps of one slice keep the claim, so the status
    /// never falls back to queued between them.
    #[tokio::test]
    async fn status_stays_running_across_the_steps_of_a_slice() {
        let db = Arc::new(
            Db::builder("worker-claim-slice-status", Arc::new(InMemory::new()))
                .build()
                .await
                .unwrap(),
        );
        let operation = enqueue(&db, 11).await;
        let operation_id = operation.operation_id();
        let poller = tokio::spawn({
            let db = Arc::clone(&db);
            async move {
                let mut observed = Vec::new();
                loop {
                    // What `get_index_operation` projects for a writer.
                    let status = IndexOperationStatus::from_record(
                        &outbox::read_operation(
                            db.as_ref(),
                            DataScope::LegacyUnscoped,
                            operation_id,
                        )
                        .await
                        .unwrap()
                        .expect("the operation is retained"),
                    );
                    assert!(
                        !matches!(&status, IndexOperationStatus::Running { common } if common.attempt != 1),
                        "a running build reports one attempt: {status:?}"
                    );
                    let terminal = matches!(status, IndexOperationStatus::Succeeded { .. });
                    if observed.last().is_none_or(|last| {
                        std::mem::discriminant(last) != std::mem::discriminant(&status)
                    }) {
                        observed.push(status);
                    }
                    if terminal {
                        return observed;
                    }
                    tokio::task::yield_now().await;
                }
            }
        });
        let finished = run_task(
            &db,
            &operation,
            CountingDriver {
                completes_at: 40,
                delay: Duration::from_millis(2),
            },
            watch::channel(false).1,
        )
        .await;
        let observed = poller.await.unwrap();
        let kinds = observed
            .iter()
            .map(|status| match status {
                IndexOperationStatus::Queued { .. } => "queued",
                IndexOperationStatus::Running { .. } => "running",
                IndexOperationStatus::Succeeded { .. } => "succeeded",
                IndexOperationStatus::Blocked { .. } | IndexOperationStatus::Aborted { .. } => {
                    "other"
                }
            })
            .collect::<Vec<_>>();
        assert!(
            matches!(
                kinds.as_slice(),
                ["running", "succeeded"] | ["queued", "running", "succeeded"]
            ),
            "the status went {kinds:?}"
        );
        assert_eq!(finished.attempt(), 1);
    }

    /// A slice ends by releasing its claim, so the dispatcher can schedule
    /// other queued work; shutdown ends it after one step.
    #[tokio::test]
    async fn a_claim_slice_releases_after_its_time_or_on_shutdown() {
        let db = Arc::new(
            Db::builder("worker-claim-slice-release", Arc::new(InMemory::new()))
                .build()
                .await
                .unwrap(),
        );
        let slow = enqueue(&db, 9).await;
        let started = std::time::Instant::now();
        let released = run_task(
            &db,
            &slow,
            CountingDriver {
                completes_at: 1_000,
                delay: CLAIM_SLICE / 3,
            },
            watch::channel(false).1,
        )
        .await;
        assert!(started.elapsed() >= CLAIM_SLICE);
        assert!(matches!(
            released.execution_state(),
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None
            }
        ));
        let entities = IndexOperationStatus::from_record(&released)
            .common()
            .progress
            .entities;
        assert!(
            (2..1_000).contains(&entities),
            "several steps ran under the claim before the slice released it: {entities}"
        );
        assert_eq!(released.attempt(), 0, "progress resets the attempts");

        // One index per database: the fixture always uses the first index ID.
        let db = Arc::new(
            Db::builder("worker-claim-slice-stop", Arc::new(InMemory::new()))
                .build()
                .await
                .unwrap(),
        );
        let stopping = enqueue(&db, 10).await;
        let (stop, shutdown) = watch::channel(false);
        stop.send_replace(true);
        let stopped = run_task(
            &db,
            &stopping,
            CountingDriver {
                completes_at: 1_000,
                delay: Duration::ZERO,
            },
            shutdown,
        )
        .await;
        assert!(matches!(
            stopped.execution_state(),
            IndexOperationExecutionState::Queued { .. }
        ));
        assert_eq!(
            IndexOperationStatus::from_record(&stopped)
                .common()
                .progress
                .entities,
            1,
            "a stopping worker releases its claim with its first step"
        );
    }

    /// Counts a step for each operation it progresses, and parks the
    /// operations a test names: [`CountingDriver`] with
    /// [`IndexOperationDriver::parked_until`].
    #[derive(Default)]
    struct ParkingDriver {
        /// Operations a step parks, until when.
        parks: parking_lot::Mutex<std::collections::HashMap<IndexOperationId, std::time::Instant>>,
        /// Operations whose steps found them parked.
        parked: parking_lot::Mutex<std::collections::HashMap<IndexOperationId, std::time::Instant>>,
        steps: parking_lot::Mutex<std::collections::HashMap<IndexOperationId, u64>>,
    }

    impl ParkingDriver {
        fn steps(&self, operation: &IndexOperationRecord) -> u64 {
            self.steps
                .lock()
                .get(&operation.operation_id())
                .copied()
                .unwrap_or(0)
        }
    }

    #[async_trait]
    impl IndexOperationDriver for Arc<ParkingDriver> {
        fn family(&self) -> IndexOperationFamily {
            IndexOperationFamily::Secondary
        }

        async fn step(
            &self,
            _db: &Db,
            _transaction: &DbTransaction,
            _scope: DataScope,
            operation: &IndexOperationRecord,
            _limits: SearchIndexBatchLimits,
        ) -> Result<outbox::IndexOperationStepExecution> {
            let operation_id = operation.operation_id();
            *self.steps.lock().entry(operation_id).or_default() += 1;
            // A step that finds its operation waiting parks it, as a build
            // that waits does, and commits the progress as it is.
            let park = self.parks.lock().get(&operation_id).copied();
            let entities = IndexOperationStatus::from_record(operation)
                .common()
                .progress
                .entities;
            let Some(until) = park else {
                return Ok(outbox::IndexOperationStepExecution::new(
                    outbox::IndexOperationStepResult::Progressed(activate_progress(entities + 1)),
                ));
            };
            self.parked.lock().insert(operation_id, until);
            Ok(outbox::IndexOperationStepExecution::new(
                outbox::IndexOperationStepResult::Progressed(activate_progress(entities)),
            ))
        }

        fn parked_until(&self, operation: &IndexOperationRecord) -> Option<std::time::Instant> {
            self.parked.lock().get(&operation.operation_id()).copied()
        }
    }

    /// The step that parks its operation releases the claim with its own
    /// commit: a slice that would run for its whole second ends after it.
    #[tokio::test]
    async fn a_parked_operation_ends_its_claim_slice() {
        let db = Arc::new(
            Db::builder("worker-parked-slice", Arc::new(InMemory::new()))
                .build()
                .await
                .unwrap(),
        );
        let waiting = enqueue(&db, 12).await;
        let driver = Arc::new(ParkingDriver::default());
        driver.parks.lock().insert(
            waiting.operation_id(),
            std::time::Instant::now() + Duration::from_secs(3_600),
        );
        let released = run_task(&db, &waiting, Arc::clone(&driver), watch::channel(false).1).await;
        assert_eq!(
            driver.steps(&waiting),
            1,
            "the step that parked the operation released its claim"
        );
        assert!(matches!(
            released.execution_state(),
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None
            }
        ));
        assert_eq!(released.progress(), waiting.progress());
    }

    /// A parked operation takes no step, and so none of its lane's time,
    /// while another operation of the lane runs: the running one is given
    /// every slice. The driver's wake gives the parked one steps again, and
    /// so does its time passing without one.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_parked_operation_leaves_its_lane_to_the_others() {
        let db = Arc::new(
            Db::builder("worker-parked-lane", Arc::new(InMemory::new()))
                .build()
                .await
                .unwrap(),
        );
        let running = enqueue_index(&db, 13, IndexId::initial(), "email").await;
        let waiting = enqueue_index(&db, 14, IndexId::new(2).unwrap(), "name").await;
        let driver = Arc::new(ParkingDriver::default());
        let far = std::time::Instant::now() + Duration::from_secs(3_600);
        driver.parks.lock().insert(waiting.operation_id(), far);
        let capability = IndexFamilyCapability::new(
            Arc::new(Arc::clone(&driver)),
            SearchIndexBackfillLimits::default().batch(),
            IndexDriverScheduling::Automatic,
        );
        struct NoCompaction;
        #[async_trait]
        impl ActiveTextCompactionDriver for NoCompaction {
            async fn compact_active_text_once(&self, _db: &Db) -> Result<bool> {
                Ok(false)
            }
        }
        let wake = IndexWorkerWakeHandle::default();
        // One task at a time in the lane, as the vector lane has.
        let supervisor = IndexWorkerSupervisor::start(
            Arc::clone(&db),
            IndexFamilyCapabilities::new(
                capability.clone(),
                capability.clone(),
                capability,
                Arc::new(NoCompaction),
            ),
            None,
            Duration::from_secs(3_600),
            IndexLifecycleConcurrency::try_new(1, 1, 1, 1).unwrap(),
            Arc::new(ClaimSequenceAllocator::new()),
            wake.clone(),
            #[cfg(feature = "index-lifecycle-testing")]
            Arc::new(crate::index_lifecycle_testing::AutomaticLifecycleMetrics::new()),
        );
        let until = async |reached: &dyn Fn() -> bool| {
            tokio::time::timeout(Duration::from_secs(60), async {
                while !reached() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("the worker runs its operations");
        };
        // The waiting operation parks in its first step, which ends its
        // slice.
        until(&|| driver.steps(&waiting) == 1 && driver.steps(&running) >= 1).await;
        // Hundreds of steps of the running one later it has taken no other,
        // nor when the running one's claim slices end and the dispatcher
        // gives the lane out again: it passes over the parked one.
        let before = driver.steps(&running);
        until(&|| driver.steps(&running) >= before + 500).await;
        tokio::time::sleep(2 * CLAIM_SLICE + CLAIM_SLICE / 2).await;
        assert_eq!(driver.steps(&waiting), 1);

        // What it waited for is there: the driver unparks it and wakes the
        // worker.
        driver.parks.lock().clear();
        driver.parked.lock().clear();
        wake.wake();
        until(&|| driver.steps(&waiting) >= 10).await;

        // Parked again, until a moment from now and with no wake: its time
        // passing gives it the next step.
        let soon = std::time::Instant::now() + Duration::from_millis(300);
        driver.parks.lock().insert(waiting.operation_id(), far);
        until(&|| driver.parked.lock().contains_key(&waiting.operation_id())).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let parked_at = driver.steps(&waiting);
        let before = driver.steps(&running);
        until(&|| driver.steps(&running) >= before + 100).await;
        assert_eq!(driver.steps(&waiting), parked_at);
        driver.parks.lock().clear();
        driver.parked.lock().insert(waiting.operation_id(), soon);
        until(&|| driver.steps(&waiting) > parked_at).await;
        assert!(std::time::Instant::now() >= soon);
        supervisor.stop().await;
    }
}
