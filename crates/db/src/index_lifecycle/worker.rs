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
use super::{ClaimSequence, IndexOperationFamily, IndexOperationId, WriterEpoch};

const DEFAULT_OPERATION_PAGE_SIZE: usize = 64;
const SUPERVISOR_RESTART_DELAY: Duration = Duration::from_millis(10);
const IDLE_DELAY: Duration = Duration::from_secs(24 * 60 * 60);
/// Operation-family lanes plus the queued-publication lane.
const DISPATCH_LANES: usize = OPERATION_FAMILIES.len() + 1;
const IDLE_DISPATCH_ATTEMPTS: usize = DISPATCH_LANES;

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

/// Typed work retained by the task set after its durable claim succeeds.
enum InFlightTask {
    Operation {
        target: IndexOperationId,
        claimed: Box<outbox::ClaimedOperation>,
        driver: Arc<dyn IndexOperationDriver>,
        limits: SearchIndexBatchLimits,
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
        let (delay_millis, did_work) = match self {
            Self::Operation {
                claimed,
                driver,
                limits,
                _permits,
                #[cfg(feature = "index-lifecycle-testing")]
                lifecycle_metrics,
                ..
            } => {
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
                #[cfg(feature = "index-lifecycle-testing")]
                lifecycle_metrics.observe_operation(
                    outbox::execute_claimed_step_with_evidence(
                        db.as_ref(),
                        claimed.as_ref(),
                        driver.as_ref(),
                        limits,
                        now_unix_millis(),
                    )
                    .await?,
                );
                #[cfg(not(feature = "index-lifecycle-testing"))]
                outbox::execute_claimed_step(
                    db.as_ref(),
                    claimed.as_ref(),
                    driver.as_ref(),
                    limits,
                    now_unix_millis(),
                )
                .await?;
                (None, true)
            }
            Self::TextCompaction { driver, _permits } => {
                let did_work = driver.compact_active_text_once(db.as_ref()).await?;
                (None, did_work)
            }
            Self::Publication {
                target,
                publisher,
                _permits,
            } => {
                let outcome = publisher.publish_once(target).await?;
                (
                    None,
                    matches!(
                        outcome,
                        PublicationOutcome::Published { .. }
                            | PublicationOutcome::Discarded { .. }
                            | PublicationOutcome::Trimmed
                            | PublicationOutcome::Blocked
                    ),
                )
            }
        };
        Ok(InFlightCompletion {
            target,
            delay_millis,
            did_work,
        })
    }
}

struct InFlightCompletion {
    target: InFlightTarget,
    delay_millis: Option<u64>,
    did_work: bool,
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
    let mut operation_cursors = [None; OPERATION_FAMILIES.len()];
    let mut operation_family_index = 0_usize;
    let mut lanes_without_work = 0_usize;
    let mut earliest_delay = None::<u64>;

    loop {
        while let Some(joined) = tasks.try_join_next() {
            if finish_joined_task(
                joined,
                targets,
                &mut earliest_delay,
                #[cfg(feature = "index-lifecycle-testing")]
                lifecycle_metrics,
            )? {
                lanes_without_work = 0;
                earliest_delay = None;
            } else {
                lanes_without_work = lanes_without_work.saturating_add(1);
            }
        }
        if *shutdown.borrow() {
            return Ok(WorkerCycleExit::Shutdown);
        }
        let lane = operation_family_index;
        operation_family_index = (operation_family_index + 1) % DISPATCH_LANES;
        let Some(&family) = OPERATION_FAMILIES.get(lane) else {
            let attempt =
                schedule_publication_task(publisher, &permits, targets, &mut earliest_delay);
            match attempt {
                DispatchAttempt::Scheduled(task) => {
                    lanes_without_work = 0;
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
                }
                DispatchAttempt::Continue => lanes_without_work = 0,
                DispatchAttempt::Idle => {
                    lanes_without_work = lanes_without_work.saturating_add(1);
                }
            }
            if lanes_without_work >= IDLE_DISPATCH_ATTEMPTS {
                lanes_without_work = 0;
                if wait_for_work(
                    shutdown,
                    tasks,
                    targets,
                    wake,
                    &mut earliest_delay,
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
            continue;
        };
        let cursor = &mut operation_cursors[lane];
        let mut attempt = schedule_operation_task(
            db,
            capabilities,
            &permits,
            targets,
            family,
            cursor,
            page_size,
            writer_epoch,
            same_epoch_proof,
            claim_sequences,
            &mut earliest_delay,
            #[cfg(feature = "index-lifecycle-testing")]
            lifecycle_metrics,
        )
        .await?;
        if family == IndexOperationFamily::Text && matches!(attempt, DispatchAttempt::Idle) {
            attempt = schedule_text_compaction_task(db, capabilities, &permits, targets).await?;
        }

        match attempt {
            DispatchAttempt::Scheduled(task) => {
                lanes_without_work = 0;
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
            DispatchAttempt::Continue => {
                lanes_without_work = 0;
                continue;
            }
            DispatchAttempt::Idle => {
                lanes_without_work = lanes_without_work.saturating_add(1);
            }
        }

        if lanes_without_work < IDLE_DISPATCH_ATTEMPTS {
            continue;
        }
        lanes_without_work = 0;
        if wait_for_work(
            shutdown,
            tasks,
            targets,
            wake,
            &mut earliest_delay,
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
    earliest_delay: &mut Option<u64>,
    queued_work: bool,
    sweep_interval: Duration,
    #[cfg(feature = "index-lifecycle-testing")] lifecycle_metrics: &Arc<
        crate::index_lifecycle_testing::AutomaticLifecycleMetrics,
    >,
) -> Result<bool> {
    let mut delay = earliest_delay
        .take()
        .map(Duration::from_millis)
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
                    earliest_delay,
                    #[cfg(feature = "index-lifecycle-testing")]
                    lifecycle_metrics,
                )? {
                    *earliest_delay = None;
                }
            }
            () = wake.notified() => {}
            () = tokio::time::sleep(delay) => {}
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
        () = tokio::time::sleep(delay) => {}
    }
    Ok(false)
}

fn schedule_publication_task(
    publisher: Option<&Arc<QueuePublisher>>,
    permits: &WorkerPermits,
    targets: &HashSet<InFlightTarget>,
    earliest_delay: &mut Option<u64>,
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
            let delay_millis = u64::try_from(not_before.saturating_duration_since(now).as_millis())
                .unwrap_or(u64::MAX)
                .max(1);
            *earliest_delay =
                Some(earliest_delay.map_or(delay_millis, |current| current.min(delay_millis)));
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
    earliest_delay: &mut Option<u64>,
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
    if let Some(delay_millis) = completion.delay_millis {
        *earliest_delay =
            Some(earliest_delay.map_or(delay_millis, |current| current.min(delay_millis)));
    }
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
    cursor: &mut Option<IndexOperationId>,
    page_size: OperationQueuePageSize,
    writer_epoch: WriterEpoch,
    same_epoch_proof: Option<SameEpochRecoveryProof>,
    claim_sequences: &ClaimSequenceAllocator,
    earliest_delay: &mut Option<u64>,
    #[cfg(feature = "index-lifecycle-testing")] lifecycle_metrics: &Arc<
        crate::index_lifecycle_testing::AutomaticLifecycleMetrics,
    >,
) -> Result<DispatchAttempt> {
    let wrapping = cursor.is_some();
    let Some((driver, limits)) = capabilities.driver(family) else {
        *cursor = None;
        return Ok(DispatchAttempt::Idle);
    };
    let Some(task_permits) = permits.try_operation(family) else {
        return Ok(DispatchAttempt::Idle);
    };
    let driver = Arc::clone(driver);
    let page = outbox::scan_operation_queue_page(db, *cursor, page_size).await?;
    for operation_id in page.operation_ids {
        *cursor = Some(operation_id);
        if targets.contains(&InFlightTarget::Operation(operation_id)) {
            continue;
        }
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
                *earliest_delay =
                    Some(earliest_delay.map_or(delay_millis, |current| current.min(delay_millis)));
                continue;
            }
            OperationPointerObservation::StalePointerRemoved => continue,
        };
        if eligible.record.family() != family {
            continue;
        }
        let claimed = outbox::claim_operation(
            db,
            &eligible,
            writer_epoch,
            claim_sequences.next()?,
            now_unix_millis(),
            permission,
        )
        .await?;
        let Some(claimed) = claimed else {
            #[cfg(feature = "index-lifecycle-testing")]
            lifecycle_metrics.observe_worker_claim_conflict();
            continue;
        };
        return Ok(DispatchAttempt::Scheduled(Box::new(
            InFlightTask::Operation {
                target: operation_id,
                claimed: Box::new(claimed),
                driver,
                limits,
                _permits: task_permits,
                #[cfg(feature = "index-lifecycle-testing")]
                lifecycle_metrics: Arc::clone(lifecycle_metrics),
            },
        )));
    }
    if page.prefix_exhausted {
        *cursor = None;
        Ok(if wrapping {
            DispatchAttempt::Continue
        } else {
            DispatchAttempt::Idle
        })
    } else {
        *cursor = page.resume_after;
        Ok(DispatchAttempt::Continue)
    }
}

fn now_unix_millis() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or(0)
}
