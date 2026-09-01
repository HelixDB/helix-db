//! Feature-gated control and evidence contracts for Index V2 lifecycle testing.
//!
//! The production worker remains the only lifecycle implementation. This module
//! can open the same drivers with background discovery disabled, then advance
//! one exact durable operation. The controller is deliberately stateless and
//! borrows [`HelixDB`] for every call.
//!
//! ```
//! use db::index_lifecycle_testing::{
//!     LifecycleTestController, LifecycleTestScheduling,
//! };
//!
//! let controller = LifecycleTestController::new();
//! assert_eq!(LifecycleTestScheduling::Explicit.as_str(), "explicit");
//! let _ = controller;
//! ```

use std::num::{NonZeroU64, NonZeroUsize};
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Instant;

use slatedb::object_store::ObjectStoreExt;

use crate::encoding::keys::scope::DataScope;
use crate::encoding::property::{encode_properties, Property};
use crate::encoding::v2::keys::ManagedIndexKey as IndexKey;
use crate::encoding::v2::keys::{DataKey, DataKeyKind, NodePropertyKey};
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::outbox::{self, ClaimPermission, OperationPointerObservation};
use crate::index_lifecycle::work;
use crate::index_lifecycle::{
    IndexDdlReceipt, IndexOperationExecutionState, IndexOperationId, IndexOperationPublicProgress,
    IndexOperationStage, IndexOperationStatus, TextBuildProgress, TextBuildStage,
    TextManifestValidationProgress, ValidatedDynamicIndexDefinition,
};
use crate::{
    DbConfig, HelixDB, HelixDbMode, HelixDbSource, HelixStorage, IndexLifecycleScheduling,
    WriterOpenMode,
};

mod contracts;

/// Serializes acceptance contracts that share the process-global failpoint slot.
static LIFECYCLE_CONTRACT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

static TEXT_SEARCH_PAGE_BARRIER: std::sync::Mutex<Option<Weak<TextSearchPageBarrierState>>> =
    std::sync::Mutex::new(None);

#[derive(Debug)]
struct TextSearchPageBarrierState {
    page: u32,
    entered: AtomicBool,
    released: AtomicBool,
    changed: tokio::sync::Notify,
}

/// One feature-gated pause immediately before a selected text manifest page is loaded.
///
/// The hook is absent from ordinary builds and is inert until a lifecycle test
/// owns this handle. Dropping the handle releases every paused search.
#[derive(Debug)]
pub struct TextSearchPageBarrier {
    state: Arc<TextSearchPageBarrierState>,
}

impl TextSearchPageBarrier {
    /// Waits until a search has reached the selected page boundary.
    pub async fn wait_until_entered(&self) {
        loop {
            let notified = self.state.changed.notified();
            if self.state.entered.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    /// Allows paused searches to continue.
    pub fn release(&self) {
        self.state.released.store(true, Ordering::Release);
        self.state.changed.notify_waiters();
    }
}

impl Drop for TextSearchPageBarrier {
    fn drop(&mut self) {
        self.release();
        let mut armed = TEXT_SEARCH_PAGE_BARRIER
            .lock()
            .expect("text search page barrier mutex is healthy");
        if armed
            .as_ref()
            .is_some_and(|state| Weak::ptr_eq(state, &Arc::downgrade(&self.state)))
        {
            *armed = None;
        }
    }
}

/// Arms one process-local pause before a selected manifest page is loaded.
pub fn arm_text_search_page_barrier(page: u32) -> TextSearchPageBarrier {
    let state = Arc::new(TextSearchPageBarrierState {
        page,
        entered: AtomicBool::new(false),
        released: AtomicBool::new(false),
        changed: tokio::sync::Notify::new(),
    });
    let mut armed = TEXT_SEARCH_PAGE_BARRIER
        .lock()
        .expect("text search page barrier mutex is healthy");
    assert!(
        armed.as_ref().and_then(Weak::upgrade).is_none(),
        "only one text search page barrier may be armed"
    );
    *armed = Some(Arc::downgrade(&state));
    TextSearchPageBarrier { state }
}

pub(crate) async fn pause_text_search_before_manifest_page(page: u32) {
    let state = TEXT_SEARCH_PAGE_BARRIER
        .lock()
        .expect("text search page barrier mutex is healthy")
        .as_ref()
        .and_then(Weak::upgrade);
    let Some(state) = state else {
        return;
    };
    if state.page != page || state.released.load(Ordering::Acquire) {
        return;
    }
    state.entered.store(true, Ordering::Release);
    state.changed.notify_waiters();
    loop {
        let notified = state.changed.notified();
        if state.released.load(Ordering::Acquire) {
            return;
        }
        notified.await;
    }
}

/// Runs the small deterministic lifecycle matrix through explicit production drivers.
///
/// This entry point exists for the dedicated integration-test target. It is
/// feature-gated with the rest of this module and is not part of normal builds.
pub async fn run_deterministic_lifecycle_contracts() {
    let _guard = LIFECYCLE_CONTRACT_LOCK.lock().await;
    contracts::run().await;
}

/// Runs deterministic secondary backfill/mutation and unique-retry contracts.
pub async fn run_deterministic_lifecycle_mutation_contracts() {
    let _guard = LIFECYCLE_CONTRACT_LOCK.lock().await;
    contracts::run_mutations().await;
}

/// Runs late-mutation convergence for every supported managed index shape.
pub async fn run_deterministic_all_index_validation_contracts() {
    let _guard = LIFECYCLE_CONTRACT_LOCK.lock().await;
    contracts::run_all_index_validation_mutations().await;
}

/// Runs public secondary/vector writes at every exact build boundary.
pub async fn run_secondary_vector_public_boundary_contracts() {
    let _guard = LIFECYCLE_CONTRACT_LOCK.lock().await;
    contracts::run_secondary_vector_public_boundaries().await;
}

/// Runs retry-safe concurrent CREATE convergence for every family.
pub async fn run_deterministic_lifecycle_race_contracts() {
    let _guard = LIFECYCLE_CONTRACT_LOCK.lock().await;
    contracts::run_races().await;
}

/// Runs repeated same-checkpoint failure and recovery contracts for every family.
pub async fn run_deterministic_lifecycle_fault_contracts() {
    let _guard = LIFECYCLE_CONTRACT_LOCK.lock().await;
    contracts::run_repeated_faults().await;
}

/// Runs tenant build, drop, and abort recovery through a cold reopen after
/// every persisted checkpoint.
pub async fn run_deterministic_lifecycle_reopen_contracts() {
    let _guard = LIFECYCLE_CONTRACT_LOCK.lock().await;
    contracts::run_tenant_reopen_recovery().await;
}

/// Returns every stable process-abort boundary from the production failpoint ADT.
pub fn index_outbox_failpoint_names() -> [&'static str; 16] {
    crate::index_lifecycle::failpoints::IndexOutboxFailpoint::ALL
        .map(|failpoint| failpoint.as_str())
}

/// Injects one recoverable process-local error at a stable outbox boundary.
pub fn inject_index_outbox_error_once(name: &str) -> Result<()> {
    let Some(failpoint) = crate::index_lifecycle::failpoints::IndexOutboxFailpoint::parse(name)
    else {
        return Err(HelixDbError::Config(format!(
            "unknown Index V2 outbox failpoint {name}"
        )));
    };
    crate::index_lifecycle::failpoints::inject_once(failpoint)
}

/// Injects one recoverable error that only the named operation can consume.
pub fn inject_index_outbox_error_once_for_operation(
    name: &str,
    operation_id: IndexOperationId,
) -> Result<()> {
    let Some(failpoint) = crate::index_lifecycle::failpoints::IndexOutboxFailpoint::parse(name)
    else {
        return Err(HelixDbError::Config(format!(
            "unknown Index V2 outbox failpoint {name}"
        )));
    };
    crate::index_lifecycle::failpoints::inject_for_operation_once(failpoint, operation_id)
}

/// Reports whether the most recently injected one-shot error fired.
pub fn index_outbox_error_was_triggered() -> bool {
    crate::index_lifecycle::failpoints::was_triggered()
}

/// Runtime scheduling used by lifecycle test handles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleTestScheduling {
    /// Preserve the ordinary configured background worker behavior.
    Automatic,
    /// Install every driver but require explicit single-step dispatch.
    Explicit,
}

impl LifecycleTestScheduling {
    /// Returns the stable report/CLI spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Automatic => "automatic",
            Self::Explicit => "explicit",
        }
    }

    const fn internal(self) -> IndexLifecycleScheduling {
        match self {
            Self::Automatic => IndexLifecycleScheduling::Configured,
            Self::Explicit => IndexLifecycleScheduling::ExplicitOnly,
        }
    }
}

/// Exact durable work item selected for one explicit turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleWorkTarget {
    /// One canonical build, drop, or abort operation.
    Operation {
        /// Namespace containing the canonical operation.
        scope: DataScope,
        /// Stable operation identity.
        operation_id: IndexOperationId,
    },
}

/// Internal text-manifest validation lane exposed only to lifecycle tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextManifestValidationLane {
    /// Page, split-count, and blob validation.
    Pages,
    /// Root validation, including empty partitions.
    Roots,
    /// Entity-state ownership and root-revision validation.
    EntityStates,
}

/// Closed lifecycle stage family used by the harness crash matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleStage {
    /// Build, drop, or abort operation stage.
    Operation(IndexOperationStage),
}

impl LifecycleStage {
    /// Every stable operation stage.
    ///
    /// Matrix-completeness tests compare generated cases with this closed list,
    /// so adding a stage to an owning ADT requires an explicit crash policy.
    pub fn all() -> Vec<Self> {
        const OPERATION_STAGES: [IndexOperationStage; 22] = [
            IndexOperationStage::Scan,
            IndexOperationStage::ScanPartitions,
            IndexOperationStage::CatchUp,
            IndexOperationStage::Validate,
            IndexOperationStage::ValidateDescriptor,
            IndexOperationStage::ValidateLegacyPhysical,
            IndexOperationStage::Compact,
            IndexOperationStage::PrepareManifests,
            IndexOperationStage::ValidateManifests,
            IndexOperationStage::Activate,
            IndexOperationStage::DeleteEntries,
            IndexOperationStage::RetireCache,
            IndexOperationStage::DeletePhysical,
            IndexOperationStage::DeleteDeltas,
            IndexOperationStage::DeleteMetadata,
            IndexOperationStage::Finalize,
            IndexOperationStage::AbortingDeleteEntries,
            IndexOperationStage::AbortingRetireCache,
            IndexOperationStage::AbortingDeletePhysical,
            IndexOperationStage::AbortingDeleteDeltas,
            IndexOperationStage::AbortingDeleteMetadata,
            IndexOperationStage::AbortingFinalize,
        ];
        OPERATION_STAGES.into_iter().map(Self::Operation).collect()
    }
}

/// Presence of one exact durable checkpoint before or after a step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleCheckpoint {
    /// The target still has durable work state.
    Present {
        /// Typed lifecycle stage.
        stage: LifecycleStage,
        /// Monotonic revision of the exact durable operation, intent, or root.
        durable_revision: u64,
        /// Cumulative operation counters; non-operation lanes retain zeroes.
        progress: IndexOperationPublicProgress,
    },
    /// The target has no remaining durable runnable record.
    Absent,
}

/// Closed outcome of one controller turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleStepOutcome {
    /// A durable checkpoint advanced.
    Progressed,
    /// The exact work item was released with retry backoff.
    TransientFailure,
    /// The operation or owning build became explicitly blocked.
    Blocked,
    /// The canonical operation completed.
    Completed,
    /// The target is already terminal and no dispatch was attempted.
    AlreadyTerminal,
    /// The persisted retry deadline has not elapsed.
    Delayed {
        /// Remaining persisted delay.
        delay_millis: u64,
    },
    /// An active request still owns this upload.
    WaitingOnOwner,
    /// A stale discovery pointer was removed.
    StalePointerRemoved,
    /// A GC root disappeared or has no runnable phase.
    Idle,
}

/// Disposable vector planning behavior observed during one lifecycle turn.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LifecycleVectorPlanningUsage {
    /// Typed HNSW/metadata planning executions.
    pub planning_executions: u64,
    /// Final exact vector writes admitted into target transactions.
    pub planned_writes: u64,
    /// Deprecated replay executions; must remain zero on exact-plan builds.
    pub replay_executions: u64,
    /// Decoded-item cache hits.
    pub item_hits: u64,
    /// Decoded-item cache misses, including cached absence.
    pub item_misses: u64,
    /// Neighbor-row cache hits.
    pub neighbor_hits: u64,
    /// Neighbor-row cache misses, including cached absence.
    pub neighbor_misses: u64,
    /// SimHash cache hits.
    pub simhash_hits: u64,
    /// SimHash cache misses, including cached absence.
    pub simhash_misses: u64,
    /// Decoded-item entries evicted by bounded LRU.
    pub item_evictions: u64,
    /// Neighbor rows evicted by bounded LRU.
    pub neighbor_evictions: u64,
    /// SimHashes evicted by bounded LRU.
    pub simhash_evictions: u64,
    /// Dirty neighbor rows flushed into the typed recorder.
    pub dirty_neighbor_flushes: u64,
    /// Peak retained session payload after enforcing the configured ceiling.
    pub retained_payload_bytes: u64,
}

/// Actual bounded resources consumed by one committed controller turn.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LifecycleStepResourceUsage {
    /// Authoritative source entities admitted by the step.
    pub entities: u64,
    /// Source bytes admitted by the step.
    pub input_bytes: u64,
    /// Physical write/delete operations staged by the step.
    pub output_operations: u64,
    /// Physical output bytes staged by the step.
    pub output_bytes: u64,
    /// Bytes in one indivisible vector output, when applicable.
    pub vector_output_bytes: u64,
    /// Bytes in one text artifact/upload, when applicable.
    pub text_artifact_bytes: u64,
    /// Content-addressed text bytes submitted to object storage.
    pub text_upload_bytes: u64,
    /// Number of immutable text artifacts selected by one compaction.
    pub text_compaction_fan_in: u64,
    /// Text compaction input bytes, when applicable.
    pub text_compaction_input_bytes: u64,
    /// Peak temporary bytes required by one text compaction.
    pub text_temporary_bytes: u64,
    /// Encoded text manifest page bytes, when applicable.
    pub text_manifest_page_bytes: u64,
    /// Encoded text manifest root bytes, when applicable.
    pub text_manifest_root_bytes: u64,
    /// Vector planner/cache behavior retained only as runtime evidence.
    pub vector_planning: LifecycleVectorPlanningUsage,
}

/// Evidence returned after one exact explicit lifecycle turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LifecycleStepEvidence {
    /// Exact requested work item.
    pub target: LifecycleWorkTarget,
    /// Durable checkpoint observed before dispatch.
    pub before: LifecycleCheckpoint,
    /// Closed result of dispatch.
    pub outcome: LifecycleStepOutcome,
    /// Durable checkpoint observed after dispatch.
    pub after: LifecycleCheckpoint,
    /// Actual bounded resource delta for this turn.
    pub resources: LifecycleStepResourceUsage,
    /// End-to-end controller latency in microseconds.
    pub elapsed_micros: u64,
}

const OPERATION_STAGE_COUNT: usize = 22;
const LIFECYCLE_STAGE_COUNT: usize = OPERATION_STAGE_COUNT;
const RESOURCE_FIELD_COUNT: usize = 12;
const VECTOR_PLANNING_FIELD_COUNT: usize = 14;
const WORKER_TASK_KIND_COUNT: usize = 3;
const LATENCY_BUCKET_UPPER_MICROS: [u64; 8] =
    [100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, u64::MAX];

/// One non-empty automatic-worker stage-transition counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LifecycleStageTransitionCount {
    /// Stage observed before the committed turn.
    pub before: LifecycleStage,
    /// Stage observed after the committed turn.
    pub after: LifecycleStage,
    /// Number of turns with this transition.
    pub count: u64,
}

/// Bounded aggregate metrics from an automatically scheduled writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutomaticLifecycleMetricsSnapshot {
    /// Total operation turns observed.
    pub lifecycle_steps: u64,
    /// Total operation turns observed by the automatic worker.
    pub operation_steps: u64,
    /// Saturating cumulative disposable resource counters.
    pub resource_totals: LifecycleStepResourceUsage,
    /// Maximum disposable resource counters from any one turn.
    pub resource_maxima: LifecycleStepResourceUsage,
    /// Fixed latency bucket upper bounds in microseconds.
    pub latency_bucket_upper_micros: [u64; 8],
    /// Saturating count in each fixed latency bucket.
    pub latency_bucket_counts: [u64; 8],
    /// Non-zero cells from the fixed stage-transition matrix.
    pub stage_transitions: Vec<LifecycleStageTransitionCount>,
    /// Saturating cumulative vector planning/cache counters.
    pub vector_planning_totals: LifecycleVectorPlanningUsage,
    /// Maximum vector planning/cache value observed in one turn.
    pub vector_planning_maxima: LifecycleVectorPlanningUsage,
    /// Bounded dispatcher concurrency and recovery counters.
    pub worker: LifecycleWorkerMetrics,
}

/// Dispatcher concurrency and recovery evidence from one writer runtime.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LifecycleWorkerMetrics {
    /// Highest total number of concurrently executing lifecycle tasks.
    pub max_in_flight: u64,
    /// Highest concurrently executing secondary operation count.
    pub max_secondary_in_flight: u64,
    /// Highest concurrently executing vector operation count.
    pub max_vector_in_flight: u64,
    /// Highest concurrently executing text operation count.
    pub max_text_in_flight: u64,
    /// Claims lost to a concurrent durable state change.
    pub claim_conflicts: u64,
    /// Worker cycles terminated by a task or dispatcher failure.
    pub failures: u64,
    /// Spawned tasks that panicked or were cancelled unexpectedly.
    pub panics: u64,
    /// Supervised same-epoch worker restarts.
    pub restarts: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AutomaticLifecycleTaskKind {
    Secondary,
    Vector,
    Text,
}

impl AutomaticLifecycleTaskKind {
    const fn index(self) -> usize {
        match self {
            Self::Secondary => 0,
            Self::Vector => 1,
            Self::Text => 2,
        }
    }
}

/// Bounded SlateDB state sampled by the lifecycle benchmark harness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LifecycleLsmSnapshot {
    /// Highest sequence durably persisted by this writer.
    pub durable_sequence: u64,
    /// Current unsegmented L0 SST count.
    pub l0_sst_count: u64,
    /// Current unsegmented compacted-run count.
    pub compacted_run_count: u64,
    /// Current manifest version observed by this writer.
    pub manifest_version: u64,
}

/// Runtime projection of one canonical index definition row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LifecycleDefinitionSnapshot {
    /// Stable lifecycle state name, or `None` when no canonical row exists.
    pub state: Option<&'static str>,
    /// Current or last generation, or `None` when no canonical row exists.
    pub generation: Option<u64>,
}

/// Atomic, fixed-capacity accumulator owned by one writer runtime.
pub(crate) struct AutomaticLifecycleMetrics {
    lifecycle_steps: AtomicU64,
    operation_steps: AtomicU64,
    resource_totals: [AtomicU64; RESOURCE_FIELD_COUNT],
    resource_maxima: [AtomicU64; RESOURCE_FIELD_COUNT],
    latency_buckets: [AtomicU64; 8],
    stage_transitions: [AtomicU64; LIFECYCLE_STAGE_COUNT * LIFECYCLE_STAGE_COUNT],
    vector_planning_totals: [AtomicU64; VECTOR_PLANNING_FIELD_COUNT],
    vector_planning_maxima: [AtomicU64; VECTOR_PLANNING_FIELD_COUNT],
    worker_in_flight: [AtomicU64; WORKER_TASK_KIND_COUNT],
    worker_in_flight_maxima: [AtomicU64; WORKER_TASK_KIND_COUNT],
    worker_total_in_flight: AtomicU64,
    worker_max_in_flight: AtomicU64,
    worker_claim_conflicts: AtomicU64,
    worker_failures: AtomicU64,
    worker_panics: AtomicU64,
    worker_restarts: AtomicU64,
}

pub(crate) struct AutomaticLifecycleTaskGuard {
    metrics: Arc<AutomaticLifecycleMetrics>,
    kind: AutomaticLifecycleTaskKind,
}

impl Drop for AutomaticLifecycleTaskGuard {
    fn drop(&mut self) {
        self.metrics.worker_in_flight[self.kind.index()].fetch_sub(1, Ordering::Relaxed);
        self.metrics
            .worker_total_in_flight
            .fetch_sub(1, Ordering::Relaxed);
    }
}

impl AutomaticLifecycleMetrics {
    pub(crate) fn new() -> Self {
        Self {
            lifecycle_steps: AtomicU64::new(0),
            operation_steps: AtomicU64::new(0),
            resource_totals: std::array::from_fn(|_| AtomicU64::new(0)),
            resource_maxima: std::array::from_fn(|_| AtomicU64::new(0)),
            latency_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            stage_transitions: std::array::from_fn(|_| AtomicU64::new(0)),
            vector_planning_totals: std::array::from_fn(|_| AtomicU64::new(0)),
            vector_planning_maxima: std::array::from_fn(|_| AtomicU64::new(0)),
            worker_in_flight: std::array::from_fn(|_| AtomicU64::new(0)),
            worker_in_flight_maxima: std::array::from_fn(|_| AtomicU64::new(0)),
            worker_total_in_flight: AtomicU64::new(0),
            worker_max_in_flight: AtomicU64::new(0),
            worker_claim_conflicts: AtomicU64::new(0),
            worker_failures: AtomicU64::new(0),
            worker_panics: AtomicU64::new(0),
            worker_restarts: AtomicU64::new(0),
        }
    }

    pub(crate) fn begin_worker_task(
        self: &Arc<Self>,
        kind: AutomaticLifecycleTaskKind,
    ) -> AutomaticLifecycleTaskGuard {
        let kind_in_flight = self.worker_in_flight[kind.index()]
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        self.worker_in_flight_maxima[kind.index()].fetch_max(kind_in_flight, Ordering::Relaxed);
        let total = self
            .worker_total_in_flight
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        self.worker_max_in_flight
            .fetch_max(total, Ordering::Relaxed);
        AutomaticLifecycleTaskGuard {
            metrics: Arc::clone(self),
            kind,
        }
    }

    pub(crate) fn observe_worker_claim_conflict(&self) {
        saturating_atomic_add(&self.worker_claim_conflicts, 1);
    }

    pub(crate) fn observe_worker_failure(&self) {
        saturating_atomic_add(&self.worker_failures, 1);
    }

    pub(crate) fn observe_worker_panic(&self) {
        saturating_atomic_add(&self.worker_panics, 1);
    }

    pub(crate) fn observe_worker_restart(&self) {
        saturating_atomic_add(&self.worker_restarts, 1);
    }

    pub(crate) fn observe_operation(&self, evidence: outbox::CommittedOperationStepEvidence) {
        saturating_atomic_add(&self.operation_steps, 1);
        self.observe(
            LifecycleStage::Operation(evidence.before_stage),
            LifecycleStage::Operation(evidence.after_stage),
            evidence.resources.into(),
            evidence.elapsed_micros,
        );
    }

    fn observe(
        &self,
        before: LifecycleStage,
        after: LifecycleStage,
        resources: LifecycleStepResourceUsage,
        elapsed_micros: u64,
    ) {
        saturating_atomic_add(&self.lifecycle_steps, 1);
        for (index, value) in resource_values(resources).into_iter().enumerate() {
            saturating_atomic_add(&self.resource_totals[index], value);
            self.resource_maxima[index].fetch_max(value, Ordering::Relaxed);
        }
        for (index, value) in vector_planning_values(resources.vector_planning)
            .into_iter()
            .enumerate()
        {
            saturating_atomic_add(&self.vector_planning_totals[index], value);
            self.vector_planning_maxima[index].fetch_max(value, Ordering::Relaxed);
        }
        let latency_index = LATENCY_BUCKET_UPPER_MICROS
            .iter()
            .position(|upper| elapsed_micros <= *upper)
            .unwrap_or(LATENCY_BUCKET_UPPER_MICROS.len() - 1);
        saturating_atomic_add(&self.latency_buckets[latency_index], 1);
        let before = lifecycle_stage_index(before);
        let after = lifecycle_stage_index(after);
        saturating_atomic_add(
            &self.stage_transitions[before * LIFECYCLE_STAGE_COUNT + after],
            1,
        );
    }

    pub(crate) fn snapshot(&self) -> AutomaticLifecycleMetricsSnapshot {
        let totals =
            std::array::from_fn(|index| self.resource_totals[index].load(Ordering::Relaxed));
        let maxima =
            std::array::from_fn(|index| self.resource_maxima[index].load(Ordering::Relaxed));
        let latency_bucket_counts =
            std::array::from_fn(|index| self.latency_buckets[index].load(Ordering::Relaxed));
        let vector_planning_totals =
            std::array::from_fn(|index| self.vector_planning_totals[index].load(Ordering::Relaxed));
        let vector_planning_maxima =
            std::array::from_fn(|index| self.vector_planning_maxima[index].load(Ordering::Relaxed));
        let mut stage_transitions = Vec::new();
        for before in 0..LIFECYCLE_STAGE_COUNT {
            for after in 0..LIFECYCLE_STAGE_COUNT {
                let count = self.stage_transitions[before * LIFECYCLE_STAGE_COUNT + after]
                    .load(Ordering::Relaxed);
                if count > 0 {
                    stage_transitions.push(LifecycleStageTransitionCount {
                        before: lifecycle_stage_from_index(before),
                        after: lifecycle_stage_from_index(after),
                        count,
                    });
                }
            }
        }
        AutomaticLifecycleMetricsSnapshot {
            lifecycle_steps: self.lifecycle_steps.load(Ordering::Relaxed),
            operation_steps: self.operation_steps.load(Ordering::Relaxed),
            resource_totals: resource_values_to_usage(totals),
            resource_maxima: resource_values_to_usage(maxima),
            latency_bucket_upper_micros: LATENCY_BUCKET_UPPER_MICROS,
            latency_bucket_counts,
            stage_transitions,
            vector_planning_totals: vector_planning_values_to_usage(vector_planning_totals),
            vector_planning_maxima: vector_planning_values_to_usage(vector_planning_maxima),
            worker: LifecycleWorkerMetrics {
                max_in_flight: self.worker_max_in_flight.load(Ordering::Relaxed),
                max_secondary_in_flight: self.worker_in_flight_maxima[0].load(Ordering::Relaxed),
                max_vector_in_flight: self.worker_in_flight_maxima[1].load(Ordering::Relaxed),
                max_text_in_flight: self.worker_in_flight_maxima[2].load(Ordering::Relaxed),
                claim_conflicts: self.worker_claim_conflicts.load(Ordering::Relaxed),
                failures: self.worker_failures.load(Ordering::Relaxed),
                panics: self.worker_panics.load(Ordering::Relaxed),
                restarts: self.worker_restarts.load(Ordering::Relaxed),
            },
        }
    }
}

fn saturating_atomic_add(value: &AtomicU64, increment: u64) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(increment))
    });
}

fn resource_values(resources: LifecycleStepResourceUsage) -> [u64; RESOURCE_FIELD_COUNT] {
    [
        resources.entities,
        resources.input_bytes,
        resources.output_operations,
        resources.output_bytes,
        resources.vector_output_bytes,
        resources.text_artifact_bytes,
        resources.text_upload_bytes,
        resources.text_compaction_fan_in,
        resources.text_compaction_input_bytes,
        resources.text_temporary_bytes,
        resources.text_manifest_page_bytes,
        resources.text_manifest_root_bytes,
    ]
}

fn resource_values_to_usage(values: [u64; RESOURCE_FIELD_COUNT]) -> LifecycleStepResourceUsage {
    LifecycleStepResourceUsage {
        entities: values[0],
        input_bytes: values[1],
        output_operations: values[2],
        output_bytes: values[3],
        vector_output_bytes: values[4],
        text_artifact_bytes: values[5],
        text_upload_bytes: values[6],
        text_compaction_fan_in: values[7],
        text_compaction_input_bytes: values[8],
        text_temporary_bytes: values[9],
        text_manifest_page_bytes: values[10],
        text_manifest_root_bytes: values[11],
        vector_planning: LifecycleVectorPlanningUsage::default(),
    }
}

fn vector_planning_values(
    usage: LifecycleVectorPlanningUsage,
) -> [u64; VECTOR_PLANNING_FIELD_COUNT] {
    [
        usage.planning_executions,
        usage.planned_writes,
        usage.replay_executions,
        usage.item_hits,
        usage.item_misses,
        usage.neighbor_hits,
        usage.neighbor_misses,
        usage.simhash_hits,
        usage.simhash_misses,
        usage.item_evictions,
        usage.neighbor_evictions,
        usage.simhash_evictions,
        usage.dirty_neighbor_flushes,
        usage.retained_payload_bytes,
    ]
}

fn vector_planning_values_to_usage(
    values: [u64; VECTOR_PLANNING_FIELD_COUNT],
) -> LifecycleVectorPlanningUsage {
    LifecycleVectorPlanningUsage {
        planning_executions: values[0],
        planned_writes: values[1],
        replay_executions: values[2],
        item_hits: values[3],
        item_misses: values[4],
        neighbor_hits: values[5],
        neighbor_misses: values[6],
        simhash_hits: values[7],
        simhash_misses: values[8],
        item_evictions: values[9],
        neighbor_evictions: values[10],
        simhash_evictions: values[11],
        dirty_neighbor_flushes: values[12],
        retained_payload_bytes: values[13],
    }
}

fn lifecycle_stage_index(stage: LifecycleStage) -> usize {
    match stage {
        LifecycleStage::Operation(stage) => operation_stage_index(stage),
    }
}

fn lifecycle_stage_from_index(index: usize) -> LifecycleStage {
    match index {
        0..OPERATION_STAGE_COUNT => LifecycleStage::Operation(operation_stage_from_index(index)),
        _ => unreachable!("fixed lifecycle stage matrix index is in range"),
    }
}

fn operation_stage_index(stage: IndexOperationStage) -> usize {
    operation_stages()
        .into_iter()
        .position(|candidate| candidate == stage)
        .expect("every typed operation stage belongs to the fixed metrics matrix")
}

fn operation_stage_from_index(index: usize) -> IndexOperationStage {
    operation_stages()[index]
}

const fn operation_stages() -> [IndexOperationStage; OPERATION_STAGE_COUNT] {
    [
        IndexOperationStage::Scan,
        IndexOperationStage::ScanPartitions,
        IndexOperationStage::CatchUp,
        IndexOperationStage::Validate,
        IndexOperationStage::ValidateDescriptor,
        IndexOperationStage::ValidateLegacyPhysical,
        IndexOperationStage::Compact,
        IndexOperationStage::PrepareManifests,
        IndexOperationStage::ValidateManifests,
        IndexOperationStage::Activate,
        IndexOperationStage::DeleteEntries,
        IndexOperationStage::RetireCache,
        IndexOperationStage::DeletePhysical,
        IndexOperationStage::DeleteDeltas,
        IndexOperationStage::DeleteMetadata,
        IndexOperationStage::Finalize,
        IndexOperationStage::AbortingDeleteEntries,
        IndexOperationStage::AbortingRetireCache,
        IndexOperationStage::AbortingDeletePhysical,
        IndexOperationStage::AbortingDeleteDeltas,
        IndexOperationStage::AbortingDeleteMetadata,
        IndexOperationStage::AbortingFinalize,
    ]
}

/// Bounded discovery result for each independently scheduled lifecycle lane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleWorkPage {
    /// Exact runnable operation identities found in this page.
    pub targets: Vec<LifecycleWorkTarget>,
    /// Whether every lane was exhausted within the requested per-lane bound.
    pub exhausted: bool,
}

/// Stateless explicit controller; every operation borrows its database owner.
#[derive(Debug, Clone, Copy, Default)]
pub struct LifecycleTestController;

impl LifecycleTestController {
    /// Constructs a stateless controller.
    pub const fn new() -> Self {
        Self
    }

    /// Seeds authoritative node-property rows in bounded setup transactions.
    ///
    /// This test-only setup boundary reserves IDs through the production
    /// allocator and serializes rows through the canonical typed codecs. It
    /// deliberately does not maintain indexes, so callers must invoke it only
    /// before accepting the index build whose source snapshot they are testing.
    pub async fn seed_node_property_rows<F>(
        &self,
        db: &HelixDB,
        scope: DataScope,
        row_count: NonZeroU64,
        batch_rows: NonZeroUsize,
        properties: F,
    ) -> Result<Range<u64>>
    where
        F: Fn(u64) -> Vec<Property>,
    {
        let HelixStorage::Writer(writer) = db.storage() else {
            return Err(HelixDbError::WriterModeRequired {
                actual: db.mode().as_str(),
            });
        };
        let ids = writer.node_ids().allocate_batch(row_count.get()).await?;
        let batch_rows = u64::try_from(batch_rows.get()).map_err(|_| {
            HelixDbError::Config("lifecycle seed batch rows exceed u64".to_string())
        })?;
        let mut batch_start = ids.start;
        while batch_start < ids.end {
            let batch_end = ids.end.min(batch_start.saturating_add(batch_rows));
            let transaction = writer.db().begin(slatedb::IsolationLevel::Snapshot).await?;
            for entity_id in batch_start..batch_end {
                transaction.put(
                    DataKey::Data {
                        scope,
                        kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity_id)),
                    }
                    .to_bytes(),
                    encode_properties(&properties(entity_id)),
                )?;
            }
            transaction.commit().await?;
            batch_start = batch_end;
        }
        Ok(ids)
    }

    /// Seeds one valid empty legacy vector namespace and enqueues its adoption.
    ///
    /// The fixture uses the deployed legacy catalog and metadata codecs, then
    /// runs the production reservation preflight before accepting the build.
    /// Legacy vector namespaces are unpartitioned and unscoped by definition,
    /// so the concrete vector definition is the only caller-supplied identity.
    #[cfg(any(test, feature = "production-coverage"))]
    pub async fn seed_empty_legacy_vector_adoption(
        &self,
        db: &HelixDB,
        definition: crate::index_lifecycle::ValidatedVectorIndexDefinition,
    ) -> Result<IndexDdlReceipt> {
        if db.inner.lifecycle_test_scheduling != IndexLifecycleScheduling::ExplicitOnly {
            return Err(HelixDbError::Config(
                "legacy vector adoption seeding requires explicit lifecycle scheduling".to_string(),
            ));
        }
        if definition.tenant_property().is_some() {
            return Err(HelixDbError::Config(
                "legacy vector adoption seeding requires an unpartitioned definition".to_string(),
            ));
        }
        let writer = db.lifecycle_test_writer_db()?;
        let runtime = definition.to_runtime();
        let physical_name = crate::search::vector_index_name(
            runtime.element_type(),
            runtime.label(),
            runtime.property(),
        );
        let physical_index_id = crate::index_lifecycle::VectorPhysicalIndexId::new(
            crate::search::vector::index_id_from_name(&physical_name),
        )?;
        let dynamic = ValidatedDynamicIndexDefinition::Vector(definition.clone());
        let (catalog_key, catalog_value) =
            crate::migrations::migration_parity_legacy_catalog_row(&dynamic, false)?;
        let metadata = crate::search::vector::VectorIndexMetadata::new(
            crate::search::vector::VectorIndexConfig::from_v2_definition(
                &definition,
                &physical_name,
            ),
        );
        let transaction = writer
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await?;
        crate::migrations::stage_index_v2_migration_reopen_for_fixture(
            &transaction,
            DataScope::LegacyUnscoped,
        )?;
        transaction.put(catalog_key, catalog_value)?;
        transaction.put(
            DataKey::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::Vector(
                    crate::encoding::v2::keys::indexes::vector::VectorKey::IndexMetadata(
                        crate::encoding::v2::keys::indexes::vector::VectorIndexMetadataKey::new(
                            physical_index_id.get(),
                        ),
                    ),
                ),
            }
            .to_bytes(),
            bytes::Bytes::copy_from_slice(
                &crate::encoding::v2::legacy::vector::metadata::encode_legacy_metadata_for_contract(
                    &metadata,
                ),
            ),
        )?;
        transaction.commit().await?;
        crate::migrations::preflight_legacy_vector_reservations(writer).await?;
        crate::index_lifecycle::lifecycle::create_legacy_vector_adoption_operation(
            writer,
            DataScope::LegacyUnscoped,
            dynamic,
            physical_index_id,
        )
        .await
    }

    /// Seeds authoritative rows plus secondary build deltas in bounded transactions.
    ///
    /// This test-only saturation boundary must run while explicit lifecycle
    /// scheduling is quiescent and one secondary generation is `Building`.
    /// It uses the production secondary mutation projection for every entity,
    /// but omits unrelated public mutation work so scale tests can measure a
    /// pre-existing catch-up backlog independently of writer throughput.
    pub async fn seed_secondary_build_delta_rows<F>(
        &self,
        db: &HelixDB,
        scope: DataScope,
        row_count: NonZeroU64,
        batch_rows: NonZeroUsize,
        properties: F,
    ) -> Result<Range<u64>>
    where
        F: Fn(u64) -> Vec<Property>,
    {
        if db.inner.lifecycle_test_scheduling != IndexLifecycleScheduling::ExplicitOnly {
            return Err(HelixDbError::Config(
                "secondary backlog seeding requires explicit lifecycle scheduling".to_string(),
            ));
        }
        let HelixStorage::Writer(writer) = db.storage() else {
            return Err(HelixDbError::WriterModeRequired {
                actual: db.mode().as_str(),
            });
        };
        let ids = writer.node_ids().allocate_batch(row_count.get()).await?;
        let batch_rows = u64::try_from(batch_rows.get()).map_err(|_| {
            HelixDbError::Config("lifecycle seed batch rows exceed u64".to_string())
        })?;
        let mut batch_start = ids.start;
        while batch_start < ids.end {
            let batch_end = ids.end.min(batch_start.saturating_add(batch_rows));
            let transaction = writer
                .db()
                .begin(slatedb::IsolationLevel::SerializableSnapshot)
                .await?;
            let mutations =
                crate::index_lifecycle::secondary::load_mutation_set(&transaction, scope).await?;
            for entity_id in batch_start..batch_end {
                let properties = properties(entity_id - ids.start);
                crate::index_lifecycle::secondary::maintain_entity(
                    &transaction,
                    scope,
                    &mutations,
                    crate::index_lifecycle::IndexElementKind::Node,
                    entity_id,
                    &[],
                    &properties,
                )
                .await?;
                transaction.put(
                    DataKey::Data {
                        scope,
                        kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity_id)),
                    }
                    .to_bytes(),
                    encode_properties(&properties),
                )?;
            }
            transaction.commit().await?;
            batch_start = batch_end;
        }
        Ok(ids)
    }

    /// Advances exactly one selected work item through the production driver.
    pub async fn advance(
        &self,
        db: &HelixDB,
        target: LifecycleWorkTarget,
    ) -> Result<LifecycleStepEvidence> {
        db.advance_index_lifecycle_lifecycle_test_work(target, None)
            .await
    }

    /// Advances one item using a caller-owned logical wall-clock observation.
    ///
    /// Subprocess recovery matrices use monotonically increasing values to
    /// cross persisted retry deadlines without making exhaustive crash tests
    /// sleep. The production driver still derives and commits every deadline.
    pub async fn advance_at_unix_millis(
        &self,
        db: &HelixDB,
        target: LifecycleWorkTarget,
        now_unix_millis: u64,
    ) -> Result<LifecycleStepEvidence> {
        db.advance_index_lifecycle_lifecycle_test_work(target, Some(now_unix_millis))
            .await
    }

    /// Discovers a bounded page from every durable lifecycle work lane.
    ///
    /// The limit applies independently to operations, uploads, and GC roots so
    /// one busy lane cannot starve another lane from a deterministic turn.
    pub async fn discover(
        &self,
        db: &HelixDB,
        per_lane_limit: NonZeroUsize,
    ) -> Result<LifecycleWorkPage> {
        db.discover_index_lifecycle_lifecycle_test_work(per_lane_limit)
            .await
    }

    /// Point-reads one exact target without claiming or advancing it.
    pub async fn inspect(
        &self,
        db: &HelixDB,
        target: LifecycleWorkTarget,
    ) -> Result<LifecycleCheckpoint> {
        db.inspect_index_lifecycle_lifecycle_test_work(target).await
    }

    /// Returns the exact internal text-manifest validation lane, when present.
    pub async fn text_manifest_validation_lane(
        &self,
        db: &HelixDB,
        scope: DataScope,
        operation_id: IndexOperationId,
    ) -> Result<Option<TextManifestValidationLane>> {
        let writer = db.lifecycle_test_writer_db()?;
        let Some((actual_scope, operation)) =
            outbox::read_queued_operation(writer, operation_id).await?
        else {
            return Ok(None);
        };
        if actual_scope != scope {
            return Err(HelixDbError::InvariantViolation(
                "text validation lane observer names the wrong scope".to_string(),
            ));
        }
        Ok(match operation.progress() {
            crate::index_lifecycle::IndexOperationProgress::TextBuild(
                TextBuildProgress::Constructing(TextBuildStage::ValidateManifests(progress)),
            ) => Some(match progress {
                TextManifestValidationProgress::Pages(_) => TextManifestValidationLane::Pages,
                TextManifestValidationProgress::Roots(_) => TextManifestValidationLane::Roots,
                TextManifestValidationProgress::EntityStates(_) => {
                    TextManifestValidationLane::EntityStates
                }
            }),
            crate::index_lifecycle::IndexOperationProgress::SecondaryBuild(_)
            | crate::index_lifecycle::IndexOperationProgress::VectorBuild(_)
            | crate::index_lifecycle::IndexOperationProgress::TextBuild(_)
            | crate::index_lifecycle::IndexOperationProgress::SecondaryCleanup(_)
            | crate::index_lifecycle::IndexOperationProgress::VectorCleanup(_)
            | crate::index_lifecycle::IndexOperationProgress::TextCleanup(_) => None,
        })
    }

    /// Snapshots fixed-capacity metrics recorded by the automatic worker.
    ///
    /// The accumulator stores only atomic totals, maxima, histogram buckets,
    /// and a fixed stage-transition matrix; its memory use does not grow with
    /// entity or step count.
    pub fn automatic_metrics(&self, db: &HelixDB) -> Result<AutomaticLifecycleMetricsSnapshot> {
        if db.inner.lifecycle_test_scheduling == IndexLifecycleScheduling::ExplicitOnly {
            return Err(HelixDbError::Config(
                "automatic lifecycle metrics require automatic test scheduling".to_string(),
            ));
        }
        Ok(db.inner.lifecycle_metrics.snapshot())
    }

    /// Samples fixed-size LSM state without retaining manifest history.
    pub fn lsm_snapshot(&self, db: &HelixDB) -> Result<LifecycleLsmSnapshot> {
        let status = db.lifecycle_test_writer_db()?.status();
        Ok(LifecycleLsmSnapshot {
            durable_sequence: status.durable_seq,
            l0_sst_count: u64::try_from(status.current_manifest.l0().len()).unwrap_or(u64::MAX),
            compacted_run_count: u64::try_from(status.current_manifest.compacted().len())
                .unwrap_or(u64::MAX),
            manifest_version: status.current_manifest.id(),
        })
    }

    /// Reads one canonical definition without retaining storage ownership.
    pub async fn definition_snapshot(
        &self,
        db: &HelixDB,
        scope: DataScope,
        definition: &ValidatedDynamicIndexDefinition,
    ) -> Result<LifecycleDefinitionSnapshot> {
        let record = crate::index_lifecycle::repository::load_index_record(
            db.lifecycle_test_writer_db()?,
            scope,
            &definition.identity(),
        )
        .await?;
        let Some(record) = record else {
            return Ok(LifecycleDefinitionSnapshot {
                state: None,
                generation: None,
            });
        };
        let state = record.state();
        Ok(LifecycleDefinitionSnapshot {
            state: Some(state.name()),
            generation: Some(state.generation().get()),
        })
    }

    /// Rewrites one multi-split Active text manifest into two valid pages.
    ///
    /// Existing split bytes and live entity state are preserved exactly.
    pub async fn repage_active_text_manifest_for_testing(
        &self,
        db: &HelixDB,
        scope: DataScope,
        definition: &ValidatedDynamicIndexDefinition,
    ) -> Result<()> {
        let handles = db.active_index_handles_loaded(scope);
        let identity = definition.identity();
        let active = handles
            .iter()
            .find(|handle| handle.identity() == &identity)
            .ok_or_else(|| {
                HelixDbError::IndexNotFound(format!(
                    "active lifecycle test definition {:?}",
                    identity
                ))
            })?;
        let authority =
            crate::index_lifecycle::text::serving::ActiveTextServingAuthority::try_from_active(
                active,
            )?;
        let partition = work::TextPartition::Unpartitioned;
        let writer = db.lifecycle_test_writer_db()?;
        let root = crate::index_lifecycle::text::serving::load_active_manifest_root(
            writer, &authority, &partition,
        )
        .await?
        .ok_or_else(|| {
            HelixDbError::IndexCatalogCorruption(
                "unpartitioned re-page fixture has no manifest root".to_string(),
            )
        })?;
        if root.page_count() != 1 {
            return Err(HelixDbError::IndexCatalogCorruption(format!(
                "re-page fixture expected one source page, found {}",
                root.page_count()
            )));
        }
        let entries =
            crate::index_lifecycle::text::serving::load_active_manifest_page(writer, &root, 0)
                .await?;
        if entries.len() < 2 {
            return Err(HelixDbError::IndexCatalogCorruption(format!(
                "re-page fixture needs at least two splits, found {}",
                entries.len()
            )));
        }
        let root_typed = crate::encoding::v2::keys::TextManifestRootKey {
            index_id: authority.index_id(),
            generation: authority.generation(),
            partition: partition.fingerprint(),
        };
        let root_key = IndexKey::Data {
            scope,
            kind: crate::encoding::v2::keys::ScopedKey::TextManifestRoot(root_typed),
        }
        .to_bytes();
        let root_bytes = writer.get(&root_key).await?.ok_or_else(|| {
            HelixDbError::IndexCatalogCorruption(
                "re-page fixture lost its manifest root".to_string(),
            )
        })?;
        let root_value = crate::encoding::v2::values::decode_manifest_root(&root_bytes)?;
        let first_page = work::TextManifestPageValue::try_new(
            authority.index_id(),
            authority.generation(),
            partition.clone(),
            0,
            vec![entries[0]],
        )
        .expect("one existing split forms a valid first page");
        let second_page = work::TextManifestPageValue::try_new(
            authority.index_id(),
            authority.generation(),
            partition.clone(),
            1,
            entries[1..].to_vec(),
        )
        .expect("remaining existing splits form a valid second page");
        let next_root = work::TextManifestRootValue::try_new(
            authority.index_id(),
            authority.generation(),
            partition,
            root_value.revision().checked_next().map_err(|_| {
                HelixDbError::IndexCatalogCorruption(
                    "re-page fixture manifest revision is exhausted".to_string(),
                )
            })?,
            2,
            u64::try_from(entries.len()).expect("bounded split count fits u64"),
        )
        .expect("two non-empty fixture pages form a valid manifest root");

        let transaction = writer
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await?;
        for (page, value) in [(0, first_page), (1, second_page)] {
            transaction.put(
                IndexKey::Data {
                    scope,
                    kind: crate::encoding::v2::keys::ScopedKey::TextManifestPage(
                        crate::encoding::v2::keys::TextManifestPageKey {
                            root: root_typed,
                            page,
                        },
                    ),
                }
                .to_bytes(),
                crate::encoding::v2::values::encode_manifest_page(&value),
            )?;
        }
        transaction.put(
            root_key,
            crate::encoding::v2::values::encode_manifest_root(&next_root),
        )?;
        transaction.commit().await?;
        Ok(())
    }

    /// Installs one independently built donor split as a second Active manifest page.
    ///
    /// Production uses the full encoded page capacity. This test-only observer
    /// copies the donor's content-addressed object into the target namespace,
    /// then preserves both live split entries behind deterministic page
    /// boundaries for the otherwise enormous cross-page search/DROP race.
    pub async fn install_donor_split_for_search_race(
        &self,
        db: &HelixDB,
        donor: &HelixDB,
        scope: DataScope,
        definition: &ValidatedDynamicIndexDefinition,
    ) -> Result<Vec<[u8; 32]>> {
        let handles = db.active_index_handles_loaded(scope);
        let identity = definition.identity();
        let active = handles
            .iter()
            .find(|handle| handle.identity() == &identity)
            .ok_or_else(|| {
                HelixDbError::IndexNotFound(format!(
                    "active lifecycle test definition {:?}",
                    identity
                ))
            })?;
        let authority =
            crate::index_lifecycle::text::serving::ActiveTextServingAuthority::try_from_active(
                active,
            )?;
        let partition = work::TextPartition::Unpartitioned;
        let writer = db.lifecycle_test_writer_db()?;
        let root = crate::index_lifecycle::text::serving::load_active_manifest_root(
            writer, &authority, &partition,
        )
        .await?
        .ok_or_else(|| {
            HelixDbError::IndexCatalogCorruption(
                "unpartitioned search-race fixture has no manifest root".to_string(),
            )
        })?;
        if root.page_count() != 1 {
            return Err(HelixDbError::IndexCatalogCorruption(format!(
                "search-race fixture expected one source page, found {}",
                root.page_count()
            )));
        }
        let entries =
            crate::index_lifecycle::text::serving::load_active_manifest_page(writer, &root, 0)
                .await?;
        if entries.len() != 1 {
            return Err(HelixDbError::IndexCatalogCorruption(format!(
                "search-race target fixture needs exactly one split, found {}",
                entries.len()
            )));
        }
        let donor_handles = donor.active_index_handles_loaded(scope);
        let donor_active = donor_handles
            .iter()
            .find(|handle| handle.identity() == &identity)
            .ok_or_else(|| {
                HelixDbError::IndexNotFound(format!(
                    "active donor lifecycle test definition {:?}",
                    identity
                ))
            })?;
        let donor_authority =
            crate::index_lifecycle::text::serving::ActiveTextServingAuthority::try_from_active(
                donor_active,
            )?;
        let donor_writer = donor.lifecycle_test_writer_db()?;
        let donor_root = crate::index_lifecycle::text::serving::load_active_manifest_root(
            donor_writer,
            &donor_authority,
            &work::TextPartition::Unpartitioned,
        )
        .await?
        .ok_or_else(|| {
            HelixDbError::IndexCatalogCorruption(
                "unpartitioned donor search-race fixture has no manifest root".to_string(),
            )
        })?;
        if donor_root.page_count() != 1 {
            return Err(HelixDbError::IndexCatalogCorruption(format!(
                "search-race donor fixture expected one page, found {}",
                donor_root.page_count()
            )));
        }
        let donor_entries = crate::index_lifecycle::text::serving::load_active_manifest_page(
            donor_writer,
            &donor_root,
            0,
        )
        .await?;
        let [donor_entry] = donor_entries.as_slice() else {
            return Err(HelixDbError::IndexCatalogCorruption(format!(
                "search-race donor fixture needs exactly one split, found {}",
                donor_entries.len()
            )));
        };
        let donor_entry = *donor_entry;
        if donor_entry.blob() == entries[0].blob() {
            return Err(HelixDbError::IndexCatalogCorruption(
                "search-race donor and target unexpectedly produced the same blob".to_string(),
            ));
        }
        let donor_blob_path =
            crate::search::text::blob_object_store_path(donor.path(), *donor_entry.blob().hash());
        let target_blob_path =
            crate::search::text::blob_object_store_path(db.path(), *donor_entry.blob().hash());
        db.object_store()
            .copy(&donor_blob_path, &target_blob_path)
            .await?;

        let root_typed = crate::encoding::v2::keys::TextManifestRootKey {
            index_id: authority.index_id(),
            generation: authority.generation(),
            partition: partition.fingerprint(),
        };
        let root_key = IndexKey::Data {
            scope,
            kind: crate::encoding::v2::keys::ScopedKey::TextManifestRoot(root_typed),
        }
        .to_bytes();
        let root_bytes = writer.get(&root_key).await?.ok_or_else(|| {
            HelixDbError::IndexCatalogCorruption(
                "search-race fixture lost its manifest root".to_string(),
            )
        })?;
        let root_value = crate::encoding::v2::values::decode_manifest_root(&root_bytes)?;
        let first_page = work::TextManifestPageValue::try_new(
            authority.index_id(),
            authority.generation(),
            partition.clone(),
            0,
            vec![entries[0]],
        )
        .expect("one existing split forms a valid first page");
        let second_page = work::TextManifestPageValue::try_new(
            authority.index_id(),
            authority.generation(),
            partition.clone(),
            1,
            vec![donor_entry],
        )
        .expect("the donor split forms a valid second page");
        let next_root = work::TextManifestRootValue::try_new(
            authority.index_id(),
            authority.generation(),
            partition,
            root_value.revision().checked_next().map_err(|_| {
                HelixDbError::IndexCatalogCorruption(
                    "search-race fixture manifest revision is exhausted".to_string(),
                )
            })?,
            2,
            2,
        )
        .expect("two non-empty fixture pages form a valid manifest root");

        let transaction = writer
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await?;
        for (page, value) in [(0, first_page), (1, second_page)] {
            let key = IndexKey::Data {
                scope,
                kind: crate::encoding::v2::keys::ScopedKey::TextManifestPage(
                    crate::encoding::v2::keys::TextManifestPageKey {
                        root: root_typed,
                        page,
                    },
                ),
            }
            .to_bytes();
            transaction.put(
                key,
                crate::encoding::v2::values::encode_manifest_page(&value),
            )?;
        }
        transaction.put(
            root_key,
            crate::encoding::v2::values::encode_manifest_root(&next_root),
        )?;
        transaction.commit().await?;
        Ok(vec![*donor_entry.blob().hash()])
    }

    /// Atomically enqueues one definition against the current graph watermark.
    pub async fn create_index(
        &self,
        db: &HelixDB,
        scope: DataScope,
        definition: ValidatedDynamicIndexDefinition,
        mode: helix_planner::ir::IndexCreateMode,
    ) -> Result<IndexDdlReceipt> {
        let writer = db.lifecycle_test_writer_db()?;
        let receipt =
            crate::index_lifecycle::lifecycle::create_index_operation_from_current_source(
                writer, scope, definition, mode,
            )
            .await?;
        if db.inner.lifecycle_test_scheduling != IndexLifecycleScheduling::ExplicitOnly {
            db.notify_index_worker();
        }
        Ok(receipt)
    }

    /// Atomically starts active cleanup or converts a build into abort cleanup.
    pub async fn drop_index(
        &self,
        db: &HelixDB,
        scope: DataScope,
        definition: &ValidatedDynamicIndexDefinition,
    ) -> Result<IndexDdlReceipt> {
        let _catalog_permit = db
            .inner
            .index_scope_gates
            .catalog_change_permit(scope)
            .await;
        let receipt = crate::index_lifecycle::lifecycle::drop_index_operation(
            db.lifecycle_test_writer_db()?,
            scope,
            definition,
        )
        .await?;
        if db.inner.lifecycle_test_scheduling != IndexLifecycleScheduling::ExplicitOnly {
            db.notify_index_worker();
        }
        Ok(receipt)
    }
}

impl HelixDB {
    /// Opens a writer with lifecycle scheduling controls.
    ///
    /// `Explicit` changes only runtime scheduling. It does not change or write a
    /// scheduling marker into the database.
    pub async fn open_for_index_lifecycle_testing(
        source: HelixDbSource,
        config: DbConfig,
        scheduling: LifecycleTestScheduling,
    ) -> Result<Self> {
        let (path, object_store) = source.into_parts()?;
        Self::open_writer_inner_with_index_scheduling(
            path,
            object_store,
            config,
            WriterOpenMode::Embedded,
            scheduling.internal(),
        )
        .await
    }

    /// Opens a lifecycle writer over a caller-provided fault-injectable store.
    ///
    /// The supplied object store remains the only persistence backend. This
    /// entry point adds explicit scheduling authority but no alternate
    /// database encoding.
    pub async fn open_with_object_store_for_index_lifecycle_testing(
        database: impl Into<String>,
        object_store: Arc<dyn slatedb::object_store::ObjectStore>,
        config: DbConfig,
        scheduling: LifecycleTestScheduling,
    ) -> Result<Self> {
        Self::open_writer_inner_with_index_scheduling(
            database.into(),
            object_store,
            config,
            WriterOpenMode::Embedded,
            scheduling.internal(),
        )
        .await
    }

    async fn advance_index_lifecycle_lifecycle_test_work(
        &self,
        target: LifecycleWorkTarget,
        now_unix_millis: Option<u64>,
    ) -> Result<LifecycleStepEvidence> {
        if self.mode() != HelixDbMode::Writer {
            return Err(HelixDbError::WriterModeRequired {
                actual: self.mode().as_str(),
            });
        }
        if self.inner.lifecycle_test_scheduling != IndexLifecycleScheduling::ExplicitOnly {
            return Err(HelixDbError::Config(
                "explicit lifecycle stepping requires explicit test scheduling".to_string(),
            ));
        }
        match target {
            LifecycleWorkTarget::Operation {
                scope,
                operation_id,
            } => {
                self.advance_lifecycle_operation(target, scope, operation_id, now_unix_millis)
                    .await
            }
        }
    }

    async fn discover_index_lifecycle_lifecycle_test_work(
        &self,
        per_lane_limit: NonZeroUsize,
    ) -> Result<LifecycleWorkPage> {
        if self.mode() != HelixDbMode::Writer {
            return Err(HelixDbError::WriterModeRequired {
                actual: self.mode().as_str(),
            });
        }
        if self.inner.lifecycle_test_scheduling != IndexLifecycleScheduling::ExplicitOnly {
            return Err(HelixDbError::Config(
                "explicit lifecycle discovery requires explicit test scheduling".to_string(),
            ));
        }
        let db = self.lifecycle_test_writer_db()?;
        let operation_page = outbox::scan_operation_queue_page(
            db,
            None,
            outbox::OperationQueuePageSize::new(per_lane_limit.get())?,
        )
        .await?;
        let mut targets = Vec::with_capacity(operation_page.operation_ids.len());
        for operation_id in operation_page.operation_ids {
            let Some((scope, _operation)) = outbox::read_queued_operation(db, operation_id).await?
            else {
                continue;
            };
            targets.push(LifecycleWorkTarget::Operation {
                scope,
                operation_id,
            });
        }
        Ok(LifecycleWorkPage {
            targets,
            exhausted: operation_page.prefix_exhausted,
        })
    }

    async fn inspect_index_lifecycle_lifecycle_test_work(
        &self,
        target: LifecycleWorkTarget,
    ) -> Result<LifecycleCheckpoint> {
        if self.inner.lifecycle_test_scheduling != IndexLifecycleScheduling::ExplicitOnly {
            return Err(HelixDbError::Config(
                "explicit lifecycle inspection requires explicit test scheduling".to_string(),
            ));
        }
        let db = self.lifecycle_test_writer_db()?;
        match target {
            LifecycleWorkTarget::Operation {
                scope,
                operation_id,
            } => {
                self.operation_checkpoint_or_absent(db, scope, operation_id)
                    .await
            }
        }
    }

    async fn advance_lifecycle_operation(
        &self,
        target: LifecycleWorkTarget,
        scope: DataScope,
        operation_id: IndexOperationId,
        logical_now_unix_millis: Option<u64>,
    ) -> Result<LifecycleStepEvidence> {
        let started = Instant::now();
        let db = self.lifecycle_test_writer_db()?;
        let Some(before_record) = outbox::read_operation(db, scope, operation_id).await? else {
            return Err(HelixDbError::IndexOperationNotFound {
                operation_id: operation_id.as_uuid().to_string(),
            });
        };
        let before = operation_checkpoint(&before_record);
        if matches!(
            before_record.execution_state(),
            IndexOperationExecutionState::Blocked(_)
        ) {
            return Ok(step_evidence(
                target,
                before,
                LifecycleStepOutcome::Blocked,
                before,
                started,
                LifecycleStepResourceUsage::default(),
            ));
        }
        if matches!(
            before_record.execution_state(),
            IndexOperationExecutionState::Completed(_)
        ) {
            return Ok(step_evidence(
                target,
                before,
                LifecycleStepOutcome::AlreadyTerminal,
                before,
                started,
                LifecycleStepResourceUsage::default(),
            ));
        }

        let writer_epoch = self.lifecycle_test_writer_epoch().await?;
        let now_unix_millis = logical_now_unix_millis.unwrap_or_else(now_unix_millis);
        let observation =
            outbox::observe_operation_pointer(db, operation_id, writer_epoch, now_unix_millis)
                .await?;
        let (eligible, permission) = match observation {
            OperationPointerObservation::Eligible(eligible) => (eligible, ClaimPermission::Normal),
            OperationPointerObservation::ClaimedByCurrentWriter(eligible) => (
                eligible,
                ClaimPermission::SameEpochRecovery(outbox::SameEpochRecoveryProof::after_join(
                    writer_epoch,
                )),
            ),
            OperationPointerObservation::Delayed { delay_millis } => {
                return Ok(step_evidence(
                    target,
                    before,
                    LifecycleStepOutcome::Delayed { delay_millis },
                    before,
                    started,
                    LifecycleStepResourceUsage::default(),
                ));
            }
            OperationPointerObservation::StalePointerRemoved => {
                return Ok(step_evidence(
                    target,
                    before,
                    LifecycleStepOutcome::StalePointerRemoved,
                    LifecycleCheckpoint::Absent,
                    started,
                    LifecycleStepResourceUsage::default(),
                ));
            }
        };
        let Some((driver, limits)) = self
            .inner
            .index_capabilities
            .explicit_driver(eligible.record.family())
        else {
            return Err(HelixDbError::InvariantViolation(
                "explicit lifecycle operation has no installed family driver".to_string(),
            ));
        };
        let Some(claimed) = outbox::claim_operation(
            db,
            &eligible,
            writer_epoch,
            self.inner.index_claim_sequences.next()?,
            now_unix_millis,
            permission,
        )
        .await?
        else {
            let after = self
                .operation_checkpoint_or_absent(db, scope, operation_id)
                .await?;
            return Ok(step_evidence(
                target,
                before,
                LifecycleStepOutcome::Idle,
                after,
                started,
                resource_delta(before, after)?,
            ));
        };
        let committed = outbox::execute_claimed_step_with_evidence(
            db,
            &claimed,
            driver.as_ref(),
            limits,
            now_unix_millis,
        )
        .await?;
        let outcome = match committed.outcome {
            outbox::CommittedOperationStep::Progressed => LifecycleStepOutcome::Progressed,
            outbox::CommittedOperationStep::TransientFailure => {
                LifecycleStepOutcome::TransientFailure
            }
            outbox::CommittedOperationStep::Blocked => LifecycleStepOutcome::Blocked,
            outbox::CommittedOperationStep::Completed => LifecycleStepOutcome::Completed,
        };
        let after = self
            .operation_checkpoint_or_absent(db, scope, operation_id)
            .await?;
        Ok(step_evidence(
            target,
            before,
            outcome,
            after,
            started,
            committed.resources.into(),
        ))
    }

    fn lifecycle_test_writer_db(&self) -> Result<&slatedb::Db> {
        let HelixStorage::Writer(writer) = self.storage() else {
            return Err(HelixDbError::WriterModeRequired {
                actual: self.mode().as_str(),
            });
        };
        Ok(writer.db())
    }

    async fn lifecycle_test_writer_epoch(&self) -> Result<crate::index_lifecycle::WriterEpoch> {
        let worker = self.inner.index_worker.lock().await;
        let Some(worker) = worker.as_ref() else {
            return Err(HelixDbError::InvariantViolation(
                "explicit lifecycle controller has no worker epoch".to_string(),
            ));
        };
        Ok(worker.writer_epoch())
    }

    async fn operation_checkpoint_or_absent(
        &self,
        db: &slatedb::Db,
        scope: DataScope,
        operation_id: IndexOperationId,
    ) -> Result<LifecycleCheckpoint> {
        Ok(
            match outbox::read_operation(db, scope, operation_id).await? {
                Some(record) => operation_checkpoint(&record),
                None => LifecycleCheckpoint::Absent,
            },
        )
    }
}

fn operation_checkpoint(
    record: &crate::index_lifecycle::IndexOperationRecord,
) -> LifecycleCheckpoint {
    let status = IndexOperationStatus::from_record(record);
    LifecycleCheckpoint::Present {
        stage: LifecycleStage::Operation(status.common().stage),
        durable_revision: record.operation_revision().get(),
        progress: status.common().progress,
    }
}

fn resource_delta(
    before: LifecycleCheckpoint,
    after: LifecycleCheckpoint,
) -> Result<LifecycleStepResourceUsage> {
    let (
        LifecycleCheckpoint::Present {
            progress: before, ..
        },
        LifecycleCheckpoint::Present {
            progress: after, ..
        },
    ) = (before, after)
    else {
        return Ok(LifecycleStepResourceUsage::default());
    };
    Ok(LifecycleStepResourceUsage {
        entities: after.entities.checked_sub(before.entities).ok_or_else(|| {
            HelixDbError::InvariantViolation(
                "lifecycle entity counter regressed across one step".to_string(),
            )
        })?,
        input_bytes: after
            .input_bytes
            .checked_sub(before.input_bytes)
            .ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "lifecycle input-byte counter regressed across one step".to_string(),
                )
            })?,
        output_operations: after
            .output_operations
            .checked_sub(before.output_operations)
            .ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "lifecycle output-operation counter regressed across one step".to_string(),
                )
            })?,
        output_bytes: after
            .output_bytes
            .checked_sub(before.output_bytes)
            .ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "lifecycle output-byte counter regressed across one step".to_string(),
                )
            })?,
        ..LifecycleStepResourceUsage::default()
    })
}

impl From<outbox::StepResourceUsage> for LifecycleStepResourceUsage {
    fn from(value: outbox::StepResourceUsage) -> Self {
        Self {
            entities: value.source_entities,
            input_bytes: value.input_bytes,
            output_operations: value.physical_operations,
            output_bytes: value.output_bytes,
            vector_output_bytes: value.single_vector_output_bytes,
            text_artifact_bytes: value.text_artifact_bytes,
            text_upload_bytes: value.text_upload_bytes,
            text_compaction_fan_in: value.compaction_fan_in,
            text_compaction_input_bytes: value.compaction_input_bytes,
            text_temporary_bytes: value.temporary_bytes,
            text_manifest_page_bytes: value.manifest_page_bytes,
            text_manifest_root_bytes: value.manifest_root_bytes,
            vector_planning: LifecycleVectorPlanningUsage {
                planning_executions: value.vector_planning.planning_executions,
                planned_writes: value.vector_planning.planned_writes,
                replay_executions: value.vector_planning.replay_executions,
                item_hits: value.vector_planning.item_hits,
                item_misses: value.vector_planning.item_misses,
                neighbor_hits: value.vector_planning.neighbor_hits,
                neighbor_misses: value.vector_planning.neighbor_misses,
                simhash_hits: value.vector_planning.simhash_hits,
                simhash_misses: value.vector_planning.simhash_misses,
                item_evictions: value.vector_planning.item_evictions,
                neighbor_evictions: value.vector_planning.neighbor_evictions,
                simhash_evictions: value.vector_planning.simhash_evictions,
                dirty_neighbor_flushes: value.vector_planning.dirty_neighbor_flushes,
                retained_payload_bytes: value.vector_planning.retained_payload_bytes,
            },
        }
    }
}

fn step_evidence(
    target: LifecycleWorkTarget,
    before: LifecycleCheckpoint,
    outcome: LifecycleStepOutcome,
    after: LifecycleCheckpoint,
    started: Instant,
    resources: LifecycleStepResourceUsage,
) -> LifecycleStepEvidence {
    LifecycleStepEvidence {
        target,
        before,
        outcome,
        after,
        resources,
        elapsed_micros: u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
    }
}

fn now_unix_millis() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::config::{SecondaryIndexDefinition, ValidatedDynamicIndexDefinition};
    use crate::index_lifecycle::lifecycle;

    #[test]
    fn scheduling_names_are_stable() {
        assert_eq!(LifecycleTestScheduling::Automatic.as_str(), "automatic");
        assert_eq!(LifecycleTestScheduling::Explicit.as_str(), "explicit");
    }

    #[test]
    fn operation_resource_delta_is_checked_and_exact() {
        let checkpoint =
            |entities, input_bytes, output_operations, output_bytes| LifecycleCheckpoint::Present {
                stage: LifecycleStage::Operation(IndexOperationStage::Scan),
                durable_revision: 1,
                progress: IndexOperationPublicProgress {
                    entities,
                    input_bytes,
                    output_operations,
                    output_bytes,
                },
            };
        assert_eq!(
            resource_delta(checkpoint(1, 2, 3, 4), checkpoint(5, 8, 13, 20)).unwrap(),
            LifecycleStepResourceUsage {
                entities: 4,
                input_bytes: 6,
                output_operations: 10,
                output_bytes: 16,
                ..LifecycleStepResourceUsage::default()
            }
        );
        assert!(resource_delta(checkpoint(2, 2, 2, 2), checkpoint(1, 2, 2, 2)).is_err());
    }

    #[test]
    fn absent_checkpoints_do_not_invent_resource_usage() {
        assert_eq!(
            resource_delta(LifecycleCheckpoint::Absent, LifecycleCheckpoint::Absent).unwrap(),
            LifecycleStepResourceUsage::default()
        );
    }

    #[test]
    fn crash_matrix_inputs_are_closed_and_duplicate_free() {
        let failpoints = index_outbox_failpoint_names();
        assert_eq!(failpoints.len(), 16);
        assert_eq!(failpoints.into_iter().collect::<BTreeSet<_>>().len(), 16);

        let stages = LifecycleStage::all();
        assert_eq!(stages.len(), LIFECYCLE_STAGE_COUNT);
        assert_eq!(LIFECYCLE_STAGE_COUNT, 22);
        assert_eq!(
            stages
                .iter()
                .map(|stage| format!("{stage:?}"))
                .collect::<BTreeSet<_>>()
                .len(),
            stages.len()
        );
    }

    #[test]
    fn automatic_metrics_use_fixed_saturating_aggregates() {
        let metrics = Arc::new(AutomaticLifecycleMetrics::new());
        metrics.observe_operation(outbox::CommittedOperationStepEvidence {
            outcome: outbox::CommittedOperationStep::Progressed,
            before_stage: IndexOperationStage::Scan,
            after_stage: IndexOperationStage::CatchUp,
            resources: outbox::StepResourceUsage {
                source_entities: 4,
                input_bytes: 8,
                single_vector_output_bytes: 12,
                vector_planning: outbox::VectorPlanningUsage {
                    planning_executions: 4,
                    planned_writes: 20,
                    item_hits: 7,
                    item_misses: 3,
                    retained_payload_bytes: 1_024,
                    ..outbox::VectorPlanningUsage::default()
                },
                ..outbox::StepResourceUsage::default()
            },
            elapsed_micros: 750,
        });
        let secondary = metrics.begin_worker_task(AutomaticLifecycleTaskKind::Secondary);
        let vector = metrics.begin_worker_task(AutomaticLifecycleTaskKind::Vector);
        drop(vector);
        drop(secondary);
        metrics.observe_worker_claim_conflict();
        metrics.observe_worker_failure();
        metrics.observe_worker_panic();
        metrics.observe_worker_restart();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.lifecycle_steps, 1);
        assert_eq!(snapshot.operation_steps, 1);
        assert_eq!(snapshot.resource_totals.entities, 4);
        assert_eq!(snapshot.resource_maxima.vector_output_bytes, 12);
        assert_eq!(snapshot.vector_planning_totals.planning_executions, 4);
        assert_eq!(snapshot.vector_planning_totals.planned_writes, 20);
        assert_eq!(
            snapshot.vector_planning_maxima.retained_payload_bytes,
            1_024
        );
        assert_eq!(snapshot.vector_planning_totals.replay_executions, 0);
        assert_eq!(snapshot.worker.max_in_flight, 2);
        assert_eq!(snapshot.worker.max_secondary_in_flight, 1);
        assert_eq!(snapshot.worker.max_vector_in_flight, 1);
        assert_eq!(snapshot.worker.claim_conflicts, 1);
        assert_eq!(snapshot.worker.failures, 1);
        assert_eq!(snapshot.worker.panics, 1);
        assert_eq!(snapshot.worker.restarts, 1);
        assert_eq!(snapshot.latency_bucket_counts.iter().sum::<u64>(), 1);
        assert_eq!(snapshot.stage_transitions.len(), 1);
    }

    #[test]
    fn fixed_metrics_matrix_covers_every_lifecycle_stage() {
        let stages = LifecycleStage::all();
        assert_eq!(stages.len(), LIFECYCLE_STAGE_COUNT);
        for (index, stage) in stages.into_iter().enumerate() {
            assert_eq!(lifecycle_stage_index(stage), index);
            assert_eq!(lifecycle_stage_from_index(index), stage);
        }
    }

    #[tokio::test]
    async fn explicit_controller_is_the_only_driver_for_an_explicit_handle() {
        let db = HelixDB::open_for_index_lifecycle_testing(
            HelixDbSource::InMemory {
                database: "explicit-lifecycle-controller".to_string(),
            },
            DbConfig::new(),
            LifecycleTestScheduling::Explicit,
        )
        .await
        .unwrap();
        let definition = ValidatedDynamicIndexDefinition::try_from(
            SecondaryIndexDefinition::node_equality("User", "email").unwrap(),
        )
        .unwrap();
        let receipt = lifecycle::create_index_operation_from_current_source(
            db.lifecycle_test_writer_db().unwrap(),
            DataScope::LegacyUnscoped,
            definition,
            helix_planner::ir::IndexCreateMode::IfNotExists,
        )
        .await
        .unwrap();
        let operation_id = receipt.operation_id().unwrap();
        tokio::task::yield_now().await;
        assert!(matches!(
            db.get_index_operation(DataScope::LegacyUnscoped, operation_id)
                .await
                .unwrap(),
            IndexOperationStatus::Queued { .. }
        ));

        let controller = LifecycleTestController::new();
        let target = LifecycleWorkTarget::Operation {
            scope: DataScope::LegacyUnscoped,
            operation_id,
        };
        assert_eq!(
            controller
                .discover(&db, NonZeroUsize::MIN)
                .await
                .unwrap()
                .targets,
            vec![target]
        );
        let mut completed = false;
        for _ in 0..16 {
            let evidence = controller.advance(&db, target).await.unwrap();
            assert_eq!(evidence.target, target);
            assert!(evidence.elapsed_micros > 0);
            if evidence.outcome == LifecycleStepOutcome::Completed {
                completed = true;
                break;
            }
        }
        assert!(completed, "empty secondary build must converge");
        assert!(matches!(
            db.get_index_operation(DataScope::LegacyUnscoped, operation_id)
                .await
                .unwrap(),
            IndexOperationStatus::Succeeded { .. }
        ));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn automatic_handles_reject_explicit_dispatch() {
        let db = HelixDB::open_for_index_lifecycle_testing(
            HelixDbSource::InMemory {
                database: "automatic-lifecycle-controller-guard".to_string(),
            },
            DbConfig::new(),
            LifecycleTestScheduling::Automatic,
        )
        .await
        .unwrap();
        let error = LifecycleTestController::new()
            .advance(
                &db,
                LifecycleWorkTarget::Operation {
                    scope: DataScope::LegacyUnscoped,
                    operation_id: IndexOperationId::new_v4(),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(error, HelixDbError::Config(_)));
        db.close().await.unwrap();
    }
}
