//! Transactional repository and driver boundary for the durable index outbox.
//!
//! The scoped operation row is authoritative. Its global row contains only the
//! scope and exact operation revision needed for tenant-independent discovery.
//! Every mutation reads the canonical record, operation, and pointer in a
//! serializable transaction before staging an exact next revision.
//!
//! Family drivers receive both the database used for disposable planning reads
//! and the [`DbTransaction`] that persists their next checkpoint. Only writes
//! staged in that repository-owned transaction can commit with recoverable
//! operation progress.

use std::num::{NonZeroU64, NonZeroUsize};
use std::ops::Bound;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use slatedb::{Db, DbReadOps, DbTransaction, IsolationLevel};

use crate::config::SearchIndexBatchLimits;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::ManagedIndexKey;
use crate::encoding::v2::keys::{GlobalKey, GlobalKind, RecordKind, ScopedKey};
use crate::encoding::v2::values::{
    decode_index_record, decode_metadata_value, decode_operation_record,
    decode_operation_record_with_compatibility, encode_index_record, encode_metadata_value,
    encode_operation_record,
};
use crate::error::{HelixDbError, Result};
use crate::execution_control;

use super::failpoints::{self, IndexOutboxFailpoint};
use super::{
    BuildOperationOutcome, ClaimSequence, IndexOperationBlocker, IndexOperationExecutionState,
    IndexOperationFamily, IndexOperationId, IndexOperationOutcome, IndexOperationProgress,
    IndexOperationRecord, IndexOperationRevision, IndexOperationStage, IndexOperationStatus,
    IndexRecordV2, IndexRevision, IndexStateTransition, IndexStateV2, IndexV2MetadataValue,
    OperationClaim, OperationQueuePointerValue, WriterEpoch,
};

/// Maximum scheduling delay used for one observation of a persisted deadline.
pub(crate) const MAX_OPERATION_BACKOFF_MILLIS: u64 = 30_000;
const BASE_OPERATION_BACKOFF_MILLIS: u64 = 1_000;

/// Checked bounded page size for fair global-pointer scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OperationQueuePageSize(NonZeroUsize);

impl OperationQueuePageSize {
    /// Builds a non-zero page size.
    pub(crate) fn new(value: usize) -> Result<Self> {
        NonZeroUsize::new(value).map(Self).ok_or_else(|| {
            HelixDbError::InvariantViolation(
                "index operation queue page size must be non-zero".to_string(),
            )
        })
    }

    pub(crate) const fn get(self) -> usize {
        self.0.get()
    }
}

/// Exact canonical precondition for atomically beginning a later operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExpectedCanonicalRevision {
    /// No logical index owns this identity yet.
    Absent,
    /// The existing canonical row must have this revision.
    Exact(IndexRevision),
}

/// One bounded page and the cursor needed to continue its cyclic scan.
#[derive(Debug)]
pub(crate) struct OperationQueuePage {
    pub(crate) operation_ids: Vec<IndexOperationId>,
    pub(crate) resume_after: Option<IndexOperationId>,
    pub(crate) prefix_exhausted: bool,
}

/// Exact operation observation safe to pass to the claim repository method.
#[derive(Debug, Clone)]
pub(crate) struct EligibleOperation {
    pub(crate) scope: DataScope,
    pub(crate) record: IndexOperationRecord,
}

/// Result of resolving one global pointer without claiming it.
#[derive(Debug, Clone)]
pub(crate) enum OperationPointerObservation {
    /// Queued work or a claim owned by a fenced prior writer.
    Eligible(EligibleOperation),
    /// Work is queued but its bounded scheduling delay has not elapsed.
    Delayed {
        /// Monotonic sleep duration derived for this observation only.
        delay_millis: u64,
    },
    /// This writer already owns the claim; only supervised restart may replace it.
    ClaimedByCurrentWriter(EligibleOperation),
    /// The pointer disappeared or an orphan pointer was removed idempotently.
    StalePointerRemoved,
}

/// Proof that a supervisor joined the previous task in the same writer epoch.
///
/// Construction is private to the worker supervisor. Persisted data can never
/// manufacture this authority.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SameEpochRecoveryProof {
    writer_epoch: WriterEpoch,
}

impl SameEpochRecoveryProof {
    pub(crate) const fn after_join(writer_epoch: WriterEpoch) -> Self {
        Self { writer_epoch }
    }
}

/// Repository authorization for replacing a durable claim.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ClaimPermission {
    /// Queued work or a claim belonging to a fenced prior writer.
    Normal,
    /// Same-writer recovery after the prior task has been joined.
    SameEpochRecovery(SameEpochRecoveryProof),
}

/// Exact claimed operation returned after its claim transaction commits.
#[derive(Debug, Clone)]
pub(crate) struct ClaimedOperation {
    pub(crate) scope: DataScope,
    pub(crate) record: IndexOperationRecord,
}

/// Closed family-driver result for one bounded claimed step.
#[derive(Debug, Clone)]
pub(crate) enum IndexOperationStepResult {
    /// Physical work and the supplied next checkpoint commit together.
    Progressed(IndexOperationProgress),
    /// Physical work and checkpoint commit together, then wait before reclaiming.
    ProgressedAfter {
        progress: IndexOperationProgress,
        delay_millis: NonZeroU64,
    },
    /// No physical work commits; the exact checkpoint is durably backed off.
    TransientFailure,
    /// No further automatic retry is legal until an explicit retry/abort.
    Blocked(IndexOperationBlocker),
    /// The canonical lifecycle state and terminal operation commit together.
    Completed(IndexOperationOutcome),
}

/// Non-persisted vector planner/cache measurements from one bounded step.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct VectorPlanningUsage {
    pub(crate) planning_executions: u64,
    pub(crate) planned_writes: u64,
    pub(crate) replay_executions: u64,
    pub(crate) item_hits: u64,
    pub(crate) item_misses: u64,
    pub(crate) neighbor_hits: u64,
    pub(crate) neighbor_misses: u64,
    pub(crate) simhash_hits: u64,
    pub(crate) simhash_misses: u64,
    pub(crate) item_evictions: u64,
    pub(crate) neighbor_evictions: u64,
    pub(crate) simhash_evictions: u64,
    pub(crate) dirty_neighbor_flushes: u64,
    pub(crate) retained_payload_bytes: u64,
}

impl VectorPlanningUsage {
    fn saturating_add(self, other: Self) -> Self {
        Self {
            planning_executions: self
                .planning_executions
                .saturating_add(other.planning_executions),
            planned_writes: self.planned_writes.saturating_add(other.planned_writes),
            replay_executions: self
                .replay_executions
                .saturating_add(other.replay_executions),
            item_hits: self.item_hits.saturating_add(other.item_hits),
            item_misses: self.item_misses.saturating_add(other.item_misses),
            neighbor_hits: self.neighbor_hits.saturating_add(other.neighbor_hits),
            neighbor_misses: self.neighbor_misses.saturating_add(other.neighbor_misses),
            simhash_hits: self.simhash_hits.saturating_add(other.simhash_hits),
            simhash_misses: self.simhash_misses.saturating_add(other.simhash_misses),
            item_evictions: self.item_evictions.saturating_add(other.item_evictions),
            neighbor_evictions: self
                .neighbor_evictions
                .saturating_add(other.neighbor_evictions),
            simhash_evictions: self
                .simhash_evictions
                .saturating_add(other.simhash_evictions),
            dirty_neighbor_flushes: self
                .dirty_neighbor_flushes
                .saturating_add(other.dirty_neighbor_flushes),
            retained_payload_bytes: self
                .retained_payload_bytes
                .max(other.retained_payload_bytes),
        }
    }
}

/// Non-persisted resources consumed by one bounded family-driver step.
///
/// These measurements travel beside the durable result and are deliberately
/// absent from operation encoding. They let lifecycle tests and automatic
/// worker telemetry prove configured bounds without changing database bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct StepResourceUsage {
    pub(crate) source_entities: u64,
    pub(crate) input_bytes: u64,
    pub(crate) physical_operations: u64,
    pub(crate) output_bytes: u64,
    pub(crate) single_vector_output_bytes: u64,
    pub(crate) text_artifact_bytes: u64,
    pub(crate) text_upload_bytes: u64,
    pub(crate) compaction_fan_in: u64,
    pub(crate) compaction_input_bytes: u64,
    pub(crate) temporary_bytes: u64,
    pub(crate) manifest_page_bytes: u64,
    pub(crate) manifest_root_bytes: u64,
    pub(crate) vector_planning: VectorPlanningUsage,
}

impl StepResourceUsage {
    /// Adds independent measurements from repository and prepared-I/O work.
    pub(crate) fn saturating_add(self, other: Self) -> Self {
        Self {
            source_entities: self.source_entities.saturating_add(other.source_entities),
            input_bytes: self.input_bytes.saturating_add(other.input_bytes),
            physical_operations: self
                .physical_operations
                .saturating_add(other.physical_operations),
            output_bytes: self.output_bytes.saturating_add(other.output_bytes),
            single_vector_output_bytes: self
                .single_vector_output_bytes
                .max(other.single_vector_output_bytes),
            text_artifact_bytes: self
                .text_artifact_bytes
                .saturating_add(other.text_artifact_bytes),
            text_upload_bytes: self
                .text_upload_bytes
                .saturating_add(other.text_upload_bytes),
            compaction_fan_in: self
                .compaction_fan_in
                .saturating_add(other.compaction_fan_in),
            compaction_input_bytes: self
                .compaction_input_bytes
                .saturating_add(other.compaction_input_bytes),
            temporary_bytes: self.temporary_bytes.max(other.temporary_bytes),
            manifest_page_bytes: self
                .manifest_page_bytes
                .saturating_add(other.manifest_page_bytes),
            manifest_root_bytes: self
                .manifest_root_bytes
                .saturating_add(other.manifest_root_bytes),
            vector_planning: self.vector_planning.saturating_add(other.vector_planning),
        }
    }
}

/// One driver result plus disposable measurements from the same bounded turn.
#[derive(Debug, Clone)]
pub(crate) struct IndexOperationStepExecution {
    result: IndexOperationStepResult,
    resources: StepResourceUsage,
}

impl IndexOperationStepExecution {
    /// Wraps an ordinary repository result with no family-specific resources.
    pub(crate) const fn new(result: IndexOperationStepResult) -> Self {
        Self {
            result,
            resources: StepResourceUsage {
                source_entities: 0,
                input_bytes: 0,
                physical_operations: 0,
                output_bytes: 0,
                single_vector_output_bytes: 0,
                text_artifact_bytes: 0,
                text_upload_bytes: 0,
                compaction_fan_in: 0,
                compaction_input_bytes: 0,
                temporary_bytes: 0,
                manifest_page_bytes: 0,
                manifest_root_bytes: 0,
                vector_planning: VectorPlanningUsage {
                    planning_executions: 0,
                    planned_writes: 0,
                    replay_executions: 0,
                    item_hits: 0,
                    item_misses: 0,
                    neighbor_hits: 0,
                    neighbor_misses: 0,
                    simhash_hits: 0,
                    simhash_misses: 0,
                    item_evictions: 0,
                    neighbor_evictions: 0,
                    simhash_evictions: 0,
                    dirty_neighbor_flushes: 0,
                    retained_payload_bytes: 0,
                },
            },
        }
    }

    /// Attaches family-specific measurements made while preparing the step.
    pub(crate) const fn with_resources(mut self, resources: StepResourceUsage) -> Self {
        self.resources = resources;
        self
    }
}

/// Family-specific physical work contract.
pub(crate) trait IndexOperationStepPermit: Send + Sync {}

impl<T: Send + Sync> IndexOperationStepPermit for T {}

/// Complete runtime preparation retained across one operation commit.
///
/// Ordinary family work carries only its execution permit. Text partition
/// construction instead carries a closed prepared upload value whose split was
/// built and uploaded before the repository transaction. Keeping this
/// distinction in an enum prevents a text upload from being mistaken for
/// ordinary work that does not require transactional attachment.
pub(crate) enum PreparedIndexOperationStep {
    DriverOwned {
        family: IndexOperationFamily,
        _permit: Box<dyn IndexOperationStepPermit>,
    },
    Secondary {
        _permit: Box<dyn IndexOperationStepPermit>,
        prepared: Box<super::secondary::PreparedSecondaryOperationStep>,
    },
    Text(Box<super::text::driver::PreparedTextOperationStep>),
}

impl PreparedIndexOperationStep {
    /// Wraps the existing permit-only family path.
    pub(crate) fn driver_owned(
        family: IndexOperationFamily,
        permit: Box<dyn IndexOperationStepPermit>,
    ) -> Self {
        Self::DriverOwned {
            family,
            _permit: permit,
        }
    }

    /// Wraps one prepared text operation step.
    pub(crate) fn text(step: super::text::driver::PreparedTextOperationStep) -> Self {
        Self::Text(Box::new(step))
    }

    /// Retains a bounded exact-key secondary batch through its transaction.
    pub(crate) fn secondary(
        permit: Box<dyn IndexOperationStepPermit>,
        step: super::secondary::PreparedSecondaryOperationStep,
    ) -> Self {
        Self::Secondary {
            _permit: permit,
            prepared: Box::new(step),
        }
    }

    /// Returns the only family authorized to consume this preparation.
    pub(crate) fn family(&self) -> IndexOperationFamily {
        match self {
            Self::DriverOwned { family, .. } => *family,
            Self::Secondary { .. } => IndexOperationFamily::Secondary,
            Self::Text(_) => IndexOperationFamily::Text,
        }
    }

    /// Stages the prepared family step into the repository-owned transaction.
    async fn stage(
        &self,
        driver: &dyn IndexOperationDriver,
        db: &Db,
        transaction: &DbTransaction,
        scope: DataScope,
        operation: &IndexOperationRecord,
        limits: SearchIndexBatchLimits,
    ) -> Result<IndexOperationStepExecution> {
        match self {
            Self::DriverOwned { .. } => {
                driver.step(db, transaction, scope, operation, limits).await
            }
            Self::Secondary { prepared, .. } => {
                prepared.stage(transaction, scope, operation, limits).await
            }
            Self::Text(step) => {
                let resources = step.resource_usage();
                Ok(IndexOperationStepExecution::new(
                    step.stage(transaction, scope, operation).await?,
                )
                .with_resources(resources))
            }
        }
    }

    /// Releases external preparation after proving no operation commit occurred.
    async fn discard(self) -> Result<()> {
        match self {
            Self::DriverOwned { .. } | Self::Secondary { .. } => Ok(()),
            Self::Text(step) => (*step).discard().await,
        }
    }

    /// Performs post-commit external work only after durability is proven.
    async fn after_commit(self) {
        if let Self::Text(step) = self {
            (*step).after_commit().await;
        }
    }
}

/// Family-specific physical work contract.
#[async_trait]
pub(crate) trait IndexOperationDriver: Send + Sync {
    /// Family this driver is authorized to mutate.
    fn family(&self) -> IndexOperationFamily;

    /// Acquires any family-owned coordination required before the step snapshot.
    ///
    /// The returned permit is retained through the repository transaction and
    /// commit. Drivers whose steps need no external coordination use the unit
    /// permit supplied by this default implementation.
    async fn acquire_step_permit(
        &self,
        _scope: DataScope,
        _operation: &IndexOperationRecord,
    ) -> Result<Box<dyn IndexOperationStepPermit>> {
        Ok(Box::new(()))
    }

    /// Prepares work that must happen before the repository transaction opens.
    ///
    /// The default retains the existing coordination permit. Text overrides
    /// this for split construction and publication reservation while keeping
    /// all other stages on the ordinary path.
    async fn prepare_step(
        &self,
        _db: &Db,
        scope: DataScope,
        operation: &IndexOperationRecord,
        _limits: SearchIndexBatchLimits,
    ) -> Result<PreparedIndexOperationStep> {
        let permit = self.acquire_step_permit(scope, operation).await?;
        Ok(PreparedIndexOperationStep::driver_owned(
            self.family(),
            permit,
        ))
    }

    /// Stages at most one bounded step into the repository-owned transaction.
    async fn step(
        &self,
        db: &Db,
        transaction: &DbTransaction,
        scope: DataScope,
        operation: &IndexOperationRecord,
        limits: SearchIndexBatchLimits,
    ) -> Result<IndexOperationStepExecution>;

    /// Applies disposable runtime cleanup only after the staged step is durable.
    ///
    /// This hook must not write database state. Its default is intentionally a
    /// no-op; vector cleanup uses it to forget a process-local retirement fence
    /// only after the canonical `Dropped` transition has committed.
    async fn after_commit(
        &self,
        _scope: DataScope,
        _index: &IndexRecordV2,
        _operation: &IndexOperationRecord,
        _committed: CommittedOperationStep,
    ) {
    }
}

/// Durable outcome of dispatching one claimed family step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommittedOperationStep {
    Progressed,
    TransientFailure,
    Blocked,
    Completed,
}

/// Non-persisted evidence for one committed or durably released operation step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CommittedOperationStepEvidence {
    pub(crate) outcome: CommittedOperationStep,
    pub(crate) before_stage: IndexOperationStage,
    pub(crate) after_stage: IndexOperationStage,
    pub(crate) resources: StepResourceUsage,
    pub(crate) elapsed_micros: u64,
}

struct StagedOperationStep {
    transaction: DbTransaction,
    index: IndexRecordV2,
    operation: IndexOperationRecord,
    committed: CommittedOperationStep,
    before_stage: IndexOperationStage,
    after_stage: IndexOperationStage,
    resources: StepResourceUsage,
}

/// Atomically stores the next canonical state, operation, and runnable pointer.
///
/// Starting a later operation deletes the exact prior terminal operation in
/// the same transaction, retaining at most one operation per logical index.
#[cfg(any(test, feature = "production-coverage"))]
pub(crate) async fn enqueue_operation(
    db: &Db,
    scope: DataScope,
    expected: ExpectedCanonicalRevision,
    next_index: &IndexRecordV2,
    operation: &IndexOperationRecord,
) -> Result<()> {
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    stage_operation(&transaction, scope, expected, next_index, operation).await?;
    transaction.commit().await?;
    Ok(())
}

/// Stages a new operation into a caller-owned serializable transaction.
///
/// ID watermark allocation and duplicate classification can therefore commit
/// atomically with the canonical record, operation, and runnable pointer.
pub(super) async fn stage_operation(
    transaction: &DbTransaction,
    scope: DataScope,
    expected: ExpectedCanonicalRevision,
    next_index: &IndexRecordV2,
    operation: &IndexOperationRecord,
) -> Result<()> {
    if !matches!(
        operation.execution_state(),
        IndexOperationExecutionState::Queued {
            not_before_unix_millis: None
        }
    ) || operation.attempt() != 0
        || operation.operation_revision() != IndexOperationRevision::initial()
    {
        return Err(corruption(
            "new index operation must begin at queued revision one with attempt zero",
        ));
    }

    let pointer = pointer_for(scope, operation);
    validate_link(scope, next_index, operation, Some(&pointer))?;
    let index_key = scoped_index_key(scope, next_index);
    let current_value = transaction.get(&index_key).await?;
    let previous_operation = match (expected, current_value) {
        (ExpectedCanonicalRevision::Absent, None) => None,
        (ExpectedCanonicalRevision::Absent, Some(_)) => {
            return Err(corruption("canonical index unexpectedly exists"));
        }
        (ExpectedCanonicalRevision::Exact(_), None) => {
            return Err(corruption("expected canonical index is missing"));
        }
        (ExpectedCanonicalRevision::Exact(expected_revision), Some(value)) => {
            let current = decode_index_record(&value)?;
            if current.revision() != expected_revision {
                return Err(corruption("canonical index revision changed"));
            }
            if current.identity() != next_index.identity()
                || current.index_id() != next_index.index_id()
            {
                return Err(corruption(
                    "later operation does not preserve canonical index identity",
                ));
            }
            if next_index.revision() != current.revision().checked_next()? {
                return Err(corruption(
                    "later operation must checked-increment the canonical revision",
                ));
            }
            let previous_id = terminal_operation_id(current.state()).ok_or_else(|| {
                corruption("a later operation may replace only a terminal retained operation")
            })?;
            let previous_key = scoped_operation_key(scope, previous_id);
            let Some(previous_value) = transaction.get(&previous_key).await? else {
                return Err(corruption("prior terminal operation is missing"));
            };
            let previous = decode_operation_record(&previous_value)?;
            validate_link(scope, &current, &previous, None)?;
            Some(previous_key)
        }
    };
    if matches!(expected, ExpectedCanonicalRevision::Absent)
        && next_index.revision() != IndexRevision::initial()
    {
        return Err(corruption("new canonical index must begin at revision one"));
    }

    if let Some(previous_operation) = previous_operation {
        transaction.delete(previous_operation)?;
    }
    transaction.put(index_key, encode_index_record(next_index))?;
    transaction.put(
        scoped_operation_key(scope, operation.operation_id()),
        encode_operation_record(operation),
    )?;
    transaction.put(
        global_operation_key(operation.operation_id()),
        encode_metadata_value(&IndexV2MetadataValue::OperationQueuePointer(pointer)),
    )?;
    Ok(())
}

/// Reads at most one bounded lexicographic page after the supplied cursor.
pub(crate) async fn scan_operation_queue_page(
    db: &Db,
    resume_after: Option<IndexOperationId>,
    page_size: OperationQueuePageSize,
) -> Result<OperationQueuePage> {
    let prefix = GlobalKey::logical_prefix(GlobalKind::OperationPointer);
    let start = resume_after.map_or(Bound::Unbounded, |operation_id| {
        Bound::Excluded(Bytes::copy_from_slice(operation_id.as_bytes()))
    });
    let mut rows = db
        .scan_prefix(&prefix, (start, Bound::<Bytes>::Unbounded))
        .await?;
    let mut operation_ids = Vec::with_capacity(page_size.get());
    while operation_ids.len() < page_size.get() {
        let Some(row) = rows.next().await? else {
            break;
        };
        let GlobalKey::OperationPointer(operation_id) = GlobalKey::parse_from_slice(&row.key)?
        else {
            return Err(corruption(
                "operation-pointer prefix yielded a different global key",
            ));
        };
        operation_ids.push(operation_id);
    }
    let prefix_exhausted = operation_ids.len() < page_size.get();
    let resume_after = operation_ids.last().copied();
    Ok(OperationQueuePage {
        operation_ids,
        resume_after,
        prefix_exhausted,
    })
}

/// Resolves one exact global pointer to its validated scoped operation.
pub(crate) async fn read_queued_operation(
    db: &Db,
    operation_id: IndexOperationId,
) -> Result<Option<(DataScope, IndexOperationRecord)>> {
    let pointer_key = global_operation_key(operation_id);
    let Some(pointer_value) = db.get(pointer_key).await? else {
        return Ok(None);
    };
    let pointer = decode_pointer(&pointer_value)?;
    let transaction = db.begin(IsolationLevel::Snapshot).await?;
    let Some((_index, operation, _pointer)) =
        load_exact_link(&transaction, pointer.scope, operation_id).await?
    else {
        return Ok(None);
    };
    Ok(Some((pointer.scope, operation)))
}

/// Completes a bounded-page integrity pass over every global operation pointer.
///
/// Writer open runs this after SlateDB fencing and before returning a public
/// handle. Valid tenant work is discovered without a tenant registry; orphan
/// pointers are removed, while any present-but-disagreeing authoritative rows
/// fail the open.
pub(crate) async fn reconcile_operation_queue(db: &Db) -> Result<()> {
    let page_size = OperationQueuePageSize::new(64)?;
    let writer_epoch = WriterEpoch::new_v4();
    let mut resume_after = None;
    loop {
        let page = scan_operation_queue_page(db, resume_after, page_size).await?;
        for operation_id in page.operation_ids {
            let _ = observe_operation_pointer(db, operation_id, writer_epoch, 0).await?;
        }
        if page.prefix_exhausted {
            return Ok(());
        }
        let Some(next) = page.resume_after else {
            return Err(corruption(
                "non-exhausted operation reconciliation page has no cursor",
            ));
        };
        resume_after = Some(next);
    }
}

/// Requeues obsolete reader-coordination blockers within one exact data scope.
///
/// Startup reads at most one bounded page before releasing the scan view.
/// Every matching operation is repaired in its own serializable transaction.
/// A crash can therefore only leave a prefix of operations repaired; replay
/// skips current records and converges.
pub(crate) async fn reconcile_legacy_reader_coordination_operations(
    db: &Db,
    scope: DataScope,
) -> Result<u64> {
    const RECONCILIATION_PAGE_SIZE: usize = 64;
    let prefix =
        ManagedIndexKey::data_prefix(scope, ScopedKey::logical_prefix(RecordKind::Operation));
    let mut repaired = 0_u64;
    let mut resume_after = None;
    loop {
        let start = resume_after.map_or(Bound::Unbounded, |operation_id: IndexOperationId| {
            Bound::Excluded(Bytes::copy_from_slice(operation_id.as_bytes()))
        });
        let mut rows = db
            .scan_prefix(&prefix, (start, Bound::<Bytes>::Unbounded))
            .await?;
        let mut operation_ids = Vec::with_capacity(RECONCILIATION_PAGE_SIZE);
        while operation_ids.len() < RECONCILIATION_PAGE_SIZE {
            let Some(row) = rows.next().await? else {
                break;
            };
            let ManagedIndexKey::Data {
                scope: row_scope,
                kind: ScopedKey::Operation(operation_key),
            } = ManagedIndexKey::parse_from_slice(scope, &row.key)?
            else {
                return Err(corruption(
                    "operation-prefix scan returned another typed key",
                ));
            };
            if row_scope != scope {
                return Err(corruption("operation-prefix scan changed data scope"));
            }
            operation_ids.push(operation_key.operation_id);
        }
        let prefix_exhausted = operation_ids.len() < RECONCILIATION_PAGE_SIZE;
        if operation_ids.is_empty() {
            return Ok(repaired);
        }
        resume_after = operation_ids.last().copied();
        drop(rows);

        for operation_id in operation_ids {
            let operation_key = scoped_operation_key(scope, operation_id);
            loop {
                let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
                let Some(value) = transaction.get(&operation_key).await? else {
                    break;
                };
                let (operation, has_legacy_reader_coordination_blocker) =
                    decode_operation_record_with_compatibility(&value)?;
                if !has_legacy_reader_coordination_blocker {
                    break;
                }
                let IndexOperationExecutionState::Blocked(_) = operation.execution_state() else {
                    return Err(corruption(
                        "legacy reader blocker decoded outside Blocked execution state",
                    ));
                };
                if operation.operation_id() != operation_id {
                    return Err(corruption(
                        "operation key disagrees with legacy blocked operation",
                    ));
                }
                let index_key = scoped_index_key_for_identity(scope, operation.identity());
                let Some(index_value) = transaction.get(&index_key).await? else {
                    return Err(corruption(
                        "legacy reader-blocked operation has no canonical index",
                    ));
                };
                let index = decode_index_record(&index_value)?;
                validate_link(scope, &index, &operation, None)?;
                let next = operation.retry().map_err(operation_model_error)?;
                let pointer = pointer_for(scope, &next);
                validate_link(scope, &index, &next, Some(&pointer))?;
                transaction.put(operation_key.clone(), encode_operation_record(&next))?;
                transaction.put(
                    global_operation_key(next.operation_id()),
                    encode_metadata_value(&IndexV2MetadataValue::OperationQueuePointer(pointer)),
                )?;
                match transaction.commit().await {
                    Ok(_) => {
                        repaired = repaired.checked_add(1).ok_or_else(|| {
                            corruption("legacy reader-blocker repair count overflowed")
                        })?;
                        break;
                    }
                    Err(error) if error.kind() == slatedb::ErrorKind::Transaction => continue,
                    Err(error) => return Err(error.into()),
                }
            }
        }
        if prefix_exhausted {
            return Ok(repaired);
        }
    }
}

/// Resolves and cross-validates one pointer, cleaning an orphan idempotently.
pub(crate) async fn observe_operation_pointer(
    db: &Db,
    operation_id: IndexOperationId,
    writer_epoch: WriterEpoch,
    now_unix_millis: u64,
) -> Result<OperationPointerObservation> {
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let pointer_key = global_operation_key(operation_id);
    let Some(pointer_value) = transaction.get(&pointer_key).await? else {
        return Ok(OperationPointerObservation::StalePointerRemoved);
    };
    let pointer = decode_pointer(&pointer_value)?;
    let operation_key = scoped_operation_key(pointer.scope, operation_id);
    let Some(operation_value) = transaction.get(&operation_key).await? else {
        transaction.delete(pointer_key)?;
        transaction.commit().await?;
        return Ok(OperationPointerObservation::StalePointerRemoved);
    };
    let operation = decode_operation_record(&operation_value)?;
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    crate::migrations::observe_legacy_text_migration_operation(&operation)?;
    let index_key = scoped_index_key_for_identity(pointer.scope, operation.identity());
    let Some(index_value) = transaction.get(index_key).await? else {
        return Err(corruption(
            "operation pointer resolves to an operation without a canonical index",
        ));
    };
    let index = decode_index_record(&index_value)?;
    validate_link(pointer.scope, &index, &operation, Some(&pointer))?;

    let eligible = EligibleOperation {
        scope: pointer.scope,
        record: operation.clone(),
    };
    match operation.execution_state() {
        IndexOperationExecutionState::Queued { .. } => {
            let schedule = operation
                .queue_schedule()
                .expect("validated queued operation has one schedule");
            if schedule.is_eligible_for(writer_epoch, now_unix_millis) {
                Ok(OperationPointerObservation::Eligible(eligible))
            } else {
                Ok(OperationPointerObservation::Delayed {
                    delay_millis: observed_delay(
                        now_unix_millis,
                        schedule
                            .not_before_unix_millis()
                            .expect("an ineligible queue schedule has one deadline"),
                    ),
                })
            }
        }
        IndexOperationExecutionState::Claimed(claim) if claim.writer_epoch == writer_epoch => Ok(
            OperationPointerObservation::ClaimedByCurrentWriter(eligible),
        ),
        IndexOperationExecutionState::Claimed(_) => {
            Ok(OperationPointerObservation::Eligible(eligible))
        }
        IndexOperationExecutionState::Blocked(_) | IndexOperationExecutionState::Completed(_) => {
            Err(corruption(
                "blocked or completed operation retained a runnable pointer",
            ))
        }
    }
}

/// Claims one exact observed operation or reports that another delivery won.
pub(crate) async fn claim_operation(
    db: &Db,
    eligible: &EligibleOperation,
    writer_epoch: WriterEpoch,
    sequence: ClaimSequence,
    now_unix_millis: u64,
    permission: ClaimPermission,
) -> Result<Option<ClaimedOperation>> {
    failpoints::trip(IndexOutboxFailpoint::ClaimBefore)?;
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let pointer_key = global_operation_key(eligible.record.operation_id());
    let Some(pointer_value) = transaction.get(&pointer_key).await? else {
        return Ok(None);
    };
    let pointer = decode_pointer(&pointer_value)?;
    if pointer.scope != eligible.scope
        || pointer.record_revision != eligible.record.operation_revision()
    {
        return Ok(None);
    }
    let operation_key = scoped_operation_key(pointer.scope, eligible.record.operation_id());
    let Some(operation_value) = transaction.get(&operation_key).await? else {
        transaction.delete(pointer_key)?;
        transaction.commit().await?;
        return Ok(None);
    };
    let operation = decode_operation_record(&operation_value)?;
    if operation.operation_revision() != eligible.record.operation_revision() {
        return Ok(None);
    }
    let authorized = match (operation.execution_state(), permission) {
        (IndexOperationExecutionState::Queued { .. }, ClaimPermission::Normal) => operation
            .queue_schedule()
            .is_some_and(|schedule| schedule.is_eligible_for(writer_epoch, now_unix_millis)),
        (IndexOperationExecutionState::Claimed(claim), ClaimPermission::Normal) => {
            claim.writer_epoch != writer_epoch
        }
        (
            IndexOperationExecutionState::Claimed(claim),
            ClaimPermission::SameEpochRecovery(proof),
        ) => claim.writer_epoch == writer_epoch && proof.writer_epoch == writer_epoch,
        (
            IndexOperationExecutionState::Queued { .. },
            ClaimPermission::SameEpochRecovery(proof),
        ) => {
            proof.writer_epoch == writer_epoch
                && operation
                    .queue_schedule()
                    .is_some_and(|schedule| schedule.is_eligible_for(writer_epoch, now_unix_millis))
        }
        (IndexOperationExecutionState::Blocked(_), _)
        | (IndexOperationExecutionState::Completed(_), _) => false,
    };
    if !authorized {
        return Ok(None);
    }

    let index_key = scoped_index_key_for_identity(pointer.scope, operation.identity());
    let Some(index_value) = transaction.get(index_key).await? else {
        return Err(corruption("claim operation has no canonical index"));
    };
    let index = decode_index_record(&index_value)?;
    validate_link(pointer.scope, &index, &operation, Some(&pointer))?;
    let claimed = operation
        .claim(OperationClaim {
            writer_epoch,
            sequence,
        })
        .map_err(operation_model_error)?;
    let next_pointer = pointer_for(pointer.scope, &claimed);
    validate_link(pointer.scope, &index, &claimed, Some(&next_pointer))?;
    transaction.put(operation_key, encode_operation_record(&claimed))?;
    transaction.put(
        pointer_key,
        encode_metadata_value(&IndexV2MetadataValue::OperationQueuePointer(next_pointer)),
    )?;
    transaction.commit().await?;
    failpoints::trip(IndexOutboxFailpoint::ClaimAfter)?;
    Ok(Some(ClaimedOperation {
        scope: pointer.scope,
        record: claimed,
    }))
}

/// Runs one claimed physical step and commits its checkpoint atomically.
#[cfg_attr(
    feature = "index-lifecycle-testing",
    allow(
        dead_code,
        reason = "feature builds use the evidence-returning wrapper in automatic and explicit harnesses"
    )
)]
pub(crate) async fn execute_claimed_step(
    db: &Db,
    claimed: &ClaimedOperation,
    driver: &dyn IndexOperationDriver,
    limits: SearchIndexBatchLimits,
    now_unix_millis: u64,
) -> Result<CommittedOperationStep> {
    Ok(
        execute_claimed_step_with_evidence(db, claimed, driver, limits, now_unix_millis)
            .await?
            .outcome,
    )
}

/// Runs one claimed step and returns disposable resource and stage evidence.
pub(crate) async fn execute_claimed_step_with_evidence(
    db: &Db,
    claimed: &ClaimedOperation,
    driver: &dyn IndexOperationDriver,
    limits: SearchIndexBatchLimits,
    now_unix_millis: u64,
) -> Result<CommittedOperationStepEvidence> {
    let started = Instant::now();
    if driver.family() != claimed.record.family() {
        return Err(corruption(
            "family capability selected a driver for a different operation family",
        ));
    }

    let prepared = match driver
        .prepare_step(db, claimed.scope, &claimed.record, limits)
        .await
    {
        Ok(prepared) => prepared,
        Err(error) => {
            return record_driver_failure(db, claimed, error, now_unix_millis, started).await
        }
    };
    if prepared.family() != driver.family() {
        prepared.discard().await?;
        return Err(corruption(
            "family driver prepared work for a different operation family",
        ));
    }

    let mut driver_failed = false;
    let staged: Result<Option<StagedOperationStep>> = async {
        let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
        failpoints::trip(IndexOutboxFailpoint::BatchReadBefore)?;
        let Some((index, operation, _pointer)) =
            load_exact_link(&transaction, claimed.scope, claimed.record.operation_id()).await?
        else {
            return Err(corruption("claimed operation disappeared before dispatch"));
        };
        if operation.operation_revision() != claimed.record.operation_revision()
            || operation.execution_state() != claimed.record.execution_state()
        {
            return Err(corruption(
                "claimed operation revision changed before family dispatch",
            ));
        }
        failpoints::trip(IndexOutboxFailpoint::BatchReadAfter)?;
        failpoints::trip(IndexOutboxFailpoint::PhysicalStagingBefore)?;
        let step = prepared
            .stage(driver, db, &transaction, claimed.scope, &operation, limits)
            .await;
        failpoints::trip(IndexOutboxFailpoint::PhysicalStagingAfter)?;
        // An error can follow staged physical writes. Drop this transaction before
        // recording a blocker or releasing the claim; never commit a partial step.
        let execution = step.inspect_err(|_| {
            driver_failed = true;
        })?;
        let IndexOperationStepExecution {
            result: step,
            resources,
        } = execution;
        if matches!(step, IndexOperationStepResult::TransientFailure) {
            return Ok(None);
        }

        failpoints::trip(IndexOutboxFailpoint::CheckpointStagingBefore)?;
        let operation_key = scoped_operation_key(claimed.scope, operation.operation_id());
        let pointer_key = global_operation_key(operation.operation_id());
        let before_status = IndexOperationStatus::from_record(&operation);
        let before_stage = before_status.common().stage;
        let (committed, after_stage, counter_resources) = match step {
            IndexOperationStepResult::Progressed(progress) => {
                let next = operation
                    .progressed(progress)
                    .map_err(operation_model_error)?;
                let next_pointer = pointer_for(claimed.scope, &next);
                validate_link(claimed.scope, &index, &next, Some(&next_pointer))?;
                transaction.put(operation_key, encode_operation_record(&next))?;
                transaction.put(
                    pointer_key,
                    encode_metadata_value(&IndexV2MetadataValue::OperationQueuePointer(
                        next_pointer,
                    )),
                )?;
                let after_status = IndexOperationStatus::from_record(&next);
                (
                    CommittedOperationStep::Progressed,
                    after_status.common().stage,
                    step_counter_resources(&before_status, &after_status)?,
                )
            }
            IndexOperationStepResult::ProgressedAfter {
                progress,
                delay_millis,
            } => {
                let not_before_unix_millis =
                    now_unix_millis.checked_add(delay_millis.get()).ok_or(
                        HelixDbError::IdentifierExhausted("index operation scheduling deadline"),
                    )?;
                let next = operation
                    .progressed_after(progress, not_before_unix_millis)
                    .map_err(operation_model_error)?;
                let next_pointer = pointer_for(claimed.scope, &next);
                validate_link(claimed.scope, &index, &next, Some(&next_pointer))?;
                transaction.put(operation_key, encode_operation_record(&next))?;
                transaction.put(
                    pointer_key,
                    encode_metadata_value(&IndexV2MetadataValue::OperationQueuePointer(
                        next_pointer,
                    )),
                )?;
                let after_status = IndexOperationStatus::from_record(&next);
                (
                    CommittedOperationStep::Progressed,
                    after_status.common().stage,
                    step_counter_resources(&before_status, &after_status)?,
                )
            }
            IndexOperationStepResult::Blocked(blocker) => {
                let next = operation.block(blocker).map_err(operation_model_error)?;
                validate_link(claimed.scope, &index, &next, None)?;
                transaction.put(operation_key, encode_operation_record(&next))?;
                failpoints::trip(IndexOutboxFailpoint::QueueRemovalBefore)?;
                transaction.delete(pointer_key)?;
                failpoints::trip(IndexOutboxFailpoint::QueueRemovalAfter)?;
                (
                    CommittedOperationStep::Blocked,
                    IndexOperationStatus::from_record(&next).common().stage,
                    StepResourceUsage::default(),
                )
            }
            IndexOperationStepResult::Completed(outcome) => {
                let transition = match outcome {
                    IndexOperationOutcome::Build(BuildOperationOutcome::Succeeded) => {
                        failpoints::trip(IndexOutboxFailpoint::ActivationBefore)?;
                        IndexStateTransition::Activate
                    }
                    IndexOperationOutcome::Build(BuildOperationOutcome::Aborted) => {
                        IndexStateTransition::CompleteAbort
                    }
                    IndexOperationOutcome::DropSucceeded => IndexStateTransition::CompleteDrop,
                };
                let next_index = index.transition(transition)?;
                let next = operation
                    .complete(outcome, next_index.revision())
                    .map_err(operation_model_error)?;
                validate_link(claimed.scope, &next_index, &next, None)?;
                transaction.put(
                    scoped_index_key(claimed.scope, &next_index),
                    encode_index_record(&next_index),
                )?;
                transaction.put(operation_key, encode_operation_record(&next))?;
                if matches!(
                    outcome,
                    IndexOperationOutcome::Build(BuildOperationOutcome::Succeeded)
                ) {
                    failpoints::trip(IndexOutboxFailpoint::ActivationAfter)?;
                }
                failpoints::trip(IndexOutboxFailpoint::QueueRemovalBefore)?;
                transaction.delete(pointer_key)?;
                failpoints::trip(IndexOutboxFailpoint::QueueRemovalAfter)?;
                (
                    CommittedOperationStep::Completed,
                    IndexOperationStatus::from_record(&next).common().stage,
                    step_counter_resources(
                        &before_status,
                        &IndexOperationStatus::from_record(&next),
                    )?,
                )
            }
            IndexOperationStepResult::TransientFailure => return Ok(None),
        };
        failpoints::trip(IndexOutboxFailpoint::CheckpointStagingAfter)?;
        failpoints::trip_for_operation(
            IndexOutboxFailpoint::CommitBefore,
            operation.operation_id(),
        )?;
        Ok(Some(StagedOperationStep {
            transaction,
            index,
            operation,
            committed,
            before_stage,
            after_stage,
            resources: classify_stage_resources(
                before_stage,
                counter_resources.saturating_add(resources),
            ),
        }))
    }
    .await;

    let Some(StagedOperationStep {
        transaction,
        index,
        operation,
        committed,
        before_stage,
        after_stage,
        resources,
    }) = (match staged {
        Ok(staged) => staged,
        Err(error) => {
            prepared.discard().await?;
            if driver_failed {
                return record_driver_failure(db, claimed, error, now_unix_millis, started).await;
            }
            return Err(error);
        }
    })
    else {
        prepared.discard().await?;
        release_transient_claim(db, claimed, now_unix_millis).await?;
        let stage = IndexOperationStatus::from_record(&claimed.record)
            .common()
            .stage;
        return Ok(CommittedOperationStepEvidence {
            outcome: CommittedOperationStep::TransientFailure,
            before_stage: stage,
            after_stage: stage,
            resources: StepResourceUsage::default(),
            elapsed_micros: elapsed_micros(started),
        });
    };

    match transaction.commit().await {
        Ok(_) => {}
        Err(commit) => {
            prepared.discard().await?;
            return Err(commit.into());
        }
    }
    prepared.after_commit().await;
    driver
        .after_commit(claimed.scope, &index, &operation, committed)
        .await;
    failpoints::trip_for_operation(IndexOutboxFailpoint::CommitAfter, operation.operation_id())?;
    Ok(CommittedOperationStepEvidence {
        outcome: committed,
        before_stage,
        after_stage,
        resources,
        elapsed_micros: elapsed_micros(started),
    })
}

/// Records failures only after failed preparation and staging have been discarded.
/// The exact claim is rechecked in a fresh serializable transaction so a stale
/// worker cannot block an operation that another writer or an abort has advanced.
async fn record_driver_failure(
    db: &Db,
    claimed: &ClaimedOperation,
    error: HelixDbError,
    now_unix_millis: u64,
    started: Instant,
) -> Result<CommittedOperationStepEvidence> {
    if matches!(
        error,
        HelixDbError::DatabaseClosed | HelixDbError::WriterFencedCommitOutcomeUnknown
    ) || matches!(&error, HelixDbError::Storage(storage) if matches!(storage.kind(), slatedb::ErrorKind::Closed(_)))
    {
        return Err(error);
    }
    let retryable = error.is_transaction_conflict()
        || matches!(&error, HelixDbError::Storage(storage) if storage.kind() == slatedb::ErrorKind::Unavailable)
        || matches!(error, HelixDbError::ObjectStore(_));
    let status = IndexOperationStatus::from_record(&claimed.record);
    tracing::warn!(
        operation_id = %claimed.record.operation_id().as_uuid(),
        index_id = claimed.record.index_id().get(),
        generation = claimed.record.generation().get(),
        stage = ?status.common().stage,
        error = %error,
        retryable,
        "index operation driver failed"
    );
    let outcome = if retryable {
        release_transient_claim(db, claimed, now_unix_millis).await?;
        CommittedOperationStep::TransientFailure
    } else {
        let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
        let Some((index, operation, _)) =
            load_exact_link(&transaction, claimed.scope, claimed.record.operation_id()).await?
        else {
            return Err(corruption("failed operation disappeared before blocking"));
        };
        if operation.operation_revision() != claimed.record.operation_revision()
            || operation.execution_state() != claimed.record.execution_state()
        {
            return Err(HelixDbError::TransactionConflict(
                "failed index claim has advanced".into(),
            ));
        }
        let next = operation
            .block(IndexOperationBlocker::InvariantViolation)
            .map_err(operation_model_error)?;
        validate_link(claimed.scope, &index, &next, None)?;
        transaction.put(
            scoped_operation_key(claimed.scope, next.operation_id()),
            encode_operation_record(&next),
        )?;
        transaction.delete(global_operation_key(next.operation_id()))?;
        transaction.commit().await?;
        CommittedOperationStep::Blocked
    };
    Ok(CommittedOperationStepEvidence {
        outcome,
        before_stage: status.common().stage,
        after_stage: status.common().stage,
        resources: StepResourceUsage::default(),
        elapsed_micros: elapsed_micros(started),
    })
}

fn step_counter_resources(
    before: &IndexOperationStatus,
    after: &IndexOperationStatus,
) -> Result<StepResourceUsage> {
    let before = before.common().progress;
    let after = after.common().progress;
    Ok(StepResourceUsage {
        source_entities: after.entities.checked_sub(before.entities).ok_or_else(|| {
            corruption("operation entity counter regressed across one committed step")
        })?,
        input_bytes: after
            .input_bytes
            .checked_sub(before.input_bytes)
            .ok_or_else(|| {
                corruption("operation input-byte counter regressed across one committed step")
            })?,
        physical_operations: after
            .output_operations
            .checked_sub(before.output_operations)
            .ok_or_else(|| {
                corruption("operation output-operation counter regressed across one committed step")
            })?,
        output_bytes: after
            .output_bytes
            .checked_sub(before.output_bytes)
            .ok_or_else(|| {
                corruption("operation output-byte counter regressed across one committed step")
            })?,
        ..StepResourceUsage::default()
    })
}

fn elapsed_micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

fn classify_stage_resources(
    stage: IndexOperationStage,
    resources: StepResourceUsage,
) -> StepResourceUsage {
    match stage {
        IndexOperationStage::Scan
        | IndexOperationStage::ScanPartitions
        | IndexOperationStage::CatchUp
        | IndexOperationStage::Validate
        | IndexOperationStage::ValidateDescriptor
        | IndexOperationStage::ValidateLegacyPhysical
        | IndexOperationStage::Compact
        | IndexOperationStage::PrepareManifests
        | IndexOperationStage::ValidateManifests
        | IndexOperationStage::Activate
        | IndexOperationStage::DeleteEntries
        | IndexOperationStage::RetireCache
        | IndexOperationStage::DeletePhysical
        | IndexOperationStage::DeleteDeltas
        | IndexOperationStage::DeleteMetadata
        | IndexOperationStage::Finalize
        | IndexOperationStage::AbortingDeleteEntries
        | IndexOperationStage::AbortingRetireCache
        | IndexOperationStage::AbortingDeletePhysical
        | IndexOperationStage::AbortingDeleteDeltas
        | IndexOperationStage::AbortingDeleteMetadata
        | IndexOperationStage::AbortingFinalize => {}
    }
    resources
}

/// Requeues a blocked operation at its exact checkpoint and recreates its pointer.
pub(crate) async fn retry_blocked_operation(
    db: &Db,
    scope: DataScope,
    operation_id: IndexOperationId,
    expected_revision: IndexOperationRevision,
    execution_control: &execution_control::ExecutionControl,
) -> Result<Option<IndexOperationRecord>> {
    execution_control.check()?;
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let operation_key = scoped_operation_key(scope, operation_id);
    let Some(value) = transaction.get(&operation_key).await? else {
        return Ok(None);
    };
    let operation = decode_operation_record(&value)?;
    if operation.operation_revision() != expected_revision {
        return Ok(None);
    }
    let index_key = scoped_index_key_for_identity(scope, operation.identity());
    let Some(index_value) = transaction.get(index_key).await? else {
        return Err(corruption("blocked operation has no canonical index"));
    };
    let index = decode_index_record(&index_value)?;
    match operation.execution_state() {
        IndexOperationExecutionState::Blocked(blocker) => {
            let next = operation.retry().map_err(operation_model_error)?;
            match blocker {
                IndexOperationBlocker::InvalidSourceData { .. }
                | IndexOperationBlocker::UniquenessViolation { .. }
                | IndexOperationBlocker::OversizedEntity { .. }
                | IndexOperationBlocker::ManifestLimit { .. }
                | IndexOperationBlocker::ObjectStoreConfigurationUnavailable
                | IndexOperationBlocker::InvariantViolation
                | IndexOperationBlocker::InvalidLegacyPhysical => {}
            }
            let pointer = pointer_for(scope, &next);
            validate_link(scope, &index, &next, Some(&pointer))?;
            transaction.put(operation_key, encode_operation_record(&next))?;
            transaction.put(
                global_operation_key(operation_id),
                encode_metadata_value(&IndexV2MetadataValue::OperationQueuePointer(pointer)),
            )?;
            transaction
                .commit()
                .await
                .map_err(HelixDbError::from_storage_commit)?;
            Ok(Some(next))
        }
        IndexOperationExecutionState::Queued { .. } | IndexOperationExecutionState::Claimed(_) => {
            let Some(pointer_value) = transaction.get(global_operation_key(operation_id)).await?
            else {
                return Err(corruption("runnable operation is missing its pointer"));
            };
            let pointer = decode_pointer(&pointer_value)?;
            validate_link(scope, &index, &operation, Some(&pointer))?;
            Ok(Some(operation))
        }
        IndexOperationExecutionState::Completed(_) => {
            validate_link(scope, &index, &operation, None)?;
            Ok(Some(operation))
        }
    }
}

/// Point-reads one operation in exactly the caller-provided scope.
///
/// The lookup never consults another scope or scans global pointers. Runnable
/// state is still cross-checked against its exact global discovery pointer.
/// Callers that own writable [`Db`] storage must pass a snapshot or transaction
/// so the canonical, operation, and pointer rows come from one committed view.
pub(crate) async fn read_operation(
    db: &(impl DbReadOps + Sync),
    scope: DataScope,
    operation_id: IndexOperationId,
) -> Result<Option<IndexOperationRecord>> {
    let operation_key = scoped_operation_key(scope, operation_id);
    let Some(operation_value) = db.get(operation_key).await? else {
        return Ok(None);
    };
    let operation = decode_operation_record(&operation_value)?;
    let index_key = scoped_index_key_for_identity(scope, operation.identity());
    let Some(index_value) = db.get(index_key).await? else {
        return Err(corruption("operation has no canonical index"));
    };
    let index = decode_index_record(&index_value)?;
    let pointer = if matches!(
        operation.execution_state(),
        IndexOperationExecutionState::Queued { .. } | IndexOperationExecutionState::Claimed(_)
    ) {
        let Some(pointer_value) = db.get(global_operation_key(operation_id)).await? else {
            return Err(corruption("runnable operation is missing its pointer"));
        };
        Some(decode_pointer(&pointer_value)?)
    } else {
        None
    };
    validate_link(scope, &index, &operation, pointer.as_ref())?;
    Ok(Some(operation))
}

/// Convergently ensures a retained operation is runnable and returns its
/// resulting public status source record.
#[cfg(any(
    test,
    feature = "production-coverage",
    feature = "migration-parity",
    feature = "index-lifecycle-testing"
))]
pub(crate) async fn retry_operation(
    db: &Db,
    scope: DataScope,
    operation_id: IndexOperationId,
) -> Result<IndexOperationRecord> {
    let execution_control = execution_control::ExecutionControl::unlimited();
    retry_operation_with_control(db, scope, operation_id, &execution_control).await
}

/// Requeues one request-owned blocked operation at its durable boundary.
pub(crate) async fn retry_operation_with_control(
    db: &Db,
    scope: DataScope,
    operation_id: IndexOperationId,
    execution_control: &execution_control::ExecutionControl,
) -> Result<IndexOperationRecord> {
    loop {
        let snapshot = db.snapshot().await?;
        let Some(current) = read_operation(snapshot.as_ref(), scope, operation_id).await? else {
            return Err(operation_not_found(operation_id));
        };
        drop(snapshot);
        if !matches!(
            current.execution_state(),
            IndexOperationExecutionState::Blocked(_)
        ) {
            return Ok(current);
        }
        let Some(next) = retry_blocked_operation(
            db,
            scope,
            operation_id,
            current.operation_revision(),
            execution_control,
        )
        .await?
        else {
            continue;
        };
        return Ok(next);
    }
}

/// Converts a constructing BUILD into abort cleanup, or converges on an
/// already-aborting/aborted BUILD with the same operation ID.
#[cfg(any(
    test,
    feature = "production-coverage",
    feature = "migration-parity",
    feature = "index-lifecycle-testing"
))]
pub(crate) async fn abort_operation(
    db: &Db,
    scope: DataScope,
    operation_id: IndexOperationId,
) -> Result<IndexOperationRecord> {
    let execution_control = execution_control::ExecutionControl::unlimited();
    abort_operation_with_control(db, scope, operation_id, &execution_control).await
}

/// Aborts one request-owned constructing operation at its durable boundary.
pub(crate) async fn abort_operation_with_control(
    db: &Db,
    scope: DataScope,
    operation_id: IndexOperationId,
    execution_control: &execution_control::ExecutionControl,
) -> Result<IndexOperationRecord> {
    execution_control.check()?;
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let operation_key = scoped_operation_key(scope, operation_id);
    let Some(operation_value) = transaction.get(&operation_key).await? else {
        return Err(operation_not_found(operation_id));
    };
    let operation = decode_operation_record(&operation_value)?;
    let index_key = scoped_index_key_for_identity(scope, operation.identity());
    let Some(index_value) = transaction.get(&index_key).await? else {
        return Err(corruption("operation has no canonical index"));
    };
    let index = decode_index_record(&index_value)?;
    let pointer_key = global_operation_key(operation_id);
    let pointer = transaction
        .get(&pointer_key)
        .await?
        .map(|value| decode_pointer(&value))
        .transpose()?;
    validate_link(scope, &index, &operation, pointer.as_ref())?;

    match (index.state(), operation.kind(), operation.execution_state()) {
        (
            IndexStateV2::Building { .. },
            super::IndexOperationKind::Build,
            IndexOperationExecutionState::Queued { .. }
            | IndexOperationExecutionState::Claimed(_)
            | IndexOperationExecutionState::Blocked(_),
        ) if operation.progress().is_constructing_build() => {
            let next_index = index.transition(IndexStateTransition::BeginAbort)?;
            let next_operation = operation
                .begin_abort(next_index.revision())
                .map_err(operation_model_error)?;
            let next_pointer = pointer_for(scope, &next_operation);
            validate_link(scope, &next_index, &next_operation, Some(&next_pointer))?;
            transaction.put(index_key, encode_index_record(&next_index))?;
            transaction.put(operation_key, encode_operation_record(&next_operation))?;
            transaction.put(
                pointer_key,
                encode_metadata_value(&IndexV2MetadataValue::OperationQueuePointer(next_pointer)),
            )?;
            transaction
                .commit()
                .await
                .map_err(HelixDbError::from_storage_commit)?;
            Ok(next_operation)
        }
        (IndexStateV2::Aborting { .. }, super::IndexOperationKind::Build, _)
            if operation.progress().is_aborting_build() =>
        {
            Ok(operation)
        }
        (
            IndexStateV2::Dropped { .. },
            super::IndexOperationKind::Build,
            IndexOperationExecutionState::Completed(IndexOperationOutcome::Build(
                BuildOperationOutcome::Aborted,
            )),
        ) => Ok(operation),
        (_, super::IndexOperationKind::Build, _) => Err(HelixDbError::IndexOperationNotAbortable {
            operation_id: operation_id.as_uuid().to_string(),
            reason: "successful or terminal build",
        }),
        (_, super::IndexOperationKind::Drop, _) => Err(HelixDbError::IndexOperationNotAbortable {
            operation_id: operation_id.as_uuid().to_string(),
            reason: "drop operations cannot be aborted",
        }),
    }
}

fn operation_not_found(operation_id: IndexOperationId) -> HelixDbError {
    HelixDbError::IndexOperationNotFound {
        operation_id: operation_id.as_uuid().to_string(),
    }
}

async fn release_transient_claim(
    db: &Db,
    claimed: &ClaimedOperation,
    now_unix_millis: u64,
) -> Result<()> {
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let Some((index, operation, _)) =
        load_exact_link(&transaction, claimed.scope, claimed.record.operation_id()).await?
    else {
        return Ok(());
    };
    if operation.operation_revision() != claimed.record.operation_revision() {
        return Ok(());
    }
    let deadline = now_unix_millis.saturating_add(backoff_millis(operation.attempt()));
    let next = operation
        .transient_failure(deadline)
        .map_err(operation_model_error)?;
    let pointer = pointer_for(claimed.scope, &next);
    validate_link(claimed.scope, &index, &next, Some(&pointer))?;
    failpoints::trip(IndexOutboxFailpoint::CheckpointStagingBefore)?;
    transaction.put(
        scoped_operation_key(claimed.scope, next.operation_id()),
        encode_operation_record(&next),
    )?;
    transaction.put(
        global_operation_key(next.operation_id()),
        encode_metadata_value(&IndexV2MetadataValue::OperationQueuePointer(pointer)),
    )?;
    failpoints::trip(IndexOutboxFailpoint::CheckpointStagingAfter)?;
    failpoints::trip_for_operation(IndexOutboxFailpoint::CommitBefore, operation.operation_id())?;
    transaction.commit().await?;
    failpoints::trip_for_operation(IndexOutboxFailpoint::CommitAfter, operation.operation_id())?;
    Ok(())
}

/// Loads one canonical record, runnable operation, and pointer as a checked link.
///
/// `None` means either half of the discoverable operation link is absent. A
/// present result has already passed the repository's scope, identity,
/// revision, family, state, and pointer cross-checks.
pub(super) async fn load_exact_link(
    transaction: &DbTransaction,
    scope: DataScope,
    operation_id: IndexOperationId,
) -> Result<
    Option<(
        IndexRecordV2,
        IndexOperationRecord,
        OperationQueuePointerValue,
    )>,
> {
    let pointer_key = global_operation_key(operation_id);
    let Some(pointer_value) = transaction.get(pointer_key).await? else {
        return Ok(None);
    };
    let pointer = decode_pointer(&pointer_value)?;
    if pointer.scope != scope {
        return Err(corruption("operation pointer changed scope"));
    }
    let operation_key = scoped_operation_key(scope, operation_id);
    let Some(operation_value) = transaction.get(operation_key).await? else {
        return Ok(None);
    };
    let operation = decode_operation_record(&operation_value)?;
    let index_key = scoped_index_key_for_identity(scope, operation.identity());
    let Some(index_value) = transaction.get(index_key).await? else {
        return Err(corruption("operation has no canonical index"));
    };
    let index = decode_index_record(&index_value)?;
    validate_link(scope, &index, &operation, Some(&pointer))?;
    Ok(Some((index, operation, pointer)))
}

pub(super) fn validate_link(
    scope: DataScope,
    index: &IndexRecordV2,
    operation: &IndexOperationRecord,
    pointer: Option<&OperationQueuePointerValue>,
) -> Result<()> {
    if !super::repository::operation_record_cursors_are_valid(scope, operation) {
        return Err(corruption(
            "operation contains a cursor or artifact key outside its exact typed V2 ownership",
        ));
    }
    if index.index_id() != operation.index_id()
        || index.identity() != operation.identity()
        || index.state().generation() != operation.generation()
        || index.revision() != operation.index_record_revision()
    {
        return Err(corruption(
            "canonical index and operation identity/revision fields disagree",
        ));
    }

    let state_matches = match index.state() {
        IndexStateV2::Building {
            build_operation_id, ..
        } => {
            *build_operation_id == operation.operation_id()
                && operation.progress().is_constructing_build()
                && !matches!(
                    operation.execution_state(),
                    IndexOperationExecutionState::Completed(_)
                )
        }
        IndexStateV2::Active {
            completed_build_operation_id,
            ..
        } => {
            *completed_build_operation_id == operation.operation_id()
                && operation.progress().is_constructing_build()
                && matches!(
                    operation.execution_state(),
                    IndexOperationExecutionState::Completed(IndexOperationOutcome::Build(
                        BuildOperationOutcome::Succeeded
                    ))
                )
        }
        IndexStateV2::Aborting {
            build_operation_id, ..
        } => {
            *build_operation_id == operation.operation_id()
                && operation.progress().is_aborting_build()
                && !matches!(
                    operation.execution_state(),
                    IndexOperationExecutionState::Completed(_)
                )
        }
        IndexStateV2::Dropping {
            drop_operation_id, ..
        } => {
            *drop_operation_id == operation.operation_id()
                && operation.kind() == super::IndexOperationKind::Drop
                && !matches!(
                    operation.execution_state(),
                    IndexOperationExecutionState::Completed(_)
                )
        }
        IndexStateV2::Dropped {
            completed_operation_id,
            ..
        } => {
            *completed_operation_id == operation.operation_id()
                && matches!(
                    (operation.kind(), operation.execution_state()),
                    (
                        super::IndexOperationKind::Build,
                        IndexOperationExecutionState::Completed(IndexOperationOutcome::Build(
                            BuildOperationOutcome::Aborted
                        ))
                    ) | (
                        super::IndexOperationKind::Drop,
                        IndexOperationExecutionState::Completed(
                            IndexOperationOutcome::DropSucceeded
                        )
                    )
                )
        }
    };
    if !state_matches {
        return Err(corruption(
            "canonical lifecycle state and retained operation disagree",
        ));
    }

    let pointer_required = matches!(
        operation.execution_state(),
        IndexOperationExecutionState::Queued { .. } | IndexOperationExecutionState::Claimed(_)
    );
    match (pointer_required, pointer) {
        (true, Some(pointer))
            if pointer.scope == scope
                && pointer.index_id == operation.index_id()
                && pointer.generation == operation.generation()
                && pointer.record_revision == operation.operation_revision() =>
        {
            Ok(())
        }
        (false, None) => Ok(()),
        (true, Some(_)) => Err(corruption(
            "runnable pointer disagrees with its authoritative operation",
        )),
        (true, None) => Err(corruption("runnable operation is missing its pointer")),
        (false, Some(_)) => Err(corruption(
            "non-runnable operation unexpectedly retains a pointer",
        )),
    }
}

fn terminal_operation_id(state: &IndexStateV2) -> Option<IndexOperationId> {
    match state {
        IndexStateV2::Active {
            completed_build_operation_id,
            ..
        } => Some(*completed_build_operation_id),
        IndexStateV2::Dropped {
            completed_operation_id,
            ..
        } => Some(*completed_operation_id),
        IndexStateV2::Building { .. }
        | IndexStateV2::Aborting { .. }
        | IndexStateV2::Dropping { .. } => None,
    }
}

pub(super) fn pointer_for(
    scope: DataScope,
    operation: &IndexOperationRecord,
) -> OperationQueuePointerValue {
    OperationQueuePointerValue {
        scope,
        index_id: operation.index_id(),
        generation: operation.generation(),
        record_revision: operation.operation_revision(),
    }
}

fn decode_pointer(value: &[u8]) -> Result<OperationQueuePointerValue> {
    let IndexV2MetadataValue::OperationQueuePointer(pointer) = decode_metadata_value(value)? else {
        return Err(corruption(
            "operation pointer key contains a different metadata value kind",
        ));
    };
    Ok(pointer)
}

fn scoped_index_key(scope: DataScope, index: &IndexRecordV2) -> Bytes {
    scoped_index_key_for_identity(scope, index.identity())
}

pub(super) fn scoped_index_key_for_identity(
    scope: DataScope,
    identity: &super::IndexIdentity,
) -> Bytes {
    ManagedIndexKey::Data {
        scope,
        kind: ScopedKey::index_record(identity.clone()),
    }
    .to_bytes()
}

pub(super) fn scoped_operation_key(scope: DataScope, operation_id: IndexOperationId) -> Bytes {
    ManagedIndexKey::Data {
        scope,
        kind: ScopedKey::operation(operation_id),
    }
    .to_bytes()
}

pub(super) fn global_operation_key(operation_id: IndexOperationId) -> Bytes {
    ManagedIndexKey::Global {
        kind: GlobalKey::OperationPointer(operation_id),
    }
    .to_bytes()
}

fn backoff_millis(attempt: u32) -> u64 {
    let exponent = attempt.saturating_sub(1).min(63);
    BASE_OPERATION_BACKOFF_MILLIS
        .saturating_mul(1_u64.checked_shl(exponent).unwrap_or(u64::MAX))
        .min(MAX_OPERATION_BACKOFF_MILLIS)
}

fn observed_delay(now_unix_millis: u64, not_before_unix_millis: u64) -> u64 {
    not_before_unix_millis
        .saturating_sub(now_unix_millis)
        .min(MAX_OPERATION_BACKOFF_MILLIS)
}

fn operation_model_error(error: super::IndexOperationModelError) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(error.to_string())
}

fn corruption(reason: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(reason.into())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;

    use super::*;
    use crate::config::{SearchIndexBackfillLimits, SecondaryIndexDefinition};
    use crate::index_lifecycle::{
        IndexGenerationId, IndexId, NoCursorProgress, OperationCounters, PhysicalGeneration,
        SecondaryBuildProgress, SecondaryBuildStage, SecondaryCleanupProgress,
        ValidatedDynamicIndexDefinition,
    };

    struct StaticDriver(IndexOperationStepResult);

    #[async_trait]
    impl IndexOperationDriver for StaticDriver {
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
        ) -> Result<IndexOperationStepExecution> {
            Ok(IndexOperationStepExecution::new(self.0.clone()))
        }
    }

    #[derive(Clone, Copy)]
    enum FailureMode {
        PrepareCorruption,
        StageCorruption,
        StageInvariant,
        Conflict,
        ObjectStore,
    }

    struct FailingDriver {
        mode: FailureMode,
        probe_key: Bytes,
    }

    #[async_trait]
    impl IndexOperationDriver for FailingDriver {
        fn family(&self) -> IndexOperationFamily {
            IndexOperationFamily::Secondary
        }

        async fn acquire_step_permit(
            &self,
            _scope: DataScope,
            _operation: &IndexOperationRecord,
        ) -> Result<Box<dyn IndexOperationStepPermit>> {
            if matches!(self.mode, FailureMode::PrepareCorruption) {
                return Err(corruption("prepare fixture"));
            }
            Ok(Box::new(()))
        }

        async fn step(
            &self,
            _db: &Db,
            transaction: &DbTransaction,
            _scope: DataScope,
            _operation: &IndexOperationRecord,
            _limits: SearchIndexBatchLimits,
        ) -> Result<IndexOperationStepExecution> {
            transaction.delete(self.probe_key.clone())?;
            Err(match self.mode {
                FailureMode::PrepareCorruption | FailureMode::StageCorruption => {
                    corruption("stage fixture")
                }
                FailureMode::StageInvariant => {
                    HelixDbError::InvariantViolation("stage fixture".into())
                }
                FailureMode::Conflict => HelixDbError::TransactionConflict("stage fixture".into()),
                FailureMode::ObjectStore => {
                    HelixDbError::ObjectStore(slatedb::object_store::Error::Generic {
                        store: "fixture",
                        source: std::io::Error::other("temporarily unavailable").into(),
                    })
                }
            })
        }
    }

    #[tokio::test]
    async fn driver_errors_discard_partial_writes_and_preserve_retry_classification() {
        for (mode, expected) in [
            (
                FailureMode::PrepareCorruption,
                CommittedOperationStep::Blocked,
            ),
            (
                FailureMode::StageCorruption,
                CommittedOperationStep::Blocked,
            ),
            (FailureMode::StageInvariant, CommittedOperationStep::Blocked),
            (
                FailureMode::Conflict,
                CommittedOperationStep::TransientFailure,
            ),
            (
                FailureMode::ObjectStore,
                CommittedOperationStep::TransientFailure,
            ),
        ] {
            let db = db("outbox-driver-error").await;
            let (index, operation) = fixture(70);
            let scope = DataScope::LegacyUnscoped;
            enqueue_operation(
                &db,
                scope,
                ExpectedCanonicalRevision::Absent,
                &index,
                &operation,
            )
            .await
            .unwrap();
            abort_operation(&db, scope, operation.operation_id())
                .await
                .unwrap();
            let epoch = WriterEpoch::from_bytes([71; 16]).unwrap();
            let OperationPointerObservation::Eligible(eligible) =
                observe_operation_pointer(&db, operation.operation_id(), epoch, 0)
                    .await
                    .unwrap()
            else {
                panic!("queued operation is eligible");
            };
            let claimed = claim_operation(
                &db,
                &eligible,
                epoch,
                ClaimSequence::new(1).unwrap(),
                0,
                ClaimPermission::Normal,
            )
            .await
            .unwrap()
            .unwrap();
            let probe_key = scoped_index_key(scope, &index);
            let before = db.get(&probe_key).await.unwrap();
            let result = execute_claimed_step(
                &db,
                &claimed,
                &FailingDriver {
                    mode,
                    probe_key: probe_key.clone(),
                },
                SearchIndexBackfillLimits::default().batch(),
                0,
            )
            .await
            .unwrap();
            assert_eq!(result, expected);
            assert_eq!(
                db.get(probe_key).await.unwrap(),
                before,
                "failed physical staging must roll back"
            );
            let value = db
                .get(scoped_operation_key(scope, operation.operation_id()))
                .await
                .unwrap()
                .unwrap();
            let retained = decode_operation_record(&value).unwrap();
            assert_eq!(retained.progress(), claimed.record.progress());
            assert_eq!(retained.attempt(), claimed.record.attempt());
            let pointer = db
                .get(global_operation_key(operation.operation_id()))
                .await
                .unwrap();
            match expected {
                CommittedOperationStep::Blocked => {
                    assert!(matches!(
                        retained.execution_state(),
                        IndexOperationExecutionState::Blocked(
                            IndexOperationBlocker::InvariantViolation
                        )
                    ));
                    assert!(pointer.is_none());
                    let retried = retry_operation(&db, scope, operation.operation_id())
                        .await
                        .unwrap();
                    assert_eq!(retried.progress(), retained.progress());
                }
                CommittedOperationStep::TransientFailure => {
                    assert!(
                        matches!(retained.execution_state(), IndexOperationExecutionState::Queued { not_before_unix_millis: Some(deadline) } if *deadline > 0)
                    );
                    assert!(pointer.is_some());
                }
                CommittedOperationStep::Progressed | CommittedOperationStep::Completed => {
                    unreachable!()
                }
            }
            db.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn stale_and_unknown_claims_cannot_become_durable_blockers() {
        let db = db("outbox-stale-failure").await;
        let (index, operation) = fixture(72);
        let scope = DataScope::LegacyUnscoped;
        enqueue_operation(
            &db,
            scope,
            ExpectedCanonicalRevision::Absent,
            &index,
            &operation,
        )
        .await
        .unwrap();
        let epoch = WriterEpoch::from_bytes([73; 16]).unwrap();
        let OperationPointerObservation::Eligible(eligible) =
            observe_operation_pointer(&db, operation.operation_id(), epoch, 0)
                .await
                .unwrap()
        else {
            panic!("queued operation is eligible");
        };
        let claimed = claim_operation(
            &db,
            &eligible,
            epoch,
            ClaimSequence::new(1).unwrap(),
            0,
            ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .unwrap();
        let before = db
            .get(scoped_operation_key(scope, operation.operation_id()))
            .await
            .unwrap();
        assert!(matches!(
            record_driver_failure(
                &db,
                &claimed,
                HelixDbError::WriterFencedCommitOutcomeUnknown,
                0,
                Instant::now()
            )
            .await,
            Err(HelixDbError::WriterFencedCommitOutcomeUnknown)
        ));
        assert_eq!(
            db.get(scoped_operation_key(scope, operation.operation_id()))
                .await
                .unwrap(),
            before
        );
        release_transient_claim(&db, &claimed, 0).await.unwrap();
        let advanced = db
            .get(scoped_operation_key(scope, operation.operation_id()))
            .await
            .unwrap();
        assert!(matches!(
            record_driver_failure(
                &db,
                &claimed,
                corruption("stale failure"),
                0,
                Instant::now()
            )
            .await,
            Err(HelixDbError::TransactionConflict(_))
        ));
        assert_eq!(
            db.get(scoped_operation_key(scope, operation.operation_id()))
                .await
                .unwrap(),
            advanced
        );
        assert!(db
            .get(global_operation_key(operation.operation_id()))
            .await
            .unwrap()
            .is_some());
        db.close().await.unwrap();
    }

    fn fixture(id_byte: u8) -> (IndexRecordV2, IndexOperationRecord) {
        let definition = ValidatedDynamicIndexDefinition::try_from(
            SecondaryIndexDefinition::node_equality("User", "email").unwrap(),
        )
        .unwrap();
        let operation_id = IndexOperationId::from_bytes([id_byte; 16]).unwrap();
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
        (index, operation)
    }

    async fn db(name: &str) -> Db {
        Db::builder(name, Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn stable_read_view_keeps_operation_and_pointer_revision_atomic() {
        let db = db("outbox-stable-operation-view").await;
        let (index, operation) = fixture(41);
        enqueue_operation(
            &db,
            DataScope::LegacyUnscoped,
            ExpectedCanonicalRevision::Absent,
            &index,
            &operation,
        )
        .await
        .unwrap();
        let snapshot = db.snapshot().await.unwrap();

        let epoch = WriterEpoch::from_bytes([42; 16]).unwrap();
        let OperationPointerObservation::Eligible(eligible) =
            observe_operation_pointer(&db, operation.operation_id(), epoch, 0)
                .await
                .unwrap()
        else {
            panic!("fresh operation must be eligible");
        };
        let claimed = claim_operation(
            &db,
            &eligible,
            epoch,
            ClaimSequence::new(1).unwrap(),
            0,
            ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            read_operation(
                snapshot.as_ref(),
                DataScope::LegacyUnscoped,
                operation.operation_id(),
            )
            .await
            .unwrap(),
            Some(operation)
        );
        assert_eq!(
            read_operation(
                &db,
                DataScope::LegacyUnscoped,
                claimed.record.operation_id()
            )
            .await
            .unwrap(),
            Some(claimed.record)
        );
    }

    #[test]
    fn backoff_and_observation_delay_are_saturating_and_bounded() {
        assert_eq!(backoff_millis(1), 1_000);
        assert_eq!(backoff_millis(2), 2_000);
        assert_eq!(backoff_millis(u32::MAX), MAX_OPERATION_BACKOFF_MILLIS);
        assert_eq!(observed_delay(100, 99), 0);
        assert_eq!(observed_delay(0, u64::MAX), MAX_OPERATION_BACKOFF_MILLIS);
    }

    /// Covers queue-page validation and error adapters independently from one
    /// durable delivery path.
    #[tokio::test]
    async fn typed_outbox_helper_boundaries_fail_closed() {
        assert!(matches!(
            OperationQueuePageSize::new(0),
            Err(HelixDbError::InvariantViolation(message))
                if message.contains("page size must be non-zero")
        ));
        assert_eq!(OperationQueuePageSize::new(1).unwrap().get(), 1);

        let db = db("outbox-helper-boundaries").await;
        let prepared =
            PreparedIndexOperationStep::driver_owned(IndexOperationFamily::Secondary, Box::new(()));
        assert_eq!(prepared.family(), IndexOperationFamily::Secondary);
        prepared.after_commit().await;
        PreparedIndexOperationStep::driver_owned(IndexOperationFamily::Vector, Box::new(()))
            .discard()
            .await
            .unwrap();

        assert!(matches!(
            operation_model_error(super::super::IndexOperationModelError::ZeroClaimSequence),
            HelixDbError::IndexCatalogCorruption(message)
                if message.contains("claim sequence must be non-zero")
        ));
        assert!(matches!(
            corruption("fixture"),
            HelixDbError::IndexCatalogCorruption(message) if message == "fixture"
        ));
        assert!(matches!(
            decode_pointer(&encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                super::super::IndexStorageVersion::CURRENT,
            ))),
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("different metadata value kind")
        ));
        db.close().await.unwrap();
    }

    /// Proves a new operation cannot evict a nonterminal retained operation,
    /// even when its proposed canonical row and pointer are internally valid.
    #[tokio::test]
    async fn later_operation_rejects_nonterminal_retained_history() {
        let db = db("outbox-nonterminal-retention").await;
        let (building, build) = fixture(61);
        enqueue_operation(
            &db,
            DataScope::LegacyUnscoped,
            ExpectedCanonicalRevision::Absent,
            &building,
            &build,
        )
        .await
        .unwrap();

        let later_id = IndexOperationId::from_bytes([62; 16]).unwrap();
        let later_revision = building.revision().checked_next().unwrap();
        let later = IndexRecordV2::building(
            building.index_id(),
            building.definition().clone(),
            later_revision,
            PhysicalGeneration::Secondary {
                generation: building.state().generation(),
            },
            later_id,
        )
        .unwrap();
        let later_operation = IndexOperationRecord::try_new(
            later_id,
            later.index_id(),
            later.identity().clone(),
            later.state().generation(),
            later.revision(),
            IndexOperationRevision::initial(),
            super::super::IndexOperationKind::Build,
            IndexOperationFamily::Secondary,
            build.progress().clone(),
            0,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .unwrap();
        assert!(matches!(
            enqueue_operation(
                &db,
                DataScope::LegacyUnscoped,
                ExpectedCanonicalRevision::Exact(building.revision()),
                &later,
                &later_operation,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("replace only a terminal retained operation")
        ));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn enqueue_claim_and_activation_are_one_exact_revision_chain() {
        struct CompleteDriver;

        #[async_trait]
        impl IndexOperationDriver for CompleteDriver {
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
            ) -> Result<IndexOperationStepExecution> {
                Ok(IndexOperationStepExecution::new(
                    IndexOperationStepResult::Completed(IndexOperationOutcome::Build(
                        BuildOperationOutcome::Succeeded,
                    )),
                ))
            }
        }

        let db = db("outbox-claim-activate").await;
        let (index, operation) = fixture(1);
        enqueue_operation(
            &db,
            DataScope::LegacyUnscoped,
            ExpectedCanonicalRevision::Absent,
            &index,
            &operation,
        )
        .await
        .unwrap();
        let observation = observe_operation_pointer(
            &db,
            operation.operation_id(),
            WriterEpoch::from_bytes([2; 16]).unwrap(),
            0,
        )
        .await
        .unwrap();
        let OperationPointerObservation::Eligible(eligible) = observation else {
            panic!("queued operation must be eligible");
        };
        let epoch = WriterEpoch::from_bytes([2; 16]).unwrap();
        let claimed = claim_operation(
            &db,
            &eligible,
            epoch,
            ClaimSequence::new(1).unwrap(),
            0,
            ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(claimed.record.attempt(), 1);
        assert!(matches!(
            claimed.record.execution_state(),
            IndexOperationExecutionState::Claimed(_)
        ));
        assert_eq!(
            execute_claimed_step(
                &db,
                &claimed,
                &CompleteDriver,
                SearchIndexBackfillLimits::default().batch(),
                0,
            )
            .await
            .unwrap(),
            CommittedOperationStep::Completed
        );
        assert!(matches!(
            observe_operation_pointer(&db, operation.operation_id(), epoch, 0)
                .await
                .unwrap(),
            OperationPointerObservation::StalePointerRemoved
        ));
    }

    #[tokio::test]
    async fn prior_epoch_takeover_is_allowed_but_same_epoch_needs_join_proof() {
        let db = db("outbox-claim-recovery").await;
        let (index, operation) = fixture(3);
        enqueue_operation(
            &db,
            DataScope::LegacyUnscoped,
            ExpectedCanonicalRevision::Absent,
            &index,
            &operation,
        )
        .await
        .unwrap();
        let first_epoch = WriterEpoch::from_bytes([4; 16]).unwrap();
        let OperationPointerObservation::Eligible(eligible) =
            observe_operation_pointer(&db, operation.operation_id(), first_epoch, 0)
                .await
                .unwrap()
        else {
            panic!("queued operation must be eligible");
        };
        let first = claim_operation(
            &db,
            &eligible,
            first_epoch,
            ClaimSequence::new(1).unwrap(),
            0,
            ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            claim_operation(
                &db,
                &eligible,
                first_epoch,
                ClaimSequence::new(2).unwrap(),
                0,
                ClaimPermission::Normal,
            )
            .await
            .unwrap()
            .is_none(),
            "duplicate delivery cannot claim a stale exact revision"
        );
        let OperationPointerObservation::ClaimedByCurrentWriter(same) =
            observe_operation_pointer(&db, operation.operation_id(), first_epoch, 0)
                .await
                .unwrap()
        else {
            panic!("same writer claim must be supervised");
        };
        assert!(claim_operation(
            &db,
            &same,
            first_epoch,
            ClaimSequence::new(2).unwrap(),
            0,
            ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .is_none());
        assert!(claim_operation(
            &db,
            &same,
            first_epoch,
            ClaimSequence::new(2).unwrap(),
            0,
            ClaimPermission::SameEpochRecovery(SameEpochRecoveryProof::after_join(first_epoch)),
        )
        .await
        .unwrap()
        .is_some());

        let second_epoch = WriterEpoch::from_bytes([5; 16]).unwrap();
        let OperationPointerObservation::Eligible(prior) =
            observe_operation_pointer(&db, operation.operation_id(), second_epoch, 0)
                .await
                .unwrap()
        else {
            panic!("new fenced writer may recover prior epoch");
        };
        assert!(claim_operation(
            &db,
            &prior,
            second_epoch,
            ClaimSequence::new(1).unwrap(),
            0,
            ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .is_some());
        let _ = first;
    }

    #[tokio::test]
    async fn stale_pointer_is_removed_and_queue_scan_is_bounded() {
        let db = db("outbox-stale-pointer").await;
        let operation_id = IndexOperationId::from_bytes([6; 16]).unwrap();
        db.put(
            global_operation_key(operation_id),
            encode_metadata_value(&IndexV2MetadataValue::OperationQueuePointer(
                OperationQueuePointerValue {
                    scope: DataScope::LegacyUnscoped,
                    index_id: IndexId::initial(),
                    generation: IndexGenerationId::initial(),
                    record_revision: IndexOperationRevision::initial(),
                },
            )),
        )
        .await
        .unwrap();
        let page = scan_operation_queue_page(&db, None, OperationQueuePageSize::new(1).unwrap())
            .await
            .unwrap();
        assert_eq!(page.operation_ids, vec![operation_id]);
        assert!(matches!(
            observe_operation_pointer(
                &db,
                operation_id,
                WriterEpoch::from_bytes([7; 16]).unwrap(),
                0,
            )
            .await
            .unwrap(),
            OperationPointerObservation::StalePointerRemoved
        ));
        assert!(
            scan_operation_queue_page(&db, None, OperationQueuePageSize::new(1).unwrap())
                .await
                .unwrap()
                .operation_ids
                .is_empty()
        );
    }

    #[tokio::test]
    async fn queue_pages_advance_in_lexicographic_order_and_wrap_explicitly() {
        let db = db("outbox-fair-pages").await;
        let ids = [
            IndexOperationId::from_bytes([3; 16]).unwrap(),
            IndexOperationId::from_bytes([1; 16]).unwrap(),
            IndexOperationId::from_bytes([2; 16]).unwrap(),
        ];
        for operation_id in ids {
            db.put(
                global_operation_key(operation_id),
                encode_metadata_value(&IndexV2MetadataValue::OperationQueuePointer(
                    OperationQueuePointerValue {
                        scope: DataScope::LegacyUnscoped,
                        index_id: IndexId::initial(),
                        generation: IndexGenerationId::initial(),
                        record_revision: IndexOperationRevision::initial(),
                    },
                )),
            )
            .await
            .unwrap();
        }

        let first = scan_operation_queue_page(&db, None, OperationQueuePageSize::new(2).unwrap())
            .await
            .unwrap();
        assert_eq!(
            first.operation_ids,
            vec![
                IndexOperationId::from_bytes([1; 16]).unwrap(),
                IndexOperationId::from_bytes([2; 16]).unwrap()
            ]
        );
        assert!(!first.prefix_exhausted);
        let second = scan_operation_queue_page(
            &db,
            first.resume_after,
            OperationQueuePageSize::new(2).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            second.operation_ids,
            vec![IndexOperationId::from_bytes([3; 16]).unwrap()]
        );
        assert!(second.prefix_exhausted);
        let wrapped = scan_operation_queue_page(&db, None, OperationQueuePageSize::new(1).unwrap())
            .await
            .unwrap();
        assert_eq!(
            wrapped.operation_ids,
            vec![IndexOperationId::from_bytes([1; 16]).unwrap()]
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn blocked_pointer_is_removed_and_retry_is_convergent() {
        let db = db("outbox-blocked-retry").await;
        let (index, operation) = fixture(10);
        enqueue_operation(
            &db,
            DataScope::LegacyUnscoped,
            ExpectedCanonicalRevision::Absent,
            &index,
            &operation,
        )
        .await
        .unwrap();
        let epoch = WriterEpoch::from_bytes([11; 16]).unwrap();
        let OperationPointerObservation::Eligible(eligible) =
            observe_operation_pointer(&db, operation.operation_id(), epoch, 0)
                .await
                .unwrap()
        else {
            panic!("queued operation must be eligible");
        };
        let claimed = claim_operation(
            &db,
            &eligible,
            epoch,
            ClaimSequence::new(1).unwrap(),
            0,
            ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            execute_claimed_step(
                &db,
                &claimed,
                &StaticDriver(IndexOperationStepResult::Blocked(
                    IndexOperationBlocker::InvariantViolation,
                )),
                SearchIndexBackfillLimits::default().batch(),
                0,
            )
            .await
            .unwrap(),
            CommittedOperationStep::Blocked
        );
        assert!(db
            .get(global_operation_key(operation.operation_id()))
            .await
            .unwrap()
            .is_none());
        let blocked_value = db
            .get(scoped_operation_key(
                DataScope::LegacyUnscoped,
                operation.operation_id(),
            ))
            .await
            .unwrap()
            .unwrap();
        let blocked = decode_operation_record(&blocked_value).unwrap();
        assert!(matches!(
            blocked.execution_state(),
            IndexOperationExecutionState::Blocked(_)
        ));

        let queued = retry_operation(&db, DataScope::LegacyUnscoped, operation.operation_id())
            .await
            .unwrap();
        assert_eq!(queued.progress(), blocked.progress());
        assert_eq!(queued.attempt(), blocked.attempt());
        assert_eq!(
            queued.operation_revision().get(),
            blocked.operation_revision().get() + 1
        );
        assert!(matches!(
            queued.execution_state(),
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None
            }
        ));
        let duplicate = retry_operation(&db, DataScope::LegacyUnscoped, operation.operation_id())
            .await
            .unwrap();
        assert_eq!(duplicate, queued);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn obsolete_reader_blocker_reconciliation_is_exactly_once() {
        let store = Arc::new(InMemory::new());
        let db = Db::builder(
            "outbox-obsolete-reader-blocker-reconciliation",
            store.clone(),
        )
        .build()
        .await
        .unwrap();
        let scope = DataScope::LegacyUnscoped;
        let (index, operation) = fixture(70);
        let claimed = operation
            .claim(OperationClaim {
                writer_epoch: WriterEpoch::from_bytes([71; 16]).unwrap(),
                sequence: ClaimSequence::new(1).unwrap(),
            })
            .unwrap();
        let blocked = claimed
            .block(IndexOperationBlocker::InvariantViolation)
            .unwrap();
        let mut legacy_bytes = encode_operation_record(&blocked).to_vec();
        const CURRENT_INVARIANT_BLOCKER_TAG: u8 = 0x07;
        const LEGACY_READER_BLOCKER_TAG: u8 = 0x05;
        assert_eq!(
            legacy_bytes.last(),
            Some(&CURRENT_INVARIANT_BLOCKER_TAG),
            "blocker tag must remain the terminal field before constructing a legacy fixture"
        );
        let blocker_tag = legacy_bytes
            .last_mut()
            .expect("encoded blocked operation has a blocker tag");
        *blocker_tag = LEGACY_READER_BLOCKER_TAG;
        db.put(
            scoped_index_key_for_identity(scope, index.identity()),
            encode_index_record(&index),
        )
        .await
        .unwrap();
        db.put(
            scoped_operation_key(scope, blocked.operation_id()),
            Bytes::from(legacy_bytes),
        )
        .await
        .unwrap();

        assert_eq!(
            reconcile_legacy_reader_coordination_operations(&db, scope)
                .await
                .unwrap(),
            1
        );
        let repaired = read_operation(&db, scope, blocked.operation_id())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            repaired.execution_state(),
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None
            }
        ));
        assert_eq!(
            repaired.operation_revision().get(),
            blocked.operation_revision().get() + 1
        );
        db.close().await.unwrap();
        let reopened = Db::builder("outbox-obsolete-reader-blocker-reconciliation", store)
            .build()
            .await
            .unwrap();
        assert_eq!(
            reconcile_legacy_reader_coordination_operations(&reopened, scope)
                .await
                .unwrap(),
            0,
            "restart replay must not revise or repoint an already repaired operation"
        );
        assert_eq!(
            read_operation(&reopened, scope, blocked.operation_id())
                .await
                .unwrap()
                .unwrap(),
            repaired
        );
        reopened.close().await.unwrap();
    }

    #[tokio::test]
    async fn obsolete_reader_blocker_reconciliation_preserves_current_blockers() {
        let db = db("outbox-current-blocker-reconciliation").await;
        let scope = DataScope::LegacyUnscoped;
        let (index, operation) = fixture(72);
        let blocked = operation
            .claim(OperationClaim {
                writer_epoch: WriterEpoch::from_bytes([73; 16]).unwrap(),
                sequence: ClaimSequence::new(1).unwrap(),
            })
            .unwrap()
            .block(IndexOperationBlocker::InvariantViolation)
            .unwrap();
        db.put(
            scoped_index_key_for_identity(scope, index.identity()),
            encode_index_record(&index),
        )
        .await
        .unwrap();
        db.put(
            scoped_operation_key(scope, blocked.operation_id()),
            encode_operation_record(&blocked),
        )
        .await
        .unwrap();

        assert_eq!(
            reconcile_legacy_reader_coordination_operations(&db, scope)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            read_operation(&db, scope, blocked.operation_id())
                .await
                .unwrap()
                .unwrap(),
            blocked
        );
        assert!(db
            .get(global_operation_key(blocked.operation_id()))
            .await
            .unwrap()
            .is_none());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn obsolete_reader_blocker_reconciliation_scans_beyond_the_first_page() {
        let db = db("outbox-paged-obsolete-reader-blocker-reconciliation").await;
        let scope = DataScope::LegacyUnscoped;
        for id_byte in 1..=64 {
            let (_, operation) = fixture(id_byte);
            let blocked = operation
                .claim(OperationClaim {
                    writer_epoch: WriterEpoch::from_bytes([id_byte; 16]).unwrap(),
                    sequence: ClaimSequence::new(1).unwrap(),
                })
                .unwrap()
                .block(IndexOperationBlocker::InvariantViolation)
                .unwrap();
            db.put(
                scoped_operation_key(scope, blocked.operation_id()),
                encode_operation_record(&blocked),
            )
            .await
            .unwrap();
        }

        let (index, operation) = fixture(65);
        let blocked = operation
            .claim(OperationClaim {
                writer_epoch: WriterEpoch::from_bytes([65; 16]).unwrap(),
                sequence: ClaimSequence::new(1).unwrap(),
            })
            .unwrap()
            .block(IndexOperationBlocker::InvariantViolation)
            .unwrap();
        let mut legacy_bytes = encode_operation_record(&blocked).to_vec();
        const CURRENT_INVARIANT_BLOCKER_TAG: u8 = 0x07;
        const LEGACY_READER_BLOCKER_TAG: u8 = 0x05;
        assert_eq!(legacy_bytes.last(), Some(&CURRENT_INVARIANT_BLOCKER_TAG));
        *legacy_bytes
            .last_mut()
            .expect("encoded blocked operation has a blocker tag") = LEGACY_READER_BLOCKER_TAG;
        db.put(
            scoped_index_key_for_identity(scope, index.identity()),
            encode_index_record(&index),
        )
        .await
        .unwrap();
        db.put(
            scoped_operation_key(scope, blocked.operation_id()),
            Bytes::from(legacy_bytes),
        )
        .await
        .unwrap();

        assert_eq!(
            reconcile_legacy_reader_coordination_operations(&db, scope)
                .await
                .unwrap(),
            1
        );
        let repaired = read_operation(&db, scope, blocked.operation_id())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            repaired.execution_state(),
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None
            }
        ));
        assert!(db
            .get(global_operation_key(blocked.operation_id()))
            .await
            .unwrap()
            .is_some());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn abort_reuses_the_build_operation_from_queued_claimed_and_blocked_states() {
        #[derive(Clone, Copy)]
        enum StartingState {
            Queued,
            Claimed,
            Blocked,
        }

        for (name, id_byte, starting_state) in [
            ("outbox-abort-queued", 20, StartingState::Queued),
            ("outbox-abort-claimed", 21, StartingState::Claimed),
            ("outbox-abort-blocked", 22, StartingState::Blocked),
        ] {
            let db = db(name).await;
            let (index, operation) = fixture(id_byte);
            enqueue_operation(
                &db,
                DataScope::LegacyUnscoped,
                ExpectedCanonicalRevision::Absent,
                &index,
                &operation,
            )
            .await
            .unwrap();

            let before_abort = match starting_state {
                StartingState::Queued => operation.clone(),
                StartingState::Claimed | StartingState::Blocked => {
                    let epoch = WriterEpoch::from_bytes([id_byte + 1; 16]).unwrap();
                    let OperationPointerObservation::Eligible(eligible) =
                        observe_operation_pointer(&db, operation.operation_id(), epoch, 0)
                            .await
                            .unwrap()
                    else {
                        panic!("queued operation must be eligible");
                    };
                    let claimed = claim_operation(
                        &db,
                        &eligible,
                        epoch,
                        ClaimSequence::new(1).unwrap(),
                        0,
                        ClaimPermission::Normal,
                    )
                    .await
                    .unwrap()
                    .unwrap();
                    match starting_state {
                        StartingState::Claimed => claimed.record,
                        StartingState::Blocked => {
                            assert_eq!(
                                execute_claimed_step(
                                    &db,
                                    &claimed,
                                    &StaticDriver(IndexOperationStepResult::Blocked(
                                        IndexOperationBlocker::InvariantViolation,
                                    )),
                                    SearchIndexBackfillLimits::default().batch(),
                                    0,
                                )
                                .await
                                .unwrap(),
                                CommittedOperationStep::Blocked
                            );
                            read_operation(&db, DataScope::LegacyUnscoped, operation.operation_id())
                                .await
                                .unwrap()
                                .unwrap()
                        }
                        StartingState::Queued => unreachable!("matched above"),
                    }
                }
            };

            let aborted = abort_operation(&db, DataScope::LegacyUnscoped, operation.operation_id())
                .await
                .unwrap();
            assert_eq!(aborted.operation_id(), operation.operation_id());
            assert_eq!(aborted.attempt(), before_abort.attempt());
            assert_eq!(
                aborted.operation_revision().get(),
                before_abort.operation_revision().get() + 1
            );
            assert!(matches!(
                aborted.execution_state(),
                IndexOperationExecutionState::Queued {
                    not_before_unix_millis: None
                }
            ));
            assert!(matches!(
                aborted.progress(),
                IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Aborting(
                    SecondaryCleanupProgress::DeleteEntries(_)
                ))
            ));
            assert_eq!(
                abort_operation(&db, DataScope::LegacyUnscoped, operation.operation_id())
                    .await
                    .unwrap(),
                aborted
            );
            let canonical = decode_index_record(
                &db.get(scoped_index_key(DataScope::LegacyUnscoped, &index))
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            assert!(matches!(canonical.state(), IndexStateV2::Aborting { .. }));
            db.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn transient_failure_persists_bounded_delay_without_busy_looping() {
        let db = db("outbox-transient-delay").await;
        let (index, operation) = fixture(12);
        enqueue_operation(
            &db,
            DataScope::LegacyUnscoped,
            ExpectedCanonicalRevision::Absent,
            &index,
            &operation,
        )
        .await
        .unwrap();
        let epoch = WriterEpoch::from_bytes([13; 16]).unwrap();
        let OperationPointerObservation::Eligible(eligible) =
            observe_operation_pointer(&db, operation.operation_id(), epoch, 100)
                .await
                .unwrap()
        else {
            panic!("queued operation must be eligible");
        };
        let claimed = claim_operation(
            &db,
            &eligible,
            epoch,
            ClaimSequence::new(1).unwrap(),
            100,
            ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            execute_claimed_step(
                &db,
                &claimed,
                &StaticDriver(IndexOperationStepResult::TransientFailure),
                SearchIndexBackfillLimits::default().batch(),
                100,
            )
            .await
            .unwrap(),
            CommittedOperationStep::TransientFailure
        );
        assert!(matches!(
            observe_operation_pointer(&db, operation.operation_id(), epoch, 100)
                .await
                .unwrap(),
            OperationPointerObservation::Delayed {
                delay_millis: 1_000
            }
        ));
        let OperationPointerObservation::Eligible(eligible) =
            observe_operation_pointer(&db, operation.operation_id(), epoch, 1_100)
                .await
                .unwrap()
        else {
            panic!("elapsed delayed work must be eligible");
        };
        assert!(claim_operation(
            &db,
            &eligible,
            epoch,
            ClaimSequence::new(2).unwrap(),
            1_100,
            ClaimPermission::SameEpochRecovery(SameEpochRecoveryProof::after_join(epoch)),
        )
        .await
        .unwrap()
        .is_some());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn successful_progress_can_persist_a_reclaim_deadline() {
        let db = db("outbox-successful-progress-delay").await;
        let (index, operation) = fixture(13);
        enqueue_operation(
            &db,
            DataScope::LegacyUnscoped,
            ExpectedCanonicalRevision::Absent,
            &index,
            &operation,
        )
        .await
        .unwrap();
        let epoch = WriterEpoch::from_bytes([14; 16]).unwrap();
        let OperationPointerObservation::Eligible(eligible) =
            observe_operation_pointer(&db, operation.operation_id(), epoch, 100)
                .await
                .unwrap()
        else {
            panic!("queued operation must be eligible");
        };
        let claimed = claim_operation(
            &db,
            &eligible,
            epoch,
            ClaimSequence::new(1).unwrap(),
            100,
            ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            execute_claimed_step(
                &db,
                &claimed,
                &StaticDriver(IndexOperationStepResult::ProgressedAfter {
                    progress: claimed.record.progress().clone(),
                    delay_millis: NonZeroU64::new(7).unwrap(),
                }),
                SearchIndexBackfillLimits::default().batch(),
                100,
            )
            .await
            .unwrap(),
            CommittedOperationStep::Progressed
        );
        assert!(matches!(
            observe_operation_pointer(&db, operation.operation_id(), epoch, 100)
                .await
                .unwrap(),
            OperationPointerObservation::Delayed { delay_millis: 7 }
        ));
        assert!(matches!(
            observe_operation_pointer(&db, operation.operation_id(), epoch, 107)
                .await
                .unwrap(),
            OperationPointerObservation::Eligible(_)
        ));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn later_operation_atomically_evicts_the_prior_terminal_record() {
        let db = db("outbox-terminal-retention").await;
        let (building, build) = fixture(14);
        enqueue_operation(
            &db,
            DataScope::LegacyUnscoped,
            ExpectedCanonicalRevision::Absent,
            &building,
            &build,
        )
        .await
        .unwrap();
        let epoch = WriterEpoch::from_bytes([15; 16]).unwrap();
        let OperationPointerObservation::Eligible(eligible) =
            observe_operation_pointer(&db, build.operation_id(), epoch, 0)
                .await
                .unwrap()
        else {
            panic!("queued build must be eligible");
        };
        let claimed = claim_operation(
            &db,
            &eligible,
            epoch,
            ClaimSequence::new(1).unwrap(),
            0,
            ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .unwrap();
        execute_claimed_step(
            &db,
            &claimed,
            &StaticDriver(IndexOperationStepResult::Completed(
                IndexOperationOutcome::Build(BuildOperationOutcome::Succeeded),
            )),
            SearchIndexBackfillLimits::default().batch(),
            0,
        )
        .await
        .unwrap();
        let active_value = db
            .get(scoped_index_key(DataScope::LegacyUnscoped, &building))
            .await
            .unwrap()
            .unwrap();
        let active = decode_index_record(&active_value).unwrap();
        assert!(matches!(
            abort_operation(&db, DataScope::LegacyUnscoped, build.operation_id(),).await,
            Err(HelixDbError::IndexOperationNotAbortable { .. })
        ));
        let drop_id = IndexOperationId::from_bytes([16; 16]).unwrap();
        let dropping = active
            .transition(IndexStateTransition::BeginDrop {
                drop_operation_id: drop_id,
            })
            .unwrap();
        let drop_operation = IndexOperationRecord::try_new(
            drop_id,
            dropping.index_id(),
            dropping.identity().clone(),
            dropping.state().generation(),
            dropping.revision(),
            IndexOperationRevision::initial(),
            super::super::IndexOperationKind::Drop,
            IndexOperationFamily::Secondary,
            IndexOperationProgress::SecondaryCleanup(SecondaryCleanupProgress::Finalize(
                NoCursorProgress::default(),
            )),
            0,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .unwrap();
        enqueue_operation(
            &db,
            DataScope::LegacyUnscoped,
            ExpectedCanonicalRevision::Exact(active.revision()),
            &dropping,
            &drop_operation,
        )
        .await
        .unwrap();

        assert!(matches!(
            abort_operation(
                &db,
                DataScope::LegacyUnscoped,
                drop_operation.operation_id(),
            )
            .await,
            Err(HelixDbError::IndexOperationNotAbortable { .. })
        ));

        assert!(db
            .get(scoped_operation_key(
                DataScope::LegacyUnscoped,
                build.operation_id(),
            ))
            .await
            .unwrap()
            .is_none());
        assert!(db
            .get(scoped_operation_key(
                DataScope::LegacyUnscoped,
                drop_operation.operation_id(),
            ))
            .await
            .unwrap()
            .is_some());
        db.close().await.unwrap();
    }
}
