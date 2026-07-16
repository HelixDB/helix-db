//! Exact-revision repository lane for durable text-upload work.
//!
//! Global upload pointers are discovery records only. Every observation and
//! transition resolves the pointer-owned scope, loads the authoritative scoped
//! intent, and cross-checks the complete `(index, generation, revision)` link.
//! Claims are fenced by the runtime writer epoch and advance both intent and
//! pointer in one serializable transaction.
//!
//! Current-epoch Active mutations remain request-owned only while their exact
//! process-local guard is in flight. Missing or mismatched guards fail closed;
//! terminal guards and fenced prior-writer work use the normal exact claim
//! protocol.

use std::num::NonZeroUsize;
use std::ops::Bound;

use async_trait::async_trait;
use bytes::Bytes;
use slatedb::{Db, DbTransaction, IsolationLevel};

use crate::encoding::v1::keys::index_v2 as index_keys;
use crate::encoding::v1::keys::tenant::DataScope;
use crate::encoding::v1::keys::{DataKeyKind, GlobalKeyKind, Key};
use crate::encoding::v1::values::index_v2 as index_values;
use crate::error::{HelixDbError, Result};

use super::super::failpoints::{self, IndexOutboxFailpoint};
use super::super::outbox::{self, ClaimPermission};
use super::super::{
    work, ClaimSequence, IndexOperationBlocker, IndexOperationRecord, IndexV2MetadataValue,
    OperationClaim, WriterEpoch,
};
use super::super::{TextUploadIntentId, UploadQueuePointerValue};
use super::active_mutation::{ActiveTextMutationOwnerObservation, ActiveTextMutationRegistry};

const BASE_UPLOAD_BACKOFF_MILLIS: u64 = 1_000;
/// Maximum scheduling delay returned by one persisted upload observation.
pub(crate) const MAX_UPLOAD_BACKOFF_MILLIS: u64 = 30_000;

/// Checked bounded page size for fair global upload-pointer scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UploadQueuePageSize(NonZeroUsize);

impl UploadQueuePageSize {
    /// Constructs a non-zero upload-lane page size.
    pub(crate) fn new(value: usize) -> Result<Self> {
        NonZeroUsize::new(value).map(Self).ok_or_else(|| {
            HelixDbError::InvariantViolation(
                "text upload queue page size must be non-zero".to_string(),
            )
        })
    }

    /// Returns the checked page bound.
    pub(crate) const fn get(self) -> usize {
        self.0.get()
    }
}

/// One bounded global upload-pointer page and its continuation cursor.
#[derive(Debug)]
pub(crate) struct UploadQueuePage {
    pub(crate) intent_ids: Vec<TextUploadIntentId>,
    pub(crate) resume_after: Option<TextUploadIntentId>,
    pub(crate) prefix_exhausted: bool,
}

/// Exact scoped intent observation safe to pass to the claim transaction.
#[derive(Debug, Clone)]
pub(crate) struct EligibleUpload {
    pub(crate) scope: DataScope,
    pub(crate) record: work::TextUploadIntentValue,
}

/// Result of resolving one upload pointer without claiming it.
#[derive(Debug, Clone)]
pub(crate) enum UploadPointerObservation {
    /// Queued work or a claim owned by a fenced prior writer.
    Eligible(EligibleUpload),
    /// Queued work whose persisted retry deadline has not elapsed.
    Delayed { delay_millis: u64 },
    /// A current-epoch active request retains reconciliation authority.
    ActiveOwnerCurrentWriter,
    /// This worker already owns the claim; only supervised restart may replace it.
    ClaimedByCurrentWriter(EligibleUpload),
    /// The pointer disappeared or an orphan pointer was removed idempotently.
    StalePointerRemoved,
}

/// Exact claimed upload returned only after intent and pointer commit together.
#[derive(Debug, Clone)]
pub(crate) struct ClaimedUpload {
    pub(crate) scope: DataScope,
    pub(crate) record: work::TextUploadIntentValue,
}

/// Closed driver result for one exact claimed upload checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TextUploadStepResult {
    /// Definitive coordinator status matched the declared blob metadata.
    PublicationSucceeded,
    /// Terminal coordinator state plus object absence became durable release authority.
    NonPublicationProven,
    /// A durable absence proof released its permit and can retire its anchor.
    NonPublicationReleased,
    /// An exact shared object transferred from live intent ownership to GC work.
    SharedBlobReclaimable,
    /// An unassigned reclaim candidate was atomically linked to its singleton root.
    ReclaimRootAssigned,
    /// The singleton fence was revalidated for atomic owner normalization.
    ReclaimFenceRevalidated,
    /// A mismatched content-addressed object blocked its exact owning build.
    BlobMismatch,
    /// The uploaded object is protected for its exact first-reference commit.
    AttachUploaded,
    /// Resolve exact Active proof presence or fenced/terminal graph abort.
    ResolveActiveReference,
    /// The durable reference authorization was released and the anchor can retire.
    ReferenceReleased,
    /// No durable phase change occurred; release the claim with backoff.
    TransientFailure,
}

/// Runtime authority retained through one upload checkpoint commit.
pub(crate) trait TextUploadStepPermit: Send {}

impl<T: Send> TextUploadStepPermit for T {}

/// One closed upload outcome plus any authority that must survive its commit.
pub(crate) struct PreparedTextUploadStep {
    outcome: TextUploadStepResult,
    _permit: Box<dyn TextUploadStepPermit>,
}

impl PreparedTextUploadStep {
    /// Prepares a definitive publication checkpoint with no external guard.
    pub(crate) fn publication_succeeded() -> Self {
        Self {
            outcome: TextUploadStepResult::PublicationSucceeded,
            _permit: Box::new(()),
        }
    }

    /// Prepares the durable release-outbox checkpoint for proven object absence.
    pub(crate) fn non_publication_proven() -> Self {
        Self {
            outcome: TextUploadStepResult::NonPublicationProven,
            _permit: Box::new(()),
        }
    }

    /// Prepares anchor cleanup after idempotent terminal permit release.
    pub(crate) fn non_publication_released() -> Self {
        Self {
            outcome: TextUploadStepResult::NonPublicationReleased,
            _permit: Box::new(()),
        }
    }

    /// Retains reference authority through shared-blob reclaim commit.
    pub(crate) fn shared_blob_reclaimable(permit: impl TextUploadStepPermit + 'static) -> Self {
        Self {
            outcome: TextUploadStepResult::SharedBlobReclaimable,
            _permit: Box::new(permit),
        }
    }

    /// Prepares repository-only creation of a singleton upload-reclaim root.
    pub(crate) fn assign_reclaim_root() -> Self {
        Self {
            outcome: TextUploadStepResult::ReclaimRootAssigned,
            _permit: Box::new(()),
        }
    }

    /// Retains no runtime guard because the coordinator owns the durable fence.
    pub(crate) fn reclaim_fence_revalidated() -> Self {
        Self {
            outcome: TextUploadStepResult::ReclaimFenceRevalidated,
            _permit: Box::new(()),
        }
    }

    /// Prepares an atomic upload/build block for mismatched object bytes.
    pub(crate) fn blob_mismatch() -> Self {
        Self {
            outcome: TextUploadStepResult::BlobMismatch,
            _permit: Box::new(()),
        }
    }

    /// Retains exact reference-validation authority through attachment commit.
    pub(crate) fn attach_uploaded(permit: impl TextUploadStepPermit + 'static) -> Self {
        Self {
            outcome: TextUploadStepResult::AttachUploaded,
            _permit: Box::new(permit),
        }
    }

    /// Retains exact object-reference authority while resolving an Active proof.
    pub(crate) fn resolve_active_reference(permit: impl TextUploadStepPermit + 'static) -> Self {
        Self {
            outcome: TextUploadStepResult::ResolveActiveReference,
            _permit: Box::new(permit),
        }
    }

    /// Prepares cleanup after coordinator release of a durable reference proof.
    pub(crate) fn reference_released() -> Self {
        Self {
            outcome: TextUploadStepResult::ReferenceReleased,
            _permit: Box::new(()),
        }
    }

    /// Prepares a no-progress result owned by repository backoff.
    pub(crate) fn transient_failure() -> Self {
        Self {
            outcome: TextUploadStepResult::TransientFailure,
            _permit: Box::new(()),
        }
    }

    /// Returns the closed repository transition selected by the driver.
    pub(crate) const fn outcome(&self) -> TextUploadStepResult {
        self.outcome
    }
}

/// Closed database mutation selected after one external upload step.
enum StagedUploadCheckpoint {
    Runnable(work::TextUploadIntentValue),
    Blocked(work::TextUploadIntentValue),
    Delete,
}

/// Coordinator-backed text upload reconciliation contract.
#[async_trait]
pub(crate) trait TextUploadDriver: Send + Sync {
    /// Resolves one exact intent without retaining a database snapshot.
    async fn prepare_step(
        &self,
        intent: &work::TextUploadIntentValue,
    ) -> Result<PreparedTextUploadStep>;
}

/// Reads at most one bounded lexicographic upload-pointer page.
pub(crate) async fn scan_upload_queue_page(
    db: &Db,
    resume_after: Option<TextUploadIntentId>,
    page_size: UploadQueuePageSize,
) -> Result<UploadQueuePage> {
    let prefix =
        index_keys::GlobalIndexV2Key::logical_prefix(index_keys::GlobalIndexV2Kind::UploadPointer);
    let start = resume_after.map_or(Bound::Unbounded, |intent_id| {
        Bound::Excluded(Bytes::copy_from_slice(intent_id.as_bytes()))
    });
    let mut rows = db
        .scan_prefix(&prefix, (start, Bound::<Bytes>::Unbounded))
        .await?;
    let mut intent_ids = Vec::with_capacity(page_size.get());
    while intent_ids.len() < page_size.get() {
        let Some(row) = rows.next().await? else {
            break;
        };
        let index_keys::GlobalIndexV2Key::UploadPointer(intent_id) =
            index_keys::GlobalIndexV2Key::parse_from_slice(&row.key)?
        else {
            return Err(corruption(
                "upload-pointer prefix yielded a different global key",
            ));
        };
        intent_ids.push(intent_id);
    }
    let prefix_exhausted = intent_ids.len() < page_size.get();
    let resume_after = intent_ids.last().copied();
    Ok(UploadQueuePage {
        intent_ids,
        resume_after,
        prefix_exhausted,
    })
}

/// Resolves one upload pointer and removes only a pointer whose intent is absent.
pub(crate) async fn observe_upload_pointer(
    db: &Db,
    intent_id: TextUploadIntentId,
    active_mutations: &ActiveTextMutationRegistry,
    writer_epoch: WriterEpoch,
    now_unix_millis: u64,
) -> Result<UploadPointerObservation> {
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let pointer_key = global_upload_key(intent_id);
    let Some(pointer_value) = transaction.get(&pointer_key).await? else {
        active_mutations.forget_terminal_after_absence(intent_id, writer_epoch);
        return Ok(UploadPointerObservation::StalePointerRemoved);
    };
    let pointer = decode_pointer(&pointer_value)?;
    let intent_key = scoped_intent_key(pointer.scope, &pointer, intent_id);
    let Some(intent_value) = transaction.get(intent_key).await? else {
        transaction.delete(pointer_key)?;
        transaction.commit().await?;
        active_mutations.forget_terminal_after_absence(intent_id, writer_epoch);
        return Ok(UploadPointerObservation::StalePointerRemoved);
    };
    let intent = decode_intent(&intent_value)?;
    validate_link(intent_id, &pointer, &intent)?;

    if matches!(
        intent.owner,
        work::TextUploadOwner::ActiveMutation {
            writer_epoch: owner_epoch,
            ..
        } if owner_epoch == writer_epoch
    ) {
        match active_mutations
            .observe(pointer.scope, &intent)
            .map_err(|error| corruption(error.to_string()))?
        {
            ActiveTextMutationOwnerObservation::InFlight => {
                return Ok(UploadPointerObservation::ActiveOwnerCurrentWriter);
            }
            ActiveTextMutationOwnerObservation::Terminal => {}
        }
    }

    let eligible = EligibleUpload {
        scope: pointer.scope,
        record: intent.clone(),
    };
    match intent.work_state {
        work::TextUploadWorkState::Queued {
            not_before_unix_millis: Some(not_before),
        } if not_before > now_unix_millis => Ok(UploadPointerObservation::Delayed {
            delay_millis: observed_delay(now_unix_millis, not_before),
        }),
        work::TextUploadWorkState::Queued { .. } => {
            Ok(UploadPointerObservation::Eligible(eligible))
        }
        work::TextUploadWorkState::Claimed(claim) if claim.writer_epoch == writer_epoch => {
            Ok(UploadPointerObservation::ClaimedByCurrentWriter(eligible))
        }
        work::TextUploadWorkState::Claimed(_) => Ok(UploadPointerObservation::Eligible(eligible)),
        work::TextUploadWorkState::Blocked(_) => Err(corruption(
            "blocked text upload intent retained a runnable pointer",
        )),
    }
}

/// Claims one exact observed upload or reports that another delivery won.
pub(crate) async fn claim_upload(
    db: &Db,
    eligible: &EligibleUpload,
    active_mutations: &ActiveTextMutationRegistry,
    writer_epoch: WriterEpoch,
    sequence: ClaimSequence,
    now_unix_millis: u64,
    permission: ClaimPermission,
) -> Result<Option<ClaimedUpload>> {
    failpoints::trip(IndexOutboxFailpoint::UploadClaimBefore)?;
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let pointer_key = global_upload_key(eligible.record.intent_id);
    let Some(pointer_value) = transaction.get(&pointer_key).await? else {
        return Ok(None);
    };
    let pointer = decode_pointer(&pointer_value)?;
    if pointer.scope != eligible.scope || pointer.record_revision != eligible.record.revision {
        return Ok(None);
    }
    let intent_key = scoped_intent_key(pointer.scope, &pointer, eligible.record.intent_id);
    let Some(intent_value) = transaction.get(&intent_key).await? else {
        transaction.delete(pointer_key)?;
        transaction.commit().await?;
        return Ok(None);
    };
    let intent = decode_intent(&intent_value)?;
    if intent.revision != eligible.record.revision {
        return Ok(None);
    }
    validate_link(intent.intent_id, &pointer, &intent)?;
    if matches!(
        intent.owner,
        work::TextUploadOwner::ActiveMutation {
            writer_epoch: owner_epoch,
            ..
        } if owner_epoch == writer_epoch
    ) {
        match active_mutations
            .observe(pointer.scope, &intent)
            .map_err(|error| corruption(error.to_string()))?
        {
            ActiveTextMutationOwnerObservation::InFlight => return Ok(None),
            ActiveTextMutationOwnerObservation::Terminal => {}
        }
    }

    let authorized = match (&intent.work_state, permission) {
        (
            work::TextUploadWorkState::Queued {
                not_before_unix_millis,
            },
            ClaimPermission::Normal,
        ) => not_before_unix_millis.is_none_or(|deadline| deadline <= now_unix_millis),
        (work::TextUploadWorkState::Claimed(claim), ClaimPermission::Normal) => {
            claim.writer_epoch != writer_epoch
        }
        (work::TextUploadWorkState::Claimed(claim), ClaimPermission::SameEpochRecovery(proof)) => {
            claim.writer_epoch == writer_epoch && proof.authorizes(writer_epoch)
        }
        (
            work::TextUploadWorkState::Queued {
                not_before_unix_millis,
            },
            ClaimPermission::SameEpochRecovery(proof),
        ) => {
            proof.authorizes(writer_epoch)
                && not_before_unix_millis.is_none_or(|deadline| deadline <= now_unix_millis)
        }
        (work::TextUploadWorkState::Blocked(_), _) => false,
    };
    if !authorized {
        return Ok(None);
    }

    let claimed = intent
        .claim(OperationClaim {
            writer_epoch,
            sequence,
        })
        .map_err(work_model_error)?;
    let next_pointer = pointer_for(pointer.scope, &claimed);
    validate_link(claimed.intent_id, &next_pointer, &claimed)?;
    transaction.put(
        intent_key,
        index_values::encode_work_value(&index_values::IndexV2WorkValue::TextUploadIntent(
            Box::new(claimed.clone()),
        )),
    )?;
    transaction.put(
        pointer_key,
        index_values::encode_metadata_value(&IndexV2MetadataValue::UploadQueuePointer(
            next_pointer,
        )),
    )?;
    transaction.commit().await?;
    failpoints::trip(IndexOutboxFailpoint::UploadClaimAfter)?;
    Ok(Some(ClaimedUpload {
        scope: pointer.scope,
        record: claimed,
    }))
}

/// Requeues one exact claimed upload with deterministic bounded backoff.
pub(crate) async fn requeue_claimed_upload(
    db: &Db,
    claimed: &ClaimedUpload,
    now_unix_millis: u64,
) -> Result<bool> {
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let Some((intent_key, pointer_key, intent, pointer)) =
        load_exact_link(&transaction, claimed).await?
    else {
        return Ok(false);
    };
    if intent.revision != claimed.record.revision || intent.work_state != claimed.record.work_state
    {
        return Ok(false);
    }
    let deadline = now_unix_millis.saturating_add(backoff_millis(intent.attempt));
    let next = intent
        .transient_failure(deadline)
        .map_err(work_model_error)?;
    let next_pointer = pointer_for(pointer.scope, &next);
    validate_link(next.intent_id, &next_pointer, &next)?;
    transaction.put(
        intent_key,
        index_values::encode_work_value(&index_values::IndexV2WorkValue::TextUploadIntent(
            Box::new(next),
        )),
    )?;
    transaction.put(
        pointer_key,
        index_values::encode_metadata_value(&IndexV2MetadataValue::UploadQueuePointer(
            next_pointer,
        )),
    )?;
    transaction.commit().await?;
    Ok(true)
}

/// Requeues the exact upload coupled to one blob-mismatch operation blocker.
///
/// The operation repository invokes this inside its retry transaction. The
/// blocker payload supplies the intent ID directly, so retry remains an O(1)
/// point read and cannot attach an unrelated upload discovered by a scan.
pub(in crate::index_v2) async fn stage_blob_mismatch_retry(
    transaction: &DbTransaction,
    scope: DataScope,
    blocked_operation: &IndexOperationRecord,
    retried_operation: &IndexOperationRecord,
    intent_id: TextUploadIntentId,
) -> Result<()> {
    if !matches!(
        blocked_operation.execution_state(),
        super::super::IndexOperationExecutionState::Blocked(
            IndexOperationBlocker::BlobPublicationMismatch {
                intent_id: blocked_intent_id,
            }
        ) if *blocked_intent_id == intent_id
    ) || !matches!(
        retried_operation.execution_state(),
        super::super::IndexOperationExecutionState::Queued {
            not_before_unix_millis: None
        }
    ) || blocked_operation.operation_id() != retried_operation.operation_id()
        || blocked_operation.index_id() != retried_operation.index_id()
        || blocked_operation.identity() != retried_operation.identity()
        || blocked_operation.generation() != retried_operation.generation()
    {
        return Err(corruption(
            "blob-mismatch retry received a different operation checkpoint",
        ));
    }
    let intent_key = Key::Data {
        scope,
        kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::TextUploadIntent(
            index_keys::TextIntentOwnedKey {
                index_id: blocked_operation.index_id(),
                generation: blocked_operation.generation(),
                intent_id,
            },
        )),
    }
    .to_bytes();
    let Some(intent_value) = transaction.get(&intent_key).await? else {
        return Err(corruption(
            "blob-mismatch operation blocker has no coupled upload intent",
        ));
    };
    let intent = decode_intent(&intent_value)?;
    let work::TextUploadOwner::Build {
        operation_id,
        expected_operation_revision,
    } = intent.owner
    else {
        return Err(corruption(
            "blob-mismatch operation blocker names an active upload",
        ));
    };
    if intent.intent_id != intent_id
        || intent.index_id != blocked_operation.index_id()
        || &intent.identity != blocked_operation.identity()
        || intent.generation != blocked_operation.generation()
        || operation_id != blocked_operation.operation_id()
        || expected_operation_revision != blocked_operation.operation_revision()
        || !matches!(intent.phase, work::TextUploadPhase::Prepared)
        || !matches!(
            intent.work_state,
            work::TextUploadWorkState::Blocked(
                IndexOperationBlocker::BlobPublicationMismatch {
                    intent_id: blocked_intent_id,
                }
            ) if blocked_intent_id == intent_id
        )
    {
        return Err(corruption(
            "blob-mismatch operation and upload blockers disagree",
        ));
    }
    let pointer_key = global_upload_key(intent_id);
    if transaction.get(&pointer_key).await?.is_some() {
        return Err(corruption(
            "blocked blob-mismatch upload retained a runnable pointer",
        ));
    }
    let next = intent
        .retry_blob_mismatch(retried_operation.operation_revision())
        .map_err(work_model_error)?;
    let pointer = pointer_for(scope, &next);
    validate_link(next.intent_id, &pointer, &next)?;
    transaction.put(
        intent_key,
        index_values::encode_work_value(&index_values::IndexV2WorkValue::TextUploadIntent(
            Box::new(next),
        )),
    )?;
    transaction.put(
        pointer_key,
        index_values::encode_metadata_value(&IndexV2MetadataValue::UploadQueuePointer(pointer)),
    )?;
    Ok(())
}

/// Runs one claimed reconciliation step and commits its exact next revision.
pub(crate) async fn execute_claimed_upload_step(
    db: &Db,
    claimed: &ClaimedUpload,
    driver: &dyn TextUploadDriver,
    now_unix_millis: u64,
) -> Result<TextUploadStepResult> {
    let read_transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let Some((_intent_key, _pointer_key, observed, _pointer)) =
        load_exact_link(&read_transaction, claimed).await?
    else {
        return Err(corruption("claimed upload disappeared before dispatch"));
    };
    if observed.revision != claimed.record.revision
        || observed.work_state != claimed.record.work_state
    {
        return Err(corruption(
            "claimed upload revision changed before reconciliation dispatch",
        ));
    }
    drop(read_transaction);

    failpoints::trip(IndexOutboxFailpoint::UploadStepBefore)?;
    let prepared = driver.prepare_step(&observed).await;
    failpoints::trip(IndexOutboxFailpoint::UploadStepAfter)?;
    let prepared = match prepared {
        Ok(prepared) => prepared,
        Err(error) => {
            tracing::warn!(
                intent_id = %observed.intent_id.as_uuid(),
                %error,
                "text upload driver returned a transient failure"
            );
            requeue_claimed_upload(db, claimed, now_unix_millis).await?;
            return Ok(TextUploadStepResult::TransientFailure);
        }
    };
    let step = prepared.outcome();
    if step == TextUploadStepResult::TransientFailure {
        requeue_claimed_upload(db, claimed, now_unix_millis).await?;
        return Ok(TextUploadStepResult::TransientFailure);
    }

    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let Some((intent_key, pointer_key, intent, pointer)) =
        load_exact_link(&transaction, claimed).await?
    else {
        return Err(corruption(
            "claimed upload disappeared before checkpoint staging",
        ));
    };
    if intent.revision != claimed.record.revision || intent.work_state != claimed.record.work_state
    {
        return Err(corruption(
            "claimed upload revision changed before checkpoint staging",
        ));
    }
    failpoints::trip(IndexOutboxFailpoint::UploadCheckpointBefore)?;
    let checkpoint = match step {
        TextUploadStepResult::PublicationSucceeded => StagedUploadCheckpoint::Runnable(
            intent.publication_succeeded().map_err(work_model_error)?,
        ),
        TextUploadStepResult::NonPublicationProven => {
            super::reclaim::validate_non_publication_proof(&transaction, claimed.scope, &intent)
                .await?;
            StagedUploadCheckpoint::Runnable(
                intent.non_publication_proven().map_err(work_model_error)?,
            )
        }
        TextUploadStepResult::NonPublicationReleased => {
            super::reclaim::stage_non_publication_cleanup(&transaction, claimed.scope, &intent)
                .await?;
            StagedUploadCheckpoint::Delete
        }
        TextUploadStepResult::SharedBlobReclaimable => {
            super::reclaim::stage_shared_blob_reclaim(&transaction, claimed.scope, &intent).await?;
            StagedUploadCheckpoint::Runnable(intent.become_reclaimable().map_err(work_model_error)?)
        }
        TextUploadStepResult::ReclaimRootAssigned => {
            match super::blob_gc::stage_upload_reclaim_root(&transaction, claimed.scope, &intent)
                .await?
            {
                super::blob_gc::UploadReclaimRootTransition::Assigned(run_id) => {
                    StagedUploadCheckpoint::Runnable(
                        intent
                            .assign_reclaim_root(run_id)
                            .map_err(work_model_error)?,
                    )
                }
                super::blob_gc::UploadReclaimRootTransition::GenerationCleanupOwnsAssignment => {
                    drop(transaction);
                    requeue_claimed_upload(db, claimed, now_unix_millis).await?;
                    return Ok(TextUploadStepResult::TransientFailure);
                }
            }
        }
        TextUploadStepResult::ReclaimFenceRevalidated => {
            let Some(next) = super::blob_gc::stage_upload_reclaim_first_pass(
                &transaction,
                claimed.scope,
                &intent,
            )
            .await?
            else {
                drop(transaction);
                requeue_claimed_upload(db, claimed, now_unix_millis).await?;
                return Ok(TextUploadStepResult::TransientFailure);
            };
            StagedUploadCheckpoint::Runnable(next)
        }
        TextUploadStepResult::BlobMismatch => {
            let (index, operation) =
                super::build_owner::load_exact(&transaction, claimed.scope, &intent).await?;
            let blocked_operation = outbox::stage_blob_mismatch_block(
                &transaction,
                claimed.scope,
                &index,
                &operation,
                intent.intent_id,
            )?;
            StagedUploadCheckpoint::Blocked(
                intent
                    .block_for_blob_mismatch(blocked_operation.operation_revision())
                    .map_err(work_model_error)?,
            )
        }
        TextUploadStepResult::AttachUploaded => {
            let authorization = super::attachment::stage_build_artifact_attachment(
                &transaction,
                claimed.scope,
                &intent,
            )
            .await?;
            StagedUploadCheckpoint::Runnable(
                intent
                    .reference_committed(authorization)
                    .map_err(work_model_error)?,
            )
        }
        TextUploadStepResult::ResolveActiveReference => {
            match super::attachment::stage_active_manifest_reference_checkpoint(
                &transaction,
                claimed.scope,
                &intent,
            )
            .await?
            {
                Some(authorization) => StagedUploadCheckpoint::Runnable(
                    intent
                        .reference_committed(authorization)
                        .map_err(work_model_error)?,
                ),
                None => {
                    super::reclaim::stage_active_graph_abort_reclaim(
                        &transaction,
                        claimed.scope,
                        &intent,
                    )
                    .await?;
                    StagedUploadCheckpoint::Runnable(
                        intent.active_graph_aborted().map_err(work_model_error)?,
                    )
                }
            }
        }
        TextUploadStepResult::ReferenceReleased => {
            super::attachment::stage_reference_anchor_cleanup(&transaction, claimed.scope, &intent)
                .await?;
            StagedUploadCheckpoint::Delete
        }
        TextUploadStepResult::TransientFailure => {
            unreachable!("transient upload steps return before checkpoint staging")
        }
    };
    match checkpoint {
        StagedUploadCheckpoint::Runnable(next) => {
            let next_pointer = pointer_for(pointer.scope, &next);
            validate_link(next.intent_id, &next_pointer, &next)?;
            transaction.put(
                intent_key,
                index_values::encode_work_value(&index_values::IndexV2WorkValue::TextUploadIntent(
                    Box::new(next),
                )),
            )?;
            transaction.put(
                pointer_key,
                index_values::encode_metadata_value(&IndexV2MetadataValue::UploadQueuePointer(
                    next_pointer,
                )),
            )?;
        }
        StagedUploadCheckpoint::Blocked(next) => {
            transaction.put(
                intent_key,
                index_values::encode_work_value(&index_values::IndexV2WorkValue::TextUploadIntent(
                    Box::new(next),
                )),
            )?;
            transaction.delete(pointer_key)?;
        }
        StagedUploadCheckpoint::Delete => {
            transaction.delete(intent_key)?;
            transaction.delete(pointer_key)?;
        }
    }
    failpoints::trip(IndexOutboxFailpoint::UploadCheckpointAfter)?;
    failpoints::trip(IndexOutboxFailpoint::UploadCommitBefore)?;
    transaction.commit().await?;
    drop(prepared);
    failpoints::trip(IndexOutboxFailpoint::UploadCommitAfter)?;
    Ok(step)
}

/// Loads and cross-checks the exact link named by one claimed observation.
async fn load_exact_link(
    transaction: &DbTransaction,
    claimed: &ClaimedUpload,
) -> Result<
    Option<(
        Bytes,
        Bytes,
        work::TextUploadIntentValue,
        UploadQueuePointerValue,
    )>,
> {
    let pointer_key = global_upload_key(claimed.record.intent_id);
    let Some(pointer_value) = transaction.get(&pointer_key).await? else {
        return Ok(None);
    };
    let pointer = decode_pointer(&pointer_value)?;
    if pointer.scope != claimed.scope {
        return Err(corruption("upload pointer changed scope"));
    }
    let intent_key = scoped_intent_key(pointer.scope, &pointer, claimed.record.intent_id);
    let Some(intent_value) = transaction.get(&intent_key).await? else {
        return Ok(None);
    };
    let intent = decode_intent(&intent_value)?;
    validate_link(intent.intent_id, &pointer, &intent)?;
    Ok(Some((intent_key, pointer_key, intent, pointer)))
}

/// Constructs the typed global discovery key for one upload UUID.
fn global_upload_key(intent_id: TextUploadIntentId) -> Bytes {
    Key::Global {
        kind: GlobalKeyKind::IndexV2(index_keys::GlobalIndexV2Key::UploadPointer(intent_id)),
    }
    .to_bytes()
}

/// Constructs the typed scoped intent key from its pointer-owned identity.
fn scoped_intent_key(
    scope: DataScope,
    pointer: &UploadQueuePointerValue,
    intent_id: TextUploadIntentId,
) -> Bytes {
    Key::Data {
        scope,
        kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::TextUploadIntent(
            index_keys::TextIntentOwnedKey {
                index_id: pointer.index_id,
                generation: pointer.generation,
                intent_id,
            },
        )),
    }
    .to_bytes()
}

/// Builds the only valid pointer value for one runnable intent revision.
fn pointer_for(scope: DataScope, intent: &work::TextUploadIntentValue) -> UploadQueuePointerValue {
    UploadQueuePointerValue {
        scope,
        index_id: intent.index_id,
        generation: intent.generation,
        record_revision: intent.revision,
    }
}

/// Decodes one upload pointer and rejects metadata-kind substitution.
fn decode_pointer(value: &[u8]) -> Result<UploadQueuePointerValue> {
    let IndexV2MetadataValue::UploadQueuePointer(pointer) =
        index_values::decode_metadata_value(value)?
    else {
        return Err(corruption(
            "upload pointer key contains a different metadata value kind",
        ));
    };
    Ok(pointer)
}

/// Decodes one upload intent and rejects work-value-kind substitution.
fn decode_intent(value: &[u8]) -> Result<work::TextUploadIntentValue> {
    let index_values::IndexV2WorkValue::TextUploadIntent(intent) =
        index_values::decode_work_value(value)?
    else {
        return Err(corruption(
            "upload pointer resolves to a different work value kind",
        ));
    };
    Ok(*intent)
}

/// Cross-checks all redundant pointer and scoped-intent identity fields.
fn validate_link(
    intent_id: TextUploadIntentId,
    pointer: &UploadQueuePointerValue,
    intent: &work::TextUploadIntentValue,
) -> Result<()> {
    if intent.intent_id != intent_id
        || intent.index_id != pointer.index_id
        || intent.generation != pointer.generation
        || intent.revision != pointer.record_revision
    {
        return Err(corruption(
            "upload pointer disagrees with its authoritative scoped intent",
        ));
    }
    if matches!(intent.work_state, work::TextUploadWorkState::Blocked(_)) {
        return Err(corruption(
            "blocked text upload intent retained a runnable pointer",
        ));
    }
    Ok(())
}

/// Computes a deterministic saturating retry delay from the durable attempt.
fn backoff_millis(attempt: u32) -> u64 {
    let shift = attempt.min(31);
    BASE_UPLOAD_BACKOFF_MILLIS
        .saturating_mul(1_u64 << shift)
        .min(MAX_UPLOAD_BACKOFF_MILLIS)
}

/// Converts a persisted wall-clock deadline into a bounded sleep duration.
fn observed_delay(now_unix_millis: u64, not_before_unix_millis: u64) -> u64 {
    not_before_unix_millis
        .saturating_sub(now_unix_millis)
        .min(MAX_UPLOAD_BACKOFF_MILLIS)
}

/// Maps construction failures at this repository boundary into invariants.
fn work_model_error(error: work::IndexWorkModelError) -> HelixDbError {
    HelixDbError::InvariantViolation(error.to_string())
}

/// Constructs a fail-closed upload-catalog corruption diagnostic.
fn corruption(reason: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(reason.into())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sha2::{Digest, Sha256};
    use slatedb::object_store::memory::InMemory;

    use super::*;
    use crate::config::TextIndexDefinition;
    use crate::index_v2::blob_publication::BlobPublicationPermit;
    use crate::index_v2::outbox::SameEpochRecoveryProof;
    use crate::index_v2::text::upload::{
        stage_prepared_upload, PreparedTextUploadIntent, PreparedUploadStageOutcome,
    };
    use crate::index_v2::{
        BlobPublicationPermitId, IndexComponent, IndexElementKind, IndexGenerationId, IndexId,
        IndexIdentity, IndexIdentityFamily, IndexOperationId, IndexOperationRevision,
        IndexRecordV2, IndexRevision, IndexStateTransition, MutationId, PhysicalGeneration,
        TextIntentRevision, ValidatedDynamicIndexDefinition,
    };

    /// Opens one isolated in-memory SlateDB for repository tests.
    async fn raw_db(name: &str) -> Db {
        Db::open(name, Arc::new(InMemory::new())).await.unwrap()
    }

    /// Constructs the canonical text identity shared by test uploads.
    fn text_identity() -> IndexIdentity {
        IndexIdentity::new(
            IndexIdentityFamily::Text,
            IndexElementKind::Node,
            IndexComponent::try_new("label", "Document").unwrap(),
            IndexComponent::try_new("property", "body").unwrap(),
        )
    }

    /// Constructs a valid split whose blob identity is derived from its bytes.
    fn split(payload: &[u8]) -> work::SplitRef {
        let blob = work::BlobRef::new(
            Sha256::digest(payload).into(),
            u64::try_from(payload.len()).unwrap(),
        );
        work::SplitRef::try_new(blob, 0, 0, 0, blob.size()).unwrap()
    }

    /// Builds a complete initial intent without requiring object I/O.
    fn prepared(
        intent_id: TextUploadIntentId,
        owner: work::TextUploadOwner,
    ) -> PreparedTextUploadIntent {
        let split = split(intent_id.as_bytes());
        PreparedTextUploadIntent::try_new(
            intent_id,
            IndexId::initial(),
            text_identity(),
            IndexGenerationId::initial(),
            work::TextPartition::Unpartitioned,
            split.blob(),
            BlobPublicationPermit::from_id(
                BlobPublicationPermitId::from_bytes(*intent_id.as_bytes()).unwrap(),
            ),
            owner,
            work::TextUploadAttachment::ManifestSplit(split),
        )
        .unwrap()
    }

    /// Commits the authoritative intent, discovery pointer, and reachability row.
    async fn put_prepared(db: &Db, scope: DataScope, prepared: &PreparedTextUploadIntent) {
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert_eq!(
            stage_prepared_upload(&transaction, scope, prepared)
                .await
                .unwrap(),
            PreparedUploadStageOutcome::Staged
        );
        transaction.commit().await.unwrap();
    }

    /// Observes build-owned fixtures through an empty Active-owner registry.
    async fn observe_upload_pointer(
        db: &Db,
        intent_id: TextUploadIntentId,
        writer_epoch: WriterEpoch,
        now_unix_millis: u64,
    ) -> Result<UploadPointerObservation> {
        super::observe_upload_pointer(
            db,
            intent_id,
            &super::super::active_mutation::ActiveTextMutationRegistry::new(),
            writer_epoch,
            now_unix_millis,
        )
        .await
    }

    /// Claims build-owned fixtures through an empty Active-owner registry.
    async fn claim_upload(
        db: &Db,
        eligible: &EligibleUpload,
        writer_epoch: WriterEpoch,
        sequence: ClaimSequence,
        now_unix_millis: u64,
        permission: ClaimPermission,
    ) -> Result<Option<ClaimedUpload>> {
        super::claim_upload(
            db,
            eligible,
            &super::super::active_mutation::ActiveTextMutationRegistry::new(),
            writer_epoch,
            sequence,
            now_unix_millis,
            permission,
        )
        .await
    }

    /// Deletes one exact key through the same serializable test boundary.
    async fn delete_key(db: &Db, key: Bytes) {
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        transaction.delete(key).unwrap();
        transaction.commit().await.unwrap();
    }

    /// Stages and claims one fresh build-owned intent for driver tests.
    async fn put_and_claim(
        db: &Db,
        intent_id: TextUploadIntentId,
        writer_epoch: WriterEpoch,
    ) -> ClaimedUpload {
        let prepared = prepared(
            intent_id,
            work::TextUploadOwner::Build {
                operation_id: IndexOperationId::from_bytes([42; 16]).unwrap(),
                expected_operation_revision: IndexOperationRevision::initial(),
            },
        );
        put_prepared(db, DataScope::LegacyUnscoped, &prepared).await;
        let UploadPointerObservation::Eligible(eligible) =
            observe_upload_pointer(db, intent_id, writer_epoch, 0)
                .await
                .unwrap()
        else {
            panic!("fresh upload is eligible");
        };
        claim_upload(
            db,
            &eligible,
            writer_epoch,
            ClaimSequence::new(1).unwrap(),
            0,
            ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .expect("fresh upload claim commits")
    }

    /// Deterministic driver returning one closed reconciliation outcome.
    struct StaticStepDriver(TextUploadStepResult);

    #[async_trait]
    impl TextUploadDriver for StaticStepDriver {
        async fn prepare_step(
            &self,
            _intent: &work::TextUploadIntentValue,
        ) -> Result<PreparedTextUploadStep> {
            Ok(match self.0 {
                TextUploadStepResult::PublicationSucceeded => {
                    PreparedTextUploadStep::publication_succeeded()
                }
                TextUploadStepResult::NonPublicationProven => {
                    PreparedTextUploadStep::non_publication_proven()
                }
                TextUploadStepResult::NonPublicationReleased => {
                    PreparedTextUploadStep::non_publication_released()
                }
                TextUploadStepResult::SharedBlobReclaimable => {
                    PreparedTextUploadStep::shared_blob_reclaimable(())
                }
                TextUploadStepResult::ReclaimRootAssigned => {
                    PreparedTextUploadStep::assign_reclaim_root()
                }
                TextUploadStepResult::ReclaimFenceRevalidated => {
                    PreparedTextUploadStep::reclaim_fence_revalidated()
                }
                TextUploadStepResult::BlobMismatch => PreparedTextUploadStep::blob_mismatch(),
                TextUploadStepResult::AttachUploaded => PreparedTextUploadStep::attach_uploaded(()),
                TextUploadStepResult::ResolveActiveReference => {
                    PreparedTextUploadStep::resolve_active_reference(())
                }
                TextUploadStepResult::ReferenceReleased => {
                    PreparedTextUploadStep::reference_released()
                }
                TextUploadStepResult::TransientFailure => {
                    PreparedTextUploadStep::transient_failure()
                }
            })
        }
    }

    /// Driver error used to prove repository-owned transient recovery.
    struct FailingStepDriver;

    #[async_trait]
    impl TextUploadDriver for FailingStepDriver {
        async fn prepare_step(
            &self,
            _intent: &work::TextUploadIntentValue,
        ) -> Result<PreparedTextUploadStep> {
            Err(HelixDbError::InvariantViolation(
                "injected upload driver outage".to_string(),
            ))
        }
    }

    #[tokio::test]
    async fn cleanup_gate_requeues_a_claimed_singleton_assignment_without_a_root() {
        let db = raw_db("text-upload-cleanup-gated-singleton").await;
        let scope = DataScope::LegacyUnscoped;
        let intent_id = TextUploadIntentId::from_bytes([55; 16]).unwrap();
        let writer_epoch = WriterEpoch::from_bytes([56; 16]).unwrap();
        let claimed = put_and_claim(&db, intent_id, writer_epoch).await;
        let reclaimable = claimed.record.become_reclaimable().unwrap();
        let rows = super::super::upload::upload_anchor_rows(scope, &reclaimable).unwrap();
        let candidate_key = Key::Data {
            scope,
            kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::BlobGcCandidate(
                index_keys::BlobGcCandidateKey {
                    index_id: reclaimable.index_id,
                    generation: reclaimable.generation,
                    owner: index_keys::BlobGcCandidateKeyOwner::UploadIntent(intent_id),
                    blob_hash: index_keys::BlobHash::new(*reclaimable.blob.hash()),
                },
            )),
        }
        .to_bytes();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        transaction.delete(&rows.reachability_key).unwrap();
        transaction
            .put(
                candidate_key,
                index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcCandidate(
                    work::BlobGcCandidateValue {
                        owner: work::BlobGcCandidateOwner::UploadIntent(intent_id),
                        index_id: reclaimable.index_id,
                        generation: reclaimable.generation,
                        blob: reclaimable.blob,
                    },
                )),
            )
            .unwrap();
        transaction
            .put(&rows.intent_key, &rows.intent_value)
            .unwrap();
        transaction
            .put(&rows.pointer_key, &rows.pointer_value)
            .unwrap();
        transaction.commit().await.unwrap();

        let definition = ValidatedDynamicIndexDefinition::try_from(
            TextIndexDefinition::new_node("Document", "body").unwrap(),
        )
        .unwrap();
        let dropping = IndexRecordV2::building(
            reclaimable.index_id,
            definition,
            IndexRevision::initial(),
            PhysicalGeneration::Text {
                generation: reclaimable.generation,
            },
            IndexOperationId::from_bytes([42; 16]).unwrap(),
        )
        .unwrap()
        .transition(IndexStateTransition::Activate)
        .unwrap()
        .transition(IndexStateTransition::BeginDrop {
            drop_operation_id: IndexOperationId::from_bytes([57; 16]).unwrap(),
        })
        .unwrap();
        db.put(
            Key::Data {
                scope,
                kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::index_record(
                    dropping.identity().clone(),
                )),
            }
            .to_bytes(),
            index_values::encode_index_record(&dropping),
        )
        .await
        .unwrap();

        let UploadPointerObservation::Eligible(eligible) =
            observe_upload_pointer(&db, intent_id, writer_epoch, 0)
                .await
                .unwrap()
        else {
            panic!("reclaimable intent is eligible before cleanup gating");
        };
        let claimed = claim_upload(
            &db,
            &eligible,
            writer_epoch,
            ClaimSequence::new(2).unwrap(),
            0,
            ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .expect("reclaimable intent claim commits");
        assert_eq!(
            execute_claimed_upload_step(
                &db,
                &claimed,
                &StaticStepDriver(TextUploadStepResult::ReclaimRootAssigned),
                100,
            )
            .await
            .unwrap(),
            TextUploadStepResult::TransientFailure
        );
        assert!(matches!(
            observe_upload_pointer(&db, intent_id, writer_epoch, 100)
                .await
                .unwrap(),
            UploadPointerObservation::Delayed { .. }
        ));
        assert!(
            super::super::blob_gc::scan_root_page(&db, None, NonZeroUsize::MIN)
                .await
                .unwrap()
                .run_ids
                .is_empty()
        );
    }

    #[tokio::test]
    async fn queue_scan_is_bounded_resumable_and_empty_safe() {
        let db = raw_db("text-upload-queue-page").await;
        assert!(UploadQueuePageSize::new(0).is_err());
        let empty = scan_upload_queue_page(&db, None, UploadQueuePageSize::new(2).unwrap())
            .await
            .unwrap();
        assert!(empty.intent_ids.is_empty());
        assert!(empty.resume_after.is_none());
        assert!(empty.prefix_exhausted);
        assert!(matches!(
            observe_upload_pointer(
                &db,
                TextUploadIntentId::from_bytes([13; 16]).unwrap(),
                WriterEpoch::from_bytes([14; 16]).unwrap(),
                0,
            )
            .await
            .unwrap(),
            UploadPointerObservation::StalePointerRemoved
        ));

        let owner = work::TextUploadOwner::Build {
            operation_id: IndexOperationId::from_bytes([15; 16]).unwrap(),
            expected_operation_revision: IndexOperationRevision::initial(),
        };
        let ids = [
            TextUploadIntentId::from_bytes([16; 16]).unwrap(),
            TextUploadIntentId::from_bytes([17; 16]).unwrap(),
            TextUploadIntentId::from_bytes([18; 16]).unwrap(),
        ];
        for intent_id in ids {
            put_prepared(
                &db,
                DataScope::Tenant(crate::encoding::v1::keys::tenant::TenantId::from_u128(42)),
                &prepared(intent_id, owner),
            )
            .await;
        }
        let first = scan_upload_queue_page(&db, None, UploadQueuePageSize::new(2).unwrap())
            .await
            .unwrap();
        assert_eq!(first.intent_ids, ids[..2]);
        assert_eq!(first.resume_after, Some(ids[1]));
        assert!(!first.prefix_exhausted);
        let second = scan_upload_queue_page(
            &db,
            first.resume_after,
            UploadQueuePageSize::new(2).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(second.intent_ids, ids[2..]);
        assert_eq!(second.resume_after, Some(ids[2]));
        assert!(second.prefix_exhausted);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn exact_revision_claim_recovery_and_backoff_form_one_chain() {
        let db = raw_db("text-upload-queue-exact-chain").await;
        let scope = DataScope::LegacyUnscoped;
        let intent_id = TextUploadIntentId::from_bytes([1; 16]).unwrap();
        let prepared = prepared(
            intent_id,
            work::TextUploadOwner::Build {
                operation_id: IndexOperationId::from_bytes([2; 16]).unwrap(),
                expected_operation_revision: IndexOperationRevision::initial(),
            },
        );
        put_prepared(&db, scope, &prepared).await;

        let page = scan_upload_queue_page(&db, None, UploadQueuePageSize::new(1).unwrap())
            .await
            .unwrap();
        assert_eq!(page.intent_ids, vec![intent_id]);
        assert_eq!(page.resume_after, Some(intent_id));

        let writer_epoch = WriterEpoch::from_bytes([3; 16]).unwrap();
        let UploadPointerObservation::Eligible(eligible) =
            observe_upload_pointer(&db, intent_id, writer_epoch, 100)
                .await
                .unwrap()
        else {
            panic!("fresh prepared upload must be eligible");
        };
        let first_claim = claim_upload(
            &db,
            &eligible,
            writer_epoch,
            ClaimSequence::new(1).unwrap(),
            100,
            ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .expect("exact observed revision is claimable");
        assert_eq!(first_claim.record.revision.get(), 2);
        assert_eq!(first_claim.record.attempt, 1);
        assert!(claim_upload(
            &db,
            &eligible,
            writer_epoch,
            ClaimSequence::new(2).unwrap(),
            100,
            ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .is_none());
        assert!(matches!(
            observe_upload_pointer(&db, intent_id, writer_epoch, 100)
                .await
                .unwrap(),
            UploadPointerObservation::ClaimedByCurrentWriter(_)
        ));

        let claimed_observation = EligibleUpload {
            scope: first_claim.scope,
            record: first_claim.record.clone(),
        };
        assert!(claim_upload(
            &db,
            &claimed_observation,
            writer_epoch,
            ClaimSequence::new(3).unwrap(),
            100,
            ClaimPermission::SameEpochRecovery(SameEpochRecoveryProof::after_join(
                WriterEpoch::from_bytes([19; 16]).unwrap(),
            )),
        )
        .await
        .unwrap()
        .is_none());
        let recovered = claim_upload(
            &db,
            &claimed_observation,
            writer_epoch,
            ClaimSequence::new(4).unwrap(),
            100,
            ClaimPermission::SameEpochRecovery(SameEpochRecoveryProof::after_join(writer_epoch)),
        )
        .await
        .unwrap()
        .expect("joined same-epoch task authorizes exact claim replacement");
        assert_eq!(recovered.record.revision.get(), 3);
        assert_eq!(recovered.record.attempt, 2);

        let next_epoch = WriterEpoch::from_bytes([20; 16]).unwrap();
        let UploadPointerObservation::Eligible(prior_writer_claim) =
            observe_upload_pointer(&db, intent_id, next_epoch, 100)
                .await
                .unwrap()
        else {
            panic!("a prior-writer durable claim must be replaceable");
        };
        let recovered = claim_upload(
            &db,
            &prior_writer_claim,
            next_epoch,
            ClaimSequence::new(1).unwrap(),
            100,
            ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .expect("new writer epoch fences and replaces the exact old claim");
        assert_eq!(recovered.record.revision.get(), 4);
        assert_eq!(recovered.record.attempt, 3);

        assert!(requeue_claimed_upload(&db, &recovered, 100).await.unwrap());
        assert!(!requeue_claimed_upload(&db, &recovered, 100).await.unwrap());
        assert!(matches!(
            observe_upload_pointer(&db, intent_id, writer_epoch, 101)
                .await
                .unwrap(),
            UploadPointerObservation::Delayed {
                delay_millis: 7_999
            }
        ));
        assert!(matches!(
            observe_upload_pointer(&db, intent_id, writer_epoch, 8_100)
                .await
                .unwrap(),
            UploadPointerObservation::Eligible(_)
        ));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn current_epoch_active_owner_requires_its_exact_guard_until_terminal() {
        let db = raw_db("text-upload-queue-active-owner").await;
        let scope = DataScope::LegacyUnscoped;
        let intent_id = TextUploadIntentId::from_bytes([4; 16]).unwrap();
        let owner_epoch = WriterEpoch::from_bytes([5; 16]).unwrap();
        let active_mutations = super::super::active_mutation::ActiveTextMutationRegistry::new();
        let prepared = prepared(
            intent_id,
            work::TextUploadOwner::ActiveMutation {
                writer_epoch: owner_epoch,
                mutation_id: MutationId::from_bytes([6; 16]).unwrap(),
                active_record_revision: IndexRevision::initial(),
            },
        );
        let guard = active_mutations.register(scope, prepared.value()).unwrap();
        put_prepared(&db, scope, &prepared).await;

        assert!(matches!(
            super::observe_upload_pointer(&db, intent_id, &active_mutations, owner_epoch, 0,)
                .await
                .unwrap(),
            UploadPointerObservation::ActiveOwnerCurrentWriter
        ));
        let current_owner_observation = EligibleUpload {
            scope,
            record: prepared.value().clone(),
        };
        assert!(super::claim_upload(
            &db,
            &current_owner_observation,
            &active_mutations,
            owner_epoch,
            ClaimSequence::new(1).unwrap(),
            0,
            ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .is_none());

        let _terminal = guard.finish().unwrap();
        let UploadPointerObservation::Eligible(terminal) =
            super::observe_upload_pointer(&db, intent_id, &active_mutations, owner_epoch, 0)
                .await
                .unwrap()
        else {
            panic!("a terminal current-writer owner becomes worker-reconcilable");
        };
        assert!(super::claim_upload(
            &db,
            &terminal,
            &active_mutations,
            owner_epoch,
            ClaimSequence::new(1).unwrap(),
            0,
            ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .is_some());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn missing_same_epoch_active_owner_fails_closed_but_fenced_owner_is_claimable() {
        let db = raw_db("text-upload-queue-missing-active-owner").await;
        let scope = DataScope::LegacyUnscoped;
        let intent_id = TextUploadIntentId::from_bytes([14; 16]).unwrap();
        let owner_epoch = WriterEpoch::from_bytes([15; 16]).unwrap();
        let active_mutations = super::super::active_mutation::ActiveTextMutationRegistry::new();
        let prepared = prepared(
            intent_id,
            work::TextUploadOwner::ActiveMutation {
                writer_epoch: owner_epoch,
                mutation_id: MutationId::from_bytes([16; 16]).unwrap(),
                active_record_revision: IndexRevision::initial(),
            },
        );
        put_prepared(&db, scope, &prepared).await;

        assert!(matches!(
            super::observe_upload_pointer(&db, intent_id, &active_mutations, owner_epoch, 0,).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        let current_owner_observation = EligibleUpload {
            scope,
            record: prepared.value().clone(),
        };
        assert!(matches!(
            super::claim_upload(
                &db,
                &current_owner_observation,
                &active_mutations,
                owner_epoch,
                ClaimSequence::new(1).unwrap(),
                0,
                ClaimPermission::Normal,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));

        let next_epoch = WriterEpoch::from_bytes([7; 16]).unwrap();
        let UploadPointerObservation::Eligible(fenced) =
            super::observe_upload_pointer(&db, intent_id, &active_mutations, next_epoch, 0)
                .await
                .unwrap()
        else {
            panic!("a prior-writer active upload becomes worker-reconcilable");
        };
        assert!(super::claim_upload(
            &db,
            &fenced,
            &active_mutations,
            next_epoch,
            ClaimSequence::new(1).unwrap(),
            0,
            ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .is_some());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn orphan_pointer_is_removed_but_disagreeing_link_fails_closed() {
        let db = raw_db("text-upload-queue-orphan-and-corrupt").await;
        let orphan_id = TextUploadIntentId::from_bytes([8; 16]).unwrap();
        let orphan_key = global_upload_key(orphan_id);
        db.put(
            orphan_key.clone(),
            index_values::encode_metadata_value(&IndexV2MetadataValue::UploadQueuePointer(
                UploadQueuePointerValue {
                    scope: DataScope::LegacyUnscoped,
                    index_id: IndexId::initial(),
                    generation: IndexGenerationId::initial(),
                    record_revision: TextIntentRevision::initial(),
                },
            )),
        )
        .await
        .unwrap();
        assert!(matches!(
            observe_upload_pointer(&db, orphan_id, WriterEpoch::from_bytes([9; 16]).unwrap(), 0,)
                .await
                .unwrap(),
            UploadPointerObservation::StalePointerRemoved
        ));
        assert!(db.get(orphan_key).await.unwrap().is_none());

        let intent_id = TextUploadIntentId::from_bytes([10; 16]).unwrap();
        let prepared = prepared(
            intent_id,
            work::TextUploadOwner::Build {
                operation_id: IndexOperationId::from_bytes([11; 16]).unwrap(),
                expected_operation_revision: IndexOperationRevision::initial(),
            },
        );
        put_prepared(&db, DataScope::LegacyUnscoped, &prepared).await;
        db.put(
            global_upload_key(intent_id),
            index_values::encode_metadata_value(&IndexV2MetadataValue::UploadQueuePointer(
                UploadQueuePointerValue {
                    scope: DataScope::LegacyUnscoped,
                    index_id: IndexId::initial(),
                    generation: IndexGenerationId::initial(),
                    record_revision: TextIntentRevision::new(2).unwrap(),
                },
            )),
        )
        .await
        .unwrap();
        assert!(matches!(
            observe_upload_pointer(
                &db,
                intent_id,
                WriterEpoch::from_bytes([12; 16]).unwrap(),
                0,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn blocked_and_substituted_queue_values_fail_closed() {
        let db = raw_db("text-upload-queue-value-kinds").await;
        let scope = DataScope::LegacyUnscoped;
        let intent_id = TextUploadIntentId::from_bytes([26; 16]).unwrap();
        let blocked_prepared = prepared(
            intent_id,
            work::TextUploadOwner::Build {
                operation_id: IndexOperationId::from_bytes([27; 16]).unwrap(),
                expected_operation_revision: IndexOperationRevision::initial(),
            },
        );
        put_prepared(&db, scope, &blocked_prepared).await;
        let pointer = pointer_for(scope, blocked_prepared.value());
        let blocked = work::TextUploadIntentValue::try_new(
            blocked_prepared.value().intent_id,
            blocked_prepared.value().revision,
            blocked_prepared.value().index_id,
            blocked_prepared.value().identity.clone(),
            blocked_prepared.value().generation,
            blocked_prepared.value().partition.clone(),
            blocked_prepared.value().blob,
            blocked_prepared.value().publication_permit_id,
            blocked_prepared.value().owner,
            blocked_prepared.value().attachment,
            blocked_prepared.value().phase.clone(),
            blocked_prepared.value().attempt,
            work::TextUploadWorkState::Blocked(
                crate::index_v2::IndexOperationBlocker::BlobPublicationMismatch { intent_id },
            ),
        )
        .unwrap();
        db.put(
            scoped_intent_key(scope, &pointer, intent_id),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextUploadIntent(
                Box::new(blocked),
            )),
        )
        .await
        .unwrap();
        assert!(matches!(
            observe_upload_pointer(
                &db,
                intent_id,
                WriterEpoch::from_bytes([28; 16]).unwrap(),
                0,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));

        let wrong_pointer_id = TextUploadIntentId::from_bytes([29; 16]).unwrap();
        db.put(
            global_upload_key(wrong_pointer_id),
            index_values::encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                crate::index_v2::IndexStorageVersion::CURRENT,
            )),
        )
        .await
        .unwrap();
        assert!(matches!(
            observe_upload_pointer(
                &db,
                wrong_pointer_id,
                WriterEpoch::from_bytes([30; 16]).unwrap(),
                0,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));

        let wrong_work_id = TextUploadIntentId::from_bytes([31; 16]).unwrap();
        let wrong_work = prepared(
            wrong_work_id,
            work::TextUploadOwner::Build {
                operation_id: IndexOperationId::from_bytes([32; 16]).unwrap(),
                expected_operation_revision: IndexOperationRevision::initial(),
            },
        );
        put_prepared(&db, scope, &wrong_work).await;
        let wrong_work_pointer = pointer_for(scope, wrong_work.value());
        let declared_split = match wrong_work.value().attachment {
            work::TextUploadAttachment::ManifestSplit(split)
            | work::TextUploadAttachment::BuildArtifact { split, .. } => split,
        };
        db.put(
            scoped_intent_key(scope, &wrong_work_pointer, wrong_work_id),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextBuildArtifact(
                work::TextBuildArtifactValue {
                    index_id: wrong_work.value().index_id,
                    generation: wrong_work.value().generation,
                    partition: wrong_work.value().partition.clone(),
                    artifact_ordinal: 0,
                    split: declared_split,
                    source_intent_id: wrong_work_id,
                },
            )),
        )
        .await
        .unwrap();
        assert!(matches!(
            observe_upload_pointer(
                &db,
                wrong_work_id,
                WriterEpoch::from_bytes([33; 16]).unwrap(),
                0,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn claim_and_requeue_losing_races_never_mutate_a_newer_owner() {
        let db = raw_db("text-upload-queue-losing-races").await;
        let scope = DataScope::LegacyUnscoped;
        let owner = work::TextUploadOwner::Build {
            operation_id: IndexOperationId::from_bytes([34; 16]).unwrap(),
            expected_operation_revision: IndexOperationRevision::initial(),
        };
        let writer_epoch = WriterEpoch::from_bytes([35; 16]).unwrap();

        let missing_pointer_id = TextUploadIntentId::from_bytes([36; 16]).unwrap();
        let missing_pointer = prepared(missing_pointer_id, owner);
        put_prepared(&db, scope, &missing_pointer).await;
        let UploadPointerObservation::Eligible(missing_pointer_observation) =
            observe_upload_pointer(&db, missing_pointer_id, writer_epoch, 0)
                .await
                .unwrap()
        else {
            panic!("prepared intent is eligible");
        };
        delete_key(&db, global_upload_key(missing_pointer_id)).await;
        assert!(claim_upload(
            &db,
            &missing_pointer_observation,
            writer_epoch,
            ClaimSequence::new(1).unwrap(),
            0,
            ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .is_none());

        let missing_intent_id = TextUploadIntentId::from_bytes([37; 16]).unwrap();
        let missing_intent = prepared(missing_intent_id, owner);
        put_prepared(&db, scope, &missing_intent).await;
        let UploadPointerObservation::Eligible(missing_intent_observation) =
            observe_upload_pointer(&db, missing_intent_id, writer_epoch, 0)
                .await
                .unwrap()
        else {
            panic!("prepared intent is eligible");
        };
        let missing_intent_pointer = pointer_for(scope, missing_intent.value());
        delete_key(
            &db,
            scoped_intent_key(scope, &missing_intent_pointer, missing_intent_id),
        )
        .await;
        assert!(claim_upload(
            &db,
            &missing_intent_observation,
            writer_epoch,
            ClaimSequence::new(2).unwrap(),
            0,
            ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .is_none());
        assert!(db
            .get(global_upload_key(missing_intent_id))
            .await
            .unwrap()
            .is_none());

        let newer_intent_id = TextUploadIntentId::from_bytes([38; 16]).unwrap();
        let newer_intent = prepared(newer_intent_id, owner);
        put_prepared(&db, scope, &newer_intent).await;
        let UploadPointerObservation::Eligible(stale_observation) =
            observe_upload_pointer(&db, newer_intent_id, writer_epoch, 0)
                .await
                .unwrap()
        else {
            panic!("prepared intent is eligible");
        };
        let newer_record = newer_intent
            .value()
            .claim(OperationClaim {
                writer_epoch: WriterEpoch::from_bytes([39; 16]).unwrap(),
                sequence: ClaimSequence::new(1).unwrap(),
            })
            .unwrap();
        db.put(
            scoped_intent_key(
                scope,
                &pointer_for(scope, newer_intent.value()),
                newer_intent_id,
            ),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextUploadIntent(
                Box::new(newer_record),
            )),
        )
        .await
        .unwrap();
        assert!(claim_upload(
            &db,
            &stale_observation,
            writer_epoch,
            ClaimSequence::new(3).unwrap(),
            0,
            ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .is_none());

        let recoverable_id = TextUploadIntentId::from_bytes([40; 16]).unwrap();
        let recoverable = prepared(recoverable_id, owner);
        put_prepared(&db, scope, &recoverable).await;
        let UploadPointerObservation::Eligible(recoverable_observation) =
            observe_upload_pointer(&db, recoverable_id, writer_epoch, 0)
                .await
                .unwrap()
        else {
            panic!("prepared intent is eligible");
        };
        let claimed = claim_upload(
            &db,
            &recoverable_observation,
            writer_epoch,
            ClaimSequence::new(4).unwrap(),
            0,
            ClaimPermission::SameEpochRecovery(SameEpochRecoveryProof::after_join(writer_epoch)),
        )
        .await
        .unwrap()
        .expect("supervised authority may claim queued work");
        db.put(
            global_upload_key(recoverable_id),
            index_values::encode_metadata_value(&IndexV2MetadataValue::UploadQueuePointer(
                UploadQueuePointerValue {
                    scope: DataScope::Tenant(
                        crate::encoding::v1::keys::tenant::TenantId::from_u128(1),
                    ),
                    index_id: claimed.record.index_id,
                    generation: claimed.record.generation,
                    record_revision: claimed.record.revision,
                },
            )),
        )
        .await
        .unwrap();
        assert!(matches!(
            requeue_claimed_upload(&db, &claimed, 0).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        db.put(
            global_upload_key(recoverable_id),
            index_values::encode_metadata_value(&IndexV2MetadataValue::UploadQueuePointer(
                pointer_for(scope, &claimed.record),
            )),
        )
        .await
        .unwrap();
        delete_key(&db, global_upload_key(recoverable_id)).await;
        assert!(!requeue_claimed_upload(&db, &claimed, 0).await.unwrap());
        db.put(
            global_upload_key(recoverable_id),
            index_values::encode_metadata_value(&IndexV2MetadataValue::UploadQueuePointer(
                pointer_for(scope, &claimed.record),
            )),
        )
        .await
        .unwrap();
        delete_key(
            &db,
            scoped_intent_key(scope, &pointer_for(scope, &claimed.record), recoverable_id),
        )
        .await;
        assert!(!requeue_claimed_upload(&db, &claimed, 0).await.unwrap());

        let exhausted_id = TextUploadIntentId::from_bytes([41; 16]).unwrap();
        let exhausted = prepared(exhausted_id, owner);
        put_prepared(&db, scope, &exhausted).await;
        let exhausted_record = work::TextUploadIntentValue::try_new(
            exhausted_id,
            TextIntentRevision::new(u64::MAX).unwrap(),
            exhausted.value().index_id,
            exhausted.value().identity.clone(),
            exhausted.value().generation,
            exhausted.value().partition.clone(),
            exhausted.value().blob,
            exhausted.value().publication_permit_id,
            exhausted.value().owner,
            exhausted.value().attachment,
            exhausted.value().phase.clone(),
            exhausted.value().attempt,
            exhausted.value().work_state.clone(),
        )
        .unwrap();
        let exhausted_pointer = pointer_for(scope, &exhausted_record);
        db.put(
            scoped_intent_key(scope, &exhausted_pointer, exhausted_id),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextUploadIntent(
                Box::new(exhausted_record),
            )),
        )
        .await
        .unwrap();
        db.put(
            global_upload_key(exhausted_id),
            index_values::encode_metadata_value(&IndexV2MetadataValue::UploadQueuePointer(
                exhausted_pointer,
            )),
        )
        .await
        .unwrap();
        let UploadPointerObservation::Eligible(exhausted_observation) =
            observe_upload_pointer(&db, exhausted_id, writer_epoch, 0)
                .await
                .unwrap()
        else {
            panic!("maximum revision remains observable");
        };
        assert!(matches!(
            claim_upload(
                &db,
                &exhausted_observation,
                writer_epoch,
                ClaimSequence::new(5).unwrap(),
                0,
                ClaimPermission::Normal,
            )
            .await,
            Err(HelixDbError::InvariantViolation(_))
        ));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn claimed_driver_checkpoint_commits_uploaded_phase_exactly_once() {
        let db = raw_db("text-upload-queue-driver-uploaded").await;
        let intent_id = TextUploadIntentId::from_bytes([43; 16]).unwrap();
        let claimed =
            put_and_claim(&db, intent_id, WriterEpoch::from_bytes([44; 16]).unwrap()).await;
        assert_eq!(
            execute_claimed_upload_step(
                &db,
                &claimed,
                &StaticStepDriver(TextUploadStepResult::PublicationSucceeded),
                100,
            )
            .await
            .unwrap(),
            TextUploadStepResult::PublicationSucceeded
        );
        let uploaded = crate::index_v2::repository::load_upload_from_pointer(&db, intent_id)
            .await
            .unwrap()
            .expect("uploaded intent remains queued for attachment");
        assert_eq!(uploaded.revision.get(), 3);
        assert!(matches!(uploaded.phase, work::TextUploadPhase::Uploaded));
        assert!(matches!(
            uploaded.work_state,
            work::TextUploadWorkState::Queued {
                not_before_unix_millis: None
            }
        ));
        assert!(matches!(
            execute_claimed_upload_step(
                &db,
                &claimed,
                &StaticStepDriver(TextUploadStepResult::PublicationSucceeded),
                100,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn transient_and_error_driver_results_release_exact_claim_with_backoff() {
        let db = raw_db("text-upload-queue-driver-transient").await;
        let writer_epoch = WriterEpoch::from_bytes([45; 16]).unwrap();
        let transient_id = TextUploadIntentId::from_bytes([46; 16]).unwrap();
        let transient = put_and_claim(&db, transient_id, writer_epoch).await;
        assert_eq!(
            execute_claimed_upload_step(
                &db,
                &transient,
                &StaticStepDriver(TextUploadStepResult::TransientFailure),
                100,
            )
            .await
            .unwrap(),
            TextUploadStepResult::TransientFailure
        );
        let transient = crate::index_v2::repository::load_upload_from_pointer(&db, transient_id)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(transient.phase, work::TextUploadPhase::Prepared));
        assert!(matches!(
            transient.work_state,
            work::TextUploadWorkState::Queued {
                not_before_unix_millis: Some(2_100)
            }
        ));

        let error_id = TextUploadIntentId::from_bytes([47; 16]).unwrap();
        let errored = put_and_claim(&db, error_id, writer_epoch).await;
        assert_eq!(
            execute_claimed_upload_step(&db, &errored, &FailingStepDriver, 200)
                .await
                .unwrap(),
            TextUploadStepResult::TransientFailure
        );
        let errored = crate::index_v2::repository::load_upload_from_pointer(&db, error_id)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(errored.phase, work::TextUploadPhase::Prepared));
        assert!(matches!(
            errored.work_state,
            work::TextUploadWorkState::Queued {
                not_before_unix_millis: Some(2_200)
            }
        ));
        db.close().await.unwrap();
    }

    #[test]
    fn upload_backoff_is_bounded_for_every_attempt_domain_extreme() {
        assert_eq!(backoff_millis(0), 1_000);
        assert_eq!(backoff_millis(1), 2_000);
        assert_eq!(backoff_millis(u32::MAX), MAX_UPLOAD_BACKOFF_MILLIS);
        assert_eq!(observed_delay(10, 5), 0);
        assert_eq!(observed_delay(0, u64::MAX), MAX_UPLOAD_BACKOFF_MILLIS);
    }
}
