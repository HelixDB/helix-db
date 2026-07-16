//! Durable text-blob garbage-collection batches and ownership barriers.
//!
//! Generation cleanup never deletes directly from a mutable candidate scan.
//! It first proves that no upload intent is assigned to an independent reclaim
//! run, selects one bounded operation-owned candidate prefix, and atomically
//! persists an immutable global run root plus one member per exact blob. The
//! owning operation stores the same run ID and strict candidate cursor in its
//! commit, so it can never point at an absent batch.
//!
//! This module owns batch creation, exact root validation, the independently
//! runnable global root lane, coordinator delete-fence acquisition, two
//! complete stable-snapshot reachability passes, and terminal disposition.
//! A member remains the retry anchor through permit-owner cleanup and is moved
//! to `CleanupCommitted` before the coordinator fence can reopen; recovery can
//! therefore never repeat object deletion after durable database cleanup.

use std::collections::HashMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::ops::Bound;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use slatedb::{Db, DbSnapshot, DbTransaction, IsolationLevel};
use tokio::sync::Mutex;

use crate::config::SearchIndexBatchLimits;
use crate::encoding::v1::keys::index_v2 as index_keys;
use crate::encoding::v1::keys::tenant::DataScope;
use crate::encoding::v1::keys::{DataKeyKind, Key};
use crate::encoding::v1::values::index_v2 as index_values;
use crate::error::{HelixDbError, Result};
use crate::index_v2::blob_publication;
use crate::index_v2::work;
use crate::index_v2::{
    BlobGcRunId, BlobGcRunRevision, GcProgress, IndexCursor, IndexOperationBlocker,
    IndexOperationRecord, IndexStateV2, OperationCounters, PrefixScanProgress, WriterEpoch,
};

const RUN_ID_COLLISION_ATTEMPTS: usize = 16;

/// One bounded result from the upload-reclaim ownership barrier.
#[derive(Debug)]
pub(super) enum ReclaimBarrierTransition {
    /// More exact intent rows remain after the returned strict cursor.
    Progressed(PrefixScanProgress),
    /// Every intent was observed without a foreign assignment.
    Complete(OperationCounters),
    /// An exact live upload-reclaim root must finish first.
    Waiting,
    /// One indivisible row cannot fit the configured transaction budget.
    Blocked(IndexOperationBlocker),
}

/// One closed generation-batch transition consumed by text cleanup.
#[derive(Debug)]
pub(super) enum GenerationBatchTransition {
    /// Barrier progress or one atomically staged immutable run.
    Progressed(GcProgress),
    /// An already-persisted root still owns this operation.
    Waiting,
    /// The exact immutable run has closed and quiesced every member fence.
    FencesClosed(GcProgress),
    /// No operation-owned generation candidate remains after the strict cursor.
    Exhausted(OperationCounters),
    /// The first indivisible member/root pair exceeds a configured limit.
    Blocked(IndexOperationBlocker),
}

/// Atomic owner-operation result for one empty terminal generation root.
#[derive(Debug)]
pub(super) enum GenerationTerminalTransition {
    /// A bounded global-root scan checkpoint remains in `DeleteBlobs`.
    Progressed(GcProgress),
    /// The terminal root was removed and another generation batch remains.
    NextBatch(GcProgress),
    /// The terminal root was removed and generation physical rows may retire.
    Complete(PrefixScanProgress),
    /// An intent-owned candidate or upload-reclaim root must finish first.
    Waiting,
    /// The indivisible terminal transaction exceeds configured limits.
    Blocked(IndexOperationBlocker),
}

/// Atomic singleton-root decision under the canonical lifecycle record.
#[derive(Debug)]
pub(super) enum UploadReclaimRootTransition {
    /// The canonical generation still accepts independent upload reclamation.
    Assigned(BlobGcRunId),
    /// Abort/drop owns this generation and will assign the intent to its root.
    GenerationCleanupOwnsAssignment,
}

/// Bounded page of independently runnable global GC roots.
pub(crate) struct BlobGcRootPage {
    pub(crate) run_ids: Vec<BlobGcRunId>,
    pub(crate) resume_after: Option<BlobGcRunId>,
    pub(crate) prefix_exhausted: bool,
}

/// Durable result of one bounded root-worker turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BlobGcRootStep {
    /// A root checkpoint committed and should receive another fair turn.
    Progressed,
    /// The root persisted a retry delay.
    Delayed { delay_millis: u64 },
    /// The root disappeared or reached a phase not handled by this slice.
    Idle,
}

/// Closed result of one coordinator fence observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FenceObservation {
    Quiescent,
    Retry,
}

/// One exact reachability pass and its complete persisted cursor contract.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ReachabilityPass {
    First {
        writer_epoch: WriterEpoch,
        attempt: work::GcScanAttempt,
        reference_cursor: Option<IndexCursor>,
    },
    Second {
        completed_first_attempt: work::GcScanAttempt,
        writer_epoch: WriterEpoch,
        attempt: work::GcScanAttempt,
        reference_cursor: Option<IndexCursor>,
    },
}

impl ReachabilityPass {
    fn from_phase(phase: &work::BlobGcPhase) -> Option<Self> {
        match phase {
            work::BlobGcPhase::FirstPass {
                writer_epoch,
                first_attempt,
                reference_cursor,
            } => Some(Self::First {
                writer_epoch: *writer_epoch,
                attempt: *first_attempt,
                reference_cursor: reference_cursor.clone(),
            }),
            work::BlobGcPhase::SecondPass {
                completed_first_attempt,
                writer_epoch,
                second_attempt,
                reference_cursor,
            } => Some(Self::Second {
                completed_first_attempt: *completed_first_attempt,
                writer_epoch: *writer_epoch,
                attempt: *second_attempt,
                reference_cursor: reference_cursor.clone(),
            }),
            work::BlobGcPhase::AwaitDeleteFences { .. }
            | work::BlobGcPhase::FencesClosed
            | work::BlobGcPhase::Delete { .. } => None,
        }
    }

    const fn writer_epoch(&self) -> WriterEpoch {
        match self {
            Self::First { writer_epoch, .. } | Self::Second { writer_epoch, .. } => *writer_epoch,
        }
    }

    const fn attempt(&self) -> work::GcScanAttempt {
        match self {
            Self::First { attempt, .. } | Self::Second { attempt, .. } => *attempt,
        }
    }

    const fn key_pass(&self) -> index_keys::BlobGcPass {
        match self {
            Self::First { .. } => index_keys::BlobGcPass::First,
            Self::Second { .. } => index_keys::BlobGcPass::Second,
        }
    }

    fn reference_cursor(&self) -> Option<&IndexCursor> {
        match self {
            Self::First {
                reference_cursor, ..
            }
            | Self::Second {
                reference_cursor, ..
            } => reference_cursor.as_ref(),
        }
    }

    fn phase_with_cursor(&self, reference_cursor: Option<IndexCursor>) -> work::BlobGcPhase {
        match self {
            Self::First {
                writer_epoch,
                attempt,
                ..
            } => work::BlobGcPhase::FirstPass {
                writer_epoch: *writer_epoch,
                first_attempt: *attempt,
                reference_cursor,
            },
            Self::Second {
                completed_first_attempt,
                writer_epoch,
                attempt,
                ..
            } => work::BlobGcPhase::SecondPass {
                completed_first_attempt: *completed_first_attempt,
                writer_epoch: *writer_epoch,
                second_attempt: *attempt,
                reference_cursor,
            },
        }
    }

    fn restarted_phase(&self, writer_epoch: WriterEpoch) -> Result<work::BlobGcPhase> {
        let next_attempt = self
            .attempt()
            .checked_next()
            .map_err(|error| invariant(error.to_string()))?;
        Ok(match self {
            Self::First { .. } => work::BlobGcPhase::FirstPass {
                writer_epoch,
                first_attempt: next_attempt,
                reference_cursor: None,
            },
            Self::Second {
                completed_first_attempt,
                ..
            } => work::BlobGcPhase::SecondPass {
                completed_first_attempt: *completed_first_attempt,
                writer_epoch,
                second_attempt: next_attempt,
                reference_cursor: None,
            },
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReachabilityPassIdentity {
    writer_epoch: WriterEpoch,
    pass: index_keys::BlobGcPass,
    attempt: work::GcScanAttempt,
}

impl ReachabilityPassIdentity {
    const fn new(pass: &ReachabilityPass) -> Self {
        Self {
            writer_epoch: pass.writer_epoch(),
            pass: pass.key_pass(),
            attempt: pass.attempt(),
        }
    }
}

/// In-memory authority whose loss forces a new pass attempt after takeover.
struct ReachabilityPassRuntime {
    identity: ReachabilityPassIdentity,
    snapshot: Arc<DbSnapshot>,
    _deletion_permit: crate::search::text::BlobDeletionPermit,
}

/// Runtime service that can recover a root without an operation pointer.
#[async_trait]
pub(crate) trait BlobGcDriver: Send + Sync {
    /// Executes at most one bounded root checkpoint.
    async fn execute_root_step(
        &self,
        db: &Db,
        run_id: BlobGcRunId,
        writer_epoch: WriterEpoch,
        now_unix_millis: u64,
    ) -> Result<BlobGcRootStep>;
}

/// Coordinator-backed text blob-GC runtime.
///
/// Each reachability pass retains the local deletion gate with its exact SlateDB
/// snapshot until that pass completes. This prevents pages from different
/// publication windows from being combined into one persisted attempt.
pub(crate) struct TextBlobGcDriver {
    coordinator: Arc<dyn blob_publication::BlobPublicationCoordinator>,
    gc_gate: crate::search::text::BlobGcGate,
    reachability: Mutex<HashMap<BlobGcRunId, Arc<ReachabilityPassRuntime>>>,
}

impl TextBlobGcDriver {
    /// Installs the exact coordinator and same-writer publication gate.
    pub(crate) fn new(
        coordinator: Arc<dyn blob_publication::BlobPublicationCoordinator>,
        gc_gate: crate::search::text::BlobGcGate,
    ) -> Self {
        Self {
            coordinator,
            gc_gate,
            reachability: Mutex::new(HashMap::new()),
        }
    }
}

impl core::fmt::Debug for TextBlobGcDriver {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("TextBlobGcDriver")
            .field("gc_gate", &self.gc_gate)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl BlobGcDriver for TextBlobGcDriver {
    async fn execute_root_step(
        &self,
        db: &Db,
        run_id: BlobGcRunId,
        writer_epoch: WriterEpoch,
        now_unix_millis: u64,
    ) -> Result<BlobGcRootStep> {
        let Some(root) = load_root(db, run_id).await? else {
            return Ok(BlobGcRootStep::Idle);
        };
        if let Some(not_before) = root.not_before_unix_millis
            && not_before > now_unix_millis
        {
            return Ok(BlobGcRootStep::Delayed {
                delay_millis: not_before - now_unix_millis,
            });
        }
        if matches!(root.phase, work::BlobGcPhase::Delete { .. }) {
            self.reachability.lock().await.remove(&run_id);
            return execute_delete_step(self.coordinator.as_ref(), db, &root, now_unix_millis)
                .await;
        }
        let work::BlobGcPhase::AwaitDeleteFences { member_cursor } = root.phase.clone() else {
            let Some(pass) = ReachabilityPass::from_phase(&root.phase) else {
                self.reachability.lock().await.remove(&run_id);
                return Ok(BlobGcRootStep::Idle);
            };
            if pass.writer_epoch() != writer_epoch {
                self.reachability.lock().await.remove(&run_id);
                replace_root(db, &root, pass.restarted_phase(writer_epoch)?, 0, None).await?;
                return Ok(BlobGcRootStep::Progressed);
            }
            let identity = ReachabilityPassIdentity::new(&pass);
            let retained_runtime = {
                let mut runtimes = self.reachability.lock().await;
                if let Some(runtime) = runtimes.get(&run_id)
                    && runtime.identity == identity
                {
                    Some(Arc::clone(runtime))
                } else {
                    runtimes.remove(&run_id);
                    if pass.reference_cursor().is_some() {
                        drop(runtimes);
                        replace_root(db, &root, pass.restarted_phase(writer_epoch)?, 0, None)
                            .await?;
                        return Ok(BlobGcRootStep::Progressed);
                    }
                    if !runtimes.is_empty() {
                        drop(runtimes);
                        let delay_millis = schedule_retry(db, &root, now_unix_millis).await?;
                        return Ok(BlobGcRootStep::Delayed { delay_millis });
                    }
                    None
                }
            };
            let runtime = if let Some(runtime) = retained_runtime {
                runtime
            } else {
                let deletion_permit = self.gc_gate.acquire_deletion().await;
                let Some(current_root) = load_root(db, run_id).await? else {
                    return Ok(BlobGcRootStep::Idle);
                };
                if current_root != root {
                    return Ok(BlobGcRootStep::Progressed);
                }
                if !revalidate_all_fences(self.coordinator.as_ref(), db, &root).await? {
                    drop(deletion_permit);
                    let delay_millis = schedule_retry(db, &root, now_unix_millis).await?;
                    return Ok(BlobGcRootStep::Delayed { delay_millis });
                }
                let runtime = Arc::new(ReachabilityPassRuntime {
                    identity,
                    snapshot: db.snapshot().await?,
                    _deletion_permit: deletion_permit,
                });
                let mut runtimes = self.reachability.lock().await;
                if !runtimes.is_empty() {
                    return Err(corruption(
                        "blob-GC reachability gate and runtime ownership disagree",
                    ));
                }
                runtimes.insert(run_id, Arc::clone(&runtime));
                runtime
            };
            let completed = execute_reachability_pass_step(db, &root, &pass, &runtime).await?;
            if completed {
                self.reachability.lock().await.remove(&run_id);
            }
            return Ok(BlobGcRootStep::Progressed);
        };
        let Some((member_key, member)) = next_member(db, run_id, member_cursor.as_ref()).await?
        else {
            if confirm_all_fences(self.coordinator.as_ref(), db, &root, member_cursor.as_ref())
                .await?
                == FenceObservation::Retry
            {
                let delay_millis = schedule_retry(db, &root, now_unix_millis).await?;
                return Ok(BlobGcRootStep::Delayed { delay_millis });
            }
            replace_root(db, &root, work::BlobGcPhase::FencesClosed, 0, None).await?;
            return Ok(BlobGcRootStep::Progressed);
        };
        if observe_delete_fence(self.coordinator.as_ref(), member.blob, run_id).await?
            == FenceObservation::Retry
        {
            let delay_millis = schedule_retry(db, &root, now_unix_millis).await?;
            return Ok(BlobGcRootStep::Delayed { delay_millis });
        }
        replace_root(
            db,
            &root,
            work::BlobGcPhase::AwaitDeleteFences {
                member_cursor: Some(IndexCursor::try_new(member_key).map_err(operation_error)?),
            },
            0,
            None,
        )
        .await?;
        Ok(BlobGcRootStep::Progressed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RunOwnerIdentity {
    scope: DataScope,
    index_id: crate::index_v2::IndexId,
    generation: crate::index_v2::IndexGenerationId,
    upload_intent_id: Option<crate::index_v2::TextUploadIntentId>,
}

impl RunOwnerIdentity {
    const fn from_root(root: &work::BlobGcRunRootValue) -> Self {
        match root.owner {
            work::BlobGcRunOwner::GenerationCleanup {
                scope,
                index_id,
                generation,
                ..
            } => Self {
                scope,
                index_id,
                generation,
                upload_intent_id: None,
            },
            work::BlobGcRunOwner::UploadReclaim {
                scope,
                intent_id,
                index_id,
                generation,
            } => Self {
                scope,
                index_id,
                generation,
                upload_intent_id: Some(intent_id),
            },
        }
    }
}

struct DispositionIntentRow {
    key: Bytes,
    value: Bytes,
    intent: work::TextUploadIntentValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DispositionIntentAction {
    Advance,
    Clean,
    Retry,
}

/// Runs one delete-phase checkpoint while the root remains the durable retry anchor.
async fn execute_delete_step(
    coordinator: &dyn blob_publication::BlobPublicationCoordinator,
    db: &Db,
    root: &work::BlobGcRunRootValue,
    now_unix_millis: u64,
) -> Result<BlobGcRootStep> {
    let work::BlobGcPhase::Delete {
        completed_first_attempt,
        completed_second_attempt,
        member_cursor,
        stale_mark_cleanup,
    } = &root.phase
    else {
        return Err(corruption("blob-GC disposition requires a Delete root"));
    };
    let Some((member_key, member)) = next_member(db, root.run_id, member_cursor.as_ref()).await?
    else {
        return execute_stale_mark_cleanup_step(
            db,
            root,
            *completed_first_attempt,
            *completed_second_attempt,
            stale_mark_cleanup,
        )
        .await;
    };
    match &member.state {
        work::BlobGcMemberState::CleanupCommitted(_) => {
            let fence_key = blob_publication::BlobDeleteFenceKey::new(member.blob, root.run_id);
            match coordinator
                .finish_delete(
                    fence_key,
                    blob_publication::CleanupCommittedAuthority::new(fence_key),
                )
                .await
            {
                Ok(()) => {}
                Err(error) if coordinator_error_is_retryable(&error) => {
                    let delay_millis = schedule_retry(db, root, now_unix_millis).await?;
                    return Ok(BlobGcRootStep::Delayed { delay_millis });
                }
                Err(error) => return Err(error.into()),
            }
            remove_cleanup_committed_member(db, root, &member_key, &member).await?;
            Ok(BlobGcRootStep::Progressed)
        }
        work::BlobGcMemberState::PendingDisposition { owner_cursor } => {
            let disposition = load_member_disposition(
                db,
                root,
                *completed_first_attempt,
                *completed_second_attempt,
                &member,
            )
            .await?;
            let Some(fence) =
                acquire_disposition_fence(coordinator, member.blob, root.run_id).await?
            else {
                let delay_millis = schedule_retry(db, root, now_unix_millis).await?;
                return Ok(BlobGcRootStep::Delayed { delay_millis });
            };
            if disposition == work::BlobGcDisposition::DeletedOrAbsent {
                match coordinator.delete(&fence).await {
                    Ok(blob_publication::BlobDeleteOutcome::DeletedOrAbsent) => {}
                    Err(error) if coordinator_error_is_retryable(&error) => {
                        let delay_millis = schedule_retry(db, root, now_unix_millis).await?;
                        return Ok(BlobGcRootStep::Delayed { delay_millis });
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            let owner = RunOwnerIdentity::from_root(root);
            let Some(intent_row) =
                next_disposition_intent(db, owner, owner_cursor.as_ref()).await?
            else {
                commit_member_cleanup(
                    db,
                    root,
                    &member_key,
                    &member,
                    *completed_first_attempt,
                    *completed_second_attempt,
                    disposition,
                )
                .await?;
                return Ok(BlobGcRootStep::Progressed);
            };
            let intent_action = disposition_intent_action(root, &member, &intent_row.intent)?;
            if intent_action == DispositionIntentAction::Retry {
                let delay_millis = schedule_retry(db, root, now_unix_millis).await?;
                return Ok(BlobGcRootStep::Delayed { delay_millis });
            }
            if intent_action == DispositionIntentAction::Clean {
                let permit = blob_publication::BlobPublicationPermit::from_id(
                    intent_row.intent.publication_permit_id,
                );
                match coordinator
                    .release(
                        &permit,
                        blob_publication::BlobPermitReleaseAuthority::fenced_disposition(
                            permit.id(),
                        ),
                    )
                    .await
                {
                    Ok(()) => {}
                    Err(error) if coordinator_error_is_retryable(&error) => {
                        let delay_millis = schedule_retry(db, root, now_unix_millis).await?;
                        return Ok(BlobGcRootStep::Delayed { delay_millis });
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            checkpoint_disposition_intent(
                db,
                root,
                &member_key,
                &member,
                &intent_row,
                intent_action == DispositionIntentAction::Clean,
            )
            .await?;
            Ok(BlobGcRootStep::Progressed)
        }
    }
}

async fn acquire_disposition_fence(
    coordinator: &dyn blob_publication::BlobPublicationCoordinator,
    blob: work::BlobRef,
    run_id: BlobGcRunId,
) -> Result<Option<blob_publication::BlobDeleteFence>> {
    let key = blob_publication::BlobDeleteFenceKey::new(blob, run_id);
    let fence = match coordinator.begin_delete(key).await {
        Ok(blob_publication::BeginBlobDelete::Acquired(fence))
        | Ok(blob_publication::BeginBlobDelete::AlreadyHeldSameRun(fence)) => fence,
        Ok(blob_publication::BeginBlobDelete::BusyOtherRun) => return Ok(None),
        Err(error) if coordinator_error_is_retryable(&error) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    match coordinator.check_quiescent(&fence).await {
        Ok(true) => Ok(Some(fence)),
        Ok(false) => Ok(None),
        Err(error) if coordinator_error_is_retryable(&error) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

async fn next_disposition_intent(
    db: &Db,
    owner: RunOwnerIdentity,
    cursor: Option<&IndexCursor>,
) -> Result<Option<DispositionIntentRow>> {
    let prefix = Key::data_prefix(
        owner.scope,
        index_keys::IndexV2Key::generation_prefix(
            index_keys::IndexV2RecordKind::TextUploadIntent,
            owner.index_id,
            owner.generation,
        ),
    );
    let start = match cursor {
        Some(cursor) => {
            let Some(suffix) = cursor.as_bytes().strip_prefix(prefix.as_ref()) else {
                return Err(corruption(
                    "blob-GC member owner cursor escaped its upload-intent generation",
                ));
            };
            Bound::Excluded(Bytes::copy_from_slice(suffix))
        }
        None => Bound::Unbounded,
    };
    let mut rows = db.scan_prefix(&prefix, (start, Bound::Unbounded)).await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let intent = decode_disposition_intent(owner, &row.key, &row.value)?;
    Ok(Some(DispositionIntentRow {
        key: row.key,
        value: row.value,
        intent,
    }))
}

fn decode_disposition_intent(
    owner: RunOwnerIdentity,
    key: &[u8],
    value: &[u8],
) -> Result<work::TextUploadIntentValue> {
    let Key::Data {
        scope,
        kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::TextUploadIntent(key_identity)),
    } = Key::parse_from_slice(owner.scope, key)?
    else {
        return Err(corruption(
            "blob-GC disposition intent lane yielded another key kind",
        ));
    };
    let index_values::IndexV2WorkValue::TextUploadIntent(intent) =
        index_values::decode_work_value(value)?
    else {
        return Err(corruption(
            "blob-GC disposition intent key contains another value kind",
        ));
    };
    let intent = *intent;
    if scope != owner.scope
        || key_identity.index_id != owner.index_id
        || key_identity.generation != owner.generation
        || key_identity.intent_id != intent.intent_id
        || intent.index_id != owner.index_id
        || intent.generation != owner.generation
    {
        return Err(corruption(
            "blob-GC disposition intent key and value ownership disagree",
        ));
    }
    Ok(intent)
}

fn disposition_intent_action(
    root: &work::BlobGcRunRootValue,
    member: &work::BlobGcCandidateMemberValue,
    intent: &work::TextUploadIntentValue,
) -> Result<DispositionIntentAction> {
    let exact_owner = match root.owner {
        work::BlobGcRunOwner::GenerationCleanup { .. } => intent.blob == member.blob,
        work::BlobGcRunOwner::UploadReclaim { intent_id, .. } => intent.intent_id == intent_id,
    };
    if !exact_owner {
        return Ok(DispositionIntentAction::Advance);
    }
    if intent.blob != member.blob
        || !matches!(
            intent.phase,
            work::TextUploadPhase::Reclaimable(work::ReclaimAssignment::Assigned(run_id))
                if run_id == root.run_id
        )
    {
        return Err(corruption(
            "blob-GC disposition intent is not assigned to its retained member run",
        ));
    }
    match intent.work_state {
        work::TextUploadWorkState::Queued { .. } => Ok(DispositionIntentAction::Clean),
        work::TextUploadWorkState::Claimed(_) => Ok(DispositionIntentAction::Retry),
        work::TextUploadWorkState::Blocked(_) => Err(corruption(
            "blob-GC assigned disposition intent is durably blocked",
        )),
    }
}

async fn checkpoint_disposition_intent(
    db: &Db,
    root: &work::BlobGcRunRootValue,
    member_key: &[u8],
    member: &work::BlobGcCandidateMemberValue,
    intent_row: &DispositionIntentRow,
    clean_intent: bool,
) -> Result<()> {
    let root_key = index_keys::GlobalIndexV2Key::BlobGcRunRoot(root.run_id).to_bytes();
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    require_exact_root(&transaction, &root_key, root).await?;
    require_exact_member(&transaction, root.run_id, member_key, member).await?;
    if transaction.get(&intent_row.key).await?.as_deref() != Some(intent_row.value.as_ref()) {
        return Err(corruption(
            "blob-GC disposition intent changed before its owner checkpoint",
        ));
    }
    let owner = RunOwnerIdentity::from_root(root);
    if decode_disposition_intent(owner, &intent_row.key, &intent_row.value)? != intent_row.intent {
        return Err(corruption(
            "blob-GC disposition intent bytes changed semantic identity",
        ));
    }
    if clean_intent {
        if disposition_intent_action(root, member, &intent_row.intent)?
            != DispositionIntentAction::Clean
        {
            return Err(corruption(
                "blob-GC disposition lost its exact assigned intent",
            ));
        }
        let rows = super::upload::upload_anchor_rows(owner.scope, &intent_row.intent)?;
        if transaction.get(&rows.pointer_key).await?.as_deref() != Some(rows.pointer_value.as_ref())
        {
            return Err(corruption(
                "blob-GC disposition intent is missing its exact upload pointer",
            ));
        }
        if transaction.get(&rows.reachability_key).await?.is_some() {
            return Err(corruption(
                "blob-GC reclaimable intent unexpectedly retained global reachability",
            ));
        }
        let (candidate_key, candidate_value) =
            disposition_intent_candidate(owner.scope, &intent_row.intent);
        if transaction.get(&candidate_key).await?.as_deref() != Some(candidate_value.as_ref()) {
            return Err(corruption(
                "blob-GC disposition intent is missing its exact scoped candidate",
            ));
        }
        transaction.delete(rows.intent_key)?;
        transaction.delete(rows.pointer_key)?;
        transaction.delete(candidate_key)?;
    }
    let next_member = work::BlobGcCandidateMemberValue {
        run_id: member.run_id,
        blob: member.blob,
        state: work::BlobGcMemberState::PendingDisposition {
            owner_cursor: Some(
                IndexCursor::try_new(intent_row.key.clone()).map_err(operation_error)?,
            ),
        },
    };
    let next_root = next_root_value(root, root.phase.clone(), 0, None)?;
    transaction.put(
        Bytes::copy_from_slice(member_key),
        encode_member(&next_member),
    )?;
    transaction.put(root_key, encode_root(&next_root))?;
    transaction.commit().await?;
    Ok(())
}

fn disposition_intent_candidate(
    scope: DataScope,
    intent: &work::TextUploadIntentValue,
) -> (Bytes, Bytes) {
    let key = scoped_key(
        scope,
        index_keys::IndexV2Key::BlobGcCandidate(index_keys::BlobGcCandidateKey {
            index_id: intent.index_id,
            generation: intent.generation,
            owner: index_keys::BlobGcCandidateKeyOwner::UploadIntent(intent.intent_id),
            blob_hash: index_keys::BlobHash::new(*intent.blob.hash()),
        }),
    );
    let value = index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcCandidate(
        work::BlobGcCandidateValue {
            owner: work::BlobGcCandidateOwner::UploadIntent(intent.intent_id),
            index_id: intent.index_id,
            generation: intent.generation,
            blob: intent.blob,
        },
    ));
    (key, value)
}

async fn commit_member_cleanup(
    db: &Db,
    root: &work::BlobGcRunRootValue,
    member_key: &[u8],
    member: &work::BlobGcCandidateMemberValue,
    completed_first_attempt: work::GcScanAttempt,
    completed_second_attempt: work::GcScanAttempt,
    disposition: work::BlobGcDisposition,
) -> Result<()> {
    let work::BlobGcMemberState::PendingDisposition { owner_cursor } = &member.state else {
        return Err(corruption(
            "blob-GC cleanup commit requires a pending member",
        ));
    };
    let owner = RunOwnerIdentity::from_root(root);
    let prefix = Key::data_prefix(
        owner.scope,
        index_keys::IndexV2Key::generation_prefix(
            index_keys::IndexV2RecordKind::TextUploadIntent,
            owner.index_id,
            owner.generation,
        ),
    );
    let start = match owner_cursor {
        Some(cursor) => {
            let Some(suffix) = cursor.as_bytes().strip_prefix(prefix.as_ref()) else {
                return Err(corruption(
                    "blob-GC member owner cursor escaped its upload-intent generation",
                ));
            };
            Bound::Excluded(Bytes::copy_from_slice(suffix))
        }
        None => Bound::Unbounded,
    };
    let root_key = index_keys::GlobalIndexV2Key::BlobGcRunRoot(root.run_id).to_bytes();
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    require_exact_root(&transaction, &root_key, root).await?;
    require_exact_member(&transaction, root.run_id, member_key, member).await?;
    let mut remaining_intents = transaction
        .scan_prefix(&prefix, (start, Bound::Unbounded))
        .await?;
    if remaining_intents.next().await?.is_some() {
        return Err(corruption(
            "blob-GC member cleanup skipped a remaining upload intent",
        ));
    }
    let exact_disposition = require_member_disposition(
        &transaction,
        root,
        completed_first_attempt,
        completed_second_attempt,
        member,
    )
    .await?;
    if exact_disposition != disposition {
        return Err(corruption(
            "blob-GC member disposition changed before cleanup commit",
        ));
    }
    match root.owner {
        work::BlobGcRunOwner::GenerationCleanup { operation_id, .. } => {
            let candidate_key = scoped_key(
                owner.scope,
                index_keys::IndexV2Key::BlobGcCandidate(index_keys::BlobGcCandidateKey {
                    index_id: owner.index_id,
                    generation: owner.generation,
                    owner: index_keys::BlobGcCandidateKeyOwner::GenerationCleanup,
                    blob_hash: index_keys::BlobHash::new(*member.blob.hash()),
                }),
            );
            let candidate_value = index_values::encode_work_value(
                &index_values::IndexV2WorkValue::BlobGcCandidate(work::BlobGcCandidateValue {
                    owner: work::BlobGcCandidateOwner::GenerationCleanup(operation_id),
                    index_id: owner.index_id,
                    generation: owner.generation,
                    blob: member.blob,
                }),
            );
            if transaction.get(&candidate_key).await?.as_deref() != Some(candidate_value.as_ref()) {
                return Err(corruption(
                    "blob-GC generation member is missing its exact scoped candidate",
                ));
            }
            transaction.delete(candidate_key)?;
        }
        work::BlobGcRunOwner::UploadReclaim { intent_id, .. } => {
            let candidate_key = scoped_key(
                owner.scope,
                index_keys::IndexV2Key::BlobGcCandidate(index_keys::BlobGcCandidateKey {
                    index_id: owner.index_id,
                    generation: owner.generation,
                    owner: index_keys::BlobGcCandidateKeyOwner::UploadIntent(intent_id),
                    blob_hash: index_keys::BlobHash::new(*member.blob.hash()),
                }),
            );
            if transaction.get(candidate_key).await?.is_some() {
                return Err(corruption(
                    "blob-GC upload member retained its candidate after permit cleanup",
                ));
            }
        }
    }
    let next_member = work::BlobGcCandidateMemberValue {
        run_id: member.run_id,
        blob: member.blob,
        state: work::BlobGcMemberState::CleanupCommitted(disposition),
    };
    let next_root = next_root_value(root, root.phase.clone(), 0, None)?;
    transaction.put(
        Bytes::copy_from_slice(member_key),
        encode_member(&next_member),
    )?;
    transaction.put(root_key, encode_root(&next_root))?;
    transaction.commit().await?;
    Ok(())
}

async fn remove_cleanup_committed_member(
    db: &Db,
    root: &work::BlobGcRunRootValue,
    member_key: &[u8],
    member: &work::BlobGcCandidateMemberValue,
) -> Result<()> {
    if !matches!(member.state, work::BlobGcMemberState::CleanupCommitted(_)) {
        return Err(corruption(
            "blob-GC member removal requires committed cleanup",
        ));
    }
    let work::BlobGcPhase::Delete {
        completed_first_attempt,
        completed_second_attempt,
        stale_mark_cleanup,
        ..
    } = &root.phase
    else {
        return Err(corruption("blob-GC member removal requires Delete"));
    };
    let root_key = index_keys::GlobalIndexV2Key::BlobGcRunRoot(root.run_id).to_bytes();
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    require_exact_root(&transaction, &root_key, root).await?;
    require_exact_member(&transaction, root.run_id, member_key, member).await?;
    let disposition = require_member_disposition(
        &transaction,
        root,
        *completed_first_attempt,
        *completed_second_attempt,
        member,
    )
    .await?;
    let work::BlobGcMemberState::CleanupCommitted(committed) = member.state else {
        return Err(corruption(
            "blob-GC member removal lost committed disposition",
        ));
    };
    if committed != disposition {
        return Err(corruption(
            "blob-GC committed disposition disagrees with its completed marks",
        ));
    }
    let next_phase = work::BlobGcPhase::Delete {
        completed_first_attempt: *completed_first_attempt,
        completed_second_attempt: *completed_second_attempt,
        member_cursor: Some(
            IndexCursor::try_new(Bytes::copy_from_slice(member_key)).map_err(operation_error)?,
        ),
        stale_mark_cleanup: stale_mark_cleanup.clone(),
    };
    let next_root = next_root_value(root, next_phase, 0, None)?;
    transaction.delete(member_key)?;
    transaction.put(root_key, encode_root(&next_root))?;
    transaction.commit().await?;
    Ok(())
}

async fn load_member_disposition(
    db: &Db,
    root: &work::BlobGcRunRootValue,
    completed_first_attempt: work::GcScanAttempt,
    completed_second_attempt: work::GcScanAttempt,
    member: &work::BlobGcCandidateMemberValue,
) -> Result<work::BlobGcDisposition> {
    let first = load_member_mark(
        db,
        root,
        index_keys::BlobGcPass::First,
        completed_first_attempt,
        member,
    )
    .await?;
    let second = load_member_mark(
        db,
        root,
        index_keys::BlobGcPass::Second,
        completed_second_attempt,
        member,
    )
    .await?;
    Ok(if first || second {
        work::BlobGcDisposition::ReferencedPreserved
    } else {
        work::BlobGcDisposition::DeletedOrAbsent
    })
}

async fn load_member_mark(
    db: &Db,
    root: &work::BlobGcRunRootValue,
    pass: index_keys::BlobGcPass,
    attempt: work::GcScanAttempt,
    member: &work::BlobGcCandidateMemberValue,
) -> Result<bool> {
    let key = member_mark_key(root.run_id, pass, attempt, member.blob);
    let Some(value) = db.get(&key).await? else {
        return Err(corruption(
            "blob-GC Delete member is missing a completed reachability mark",
        ));
    };
    validate_member_mark(root, pass, attempt, member, &key, &value)
}

async fn require_member_disposition(
    transaction: &DbTransaction,
    root: &work::BlobGcRunRootValue,
    completed_first_attempt: work::GcScanAttempt,
    completed_second_attempt: work::GcScanAttempt,
    member: &work::BlobGcCandidateMemberValue,
) -> Result<work::BlobGcDisposition> {
    let first_key = member_mark_key(
        root.run_id,
        index_keys::BlobGcPass::First,
        completed_first_attempt,
        member.blob,
    );
    let second_key = member_mark_key(
        root.run_id,
        index_keys::BlobGcPass::Second,
        completed_second_attempt,
        member.blob,
    );
    let Some(first_value) = transaction.get(&first_key).await? else {
        return Err(corruption(
            "blob-GC cleanup commit lost its first-pass member mark",
        ));
    };
    let Some(second_value) = transaction.get(&second_key).await? else {
        return Err(corruption(
            "blob-GC cleanup commit lost its second-pass member mark",
        ));
    };
    let first = validate_member_mark(
        root,
        index_keys::BlobGcPass::First,
        completed_first_attempt,
        member,
        &first_key,
        &first_value,
    )?;
    let second = validate_member_mark(
        root,
        index_keys::BlobGcPass::Second,
        completed_second_attempt,
        member,
        &second_key,
        &second_value,
    )?;
    Ok(if first || second {
        work::BlobGcDisposition::ReferencedPreserved
    } else {
        work::BlobGcDisposition::DeletedOrAbsent
    })
}

fn member_mark_key(
    run_id: BlobGcRunId,
    pass: index_keys::BlobGcPass,
    attempt: work::GcScanAttempt,
    blob: work::BlobRef,
) -> Bytes {
    index_keys::GlobalIndexV2Key::BlobGcReachabilityMark {
        run_id,
        pass,
        scan_attempt: NonZeroU64::new(attempt.get())
            .expect("typed GC scan attempt is always non-zero"),
        blob_hash: index_keys::BlobHash::new(*blob.hash()),
    }
    .to_bytes()
}

fn validate_member_mark(
    root: &work::BlobGcRunRootValue,
    expected_pass: index_keys::BlobGcPass,
    expected_attempt: work::GcScanAttempt,
    member: &work::BlobGcCandidateMemberValue,
    key: &[u8],
    value: &[u8],
) -> Result<bool> {
    let index_keys::GlobalIndexV2Key::BlobGcReachabilityMark {
        run_id,
        pass,
        scan_attempt,
        blob_hash,
    } = index_keys::GlobalIndexV2Key::parse_from_slice(key)?
    else {
        return Err(corruption("blob-GC member mark has another key kind"));
    };
    let index_values::IndexV2WorkValue::BlobGcEntry(work::BlobGcEntryValue::ReachabilityMark(mark)) =
        index_values::decode_work_value(value)?
    else {
        return Err(corruption("blob-GC member mark has another value kind"));
    };
    if run_id != root.run_id
        || pass != expected_pass
        || scan_attempt.get() != expected_attempt.get()
        || blob_hash.as_bytes() != member.blob.hash()
        || mark.run_id != root.run_id
        || mark.first_pass != (expected_pass == index_keys::BlobGcPass::First)
        || mark.scan_attempt != expected_attempt
        || mark.blob_hash != blob_hash
    {
        return Err(corruption(
            "blob-GC completed member mark disagrees with its root and member",
        ));
    }
    Ok(mark.referenced)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DecodedRunMark {
    pass: index_keys::BlobGcPass,
    attempt: work::GcScanAttempt,
    blob_hash: index_keys::BlobHash,
}

struct TerminalMarkWitness {
    hashes: Vec<index_keys::BlobHash>,
    keys: Vec<Bytes>,
    input_bytes: u64,
}

async fn execute_stale_mark_cleanup_step(
    db: &Db,
    root: &work::BlobGcRunRootValue,
    completed_first_attempt: work::GcScanAttempt,
    completed_second_attempt: work::GcScanAttempt,
    cleanup: &work::StaleMarkCleanup,
) -> Result<BlobGcRootStep> {
    if matches!(cleanup, work::StaleMarkCleanup::Complete) {
        return match root.owner {
            work::BlobGcRunOwner::GenerationCleanup { .. } => Ok(BlobGcRootStep::Idle),
            work::BlobGcRunOwner::UploadReclaim { .. } => {
                finalize_upload_reclaim_root(
                    db,
                    root,
                    completed_first_attempt,
                    completed_second_attempt,
                )
                .await?;
                Ok(BlobGcRootStep::Progressed)
            }
        };
    }
    let work::StaleMarkCleanup::Pending { mark_cursor } = cleanup else {
        return Err(corruption(
            "blob-GC stale-mark cleanup has an unknown state",
        ));
    };
    let prefix = index_keys::GlobalIndexV2Key::blob_gc_reachability_mark_run_prefix(root.run_id);
    let start = match mark_cursor {
        Some(cursor) => {
            let Some(suffix) = cursor.as_bytes().strip_prefix(prefix.as_ref()) else {
                return Err(corruption(
                    "blob-GC stale-mark cursor escaped its exact run prefix",
                ));
            };
            Bound::Excluded(Bytes::copy_from_slice(suffix))
        }
        None => Bound::Unbounded,
    };
    let mut rows = db.scan_prefix(&prefix, (start, Bound::Unbounded)).await?;
    let Some(row) = rows.next().await? else {
        complete_stale_mark_cleanup(db, root, completed_first_attempt, completed_second_attempt)
            .await?;
        return Ok(BlobGcRootStep::Progressed);
    };
    let mark = decode_run_mark(root, &row.key, &row.value)?;
    let stale = classify_mark_attempt(mark, completed_first_attempt, completed_second_attempt)?;
    let root_key = index_keys::GlobalIndexV2Key::BlobGcRunRoot(root.run_id).to_bytes();
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    require_exact_root(&transaction, &root_key, root).await?;
    require_no_members(&transaction, root).await?;
    if transaction.get(&row.key).await?.as_deref() != Some(row.value.as_ref()) {
        return Err(corruption(
            "blob-GC reachability mark changed during stale cleanup",
        ));
    }
    let exact_mark = decode_run_mark(root, &row.key, &row.value)?;
    if classify_mark_attempt(
        exact_mark,
        completed_first_attempt,
        completed_second_attempt,
    )? != stale
    {
        return Err(corruption(
            "blob-GC reachability mark classification changed during cleanup",
        ));
    }
    if stale {
        transaction.delete(&row.key)?;
    }
    let work::BlobGcPhase::Delete { member_cursor, .. } = &root.phase else {
        return Err(corruption("blob-GC stale-mark cleanup requires Delete"));
    };
    let next_phase = work::BlobGcPhase::Delete {
        completed_first_attempt,
        completed_second_attempt,
        member_cursor: member_cursor.clone(),
        stale_mark_cleanup: work::StaleMarkCleanup::Pending {
            mark_cursor: Some(IndexCursor::try_new(row.key).map_err(operation_error)?),
        },
    };
    let next_root = next_root_value(root, next_phase, 0, None)?;
    transaction.put(root_key, encode_root(&next_root))?;
    transaction.commit().await?;
    Ok(BlobGcRootStep::Progressed)
}

fn decode_run_mark(
    root: &work::BlobGcRunRootValue,
    key: &[u8],
    value: &[u8],
) -> Result<DecodedRunMark> {
    let index_keys::GlobalIndexV2Key::BlobGcReachabilityMark {
        run_id,
        pass,
        scan_attempt,
        blob_hash,
    } = index_keys::GlobalIndexV2Key::parse_from_slice(key)?
    else {
        return Err(corruption("blob-GC mark lane yielded another key kind"));
    };
    let index_values::IndexV2WorkValue::BlobGcEntry(work::BlobGcEntryValue::ReachabilityMark(mark)) =
        index_values::decode_work_value(value)?
    else {
        return Err(corruption("blob-GC mark key contains another value kind"));
    };
    let attempt = work::GcScanAttempt::new(scan_attempt.get())
        .map_err(|error| corruption(error.to_string()))?;
    if run_id != root.run_id
        || mark.run_id != root.run_id
        || mark.first_pass != (pass == index_keys::BlobGcPass::First)
        || mark.scan_attempt != attempt
        || mark.blob_hash != blob_hash
    {
        return Err(corruption(
            "blob-GC reachability mark key and value disagree",
        ));
    }
    Ok(DecodedRunMark {
        pass,
        attempt,
        blob_hash,
    })
}

fn classify_mark_attempt(
    mark: DecodedRunMark,
    completed_first_attempt: work::GcScanAttempt,
    completed_second_attempt: work::GcScanAttempt,
) -> Result<bool> {
    let completed = match mark.pass {
        index_keys::BlobGcPass::First => completed_first_attempt,
        index_keys::BlobGcPass::Second => completed_second_attempt,
    };
    if mark.attempt.get() > completed.get() {
        return Err(corruption(
            "blob-GC Delete root contains a future-attempt reachability mark",
        ));
    }
    Ok(mark.attempt != completed)
}

async fn complete_stale_mark_cleanup(
    db: &Db,
    root: &work::BlobGcRunRootValue,
    completed_first_attempt: work::GcScanAttempt,
    completed_second_attempt: work::GcScanAttempt,
) -> Result<()> {
    let root_key = index_keys::GlobalIndexV2Key::BlobGcRunRoot(root.run_id).to_bytes();
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    require_exact_root(&transaction, &root_key, root).await?;
    require_no_members(&transaction, root).await?;
    validate_current_mark_witness(
        &transaction,
        root,
        completed_first_attempt,
        completed_second_attempt,
    )
    .await?;
    let work::BlobGcPhase::Delete { member_cursor, .. } = &root.phase else {
        return Err(corruption("blob-GC stale-mark completion requires Delete"));
    };
    let next_phase = work::BlobGcPhase::Delete {
        completed_first_attempt,
        completed_second_attempt,
        member_cursor: member_cursor.clone(),
        stale_mark_cleanup: work::StaleMarkCleanup::Complete,
    };
    let next_root = next_root_value(root, next_phase, 0, None)?;
    transaction.put(root_key, encode_root(&next_root))?;
    transaction.commit().await?;
    Ok(())
}

async fn require_no_members(
    transaction: &DbTransaction,
    root: &work::BlobGcRunRootValue,
) -> Result<()> {
    let prefix = index_keys::GlobalIndexV2Key::blob_gc_candidate_member_prefix(root.run_id);
    let mut members = transaction
        .scan_prefix(&prefix, (Bound::Unbounded, Bound::<Bytes>::Unbounded))
        .await?;
    if members.next().await?.is_some() {
        return Err(corruption(
            "blob-GC stale-mark cleanup began before every member was removed",
        ));
    }
    Ok(())
}

async fn validate_current_mark_witness(
    transaction: &DbTransaction,
    root: &work::BlobGcRunRootValue,
    completed_first_attempt: work::GcScanAttempt,
    completed_second_attempt: work::GcScanAttempt,
) -> Result<TerminalMarkWitness> {
    let run_prefix =
        index_keys::GlobalIndexV2Key::blob_gc_reachability_mark_run_prefix(root.run_id);
    let mut all_marks = transaction
        .scan_prefix(&run_prefix, (Bound::Unbounded, Bound::<Bytes>::Unbounded))
        .await?;
    let expected_count = usize::try_from(root.candidate_count.get())
        .map_err(|_| corruption("blob-GC candidate count does not fit this architecture"))?;
    let mut total = 0_usize;
    let mut keys = Vec::with_capacity(expected_count.saturating_mul(2));
    let mut input_bytes = 0_u64;
    while let Some(row) = all_marks.next().await? {
        let mark = decode_run_mark(root, &row.key, &row.value)?;
        if classify_mark_attempt(mark, completed_first_attempt, completed_second_attempt)? {
            return Err(corruption(
                "blob-GC stale-mark cleanup completed with an older attempt retained",
            ));
        }
        total = total
            .checked_add(1)
            .ok_or_else(|| corruption("blob-GC current mark count overflowed"))?;
        input_bytes = input_bytes
            .checked_add(measured_row(&row.key, Some(&row.value)))
            .ok_or_else(|| corruption("blob-GC terminal mark input bytes overflowed"))?;
        keys.push(row.key);
    }
    if total
        != expected_count
            .checked_mul(2)
            .ok_or_else(|| corruption("blob-GC expected mark count overflowed"))?
    {
        return Err(corruption(
            "blob-GC terminal mark count disagrees with the immutable candidate count",
        ));
    }
    let first_prefix = index_keys::GlobalIndexV2Key::blob_gc_reachability_mark_prefix(
        root.run_id,
        index_keys::BlobGcPass::First,
        NonZeroU64::new(completed_first_attempt.get())
            .expect("typed GC scan attempt is always non-zero"),
    );
    let second_prefix = index_keys::GlobalIndexV2Key::blob_gc_reachability_mark_prefix(
        root.run_id,
        index_keys::BlobGcPass::Second,
        NonZeroU64::new(completed_second_attempt.get())
            .expect("typed GC scan attempt is always non-zero"),
    );
    let mut first = transaction
        .scan_prefix(&first_prefix, (Bound::Unbounded, Bound::<Bytes>::Unbounded))
        .await?;
    let mut second = transaction
        .scan_prefix(
            &second_prefix,
            (Bound::Unbounded, Bound::<Bytes>::Unbounded),
        )
        .await?;
    let mut hashes = Vec::with_capacity(expected_count);
    for _ in 0..expected_count {
        let Some(first_row) = first.next().await? else {
            return Err(corruption(
                "blob-GC terminal witness is missing a first-pass mark",
            ));
        };
        let Some(second_row) = second.next().await? else {
            return Err(corruption(
                "blob-GC terminal witness is missing a second-pass mark",
            ));
        };
        let first_mark = decode_run_mark(root, &first_row.key, &first_row.value)?;
        let second_mark = decode_run_mark(root, &second_row.key, &second_row.value)?;
        input_bytes = input_bytes
            .checked_add(measured_row(&first_row.key, Some(&first_row.value)))
            .and_then(|total| {
                total.checked_add(measured_row(&second_row.key, Some(&second_row.value)))
            })
            .ok_or_else(|| corruption("blob-GC terminal witness input bytes overflowed"))?;
        if first_mark.pass != index_keys::BlobGcPass::First
            || first_mark.attempt != completed_first_attempt
            || second_mark.pass != index_keys::BlobGcPass::Second
            || second_mark.attempt != completed_second_attempt
            || first_mark.blob_hash != second_mark.blob_hash
        {
            return Err(corruption(
                "blob-GC terminal pass witnesses disagree on their exact hash set",
            ));
        }
        hashes.push(first_mark.blob_hash);
    }
    if first.next().await?.is_some() || second.next().await?.is_some() {
        return Err(corruption(
            "blob-GC terminal witness contains extra current-attempt marks",
        ));
    }
    Ok(TerminalMarkWitness {
        hashes,
        keys,
        input_bytes,
    })
}

async fn finalize_upload_reclaim_root(
    db: &Db,
    root: &work::BlobGcRunRootValue,
    completed_first_attempt: work::GcScanAttempt,
    completed_second_attempt: work::GcScanAttempt,
) -> Result<()> {
    let work::BlobGcRunOwner::UploadReclaim {
        scope,
        intent_id,
        index_id,
        generation,
    } = root.owner
    else {
        return Err(corruption(
            "generation-owned root cannot use upload terminal cleanup",
        ));
    };
    if root.candidate_count.get() != 1 {
        return Err(corruption(
            "upload-reclaim root must retain exactly one original candidate",
        ));
    }
    let root_key = index_keys::GlobalIndexV2Key::BlobGcRunRoot(root.run_id).to_bytes();
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    require_exact_root(&transaction, &root_key, root).await?;
    require_no_members(&transaction, root).await?;
    let witness = validate_current_mark_witness(
        &transaction,
        root,
        completed_first_attempt,
        completed_second_attempt,
    )
    .await?;
    let [blob_hash] = witness.hashes.as_slice() else {
        return Err(corruption(
            "upload-reclaim root terminal witness is not a singleton",
        ));
    };
    let intent_key = scoped_key(
        scope,
        index_keys::IndexV2Key::TextUploadIntent(index_keys::TextIntentOwnedKey {
            index_id,
            generation,
            intent_id,
        }),
    );
    let pointer_key = index_keys::GlobalIndexV2Key::UploadPointer(intent_id).to_bytes();
    let candidate_key = scoped_key(
        scope,
        index_keys::IndexV2Key::BlobGcCandidate(index_keys::BlobGcCandidateKey {
            index_id,
            generation,
            owner: index_keys::BlobGcCandidateKeyOwner::UploadIntent(intent_id),
            blob_hash: *blob_hash,
        }),
    );
    for key in [&intent_key, &pointer_key, &candidate_key] {
        if transaction.get(key).await?.is_some() {
            return Err(corruption(
                "upload-reclaim root retained owner rows after cleanup commit",
            ));
        }
    }
    for key in witness.keys {
        transaction.delete(key)?;
    }
    transaction.delete(root_key)?;
    transaction.commit().await?;
    Ok(())
}

fn coordinator_error_is_retryable(error: &blob_publication::BlobPublicationError) -> bool {
    matches!(
        error,
        blob_publication::BlobPublicationError::PublicationOutcomeAmbiguous(_)
            | blob_publication::BlobPublicationError::ObjectStore(_)
            | blob_publication::BlobPublicationError::CoordinatorUnavailable(_)
    )
}

/// One immutable member plus its position inside a stable reference prefix.
struct ReachabilityMemberScan {
    member_key: Bytes,
    member: work::BlobGcCandidateMemberValue,
    reference_start: Bound<Bytes>,
    referenced: bool,
}

/// Advances one reference row, one member mark, or one complete pass.
async fn execute_reachability_pass_step(
    db: &Db,
    root: &work::BlobGcRunRootValue,
    pass: &ReachabilityPass,
    runtime: &ReachabilityPassRuntime,
) -> Result<bool> {
    let Some(scan) = reachability_member_scan(db, root, pass, &runtime.snapshot).await? else {
        complete_reachability_pass(db, root, pass).await?;
        return Ok(true);
    };
    let reference_prefix = index_keys::GlobalIndexV2Key::blob_reachability_reference_prefix(
        index_keys::BlobHash::new(*scan.member.blob.hash()),
    );
    let mut references = runtime
        .snapshot
        .scan_prefix(
            &reference_prefix,
            (scan.reference_start.clone(), Bound::Unbounded),
        )
        .await?;
    if let Some(row) = references.next().await? {
        validate_reachability_reference(&scan.member, &row.key, &row.value)?;
        replace_root(
            db,
            root,
            pass.phase_with_cursor(Some(
                IndexCursor::try_new(row.key).map_err(operation_error)?,
            )),
            0,
            None,
        )
        .await?;
        return Ok(false);
    }
    checkpoint_reachability_mark(db, root, pass, &scan).await?;
    Ok(false)
}

/// Resolves the cursor union without scanning unrelated reference hashes.
async fn reachability_member_scan(
    db: &Db,
    root: &work::BlobGcRunRootValue,
    pass: &ReachabilityPass,
    snapshot: &DbSnapshot,
) -> Result<Option<ReachabilityMemberScan>> {
    let Some(cursor) = pass.reference_cursor() else {
        return Ok(next_member(db, root.run_id, None)
            .await?
            .map(|(member_key, member)| ReachabilityMemberScan {
                member_key,
                member,
                reference_start: Bound::Unbounded,
                referenced: false,
            }));
    };
    match index_keys::GlobalIndexV2Key::parse_from_slice(cursor.as_bytes())? {
        index_keys::GlobalIndexV2Key::BlobGcCandidateMember { run_id, .. } => {
            if run_id != root.run_id {
                return Err(corruption("blob-GC pass member cursor names another run"));
            }
            Ok(next_member(db, root.run_id, Some(cursor))
                .await?
                .map(|(member_key, member)| ReachabilityMemberScan {
                    member_key,
                    member,
                    reference_start: Bound::Unbounded,
                    referenced: false,
                }))
        }
        index_keys::GlobalIndexV2Key::BlobReachabilityReference(reference) => {
            let member_key = index_keys::GlobalIndexV2Key::BlobGcCandidateMember {
                run_id: root.run_id,
                blob_hash: reference.blob_hash,
            }
            .to_bytes();
            let Some(member_value) = db.get(&member_key).await? else {
                return Err(corruption(
                    "blob-GC reference cursor names a missing immutable member",
                ));
            };
            let member = decode_member(root.run_id, &member_key, &member_value)?;
            let Some(reference_value) = snapshot.get(cursor.as_bytes()).await? else {
                return Err(corruption(
                    "blob-GC reference cursor is absent from its retained snapshot",
                ));
            };
            validate_reachability_reference(&member, cursor.as_bytes(), &reference_value)?;
            let reference_prefix = index_keys::GlobalIndexV2Key::blob_reachability_reference_prefix(
                reference.blob_hash,
            );
            let Some(reference_suffix) = cursor.as_bytes().strip_prefix(reference_prefix.as_ref())
            else {
                return Err(corruption(
                    "blob-GC reference cursor escaped its exact hash prefix",
                ));
            };
            Ok(Some(ReachabilityMemberScan {
                member_key,
                member,
                reference_start: Bound::Excluded(Bytes::copy_from_slice(reference_suffix)),
                referenced: true,
            }))
        }
        index_keys::GlobalIndexV2Key::StorageVersion
        | index_keys::GlobalIndexV2Key::LogicalIndexIdWatermark
        | index_keys::GlobalIndexV2Key::VectorPhysicalIdWatermark
        | index_keys::GlobalIndexV2Key::OperationPointer(_)
        | index_keys::GlobalIndexV2Key::UploadPointer(_)
        | index_keys::GlobalIndexV2Key::BlobGcRunRoot(_)
        | index_keys::GlobalIndexV2Key::BlobGcReachabilityMark { .. } => Err(corruption(
            "blob-GC pass cursor has another global key kind",
        )),
    }
}

/// Validates the redundant key/value reference contract against one member.
fn validate_reachability_reference(
    member: &work::BlobGcCandidateMemberValue,
    key: &[u8],
    value: &[u8],
) -> Result<()> {
    let index_keys::GlobalIndexV2Key::BlobReachabilityReference(reference_key) =
        index_keys::GlobalIndexV2Key::parse_from_slice(key)?
    else {
        return Err(corruption(
            "blob-GC reference prefix yielded another global key kind",
        ));
    };
    let index_values::IndexV2WorkValue::BlobReachabilityReference(reference_value) =
        index_values::decode_work_value(value)?
    else {
        return Err(corruption(
            "blob-GC reference key contains another value kind",
        ));
    };
    if reference_key.blob_hash.as_bytes() != member.blob.hash()
        || reference_value.blob != member.blob
        || reference_value.owner_kind != reference_key.owner_kind
        || reference_value.scope != reference_key.scope
        || reference_value.owner_logical_key != reference_key.owner_logical_key
        || reference_value.owner_slot != reference_key.owner_slot
    {
        return Err(corruption(
            "blob-GC reachability reference key, value, and member disagree",
        ));
    }
    Ok(())
}

/// Atomically writes one current-attempt mark and its completed-member cursor.
async fn checkpoint_reachability_mark(
    db: &Db,
    root: &work::BlobGcRunRootValue,
    pass: &ReachabilityPass,
    scan: &ReachabilityMemberScan,
) -> Result<()> {
    let scan_attempt =
        NonZeroU64::new(pass.attempt().get()).expect("typed GC scan attempt is always non-zero");
    let blob_hash = index_keys::BlobHash::new(*scan.member.blob.hash());
    let mark_key = index_keys::GlobalIndexV2Key::BlobGcReachabilityMark {
        run_id: root.run_id,
        pass: pass.key_pass(),
        scan_attempt,
        blob_hash,
    }
    .to_bytes();
    let mark_value = index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcEntry(
        work::BlobGcEntryValue::ReachabilityMark(work::BlobGcReachabilityMarkValue {
            run_id: root.run_id,
            first_pass: pass.key_pass() == index_keys::BlobGcPass::First,
            scan_attempt: pass.attempt(),
            blob_hash,
            referenced: scan.referenced,
        }),
    ));
    let root_key = index_keys::GlobalIndexV2Key::BlobGcRunRoot(root.run_id).to_bytes();
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    require_exact_root(&transaction, &root_key, root).await?;
    let Some(member_value) = transaction.get(&scan.member_key).await? else {
        return Err(corruption(
            "blob-GC member disappeared before its reachability mark",
        ));
    };
    if decode_member(root.run_id, &scan.member_key, &member_value)? != scan.member {
        return Err(corruption(
            "blob-GC member changed before its reachability mark",
        ));
    }
    if transaction.get(&mark_key).await?.is_some() {
        return Err(corruption(
            "blob-GC current-attempt mark existed before member completion",
        ));
    }
    let next = next_root_value(
        root,
        pass.phase_with_cursor(Some(
            IndexCursor::try_new(scan.member_key.clone()).map_err(operation_error)?,
        )),
        0,
        None,
    )?;
    transaction.put(mark_key, mark_value)?;
    transaction.put(root_key, encode_root(&next))?;
    transaction.commit().await?;
    Ok(())
}

/// Validates the exact current mark set and atomically enters the next phase.
async fn complete_reachability_pass(
    db: &Db,
    root: &work::BlobGcRunRootValue,
    pass: &ReachabilityPass,
) -> Result<()> {
    let root_key = index_keys::GlobalIndexV2Key::BlobGcRunRoot(root.run_id).to_bytes();
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    require_exact_root(&transaction, &root_key, root).await?;
    validate_exact_mark_set(&transaction, root, pass.key_pass(), pass.attempt()).await?;
    let next_phase = match pass {
        ReachabilityPass::First {
            writer_epoch,
            attempt,
            ..
        } => work::BlobGcPhase::SecondPass {
            completed_first_attempt: *attempt,
            writer_epoch: *writer_epoch,
            second_attempt: work::GcScanAttempt::new(1)
                .map_err(|error| invariant(error.to_string()))?,
            reference_cursor: None,
        },
        ReachabilityPass::Second {
            completed_first_attempt,
            attempt,
            ..
        } => {
            validate_exact_mark_set(
                &transaction,
                root,
                index_keys::BlobGcPass::First,
                *completed_first_attempt,
            )
            .await?;
            work::BlobGcPhase::Delete {
                completed_first_attempt: *completed_first_attempt,
                completed_second_attempt: *attempt,
                member_cursor: None,
                stale_mark_cleanup: work::StaleMarkCleanup::Pending { mark_cursor: None },
            }
        }
    };
    let next = next_root_value(root, next_phase, 0, None)?;
    transaction.put(root_key, encode_root(&next))?;
    transaction.commit().await?;
    Ok(())
}

/// Proves a one-to-one ordered correspondence between members and marks.
async fn validate_exact_mark_set(
    transaction: &DbTransaction,
    root: &work::BlobGcRunRootValue,
    pass: index_keys::BlobGcPass,
    attempt: work::GcScanAttempt,
) -> Result<()> {
    let member_prefix = index_keys::GlobalIndexV2Key::blob_gc_candidate_member_prefix(root.run_id);
    let mark_prefix = index_keys::GlobalIndexV2Key::blob_gc_reachability_mark_prefix(
        root.run_id,
        pass,
        NonZeroU64::new(attempt.get()).expect("typed GC scan attempt is always non-zero"),
    );
    let mut members = transaction
        .scan_prefix(
            &member_prefix,
            (Bound::Unbounded, Bound::<Bytes>::Unbounded),
        )
        .await?;
    let mut marks = transaction
        .scan_prefix(&mark_prefix, (Bound::Unbounded, Bound::<Bytes>::Unbounded))
        .await?;
    let expected_count = usize::try_from(root.candidate_count.get())
        .map_err(|_| corruption("blob-GC candidate count does not fit this architecture"))?;
    for _ in 0..expected_count {
        let Some(member_row) = members.next().await? else {
            return Err(corruption(
                "blob-GC pass completion is missing an immutable member",
            ));
        };
        let Some(mark_row) = marks.next().await? else {
            return Err(corruption(
                "blob-GC pass completion is missing a current-attempt mark",
            ));
        };
        let member = decode_member(root.run_id, &member_row.key, &member_row.value)?;
        let index_keys::GlobalIndexV2Key::BlobGcReachabilityMark {
            run_id,
            pass: mark_pass,
            scan_attempt,
            blob_hash,
        } = index_keys::GlobalIndexV2Key::parse_from_slice(&mark_row.key)?
        else {
            return Err(corruption(
                "blob-GC mark prefix yielded another global key kind",
            ));
        };
        let index_values::IndexV2WorkValue::BlobGcEntry(work::BlobGcEntryValue::ReachabilityMark(
            mark,
        )) = index_values::decode_work_value(&mark_row.value)?
        else {
            return Err(corruption("blob-GC mark key contains another value kind"));
        };
        if run_id != root.run_id
            || mark_pass != pass
            || scan_attempt.get() != attempt.get()
            || blob_hash.as_bytes() != member.blob.hash()
            || mark.run_id != root.run_id
            || mark.first_pass != (pass == index_keys::BlobGcPass::First)
            || mark.scan_attempt != attempt
            || mark.blob_hash != blob_hash
        {
            return Err(corruption(
                "blob-GC current mark set disagrees with its immutable member set",
            ));
        }
    }
    if members.next().await?.is_some() || marks.next().await?.is_some() {
        return Err(corruption(
            "blob-GC pass completion has extra members or current-attempt marks",
        ));
    }
    Ok(())
}

async fn require_exact_root(
    transaction: &DbTransaction,
    root_key: &[u8],
    expected: &work::BlobGcRunRootValue,
) -> Result<()> {
    let Some(root_value) = transaction.get(root_key).await? else {
        return Err(corruption("blob-GC root disappeared before checkpoint"));
    };
    if decode_root(&root_value)? != *expected {
        return Err(corruption(
            "blob-GC root changed before its exact checkpoint",
        ));
    }
    Ok(())
}

async fn require_exact_member(
    transaction: &DbTransaction,
    run_id: BlobGcRunId,
    member_key: &[u8],
    expected: &work::BlobGcCandidateMemberValue,
) -> Result<()> {
    let Some(value) = transaction.get(member_key).await? else {
        return Err(corruption(
            "blob-GC member disappeared before its exact checkpoint",
        ));
    };
    if decode_member(run_id, member_key, &value)? != *expected {
        return Err(corruption(
            "blob-GC member changed before its exact checkpoint",
        ));
    }
    Ok(())
}

/// Reads one bounded page from the global root lane.
pub(crate) async fn scan_root_page(
    db: &Db,
    resume_after: Option<BlobGcRunId>,
    page_size: NonZeroUsize,
) -> Result<BlobGcRootPage> {
    let prefix =
        index_keys::GlobalIndexV2Key::logical_prefix(index_keys::GlobalIndexV2Kind::BlobGcRunRoot);
    let start = resume_after.map_or(Bound::Unbounded, |run_id| {
        Bound::Excluded(Bytes::copy_from_slice(run_id.as_bytes()))
    });
    let mut rows = db.scan_prefix(&prefix, (start, Bound::Unbounded)).await?;
    let mut run_ids = Vec::with_capacity(page_size.get());
    while run_ids.len() < page_size.get() {
        let Some(row) = rows.next().await? else {
            break;
        };
        let index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id) =
            index_keys::GlobalIndexV2Key::parse_from_slice(&row.key)?
        else {
            return Err(corruption("blob-GC root lane yielded another key kind"));
        };
        let root = decode_root(&row.value)?;
        if root.run_id != run_id {
            return Err(corruption("blob-GC root key and value disagree"));
        }
        run_ids.push(run_id);
    }
    Ok(BlobGcRootPage {
        prefix_exhausted: run_ids.len() < page_size.get(),
        resume_after: run_ids.last().copied(),
        run_ids,
    })
}

/// Scans one bounded intent page and waits on any exact foreign reclaim root.
pub(super) async fn scan_reclaim_barrier(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &PrefixScanProgress,
    limits: SearchIndexBatchLimits,
) -> Result<ReclaimBarrierTransition> {
    let prefix = Key::data_prefix(
        scope,
        index_keys::IndexV2Key::generation_prefix(
            index_keys::IndexV2RecordKind::TextUploadIntent,
            operation.index_id(),
            operation.generation(),
        ),
    );
    let start = match progress.cursor.as_ref() {
        Some(cursor) => {
            let Some(suffix) = cursor.as_bytes().strip_prefix(prefix.as_ref()) else {
                return Err(corruption(
                    "text reclaim-barrier cursor is outside its exact generation prefix",
                ));
            };
            Bound::Excluded(Bytes::copy_from_slice(suffix))
        }
        None => Bound::Unbounded,
    };
    let mut rows = transaction
        .scan_prefix(&prefix, (start, Bound::Unbounded))
        .await?;
    let mut visited = 0_usize;
    let mut input_bytes = 0_u64;
    let mut completed_cursor = progress.cursor.clone();
    while visited < limits.max_entities().get() {
        let Some(row) = rows.next().await? else {
            return Ok(ReclaimBarrierTransition::Complete(add_counters(
                progress.counters,
                visited,
                input_bytes,
                0,
                0,
            )?));
        };
        let row_bytes = measured_row(&row.key, Some(&row.value));
        if input_bytes.saturating_add(row_bytes) > limits.max_input_bytes().get() {
            if visited == 0 {
                return Ok(ReclaimBarrierTransition::Blocked(
                    IndexOperationBlocker::InvariantViolation,
                ));
            }
            break;
        }
        let intent = decode_intent(scope, operation, &row.key, &row.value)?;
        if let work::TextUploadPhase::Reclaimable(work::ReclaimAssignment::Assigned(run_id)) =
            intent.phase
        {
            if visited != 0 {
                break;
            }
            validate_upload_reclaim_root(transaction, scope, operation, &intent, run_id).await?;
            return Ok(ReclaimBarrierTransition::Waiting);
        }
        visited = visited
            .checked_add(1)
            .ok_or_else(|| invariant("text reclaim-barrier entity count overflowed"))?;
        input_bytes = input_bytes
            .checked_add(row_bytes)
            .ok_or_else(|| invariant("text reclaim-barrier input counter overflowed"))?;
        completed_cursor = Some(IndexCursor::try_new(row.key).map_err(operation_error)?);
    }
    Ok(ReclaimBarrierTransition::Progressed(PrefixScanProgress {
        cursor: completed_cursor,
        counters: add_counters(progress.counters, visited, input_bytes, 0, 0)?,
    }))
}

/// Stages one bounded immutable generation-owned run or advances its barrier.
pub(super) async fn stage_generation_batch(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &GcProgress,
    limits: SearchIndexBatchLimits,
) -> Result<GenerationBatchTransition> {
    if let Some(run_id) = progress.gc_run_id {
        let Some(_) = progress.candidate_cursor else {
            return Err(corruption(
                "text cleanup operation names a GC run without its candidate cursor",
            ));
        };
        if progress.stage_cursor.is_some() {
            return Err(corruption(
                "text cleanup operation retains a stage cursor while a GC run is assigned",
            ));
        }
        let observed = load_generation_root(transaction, scope, operation, run_id).await?;
        return match observed.root.phase {
            work::BlobGcPhase::AwaitDeleteFences { .. } => Ok(GenerationBatchTransition::Waiting),
            work::BlobGcPhase::FencesClosed => {
                Ok(GenerationBatchTransition::FencesClosed(progress.clone()))
            }
            work::BlobGcPhase::FirstPass { .. }
            | work::BlobGcPhase::SecondPass { .. }
            | work::BlobGcPhase::Delete { .. } => Err(corruption(
                "blob-GC root advanced beyond fences before owner retirement",
            )),
        };
    }

    let barrier_complete = match progress.stage_cursor.as_ref() {
        Some(cursor) => is_generation_candidate_start_cursor(scope, operation, cursor)?,
        None => false,
    };
    if !barrier_complete {
        let barrier = PrefixScanProgress {
            cursor: progress.stage_cursor.clone(),
            counters: progress.counters,
        };
        return Ok(
            match scan_reclaim_barrier(transaction, scope, operation, &barrier, limits).await? {
                ReclaimBarrierTransition::Progressed(next) => {
                    GenerationBatchTransition::Progressed(GcProgress {
                        gc_run_id: None,
                        candidate_cursor: progress.candidate_cursor.clone(),
                        stage_cursor: next.cursor,
                        counters: next.counters,
                    })
                }
                ReclaimBarrierTransition::Complete(counters) => {
                    GenerationBatchTransition::Progressed(GcProgress {
                        gc_run_id: None,
                        candidate_cursor: progress.candidate_cursor.clone(),
                        stage_cursor: Some(generation_candidate_start_cursor(scope, operation)?),
                        counters,
                    })
                }
                ReclaimBarrierTransition::Waiting => GenerationBatchTransition::Waiting,
                ReclaimBarrierTransition::Blocked(blocker) => {
                    GenerationBatchTransition::Blocked(blocker)
                }
            },
        );
    }

    let prefix = Key::data_prefix(
        scope,
        index_keys::IndexV2Key::generation_cleanup_candidate_prefix(
            operation.index_id(),
            operation.generation(),
        ),
    );
    let start = match progress.candidate_cursor.as_ref() {
        Some(cursor) => {
            let Some(suffix) = cursor.as_bytes().strip_prefix(prefix.as_ref()) else {
                return Err(corruption(
                    "text generation-candidate cursor is outside its exact owner lane",
                ));
            };
            Bound::Excluded(Bytes::copy_from_slice(suffix))
        }
        None => Bound::Unbounded,
    };
    let mut rows = transaction
        .scan_prefix(&prefix, (start, Bound::Unbounded))
        .await?;
    let Some(first_row) = rows.next().await? else {
        return Ok(GenerationBatchTransition::Exhausted(progress.counters));
    };
    let first = decode_generation_candidate(scope, operation, &first_row.key, &first_row.value)?;
    let (run_id, root_key, root_lookup_bytes) = unused_run_id(transaction).await?;
    let mut batch = GenerationBatch::new(
        scope,
        operation,
        run_id,
        root_key,
        root_lookup_bytes,
        limits,
    )?;
    let first_input_bytes = measured_row(&first_row.key, Some(&first_row.value));
    match batch.admit(first_row.key, first, first_input_bytes)? {
        BatchAdmission::Admitted => {}
        BatchAdmission::Full | BatchAdmission::Indivisible => {
            return Ok(GenerationBatchTransition::Blocked(
                IndexOperationBlocker::InvariantViolation,
            ));
        }
    }
    while batch.can_admit_another() {
        let Some(row) = rows.next().await? else {
            break;
        };
        let candidate = decode_generation_candidate(scope, operation, &row.key, &row.value)?;
        let input_bytes = measured_row(&row.key, Some(&row.value));
        match batch.admit(row.key, candidate, input_bytes)? {
            BatchAdmission::Admitted => {}
            BatchAdmission::Full => break,
            BatchAdmission::Indivisible => {
                return Err(invariant(
                    "non-first text GC member was classified as indivisible",
                ));
            }
        }
    }
    let (candidate_cursor, counters) = batch.stage(transaction, progress.counters)?;
    Ok(GenerationBatchTransition::Progressed(GcProgress {
        gc_run_id: Some(run_id),
        candidate_cursor: Some(candidate_cursor),
        stage_cursor: None,
        counters,
    }))
}

/// Returns the typed, non-persisted marker between an intent barrier and a candidate scan.
pub(super) fn generation_candidate_start_cursor(
    scope: DataScope,
    operation: &IndexOperationRecord,
) -> Result<IndexCursor> {
    IndexCursor::try_new(scoped_key(
        scope,
        index_keys::IndexV2Key::BlobGcCandidate(index_keys::BlobGcCandidateKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            owner: index_keys::BlobGcCandidateKeyOwner::GenerationCleanup,
            blob_hash: index_keys::BlobHash::new([0; 32]),
        }),
    ))
    .map_err(operation_error)
}

/// Recognizes only the exact zero-hash marker for this generation.
pub(super) fn is_generation_candidate_start_cursor(
    scope: DataScope,
    operation: &IndexOperationRecord,
    cursor: &IndexCursor,
) -> Result<bool> {
    let Ok(Key::Data {
        kind:
            DataKeyKind::IndexV2(index_keys::IndexV2Key::BlobGcCandidate(
                index_keys::BlobGcCandidateKey {
                    index_id,
                    generation,
                    owner: index_keys::BlobGcCandidateKeyOwner::GenerationCleanup,
                    blob_hash,
                },
            )),
        ..
    }) = Key::parse_from_slice(scope, cursor.as_bytes())
    else {
        return Ok(false);
    };
    if index_id != operation.index_id() || generation != operation.generation() {
        return Err(corruption(
            "text generation-candidate marker names another generation",
        ));
    }
    if blob_hash.as_bytes() != &[0; 32] {
        return Err(corruption(
            "text generation-candidate marker has a non-zero hash",
        ));
    }
    Ok(true)
}

/// One bounded, immutable root/member write set.
struct GenerationBatch {
    root_key: Bytes,
    root: work::BlobGcRunRootValue,
    limits: SearchIndexBatchLimits,
    input_bytes: u64,
    output_operations: u64,
    output_bytes: u64,
    members: Vec<(Bytes, Bytes, IndexCursor, Option<Bytes>)>,
}

/// One validated generation candidate and its optional current-operation owner rewrite.
struct SelectedGenerationCandidate {
    value: work::BlobGcCandidateValue,
    owner_rewrite: Option<Bytes>,
}

impl GenerationBatch {
    fn new(
        scope: DataScope,
        operation: &IndexOperationRecord,
        run_id: BlobGcRunId,
        root_key: Bytes,
        root_lookup_bytes: u64,
        limits: SearchIndexBatchLimits,
    ) -> Result<Self> {
        let root = work::BlobGcRunRootValue::try_new(
            run_id,
            work::BlobGcRunOwner::GenerationCleanup {
                scope,
                operation_id: operation.operation_id(),
                index_id: operation.index_id(),
                generation: operation.generation(),
            },
            BlobGcRunRevision::initial(),
            0,
            None,
            work::BlobGcPhase::AwaitDeleteFences {
                member_cursor: None,
            },
            1,
        )
        .map_err(|error| invariant(error.to_string()))?;
        Ok(Self {
            root_key,
            root,
            limits,
            input_bytes: root_lookup_bytes,
            output_operations: 0,
            output_bytes: 0,
            members: Vec::new(),
        })
    }

    fn can_admit_another(&self) -> bool {
        self.members.len() < self.limits.max_entities().get()
            && self.members.len() < usize::try_from(u32::MAX).unwrap_or(usize::MAX)
    }

    fn admit(
        &mut self,
        candidate_key: Bytes,
        candidate: SelectedGenerationCandidate,
        candidate_input_bytes: u64,
    ) -> Result<BatchAdmission> {
        if !self.can_admit_another() {
            return Ok(BatchAdmission::Full);
        }
        let member_key = index_keys::GlobalIndexV2Key::BlobGcCandidateMember {
            run_id: self.root.run_id,
            blob_hash: index_keys::BlobHash::new(*candidate.value.blob.hash()),
        }
        .to_bytes();
        let member_value =
            index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcEntry(
                work::BlobGcEntryValue::CandidateMember(work::BlobGcCandidateMemberValue {
                    run_id: self.root.run_id,
                    blob: candidate.value.blob,
                    state: work::BlobGcMemberState::PendingDisposition { owner_cursor: None },
                }),
            ));
        let member_output_bytes = measured_row(&member_key, Some(&member_value));
        let first = self.members.is_empty();
        let root_output_bytes = if first {
            let root_value =
                index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcEntry(
                    work::BlobGcEntryValue::RunRoot(self.root.clone()),
                ));
            measured_row(&self.root_key, Some(&root_value))
        } else {
            0
        };
        let admitted_output_operations = 1_u64
            .saturating_add(u64::from(first))
            .saturating_add(u64::from(candidate.owner_rewrite.is_some()));
        let owner_rewrite_output_bytes = candidate
            .owner_rewrite
            .as_ref()
            .map_or(0, |value| measured_row(&candidate_key, Some(value)));
        let fits = self.input_bytes.saturating_add(candidate_input_bytes)
            <= self.limits.max_input_bytes().get()
            && self
                .output_operations
                .saturating_add(admitted_output_operations)
                <= self.limits.max_output_operations().get()
            && self
                .output_bytes
                .saturating_add(root_output_bytes)
                .saturating_add(member_output_bytes)
                .saturating_add(owner_rewrite_output_bytes)
                <= self.limits.max_output_bytes().get();
        if !fits {
            return Ok(if first {
                BatchAdmission::Indivisible
            } else {
                BatchAdmission::Full
            });
        }
        self.input_bytes = self
            .input_bytes
            .checked_add(candidate_input_bytes)
            .ok_or_else(|| invariant("text GC batch input counter overflowed"))?;
        self.output_operations = self
            .output_operations
            .checked_add(admitted_output_operations)
            .ok_or_else(|| invariant("text GC batch output operation count overflowed"))?;
        self.output_bytes = self
            .output_bytes
            .checked_add(root_output_bytes)
            .and_then(|bytes| bytes.checked_add(member_output_bytes))
            .and_then(|bytes| bytes.checked_add(owner_rewrite_output_bytes))
            .ok_or_else(|| invariant("text GC batch output counter overflowed"))?;
        self.members.push((
            member_key,
            member_value,
            IndexCursor::try_new(candidate_key).map_err(operation_error)?,
            candidate.owner_rewrite,
        ));
        Ok(BatchAdmission::Admitted)
    }

    fn stage(
        mut self,
        transaction: &DbTransaction,
        counters: OperationCounters,
    ) -> Result<(IndexCursor, OperationCounters)> {
        let candidate_count = u32::try_from(self.members.len())
            .map_err(|_| invariant("text GC batch exceeds u32 members"))?;
        self.root = work::BlobGcRunRootValue::try_new(
            self.root.run_id,
            self.root.owner,
            self.root.revision,
            self.root.attempt,
            self.root.not_before_unix_millis,
            self.root.phase,
            candidate_count,
        )
        .map_err(|error| invariant(error.to_string()))?;
        let root_value =
            index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcEntry(
                work::BlobGcEntryValue::RunRoot(self.root),
            ));
        transaction.put(self.root_key, root_value)?;
        for (member_key, member_value, candidate_cursor, owner_rewrite) in &self.members {
            transaction.put(member_key, member_value)?;
            if let Some(owner_rewrite) = owner_rewrite {
                transaction.put(candidate_cursor.as_bytes(), owner_rewrite)?;
            }
        }
        let cursor = self
            .members
            .last()
            .map(|(_, _, cursor, _)| cursor.clone())
            .ok_or_else(|| invariant("text GC batch cannot stage without a member"))?;
        Ok((
            cursor,
            add_counters(
                counters,
                self.members.len(),
                self.input_bytes,
                self.output_operations,
                self.output_bytes,
            )?,
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchAdmission {
    Admitted,
    Full,
    Indivisible,
}

/// Point-reads and cross-checks one exact global root.
async fn load_root(db: &Db, run_id: BlobGcRunId) -> Result<Option<work::BlobGcRunRootValue>> {
    let key = index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes();
    let Some(value) = db.get(key).await? else {
        return Ok(None);
    };
    let root = decode_root(&value)?;
    if root.run_id != run_id {
        return Err(corruption("blob-GC root key and value disagree"));
    }
    Ok(Some(root))
}

/// Reads the next immutable member strictly after the retained full-key cursor.
async fn next_member(
    db: &Db,
    run_id: BlobGcRunId,
    cursor: Option<&IndexCursor>,
) -> Result<Option<(Bytes, work::BlobGcCandidateMemberValue)>> {
    let prefix = index_keys::GlobalIndexV2Key::blob_gc_candidate_member_prefix(run_id);
    let start = match cursor {
        Some(cursor) => {
            let Some(suffix) = cursor.as_bytes().strip_prefix(prefix.as_ref()) else {
                return Err(corruption(
                    "blob-GC member cursor is outside its exact run prefix",
                ));
            };
            Bound::Excluded(Bytes::copy_from_slice(suffix))
        }
        None => Bound::Unbounded,
    };
    let mut rows = db.scan_prefix(&prefix, (start, Bound::Unbounded)).await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let member = decode_member(run_id, &row.key, &row.value)?;
    Ok(Some((row.key, member)))
}

/// Re-enumerates the original member set before authorizing `FencesClosed`.
async fn confirm_all_fences(
    coordinator: &dyn blob_publication::BlobPublicationCoordinator,
    db: &Db,
    root: &work::BlobGcRunRootValue,
    member_cursor: Option<&IndexCursor>,
) -> Result<FenceObservation> {
    let (observation, last_member_key) = observe_all_fences(coordinator, db, root).await?;
    if observation == FenceObservation::Retry {
        return Ok(observation);
    }
    if member_cursor.map(IndexCursor::as_bytes) != last_member_key.as_ref() {
        return Err(corruption(
            "blob-GC fence cursor is not the exact final immutable member",
        ));
    }
    Ok(FenceObservation::Quiescent)
}

/// Reacquires and revalidates every immutable same-run fence after recovery.
///
/// This external preparation runs before any fenced owner-retirement commit.
/// The member set is bounded by the configured generation batch size and is
/// immutable once the root exists.
pub(super) async fn revalidate_all_fences(
    coordinator: &dyn blob_publication::BlobPublicationCoordinator,
    db: &Db,
    root: &work::BlobGcRunRootValue,
) -> Result<bool> {
    if !matches!(
        root.phase,
        work::BlobGcPhase::FencesClosed
            | work::BlobGcPhase::FirstPass { .. }
            | work::BlobGcPhase::SecondPass { .. }
    ) {
        return Err(corruption(
            "blob-GC fence recovery requires owner retirement or a reachability pass",
        ));
    }
    Ok(observe_all_fences(coordinator, db, root).await?.0 == FenceObservation::Quiescent)
}

async fn observe_all_fences(
    coordinator: &dyn blob_publication::BlobPublicationCoordinator,
    db: &Db,
    root: &work::BlobGcRunRootValue,
) -> Result<(FenceObservation, Option<Bytes>)> {
    let prefix = index_keys::GlobalIndexV2Key::blob_gc_candidate_member_prefix(root.run_id);
    let mut rows = db
        .scan_prefix(&prefix, (Bound::Unbounded, Bound::<Bytes>::Unbounded))
        .await?;
    let expected_count = usize::try_from(root.candidate_count.get())
        .map_err(|_| corruption("blob-GC candidate count does not fit this architecture"))?;
    let mut observed_count = 0_usize;
    let mut last_member_key = None;
    while let Some(row) = rows.next().await? {
        observed_count = observed_count
            .checked_add(1)
            .ok_or_else(|| corruption("blob-GC member count overflowed"))?;
        if observed_count > expected_count {
            return Err(corruption(
                "blob-GC root has more members than its immutable count",
            ));
        }
        let member = decode_member(root.run_id, &row.key, &row.value)?;
        if observe_delete_fence(coordinator, member.blob, root.run_id).await?
            == FenceObservation::Retry
        {
            return Ok((FenceObservation::Retry, None));
        }
        last_member_key = Some(row.key);
    }
    if observed_count != expected_count {
        return Err(corruption(
            "blob-GC root member set does not match its immutable count",
        ));
    }
    let Some(last_member_key) = last_member_key else {
        return Err(corruption("blob-GC root has no immutable member"));
    };
    Ok((FenceObservation::Quiescent, Some(last_member_key)))
}

/// Acquires or revalidates one same-run fence and reports durable retry needs.
async fn observe_delete_fence(
    coordinator: &dyn blob_publication::BlobPublicationCoordinator,
    blob: work::BlobRef,
    run_id: BlobGcRunId,
) -> Result<FenceObservation> {
    let key = blob_publication::BlobDeleteFenceKey::new(blob, run_id);
    let fence = match coordinator.begin_delete(key).await {
        Ok(blob_publication::BeginBlobDelete::Acquired(fence))
        | Ok(blob_publication::BeginBlobDelete::AlreadyHeldSameRun(fence)) => fence,
        Ok(blob_publication::BeginBlobDelete::BusyOtherRun) => {
            return Ok(FenceObservation::Retry);
        }
        Err(error)
            if matches!(
                &error,
                blob_publication::BlobPublicationError::PublicationOutcomeAmbiguous(_)
                    | blob_publication::BlobPublicationError::ObjectStore(_)
                    | blob_publication::BlobPublicationError::CoordinatorUnavailable(_)
            ) =>
        {
            return Ok(FenceObservation::Retry);
        }
        Err(error) => return Err(error.into()),
    };
    match coordinator.check_quiescent(&fence).await {
        Ok(true) => Ok(FenceObservation::Quiescent),
        Ok(false) => Ok(FenceObservation::Retry),
        Err(error)
            if matches!(
                &error,
                blob_publication::BlobPublicationError::PublicationOutcomeAmbiguous(_)
                    | blob_publication::BlobPublicationError::ObjectStore(_)
                    | blob_publication::BlobPublicationError::CoordinatorUnavailable(_)
            ) =>
        {
            Ok(FenceObservation::Retry)
        }
        Err(error) => Err(error.into()),
    }
}

/// Decodes one member row and verifies its key, run, blob, and phase identity.
fn decode_member(
    run_id: BlobGcRunId,
    row_key: &[u8],
    row_value: &[u8],
) -> Result<work::BlobGcCandidateMemberValue> {
    let index_keys::GlobalIndexV2Key::BlobGcCandidateMember {
        run_id: key_run_id,
        blob_hash,
    } = index_keys::GlobalIndexV2Key::parse_from_slice(row_key)?
    else {
        return Err(corruption("blob-GC member lane yielded another key kind"));
    };
    let index_values::IndexV2WorkValue::BlobGcEntry(work::BlobGcEntryValue::CandidateMember(
        member,
    )) = index_values::decode_work_value(row_value)?
    else {
        return Err(corruption("blob-GC member key contains another value kind"));
    };
    if key_run_id != run_id || member.run_id != run_id || member.blob.hash() != blob_hash.as_bytes()
    {
        return Err(corruption("blob-GC member key and value disagree"));
    }
    Ok(member)
}

fn encode_member(member: &work::BlobGcCandidateMemberValue) -> Bytes {
    index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcEntry(
        work::BlobGcEntryValue::CandidateMember(member.clone()),
    ))
}

/// Persists checked exponential backoff without losing root discoverability.
async fn schedule_retry(
    db: &Db,
    root: &work::BlobGcRunRootValue,
    now_unix_millis: u64,
) -> Result<u64> {
    let attempt = root
        .attempt
        .checked_add(1)
        .ok_or(HelixDbError::IdentifierExhausted("blob GC retry attempt"))?;
    let exponent = attempt.min(10);
    let delay_millis = 10_u64.checked_shl(exponent).unwrap_or(10_240).min(10_240);
    let not_before = now_unix_millis
        .checked_add(delay_millis)
        .ok_or(HelixDbError::IdentifierExhausted("blob GC retry deadline"))?;
    replace_root(db, root, root.phase.clone(), attempt, Some(not_before)).await?;
    Ok(delay_millis)
}

/// Replaces one exact root revision through a serializable compare-and-swap.
async fn replace_root(
    db: &Db,
    source: &work::BlobGcRunRootValue,
    phase: work::BlobGcPhase,
    attempt: u32,
    not_before_unix_millis: Option<u64>,
) -> Result<()> {
    let key = index_keys::GlobalIndexV2Key::BlobGcRunRoot(source.run_id).to_bytes();
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    require_exact_root(&transaction, &key, source).await?;
    let next = next_root_value(source, phase, attempt, not_before_unix_millis)?;
    transaction.put(key, encode_root(&next))?;
    transaction.commit().await?;
    Ok(())
}

fn next_root_value(
    source: &work::BlobGcRunRootValue,
    phase: work::BlobGcPhase,
    attempt: u32,
    not_before_unix_millis: Option<u64>,
) -> Result<work::BlobGcRunRootValue> {
    work::BlobGcRunRootValue::try_new(
        source.run_id,
        source.owner,
        source
            .revision
            .checked_next()
            .map_err(|error| invariant(error.to_string()))?,
        attempt,
        not_before_unix_millis,
        phase,
        source.candidate_count.get(),
    )
    .map_err(|error| invariant(error.to_string()))
}

async fn unused_run_id(transaction: &DbTransaction) -> Result<(BlobGcRunId, Bytes, u64)> {
    unused_run_id_from(transaction, BlobGcRunId::new_v4).await
}

/// Selects one absent global run ID from a bounded candidate source.
///
/// Keeping candidate generation outside the collision loop makes the fail-closed
/// exhaustion contract deterministic under test. Production supplies UUIDv4
/// candidates through [`unused_run_id`]; no generated ID is accepted until its
/// canonical root key has been observed absent in the caller's transaction.
async fn unused_run_id_from(
    transaction: &DbTransaction,
    mut next_run_id: impl FnMut() -> BlobGcRunId,
) -> Result<(BlobGcRunId, Bytes, u64)> {
    let mut input_bytes = 0_u64;
    for _ in 0..RUN_ID_COLLISION_ATTEMPTS {
        let run_id = next_run_id();
        let key = index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes();
        let observed = transaction.get(&key).await?;
        input_bytes = input_bytes
            .checked_add(measured_row(&key, observed.as_ref()))
            .ok_or_else(|| invariant("blob GC run collision input counter overflowed"))?;
        if observed.is_none() {
            return Ok((run_id, key, input_bytes));
        }
    }
    Err(HelixDbError::IdentifierExhausted(
        "blob GC run ID collision budget",
    ))
}

/// Atomically assigns one claimed intent to a singleton upload-reclaim root.
///
/// The caller writes the returned next intent revision and its matching upload
/// pointer in the same transaction. Root, member, candidate, intent, and
/// pointer therefore become mutually visible as one outbox transition; a
/// restart can discover the run from either the upload pointer or root lane.
/// The same transaction point-reads the canonical record. `Aborting` and
/// `Dropping` return generation-cleanup ownership without staging a root, while
/// a concurrent transition conflicts with a stale `Building`/`Active` read.
pub(super) async fn stage_upload_reclaim_root(
    transaction: &DbTransaction,
    scope: DataScope,
    intent: &work::TextUploadIntentValue,
) -> Result<UploadReclaimRootTransition> {
    if !matches!(
        intent.phase,
        work::TextUploadPhase::Reclaimable(work::ReclaimAssignment::Unassigned)
    ) || !matches!(intent.work_state, work::TextUploadWorkState::Claimed(_))
    {
        return Err(corruption(
            "upload-reclaim root creation requires one claimed unassigned intent",
        ));
    }
    let rows = super::upload::upload_anchor_rows(scope, intent)?;
    if transaction.get(&rows.reachability_key).await?.is_some() {
        return Err(corruption(
            "upload-reclaim root creation found a live intent reachability row",
        ));
    }
    let (candidate_key, candidate_value) = disposition_intent_candidate(scope, intent);
    if transaction.get(&candidate_key).await?.as_deref() != Some(candidate_value.as_ref()) {
        return Err(corruption(
            "upload-reclaim root creation is missing its exact scoped candidate",
        ));
    }
    let record_key = Key::Data {
        scope,
        kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::index_record(
            intent.identity.clone(),
        )),
    }
    .to_bytes();
    let Some(record_value) = transaction.get(&record_key).await? else {
        return Err(corruption(
            "upload-reclaim root creation is missing its canonical index record",
        ));
    };
    let record = index_values::decode_index_record(&record_value)?;
    if record.index_id() != intent.index_id
        || record.identity() != &intent.identity
        || record.state().generation() != intent.generation
    {
        return Err(corruption(
            "upload-reclaim root creation and canonical index generation disagree",
        ));
    }
    match record.state() {
        IndexStateV2::Building { .. } | IndexStateV2::Active { .. } => {}
        IndexStateV2::Aborting { .. } | IndexStateV2::Dropping { .. } => {
            return Ok(UploadReclaimRootTransition::GenerationCleanupOwnsAssignment);
        }
        IndexStateV2::Dropped { .. } => {
            return Err(corruption(
                "dropped text generation retained an upload-reclaim intent",
            ));
        }
    }
    let (run_id, root_key, _) = unused_run_id(transaction).await?;
    let root = work::BlobGcRunRootValue::try_new(
        run_id,
        work::BlobGcRunOwner::UploadReclaim {
            scope,
            intent_id: intent.intent_id,
            index_id: intent.index_id,
            generation: intent.generation,
        },
        BlobGcRunRevision::initial(),
        0,
        None,
        work::BlobGcPhase::AwaitDeleteFences {
            member_cursor: None,
        },
        1,
    )
    .map_err(|error| invariant(error.to_string()))?;
    let member = work::BlobGcCandidateMemberValue {
        run_id,
        blob: intent.blob,
        state: work::BlobGcMemberState::PendingDisposition { owner_cursor: None },
    };
    let member_key = index_keys::GlobalIndexV2Key::BlobGcCandidateMember {
        run_id,
        blob_hash: index_keys::BlobHash::new(*intent.blob.hash()),
    }
    .to_bytes();
    transaction.put(root_key, encode_root(&root))?;
    transaction.put(member_key, encode_member(&member))?;
    Ok(UploadReclaimRootTransition::Assigned(run_id))
}

/// Advances one exactly assigned upload root from closed fences to pass one.
///
/// `None` means the independently runnable root has not reached
/// `FencesClosed`, or has already advanced, so the upload claim must be
/// requeued without changing the durable assignment.
pub(super) async fn stage_upload_reclaim_first_pass(
    transaction: &DbTransaction,
    scope: DataScope,
    intent: &work::TextUploadIntentValue,
) -> Result<Option<work::TextUploadIntentValue>> {
    let work::TextUploadPhase::Reclaimable(work::ReclaimAssignment::Assigned(run_id)) =
        intent.phase
    else {
        return Err(corruption(
            "upload-reclaim owner normalization requires an assigned intent",
        ));
    };
    let work::TextUploadWorkState::Claimed(claim) = intent.work_state else {
        return Err(corruption(
            "upload-reclaim owner normalization requires a claimed intent",
        ));
    };
    let root_key = index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes();
    let Some(root_value) = transaction.get(&root_key).await? else {
        return Err(corruption(
            "assigned upload-reclaim intent names a missing root",
        ));
    };
    let root = decode_root(&root_value)?;
    if root.owner
        != (work::BlobGcRunOwner::UploadReclaim {
            scope,
            intent_id: intent.intent_id,
            index_id: intent.index_id,
            generation: intent.generation,
        })
        || root.candidate_count.get() != 1
    {
        return Err(corruption(
            "assigned upload-reclaim intent and root ownership disagree",
        ));
    }
    if !matches!(root.phase, work::BlobGcPhase::FencesClosed) {
        return Ok(None);
    }
    let member_key = index_keys::GlobalIndexV2Key::BlobGcCandidateMember {
        run_id,
        blob_hash: index_keys::BlobHash::new(*intent.blob.hash()),
    }
    .to_bytes();
    let Some(member_value) = transaction.get(&member_key).await? else {
        return Err(corruption(
            "upload-reclaim root is missing its singleton member",
        ));
    };
    let member = decode_member(run_id, &member_key, &member_value)?;
    if member.blob != intent.blob
        || !matches!(
            member.state,
            work::BlobGcMemberState::PendingDisposition { owner_cursor: None }
        )
    {
        return Err(corruption(
            "upload-reclaim singleton member disagrees with its assigned intent",
        ));
    }
    let rows = super::upload::upload_anchor_rows(scope, intent)?;
    if transaction.get(&rows.reachability_key).await?.is_some() {
        return Err(corruption(
            "upload-reclaim assigned intent unexpectedly regained reachability",
        ));
    }
    let (candidate_key, candidate_value) = disposition_intent_candidate(scope, intent);
    if transaction.get(&candidate_key).await?.as_deref() != Some(candidate_value.as_ref()) {
        return Err(corruption(
            "upload-reclaim assigned intent lost its scoped candidate",
        ));
    }
    let next_root = next_root_value(
        &root,
        work::BlobGcPhase::FirstPass {
            writer_epoch: claim.writer_epoch,
            first_attempt: work::GcScanAttempt::new(1)
                .map_err(|error| invariant(error.to_string()))?,
            reference_cursor: None,
        },
        0,
        None,
    )?;
    transaction.put(root_key, encode_root(&next_root))?;
    intent
        .complete_reclaim_owner_normalization(run_id)
        .map(Some)
        .map_err(|error| invariant(error.to_string()))
}

/// Point-reads and validates the exact generation-owned root named by an operation.
///
/// Callers must additionally require the phase appropriate to their closed
/// operation stage. Returning the typed root keeps phase matching exhaustive
/// instead of reducing it to an unstructured boolean.
pub(super) async fn load_generation_root(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    run_id: BlobGcRunId,
) -> Result<GenerationRootObservation> {
    let key = index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes();
    let Some(value) = transaction.get(&key).await? else {
        return Err(corruption(
            "text cleanup operation points to a missing blob-GC root",
        ));
    };
    let root = decode_root(&value)?;
    if root.run_id != run_id
        || root.owner
            != (work::BlobGcRunOwner::GenerationCleanup {
                scope,
                operation_id: operation.operation_id(),
                index_id: operation.index_id(),
                generation: operation.generation(),
            })
    {
        return Err(corruption(
            "text cleanup operation and blob-GC root disagree",
        ));
    }
    Ok(GenerationRootObservation {
        root,
        input_bytes: measured_row(&key, Some(&value)),
    })
}

/// Validated generation root plus the exact storage input charged by its read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct GenerationRootObservation {
    pub(super) root: work::BlobGcRunRootValue,
    pub(super) input_bytes: u64,
}

/// Stages the only legal atomic handoff from an empty generation-owned root.
///
/// The owning operation and its global pointer are updated by the caller in
/// the same transaction. Until this function deletes the root, the operation
/// therefore continues to name a durable retry anchor. The `stage_cursor`
/// pages through the global root lane to prove that no upload-reclaim owner for
/// the drained generation remains without an unbounded transaction.
pub(super) async fn stage_generation_terminal_handoff(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &GcProgress,
    limits: SearchIndexBatchLimits,
) -> Result<GenerationTerminalTransition> {
    let Some(run_id) = progress.gc_run_id else {
        return Err(corruption(
            "text blob handoff has no assigned generation root",
        ));
    };
    let Some(candidate_cursor) = progress.candidate_cursor.as_ref() else {
        return Err(corruption(
            "text blob handoff lost its strict generation candidate boundary",
        ));
    };
    let observed = load_generation_root(transaction, scope, operation, run_id).await?;
    let work::BlobGcPhase::Delete {
        completed_first_attempt,
        completed_second_attempt,
        stale_mark_cleanup: work::StaleMarkCleanup::Complete,
        ..
    } = observed.root.phase
    else {
        return Ok(GenerationTerminalTransition::Waiting);
    };
    require_no_members(transaction, &observed.root).await?;
    let witness = validate_current_mark_witness(
        transaction,
        &observed.root,
        completed_first_attempt,
        completed_second_attempt,
    )
    .await?;
    let mut input_bytes = observed
        .input_bytes
        .checked_add(witness.input_bytes)
        .ok_or_else(|| corruption("blob-GC terminal handoff input bytes overflowed"))?;

    let generation_prefix = Key::data_prefix(
        scope,
        index_keys::IndexV2Key::generation_cleanup_candidate_prefix(
            operation.index_id(),
            operation.generation(),
        ),
    );
    let Some(candidate_suffix) = candidate_cursor
        .as_bytes()
        .strip_prefix(generation_prefix.as_ref())
    else {
        return Err(corruption(
            "text blob handoff candidate cursor escaped its generation lane",
        ));
    };
    let mut next_candidates = transaction
        .scan_prefix(
            &generation_prefix,
            (
                Bound::Excluded(Bytes::copy_from_slice(candidate_suffix)),
                Bound::Unbounded,
            ),
        )
        .await?;
    if let Some(row) = next_candidates.next().await? {
        decode_generation_candidate(scope, operation, &row.key, &row.value)?;
        input_bytes = input_bytes
            .checked_add(measured_row(&row.key, Some(&row.value)))
            .ok_or_else(|| corruption("blob-GC next-batch input bytes overflowed"))?;
        let Some((output_operations, output_bytes)) =
            stage_terminal_root_delete(transaction, &observed.root, witness, input_bytes, limits)?
        else {
            return Ok(GenerationTerminalTransition::Blocked(
                IndexOperationBlocker::InvariantViolation,
            ));
        };
        return Ok(GenerationTerminalTransition::NextBatch(GcProgress {
            gc_run_id: None,
            candidate_cursor: Some(candidate_cursor.clone()),
            stage_cursor: None,
            counters: add_counters(
                progress.counters,
                1,
                input_bytes,
                output_operations,
                output_bytes,
            )?,
        }));
    }

    let all_candidate_prefix = Key::data_prefix(
        scope,
        index_keys::IndexV2Key::generation_prefix(
            index_keys::IndexV2RecordKind::BlobGcCandidate,
            operation.index_id(),
            operation.generation(),
        ),
    );
    let mut remaining_candidates = transaction
        .scan_prefix(
            &all_candidate_prefix,
            (Bound::Unbounded, Bound::<Bytes>::Unbounded),
        )
        .await?;
    if let Some(row) = remaining_candidates.next().await? {
        let owner = decode_terminal_candidate(scope, operation, &row.key, &row.value)?;
        return match owner {
            work::BlobGcCandidateOwner::UploadIntent(_) => {
                Ok(GenerationTerminalTransition::Waiting)
            }
            work::BlobGcCandidateOwner::GenerationCleanup(_) => Err(corruption(
                "processed generation candidate remained behind the strict handoff cursor",
            )),
        };
    }

    let root_prefix =
        index_keys::GlobalIndexV2Key::logical_prefix(index_keys::GlobalIndexV2Kind::BlobGcRunRoot);
    let root_start = match progress.stage_cursor.as_ref() {
        Some(cursor) => {
            let Some(suffix) = cursor.as_bytes().strip_prefix(root_prefix.as_ref()) else {
                return Err(corruption(
                    "text blob handoff stage cursor escaped the global root lane",
                ));
            };
            let index_keys::GlobalIndexV2Key::BlobGcRunRoot(_) =
                index_keys::GlobalIndexV2Key::parse_from_slice(cursor.as_bytes())?
            else {
                return Err(corruption(
                    "text blob handoff stage cursor is not a complete root key",
                ));
            };
            Bound::Excluded(Bytes::copy_from_slice(suffix))
        }
        None => Bound::Unbounded,
    };
    let mut roots = transaction
        .scan_prefix(&root_prefix, (root_start, Bound::Unbounded))
        .await?;
    let mut visited = 0_usize;
    let mut last_root_key = None;
    while visited < limits.max_entities().get() {
        let Some(row) = roots.next().await? else {
            let Some((output_operations, output_bytes)) = stage_terminal_root_delete(
                transaction,
                &observed.root,
                witness,
                input_bytes,
                limits,
            )?
            else {
                return Ok(GenerationTerminalTransition::Blocked(
                    IndexOperationBlocker::InvariantViolation,
                ));
            };
            return Ok(GenerationTerminalTransition::Complete(PrefixScanProgress {
                cursor: None,
                counters: add_counters(
                    progress.counters,
                    visited,
                    input_bytes,
                    output_operations,
                    output_bytes,
                )?,
            }));
        };
        let row_bytes = measured_row(&row.key, Some(&row.value));
        if input_bytes.saturating_add(row_bytes) > limits.max_input_bytes().get() {
            let Some(last_root_key) = last_root_key else {
                return Ok(GenerationTerminalTransition::Blocked(
                    IndexOperationBlocker::InvariantViolation,
                ));
            };
            return Ok(GenerationTerminalTransition::Progressed(GcProgress {
                gc_run_id: progress.gc_run_id,
                candidate_cursor: progress.candidate_cursor.clone(),
                stage_cursor: Some(IndexCursor::try_new(last_root_key).map_err(operation_error)?),
                counters: add_counters(progress.counters, visited, input_bytes, 0, 0)?,
            }));
        }
        let index_keys::GlobalIndexV2Key::BlobGcRunRoot(row_run_id) =
            index_keys::GlobalIndexV2Key::parse_from_slice(&row.key)?
        else {
            return Err(corruption(
                "blob-GC terminal root scan yielded another key kind",
            ));
        };
        let row_root = decode_root(&row.value)?;
        if row_root.run_id != row_run_id {
            return Err(corruption(
                "blob-GC terminal root scan found key/value run disagreement",
            ));
        }
        input_bytes = input_bytes
            .checked_add(row_bytes)
            .ok_or_else(|| corruption("blob-GC terminal root scan bytes overflowed"))?;
        if row_run_id == run_id {
            if row_root != observed.root {
                return Err(corruption(
                    "blob-GC terminal root changed inside its handoff transaction",
                ));
            }
        } else if root_owns_generation(&row_root, scope, operation) {
            return match row_root.owner {
                work::BlobGcRunOwner::UploadReclaim { .. } => {
                    Ok(GenerationTerminalTransition::Waiting)
                }
                work::BlobGcRunOwner::GenerationCleanup { .. } => Err(corruption(
                    "text generation operation has a second live blob-GC root",
                )),
            };
        }
        visited = visited
            .checked_add(1)
            .ok_or_else(|| corruption("blob-GC terminal root visit count overflowed"))?;
        last_root_key = Some(row.key);
    }
    let Some(last_root_key) = last_root_key else {
        return Err(corruption(
            "positive blob-GC root page limit made no progress",
        ));
    };
    if input_bytes > limits.max_input_bytes().get() {
        return Ok(GenerationTerminalTransition::Blocked(
            IndexOperationBlocker::InvariantViolation,
        ));
    }
    Ok(GenerationTerminalTransition::Progressed(GcProgress {
        gc_run_id: progress.gc_run_id,
        candidate_cursor: progress.candidate_cursor.clone(),
        stage_cursor: Some(IndexCursor::try_new(last_root_key).map_err(operation_error)?),
        counters: add_counters(progress.counters, visited, input_bytes, 0, 0)?,
    }))
}

fn stage_terminal_root_delete(
    transaction: &DbTransaction,
    root: &work::BlobGcRunRootValue,
    witness: TerminalMarkWitness,
    input_bytes: u64,
    limits: SearchIndexBatchLimits,
) -> Result<Option<(u64, u64)>> {
    let root_key = index_keys::GlobalIndexV2Key::BlobGcRunRoot(root.run_id).to_bytes();
    let output_operations = u64::try_from(witness.keys.len())
        .map_err(|_| corruption("blob-GC terminal mark count exceeds u64"))?
        .checked_add(1)
        .ok_or_else(|| corruption("blob-GC terminal delete count overflowed"))?;
    let output_bytes = witness.keys.iter().try_fold(
        u64::try_from(root_key.len())
            .map_err(|_| corruption("blob-GC root key length exceeds u64"))?,
        |total, key| {
            total
                .checked_add(
                    u64::try_from(key.len())
                        .map_err(|_| corruption("blob-GC mark key length exceeds u64"))?,
                )
                .ok_or_else(|| corruption("blob-GC terminal output bytes overflowed"))
        },
    )?;
    if input_bytes > limits.max_input_bytes().get()
        || output_operations > limits.max_output_operations().get()
        || output_bytes > limits.max_output_bytes().get()
    {
        return Ok(None);
    }
    for key in witness.keys {
        transaction.delete(key)?;
    }
    transaction.delete(root_key)?;
    Ok(Some((output_operations, output_bytes)))
}

fn root_owns_generation(
    root: &work::BlobGcRunRootValue,
    scope: DataScope,
    operation: &IndexOperationRecord,
) -> bool {
    match root.owner {
        work::BlobGcRunOwner::GenerationCleanup {
            scope: owner_scope,
            index_id,
            generation,
            ..
        }
        | work::BlobGcRunOwner::UploadReclaim {
            scope: owner_scope,
            index_id,
            generation,
            ..
        } => {
            owner_scope == scope
                && index_id == operation.index_id()
                && generation == operation.generation()
        }
    }
}

fn decode_terminal_candidate(
    scope: DataScope,
    operation: &IndexOperationRecord,
    key: &[u8],
    value: &[u8],
) -> Result<work::BlobGcCandidateOwner> {
    let Key::Data {
        scope: key_scope,
        kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::BlobGcCandidate(candidate_key)),
    } = Key::parse_from_slice(scope, key)?
    else {
        return Err(corruption(
            "text terminal candidate scan yielded another key kind",
        ));
    };
    let index_values::IndexV2WorkValue::BlobGcCandidate(candidate) =
        index_values::decode_work_value(value)?
    else {
        return Err(corruption(
            "text terminal candidate key contains another value kind",
        ));
    };
    let owner_matches = matches!(
        (candidate_key.owner, candidate.owner),
        (
            index_keys::BlobGcCandidateKeyOwner::GenerationCleanup,
            work::BlobGcCandidateOwner::GenerationCleanup(operation_id),
        ) if operation_id == operation.operation_id()
    ) || matches!(
        (candidate_key.owner, candidate.owner),
        (
            index_keys::BlobGcCandidateKeyOwner::UploadIntent(key_intent),
            work::BlobGcCandidateOwner::UploadIntent(value_intent),
        ) if key_intent == value_intent
    );
    if key_scope != scope
        || candidate_key.index_id != operation.index_id()
        || candidate_key.generation != operation.generation()
        || candidate_key.blob_hash.as_bytes() != candidate.blob.hash()
        || candidate.index_id != operation.index_id()
        || candidate.generation != operation.generation()
        || !owner_matches
    {
        return Err(corruption(
            "text terminal candidate key and value ownership disagree",
        ));
    }
    Ok(candidate.owner)
}

/// Exact membership observation used while retiring generation-local owners.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GenerationMemberObservation {
    /// The hash belongs to another immutable batch and its owner remains live.
    Absent { input_bytes: u64 },
    /// The hash belongs to this run and still awaits its final disposition.
    Pending { input_bytes: u64 },
}

impl GenerationMemberObservation {
    /// Returns the measured point-read bytes, including the absent lookup key.
    pub(super) const fn input_bytes(self) -> u64 {
        match self {
            Self::Absent { input_bytes } | Self::Pending { input_bytes } => input_bytes,
        }
    }
}

/// Observes whether one exact blob is a pending member of the immutable run.
///
/// A present row is cross-checked against the complete blob metadata. A
/// terminal member during owner retirement is corruption because disposition
/// cannot begin until every current-run local owner has been normalized.
pub(super) async fn observe_generation_member(
    transaction: &DbTransaction,
    run_id: BlobGcRunId,
    blob: work::BlobRef,
) -> Result<GenerationMemberObservation> {
    let key = index_keys::GlobalIndexV2Key::BlobGcCandidateMember {
        run_id,
        blob_hash: index_keys::BlobHash::new(*blob.hash()),
    }
    .to_bytes();
    let observed = transaction.get(&key).await?;
    let input_bytes = measured_row(&key, observed.as_ref());
    let Some(value) = observed else {
        return Ok(GenerationMemberObservation::Absent { input_bytes });
    };
    let member = decode_member(run_id, &key, &value)?;
    if member.blob != blob {
        return Err(corruption(
            "blob-GC member and generation owner disagree on blob metadata",
        ));
    }
    Ok(GenerationMemberObservation::Pending { input_bytes })
}

async fn validate_upload_reclaim_root(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    intent: &work::TextUploadIntentValue,
    run_id: BlobGcRunId,
) -> Result<()> {
    let key = index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes();
    let Some(value) = transaction.get(&key).await? else {
        return Err(corruption(
            "assigned text reclaim intent names a missing blob-GC root",
        ));
    };
    let root = decode_root(&value)?;
    if root.run_id != run_id
        || root.owner
            != (work::BlobGcRunOwner::UploadReclaim {
                scope,
                intent_id: intent.intent_id,
                index_id: operation.index_id(),
                generation: operation.generation(),
            })
    {
        return Err(corruption(
            "assigned text reclaim intent and blob-GC root disagree",
        ));
    }
    Ok(())
}

fn decode_root(value: &[u8]) -> Result<work::BlobGcRunRootValue> {
    let index_values::IndexV2WorkValue::BlobGcEntry(work::BlobGcEntryValue::RunRoot(root)) =
        index_values::decode_work_value(value)?
    else {
        return Err(corruption("blob-GC root key contains another value kind"));
    };
    Ok(root)
}

fn encode_root(root: &work::BlobGcRunRootValue) -> Bytes {
    index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcEntry(
        work::BlobGcEntryValue::RunRoot(root.clone()),
    ))
}

fn decode_intent(
    scope: DataScope,
    operation: &IndexOperationRecord,
    key: &[u8],
    value: &[u8],
) -> Result<work::TextUploadIntentValue> {
    let Key::Data {
        kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::TextUploadIntent(key)),
        ..
    } = Key::parse_from_slice(scope, key)?
    else {
        return Err(corruption(
            "text reclaim-barrier scan yielded another key kind",
        ));
    };
    let index_values::IndexV2WorkValue::TextUploadIntent(intent) =
        index_values::decode_work_value(value)?
    else {
        return Err(corruption(
            "text upload intent key contains another work value kind",
        ));
    };
    let intent = *intent;
    if key.index_id != operation.index_id()
        || key.generation != operation.generation()
        || key.intent_id != intent.intent_id
        || intent.index_id != operation.index_id()
        || intent.generation != operation.generation()
        || intent.identity != *operation.identity()
    {
        return Err(corruption(
            "text reclaim-barrier intent key/value ownership mismatch",
        ));
    }
    Ok(intent)
}

fn decode_generation_candidate(
    scope: DataScope,
    operation: &IndexOperationRecord,
    key: &[u8],
    value: &[u8],
) -> Result<SelectedGenerationCandidate> {
    let Key::Data {
        scope: key_scope,
        kind:
            DataKeyKind::IndexV2(index_keys::IndexV2Key::BlobGcCandidate(
                index_keys::BlobGcCandidateKey {
                    index_id,
                    generation,
                    owner: index_keys::BlobGcCandidateKeyOwner::GenerationCleanup,
                    blob_hash,
                },
            )),
    } = Key::parse_from_slice(scope, key)?
    else {
        return Err(corruption(
            "text generation-candidate lane yielded another key kind",
        ));
    };
    let index_values::IndexV2WorkValue::BlobGcCandidate(candidate) =
        index_values::decode_work_value(value)?
    else {
        return Err(corruption(
            "text generation-candidate key contains another value kind",
        ));
    };
    if key_scope != scope
        || index_id != operation.index_id()
        || generation != operation.generation()
        || blob_hash.as_bytes() != candidate.blob.hash()
        || candidate.index_id != operation.index_id()
        || candidate.generation != operation.generation()
        || !matches!(
            candidate.owner,
            work::BlobGcCandidateOwner::GenerationCleanup(_)
        )
    {
        return Err(corruption(
            "text generation-candidate key/value ownership mismatch",
        ));
    }
    let owner_rewrite = if candidate.owner
        == work::BlobGcCandidateOwner::GenerationCleanup(operation.operation_id())
    {
        None
    } else {
        Some(index_values::encode_work_value(
            &index_values::IndexV2WorkValue::BlobGcCandidate(work::BlobGcCandidateValue {
                owner: work::BlobGcCandidateOwner::GenerationCleanup(operation.operation_id()),
                ..candidate
            }),
        ))
    };
    Ok(SelectedGenerationCandidate {
        value: candidate,
        owner_rewrite,
    })
}

fn scoped_key(scope: DataScope, key: index_keys::IndexV2Key) -> Bytes {
    Key::Data {
        scope,
        kind: DataKeyKind::IndexV2(key),
    }
    .to_bytes()
}

fn measured_row<V: AsRef<[u8]> + ?Sized>(key: &[u8], value: Option<&V>) -> u64 {
    u64::try_from(
        key.len()
            .saturating_add(value.map_or(0, |value| value.as_ref().len())),
    )
    .unwrap_or(u64::MAX)
}

fn add_counters(
    base: OperationCounters,
    entities: usize,
    input_bytes: u64,
    output_operations: u64,
    output_bytes: u64,
) -> Result<OperationCounters> {
    Ok(OperationCounters {
        entities: base
            .entities
            .checked_add(
                u64::try_from(entities)
                    .map_err(|_| invariant("text GC entity count exceeds u64"))?,
            )
            .ok_or_else(|| invariant("text GC cumulative entity count overflowed"))?,
        input_bytes: base
            .input_bytes
            .checked_add(input_bytes)
            .ok_or_else(|| invariant("text GC cumulative input bytes overflowed"))?,
        output_operations: base
            .output_operations
            .checked_add(output_operations)
            .ok_or_else(|| invariant("text GC cumulative operation count overflowed"))?,
        output_bytes: base
            .output_bytes
            .checked_add(output_bytes)
            .ok_or_else(|| invariant("text GC cumulative output bytes overflowed"))?,
    })
}

fn operation_error(error: crate::index_v2::IndexOperationModelError) -> HelixDbError {
    HelixDbError::InvariantViolation(error.to_string())
}

fn invariant(reason: impl Into<String>) -> HelixDbError {
    HelixDbError::InvariantViolation(reason.into())
}

fn corruption(reason: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(reason.into())
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::sync::Arc;

    use sha2::{Digest, Sha256};
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::{ObjectStoreExt, PutPayload};
    use slatedb::{Db, IsolationLevel};

    use super::*;
    use crate::config::TextIndexDefinition;
    use crate::index_v2::blob_publication::{
        BeginBlobDelete, BlobDeleteFenceKey, BlobPublicationCoordinator, BlobPublicationTiming,
        ProcessLocalBlobPublicationCoordinator,
    };
    use crate::index_v2::{
        BlobPublicationPermitId, ClaimSequence, IndexComponent, IndexElementKind,
        IndexGenerationId, IndexId, IndexIdentity, IndexIdentityFamily,
        IndexOperationExecutionState, IndexOperationFamily, IndexOperationId, IndexOperationKind,
        IndexOperationProgress, IndexOperationRevision, IndexRecordV2, IndexRevision,
        IndexStateTransition, OperationClaim, PhysicalGeneration, TextCleanupProgress,
        TextIntentRevision, TextUploadIntentId, ValidatedDynamicIndexDefinition,
    };

    /// Opens one isolated database for immutable GC-batch contracts.
    async fn test_db(name: &str) -> Db {
        Db::open(name, Arc::new(InMemory::new()))
            .await
            .expect("blob-GC contract database opens")
    }

    /// Returns one valid text drop operation owning every local candidate.
    fn operation() -> IndexOperationRecord {
        IndexOperationRecord::try_new(
            IndexOperationId::from_bytes([1; 16]).expect("operation ID is non-nil"),
            IndexId::initial(),
            IndexIdentity::new(
                IndexIdentityFamily::Text,
                IndexElementKind::Node,
                IndexComponent::try_new("label", "Document").expect("label validates"),
                IndexComponent::try_new("property", "body").expect("property validates"),
            ),
            IndexGenerationId::initial(),
            IndexRevision::initial(),
            IndexOperationRevision::initial(),
            IndexOperationKind::Drop,
            IndexOperationFamily::Text,
            IndexOperationProgress::TextCleanup(TextCleanupProgress::AcquireDeleteFences(
                GcProgress {
                    gc_run_id: None,
                    candidate_cursor: None,
                    stage_cursor: None,
                    counters: OperationCounters::default(),
                },
            )),
            0,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .expect("text drop operation validates")
    }

    /// Constructs the canonical Building record for the test operation's generation.
    fn building_record(operation: &IndexOperationRecord) -> IndexRecordV2 {
        let definition = ValidatedDynamicIndexDefinition::try_from(
            TextIndexDefinition::new_node("Document", "body").expect("text definition validates"),
        )
        .expect("dynamic text definition validates");
        IndexRecordV2::building(
            operation.index_id(),
            definition,
            IndexRevision::initial(),
            PhysicalGeneration::Text {
                generation: operation.generation(),
            },
            operation.operation_id(),
        )
        .expect("Building text record validates")
    }

    /// Writes one exact canonical record under its identity-derived scoped key.
    async fn put_record(db: &Db, scope: DataScope, record: &IndexRecordV2) {
        db.put(
            scoped_key(
                scope,
                index_keys::IndexV2Key::index_record(record.identity().clone()),
            ),
            index_values::encode_index_record(record),
        )
        .await
        .expect("canonical record is written");
    }

    /// Constructs one claimed, unassigned reclaim intent for singleton-root tests.
    fn claimed_reclaim_intent(
        operation: &IndexOperationRecord,
        seed: u8,
    ) -> work::TextUploadIntentValue {
        let permit_seed = seed
            .checked_add(1)
            .expect("test permit seed does not overflow");
        let epoch_seed = seed
            .checked_add(2)
            .expect("test epoch seed does not overflow");
        let blob = work::BlobRef::new([seed; 32], 64);
        work::TextUploadIntentValue::try_new(
            TextUploadIntentId::from_bytes([seed; 16]).expect("intent ID is non-nil"),
            TextIntentRevision::initial(),
            operation.index_id(),
            operation.identity().clone(),
            operation.generation(),
            work::TextPartition::Unpartitioned,
            blob,
            BlobPublicationPermitId::from_bytes([permit_seed; 16]).expect("permit ID is non-nil"),
            work::TextUploadOwner::Build {
                operation_id: operation.operation_id(),
                expected_operation_revision: operation.operation_revision(),
            },
            work::TextUploadAttachment::BuildArtifact {
                artifact_ordinal: 0,
                split: work::SplitRef::try_new(blob, 0, 0, 0, blob.size())
                    .expect("test split validates"),
            },
            work::TextUploadPhase::Reclaimable(work::ReclaimAssignment::Unassigned),
            0,
            work::TextUploadWorkState::Claimed(OperationClaim {
                writer_epoch: WriterEpoch::from_bytes([epoch_seed; 16])
                    .expect("writer epoch is non-nil"),
                sequence: ClaimSequence::new(1).expect("claim sequence is non-zero"),
            }),
        )
        .expect("claimed reclaim intent validates")
    }

    /// Constructs positive transaction limits for one GC batch.
    fn limits(
        max_entities: usize,
        max_input_bytes: u64,
        max_output_operations: u64,
        max_output_bytes: u64,
    ) -> SearchIndexBatchLimits {
        SearchIndexBatchLimits::try_new(
            NonZeroUsize::new(max_entities).expect("entity limit is positive"),
            NonZeroU64::new(max_input_bytes).expect("input limit is positive"),
            NonZeroU64::new(max_output_operations).expect("operation limit is positive"),
            NonZeroU64::new(max_output_bytes).expect("output limit is positive"),
            NonZeroU64::MIN,
        )
        .expect("GC test limits validate")
    }

    /// Writes one exact candidate variant and returns its scoped key.
    async fn put_candidate(
        db: &Db,
        scope: DataScope,
        operation: &IndexOperationRecord,
        seed: u8,
        owner: index_keys::BlobGcCandidateKeyOwner,
    ) -> Bytes {
        let blob = work::BlobRef::new([seed; 32], 64);
        let value_owner = match owner {
            index_keys::BlobGcCandidateKeyOwner::GenerationCleanup => {
                work::BlobGcCandidateOwner::GenerationCleanup(operation.operation_id())
            }
            index_keys::BlobGcCandidateKeyOwner::UploadIntent(intent_id) => {
                work::BlobGcCandidateOwner::UploadIntent(intent_id)
            }
        };
        let key = scoped_key(
            scope,
            index_keys::IndexV2Key::BlobGcCandidate(index_keys::BlobGcCandidateKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                owner,
                blob_hash: index_keys::BlobHash::new(*blob.hash()),
            }),
        );
        db.put(
            key.clone(),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcCandidate(
                work::BlobGcCandidateValue {
                    owner: value_owner,
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    blob,
                },
            )),
        )
        .await
        .expect("candidate is written");
        key
    }

    /// Executes and commits one generation-batch transition.
    async fn run_batch(
        db: &Db,
        scope: DataScope,
        operation: &IndexOperationRecord,
        progress: &GcProgress,
        limits: SearchIndexBatchLimits,
    ) -> Result<GenerationBatchTransition> {
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("GC transaction begins");
        let transition =
            stage_generation_batch(&transaction, scope, operation, progress, limits).await?;
        transaction.commit().await.expect("GC transaction commits");
        Ok(transition)
    }

    /// Persists one exact immutable generation root for fence-worker tests.
    async fn staged_generation_root(
        db: &Db,
        scope: DataScope,
        operation: &IndexOperationRecord,
        seeds: &[u8],
    ) -> (BlobGcRunId, Vec<work::BlobRef>) {
        for seed in seeds {
            put_candidate(
                db,
                scope,
                operation,
                *seed,
                index_keys::BlobGcCandidateKeyOwner::GenerationCleanup,
            )
            .await;
        }
        let GenerationBatchTransition::Progressed(progress) = run_batch(
            db,
            scope,
            operation,
            &ready_progress(scope, operation),
            limits(
                seeds.len(),
                64 * 1024,
                u64::try_from(seeds.len()).expect("test member count fits u64") + 1,
                64 * 1024,
            ),
        )
        .await
        .expect("generation root stages") else {
            panic!("generation candidates produce one immutable root");
        };
        (
            progress.gc_run_id.expect("staged root ID is retained"),
            seeds
                .iter()
                .map(|seed| work::BlobRef::new([*seed; 32], 64))
                .collect(),
        )
    }

    /// Returns one coordinator whose in-memory fence state is inspectable.
    fn coordinator(name: &str) -> Arc<ProcessLocalBlobPublicationCoordinator> {
        Arc::new(ProcessLocalBlobPublicationCoordinator::new(
            Arc::new(InMemory::new()),
            name,
            BlobPublicationTiming::default(),
        ))
    }

    /// Writes one exact global reference for a test member.
    async fn put_reference(db: &Db, blob: work::BlobRef, owner_seed: u8) {
        let owner_logical_key =
            index_keys::IndexV2Key::TextUploadIntent(index_keys::TextIntentOwnedKey {
                index_id: IndexId::initial(),
                generation: IndexGenerationId::initial(),
                intent_id: TextUploadIntentId::from_bytes([owner_seed; 16])
                    .expect("reference owner ID is non-nil"),
            })
            .to_bytes();
        let owner_kind = index_keys::BlobReferenceOwnerKind::UploadIntent;
        let reference_key = index_keys::BlobReferenceGlobalKey::try_new(
            index_keys::BlobHash::new(*blob.hash()),
            owner_kind,
            DataScope::LegacyUnscoped,
            owner_logical_key.clone(),
            0,
        )
        .expect("reference key validates");
        let reference_value = work::BlobReachabilityReferenceValue::try_new(
            blob,
            owner_kind,
            DataScope::LegacyUnscoped,
            owner_logical_key,
            0,
        )
        .expect("reference value validates");
        db.put(
            index_keys::GlobalIndexV2Key::BlobReachabilityReference(reference_key).to_bytes(),
            index_values::encode_work_value(
                &index_values::IndexV2WorkValue::BlobReachabilityReference(reference_value),
            ),
        )
        .await
        .expect("reference is written");
    }

    /// Reads one exact pass/attempt mark set in hash order.
    async fn mark_references(
        db: &Db,
        run_id: BlobGcRunId,
        pass: index_keys::BlobGcPass,
        attempt: u64,
    ) -> Vec<bool> {
        let prefix = index_keys::GlobalIndexV2Key::blob_gc_reachability_mark_prefix(
            run_id,
            pass,
            NonZeroU64::new(attempt).expect("test attempt is non-zero"),
        );
        let mut rows = db
            .scan_prefix(&prefix, (Bound::Unbounded, Bound::<Bytes>::Unbounded))
            .await
            .expect("mark prefix is readable");
        let mut referenced = Vec::new();
        while let Some(row) = rows.next().await.expect("mark row is readable") {
            let index_values::IndexV2WorkValue::BlobGcEntry(
                work::BlobGcEntryValue::ReachabilityMark(mark),
            ) = index_values::decode_work_value(&row.value).expect("mark decodes")
            else {
                panic!("mark key contains another value kind");
            };
            referenced.push(mark.referenced);
        }
        referenced
    }

    /// Writes one exact non-empty Delete root and every member's current marks.
    async fn put_delete_root(
        db: &Db,
        scope: DataScope,
        operation: &IndexOperationRecord,
        members: &[(work::BlobRef, work::BlobGcMemberState, (bool, bool))],
        stale_first_mark: bool,
    ) -> BlobGcRunId {
        assert!(
            !members.is_empty(),
            "a Delete root must own at least one member"
        );
        let run_id = BlobGcRunId::from_bytes([201; 16]).expect("run ID is non-nil");
        for (blob, state, references) in members {
            let member_key = index_keys::GlobalIndexV2Key::BlobGcCandidateMember {
                run_id,
                blob_hash: index_keys::BlobHash::new(*blob.hash()),
            }
            .to_bytes();
            db.put(
                member_key,
                encode_member(&work::BlobGcCandidateMemberValue {
                    run_id,
                    blob: *blob,
                    state: state.clone(),
                }),
            )
            .await
            .expect("Delete member is written");
            if matches!(state, work::BlobGcMemberState::PendingDisposition { .. }) {
                let candidate_key = scoped_key(
                    scope,
                    index_keys::IndexV2Key::BlobGcCandidate(index_keys::BlobGcCandidateKey {
                        index_id: operation.index_id(),
                        generation: operation.generation(),
                        owner: index_keys::BlobGcCandidateKeyOwner::GenerationCleanup,
                        blob_hash: index_keys::BlobHash::new(*blob.hash()),
                    }),
                );
                db.put(
                    candidate_key,
                    index_values::encode_work_value(
                        &index_values::IndexV2WorkValue::BlobGcCandidate(
                            work::BlobGcCandidateValue {
                                owner: work::BlobGcCandidateOwner::GenerationCleanup(
                                    operation.operation_id(),
                                ),
                                index_id: operation.index_id(),
                                generation: operation.generation(),
                                blob: *blob,
                            },
                        ),
                    ),
                )
                .await
                .expect("Delete candidate is written");
            }
            for (pass, referenced) in [
                (index_keys::BlobGcPass::First, references.0),
                (index_keys::BlobGcPass::Second, references.1),
            ] {
                let attempt = work::GcScanAttempt::new(2).unwrap();
                let blob_hash = index_keys::BlobHash::new(*blob.hash());
                db.put(
                    member_mark_key(run_id, pass, attempt, *blob),
                    index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcEntry(
                        work::BlobGcEntryValue::ReachabilityMark(
                            work::BlobGcReachabilityMarkValue {
                                run_id,
                                first_pass: pass == index_keys::BlobGcPass::First,
                                scan_attempt: attempt,
                                blob_hash,
                                referenced,
                            },
                        ),
                    )),
                )
                .await
                .expect("current Delete mark is written");
            }
            if stale_first_mark {
                let attempt = work::GcScanAttempt::new(1).unwrap();
                let blob_hash = index_keys::BlobHash::new(*blob.hash());
                db.put(
                    member_mark_key(run_id, index_keys::BlobGcPass::First, attempt, *blob),
                    index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcEntry(
                        work::BlobGcEntryValue::ReachabilityMark(
                            work::BlobGcReachabilityMarkValue {
                                run_id,
                                first_pass: true,
                                scan_attempt: attempt,
                                blob_hash,
                                referenced: false,
                            },
                        ),
                    )),
                )
                .await
                .expect("older Delete mark is written");
            }
        }
        let root = work::BlobGcRunRootValue::try_new(
            run_id,
            work::BlobGcRunOwner::GenerationCleanup {
                scope,
                operation_id: operation.operation_id(),
                index_id: operation.index_id(),
                generation: operation.generation(),
            },
            BlobGcRunRevision::initial(),
            0,
            None,
            work::BlobGcPhase::Delete {
                completed_first_attempt: work::GcScanAttempt::new(2).unwrap(),
                completed_second_attempt: work::GcScanAttempt::new(2).unwrap(),
                member_cursor: None,
                stale_mark_cleanup: work::StaleMarkCleanup::Pending { mark_cursor: None },
            },
            u32::try_from(members.len()).expect("test member count fits u32"),
        )
        .expect("Delete root validates");
        db.put(
            index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes(),
            encode_root(&root),
        )
        .await
        .expect("Delete root is written");
        run_id
    }

    /// Returns progress whose completed barrier authorizes candidate selection.
    fn ready_progress(scope: DataScope, operation: &IndexOperationRecord) -> GcProgress {
        GcProgress {
            gc_run_id: None,
            candidate_cursor: None,
            stage_cursor: Some(
                generation_candidate_start_cursor(scope, operation)
                    .expect("candidate marker validates"),
            ),
            counters: OperationCounters::default(),
        }
    }

    #[tokio::test]
    async fn run_id_allocation_skips_collisions_and_fails_closed_at_its_budget() {
        let db = test_db("text-gc-run-id-collisions").await;
        let occupied = BlobGcRunId::from_bytes([7; 16]).expect("occupied run ID is non-nil");
        let available = BlobGcRunId::from_bytes([8; 16]).expect("available run ID is non-nil");
        let occupied_key = index_keys::GlobalIndexV2Key::BlobGcRunRoot(occupied).to_bytes();
        db.put(&occupied_key, Bytes::from_static(b"occupied"))
            .await
            .expect("occupied root key is written");

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("allocation transaction begins");
        let mut candidates = [occupied, available].into_iter();
        let (selected, selected_key, observed_bytes) = unused_run_id_from(&transaction, || {
            candidates
                .next()
                .expect("test candidate sequence is complete")
        })
        .await
        .expect("allocator skips an occupied root key");
        assert_eq!(selected, available);
        assert_eq!(
            selected_key,
            index_keys::GlobalIndexV2Key::BlobGcRunRoot(available).to_bytes()
        );
        assert!(
            observed_bytes > u64::try_from(occupied_key.len()).expect("key length fits u64"),
            "collision accounting includes both the occupied row and absent candidate key"
        );

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("exhaustion transaction begins");
        assert!(matches!(
            unused_run_id_from(&transaction, || occupied).await,
            Err(HelixDbError::IdentifierExhausted(
                "blob GC run ID collision budget"
            ))
        ));
    }

    #[test]
    fn reachability_reference_validation_is_key_value_and_member_exact() {
        let run_id = BlobGcRunId::from_bytes([11; 16]).expect("run ID is non-nil");
        let blob = work::BlobRef::new([12; 32], 64);
        let member = work::BlobGcCandidateMemberValue {
            run_id,
            blob,
            state: work::BlobGcMemberState::PendingDisposition { owner_cursor: None },
        };
        let owner_logical_key =
            index_keys::IndexV2Key::TextUploadIntent(index_keys::TextIntentOwnedKey {
                index_id: IndexId::initial(),
                generation: IndexGenerationId::initial(),
                intent_id: TextUploadIntentId::from_bytes([13; 16]).expect("owner ID is non-nil"),
            })
            .to_bytes();
        let owner_kind = index_keys::BlobReferenceOwnerKind::UploadIntent;
        let reference_key = index_keys::GlobalIndexV2Key::BlobReachabilityReference(
            index_keys::BlobReferenceGlobalKey::try_new(
                index_keys::BlobHash::new(*blob.hash()),
                owner_kind,
                DataScope::LegacyUnscoped,
                owner_logical_key.clone(),
                0,
            )
            .expect("reference key validates"),
        )
        .to_bytes();
        let reference_value = index_values::encode_work_value(
            &index_values::IndexV2WorkValue::BlobReachabilityReference(
                work::BlobReachabilityReferenceValue::try_new(
                    blob,
                    owner_kind,
                    DataScope::LegacyUnscoped,
                    owner_logical_key.clone(),
                    0,
                )
                .expect("reference value validates"),
            ),
        );
        validate_reachability_reference(&member, &reference_key, &reference_value)
            .expect("exact reference validates");

        assert!(matches!(
            validate_reachability_reference(
                &member,
                &index_keys::GlobalIndexV2Key::BlobGcCandidateMember {
                    run_id,
                    blob_hash: index_keys::BlobHash::new(*blob.hash()),
                }
                .to_bytes(),
                &reference_value,
            ),
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        assert!(matches!(
            validate_reachability_reference(
                &member,
                &reference_key,
                &index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcEntry(
                    work::BlobGcEntryValue::CandidateMember(member.clone()),
                ),),
            ),
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        let mismatching_value = index_values::encode_work_value(
            &index_values::IndexV2WorkValue::BlobReachabilityReference(
                work::BlobReachabilityReferenceValue::try_new(
                    work::BlobRef::new([12; 32], 65),
                    owner_kind,
                    DataScope::LegacyUnscoped,
                    owner_logical_key,
                    0,
                )
                .expect("independently valid mismatching reference encodes"),
            ),
        );
        assert!(matches!(
            validate_reachability_reference(&member, &reference_key, &mismatching_value),
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
    }

    #[tokio::test]
    async fn upload_reclaim_root_requires_exact_candidate_and_absent_reachability() {
        let db = test_db("text-gc-upload-root-anchors").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let intent = claimed_reclaim_intent(&operation, 31);

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("missing-candidate transaction begins");
        assert!(matches!(
            stage_upload_reclaim_root(&transaction, scope, &intent).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        drop(transaction);

        let (candidate_key, candidate_value) = disposition_intent_candidate(scope, &intent);
        db.put(&candidate_key, candidate_value)
            .await
            .expect("exact candidate is written");
        let anchors = super::super::upload::upload_anchor_rows(scope, &intent)
            .expect("intent anchors encode");
        db.put(&anchors.reachability_key, &anchors.reachability_value)
            .await
            .expect("conflicting live reachability is written");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("live-reference transaction begins");
        assert!(matches!(
            stage_upload_reclaim_root(&transaction, scope, &intent).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        drop(transaction);
        assert!(scan_root_page(&db, None, NonZeroUsize::MIN)
            .await
            .expect("root lane remains readable")
            .run_ids
            .is_empty());
    }

    #[tokio::test]
    async fn upload_reclaim_root_requires_one_matching_live_canonical_generation() {
        let db = test_db("text-gc-upload-root-canonical-generation").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let intent = claimed_reclaim_intent(&operation, 52);
        let (candidate_key, candidate_value) = disposition_intent_candidate(scope, &intent);
        db.put(candidate_key, candidate_value)
            .await
            .expect("exact candidate is written");

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("missing-record transaction begins");
        assert!(matches!(
            stage_upload_reclaim_root(&transaction, scope, &intent).await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "upload-reclaim root creation is missing its canonical index record"
        ));
        drop(transaction);

        let exact = building_record(&operation);
        let mismatched = IndexRecordV2::building(
            operation.index_id(),
            exact.definition().clone(),
            IndexRevision::initial(),
            PhysicalGeneration::Text {
                generation: IndexGenerationId::new(2).expect("second generation is non-zero"),
            },
            operation.operation_id(),
        )
        .expect("mismatching generation remains independently valid");
        put_record(&db, scope, &mismatched).await;
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("mismatched-record transaction begins");
        assert!(matches!(
            stage_upload_reclaim_root(&transaction, scope, &intent).await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "upload-reclaim root creation and canonical index generation disagree"
        ));
        drop(transaction);

        let dropped = exact
            .transition(IndexStateTransition::BeginAbort)
            .expect("Building record enters Aborting")
            .transition(IndexStateTransition::CompleteAbort)
            .expect("Aborting record enters Dropped");
        put_record(&db, scope, &dropped).await;
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("Dropped-record transaction begins");
        assert!(matches!(
            stage_upload_reclaim_root(&transaction, scope, &intent).await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "dropped text generation retained an upload-reclaim intent"
        ));
        drop(transaction);

        assert!(scan_root_page(&db, None, NonZeroUsize::MIN)
            .await
            .expect("root lane remains readable")
            .run_ids
            .is_empty());
    }

    #[tokio::test]
    async fn singleton_assignment_before_abort_remains_visible_to_the_cleanup_barrier() {
        let db = test_db("text-gc-singleton-before-abort").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let record = building_record(&operation);
        put_record(&db, scope, &record).await;
        let intent = claimed_reclaim_intent(&operation, 35);
        let (candidate_key, candidate_value) = disposition_intent_candidate(scope, &intent);
        db.put(&candidate_key, candidate_value)
            .await
            .expect("intent-qualified candidate is written");
        let claimed_rows = super::super::upload::upload_anchor_rows(scope, &intent)
            .expect("claimed intent anchors encode");
        db.put(&claimed_rows.intent_key, &claimed_rows.intent_value)
            .await
            .expect("claimed intent is written");
        db.put(&claimed_rows.pointer_key, &claimed_rows.pointer_value)
            .await
            .expect("claimed pointer is written");

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("singleton assignment transaction begins");
        let UploadReclaimRootTransition::Assigned(run_id) =
            stage_upload_reclaim_root(&transaction, scope, &intent)
                .await
                .expect("Building generation accepts singleton assignment")
        else {
            panic!("Building generation owns independent upload reclamation");
        };
        let assigned = intent
            .assign_reclaim_root(run_id)
            .expect("unassigned reclaim intent accepts its exact run ID");
        let assigned_rows = super::super::upload::upload_anchor_rows(scope, &assigned)
            .expect("assigned intent anchors encode");
        transaction
            .put(&assigned_rows.intent_key, &assigned_rows.intent_value)
            .expect("assigned intent is staged");
        transaction
            .put(&assigned_rows.pointer_key, &assigned_rows.pointer_value)
            .expect("assigned pointer is staged");
        transaction
            .commit()
            .await
            .expect("singleton assignment commits before abort");

        let aborting = record
            .transition(IndexStateTransition::BeginAbort)
            .expect("Building record enters Aborting");
        put_record(&db, scope, &aborting).await;
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("cleanup barrier transaction begins");
        assert!(matches!(
            scan_reclaim_barrier(
                &transaction,
                scope,
                &operation,
                &PrefixScanProgress {
                    cursor: None,
                    counters: OperationCounters::default(),
                },
                limits(8, 64 * 1024, 8, 64 * 1024),
            )
            .await
            .expect("cleanup barrier validates the assigned root"),
            ReclaimBarrierTransition::Waiting
        ));
        let work::TextUploadPhase::Reclaimable(work::ReclaimAssignment::Assigned(run_id)) =
            assigned.phase
        else {
            panic!("singleton assignment stores its exact run ID");
        };
        assert!(load_root(&db, run_id).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn abort_and_drop_gates_prevent_late_singleton_assignment() {
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();

        let dropping_db = test_db("text-gc-drop-before-singleton").await;
        let building = building_record(&operation);
        let dropping = building
            .transition(IndexStateTransition::Activate)
            .expect("Building record activates")
            .transition(IndexStateTransition::BeginDrop {
                drop_operation_id: IndexOperationId::from_bytes([39; 16])
                    .expect("drop operation ID is non-nil"),
            })
            .expect("Active record enters Dropping");
        put_record(&dropping_db, scope, &dropping).await;
        let dropping_intent = claimed_reclaim_intent(&operation, 40);
        let (candidate_key, candidate_value) =
            disposition_intent_candidate(scope, &dropping_intent);
        dropping_db
            .put(candidate_key, candidate_value)
            .await
            .expect("Dropping candidate is written");
        let transaction = dropping_db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("Dropping assignment transaction begins");
        assert!(matches!(
            stage_upload_reclaim_root(&transaction, scope, &dropping_intent)
                .await
                .expect("Dropping gate returns a closed decision"),
            UploadReclaimRootTransition::GenerationCleanupOwnsAssignment
        ));
        transaction
            .commit()
            .await
            .expect("read-only Dropping decision commits");
        assert!(scan_root_page(&dropping_db, None, NonZeroUsize::MIN)
            .await
            .expect("Dropping root lane remains readable")
            .run_ids
            .is_empty());

        let aborting_db = test_db("text-gc-abort-races-singleton").await;
        put_record(&aborting_db, scope, &building).await;
        let aborting_intent = claimed_reclaim_intent(&operation, 43);
        let (candidate_key, candidate_value) =
            disposition_intent_candidate(scope, &aborting_intent);
        aborting_db
            .put(candidate_key, candidate_value)
            .await
            .expect("Aborting-race candidate is written");
        let transaction = aborting_db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("racing singleton assignment begins");
        assert!(matches!(
            stage_upload_reclaim_root(&transaction, scope, &aborting_intent)
                .await
                .expect("the open assignment observes Building"),
            UploadReclaimRootTransition::Assigned(_)
        ));
        let aborting = building
            .transition(IndexStateTransition::BeginAbort)
            .expect("Building record enters Aborting");
        put_record(&aborting_db, scope, &aborting).await;
        transaction
            .commit()
            .await
            .expect_err("Aborting record conflicts with the stale assignment snapshot");
        assert!(scan_root_page(&aborting_db, None, NonZeroUsize::MIN)
            .await
            .expect("Aborting root lane remains readable")
            .run_ids
            .is_empty());
    }

    /// Proves a referenced singleton finishes before generation candidates can start.
    #[tokio::test]
    async fn referenced_singleton_reclaim_preserves_blob_before_generation_prepare_candidates() {
        let db = test_db("text-gc-referenced-singleton-generation-race").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let definition = ValidatedDynamicIndexDefinition::try_from(
            TextIndexDefinition::new_node("Document", "body").expect("text definition validates"),
        )
        .expect("dynamic text definition validates");
        let build_operation_id =
            IndexOperationId::from_bytes([225; 16]).expect("build operation ID is non-nil");
        let building = IndexRecordV2::building(
            operation.index_id(),
            definition,
            IndexRevision::initial(),
            PhysicalGeneration::Text {
                generation: operation.generation(),
            },
            build_operation_id,
        )
        .expect("Building text record validates");
        let active = building
            .transition(IndexStateTransition::Activate)
            .expect("Building record activates");
        put_record(&db, scope, &active).await;

        let payload = Bytes::from_static(b"referenced singleton blob");
        let blob = work::BlobRef::new(
            Sha256::digest(&payload).into(),
            u64::try_from(payload.len()).expect("test payload length fits u64"),
        );
        let store = Arc::new(InMemory::new());
        let db_path = "text-gc-referenced-singleton-generation-race-blobs";
        let location = crate::search::text::blob_object_store_path(db_path, *blob.hash());
        store
            .put(&location, PutPayload::from_bytes(payload))
            .await
            .expect("referenced object is written");
        let coordinator = Arc::new(ProcessLocalBlobPublicationCoordinator::new(
            store.clone(),
            db_path,
            BlobPublicationTiming::default(),
        ));
        let writer_epoch = WriterEpoch::from_bytes([226; 16]).expect("writer epoch is non-nil");
        let intent_id = TextUploadIntentId::from_bytes([227; 16]).expect("intent ID is non-nil");
        let permit = coordinator
            .reserve(blob, intent_id, writer_epoch)
            .await
            .expect("publication permit is reserved");
        coordinator.expire_unused_permit(permit);
        let intent = work::TextUploadIntentValue::try_new(
            intent_id,
            TextIntentRevision::initial(),
            operation.index_id(),
            operation.identity().clone(),
            operation.generation(),
            work::TextPartition::Unpartitioned,
            blob,
            permit.id(),
            work::TextUploadOwner::Build {
                operation_id: build_operation_id,
                expected_operation_revision: IndexOperationRevision::initial(),
            },
            work::TextUploadAttachment::BuildArtifact {
                artifact_ordinal: 0,
                split: work::SplitRef::try_new(blob, 0, 0, 0, blob.size())
                    .expect("test split validates"),
            },
            work::TextUploadPhase::Reclaimable(work::ReclaimAssignment::Unassigned),
            0,
            work::TextUploadWorkState::Claimed(OperationClaim {
                writer_epoch,
                sequence: ClaimSequence::new(1).expect("claim sequence is non-zero"),
            }),
        )
        .expect("claimed singleton intent validates");
        let (candidate_key, candidate_value) = disposition_intent_candidate(scope, &intent);
        db.put(&candidate_key, candidate_value)
            .await
            .expect("intent-qualified candidate is written");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("singleton assignment transaction begins");
        let UploadReclaimRootTransition::Assigned(run_id) =
            stage_upload_reclaim_root(&transaction, scope, &intent)
                .await
                .expect("Active generation accepts singleton assignment")
        else {
            panic!("Active generation owns independent upload reclamation");
        };
        let assigned = intent
            .assign_reclaim_root(run_id)
            .expect("claimed singleton accepts its exact root");
        let assigned_rows = super::super::upload::upload_anchor_rows(scope, &assigned)
            .expect("assigned singleton rows encode");
        transaction
            .put(&assigned_rows.intent_key, &assigned_rows.intent_value)
            .expect("assigned intent is staged");
        transaction
            .put(&assigned_rows.pointer_key, &assigned_rows.pointer_value)
            .expect("assigned pointer is staged");
        transaction
            .commit()
            .await
            .expect("singleton assignment commits");
        put_reference(&db, blob, 228).await;

        let dropping = active
            .transition(IndexStateTransition::BeginDrop {
                drop_operation_id: operation.operation_id(),
            })
            .expect("Active record enters Dropping");
        put_record(&db, scope, &dropping).await;
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("generation barrier transaction begins");
        assert!(matches!(
            scan_reclaim_barrier(
                &transaction,
                scope,
                &operation,
                &PrefixScanProgress {
                    cursor: None,
                    counters: OperationCounters::default(),
                },
                limits(8, 64 * 1024, 8, 64 * 1024),
            )
            .await
            .expect("assigned singleton remains visible to generation cleanup"),
            ReclaimBarrierTransition::Waiting
        ));
        drop(transaction);

        let driver =
            TextBlobGcDriver::new(coordinator.clone(), crate::search::text::BlobGcGate::new());
        for _ in 0..2 {
            assert_eq!(
                driver
                    .execute_root_step(&db, run_id, writer_epoch, 12_000)
                    .await
                    .expect("singleton delete fence closes"),
                BlobGcRootStep::Progressed
            );
        }
        let claimed = assigned
            .claim(OperationClaim {
                writer_epoch,
                sequence: ClaimSequence::new(2).expect("claim sequence is non-zero"),
            })
            .expect("assigned singleton is claimable after fences close");
        let claimed_rows = super::super::upload::upload_anchor_rows(scope, &claimed)
            .expect("claimed singleton rows encode");
        db.put(&claimed_rows.intent_key, &claimed_rows.intent_value)
            .await
            .expect("claimed singleton is written");
        db.put(&claimed_rows.pointer_key, &claimed_rows.pointer_value)
            .await
            .expect("claimed pointer is written");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("first-pass handoff transaction begins");
        let normalized = stage_upload_reclaim_first_pass(&transaction, scope, &claimed)
            .await
            .expect("singleton owner normalization validates")
            .expect("FencesClosed singleton enters its first pass");
        let normalized_rows = super::super::upload::upload_anchor_rows(scope, &normalized)
            .expect("normalized singleton rows encode");
        transaction
            .put(&normalized_rows.intent_key, &normalized_rows.intent_value)
            .expect("normalized singleton intent is staged");
        transaction
            .put(&normalized_rows.pointer_key, &normalized_rows.pointer_value)
            .expect("normalized singleton pointer is staged");
        transaction
            .commit()
            .await
            .expect("singleton enters its first pass atomically");

        for _ in 0..32 {
            if load_root(&db, run_id)
                .await
                .expect("singleton root remains readable")
                .is_none()
            {
                break;
            }
            assert_eq!(
                driver
                    .execute_root_step(&db, run_id, writer_epoch, 12_000)
                    .await
                    .expect("referenced singleton advances to terminal cleanup"),
                BlobGcRootStep::Progressed
            );
        }
        assert!(load_root(&db, run_id).await.unwrap().is_none());
        assert!(db.get(&assigned_rows.intent_key).await.unwrap().is_none());
        assert!(db.get(&assigned_rows.pointer_key).await.unwrap().is_none());
        assert!(db.get(&candidate_key).await.unwrap().is_none());
        assert!(
            store.get(&location).await.is_ok(),
            "referenced blob is preserved"
        );

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("generation candidate preparation transaction begins");
        let GenerationBatchTransition::Progressed(progress) = stage_generation_batch(
            &transaction,
            scope,
            &operation,
            &GcProgress {
                gc_run_id: None,
                candidate_cursor: None,
                stage_cursor: None,
                counters: OperationCounters::default(),
            },
            limits(8, 64 * 1024, 8, 64 * 1024),
        )
        .await
        .expect("generation preparation resumes after singleton completion") else {
            panic!("generation preparation persists its completed singleton barrier");
        };
        assert!(progress.gc_run_id.is_none());
        assert!(is_generation_candidate_start_cursor(
            scope,
            &operation,
            progress
                .stage_cursor
                .as_ref()
                .expect("completed barrier marker is retained"),
        )
        .expect("completed barrier marker validates"));
        transaction
            .commit()
            .await
            .expect("generation barrier checkpoint commits");
    }

    /// Proves an unrelated root phase cannot serialize distinct-hash root creation.
    #[tokio::test]
    async fn distinct_hash_root_creation_survives_every_other_root_terminal_phase() {
        let db = test_db("text-gc-distinct-root-creation-phases").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let active = building_record(&operation)
            .transition(IndexStateTransition::Activate)
            .expect("Building record activates");
        put_record(&db, scope, &active).await;

        let other_run = BlobGcRunId::from_bytes([229; 16]).expect("other run ID is non-nil");
        let other_blob = work::BlobRef::new([230; 32], 64);
        let other_member_key = index_keys::GlobalIndexV2Key::BlobGcCandidateMember {
            run_id: other_run,
            blob_hash: index_keys::BlobHash::new(*other_blob.hash()),
        }
        .to_bytes();
        let other_member = work::BlobGcCandidateMemberValue {
            run_id: other_run,
            blob: other_blob,
            state: work::BlobGcMemberState::PendingDisposition { owner_cursor: None },
        };
        let writer_epoch = WriterEpoch::from_bytes([231; 16]).expect("writer epoch is non-nil");
        let attempt = work::GcScanAttempt::new(1).expect("scan attempt is non-zero");
        let phase_matrix = [
            (work::BlobGcPhase::FencesClosed, true),
            (
                work::BlobGcPhase::FirstPass {
                    writer_epoch,
                    first_attempt: attempt,
                    reference_cursor: None,
                },
                true,
            ),
            (
                work::BlobGcPhase::Delete {
                    completed_first_attempt: attempt,
                    completed_second_attempt: attempt,
                    member_cursor: None,
                    stale_mark_cleanup: work::StaleMarkCleanup::Pending { mark_cursor: None },
                },
                true,
            ),
            (
                work::BlobGcPhase::Delete {
                    completed_first_attempt: attempt,
                    completed_second_attempt: attempt,
                    member_cursor: Some(
                        IndexCursor::try_new(other_member_key.clone())
                            .expect("post-finish member cursor validates"),
                    ),
                    stale_mark_cleanup: work::StaleMarkCleanup::Pending { mark_cursor: None },
                },
                false,
            ),
        ];

        for ((phase, member_present), seed) in phase_matrix.into_iter().zip([61_u8, 71, 81, 91]) {
            let other_root = work::BlobGcRunRootValue::try_new(
                other_run,
                work::BlobGcRunOwner::GenerationCleanup {
                    scope: DataScope::Tenant(
                        crate::encoding::v1::keys::tenant::TenantId::from_u128(2),
                    ),
                    operation_id: IndexOperationId::from_bytes([232; 16])
                        .expect("other operation ID is non-nil"),
                    index_id: IndexId::new(2).expect("other index ID is non-zero"),
                    generation: IndexGenerationId::initial(),
                },
                BlobGcRunRevision::initial(),
                0,
                None,
                phase.clone(),
                1,
            )
            .expect("other root phase validates");
            db.put(
                index_keys::GlobalIndexV2Key::BlobGcRunRoot(other_run).to_bytes(),
                encode_root(&other_root),
            )
            .await
            .expect("other root phase is written");
            if member_present {
                db.put(&other_member_key, encode_member(&other_member))
                    .await
                    .expect("other root member is written");
            } else {
                db.delete(&other_member_key)
                    .await
                    .expect("post-finish member is absent");
            }

            let intent = claimed_reclaim_intent(&operation, seed);
            assert_ne!(intent.blob, other_blob);
            let (candidate_key, candidate_value) = disposition_intent_candidate(scope, &intent);
            db.put(&candidate_key, candidate_value)
                .await
                .expect("distinct candidate is written");
            let transaction = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .expect("distinct root transaction begins");
            let UploadReclaimRootTransition::Assigned(run_id) =
                stage_upload_reclaim_root(&transaction, scope, &intent)
                    .await
                    .expect("unrelated root phase permits distinct root creation")
            else {
                panic!("Active generation accepts independent reclaim roots");
            };
            let assigned = intent
                .assign_reclaim_root(run_id)
                .expect("distinct singleton accepts its exact run");
            let rows = super::super::upload::upload_anchor_rows(scope, &assigned)
                .expect("distinct singleton rows encode");
            transaction
                .put(&rows.intent_key, &rows.intent_value)
                .expect("assigned intent is staged");
            transaction
                .put(&rows.pointer_key, &rows.pointer_value)
                .expect("assigned pointer is staged");
            transaction
                .commit()
                .await
                .expect("distinct root creation commits");
            assert!(load_root(&db, run_id).await.unwrap().is_some());
            assert_eq!(load_root(&db, other_run).await.unwrap(), Some(other_root));

            db.delete(index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes())
                .await
                .expect("distinct root is removed for the next matrix row");
            db.delete(
                index_keys::GlobalIndexV2Key::BlobGcCandidateMember {
                    run_id,
                    blob_hash: index_keys::BlobHash::new(*intent.blob.hash()),
                }
                .to_bytes(),
            )
            .await
            .expect("distinct member is removed for the next matrix row");
            db.delete(candidate_key)
                .await
                .expect("distinct candidate is removed for the next matrix row");
            db.delete(rows.intent_key)
                .await
                .expect("distinct intent is removed for the next matrix row");
            db.delete(rows.pointer_key)
                .await
                .expect("distinct pointer is removed for the next matrix row");
        }
    }

    #[tokio::test]
    async fn generation_batch_stages_one_root_and_bounded_immutable_members() {
        let db = test_db("text-gc-generation-batch").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let first_key = put_candidate(
            &db,
            scope,
            &operation,
            1,
            index_keys::BlobGcCandidateKeyOwner::GenerationCleanup,
        )
        .await;
        let second_key = put_candidate(
            &db,
            scope,
            &operation,
            2,
            index_keys::BlobGcCandidateKeyOwner::GenerationCleanup,
        )
        .await;
        put_candidate(
            &db,
            scope,
            &operation,
            3,
            index_keys::BlobGcCandidateKeyOwner::GenerationCleanup,
        )
        .await;

        let GenerationBatchTransition::Progressed(progress) = run_batch(
            &db,
            scope,
            &operation,
            &ready_progress(scope, &operation),
            limits(2, 64 * 1024, 3, 64 * 1024),
        )
        .await
        .expect("bounded generation batch stages") else {
            panic!("candidate selection returns persisted run progress");
        };
        let run_id = progress.gc_run_id.expect("staged batch stores its run ID");
        assert_eq!(
            progress
                .candidate_cursor
                .as_ref()
                .expect("staged batch stores a strict cursor")
                .as_bytes(),
            &second_key
        );
        assert_eq!(progress.counters.entities, 2);
        assert_eq!(progress.counters.output_operations, 3);
        assert!(db.get(&first_key).await.unwrap().is_some());
        assert!(db.get(&second_key).await.unwrap().is_some());

        let root_key = index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes();
        let root_value = db
            .get(&root_key)
            .await
            .expect("root is readable")
            .expect("root exists");
        let mut exact_output_bytes = measured_row(&root_key, Some(&root_value));
        let root = decode_root(&root_value).expect("root decodes");
        assert_eq!(root.run_id, run_id);
        assert_eq!(root.candidate_count.get(), 2);
        assert_eq!(
            root.owner,
            work::BlobGcRunOwner::GenerationCleanup {
                scope,
                operation_id: operation.operation_id(),
                index_id: operation.index_id(),
                generation: operation.generation(),
            }
        );
        assert!(matches!(
            root.phase,
            work::BlobGcPhase::AwaitDeleteFences {
                member_cursor: None
            }
        ));
        for seed in [1, 2] {
            let member_key = index_keys::GlobalIndexV2Key::BlobGcCandidateMember {
                run_id,
                blob_hash: index_keys::BlobHash::new([seed; 32]),
            }
            .to_bytes();
            let member_value = db
                .get(&member_key)
                .await
                .expect("member is readable")
                .expect("member exists");
            exact_output_bytes = exact_output_bytes
                .checked_add(measured_row(&member_key, Some(&member_value)))
                .expect("test output measurement does not overflow");
            let index_values::IndexV2WorkValue::BlobGcEntry(
                work::BlobGcEntryValue::CandidateMember(member),
            ) = index_values::decode_work_value(&member_value).expect("member decodes")
            else {
                panic!("member key contains a member value");
            };
            assert_eq!(member.run_id, run_id);
            assert!(matches!(
                member.state,
                work::BlobGcMemberState::PendingDisposition { owner_cursor: None }
            ));
        }
        assert_eq!(progress.counters.output_bytes, exact_output_bytes);

        assert!(matches!(
            run_batch(
                &db,
                scope,
                &operation,
                &progress,
                limits(2, 64 * 1024, 3, 64 * 1024),
            )
            .await
            .expect("persisted root is recoverable"),
            GenerationBatchTransition::Waiting
        ));
    }

    #[tokio::test]
    async fn generation_cursor_handoff_processes_three_maximum_size_batches() {
        let db = test_db("text-gc-three-generation-batches").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let mut candidate_keys = Vec::new();
        for seed in [4, 5, 6] {
            candidate_keys.push(
                put_candidate(
                    &db,
                    scope,
                    &operation,
                    seed,
                    index_keys::BlobGcCandidateKeyOwner::GenerationCleanup,
                )
                .await,
            );
        }
        let limits = limits(1, 64 * 1024, 8, 64 * 1024);
        let mut progress = ready_progress(scope, &operation);

        for (position, (seed, candidate_key)) in
            [4_u8, 5, 6].into_iter().zip(&candidate_keys).enumerate()
        {
            if position != 0 {
                let GenerationBatchTransition::Progressed(barrier) =
                    run_batch(&db, scope, &operation, &progress, limits)
                        .await
                        .expect("the next batch rechecks the reclaim barrier")
                else {
                    panic!("the next batch first restores its typed barrier marker");
                };
                assert!(barrier.gc_run_id.is_none());
                assert!(is_generation_candidate_start_cursor(
                    scope,
                    &operation,
                    barrier
                        .stage_cursor
                        .as_ref()
                        .expect("barrier marker is retained"),
                )
                .expect("barrier marker validates"));
                progress = barrier;
            }

            let GenerationBatchTransition::Progressed(staged) =
                run_batch(&db, scope, &operation, &progress, limits)
                    .await
                    .expect("one maximum-size batch stages")
            else {
                panic!("one remaining candidate produces one durable run");
            };
            let run_id = staged.gc_run_id.expect("staged run ID is retained");
            assert_eq!(
                staged
                    .candidate_cursor
                    .as_ref()
                    .expect("strict candidate cursor is retained")
                    .as_bytes(),
                candidate_key.as_ref()
            );
            let blob = work::BlobRef::new([seed; 32], 64);
            let member_key = index_keys::GlobalIndexV2Key::BlobGcCandidateMember {
                run_id,
                blob_hash: index_keys::BlobHash::new(*blob.hash()),
            }
            .to_bytes();
            let root = load_root(&db, run_id)
                .await
                .expect("staged root is readable")
                .expect("staged root exists");
            let attempt = work::GcScanAttempt::new(1).expect("test attempt is non-zero");
            for pass in [
                index_keys::BlobGcPass::First,
                index_keys::BlobGcPass::Second,
            ] {
                db.put(
                    member_mark_key(run_id, pass, attempt, blob),
                    index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcEntry(
                        work::BlobGcEntryValue::ReachabilityMark(
                            work::BlobGcReachabilityMarkValue {
                                run_id,
                                first_pass: pass == index_keys::BlobGcPass::First,
                                scan_attempt: attempt,
                                blob_hash: index_keys::BlobHash::new(*blob.hash()),
                                referenced: false,
                            },
                        ),
                    )),
                )
                .await
                .expect("terminal mark is written");
            }
            db.delete(candidate_key)
                .await
                .expect("disposed candidate is removed");
            db.delete(&member_key)
                .await
                .expect("disposed member is removed");
            let terminal_root = next_root_value(
                &root,
                work::BlobGcPhase::Delete {
                    completed_first_attempt: attempt,
                    completed_second_attempt: attempt,
                    member_cursor: Some(
                        IndexCursor::try_new(member_key).expect("member cursor validates"),
                    ),
                    stale_mark_cleanup: work::StaleMarkCleanup::Complete,
                },
                0,
                None,
            )
            .expect("terminal root validates");
            db.put(
                index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes(),
                encode_root(&terminal_root),
            )
            .await
            .expect("terminal root is written");

            let mut handoff_progress = staged;
            let terminal = loop {
                let transaction = db
                    .begin(IsolationLevel::SerializableSnapshot)
                    .await
                    .expect("terminal handoff transaction begins");
                let terminal = stage_generation_terminal_handoff(
                    &transaction,
                    scope,
                    &operation,
                    &handoff_progress,
                    limits,
                )
                .await
                .expect("terminal handoff validates");
                transaction
                    .commit()
                    .await
                    .expect("terminal handoff commits");
                let GenerationTerminalTransition::Progressed(next) = terminal else {
                    break terminal;
                };
                handoff_progress = next;
            };
            assert!(load_root(&db, run_id).await.unwrap().is_none());
            if position + 1 == candidate_keys.len() {
                let GenerationTerminalTransition::Complete(complete) = terminal else {
                    panic!("the final batch exhausts every candidate and foreign root");
                };
                assert!(complete.cursor.is_none());
            } else {
                let GenerationTerminalTransition::NextBatch(next) = terminal else {
                    panic!("a later candidate returns to exact batch acquisition");
                };
                assert_eq!(next.candidate_cursor, handoff_progress.candidate_cursor);
                assert!(next.gc_run_id.is_none());
                progress = next;
            }
        }

        for key in candidate_keys {
            assert!(db.get(key).await.unwrap().is_none());
        }
        assert!(scan_root_page(&db, None, NonZeroUsize::MIN)
            .await
            .expect("global root lane remains readable")
            .run_ids
            .is_empty());
    }

    #[tokio::test]
    async fn generation_batch_blocks_before_an_indivisible_root_member_pair() {
        let db = test_db("text-gc-indivisible-root-member").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let candidate_key = put_candidate(
            &db,
            scope,
            &operation,
            11,
            index_keys::BlobGcCandidateKeyOwner::GenerationCleanup,
        )
        .await;

        assert!(matches!(
            run_batch(
                &db,
                scope,
                &operation,
                &ready_progress(scope, &operation),
                limits(1, 64 * 1024, 1, 1),
            )
            .await
            .expect("indivisible batch returns a typed blocker"),
            GenerationBatchTransition::Blocked(IndexOperationBlocker::InvariantViolation)
        ));
        assert!(db.get(candidate_key).await.unwrap().is_some());
        let mut roots = db
            .scan_prefix(
                &index_keys::GlobalIndexV2Key::logical_prefix(
                    index_keys::GlobalIndexV2Kind::BlobGcRunRoot,
                ),
                (Bound::Unbounded, Bound::<Bytes>::Unbounded),
            )
            .await
            .expect("root lane is readable");
        assert!(roots.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn generation_batch_exhaustion_ignores_intent_owned_candidates() {
        let db = test_db("text-gc-generation-lane-exhaustion").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        put_candidate(
            &db,
            scope,
            &operation,
            21,
            index_keys::BlobGcCandidateKeyOwner::UploadIntent(
                TextUploadIntentId::from_bytes([22; 16]).expect("intent ID is non-nil"),
            ),
        )
        .await;

        let GenerationBatchTransition::Exhausted(counters) = run_batch(
            &db,
            scope,
            &operation,
            &ready_progress(scope, &operation),
            limits(4, 64 * 1024, 8, 64 * 1024),
        )
        .await
        .expect("foreign candidate lane is excluded") else {
            panic!("no generation candidate exhausts only this owner lane");
        };
        assert_eq!(counters, OperationCounters::default());
    }

    #[tokio::test]
    async fn generation_batch_atomically_rehomes_a_prior_operation_candidate() {
        let db = test_db("text-gc-candidate-owner-handoff").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let candidate_key = put_candidate(
            &db,
            scope,
            &operation,
            31,
            index_keys::BlobGcCandidateKeyOwner::GenerationCleanup,
        )
        .await;
        db.put(
            candidate_key.clone(),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcCandidate(
                work::BlobGcCandidateValue {
                    owner: work::BlobGcCandidateOwner::GenerationCleanup(
                        IndexOperationId::from_bytes([32; 16]).unwrap(),
                    ),
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    blob: work::BlobRef::new([31; 32], 64),
                },
            )),
        )
        .await
        .expect("prior-operation candidate is written");

        assert!(matches!(
            run_batch(
                &db,
                scope,
                &operation,
                &ready_progress(scope, &operation),
                limits(4, 64 * 1024, 2, 64 * 1024),
            )
            .await
            .expect("owner handoff is included in the bounded write set"),
            GenerationBatchTransition::Blocked(IndexOperationBlocker::InvariantViolation)
        ));
        assert!(scan_root_page(&db, None, NonZeroUsize::MIN)
            .await
            .expect("root lane remains readable")
            .run_ids
            .is_empty());

        let GenerationBatchTransition::Progressed(progress) = run_batch(
            &db,
            scope,
            &operation,
            &ready_progress(scope, &operation),
            limits(4, 64 * 1024, 8, 64 * 1024),
        )
        .await
        .expect("candidate owner handoff and root creation commit together") else {
            panic!("one prior-operation candidate creates one generation batch");
        };
        assert!(progress.gc_run_id.is_some());
        let candidate = db
            .get(candidate_key)
            .await
            .expect("reowned candidate remains readable")
            .expect("reowned candidate remains present");
        let index_values::IndexV2WorkValue::BlobGcCandidate(candidate) =
            index_values::decode_work_value(&candidate).expect("reowned candidate decodes")
        else {
            panic!("candidate key retains its typed candidate value");
        };
        assert_eq!(
            candidate.owner,
            work::BlobGcCandidateOwner::GenerationCleanup(operation.operation_id())
        );
        assert_eq!(progress.counters.output_operations, 3);
    }

    #[tokio::test]
    async fn global_root_lane_is_bounded_and_resumes_strictly_by_run_id() {
        let db = test_db("text-gc-root-page").await;
        let operation = operation();
        let run_ids = [
            BlobGcRunId::from_bytes([41; 16]).expect("run ID is non-nil"),
            BlobGcRunId::from_bytes([42; 16]).expect("run ID is non-nil"),
        ];
        for run_id in run_ids {
            let root = work::BlobGcRunRootValue::try_new(
                run_id,
                work::BlobGcRunOwner::GenerationCleanup {
                    scope: DataScope::LegacyUnscoped,
                    operation_id: operation.operation_id(),
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                },
                BlobGcRunRevision::initial(),
                0,
                None,
                work::BlobGcPhase::AwaitDeleteFences {
                    member_cursor: None,
                },
                1,
            )
            .expect("root validates");
            db.put(
                index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes(),
                index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcEntry(
                    work::BlobGcEntryValue::RunRoot(root),
                )),
            )
            .await
            .expect("root is written");
        }

        let first = scan_root_page(&db, None, NonZeroUsize::MIN)
            .await
            .expect("first root page scans");
        assert_eq!(first.run_ids, vec![run_ids[0]]);
        assert_eq!(first.resume_after, Some(run_ids[0]));
        assert!(!first.prefix_exhausted);

        let second = scan_root_page(&db, first.resume_after, NonZeroUsize::MIN)
            .await
            .expect("second root page resumes strictly");
        assert_eq!(second.run_ids, vec![run_ids[1]]);
        assert_eq!(second.resume_after, Some(run_ids[1]));
        assert!(!second.prefix_exhausted);

        let exhausted = scan_root_page(&db, second.resume_after, NonZeroUsize::MIN)
            .await
            .expect("root prefix exhaustion is explicit");
        assert!(exhausted.run_ids.is_empty());
        assert!(exhausted.resume_after.is_none());
        assert!(exhausted.prefix_exhausted);
    }

    #[tokio::test]
    async fn root_driver_closes_exact_member_set_before_fences_closed() {
        let db = test_db("text-gc-close-exact-member-set").await;
        let operation = operation();
        let (run_id, blobs) =
            staged_generation_root(&db, DataScope::LegacyUnscoped, &operation, &[51, 52]).await;
        let coordinator = coordinator("text-gc-close-exact-member-set");
        let driver =
            TextBlobGcDriver::new(coordinator.clone(), crate::search::text::BlobGcGate::new());
        let writer_epoch = WriterEpoch::from_bytes([53; 16]).expect("writer epoch is non-nil");

        for _ in 0..3 {
            assert_eq!(
                driver
                    .execute_root_step(&db, run_id, writer_epoch, 1_000)
                    .await
                    .expect("fence step succeeds"),
                BlobGcRootStep::Progressed
            );
        }
        let root = load_root(&db, run_id)
            .await
            .expect("root remains readable")
            .expect("root remains durable");
        assert!(matches!(root.phase, work::BlobGcPhase::FencesClosed));
        assert_eq!(root.revision.get(), 4);
        assert_eq!(root.attempt, 0);
        assert_eq!(root.not_before_unix_millis, None);

        for (offset, blob) in blobs.into_iter().enumerate() {
            assert!(matches!(
                coordinator
                    .reserve(
                        blob,
                        TextUploadIntentId::from_bytes([54 + u8::try_from(offset).unwrap(); 16])
                            .unwrap(),
                        writer_epoch,
                    )
                    .await,
                Err(blob_publication::BlobPublicationError::DeleteFenceClosed)
            ));
        }
    }

    #[tokio::test]
    async fn root_driver_persists_backoff_for_foreign_fence_and_same_run_quiescence() {
        let db = test_db("text-gc-fence-backoff").await;
        let operation = operation();
        let (run_id, blobs) =
            staged_generation_root(&db, DataScope::LegacyUnscoped, &operation, &[61]).await;
        let blob = blobs[0];
        let coordinator = coordinator("text-gc-fence-backoff");
        let other_run = BlobGcRunId::from_bytes([62; 16]).expect("run ID is non-nil");
        assert!(matches!(
            coordinator
                .begin_delete(BlobDeleteFenceKey::new(blob, other_run))
                .await
                .expect("foreign fence is acquired"),
            BeginBlobDelete::Acquired(_)
        ));
        let driver = TextBlobGcDriver::new(coordinator, crate::search::text::BlobGcGate::new());
        let writer_epoch = WriterEpoch::from_bytes([63; 16]).expect("writer epoch is non-nil");

        assert_eq!(
            driver
                .execute_root_step(&db, run_id, writer_epoch, 2_000)
                .await
                .expect("foreign fence becomes durable delay"),
            BlobGcRootStep::Delayed { delay_millis: 20 }
        );
        let delayed = load_root(&db, run_id).await.unwrap().unwrap();
        assert_eq!(delayed.attempt, 1);
        assert_eq!(delayed.not_before_unix_millis, Some(2_020));
        assert!(matches!(
            delayed.phase,
            work::BlobGcPhase::AwaitDeleteFences {
                member_cursor: None
            }
        ));
        assert_eq!(
            driver
                .execute_root_step(&db, run_id, writer_epoch, 2_010)
                .await
                .expect("persisted deadline is honored"),
            BlobGcRootStep::Delayed { delay_millis: 10 }
        );
        assert_eq!(load_root(&db, run_id).await.unwrap().unwrap(), delayed);
    }

    #[tokio::test]
    async fn root_driver_reacquires_same_run_after_quiescence_wait() {
        let db = test_db("text-gc-same-run-quiescence").await;
        let operation = operation();
        let (run_id, blobs) =
            staged_generation_root(&db, DataScope::LegacyUnscoped, &operation, &[71]).await;
        let coordinator = coordinator("text-gc-same-run-quiescence");
        let writer_epoch = WriterEpoch::from_bytes([72; 16]).expect("writer epoch is non-nil");
        let permit = coordinator
            .reserve(
                blobs[0],
                TextUploadIntentId::from_bytes([73; 16]).expect("intent ID is non-nil"),
                writer_epoch,
            )
            .await
            .expect("reservation keeps the fence nonquiescent");
        let driver =
            TextBlobGcDriver::new(coordinator.clone(), crate::search::text::BlobGcGate::new());

        assert_eq!(
            driver
                .execute_root_step(&db, run_id, writer_epoch, 3_000)
                .await
                .expect("nonquiescent member is delayed"),
            BlobGcRootStep::Delayed { delay_millis: 20 }
        );
        coordinator.expire_unused_permit(permit);
        assert_eq!(
            driver
                .execute_root_step(&db, run_id, writer_epoch, 3_020)
                .await
                .expect("same-run fence resumes after quiescence"),
            BlobGcRootStep::Progressed
        );
        let root = load_root(&db, run_id).await.unwrap().unwrap();
        assert_eq!(root.attempt, 0);
        assert_eq!(root.not_before_unix_millis, None);
        assert!(matches!(
            root.phase,
            work::BlobGcPhase::AwaitDeleteFences {
                member_cursor: Some(_)
            }
        ));
    }

    #[tokio::test]
    async fn root_driver_rejects_missing_immutable_member_before_fences_closed() {
        let db = test_db("text-gc-missing-member").await;
        let operation = operation();
        let (run_id, _) =
            staged_generation_root(&db, DataScope::LegacyUnscoped, &operation, &[81, 82]).await;
        db.delete(
            index_keys::GlobalIndexV2Key::BlobGcCandidateMember {
                run_id,
                blob_hash: index_keys::BlobHash::new([82; 32]),
            }
            .to_bytes(),
        )
        .await
        .expect("test removes one immutable member");
        let coordinator = coordinator("text-gc-missing-member");
        let driver = TextBlobGcDriver::new(coordinator, crate::search::text::BlobGcGate::new());
        let writer_epoch = WriterEpoch::from_bytes([83; 16]).expect("writer epoch is non-nil");

        assert_eq!(
            driver
                .execute_root_step(&db, run_id, writer_epoch, 4_000)
                .await
                .expect("remaining member fence closes"),
            BlobGcRootStep::Progressed
        );
        let error = driver
            .execute_root_step(&db, run_id, writer_epoch, 4_000)
            .await
            .expect_err("missing immutable member prevents phase transition");
        assert!(matches!(error, HelixDbError::IndexCatalogCorruption(_)));
        assert!(matches!(
            load_root(&db, run_id).await.unwrap().unwrap().phase,
            work::BlobGcPhase::AwaitDeleteFences { .. }
        ));
    }

    #[tokio::test]
    async fn root_driver_uses_two_distinct_complete_reachability_snapshots() {
        let db = test_db("text-gc-two-pass-reachability").await;
        let operation = operation();
        let (run_id, blobs) =
            staged_generation_root(&db, DataScope::LegacyUnscoped, &operation, &[91, 92]).await;
        let coordinator = coordinator("text-gc-two-pass-reachability");
        let driver = TextBlobGcDriver::new(coordinator, crate::search::text::BlobGcGate::new());
        let writer_epoch = WriterEpoch::from_bytes([93; 16]).expect("writer epoch is non-nil");
        for _ in 0..3 {
            assert_eq!(
                driver
                    .execute_root_step(&db, run_id, writer_epoch, 5_000)
                    .await
                    .expect("fence acquisition advances"),
                BlobGcRootStep::Progressed
            );
        }
        put_reference(&db, blobs[0], 94).await;
        let fences_closed = load_root(&db, run_id).await.unwrap().unwrap();
        replace_root(
            &db,
            &fences_closed,
            work::BlobGcPhase::FirstPass {
                writer_epoch,
                first_attempt: work::GcScanAttempt::new(1).unwrap(),
                reference_cursor: None,
            },
            0,
            None,
        )
        .await
        .expect("owner lane enters the first pass");

        assert_eq!(
            driver
                .execute_root_step(&db, run_id, writer_epoch, 5_000)
                .await
                .expect("first reference advances the first pass"),
            BlobGcRootStep::Progressed
        );
        put_reference(&db, blobs[1], 95).await;
        for _ in 0..8 {
            if matches!(
                load_root(&db, run_id).await.unwrap().unwrap().phase,
                work::BlobGcPhase::SecondPass { .. }
            ) {
                break;
            }
            assert_eq!(
                driver
                    .execute_root_step(&db, run_id, writer_epoch, 5_000)
                    .await
                    .expect("first pass advances"),
                BlobGcRootStep::Progressed
            );
        }
        assert!(matches!(
            load_root(&db, run_id).await.unwrap().unwrap().phase,
            work::BlobGcPhase::SecondPass {
                completed_first_attempt,
                second_attempt,
                reference_cursor: None,
                ..
            } if completed_first_attempt.get() == 1 && second_attempt.get() == 1
        ));
        assert_eq!(
            mark_references(&db, run_id, index_keys::BlobGcPass::First, 1).await,
            vec![true, false],
            "the first snapshot cannot observe the later reference"
        );

        for _ in 0..10 {
            if matches!(
                load_root(&db, run_id).await.unwrap().unwrap().phase,
                work::BlobGcPhase::Delete { .. }
            ) {
                break;
            }
            assert_eq!(
                driver
                    .execute_root_step(&db, run_id, writer_epoch, 5_000)
                    .await
                    .expect("second pass advances"),
                BlobGcRootStep::Progressed
            );
        }
        assert!(matches!(
            load_root(&db, run_id).await.unwrap().unwrap().phase,
            work::BlobGcPhase::Delete {
                completed_first_attempt,
                completed_second_attempt,
                member_cursor: None,
                stale_mark_cleanup: work::StaleMarkCleanup::Pending { mark_cursor: None },
            } if completed_first_attempt.get() == 1 && completed_second_attempt.get() == 1
        ));
        assert_eq!(
            mark_references(&db, run_id, index_keys::BlobGcPass::Second, 1).await,
            vec![true, true],
            "the second pass owns a fresh stable snapshot"
        );
    }

    #[tokio::test]
    async fn reachability_snapshot_loss_and_writer_takeover_restart_only_current_attempt() {
        let db = test_db("text-gc-reachability-restart").await;
        let operation = operation();
        let (run_id, _) =
            staged_generation_root(&db, DataScope::LegacyUnscoped, &operation, &[101]).await;
        let coordinator = coordinator("text-gc-reachability-restart");
        let driver = TextBlobGcDriver::new(coordinator, crate::search::text::BlobGcGate::new());
        let first_epoch = WriterEpoch::from_bytes([102; 16]).expect("writer epoch is non-nil");
        for _ in 0..2 {
            driver
                .execute_root_step(&db, run_id, first_epoch, 6_000)
                .await
                .expect("single-member fences close");
        }
        let fences_closed = load_root(&db, run_id).await.unwrap().unwrap();
        replace_root(
            &db,
            &fences_closed,
            work::BlobGcPhase::FirstPass {
                writer_epoch: first_epoch,
                first_attempt: work::GcScanAttempt::new(1).unwrap(),
                reference_cursor: None,
            },
            0,
            None,
        )
        .await
        .expect("owner lane enters the first pass");
        driver
            .execute_root_step(&db, run_id, first_epoch, 6_000)
            .await
            .expect("first attempt marks its member");
        assert_eq!(
            mark_references(&db, run_id, index_keys::BlobGcPass::First, 1).await,
            vec![false]
        );

        driver.reachability.lock().await.remove(&run_id);
        driver
            .execute_root_step(&db, run_id, first_epoch, 6_000)
            .await
            .expect("lost snapshot restarts the current pass");
        assert!(matches!(
            load_root(&db, run_id).await.unwrap().unwrap().phase,
            work::BlobGcPhase::FirstPass {
                writer_epoch,
                first_attempt,
                reference_cursor: None,
            } if writer_epoch == first_epoch && first_attempt.get() == 2
        ));
        driver
            .execute_root_step(&db, run_id, first_epoch, 6_000)
            .await
            .expect("second first-pass attempt marks its member");
        driver
            .execute_root_step(&db, run_id, first_epoch, 6_000)
            .await
            .expect("second first-pass attempt completes");
        assert_eq!(
            mark_references(&db, run_id, index_keys::BlobGcPass::First, 1).await,
            vec![false],
            "older-attempt marks remain ignored"
        );
        assert_eq!(
            mark_references(&db, run_id, index_keys::BlobGcPass::First, 2).await,
            vec![false]
        );

        let second_epoch = WriterEpoch::from_bytes([103; 16]).expect("writer epoch is non-nil");
        driver
            .execute_root_step(&db, run_id, second_epoch, 6_000)
            .await
            .expect("writer takeover restarts only the second pass");
        assert!(matches!(
            load_root(&db, run_id).await.unwrap().unwrap().phase,
            work::BlobGcPhase::SecondPass {
                completed_first_attempt,
                writer_epoch,
                second_attempt,
                reference_cursor: None,
            } if completed_first_attempt.get() == 2
                && writer_epoch == second_epoch
                && second_attempt.get() == 2
        ));

        driver
            .execute_root_step(&db, run_id, second_epoch, 6_000)
            .await
            .expect("second-pass attempt marks its member");
        assert_eq!(
            mark_references(&db, run_id, index_keys::BlobGcPass::Second, 2).await,
            vec![false]
        );
        driver.reachability.lock().await.remove(&run_id);
        driver
            .execute_root_step(&db, run_id, second_epoch, 6_000)
            .await
            .expect("lost second-pass snapshot restarts only the second pass");
        assert!(matches!(
            load_root(&db, run_id).await.unwrap().unwrap().phase,
            work::BlobGcPhase::SecondPass {
                completed_first_attempt,
                writer_epoch,
                second_attempt,
                reference_cursor: None,
            } if completed_first_attempt.get() == 2
                && writer_epoch == second_epoch
                && second_attempt.get() == 3
        ));
        assert_eq!(
            mark_references(&db, run_id, index_keys::BlobGcPass::First, 2).await,
            vec![false],
            "second-pass loss cannot invalidate the completed first pass"
        );
        assert_eq!(
            mark_references(&db, run_id, index_keys::BlobGcPass::Second, 2).await,
            vec![false],
            "the stale second-pass attempt remains ignored"
        );

        driver
            .execute_root_step(&db, run_id, second_epoch, 6_000)
            .await
            .expect("restarted second pass marks its member");
        driver
            .execute_root_step(&db, run_id, second_epoch, 6_000)
            .await
            .expect("restarted second pass completes");
        assert!(matches!(
            load_root(&db, run_id).await.unwrap().unwrap().phase,
            work::BlobGcPhase::Delete {
                completed_first_attempt,
                completed_second_attempt,
                ..
            } if completed_first_attempt.get() == 2
                && completed_second_attempt.get() == 3
        ));
    }

    #[tokio::test]
    async fn pass_completion_rejects_a_count_equal_mark_for_the_wrong_member() {
        let db = test_db("text-gc-wrong-current-mark").await;
        let operation = operation();
        let (run_id, _) =
            staged_generation_root(&db, DataScope::LegacyUnscoped, &operation, &[111]).await;
        let coordinator = coordinator("text-gc-wrong-current-mark");
        let driver = TextBlobGcDriver::new(coordinator, crate::search::text::BlobGcGate::new());
        let writer_epoch = WriterEpoch::from_bytes([112; 16]).expect("writer epoch is non-nil");
        for _ in 0..2 {
            driver
                .execute_root_step(&db, run_id, writer_epoch, 7_000)
                .await
                .expect("single-member fences close");
        }
        let fences_closed = load_root(&db, run_id).await.unwrap().unwrap();
        replace_root(
            &db,
            &fences_closed,
            work::BlobGcPhase::FirstPass {
                writer_epoch,
                first_attempt: work::GcScanAttempt::new(1).unwrap(),
                reference_cursor: None,
            },
            0,
            None,
        )
        .await
        .expect("owner lane enters the first pass");
        driver
            .execute_root_step(&db, run_id, writer_epoch, 7_000)
            .await
            .expect("first-pass member mark commits");

        let exact_mark_key = index_keys::GlobalIndexV2Key::BlobGcReachabilityMark {
            run_id,
            pass: index_keys::BlobGcPass::First,
            scan_attempt: NonZeroU64::MIN,
            blob_hash: index_keys::BlobHash::new([111; 32]),
        }
        .to_bytes();
        db.delete(exact_mark_key)
            .await
            .expect("exact mark is removed for corruption injection");
        let wrong_hash = index_keys::BlobHash::new([113; 32]);
        db.put(
            index_keys::GlobalIndexV2Key::BlobGcReachabilityMark {
                run_id,
                pass: index_keys::BlobGcPass::First,
                scan_attempt: NonZeroU64::MIN,
                blob_hash: wrong_hash,
            }
            .to_bytes(),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcEntry(
                work::BlobGcEntryValue::ReachabilityMark(work::BlobGcReachabilityMarkValue {
                    run_id,
                    first_pass: true,
                    scan_attempt: work::GcScanAttempt::new(1).unwrap(),
                    blob_hash: wrong_hash,
                    referenced: false,
                }),
            )),
        )
        .await
        .expect("count-equal wrong mark is injected");

        let error = driver
            .execute_root_step(&db, run_id, writer_epoch, 7_000)
            .await
            .expect_err("wrong member mark must not complete a pass");
        assert!(matches!(error, HelixDbError::IndexCatalogCorruption(_)));
        assert!(matches!(
            load_root(&db, run_id).await.unwrap().unwrap().phase,
            work::BlobGcPhase::FirstPass { .. }
        ));
    }

    #[tokio::test]
    async fn a_second_reachability_root_backs_off_without_blocking_the_active_pass() {
        let db = test_db("text-gc-concurrent-reachability-roots").await;
        let operation = operation();
        let first_run = BlobGcRunId::from_bytes([121; 16]).expect("first run ID is non-nil");
        let second_run = BlobGcRunId::from_bytes([122; 16]).expect("second run ID is non-nil");
        let writer_epoch = WriterEpoch::from_bytes([123; 16]).expect("writer epoch is non-nil");
        for (run_id, seed) in [(first_run, 124_u8), (second_run, 125_u8)] {
            let blob = work::BlobRef::new([seed; 32], 64);
            db.put(
                index_keys::GlobalIndexV2Key::BlobGcCandidateMember {
                    run_id,
                    blob_hash: index_keys::BlobHash::new(*blob.hash()),
                }
                .to_bytes(),
                index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcEntry(
                    work::BlobGcEntryValue::CandidateMember(work::BlobGcCandidateMemberValue {
                        run_id,
                        blob,
                        state: work::BlobGcMemberState::PendingDisposition { owner_cursor: None },
                    }),
                )),
            )
            .await
            .expect("concurrent member is written");
            let root = work::BlobGcRunRootValue::try_new(
                run_id,
                work::BlobGcRunOwner::GenerationCleanup {
                    scope: DataScope::LegacyUnscoped,
                    operation_id: operation.operation_id(),
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                },
                BlobGcRunRevision::initial(),
                0,
                None,
                work::BlobGcPhase::FirstPass {
                    writer_epoch,
                    first_attempt: work::GcScanAttempt::new(1).unwrap(),
                    reference_cursor: None,
                },
                1,
            )
            .expect("concurrent root validates");
            db.put(
                index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes(),
                encode_root(&root),
            )
            .await
            .expect("concurrent root is written");
        }
        let coordinator = coordinator("text-gc-concurrent-reachability-roots");
        let driver = TextBlobGcDriver::new(coordinator, crate::search::text::BlobGcGate::new());
        assert_eq!(
            driver
                .execute_root_step(&db, first_run, writer_epoch, 8_000)
                .await
                .expect("first pass acquires the runtime"),
            BlobGcRootStep::Progressed
        );
        let second = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            driver.execute_root_step(&db, second_run, writer_epoch, 8_000),
        )
        .await
        .expect("second root must not wait on the retained deletion gate")
        .expect("second root durably backs off");
        assert_eq!(second, BlobGcRootStep::Delayed { delay_millis: 20 });
        assert_eq!(
            driver
                .execute_root_step(&db, first_run, writer_epoch, 8_000)
                .await
                .expect("active pass can still complete"),
            BlobGcRootStep::Progressed
        );
    }

    #[tokio::test]
    async fn delete_phase_commits_cleanup_before_finish_and_retires_only_stale_marks() {
        let db = test_db("text-gc-delete-disposition").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let blob = work::BlobRef::new([211; 32], 64);
        let run_id = put_delete_root(
            &db,
            scope,
            &operation,
            &[(
                blob,
                work::BlobGcMemberState::PendingDisposition { owner_cursor: None },
                (false, false),
            )],
            true,
        )
        .await;
        let coordinator = coordinator("text-gc-delete-disposition");
        let driver =
            TextBlobGcDriver::new(coordinator.clone(), crate::search::text::BlobGcGate::new());
        let writer_epoch = WriterEpoch::from_bytes([212; 16]).expect("writer epoch is non-nil");

        assert_eq!(
            driver
                .execute_root_step(&db, run_id, writer_epoch, 10_000)
                .await
                .expect("object disposition commits"),
            BlobGcRootStep::Progressed
        );
        let member_key = index_keys::GlobalIndexV2Key::BlobGcCandidateMember {
            run_id,
            blob_hash: index_keys::BlobHash::new(*blob.hash()),
        }
        .to_bytes();
        let member_value = db
            .get(&member_key)
            .await
            .expect("member remains readable")
            .expect("cleanup-committed member remains durable");
        assert!(matches!(
            decode_member(run_id, &member_key, &member_value)
                .expect("cleanup-committed member decodes")
                .state,
            work::BlobGcMemberState::CleanupCommitted(work::BlobGcDisposition::DeletedOrAbsent)
        ));
        assert!(matches!(
            coordinator
                .reserve(
                    blob,
                    TextUploadIntentId::from_bytes([213; 16]).unwrap(),
                    writer_epoch,
                )
                .await,
            Err(blob_publication::BlobPublicationError::DeleteFenceClosed)
        ));

        assert_eq!(
            driver
                .execute_root_step(&db, run_id, writer_epoch, 10_000)
                .await
                .expect("coordinator finish precedes ordered member removal"),
            BlobGcRootStep::Progressed
        );
        assert!(db.get(&member_key).await.unwrap().is_none());
        assert!(
            coordinator
                .reserve(
                    blob,
                    TextUploadIntentId::from_bytes([214; 16]).unwrap(),
                    writer_epoch,
                )
                .await
                .is_ok(),
            "finish_delete reopens only after CleanupCommitted is durable"
        );

        for _ in 0..4 {
            assert_eq!(
                driver
                    .execute_root_step(&db, run_id, writer_epoch, 10_000)
                    .await
                    .expect("bounded stale-mark cleanup advances"),
                BlobGcRootStep::Progressed
            );
        }
        let root = load_root(&db, run_id).await.unwrap().unwrap();
        assert!(matches!(
            root.phase,
            work::BlobGcPhase::Delete {
                stale_mark_cleanup: work::StaleMarkCleanup::Complete,
                ..
            }
        ));
        assert!(
            db.get(member_mark_key(
                run_id,
                index_keys::BlobGcPass::First,
                work::GcScanAttempt::new(1).unwrap(),
                blob,
            ))
            .await
            .unwrap()
            .is_none(),
            "older-attempt mark is removed"
        );
        assert_eq!(
            mark_references(&db, run_id, index_keys::BlobGcPass::First, 2).await,
            vec![false]
        );
        assert_eq!(
            mark_references(&db, run_id, index_keys::BlobGcPass::Second, 2).await,
            vec![false]
        );
        assert_eq!(
            driver
                .execute_root_step(&db, run_id, writer_epoch, 10_000)
                .await
                .expect("generation terminal root waits for its operation handoff"),
            BlobGcRootStep::Idle
        );
    }

    #[tokio::test]
    async fn delete_phase_waits_for_claimed_intent_then_releases_permit_and_anchors() {
        let db = test_db("text-gc-delete-permit-owner").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let blob = work::BlobRef::new([216; 32], 64);
        let run_id = put_delete_root(
            &db,
            scope,
            &operation,
            &[(
                blob,
                work::BlobGcMemberState::PendingDisposition { owner_cursor: None },
                (false, false),
            )],
            false,
        )
        .await;
        let coordinator = coordinator("text-gc-delete-permit-owner");
        let writer_epoch = WriterEpoch::from_bytes([217; 16]).expect("writer epoch is non-nil");
        let intent_id = TextUploadIntentId::from_bytes([218; 16]).expect("intent ID is non-nil");
        let permit = coordinator
            .reserve(blob, intent_id, writer_epoch)
            .await
            .expect("permit is reserved before its delete fence closes");
        coordinator.expire_unused_permit(permit);
        let split = work::SplitRef::try_new(blob, 0, 0, 0, blob.size())
            .expect("test split sizes are valid");
        let intent = work::TextUploadIntentValue::try_new(
            intent_id,
            TextIntentRevision::initial(),
            operation.index_id(),
            operation.identity().clone(),
            operation.generation(),
            work::TextPartition::Unpartitioned,
            blob,
            permit.id(),
            work::TextUploadOwner::Build {
                operation_id: operation.operation_id(),
                expected_operation_revision: operation.operation_revision(),
            },
            work::TextUploadAttachment::BuildArtifact {
                artifact_ordinal: 0,
                split,
            },
            work::TextUploadPhase::Reclaimable(work::ReclaimAssignment::Assigned(run_id)),
            0,
            work::TextUploadWorkState::Queued {
                not_before_unix_millis: None,
            },
        )
        .expect("assigned reclaim intent validates");
        let claimed_intent = intent
            .claim(OperationClaim {
                writer_epoch,
                sequence: ClaimSequence::new(1).expect("claim sequence is non-zero"),
            })
            .expect("assigned reclaim intent can be claimed");
        let claimed_rows = super::super::upload::upload_anchor_rows(scope, &claimed_intent)
            .expect("assigned intent rows encode");
        db.put(&claimed_rows.intent_key, &claimed_rows.intent_value)
            .await
            .unwrap();
        db.put(&claimed_rows.pointer_key, &claimed_rows.pointer_value)
            .await
            .unwrap();
        let (candidate_key, candidate_value) = disposition_intent_candidate(scope, &intent);
        db.put(&candidate_key, candidate_value).await.unwrap();
        let driver =
            TextBlobGcDriver::new(coordinator.clone(), crate::search::text::BlobGcGate::new());

        assert!(matches!(
            driver
                .execute_root_step(&db, run_id, writer_epoch, 10_500)
                .await
                .expect("claimed upload owner delays disposition"),
            BlobGcRootStep::Delayed { .. }
        ));
        assert_eq!(
            db.get(&claimed_rows.intent_key).await.unwrap(),
            Some(claimed_rows.intent_value)
        );
        assert!(db.get(&claimed_rows.pointer_key).await.unwrap().is_some());
        assert!(db.get(&candidate_key).await.unwrap().is_some());
        assert_eq!(
            coordinator
                .publication_status(&permit)
                .await
                .expect("claimed intent retains its permit"),
            blob_publication::BlobPublicationStatus::ExpiredUnused
        );

        let queued_intent = claimed_intent
            .transient_failure(0)
            .expect("upload owner can return its claim to the queue");
        let queued_rows = super::super::upload::upload_anchor_rows(scope, &queued_intent)
            .expect("requeued intent rows encode");
        db.put(&queued_rows.intent_key, &queued_rows.intent_value)
            .await
            .unwrap();
        db.put(&queued_rows.pointer_key, &queued_rows.pointer_value)
            .await
            .unwrap();
        assert_eq!(
            driver
                .execute_root_step(&db, run_id, writer_epoch, 20_500)
                .await
                .expect("assigned permit owner is cleaned"),
            BlobGcRootStep::Progressed
        );
        assert!(db.get(&queued_rows.intent_key).await.unwrap().is_none());
        assert!(db.get(&queued_rows.pointer_key).await.unwrap().is_none());
        assert!(db.get(&candidate_key).await.unwrap().is_none());
        assert!(matches!(
            coordinator.publication_status(&permit).await,
            Err(blob_publication::BlobPublicationError::UnknownPermit)
        ));
        assert_eq!(
            driver
                .execute_root_step(&db, run_id, writer_epoch, 10_500)
                .await
                .expect("owner exhaustion commits the member cleanup"),
            BlobGcRootStep::Progressed
        );
        let member_key = index_keys::GlobalIndexV2Key::BlobGcCandidateMember {
            run_id,
            blob_hash: index_keys::BlobHash::new(*blob.hash()),
        }
        .to_bytes();
        let member_value = db.get(&member_key).await.unwrap().unwrap();
        assert!(matches!(
            decode_member(run_id, &member_key, &member_value)
                .expect("cleaned member decodes")
                .state,
            work::BlobGcMemberState::CleanupCommitted(work::BlobGcDisposition::DeletedOrAbsent)
        ));
    }

    #[tokio::test]
    async fn multi_member_delete_reopens_after_the_first_ordered_removal() {
        let db = test_db("text-gc-multi-member-delete-reopen").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let first_blob = work::BlobRef::new([219; 32], 64);
        let second_blob = work::BlobRef::new([220; 32], 64);
        let run_id = put_delete_root(
            &db,
            scope,
            &operation,
            &[
                (
                    first_blob,
                    work::BlobGcMemberState::CleanupCommitted(
                        work::BlobGcDisposition::ReferencedPreserved,
                    ),
                    (true, true),
                ),
                (
                    second_blob,
                    work::BlobGcMemberState::CleanupCommitted(
                        work::BlobGcDisposition::ReferencedPreserved,
                    ),
                    (true, true),
                ),
            ],
            false,
        )
        .await;
        let first_key = index_keys::GlobalIndexV2Key::BlobGcCandidateMember {
            run_id,
            blob_hash: index_keys::BlobHash::new(*first_blob.hash()),
        }
        .to_bytes();
        let second_key = index_keys::GlobalIndexV2Key::BlobGcCandidateMember {
            run_id,
            blob_hash: index_keys::BlobHash::new(*second_blob.hash()),
        }
        .to_bytes();
        assert!(
            first_key < second_key,
            "member order is canonical hash order"
        );

        let coordinator = coordinator("text-gc-multi-member-delete-reopen");
        for blob in [first_blob, second_blob] {
            assert!(matches!(
                coordinator
                    .begin_delete(BlobDeleteFenceKey::new(blob, run_id))
                    .await
                    .expect("member delete fence is restored"),
                BeginBlobDelete::Acquired(_)
            ));
        }
        let writer_epoch = WriterEpoch::from_bytes([221; 16]).expect("writer epoch is non-nil");
        let driver =
            TextBlobGcDriver::new(coordinator.clone(), crate::search::text::BlobGcGate::new());
        assert_eq!(
            driver
                .execute_root_step(&db, run_id, writer_epoch, 11_000)
                .await
                .expect("first ordered member is removed"),
            BlobGcRootStep::Progressed
        );
        assert!(db.get(&first_key).await.unwrap().is_none());
        assert!(db.get(&second_key).await.unwrap().is_some());
        drop(driver);

        let reopened_driver =
            TextBlobGcDriver::new(coordinator, crate::search::text::BlobGcGate::new());
        assert_eq!(
            reopened_driver
                .execute_root_step(&db, run_id, writer_epoch, 11_000)
                .await
                .expect("reopened driver resumes at the second ordered member"),
            BlobGcRootStep::Progressed
        );
        assert!(db.get(&first_key).await.unwrap().is_none());
        assert!(db.get(&second_key).await.unwrap().is_none());
        let root = load_root(&db, run_id)
            .await
            .expect("root remains readable")
            .expect("root remains until mark cleanup");
        let work::BlobGcPhase::Delete {
            member_cursor: Some(member_cursor),
            ..
        } = root.phase
        else {
            panic!("Delete root retains its final ordered member cursor");
        };
        assert_eq!(member_cursor.as_bytes(), second_key.as_ref());
    }

    #[tokio::test]
    async fn cleanup_committed_recovery_never_repeats_object_delete() {
        let db = test_db("text-gc-cleanup-committed-no-repeat").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let blob = work::BlobRef::new([221; 32], 64);
        let run_id = put_delete_root(
            &db,
            scope,
            &operation,
            &[(
                blob,
                work::BlobGcMemberState::CleanupCommitted(
                    work::BlobGcDisposition::ReferencedPreserved,
                ),
                (true, true),
            )],
            false,
        )
        .await;
        let store = Arc::new(InMemory::new());
        let db_path = "text-gc-cleanup-committed-no-repeat-blobs";
        let location = crate::search::text::blob_object_store_path(db_path, *blob.hash());
        store
            .put(
                &location,
                PutPayload::from_bytes(Bytes::from(vec![221; 64])),
            )
            .await
            .expect("replacement object is written");
        let coordinator = Arc::new(ProcessLocalBlobPublicationCoordinator::new(
            store.clone(),
            db_path,
            BlobPublicationTiming::default(),
        ));
        assert!(matches!(
            coordinator
                .begin_delete(BlobDeleteFenceKey::new(blob, run_id))
                .await
                .expect("same run fence is restored"),
            BeginBlobDelete::Acquired(_)
        ));
        let driver = TextBlobGcDriver::new(coordinator, crate::search::text::BlobGcGate::new());
        let writer_epoch = WriterEpoch::from_bytes([222; 16]).expect("writer epoch is non-nil");

        assert_eq!(
            driver
                .execute_root_step(&db, run_id, writer_epoch, 11_000)
                .await
                .expect("CleanupCommitted recovery only finishes and removes"),
            BlobGcRootStep::Progressed
        );
        assert!(
            store.get(&location).await.is_ok(),
            "CleanupCommitted recovery cannot call object delete again"
        );
    }
}
