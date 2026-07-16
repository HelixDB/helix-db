//! Bounded owner-preserving preparation for text generation cleanup.
//!
//! Abort and drop cleanup first run a scoped ownership barrier over every
//! upload intent. An intent already assigned to an upload-reclaim root keeps
//! the generation operation waiting until that independent root disappears.
//! Only after the barrier completes does this module materialize one
//! operation-owned candidate per exact blob referenced by manifest pages,
//! build artifacts, or retained upload intents.
//!
//! Candidate preparation never deletes or rewrites an owner or global
//! reachability entry. Later delete-fence stages can therefore prove that the
//! complete immutable candidate set was durable before logical retirement.
//! The zero-hash generation-candidate key is used only as a typed cursor marker
//! between the ownership barrier and manifest scan; no marker row is written.

use std::collections::HashMap;
use std::ops::Bound;

use bytes::Bytes;
use slatedb::{Db, DbTransaction, IsolationLevel};

use crate::config::SearchIndexBatchLimits;
use crate::encoding::v1::keys::index_v2 as index_keys;
use crate::encoding::v1::keys::tenant::DataScope;
use crate::encoding::v1::keys::{DataKeyKind, GlobalKeyKind, Key};
use crate::encoding::v1::values::index_v2 as index_values;
use crate::error::{HelixDbError, Result};
use crate::index_v2::blob_publication;
use crate::index_v2::outbox::IndexOperationStepResult;
use crate::index_v2::work;
use crate::index_v2::{
    BuildOperationOutcome, GcProgress, IndexCursor, IndexOperationExecutionState,
    IndexOperationOutcome, IndexOperationProgress, IndexOperationRecord, IndexV2MetadataValue,
    OperationCounters, PrefixScanProgress, TextBuildProgress, TextCleanupProgress,
};

/// Runs one bounded abort/drop cleanup transition.
pub(super) async fn step_cleanup(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &TextCleanupProgress,
    aborting: bool,
    limits: SearchIndexBatchLimits,
) -> Result<IndexOperationStepResult> {
    match progress {
        TextCleanupProgress::BeginDrain(progress) => Ok(IndexOperationStepResult::Blocked(
            if progress.drain_epoch.is_some() {
                crate::index_v2::IndexOperationBlocker::InvariantViolation
            } else {
                crate::index_v2::IndexOperationBlocker::ReaderCoordinationUnavailable
            },
        )),
        TextCleanupProgress::PrepareCandidates(progress) => {
            prepare_candidates(transaction, scope, operation, progress, aborting, limits).await
        }
        TextCleanupProgress::AcquireDeleteFences(progress) => {
            acquire_delete_fences(transaction, scope, operation, progress, aborting, limits).await
        }
        TextCleanupProgress::RetireManifest(progress) => {
            retire_manifest_references(transaction, scope, operation, progress, aborting, limits)
                .await
        }
        TextCleanupProgress::RetireArtifacts(progress) => {
            retire_artifact_owners(transaction, scope, operation, progress, aborting, limits).await
        }
        TextCleanupProgress::MarkReachability(progress) => {
            observe_reachability_passes(transaction, scope, operation, progress, aborting).await
        }
        TextCleanupProgress::DeleteBlobs(progress) => {
            match super::blob_gc::stage_generation_terminal_handoff(
                transaction,
                scope,
                operation,
                progress,
                limits,
            )
            .await?
            {
                super::blob_gc::GenerationTerminalTransition::Progressed(progress) => Ok(
                    progressed_cleanup(aborting, TextCleanupProgress::DeleteBlobs(progress)),
                ),
                super::blob_gc::GenerationTerminalTransition::NextBatch(progress) => {
                    Ok(progressed_cleanup(
                        aborting,
                        TextCleanupProgress::AcquireDeleteFences(progress),
                    ))
                }
                super::blob_gc::GenerationTerminalTransition::Complete(progress) => Ok(
                    progressed_cleanup(aborting, TextCleanupProgress::DeleteEntityState(progress)),
                ),
                super::blob_gc::GenerationTerminalTransition::Waiting => {
                    Ok(IndexOperationStepResult::TransientFailure)
                }
                super::blob_gc::GenerationTerminalTransition::Blocked(blocker) => {
                    Ok(IndexOperationStepResult::Blocked(blocker))
                }
            }
        }
        TextCleanupProgress::DeleteEntityState(progress) => {
            delete_physical_generation_rows(
                transaction,
                scope,
                operation,
                progress,
                aborting,
                limits,
            )
            .await
        }
        TextCleanupProgress::FinishDrain(progress) => Ok(IndexOperationStepResult::Blocked(
            if progress.drain_epoch.is_some() {
                crate::index_v2::IndexOperationBlocker::InvariantViolation
            } else {
                crate::index_v2::IndexOperationBlocker::ReaderCoordinationUnavailable
            },
        )),
        TextCleanupProgress::RetireUploadIntents(_) => {
            Ok(IndexOperationStepResult::TransientFailure)
        }
        TextCleanupProgress::Finalize(_) => Ok(IndexOperationStepResult::Completed(if aborting {
            IndexOperationOutcome::Build(BuildOperationOutcome::Aborted)
        } else {
            IndexOperationOutcome::DropSucceeded
        })),
    }
}

/// Closed order for post-GC generation-row validation and deletion.
///
/// The first three lanes are absence barriers: deleting their rows here could
/// discard publication permits or blob ownership outside the GC protocol. The
/// remaining lanes contain only generation-local state whose blob references
/// were already retired by the completed immutable batches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PhysicalCleanupLane {
    UploadIntentBarrier,
    ArtifactBarrier,
    CandidateBarrier,
    ActiveMutationProof,
    ManifestPage,
    ManifestRoot,
    EntityState,
    BuildDelta,
    AppliedState,
}

impl PhysicalCleanupLane {
    const fn first() -> Self {
        Self::UploadIntentBarrier
    }

    const fn next(self) -> Option<Self> {
        match self {
            Self::UploadIntentBarrier => Some(Self::ArtifactBarrier),
            Self::ArtifactBarrier => Some(Self::CandidateBarrier),
            Self::CandidateBarrier => Some(Self::ActiveMutationProof),
            Self::ActiveMutationProof => Some(Self::ManifestPage),
            Self::ManifestPage => Some(Self::ManifestRoot),
            Self::ManifestRoot => Some(Self::EntityState),
            Self::EntityState => Some(Self::BuildDelta),
            Self::BuildDelta => Some(Self::AppliedState),
            Self::AppliedState => None,
        }
    }

    const fn record_kind(self) -> index_keys::IndexV2RecordKind {
        match self {
            Self::UploadIntentBarrier => index_keys::IndexV2RecordKind::TextUploadIntent,
            Self::ArtifactBarrier => index_keys::IndexV2RecordKind::TextBuildArtifact,
            Self::CandidateBarrier => index_keys::IndexV2RecordKind::BlobGcCandidate,
            Self::ActiveMutationProof => index_keys::IndexV2RecordKind::ActiveMutationCommitProof,
            Self::ManifestPage => index_keys::IndexV2RecordKind::TextManifestPage,
            Self::ManifestRoot => index_keys::IndexV2RecordKind::TextManifestRoot,
            Self::EntityState => index_keys::IndexV2RecordKind::TextEntityState,
            Self::BuildDelta => index_keys::IndexV2RecordKind::BuildDelta,
            Self::AppliedState => index_keys::IndexV2RecordKind::AppliedState,
        }
    }

    const fn deletes_rows(self) -> bool {
        matches!(
            self,
            Self::ActiveMutationProof
                | Self::ManifestPage
                | Self::ManifestRoot
                | Self::EntityState
                | Self::BuildDelta
                | Self::AppliedState
        )
    }
}

/// Deletes one bounded post-GC physical batch and retains an exact typed cursor.
async fn delete_physical_generation_rows(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &PrefixScanProgress,
    aborting: bool,
    limits: SearchIndexBatchLimits,
) -> Result<IndexOperationStepResult> {
    let mut lane = match progress.cursor.as_ref() {
        Some(cursor) => physical_cleanup_lane_from_cursor(scope, operation, cursor)?,
        None => PhysicalCleanupLane::first(),
    };
    let mut resume = progress.cursor.as_ref().map(IndexCursor::as_bytes);
    loop {
        let mut rows =
            scan_generation(transaction, scope, lane.record_kind(), operation, resume).await?;
        let mut batch = RetirementBatch::new(progress.counters, limits, 0);
        let mut completed_cursor = None;
        while batch.can_visit_another() {
            let Some(row) = rows.next().await? else {
                break;
            };
            validate_physical_cleanup_row(scope, operation, lane, &row.key, &row.value)?;
            if !lane.deletes_rows() {
                return Err(corruption(format!(
                    "text physical cleanup found a surviving {:?} row",
                    lane.record_kind()
                )));
            }
            match batch.admit(
                measured_row(&row.key, Some(&row.value)),
                vec![row.key.clone()],
            )? {
                RetirementAdmission::Admitted => {
                    completed_cursor =
                        Some(IndexCursor::try_new(row.key).map_err(operation_error)?);
                }
                RetirementAdmission::Full => break,
                RetirementAdmission::Indivisible => {
                    return Ok(IndexOperationStepResult::Blocked(
                        crate::index_v2::IndexOperationBlocker::InvariantViolation,
                    ));
                }
            }
        }
        if let Some(cursor) = completed_cursor {
            batch.stage(transaction)?;
            return Ok(progressed_cleanup(
                aborting,
                TextCleanupProgress::DeleteEntityState(PrefixScanProgress {
                    cursor: Some(cursor),
                    counters: batch.finish()?,
                }),
            ));
        }
        let Some(next) = lane.next() else {
            return Ok(progressed_cleanup(
                aborting,
                TextCleanupProgress::FinishDrain(crate::index_v2::DrainProgress {
                    drain_epoch: None,
                    counters: progress.counters,
                }),
            ));
        };
        lane = next;
        resume = None;
    }
}

/// Resolves a persisted physical cursor to its one legal generation lane.
fn physical_cleanup_lane_from_cursor(
    scope: DataScope,
    operation: &IndexOperationRecord,
    cursor: &IndexCursor,
) -> Result<PhysicalCleanupLane> {
    let Key::Data {
        scope: cursor_scope,
        kind: DataKeyKind::IndexV2(key),
    } = Key::parse_from_slice(scope, cursor.as_bytes())?
    else {
        return Err(corruption(
            "text physical cleanup cursor is not a scoped V2 key",
        ));
    };
    if cursor_scope != scope {
        return Err(corruption(
            "text physical cleanup cursor names another scope",
        ));
    }
    let (lane, index_id, generation) = match key {
        index_keys::IndexV2Key::ActiveMutationCommitProof(key) => (
            PhysicalCleanupLane::ActiveMutationProof,
            key.index_id,
            key.generation,
        ),
        index_keys::IndexV2Key::TextManifestPage(key) => (
            PhysicalCleanupLane::ManifestPage,
            key.root.index_id,
            key.root.generation,
        ),
        index_keys::IndexV2Key::TextManifestRoot(key) => (
            PhysicalCleanupLane::ManifestRoot,
            key.index_id,
            key.generation,
        ),
        index_keys::IndexV2Key::TextEntityState(key) => (
            PhysicalCleanupLane::EntityState,
            key.root.index_id,
            key.root.generation,
        ),
        index_keys::IndexV2Key::BuildDelta(key) => (
            PhysicalCleanupLane::BuildDelta,
            key.index_id,
            key.generation,
        ),
        index_keys::IndexV2Key::AppliedState(key) => (
            PhysicalCleanupLane::AppliedState,
            key.index_id,
            key.generation,
        ),
        index_keys::IndexV2Key::IndexRecord(_)
        | index_keys::IndexV2Key::Operation(_)
        | index_keys::IndexV2Key::SecondaryEntry(_)
        | index_keys::IndexV2Key::TextUploadIntent(_)
        | index_keys::IndexV2Key::TextBuildArtifact(_)
        | index_keys::IndexV2Key::BlobGcCandidate(_)
        | index_keys::IndexV2Key::VectorPartitionMapping(_) => {
            return Err(corruption(
                "text physical cleanup cursor is outside its deletable lane set",
            ));
        }
    };
    if index_id != operation.index_id() || generation != operation.generation() {
        return Err(corruption(
            "text physical cleanup cursor names another generation",
        ));
    }
    Ok(lane)
}

/// Validates the redundant typed key/value ownership of one physical row.
fn validate_physical_cleanup_row(
    scope: DataScope,
    operation: &IndexOperationRecord,
    lane: PhysicalCleanupLane,
    key: &[u8],
    value: &[u8],
) -> Result<()> {
    match lane {
        PhysicalCleanupLane::UploadIntentBarrier => {
            decode_intent_row(scope, operation, key, value)?;
        }
        PhysicalCleanupLane::ArtifactBarrier => {
            super::attachment::decode_build_artifact(scope, operation, key, value)?;
        }
        PhysicalCleanupLane::CandidateBarrier => {
            let Key::Data {
                scope: key_scope,
                kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::BlobGcCandidate(key)),
            } = Key::parse_from_slice(scope, key)?
            else {
                return Err(corruption(
                    "text physical candidate lane yielded another key kind",
                ));
            };
            let index_values::IndexV2WorkValue::BlobGcCandidate(candidate) =
                index_values::decode_work_value(value)?
            else {
                return Err(corruption(
                    "text physical candidate key contains another value kind",
                ));
            };
            let owner_matches = match (key.owner, candidate.owner) {
                (
                    index_keys::BlobGcCandidateKeyOwner::GenerationCleanup,
                    work::BlobGcCandidateOwner::GenerationCleanup(operation_id),
                ) => operation_id == operation.operation_id(),
                (
                    index_keys::BlobGcCandidateKeyOwner::UploadIntent(key_intent),
                    work::BlobGcCandidateOwner::UploadIntent(value_intent),
                ) => key_intent == value_intent,
                (
                    index_keys::BlobGcCandidateKeyOwner::GenerationCleanup,
                    work::BlobGcCandidateOwner::UploadIntent(_),
                )
                | (
                    index_keys::BlobGcCandidateKeyOwner::UploadIntent(_),
                    work::BlobGcCandidateOwner::GenerationCleanup(_),
                ) => false,
            };
            if key_scope != scope
                || key.index_id != operation.index_id()
                || key.generation != operation.generation()
                || key.blob_hash.as_bytes() != candidate.blob.hash()
                || candidate.index_id != operation.index_id()
                || candidate.generation != operation.generation()
                || !owner_matches
            {
                return Err(corruption(
                    "text physical candidate key/value ownership mismatch",
                ));
            }
        }
        PhysicalCleanupLane::ActiveMutationProof => {
            let Key::Data {
                scope: key_scope,
                kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::ActiveMutationCommitProof(key)),
            } = Key::parse_from_slice(scope, key)?
            else {
                return Err(corruption(
                    "text physical proof lane yielded another key kind",
                ));
            };
            let index_values::IndexV2WorkValue::ActiveMutationCommitProof(proof) =
                index_values::decode_work_value(value)?
            else {
                return Err(corruption(
                    "text physical proof key contains another value kind",
                ));
            };
            if key_scope != scope
                || key.index_id != operation.index_id()
                || key.generation != operation.generation()
                || key.intent_id != proof.intent_id
                || proof.index_id != operation.index_id()
                || proof.generation != operation.generation()
            {
                return Err(corruption(
                    "text physical proof key/value ownership mismatch",
                ));
            }
        }
        PhysicalCleanupLane::ManifestPage => {
            decode_manifest_page(scope, operation, key, value)?;
        }
        PhysicalCleanupLane::ManifestRoot => {
            let Key::Data {
                scope: key_scope,
                kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::TextManifestRoot(key)),
            } = Key::parse_from_slice(scope, key)?
            else {
                return Err(corruption(
                    "text physical manifest-root lane yielded another key kind",
                ));
            };
            let index_values::IndexV2WorkValue::TextManifestRoot(root) =
                index_values::decode_work_value(value)?
            else {
                return Err(corruption(
                    "text physical manifest-root key contains another value kind",
                ));
            };
            if key_scope != scope
                || key.index_id != operation.index_id()
                || key.generation != operation.generation()
                || key.partition != root.partition().fingerprint()
                || root.index_id() != operation.index_id()
                || root.generation() != operation.generation()
            {
                return Err(corruption(
                    "text physical manifest-root key/value ownership mismatch",
                ));
            }
        }
        PhysicalCleanupLane::EntityState => {
            let Key::Data {
                scope: key_scope,
                kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::TextEntityState(key)),
            } = Key::parse_from_slice(scope, key)?
            else {
                return Err(corruption(
                    "text physical entity-state lane yielded another key kind",
                ));
            };
            let index_values::IndexV2WorkValue::TextEntityState(state) =
                index_values::decode_work_value(value)?
            else {
                return Err(corruption(
                    "text physical entity-state key contains another value kind",
                ));
            };
            if key_scope != scope
                || key.root.index_id != operation.index_id()
                || key.root.generation != operation.generation()
                || key.root.partition != state.partition.fingerprint()
                || key.entity.kind != state.entity_kind
                || key.entity.id != state.entity_id
                || state.index_id != operation.index_id()
                || state.generation != operation.generation()
            {
                return Err(corruption(
                    "text physical entity-state key/value ownership mismatch",
                ));
            }
        }
        PhysicalCleanupLane::BuildDelta | PhysicalCleanupLane::AppliedState => {
            let Key::Data {
                scope: key_scope,
                kind: DataKeyKind::IndexV2(key_kind),
            } = Key::parse_from_slice(scope, key)?
            else {
                return Err(corruption(
                    "text physical builder-state lane yielded another key kind",
                ));
            };
            let key = match (lane, key_kind) {
                (PhysicalCleanupLane::BuildDelta, index_keys::IndexV2Key::BuildDelta(key))
                | (PhysicalCleanupLane::AppliedState, index_keys::IndexV2Key::AppliedState(key)) => {
                    key
                }
                (PhysicalCleanupLane::BuildDelta, _) | (PhysicalCleanupLane::AppliedState, _) => {
                    return Err(corruption(
                        "text physical builder-state lane yielded another V2 key",
                    ));
                }
                (
                    PhysicalCleanupLane::UploadIntentBarrier
                    | PhysicalCleanupLane::ArtifactBarrier
                    | PhysicalCleanupLane::CandidateBarrier
                    | PhysicalCleanupLane::ActiveMutationProof
                    | PhysicalCleanupLane::ManifestPage
                    | PhysicalCleanupLane::ManifestRoot
                    | PhysicalCleanupLane::EntityState,
                    _,
                ) => {
                    return Err(corruption(
                        "text physical cleanup reached an impossible lane match",
                    ));
                }
            };
            let value_matches = match (lane, index_values::decode_work_value(value)?) {
                (
                    PhysicalCleanupLane::BuildDelta,
                    index_values::IndexV2WorkValue::CoalescedBuildDelta(delta),
                ) => {
                    delta.index_id == operation.index_id()
                        && delta.generation == operation.generation()
                        && delta.entity_kind == key.entity.kind
                        && delta.entity_id == key.entity.id
                }
                (
                    PhysicalCleanupLane::AppliedState,
                    index_values::IndexV2WorkValue::AppliedEntityState(state),
                ) => {
                    state.index_id == operation.index_id()
                        && state.generation == operation.generation()
                        && state.entity_kind == key.entity.kind
                        && state.entity_id == key.entity.id
                        && matches!(state.state, work::AppliedFamilyState::Text(_))
                }
                (PhysicalCleanupLane::BuildDelta, _) | (PhysicalCleanupLane::AppliedState, _) => {
                    false
                }
                (
                    PhysicalCleanupLane::UploadIntentBarrier
                    | PhysicalCleanupLane::ArtifactBarrier
                    | PhysicalCleanupLane::CandidateBarrier
                    | PhysicalCleanupLane::ActiveMutationProof
                    | PhysicalCleanupLane::ManifestPage
                    | PhysicalCleanupLane::ManifestRoot
                    | PhysicalCleanupLane::EntityState,
                    _,
                ) => false,
            };
            if key_scope != scope
                || key.index_id != operation.index_id()
                || key.generation != operation.generation()
                || !value_matches
            {
                return Err(corruption(
                    "text physical builder-state key/value ownership mismatch",
                ));
            }
        }
    }
    Ok(())
}

/// One externally prepared upload-intent normalization commit.
///
/// Coordinator status, terminal release, and same-run fence checks happen
/// before the repository transaction. The staged transaction revalidates the
/// exact operation, root, first remaining intent, member, pointer, and support
/// rows before applying only the closed action retained here.
pub(crate) struct PreparedUploadIntentRetirement {
    source_operation: IndexOperationRecord,
    source_root: work::BlobGcRunRootValue,
    progress: GcProgress,
    aborting: bool,
    limits: SearchIndexBatchLimits,
    preparation_input_bytes: u64,
    expected: Option<PreparedUploadIntentRow>,
    action: PreparedUploadIntentAction,
}

/// The two generation-owner lanes that require the immutable fence set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FencedOwnerRetirementProgress {
    /// Remove current-run manifest reachability while retaining manifest pages.
    Manifest(GcProgress),
    /// Remove current-run build-artifact ownership rows.
    Artifacts(GcProgress),
}

impl FencedOwnerRetirementProgress {
    fn gc_progress(&self) -> &GcProgress {
        match self {
            Self::Manifest(progress) | Self::Artifacts(progress) => progress,
        }
    }
}

/// One owner-retirement commit authorized by a recovered immutable fence set.
///
/// This preparation is process-local by design: a restarted writer has no
/// retained value and must re-enumerate and reacquire every exact same-run
/// fence before it can prepare another owner-retirement transaction.
pub(crate) struct PreparedFencedOwnerRetirement {
    source_operation: IndexOperationRecord,
    source_root: work::BlobGcRunRootValue,
    progress: FencedOwnerRetirementProgress,
    aborting: bool,
    limits: SearchIndexBatchLimits,
    fences_revalidated: bool,
}

struct PreparedUploadIntentRow {
    key: Bytes,
    value: Bytes,
    intent: work::TextUploadIntentValue,
    current_member: bool,
}

enum PreparedUploadIntentAction {
    Retry,
    Block(crate::index_v2::IndexOperationBlocker),
    Advance,
    PhaseTransition(work::TextUploadIntentValue),
    Reclaim(work::TextUploadIntentValue),
    Assign(work::TextUploadIntentValue),
    DeleteNonPublication,
    DeleteReference { proof_key: Option<Bytes> },
    BeginFirstPass(work::BlobGcRunRootValue),
}

impl PreparedUploadIntentAction {
    /// Measures the canonical repository writes for one row-scoped action.
    fn output_usage(
        &self,
        scope: DataScope,
        intent: &work::TextUploadIntentValue,
    ) -> Result<(u64, u64)> {
        let current_rows = super::upload::upload_anchor_rows(scope, intent)?;
        let mut output_rows: Vec<(Bytes, Option<Bytes>)> = Vec::new();
        match self {
            Self::Advance => {}
            Self::PhaseTransition(next) | Self::Assign(next) => {
                let next_rows = super::upload::upload_anchor_rows(scope, next)?;
                output_rows.push((next_rows.intent_key, Some(next_rows.intent_value)));
                output_rows.push((next_rows.pointer_key, Some(next_rows.pointer_value)));
            }
            Self::Reclaim(next) => {
                let (candidate_key, candidate_value) = intent_candidate_row(scope, intent);
                let next_rows = super::upload::upload_anchor_rows(scope, next)?;
                output_rows.push((candidate_key, Some(candidate_value)));
                output_rows.push((current_rows.reachability_key, None));
                output_rows.push((next_rows.intent_key, Some(next_rows.intent_value)));
                output_rows.push((next_rows.pointer_key, Some(next_rows.pointer_value)));
            }
            Self::DeleteNonPublication => {
                output_rows.push((current_rows.reachability_key, None));
                output_rows.push((current_rows.intent_key, None));
                output_rows.push((current_rows.pointer_key, None));
            }
            Self::DeleteReference { proof_key } => {
                if let Some(proof_key) = proof_key {
                    output_rows.push((proof_key.clone(), None));
                }
                output_rows.push((current_rows.intent_key, None));
                output_rows.push((current_rows.pointer_key, None));
            }
            Self::Retry | Self::Block(_) | Self::BeginFirstPass(_) => {
                return Err(corruption(
                    "prepared text intent action has no row-scoped output contract",
                ));
            }
        }
        let output_operations = u64::try_from(output_rows.len())
            .map_err(|_| invariant("text intent retirement output count exceeds u64"))?;
        let output_bytes = output_rows.iter().try_fold(0_u64, |total, (key, value)| {
            total
                .checked_add(measured_row(key, value.as_ref()))
                .ok_or_else(|| invariant("text intent retirement output bytes overflowed"))
        })?;
        Ok((output_operations, output_bytes))
    }
}

/// Reacquires the complete immutable fence set before generation owners move.
pub(super) async fn prepare_fenced_owner_retirement(
    db: &Db,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: FencedOwnerRetirementProgress,
    aborting: bool,
    limits: SearchIndexBatchLimits,
    coordinator: &dyn blob_publication::BlobPublicationCoordinator,
) -> Result<PreparedFencedOwnerRetirement> {
    let gc_progress = progress.gc_progress();
    let Some(run_id) = gc_progress.gc_run_id else {
        return Err(corruption(
            "text owner retirement has no assigned blob-GC run",
        ));
    };
    let Some(_) = gc_progress.candidate_cursor else {
        return Err(corruption(
            "text owner retirement lost its immutable candidate boundary",
        ));
    };
    let transaction = db.begin(IsolationLevel::Snapshot).await?;
    let observed_root =
        super::blob_gc::load_generation_root(&transaction, scope, operation, run_id).await?;
    if !matches!(observed_root.root.phase, work::BlobGcPhase::FencesClosed) {
        return Err(corruption(
            "text owner retirement requires an exact FencesClosed root",
        ));
    }
    drop(transaction);
    let fences_revalidated =
        super::blob_gc::revalidate_all_fences(coordinator, db, &observed_root.root).await?;
    Ok(PreparedFencedOwnerRetirement {
        source_operation: operation.clone(),
        source_root: observed_root.root,
        progress,
        aborting,
        limits,
        fences_revalidated,
    })
}

impl PreparedFencedOwnerRetirement {
    /// Stages one bounded owner batch after revalidating its exact durable root.
    pub(super) async fn stage(
        &self,
        transaction: &DbTransaction,
        scope: DataScope,
        operation: &IndexOperationRecord,
    ) -> Result<IndexOperationStepResult> {
        if operation != &self.source_operation {
            return Err(corruption(
                "prepared text owner retirement no longer matches its claimed operation",
            ));
        }
        if !self.fences_revalidated {
            return Ok(IndexOperationStepResult::TransientFailure);
        }
        let Some(run_id) = self.progress.gc_progress().gc_run_id else {
            return Err(corruption(
                "prepared text owner retirement lost its assigned GC run",
            ));
        };
        let observed_root =
            super::blob_gc::load_generation_root(transaction, scope, operation, run_id).await?;
        if observed_root.root != self.source_root
            || !matches!(observed_root.root.phase, work::BlobGcPhase::FencesClosed)
        {
            return Ok(IndexOperationStepResult::TransientFailure);
        }
        match &self.progress {
            FencedOwnerRetirementProgress::Manifest(progress) => {
                retire_manifest_references(
                    transaction,
                    scope,
                    operation,
                    progress,
                    self.aborting,
                    self.limits,
                )
                .await
            }
            FencedOwnerRetirementProgress::Artifacts(progress) => {
                retire_artifact_owners(
                    transaction,
                    scope,
                    operation,
                    progress,
                    self.aborting,
                    self.limits,
                )
                .await
            }
        }
    }
}

/// Prepares one exact `RetireUploadIntents` action without retaining a DB view.
pub(super) async fn prepare_upload_intent_retirement(
    db: &Db,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &GcProgress,
    aborting: bool,
    limits: SearchIndexBatchLimits,
    coordinator: &dyn blob_publication::BlobPublicationCoordinator,
) -> Result<PreparedUploadIntentRetirement> {
    let IndexOperationExecutionState::Claimed(claim) = operation.execution_state() else {
        return Err(corruption(
            "text intent retirement preparation requires a claimed operation",
        ));
    };
    let Some(run_id) = progress.gc_run_id else {
        return Err(corruption(
            "text intent retirement requires an assigned generation GC run",
        ));
    };
    if progress.candidate_cursor.is_none() {
        return Err(corruption(
            "text intent retirement lost its generation candidate boundary",
        ));
    }

    let transaction = db.begin(IsolationLevel::Snapshot).await?;
    let observed_root =
        super::blob_gc::load_generation_root(&transaction, scope, operation, run_id).await?;
    if !matches!(observed_root.root.phase, work::BlobGcPhase::FencesClosed) {
        return Err(corruption(
            "text intent retirement requires an exact FencesClosed root",
        ));
    }
    let mut rows = scan_generation(
        &transaction,
        scope,
        index_keys::IndexV2RecordKind::TextUploadIntent,
        operation,
        progress.stage_cursor.as_ref().map(IndexCursor::as_bytes),
    )
    .await?;
    let Some(row) = rows.next().await? else {
        drop(rows);
        drop(transaction);
        let ready =
            super::blob_gc::revalidate_all_fences(coordinator, db, &observed_root.root).await?;
        let action = if ready {
            let next_root = work::BlobGcRunRootValue::try_new(
                observed_root.root.run_id,
                observed_root.root.owner,
                observed_root
                    .root
                    .revision
                    .checked_next()
                    .map_err(|error| invariant(error.to_string()))?,
                0,
                None,
                work::BlobGcPhase::FirstPass {
                    writer_epoch: claim.writer_epoch,
                    first_attempt: work::GcScanAttempt::new(1)
                        .map_err(|error| invariant(error.to_string()))?,
                    reference_cursor: None,
                },
                observed_root.root.candidate_count.get(),
            )
            .map_err(|error| invariant(error.to_string()))?;
            PreparedUploadIntentAction::BeginFirstPass(next_root)
        } else {
            PreparedUploadIntentAction::Retry
        };
        return Ok(PreparedUploadIntentRetirement {
            source_operation: operation.clone(),
            source_root: observed_root.root,
            progress: progress.clone(),
            aborting,
            limits,
            preparation_input_bytes: observed_root.input_bytes,
            expected: None,
            action,
        });
    };
    let (intent_key, intent) = decode_intent_row(scope, operation, &row.key, &row.value)?;
    let support_bytes =
        validate_intent_support(&transaction, scope, operation, intent_key, &intent).await?;
    let member =
        super::blob_gc::observe_generation_member(&transaction, run_id, intent.blob).await?;
    let current_member = matches!(
        member,
        super::blob_gc::GenerationMemberObservation::Pending { .. }
    );
    let preparation_input_bytes = observed_root
        .input_bytes
        .saturating_add(measured_row(&row.key, Some(&row.value)))
        .saturating_add(support_bytes)
        .saturating_add(member.input_bytes());
    let expected = PreparedUploadIntentRow {
        key: row.key,
        value: row.value,
        intent: intent.clone(),
        current_member,
    };
    if !current_member {
        return Ok(PreparedUploadIntentRetirement {
            source_operation: operation.clone(),
            source_root: observed_root.root,
            progress: progress.clone(),
            aborting,
            limits,
            preparation_input_bytes,
            expected: Some(expected),
            action: PreparedUploadIntentAction::Advance,
        });
    }

    let prevalidated_cleanup_key = match (&intent.work_state, &intent.phase) {
        (work::TextUploadWorkState::Queued { .. }, work::TextUploadPhase::NonPublicationProven) => {
            Some(
                super::reclaim::prepare_fenced_non_publication_cleanup(
                    &transaction,
                    scope,
                    &intent,
                )
                .await?,
            )
        }
        (
            work::TextUploadWorkState::Queued { .. },
            work::TextUploadPhase::ReferenceCommitted(_),
        ) => {
            super::attachment::prepare_fenced_reference_anchor_cleanup(&transaction, scope, &intent)
                .await?
        }
        _ => None,
    };
    drop(rows);
    drop(transaction);

    let fence_key = blob_publication::BlobDeleteFenceKey::new(intent.blob, run_id);
    let fence = match coordinator.begin_delete(fence_key).await {
        Ok(blob_publication::BeginBlobDelete::Acquired(fence))
        | Ok(blob_publication::BeginBlobDelete::AlreadyHeldSameRun(fence)) => fence,
        Ok(blob_publication::BeginBlobDelete::BusyOtherRun) => {
            return Ok(PreparedUploadIntentRetirement {
                source_operation: operation.clone(),
                source_root: observed_root.root,
                progress: progress.clone(),
                aborting,
                limits,
                preparation_input_bytes,
                expected: Some(expected),
                action: PreparedUploadIntentAction::Retry,
            });
        }
        Err(error) if coordinator_error_is_retryable(&error) => {
            return Ok(PreparedUploadIntentRetirement {
                source_operation: operation.clone(),
                source_root: observed_root.root,
                progress: progress.clone(),
                aborting,
                limits,
                preparation_input_bytes,
                expected: Some(expected),
                action: PreparedUploadIntentAction::Retry,
            });
        }
        Err(error) => return Err(error.into()),
    };
    match coordinator.check_quiescent(&fence).await {
        Ok(true) => {}
        Ok(false) => {
            return Ok(PreparedUploadIntentRetirement {
                source_operation: operation.clone(),
                source_root: observed_root.root,
                progress: progress.clone(),
                aborting,
                limits,
                preparation_input_bytes,
                expected: Some(expected),
                action: PreparedUploadIntentAction::Retry,
            });
        }
        Err(error) if coordinator_error_is_retryable(&error) => {
            return Ok(PreparedUploadIntentRetirement {
                source_operation: operation.clone(),
                source_root: observed_root.root,
                progress: progress.clone(),
                aborting,
                limits,
                preparation_input_bytes,
                expected: Some(expected),
                action: PreparedUploadIntentAction::Retry,
            });
        }
        Err(error) => return Err(error.into()),
    }

    let permit = blob_publication::BlobPublicationPermit::from_id(intent.publication_permit_id);
    let mismatch = || {
        PreparedUploadIntentAction::Block(
            crate::index_v2::IndexOperationBlocker::BlobPublicationMismatch {
                intent_id: intent.intent_id,
            },
        )
    };
    let action = match (&intent.work_state, &intent.phase) {
        (work::TextUploadWorkState::Claimed(_), _) => PreparedUploadIntentAction::Retry,
        (work::TextUploadWorkState::Blocked(blocker), _) => {
            PreparedUploadIntentAction::Block(blocker.clone())
        }
        (work::TextUploadWorkState::Queued { .. }, work::TextUploadPhase::Prepared) => {
            match coordinator.publication_status(&permit).await {
                Ok(blob_publication::BlobPublicationStatus::Succeeded(metadata))
                    if metadata.blob() == intent.blob =>
                {
                    PreparedUploadIntentAction::PhaseTransition(
                        intent
                            .cleanup_transition(
                                work::TextUploadCleanupTransition::PublicationSucceeded,
                            )
                            .map_err(|error| invariant(error.to_string()))?,
                    )
                }
                Ok(blob_publication::BlobPublicationStatus::Succeeded(_)) => mismatch(),
                Ok(
                    blob_publication::BlobPublicationStatus::Reserved
                    | blob_publication::BlobPublicationStatus::InFlight,
                ) => PreparedUploadIntentAction::Retry,
                Ok(
                    blob_publication::BlobPublicationStatus::DefinitivelyFailed
                    | blob_publication::BlobPublicationStatus::ExpiredUnused,
                ) => match coordinator.inspect_fenced_blob(&fence).await {
                    Ok(blob_publication::FencedBlobObservation::Exact) => {
                        PreparedUploadIntentAction::Reclaim(
                            intent
                                .cleanup_transition(work::TextUploadCleanupTransition::Reclaimable)
                                .map_err(|error| invariant(error.to_string()))?,
                        )
                    }
                    Ok(blob_publication::FencedBlobObservation::Absent) => {
                        PreparedUploadIntentAction::PhaseTransition(
                            intent
                                .cleanup_transition(
                                    work::TextUploadCleanupTransition::NonPublicationProven,
                                )
                                .map_err(|error| invariant(error.to_string()))?,
                        )
                    }
                    Ok(blob_publication::FencedBlobObservation::Mismatch) => mismatch(),
                    Err(error) if coordinator_error_is_retryable(&error) => {
                        PreparedUploadIntentAction::Retry
                    }
                    Err(error) => return Err(error.into()),
                },
                Err(error) if coordinator_error_is_retryable(&error) => {
                    PreparedUploadIntentAction::Retry
                }
                Err(blob_publication::BlobPublicationError::UnknownPermit) => {
                    PreparedUploadIntentAction::Block(
                        crate::index_v2::IndexOperationBlocker::InvariantViolation,
                    )
                }
                Err(error) => return Err(error.into()),
            }
        }
        (work::TextUploadWorkState::Queued { .. }, work::TextUploadPhase::Uploaded) => {
            PreparedUploadIntentAction::Reclaim(
                intent
                    .cleanup_transition(work::TextUploadCleanupTransition::Reclaimable)
                    .map_err(|error| invariant(error.to_string()))?,
            )
        }
        (work::TextUploadWorkState::Queued { .. }, work::TextUploadPhase::NonPublicationProven) => {
            let delete = PreparedUploadIntentAction::DeleteNonPublication;
            let (output_operations, output_bytes) = delete.output_usage(scope, &intent)?;
            if preparation_input_bytes > limits.max_input_bytes().get()
                || output_operations > limits.max_output_operations().get()
                || output_bytes > limits.max_output_bytes().get()
            {
                PreparedUploadIntentAction::Block(
                    crate::index_v2::IndexOperationBlocker::InvariantViolation,
                )
            } else {
                match coordinator
                    .release(
                        &permit,
                        blob_publication::BlobPermitReleaseAuthority::definitive_non_publication(
                            permit.id(),
                        ),
                    )
                    .await
                {
                    Ok(()) => delete,
                    Err(error) if coordinator_error_is_retryable(&error) => {
                        PreparedUploadIntentAction::Retry
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
        (
            work::TextUploadWorkState::Queued { .. },
            work::TextUploadPhase::ReferenceCommitted(_),
        ) => {
            let delete = PreparedUploadIntentAction::DeleteReference {
                proof_key: prevalidated_cleanup_key,
            };
            let (output_operations, output_bytes) = delete.output_usage(scope, &intent)?;
            if preparation_input_bytes > limits.max_input_bytes().get()
                || output_operations > limits.max_output_operations().get()
                || output_bytes > limits.max_output_bytes().get()
            {
                PreparedUploadIntentAction::Block(
                    crate::index_v2::IndexOperationBlocker::InvariantViolation,
                )
            } else {
                match coordinator
                    .release(
                        &permit,
                        blob_publication::BlobPermitReleaseAuthority::reference_committed(
                            permit.id(),
                        ),
                    )
                    .await
                {
                    Ok(()) => delete,
                    Err(error) if coordinator_error_is_retryable(&error) => {
                        PreparedUploadIntentAction::Retry
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
        (
            work::TextUploadWorkState::Queued { .. },
            work::TextUploadPhase::Reclaimable(work::ReclaimAssignment::Unassigned),
        ) => PreparedUploadIntentAction::Assign(
            intent
                .cleanup_transition(work::TextUploadCleanupTransition::AssignReclaim(run_id))
                .map_err(|error| invariant(error.to_string()))?,
        ),
        (
            work::TextUploadWorkState::Queued { .. },
            work::TextUploadPhase::Reclaimable(work::ReclaimAssignment::Assigned(assigned)),
        ) if *assigned == run_id => PreparedUploadIntentAction::Advance,
        (
            work::TextUploadWorkState::Queued { .. },
            work::TextUploadPhase::Reclaimable(work::ReclaimAssignment::Assigned(_)),
        ) => {
            return Err(corruption(
                "fenced generation intent is assigned to another GC run",
            ));
        }
    };
    Ok(PreparedUploadIntentRetirement {
        source_operation: operation.clone(),
        source_root: observed_root.root,
        progress: progress.clone(),
        aborting,
        limits,
        preparation_input_bytes,
        expected: Some(expected),
        action,
    })
}

impl PreparedUploadIntentRetirement {
    /// Stages only the exact fenced normalization selected during preparation.
    pub(super) async fn stage(
        &self,
        transaction: &DbTransaction,
        scope: DataScope,
        operation: &IndexOperationRecord,
    ) -> Result<IndexOperationStepResult> {
        if operation != &self.source_operation {
            return Err(corruption(
                "prepared text intent retirement no longer matches its claimed operation",
            ));
        }
        if matches!(self.action, PreparedUploadIntentAction::Retry) {
            return Ok(IndexOperationStepResult::TransientFailure);
        }
        let Some(run_id) = self.progress.gc_run_id else {
            return Err(corruption(
                "prepared text intent retirement lost its assigned GC run",
            ));
        };
        let observed_root =
            super::blob_gc::load_generation_root(transaction, scope, operation, run_id).await?;
        if observed_root.root != self.source_root
            || !matches!(observed_root.root.phase, work::BlobGcPhase::FencesClosed)
        {
            return Ok(IndexOperationStepResult::TransientFailure);
        }

        if let PreparedUploadIntentAction::BeginFirstPass(next_root) = &self.action {
            if self.expected.is_some() {
                return Err(corruption(
                    "first-pass preparation unexpectedly retained an intent row",
                ));
            }
            let mut rows = scan_generation(
                transaction,
                scope,
                index_keys::IndexV2RecordKind::TextUploadIntent,
                operation,
                self.progress
                    .stage_cursor
                    .as_ref()
                    .map(IndexCursor::as_bytes),
            )
            .await?;
            if rows.next().await?.is_some() {
                return Ok(IndexOperationStepResult::TransientFailure);
            }
            let root_key = index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes();
            let root_value =
                index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcEntry(
                    work::BlobGcEntryValue::RunRoot(next_root.clone()),
                ));
            let output_operations = 1_u64;
            let output_bytes = measured_row(&root_key, Some(&root_value));
            if self.preparation_input_bytes > self.limits.max_input_bytes().get()
                || output_operations > self.limits.max_output_operations().get()
                || output_bytes > self.limits.max_output_bytes().get()
            {
                return Ok(IndexOperationStepResult::Blocked(
                    crate::index_v2::IndexOperationBlocker::InvariantViolation,
                ));
            }
            transaction.put(root_key, root_value)?;
            let counters = add_counters(
                self.progress.counters,
                0,
                self.preparation_input_bytes,
                output_operations,
                output_bytes,
            )?;
            return Ok(progressed_cleanup(
                self.aborting,
                TextCleanupProgress::MarkReachability(GcProgress {
                    gc_run_id: self.progress.gc_run_id,
                    candidate_cursor: self.progress.candidate_cursor.clone(),
                    stage_cursor: None,
                    counters,
                }),
            ));
        }

        let Some(expected) = self.expected.as_ref() else {
            return Err(corruption(
                "prepared text intent action lost its exact source row",
            ));
        };
        let mut rows = scan_generation(
            transaction,
            scope,
            index_keys::IndexV2RecordKind::TextUploadIntent,
            operation,
            self.progress
                .stage_cursor
                .as_ref()
                .map(IndexCursor::as_bytes),
        )
        .await?;
        let Some(row) = rows.next().await? else {
            return Ok(IndexOperationStepResult::TransientFailure);
        };
        if row.key != expected.key || row.value != expected.value {
            return Ok(IndexOperationStepResult::TransientFailure);
        }
        let (intent_key, intent) = decode_intent_row(scope, operation, &row.key, &row.value)?;
        if intent != expected.intent {
            return Err(corruption(
                "prepared text intent bytes decoded to a different semantic value",
            ));
        }
        validate_intent_support(transaction, scope, operation, intent_key, &intent).await?;
        let member =
            super::blob_gc::observe_generation_member(transaction, run_id, intent.blob).await?;
        if matches!(
            member,
            super::blob_gc::GenerationMemberObservation::Pending { .. }
        ) != expected.current_member
        {
            return Ok(IndexOperationStepResult::TransientFailure);
        }
        if let PreparedUploadIntentAction::Block(blocker) = &self.action {
            return Ok(IndexOperationStepResult::Blocked(blocker.clone()));
        }

        let current_rows = super::upload::upload_anchor_rows(scope, &intent)?;
        let (output_operations, output_bytes) = self.action.output_usage(scope, &intent)?;
        if self.preparation_input_bytes > self.limits.max_input_bytes().get()
            || output_operations > self.limits.max_output_operations().get()
            || output_bytes > self.limits.max_output_bytes().get()
        {
            return Ok(IndexOperationStepResult::Blocked(
                crate::index_v2::IndexOperationBlocker::InvariantViolation,
            ));
        }

        let advance_cursor = match &self.action {
            PreparedUploadIntentAction::Advance => true,
            PreparedUploadIntentAction::PhaseTransition(next) => {
                let next_rows = super::upload::upload_anchor_rows(scope, next)?;
                transaction.put(next_rows.intent_key, next_rows.intent_value)?;
                transaction.put(next_rows.pointer_key, next_rows.pointer_value)?;
                false
            }
            PreparedUploadIntentAction::Reclaim(next) => {
                super::reclaim::stage_fenced_intent_reclaim(transaction, scope, &intent).await?;
                let next_rows = super::upload::upload_anchor_rows(scope, next)?;
                transaction.put(next_rows.intent_key, next_rows.intent_value)?;
                transaction.put(next_rows.pointer_key, next_rows.pointer_value)?;
                false
            }
            PreparedUploadIntentAction::Assign(next) => {
                let next_rows = super::upload::upload_anchor_rows(scope, next)?;
                transaction.put(next_rows.intent_key, next_rows.intent_value)?;
                transaction.put(next_rows.pointer_key, next_rows.pointer_value)?;
                true
            }
            PreparedUploadIntentAction::DeleteNonPublication => {
                super::reclaim::stage_fenced_non_publication_cleanup(transaction, scope, &intent)
                    .await?;
                transaction.delete(current_rows.intent_key)?;
                transaction.delete(current_rows.pointer_key)?;
                true
            }
            PreparedUploadIntentAction::DeleteReference { proof_key } => {
                let staged_proof = super::attachment::stage_fenced_reference_anchor_cleanup(
                    transaction,
                    scope,
                    &intent,
                )
                .await?;
                if staged_proof != *proof_key {
                    return Ok(IndexOperationStepResult::TransientFailure);
                }
                transaction.delete(current_rows.intent_key)?;
                transaction.delete(current_rows.pointer_key)?;
                true
            }
            PreparedUploadIntentAction::Retry
            | PreparedUploadIntentAction::Block(_)
            | PreparedUploadIntentAction::BeginFirstPass(_) => {
                return Err(corruption(
                    "prepared text intent action changed during staging",
                ));
            }
        };
        let stage_cursor = if advance_cursor {
            Some(IndexCursor::try_new(expected.key.clone()).map_err(operation_error)?)
        } else {
            self.progress.stage_cursor.clone()
        };
        Ok(progressed_cleanup(
            self.aborting,
            TextCleanupProgress::RetireUploadIntents(GcProgress {
                gc_run_id: self.progress.gc_run_id,
                candidate_cursor: self.progress.candidate_cursor.clone(),
                stage_cursor,
                counters: add_counters(
                    self.progress.counters,
                    1,
                    self.preparation_input_bytes,
                    output_operations,
                    output_bytes,
                )?,
            }),
        ))
    }
}

async fn prepare_candidates(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &PrefixScanProgress,
    aborting: bool,
    limits: SearchIndexBatchLimits,
) -> Result<IndexOperationStepResult> {
    let Some(cursor) = progress.cursor.as_ref() else {
        return scan_reclaim_barrier(transaction, scope, operation, progress, aborting, limits)
            .await;
    };
    if is_scoped_kind(
        scope,
        cursor,
        index_keys::IndexV2RecordKind::TextUploadIntent,
    )? {
        return scan_reclaim_barrier(transaction, scope, operation, progress, aborting, limits)
            .await;
    }
    if super::blob_gc::is_generation_candidate_start_cursor(scope, operation, cursor)?
        || is_scoped_kind(
            scope,
            cursor,
            index_keys::IndexV2RecordKind::TextManifestPage,
        )?
        || is_manifest_slot_cursor(cursor)?
    {
        return scan_manifest_candidates(transaction, scope, operation, progress, aborting, limits)
            .await;
    }
    if is_scoped_kind(
        scope,
        cursor,
        index_keys::IndexV2RecordKind::TextBuildArtifact,
    )? {
        return scan_artifact_candidates(transaction, scope, operation, progress, aborting, limits)
            .await;
    }
    if matches!(
        index_keys::GlobalIndexV2Key::parse_from_slice(cursor.as_bytes()),
        Ok(index_keys::GlobalIndexV2Key::UploadPointer(_))
    ) {
        return scan_intent_candidates(transaction, scope, operation, progress, aborting, limits)
            .await;
    }
    Err(corruption(
        "text candidate preparation cursor is outside its closed lane set",
    ))
}

/// Completes the pre-candidate scan that waits for foreign upload-reclaim roots.
async fn scan_reclaim_barrier(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &PrefixScanProgress,
    aborting: bool,
    limits: SearchIndexBatchLimits,
) -> Result<IndexOperationStepResult> {
    Ok(
        match super::blob_gc::scan_reclaim_barrier(transaction, scope, operation, progress, limits)
            .await?
        {
            super::blob_gc::ReclaimBarrierTransition::Progressed(next) => {
                progressed_cleanup(aborting, TextCleanupProgress::PrepareCandidates(next))
            }
            super::blob_gc::ReclaimBarrierTransition::Complete(counters) => progressed_cleanup(
                aborting,
                TextCleanupProgress::PrepareCandidates(PrefixScanProgress {
                    cursor: Some(super::blob_gc::generation_candidate_start_cursor(
                        scope, operation,
                    )?),
                    counters,
                }),
            ),
            super::blob_gc::ReclaimBarrierTransition::Waiting => {
                IndexOperationStepResult::TransientFailure
            }
            super::blob_gc::ReclaimBarrierTransition::Blocked(blocker) => {
                IndexOperationStepResult::Blocked(blocker)
            }
        },
    )
}

async fn acquire_delete_fences(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &GcProgress,
    aborting: bool,
    limits: SearchIndexBatchLimits,
) -> Result<IndexOperationStepResult> {
    Ok(
        match super::blob_gc::stage_generation_batch(
            transaction,
            scope,
            operation,
            progress,
            limits,
        )
        .await?
        {
            super::blob_gc::GenerationBatchTransition::Progressed(next) => {
                progressed_cleanup(aborting, TextCleanupProgress::AcquireDeleteFences(next))
            }
            super::blob_gc::GenerationBatchTransition::Waiting => {
                IndexOperationStepResult::TransientFailure
            }
            super::blob_gc::GenerationBatchTransition::FencesClosed(next) => {
                progressed_cleanup(aborting, TextCleanupProgress::RetireManifest(next))
            }
            super::blob_gc::GenerationBatchTransition::Exhausted(counters) => progressed_cleanup(
                aborting,
                TextCleanupProgress::DeleteEntityState(PrefixScanProgress {
                    cursor: None,
                    counters,
                }),
            ),
            super::blob_gc::GenerationBatchTransition::Blocked(blocker) => {
                IndexOperationStepResult::Blocked(blocker)
            }
        },
    )
}

/// Retires only current-run manifest reachability rows while retaining pages.
///
/// A page remains the immutable source used by later candidate batches. Each
/// visited slot therefore advances an exact owner cursor, but its global
/// reachability row is deleted only when the split hash belongs to the current
/// immutable run. The operation checkpoint and those deletes share the
/// repository transaction.
async fn retire_manifest_references(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &GcProgress,
    aborting: bool,
    limits: SearchIndexBatchLimits,
) -> Result<IndexOperationStepResult> {
    let (run_id, root_input_bytes) =
        fences_closed_generation_run(transaction, scope, operation, progress).await?;
    if root_input_bytes > limits.max_input_bytes().get() {
        return Ok(IndexOperationStepResult::Blocked(
            crate::index_v2::IndexOperationBlocker::InvariantViolation,
        ));
    }
    let resume = match progress.stage_cursor.as_ref() {
        Some(cursor) => manifest_owner_resume(scope, operation, cursor)?,
        None => ManifestResume::NextPage(None),
    };
    let (page_key, page_value, page_row_key, page_row_value, first_slot) = match resume {
        ManifestResume::NextPage(resume) => {
            let mut rows = scan_generation(
                transaction,
                scope,
                index_keys::IndexV2RecordKind::TextManifestPage,
                operation,
                resume.as_ref(),
            )
            .await?;
            let Some(row) = rows.next().await? else {
                let counters = add_counters(progress.counters, 0, root_input_bytes, 0, 0)?;
                return Ok(progressed_cleanup(
                    aborting,
                    TextCleanupProgress::RetireArtifacts(GcProgress {
                        gc_run_id: progress.gc_run_id,
                        candidate_cursor: progress.candidate_cursor.clone(),
                        stage_cursor: None,
                        counters,
                    }),
                ));
            };
            let (key, page) = decode_manifest_page(scope, operation, &row.key, &row.value)?;
            (key, page, row.key, row.value, 0_usize)
        }
        ManifestResume::WithinPage { page_key, slot } => {
            let encoded_key = scoped_key(scope, index_keys::IndexV2Key::TextManifestPage(page_key));
            let Some(value) = transaction.get(&encoded_key).await? else {
                return Err(corruption(
                    "text manifest retirement cursor names a missing page",
                ));
            };
            let (_, page) = decode_manifest_page(scope, operation, &encoded_key, &value)?;
            (page_key, page, encoded_key, value, slot)
        }
    };
    if first_slot >= page_value.entries().len() {
        return Err(corruption(
            "text manifest retirement cursor exceeds its bounded page",
        ));
    }
    let fixed_input_bytes = root_input_bytes
        .checked_add(measured_row(&page_row_key, Some(&page_row_value)))
        .ok_or_else(|| invariant("text manifest retirement input counter overflowed"))?;
    let mut batch = RetirementBatch::new(progress.counters, limits, fixed_input_bytes);
    let mut completed_cursor = None;
    let mut completed_slot = None;
    for (slot, split) in page_value.entries().iter().enumerate().skip(first_slot) {
        if !batch.can_visit_another() {
            break;
        }
        let owner_slot =
            u32::try_from(slot).map_err(|_| invariant("bounded text manifest slot exceeds u32"))?;
        let (reference_key, reference_value) = super::attachment::manifest_page_reachability_row(
            split.blob(),
            scope,
            page_key,
            owner_slot,
        );
        let member =
            super::blob_gc::observe_generation_member(transaction, run_id, split.blob()).await?;
        let mut input_bytes = member.input_bytes();
        let delete_keys = match member {
            super::blob_gc::GenerationMemberObservation::Absent { .. } => Vec::new(),
            super::blob_gc::GenerationMemberObservation::Pending { .. } => {
                let Some(observed_reference) = transaction.get(&reference_key).await? else {
                    return Err(corruption(
                        "current-run manifest split is missing its reachability entry",
                    ));
                };
                if observed_reference != reference_value {
                    return Err(corruption(
                        "current-run manifest reachability entry disagrees with its owner",
                    ));
                }
                input_bytes = input_bytes
                    .checked_add(measured_row(&reference_key, Some(&observed_reference)))
                    .ok_or_else(|| {
                        invariant("text manifest retirement input counter overflowed")
                    })?;
                vec![reference_key.clone()]
            }
        };
        match batch.admit(input_bytes, delete_keys)? {
            RetirementAdmission::Admitted => {
                completed_cursor =
                    Some(IndexCursor::try_new(reference_key).map_err(operation_error)?);
                completed_slot = Some(slot);
            }
            RetirementAdmission::Full => break,
            RetirementAdmission::Indivisible => {
                return Ok(IndexOperationStepResult::Blocked(
                    crate::index_v2::IndexOperationBlocker::InvariantViolation,
                ));
            }
        }
    }
    let Some(mut completed_cursor) = completed_cursor else {
        return Ok(IndexOperationStepResult::Blocked(
            crate::index_v2::IndexOperationBlocker::InvariantViolation,
        ));
    };
    if completed_slot
        .and_then(|slot| slot.checked_add(1))
        .is_some_and(|slot| slot == page_value.entries().len())
    {
        completed_cursor = IndexCursor::try_new(page_row_key).map_err(operation_error)?;
    }
    batch.stage(transaction)?;
    Ok(progressed_cleanup(
        aborting,
        TextCleanupProgress::RetireManifest(GcProgress {
            gc_run_id: progress.gc_run_id,
            candidate_cursor: progress.candidate_cursor.clone(),
            stage_cursor: Some(completed_cursor),
            counters: batch.finish()?,
        }),
    ))
}

/// Deletes current-run build artifacts together with their exact references.
///
/// Artifacts whose hashes belong to another immutable batch remain byte-for-
/// byte intact and are revisited when that later batch owns their hash.
async fn retire_artifact_owners(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &GcProgress,
    aborting: bool,
    limits: SearchIndexBatchLimits,
) -> Result<IndexOperationStepResult> {
    let (run_id, root_input_bytes) =
        fences_closed_generation_run(transaction, scope, operation, progress).await?;
    if root_input_bytes > limits.max_input_bytes().get() {
        return Ok(IndexOperationStepResult::Blocked(
            crate::index_v2::IndexOperationBlocker::InvariantViolation,
        ));
    }
    let resume = progress.stage_cursor.as_ref().map(IndexCursor::as_bytes);
    let mut rows = scan_generation(
        transaction,
        scope,
        index_keys::IndexV2RecordKind::TextBuildArtifact,
        operation,
        resume,
    )
    .await?;
    let mut batch = RetirementBatch::new(progress.counters, limits, root_input_bytes);
    let mut completed_cursor = None;
    let mut prefix_exhausted = false;
    while batch.can_visit_another() {
        let Some(row) = rows.next().await? else {
            prefix_exhausted = true;
            break;
        };
        let (artifact_key, artifact) =
            super::attachment::decode_build_artifact(scope, operation, &row.key, &row.value)?;
        let member =
            super::blob_gc::observe_generation_member(transaction, run_id, artifact.split.blob())
                .await?;
        let mut input_bytes = measured_row(&row.key, Some(&row.value))
            .checked_add(member.input_bytes())
            .ok_or_else(|| invariant("text artifact retirement input counter overflowed"))?;
        let delete_keys = match member {
            super::blob_gc::GenerationMemberObservation::Absent { .. } => Vec::new(),
            super::blob_gc::GenerationMemberObservation::Pending { .. } => {
                let (reference_key, reference_value) =
                    super::attachment::build_artifact_reachability_row(
                        artifact.split.blob(),
                        scope,
                        artifact_key,
                    );
                let Some(observed_reference) = transaction.get(&reference_key).await? else {
                    return Err(corruption(
                        "current-run text artifact is missing its reachability entry",
                    ));
                };
                if observed_reference != reference_value {
                    return Err(corruption(
                        "current-run text artifact reachability entry disagrees with its owner",
                    ));
                }
                input_bytes = input_bytes
                    .checked_add(measured_row(&reference_key, Some(&observed_reference)))
                    .ok_or_else(|| {
                        invariant("text artifact retirement input counter overflowed")
                    })?;
                vec![row.key.clone(), reference_key]
            }
        };
        match batch.admit(input_bytes, delete_keys)? {
            RetirementAdmission::Admitted => {
                completed_cursor = Some(IndexCursor::try_new(row.key).map_err(operation_error)?);
            }
            RetirementAdmission::Full => break,
            RetirementAdmission::Indivisible => {
                return Ok(IndexOperationStepResult::Blocked(
                    crate::index_v2::IndexOperationBlocker::InvariantViolation,
                ));
            }
        }
    }
    batch.stage(transaction)?;
    let counters = batch.finish()?;
    if prefix_exhausted {
        return Ok(progressed_cleanup(
            aborting,
            TextCleanupProgress::RetireUploadIntents(GcProgress {
                gc_run_id: progress.gc_run_id,
                candidate_cursor: progress.candidate_cursor.clone(),
                stage_cursor: None,
                counters,
            }),
        ));
    }
    let Some(completed_cursor) = completed_cursor else {
        return Ok(IndexOperationStepResult::Blocked(
            crate::index_v2::IndexOperationBlocker::InvariantViolation,
        ));
    };
    Ok(progressed_cleanup(
        aborting,
        TextCleanupProgress::RetireArtifacts(GcProgress {
            gc_run_id: progress.gc_run_id,
            candidate_cursor: progress.candidate_cursor.clone(),
            stage_cursor: Some(completed_cursor),
            counters,
        }),
    ))
}

/// Requires one assigned generation root whose member fences are all closed.
async fn fences_closed_generation_run(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &GcProgress,
) -> Result<(crate::index_v2::BlobGcRunId, u64)> {
    let Some(run_id) = progress.gc_run_id else {
        return Err(corruption(
            "text owner retirement has no assigned blob-GC run",
        ));
    };
    let Some(_) = progress.candidate_cursor else {
        return Err(corruption(
            "text owner retirement lost its immutable candidate boundary",
        ));
    };
    let observed =
        super::blob_gc::load_generation_root(transaction, scope, operation, run_id).await?;
    if !matches!(observed.root.phase, work::BlobGcPhase::FencesClosed) {
        return Err(corruption(
            "text owner retirement requires an exact FencesClosed root",
        ));
    }
    Ok((run_id, observed.input_bytes))
}

/// Waits for the independent root lane to prove both stable reachability passes.
async fn observe_reachability_passes(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &GcProgress,
    aborting: bool,
) -> Result<IndexOperationStepResult> {
    let Some(run_id) = progress.gc_run_id else {
        return Err(corruption(
            "text reachability observation has no assigned blob-GC run",
        ));
    };
    if progress.candidate_cursor.is_none() || progress.stage_cursor.is_some() {
        return Err(corruption(
            "text reachability observation lost its run boundary or retained a stage cursor",
        ));
    }
    let observed =
        super::blob_gc::load_generation_root(transaction, scope, operation, run_id).await?;
    match observed.root.phase {
        work::BlobGcPhase::FirstPass { .. } | work::BlobGcPhase::SecondPass { .. } => {
            Ok(IndexOperationStepResult::TransientFailure)
        }
        work::BlobGcPhase::Delete { .. } => Ok(progressed_cleanup(
            aborting,
            TextCleanupProgress::DeleteBlobs(progress.clone()),
        )),
        work::BlobGcPhase::AwaitDeleteFences { .. } | work::BlobGcPhase::FencesClosed => Err(
            corruption("text reachability operation and blob-GC root regressed"),
        ),
    }
}

async fn scan_manifest_candidates(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &PrefixScanProgress,
    aborting: bool,
    limits: SearchIndexBatchLimits,
) -> Result<IndexOperationStepResult> {
    let cursor = progress
        .cursor
        .as_ref()
        .expect("manifest lane has a cursor");
    let (page_key, page_value, first_slot) =
        match candidate_manifest_resume(scope, operation, cursor)? {
            ManifestResume::NextPage(resume) => {
                let mut rows = scan_generation(
                    transaction,
                    scope,
                    index_keys::IndexV2RecordKind::TextManifestPage,
                    operation,
                    resume.as_ref(),
                )
                .await?;
                let Some(row) = rows.next().await? else {
                    return scan_artifact_candidates_from(
                        transaction,
                        scope,
                        operation,
                        progress.counters,
                        None,
                        aborting,
                        limits,
                    )
                    .await;
                };
                let (key, page) = decode_manifest_page(scope, operation, &row.key, &row.value)?;
                (key, page, 0_usize)
            }
            ManifestResume::WithinPage { page_key, slot } => {
                let encoded_key =
                    scoped_key(scope, index_keys::IndexV2Key::TextManifestPage(page_key));
                let Some(value) = transaction.get(&encoded_key).await? else {
                    return Err(corruption(
                        "text manifest candidate cursor names a missing page",
                    ));
                };
                let (_, page) = decode_manifest_page(scope, operation, &encoded_key, &value)?;
                (page_key, page, slot)
            }
        };
    let page_row_key = scoped_key(scope, index_keys::IndexV2Key::TextManifestPage(page_key));
    let page_row_value = index_values::encode_work_value(
        &index_values::IndexV2WorkValue::TextManifestPage(page_value.clone()),
    );
    let mut batch = CandidateBatch::new(progress.counters, limits);
    if batch.charge_owner_row(&page_row_key, &page_row_value)? == CandidateAdmission::Indivisible {
        return Ok(IndexOperationStepResult::Blocked(
            crate::index_v2::IndexOperationBlocker::InvariantViolation,
        ));
    }
    let mut completed_cursor = None;
    for (slot, split) in page_value.entries().iter().enumerate().skip(first_slot) {
        let slot =
            u32::try_from(slot).map_err(|_| invariant("bounded text manifest slot exceeds u32"))?;
        let (reference_key, reference_value) =
            super::attachment::manifest_page_reachability_row(split.blob(), scope, page_key, slot);
        let Some(observed_reference) = transaction.get(&reference_key).await? else {
            return Err(corruption(
                "text manifest split is missing its global reachability entry",
            ));
        };
        if observed_reference != reference_value {
            return Err(corruption(
                "text manifest split reachability entry disagrees with its owner",
            ));
        }
        match batch
            .admit_candidate(
                transaction,
                scope,
                operation,
                split.blob(),
                measured_row(&reference_key, Some(&observed_reference)),
            )
            .await?
        {
            CandidateAdmission::Admitted => {
                completed_cursor =
                    Some(IndexCursor::try_new(reference_key).map_err(operation_error)?);
            }
            CandidateAdmission::Full => break,
            CandidateAdmission::Indivisible => {
                return Ok(IndexOperationStepResult::Blocked(
                    crate::index_v2::IndexOperationBlocker::InvariantViolation,
                ));
            }
        }
    }
    let Some(mut completed_cursor) = completed_cursor else {
        return Ok(IndexOperationStepResult::Blocked(
            crate::index_v2::IndexOperationBlocker::InvariantViolation,
        ));
    };
    let last_slot =
        match index_keys::GlobalIndexV2Key::parse_from_slice(completed_cursor.as_bytes()) {
            Ok(index_keys::GlobalIndexV2Key::BlobReachabilityReference(reference)) => {
                usize::try_from(reference.owner_slot)
                    .map_err(|_| invariant("bounded text manifest slot exceeds usize"))?
            }
            _ => {
                return Err(corruption(
                    "text manifest batch produced another cursor kind",
                ))
            }
        };
    if last_slot + 1 == page_value.entries().len() {
        completed_cursor = IndexCursor::try_new(page_row_key).map_err(operation_error)?;
    }
    batch.stage(transaction)?;
    Ok(progressed_cleanup(
        aborting,
        TextCleanupProgress::PrepareCandidates(PrefixScanProgress {
            cursor: Some(completed_cursor),
            counters: batch.finish()?,
        }),
    ))
}

async fn scan_artifact_candidates(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &PrefixScanProgress,
    aborting: bool,
    limits: SearchIndexBatchLimits,
) -> Result<IndexOperationStepResult> {
    scan_artifact_candidates_from(
        transaction,
        scope,
        operation,
        progress.counters,
        progress.cursor.as_ref().map(IndexCursor::as_bytes),
        aborting,
        limits,
    )
    .await
}

async fn scan_artifact_candidates_from(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    counters: OperationCounters,
    resume: Option<&Bytes>,
    aborting: bool,
    limits: SearchIndexBatchLimits,
) -> Result<IndexOperationStepResult> {
    let mut rows = scan_generation(
        transaction,
        scope,
        index_keys::IndexV2RecordKind::TextBuildArtifact,
        operation,
        resume,
    )
    .await?;
    let mut batch = CandidateBatch::new(counters, limits);
    let mut completed_cursor = None;
    while batch.can_read_another() {
        let Some(row) = rows.next().await? else {
            if completed_cursor.is_some() {
                break;
            }
            return scan_intent_candidates_from(
                transaction,
                scope,
                operation,
                counters,
                None,
                aborting,
                limits,
            )
            .await;
        };
        let (key, artifact) =
            super::attachment::decode_build_artifact(scope, operation, &row.key, &row.value)?;
        let (reference_key, reference_value) =
            super::attachment::build_artifact_reachability_row(artifact.split.blob(), scope, key);
        let Some(observed_reference) = transaction.get(&reference_key).await? else {
            return Err(corruption(
                "text build artifact is missing its global reachability entry",
            ));
        };
        if observed_reference != reference_value {
            return Err(corruption(
                "text build artifact reachability entry disagrees with its owner",
            ));
        }
        let source_bytes = measured_row(&row.key, Some(&row.value))
            .saturating_add(measured_row(&reference_key, Some(&observed_reference)));
        match batch
            .admit_candidate(
                transaction,
                scope,
                operation,
                artifact.split.blob(),
                source_bytes,
            )
            .await?
        {
            CandidateAdmission::Admitted => {
                completed_cursor = Some(IndexCursor::try_new(row.key).map_err(operation_error)?);
            }
            CandidateAdmission::Full => break,
            CandidateAdmission::Indivisible => {
                return Ok(IndexOperationStepResult::Blocked(
                    crate::index_v2::IndexOperationBlocker::InvariantViolation,
                ));
            }
        }
    }
    let Some(completed_cursor) = completed_cursor else {
        return Ok(IndexOperationStepResult::Blocked(
            crate::index_v2::IndexOperationBlocker::InvariantViolation,
        ));
    };
    batch.stage(transaction)?;
    Ok(progressed_cleanup(
        aborting,
        TextCleanupProgress::PrepareCandidates(PrefixScanProgress {
            cursor: Some(completed_cursor),
            counters: batch.finish()?,
        }),
    ))
}

async fn scan_intent_candidates(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &PrefixScanProgress,
    aborting: bool,
    limits: SearchIndexBatchLimits,
) -> Result<IndexOperationStepResult> {
    let Some(cursor) = progress.cursor.as_ref() else {
        return Err(corruption("text intent candidate lane lost its cursor"));
    };
    let index_keys::GlobalIndexV2Key::UploadPointer(intent_id) =
        index_keys::GlobalIndexV2Key::parse_from_slice(cursor.as_bytes())?
    else {
        return Err(corruption(
            "text intent candidate lane has another cursor kind",
        ));
    };
    let resume = scoped_key(
        scope,
        index_keys::IndexV2Key::TextUploadIntent(index_keys::TextIntentOwnedKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            intent_id,
        }),
    );
    scan_intent_candidates_from(
        transaction,
        scope,
        operation,
        progress.counters,
        Some(&resume),
        aborting,
        limits,
    )
    .await
}

async fn scan_intent_candidates_from(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    counters: OperationCounters,
    resume: Option<&Bytes>,
    aborting: bool,
    limits: SearchIndexBatchLimits,
) -> Result<IndexOperationStepResult> {
    let mut rows = scan_generation(
        transaction,
        scope,
        index_keys::IndexV2RecordKind::TextUploadIntent,
        operation,
        resume,
    )
    .await?;
    let mut batch = CandidateBatch::new(counters, limits);
    let mut completed_cursor = None;
    while batch.can_read_another() {
        let Some(row) = rows.next().await? else {
            if completed_cursor.is_some() {
                break;
            }
            return Ok(progressed_cleanup(
                aborting,
                TextCleanupProgress::AcquireDeleteFences(GcProgress {
                    gc_run_id: None,
                    candidate_cursor: None,
                    stage_cursor: None,
                    counters,
                }),
            ));
        };
        let (key, intent) = decode_intent_row(scope, operation, &row.key, &row.value)?;
        let support_bytes =
            validate_intent_support(transaction, scope, operation, key, &intent).await?;
        let source_bytes = measured_row(&row.key, Some(&row.value)).saturating_add(support_bytes);
        match batch
            .admit_candidate(transaction, scope, operation, intent.blob, source_bytes)
            .await?
        {
            CandidateAdmission::Admitted => {
                completed_cursor = Some(
                    IndexCursor::try_new(
                        index_keys::GlobalIndexV2Key::UploadPointer(intent.intent_id).to_bytes(),
                    )
                    .map_err(operation_error)?,
                );
            }
            CandidateAdmission::Full => break,
            CandidateAdmission::Indivisible => {
                return Ok(IndexOperationStepResult::Blocked(
                    crate::index_v2::IndexOperationBlocker::InvariantViolation,
                ));
            }
        }
    }
    let Some(completed_cursor) = completed_cursor else {
        return Ok(IndexOperationStepResult::Blocked(
            crate::index_v2::IndexOperationBlocker::InvariantViolation,
        ));
    };
    batch.stage(transaction)?;
    Ok(progressed_cleanup(
        aborting,
        TextCleanupProgress::PrepareCandidates(PrefixScanProgress {
            cursor: Some(completed_cursor),
            counters: batch.finish()?,
        }),
    ))
}

async fn validate_intent_support(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    key: index_keys::TextIntentOwnedKey,
    intent: &work::TextUploadIntentValue,
) -> Result<u64> {
    let pointer_key = index_keys::GlobalIndexV2Key::UploadPointer(intent.intent_id).to_bytes();
    let pointer_value = transaction.get(&pointer_key).await?;
    match (&intent.work_state, pointer_value.as_ref()) {
        (work::TextUploadWorkState::Blocked(_), None) => {}
        (
            work::TextUploadWorkState::Queued { .. } | work::TextUploadWorkState::Claimed(_),
            Some(value),
        ) => {
            let IndexV2MetadataValue::UploadQueuePointer(pointer) =
                index_values::decode_metadata_value(value)?
            else {
                return Err(corruption(
                    "text upload pointer contains another metadata value kind",
                ));
            };
            if pointer.scope != scope
                || pointer.index_id != operation.index_id()
                || pointer.generation != operation.generation()
                || pointer.record_revision != intent.revision
            {
                return Err(corruption("text upload intent and pointer disagree"));
            }
        }
        (work::TextUploadWorkState::Blocked(_), Some(_))
        | (
            work::TextUploadWorkState::Queued { .. } | work::TextUploadWorkState::Claimed(_),
            None,
        ) => {
            return Err(corruption(
                "text upload intent pointer presence disagrees with work state",
            ));
        }
    }
    let mut input_bytes = measured_row(&pointer_key, pointer_value.as_ref());
    match &intent.phase {
        work::TextUploadPhase::Prepared
        | work::TextUploadPhase::Uploaded
        | work::TextUploadPhase::NonPublicationProven => {
            let (reference_key, reference_value) =
                intent_reachability_row(intent.blob, scope, key)?;
            let Some(observed) = transaction.get(&reference_key).await? else {
                return Err(corruption(
                    "live text upload intent is missing its reachability entry",
                ));
            };
            if observed != reference_value {
                return Err(corruption(
                    "text upload intent reachability entry disagrees with its owner",
                ));
            }
            input_bytes = input_bytes.saturating_add(measured_row(&reference_key, Some(&observed)));
        }
        work::TextUploadPhase::Reclaimable(assignment) => {
            let candidate_key = scoped_key(
                scope,
                index_keys::IndexV2Key::BlobGcCandidate(index_keys::BlobGcCandidateKey {
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    owner: index_keys::BlobGcCandidateKeyOwner::UploadIntent(intent.intent_id),
                    blob_hash: index_keys::BlobHash::new(*intent.blob.hash()),
                }),
            );
            let Some(candidate_value) = transaction.get(&candidate_key).await? else {
                return Err(corruption(
                    "reclaimable text upload intent is missing its qualified candidate",
                ));
            };
            let index_values::IndexV2WorkValue::BlobGcCandidate(candidate) =
                index_values::decode_work_value(&candidate_value)?
            else {
                return Err(corruption(
                    "reclaimable text upload candidate contains another value kind",
                ));
            };
            if candidate
                != (work::BlobGcCandidateValue {
                    owner: work::BlobGcCandidateOwner::UploadIntent(intent.intent_id),
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    blob: intent.blob,
                })
            {
                return Err(corruption(
                    "reclaimable text upload intent and candidate disagree",
                ));
            }
            if matches!(assignment, work::ReclaimAssignment::Assigned(_)) {
                return Err(corruption(
                    "generation candidate preparation passed an assigned reclaim root",
                ));
            }
            input_bytes =
                input_bytes.saturating_add(measured_row(&candidate_key, Some(&candidate_value)));
        }
        work::TextUploadPhase::ReferenceCommitted(_) => {}
    }
    Ok(input_bytes)
}

/// One bounded generation-candidate write set with exact resource accounting.
struct CandidateBatch {
    base: OperationCounters,
    limits: SearchIndexBatchLimits,
    entities: usize,
    input_bytes: u64,
    output_bytes: u64,
    writes: Vec<(Bytes, Bytes)>,
    planned: HashMap<index_keys::BlobHash, work::BlobRef>,
}

impl CandidateBatch {
    fn new(base: OperationCounters, limits: SearchIndexBatchLimits) -> Self {
        Self {
            base,
            limits,
            entities: 0,
            input_bytes: 0,
            output_bytes: 0,
            writes: Vec::new(),
            planned: HashMap::new(),
        }
    }

    fn can_read_another(&self) -> bool {
        self.entities < self.limits.max_entities().get()
    }

    fn charge_owner_row(&mut self, key: &[u8], value: &[u8]) -> Result<CandidateAdmission> {
        let bytes = measured_row(key, Some(value));
        if self.input_bytes.saturating_add(bytes) > self.limits.max_input_bytes().get() {
            return Ok(if self.entities == 0 {
                CandidateAdmission::Indivisible
            } else {
                CandidateAdmission::Full
            });
        }
        self.input_bytes = self
            .input_bytes
            .checked_add(bytes)
            .ok_or_else(|| invariant("text cleanup input counter overflowed"))?;
        Ok(CandidateAdmission::Admitted)
    }

    async fn admit_candidate(
        &mut self,
        transaction: &DbTransaction,
        scope: DataScope,
        operation: &IndexOperationRecord,
        blob: work::BlobRef,
        source_input_bytes: u64,
    ) -> Result<CandidateAdmission> {
        if !self.can_read_another() {
            return Ok(CandidateAdmission::Full);
        }
        let blob_hash = index_keys::BlobHash::new(*blob.hash());
        if let Some(planned) = self.planned.get(&blob_hash) {
            if planned != &blob {
                return Err(corruption(
                    "one text blob hash resolves to different candidate metadata",
                ));
            }
            return self.admit_measurement(source_input_bytes, 0, 0);
        }
        let key = scoped_key(
            scope,
            index_keys::IndexV2Key::BlobGcCandidate(index_keys::BlobGcCandidateKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                owner: index_keys::BlobGcCandidateKeyOwner::GenerationCleanup,
                blob_hash,
            }),
        );
        let observed = transaction.get(&key).await?;
        let candidate = work::BlobGcCandidateValue {
            owner: work::BlobGcCandidateOwner::GenerationCleanup(operation.operation_id()),
            index_id: operation.index_id(),
            generation: operation.generation(),
            blob,
        };
        let value = index_values::encode_work_value(
            &index_values::IndexV2WorkValue::BlobGcCandidate(candidate),
        );
        let candidate_input = measured_row(&key, observed.as_ref());
        let (output_operations, output_bytes) = match observed {
            Some(observed) => {
                let index_values::IndexV2WorkValue::BlobGcCandidate(existing) =
                    index_values::decode_work_value(&observed)?
                else {
                    return Err(corruption(
                        "text generation candidate key contains another value kind",
                    ));
                };
                if existing != candidate {
                    return Err(corruption(
                        "text generation candidate disagrees with its cleanup owner",
                    ));
                }
                (0, 0)
            }
            None => (1, measured_row(&key, Some(&value))),
        };
        let admission = self.admit_measurement(
            source_input_bytes.saturating_add(candidate_input),
            output_operations,
            output_bytes,
        )?;
        if admission == CandidateAdmission::Admitted {
            self.planned.insert(blob_hash, blob);
            if output_operations != 0 {
                self.writes.push((key, value));
            }
        }
        Ok(admission)
    }

    fn admit_measurement(
        &mut self,
        input_bytes: u64,
        output_operations: u64,
        output_bytes: u64,
    ) -> Result<CandidateAdmission> {
        let fits = self.input_bytes.saturating_add(input_bytes)
            <= self.limits.max_input_bytes().get()
            && u64::try_from(self.writes.len())
                .unwrap_or(u64::MAX)
                .saturating_add(output_operations)
                <= self.limits.max_output_operations().get()
            && self.output_bytes.saturating_add(output_bytes)
                <= self.limits.max_output_bytes().get();
        if !fits {
            return Ok(if self.entities == 0 {
                CandidateAdmission::Indivisible
            } else {
                CandidateAdmission::Full
            });
        }
        self.entities = self
            .entities
            .checked_add(1)
            .ok_or_else(|| invariant("text cleanup entity counter overflowed"))?;
        self.input_bytes = self
            .input_bytes
            .checked_add(input_bytes)
            .ok_or_else(|| invariant("text cleanup input counter overflowed"))?;
        self.output_bytes = self
            .output_bytes
            .checked_add(output_bytes)
            .ok_or_else(|| invariant("text cleanup output counter overflowed"))?;
        Ok(CandidateAdmission::Admitted)
    }

    fn stage(&self, transaction: &DbTransaction) -> Result<()> {
        for (key, value) in &self.writes {
            transaction.put(key, value)?;
        }
        Ok(())
    }

    fn finish(&self) -> Result<OperationCounters> {
        add_counters(
            self.base,
            self.entities,
            self.input_bytes,
            u64::try_from(self.writes.len())
                .map_err(|_| invariant("text cleanup write count exceeds u64"))?,
            self.output_bytes,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateAdmission {
    Admitted,
    Full,
    Indivisible,
}

/// One bounded atomic set of generation-owner deletions.
///
/// The batch counts every visited owner even when its hash is absent from the
/// current run. Delete output bytes are the canonical key bytes submitted to
/// SlateDB; operation-checkpoint bytes remain repository-owned accounting.
struct RetirementBatch {
    base: OperationCounters,
    limits: SearchIndexBatchLimits,
    entities: usize,
    input_bytes: u64,
    output_bytes: u64,
    delete_keys: Vec<Bytes>,
}

impl RetirementBatch {
    fn new(
        base: OperationCounters,
        limits: SearchIndexBatchLimits,
        fixed_input_bytes: u64,
    ) -> Self {
        Self {
            base,
            limits,
            entities: 0,
            input_bytes: fixed_input_bytes,
            output_bytes: 0,
            delete_keys: Vec::new(),
        }
    }

    fn can_visit_another(&self) -> bool {
        self.entities < self.limits.max_entities().get()
    }

    fn admit(&mut self, input_bytes: u64, delete_keys: Vec<Bytes>) -> Result<RetirementAdmission> {
        let output_operations = u64::try_from(delete_keys.len())
            .map_err(|_| invariant("text retirement delete count exceeds u64"))?;
        let output_bytes = delete_keys.iter().try_fold(0_u64, |total, key| {
            total
                .checked_add(
                    u64::try_from(key.len())
                        .map_err(|_| invariant("text retirement key length exceeds u64"))?,
                )
                .ok_or_else(|| invariant("text retirement output byte count overflowed"))
        })?;
        let planned_operations = u64::try_from(self.delete_keys.len())
            .map_err(|_| invariant("text retirement planned delete count exceeds u64"))?;
        let fits = self.can_visit_another()
            && self.input_bytes.saturating_add(input_bytes) <= self.limits.max_input_bytes().get()
            && planned_operations.saturating_add(output_operations)
                <= self.limits.max_output_operations().get()
            && self.output_bytes.saturating_add(output_bytes)
                <= self.limits.max_output_bytes().get();
        if !fits {
            return Ok(if self.entities == 0 {
                RetirementAdmission::Indivisible
            } else {
                RetirementAdmission::Full
            });
        }
        self.entities = self
            .entities
            .checked_add(1)
            .ok_or_else(|| invariant("text retirement entity counter overflowed"))?;
        self.input_bytes = self
            .input_bytes
            .checked_add(input_bytes)
            .ok_or_else(|| invariant("text retirement input counter overflowed"))?;
        self.output_bytes = self
            .output_bytes
            .checked_add(output_bytes)
            .ok_or_else(|| invariant("text retirement output counter overflowed"))?;
        self.delete_keys.extend(delete_keys);
        Ok(RetirementAdmission::Admitted)
    }

    fn stage(&self, transaction: &DbTransaction) -> Result<()> {
        for key in &self.delete_keys {
            transaction.delete(key)?;
        }
        Ok(())
    }

    fn finish(&self) -> Result<OperationCounters> {
        add_counters(
            self.base,
            self.entities,
            self.input_bytes,
            u64::try_from(self.delete_keys.len())
                .map_err(|_| invariant("text retirement delete count exceeds u64"))?,
            self.output_bytes,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetirementAdmission {
    Admitted,
    Full,
    Indivisible,
}

enum ManifestResume {
    NextPage(Option<Bytes>),
    WithinPage {
        page_key: index_keys::TextManifestPageKey,
        slot: usize,
    },
}

fn candidate_manifest_resume(
    scope: DataScope,
    operation: &IndexOperationRecord,
    cursor: &IndexCursor,
) -> Result<ManifestResume> {
    if super::blob_gc::is_generation_candidate_start_cursor(scope, operation, cursor)? {
        return Ok(ManifestResume::NextPage(None));
    }
    manifest_owner_resume(scope, operation, cursor)
}

/// Resolves one complete manifest-page or manifest-slot owner cursor.
fn manifest_owner_resume(
    scope: DataScope,
    operation: &IndexOperationRecord,
    cursor: &IndexCursor,
) -> Result<ManifestResume> {
    if let Ok(Key::Data {
        kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::TextManifestPage(key)),
        ..
    }) = Key::parse_from_slice(scope, cursor.as_bytes())
    {
        if key.root.index_id != operation.index_id()
            || key.root.generation != operation.generation()
        {
            return Err(corruption(
                "text manifest cleanup cursor names another generation",
            ));
        }
        return Ok(ManifestResume::NextPage(Some(cursor.as_bytes().clone())));
    }
    let index_keys::GlobalIndexV2Key::BlobReachabilityReference(reference) =
        index_keys::GlobalIndexV2Key::parse_from_slice(cursor.as_bytes())?
    else {
        return Err(corruption(
            "text manifest cleanup cursor has another global key kind",
        ));
    };
    if reference.owner_kind != index_keys::BlobReferenceOwnerKind::ManifestPageSplit
        || reference.scope != scope
    {
        return Err(corruption(
            "text manifest cleanup slot cursor has another owner lane",
        ));
    }
    let index_keys::IndexV2Key::TextManifestPage(page_key) =
        index_keys::IndexV2Key::parse_from_slice(&reference.owner_logical_key)?
    else {
        return Err(corruption(
            "text manifest cleanup slot cursor owner is not a manifest page",
        ));
    };
    if page_key.root.index_id != operation.index_id()
        || page_key.root.generation != operation.generation()
    {
        return Err(corruption(
            "text manifest cleanup slot cursor names another generation",
        ));
    }
    let slot = usize::try_from(reference.owner_slot)
        .map_err(|_| invariant("bounded text manifest slot exceeds usize"))?
        .checked_add(1)
        .ok_or_else(|| invariant("text manifest cleanup slot overflowed"))?;
    Ok(ManifestResume::WithinPage { page_key, slot })
}

fn decode_manifest_page(
    scope: DataScope,
    operation: &IndexOperationRecord,
    key: &[u8],
    value: &[u8],
) -> Result<(index_keys::TextManifestPageKey, work::TextManifestPageValue)> {
    let Key::Data {
        kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::TextManifestPage(key)),
        ..
    } = Key::parse_from_slice(scope, key)?
    else {
        return Err(corruption(
            "text manifest candidate scan yielded another key kind",
        ));
    };
    let index_values::IndexV2WorkValue::TextManifestPage(page) =
        index_values::decode_work_value(value)?
    else {
        return Err(corruption(
            "text manifest page key contains another work value kind",
        ));
    };
    if key.root.index_id != operation.index_id()
        || key.root.generation != operation.generation()
        || key.root.partition != page.partition().fingerprint()
        || key.page != page.page()
        || page.index_id() != operation.index_id()
        || page.generation() != operation.generation()
    {
        return Err(corruption(
            "text manifest page key/value ownership mismatch",
        ));
    }
    Ok((key, page))
}

fn decode_intent_row(
    scope: DataScope,
    operation: &IndexOperationRecord,
    key: &[u8],
    value: &[u8],
) -> Result<(index_keys::TextIntentOwnedKey, work::TextUploadIntentValue)> {
    let Key::Data {
        kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::TextUploadIntent(key)),
        ..
    } = Key::parse_from_slice(scope, key)?
    else {
        return Err(corruption(
            "text upload cleanup scan yielded another key kind",
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
            "text upload intent key/value ownership mismatch",
        ));
    }
    Ok((key, intent))
}

async fn scan_generation(
    transaction: &DbTransaction,
    scope: DataScope,
    kind: index_keys::IndexV2RecordKind,
    operation: &IndexOperationRecord,
    resume: Option<&Bytes>,
) -> Result<slatedb::DbIterator> {
    let prefix = Key::data_prefix(
        scope,
        index_keys::IndexV2Key::generation_prefix(
            kind,
            operation.index_id(),
            operation.generation(),
        ),
    );
    let start = match resume {
        Some(cursor) => {
            let Some(suffix) = cursor.strip_prefix(prefix.as_ref()) else {
                return Err(corruption(
                    "text cleanup cursor is outside its exact generation prefix",
                ));
            };
            Bound::Excluded(Bytes::copy_from_slice(suffix))
        }
        None => Bound::Unbounded,
    };
    Ok(transaction
        .scan_prefix(&prefix, (start, Bound::Unbounded))
        .await?)
}

fn is_scoped_kind(
    scope: DataScope,
    cursor: &IndexCursor,
    expected: index_keys::IndexV2RecordKind,
) -> Result<bool> {
    let Ok(Key::Data {
        kind: DataKeyKind::IndexV2(key),
        ..
    }) = Key::parse_from_slice(scope, cursor.as_bytes())
    else {
        return Ok(false);
    };
    Ok(key.record_kind() == expected)
}

fn is_manifest_slot_cursor(cursor: &IndexCursor) -> Result<bool> {
    let Ok(index_keys::GlobalIndexV2Key::BlobReachabilityReference(reference)) =
        index_keys::GlobalIndexV2Key::parse_from_slice(cursor.as_bytes())
    else {
        return Ok(false);
    };
    Ok(reference.owner_kind == index_keys::BlobReferenceOwnerKind::ManifestPageSplit)
}

fn intent_reachability_row(
    blob: work::BlobRef,
    scope: DataScope,
    owner: index_keys::TextIntentOwnedKey,
) -> Result<(Bytes, Bytes)> {
    let owner_logical_key = index_keys::IndexV2Key::TextUploadIntent(owner).to_bytes();
    let owner_kind = index_keys::BlobReferenceOwnerKind::UploadIntent;
    let value = work::BlobReachabilityReferenceValue::try_new(
        blob,
        owner_kind,
        scope,
        owner_logical_key.clone(),
        0,
    )
    .map_err(|error| invariant(error.to_string()))?;
    let key = index_keys::BlobReferenceGlobalKey::try_new(
        index_keys::BlobHash::new(*blob.hash()),
        owner_kind,
        scope,
        owner_logical_key,
        0,
    )?;
    Ok((
        Key::Global {
            kind: GlobalKeyKind::IndexV2(index_keys::GlobalIndexV2Key::BlobReachabilityReference(
                key,
            )),
        }
        .to_bytes(),
        index_values::encode_work_value(
            &index_values::IndexV2WorkValue::BlobReachabilityReference(value),
        ),
    ))
}

fn intent_candidate_row(scope: DataScope, intent: &work::TextUploadIntentValue) -> (Bytes, Bytes) {
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
                    .map_err(|_| invariant("text cleanup entity count exceeds u64"))?,
            )
            .ok_or_else(|| invariant("text cleanup cumulative entity count overflowed"))?,
        input_bytes: base
            .input_bytes
            .checked_add(input_bytes)
            .ok_or_else(|| invariant("text cleanup cumulative input bytes overflowed"))?,
        output_operations: base
            .output_operations
            .checked_add(output_operations)
            .ok_or_else(|| invariant("text cleanup cumulative operation count overflowed"))?,
        output_bytes: base
            .output_bytes
            .checked_add(output_bytes)
            .ok_or_else(|| invariant("text cleanup cumulative output bytes overflowed"))?,
    })
}

fn progressed_cleanup(aborting: bool, progress: TextCleanupProgress) -> IndexOperationStepResult {
    IndexOperationStepResult::Progressed(if aborting {
        IndexOperationProgress::TextBuild(TextBuildProgress::Aborting(progress))
    } else {
        IndexOperationProgress::TextCleanup(progress)
    })
}

fn operation_error(error: crate::index_v2::IndexOperationModelError) -> HelixDbError {
    HelixDbError::InvariantViolation(error.to_string())
}

fn coordinator_error_is_retryable(error: &blob_publication::BlobPublicationError) -> bool {
    matches!(
        error,
        blob_publication::BlobPublicationError::PublicationOutcomeAmbiguous(_)
            | blob_publication::BlobPublicationError::ObjectStore(_)
            | blob_publication::BlobPublicationError::CoordinatorUnavailable(_)
            | blob_publication::BlobPublicationError::DeleteFenceNotQuiescent
    )
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
    use slatedb::object_store::ObjectStoreExt;
    use slatedb::{Db, IsolationLevel};

    use super::*;
    use crate::index_v2::blob_publication::BlobPublicationCoordinator;
    use crate::index_v2::{
        BlobGcRunId, BlobGcRunRevision, BlobPublicationPermitId, ClaimSequence, IndexComponent,
        IndexElementKind, IndexEntityId, IndexGenerationId, IndexId, IndexIdentity,
        IndexIdentityFamily, IndexOperationExecutionState, IndexOperationFamily, IndexOperationId,
        IndexOperationKind, IndexOperationRevision, IndexRevision, MutationId, OperationClaim,
        TextIntentRevision, TextLogicalVersion, TextManifestRevision, TextUploadIntentId,
        UploadQueuePointerValue, WriterEpoch,
    };

    /// Opens one isolated database for cleanup storage-contract tests.
    async fn test_db(name: &str) -> Db {
        Db::open(name, Arc::new(InMemory::new()))
            .await
            .expect("text cleanup contract database opens")
    }

    /// Returns one valid text drop operation whose generation owns test rows.
    fn operation() -> IndexOperationRecord {
        let identity = IndexIdentity::new(
            IndexIdentityFamily::Text,
            IndexElementKind::Node,
            IndexComponent::try_new("label", "Document").expect("label component validates"),
            IndexComponent::try_new("property", "body").expect("property component validates"),
        );
        IndexOperationRecord::try_new(
            IndexOperationId::from_bytes([1; 16]).expect("operation ID is non-nil"),
            IndexId::initial(),
            identity,
            IndexGenerationId::initial(),
            IndexRevision::initial(),
            IndexOperationRevision::initial(),
            IndexOperationKind::Drop,
            IndexOperationFamily::Text,
            IndexOperationProgress::TextCleanup(TextCleanupProgress::PrepareCandidates(
                PrefixScanProgress {
                    cursor: None,
                    counters: OperationCounters::default(),
                },
            )),
            0,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .expect("text cleanup operation is valid")
    }

    /// Constructs positive transaction limits for one cleanup step.
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
        .expect("cleanup test limits are valid")
    }

    /// Constructs one deterministic split with internally consistent bounds.
    fn split(seed: u8) -> work::SplitRef {
        let blob = work::BlobRef::new([seed; 32], 64);
        work::SplitRef::try_new(blob, 0, 0, 0, blob.size()).expect("matching split sizes are valid")
    }

    /// Constructs a split whose blob identity matches exact payload bytes.
    fn content_split(payload: &[u8]) -> work::SplitRef {
        let blob = work::BlobRef::new(
            Sha256::digest(payload).into(),
            u64::try_from(payload.len()).expect("test payload length fits u64"),
        );
        work::SplitRef::try_new(blob, 0, 0, 0, blob.size()).expect("content split validates")
    }

    /// Writes one manifest page and optionally all exact reachability rows.
    async fn put_manifest_page(
        db: &Db,
        scope: DataScope,
        operation: &IndexOperationRecord,
        entries: Vec<work::SplitRef>,
        with_reachability: bool,
    ) -> (Bytes, index_keys::TextManifestPageKey) {
        let partition = work::TextPartition::Unpartitioned;
        let key = index_keys::TextManifestPageKey {
            root: index_keys::TextManifestRootKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                partition: partition.fingerprint(),
            },
            page: 0,
        };
        let value = work::TextManifestPageValue::try_new(
            operation.index_id(),
            operation.generation(),
            partition,
            key.page,
            entries.clone(),
        )
        .expect("manifest page validates");
        let encoded_key = scoped_key(scope, index_keys::IndexV2Key::TextManifestPage(key));
        db.put(
            encoded_key.clone(),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestPage(
                value,
            )),
        )
        .await
        .expect("manifest page is written");
        if with_reachability {
            for (slot, entry) in entries.into_iter().enumerate() {
                let slot = u32::try_from(slot).expect("test slot fits u32");
                let (reference_key, reference_value) =
                    super::super::attachment::manifest_page_reachability_row(
                        entry.blob(),
                        scope,
                        key,
                        slot,
                    );
                db.put(reference_key, reference_value)
                    .await
                    .expect("manifest reachability row is written");
            }
        }
        (encoded_key, key)
    }

    /// Writes one hidden artifact and its exact global reachability row.
    async fn put_artifact(
        db: &Db,
        scope: DataScope,
        operation: &IndexOperationRecord,
        split: work::SplitRef,
        ordinal: u32,
    ) -> (Bytes, Bytes) {
        let partition = work::TextPartition::Unpartitioned;
        let key = index_keys::TextBuildArtifactKey {
            root: index_keys::TextManifestRootKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                partition: partition.fingerprint(),
            },
            ordinal,
        };
        let owner_key = scoped_key(scope, index_keys::IndexV2Key::TextBuildArtifact(key));
        db.put(
            owner_key.clone(),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextBuildArtifact(
                work::TextBuildArtifactValue {
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    partition,
                    artifact_ordinal: key.ordinal,
                    split,
                    source_intent_id: TextUploadIntentId::from_bytes([41; 16])
                        .expect("source intent ID is non-nil"),
                },
            )),
        )
        .await
        .expect("artifact owner is written");
        let (reference_key, reference_value) =
            super::super::attachment::build_artifact_reachability_row(split.blob(), scope, key);
        db.put(reference_key.clone(), reference_value)
            .await
            .expect("artifact reachability row is written");
        (owner_key, reference_key)
    }

    /// Writes every post-GC deletable text row and returns its exact keys.
    async fn put_physical_generation_rows(
        db: &Db,
        scope: DataScope,
        operation: &IndexOperationRecord,
    ) -> Vec<Bytes> {
        let split = split(61);
        let (page_key, typed_page_key) =
            put_manifest_page(db, scope, operation, vec![split], false).await;
        let partition = work::TextPartition::Unpartitioned;
        let root_key = scoped_key(
            scope,
            index_keys::IndexV2Key::TextManifestRoot(typed_page_key.root),
        );
        db.put(
            &root_key,
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestRoot(
                work::TextManifestRootValue::try_new(
                    operation.index_id(),
                    operation.generation(),
                    partition.clone(),
                    TextManifestRevision::initial(),
                    1,
                    1,
                )
                .expect("manifest root validates"),
            )),
        )
        .await
        .expect("manifest root is written");

        let intent_id = TextUploadIntentId::from_bytes([62; 16]).expect("intent ID is non-nil");
        let proof_key = scoped_key(
            scope,
            index_keys::IndexV2Key::ActiveMutationCommitProof(index_keys::TextIntentOwnedKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                intent_id,
            }),
        );
        db.put(
            &proof_key,
            index_values::encode_work_value(
                &index_values::IndexV2WorkValue::ActiveMutationCommitProof(
                    work::ActiveMutationCommitProofValue {
                        intent_id,
                        index_id: operation.index_id(),
                        generation: operation.generation(),
                        partition: partition.clone(),
                        writer_epoch: WriterEpoch::from_bytes([63; 16])
                            .expect("writer epoch is non-nil"),
                        mutation_id: MutationId::from_bytes([64; 16])
                            .expect("mutation ID is non-nil"),
                        active_record_revision: operation.index_record_revision(),
                        logical_version: TextLogicalVersion::initial(),
                        destination: work::TextManifestSplitLocation::try_new(0, 0)
                            .expect("manifest location validates"),
                        split,
                    },
                ),
            ),
        )
        .await
        .expect("active mutation proof is written");

        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(65),
        };
        let entity_key = scoped_key(
            scope,
            index_keys::IndexV2Key::TextEntityState(index_keys::TextEntityStateKey {
                root: typed_page_key.root,
                entity,
            }),
        );
        db.put(
            &entity_key,
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextEntityState(
                work::TextEntityStateValue {
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    partition: partition.clone(),
                    entity_kind: entity.kind,
                    entity_id: entity.id,
                    logical_version: TextLogicalVersion::initial(),
                    live: true,
                },
            )),
        )
        .await
        .expect("text entity state is written");

        let builder_key = index_keys::IndexEntityStateKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            entity,
        };
        let delta_key = scoped_key(scope, index_keys::IndexV2Key::BuildDelta(builder_key));
        db.put(
            &delta_key,
            index_values::encode_work_value(&index_values::IndexV2WorkValue::CoalescedBuildDelta(
                work::CoalescedBuildDeltaValue {
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    entity_kind: entity.kind,
                    entity_id: entity.id,
                },
            )),
        )
        .await
        .expect("text build delta is written");
        let applied_key = scoped_key(scope, index_keys::IndexV2Key::AppliedState(builder_key));
        db.put(
            &applied_key,
            index_values::encode_work_value(&index_values::IndexV2WorkValue::AppliedEntityState(
                work::AppliedEntityStateValue {
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    entity_kind: entity.kind,
                    entity_id: entity.id,
                    state: work::AppliedFamilyState::Text(Some((
                        partition,
                        TextLogicalVersion::initial(),
                    ))),
                },
            )),
        )
        .await
        .expect("text applied state is written");

        vec![
            proof_key,
            page_key,
            root_key,
            entity_key,
            delta_key,
            applied_key,
        ]
    }

    /// Writes one exact `FencesClosed` generation run and pending members.
    async fn put_fences_closed_run(
        db: &Db,
        scope: DataScope,
        operation: &IndexOperationRecord,
        blobs: &[work::BlobRef],
    ) -> BlobGcRunId {
        let run_id = BlobGcRunId::from_bytes([71; 16]).expect("run ID is non-nil");
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
            work::BlobGcPhase::FencesClosed,
            u32::try_from(blobs.len()).expect("test member count fits u32"),
        )
        .expect("generation root validates");
        db.put(
            index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes(),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcEntry(
                work::BlobGcEntryValue::RunRoot(root),
            )),
        )
        .await
        .expect("generation root is written");
        for blob in blobs {
            db.put(
                index_keys::GlobalIndexV2Key::BlobGcCandidateMember {
                    run_id,
                    blob_hash: index_keys::BlobHash::new(*blob.hash()),
                }
                .to_bytes(),
                index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcEntry(
                    work::BlobGcEntryValue::CandidateMember(work::BlobGcCandidateMemberValue {
                        run_id,
                        blob: *blob,
                        state: work::BlobGcMemberState::PendingDisposition { owner_cursor: None },
                    }),
                )),
            )
            .await
            .expect("generation member is written");
        }
        run_id
    }

    /// Writes an empty terminal generation root plus both retained current mark sets.
    async fn put_terminal_delete_root(
        db: &Db,
        scope: DataScope,
        operation: &IndexOperationRecord,
        blob: work::BlobRef,
    ) -> BlobGcRunId {
        let run_id = put_fences_closed_run(db, scope, operation, &[blob]).await;
        db.delete(
            index_keys::GlobalIndexV2Key::BlobGcCandidateMember {
                run_id,
                blob_hash: index_keys::BlobHash::new(*blob.hash()),
            }
            .to_bytes(),
        )
        .await
        .expect("terminal member is removed");
        let root_key = index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes();
        let root_value = db.get(&root_key).await.unwrap().unwrap();
        let index_values::IndexV2WorkValue::BlobGcEntry(work::BlobGcEntryValue::RunRoot(mut root)) =
            index_values::decode_work_value(&root_value).unwrap()
        else {
            panic!("terminal root key contains the root value kind");
        };
        root.phase = work::BlobGcPhase::Delete {
            completed_first_attempt: work::GcScanAttempt::new(1).unwrap(),
            completed_second_attempt: work::GcScanAttempt::new(1).unwrap(),
            member_cursor: Some(
                IndexCursor::try_new(
                    index_keys::GlobalIndexV2Key::BlobGcCandidateMember {
                        run_id,
                        blob_hash: index_keys::BlobHash::new(*blob.hash()),
                    }
                    .to_bytes(),
                )
                .unwrap(),
            ),
            stale_mark_cleanup: work::StaleMarkCleanup::Complete,
        };
        db.put(
            root_key,
            index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcEntry(
                work::BlobGcEntryValue::RunRoot(root),
            )),
        )
        .await
        .expect("terminal root is written");
        for pass in [
            index_keys::BlobGcPass::First,
            index_keys::BlobGcPass::Second,
        ] {
            let blob_hash = index_keys::BlobHash::new(*blob.hash());
            db.put(
                index_keys::GlobalIndexV2Key::BlobGcReachabilityMark {
                    run_id,
                    pass,
                    scan_attempt: NonZeroU64::MIN,
                    blob_hash,
                }
                .to_bytes(),
                index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcEntry(
                    work::BlobGcEntryValue::ReachabilityMark(work::BlobGcReachabilityMarkValue {
                        run_id,
                        first_pass: pass == index_keys::BlobGcPass::First,
                        scan_attempt: work::GcScanAttempt::new(1).unwrap(),
                        blob_hash,
                        referenced: false,
                    }),
                )),
            )
            .await
            .expect("terminal current mark is written");
        }
        run_id
    }

    /// Constructs an assigned operation checkpoint for owner retirement.
    fn retirement_progress(
        scope: DataScope,
        operation: &IndexOperationRecord,
        run_id: BlobGcRunId,
        last_blob: work::BlobRef,
    ) -> GcProgress {
        GcProgress {
            gc_run_id: Some(run_id),
            candidate_cursor: Some(
                IndexCursor::try_new(generation_candidate_key(scope, operation, last_blob))
                    .expect("candidate cursor validates"),
            ),
            stage_cursor: None,
            counters: OperationCounters::default(),
        }
    }

    /// Writes one prepared intent, runnable pointer, and live reachability row.
    async fn put_prepared_intent(
        db: &Db,
        scope: DataScope,
        operation: &IndexOperationRecord,
        split: work::SplitRef,
    ) -> (Bytes, Bytes, Bytes) {
        let intent = work::TextUploadIntentValue::try_new(
            TextUploadIntentId::from_bytes([42; 16]).expect("intent ID is non-nil"),
            TextIntentRevision::initial(),
            operation.index_id(),
            operation.identity().clone(),
            operation.generation(),
            work::TextPartition::Unpartitioned,
            split.blob(),
            BlobPublicationPermitId::from_bytes([43; 16]).expect("permit ID is non-nil"),
            work::TextUploadOwner::Build {
                operation_id: operation.operation_id(),
                expected_operation_revision: operation.operation_revision(),
            },
            work::TextUploadAttachment::ManifestSplit(split),
            work::TextUploadPhase::Prepared,
            0,
            work::TextUploadWorkState::Queued {
                not_before_unix_millis: None,
            },
        )
        .expect("prepared intent validates");
        let owned_key = index_keys::TextIntentOwnedKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            intent_id: intent.intent_id,
        };
        let intent_key = scoped_key(scope, index_keys::IndexV2Key::TextUploadIntent(owned_key));
        db.put(
            intent_key.clone(),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextUploadIntent(
                Box::new(intent.clone()),
            )),
        )
        .await
        .expect("prepared intent is written");
        let pointer_key = index_keys::GlobalIndexV2Key::UploadPointer(intent.intent_id).to_bytes();
        db.put(
            pointer_key.clone(),
            index_values::encode_metadata_value(&IndexV2MetadataValue::UploadQueuePointer(
                UploadQueuePointerValue {
                    scope,
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    record_revision: intent.revision,
                },
            )),
        )
        .await
        .expect("prepared intent pointer is written");
        let (reference_key, reference_value) =
            intent_reachability_row(intent.blob, scope, owned_key)
                .expect("intent reachability row validates");
        db.put(reference_key.clone(), reference_value)
            .await
            .expect("intent reachability row is written");
        (intent_key, pointer_key, reference_key)
    }

    /// Writes one coordinator-backed intent with its phase-appropriate anchors.
    async fn put_coordinated_intent(
        db: &Db,
        scope: DataScope,
        operation: &IndexOperationRecord,
        split: work::SplitRef,
        permit: blob_publication::BlobPublicationPermit,
        phase: work::TextUploadPhase,
        intent_id: TextUploadIntentId,
    ) -> (
        work::TextUploadIntentValue,
        super::super::upload::PreparedUploadRows,
    ) {
        let intent = work::TextUploadIntentValue::try_new(
            intent_id,
            TextIntentRevision::initial(),
            operation.index_id(),
            operation.identity().clone(),
            operation.generation(),
            work::TextPartition::Unpartitioned,
            split.blob(),
            permit.id(),
            work::TextUploadOwner::Build {
                operation_id: operation.operation_id(),
                expected_operation_revision: operation.operation_revision(),
            },
            work::TextUploadAttachment::BuildArtifact {
                artifact_ordinal: 0,
                split,
            },
            phase.clone(),
            0,
            work::TextUploadWorkState::Queued {
                not_before_unix_millis: None,
            },
        )
        .expect("coordinated cleanup intent validates");
        let rows = super::super::upload::upload_anchor_rows(scope, &intent)
            .expect("coordinated cleanup rows encode");
        db.put(&rows.intent_key, &rows.intent_value)
            .await
            .expect("coordinated cleanup intent is written");
        db.put(&rows.pointer_key, &rows.pointer_value)
            .await
            .expect("coordinated cleanup pointer is written");
        if !matches!(phase, work::TextUploadPhase::ReferenceCommitted(_)) {
            db.put(&rows.reachability_key, &rows.reachability_value)
                .await
                .expect("coordinated cleanup reachability is written");
        }
        (intent, rows)
    }

    /// Returns one exact claimed retirement operation for a prepared turn.
    fn claimed_retirement_operation(progress: GcProgress, sequence: u64) -> IndexOperationRecord {
        let base = operation();
        IndexOperationRecord::try_new(
            base.operation_id(),
            base.index_id(),
            base.identity().clone(),
            base.generation(),
            base.index_record_revision(),
            IndexOperationRevision::new(sequence).expect("operation revision is non-zero"),
            base.kind(),
            base.family(),
            IndexOperationProgress::TextCleanup(TextCleanupProgress::RetireUploadIntents(progress)),
            0,
            IndexOperationExecutionState::Claimed(OperationClaim {
                writer_epoch: WriterEpoch::from_bytes([91; 16]).expect("writer epoch is non-nil"),
                sequence: ClaimSequence::new(sequence).expect("claim sequence is non-zero"),
            }),
        )
        .expect("claimed intent-retirement operation validates")
    }

    /// Prepares, stages, and commits one exact intent-retirement turn.
    async fn run_intent_retirement(
        db: &Db,
        scope: DataScope,
        operation: &IndexOperationRecord,
        progress: &GcProgress,
        coordinator: &dyn blob_publication::BlobPublicationCoordinator,
    ) -> IndexOperationStepResult {
        let prepared = prepare_upload_intent_retirement(
            db,
            scope,
            operation,
            progress,
            false,
            limits(8, 64 * 1024, 8, 64 * 1024),
            coordinator,
        )
        .await
        .expect("intent retirement prepares");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("intent retirement transaction opens");
        let result = prepared
            .stage(&transaction, scope, operation)
            .await
            .expect("intent retirement stages");
        transaction
            .commit()
            .await
            .expect("intent retirement transaction commits");
        result
    }

    /// Extracts the exact retained GC progress from one normalization turn.
    fn intent_retirement_progress(result: IndexOperationStepResult) -> GcProgress {
        let IndexOperationStepResult::Progressed(IndexOperationProgress::TextCleanup(
            TextCleanupProgress::RetireUploadIntents(progress),
        )) = result
        else {
            panic!("intent retirement must retain its cleanup lane");
        };
        progress
    }

    /// Writes one reclaimable intent assigned to an independent GC run.
    async fn put_assigned_reclaim_intent(
        db: &Db,
        scope: DataScope,
        operation: &IndexOperationRecord,
        run_id: BlobGcRunId,
    ) -> work::TextUploadIntentValue {
        let split = split(51);
        let intent = work::TextUploadIntentValue::try_new(
            TextUploadIntentId::from_bytes([52; 16]).expect("intent ID is non-nil"),
            TextIntentRevision::initial(),
            operation.index_id(),
            operation.identity().clone(),
            operation.generation(),
            work::TextPartition::Unpartitioned,
            split.blob(),
            BlobPublicationPermitId::from_bytes([53; 16]).expect("permit ID is non-nil"),
            work::TextUploadOwner::Build {
                operation_id: operation.operation_id(),
                expected_operation_revision: operation.operation_revision(),
            },
            work::TextUploadAttachment::ManifestSplit(split),
            work::TextUploadPhase::Reclaimable(work::ReclaimAssignment::Assigned(run_id)),
            0,
            work::TextUploadWorkState::Queued {
                not_before_unix_millis: None,
            },
        )
        .expect("assigned reclaim intent validates");
        let key = scoped_key(
            scope,
            index_keys::IndexV2Key::TextUploadIntent(index_keys::TextIntentOwnedKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                intent_id: intent.intent_id,
            }),
        );
        db.put(
            key,
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextUploadIntent(
                Box::new(intent.clone()),
            )),
        )
        .await
        .expect("assigned reclaim intent is written");
        intent
    }

    /// Executes and commits one cleanup transaction.
    async fn run_step(
        db: &Db,
        scope: DataScope,
        operation: &IndexOperationRecord,
        progress: &TextCleanupProgress,
        aborting: bool,
        limits: SearchIndexBatchLimits,
    ) -> Result<IndexOperationStepResult> {
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("cleanup transaction begins");
        let result =
            step_cleanup(&transaction, scope, operation, progress, aborting, limits).await?;
        transaction
            .commit()
            .await
            .expect("cleanup transaction commits");
        Ok(result)
    }

    /// Extracts drop cleanup progress while rejecting a lane change.
    fn drop_progress(result: IndexOperationStepResult) -> TextCleanupProgress {
        let IndexOperationStepResult::Progressed(IndexOperationProgress::TextCleanup(progress)) =
            result
        else {
            panic!("text drop step returns drop cleanup progress");
        };
        progress
    }

    /// Returns the exact operation-owned candidate key for one blob.
    fn generation_candidate_key(
        scope: DataScope,
        operation: &IndexOperationRecord,
        blob: work::BlobRef,
    ) -> Bytes {
        scoped_key(
            scope,
            index_keys::IndexV2Key::BlobGcCandidate(index_keys::BlobGcCandidateKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                owner: index_keys::BlobGcCandidateKeyOwner::GenerationCleanup,
                blob_hash: index_keys::BlobHash::new(*blob.hash()),
            }),
        )
    }

    #[tokio::test]
    async fn physical_cleanup_deletes_each_typed_lane_boundedly_then_requires_finish_drain() {
        let db = test_db("text-cleanup-physical-lanes").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let physical_keys = put_physical_generation_rows(&db, scope, &operation).await;
        let limits = limits(1, 64 * 1024, 1, 64 * 1024);
        let mut progress = TextCleanupProgress::DeleteEntityState(PrefixScanProgress {
            cursor: None,
            counters: OperationCounters::default(),
        });
        let finish = loop {
            let next = drop_progress(
                run_step(&db, scope, &operation, &progress, false, limits)
                    .await
                    .expect("physical cleanup advances"),
            );
            match next {
                TextCleanupProgress::DeleteEntityState(_) => progress = next,
                TextCleanupProgress::FinishDrain(_) => break next,
                TextCleanupProgress::BeginDrain(_)
                | TextCleanupProgress::PrepareCandidates(_)
                | TextCleanupProgress::AcquireDeleteFences(_)
                | TextCleanupProgress::RetireManifest(_)
                | TextCleanupProgress::RetireArtifacts(_)
                | TextCleanupProgress::RetireUploadIntents(_)
                | TextCleanupProgress::MarkReachability(_)
                | TextCleanupProgress::DeleteBlobs(_)
                | TextCleanupProgress::Finalize(_) => {
                    panic!("physical cleanup reached an unexpected lane")
                }
            }
        };
        for key in physical_keys {
            assert!(
                db.get(key)
                    .await
                    .expect("physical row absence is readable")
                    .is_none(),
                "every typed physical row is removed"
            );
        }
        assert!(matches!(
            finish,
            TextCleanupProgress::FinishDrain(crate::index_v2::DrainProgress {
                drain_epoch: None,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn physical_cleanup_rejects_every_surviving_owner_before_any_row_delete() {
        let db = test_db("text-cleanup-physical-candidate-barrier").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let physical_keys = put_physical_generation_rows(&db, scope, &operation).await;
        let blob = split(66).blob();
        let candidate_key = generation_candidate_key(scope, &operation, blob);
        db.put(
            &candidate_key,
            index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcCandidate(
                work::BlobGcCandidateValue {
                    owner: work::BlobGcCandidateOwner::GenerationCleanup(operation.operation_id()),
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    blob,
                },
            )),
        )
        .await
        .expect("surviving candidate is written");
        let progress = TextCleanupProgress::DeleteEntityState(PrefixScanProgress {
            cursor: None,
            counters: OperationCounters::default(),
        });
        let step_limits = limits(8, 64 * 1024, 8, 64 * 1024);
        assert!(matches!(
            run_step(&db, scope, &operation, &progress, false, step_limits,).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        assert!(db.get(&candidate_key).await.unwrap().is_some());
        db.delete(&candidate_key)
            .await
            .expect("candidate is removed for the next barrier check");

        let (artifact_key, artifact_reference_key) =
            put_artifact(&db, scope, &operation, split(67), 0).await;
        assert!(matches!(
            run_step(&db, scope, &operation, &progress, false, step_limits,).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        db.delete(artifact_key)
            .await
            .expect("artifact owner is removed for the next barrier check");
        db.delete(artifact_reference_key)
            .await
            .expect("artifact reference is removed with its test owner");

        let (intent_key, _, _) = put_prepared_intent(&db, scope, &operation, split(68)).await;
        assert!(matches!(
            run_step(&db, scope, &operation, &progress, false, step_limits,).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        assert!(db.get(intent_key).await.unwrap().is_some());
        for key in physical_keys {
            assert!(
                db.get(key).await.unwrap().is_some(),
                "the absence barrier runs before physical deletion"
            );
        }
    }

    #[tokio::test]
    async fn physical_cleanup_blocks_below_exact_row_limits_and_accepts_the_boundary() {
        let db = test_db("text-cleanup-physical-exact-limits").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let physical_keys = put_physical_generation_rows(&db, scope, &operation).await;
        let proof_key = physical_keys.first().expect("proof key is retained first");
        let proof_value = db
            .get(proof_key)
            .await
            .expect("proof row is readable")
            .expect("proof row exists");
        let input_bytes = measured_row(proof_key, Some(&proof_value));
        let output_bytes = u64::try_from(proof_key.len()).expect("proof key length fits u64");
        let progress = TextCleanupProgress::DeleteEntityState(PrefixScanProgress {
            cursor: None,
            counters: OperationCounters::default(),
        });
        assert!(matches!(
            run_step(
                &db,
                scope,
                &operation,
                &progress,
                false,
                limits(1, input_bytes - 1, 1, output_bytes),
            )
            .await
            .expect("under-limit cleanup returns a typed blocker"),
            IndexOperationStepResult::Blocked(
                crate::index_v2::IndexOperationBlocker::InvariantViolation
            )
        ));
        assert!(db.get(proof_key).await.unwrap().is_some());
        assert!(matches!(
            drop_progress(
                run_step(
                    &db,
                    scope,
                    &operation,
                    &progress,
                    false,
                    limits(1, input_bytes, 1, output_bytes),
                )
                .await
                .expect("exact-limit cleanup advances"),
            ),
            TextCleanupProgress::DeleteEntityState(PrefixScanProgress {
                cursor: Some(_),
                ..
            })
        ));
        assert!(db.get(proof_key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn empty_physical_cleanup_preserves_abort_kind_through_finish_drain() {
        let db = test_db("text-cleanup-empty-physical-abort").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let limits = limits(1, 1024, 1, 1024);
        let result = run_step(
            &db,
            scope,
            &operation,
            &TextCleanupProgress::DeleteEntityState(PrefixScanProgress {
                cursor: None,
                counters: OperationCounters::default(),
            }),
            true,
            limits,
        )
        .await
        .expect("empty abort cleanup advances");
        let IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
            TextBuildProgress::Aborting(finish),
        )) = result
        else {
            panic!("physical abort cleanup retains its build operation kind");
        };
        assert!(matches!(
            finish,
            TextCleanupProgress::FinishDrain(crate::index_v2::DrainProgress {
                drain_epoch: None,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn empty_cleanup_crosses_the_barrier_and_enters_fence_acquisition() {
        let db = test_db("text-cleanup-empty-generation").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let limits = limits(8, 16 * 1024, 8, 16 * 1024);

        let barrier = drop_progress(
            run_step(
                &db,
                scope,
                &operation,
                &TextCleanupProgress::PrepareCandidates(PrefixScanProgress {
                    cursor: None,
                    counters: OperationCounters::default(),
                }),
                false,
                limits,
            )
            .await
            .expect("empty reclaim barrier advances"),
        );
        let TextCleanupProgress::PrepareCandidates(barrier) = barrier else {
            panic!("empty reclaim barrier remains in candidate preparation");
        };
        assert!(super::super::blob_gc::is_generation_candidate_start_cursor(
            scope,
            &operation,
            barrier.cursor.as_ref().expect("barrier stores its marker")
        )
        .expect("candidate marker is valid"));
        assert!(
            db.get(barrier.cursor.as_ref().unwrap().as_bytes())
                .await
                .expect("candidate marker key is readable")
                .is_none(),
            "the typed phase marker is never persisted as a candidate row"
        );

        let next = drop_progress(
            run_step(
                &db,
                scope,
                &operation,
                &TextCleanupProgress::PrepareCandidates(barrier),
                false,
                limits,
            )
            .await
            .expect("empty candidate lanes advance"),
        );
        assert!(matches!(
            next,
            TextCleanupProgress::AcquireDeleteFences(GcProgress {
                gc_run_id: None,
                candidate_cursor: None,
                stage_cursor: None,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn manifest_candidates_resume_inside_a_page_and_preserve_owners() {
        let db = test_db("text-cleanup-manifest-resume").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let first = split(11);
        let second = split(12);
        let (page_key, _) =
            put_manifest_page(&db, scope, &operation, vec![first, second], true).await;
        let limits = limits(1, 64 * 1024, 1, 64 * 1024);
        let start = PrefixScanProgress {
            cursor: Some(
                super::super::blob_gc::generation_candidate_start_cursor(scope, &operation)
                    .unwrap(),
            ),
            counters: OperationCounters::default(),
        };

        let first_progress = drop_progress(
            run_step(
                &db,
                scope,
                &operation,
                &TextCleanupProgress::PrepareCandidates(start),
                false,
                limits,
            )
            .await
            .expect("first manifest slot advances"),
        );
        let TextCleanupProgress::PrepareCandidates(first_progress) = first_progress else {
            panic!("first manifest slot stays in candidate preparation");
        };
        assert!(is_manifest_slot_cursor(
            first_progress
                .cursor
                .as_ref()
                .expect("first slot stores a cursor")
        )
        .expect("first slot cursor is valid"));
        assert!(db
            .get(generation_candidate_key(scope, &operation, first.blob()))
            .await
            .expect("first candidate is readable")
            .is_some());
        assert!(db.get(&page_key).await.unwrap().is_some());

        let second_progress = drop_progress(
            run_step(
                &db,
                scope,
                &operation,
                &TextCleanupProgress::PrepareCandidates(first_progress),
                false,
                limits,
            )
            .await
            .expect("second manifest slot advances"),
        );
        let TextCleanupProgress::PrepareCandidates(second_progress) = second_progress else {
            panic!("second manifest slot stays in candidate preparation");
        };
        assert_eq!(
            second_progress
                .cursor
                .as_ref()
                .expect("completed page stores its key")
                .as_bytes(),
            &page_key
        );
        assert_eq!(second_progress.counters.entities, 2);
        assert_eq!(second_progress.counters.output_operations, 2);
        assert!(db
            .get(generation_candidate_key(scope, &operation, second.blob()))
            .await
            .expect("second candidate is readable")
            .is_some());
        assert!(
            db.get(&page_key).await.unwrap().is_some(),
            "candidate preparation must retain the manifest owner"
        );
    }

    #[tokio::test]
    async fn artifact_and_intent_candidates_coalesce_without_retiring_owners() {
        let db = test_db("text-cleanup-artifact-intent-coalesce").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let split = split(41);
        let (artifact_key, artifact_reference_key) =
            put_artifact(&db, scope, &operation, split, 0).await;
        let (intent_key, pointer_key, intent_reference_key) =
            put_prepared_intent(&db, scope, &operation, split).await;
        let limits = limits(8, 64 * 1024, 8, 64 * 1024);

        let artifact_progress = drop_progress(
            run_step(
                &db,
                scope,
                &operation,
                &TextCleanupProgress::PrepareCandidates(PrefixScanProgress {
                    cursor: Some(
                        super::super::blob_gc::generation_candidate_start_cursor(scope, &operation)
                            .unwrap(),
                    ),
                    counters: OperationCounters::default(),
                }),
                false,
                limits,
            )
            .await
            .expect("artifact candidate advances"),
        );
        let TextCleanupProgress::PrepareCandidates(artifact_progress) = artifact_progress else {
            panic!("artifact scan remains in candidate preparation");
        };
        assert_eq!(artifact_progress.counters.entities, 1);
        assert_eq!(artifact_progress.counters.output_operations, 1);

        let intent_progress = drop_progress(
            run_step(
                &db,
                scope,
                &operation,
                &TextCleanupProgress::PrepareCandidates(artifact_progress),
                false,
                limits,
            )
            .await
            .expect("intent candidate advances"),
        );
        let TextCleanupProgress::PrepareCandidates(intent_progress) = intent_progress else {
            panic!("intent scan remains in candidate preparation");
        };
        assert_eq!(intent_progress.counters.entities, 2);
        assert_eq!(
            intent_progress.counters.output_operations, 1,
            "the shared blob keeps one generation-owned candidate"
        );
        assert!(matches!(
            index_keys::GlobalIndexV2Key::parse_from_slice(
                intent_progress.cursor.as_ref().unwrap().as_bytes()
            ),
            Ok(index_keys::GlobalIndexV2Key::UploadPointer(_))
        ));

        let candidate_value = db
            .get(generation_candidate_key(scope, &operation, split.blob()))
            .await
            .expect("generation candidate is readable")
            .expect("generation candidate exists");
        let index_values::IndexV2WorkValue::BlobGcCandidate(candidate) =
            index_values::decode_work_value(&candidate_value).expect("candidate decodes")
        else {
            panic!("candidate key contains the candidate value kind");
        };
        assert_eq!(
            candidate.owner,
            work::BlobGcCandidateOwner::GenerationCleanup(operation.operation_id())
        );
        for key in [
            artifact_key,
            artifact_reference_key,
            intent_key,
            pointer_key,
            intent_reference_key,
        ] {
            assert!(
                db.get(&key).await.expect("owner row is readable").is_some(),
                "candidate preparation retains every owner and support row"
            );
        }
    }

    #[tokio::test]
    async fn fences_closed_handoff_preserves_both_operation_gc_cursors() {
        let db = test_db("text-cleanup-fences-closed-handoff").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let blob = split(61).blob();
        let run_id = put_fences_closed_run(&db, scope, &operation, &[blob]).await;
        let progress = retirement_progress(scope, &operation, run_id, blob);

        let next = drop_progress(
            run_step(
                &db,
                scope,
                &operation,
                &TextCleanupProgress::AcquireDeleteFences(progress.clone()),
                false,
                limits(8, 64 * 1024, 8, 64 * 1024),
            )
            .await
            .expect("closed fences authorize owner retirement"),
        );
        assert_eq!(next, TextCleanupProgress::RetireManifest(progress));
    }

    #[tokio::test]
    async fn completed_reachability_root_advances_the_parked_operation_lane() {
        let db = test_db("text-cleanup-reachability-handoff").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let blob = split(62).blob();
        let run_id = put_fences_closed_run(&db, scope, &operation, &[blob]).await;
        let progress = retirement_progress(scope, &operation, run_id, blob);
        let root_key = index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes();
        let root_value = db
            .get(&root_key)
            .await
            .expect("reachability root is readable")
            .expect("reachability root exists");
        let index_values::IndexV2WorkValue::BlobGcEntry(work::BlobGcEntryValue::RunRoot(mut root)) =
            index_values::decode_work_value(&root_value).expect("reachability root decodes")
        else {
            panic!("reachability root key contains another value kind");
        };
        root.phase = work::BlobGcPhase::Delete {
            completed_first_attempt: work::GcScanAttempt::new(1).unwrap(),
            completed_second_attempt: work::GcScanAttempt::new(1).unwrap(),
            member_cursor: None,
            stale_mark_cleanup: work::StaleMarkCleanup::Pending { mark_cursor: None },
        };
        db.put(
            root_key,
            index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcEntry(
                work::BlobGcEntryValue::RunRoot(root),
            )),
        )
        .await
        .expect("completed reachability root is written");

        let next = drop_progress(
            run_step(
                &db,
                scope,
                &operation,
                &TextCleanupProgress::MarkReachability(progress.clone()),
                false,
                limits(8, 64 * 1024, 8, 64 * 1024),
            )
            .await
            .expect("completed reachability advances the operation"),
        );
        assert_eq!(next, TextCleanupProgress::DeleteBlobs(progress));
    }

    #[tokio::test]
    async fn terminal_blob_batch_atomically_advances_to_entity_cleanup() {
        let db = test_db("text-cleanup-terminal-blob-handoff").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let blob = split(63).blob();
        let run_id = put_terminal_delete_root(&db, scope, &operation, blob).await;
        let progress = retirement_progress(scope, &operation, run_id, blob);

        let next = drop_progress(
            run_step(
                &db,
                scope,
                &operation,
                &TextCleanupProgress::DeleteBlobs(progress),
                false,
                limits(8, 64 * 1024, 8, 64 * 1024),
            )
            .await
            .expect("terminal root handoff advances"),
        );
        assert!(matches!(
            next,
            TextCleanupProgress::DeleteEntityState(PrefixScanProgress { cursor: None, .. })
        ));
        assert!(db
            .get(index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes())
            .await
            .unwrap()
            .is_none());
        for pass in [
            index_keys::BlobGcPass::First,
            index_keys::BlobGcPass::Second,
        ] {
            assert!(db
                .get(
                    index_keys::GlobalIndexV2Key::BlobGcReachabilityMark {
                        run_id,
                        pass,
                        scan_attempt: NonZeroU64::MIN,
                        blob_hash: index_keys::BlobHash::new(*blob.hash()),
                    }
                    .to_bytes(),
                )
                .await
                .unwrap()
                .is_none());
        }
    }

    #[tokio::test]
    async fn terminal_blob_batch_preserves_strict_cursor_for_the_next_batch() {
        let db = test_db("text-cleanup-next-blob-batch").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let first_blob = split(64).blob();
        let second_blob = split(65).blob();
        let run_id = put_terminal_delete_root(&db, scope, &operation, first_blob).await;
        let second_key = generation_candidate_key(scope, &operation, second_blob);
        db.put(
            &second_key,
            index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcCandidate(
                work::BlobGcCandidateValue {
                    owner: work::BlobGcCandidateOwner::GenerationCleanup(operation.operation_id()),
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    blob: second_blob,
                },
            )),
        )
        .await
        .expect("next generation candidate is written");
        let progress = retirement_progress(scope, &operation, run_id, first_blob);
        let expected_cursor = progress.candidate_cursor.clone();

        let next = drop_progress(
            run_step(
                &db,
                scope,
                &operation,
                &TextCleanupProgress::DeleteBlobs(progress),
                false,
                limits(8, 64 * 1024, 8, 64 * 1024),
            )
            .await
            .expect("terminal root hands off to another candidate batch"),
        );
        assert!(matches!(
            next,
            TextCleanupProgress::AcquireDeleteFences(GcProgress {
                gc_run_id: None,
                candidate_cursor,
                stage_cursor: None,
                ..
            }) if candidate_cursor == expected_cursor
        ));
        assert!(db.get(second_key).await.unwrap().is_some());
        assert!(db
            .get(index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes())
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn terminal_blob_handoff_pages_global_roots_and_waits_for_upload_owner() {
        let db = test_db("text-cleanup-terminal-root-page").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let blob = split(66).blob();
        let run_id = put_terminal_delete_root(&db, scope, &operation, blob).await;
        let upload_run = BlobGcRunId::from_bytes([90; 16]).expect("upload run ID is non-nil");
        let upload_root = work::BlobGcRunRootValue::try_new(
            upload_run,
            work::BlobGcRunOwner::UploadReclaim {
                scope,
                intent_id: TextUploadIntentId::from_bytes([91; 16]).unwrap(),
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
        .expect("foreign upload root validates");
        let upload_root_key = index_keys::GlobalIndexV2Key::BlobGcRunRoot(upload_run).to_bytes();
        db.put(
            &upload_root_key,
            index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcEntry(
                work::BlobGcEntryValue::RunRoot(upload_root),
            )),
        )
        .await
        .expect("foreign upload root is written");
        let mut progress = retirement_progress(scope, &operation, run_id, blob);
        let page_limits = limits(1, 64 * 1024, 8, 64 * 1024);

        let first = drop_progress(
            run_step(
                &db,
                scope,
                &operation,
                &TextCleanupProgress::DeleteBlobs(progress),
                false,
                page_limits,
            )
            .await
            .expect("first global root page advances"),
        );
        let TextCleanupProgress::DeleteBlobs(first) = first else {
            panic!("bounded root page remains in DeleteBlobs");
        };
        assert!(first.stage_cursor.is_some());
        progress = first;

        assert!(matches!(
            run_step(
                &db,
                scope,
                &operation,
                &TextCleanupProgress::DeleteBlobs(progress.clone()),
                false,
                page_limits,
            )
            .await
            .expect("matching upload root causes a durable retry"),
            IndexOperationStepResult::TransientFailure
        ));
        assert!(
            db.get(index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes())
                .await
                .unwrap()
                .is_some(),
            "generation root remains the retry anchor while upload cleanup runs"
        );
        db.delete(upload_root_key)
            .await
            .expect("foreign upload root finishes");

        let next = drop_progress(
            run_step(
                &db,
                scope,
                &operation,
                &TextCleanupProgress::DeleteBlobs(progress),
                false,
                page_limits,
            )
            .await
            .expect("handoff resumes after upload root absence"),
        );
        assert!(matches!(next, TextCleanupProgress::DeleteEntityState(_)));
    }

    #[tokio::test]
    async fn terminal_absence_persists_then_releases_before_first_pass() {
        let db = test_db("text-cleanup-retire-intent-absence").await;
        let scope = DataScope::LegacyUnscoped;
        let base = operation();
        let payload = Bytes::from_static(b"absent cleanup upload");
        let split = content_split(&payload);
        let store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let coordinator = blob_publication::ProcessLocalBlobPublicationCoordinator::new(
            store,
            "text-cleanup-retire-intent-absence-blobs",
            blob_publication::BlobPublicationTiming::default(),
        );
        let intent_id = TextUploadIntentId::from_bytes([81; 16]).expect("intent ID is non-nil");
        let permit = coordinator
            .reserve(
                split.blob(),
                intent_id,
                WriterEpoch::from_bytes([82; 16]).expect("writer epoch is non-nil"),
            )
            .await
            .expect("publication permit is reserved");
        coordinator.expire_unused_permit(permit);
        let (_intent, rows) = put_coordinated_intent(
            &db,
            scope,
            &base,
            split,
            permit,
            work::TextUploadPhase::Prepared,
            intent_id,
        )
        .await;
        let run_id = put_fences_closed_run(&db, scope, &base, &[split.blob()]).await;
        let progress = retirement_progress(scope, &base, run_id, split.blob());

        let first_operation = claimed_retirement_operation(progress.clone(), 1);
        let first = intent_retirement_progress(
            run_intent_retirement(&db, scope, &first_operation, &progress, &coordinator).await,
        );
        assert!(first.stage_cursor.is_none());
        let first_value = db
            .get(&rows.intent_key)
            .await
            .expect("absence-proof intent is readable")
            .expect("absence-proof intent remains durable");
        assert!(matches!(
            index_values::decode_work_value(&first_value).expect("absence-proof intent decodes"),
            index_values::IndexV2WorkValue::TextUploadIntent(intent)
                if matches!(intent.phase, work::TextUploadPhase::NonPublicationProven)
        ));
        assert_eq!(
            coordinator
                .publication_status(&permit)
                .await
                .expect("durable absence proof retains its permit"),
            blob_publication::BlobPublicationStatus::ExpiredUnused
        );

        let second_operation = claimed_retirement_operation(first.clone(), 2);
        let second = intent_retirement_progress(
            run_intent_retirement(&db, scope, &second_operation, &first, &coordinator).await,
        );
        assert_eq!(
            second
                .stage_cursor
                .as_ref()
                .expect("deleted intent advances its cursor")
                .as_bytes(),
            &rows.intent_key
        );
        for key in [&rows.intent_key, &rows.pointer_key, &rows.reachability_key] {
            assert!(db
                .get(key)
                .await
                .expect("retired anchor is readable")
                .is_none());
        }
        assert!(matches!(
            coordinator.publication_status(&permit).await,
            Err(blob_publication::BlobPublicationError::UnknownPermit)
        ));

        let third_operation = claimed_retirement_operation(second.clone(), 3);
        let third =
            run_intent_retirement(&db, scope, &third_operation, &second, &coordinator).await;
        let IndexOperationStepResult::Progressed(IndexOperationProgress::TextCleanup(
            TextCleanupProgress::MarkReachability(next),
        )) = third
        else {
            panic!("exhausted normalized intents must enter first-pass reachability");
        };
        assert_eq!(next.gc_run_id, Some(run_id));
        assert!(next.stage_cursor.is_none());
        let root_value = db
            .get(index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes())
            .await
            .expect("first-pass root is readable")
            .expect("first-pass root remains durable");
        assert!(matches!(
            index_values::decode_work_value(&root_value).expect("first-pass root decodes"),
            index_values::IndexV2WorkValue::BlobGcEntry(work::BlobGcEntryValue::RunRoot(root))
                if matches!(root.phase, work::BlobGcPhase::FirstPass { first_attempt, reference_cursor: None, .. } if first_attempt.get() == 1)
        ));
    }

    #[tokio::test]
    async fn terminal_shared_blob_becomes_assigned_reclaim_without_release() {
        let db = test_db("text-cleanup-retire-intent-shared").await;
        let scope = DataScope::LegacyUnscoped;
        let base = operation();
        let payload = Bytes::from_static(b"shared cleanup upload");
        let split = content_split(&payload);
        let store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let coordinator = blob_publication::ProcessLocalBlobPublicationCoordinator::new(
            store,
            "text-cleanup-retire-intent-shared-blobs",
            blob_publication::BlobPublicationTiming::default(),
        );
        let shared_permit = coordinator
            .reserve(
                split.blob(),
                TextUploadIntentId::from_bytes([83; 16]).expect("shared intent ID is non-nil"),
                WriterEpoch::from_bytes([84; 16]).expect("shared epoch is non-nil"),
            )
            .await
            .expect("shared publication permit is reserved");
        coordinator
            .publish(&shared_permit, payload)
            .await
            .expect("shared object publishes");
        coordinator
            .release(
                &shared_permit,
                blob_publication::BlobPermitReleaseAuthority::reference_committed(
                    shared_permit.id(),
                ),
            )
            .await
            .expect("shared publication permit releases");
        let intent_id = TextUploadIntentId::from_bytes([85; 16]).expect("intent ID is non-nil");
        let permit = coordinator
            .reserve(
                split.blob(),
                intent_id,
                WriterEpoch::from_bytes([86; 16]).expect("writer epoch is non-nil"),
            )
            .await
            .expect("cleanup publication permit is reserved");
        assert!(matches!(
            coordinator
                .publish(&permit, Bytes::from_static(b"wrong cleanup bytes"))
                .await,
            Err(blob_publication::BlobPublicationError::PayloadMismatch)
        ));
        let (_intent, rows) = put_coordinated_intent(
            &db,
            scope,
            &base,
            split,
            permit,
            work::TextUploadPhase::Prepared,
            intent_id,
        )
        .await;
        let run_id = put_fences_closed_run(&db, scope, &base, &[split.blob()]).await;
        let progress = retirement_progress(scope, &base, run_id, split.blob());

        let first_operation = claimed_retirement_operation(progress.clone(), 1);
        let first = intent_retirement_progress(
            run_intent_retirement(&db, scope, &first_operation, &progress, &coordinator).await,
        );
        assert!(first.stage_cursor.is_none());
        assert!(db
            .get(&rows.reachability_key)
            .await
            .expect("intent reachability is readable")
            .is_none());
        let candidate_key = intent_candidate_row(
            scope,
            &crate::index_v2::repository::load_upload_from_pointer(&db, intent_id)
                .await
                .expect("reclaim intent is readable")
                .expect("reclaim intent remains durable"),
        )
        .0;
        assert!(db
            .get(&candidate_key)
            .await
            .expect("intent candidate is readable")
            .is_some());

        let second_operation = claimed_retirement_operation(first.clone(), 2);
        let second = intent_retirement_progress(
            run_intent_retirement(&db, scope, &second_operation, &first, &coordinator).await,
        );
        assert_eq!(
            second
                .stage_cursor
                .as_ref()
                .expect("assigned reclaim advances its cursor")
                .as_bytes(),
            &rows.intent_key
        );
        let assigned = crate::index_v2::repository::load_upload_from_pointer(&db, intent_id)
            .await
            .expect("assigned reclaim intent is readable")
            .expect("assigned reclaim intent remains durable");
        assert!(matches!(
            assigned.phase,
            work::TextUploadPhase::Reclaimable(work::ReclaimAssignment::Assigned(assigned_run))
                if assigned_run == run_id
        ));
        assert_eq!(
            coordinator
                .publication_status(&permit)
                .await
                .expect("assigned reclaim retains terminal permit"),
            blob_publication::BlobPublicationStatus::DefinitivelyFailed
        );
    }

    #[tokio::test]
    async fn definitive_success_reaches_uploaded_then_assigned_reclaim() {
        let db = test_db("text-cleanup-retire-intent-success").await;
        let scope = DataScope::LegacyUnscoped;
        let base = operation();
        let payload = Bytes::from_static(b"successful cleanup upload");
        let split = content_split(&payload);
        let store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let coordinator = blob_publication::ProcessLocalBlobPublicationCoordinator::new(
            store,
            "text-cleanup-retire-intent-success-blobs",
            blob_publication::BlobPublicationTiming::default(),
        );
        let intent_id = TextUploadIntentId::from_bytes([87; 16]).expect("intent ID is non-nil");
        let permit = coordinator
            .reserve(
                split.blob(),
                intent_id,
                WriterEpoch::from_bytes([88; 16]).expect("writer epoch is non-nil"),
            )
            .await
            .expect("publication permit is reserved");
        coordinator
            .publish(&permit, payload)
            .await
            .expect("publication succeeds");
        let (_intent, rows) = put_coordinated_intent(
            &db,
            scope,
            &base,
            split,
            permit,
            work::TextUploadPhase::Prepared,
            intent_id,
        )
        .await;
        let run_id = put_fences_closed_run(&db, scope, &base, &[split.blob()]).await;
        let progress = retirement_progress(scope, &base, run_id, split.blob());

        let first_operation = claimed_retirement_operation(progress.clone(), 1);
        let first = intent_retirement_progress(
            run_intent_retirement(&db, scope, &first_operation, &progress, &coordinator).await,
        );
        let uploaded = crate::index_v2::repository::load_upload_from_pointer(&db, intent_id)
            .await
            .expect("uploaded cleanup intent is readable")
            .expect("uploaded cleanup intent remains durable");
        assert!(matches!(uploaded.phase, work::TextUploadPhase::Uploaded));
        assert!(first.stage_cursor.is_none());
        assert!(db
            .get(&rows.reachability_key)
            .await
            .expect("uploaded reachability is readable")
            .is_some());

        let second_operation = claimed_retirement_operation(first.clone(), 2);
        let second = intent_retirement_progress(
            run_intent_retirement(&db, scope, &second_operation, &first, &coordinator).await,
        );
        let reclaimable = crate::index_v2::repository::load_upload_from_pointer(&db, intent_id)
            .await
            .expect("reclaimable cleanup intent is readable")
            .expect("reclaimable cleanup intent remains durable");
        assert!(matches!(
            reclaimable.phase,
            work::TextUploadPhase::Reclaimable(work::ReclaimAssignment::Unassigned)
        ));
        assert!(second.stage_cursor.is_none());
        assert!(db
            .get(&rows.reachability_key)
            .await
            .expect("retired upload reachability is readable")
            .is_none());

        let third_operation = claimed_retirement_operation(second.clone(), 3);
        let third = intent_retirement_progress(
            run_intent_retirement(&db, scope, &third_operation, &second, &coordinator).await,
        );
        assert_eq!(
            third
                .stage_cursor
                .as_ref()
                .expect("assigned reclaim advances its cursor")
                .as_bytes(),
            &rows.intent_key
        );
        let assigned = crate::index_v2::repository::load_upload_from_pointer(&db, intent_id)
            .await
            .expect("assigned cleanup intent is readable")
            .expect("assigned cleanup intent remains durable");
        assert!(matches!(
            assigned.phase,
            work::TextUploadPhase::Reclaimable(work::ReclaimAssignment::Assigned(assigned_run))
                if assigned_run == run_id
        ));
        assert_eq!(
            coordinator
                .publication_status(&permit)
                .await
                .expect("assigned successful upload retains its permit"),
            blob_publication::BlobPublicationStatus::Succeeded(
                blob_publication::VerifiedBlobMetadata::new(split.blob())
            )
        );
    }

    #[tokio::test]
    async fn reference_committed_releases_historical_authority_before_delete() {
        let db = test_db("text-cleanup-retire-reference-committed").await;
        let scope = DataScope::LegacyUnscoped;
        let base = operation();
        let payload = Bytes::from_static(b"committed cleanup upload");
        let split = content_split(&payload);
        let store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let coordinator = blob_publication::ProcessLocalBlobPublicationCoordinator::new(
            store,
            "text-cleanup-retire-reference-committed-blobs",
            blob_publication::BlobPublicationTiming::default(),
        );
        let intent_id = TextUploadIntentId::from_bytes([89; 16]).expect("intent ID is non-nil");
        let permit = coordinator
            .reserve(
                split.blob(),
                intent_id,
                WriterEpoch::from_bytes([90; 16]).expect("writer epoch is non-nil"),
            )
            .await
            .expect("publication permit is reserved");
        coordinator
            .publish(&permit, payload)
            .await
            .expect("publication succeeds");
        let artifact_key =
            index_keys::IndexV2Key::TextBuildArtifact(index_keys::TextBuildArtifactKey {
                root: index_keys::TextManifestRootKey {
                    index_id: base.index_id(),
                    generation: base.generation(),
                    partition: work::TextPartition::Unpartitioned.fingerprint(),
                },
                ordinal: 0,
            })
            .to_bytes();
        let authorization = work::UploadDestinationAuthorization::try_new(
            index_keys::BlobReferenceOwnerKind::BuildArtifact,
            artifact_key,
            0,
            None,
        )
        .expect("historical artifact authority validates");
        let (_intent, rows) = put_coordinated_intent(
            &db,
            scope,
            &base,
            split,
            permit,
            work::TextUploadPhase::ReferenceCommitted(authorization),
            intent_id,
        )
        .await;
        let run_id = put_fences_closed_run(&db, scope, &base, &[split.blob()]).await;
        let progress = retirement_progress(scope, &base, run_id, split.blob());
        let claimed = claimed_retirement_operation(progress.clone(), 1);

        let released_before_commit = prepare_upload_intent_retirement(
            &db,
            scope,
            &claimed,
            &progress,
            false,
            limits(8, 64 * 1024, 8, 64 * 1024),
            &coordinator,
        )
        .await
        .expect("reference cleanup prepares before an injected crash");
        drop(released_before_commit);
        assert!(db
            .get(&rows.intent_key)
            .await
            .expect("uncommitted intent is readable")
            .is_some());
        assert!(matches!(
            coordinator.publication_status(&permit).await,
            Err(blob_publication::BlobPublicationError::UnknownPermit)
        ));

        let next = intent_retirement_progress(
            run_intent_retirement(&db, scope, &claimed, &progress, &coordinator).await,
        );
        assert_eq!(
            next.stage_cursor
                .as_ref()
                .expect("released reference advances its cursor")
                .as_bytes(),
            &rows.intent_key
        );
        assert!(db
            .get(&rows.intent_key)
            .await
            .expect("retired reference intent is readable")
            .is_none());
        assert!(db
            .get(&rows.pointer_key)
            .await
            .expect("retired reference pointer is readable")
            .is_none());
        assert!(matches!(
            coordinator.publication_status(&permit).await,
            Err(blob_publication::BlobPublicationError::UnknownPermit)
        ));
    }

    #[tokio::test]
    async fn release_limits_and_outage_leave_nonpublication_authority_replayable() {
        let db = test_db("text-cleanup-retire-release-boundaries").await;
        let scope = DataScope::LegacyUnscoped;
        let base = operation();
        let split = content_split(b"release boundary upload");
        let store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let coordinator = blob_publication::ProcessLocalBlobPublicationCoordinator::new(
            store,
            "text-cleanup-retire-release-boundaries-blobs",
            blob_publication::BlobPublicationTiming::default(),
        );
        let intent_id = TextUploadIntentId::from_bytes([92; 16]).expect("intent ID is non-nil");
        let permit = coordinator
            .reserve(
                split.blob(),
                intent_id,
                WriterEpoch::from_bytes([93; 16]).expect("writer epoch is non-nil"),
            )
            .await
            .expect("publication permit is reserved");
        coordinator.expire_unused_permit(permit);
        let (_intent, rows) = put_coordinated_intent(
            &db,
            scope,
            &base,
            split,
            permit,
            work::TextUploadPhase::NonPublicationProven,
            intent_id,
        )
        .await;
        let run_id = put_fences_closed_run(&db, scope, &base, &[split.blob()]).await;
        let progress = retirement_progress(scope, &base, run_id, split.blob());
        let claimed = claimed_retirement_operation(progress.clone(), 1);

        let blocked = prepare_upload_intent_retirement(
            &db,
            scope,
            &claimed,
            &progress,
            false,
            limits(8, 64 * 1024, 1, 64 * 1024),
            &coordinator,
        )
        .await
        .expect("oversized cleanup prepares a durable blocker");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("blocked cleanup transaction opens");
        assert!(matches!(
            blocked
                .stage(&transaction, scope, &claimed)
                .await
                .expect("blocked cleanup stages no writes"),
            IndexOperationStepResult::Blocked(
                crate::index_v2::IndexOperationBlocker::InvariantViolation
            )
        ));
        drop(transaction);
        assert_eq!(
            coordinator
                .publication_status(&permit)
                .await
                .expect("output blocker cannot release the permit"),
            blob_publication::BlobPublicationStatus::ExpiredUnused
        );

        coordinator.fail_next_release();
        assert!(matches!(
            run_intent_retirement(&db, scope, &claimed, &progress, &coordinator).await,
            IndexOperationStepResult::TransientFailure
        ));
        assert_eq!(
            coordinator
                .publication_status(&permit)
                .await
                .expect("release outage retains the permit"),
            blob_publication::BlobPublicationStatus::ExpiredUnused
        );
        for key in [&rows.intent_key, &rows.pointer_key, &rows.reachability_key] {
            assert!(db
                .get(key)
                .await
                .expect("retryable cleanup anchor is readable")
                .is_some());
        }

        let next = intent_retirement_progress(
            run_intent_retirement(&db, scope, &claimed, &progress, &coordinator).await,
        );
        assert_eq!(
            next.stage_cursor
                .as_ref()
                .expect("successful replay advances the intent cursor")
                .as_bytes(),
            &rows.intent_key
        );
    }

    #[tokio::test]
    async fn claimed_intent_retries_without_cleanup_transition() {
        let db = test_db("text-cleanup-retire-claimed-intent").await;
        let scope = DataScope::LegacyUnscoped;
        let base = operation();
        let split = content_split(b"claimed cleanup upload");
        let store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let coordinator = blob_publication::ProcessLocalBlobPublicationCoordinator::new(
            store,
            "text-cleanup-retire-claimed-intent-blobs",
            blob_publication::BlobPublicationTiming::default(),
        );
        let intent_id = TextUploadIntentId::from_bytes([94; 16]).expect("intent ID is non-nil");
        let permit = coordinator
            .reserve(
                split.blob(),
                intent_id,
                WriterEpoch::from_bytes([95; 16]).expect("writer epoch is non-nil"),
            )
            .await
            .expect("publication permit is reserved");
        coordinator.expire_unused_permit(permit);
        let (intent, _) = put_coordinated_intent(
            &db,
            scope,
            &base,
            split,
            permit,
            work::TextUploadPhase::Prepared,
            intent_id,
        )
        .await;
        let claimed_intent = intent
            .claim(OperationClaim {
                writer_epoch: WriterEpoch::from_bytes([96; 16]).expect("claim epoch is non-nil"),
                sequence: ClaimSequence::new(1).expect("claim sequence is non-zero"),
            })
            .expect("queued intent can be claimed");
        let claimed_rows = super::super::upload::upload_anchor_rows(scope, &claimed_intent)
            .expect("claimed intent anchors encode");
        db.put(&claimed_rows.intent_key, &claimed_rows.intent_value)
            .await
            .expect("claimed intent is written");
        db.put(&claimed_rows.pointer_key, &claimed_rows.pointer_value)
            .await
            .expect("claimed pointer is written");
        let run_id = put_fences_closed_run(&db, scope, &base, &[split.blob()]).await;
        let progress = retirement_progress(scope, &base, run_id, split.blob());
        let claimed_operation = claimed_retirement_operation(progress.clone(), 1);

        assert!(matches!(
            run_intent_retirement(&db, scope, &claimed_operation, &progress, &coordinator,).await,
            IndexOperationStepResult::TransientFailure
        ));
        assert_eq!(
            db.get(&claimed_rows.intent_key)
                .await
                .expect("claimed intent is readable"),
            Some(claimed_rows.intent_value)
        );
        assert_eq!(
            coordinator
                .publication_status(&permit)
                .await
                .expect("claimed intent retains its permit"),
            blob_publication::BlobPublicationStatus::ExpiredUnused
        );
    }

    #[tokio::test]
    async fn mismatching_fenced_object_blocks_without_releasing_anchors() {
        let db = test_db("text-cleanup-retire-mismatching-object").await;
        let scope = DataScope::LegacyUnscoped;
        let base = operation();
        let payload = Bytes::from_static(b"declared cleanup upload");
        let split = content_split(&payload);
        let store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let coordinator = blob_publication::ProcessLocalBlobPublicationCoordinator::new(
            Arc::clone(&store),
            "text-cleanup-retire-mismatching-object-blobs",
            blob_publication::BlobPublicationTiming::default(),
        );
        let intent_id = TextUploadIntentId::from_bytes([97; 16]).expect("intent ID is non-nil");
        let permit = coordinator
            .reserve(
                split.blob(),
                intent_id,
                WriterEpoch::from_bytes([98; 16]).expect("writer epoch is non-nil"),
            )
            .await
            .expect("publication permit is reserved");
        assert!(matches!(
            coordinator
                .publish(&permit, Bytes::from_static(b"wrong cleanup upload"))
                .await,
            Err(blob_publication::BlobPublicationError::PayloadMismatch)
        ));
        store
            .put(
                &crate::search::text::blob_object_store_path(
                    "text-cleanup-retire-mismatching-object-blobs",
                    *split.blob().hash(),
                ),
                slatedb::object_store::PutPayload::from_bytes(Bytes::from_static(
                    b"wrong cleanup upload",
                )),
            )
            .await
            .expect("mismatching object bytes are injected");
        let (_intent, rows) = put_coordinated_intent(
            &db,
            scope,
            &base,
            split,
            permit,
            work::TextUploadPhase::Prepared,
            intent_id,
        )
        .await;
        let run_id = put_fences_closed_run(&db, scope, &base, &[split.blob()]).await;
        let progress = retirement_progress(scope, &base, run_id, split.blob());
        let claimed = claimed_retirement_operation(progress.clone(), 1);

        assert!(matches!(
            run_intent_retirement(&db, scope, &claimed, &progress, &coordinator).await,
            IndexOperationStepResult::Blocked(
                crate::index_v2::IndexOperationBlocker::BlobPublicationMismatch {
                    intent_id: blocked_intent_id
                }
            ) if blocked_intent_id == intent_id
        ));
        for key in [&rows.intent_key, &rows.pointer_key, &rows.reachability_key] {
            assert!(db
                .get(key)
                .await
                .expect("mismatching cleanup anchor is readable")
                .is_some());
        }
        assert_eq!(
            coordinator
                .publication_status(&permit)
                .await
                .expect("mismatch retains its permit"),
            blob_publication::BlobPublicationStatus::DefinitivelyFailed
        );
    }

    #[tokio::test]
    async fn owner_retirement_revalidates_fences_and_rejects_a_stale_root() {
        let db = test_db("text-cleanup-retire-owner-revalidation").await;
        let scope = DataScope::LegacyUnscoped;
        let base = operation();
        let split = split(99);
        let (_page_row, page_key) = put_manifest_page(&db, scope, &base, vec![split], true).await;
        let (reference_key, _) = super::super::attachment::manifest_page_reachability_row(
            split.blob(),
            scope,
            page_key,
            0,
        );
        let run_id = put_fences_closed_run(&db, scope, &base, &[split.blob()]).await;
        let progress = retirement_progress(scope, &base, run_id, split.blob());
        let first_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let foreign = blob_publication::ProcessLocalBlobPublicationCoordinator::new(
            first_store,
            "text-cleanup-retire-owner-foreign-fence",
            blob_publication::BlobPublicationTiming::default(),
        );
        let foreign_run = BlobGcRunId::from_bytes([100; 16]).expect("foreign run ID is non-nil");
        foreign
            .begin_delete(blob_publication::BlobDeleteFenceKey::new(
                split.blob(),
                foreign_run,
            ))
            .await
            .expect("foreign fence is acquired");
        let waiting = prepare_fenced_owner_retirement(
            &db,
            scope,
            &base,
            FencedOwnerRetirementProgress::Manifest(progress.clone()),
            false,
            limits(8, 64 * 1024, 8, 64 * 1024),
            &foreign,
        )
        .await
        .expect("foreign fence prepares a retry");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("waiting owner transaction opens");
        assert!(matches!(
            waiting
                .stage(&transaction, scope, &base)
                .await
                .expect("foreign fence stages no owner writes"),
            IndexOperationStepResult::TransientFailure
        ));
        drop(transaction);
        assert!(db
            .get(&reference_key)
            .await
            .expect("waiting owner reference is readable")
            .is_some());

        let recovered_store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(InMemory::new());
        let recovered = blob_publication::ProcessLocalBlobPublicationCoordinator::new(
            recovered_store,
            "text-cleanup-retire-owner-recovered-fence",
            blob_publication::BlobPublicationTiming::default(),
        );
        let stale = prepare_fenced_owner_retirement(
            &db,
            scope,
            &base,
            FencedOwnerRetirementProgress::Manifest(progress.clone()),
            false,
            limits(8, 64 * 1024, 8, 64 * 1024),
            &recovered,
        )
        .await
        .expect("recovered owner fences prepare");
        let root_key = index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes();
        let root_value = db
            .get(&root_key)
            .await
            .expect("owner root is readable")
            .expect("owner root exists");
        let index_values::IndexV2WorkValue::BlobGcEntry(work::BlobGcEntryValue::RunRoot(mut root)) =
            index_values::decode_work_value(&root_value).expect("owner root decodes")
        else {
            panic!("owner root key contains another value kind");
        };
        root.revision = root
            .revision
            .checked_next()
            .expect("test root revision advances");
        db.put(
            &root_key,
            index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcEntry(
                work::BlobGcEntryValue::RunRoot(root),
            )),
        )
        .await
        .expect("owner root is advanced after preparation");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("stale owner transaction opens");
        assert!(matches!(
            stale
                .stage(&transaction, scope, &base)
                .await
                .expect("stale owner root stages no writes"),
            IndexOperationStepResult::TransientFailure
        ));
        drop(transaction);
        assert!(db
            .get(&reference_key)
            .await
            .expect("stale owner reference is readable")
            .is_some());

        let prepared = prepare_fenced_owner_retirement(
            &db,
            scope,
            &base,
            FencedOwnerRetirementProgress::Manifest(progress),
            false,
            limits(8, 64 * 1024, 8, 64 * 1024),
            &recovered,
        )
        .await
        .expect("current owner root prepares");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("current owner transaction opens");
        let result = prepared
            .stage(&transaction, scope, &base)
            .await
            .expect("current owner retirement stages");
        transaction
            .commit()
            .await
            .expect("current owner retirement commits");
        assert!(matches!(result, IndexOperationStepResult::Progressed(_)));
        assert!(db
            .get(&reference_key)
            .await
            .expect("retired owner reference is readable")
            .is_none());
    }

    #[tokio::test]
    async fn manifest_retirement_deletes_only_current_run_references() {
        let db = test_db("text-cleanup-retire-manifest-current-run").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let current = split(71);
        let later = split(72);
        let (page_row_key, page_key) =
            put_manifest_page(&db, scope, &operation, vec![current, later], true).await;
        let (current_reference, _) = super::super::attachment::manifest_page_reachability_row(
            current.blob(),
            scope,
            page_key,
            0,
        );
        let (later_reference, _) = super::super::attachment::manifest_page_reachability_row(
            later.blob(),
            scope,
            page_key,
            1,
        );
        let run_id = put_fences_closed_run(&db, scope, &operation, &[current.blob()]).await;
        let progress = retirement_progress(scope, &operation, run_id, current.blob());

        let first = drop_progress(
            run_step(
                &db,
                scope,
                &operation,
                &TextCleanupProgress::RetireManifest(progress.clone()),
                false,
                limits(8, 64 * 1024, 8, 64 * 1024),
            )
            .await
            .expect("manifest owner retirement advances"),
        );
        let TextCleanupProgress::RetireManifest(first) = first else {
            panic!("one manifest page retains the manifest-retirement lane");
        };
        assert_eq!(first.gc_run_id, Some(run_id));
        assert_eq!(first.candidate_cursor, progress.candidate_cursor);
        assert_eq!(
            first
                .stage_cursor
                .as_ref()
                .expect("completed page cursor is retained")
                .as_bytes(),
            &page_row_key
        );
        assert_eq!(first.counters.entities, 2);
        assert_eq!(first.counters.output_operations, 1);
        assert!(db.get(&page_row_key).await.unwrap().is_some());
        assert!(db.get(&current_reference).await.unwrap().is_none());
        assert!(db.get(&later_reference).await.unwrap().is_some());

        let next = drop_progress(
            run_step(
                &db,
                scope,
                &operation,
                &TextCleanupProgress::RetireManifest(first),
                false,
                limits(8, 64 * 1024, 8, 64 * 1024),
            )
            .await
            .expect("manifest exhaustion advances to artifacts"),
        );
        let TextCleanupProgress::RetireArtifacts(next) = next else {
            panic!("manifest exhaustion enters artifact retirement");
        };
        assert_eq!(next.gc_run_id, Some(run_id));
        assert!(next.stage_cursor.is_none());
    }

    #[tokio::test]
    async fn artifact_retirement_deletes_only_current_run_owner_pairs() {
        let db = test_db("text-cleanup-retire-artifact-current-run").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let current = split(81);
        let later = split(82);
        let (current_owner, current_reference) =
            put_artifact(&db, scope, &operation, current, 0).await;
        let (later_owner, later_reference) = put_artifact(&db, scope, &operation, later, 1).await;
        let run_id = put_fences_closed_run(&db, scope, &operation, &[current.blob()]).await;
        let progress = retirement_progress(scope, &operation, run_id, current.blob());

        let next = drop_progress(
            run_step(
                &db,
                scope,
                &operation,
                &TextCleanupProgress::RetireArtifacts(progress.clone()),
                false,
                limits(8, 64 * 1024, 8, 64 * 1024),
            )
            .await
            .expect("artifact owner retirement advances"),
        );
        let TextCleanupProgress::RetireUploadIntents(next) = next else {
            panic!("artifact exhaustion enters intent retirement");
        };
        assert_eq!(next.gc_run_id, Some(run_id));
        assert_eq!(next.candidate_cursor, progress.candidate_cursor);
        assert!(next.stage_cursor.is_none());
        assert_eq!(next.counters.entities, 2);
        assert_eq!(next.counters.output_operations, 2);
        assert!(db.get(&current_owner).await.unwrap().is_none());
        assert!(db.get(&current_reference).await.unwrap().is_none());
        assert!(db.get(&later_owner).await.unwrap().is_some());
        assert!(db.get(&later_reference).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn reclaim_barrier_requires_and_waits_for_the_exact_foreign_root() {
        let db = test_db("text-cleanup-reclaim-barrier").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let run_id = BlobGcRunId::from_bytes([61; 16]).expect("run ID is non-nil");
        let intent = put_assigned_reclaim_intent(&db, scope, &operation, run_id).await;
        let progress = TextCleanupProgress::PrepareCandidates(PrefixScanProgress {
            cursor: None,
            counters: OperationCounters::default(),
        });
        let limits = limits(8, 64 * 1024, 8, 64 * 1024);

        let error = run_step(&db, scope, &operation, &progress, false, limits)
            .await
            .expect_err("missing assigned root is corruption");
        assert!(matches!(error, HelixDbError::IndexCatalogCorruption(_)));

        let root = work::BlobGcRunRootValue::try_new(
            run_id,
            work::BlobGcRunOwner::UploadReclaim {
                scope,
                intent_id: intent.intent_id,
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
        .expect("matching reclaim root validates");
        db.put(
            index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes(),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::BlobGcEntry(
                work::BlobGcEntryValue::RunRoot(root),
            )),
        )
        .await
        .expect("matching reclaim root is written");
        assert!(matches!(
            run_step(&db, scope, &operation, &progress, false, limits)
                .await
                .expect("live assigned root is a retryable wait"),
            IndexOperationStepResult::TransientFailure
        ));
    }

    #[tokio::test]
    async fn missing_manifest_reachability_fails_closed_without_a_candidate() {
        let db = test_db("text-cleanup-missing-manifest-reference").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let split = split(21);
        put_manifest_page(&db, scope, &operation, vec![split], false).await;
        let progress = TextCleanupProgress::PrepareCandidates(PrefixScanProgress {
            cursor: Some(
                super::super::blob_gc::generation_candidate_start_cursor(scope, &operation)
                    .unwrap(),
            ),
            counters: OperationCounters::default(),
        });

        let error = run_step(
            &db,
            scope,
            &operation,
            &progress,
            false,
            limits(8, 64 * 1024, 8, 64 * 1024),
        )
        .await
        .expect_err("missing reachability is corruption");
        assert!(matches!(error, HelixDbError::IndexCatalogCorruption(_)));
        assert!(db
            .get(generation_candidate_key(scope, &operation, split.blob()))
            .await
            .expect("candidate key is readable")
            .is_none());
    }

    #[tokio::test]
    async fn indivisible_manifest_owner_blocks_instead_of_retrying() {
        let db = test_db("text-cleanup-indivisible-manifest-owner").await;
        let scope = DataScope::LegacyUnscoped;
        let operation = operation();
        let split = split(31);
        put_manifest_page(&db, scope, &operation, vec![split], true).await;
        let progress = TextCleanupProgress::PrepareCandidates(PrefixScanProgress {
            cursor: Some(
                super::super::blob_gc::generation_candidate_start_cursor(scope, &operation)
                    .unwrap(),
            ),
            counters: OperationCounters::default(),
        });

        assert!(matches!(
            run_step(&db, scope, &operation, &progress, false, limits(1, 1, 1, 1),)
                .await
                .expect("indivisible owner returns a typed result"),
            IndexOperationStepResult::Blocked(
                crate::index_v2::IndexOperationBlocker::InvariantViolation
            )
        ));
        assert!(db
            .get(generation_candidate_key(scope, &operation, split.blob()))
            .await
            .expect("candidate key is readable")
            .is_none());
    }

    #[test]
    fn cleanup_progress_preserves_abort_and_drop_operation_kinds() {
        let progress = TextCleanupProgress::Finalize(crate::index_v2::NoCursorProgress {
            counters: OperationCounters::default(),
        });
        assert!(matches!(
            progressed_cleanup(true, progress.clone()),
            IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
                TextBuildProgress::Aborting(TextCleanupProgress::Finalize(_))
            ))
        ));
        assert!(matches!(
            progressed_cleanup(false, progress),
            IndexOperationStepResult::Progressed(IndexOperationProgress::TextCleanup(
                TextCleanupProgress::Finalize(_)
            ))
        ));
    }
}
