//! Exact graph-outcome resolution for request-owned Active text uploads.
//!
//! The authoritative graph transaction cannot update upload intents because
//! their independent outbox transactions commit after its snapshot begins. It
//! instead writes one exact proof per published destination. This module reads
//! only those staged proof bytes from a fresh transaction, classifies an
//! ambiguous graph commit, and moves matching intents into the durable
//! `ReferenceCommitted` release outbox while the request owner is still live.
//!
//! Once an exact proof is observed, graph success is final. Coordinator release
//! and proof/intent/pointer cleanup are attempted immediately, but any outage
//! returns committed-with-deferred-finalization and leaves runnable durable work
//! for the terminal owner. No object or manifest presence substitutes for the
//! exact proof.

use std::sync::Arc;

use slatedb::{Db, IsolationLevel};

use crate::encoding::v1::keys::index_v2 as index_keys;
use crate::encoding::v1::keys::tenant::DataScope;
use crate::error::HelixDbError;

use super::super::{blob_publication, repository, work};
use super::active_mutation::ActiveTextMutationRegistryError;
use super::active_publication::ActiveTextPublication;
use super::active_request::StagedActiveTextMutation;
use super::{attachment, upload};

/// Caller-visible result of the authoritative graph transaction.
#[derive(Debug)]
pub(crate) enum ActiveTextGraphCommitObservation {
    /// SlateDB returned success; every staged proof must now exist exactly.
    Committed,
    /// SlateDB returned an error whose physical commit outcome needs proof.
    Ambiguous(HelixDbError),
}

/// Whether request-side release and cleanup completed after graph success.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActiveTextFinalization {
    /// Every permit was released and every request-owned anchor was removed.
    Complete,
    /// Graph success is final; the global upload lane retains cleanup work.
    Deferred,
}

/// Exact outcome after reading the staged proof set from a fresh transaction.
#[derive(Debug)]
pub(crate) enum ActiveTextGraphResolution {
    /// The graph mutation committed, independently of cleanup availability.
    Committed(ActiveTextFinalization),
    /// An ambiguous commit left every exact proof absent.
    Aborted { commit_error: HelixDbError },
}

/// Failure before an exact graph outcome could be established.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ActiveTextResolutionError {
    /// Canonical proof or upload state could not be read safely.
    #[error("Active text outcome resolution failed: {0}")]
    Database(#[from] HelixDbError),
    /// A known successful graph commit did not retain its exact proof set.
    #[error("a committed Active text graph mutation is missing its exact commit proof")]
    CommittedProofMissing,
    /// An upload-free transaction has no durable proof with which to classify an error.
    #[error("an upload-free Active text graph commit remains ambiguous: {0}")]
    AmbiguousWithoutProof(Box<HelixDbError>),
    /// Process-local request ownership could not become terminal after resolution.
    #[error("Active text owner resolution failed: {0}")]
    Owner(#[from] ActiveTextMutationRegistryError),
}

impl ActiveTextResolutionError {
    /// Restores the authoritative database error at the request boundary.
    pub(crate) fn into_database_error(self) -> HelixDbError {
        match self {
            Self::Database(error) => error,
            Self::AmbiguousWithoutProof(error) => *error,
            Self::CommittedProofMissing => HelixDbError::IndexCatalogCorruption(
                "committed Active text graph mutation lost its exact proof set".to_string(),
            ),
            Self::Owner(error) => HelixDbError::InvariantViolation(error.to_string()),
        }
    }
}

/// One graph transaction's complete retained Active-text outcome authority.
///
/// Each request enters this owner only after its publication capability and
/// exact staged proof set agree. A multi-entity write therefore cannot lose an
/// earlier request while later graph mutations continue to use the same
/// SlateDB transaction. Resolution observes the union of proof rows once,
/// which also lets one proof-bearing mutation classify upload-free retirements
/// committed by that same atomic transaction.
#[derive(Default)]
pub(crate) struct ActiveTextTransactionOutbox {
    requests: Vec<PendingActiveTextMutation>,
    proof_count: usize,
}

impl core::fmt::Debug for ActiveTextTransactionOutbox {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("ActiveTextTransactionOutbox")
            .field("request_count", &self.requests.len())
            .field("proof_count", &self.proof_count)
            .finish()
    }
}

/// Inseparable request publication and the proofs staged from its exact bytes.
struct PendingActiveTextMutation {
    publication: ActiveTextPublication,
    staged: StagedActiveTextMutation,
}

impl ActiveTextTransactionOutbox {
    /// Returns whether this transaction owns no Active-text request outcomes.
    pub(crate) const fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    /// Retains one fully staged request until the graph transaction resolves.
    pub(crate) fn retain(
        &mut self,
        publication: ActiveTextPublication,
        staged: StagedActiveTextMutation,
    ) -> Result<(), HelixDbError> {
        if staged.len() != publication.uploaded().len() {
            return Err(corruption(
                "Active text staged proofs disagree with request publication count",
            ));
        }
        self.proof_count = self.proof_count.checked_add(staged.len()).ok_or_else(|| {
            HelixDbError::InvariantViolation(
                "Active text transaction proof count overflowed".to_string(),
            )
        })?;
        self.requests.push(PendingActiveTextMutation {
            publication,
            staged,
        });
        Ok(())
    }
}

/// Resolves one graph commit and best-effort completes its durable release outbox.
///
/// `staged` must be the capability returned while buffering the same graph
/// transaction and `publication` must be retained from its request publication.
/// A proof-bearing committed result is returned even when coordinator release
/// or cleanup fails, preventing a caller from retrying an already committed
/// graph mutation as though it had aborted.
#[cfg(test)]
pub(crate) async fn resolve_active_text_graph_outcome(
    db: &Db,
    publication: ActiveTextPublication,
    staged: StagedActiveTextMutation,
    commit: ActiveTextGraphCommitObservation,
) -> Result<ActiveTextGraphResolution, ActiveTextResolutionError> {
    let mut outbox = ActiveTextTransactionOutbox::default();
    outbox.retain(publication, staged)?;
    resolve_active_text_transaction_outbox(db, outbox, commit).await
}

/// Resolves every Active-text request staged by one atomic graph transaction.
///
/// A transaction-wide proof observation is required because one executable
/// write request may mutate several entities. Presence of the complete union
/// proves that upload-bearing and upload-free requests committed together;
/// complete absence after the writer-continuity barrier proves they all
/// aborted. A partial union is corruption because SlateDB commits them in one
/// physical transaction.
pub(crate) async fn resolve_active_text_transaction_outbox(
    db: &Db,
    outbox: ActiveTextTransactionOutbox,
    commit: ActiveTextGraphCommitObservation,
) -> Result<ActiveTextGraphResolution, ActiveTextResolutionError> {
    if outbox.is_empty() {
        return Err(corruption(
            "Active text transaction outcome resolver received no retained requests",
        )
        .into());
    }
    if outbox.proof_count == 0 {
        let owner = finish_aborted_requests(outbox.requests);
        if let Some(owner) = owner {
            return Err(owner.into());
        }
        return match commit {
            ActiveTextGraphCommitObservation::Committed => Ok(
                ActiveTextGraphResolution::Committed(ActiveTextFinalization::Complete),
            ),
            ActiveTextGraphCommitObservation::Ambiguous(error) => Err(
                ActiveTextResolutionError::AmbiguousWithoutProof(Box::new(error)),
            ),
        };
    }

    match observe_exact_proof_set(db, &outbox).await? {
        ExactProofSet::Absent => match commit {
            ActiveTextGraphCommitObservation::Committed => {
                Err(ActiveTextResolutionError::CommittedProofMissing)
            }
            ActiveTextGraphCommitObservation::Ambiguous(commit_error) => {
                if let Some(owner) = finish_aborted_requests(outbox.requests) {
                    return Err(owner.into());
                }
                Ok(ActiveTextGraphResolution::Aborted { commit_error })
            }
        },
        ExactProofSet::Committed => Ok(ActiveTextGraphResolution::Committed(
            finalize_committed_requests(db, outbox.requests).await,
        )),
    }
}

/// Makes every aborted/no-proof request owner terminal before returning.
fn finish_aborted_requests(
    requests: Vec<PendingActiveTextMutation>,
) -> Option<ActiveTextMutationRegistryError> {
    let mut first_error = None;
    for request in requests {
        if let Err(error) = request.publication.finish()
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    first_error
}

/// Finalizes each committed request while preserving success on cleanup outage.
async fn finalize_committed_requests(
    db: &Db,
    requests: Vec<PendingActiveTextMutation>,
) -> ActiveTextFinalization {
    let mut transaction_finalization = ActiveTextFinalization::Complete;
    for request in requests {
        if request.staged.is_empty() {
            if let Err(error) = request.publication.finish() {
                tracing::warn!(
                    %error,
                    "committed upload-free Active text owner could not become terminal"
                );
                transaction_finalization = ActiveTextFinalization::Deferred;
            }
            continue;
        }
        let committed = match checkpoint_reference_committed(
            db,
            request.publication.uploaded(),
            &request.staged,
        )
        .await
        {
            Ok(committed) => committed,
            Err(error) => {
                tracing::warn!(
                    %error,
                    "committed Active text mutation retains proof for deferred finalization"
                );
                if let Err(owner_error) = request.publication.finish() {
                    tracing::warn!(
                        %owner_error,
                        "committed Active text mutation owner could not become terminal"
                    );
                }
                transaction_finalization = ActiveTextFinalization::Deferred;
                continue;
            }
        };
        if finalize_reference_committed(db, request.publication, &request.staged, &committed).await
            == ActiveTextFinalization::Deferred
        {
            transaction_finalization = ActiveTextFinalization::Deferred;
        }
    }
    transaction_finalization
}

/// All exact proof rows in one atomic graph transaction are present or absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExactProofSet {
    Absent,
    Committed,
}

/// Reads only the exact staged proof keys from a fresh snapshot.
///
/// Proof presence is durable success even if this writer is fenced immediately
/// afterward. Proof absence is authoritative only after the same fresh
/// transaction commits a real marker rewrite. Any newer writer capable of
/// cleaning a proof must first commit that cleanup, which fences this barrier;
/// an open-only writer cannot have removed the proof. A failed barrier becomes
/// the typed unknown outcome instead of a false abort.
async fn observe_exact_proof_set(
    db: &Db,
    outbox: &ActiveTextTransactionOutbox,
) -> Result<ExactProofSet, HelixDbError> {
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .map_err(proof_observation_error)?;
    let mut present = 0_usize;
    for request in &outbox.requests {
        for proof in request.staged.proofs() {
            match transaction
                .get(proof.proof_key())
                .await
                .map_err(proof_observation_error)?
            {
                None => {}
                Some(value) if value == *proof.proof_value() => {
                    present = present.checked_add(1).ok_or_else(|| {
                        HelixDbError::InvariantViolation(
                            "Active text observed proof count overflowed".to_string(),
                        )
                    })?;
                }
                Some(_) => {
                    return Err(corruption(
                        "Active text commit proof key contains another proof value",
                    ));
                }
            }
        }
    }
    if present == 0 {
        repository::stage_writer_continuity_barrier(&transaction)
            .await
            .map_err(proof_observation_database_error)?;
        transaction
            .commit()
            .await
            .map_err(proof_observation_error)?;
        Ok(ExactProofSet::Absent)
    } else if present == outbox.proof_count {
        Ok(ExactProofSet::Committed)
    } else {
        Err(corruption(
            "atomic Active text graph transaction retained only part of its proof set",
        ))
    }
}

/// Preserves SlateDB's writer-fence boundary as a stable retryable outcome.
fn proof_observation_error(error: slatedb::Error) -> HelixDbError {
    if error.kind() == slatedb::ErrorKind::Closed(slatedb::CloseReason::Fenced) {
        HelixDbError::WriterFencedCommitOutcomeUnknown
    } else {
        error.into()
    }
}

/// Maps repository reads/writes through the same stable fence outcome.
fn proof_observation_database_error(error: HelixDbError) -> HelixDbError {
    if matches!(
        &error,
        HelixDbError::Storage(storage)
            if storage.kind() == slatedb::ErrorKind::Closed(slatedb::CloseReason::Fenced)
    ) {
        HelixDbError::WriterFencedCommitOutcomeUnknown
    } else {
        error
    }
}

/// Moves exact proof-bearing uploads into the durable release-outbox phase.
async fn checkpoint_reference_committed(
    db: &Db,
    uploaded: &[work::TextUploadIntentValue],
    staged: &StagedActiveTextMutation,
) -> Result<Vec<work::TextUploadIntentValue>, HelixDbError> {
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let scope = staged.scope();
    let mut committed = Vec::with_capacity(uploaded.len());
    for (expected, proof) in uploaded.iter().zip(staged.proofs()) {
        if transaction.get(proof.proof_key()).await?.as_deref()
            != Some(proof.proof_value().as_ref())
        {
            return Err(corruption(
                "Active text commit proof changed before reference checkpoint",
            ));
        }
        let Some(actual) =
            repository::load_upload_from_pointer(&transaction, expected.intent_id).await?
        else {
            return Err(corruption(
                "proof-bearing Active text upload lost its durable intent pointer",
            ));
        };
        let authorization = proof.authorization().clone();
        let expected_committed = expected
            .active_request_reference_committed(authorization.clone())
            .map_err(work_model_error)?;
        let current_rows = upload::upload_anchor_rows(scope, &actual)?;
        let observed_anchor = transaction.get(&current_rows.reachability_key).await?;
        let uploaded = match &actual.phase {
            work::TextUploadPhase::Prepared => {
                if observed_anchor.as_deref() != Some(current_rows.reachability_value.as_ref()) {
                    return Err(corruption(
                        "prepared Active text upload lost its intent reachability anchor",
                    ));
                }
                let normalized = actual
                    .active_request_publication_succeeded()
                    .map_err(work_model_error)?;
                if normalized != *expected {
                    return Err(corruption(
                        "prepared Active text upload disagrees with definitive publication",
                    ));
                }
                normalized
            }
            work::TextUploadPhase::Uploaded => {
                if actual != *expected
                    || observed_anchor.as_deref() != Some(current_rows.reachability_value.as_ref())
                {
                    return Err(corruption(
                        "durable Active text upload disagrees with request publication",
                    ));
                }
                actual
            }
            work::TextUploadPhase::ReferenceCommitted(actual_authorization)
                if actual == expected_committed
                    && actual_authorization == proof.authorization()
                    && observed_anchor.is_none() =>
            {
                validate_destination_reference(&transaction, scope, &actual, proof).await?;
                committed.push(actual);
                continue;
            }
            work::TextUploadPhase::ReferenceCommitted(_)
            | work::TextUploadPhase::Reclaimable(_)
            | work::TextUploadPhase::NonPublicationProven => {
                return Err(corruption(
                    "proof-bearing Active text upload is in another lifecycle phase",
                ));
            }
        };
        validate_destination_reference(&transaction, scope, &uploaded, proof).await?;
        let next = uploaded
            .active_request_reference_committed(authorization)
            .map_err(work_model_error)?;
        if next != expected_committed {
            return Err(corruption(
                "Active text reference checkpoint changed its admitted upload identity",
            ));
        }
        let next_rows = upload::upload_anchor_rows(scope, &next)?;
        transaction.delete(current_rows.reachability_key)?;
        transaction.put(next_rows.intent_key, next_rows.intent_value)?;
        transaction.put(next_rows.pointer_key, next_rows.pointer_value)?;
        committed.push(next);
    }
    transaction.commit().await?;
    Ok(committed)
}

/// Releases permits and removes only their exact proof-bearing anchors.
async fn finalize_reference_committed(
    db: &Db,
    publication: ActiveTextPublication,
    staged: &StagedActiveTextMutation,
    committed: &[work::TextUploadIntentValue],
) -> ActiveTextFinalization {
    let Some(coordinator) = publication.coordinator().map(Arc::clone) else {
        tracing::warn!("upload-bearing Active text publication lost its coordinator");
        let _ = publication.finish();
        return ActiveTextFinalization::Deferred;
    };
    let mut cleaned = Vec::with_capacity(committed.len());
    for (intent, proof) in committed.iter().zip(staged.proofs()) {
        let permit = blob_publication::BlobPublicationPermit::from_id(intent.publication_permit_id);
        let released = coordinator
            .release(
                &permit,
                blob_publication::BlobPermitReleaseAuthority::reference_committed(permit.id()),
            )
            .await;
        let cleaned_intent = match released {
            Ok(()) => match cleanup_released_reference(db, staged.scope(), intent, proof).await {
                Ok(()) => true,
                Err(error) => {
                    tracing::warn!(
                        intent_id = %intent.intent_id.as_uuid(),
                        %error,
                        "released Active text reference retains durable cleanup work"
                    );
                    false
                }
            },
            Err(error) => {
                tracing::warn!(
                    intent_id = %intent.intent_id.as_uuid(),
                    %error,
                    "committed Active text reference retains its release outbox"
                );
                false
            }
        };
        cleaned.push(cleaned_intent);
    }

    let terminal = match publication.into_terminal() {
        Ok(terminal) => terminal,
        Err(error) => {
            tracing::warn!(%error, "resolved Active text owners could not become terminal");
            return ActiveTextFinalization::Deferred;
        }
    };
    let mut complete =
        cleaned.len() == terminal.uploads.len() && terminal.uploads.len() == committed.len();
    for ((upload, cleaned_intent), intent) in
        terminal.uploads.into_iter().zip(cleaned).zip(committed)
    {
        if upload.value.intent_id != intent.intent_id {
            tracing::warn!(
                intent_id = %upload.value.intent_id.as_uuid(),
                "terminal Active text owner does not belong to the committed proof set"
            );
            complete = false;
            continue;
        }
        if cleaned_intent {
            if let Err(error) = upload.owner.cleanup_after_intent_absence() {
                tracing::warn!(
                    intent_id = %upload.value.intent_id.as_uuid(),
                    %error,
                    "cleaned Active text intent retained process-local terminal ownership"
                );
                complete = false;
            }
        } else {
            complete = false;
        }
    }
    if complete {
        ActiveTextFinalization::Complete
    } else {
        ActiveTextFinalization::Deferred
    }
}

/// Deletes one released `ReferenceCommitted` intent and its exact proof.
async fn cleanup_released_reference(
    db: &Db,
    scope: DataScope,
    expected: &work::TextUploadIntentValue,
    proof: &super::active_attachment::StagedActiveTextCommitProof,
) -> Result<(), HelixDbError> {
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    if transaction.get(proof.proof_key()).await?.as_deref() != Some(proof.proof_value().as_ref()) {
        return Err(corruption(
            "released Active text reference lost its exact commit proof",
        ));
    }
    let Some(actual) =
        repository::load_upload_from_pointer(&transaction, expected.intent_id).await?
    else {
        return Err(corruption(
            "released Active text reference lost its durable intent pointer",
        ));
    };
    if actual != *expected {
        return Err(corruption(
            "released Active text reference disagrees with its durable intent",
        ));
    }
    let rows = upload::upload_anchor_rows(scope, &actual)?;
    if transaction.get(&rows.reachability_key).await?.is_some() {
        return Err(corruption(
            "ReferenceCommitted Active text intent retained upload reachability",
        ));
    }
    transaction.delete(rows.intent_key)?;
    transaction.delete(rows.pointer_key)?;
    transaction.delete(proof.proof_key())?;
    let commit = transaction.commit().await.map_err(HelixDbError::from);
    if let Err(commit) = commit {
        let verification = db.begin(IsolationLevel::SerializableSnapshot).await?;
        let intent_absent = repository::load_upload_from_pointer(&verification, expected.intent_id)
            .await?
            .is_none();
        let proof_absent = verification.get(proof.proof_key()).await?.is_none();
        let anchor_rows = upload::upload_anchor_rows(scope, expected)?;
        let anchor_absent = verification
            .get(anchor_rows.reachability_key)
            .await?
            .is_none();
        if !intent_absent || !proof_absent || !anchor_absent {
            return Err(commit);
        }
    }
    Ok(())
}

/// Verifies the exact manifest reachability row named by historical authority.
async fn validate_destination_reference(
    transaction: &slatedb::DbTransaction,
    scope: DataScope,
    intent: &work::TextUploadIntentValue,
    proof: &super::active_attachment::StagedActiveTextCommitProof,
) -> Result<(), HelixDbError> {
    let authorization = proof.authorization();
    if authorization.owner_kind != index_keys::BlobReferenceOwnerKind::ManifestPageSplit
        || authorization.proof_logical_key.is_none()
    {
        return Err(corruption(
            "Active text proof has invalid manifest destination authority",
        ));
    }
    let index_keys::IndexV2Key::TextManifestPage(page) =
        index_keys::IndexV2Key::parse_from_slice(&authorization.owner_logical_key)?
    else {
        return Err(corruption(
            "Active text proof destination is not a manifest page",
        ));
    };
    if page.root.index_id != intent.index_id
        || page.root.generation != intent.generation
        || page.root.partition != intent.partition.fingerprint()
    {
        return Err(corruption(
            "Active text proof destination disagrees with its upload generation",
        ));
    }
    let (key, value) = attachment::manifest_page_reachability_row(
        intent.blob,
        scope,
        page,
        authorization.owner_slot,
    );
    if transaction.get(key).await?.as_deref() != Some(value.as_ref()) {
        return Err(corruption(
            "Active text proof is missing its exact manifest reachability row",
        ));
    }
    Ok(())
}

fn work_model_error(error: work::IndexWorkModelError) -> HelixDbError {
    HelixDbError::InvariantViolation(error.to_string())
}

fn corruption(reason: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(reason.into())
}
