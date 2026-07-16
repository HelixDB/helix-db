//! Crash-safe terminal non-publication and shared-blob reclaim transitions.
//!
//! A terminally absent publication first enters the durable
//! `NonPublicationProven` release-outbox phase. Only a later claimed delivery
//! releases the coordinator permit and removes the intent anchor. When the
//! same terminal check instead finds an exact pre-existing object, this module
//! atomically transfers reachability from the intent to an intent-qualified GC
//! candidate while a coordinator reference guard remains held by the caller.
//! Active owners additionally require their exact graph commit-proof key to be
//! absent before either terminal non-publication or uploaded-object reclaim can
//! retire intent reachability.

use bytes::Bytes;
use slatedb::DbTransaction;

use crate::encoding::v1::keys::index_v2 as index_keys;
use crate::encoding::v1::keys::tenant::DataScope;
use crate::encoding::v1::keys::{DataKeyKind, GlobalKeyKind, Key};
use crate::encoding::v1::values::index_v2 as index_values;
use crate::error::{HelixDbError, Result};

use super::super::work;

/// Validates exact owner authority before recording terminal object absence.
pub(super) async fn validate_non_publication_proof(
    transaction: &DbTransaction,
    scope: DataScope,
    intent: &work::TextUploadIntentValue,
) -> Result<()> {
    if !matches!(intent.phase, work::TextUploadPhase::Prepared)
        || !matches!(intent.work_state, work::TextUploadWorkState::Claimed(_))
    {
        return Err(corruption(
            "non-publication proof requires an exact claimed Prepared intent",
        ));
    }
    match intent.owner {
        work::TextUploadOwner::Build { .. } => {
            super::build_owner::load_exact(transaction, scope, intent)
                .await
                .map(|_| ())
        }
        work::TextUploadOwner::ActiveMutation { .. } => {
            validate_active_proof_absent(transaction, scope, intent).await
        }
    }
}

/// Removes the intent anchor after its durable absence proof released the permit.
pub(super) async fn stage_non_publication_cleanup(
    transaction: &DbTransaction,
    scope: DataScope,
    intent: &work::TextUploadIntentValue,
) -> Result<()> {
    let reference_key = validate_non_publication_cleanup(
        transaction,
        scope,
        intent,
        NonPublicationCleanupAuthority::ClaimedUpload,
    )
    .await?;
    transaction.delete(reference_key)?;
    Ok(())
}

/// Prevalidates one fenced non-publication anchor before external release.
pub(super) async fn prepare_fenced_non_publication_cleanup(
    transaction: &DbTransaction,
    scope: DataScope,
    intent: &work::TextUploadIntentValue,
) -> Result<Bytes> {
    validate_non_publication_cleanup(
        transaction,
        scope,
        intent,
        NonPublicationCleanupAuthority::FencedGeneration,
    )
    .await
}

/// Removes one fenced generation's released non-publication anchor.
///
/// The caller retains the exact generation/member fences and both local gates
/// through commit. A queued intent is required because generation cleanup does
/// not fabricate an upload-worker claim after the idempotent external release.
pub(super) async fn stage_fenced_non_publication_cleanup(
    transaction: &DbTransaction,
    scope: DataScope,
    intent: &work::TextUploadIntentValue,
) -> Result<()> {
    let reference_key = prepare_fenced_non_publication_cleanup(transaction, scope, intent).await?;
    transaction.delete(reference_key)?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NonPublicationCleanupAuthority {
    ClaimedUpload,
    FencedGeneration,
}

async fn validate_non_publication_cleanup(
    transaction: &DbTransaction,
    scope: DataScope,
    intent: &work::TextUploadIntentValue,
    authority: NonPublicationCleanupAuthority,
) -> Result<Bytes> {
    match authority {
        NonPublicationCleanupAuthority::ClaimedUpload
            if !matches!(intent.phase, work::TextUploadPhase::NonPublicationProven)
                || !matches!(intent.work_state, work::TextUploadWorkState::Claimed(_)) =>
        {
            return Err(corruption(
                "non-publication cleanup requires a claimed absence proof",
            ));
        }
        NonPublicationCleanupAuthority::FencedGeneration
            if !matches!(intent.phase, work::TextUploadPhase::NonPublicationProven)
                || !matches!(intent.work_state, work::TextUploadWorkState::Queued { .. }) =>
        {
            return Err(corruption(
                "fenced non-publication cleanup requires an exact queued absence proof",
            ));
        }
        NonPublicationCleanupAuthority::ClaimedUpload
        | NonPublicationCleanupAuthority::FencedGeneration => {}
    }
    if matches!(intent.owner, work::TextUploadOwner::ActiveMutation { .. }) {
        validate_active_proof_absent(transaction, scope, intent).await?;
    }
    let (reference_key, reference_value) = intent_reachability_row(scope, intent);
    if transaction.get(&reference_key).await?.as_deref() != Some(reference_value.as_ref()) {
        return Err(corruption(
            "non-publication proof is missing its exact intent reachability row",
        ));
    }
    Ok(reference_key)
}

/// Transfers an exact shared blob from live intent ownership to reclaim work.
pub(super) async fn stage_shared_blob_reclaim(
    transaction: &DbTransaction,
    scope: DataScope,
    intent: &work::TextUploadIntentValue,
) -> Result<()> {
    if !matches!(intent.phase, work::TextUploadPhase::Prepared)
        || !matches!(intent.work_state, work::TextUploadWorkState::Claimed(_))
    {
        return Err(corruption(
            "shared-blob reclaim requires an exact claimed Prepared intent",
        ));
    }
    match intent.owner {
        work::TextUploadOwner::Build { .. } => {
            super::build_owner::load_exact(transaction, scope, intent).await?;
        }
        work::TextUploadOwner::ActiveMutation { .. } => {
            validate_active_proof_absent(transaction, scope, intent).await?;
        }
    }

    stage_intent_qualified_candidate(transaction, scope, intent).await
}

/// Transfers an uploaded Active object to reclaim after exact graph abort.
pub(super) async fn stage_active_graph_abort_reclaim(
    transaction: &DbTransaction,
    scope: DataScope,
    intent: &work::TextUploadIntentValue,
) -> Result<()> {
    if !matches!(intent.phase, work::TextUploadPhase::Uploaded)
        || !matches!(intent.work_state, work::TextUploadWorkState::Claimed(_))
        || !matches!(intent.owner, work::TextUploadOwner::ActiveMutation { .. })
    {
        return Err(corruption(
            "Active graph-abort reclaim requires an exact claimed Uploaded intent",
        ));
    }
    validate_active_proof_absent(transaction, scope, intent).await?;
    stage_intent_qualified_candidate(transaction, scope, intent).await
}

/// Transfers one fenced generation intent to intent-qualified reclaim work.
///
/// The exact generation member and delete fence are validated by cleanup before
/// this boundary. Both `Prepared` terminal shared objects and unattached
/// `Uploaded` objects retain their publication permit until fenced disposition.
pub(super) async fn stage_fenced_intent_reclaim(
    transaction: &DbTransaction,
    scope: DataScope,
    intent: &work::TextUploadIntentValue,
) -> Result<()> {
    if !matches!(
        intent.phase,
        work::TextUploadPhase::Prepared | work::TextUploadPhase::Uploaded
    ) || !matches!(intent.work_state, work::TextUploadWorkState::Queued { .. })
    {
        return Err(corruption(
            "fenced intent reclaim requires queued Prepared or Uploaded work",
        ));
    }
    stage_intent_qualified_candidate(transaction, scope, intent).await
}

/// Atomically replaces one intent reachability row with its exact candidate.
async fn stage_intent_qualified_candidate(
    transaction: &DbTransaction,
    scope: DataScope,
    intent: &work::TextUploadIntentValue,
) -> Result<()> {
    let candidate_key = scoped_key(
        scope,
        index_keys::IndexV2Key::BlobGcCandidate(index_keys::BlobGcCandidateKey {
            index_id: intent.index_id,
            generation: intent.generation,
            owner: index_keys::BlobGcCandidateKeyOwner::UploadIntent(intent.intent_id),
            blob_hash: index_keys::BlobHash::new(*intent.blob.hash()),
        }),
    );
    if transaction.get(&candidate_key).await?.is_some() {
        return Err(corruption(
            "shared-blob reclaim candidate was occupied before ownership transfer",
        ));
    }
    let candidate_value = index_values::encode_work_value(
        &index_values::IndexV2WorkValue::BlobGcCandidate(work::BlobGcCandidateValue {
            owner: work::BlobGcCandidateOwner::UploadIntent(intent.intent_id),
            index_id: intent.index_id,
            generation: intent.generation,
            blob: intent.blob,
        }),
    );
    let (reference_key, reference_value) = intent_reachability_row(scope, intent);
    if transaction.get(&reference_key).await?.as_deref() != Some(reference_value.as_ref()) {
        return Err(corruption(
            "shared-blob reclaim is missing its exact intent reachability row",
        ));
    }

    transaction.put(candidate_key, candidate_value)?;
    transaction.delete(reference_key)?;
    Ok(())
}

/// Proves that one Active owner has no graph-commit proof to preserve.
async fn validate_active_proof_absent(
    transaction: &DbTransaction,
    scope: DataScope,
    intent: &work::TextUploadIntentValue,
) -> Result<()> {
    let work::TextUploadOwner::ActiveMutation { .. } = intent.owner else {
        return Err(corruption(
            "Active proof-absence validation received a build-owned upload",
        ));
    };
    let proof_key = scoped_key(
        scope,
        index_keys::IndexV2Key::ActiveMutationCommitProof(index_keys::TextIntentOwnedKey {
            index_id: intent.index_id,
            generation: intent.generation,
            intent_id: intent.intent_id,
        }),
    );
    if transaction.get(proof_key).await?.is_some() {
        return Err(corruption(
            "Active upload cannot be reclaimed while its exact commit proof exists",
        ));
    }
    Ok(())
}

/// Constructs one scoped physical row key from a typed logical V2 key.
fn scoped_key(scope: DataScope, logical_key: index_keys::IndexV2Key) -> Bytes {
    Key::Data {
        scope,
        kind: DataKeyKind::IndexV2(logical_key),
    }
    .to_bytes()
}

/// Constructs the exact global reachability row owned by one upload intent.
pub(super) fn intent_reachability_row(
    scope: DataScope,
    intent: &work::TextUploadIntentValue,
) -> (Bytes, Bytes) {
    let owner_kind = index_keys::BlobReferenceOwnerKind::UploadIntent;
    let owner_logical_key =
        index_keys::IndexV2Key::TextUploadIntent(index_keys::TextIntentOwnedKey {
            index_id: intent.index_id,
            generation: intent.generation,
            intent_id: intent.intent_id,
        })
        .to_bytes();
    let value = work::BlobReachabilityReferenceValue::try_new(
        intent.blob,
        owner_kind,
        scope,
        owner_logical_key.clone(),
        0,
    )
    .expect("typed upload intent satisfies the reachability value contract");
    let key = index_keys::BlobReferenceGlobalKey::try_new(
        index_keys::BlobHash::new(*intent.blob.hash()),
        owner_kind,
        scope,
        owner_logical_key,
        0,
    )
    .expect("validated upload reachability value has the same typed key contract");
    (
        Key::Global {
            kind: GlobalKeyKind::IndexV2(index_keys::GlobalIndexV2Key::BlobReachabilityReference(
                key,
            )),
        }
        .to_bytes(),
        index_values::encode_work_value(
            &index_values::IndexV2WorkValue::BlobReachabilityReference(value),
        ),
    )
}

fn corruption(reason: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(reason.into())
}
