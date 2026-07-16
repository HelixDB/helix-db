//! Atomic first-reference transitions for uploaded text build artifacts.
//!
//! The upload intent remains authoritative until this module creates the exact
//! scoped artifact and global blob-reference row in the same transaction that
//! removes the intent-owned reference and records `ReferenceCommitted`. The
//! caller holds coordinator reference authority through the transaction commit.
//! Manifest-page and active-mutation attachments have distinct proof contracts
//! and are deliberately installed by later modules rather than inferred here.

use bytes::Bytes;
use slatedb::DbTransaction;

use crate::encoding::v1::keys::index_v2 as index_keys;
use crate::encoding::v1::keys::tenant::DataScope;
use crate::encoding::v1::keys::{DataKeyKind, GlobalKeyKind, Key};
use crate::encoding::v1::values::index_v2 as index_values;
use crate::error::{HelixDbError, Result};

use super::super::work;

/// Stages one exact build-artifact owner and transfers blob reachability to it.
pub(super) async fn stage_build_artifact_attachment(
    transaction: &DbTransaction,
    scope: DataScope,
    intent: &work::TextUploadIntentValue,
) -> Result<work::UploadDestinationAuthorization> {
    if !matches!(intent.phase, work::TextUploadPhase::Uploaded)
        || !matches!(intent.work_state, work::TextUploadWorkState::Claimed(_))
    {
        return Err(corruption(
            "build-artifact attachment requires an exact claimed Uploaded intent",
        ));
    }
    if !matches!(intent.owner, work::TextUploadOwner::Build { .. }) {
        return Err(corruption(
            "active text uploads cannot create hidden build artifacts",
        ));
    }
    let work::TextUploadAttachment::BuildArtifact {
        artifact_ordinal,
        split,
    } = intent.attachment
    else {
        return Err(corruption(
            "build-artifact attachment received a manifest destination",
        ));
    };

    super::build_owner::load_exact(transaction, scope, intent).await?;

    let artifact_owner = index_keys::TextBuildArtifactKey {
        root: index_keys::TextManifestRootKey {
            index_id: intent.index_id,
            generation: intent.generation,
            partition: intent.partition.fingerprint(),
        },
        ordinal: artifact_ordinal,
    };
    let artifact_logical_key = index_keys::IndexV2Key::TextBuildArtifact(artifact_owner);
    let artifact_key = scoped_key(scope, artifact_logical_key.clone());
    let artifact_value = index_values::encode_work_value(
        &index_values::IndexV2WorkValue::TextBuildArtifact(work::TextBuildArtifactValue {
            index_id: intent.index_id,
            generation: intent.generation,
            partition: intent.partition.clone(),
            artifact_ordinal,
            split,
            source_intent_id: intent.intent_id,
        }),
    );
    let (artifact_reference_key, artifact_reference_value) = reachability_row(
        intent.blob,
        scope,
        ReachabilityOwner::BuildArtifact(artifact_owner),
        0,
    );

    let intent_owner = index_keys::TextIntentOwnedKey {
        index_id: intent.index_id,
        generation: intent.generation,
        intent_id: intent.intent_id,
    };
    let (intent_reference_key, intent_reference_value) = reachability_row(
        intent.blob,
        scope,
        ReachabilityOwner::UploadIntent(intent_owner),
        0,
    );

    let existing_artifact = transaction.get(&artifact_key).await?;
    let existing_artifact_reference = transaction.get(&artifact_reference_key).await?;
    let existing_intent_reference = transaction.get(&intent_reference_key).await?;
    if existing_intent_reference.as_deref() != Some(intent_reference_value.as_ref()) {
        return Err(corruption(
            "uploaded text intent is missing its exact live-reference row",
        ));
    }
    if existing_artifact.is_some() || existing_artifact_reference.is_some() {
        return Err(corruption(
            "build-artifact destination was already occupied before attachment",
        ));
    }

    transaction.put(artifact_key, artifact_value)?;
    transaction.put(artifact_reference_key, artifact_reference_value)?;
    transaction.delete(intent_reference_key)?;
    let authorization = work::UploadDestinationAuthorization::try_new(
        index_keys::BlobReferenceOwnerKind::BuildArtifact,
        artifact_logical_key.to_bytes(),
        0,
        None,
    )
    .expect("typed build-artifact owner satisfies the destination authorization contract");
    Ok(authorization)
}

/// Moves a recovered Active upload to its exact proof-backed manifest reference.
///
/// Absence is returned explicitly because an uploaded Active intent may belong
/// to a graph transaction that aborted. A present proof carries the bounded
/// page/slot location needed to point-read the manifest entry and its global
/// reachability row; recovery never scans a partition or guesses from logical
/// version.
pub(super) async fn stage_active_manifest_reference_checkpoint(
    transaction: &DbTransaction,
    scope: DataScope,
    intent: &work::TextUploadIntentValue,
) -> Result<Option<work::UploadDestinationAuthorization>> {
    if !matches!(intent.phase, work::TextUploadPhase::Uploaded)
        || !matches!(intent.work_state, work::TextUploadWorkState::Claimed(_))
    {
        return Err(corruption(
            "Active manifest recovery requires an exact claimed Uploaded intent",
        ));
    }
    let work::TextUploadOwner::ActiveMutation {
        writer_epoch,
        mutation_id,
        active_record_revision,
    } = intent.owner
    else {
        return Err(corruption(
            "build-owned upload cannot recover an Active manifest proof",
        ));
    };
    let work::TextUploadAttachment::ManifestSplit(split) = intent.attachment else {
        return Err(corruption(
            "Active manifest recovery requires a manifest split attachment",
        ));
    };
    let intent_owner = index_keys::TextIntentOwnedKey {
        index_id: intent.index_id,
        generation: intent.generation,
        intent_id: intent.intent_id,
    };
    let proof_logical_key = index_keys::IndexV2Key::ActiveMutationCommitProof(intent_owner);
    let proof_key = scoped_key(scope, proof_logical_key.clone());
    let Some(proof_value) = transaction.get(&proof_key).await? else {
        return Ok(None);
    };
    let index_values::IndexV2WorkValue::ActiveMutationCommitProof(proof) =
        index_values::decode_work_value(&proof_value)?
    else {
        return Err(corruption(
            "Active manifest proof key contains another value kind",
        ));
    };
    if proof.intent_id != intent.intent_id
        || proof.index_id != intent.index_id
        || proof.generation != intent.generation
        || proof.partition != intent.partition
        || proof.writer_epoch != writer_epoch
        || proof.mutation_id != mutation_id
        || proof.active_record_revision != active_record_revision
        || proof.split != split
    {
        return Err(corruption(
            "Active manifest proof disagrees with its recovered upload",
        ));
    }
    let page_owner = index_keys::TextManifestPageKey {
        root: index_keys::TextManifestRootKey {
            index_id: intent.index_id,
            generation: intent.generation,
            partition: intent.partition.fingerprint(),
        },
        page: proof.destination.page(),
    };
    let page_logical_key = index_keys::IndexV2Key::TextManifestPage(page_owner);
    let page_key = scoped_key(scope, page_logical_key.clone());
    let Some(page_value) = transaction.get(&page_key).await? else {
        return Err(corruption(
            "Active manifest proof destination page is absent",
        ));
    };
    let index_values::IndexV2WorkValue::TextManifestPage(page) =
        index_values::decode_work_value(&page_value)?
    else {
        return Err(corruption(
            "Active manifest proof destination contains another value kind",
        ));
    };
    let slot = usize::try_from(proof.destination.slot())
        .expect("validated manifest split slot fits usize");
    if page.index_id() != intent.index_id
        || page.generation() != intent.generation
        || page.partition() != &intent.partition
        || page.page() != proof.destination.page()
        || page.entries().get(slot) != Some(&split)
    {
        return Err(corruption(
            "Active manifest proof does not name its exact page split",
        ));
    }
    let (destination_reference_key, destination_reference_value) =
        manifest_page_reachability_row(intent.blob, scope, page_owner, proof.destination.slot());
    if transaction.get(destination_reference_key).await?.as_deref()
        != Some(destination_reference_value.as_ref())
    {
        return Err(corruption(
            "Active manifest proof destination lost its reachability row",
        ));
    }
    let (intent_reference_key, intent_reference_value) = reachability_row(
        intent.blob,
        scope,
        ReachabilityOwner::UploadIntent(intent_owner),
        0,
    );
    if transaction.get(&intent_reference_key).await?.as_deref()
        != Some(intent_reference_value.as_ref())
    {
        return Err(corruption(
            "recovered Active upload lost its intent reachability row",
        ));
    }
    transaction.delete(intent_reference_key)?;
    Ok(Some(
        work::UploadDestinationAuthorization::try_new(
            index_keys::BlobReferenceOwnerKind::ManifestPageSplit,
            page_logical_key.to_bytes(),
            proof.destination.slot(),
            Some(proof_logical_key.to_bytes()),
        )
        .expect("validated Active proof destination forms bounded historical authority"),
    ))
}

/// Verifies historical reference authority before released-anchor deletion.
pub(super) async fn stage_reference_anchor_cleanup(
    transaction: &DbTransaction,
    scope: DataScope,
    intent: &work::TextUploadIntentValue,
) -> Result<()> {
    let proof = validate_reference_anchor_cleanup(
        transaction,
        scope,
        intent,
        ReferenceAnchorCleanupAuthority::ClaimedUpload,
    )
    .await?;
    if let ReferenceProofCleanup::Present(proof_key) = proof {
        transaction.delete(proof_key)?;
    }
    Ok(())
}

/// Prevalidates exact historical authority before external fenced release.
///
/// The returned key is the deterministic Active proof deletion submitted by
/// the later commit even when that proof is already absent. Build owners have
/// no proof key and return `None`.
pub(super) async fn prepare_fenced_reference_anchor_cleanup(
    transaction: &DbTransaction,
    scope: DataScope,
    intent: &work::TextUploadIntentValue,
) -> Result<Option<Bytes>> {
    Ok(
        match validate_reference_anchor_cleanup(
            transaction,
            scope,
            intent,
            ReferenceAnchorCleanupAuthority::FencedGeneration,
        )
        .await?
        {
            ReferenceProofCleanup::None => None,
            ReferenceProofCleanup::Absent(key) | ReferenceProofCleanup::Present(key) => Some(key),
        },
    )
}

/// Verifies and removes proof residue for one fenced generation cleanup.
///
/// The caller must retain the exact generation root, member fence, scope gate,
/// and blob-deletion gate through commit. This boundary accepts queued work so
/// cleanup never manufactures a worker claim after externally releasing the
/// historical permit authority.
pub(super) async fn stage_fenced_reference_anchor_cleanup(
    transaction: &DbTransaction,
    scope: DataScope,
    intent: &work::TextUploadIntentValue,
) -> Result<Option<Bytes>> {
    let proof_key = prepare_fenced_reference_anchor_cleanup(transaction, scope, intent).await?;
    if let Some(proof_key) = proof_key.as_ref() {
        transaction.delete(proof_key)?;
    }
    Ok(proof_key)
}

/// The two repository authorities allowed to retire a committed anchor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReferenceAnchorCleanupAuthority {
    ClaimedUpload,
    FencedGeneration,
}

/// Exact Active proof observation selected before any deletion is staged.
enum ReferenceProofCleanup {
    None,
    Absent(Bytes),
    Present(Bytes),
}

async fn validate_reference_anchor_cleanup(
    transaction: &DbTransaction,
    scope: DataScope,
    intent: &work::TextUploadIntentValue,
    authority: ReferenceAnchorCleanupAuthority,
) -> Result<ReferenceProofCleanup> {
    let work::TextUploadPhase::ReferenceCommitted(authorization) = &intent.phase else {
        return Err(corruption(
            "reference cleanup requires a ReferenceCommitted intent",
        ));
    };
    let valid_work_state = match authority {
        ReferenceAnchorCleanupAuthority::ClaimedUpload => {
            matches!(intent.work_state, work::TextUploadWorkState::Claimed(_))
        }
        ReferenceAnchorCleanupAuthority::FencedGeneration => {
            matches!(intent.work_state, work::TextUploadWorkState::Queued { .. })
        }
    };
    if !valid_work_state {
        return Err(corruption(match authority {
            ReferenceAnchorCleanupAuthority::ClaimedUpload => {
                "reference cleanup requires an exact claimed intent"
            }
            ReferenceAnchorCleanupAuthority::FencedGeneration => {
                "fenced reference cleanup requires an exact queued intent"
            }
        }));
    }
    let active_proof = match (&intent.owner, &authorization.proof_logical_key) {
        (work::TextUploadOwner::Build { .. }, None) => None,
        (
            work::TextUploadOwner::ActiveMutation {
                writer_epoch,
                mutation_id,
                active_record_revision,
            },
            Some(proof_logical_key),
        ) => Some((
            *writer_epoch,
            *mutation_id,
            *active_record_revision,
            proof_logical_key,
        )),
        _ => {
            return Err(corruption(
                "reference cleanup owner disagrees with its proof authority",
            ));
        }
    };
    let destination = index_keys::IndexV2Key::parse_from_slice(&authorization.owner_logical_key)
        .expect("validated destination authorization retains a typed logical key");
    let destination_matches = match (&intent.attachment, destination) {
        (
            work::TextUploadAttachment::BuildArtifact {
                artifact_ordinal, ..
            },
            index_keys::IndexV2Key::TextBuildArtifact(key),
        ) => {
            authorization.owner_kind == index_keys::BlobReferenceOwnerKind::BuildArtifact
                && authorization.owner_slot == 0
                && key.root.index_id == intent.index_id
                && key.root.generation == intent.generation
                && key.root.partition == intent.partition.fingerprint()
                && key.ordinal == *artifact_ordinal
        }
        (
            work::TextUploadAttachment::ManifestSplit(_),
            index_keys::IndexV2Key::TextManifestPage(key),
        ) => {
            authorization.owner_kind == index_keys::BlobReferenceOwnerKind::ManifestPageSplit
                && key.root.index_id == intent.index_id
                && key.root.generation == intent.generation
                && key.root.partition == intent.partition.fingerprint()
        }
        _ => false,
    };
    if !destination_matches {
        return Err(corruption(
            "reference cleanup authorization disagrees with the upload destination",
        ));
    }
    let intent_owner = index_keys::TextIntentOwnedKey {
        index_id: intent.index_id,
        generation: intent.generation,
        intent_id: intent.intent_id,
    };
    let (intent_reference_key, _intent_reference_value) = reachability_row(
        intent.blob,
        scope,
        ReachabilityOwner::UploadIntent(intent_owner),
        0,
    );
    if transaction.get(intent_reference_key).await?.is_some() {
        return Err(corruption(
            "ReferenceCommitted text intent unexpectedly retained upload reachability",
        ));
    }
    let Some((writer_epoch, mutation_id, active_record_revision, proof_logical_key)) = active_proof
    else {
        return Ok(ReferenceProofCleanup::None);
    };
    let proof_logical_key = index_keys::IndexV2Key::parse_from_slice(proof_logical_key)
        .expect("validated Active proof authority retains a typed logical key");
    let index_keys::IndexV2Key::ActiveMutationCommitProof(proof_owner) = proof_logical_key else {
        return Err(corruption(
            "Active reference cleanup authority is not a commit proof",
        ));
    };
    if proof_owner.index_id != intent.index_id
        || proof_owner.generation != intent.generation
        || proof_owner.intent_id != intent.intent_id
    {
        return Err(corruption(
            "Active reference cleanup proof key disagrees with its upload",
        ));
    }
    let proof_key = scoped_key(
        scope,
        index_keys::IndexV2Key::ActiveMutationCommitProof(proof_owner),
    );
    let Some(proof_value) = transaction.get(&proof_key).await? else {
        return Ok(ReferenceProofCleanup::Absent(proof_key));
    };
    let index_values::IndexV2WorkValue::ActiveMutationCommitProof(proof) =
        index_values::decode_work_value(&proof_value)?
    else {
        return Err(corruption(
            "Active reference cleanup proof key contains another value kind",
        ));
    };
    let work::TextUploadAttachment::ManifestSplit(split) = intent.attachment else {
        return Err(corruption(
            "Active reference cleanup requires a manifest split attachment",
        ));
    };
    let index_keys::IndexV2Key::TextManifestPage(destination_page) =
        index_keys::IndexV2Key::parse_from_slice(&authorization.owner_logical_key)
            .expect("validated destination authority retains a typed logical key")
    else {
        return Err(corruption(
            "Active reference cleanup destination is not a manifest page",
        ));
    };
    if proof.intent_id != intent.intent_id
        || proof.index_id != intent.index_id
        || proof.generation != intent.generation
        || proof.partition != intent.partition
        || proof.writer_epoch != writer_epoch
        || proof.mutation_id != mutation_id
        || proof.active_record_revision != active_record_revision
        || proof.destination.page() != destination_page.page
        || proof.destination.slot() != authorization.owner_slot
        || proof.split != split
    {
        return Err(corruption(
            "Active reference cleanup proof disagrees with its historical upload authority",
        ));
    }
    Ok(ReferenceProofCleanup::Present(proof_key))
}

/// Constructs one scoped physical row key from a typed logical V2 key.
pub(super) fn scoped_key(scope: DataScope, logical_key: index_keys::IndexV2Key) -> Bytes {
    Key::Data {
        scope,
        kind: DataKeyKind::IndexV2(logical_key),
    }
    .to_bytes()
}

/// Closed owner shapes accepted by build-artifact reachability transitions.
enum ReachabilityOwner {
    UploadIntent(index_keys::TextIntentOwnedKey),
    BuildArtifact(index_keys::TextBuildArtifactKey),
    ManifestPage(index_keys::TextManifestPageKey),
}

impl ReachabilityOwner {
    /// Returns the frozen owner lane and its matching typed logical key.
    fn into_parts(self) -> (index_keys::BlobReferenceOwnerKind, index_keys::IndexV2Key) {
        match self {
            Self::UploadIntent(key) => (
                index_keys::BlobReferenceOwnerKind::UploadIntent,
                index_keys::IndexV2Key::TextUploadIntent(key),
            ),
            Self::BuildArtifact(key) => (
                index_keys::BlobReferenceOwnerKind::BuildArtifact,
                index_keys::IndexV2Key::TextBuildArtifact(key),
            ),
            Self::ManifestPage(key) => (
                index_keys::BlobReferenceOwnerKind::ManifestPageSplit,
                index_keys::IndexV2Key::TextManifestPage(key),
            ),
        }
    }
}

/// Constructs an exact global reachability key/value pair for one typed owner.
fn reachability_row(
    blob: work::BlobRef,
    scope: DataScope,
    owner: ReachabilityOwner,
    owner_slot: u32,
) -> (Bytes, Bytes) {
    let (owner_kind, owner_logical_key) = owner.into_parts();
    let owner_logical_key = owner_logical_key.to_bytes();
    let value = work::BlobReachabilityReferenceValue::try_new(
        blob,
        owner_kind,
        scope,
        owner_logical_key.clone(),
        owner_slot,
    )
    .expect("typed reachability owner satisfies the canonical value contract");
    let global_key = index_keys::BlobReferenceGlobalKey::try_new(
        index_keys::BlobHash::new(*blob.hash()),
        owner_kind,
        scope,
        owner_logical_key,
        owner_slot,
    )
    .expect("validated reachability value has the same typed key contract");
    (
        Key::Global {
            kind: GlobalKeyKind::IndexV2(index_keys::GlobalIndexV2Key::BlobReachabilityReference(
                global_key,
            )),
        }
        .to_bytes(),
        index_values::encode_work_value(
            &index_values::IndexV2WorkValue::BlobReachabilityReference(value),
        ),
    )
}

/// Constructs the exact global reachability row owned by one build artifact.
///
/// Compaction uses this same boundary when it retires replaced artifacts, so
/// attachment and retirement cannot disagree about the frozen owner key or
/// slot used by the global reachability index.
pub(super) fn build_artifact_reachability_row(
    blob: work::BlobRef,
    scope: DataScope,
    owner: index_keys::TextBuildArtifactKey,
) -> (Bytes, Bytes) {
    reachability_row(blob, scope, ReachabilityOwner::BuildArtifact(owner), 0)
}

/// Constructs the exact global reachability row owned by one manifest-page slot.
pub(super) fn manifest_page_reachability_row(
    blob: work::BlobRef,
    scope: DataScope,
    owner: index_keys::TextManifestPageKey,
    slot: u32,
) -> (Bytes, Bytes) {
    reachability_row(blob, scope, ReachabilityOwner::ManifestPage(owner), slot)
}

/// Decodes and cross-checks one generation-owned text build artifact.
pub(super) fn decode_build_artifact(
    scope: DataScope,
    operation: &super::super::IndexOperationRecord,
    key: &[u8],
    value: &[u8],
) -> Result<(
    index_keys::TextBuildArtifactKey,
    work::TextBuildArtifactValue,
)> {
    let Key::Data {
        kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::TextBuildArtifact(key)),
        ..
    } = Key::parse_from_slice(scope, key)?
    else {
        return Err(corruption(
            "text artifact prefix yielded another typed key kind",
        ));
    };
    let index_values::IndexV2WorkValue::TextBuildArtifact(artifact) =
        index_values::decode_work_value(value)?
    else {
        return Err(corruption(
            "text artifact key contains another typed value kind",
        ));
    };
    if key.root.index_id != operation.index_id()
        || key.root.generation != operation.generation()
        || key.root.partition != artifact.partition.fingerprint()
        || key.ordinal != artifact.artifact_ordinal
        || artifact.index_id != operation.index_id()
        || artifact.generation != operation.generation()
    {
        return Err(corruption(
            "text build artifact key/value ownership mismatch",
        ));
    }
    Ok((key, artifact))
}

fn corruption(reason: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(reason.into())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sha2::{Digest, Sha256};
    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, IsolationLevel};

    use super::*;
    use crate::config::TextIndexDefinition;
    use crate::index_v2::outbox;
    use crate::index_v2::outbox::ExpectedCanonicalRevision;
    use crate::index_v2::{
        BlobPublicationPermitId, ClaimSequence, IndexComponent, IndexElementKind,
        IndexGenerationId, IndexId, IndexIdentity, IndexIdentityFamily, IndexOperationFamily,
        IndexOperationId, IndexOperationKind, IndexOperationProgress, IndexOperationRecord,
        IndexOperationRevision, IndexRecordV2, IndexRevision, MutationId, OperationClaim,
        OperationCounters, PhysicalGeneration, PrefixScanProgress, TextBuildProgress,
        TextBuildStage, TextIntentRevision, TextUploadIntentId, ValidatedDynamicIndexDefinition,
        WriterEpoch,
    };

    /// Opens one isolated in-memory database for attachment contract tests.
    async fn raw_db(name: &str) -> Db {
        Db::open(name, Arc::new(InMemory::new())).await.unwrap()
    }

    /// Returns the canonical text identity used by every local fixture.
    fn text_identity() -> IndexIdentity {
        IndexIdentity::new(
            IndexIdentityFamily::Text,
            IndexElementKind::Node,
            IndexComponent::try_new("label", "Document").unwrap(),
            IndexComponent::try_new("property", "body").unwrap(),
        )
    }

    /// Builds one deterministic split for a declared upload attachment.
    fn split() -> work::SplitRef {
        let payload = b"attachment-contract";
        let blob = work::BlobRef::new(
            Sha256::digest(payload).into(),
            u64::try_from(payload.len()).unwrap(),
        );
        work::SplitRef::try_new(blob, 0, 0, 0, blob.size()).unwrap()
    }

    /// Constructs a valid claimed build-artifact upload at the requested phase.
    fn build_intent(phase: work::TextUploadPhase) -> work::TextUploadIntentValue {
        let split = split();
        work::TextUploadIntentValue::try_new(
            TextUploadIntentId::from_bytes([51; 16]).unwrap(),
            TextIntentRevision::initial(),
            IndexId::initial(),
            text_identity(),
            IndexGenerationId::initial(),
            work::TextPartition::Unpartitioned,
            split.blob(),
            BlobPublicationPermitId::from_bytes([52; 16]).unwrap(),
            work::TextUploadOwner::Build {
                operation_id: IndexOperationId::from_bytes([53; 16]).unwrap(),
                expected_operation_revision: IndexOperationRevision::initial(),
            },
            work::TextUploadAttachment::BuildArtifact {
                artifact_ordinal: 3,
                split,
            },
            phase,
            1,
            work::TextUploadWorkState::Claimed(OperationClaim {
                writer_epoch: WriterEpoch::from_bytes([54; 16]).unwrap(),
                sequence: ClaimSequence::new(1).unwrap(),
            }),
        )
        .unwrap()
    }

    /// Installs the exact canonical Building record, operation, and pointer.
    async fn enqueue_build_operation(db: &Db, scope: DataScope) {
        let operation_id = IndexOperationId::from_bytes([53; 16]).unwrap();
        let definition = ValidatedDynamicIndexDefinition::try_from(
            TextIndexDefinition::new_node("Document", "body").unwrap(),
        )
        .unwrap();
        let index = IndexRecordV2::building(
            IndexId::initial(),
            definition,
            IndexRevision::initial(),
            PhysicalGeneration::Text {
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
            IndexOperationFamily::Text,
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::Compact(PrefixScanProgress {
                    cursor: None,
                    counters: OperationCounters::default(),
                }),
            )),
            0,
            crate::index_v2::IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .unwrap();
        outbox::enqueue_operation(
            db,
            scope,
            ExpectedCanonicalRevision::Absent,
            &index,
            &operation,
        )
        .await
        .unwrap();
    }

    /// Extracts one corruption reason while proving the public error category.
    fn corruption_reason(error: HelixDbError) -> String {
        let HelixDbError::IndexCatalogCorruption(reason) = error else {
            panic!("attachment contract returns catalog corruption");
        };
        reason
    }

    #[tokio::test]
    async fn attachment_rejects_invalid_phase_owner_destination_and_missing_operation() {
        let db = raw_db("text-attachment-invalid-shapes").await;
        let scope = DataScope::LegacyUnscoped;
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();

        let prepared = build_intent(work::TextUploadPhase::Prepared);
        assert!(corruption_reason(
            stage_build_artifact_attachment(&transaction, scope, &prepared)
                .await
                .unwrap_err()
        )
        .contains("claimed Uploaded"));

        let split = split();
        let active = work::TextUploadIntentValue::try_new(
            TextUploadIntentId::from_bytes([55; 16]).unwrap(),
            TextIntentRevision::initial(),
            IndexId::initial(),
            text_identity(),
            IndexGenerationId::initial(),
            work::TextPartition::Unpartitioned,
            split.blob(),
            BlobPublicationPermitId::from_bytes([56; 16]).unwrap(),
            work::TextUploadOwner::ActiveMutation {
                writer_epoch: WriterEpoch::from_bytes([57; 16]).unwrap(),
                mutation_id: MutationId::from_bytes([58; 16]).unwrap(),
                active_record_revision: IndexRevision::initial(),
            },
            work::TextUploadAttachment::ManifestSplit(split),
            work::TextUploadPhase::Uploaded,
            1,
            work::TextUploadWorkState::Claimed(OperationClaim {
                writer_epoch: WriterEpoch::from_bytes([57; 16]).unwrap(),
                sequence: ClaimSequence::new(1).unwrap(),
            }),
        )
        .unwrap();
        assert!(corruption_reason(
            stage_build_artifact_attachment(&transaction, scope, &active)
                .await
                .unwrap_err()
        )
        .contains("active text uploads"));

        let mut manifest_destination = build_intent(work::TextUploadPhase::Uploaded);
        manifest_destination.attachment = work::TextUploadAttachment::ManifestSplit(split);
        assert!(corruption_reason(
            stage_build_artifact_attachment(&transaction, scope, &manifest_destination)
                .await
                .unwrap_err()
        )
        .contains("manifest destination"));

        let uploaded = build_intent(work::TextUploadPhase::Uploaded);
        assert!(corruption_reason(
            stage_build_artifact_attachment(&transaction, scope, &uploaded)
                .await
                .unwrap_err()
        )
        .contains("no exact runnable operation"));
        drop(transaction);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn attachment_rejects_checkpoint_drift_missing_reference_and_occupied_destination() {
        let db = raw_db("text-attachment-durable-drift").await;
        let scope = DataScope::LegacyUnscoped;
        enqueue_build_operation(&db, scope).await;

        let mut drifted = build_intent(work::TextUploadPhase::Uploaded);
        drifted.owner = work::TextUploadOwner::Build {
            operation_id: IndexOperationId::from_bytes([53; 16]).unwrap(),
            expected_operation_revision: IndexOperationRevision::initial().checked_next().unwrap(),
        };
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(corruption_reason(
            stage_build_artifact_attachment(&transaction, scope, &drifted)
                .await
                .unwrap_err()
        )
        .contains("exact Building checkpoint"));
        drop(transaction);

        let uploaded = build_intent(work::TextUploadPhase::Uploaded);
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(corruption_reason(
            stage_build_artifact_attachment(&transaction, scope, &uploaded)
                .await
                .unwrap_err()
        )
        .contains("missing its exact live-reference"));
        drop(transaction);

        let intent_owner = index_keys::TextIntentOwnedKey {
            index_id: uploaded.index_id,
            generation: uploaded.generation,
            intent_id: uploaded.intent_id,
        };
        let (intent_reference_key, intent_reference_value) = reachability_row(
            uploaded.blob,
            scope,
            ReachabilityOwner::UploadIntent(intent_owner),
            0,
        );
        let artifact_logical_key =
            index_keys::IndexV2Key::TextBuildArtifact(index_keys::TextBuildArtifactKey {
                root: index_keys::TextManifestRootKey {
                    index_id: uploaded.index_id,
                    generation: uploaded.generation,
                    partition: uploaded.partition.fingerprint(),
                },
                ordinal: 3,
            });
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        transaction
            .put(intent_reference_key, intent_reference_value)
            .unwrap();
        transaction
            .put(
                scoped_key(scope, artifact_logical_key),
                Bytes::from_static(b"occupied"),
            )
            .unwrap();
        transaction.commit().await.unwrap();

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(corruption_reason(
            stage_build_artifact_attachment(&transaction, scope, &uploaded)
                .await
                .unwrap_err()
        )
        .contains("already occupied"));
        drop(transaction);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn reference_cleanup_rejects_wrong_phase_owner_and_retained_intent_reference() {
        let db = raw_db("text-reference-anchor-cleanup-errors").await;
        let scope = DataScope::LegacyUnscoped;
        let uploaded = build_intent(work::TextUploadPhase::Uploaded);
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(corruption_reason(
            stage_reference_anchor_cleanup(&transaction, scope, &uploaded)
                .await
                .unwrap_err()
        )
        .contains("requires a ReferenceCommitted"));

        let artifact_key =
            index_keys::IndexV2Key::TextBuildArtifact(index_keys::TextBuildArtifactKey {
                root: index_keys::TextManifestRootKey {
                    index_id: uploaded.index_id,
                    generation: uploaded.generation,
                    partition: uploaded.partition.fingerprint(),
                },
                ordinal: 3,
            })
            .to_bytes();
        let proof_key =
            index_keys::IndexV2Key::ActiveMutationCommitProof(index_keys::TextIntentOwnedKey {
                index_id: uploaded.index_id,
                generation: uploaded.generation,
                intent_id: uploaded.intent_id,
            })
            .to_bytes();
        let mut proof_owned = uploaded.clone();
        proof_owned.phase = work::TextUploadPhase::ReferenceCommitted(
            work::UploadDestinationAuthorization::try_new(
                index_keys::BlobReferenceOwnerKind::BuildArtifact,
                artifact_key.clone(),
                0,
                Some(proof_key),
            )
            .unwrap(),
        );
        assert!(corruption_reason(
            stage_reference_anchor_cleanup(&transaction, scope, &proof_owned)
                .await
                .unwrap_err()
        )
        .contains("owner disagrees"));

        let mut referenced = uploaded;
        referenced.phase = work::TextUploadPhase::ReferenceCommitted(
            work::UploadDestinationAuthorization::try_new(
                index_keys::BlobReferenceOwnerKind::BuildArtifact,
                artifact_key.clone(),
                0,
                None,
            )
            .unwrap(),
        );
        let mut queued = referenced.clone();
        queued.work_state = work::TextUploadWorkState::Queued {
            not_before_unix_millis: None,
        };
        assert!(corruption_reason(
            stage_reference_anchor_cleanup(&transaction, scope, &queued)
                .await
                .unwrap_err()
        )
        .contains("exact claimed intent"));

        let mut wrong_destination = referenced.clone();
        wrong_destination.phase = work::TextUploadPhase::ReferenceCommitted(
            work::UploadDestinationAuthorization::try_new(
                index_keys::BlobReferenceOwnerKind::BuildArtifact,
                index_keys::IndexV2Key::TextBuildArtifact(index_keys::TextBuildArtifactKey {
                    root: index_keys::TextManifestRootKey {
                        index_id: referenced.index_id,
                        generation: referenced.generation,
                        partition: referenced.partition.fingerprint(),
                    },
                    ordinal: 4,
                })
                .to_bytes(),
                0,
                None,
            )
            .unwrap(),
        );
        assert!(corruption_reason(
            stage_reference_anchor_cleanup(&transaction, scope, &wrong_destination)
                .await
                .unwrap_err()
        )
        .contains("authorization disagrees"));

        let mut manifest_reference = referenced.clone();
        manifest_reference.attachment = work::TextUploadAttachment::ManifestSplit(split());
        manifest_reference.phase = work::TextUploadPhase::ReferenceCommitted(
            work::UploadDestinationAuthorization::try_new(
                index_keys::BlobReferenceOwnerKind::ManifestPageSplit,
                index_keys::IndexV2Key::TextManifestPage(index_keys::TextManifestPageKey {
                    root: index_keys::TextManifestRootKey {
                        index_id: referenced.index_id,
                        generation: referenced.generation,
                        partition: referenced.partition.fingerprint(),
                    },
                    page: 0,
                })
                .to_bytes(),
                0,
                None,
            )
            .unwrap(),
        );
        stage_reference_anchor_cleanup(&transaction, scope, &manifest_reference)
            .await
            .unwrap();

        let intent_owner = index_keys::TextIntentOwnedKey {
            index_id: referenced.index_id,
            generation: referenced.generation,
            intent_id: referenced.intent_id,
        };
        let (intent_reference_key, intent_reference_value) = reachability_row(
            referenced.blob,
            scope,
            ReachabilityOwner::UploadIntent(intent_owner),
            0,
        );
        transaction
            .put(intent_reference_key, intent_reference_value)
            .unwrap();
        assert!(corruption_reason(
            stage_reference_anchor_cleanup(&transaction, scope, &referenced)
                .await
                .unwrap_err()
        )
        .contains("unexpectedly retained upload reachability"));

        drop(transaction);
        db.close().await.unwrap();
    }
}
