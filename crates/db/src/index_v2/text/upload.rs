//! Atomic preparation of durable text upload intents.
//!
//! Blob publication is not admitted until this repository boundary has
//! atomically committed the scoped `Prepared` intent, its global runnable
//! pointer, and its global live-reference row. The coordinator permit is
//! reserved before the caller opens this transaction and is persisted only as
//! its opaque ID, making the transaction a durable outbox rather than a claim
//! that database and object-store writes are atomic.

use bytes::Bytes;
use slatedb::{Db, DbReadOps, DbTransaction, IsolationLevel};

use crate::encoding::v1::keys::index_v2 as index_keys;
use crate::encoding::v1::keys::tenant::DataScope;
use crate::encoding::v1::keys::{DataKeyKind, GlobalKeyKind, Key};
use crate::encoding::v1::values::index_v2 as index_values;
use crate::error::{HelixDbError, Result};

use super::super::{blob_publication, work};
#[cfg(any(test, feature = "production-coverage"))]
use super::super::{IndexGenerationId, IndexId, IndexIdentity};
use super::super::{
    IndexV2MetadataValue, TextIntentRevision, TextUploadIntentId, UploadQueuePointerValue,
};

const UPLOAD_INTENT_REFERENCE_SLOT: u32 = 0;

/// A complete initial upload row whose phase and scheduling state cannot vary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedTextUploadIntent(work::TextUploadIntentValue);

impl PreparedTextUploadIntent {
    /// Validates one exact reserved upload before any database transaction.
    #[cfg(any(test, feature = "production-coverage"))]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        intent_id: TextUploadIntentId,
        index_id: IndexId,
        identity: IndexIdentity,
        generation: IndexGenerationId,
        partition: work::TextPartition,
        blob: work::BlobRef,
        permit: blob_publication::BlobPublicationPermit,
        owner: work::TextUploadOwner,
        attachment: work::TextUploadAttachment,
    ) -> std::result::Result<Self, work::IndexWorkModelError> {
        let spec = work::TextUploadSpec::try_new(
            index_id, identity, generation, partition, blob, owner, attachment,
        )?;
        Ok(Self::from_spec(intent_id, permit, spec))
    }

    /// Combines a validated immutable specification with an opaque permit ID.
    pub(crate) fn from_spec(
        intent_id: TextUploadIntentId,
        permit: blob_publication::BlobPublicationPermit,
        spec: work::TextUploadSpec,
    ) -> Self {
        Self(
            work::TextUploadIntentValue::try_from_spec(
                intent_id,
                TextIntentRevision::initial(),
                permit.id(),
                spec,
                work::TextUploadPhase::Prepared,
                0,
                work::TextUploadWorkState::Queued {
                    not_before_unix_millis: None,
                },
            )
            .expect("validated upload specification accepts the closed initial outbox state"),
        )
    }

    /// Borrows the exact value encoded into the scoped intent row.
    pub(crate) const fn value(&self) -> &work::TextUploadIntentValue {
        &self.0
    }

    /// Reconstructs the exact coordinator permit from durable intent state.
    pub(crate) const fn permit(&self) -> blob_publication::BlobPublicationPermit {
        blob_publication::BlobPublicationPermit::from_id(self.0.publication_permit_id)
    }
}

/// Result of comparing one prepared triple with its durable UUID namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PreparedUploadStageOutcome {
    /// All three rows were newly staged in the caller's transaction.
    Staged,
    /// The exact complete triple was already durable and needed no writes.
    AlreadyDurable,
    /// The global intent UUID belongs to a different complete upload.
    IdentifierCollision,
}

/// Read-only classification of one complete prepared upload namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PreparedUploadObservation {
    /// None of the intent, pointer, or reachability rows exists.
    Absent,
    /// All three rows exactly match the prepared upload.
    Exact,
    /// The global intent UUID already belongs to another complete namespace.
    IdentifierCollision,
}

/// Encoded rows that form one indivisible prepared-upload outbox triple.
pub(super) struct PreparedUploadRows {
    pub(super) intent_key: Bytes,
    pub(super) intent_value: Bytes,
    pub(super) pointer_key: Bytes,
    pub(super) pointer_value: Bytes,
    pub(super) reachability_key: Bytes,
    pub(super) reachability_value: Bytes,
}

/// Exact serialized database work for one initially absent upload triple.
///
/// The values are measured from the same canonical V1 rows used by staging.
/// Private fields prevent callers from substituting estimated lengths into an
/// Active request preflight.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PreparedUploadMeasurements {
    input_bytes: u64,
    output_operations: u64,
    output_bytes: u64,
}

impl PreparedUploadMeasurements {
    /// Returns the three absent point-read key bytes.
    pub(super) const fn input_bytes(self) -> u64 {
        self.input_bytes
    }

    /// Returns the exact intent, pointer, and reachability write count.
    pub(super) const fn output_operations(self) -> u64 {
        self.output_operations
    }

    /// Returns the complete serialized bytes of those three writes.
    pub(super) const fn output_bytes(self) -> u64 {
        self.output_bytes
    }
}

/// Constructs the exact typed rows shared by staging and commit resolution.
fn prepared_upload_rows(
    scope: DataScope,
    prepared: &PreparedTextUploadIntent,
) -> Result<PreparedUploadRows> {
    upload_anchor_rows(scope, prepared.value())
}

/// Constructs the canonical intent, pointer, and intent-reachability rows.
///
/// Mutable upload phases change only the first two encoded values. The keys and
/// intent-owned reachability row remain fixed until a private attachment or
/// reclaim transition removes that anchor.
pub(super) fn upload_anchor_rows(
    scope: DataScope,
    intent: &work::TextUploadIntentValue,
) -> Result<PreparedUploadRows> {
    let intent_logical_key =
        index_keys::IndexV2Key::TextUploadIntent(index_keys::TextIntentOwnedKey {
            index_id: intent.index_id,
            generation: intent.generation,
            intent_id: intent.intent_id,
        });
    let intent_key = Key::Data {
        scope,
        kind: DataKeyKind::IndexV2(intent_logical_key.clone()),
    }
    .to_bytes();
    let intent_value = index_values::encode_work_value(
        &index_values::IndexV2WorkValue::TextUploadIntent(Box::new(intent.clone())),
    );

    let pointer_key = Key::Global {
        kind: GlobalKeyKind::IndexV2(index_keys::GlobalIndexV2Key::UploadPointer(
            intent.intent_id,
        )),
    }
    .to_bytes();
    let pointer_value = index_values::encode_metadata_value(
        &IndexV2MetadataValue::UploadQueuePointer(UploadQueuePointerValue {
            scope,
            index_id: intent.index_id,
            generation: intent.generation,
            record_revision: intent.revision,
        }),
    );

    let owner_logical_key = intent_logical_key.to_bytes();
    let reachability = work::BlobReachabilityReferenceValue::try_new(
        intent.blob,
        index_keys::BlobReferenceOwnerKind::UploadIntent,
        scope,
        owner_logical_key.clone(),
        UPLOAD_INTENT_REFERENCE_SLOT,
    )
    .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?;
    let reachability_key = Key::Global {
        kind: GlobalKeyKind::IndexV2(index_keys::GlobalIndexV2Key::BlobReachabilityReference(
            index_keys::BlobReferenceGlobalKey::try_new(
                index_keys::BlobHash::new(*intent.blob.hash()),
                index_keys::BlobReferenceOwnerKind::UploadIntent,
                scope,
                owner_logical_key,
                UPLOAD_INTENT_REFERENCE_SLOT,
            )
            .expect("fixed-width text upload intent keys satisfy the blob-owner key bound"),
        )),
    }
    .to_bytes();
    let reachability_value = index_values::encode_work_value(
        &index_values::IndexV2WorkValue::BlobReachabilityReference(reachability),
    );
    Ok(PreparedUploadRows {
        intent_key,
        intent_value,
        pointer_key,
        pointer_value,
        reachability_key,
        reachability_value,
    })
}

/// Measures one future prepared upload without allocating an ID or permit.
///
/// Intent and permit identifiers are fixed-width non-nil values. The sizing
/// representatives therefore produce the exact same row lengths as the real
/// reserved child while preserving the required order: measure first, reserve
/// second, then stage the independently committed outbox triple.
pub(super) fn measure_prepared_upload_spec(
    scope: DataScope,
    spec: &work::TextUploadSpec,
) -> Result<PreparedUploadMeasurements> {
    let intent_id = TextUploadIntentId::from_bytes([u8::MAX; 16])
        .expect("all-ones sizing upload intent ID is non-nil");
    let permit_id = super::super::BlobPublicationPermitId::from_bytes([u8::MAX; 16])
        .expect("all-ones sizing publication permit ID is non-nil");
    let prepared = PreparedTextUploadIntent::from_spec(
        intent_id,
        blob_publication::BlobPublicationPermit::from_id(permit_id),
        spec.clone(),
    );
    let rows = prepared_upload_rows(scope, &prepared)?;
    let input_bytes = [&rows.intent_key, &rows.pointer_key, &rows.reachability_key]
        .into_iter()
        .fold(0_u64, |bytes, key| {
            bytes.saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX))
        });
    let output_rows = [
        (&rows.intent_key, &rows.intent_value),
        (&rows.pointer_key, &rows.pointer_value),
        (&rows.reachability_key, &rows.reachability_value),
    ];
    let output_bytes = output_rows.iter().fold(0_u64, |bytes, (key, value)| {
        bytes
            .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX))
            .saturating_add(u64::try_from(value.len()).unwrap_or(u64::MAX))
    });
    Ok(PreparedUploadMeasurements {
        input_bytes,
        output_operations: u64::try_from(output_rows.len()).unwrap_or(u64::MAX),
        output_bytes,
    })
}

/// Point-reads the complete prepared triple without staging repairs.
///
/// Partial rows or a same-pointer namespace with disagreeing scoped values are
/// corruption. A different global pointer is a UUID collision and proves only
/// that this prepared child was not committed.
pub(crate) async fn observe_prepared_upload(
    reader: &(impl DbReadOps + Sync),
    scope: DataScope,
    prepared: &PreparedTextUploadIntent,
) -> Result<PreparedUploadObservation> {
    let rows = prepared_upload_rows(scope, prepared)?;
    let existing_intent = reader.get(&rows.intent_key).await?;
    let existing_pointer = reader.get(&rows.pointer_key).await?;
    let existing_reachability = reader.get(&rows.reachability_key).await?;
    if existing_pointer
        .as_deref()
        .is_some_and(|existing_pointer| existing_pointer != rows.pointer_value.as_ref())
    {
        return Ok(PreparedUploadObservation::IdentifierCollision);
    }
    match (
        existing_intent.as_deref(),
        existing_pointer.as_deref(),
        existing_reachability.as_deref(),
    ) {
        (None, None, None) => Ok(PreparedUploadObservation::Absent),
        (Some(existing_intent), Some(existing_pointer), Some(existing_reachability))
            if existing_intent == rows.intent_value.as_ref()
                && existing_pointer == rows.pointer_value.as_ref()
                && existing_reachability == rows.reachability_value.as_ref() =>
        {
            Ok(PreparedUploadObservation::Exact)
        }
        _ => Err(HelixDbError::IndexCatalogCorruption(
            "text upload intent, pointer, and live reference are partial or disagree".to_string(),
        )),
    }
}

/// Stages an intent, pointer, and live-reference row as one idempotent unit.
///
/// The caller owns the serializable transaction and commits it before calling
/// `BlobPublicationCoordinator::publish`. Replaying the exact already-durable
/// triple is a no-op; partial or disagreeing state is corruption and never
/// repaired by overwriting one row.
pub(crate) async fn stage_prepared_upload(
    transaction: &DbTransaction,
    scope: DataScope,
    prepared: &PreparedTextUploadIntent,
) -> Result<PreparedUploadStageOutcome> {
    let rows = prepared_upload_rows(scope, prepared)?;
    let existing_intent = transaction.get(&rows.intent_key).await?;
    let existing_pointer = transaction.get(&rows.pointer_key).await?;
    let existing_reachability = transaction.get(&rows.reachability_key).await?;
    if existing_pointer
        .as_deref()
        .is_some_and(|existing_pointer| existing_pointer != rows.pointer_value.as_ref())
    {
        return Ok(PreparedUploadStageOutcome::IdentifierCollision);
    }
    match (
        existing_intent.as_deref(),
        existing_pointer.as_deref(),
        existing_reachability.as_deref(),
    ) {
        (None, None, None) => {
            transaction.put(rows.intent_key, rows.intent_value)?;
            transaction.put(rows.pointer_key, rows.pointer_value)?;
            transaction.put(rows.reachability_key, rows.reachability_value)?;
            Ok(PreparedUploadStageOutcome::Staged)
        }
        (Some(existing_intent), Some(existing_pointer), Some(existing_reachability))
            if existing_intent == rows.intent_value.as_ref()
                && existing_pointer == rows.pointer_value.as_ref()
                && existing_reachability == rows.reachability_value.as_ref() =>
        {
            Ok(PreparedUploadStageOutcome::AlreadyDurable)
        }
        _ => Err(HelixDbError::IndexCatalogCorruption(
            "text upload intent, pointer, and live reference are partial or disagree".to_string(),
        )),
    }
}

/// Best-effort durable checkpoint after request-owned Active publication.
///
/// The exact prepared intent, pointer, and live-reference triple must still be
/// present. The request-specific model transition advances the intent and
/// pointer together without manufacturing a worker claim while the registered
/// request owner remains in flight. Replaying the exact uploaded revision is a
/// no-op; every partial or disagreeing shape fails closed.
pub(crate) async fn checkpoint_active_request_publication(
    db: &Db,
    scope: DataScope,
    prepared: &PreparedTextUploadIntent,
) -> Result<work::TextUploadIntentValue> {
    let current = prepared_upload_rows(scope, prepared)?;
    let next = prepared
        .value()
        .active_request_publication_succeeded()
        .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?;
    let next_intent_value = index_values::encode_work_value(
        &index_values::IndexV2WorkValue::TextUploadIntent(Box::new(next.clone())),
    );
    let next_pointer_value = index_values::encode_metadata_value(
        &IndexV2MetadataValue::UploadQueuePointer(UploadQueuePointerValue {
            scope,
            index_id: next.index_id,
            generation: next.generation,
            record_revision: next.revision,
        }),
    );
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let observed_intent = transaction.get(&current.intent_key).await?;
    let observed_pointer = transaction.get(&current.pointer_key).await?;
    let observed_reference = transaction.get(&current.reachability_key).await?;
    match (
        observed_intent.as_deref(),
        observed_pointer.as_deref(),
        observed_reference.as_deref(),
    ) {
        (Some(intent), Some(pointer), Some(reference))
            if intent == current.intent_value.as_ref()
                && pointer == current.pointer_value.as_ref()
                && reference == current.reachability_value.as_ref() =>
        {
            transaction.put(&current.intent_key, next_intent_value)?;
            transaction.put(&current.pointer_key, next_pointer_value)?;
        }
        (Some(intent), Some(pointer), Some(reference))
            if intent == next_intent_value.as_ref()
                && pointer == next_pointer_value.as_ref()
                && reference == current.reachability_value.as_ref() => {}
        _ => {
            return Err(HelixDbError::IndexCatalogCorruption(
                "Active text publication checkpoint no longer owns its exact upload triple"
                    .to_string(),
            ));
        }
    }
    transaction.commit().await?;
    Ok(next)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use sha2::{Digest, Sha256};
    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, IsolationLevel};

    use super::*;
    use crate::index_v2::blob_publication::BlobPublicationCoordinator;
    use crate::index_v2::{
        BlobPublicationPermitId, IndexComponent, IndexElementKind, IndexIdentityFamily,
        IndexOperationId, IndexOperationRevision, WriterEpoch,
    };

    async fn raw_db(name: &str) -> Db {
        Db::open(name, Arc::new(InMemory::new())).await.unwrap()
    }

    fn text_identity() -> IndexIdentity {
        IndexIdentity::new(
            IndexIdentityFamily::Text,
            IndexElementKind::Node,
            IndexComponent::try_new("label", "Document").unwrap(),
            IndexComponent::try_new("property", "body").unwrap(),
        )
    }

    fn split(payload: &[u8]) -> work::SplitRef {
        let blob = work::BlobRef::new(
            Sha256::digest(payload).into(),
            u64::try_from(payload.len()).unwrap(),
        );
        work::SplitRef::try_new(blob, 0, 0, 0, blob.size()).unwrap()
    }

    fn upload_spec(payload: &[u8]) -> work::TextUploadSpec {
        let split = split(payload);
        work::TextUploadSpec::try_new(
            IndexId::initial(),
            text_identity(),
            IndexGenerationId::initial(),
            work::TextPartition::Unpartitioned,
            split.blob(),
            work::TextUploadOwner::Build {
                operation_id: IndexOperationId::from_bytes([3; 16]).unwrap(),
                expected_operation_revision: IndexOperationRevision::initial(),
            },
            work::TextUploadAttachment::ManifestSplit(split),
        )
        .unwrap()
    }

    async fn reserved_prepared(
        coordinator: &blob_publication::ProcessLocalBlobPublicationCoordinator,
        intent_id: TextUploadIntentId,
    ) -> PreparedTextUploadIntent {
        let split = split(b"durable prepared upload");
        let permit = coordinator
            .reserve(
                split.blob(),
                intent_id,
                WriterEpoch::from_bytes([2; 16]).unwrap(),
            )
            .await
            .unwrap();
        PreparedTextUploadIntent::try_new(
            intent_id,
            IndexId::initial(),
            text_identity(),
            IndexGenerationId::initial(),
            work::TextPartition::Unpartitioned,
            split.blob(),
            permit,
            work::TextUploadOwner::Build {
                operation_id: IndexOperationId::from_bytes([3; 16]).unwrap(),
                expected_operation_revision: IndexOperationRevision::initial(),
            },
            work::TextUploadAttachment::ManifestSplit(split),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn prepared_intent_pointer_and_reference_commit_atomically_and_replay() {
        let db = raw_db("text-upload-prepare-atomic").await;
        let coordinator = blob_publication::ProcessLocalBlobPublicationCoordinator::new(
            Arc::new(InMemory::new()),
            "text-upload-prepare-atomic",
            blob_publication::BlobPublicationTiming::default(),
        );
        let intent_id = TextUploadIntentId::from_bytes([1; 16]).unwrap();
        let prepared = reserved_prepared(&coordinator, intent_id).await;
        assert_eq!(
            observe_prepared_upload(&db, DataScope::LegacyUnscoped, &prepared)
                .await
                .unwrap(),
            PreparedUploadObservation::Absent
        );
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert_eq!(
            stage_prepared_upload(&transaction, DataScope::LegacyUnscoped, &prepared)
                .await
                .unwrap(),
            PreparedUploadStageOutcome::Staged
        );
        assert_eq!(
            crate::index_v2::repository::load_upload_from_pointer(&db, intent_id)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            observe_prepared_upload(&db, DataScope::LegacyUnscoped, &prepared)
                .await
                .unwrap(),
            PreparedUploadObservation::Absent
        );
        transaction.commit().await.unwrap();
        assert_eq!(
            crate::index_v2::repository::load_upload_from_pointer(&db, intent_id)
                .await
                .unwrap(),
            Some(prepared.value().clone())
        );
        assert_eq!(
            observe_prepared_upload(&db, DataScope::LegacyUnscoped, &prepared)
                .await
                .unwrap(),
            PreparedUploadObservation::Exact
        );

        let replay = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert_eq!(
            stage_prepared_upload(&replay, DataScope::LegacyUnscoped, &prepared)
                .await
                .unwrap(),
            PreparedUploadStageOutcome::AlreadyDurable
        );
        replay.commit().await.unwrap();
    }

    #[tokio::test]
    async fn active_request_publication_checkpoint_replays_exactly_and_rejects_corruption() {
        let db = raw_db("active-text-upload-checkpoint").await;
        let coordinator = blob_publication::ProcessLocalBlobPublicationCoordinator::new(
            Arc::new(InMemory::new()),
            "active-text-upload-checkpoint",
            blob_publication::BlobPublicationTiming::default(),
        );
        let payload = b"request-owned publication checkpoint";
        let split = split(payload);
        let intent_id = TextUploadIntentId::from_bytes([0x73; 16]).unwrap();
        let writer_epoch = WriterEpoch::from_bytes([0x74; 16]).unwrap();
        let permit = coordinator
            .reserve(split.blob(), intent_id, writer_epoch)
            .await
            .unwrap();
        let prepared = PreparedTextUploadIntent::try_new(
            intent_id,
            IndexId::initial(),
            text_identity(),
            IndexGenerationId::initial(),
            work::TextPartition::Unpartitioned,
            split.blob(),
            permit,
            work::TextUploadOwner::ActiveMutation {
                writer_epoch,
                mutation_id: crate::index_v2::MutationId::from_bytes([0x75; 16]).unwrap(),
                active_record_revision: crate::index_v2::IndexRevision::initial(),
            },
            work::TextUploadAttachment::ManifestSplit(split),
        )
        .unwrap();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert_eq!(
            stage_prepared_upload(&transaction, DataScope::LegacyUnscoped, &prepared)
                .await
                .unwrap(),
            PreparedUploadStageOutcome::Staged
        );
        transaction.commit().await.unwrap();

        let uploaded =
            checkpoint_active_request_publication(&db, DataScope::LegacyUnscoped, &prepared)
                .await
                .unwrap();
        assert!(matches!(uploaded.phase, work::TextUploadPhase::Uploaded));
        assert_eq!(uploaded.revision.get(), 2);
        assert_eq!(
            checkpoint_active_request_publication(&db, DataScope::LegacyUnscoped, &prepared,)
                .await
                .unwrap(),
            uploaded
        );
        assert_eq!(
            crate::index_v2::repository::load_upload_from_pointer(&db, intent_id)
                .await
                .unwrap(),
            Some(uploaded)
        );

        let rows = prepared_upload_rows(DataScope::LegacyUnscoped, &prepared).unwrap();
        db.put(
            rows.reachability_key,
            Bytes::from_static(b"corrupt active upload reference"),
        )
        .await
        .unwrap();
        assert!(matches!(
            checkpoint_active_request_publication(&db, DataScope::LegacyUnscoped, &prepared,).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        db.close().await.unwrap();
    }

    #[test]
    fn prepared_upload_spec_measurement_matches_real_fixed_width_rows() {
        let scope = DataScope::LegacyUnscoped;
        let spec = upload_spec(b"exact prepared upload row measurement");
        let measured = measure_prepared_upload_spec(scope, &spec).unwrap();
        let prepared = PreparedTextUploadIntent::from_spec(
            TextUploadIntentId::from_bytes([0x71; 16]).unwrap(),
            blob_publication::BlobPublicationPermit::from_id(
                BlobPublicationPermitId::from_bytes([0x72; 16]).unwrap(),
            ),
            spec,
        );
        let rows = prepared_upload_rows(scope, &prepared).unwrap();
        assert_eq!(
            measured.input_bytes(),
            [&rows.intent_key, &rows.pointer_key, &rows.reachability_key]
                .into_iter()
                .map(|key| u64::try_from(key.len()).unwrap())
                .sum::<u64>()
        );
        assert_eq!(measured.output_operations(), 3);
        assert_eq!(
            measured.output_bytes(),
            [
                (&rows.intent_key, &rows.intent_value),
                (&rows.pointer_key, &rows.pointer_value),
                (&rows.reachability_key, &rows.reachability_value),
            ]
            .into_iter()
            .map(|(key, value)| u64::try_from(key.len() + value.len()).unwrap())
            .sum::<u64>()
        );
    }

    #[tokio::test]
    async fn global_uuid_collision_is_retryable_without_staging_partial_rows() {
        let db = raw_db("text-upload-prepare-collision").await;
        let coordinator = blob_publication::ProcessLocalBlobPublicationCoordinator::new(
            Arc::new(InMemory::new()),
            "text-upload-prepare-collision",
            blob_publication::BlobPublicationTiming::default(),
        );
        let intent_id = TextUploadIntentId::from_bytes([8; 16]).unwrap();
        let prepared = reserved_prepared(&coordinator, intent_id).await;
        let pointer_key = Key::Global {
            kind: GlobalKeyKind::IndexV2(index_keys::GlobalIndexV2Key::UploadPointer(intent_id)),
        }
        .to_bytes();
        db.put(
            pointer_key,
            index_values::encode_metadata_value(&IndexV2MetadataValue::UploadQueuePointer(
                UploadQueuePointerValue {
                    scope: DataScope::LegacyUnscoped,
                    index_id: IndexId::new(2).unwrap(),
                    generation: IndexGenerationId::initial(),
                    record_revision: TextIntentRevision::initial(),
                },
            )),
        )
        .await
        .unwrap();

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert_eq!(
            stage_prepared_upload(&transaction, DataScope::LegacyUnscoped, &prepared)
                .await
                .unwrap(),
            PreparedUploadStageOutcome::IdentifierCollision
        );
        assert_eq!(
            observe_prepared_upload(&db, DataScope::LegacyUnscoped, &prepared)
                .await
                .unwrap(),
            PreparedUploadObservation::IdentifierCollision
        );
    }

    #[tokio::test]
    async fn partial_existing_state_fails_closed_without_repair() {
        let db = raw_db("text-upload-prepare-partial").await;
        let coordinator = blob_publication::ProcessLocalBlobPublicationCoordinator::new(
            Arc::new(InMemory::new()),
            "text-upload-prepare-partial",
            blob_publication::BlobPublicationTiming::default(),
        );
        let intent_id = TextUploadIntentId::from_bytes([4; 16]).unwrap();
        let prepared = reserved_prepared(&coordinator, intent_id).await;
        let intent_key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::TextUploadIntent(
                index_keys::TextIntentOwnedKey {
                    index_id: prepared.value().index_id,
                    generation: prepared.value().generation,
                    intent_id,
                },
            )),
        }
        .to_bytes();
        db.put(intent_key, Bytes::from_static(b"partial"))
            .await
            .unwrap();

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            stage_prepared_upload(&transaction, DataScope::LegacyUnscoped, &prepared).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        assert!(matches!(
            observe_prepared_upload(&db, DataScope::LegacyUnscoped, &prepared).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
    }

    #[test]
    fn prepared_value_has_one_closed_initial_state_and_persisted_permit_id() {
        let permit_id = BlobPublicationPermitId::from_bytes([5; 16]).unwrap();
        let permit = blob_publication::BlobPublicationPermit::from_id(permit_id);
        let split = split(b"closed prepared state");
        let prepared = PreparedTextUploadIntent::try_new(
            TextUploadIntentId::from_bytes([6; 16]).unwrap(),
            IndexId::initial(),
            text_identity(),
            IndexGenerationId::initial(),
            work::TextPartition::Unpartitioned,
            split.blob(),
            permit,
            work::TextUploadOwner::Build {
                operation_id: IndexOperationId::from_bytes([7; 16]).unwrap(),
                expected_operation_revision: IndexOperationRevision::initial(),
            },
            work::TextUploadAttachment::ManifestSplit(split),
        )
        .unwrap();
        assert_eq!(prepared.value().publication_permit_id, permit_id);
        assert_eq!(prepared.value().revision, TextIntentRevision::initial());
        assert_eq!(prepared.value().phase, work::TextUploadPhase::Prepared);
        assert_eq!(prepared.value().attempt, 0);
        assert_eq!(
            prepared.value().work_state,
            work::TextUploadWorkState::Queued {
                not_before_unix_millis: None
            }
        );
    }

    #[test]
    fn prepared_constructor_rejects_invalid_spec_before_outbox_state_exists() {
        let declared_split = split(b"declared prepared upload blob");
        let different_split = split(b"different prepared upload attachment");
        let permit = blob_publication::BlobPublicationPermit::from_id(
            BlobPublicationPermitId::from_bytes([42; 16]).unwrap(),
        );
        assert!(matches!(
            PreparedTextUploadIntent::try_new(
                TextUploadIntentId::from_bytes([41; 16]).unwrap(),
                IndexId::initial(),
                text_identity(),
                IndexGenerationId::initial(),
                work::TextPartition::Unpartitioned,
                declared_split.blob(),
                permit,
                work::TextUploadOwner::Build {
                    operation_id: IndexOperationId::from_bytes([43; 16]).unwrap(),
                    expected_operation_revision: IndexOperationRevision::initial(),
                },
                work::TextUploadAttachment::ManifestSplit(different_split),
            ),
            Err(work::IndexWorkModelError::InvalidUploadState)
        ));
        assert!(matches!(
            PreparedTextUploadIntent::try_new(
                TextUploadIntentId::from_bytes([44; 16]).unwrap(),
                IndexId::initial(),
                IndexIdentity::new(
                    IndexIdentityFamily::Vector,
                    IndexElementKind::Node,
                    IndexComponent::try_new("label", "Document").unwrap(),
                    IndexComponent::try_new("property", "embedding").unwrap(),
                ),
                IndexGenerationId::initial(),
                work::TextPartition::Unpartitioned,
                declared_split.blob(),
                permit,
                work::TextUploadOwner::Build {
                    operation_id: IndexOperationId::from_bytes([45; 16]).unwrap(),
                    expected_operation_revision: IndexOperationRevision::initial(),
                },
                work::TextUploadAttachment::ManifestSplit(declared_split),
            ),
            Err(work::IndexWorkModelError::InvalidUploadState)
        ));
    }
}
