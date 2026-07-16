//! Coordinator-backed reconciliation for durable text-upload phases.
//!
//! This driver performs no database writes. It converts authoritative
//! coordinator state into a closed repository checkpoint and, when an uploaded
//! build artifact is ready for first attachment, returns a reference guard that
//! the repository retains through commit. `ReferenceCommitted` build and Active
//! owners share the same idempotent release step; their distinct repository
//! cleanup contracts validate the historical destination and optional proof.

use std::sync::Arc;

use async_trait::async_trait;

use crate::error::{HelixDbError, Result};

use super::super::{blob_publication, work};
use super::upload_queue::{PreparedTextUploadStep, TextUploadDriver};

/// Complete coordinator dependency used by the text upload worker lane.
pub(crate) struct CoordinatorTextUploadDriver {
    coordinator: Arc<dyn blob_publication::BlobPublicationCoordinator>,
}

impl CoordinatorTextUploadDriver {
    /// Binds one worker driver to the database identity's coordinator.
    pub(crate) fn new(coordinator: Arc<dyn blob_publication::BlobPublicationCoordinator>) -> Self {
        Self { coordinator }
    }
}

#[async_trait]
impl TextUploadDriver for CoordinatorTextUploadDriver {
    async fn prepare_step(
        &self,
        intent: &work::TextUploadIntentValue,
    ) -> Result<PreparedTextUploadStep> {
        let permit = blob_publication::BlobPublicationPermit::from_id(intent.publication_permit_id);
        match &intent.phase {
            work::TextUploadPhase::Prepared => {
                let status = self
                    .coordinator
                    .publication_status(&permit)
                    .await
                    .map_err(coordinator_error)?;
                match status {
                    blob_publication::BlobPublicationStatus::Succeeded(metadata)
                        if metadata.blob() == intent.blob =>
                    {
                        Ok(PreparedTextUploadStep::publication_succeeded())
                    }
                    blob_publication::BlobPublicationStatus::Succeeded(_)
                        if matches!(intent.owner, work::TextUploadOwner::Build { .. }) =>
                    {
                        Ok(PreparedTextUploadStep::blob_mismatch())
                    }
                    blob_publication::BlobPublicationStatus::Succeeded(_) => {
                        Ok(PreparedTextUploadStep::transient_failure())
                    }
                    blob_publication::BlobPublicationStatus::Reserved
                    | blob_publication::BlobPublicationStatus::InFlight => {
                        Ok(PreparedTextUploadStep::transient_failure())
                    }
                    blob_publication::BlobPublicationStatus::DefinitivelyFailed
                    | blob_publication::BlobPublicationStatus::ExpiredUnused => {
                        match self.coordinator.validate_reference(intent.blob).await {
                            Ok(guard) if guard.blob() == intent.blob => {
                                Ok(PreparedTextUploadStep::shared_blob_reclaimable(guard))
                            }
                            Ok(_)
                                if matches!(intent.owner, work::TextUploadOwner::Build { .. }) =>
                            {
                                Ok(PreparedTextUploadStep::blob_mismatch())
                            }
                            Ok(_) => Ok(PreparedTextUploadStep::transient_failure()),
                            Err(blob_publication::BlobPublicationError::ReferenceAbsent) => {
                                Ok(PreparedTextUploadStep::non_publication_proven())
                            }
                            Err(blob_publication::BlobPublicationError::ReferenceMismatch)
                                if matches!(intent.owner, work::TextUploadOwner::Build { .. }) =>
                            {
                                Ok(PreparedTextUploadStep::blob_mismatch())
                            }
                            Err(blob_publication::BlobPublicationError::ReferenceMismatch) => {
                                Ok(PreparedTextUploadStep::transient_failure())
                            }
                            Err(error) => Err(coordinator_error(error)),
                        }
                    }
                }
            }
            work::TextUploadPhase::NonPublicationProven => {
                self.coordinator
                    .release(
                        &permit,
                        blob_publication::BlobPermitReleaseAuthority::definitive_non_publication(
                            permit.id(),
                        ),
                    )
                    .await
                    .map_err(coordinator_error)?;
                Ok(PreparedTextUploadStep::non_publication_released())
            }
            work::TextUploadPhase::Uploaded
                if matches!(
                    (&intent.owner, &intent.attachment),
                    (
                        work::TextUploadOwner::Build { .. },
                        work::TextUploadAttachment::BuildArtifact { .. }
                    )
                ) =>
            {
                let guard = self
                    .coordinator
                    .validate_reference(intent.blob)
                    .await
                    .map_err(coordinator_error)?;
                if guard.blob() != intent.blob {
                    return Err(corruption(
                        "coordinator reference guard names a different text blob",
                    ));
                }
                Ok(PreparedTextUploadStep::attach_uploaded(guard))
            }
            work::TextUploadPhase::Uploaded
                if matches!(intent.owner, work::TextUploadOwner::ActiveMutation { .. }) =>
            {
                let guard = self
                    .coordinator
                    .validate_reference(intent.blob)
                    .await
                    .map_err(coordinator_error)?;
                if guard.blob() != intent.blob {
                    return Err(corruption(
                        "coordinator Active reference guard names a different text blob",
                    ));
                }
                Ok(PreparedTextUploadStep::resolve_active_reference(guard))
            }
            work::TextUploadPhase::ReferenceCommitted(_) => {
                self.coordinator
                    .release(
                        &permit,
                        blob_publication::BlobPermitReleaseAuthority::reference_committed(
                            permit.id(),
                        ),
                    )
                    .await
                    .map_err(coordinator_error)?;
                Ok(PreparedTextUploadStep::reference_released())
            }
            work::TextUploadPhase::Reclaimable(work::ReclaimAssignment::Unassigned) => {
                Ok(PreparedTextUploadStep::assign_reclaim_root())
            }
            work::TextUploadPhase::Reclaimable(work::ReclaimAssignment::Assigned(run_id)) => {
                let key = blob_publication::BlobDeleteFenceKey::new(intent.blob, *run_id);
                let fence = match self.coordinator.begin_delete(key).await {
                    Ok(blob_publication::BeginBlobDelete::Acquired(fence))
                    | Ok(blob_publication::BeginBlobDelete::AlreadyHeldSameRun(fence)) => fence,
                    Ok(blob_publication::BeginBlobDelete::BusyOtherRun) => {
                        return Ok(PreparedTextUploadStep::transient_failure());
                    }
                    Err(error) => return Err(coordinator_error(error)),
                };
                if !self
                    .coordinator
                    .check_quiescent(&fence)
                    .await
                    .map_err(coordinator_error)?
                {
                    return Ok(PreparedTextUploadStep::transient_failure());
                }
                Ok(PreparedTextUploadStep::reclaim_fence_revalidated())
            }
            work::TextUploadPhase::Uploaded => Ok(PreparedTextUploadStep::transient_failure()),
        }
    }
}

fn coordinator_error(error: blob_publication::BlobPublicationError) -> HelixDbError {
    HelixDbError::InvariantViolation(format!(
        "text blob publication reconciliation failed: {error}"
    ))
}

fn corruption(reason: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(reason.into())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use sha2::{Digest, Sha256};
    use slatedb::object_store::{memory::InMemory, ObjectStore, ObjectStoreExt, PutPayload};
    use slatedb::{Db, IsolationLevel};

    use super::*;
    use crate::config::TextIndexDefinition;
    use crate::encoding::v1::keys::index_v2 as index_keys;
    use crate::encoding::v1::keys::tenant::{DataScope, TenantId};
    use crate::encoding::v1::keys::{DataKeyKind, GlobalKeyKind, Key};
    use crate::encoding::v1::values::index_v2 as index_values;
    use crate::index_v2::blob_publication::BlobPublicationCoordinator;
    use crate::index_v2::outbox::{self, ClaimPermission, ExpectedCanonicalRevision};
    use crate::index_v2::text::upload::{
        stage_prepared_upload, PreparedTextUploadIntent, PreparedUploadStageOutcome,
    };
    use crate::index_v2::text::upload_queue::{
        self, ClaimedUpload, TextUploadStepResult, UploadPointerObservation,
    };
    use crate::index_v2::{
        ClaimSequence, IndexGenerationId, IndexId, IndexOperationExecutionState,
        IndexOperationFamily, IndexOperationId, IndexOperationKind, IndexOperationProgress,
        IndexOperationRecord, IndexOperationRevision, IndexRecordV2, IndexRevision,
        IndexStateTransition, OperationCounters, PhysicalGeneration, PrefixScanProgress,
        TextBuildProgress, TextBuildStage, TextPartition, TextUploadIntentId,
        ValidatedDynamicIndexDefinition, WriterEpoch,
    };

    /// Claims the current exact upload revision at an elapsed retry deadline.
    async fn claim(
        db: &Db,
        intent_id: TextUploadIntentId,
        writer_epoch: WriterEpoch,
        sequence: u64,
    ) -> ClaimedUpload {
        let active_mutations =
            crate::index_v2::text::active_mutation::ActiveTextMutationRegistry::new();
        let UploadPointerObservation::Eligible(eligible) = upload_queue::observe_upload_pointer(
            db,
            intent_id,
            &active_mutations,
            writer_epoch,
            u64::MAX,
        )
        .await
        .unwrap() else {
            panic!("queued upload is eligible");
        };
        upload_queue::claim_upload(
            db,
            &eligible,
            &active_mutations,
            writer_epoch,
            ClaimSequence::new(sequence).unwrap(),
            u64::MAX,
            ClaimPermission::Normal,
        )
        .await
        .unwrap()
        .expect("exact upload claim commits")
    }

    /// Constructs a global reachability key from one typed owner key.
    fn reachability_key(
        blob: work::BlobRef,
        owner_kind: index_keys::BlobReferenceOwnerKind,
        scope: DataScope,
        owner_logical_key: Bytes,
    ) -> Bytes {
        Key::Global {
            kind: GlobalKeyKind::IndexV2(index_keys::GlobalIndexV2Key::BlobReachabilityReference(
                index_keys::BlobReferenceGlobalKey::try_new(
                    index_keys::BlobHash::new(*blob.hash()),
                    owner_kind,
                    scope,
                    owner_logical_key,
                    0,
                )
                .unwrap(),
            )),
        }
        .to_bytes()
    }

    /// Complete staged build upload used by terminal reconciliation tests.
    struct BuildUploadFixture {
        db: Db,
        store: Arc<dyn ObjectStore>,
        db_path: &'static str,
        coordinator: Arc<blob_publication::ProcessLocalBlobPublicationCoordinator>,
        driver: CoordinatorTextUploadDriver,
        scope: DataScope,
        index: IndexRecordV2,
        operation_id: IndexOperationId,
        intent_id: TextUploadIntentId,
        writer_epoch: WriterEpoch,
        permit: blob_publication::BlobPublicationPermit,
        payload: Bytes,
        blob: work::BlobRef,
    }

    /// Complete staged Active upload used by prior-writer terminal recovery tests.
    struct ActiveUploadFixture {
        db: Db,
        store: Arc<dyn ObjectStore>,
        db_path: &'static str,
        coordinator: Arc<blob_publication::ProcessLocalBlobPublicationCoordinator>,
        driver: CoordinatorTextUploadDriver,
        scope: DataScope,
        index_id: IndexId,
        generation: IndexGenerationId,
        intent_id: TextUploadIntentId,
        owner_epoch: WriterEpoch,
        worker_epoch: WriterEpoch,
        permit: blob_publication::BlobPublicationPermit,
        payload: Bytes,
        blob: work::BlobRef,
    }

    /// Stages one exact `Prepared` build intent without performing object I/O.
    async fn staged_build_upload(
        db_path: &'static str,
        seed: u8,
        payload: Bytes,
    ) -> BuildUploadFixture {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::builder(db_path, Arc::clone(&store))
            .build()
            .await
            .unwrap();
        let coordinator = Arc::new(
            blob_publication::ProcessLocalBlobPublicationCoordinator::new(
                Arc::clone(&store),
                db_path,
                blob_publication::BlobPublicationTiming::default(),
            ),
        );
        let coordinator_dependency: Arc<dyn blob_publication::BlobPublicationCoordinator> =
            coordinator.clone();
        let driver = CoordinatorTextUploadDriver::new(coordinator_dependency);
        let scope = DataScope::Tenant(TenantId::from_u128(u128::from(seed)));
        let operation_id = IndexOperationId::from_bytes([seed; 16]).unwrap();
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
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .unwrap();
        outbox::enqueue_operation(
            &db,
            scope,
            ExpectedCanonicalRevision::Absent,
            &index,
            &operation,
        )
        .await
        .unwrap();

        let blob = work::BlobRef::new(
            Sha256::digest(&payload).into(),
            u64::try_from(payload.len()).unwrap(),
        );
        let split = work::SplitRef::try_new(blob, 0, 0, 0, blob.size()).unwrap();
        let intent_id = TextUploadIntentId::from_bytes([seed.saturating_add(1); 16]).unwrap();
        let writer_epoch = WriterEpoch::from_bytes([seed.saturating_add(2); 16]).unwrap();
        let permit = coordinator
            .reserve(blob, intent_id, writer_epoch)
            .await
            .unwrap();
        let prepared = PreparedTextUploadIntent::try_new(
            intent_id,
            index.index_id(),
            index.identity().clone(),
            index.state().generation(),
            TextPartition::Unpartitioned,
            blob,
            permit,
            work::TextUploadOwner::Build {
                operation_id,
                expected_operation_revision: operation.operation_revision(),
            },
            work::TextUploadAttachment::BuildArtifact {
                artifact_ordinal: 1,
                split,
            },
        )
        .unwrap();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert_eq!(
            stage_prepared_upload(&transaction, scope, &prepared)
                .await
                .unwrap(),
            PreparedUploadStageOutcome::Staged
        );
        transaction.commit().await.unwrap();
        BuildUploadFixture {
            db,
            store,
            db_path,
            coordinator,
            driver,
            scope,
            index,
            operation_id,
            intent_id,
            writer_epoch,
            permit,
            payload,
            blob,
        }
    }

    /// Stages one exact `Prepared` Active intent without registering its old owner locally.
    async fn staged_active_upload(
        db_path: &'static str,
        seed: u8,
        payload: Bytes,
    ) -> ActiveUploadFixture {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::builder(db_path, Arc::clone(&store))
            .build()
            .await
            .unwrap();
        let coordinator = Arc::new(
            blob_publication::ProcessLocalBlobPublicationCoordinator::new(
                Arc::clone(&store),
                db_path,
                blob_publication::BlobPublicationTiming::default(),
            ),
        );
        let coordinator_dependency: Arc<dyn blob_publication::BlobPublicationCoordinator> =
            coordinator.clone();
        let driver = CoordinatorTextUploadDriver::new(coordinator_dependency);
        let scope = DataScope::Tenant(TenantId::from_u128(u128::from(seed)));
        let index_id = IndexId::initial();
        let generation = IndexGenerationId::initial();
        let definition = ValidatedDynamicIndexDefinition::try_from(
            TextIndexDefinition::new_node("Document", "body").unwrap(),
        )
        .unwrap();
        let active = IndexRecordV2::building(
            index_id,
            definition,
            IndexRevision::initial(),
            PhysicalGeneration::Text { generation },
            IndexOperationId::from_bytes([seed.saturating_add(5); 16]).unwrap(),
        )
        .unwrap()
        .transition(IndexStateTransition::Activate)
        .unwrap();
        db.put(
            Key::Data {
                scope,
                kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::index_record(
                    active.identity().clone(),
                )),
            }
            .to_bytes(),
            index_values::encode_index_record(&active),
        )
        .await
        .unwrap();
        let blob = work::BlobRef::new(
            Sha256::digest(&payload).into(),
            u64::try_from(payload.len()).unwrap(),
        );
        let split = work::SplitRef::try_new(blob, 0, 0, 0, blob.size()).unwrap();
        let intent_id = TextUploadIntentId::from_bytes([seed.saturating_add(1); 16]).unwrap();
        let owner_epoch = WriterEpoch::from_bytes([seed.saturating_add(2); 16]).unwrap();
        let worker_epoch = WriterEpoch::from_bytes([seed.saturating_add(3); 16]).unwrap();
        let permit = coordinator
            .reserve(blob, intent_id, owner_epoch)
            .await
            .unwrap();
        let prepared = PreparedTextUploadIntent::try_new(
            intent_id,
            index_id,
            active.identity().clone(),
            generation,
            TextPartition::Unpartitioned,
            blob,
            permit,
            work::TextUploadOwner::ActiveMutation {
                writer_epoch: owner_epoch,
                mutation_id: crate::index_v2::MutationId::from_bytes([seed.saturating_add(4); 16])
                    .unwrap(),
                active_record_revision: active.revision(),
            },
            work::TextUploadAttachment::ManifestSplit(split),
        )
        .unwrap();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert_eq!(
            stage_prepared_upload(&transaction, scope, &prepared)
                .await
                .unwrap(),
            PreparedUploadStageOutcome::Staged
        );
        transaction.commit().await.unwrap();
        ActiveUploadFixture {
            db,
            store,
            db_path,
            coordinator,
            driver,
            scope,
            index_id,
            generation,
            intent_id,
            owner_epoch,
            worker_epoch,
            permit,
            payload,
            blob,
        }
    }

    /// Injects mismatched object bytes and commits the resulting coupled block.
    ///
    /// Direct object-store access is deliberately confined to this corruption
    /// fixture; production object writes remain publication-coordinator owned.
    async fn inject_and_block_blob_mismatch(fixture: &BuildUploadFixture) {
        assert!(matches!(
            fixture
                .coordinator
                .publish(&fixture.permit, Bytes::from_static(b"wrong payload"))
                .await,
            Err(blob_publication::BlobPublicationError::PayloadMismatch)
        ));
        let location =
            crate::search::text::blob_object_store_path(fixture.db_path, *fixture.blob.hash());
        fixture
            .store
            .put(
                &location,
                PutPayload::from_bytes(Bytes::from_static(b"corrupt object bytes")),
            )
            .await
            .unwrap();
        let mismatch = claim(&fixture.db, fixture.intent_id, fixture.writer_epoch, 1).await;
        assert_eq!(
            upload_queue::execute_claimed_upload_step(
                &fixture.db,
                &mismatch,
                &fixture.driver,
                u64::MAX,
            )
            .await
            .unwrap(),
            TextUploadStepResult::BlobMismatch
        );
    }

    #[tokio::test]
    async fn build_artifact_reconciles_reserved_publish_attach_release_and_retry() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db_path = "text-upload-build-artifact-reconciliation";
        let db = Db::builder(db_path, Arc::clone(&store))
            .build()
            .await
            .unwrap();
        let coordinator = Arc::new(
            blob_publication::ProcessLocalBlobPublicationCoordinator::new(
                Arc::clone(&store),
                db_path,
                blob_publication::BlobPublicationTiming::default(),
            ),
        );
        let coordinator_dependency: Arc<dyn blob_publication::BlobPublicationCoordinator> =
            coordinator.clone();
        let driver = CoordinatorTextUploadDriver::new(coordinator_dependency);
        let scope = DataScope::Tenant(TenantId::from_u128(41));
        let operation_id = IndexOperationId::from_bytes([42; 16]).unwrap();
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
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .unwrap();
        outbox::enqueue_operation(
            &db,
            scope,
            ExpectedCanonicalRevision::Absent,
            &index,
            &operation,
        )
        .await
        .unwrap();

        let payload = Bytes::from_static(b"durable build artifact");
        let blob = work::BlobRef::new(
            Sha256::digest(&payload).into(),
            u64::try_from(payload.len()).unwrap(),
        );
        let split = work::SplitRef::try_new(blob, 0, 0, 0, blob.size()).unwrap();
        let intent_id = TextUploadIntentId::from_bytes([43; 16]).unwrap();
        let writer_epoch = WriterEpoch::from_bytes([44; 16]).unwrap();
        let permit = coordinator
            .reserve(blob, intent_id, writer_epoch)
            .await
            .unwrap();
        let prepared = PreparedTextUploadIntent::try_new(
            intent_id,
            index.index_id(),
            index.identity().clone(),
            index.state().generation(),
            TextPartition::Unpartitioned,
            blob,
            permit,
            work::TextUploadOwner::Build {
                operation_id,
                expected_operation_revision: operation.operation_revision(),
            },
            work::TextUploadAttachment::BuildArtifact {
                artifact_ordinal: 7,
                split,
            },
        )
        .unwrap();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert_eq!(
            stage_prepared_upload(&transaction, scope, &prepared)
                .await
                .unwrap(),
            PreparedUploadStageOutcome::Staged
        );
        transaction.commit().await.unwrap();

        let reserved = claim(&db, intent_id, writer_epoch, 1).await;
        assert_eq!(
            upload_queue::execute_claimed_upload_step(&db, &reserved, &driver, u64::MAX,)
                .await
                .unwrap(),
            TextUploadStepResult::TransientFailure
        );
        assert!(matches!(
            crate::index_v2::repository::load_upload_from_pointer(&db, intent_id)
                .await
                .unwrap()
                .unwrap()
                .phase,
            work::TextUploadPhase::Prepared
        ));

        assert!(matches!(
            coordinator.publish(&permit, payload).await.unwrap(),
            blob_publication::BlobPublicationStatus::Succeeded(_)
        ));
        let mut mismatched = crate::index_v2::repository::load_upload_from_pointer(&db, intent_id)
            .await
            .unwrap()
            .unwrap();
        mismatched.blob = work::BlobRef::new([99; 32], mismatched.blob.size());
        assert_eq!(
            driver.prepare_step(&mismatched).await.unwrap().outcome(),
            TextUploadStepResult::BlobMismatch
        );
        let published = claim(&db, intent_id, writer_epoch, 2).await;
        assert_eq!(
            upload_queue::execute_claimed_upload_step(&db, &published, &driver, u64::MAX,)
                .await
                .unwrap(),
            TextUploadStepResult::PublicationSucceeded
        );

        let uploaded = claim(&db, intent_id, writer_epoch, 3).await;
        assert_eq!(
            upload_queue::execute_claimed_upload_step(&db, &uploaded, &driver, u64::MAX,)
                .await
                .unwrap(),
            TextUploadStepResult::AttachUploaded
        );
        let referenced = crate::index_v2::repository::load_upload_from_pointer(&db, intent_id)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            referenced.phase,
            work::TextUploadPhase::ReferenceCommitted(_)
        ));

        let artifact_logical_key =
            index_keys::IndexV2Key::TextBuildArtifact(index_keys::TextBuildArtifactKey {
                root: index_keys::TextManifestRootKey {
                    index_id: index.index_id(),
                    generation: index.state().generation(),
                    partition: TextPartition::Unpartitioned.fingerprint(),
                },
                ordinal: 7,
            });
        let artifact_key = Key::Data {
            scope,
            kind: DataKeyKind::IndexV2(artifact_logical_key.clone()),
        }
        .to_bytes();
        let artifact_value = db.get(&artifact_key).await.unwrap().unwrap();
        assert!(matches!(
            index_values::decode_work_value(&artifact_value).unwrap(),
            index_values::IndexV2WorkValue::TextBuildArtifact(artifact)
                if artifact.source_intent_id == intent_id && artifact.split == split
        ));
        let artifact_reference_key = reachability_key(
            blob,
            index_keys::BlobReferenceOwnerKind::BuildArtifact,
            scope,
            artifact_logical_key.to_bytes(),
        );
        assert!(db.get(&artifact_reference_key).await.unwrap().is_some());
        let intent_reference_key = reachability_key(
            blob,
            index_keys::BlobReferenceOwnerKind::UploadIntent,
            scope,
            index_keys::IndexV2Key::TextUploadIntent(index_keys::TextIntentOwnedKey {
                index_id: index.index_id(),
                generation: index.state().generation(),
                intent_id,
            })
            .to_bytes(),
        );
        assert!(db.get(intent_reference_key).await.unwrap().is_none());

        coordinator.fail_next_release();
        let release_outage = claim(&db, intent_id, writer_epoch, 4).await;
        assert_eq!(
            upload_queue::execute_claimed_upload_step(&db, &release_outage, &driver, u64::MAX,)
                .await
                .unwrap(),
            TextUploadStepResult::TransientFailure
        );
        let release_retry = claim(&db, intent_id, writer_epoch, 5).await;
        assert_eq!(
            upload_queue::execute_claimed_upload_step(&db, &release_retry, &driver, u64::MAX,)
                .await
                .unwrap(),
            TextUploadStepResult::ReferenceReleased
        );
        assert!(
            crate::index_v2::repository::load_upload_from_pointer(&db, intent_id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            coordinator.publication_status(&permit).await,
            Err(blob_publication::BlobPublicationError::UnknownPermit)
        ));
        assert!(db.get(artifact_key).await.unwrap().is_some());
        assert!(db.get(artifact_reference_key).await.unwrap().is_some());
        assert!(matches!(
            outbox::observe_operation_pointer(&db, operation_id, writer_epoch, u64::MAX)
                .await
                .unwrap(),
            outbox::OperationPointerObservation::Eligible(_)
        ));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn blob_mismatch_blocks_upload_and_build_then_retry_requeues_exact_pair() {
        let fixture = staged_build_upload(
            "text-upload-blob-mismatch-retry",
            51,
            Bytes::from_static(b"declared build artifact"),
        )
        .await;
        inject_and_block_blob_mismatch(&fixture).await;

        let scoped_intent_key = Key::Data {
            scope: fixture.scope,
            kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::TextUploadIntent(
                index_keys::TextIntentOwnedKey {
                    index_id: fixture.index.index_id(),
                    generation: fixture.index.state().generation(),
                    intent_id: fixture.intent_id,
                },
            )),
        }
        .to_bytes();
        let global_upload_pointer_key = Key::Global {
            kind: GlobalKeyKind::IndexV2(index_keys::GlobalIndexV2Key::UploadPointer(
                fixture.intent_id,
            )),
        }
        .to_bytes();
        let global_operation_pointer_key = Key::Global {
            kind: GlobalKeyKind::IndexV2(index_keys::GlobalIndexV2Key::OperationPointer(
                fixture.operation_id,
            )),
        }
        .to_bytes();
        assert!(fixture
            .db
            .get(&global_upload_pointer_key)
            .await
            .unwrap()
            .is_none());
        assert!(fixture
            .db
            .get(&global_operation_pointer_key)
            .await
            .unwrap()
            .is_none());
        let blocked_operation =
            outbox::read_operation(&fixture.db, fixture.scope, fixture.operation_id)
                .await
                .unwrap()
                .unwrap();
        assert!(matches!(
            blocked_operation.execution_state(),
            IndexOperationExecutionState::Blocked(
                crate::index_v2::IndexOperationBlocker::BlobPublicationMismatch { intent_id }
            ) if *intent_id == fixture.intent_id
        ));
        let blocked_upload_value = fixture.db.get(&scoped_intent_key).await.unwrap().unwrap();
        let index_values::IndexV2WorkValue::TextUploadIntent(blocked_upload) =
            index_values::decode_work_value(&blocked_upload_value).unwrap()
        else {
            panic!("scoped upload key contains an upload intent");
        };
        assert!(matches!(
            blocked_upload.work_state,
            work::TextUploadWorkState::Blocked(
                crate::index_v2::IndexOperationBlocker::BlobPublicationMismatch { intent_id }
            ) if intent_id == fixture.intent_id
        ));
        assert!(matches!(
            blocked_upload.owner,
            work::TextUploadOwner::Build {
                operation_id,
                expected_operation_revision,
            } if operation_id == fixture.operation_id
                && expected_operation_revision == blocked_operation.operation_revision()
        ));

        let retried = outbox::retry_operation(&fixture.db, fixture.scope, fixture.operation_id)
            .await
            .unwrap();
        assert!(matches!(
            retried.execution_state(),
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None
            }
        ));
        assert!(fixture
            .db
            .get(&global_upload_pointer_key)
            .await
            .unwrap()
            .is_some());
        assert!(fixture
            .db
            .get(&global_operation_pointer_key)
            .await
            .unwrap()
            .is_some());
        let retried_upload =
            crate::index_v2::repository::load_upload_from_pointer(&fixture.db, fixture.intent_id)
                .await
                .unwrap()
                .unwrap();
        assert!(matches!(
            retried_upload.work_state,
            work::TextUploadWorkState::Queued {
                not_before_unix_millis: None
            }
        ));
        assert!(matches!(
            retried_upload.owner,
            work::TextUploadOwner::Build {
                operation_id,
                expected_operation_revision,
            } if operation_id == fixture.operation_id
                && expected_operation_revision == retried.operation_revision()
        ));
        let converged = outbox::retry_operation(&fixture.db, fixture.scope, fixture.operation_id)
            .await
            .unwrap();
        assert_eq!(converged, retried);
        assert!(matches!(
            fixture
                .coordinator
                .publication_status(&fixture.permit)
                .await
                .unwrap(),
            blob_publication::BlobPublicationStatus::DefinitivelyFailed
        ));
        fixture.db.close().await.unwrap();
    }

    #[tokio::test]
    async fn blob_mismatch_retry_is_atomic_when_coupled_upload_is_missing() {
        let fixture = staged_build_upload(
            "text-upload-blob-mismatch-missing-coupling",
            56,
            Bytes::from_static(b"missing coupled build artifact"),
        )
        .await;
        inject_and_block_blob_mismatch(&fixture).await;

        let scoped_intent_key = Key::Data {
            scope: fixture.scope,
            kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::TextUploadIntent(
                index_keys::TextIntentOwnedKey {
                    index_id: fixture.index.index_id(),
                    generation: fixture.index.state().generation(),
                    intent_id: fixture.intent_id,
                },
            )),
        }
        .to_bytes();
        let transaction = fixture
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        // Test-only corruption injection used to prove the retry transaction
        // cannot make just the operation runnable.
        transaction.delete(scoped_intent_key).unwrap();
        transaction.commit().await.unwrap();
        assert!(matches!(
            outbox::retry_operation(&fixture.db, fixture.scope, fixture.operation_id).await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("no coupled upload intent")
        ));
        let still_blocked =
            outbox::read_operation(&fixture.db, fixture.scope, fixture.operation_id)
                .await
                .unwrap()
                .unwrap();
        assert!(matches!(
            still_blocked.execution_state(),
            IndexOperationExecutionState::Blocked(
                crate::index_v2::IndexOperationBlocker::BlobPublicationMismatch { intent_id }
            ) if *intent_id == fixture.intent_id
        ));
        let global_operation_pointer_key = Key::Global {
            kind: GlobalKeyKind::IndexV2(index_keys::GlobalIndexV2Key::OperationPointer(
                fixture.operation_id,
            )),
        }
        .to_bytes();
        assert!(fixture
            .db
            .get(global_operation_pointer_key)
            .await
            .unwrap()
            .is_none());
        fixture.db.close().await.unwrap();
    }

    #[tokio::test]
    async fn blob_mismatch_retry_rejects_checkpoint_drift_and_retained_pointer() {
        let fixture = staged_build_upload(
            "text-upload-blob-mismatch-negative-contracts",
            58,
            Bytes::from_static(b"drifting coupled build artifact"),
        )
        .await;
        inject_and_block_blob_mismatch(&fixture).await;

        let blocked_operation =
            outbox::read_operation(&fixture.db, fixture.scope, fixture.operation_id)
                .await
                .unwrap()
                .unwrap();
        let transaction = fixture
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            upload_queue::stage_blob_mismatch_retry(
                &transaction,
                fixture.scope,
                &blocked_operation,
                &blocked_operation,
                fixture.intent_id,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("different operation checkpoint")
        ));
        drop(transaction);

        let scoped_intent_key = Key::Data {
            scope: fixture.scope,
            kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::TextUploadIntent(
                index_keys::TextIntentOwnedKey {
                    index_id: fixture.index.index_id(),
                    generation: fixture.index.state().generation(),
                    intent_id: fixture.intent_id,
                },
            )),
        }
        .to_bytes();
        let original_upload_value = fixture.db.get(&scoped_intent_key).await.unwrap().unwrap();
        let index_values::IndexV2WorkValue::TextUploadIntent(blocked_upload) =
            index_values::decode_work_value(&original_upload_value).unwrap()
        else {
            panic!("scoped upload key contains an upload intent");
        };
        let blocked_upload_revision = blocked_upload.revision;
        let mut drifted_upload = *blocked_upload;
        drifted_upload.owner = work::TextUploadOwner::Build {
            operation_id: fixture.operation_id,
            expected_operation_revision: IndexOperationRevision::initial(),
        };
        fixture
            .db
            .put(
                scoped_intent_key.clone(),
                index_values::encode_work_value(&index_values::IndexV2WorkValue::TextUploadIntent(
                    Box::new(drifted_upload),
                )),
            )
            .await
            .unwrap();
        assert!(matches!(
            outbox::retry_operation(&fixture.db, fixture.scope, fixture.operation_id).await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("operation and upload blockers disagree")
        ));
        fixture
            .db
            .put(scoped_intent_key, original_upload_value)
            .await
            .unwrap();

        let global_upload_pointer_key = Key::Global {
            kind: GlobalKeyKind::IndexV2(index_keys::GlobalIndexV2Key::UploadPointer(
                fixture.intent_id,
            )),
        }
        .to_bytes();
        fixture
            .db
            .put(
                global_upload_pointer_key.clone(),
                index_values::encode_metadata_value(
                    &crate::index_v2::IndexV2MetadataValue::UploadQueuePointer(
                        crate::index_v2::UploadQueuePointerValue {
                            scope: fixture.scope,
                            index_id: fixture.index.index_id(),
                            generation: fixture.index.state().generation(),
                            record_revision: blocked_upload_revision,
                        },
                    ),
                ),
            )
            .await
            .unwrap();
        assert!(matches!(
            outbox::retry_operation(&fixture.db, fixture.scope, fixture.operation_id).await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("retained a runnable pointer")
        ));
        let still_blocked =
            outbox::read_operation(&fixture.db, fixture.scope, fixture.operation_id)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(still_blocked, blocked_operation);
        assert!(fixture
            .db
            .get(&global_upload_pointer_key)
            .await
            .unwrap()
            .is_some());
        let transaction = fixture
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        transaction.delete(global_upload_pointer_key).unwrap();
        transaction.commit().await.unwrap();
        fixture.db.close().await.unwrap();
    }

    #[tokio::test]
    async fn terminal_absence_is_durable_before_release_and_replays_after_release_crash() {
        let fixture = staged_build_upload(
            "text-upload-terminal-absence-reconciliation",
            61,
            Bytes::from_static(b"terminally absent build artifact"),
        )
        .await;
        assert!(matches!(
            fixture
                .coordinator
                .publish(&fixture.permit, Bytes::from_static(b"wrong payload"))
                .await,
            Err(blob_publication::BlobPublicationError::PayloadMismatch)
        ));

        let failed = claim(&fixture.db, fixture.intent_id, fixture.writer_epoch, 1).await;
        assert_eq!(
            upload_queue::execute_claimed_upload_step(
                &fixture.db,
                &failed,
                &fixture.driver,
                u64::MAX,
            )
            .await
            .unwrap(),
            TextUploadStepResult::NonPublicationProven
        );
        let proven =
            crate::index_v2::repository::load_upload_from_pointer(&fixture.db, fixture.intent_id)
                .await
                .unwrap()
                .unwrap();
        assert!(matches!(
            proven.phase,
            work::TextUploadPhase::NonPublicationProven
        ));
        assert!(matches!(
            fixture
                .coordinator
                .publication_status(&fixture.permit)
                .await
                .unwrap(),
            blob_publication::BlobPublicationStatus::DefinitivelyFailed
        ));

        let intent_reference_key = reachability_key(
            fixture.blob,
            index_keys::BlobReferenceOwnerKind::UploadIntent,
            fixture.scope,
            index_keys::IndexV2Key::TextUploadIntent(index_keys::TextIntentOwnedKey {
                index_id: fixture.index.index_id(),
                generation: fixture.index.state().generation(),
                intent_id: fixture.intent_id,
            })
            .to_bytes(),
        );
        assert!(fixture
            .db
            .get(&intent_reference_key)
            .await
            .unwrap()
            .is_some());

        fixture.coordinator.fail_next_release();
        let outage = claim(&fixture.db, fixture.intent_id, fixture.writer_epoch, 2).await;
        assert_eq!(
            upload_queue::execute_claimed_upload_step(
                &fixture.db,
                &outage,
                &fixture.driver,
                u64::MAX,
            )
            .await
            .unwrap(),
            TextUploadStepResult::TransientFailure
        );

        let release_claim = claim(&fixture.db, fixture.intent_id, fixture.writer_epoch, 3).await;
        let release_only = fixture
            .driver
            .prepare_step(&release_claim.record)
            .await
            .unwrap();
        assert_eq!(
            release_only.outcome(),
            TextUploadStepResult::NonPublicationReleased
        );
        assert!(matches!(
            fixture
                .coordinator
                .publication_status(&fixture.permit)
                .await,
            Err(blob_publication::BlobPublicationError::UnknownPermit)
        ));
        assert!(fixture
            .db
            .get(&intent_reference_key)
            .await
            .unwrap()
            .is_some());

        assert_eq!(
            upload_queue::execute_claimed_upload_step(
                &fixture.db,
                &release_claim,
                &fixture.driver,
                u64::MAX,
            )
            .await
            .unwrap(),
            TextUploadStepResult::NonPublicationReleased
        );
        assert!(crate::index_v2::repository::load_upload_from_pointer(
            &fixture.db,
            fixture.intent_id,
        )
        .await
        .unwrap()
        .is_none());
        assert!(fixture
            .db
            .get(intent_reference_key)
            .await
            .unwrap()
            .is_none());
        assert!(matches!(
            outbox::observe_operation_pointer(
                &fixture.db,
                fixture.operation_id,
                fixture.writer_epoch,
                u64::MAX,
            )
            .await
            .unwrap(),
            outbox::OperationPointerObservation::Eligible(_)
        ));
        fixture.db.close().await.unwrap();
    }

    #[tokio::test]
    async fn prior_writer_active_terminal_absence_releases_before_anchor_cleanup() {
        let fixture = staged_active_upload(
            "text-upload-active-terminal-absence",
            91,
            Bytes::from_static(b"terminally absent active manifest split"),
        )
        .await;
        assert_ne!(fixture.owner_epoch, fixture.worker_epoch);
        assert!(matches!(
            fixture
                .coordinator
                .publish(&fixture.permit, Bytes::from_static(b"wrong payload"))
                .await,
            Err(blob_publication::BlobPublicationError::PayloadMismatch)
        ));

        let failed = claim(&fixture.db, fixture.intent_id, fixture.worker_epoch, 1).await;
        assert_eq!(
            upload_queue::execute_claimed_upload_step(
                &fixture.db,
                &failed,
                &fixture.driver,
                u64::MAX,
            )
            .await
            .unwrap(),
            TextUploadStepResult::NonPublicationProven
        );
        let proven =
            crate::index_v2::repository::load_upload_from_pointer(&fixture.db, fixture.intent_id)
                .await
                .unwrap()
                .unwrap();
        assert!(matches!(
            proven.phase,
            work::TextUploadPhase::NonPublicationProven
        ));
        let intent_reference_key = reachability_key(
            fixture.blob,
            index_keys::BlobReferenceOwnerKind::UploadIntent,
            fixture.scope,
            index_keys::IndexV2Key::TextUploadIntent(index_keys::TextIntentOwnedKey {
                index_id: fixture.index_id,
                generation: fixture.generation,
                intent_id: fixture.intent_id,
            })
            .to_bytes(),
        );
        assert!(fixture
            .db
            .get(&intent_reference_key)
            .await
            .unwrap()
            .is_some());

        fixture.coordinator.fail_next_release();
        let outage = claim(&fixture.db, fixture.intent_id, fixture.worker_epoch, 2).await;
        assert_eq!(
            upload_queue::execute_claimed_upload_step(
                &fixture.db,
                &outage,
                &fixture.driver,
                u64::MAX,
            )
            .await
            .unwrap(),
            TextUploadStepResult::TransientFailure
        );
        assert!(fixture
            .db
            .get(&intent_reference_key)
            .await
            .unwrap()
            .is_some());

        let released = claim(&fixture.db, fixture.intent_id, fixture.worker_epoch, 3).await;
        assert_eq!(
            upload_queue::execute_claimed_upload_step(
                &fixture.db,
                &released,
                &fixture.driver,
                u64::MAX,
            )
            .await
            .unwrap(),
            TextUploadStepResult::NonPublicationReleased
        );
        assert!(crate::index_v2::repository::load_upload_from_pointer(
            &fixture.db,
            fixture.intent_id,
        )
        .await
        .unwrap()
        .is_none());
        assert!(fixture
            .db
            .get(intent_reference_key)
            .await
            .unwrap()
            .is_none());
        assert!(matches!(
            fixture
                .coordinator
                .publication_status(&fixture.permit)
                .await,
            Err(blob_publication::BlobPublicationError::UnknownPermit)
        ));
        fixture.db.close().await.unwrap();
    }

    #[tokio::test]
    async fn prior_writer_active_shared_blob_transfers_to_exact_candidate() {
        let fixture = staged_active_upload(
            "text-upload-active-terminal-shared",
            101,
            Bytes::from_static(b"shared active manifest split"),
        )
        .await;
        let shared_intent = TextUploadIntentId::from_bytes([106; 16]).unwrap();
        let shared_epoch = WriterEpoch::from_bytes([107; 16]).unwrap();
        let shared_permit = fixture
            .coordinator
            .reserve(fixture.blob, shared_intent, shared_epoch)
            .await
            .unwrap();
        assert!(matches!(
            fixture
                .coordinator
                .publish(&shared_permit, fixture.payload.clone())
                .await
                .unwrap(),
            blob_publication::BlobPublicationStatus::Succeeded(_)
        ));
        fixture
            .coordinator
            .release(
                &shared_permit,
                blob_publication::BlobPermitReleaseAuthority::reference_committed(
                    shared_permit.id(),
                ),
            )
            .await
            .unwrap();
        assert!(matches!(
            fixture
                .coordinator
                .publish(&fixture.permit, Bytes::from_static(b"wrong payload"))
                .await,
            Err(blob_publication::BlobPublicationError::PayloadMismatch)
        ));

        let failed = claim(&fixture.db, fixture.intent_id, fixture.worker_epoch, 1).await;
        assert_eq!(
            upload_queue::execute_claimed_upload_step(
                &fixture.db,
                &failed,
                &fixture.driver,
                u64::MAX,
            )
            .await
            .unwrap(),
            TextUploadStepResult::SharedBlobReclaimable
        );
        let reclaimable =
            crate::index_v2::repository::load_upload_from_pointer(&fixture.db, fixture.intent_id)
                .await
                .unwrap()
                .unwrap();
        assert!(matches!(
            reclaimable.phase,
            work::TextUploadPhase::Reclaimable(work::ReclaimAssignment::Unassigned)
        ));
        let candidate_key = Key::Data {
            scope: fixture.scope,
            kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::BlobGcCandidate(
                index_keys::BlobGcCandidateKey {
                    index_id: fixture.index_id,
                    generation: fixture.generation,
                    owner: index_keys::BlobGcCandidateKeyOwner::UploadIntent(fixture.intent_id),
                    blob_hash: index_keys::BlobHash::new(*fixture.blob.hash()),
                },
            )),
        }
        .to_bytes();
        assert!(fixture.db.get(candidate_key).await.unwrap().is_some());
        let intent_reference_key = reachability_key(
            fixture.blob,
            index_keys::BlobReferenceOwnerKind::UploadIntent,
            fixture.scope,
            index_keys::IndexV2Key::TextUploadIntent(index_keys::TextIntentOwnedKey {
                index_id: fixture.index_id,
                generation: fixture.generation,
                intent_id: fixture.intent_id,
            })
            .to_bytes(),
        );
        assert!(fixture
            .db
            .get(intent_reference_key)
            .await
            .unwrap()
            .is_none());
        let reclaim_claim = claim(&fixture.db, fixture.intent_id, fixture.worker_epoch, 2).await;
        assert_eq!(
            upload_queue::execute_claimed_upload_step(
                &fixture.db,
                &reclaim_claim,
                &fixture.driver,
                u64::MAX,
            )
            .await
            .unwrap(),
            TextUploadStepResult::ReclaimRootAssigned
        );
        let assigned =
            crate::index_v2::repository::load_upload_from_pointer(&fixture.db, fixture.intent_id)
                .await
                .unwrap()
                .unwrap();
        let work::TextUploadPhase::Reclaimable(work::ReclaimAssignment::Assigned(run_id)) =
            assigned.phase
        else {
            panic!("second reclaim delivery stores one exact root assignment");
        };
        let root_value = fixture
            .db
            .get(index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            index_values::decode_work_value(&root_value).unwrap(),
            index_values::IndexV2WorkValue::BlobGcEntry(work::BlobGcEntryValue::RunRoot(root))
                if root.run_id == run_id
                    && root.owner == (work::BlobGcRunOwner::UploadReclaim {
                        scope: fixture.scope,
                        intent_id: fixture.intent_id,
                        index_id: fixture.index_id,
                        generation: fixture.generation,
                    })
                    && matches!(root.phase, work::BlobGcPhase::AwaitDeleteFences {
                        member_cursor: None
                    })
                    && root.candidate_count.get() == 1
        ));
        let member_key = index_keys::GlobalIndexV2Key::BlobGcCandidateMember {
            run_id,
            blob_hash: index_keys::BlobHash::new(*fixture.blob.hash()),
        }
        .to_bytes();
        assert!(matches!(
            index_values::decode_work_value(
                &fixture.db.get(member_key).await.unwrap().unwrap()
            )
            .unwrap(),
            index_values::IndexV2WorkValue::BlobGcEntry(
                work::BlobGcEntryValue::CandidateMember(member)
            ) if member.run_id == run_id
                && member.blob == fixture.blob
                && matches!(member.state, work::BlobGcMemberState::PendingDisposition {
                    owner_cursor: None
                })
        ));
        let gc_driver = crate::index_v2::text::blob_gc::TextBlobGcDriver::new(
            fixture.coordinator.clone(),
            crate::search::text::BlobGcGate::new(),
        );
        for _ in 0..2 {
            assert_eq!(
                crate::index_v2::text::blob_gc::BlobGcDriver::execute_root_step(
                    &gc_driver,
                    &fixture.db,
                    run_id,
                    fixture.worker_epoch,
                    1_000,
                )
                .await
                .unwrap(),
                crate::index_v2::text::blob_gc::BlobGcRootStep::Progressed
            );
        }
        let reclaim_owner = claim(&fixture.db, fixture.intent_id, fixture.worker_epoch, 3).await;
        assert_eq!(
            upload_queue::execute_claimed_upload_step(
                &fixture.db,
                &reclaim_owner,
                &fixture.driver,
                u64::MAX,
            )
            .await
            .unwrap(),
            TextUploadStepResult::ReclaimFenceRevalidated
        );
        let root_value = fixture
            .db
            .get(index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            index_values::decode_work_value(&root_value).unwrap(),
            index_values::IndexV2WorkValue::BlobGcEntry(work::BlobGcEntryValue::RunRoot(root))
                if matches!(root.phase, work::BlobGcPhase::FirstPass {
                    writer_epoch,
                    first_attempt,
                    reference_cursor: None,
                } if writer_epoch == fixture.worker_epoch && first_attempt.get() == 1)
        ));

        for _ in 0..20 {
            if fixture
                .db
                .get(index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes())
                .await
                .unwrap()
                .is_none()
            {
                break;
            }
            assert!(matches!(
                crate::index_v2::text::blob_gc::BlobGcDriver::execute_root_step(
                    &gc_driver,
                    &fixture.db,
                    run_id,
                    fixture.worker_epoch,
                    1_000,
                )
                .await
                .unwrap(),
                crate::index_v2::text::blob_gc::BlobGcRootStep::Progressed
                    | crate::index_v2::text::blob_gc::BlobGcRootStep::Idle
            ));
        }
        assert!(
            fixture
                .db
                .get(index_keys::GlobalIndexV2Key::BlobGcRunRoot(run_id).to_bytes())
                .await
                .unwrap()
                .is_none(),
            "singleton upload reclaim removes its terminal root"
        );
        assert!(crate::index_v2::repository::load_upload_from_pointer(
            &fixture.db,
            fixture.intent_id,
        )
        .await
        .unwrap()
        .is_none());
        assert!(matches!(
            fixture
                .coordinator
                .publication_status(&fixture.permit)
                .await,
            Err(blob_publication::BlobPublicationError::UnknownPermit)
        ));
        assert!(matches!(
            fixture.coordinator.validate_reference(fixture.blob).await,
            Err(blob_publication::BlobPublicationError::ReferenceAbsent)
        ));
        fixture.db.close().await.unwrap();
    }

    #[tokio::test]
    async fn prior_writer_active_blob_mismatch_retries_without_blocking_or_reclaiming() {
        let fixture = staged_active_upload(
            "text-upload-active-terminal-mismatch",
            111,
            Bytes::from_static(b"mismatched active manifest split"),
        )
        .await;
        assert!(matches!(
            fixture
                .coordinator
                .publish(&fixture.permit, Bytes::from_static(b"wrong payload"))
                .await,
            Err(blob_publication::BlobPublicationError::PayloadMismatch)
        ));
        let location =
            crate::search::text::blob_object_store_path(fixture.db_path, *fixture.blob.hash());
        fixture
            .store
            .put(
                &location,
                PutPayload::from_bytes(Bytes::from(vec![0xEE; fixture.payload.len()])),
            )
            .await
            .unwrap();

        let mismatch = claim(&fixture.db, fixture.intent_id, fixture.worker_epoch, 1).await;
        assert_eq!(
            upload_queue::execute_claimed_upload_step(
                &fixture.db,
                &mismatch,
                &fixture.driver,
                u64::MAX,
            )
            .await
            .unwrap(),
            TextUploadStepResult::TransientFailure
        );
        let retained =
            crate::index_v2::repository::load_upload_from_pointer(&fixture.db, fixture.intent_id)
                .await
                .unwrap()
                .unwrap();
        assert!(matches!(retained.phase, work::TextUploadPhase::Prepared));
        assert!(matches!(
            retained.work_state,
            work::TextUploadWorkState::Queued {
                not_before_unix_millis: Some(u64::MAX)
            }
        ));
        let candidate_key = Key::Data {
            scope: fixture.scope,
            kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::BlobGcCandidate(
                index_keys::BlobGcCandidateKey {
                    index_id: fixture.index_id,
                    generation: fixture.generation,
                    owner: index_keys::BlobGcCandidateKeyOwner::UploadIntent(fixture.intent_id),
                    blob_hash: index_keys::BlobHash::new(*fixture.blob.hash()),
                },
            )),
        }
        .to_bytes();
        assert!(fixture.db.get(candidate_key).await.unwrap().is_none());
        let intent_reference_key = reachability_key(
            fixture.blob,
            index_keys::BlobReferenceOwnerKind::UploadIntent,
            fixture.scope,
            index_keys::IndexV2Key::TextUploadIntent(index_keys::TextIntentOwnedKey {
                index_id: fixture.index_id,
                generation: fixture.generation,
                intent_id: fixture.intent_id,
            })
            .to_bytes(),
        );
        assert!(fixture
            .db
            .get(intent_reference_key)
            .await
            .unwrap()
            .is_some());
        fixture.db.close().await.unwrap();
    }

    #[tokio::test]
    async fn terminal_shared_blob_transfers_to_intent_qualified_reclaim_without_release() {
        let fixture = staged_build_upload(
            "text-upload-terminal-shared-reconciliation",
            71,
            Bytes::from_static(b"shared build artifact"),
        )
        .await;
        let shared_intent = TextUploadIntentId::from_bytes([74; 16]).unwrap();
        let shared_epoch = WriterEpoch::from_bytes([75; 16]).unwrap();
        let shared_permit = fixture
            .coordinator
            .reserve(fixture.blob, shared_intent, shared_epoch)
            .await
            .unwrap();
        assert!(matches!(
            fixture
                .coordinator
                .publish(&shared_permit, fixture.payload.clone())
                .await
                .unwrap(),
            blob_publication::BlobPublicationStatus::Succeeded(_)
        ));
        fixture
            .coordinator
            .release(
                &shared_permit,
                blob_publication::BlobPermitReleaseAuthority::reference_committed(
                    shared_permit.id(),
                ),
            )
            .await
            .unwrap();
        assert!(matches!(
            fixture
                .coordinator
                .publish(&fixture.permit, Bytes::from_static(b"wrong payload"))
                .await,
            Err(blob_publication::BlobPublicationError::PayloadMismatch)
        ));

        let failed = claim(&fixture.db, fixture.intent_id, fixture.writer_epoch, 1).await;
        assert_eq!(
            upload_queue::execute_claimed_upload_step(
                &fixture.db,
                &failed,
                &fixture.driver,
                u64::MAX,
            )
            .await
            .unwrap(),
            TextUploadStepResult::SharedBlobReclaimable
        );
        let reclaimable =
            crate::index_v2::repository::load_upload_from_pointer(&fixture.db, fixture.intent_id)
                .await
                .unwrap()
                .unwrap();
        assert!(matches!(
            reclaimable.phase,
            work::TextUploadPhase::Reclaimable(work::ReclaimAssignment::Unassigned)
        ));
        assert!(matches!(
            fixture
                .coordinator
                .publication_status(&fixture.permit)
                .await
                .unwrap(),
            blob_publication::BlobPublicationStatus::DefinitivelyFailed
        ));

        let candidate_key = Key::Data {
            scope: fixture.scope,
            kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::BlobGcCandidate(
                index_keys::BlobGcCandidateKey {
                    index_id: fixture.index.index_id(),
                    generation: fixture.index.state().generation(),
                    owner: index_keys::BlobGcCandidateKeyOwner::UploadIntent(fixture.intent_id),
                    blob_hash: index_keys::BlobHash::new(*fixture.blob.hash()),
                },
            )),
        }
        .to_bytes();
        let candidate_value = fixture.db.get(candidate_key).await.unwrap().unwrap();
        assert!(matches!(
            index_values::decode_work_value(&candidate_value).unwrap(),
            index_values::IndexV2WorkValue::BlobGcCandidate(candidate)
                if candidate.owner == work::BlobGcCandidateOwner::UploadIntent(fixture.intent_id)
                    && candidate.index_id == fixture.index.index_id()
                    && candidate.generation == fixture.index.state().generation()
                    && candidate.blob == fixture.blob
        ));
        let intent_reference_key = reachability_key(
            fixture.blob,
            index_keys::BlobReferenceOwnerKind::UploadIntent,
            fixture.scope,
            index_keys::IndexV2Key::TextUploadIntent(index_keys::TextIntentOwnedKey {
                index_id: fixture.index.index_id(),
                generation: fixture.index.state().generation(),
                intent_id: fixture.intent_id,
            })
            .to_bytes(),
        );
        assert!(fixture
            .db
            .get(intent_reference_key)
            .await
            .unwrap()
            .is_none());
        drop(
            fixture
                .coordinator
                .validate_reference(fixture.blob)
                .await
                .unwrap(),
        );
        fixture.db.close().await.unwrap();
    }

    #[tokio::test]
    async fn terminal_reclaim_contracts_reject_invalid_phase_owner_candidate_and_reference() {
        let fixture = staged_build_upload(
            "text-upload-terminal-reclaim-negative-contracts",
            81,
            Bytes::from_static(b"negative reclaim contracts"),
        )
        .await;
        let queued =
            crate::index_v2::repository::load_upload_from_pointer(&fixture.db, fixture.intent_id)
                .await
                .unwrap()
                .unwrap();
        let transaction = fixture
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            crate::index_v2::text::reclaim::validate_non_publication_proof(
                &transaction,
                fixture.scope,
                &queued,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("claimed Prepared")
        ));
        assert!(matches!(
            crate::index_v2::text::reclaim::stage_shared_blob_reclaim(
                &transaction,
                fixture.scope,
                &queued,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("claimed Prepared")
        ));
        drop(transaction);

        let claimed = claim(&fixture.db, fixture.intent_id, fixture.writer_epoch, 1).await;
        let mut active = claimed.record.clone();
        active.owner = work::TextUploadOwner::ActiveMutation {
            writer_epoch: fixture.writer_epoch,
            mutation_id: crate::index_v2::MutationId::from_bytes([84; 16]).unwrap(),
            active_record_revision: IndexRevision::initial(),
        };
        let transaction = fixture
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            crate::index_v2::text::build_owner::load_exact(
                &transaction,
                fixture.scope,
                &active,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("active upload")
        ));
        assert!(matches!(
            crate::index_v2::text::reclaim::stage_non_publication_cleanup(
                &transaction,
                fixture.scope,
                &claimed.record,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("claimed absence proof")
        ));
        drop(transaction);

        let candidate_key = Key::Data {
            scope: fixture.scope,
            kind: DataKeyKind::IndexV2(index_keys::IndexV2Key::BlobGcCandidate(
                index_keys::BlobGcCandidateKey {
                    index_id: fixture.index.index_id(),
                    generation: fixture.index.state().generation(),
                    owner: index_keys::BlobGcCandidateKeyOwner::UploadIntent(fixture.intent_id),
                    blob_hash: index_keys::BlobHash::new(*fixture.blob.hash()),
                },
            )),
        }
        .to_bytes();
        let candidate_value = index_values::encode_work_value(
            &index_values::IndexV2WorkValue::BlobGcCandidate(work::BlobGcCandidateValue {
                owner: work::BlobGcCandidateOwner::UploadIntent(fixture.intent_id),
                index_id: fixture.index.index_id(),
                generation: fixture.index.state().generation(),
                blob: fixture.blob,
            }),
        );
        let transaction = fixture
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        transaction.put(candidate_key, candidate_value).unwrap();
        assert!(matches!(
            crate::index_v2::text::reclaim::stage_shared_blob_reclaim(
                &transaction,
                fixture.scope,
                &claimed.record,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("candidate was occupied")
        ));
        drop(transaction);

        let intent_reference_key = reachability_key(
            fixture.blob,
            index_keys::BlobReferenceOwnerKind::UploadIntent,
            fixture.scope,
            index_keys::IndexV2Key::TextUploadIntent(index_keys::TextIntentOwnedKey {
                index_id: fixture.index.index_id(),
                generation: fixture.index.state().generation(),
                intent_id: fixture.intent_id,
            })
            .to_bytes(),
        );
        let transaction = fixture
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        transaction.delete(intent_reference_key).unwrap();
        transaction.commit().await.unwrap();
        let transaction = fixture
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            crate::index_v2::text::reclaim::stage_shared_blob_reclaim(
                &transaction,
                fixture.scope,
                &claimed.record,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("missing its exact intent reachability")
        ));
        let proven = claimed.record.non_publication_proven().unwrap();
        let proven_claimed = proven
            .claim(crate::index_v2::OperationClaim {
                writer_epoch: fixture.writer_epoch,
                sequence: ClaimSequence::new(2).unwrap(),
            })
            .unwrap();
        assert!(matches!(
            crate::index_v2::text::reclaim::stage_non_publication_cleanup(
                &transaction,
                fixture.scope,
                &proven_claimed,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("missing its exact intent reachability")
        ));
        drop(transaction);
        fixture.db.close().await.unwrap();
    }
}
