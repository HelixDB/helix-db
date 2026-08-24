use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use slatedb::object_store::memory::InMemory;
use slatedb::{Db, IsolationLevel};

use super::*;
use crate::index_lifecycle::{
    ClaimSequence, IndexGenerationId, IndexId, IndexOperationId, IndexOperationKind,
    IndexOperationRevision, IndexRevision, OperationClaim, WriterEpoch,
};

fn operation() -> IndexOperationRecord {
    let runtime = crate::config::TextIndexDefinition::new_node("Document", "body")
        .expect("prepared-step definition validates");
    let definition = ValidatedTextIndexDefinition::try_from_runtime(&runtime)
        .expect("prepared-step definition has a V2 representation");
    IndexOperationRecord::try_new(
        IndexOperationId::new_v4(),
        IndexId::initial(),
        definition.identity(),
        IndexGenerationId::initial(),
        IndexRevision::initial(),
        IndexOperationRevision::initial(),
        IndexOperationKind::Build,
        IndexOperationFamily::Text,
        IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::CatchUp(PrefixScanProgress {
                cursor: None,
                counters: OperationCounters::default(),
            }),
        )),
        0,
        IndexOperationExecutionState::Queued {
            not_before_unix_millis: None,
        },
    )
    .expect("prepared-step operation is internally consistent")
}

fn progressed(operation: &IndexOperationRecord) -> IndexOperationProgress {
    operation.progress().clone()
}

fn claimed_operation() -> IndexOperationRecord {
    operation()
        .claim(OperationClaim {
            writer_epoch: WriterEpoch::from_bytes([5; 16]).unwrap(),
            sequence: ClaimSequence::new(1).unwrap(),
        })
        .unwrap()
}

fn definition() -> ValidatedTextIndexDefinition {
    ValidatedTextIndexDefinition::try_from_runtime(
        &crate::config::TextIndexDefinition::new_node("Document", "body").unwrap(),
    )
    .unwrap()
}

fn limits(max_output_bytes: u64) -> SearchIndexBatchLimits {
    batch_limits(8, u64::MAX, u64::MAX, max_output_bytes)
}

fn batch_limits(
    max_entities: usize,
    max_input_bytes: u64,
    max_output_operations: u64,
    max_output_bytes: u64,
) -> SearchIndexBatchLimits {
    SearchIndexBatchLimits::try_new(
        NonZeroUsize::new(max_entities).unwrap(),
        NonZeroU64::new(max_input_bytes).unwrap(),
        NonZeroU64::new(max_output_operations).unwrap(),
        NonZeroU64::new(max_output_bytes).unwrap(),
        NonZeroU64::new(max_output_bytes).unwrap(),
    )
    .unwrap()
}

fn split_input(
    counters: OperationCounters,
    source: PreparedTextUploadSource,
) -> PreparedTextSplitInput {
    PreparedTextSplitInput {
        partition: TextPartition::Unpartitioned,
        documents: vec![crate::search::text::TextDocumentInput::new(
            7,
            "one searchable document",
        )],
        completed_counters: counters,
        source,
        expected_reads: Vec::new(),
        lifecycle_writes: Vec::new(),
    }
}

fn expected(key: &'static [u8], value: Option<&'static [u8]>) -> PreparedTextExpectedRead {
    PreparedTextExpectedRead {
        key: Bytes::from_static(key),
        value: value.map(Bytes::from_static),
    }
}

fn upload(
    operation: &IndexOperationRecord,
    artifact_key: &'static [u8],
) -> PreparedTextBuildUpload {
    PreparedTextBuildUpload {
        source_operation: operation.clone(),
        progress: progressed(operation),
        artifact_key: Bytes::from_static(artifact_key),
        artifact_value: Bytes::from_static(b"artifact-value"),
        expected_reads: vec![expected(b"upload-observation", None)],
        lifecycle_writes: vec![
            PreparedTextWrite::Put {
                key: Bytes::from_static(b"upload-put"),
                value: Bytes::from_static(b"put-value"),
            },
            PreparedTextWrite::Delete {
                key: Bytes::from_static(b"upload-delete"),
            },
        ],
        retired_artifact_keys: Vec::new(),
        uploaded_bytes: 14,
    }
}

#[tokio::test]
async fn repository_and_upload_preparations_obey_exact_observations_and_variants() {
    let db = Db::open(
        "text-driver-prepared-operation-contracts",
        Arc::new(InMemory::new()),
    )
    .await
    .expect("prepared-step database opens");
    let other_operation = operation();
    let operation = operation();
    let scope = DataScope::LegacyUnscoped;

    let repository = PreparedTextOperationStep::Repository(Box::new(PreparedTextRepositoryStep {
        source_operation: operation.clone(),
        expected_reads: vec![expected(b"repository-observation", None)],
        writes: vec![
            PreparedTextWrite::Put {
                key: Bytes::from_static(b"repository-put"),
                value: Bytes::from_static(b"put-value"),
            },
            PreparedTextWrite::Delete {
                key: Bytes::from_static(b"repository-delete"),
            },
        ],
        result: IndexOperationStepResult::Progressed(progressed(&operation)),
    }));
    assert_eq!(repository.resource_usage(), StepResourceUsage::default());
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert!(repository
        .stage(&transaction, scope, &other_operation)
        .await
        .is_err());
    drop(transaction);

    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(b"repository-observation", b"now-stale")
        .unwrap();
    assert!(matches!(
        repository
            .stage(&transaction, scope, &operation)
            .await
            .unwrap(),
        IndexOperationStepResult::TransientFailure
    ));
    drop(transaction);

    let repository_current =
        PreparedTextOperationStep::Repository(Box::new(PreparedTextRepositoryStep {
            source_operation: operation.clone(),
            expected_reads: vec![expected(b"repository-current", Some(b"current"))],
            writes: vec![
                PreparedTextWrite::Put {
                    key: Bytes::from_static(b"repository-current-put"),
                    value: Bytes::from_static(b"put-value"),
                },
                PreparedTextWrite::Delete {
                    key: Bytes::from_static(b"repository-current-delete"),
                },
            ],
            result: IndexOperationStepResult::Progressed(progressed(&operation)),
        }));
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction.put(b"repository-current", b"current").unwrap();
    transaction
        .put(b"repository-current-delete", b"delete-me")
        .unwrap();
    assert!(matches!(
        repository_current
            .stage(&transaction, scope, &operation)
            .await
            .unwrap(),
        IndexOperationStepResult::Progressed(_)
    ));
    assert_eq!(
        transaction.get(b"repository-current-put").await.unwrap(),
        Some(Bytes::from_static(b"put-value"))
    );
    assert_eq!(
        transaction.get(b"repository-current-delete").await.unwrap(),
        None
    );
    drop(transaction);

    let partition = PreparedTextOperationStep::PartitionUpload(Box::new(upload(
        &operation,
        b"partition-artifact",
    )));
    assert_eq!(
        partition.resource_usage(),
        StepResourceUsage {
            text_artifact_bytes: 14,
            text_upload_bytes: 14,
            ..StepResourceUsage::default()
        }
    );
    let catch_up = PreparedTextOperationStep::CatchUpUpload(Box::new(upload(
        &operation,
        b"catch-up-artifact",
    )));
    assert_eq!(catch_up.resource_usage(), partition.resource_usage());
    let compaction = PreparedTextOperationStep::CompactionUpload(Box::new(upload(
        &operation,
        b"compaction-artifact",
    )));
    assert_eq!(
        compaction.resource_usage(),
        StepResourceUsage {
            text_artifact_bytes: 14,
            text_upload_bytes: 14,
            compaction_fan_in: 0,
            compaction_input_bytes: 0,
            temporary_bytes: 14,
            ..StepResourceUsage::default()
        }
    );

    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert!(partition
        .stage(&transaction, scope, &other_operation)
        .await
        .is_err());
    drop(transaction);

    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(b"partition-artifact", b"already-occupied")
        .unwrap();
    assert!(partition
        .stage(&transaction, scope, &operation)
        .await
        .is_err());
    drop(transaction);

    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(b"upload-observation", b"now-stale")
        .unwrap();
    assert!(matches!(
        catch_up
            .stage(&transaction, scope, &operation)
            .await
            .unwrap(),
        IndexOperationStepResult::TransientFailure
    ));
    drop(transaction);

    let current_upload =
        PreparedTextOperationStep::PartitionUpload(Box::new(PreparedTextBuildUpload {
            expected_reads: vec![expected(b"upload-current", Some(b"current"))],
            ..upload(&operation, b"current-artifact")
        }));
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction.put(b"upload-current", b"current").unwrap();
    transaction.put(b"upload-delete", b"delete-me").unwrap();
    assert!(matches!(
        current_upload
            .stage(&transaction, scope, &operation)
            .await
            .unwrap(),
        IndexOperationStepResult::Progressed(_)
    ));
    assert_eq!(
        transaction.get(b"current-artifact").await.unwrap(),
        Some(Bytes::from_static(b"artifact-value"))
    );
    assert_eq!(
        transaction.get(b"upload-put").await.unwrap(),
        Some(Bytes::from_static(b"put-value"))
    );
    assert_eq!(transaction.get(b"upload-delete").await.unwrap(), None);
    drop(transaction);

    partition.discard().await.unwrap();
    compaction.after_commit().await;
    db.close().await.expect("prepared-step database closes");
}

#[tokio::test]
async fn compaction_retirement_and_manifest_roots_fail_closed_at_every_boundary() {
    let db = Db::open(
        "text-driver-prepared-root-contracts",
        Arc::new(InMemory::new()),
    )
    .await
    .expect("prepared-root database opens");
    let other_operation = operation();
    let operation = operation();
    let scope = DataScope::LegacyUnscoped;

    let retirement = PreparedTextOperationStep::CompactionRetirement(Box::new(
        PreparedTextCompactionRetirement {
            source_operation: operation.clone(),
            expected_reads: vec![expected(b"retirement-observation", None)],
            input_artifact_keys: Vec::new(),
            progress: progressed(&operation),
        },
    ));
    assert_eq!(
        retirement.resource_usage(),
        StepResourceUsage {
            compaction_fan_in: 0,
            compaction_input_bytes: 0,
            temporary_bytes: 0,
            ..StepResourceUsage::default()
        }
    );
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert!(retirement
        .stage(&transaction, scope, &other_operation)
        .await
        .is_err());
    drop(transaction);

    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(b"retirement-observation", b"now-stale")
        .unwrap();
    assert!(matches!(
        retirement
            .stage(&transaction, scope, &operation)
            .await
            .unwrap(),
        IndexOperationStepResult::TransientFailure
    ));
    drop(transaction);

    let current_retirement = PreparedTextOperationStep::CompactionRetirement(Box::new(
        PreparedTextCompactionRetirement {
            source_operation: operation.clone(),
            expected_reads: Vec::new(),
            input_artifact_keys: Vec::new(),
            progress: progressed(&operation),
        },
    ));
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert!(current_retirement
        .stage(&transaction, scope, &operation)
        .await
        .is_err());
    drop(transaction);

    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let non_empty = work::TextManifestRootValue::try_new(
        operation.index_id(),
        operation.generation(),
        TextPartition::Unpartitioned,
        TextManifestRevision::new(2).unwrap(),
        1,
        1,
    )
    .unwrap();
    let root_key = scoped_index_key(
        scope,
        ScopedKey::TextManifestRoot(TextManifestRootKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            partition: TextPartition::Unpartitioned.fingerprint(),
        }),
    );
    transaction
        .put(&root_key, encode_manifest_root(&non_empty))
        .unwrap();
    assert!(prepare_empty_manifest_root(
        &transaction,
        scope,
        &operation,
        TextPartition::Unpartitioned,
    )
    .await
    .is_err());
    drop(transaction);

    let exhausted_root = work::TextManifestRootValue::try_new(
        operation.index_id(),
        operation.generation(),
        TextPartition::Unpartitioned,
        TextManifestRevision::new(u64::MAX).unwrap(),
        0,
        0,
    )
    .unwrap();
    let mut exhausted = PreparedCatchUpManifestRoot {
        observation: expected(b"exhausted-root", None),
        root: exhausted_root,
        write: None,
    };
    assert_eq!(exhausted.next_logical_version(), None);
    assert_eq!(exhausted.advance_for_entity_transition().unwrap(), None);
    assert!(exhausted.into_parts().1.is_none());

    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let wrong_owner = work::TextManifestRootValue::empty(
        IndexId::new(2).unwrap(),
        operation.generation(),
        TextPartition::Unpartitioned,
    );
    transaction
        .put(&root_key, encode_manifest_root(&wrong_owner))
        .unwrap();
    assert!(prepare_catch_up_manifest_root(
        &transaction,
        scope,
        &operation,
        TextPartition::Unpartitioned,
    )
    .await
    .is_err());
    drop(transaction);

    assert!(matches!(
        operation_error(crate::index_lifecycle::IndexOperationModelError::ZeroClaimSequence),
        HelixDbError::InvariantViolation(_)
    ));
    assert!(matches!(
        work_error(crate::index_lifecycle::work::IndexWorkModelError::EmptyTenantPartition),
        HelixDbError::InvariantViolation(_)
    ));
    db.close().await.expect("prepared-root database closes");
}

#[tokio::test]
async fn build_upload_classifies_limits_and_encodes_partition_and_catch_up_resumes() {
    let unclaimed_operation = operation();
    let operation = claimed_operation();
    let definition = definition();
    let object_store = Arc::new(InMemory::new());
    let runtime = TextStorageRuntime {
        object_store,
        db_path: "text-driver-build-upload-contracts".to_string(),
        compaction_limits: crate::config::SearchIndexBackfillLimits::default().text_compaction(),
    };
    let source_progress = SourceScanProgress {
        inclusive_upper_bound: IndexCursor::try_new(Bytes::from_static(b"upper")).unwrap(),
        cursor: None,
        counters: OperationCounters::default(),
    };
    let partition_source = || PreparedTextUploadSource::Partition {
        progress: source_progress.clone(),
        completed_cursor: IndexCursor::try_new(Bytes::from_static(b"completed")).unwrap(),
    };

    let blocked_payload = prepare_build_upload(
        &operation,
        DataScope::LegacyUnscoped,
        &definition,
        limits(1),
        &runtime,
        split_input(OperationCounters::default(), partition_source()),
    )
    .await
    .unwrap();
    let PreparedTextOperationStep::Repository(blocked_payload) = blocked_payload else {
        panic!("one-byte output limit blocks the physical split payload")
    };
    assert!(matches!(
        blocked_payload.result,
        IndexOperationStepResult::Blocked(IndexOperationBlocker::ManifestLimit { limit: 1, .. })
    ));

    let blocked_ordinal = prepare_build_upload(
        &operation,
        DataScope::LegacyUnscoped,
        &definition,
        limits(u64::MAX),
        &runtime,
        split_input(
            OperationCounters {
                output_operations: u64::from(u32::MAX) + 1,
                ..OperationCounters::default()
            },
            partition_source(),
        ),
    )
    .await
    .unwrap();
    let PreparedTextOperationStep::Repository(blocked_ordinal) = blocked_ordinal else {
        panic!("an exhausted artifact ordinal blocks before publication")
    };
    assert!(matches!(
        blocked_ordinal.result,
        IndexOperationStepResult::Blocked(IndexOperationBlocker::ManifestLimit {
            limit,
            ..
        }) if limit == u64::from(u32::MAX)
    ));

    let partition = prepare_build_upload(
        &operation,
        DataScope::LegacyUnscoped,
        &definition,
        limits(u64::MAX),
        &runtime,
        split_input(OperationCounters::default(), partition_source()),
    )
    .await
    .unwrap();
    let PreparedTextOperationStep::PartitionUpload(partition) = partition else {
        panic!("a bounded partition batch chooses one exact upload")
    };
    assert!(partition.uploaded_bytes > 1);
    assert!(matches!(
        partition.progress,
        IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::ScanPartitions(SourceScanProgress {
                cursor: Some(_),
                ..
            })
        ))
    ));

    let catch_up = prepare_build_upload(
        &operation,
        DataScope::LegacyUnscoped,
        &definition,
        limits(u64::MAX),
        &runtime,
        split_input(
            OperationCounters::default(),
            PreparedTextUploadSource::CatchUp,
        ),
    )
    .await
    .unwrap();
    let PreparedTextOperationStep::CatchUpUpload(catch_up) = catch_up else {
        panic!("late authoritative work chooses the explicit catch-up upload")
    };
    assert!(matches!(
        catch_up.progress,
        IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::CatchUp(_)
        ))
    ));

    assert!(prepare_build_upload(
        &unclaimed_operation,
        DataScope::LegacyUnscoped,
        &definition,
        limits(u64::MAX),
        &runtime,
        split_input(
            OperationCounters::default(),
            PreparedTextUploadSource::CatchUp
        ),
    )
    .await
    .is_err());
}

#[test]
fn text_driver_key_projection_and_counter_helpers_fail_closed() {
    let operation = operation();
    let definition = definition();
    let scope = DataScope::LegacyUnscoped;
    let entity = IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(7),
    };
    let state = TextEntityStateValue {
        index_id: operation.index_id(),
        generation: operation.generation(),
        partition: TextPartition::Unpartitioned,
        entity_kind: entity.kind,
        entity_id: entity.id,
        logical_version: TextLogicalVersion::initial(),
        live: true,
    };
    let state_key = scoped_index_key(
        scope,
        ScopedKey::TextEntityState(TextEntityStateKey {
            root: TextManifestRootKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                partition: TextPartition::Unpartitioned.fingerprint(),
            },
            entity,
        }),
    );
    assert_eq!(
        decode_entity_state(
            scope,
            &state_key,
            &encode_text_entity_state(&state),
            &operation
        )
        .unwrap()
        .1,
        state
    );
    let wrong_state = TextEntityStateValue {
        entity_id: IndexEntityId::new(8),
        ..state.clone()
    };
    assert!(decode_entity_state(
        scope,
        &state_key,
        &encode_text_entity_state(&wrong_state),
        &operation,
    )
    .is_err());
    let wrong_key = scoped_index_key(
        scope,
        ScopedKey::AppliedState(IndexEntityStateKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            entity,
        }),
    );
    assert!(decode_entity_state(
        scope,
        &wrong_key,
        &encode_text_entity_state(&state),
        &operation,
    )
    .is_err());
    assert!(decode_entity_state(scope, &state_key, b"malformed", &operation).is_err());

    let indexed = vec![
        property::Property::string("$label", "Document"),
        property::Property::string("body", "searchable"),
    ];
    assert_eq!(
        text_document(&definition, &indexed, &state)
            .unwrap()
            .unwrap(),
        crate::search::text::TextDocumentInput::new(7, "searchable")
    );
    let moved_state = TextEntityStateValue {
        partition: TextPartition::try_tenant_value(Bytes::from_static(b"other")).unwrap(),
        ..state.clone()
    };
    assert_eq!(
        text_document(&definition, &indexed, &moved_state).unwrap(),
        None
    );
    assert_eq!(
        text_document(
            &definition,
            &[property::Property::string("$label", "Other")],
            &state,
        )
        .unwrap(),
        None
    );
    assert!(text_document(
        &definition,
        &[
            property::Property::string("$label", "Document"),
            property::Property::new(
                "body",
                crate::encoding::v2::values::property::property_value::PropertyValue::I64(1),
            ),
        ],
        &state,
    )
    .is_err());

    let node_key = authoritative_property_key(scope, entity);
    assert_eq!(
        source_entity(scope, IndexElementKind::Node, &node_key).unwrap(),
        Some(entity.id)
    );
    let edge = IndexEntity {
        kind: IndexElementKind::Edge,
        id: IndexEntityId::new(9),
    };
    let edge_key = authoritative_property_key(scope, edge);
    assert_eq!(
        source_entity(scope, IndexElementKind::Edge, &edge_key).unwrap(),
        Some(edge.id)
    );
    assert_eq!(
        source_entity(scope, IndexElementKind::Edge, &node_key).unwrap(),
        None
    );
    assert!(source_entity(scope, IndexElementKind::Node, &edge_key).is_err());
    assert_ne!(
        source_prefix(scope, IndexElementKind::Node),
        source_prefix(scope, IndexElementKind::Edge)
    );

    let prefix = Bytes::from_static(b"prefix/");
    assert_eq!(cursor_suffix(&prefix, None).unwrap(), None);
    let complete = IndexCursor::try_new(Bytes::from_static(b"prefix/suffix")).unwrap();
    assert_eq!(
        cursor_suffix(&prefix, Some(&complete)).unwrap(),
        Some(Bytes::from_static(b"suffix"))
    );
    let foreign = IndexCursor::try_new(Bytes::from_static(b"foreign/suffix")).unwrap();
    assert!(cursor_suffix(&prefix, Some(&foreign)).is_err());
    assert_eq!(checked_add(2, 3, "fixture").unwrap(), 5);
    assert!(checked_add(u64::MAX, 1, "fixture").is_err());
    assert!(matches!(
        invalid_source(IndexElementKind::Edge, IndexEntityId::new(9)),
        IndexOperationStepResult::Blocked(IndexOperationBlocker::InvalidSourceData {
            entity_kind: IndexElementKind::Edge,
            entity_id,
        }) if entity_id == IndexEntityId::new(9)
    ));
    assert!(initial_partition_scan(&operation, scope, OperationCounters::default()).is_ok());
}

async fn scan_source_case(
    database: &'static str,
    value: Bytes,
    limits: SearchIndexBatchLimits,
    preexisting_state: bool,
) -> Result<IndexOperationStepResult> {
    let db = Db::open(database, Arc::new(InMemory::new())).await.unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let operation = operation();
    let definition = definition();
    let scope = DataScope::LegacyUnscoped;
    let entity = IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(7),
    };
    let source_key = authoritative_property_key(scope, entity);
    transaction.put(&source_key, value).unwrap();
    if preexisting_state {
        transaction
            .put(
                scoped_index_key(
                    scope,
                    ScopedKey::TextEntityState(TextEntityStateKey {
                        root: TextManifestRootKey {
                            index_id: operation.index_id(),
                            generation: operation.generation(),
                            partition: TextPartition::Unpartitioned.fingerprint(),
                        },
                        entity,
                    }),
                ),
                b"occupied",
            )
            .unwrap();
    }
    let result = scan_source(
        &transaction,
        scope,
        &operation,
        &definition,
        &SourceScanProgress {
            inclusive_upper_bound: IndexCursor::try_new(source_key).unwrap(),
            cursor: None,
            counters: OperationCounters::default(),
        },
        limits,
        IndexLifecycleScanTuning::default(),
    )
    .await;
    drop(transaction);
    db.close().await.unwrap();
    result
}

#[tokio::test]
async fn source_scan_attributes_every_input_output_and_corruption_boundary() {
    let malformed = scan_source_case(
        "text-driver-source-malformed",
        Bytes::from_static(b"malformed"),
        batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
        false,
    )
    .await
    .unwrap();
    assert!(matches!(
        malformed,
        IndexOperationStepResult::Blocked(IndexOperationBlocker::InvalidSourceData { .. })
    ));

    let oversized_input = scan_source_case(
        "text-driver-source-input-limit",
        Bytes::from(vec![0; 128]),
        batch_limits(8, 1, u64::MAX, u64::MAX),
        false,
    )
    .await
    .unwrap();
    assert!(matches!(
        oversized_input,
        IndexOperationStepResult::Blocked(IndexOperationBlocker::OversizedEntity { limit: 1, .. })
    ));

    let ignored = scan_source_case(
        "text-driver-source-not-indexed",
        property::encode_properties(&[property::Property::string("$label", "Other")]),
        batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
        false,
    )
    .await
    .unwrap();
    assert!(matches!(
        ignored,
        IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
            TextBuildProgress::Constructing(TextBuildStage::ScanPartitions(_))
        ))
    ));

    let indexed = property::encode_properties(&[
        property::Property::string("$label", "Document"),
        property::Property::string("body", "searchable"),
    ]);
    for (database, limits) in [
        (
            "text-driver-source-output-operations",
            batch_limits(8, u64::MAX, 1, u64::MAX),
        ),
        (
            "text-driver-source-output-bytes",
            batch_limits(8, u64::MAX, u64::MAX, 1),
        ),
    ] {
        assert!(matches!(
            scan_source_case(database, indexed.clone(), limits, false)
                .await
                .unwrap(),
            IndexOperationStepResult::Blocked(IndexOperationBlocker::OversizedEntity { .. })
        ));
    }
    assert!(scan_source_case(
        "text-driver-source-preexisting",
        indexed,
        batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
        true,
    )
    .await
    .is_err());

    let db = Db::open("text-driver-source-cursors", Arc::new(InMemory::new()))
        .await
        .unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let operation = operation();
    let definition = definition();
    let scope = DataScope::LegacyUnscoped;
    let upper = authoritative_property_key(
        scope,
        IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(7),
        },
    );
    let equal = IndexCursor::try_new(upper.clone()).unwrap();
    assert!(matches!(
        scan_source(
            &transaction,
            scope,
            &operation,
            &definition,
            &SourceScanProgress {
                inclusive_upper_bound: equal.clone(),
                cursor: Some(equal),
                counters: OperationCounters::default(),
            },
            batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
            IndexLifecycleScanTuning::default(),
        )
        .await
        .unwrap(),
        IndexOperationStepResult::Progressed(_)
    ));
    let greater = authoritative_property_key(
        scope,
        IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(8),
        },
    );
    assert!(scan_source(
        &transaction,
        scope,
        &operation,
        &definition,
        &SourceScanProgress {
            inclusive_upper_bound: IndexCursor::try_new(upper).unwrap(),
            cursor: Some(IndexCursor::try_new(greater).unwrap()),
            counters: OperationCounters::default(),
        },
        batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
        IndexLifecycleScanTuning::default(),
    )
    .await
    .is_err());
    drop(transaction);
    db.close().await.unwrap();
}

#[tokio::test]
async fn partition_scan_separates_root_creation_document_upload_and_empty_exhaustion() {
    let empty_operation = operation();
    let db = Db::open(
        "text-driver-partition-scan-contracts",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let operation = operation();
    let definition = definition();
    let scope = DataScope::LegacyUnscoped;
    let entity = IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(7),
    };
    let partition = TextPartition::Unpartitioned;
    let state_key = scoped_index_key(
        scope,
        ScopedKey::TextEntityState(TextEntityStateKey {
            root: TextManifestRootKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                partition: partition.fingerprint(),
            },
            entity,
        }),
    );
    let state = TextEntityStateValue {
        index_id: operation.index_id(),
        generation: operation.generation(),
        partition: partition.clone(),
        entity_kind: entity.kind,
        entity_id: entity.id,
        logical_version: TextLogicalVersion::initial(),
        live: true,
    };
    transaction
        .put(&state_key, encode_text_entity_state(&state))
        .unwrap();
    transaction
        .put(
            authoritative_property_key(scope, entity),
            property::encode_properties(&[
                property::Property::string("$label", "Document"),
                property::Property::string("body", "searchable"),
            ]),
        )
        .unwrap();
    let progress = initial_partition_scan(&operation, scope, OperationCounters::default()).unwrap();

    let missing_root = scan_partition_documents(
        &transaction,
        scope,
        &operation,
        &definition,
        &progress,
        batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
        IndexLifecycleScanTuning::default(),
    )
    .await
    .unwrap();
    let PartitionScanSelection::Repository {
        empty_root: Some(root),
        result: IndexOperationStepResult::Progressed(_),
    } = missing_root
    else {
        panic!("a missing canonical root is created before any upload")
    };
    let root_input_bytes = root.input_bytes();
    let root_output_bytes = root.output_bytes();
    assert!(root.requires_creation());

    for (limits, expected_limit) in [
        (batch_limits(8, 1, u64::MAX, u64::MAX), 1),
        (batch_limits(8, u64::MAX, u64::MAX, 1), 1),
    ] {
        let PartitionScanSelection::Repository {
            result:
                IndexOperationStepResult::Blocked(IndexOperationBlocker::ManifestLimit {
                    limit, ..
                }),
            ..
        } = scan_partition_documents(
            &transaction,
            scope,
            &operation,
            &definition,
            &progress,
            limits,
            IndexLifecycleScanTuning::default(),
        )
        .await
        .unwrap()
        else {
            panic!("empty-root resource boundaries are durable manifest blockers")
        };
        assert_eq!(limit, expected_limit);
    }
    assert!(root_input_bytes > 1);
    assert!(root_output_bytes > 1);
    let seed_limit = root_input_bytes.saturating_add(1);
    let PartitionScanSelection::Repository {
        result: IndexOperationStepResult::Blocked(IndexOperationBlocker::ManifestLimit { .. }),
        ..
    } = scan_partition_documents(
        &transaction,
        scope,
        &operation,
        &definition,
        &progress,
        batch_limits(8, seed_limit, u64::MAX, u64::MAX),
        IndexLifecycleScanTuning::default(),
    )
    .await
    .unwrap()
    else {
        panic!("root plus first state row is bounded as one seed observation")
    };

    let root_key = scoped_index_key(
        scope,
        ScopedKey::TextManifestRoot(TextManifestRootKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            partition: partition.fingerprint(),
        }),
    );
    transaction
        .put(
            root_key,
            encode_manifest_root(&work::TextManifestRootValue::empty(
                operation.index_id(),
                operation.generation(),
                partition.clone(),
            )),
        )
        .unwrap();
    let PartitionScanSelection::Upload(upload) = scan_partition_documents(
        &transaction,
        scope,
        &operation,
        &definition,
        &progress,
        batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
        IndexLifecycleScanTuning::default(),
    )
    .await
    .unwrap() else {
        panic!("one live current document is an explicit upload selection")
    };
    assert_eq!(upload.partition, partition);
    assert_eq!(upload.documents.len(), 1);
    assert_eq!(upload.completed_cursor.as_bytes(), &state_key);

    transaction
        .put(
            &state_key,
            encode_text_entity_state(&TextEntityStateValue {
                live: false,
                ..state.clone()
            }),
        )
        .unwrap();
    assert!(matches!(
        scan_partition_documents(
            &transaction,
            scope,
            &operation,
            &definition,
            &progress,
            batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
            IndexLifecycleScanTuning::default(),
        )
        .await
        .unwrap(),
        PartitionScanSelection::Repository {
            result: IndexOperationStepResult::Progressed(_),
            ..
        }
    ));
    transaction
        .put(&state_key, encode_text_entity_state(&state))
        .unwrap();
    transaction
        .put(authoritative_property_key(scope, entity), b"malformed")
        .unwrap();
    assert!(matches!(
        scan_partition_documents(
            &transaction,
            scope,
            &operation,
            &definition,
            &progress,
            batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
            IndexLifecycleScanTuning::default(),
        )
        .await
        .unwrap(),
        PartitionScanSelection::Repository {
            result: IndexOperationStepResult::Blocked(
                IndexOperationBlocker::InvalidSourceData { .. }
            ),
            ..
        }
    ));

    let mut wrong_progress = progress.clone();
    wrong_progress.inclusive_upper_bound =
        IndexCursor::try_new(Bytes::from_static(b"wrong")).unwrap();
    assert!(scan_partition_documents(
        &transaction,
        scope,
        &operation,
        &definition,
        &wrong_progress,
        batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
        IndexLifecycleScanTuning::default(),
    )
    .await
    .is_err());
    drop(transaction);
    db.close().await.unwrap();

    let empty_db = Db::open(
        "text-driver-partition-empty-contracts",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let transaction = empty_db.begin(IsolationLevel::Snapshot).await.unwrap();
    let empty_progress =
        initial_partition_scan(&empty_operation, scope, OperationCounters::default()).unwrap();
    assert!(matches!(
        scan_partition_documents(
            &transaction,
            scope,
            &empty_operation,
            &definition,
            &empty_progress,
            batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
            IndexLifecycleScanTuning::default(),
        )
        .await
        .unwrap(),
        PartitionScanSelection::Repository {
            empty_root: Some(_),
            ..
        }
    ));
    let tenant_runtime = crate::config::TextIndexDefinition::new_node("Document", "body")
        .unwrap()
        .with_tenant_property("tenant")
        .unwrap();
    let tenant_definition =
        ValidatedTextIndexDefinition::try_from_runtime(&tenant_runtime).unwrap();
    assert!(matches!(
        scan_partition_documents(
            &transaction,
            scope,
            &empty_operation,
            &tenant_definition,
            &empty_progress,
            batch_limits(8, u64::MAX, u64::MAX, u64::MAX),
            IndexLifecycleScanTuning::default(),
        )
        .await
        .unwrap(),
        PartitionScanSelection::Repository {
            empty_root: None,
            result: IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
                TextBuildProgress::Constructing(TextBuildStage::CatchUp(_))
            )),
        }
    ));
    drop(transaction);
    empty_db.close().await.unwrap();
}

#[tokio::test]
async fn compaction_preparation_requires_a_claim_and_exhaustion_advances_exactly_once() {
    let scope = DataScope::LegacyUnscoped;
    let definition = definition();
    let operation_id = IndexOperationId::new_v4();
    let progress = PrefixScanProgress {
        cursor: None,
        counters: OperationCounters::default(),
    };
    let unclaimed = IndexOperationRecord::try_new(
        operation_id,
        IndexId::initial(),
        definition.identity(),
        IndexGenerationId::initial(),
        IndexRevision::initial(),
        IndexOperationRevision::initial(),
        IndexOperationKind::Build,
        IndexOperationFamily::Text,
        IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::Compact(progress.clone()),
        )),
        0,
        IndexOperationExecutionState::Queued {
            not_before_unix_millis: None,
        },
    )
    .unwrap();
    let claimed = unclaimed
        .clone()
        .claim(OperationClaim {
            writer_epoch: WriterEpoch::from_bytes([9; 16]).unwrap(),
            sequence: ClaimSequence::new(1).unwrap(),
        })
        .unwrap();
    let runtime = TextStorageRuntime {
        object_store: Arc::new(InMemory::new()),
        db_path: "text-driver-empty-compaction".to_string(),
        compaction_limits: crate::config::SearchIndexBackfillLimits::default().text_compaction(),
    };
    let db = Db::open("text-driver-empty-compaction", Arc::new(InMemory::new()))
        .await
        .unwrap();
    assert!(prepare_compaction_step(
        &db,
        scope,
        &unclaimed,
        &progress,
        limits(u64::MAX),
        &runtime,
    )
    .await
    .is_err());

    let dynamic = ValidatedDynamicIndexDefinition::Text(definition.clone());
    let record = IndexRecordV2::building(
        IndexId::initial(),
        dynamic,
        IndexRevision::initial(),
        crate::index_lifecycle::PhysicalGeneration::Text {
            generation: IndexGenerationId::initial(),
        },
        operation_id,
    )
    .unwrap();
    db.put(
        scoped_index_key(scope, ScopedKey::index_record(record.identity().clone())),
        crate::encoding::v2::values::encode_index_record(&record),
    )
    .await
    .unwrap();
    let PreparedTextOperationStep::Repository(prepared) =
        prepare_compaction_step(&db, scope, &claimed, &progress, limits(u64::MAX), &runtime)
            .await
            .unwrap()
    else {
        panic!("an empty compaction lane advances through a repository step")
    };
    assert!(prepared.expected_reads.is_empty());
    assert!(prepared.writes.is_empty());
    assert!(matches!(
        prepared.result,
        IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
            TextBuildProgress::Constructing(TextBuildStage::PrepareManifests(PrefixScanProgress {
                cursor: None,
                ..
            }))
        ))
    ));

    let partition = TextPartition::Unpartitioned;
    let artifact_owner = TextBuildArtifactKey {
        root: TextManifestRootKey {
            index_id: IndexId::initial(),
            generation: IndexGenerationId::initial(),
            partition: partition.fingerprint(),
        },
        ordinal: 0,
    };
    let artifact_key = scoped_index_key(scope, ScopedKey::TextBuildArtifact(artifact_owner));
    let split = work::SplitRef::try_new(
        work::BlobRef::new([3; 32], 1),
        0,
        1,
        0,
        1,
        work::SplitPruning::Unavailable,
    )
    .unwrap();
    db.put(
        artifact_key,
        crate::encoding::v2::values::encode_build_artifact(&work::TextBuildArtifactValue {
            index_id: IndexId::initial(),
            generation: IndexGenerationId::initial(),
            partition,
            artifact_ordinal: 0,
            split,
        }),
    )
    .await
    .unwrap();
    let PreparedTextOperationStep::Repository(prepared) =
        prepare_compaction_step(&db, scope, &claimed, &progress, limits(u64::MAX), &runtime)
            .await
            .unwrap()
    else {
        panic!("an undersized compaction group advances its cursor")
    };
    assert_eq!(prepared.expected_reads.len(), 1);
    assert!(prepared.writes.is_empty());
    assert!(matches!(
        prepared.result,
        IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
            TextBuildProgress::Constructing(TextBuildStage::Compact(PrefixScanProgress {
                cursor: Some(_),
                ..
            }))
        ))
    ));

    let runtime_definition = definition.to_runtime();
    let first_runtime_split = crate::search::text::persist_documents_as_split(
        &runtime.object_store,
        &runtime.db_path,
        &runtime_definition,
        &[
            crate::search::text::TextDocumentInput::new(7, "old searchable document")
                .with_logical_version(1),
        ],
    )
    .await
    .unwrap()
    .expect("first compaction input split is non-empty");
    let second_runtime_split = crate::search::text::persist_documents_as_split(
        &runtime.object_store,
        &runtime.db_path,
        &runtime_definition,
        &[
            crate::search::text::TextDocumentInput::new(7, "current searchable document")
                .with_logical_version(2),
        ],
    )
    .await
    .unwrap()
    .expect("second compaction input split is non-empty");
    let first_split = work::SplitRef::try_new(
        work::BlobRef::new(
            first_runtime_split.blob.sha256,
            first_runtime_split.blob.size_bytes,
        ),
        first_runtime_split.footer_offset,
        first_runtime_split.footer_len,
        first_runtime_split.hotcache_len,
        first_runtime_split.total_size_bytes,
        work::SplitPruning::Unavailable,
    )
    .unwrap();
    let second_split = work::SplitRef::try_new(
        work::BlobRef::new(
            second_runtime_split.blob.sha256,
            second_runtime_split.blob.size_bytes,
        ),
        second_runtime_split.footer_offset,
        second_runtime_split.footer_len,
        second_runtime_split.hotcache_len,
        second_runtime_split.total_size_bytes,
        work::SplitPruning::Unavailable,
    )
    .unwrap();
    for (ordinal, split) in [(0, first_split), (1, second_split)] {
        let owner = TextBuildArtifactKey {
            root: TextManifestRootKey {
                index_id: IndexId::initial(),
                generation: IndexGenerationId::initial(),
                partition: TextPartition::Unpartitioned.fingerprint(),
            },
            ordinal,
        };
        db.put(
            scoped_index_key(scope, ScopedKey::TextBuildArtifact(owner)),
            crate::encoding::v2::values::encode_build_artifact(&work::TextBuildArtifactValue {
                index_id: IndexId::initial(),
                generation: IndexGenerationId::initial(),
                partition: TextPartition::Unpartitioned,
                artifact_ordinal: ordinal,
                split,
            }),
        )
        .await
        .unwrap();
    }
    let entity = crate::encoding::v2::keys::IndexEntity {
        kind: crate::index_lifecycle::IndexElementKind::Node,
        id: crate::index_lifecycle::IndexEntityId::new(7),
    };
    db.put(
        scoped_index_key(
            scope,
            ScopedKey::TextEntityState(crate::encoding::v2::keys::TextEntityStateKey {
                root: TextManifestRootKey {
                    index_id: IndexId::initial(),
                    generation: IndexGenerationId::initial(),
                    partition: TextPartition::Unpartitioned.fingerprint(),
                },
                entity,
            }),
        ),
        crate::encoding::v2::values::encode_text_entity_state(&work::TextEntityStateValue {
            index_id: IndexId::initial(),
            generation: IndexGenerationId::initial(),
            partition: TextPartition::Unpartitioned,
            entity_kind: crate::index_lifecycle::IndexElementKind::Node,
            entity_id: entity.id,
            logical_version: crate::index_lifecycle::TextLogicalVersion::new(2).unwrap(),
            live: true,
        }),
    )
    .await
    .unwrap();
    let PreparedTextOperationStep::CompactionUpload(prepared) =
        prepare_compaction_step(&db, scope, &claimed, &progress, limits(u64::MAX), &runtime)
            .await
            .unwrap()
    else {
        panic!("two live-versioned splits produce one exact compaction upload")
    };
    assert_eq!(prepared.retired_artifact_keys.len(), 2);
    assert!(prepared.uploaded_bytes > 0);
    assert!(prepared.expected_reads.len() >= 3);
    db.close().await.unwrap();
}
