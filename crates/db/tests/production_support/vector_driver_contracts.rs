//! Production contracts for the vector lifecycle driver's step boundaries.
//!
//! This child of the vector lifecycle driver runs its build, cleanup, legacy
//! adoption, and planning scenarios against raw SlateDB databases through the
//! production outbox claim and step entry points. The driver's unit tests
//! share these fixtures and run every scenario, and the production-coverage
//! build runs the same scenarios through [`run`]. Every fixture row uses the
//! current key and value codecs; no row family or encoding is introduced.

use std::num::{NonZeroU64, NonZeroUsize};

use slatedb::object_store::memory::InMemory;

use super::*;
use crate::config::{SearchIndexBackfillLimits, VectorIndexDefinition};
use crate::encoding::property::property_value::PropertyValue;
use crate::encoding::property::Property;
use crate::encoding::v2::keys::NodePropertyKey;
use crate::encoding::v2::values::encode_build_delta;
use crate::encoding::v2::values::property::encode_properties;
use crate::index_lifecycle::lifecycle::{
    create_index_operation, create_legacy_vector_adoption_operation, drop_index_operation,
    InitialBuildProgress,
};
use crate::index_lifecycle::outbox::{
    claim_operation, execute_claimed_step, observe_operation_pointer, ClaimPermission,
    CommittedOperationStep, OperationPointerObservation,
};
use crate::index_lifecycle::repository::peek_vector_physical_id;
use crate::index_lifecycle::{
    ActiveIndexHandle, ClaimSequence, IndexDdlReceipt, IndexOperationId, IndexScopeGates,
    IndexStateV2, WriterEpoch,
};
use crate::migrations::startup::bootstrap_writer;
use crate::search::vector::{
    SimHasherRegistry, ValidatedVectorGenerationHandle, VectorCacheRegistry,
};

/// Runs every driver step-boundary scenario in isolated databases.
#[cfg(not(test))]
pub(crate) async fn run() {
    diagnostic_and_error_adapters_preserve_their_error_categories().await;
    partitioned_drop_resumes_each_mapping_and_removes_every_namespace().await;
    cleanup_checkpoint_rejections_and_limit_blockers_are_typed().await;
    source_scan_rejects_every_preplanning_boundary().await;
    descriptor_validation_cursor_dispatch_is_typed().await;
    pre_queue_build_deltas_block_catch_up_validation_and_activation().await;
    typed_row_decoders_and_batch_accounting_fail_closed().await;
    oversized_partition_build_blocks_before_mapping_or_watermark_writes().await;
    abort_removes_hidden_physical_rows_and_builder_work().await;
    adoption_abort_restores_source_reservation_without_deleting_physical_rows().await;
    a_physical_id_allocated_before_planning_conflicts_and_a_stale_watermark_fails_closed().await;
    cleanup_batches_resume_and_block_on_oversized_rows().await;
    descriptor_validation_batches_and_blocks().await;
    adoption_and_activation_guards_fail_closed().await;
}

const NOW_MILLIS: u64 = 1;

pub(super) async fn test_db(name: &str) -> Db {
    let db = Db::builder(name, Arc::new(InMemory::new()))
        .build()
        .await
        .expect("vector driver test database opens");
    bootstrap_writer(&db)
        .await
        .expect("vector driver test database bootstraps V2 metadata");
    db
}

pub(super) fn driver() -> VectorIndexDriver {
    VectorIndexDriver::new(
        Arc::new(IndexScopeGates::default()),
        Arc::new(VectorCacheRegistry::default()),
        Arc::new(SimHasherRegistry::default()),
    )
}

pub(super) fn definition(tenant_property: Option<&str>) -> ValidatedDynamicIndexDefinition {
    let runtime = VectorIndexDefinition::new_node(
        "Document",
        "embedding",
        3,
        VectorDistanceMetric::Euclidean,
    )
    .expect("vector definition validates");
    let runtime = match tenant_property {
        Some(tenant_property) => runtime
            .with_tenant_property(tenant_property)
            .expect("tenant property validates"),
        None => runtime,
    };
    ValidatedDynamicIndexDefinition::Vector(
        ValidatedVectorIndexDefinition::try_from_runtime(&runtime)
            .expect("V2 vector definition validates"),
    )
}

pub(super) fn properties(vector: [f32; 3], tenant: Option<i64>) -> Vec<Property> {
    let mut properties = vec![
        Property::new("$label", PropertyValue::String("Document".to_string())),
        Property::new("embedding", PropertyValue::F32Array(vector.to_vec())),
    ];
    if let Some(tenant) = tenant {
        properties.push(Property::new("account_id", PropertyValue::I64(tenant)));
    }
    properties
}

pub(super) fn source_key(scope: DataScope, entity_id: u64) -> Bytes {
    DataKey::Data {
        scope,
        kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity_id)),
    }
    .to_bytes()
}

pub(super) fn source_cursor(scope: DataScope, entity_id: u64) -> IndexCursor {
    IndexCursor::try_new(source_key(scope, entity_id)).expect("source key is a valid cursor")
}

pub(super) async fn put_source(db: &Db, scope: DataScope, entity_id: u64, properties: &[Property]) {
    db.put(source_key(scope, entity_id), encode_properties(properties))
        .await
        .expect("vector source is written");
}

pub(super) async fn create_build(
    db: &Db,
    scope: DataScope,
    definition: &ValidatedDynamicIndexDefinition,
    upper_entity_id: u64,
) -> (IndexOperationId, IndexId, IndexGenerationId) {
    let receipt = create_index_operation(
        db,
        scope,
        definition.clone(),
        helix_planner::ir::IndexCreateMode::ErrorIfExists,
        InitialBuildProgress::vector(source_cursor(scope, upper_entity_id)),
    )
    .await
    .expect("vector build is enqueued");
    let IndexDdlReceipt::Accepted {
        operation_id,
        index_id,
        generation,
    } = receipt
    else {
        panic!("new vector definition must enqueue a build");
    };
    (operation_id, index_id, generation)
}

pub(super) async fn drive_one(
    db: &Db,
    driver: &VectorIndexDriver,
    operation_id: IndexOperationId,
    claim_sequence: &mut u64,
    limits: SearchIndexBatchLimits,
) -> CommittedOperationStep {
    let writer_epoch = WriterEpoch::from_bytes([0x6B; 16]).expect("writer epoch is non-nil");
    let observation = observe_operation_pointer(db, operation_id, writer_epoch, NOW_MILLIS)
        .await
        .expect("vector operation pointer is readable");
    let OperationPointerObservation::Eligible(eligible) = observation else {
        panic!("queued vector operation must be eligible: {observation:?}");
    };
    let sequence = ClaimSequence::new(*claim_sequence).expect("claim sequence is non-zero");
    *claim_sequence = claim_sequence
        .checked_add(1)
        .expect("claim sequence remains bounded");
    let claimed = claim_operation(
        db,
        &eligible,
        writer_epoch,
        sequence,
        NOW_MILLIS,
        ClaimPermission::Normal,
    )
    .await
    .expect("vector claim succeeds")
    .expect("vector revision is claimable");
    execute_claimed_step(db, &claimed, driver, limits, NOW_MILLIS)
        .await
        .expect("vector step commits")
}

pub(super) async fn drive_to_terminal(
    db: &Db,
    driver: &VectorIndexDriver,
    operation_id: IndexOperationId,
    claim_sequence: &mut u64,
) -> CommittedOperationStep {
    for _ in 0..64 {
        let step = drive_one(
            db,
            driver,
            operation_id,
            claim_sequence,
            SearchIndexBackfillLimits::default().batch(),
        )
        .await;
        if step != CommittedOperationStep::Progressed {
            return step;
        }
    }
    panic!("vector operation exceeded bounded test checkpoints")
}

pub(super) async fn read_index(
    db: &Db,
    scope: DataScope,
    definition: &ValidatedDynamicIndexDefinition,
) -> IndexRecordV2 {
    let key = scoped_index_key(scope, ScopedKey::index_record(definition.identity()));
    let value = db
        .get(key)
        .await
        .expect("canonical vector index is readable")
        .expect("canonical vector index exists");
    decode_index_record(&value).expect("canonical vector index decodes")
}

pub(super) async fn read_operation(
    db: &Db,
    scope: DataScope,
    operation_id: IndexOperationId,
) -> IndexOperationRecord {
    let value = db
        .get(crate::index_lifecycle::outbox::scoped_operation_key(
            scope,
            operation_id,
        ))
        .await
        .expect("vector operation is readable")
        .expect("vector operation exists");
    crate::encoding::v2::values::decode_operation_record(&value).expect("vector operation decodes")
}

pub(super) async fn mapping_values(
    db: &Db,
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
) -> Vec<crate::index_lifecycle::work::VectorPartitionMappingValue> {
    let prefix = generation_prefix(
        scope,
        RecordKind::VectorPartitionMapping,
        index_id,
        generation,
    );
    let mut rows = db
        .scan_prefix(prefix, ..)
        .await
        .expect("vector mappings are readable");
    let mut values = Vec::new();
    while let Some(row) = rows.next().await.expect("vector mapping row is readable") {
        let value = decode_partition_mapping(&row.value).expect("vector mapping value decodes");
        values.push(value);
    }
    values
}

/// Plans `entity_id` into a fresh tenant partition of the build `operation`,
/// reading the watermark through `transaction` and rows through a planning
/// snapshot opened now.
async fn plan_new_partition(
    db: &Db,
    transaction: &DbTransaction,
    operation: &IndexOperationRecord,
    record: &IndexRecordV2,
    definition: &ValidatedVectorIndexDefinition,
    tenant: i64,
) -> Result<EntityPlanOutcome> {
    let planning = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let document = vector_document(definition, &properties([1.0, 2.0, 3.0], Some(tenant)))
        .unwrap()
        .unwrap();
    plan_and_apply::<vector::distance::Euclidean>(
        &planning,
        &VectorWriteRecorder::new(),
        transaction,
        &VectorPlanTarget::build(DataScope::LegacyUnscoped, operation, record)?,
        definition,
        Arc::new(SimHasherRegistry::default()),
        crate::batch_reads::BatchReads::Single,
        IndexEntityId::new(1),
        &[],
        Some(&document),
        &VectorBatchAccounting::new(
            OperationCounters::default(),
            SearchIndexBackfillLimits::default().batch(),
        ),
        &mut driver().build_cache.checkout_fresh().await,
    )
    .await
}

/// Exercises diagnostic and typed-error adapters that sit below the
/// lifecycle state machine but still belong to the production surface.
pub(super) async fn diagnostic_and_error_adapters_preserve_their_error_categories() {
    assert!(format!("{:?}", driver()).contains("VectorIndexDriver"));
    assert!(matches!(
        invalid_source(IndexElementKind::Node, IndexEntityId::initial()),
        IndexOperationStepResult::Blocked(IndexOperationBlocker::InvalidSourceData {
            entity_kind: IndexElementKind::Node,
            entity_id,
        }) if entity_id == IndexEntityId::initial()
    ));
    assert!(matches!(
        checked_add(u64::MAX, 1, "fixture"),
        Err(HelixDbError::IndexCatalogCorruption(reason)) if reason.contains("fixture")
    ));
    let db = test_db("vector-driver-error-adapters").await;
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let first = MeasuredVectorTransaction::new(&transaction);
    let foreign_checkpoint = first.checkpoint();
    let second = MeasuredVectorTransaction::new(&transaction);
    let measurement_failure = second
        .plan_since(foreign_checkpoint)
        .expect_err("checkpoint belongs to another recorder");
    assert!(matches!(
        measurement_error(measurement_failure),
        HelixDbError::IndexCatalogCorruption(reason) if reason.contains("measurement")
    ));
    assert!(matches!(
        corruption("fixture corruption"),
        HelixDbError::IndexCatalogCorruption(reason) if reason == "fixture corruption"
    ));
    assert!(matches!(
        operation_error(crate::index_lifecycle::IndexOperationModelError::OversizedCursor {
            actual: 2,
            maximum: 1,
        }),
        HelixDbError::InvariantViolation(reason) if reason.contains("cursor")
    ));
    drop(transaction);
    db.close().await.expect("vector test database closes");
}

/// Proves partition mappings remain the cleanup cursor until their entire
/// physical namespace is gone, including one-row batch restarts.
pub(super) async fn partitioned_drop_resumes_each_mapping_and_removes_every_namespace() {
    let db = test_db("vector-driver-partitioned-drop").await;
    let scope = DataScope::LegacyUnscoped;
    let definition = definition(Some("account_id"));
    put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], Some(10))).await;
    put_source(&db, scope, 1, &properties([4.0, 5.0, 6.0], Some(20))).await;
    let (build_id, index_id, generation) = create_build(&db, scope, &definition, 1).await;
    let driver = driver();
    let mut claim_sequence = 1;
    assert_eq!(
        drive_to_terminal(&db, &driver, build_id, &mut claim_sequence).await,
        CommittedOperationStep::Completed
    );
    let active = read_index(&db, scope, &definition).await;
    let active_handle = ActiveIndexHandle::try_from_record(scope, &active)
        .expect("partitioned Active record projects a handle");
    let mappings = mapping_values(&db, scope, index_id, generation).await;
    assert_eq!(mappings.len(), 2);
    let indexes = mappings
        .iter()
        .map(|mapping| {
            let generation = ValidatedVectorGenerationHandle::try_from_active::<
                vector::distance::Euclidean,
            >(&active_handle, mapping.physical_index_id)
            .expect("partition mapping validates against the Active handle");
            VectorIndex::<vector::distance::Euclidean>::from_generation(&generation)
        })
        .collect::<Vec<_>>();

    let IndexDdlReceipt::Accepted {
        operation_id: drop_id,
        ..
    } = drop_index_operation(&db, scope, &definition)
        .await
        .expect("partitioned drop enqueues")
    else {
        panic!("partitioned Active drop creates a cleanup operation");
    };
    let one_entity = SearchIndexBatchLimits::try_new(
        NonZeroUsize::MIN,
        NonZeroU64::new(1024 * 1024).unwrap(),
        NonZeroU64::new(1024).unwrap(),
        NonZeroU64::new(16 * 1024 * 1024).unwrap(),
        NonZeroU64::new(16 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    for _ in 0..64 {
        let step = drive_one(&db, &driver, drop_id, &mut claim_sequence, one_entity).await;
        if step == CommittedOperationStep::Completed {
            break;
        }
        assert_eq!(step, CommittedOperationStep::Progressed);
    }
    assert!(matches!(
        read_index(&db, scope, &definition).await.state(),
        IndexStateV2::Dropped { .. }
    ));
    assert!(mapping_values(&db, scope, index_id, generation)
        .await
        .is_empty());
    for index in indexes {
        assert!(index
            .cleanup_scan(&db)
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .is_none());
    }
    db.close().await.expect("vector test database closes");
}

/// Exercises every cleanup checkpoint rejection before the outbox is
/// allowed to commit a new durable progress value.
pub(super) async fn cleanup_checkpoint_rejections_and_limit_blockers_are_typed() {
    let db = test_db("vector-driver-cleanup-boundaries").await;
    let scope = DataScope::LegacyUnscoped;
    let definition = definition(None);
    put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], None)).await;
    let (build_id, _, _) = create_build(&db, scope, &definition, 0).await;
    let driver = driver();
    let mut claim_sequence = 1;
    assert_eq!(
        drive_to_terminal(&db, &driver, build_id, &mut claim_sequence).await,
        CommittedOperationStep::Completed
    );
    let IndexDdlReceipt::Accepted {
        operation_id: drop_id,
        ..
    } = drop_index_operation(&db, scope, &definition)
        .await
        .expect("drop operation enqueues")
    else {
        panic!("Active vector drop creates a cleanup operation");
    };
    let record = read_index(&db, scope, &definition).await;
    let operation = crate::index_lifecycle::outbox::read_operation(&db, scope, drop_id)
        .await
        .unwrap()
        .expect("drop operation exists");
    let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
        unreachable!("fixture definition is vector");
    };
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let limits = SearchIndexBackfillLimits::default().batch();

    let stale_cursor = IndexCursor::try_new(Bytes::from_static(b"stale-cursor")).unwrap();
    for progress in [
        VectorCleanupProgress::DeletePhysical(PrefixScanProgress {
            cursor: Some(stale_cursor.clone()),
            counters: OperationCounters::default(),
        }),
        VectorCleanupProgress::DeleteDeltas(PrefixScanProgress {
            cursor: Some(stale_cursor),
            counters: OperationCounters::default(),
        }),
    ] {
        assert!(matches!(
            step_cleanup::<vector::distance::Euclidean>(
                &transaction,
                scope,
                &operation,
                &record,
                vector_definition,
                &progress,
                false,
                limits,
                driver.cache_registry.as_ref(),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
    }

    let tiny = SearchIndexBatchLimits::try_new(
        NonZeroUsize::MIN,
        NonZeroU64::MIN,
        NonZeroU64::MIN,
        NonZeroU64::MIN,
        NonZeroU64::MIN,
    )
    .unwrap();
    assert!(matches!(
        step_cleanup::<vector::distance::Euclidean>(
            &transaction,
            scope,
            &operation,
            &record,
            vector_definition,
            &VectorCleanupProgress::DeletePhysical(PrefixScanProgress {
                cursor: None,
                counters: OperationCounters::default(),
            }),
            false,
            tiny,
            driver.cache_registry.as_ref(),
        )
        .await
        .unwrap()
        .result,
        IndexOperationStepResult::Blocked(IndexOperationBlocker::OversizedEntity { .. })
    ));
    assert!(matches!(
        step_cleanup::<vector::distance::Euclidean>(
            &transaction,
            scope,
            &operation,
            &record,
            vector_definition,
            &VectorCleanupProgress::RetireCache(NoCursorProgress::default()),
            false,
            limits,
            driver.cache_registry.as_ref(),
        )
        .await
        .unwrap()
        .result,
        IndexOperationStepResult::Progressed(IndexOperationProgress::VectorCleanup(
            VectorCleanupProgress::DeletePhysical(_)
        ))
    ));
    assert!(matches!(
        step_cleanup::<vector::distance::Euclidean>(
            &transaction,
            scope,
            &operation,
            &record,
            vector_definition,
            &VectorCleanupProgress::Finalize(NoCursorProgress::default()),
            false,
            limits,
            driver.cache_registry.as_ref(),
        )
        .await
        .unwrap()
        .result,
        IndexOperationStepResult::Completed(IndexOperationOutcome::DropSucceeded)
    ));
    drop(transaction);
    db.close().await.expect("vector test database closes");
}

/// Covers source-bound ordering, indivisible input admission, malformed
/// documents, and duplicate applied-state rejection before HNSW planning.
pub(super) async fn source_scan_rejects_every_preplanning_boundary() {
    let db = test_db("vector-driver-source-boundaries").await;
    let scope = DataScope::LegacyUnscoped;
    let definition = definition(None);
    put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], None)).await;
    let (operation_id, _, _) = create_build(&db, scope, &definition, 0).await;
    let operation = crate::index_lifecycle::outbox::read_operation(&db, scope, operation_id)
        .await
        .unwrap()
        .expect("build operation exists");
    let record = read_index(&db, scope, &definition).await;
    let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
        unreachable!("fixture definition is vector");
    };
    let IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
        VectorBuildStage::Scan(initial_progress),
    )) = operation.progress()
    else {
        panic!("new vector build begins at source scan");
    };
    let driver = driver();
    let limits = SearchIndexBackfillLimits::default().batch();

    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let equal = SourceScanProgress {
        inclusive_upper_bound: initial_progress.inclusive_upper_bound.clone(),
        cursor: Some(initial_progress.inclusive_upper_bound.clone()),
        counters: OperationCounters::default(),
    };
    assert!(matches!(
        scan_source::<vector::distance::Euclidean>(
            &db,
            &transaction,
            scope,
            &operation,
            &record,
            vector_definition,
            &equal,
            limits,
            IndexLifecycleScanTuning::default(),
            Arc::clone(&driver.simhasher_registry),
            driver.batch_reads,
            &mut driver.build_cache.checkout_fresh().await,
        )
        .await
        .unwrap()
        .result,
        IndexOperationStepResult::Progressed(IndexOperationProgress::VectorBuild(
            VectorBuildProgress::Constructing(VectorBuildStage::ValidateDescriptor(_))
        ))
    ));
    let greater = SourceScanProgress {
        inclusive_upper_bound: initial_progress.inclusive_upper_bound.clone(),
        cursor: Some(source_cursor(scope, 1)),
        counters: OperationCounters::default(),
    };
    assert!(matches!(
        scan_source::<vector::distance::Euclidean>(
            &db,
            &transaction,
            scope,
            &operation,
            &record,
            vector_definition,
            &greater,
            limits,
            IndexLifecycleScanTuning::default(),
            Arc::clone(&driver.simhasher_registry),
            driver.batch_reads,
            &mut driver.build_cache.checkout_fresh().await,
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    drop(transaction);

    let tiny = SearchIndexBatchLimits::try_new(
        NonZeroUsize::MIN,
        NonZeroU64::MIN,
        NonZeroU64::new(1024).unwrap(),
        NonZeroU64::new(1024).unwrap(),
        NonZeroU64::new(1024).unwrap(),
    )
    .unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert!(matches!(
        scan_source::<vector::distance::Euclidean>(
            &db,
            &transaction,
            scope,
            &operation,
            &record,
            vector_definition,
            initial_progress,
            tiny,
            IndexLifecycleScanTuning::default(),
            Arc::clone(&driver.simhasher_registry),
            driver.batch_reads,
            &mut driver.build_cache.checkout_fresh().await,
        )
        .await
        .unwrap()
        .result,
        IndexOperationStepResult::Blocked(IndexOperationBlocker::OversizedEntity {
            entity_id,
            ..
        }) if entity_id == IndexEntityId::new(0)
    ));
    drop(transaction);

    db.put(source_key(scope, 0), Bytes::from_static(&[0xff]))
        .await
        .unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert!(matches!(
        scan_source::<vector::distance::Euclidean>(
            &db,
            &transaction,
            scope,
            &operation,
            &record,
            vector_definition,
            initial_progress,
            limits,
            IndexLifecycleScanTuning::default(),
            Arc::clone(&driver.simhasher_registry),
            driver.batch_reads,
            &mut driver.build_cache.checkout_fresh().await,
        )
        .await
        .unwrap()
        .result,
        IndexOperationStepResult::Blocked(IndexOperationBlocker::InvalidSourceData {
            entity_id,
            ..
        }) if entity_id == IndexEntityId::new(0)
    ));
    drop(transaction);

    let wrong_dimension = vec![
        Property::new("$label", PropertyValue::String("Document".to_string())),
        Property::new("embedding", PropertyValue::F32Array(vec![1.0, 2.0])),
    ];
    put_source(&db, scope, 0, &wrong_dimension).await;
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert!(matches!(
        scan_source::<vector::distance::Euclidean>(
            &db,
            &transaction,
            scope,
            &operation,
            &record,
            vector_definition,
            initial_progress,
            limits,
            IndexLifecycleScanTuning::default(),
            Arc::clone(&driver.simhasher_registry),
            driver.batch_reads,
            &mut driver.build_cache.checkout_fresh().await,
        )
        .await
        .unwrap()
        .result,
        IndexOperationStepResult::Blocked(IndexOperationBlocker::InvalidSourceData { .. })
    ));
    drop(transaction);

    put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], None)).await;
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    stage_applied(
        &transaction,
        scope,
        &operation,
        IndexElementKind::Node,
        IndexEntityId::new(0),
        Some(TextPartition::Unpartitioned),
    )
    .unwrap();
    assert!(matches!(
        scan_source::<vector::distance::Euclidean>(
            &db,
            &transaction,
            scope,
            &operation,
            &record,
            vector_definition,
            initial_progress,
            limits,
            IndexLifecycleScanTuning::default(),
            Arc::clone(&driver.simhasher_registry),
            driver.batch_reads,
            &mut driver.build_cache.checkout_fresh().await,
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(reason))
            if reason.contains("existing applied state")
    ));
    drop(transaction);
    db.close().await.expect("vector test database closes");
}

/// Verifies descriptor-validation cursors fail on malformed bytes and
/// dispatch typed non-V2 and mapping keys to their exact scan lanes.
pub(super) async fn descriptor_validation_cursor_dispatch_is_typed() {
    let db = test_db("vector-driver-validation-cursors").await;
    let scope = DataScope::LegacyUnscoped;
    let definition = definition(None);
    let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
    let operation = crate::index_lifecycle::outbox::read_operation(&db, scope, operation_id)
        .await
        .unwrap()
        .expect("build operation exists");
    let record = read_index(&db, scope, &definition).await;
    let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
        unreachable!("fixture definition is vector");
    };
    let limits = SearchIndexBackfillLimits::default().batch();
    let driver = driver();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();

    let malformed = PrefixScanProgress {
        cursor: Some(
            IndexCursor::try_new(Bytes::from_static(b"malformed"))
                .expect("malformed bytes still fit the cursor envelope"),
        ),
        counters: OperationCounters::default(),
    };
    assert!(validate_descriptor::<vector::distance::Euclidean>(
        &db,
        &transaction,
        scope,
        &operation,
        &record,
        vector_definition,
        &malformed,
        limits,
        Arc::clone(&driver.simhasher_registry),
    )
    .await
    .is_err());

    for cursor in [
        source_cursor(scope, 0),
        IndexCursor::try_new({
            let mut storage_version = bytes::BytesMut::new();
            GlobalKey::StorageVersion.encode_into(&mut storage_version);
            storage_version.freeze()
        })
        .expect("storage-version key is a bounded cursor"),
        IndexCursor::try_new(
            IndexKey::Global {
                kind: GlobalKey::StorageVersion,
            }
            .to_bytes(),
        )
        .expect("managed storage-version key is a bounded cursor"),
    ] {
        let progress = PrefixScanProgress {
            cursor: Some(cursor),
            counters: OperationCounters::default(),
        };
        validate_descriptor::<vector::distance::Euclidean>(
            &db,
            &transaction,
            scope,
            &operation,
            &record,
            vector_definition,
            &progress,
            limits,
            Arc::clone(&driver.simhasher_registry),
        )
        .await
        .expect_err("typed non-V2 cursor cannot resume the applied-state lane");
    }
    let mapping = PrefixScanProgress {
        cursor: Some(
            IndexCursor::try_new(scoped_index_key(
                scope,
                ScopedKey::VectorPartitionMapping(
                    crate::encoding::v2::keys::VectorPartitionMappingKey {
                        index_id,
                        generation,
                        partition: TextPartition::Unpartitioned.fingerprint(),
                    },
                ),
            ))
            .expect("mapping key is a bounded cursor"),
        ),
        counters: OperationCounters::default(),
    };
    validate_descriptor::<vector::distance::Euclidean>(
        &db,
        &transaction,
        scope,
        &operation,
        &record,
        vector_definition,
        &mapping,
        limits,
        Arc::clone(&driver.simhasher_registry),
    )
    .await
    .expect("typed mapping cursor resumes mapping validation");
    drop(transaction);
    db.close().await.expect("vector test database closes");
}

/// A build started before operations were queued may hold `BuildDelta`
/// rows that nothing replays; every stage that could activate them
/// blocks. Without a delta the same stages move on, so each block is the
/// delta's.
pub(super) async fn pre_queue_build_deltas_block_catch_up_validation_and_activation() {
    let db = test_db("vector-driver-pre-queue-deltas").await;
    let scope = DataScope::LegacyUnscoped;
    let definition = definition(None);
    let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
    let operation = read_operation(&db, scope, operation_id).await;
    let record = read_index(&db, scope, &definition).await;
    let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
        unreachable!("fixture definition is vector");
    };
    let driver = driver();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let counters = OperationCounters::default();
    let catch_up = VectorBuildStage::CatchUp(PrefixScanProgress {
        cursor: None,
        counters,
    });
    let validate = VectorBuildStage::ValidateDescriptor(PrefixScanProgress {
        cursor: None,
        counters,
    });
    let activate = VectorBuildStage::Activate(NoCursorProgress { counters });
    let step = |stage: VectorBuildStage| {
        let (db, transaction, operation, record, driver) =
            (&db, &transaction, &operation, &record, &driver);
        let registry = Arc::clone(&driver.simhasher_registry);
        async move {
            step_build::<vector::distance::Euclidean>(
                db,
                transaction,
                scope,
                operation,
                record,
                vector_definition,
                &stage,
                SearchIndexBackfillLimits::default().batch(),
                IndexLifecycleScanTuning::default(),
                registry,
                driver.batch_reads,
                &driver.build_cache,
            )
            .await
            .expect("vector build step runs")
            .result
        }
    };

    assert!(matches!(
        step(catch_up.clone()).await,
        IndexOperationStepResult::Progressed(IndexOperationProgress::VectorBuild(
            VectorBuildProgress::Constructing(VectorBuildStage::ValidateDescriptor(_))
        ))
    ));
    assert!(matches!(
        step(validate.clone()).await,
        IndexOperationStepResult::Progressed(IndexOperationProgress::VectorBuild(
            VectorBuildProgress::Constructing(VectorBuildStage::Activate(_))
        ))
    ));
    assert!(matches!(
        step(activate.clone()).await,
        IndexOperationStepResult::Completed(IndexOperationOutcome::Build(
            BuildOperationOutcome::Succeeded
        ))
    ));

    let entity = IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(0),
    };
    transaction
        .put(
            scoped_index_key(
                scope,
                ScopedKey::BuildDelta(IndexEntityStateKey {
                    index_id,
                    generation,
                    entity,
                }),
            ),
            encode_build_delta(&CoalescedBuildDeltaValue {
                index_id,
                generation,
                entity_kind: entity.kind,
                entity_id: entity.id,
                state: crate::index_lifecycle::work::CoalescedBuildDeltaState::Marker,
            }),
        )
        .unwrap();
    for stage in [catch_up, validate, activate] {
        assert!(
            matches!(
                step(stage.clone()).await,
                IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation)
            ),
            "{stage:?} must block on a pre-queue build delta"
        );
    }
    drop(transaction);
    db.close().await.expect("vector test database closes");
}

/// Drives every typed vector work-row decoder through wrong-key,
/// wrong-value, and key/value-ownership failures and covers helper states
/// that normal lifecycle construction makes unreachable.
pub(super) async fn typed_row_decoders_and_batch_accounting_fail_closed() {
    let db = test_db("vector-driver-row-boundaries").await;
    let scope = DataScope::LegacyUnscoped;
    let definition = definition(None);
    put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], None)).await;
    let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
    let operation = crate::index_lifecycle::outbox::read_operation(&db, scope, operation_id)
        .await
        .unwrap()
        .expect("build operation exists");
    let entity = IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(0),
    };
    let other_entity = IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(1),
    };
    let delta_key = scoped_index_key(
        scope,
        ScopedKey::BuildDelta(IndexEntityStateKey {
            index_id,
            generation,
            entity,
        }),
    );
    let applied_key = applied_key(
        scope,
        index_id,
        generation,
        IndexElementKind::Node,
        IndexEntityId::new(0),
    );
    let delta_value = CoalescedBuildDeltaValue {
        index_id,
        generation,
        entity_kind: entity.kind,
        entity_id: entity.id,
        state: crate::index_lifecycle::work::CoalescedBuildDeltaState::Marker,
    };
    let applied_value = AppliedEntityStateValue {
        index_id,
        generation,
        entity_kind: entity.kind,
        entity_id: entity.id,
        state: AppliedFamilyState::Vector(Some(TextPartition::Unpartitioned)),
    };
    let encoded_delta = encode_build_delta(&delta_value);
    let encoded_applied = encode_applied_state(&applied_value.clone());

    assert!(matches!(
        decode_delta(scope, &applied_key, &encoded_delta),
        Err(HelixDbError::IndexCatalogCorruption(reason))
            if reason.contains("another key kind")
    ));
    assert!(matches!(
        decode_delta(scope, &delta_key, &encoded_applied),
        Err(HelixDbError::IndexCatalogCorruption(reason))
            if reason.contains("another value kind")
    ));
    let mismatched_delta = encode_build_delta(&CoalescedBuildDeltaValue {
        entity_id: other_entity.id,
        ..delta_value
    });
    assert!(matches!(
        decode_delta(scope, &delta_key, &mismatched_delta),
        Err(HelixDbError::IndexCatalogCorruption(reason))
            if reason.contains("key/value mismatch")
    ));

    assert!(matches!(
        decode_applied(scope, &delta_key, &encoded_applied),
        Err(HelixDbError::IndexCatalogCorruption(reason))
            if reason.contains("another key kind")
    ));
    assert!(matches!(
        decode_applied(scope, &applied_key, &encoded_delta),
        Err(HelixDbError::IndexCatalogCorruption(reason))
            if reason.contains("another value kind")
    ));
    let mismatched_applied = encode_applied_state(&AppliedEntityStateValue {
        entity_id: other_entity.id,
        ..applied_value.clone()
    });
    assert!(matches!(
        decode_applied(scope, &applied_key, &mismatched_applied),
        Err(HelixDbError::IndexCatalogCorruption(reason))
            if reason.contains("key/value mismatch")
    ));

    let tenant = VectorTenantPartition::try_new(Bytes::from_static(b"tenant")).unwrap();
    let mapping_key = scoped_index_key(
        scope,
        ScopedKey::VectorPartitionMapping(crate::encoding::v2::keys::VectorPartitionMappingKey {
            index_id,
            generation,
            partition: tenant.fingerprint(),
        }),
    );
    let mapping_value = crate::index_lifecycle::work::VectorPartitionMappingValue {
        index_id,
        generation,
        partition: tenant.clone(),
        physical_index_id: VectorPhysicalIndexId::initial(),
    };
    let encoded_mapping = encode_partition_mapping(&mapping_value.clone());
    assert!(matches!(
        decode_mapping(scope, &delta_key, &encoded_mapping, &operation),
        Err(HelixDbError::IndexCatalogCorruption(reason))
            if reason.contains("another key kind")
    ));
    assert!(matches!(
        decode_mapping(scope, &mapping_key, &encoded_delta, &operation),
        Err(HelixDbError::IndexCatalogCorruption(reason))
            if reason.contains("another value kind")
    ));
    let mismatched_mapping =
        encode_partition_mapping(&crate::index_lifecycle::work::VectorPartitionMappingValue {
            index_id: IndexId::new(index_id.get() + 1).unwrap(),
            ..mapping_value
        });
    assert!(matches!(
        decode_mapping(scope, &mapping_key, &mismatched_mapping, &operation),
        Err(HelixDbError::IndexCatalogCorruption(reason))
            if reason.contains("ownership mismatch")
    ));

    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(applied_key.clone(), mismatched_applied)
        .unwrap();
    assert!(matches!(
        load_applied(
            &transaction,
            scope,
            index_id,
            generation,
            entity.kind,
            entity.id,
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(reason))
            if reason.contains("key/value mismatch")
    ));
    transaction
        .put(
            applied_key.clone(),
            encode_applied_state(&AppliedEntityStateValue {
                state: AppliedFamilyState::Secondary(None),
                ..applied_value
            }),
        )
        .unwrap();
    assert!(matches!(
        load_applied(
            &transaction,
            scope,
            index_id,
            generation,
            entity.kind,
            entity.id,
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(reason))
            if reason.contains("another applied family")
    ));
    stage_applied(
        &transaction,
        scope,
        &operation,
        entity.kind,
        entity.id,
        None,
    )
    .unwrap();
    assert!(load_applied(
        &transaction,
        scope,
        index_id,
        generation,
        entity.kind,
        entity.id,
    )
    .await
    .unwrap()
    .is_none());
    assert!(!generation_has_rows(
        &transaction,
        scope,
        RecordKind::VectorPartitionMapping,
        index_id,
        generation,
    )
    .await
    .unwrap());
    transaction
        .put(delta_key.clone(), encoded_delta.clone())
        .unwrap();
    assert!(generation_has_rows(
        &transaction,
        scope,
        RecordKind::BuildDelta,
        index_id,
        generation,
    )
    .await
    .unwrap());

    let edge = IndexEntity {
        kind: IndexElementKind::Edge,
        id: IndexEntityId::new(7),
    };
    let edge_key = DataKey::Data {
        scope,
        kind: DataKeyKind::EdgePropertyById(crate::encoding::v2::keys::EdgePropertyByIdKey::new(
            edge.id.get(),
        )),
    }
    .to_bytes();
    assert_eq!(
        source_entity(scope, IndexElementKind::Edge, &edge_key).unwrap(),
        Some(edge.id)
    );
    assert_eq!(
        source_entity(scope, IndexElementKind::Edge, &source_key(scope, 0)).unwrap(),
        None
    );
    assert!(source_entity(scope, IndexElementKind::Node, &edge_key).is_err());
    let global = IndexKey::Global {
        kind: GlobalKey::StorageVersion,
    }
    .to_bytes();
    assert!(source_entity(scope, IndexElementKind::Node, &global).is_err());

    let prefix = source_prefix(scope, IndexElementKind::Node);
    assert_eq!(cursor_suffix(&prefix, None).unwrap(), None);
    assert_eq!(
        cursor_suffix(&prefix, Some(&source_cursor(scope, 0))).unwrap(),
        Some(source_key(scope, 0).slice(prefix.len()..))
    );
    assert!(cursor_suffix(
        &prefix,
        Some(&IndexCursor::try_new(Bytes::from_static(b"outside")).unwrap()),
    )
    .is_err());

    assert!(load_operation_index(&transaction, scope, &operation)
        .await
        .is_ok());
    assert!(matches!(
        load_operation_index(
            &transaction,
            DataScope::Tenant(
                crate::encoding::v2::keys::scope::TenantId::from_u128(1)
            ),
            &operation,
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(reason))
            if reason.contains("no canonical index")
    ));

    let limits = SearchIndexBackfillLimits::default().batch();
    let progress = SourceScanProgress {
        inclusive_upper_bound: source_cursor(scope, 0),
        cursor: None,
        counters: OperationCounters::default(),
    };
    assert!(matches!(
        finish_or_block_scan(
            EntityPlanOutcome::Blocked(IndexOperationBlocker::InvariantViolation),
            VectorBatchAccounting::new(OperationCounters::default(), limits),
            entity.kind,
            entity.id,
            &progress,
            None,
            VectorBuildSessionStats::default(),
        )
        .unwrap()
        .result,
        IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation)
    ));
    assert!(matches!(
        finish_or_block_scan(
            EntityPlanOutcome::BatchFull,
            VectorBatchAccounting::new(OperationCounters::default(), limits),
            entity.kind,
            entity.id,
            &progress,
            None,
            VectorBuildSessionStats::default(),
        )
        .unwrap()
        .result,
        IndexOperationStepResult::Progressed(IndexOperationProgress::VectorBuild(
            VectorBuildProgress::Constructing(VectorBuildStage::Scan(_))
        ))
    ));
    assert!(finish_or_block_scan(
        EntityPlanOutcome::Admitted {
            vector_writes: VectorWriteMeasurement::zero(),
            single_vector_output_bytes: 0,
            lifecycle_operations: 0,
            lifecycle_bytes: 0,
            next_partition: None,
        },
        VectorBatchAccounting::new(OperationCounters::default(), limits),
        entity.kind,
        entity.id,
        &progress,
        None,
        VectorBuildSessionStats::default(),
    )
    .is_err());

    let mut accounting = VectorBatchAccounting::new(
        OperationCounters {
            entities: u64::MAX,
            ..OperationCounters::default()
        },
        limits,
    );
    accounting
        .admit(1, VectorWriteMeasurement::zero(), 0, 1, 1)
        .unwrap();
    assert!(accounting.finish().is_err());

    let mut planning_accounting = VectorBatchAccounting::new(OperationCounters::default(), limits);
    planning_accounting.record_planning();
    planning_accounting
        .admit(1, VectorWriteMeasurement::zero(), 30, 0, 0)
        .unwrap();
    let planning = planning_accounting.planning_usage(VectorBuildSessionStats::default());
    assert_eq!(planning.planning_executions, 1);
    assert_eq!(planning.planned_writes, 0);
    assert_eq!(planning.replay_executions, 0);

    let record = read_index(&db, scope, &definition).await;
    let target = VectorPlanTarget::build(scope, &operation, &record).unwrap();
    assert!(
        lifecycle_write_measurement(
            &target,
            entity.kind,
            entity.id,
            AppliedStateTransition::Put(&TextPartition::Unpartitioned),
            Some((
                &TextPartition::Unpartitioned,
                VectorPhysicalIndexId::initial()
            )),
            &[],
        )
        .is_err(),
        "only a tenant partition owns a mapping"
    );
    assert_eq!(
        lifecycle_write_measurement(
            &target,
            entity.kind,
            entity.id,
            AppliedStateTransition::Delete,
            None,
            &[],
        )
        .unwrap()
        .0,
        1
    );
    assert!(matches!(record.state(), IndexStateV2::Building { .. }));
    drop(transaction);
    db.close().await.expect("vector test database closes");
}

pub(super) async fn oversized_partition_build_blocks_before_mapping_or_watermark_writes() {
    let db = test_db("vector-driver-block-before-physical").await;
    let scope = DataScope::LegacyUnscoped;
    let definition = definition(Some("account_id"));
    put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], Some(10))).await;
    let (build_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
    let before_watermark = peek_vector_physical_id(&db)
        .await
        .expect("vector watermark is readable");
    let tiny_output = SearchIndexBatchLimits::try_new(
        NonZeroUsize::MIN,
        NonZeroU64::new(1024 * 1024).expect("input limit is positive"),
        NonZeroU64::MIN,
        NonZeroU64::MIN,
        NonZeroU64::MIN,
    )
    .expect("tiny output policy validates");
    let mut claim_sequence = 1;
    assert_eq!(
        drive_one(&db, &driver(), build_id, &mut claim_sequence, tiny_output,).await,
        CommittedOperationStep::Blocked
    );
    assert!(mapping_values(&db, scope, index_id, generation)
        .await
        .is_empty());
    assert_eq!(
        peek_vector_physical_id(&db)
            .await
            .expect("vector watermark remains readable"),
        before_watermark
    );
    db.close().await.expect("vector test database closes");
}

pub(super) async fn abort_removes_hidden_physical_rows_and_builder_work() {
    let db = test_db("vector-driver-abort-cleanup").await;
    let scope = DataScope::LegacyUnscoped;
    let definition = definition(None);
    put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], None)).await;
    let (build_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
    let driver = driver();
    let mut claim_sequence = 1;
    assert_eq!(
        drive_one(
            &db,
            &driver,
            build_id,
            &mut claim_sequence,
            SearchIndexBackfillLimits::default().batch(),
        )
        .await,
        CommittedOperationStep::Progressed
    );
    let receipt = drop_index_operation(&db, scope, &definition)
        .await
        .expect("building vector converts to abort cleanup");
    assert!(matches!(
        receipt,
        IndexDdlReceipt::ExistingOperation { operation_id } if operation_id == build_id
    ));
    assert_eq!(
        drive_to_terminal(&db, &driver, build_id, &mut claim_sequence).await,
        CommittedOperationStep::Completed
    );
    assert!(matches!(
        read_index(&db, scope, &definition).await.state(),
        IndexStateV2::Dropped { .. }
    ));
    for kind in [
        RecordKind::BuildDelta,
        RecordKind::AppliedState,
        RecordKind::VectorPartitionMapping,
    ] {
        let prefix = generation_prefix(scope, kind, index_id, generation);
        let mut rows = db
            .scan_prefix(prefix, ..)
            .await
            .expect("cleanup generation prefix is readable");
        assert!(rows
            .next()
            .await
            .expect("cleanup generation row is readable")
            .is_none());
    }
    db.close().await.expect("vector test database closes");
}

pub(super) async fn adoption_abort_restores_source_reservation_without_deleting_physical_rows() {
    let db = test_db("vector-driver-adoption-abort").await;
    let scope = DataScope::LegacyUnscoped;
    let definition = definition(None);
    let physical_index_id = VectorPhysicalIndexId::new(55).expect("fixture ID is nonzero");
    let physical_row_key = DataKey::Data {
        scope,
        kind: DataKeyKind::Vector(
            crate::encoding::v2::keys::indexes::vector::VectorKey::SimHash(
                crate::encoding::v2::keys::indexes::vector::VectorSimHashKey::new(
                    physical_index_id.get(),
                    77,
                ),
            ),
        ),
    }
    .to_bytes();
    let physical_row_value = Bytes::copy_from_slice(
        &crate::encoding::v2::values::indexes::vector::simhash::encode_simhash(17),
    );
    let directory_keys = [1_u64, 2_u64].map(|node_id| {
        DataKey::Data {
            scope,
            kind: DataKeyKind::Vector(
                crate::encoding::v2::keys::indexes::vector::VectorKey::SimHashDirectory(
                    crate::encoding::v2::keys::indexes::vector::VectorSimHashDirectoryKey::new(
                        physical_index_id.get(),
                        node_id,
                        node_id,
                    ),
                ),
            ),
        }
        .to_bytes()
    });
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("legacy source transaction opens");
    transaction
        .put(&physical_row_key, &physical_row_value)
        .expect("legacy physical row stages");
    for directory_key in &directory_keys {
        transaction
            .put(
                directory_key,
                crate::encoding::v2::values::indexes::vector::markers::encode_simhash_directory_marker_v1(
                ),
            )
            .expect("partial directory marker stages");
    }
    transaction
        .put(
            IndexKey::Global {
                kind: GlobalKey::LegacyVectorPhysicalReservation(physical_index_id),
            }
            .to_bytes(),
            encode_metadata_value(&IndexV2MetadataValue::LegacyVectorPhysicalReservation(
                LegacyVectorPhysicalReservation::LegacySource,
            )),
        )
        .expect("legacy source reservation stages");
    transaction
        .commit()
        .await
        .expect("legacy source transaction commits");

    let receipt =
        create_legacy_vector_adoption_operation(&db, scope, definition.clone(), physical_index_id)
            .await
            .expect("legacy adoption enqueues");
    let IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
        panic!("new legacy adoption must enqueue one build")
    };
    assert!(matches!(
        crate::index_lifecycle::repository::load_legacy_vector_physical_reservation(
            &db,
            physical_index_id,
        )
        .await
        .expect("building reservation reads"),
        Some(LegacyVectorPhysicalReservation::AdoptionBuilding {
            operation_id: owner_operation,
            ..
        }) if owner_operation == operation_id
    ));
    let receipt = drop_index_operation(&db, scope, &definition)
        .await
        .expect("adoption converts to abort cleanup");
    assert!(matches!(
        receipt,
        IndexDdlReceipt::ExistingOperation { operation_id: aborted } if aborted == operation_id
    ));

    // Directory cleanup is bounded: a lone marker no batch can hold blocks,
    // and a one-row batch leaves the rest of the directory for the next step.
    // A reservation the abort does not own fails closed.
    let aborting = read_operation(&db, scope, operation_id).await;
    let aborting_record = read_index(&db, scope, &definition).await;
    let delete = VectorCleanupProgress::DeletePhysical(PrefixScanProgress {
        cursor: None,
        counters: OperationCounters::default(),
    });
    let reservation_key = IndexKey::Global {
        kind: GlobalKey::LegacyVectorPhysicalReservation(physical_index_id),
    }
    .to_bytes();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert!(matches!(
        cleanup_result(
            &transaction,
            &aborting,
            &aborting_record,
            &definition,
            delete.clone(),
            true,
            batch_limits(1024, 1, 1024, 16 * 1024 * 1024),
        )
        .await,
        Ok(IndexOperationStepResult::Blocked(
            IndexOperationBlocker::OversizedEntity { .. }
        ))
    ));
    assert!(matches!(
        cleanup_result(
            &transaction,
            &aborting,
            &aborting_record,
            &definition,
            delete.clone(),
            true,
            batch_limits(1, 1024 * 1024, 1024, 16 * 1024 * 1024),
        )
        .await,
        Ok(IndexOperationStepResult::Progressed(
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(
                VectorCleanupProgress::DeletePhysical(_)
            ))
        ))
    ));
    for reservation in [
        LegacyVectorPhysicalReservation::LegacySource,
        LegacyVectorPhysicalReservation::AdoptionBuilding {
            index_id: aborting.index_id(),
            generation: aborting.generation(),
            operation_id: IndexOperationId::new_v4(),
        },
    ] {
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        transaction
            .put(
                &reservation_key,
                encode_metadata_value(&IndexV2MetadataValue::LegacyVectorPhysicalReservation(
                    reservation,
                )),
            )
            .unwrap();
        assert!(matches!(
            cleanup_result(
                &transaction,
                &aborting,
                &aborting_record,
                &definition,
                delete.clone(),
                true,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
    }
    drop(transaction);
    let mut claim_sequence = 1;
    assert_eq!(
        drive_to_terminal(&db, &driver(), operation_id, &mut claim_sequence).await,
        CommittedOperationStep::Completed
    );
    assert!(matches!(
        read_index(&db, scope, &definition).await.state(),
        IndexStateV2::Dropped { .. }
    ));
    assert_eq!(
        crate::index_lifecycle::repository::load_legacy_vector_physical_reservation(
            &db,
            physical_index_id,
        )
        .await
        .expect("restored source reservation reads"),
        Some(LegacyVectorPhysicalReservation::LegacySource)
    );
    assert_eq!(
        db.get(physical_row_key)
            .await
            .expect("legacy physical row reads"),
        Some(physical_row_value),
        "adoption abort must not delete or rewrite legacy physical rows"
    );
    for directory_key in directory_keys {
        assert!(
            db.get(directory_key)
                .await
                .expect("partial directory marker reads")
                .is_none(),
            "adoption abort must delete only its partial directory"
        );
    }
    db.close().await.expect("vector test database closes");
}

/// A physical ID the step transaction sees as free but another index in
/// the scope allocated, with its namespace, before planning opened is a
/// retryable conflict; a namespace the transaction sees too means the
/// watermark trails it, which fails closed.
pub(super) async fn a_physical_id_allocated_before_planning_conflicts_and_a_stale_watermark_fails_closed(
) {
    let db = test_db("vector-driver-allocation-race").await;
    let scope = DataScope::LegacyUnscoped;
    let definition = definition(Some("account_id"));
    let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
        unreachable!("fixture definition is vector");
    };
    put_source(&db, scope, 1, &properties([1.0, 2.0, 3.0], Some(7))).await;
    let (operation_id, _, _) = create_build(&db, scope, &definition, 1).await;
    let record = read_index(&db, scope, &definition).await;
    let operation = read_operation(&db, scope, operation_id).await;
    let other = ValidatedDynamicIndexDefinition::Vector(
        ValidatedVectorIndexDefinition::try_from_runtime(
            &VectorIndexDefinition::new_node(
                "Picture",
                "embedding",
                3,
                VectorDistanceMetric::Euclidean,
            )
            .unwrap()
            .with_tenant_property("account_id")
            .unwrap(),
        )
        .unwrap(),
    );
    let (other_id, _, _) = create_build(&db, scope, &other, 1).await;
    let other_record = read_index(&db, scope, &other).await;
    let other_namespace = |physical_index_id| {
        let handle = ValidatedVectorBuildGenerationHandle::try_from_building::<
            vector::distance::Euclidean,
        >(scope, &other_record, other_id, physical_index_id)
        .unwrap();
        let ValidatedDynamicIndexDefinition::Vector(other_vector) = &other else {
            unreachable!("fixture definition is vector");
        };
        (
            VectorIndex::<vector::distance::Euclidean>::from_generation(handle.generation()),
            VectorIndexConfig::from_v2_definition(
                other_vector,
                handle.generation().physical_name(),
            ),
        )
    };

    // The step transaction reads the watermark, then the other build
    // allocates the same ID and creates its namespace.
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let peeked = peek_vector_physical_id(&transaction).await.unwrap();
    let concurrent = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let allocated = crate::index_lifecycle::repository::stage_vector_partition_mapping(
        &concurrent,
        scope,
        other_record.index_id(),
        other_record.state().generation(),
        VectorPhysicalLayout::Partitioned,
        &VectorTenantPartition::try_from_partition(
            vector_document(vector_definition, &properties([0.0, 0.0, 1.0], Some(8)))
                .unwrap()
                .unwrap()
                .partition()
                .clone(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(allocated, peeked);
    let (index, config) = other_namespace(allocated);
    index.create(&concurrent, config).await.unwrap();
    concurrent.commit().await.unwrap();
    assert!(matches!(
        plan_new_partition(&db, &transaction, &operation, &record, vector_definition, 7).await,
        Err(error) if error.is_transaction_conflict()
    ));
    drop(transaction);

    // A later transaction reads the advanced watermark and plans.
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    assert!(matches!(
        plan_new_partition(&db, &transaction, &operation, &record, vector_definition, 7).await,
        Ok(EntityPlanOutcome::Admitted { .. })
    ));
    drop(transaction);

    // A namespace at the next ID that the watermark never covered.
    let stale = peek_vector_physical_id(&db).await.unwrap();
    let (index, config) = other_namespace(stale);
    let create = db.begin(IsolationLevel::Snapshot).await.unwrap();
    index.create(&create, config).await.unwrap();
    create.commit().await.unwrap();
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    assert!(matches!(
        plan_new_partition(&db, &transaction, &operation, &record, vector_definition, 7).await,
        Err(HelixDbError::IndexCatalogCorruption(reason)) if reason.contains("watermark")
    ));
    drop(transaction);
    db.close().await.expect("vector test database closes");
}

/// Batch limits with every ceiling explicit; the single-vector ceiling
/// equals the output-byte ceiling.
fn batch_limits(
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
        NonZeroU64::new(max_output_bytes).expect("vector output limit is positive"),
    )
    .expect("fixture batch limits validate")
}

/// Runs one cleanup step of `operation` through `transaction` without
/// committing it.
async fn cleanup_result(
    transaction: &DbTransaction,
    operation: &IndexOperationRecord,
    record: &IndexRecordV2,
    definition: &ValidatedDynamicIndexDefinition,
    progress: VectorCleanupProgress,
    aborting: bool,
    limits: SearchIndexBatchLimits,
) -> Result<IndexOperationStepResult> {
    let ValidatedDynamicIndexDefinition::Vector(definition) = definition else {
        unreachable!("fixture definition is vector");
    };
    step_cleanup::<vector::distance::Euclidean>(
        transaction,
        DataScope::LegacyUnscoped,
        operation,
        record,
        definition,
        &progress,
        aborting,
        limits,
        &VectorCacheRegistry::default(),
    )
    .await
    .map(|step| step.result)
}

/// Runs descriptor validation of `operation` from `cursor` through
/// `transaction` without committing it.
async fn validation_result(
    db: &Db,
    transaction: &DbTransaction,
    operation: &IndexOperationRecord,
    record: &IndexRecordV2,
    definition: &ValidatedDynamicIndexDefinition,
    cursor: Option<Bytes>,
    limits: SearchIndexBatchLimits,
) -> Result<IndexOperationStepResult> {
    let ValidatedDynamicIndexDefinition::Vector(definition) = definition else {
        unreachable!("fixture definition is vector");
    };
    validate_descriptor::<vector::distance::Euclidean>(
        db,
        transaction,
        DataScope::LegacyUnscoped,
        operation,
        record,
        definition,
        &PrefixScanProgress {
            cursor: cursor.map(|cursor| IndexCursor::try_new(cursor).expect("cursor is bounded")),
            counters: OperationCounters::default(),
        },
        limits,
        Arc::new(SimHasherRegistry::default()),
    )
    .await
}

/// Runs the activation step of `operation` through `transaction` without
/// committing it.
async fn activation_result(
    db: &Db,
    transaction: &DbTransaction,
    operation: &IndexOperationRecord,
    record: &IndexRecordV2,
    definition: &ValidatedVectorIndexDefinition,
) -> Result<VectorStepResult> {
    let driver = driver();
    step_build::<vector::distance::Euclidean>(
        db,
        transaction,
        DataScope::LegacyUnscoped,
        operation,
        record,
        definition,
        &VectorBuildStage::Activate(NoCursorProgress::default()),
        SearchIndexBackfillLimits::default().batch(),
        IndexLifecycleScanTuning::default(),
        Arc::clone(&driver.simhasher_registry),
        driver.batch_reads,
        &driver.build_cache,
    )
    .await
}

/// Returns the operation an accepted drop enqueued.
fn accepted_drop(receipt: IndexDdlReceipt) -> IndexOperationId {
    let IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
        panic!("dropping an Active vector index enqueues cleanup");
    };
    operation_id
}

/// Returns the keys of every row under `prefix`.
async fn prefix_keys(read: &(impl slatedb::DbReadOps + Sync), prefix: Bytes) -> Vec<Bytes> {
    let mut rows = read
        .scan_prefix(&prefix, ..)
        .await
        .expect("fixture prefix is readable");
    let mut keys = Vec::new();
    while let Some(row) = rows.next().await.expect("fixture row is readable") {
        keys.push(row.key);
    }
    keys
}

/// Returns the stored key and value bytes of every row under `prefix`.
async fn prefix_rows(db: &Db, prefix: Bytes) -> Vec<(Bytes, u64)> {
    let mut rows = db
        .scan_prefix(&prefix, ..)
        .await
        .expect("fixture prefix is readable");
    let mut sized = Vec::new();
    while let Some(row) = rows.next().await.expect("fixture row is readable") {
        let bytes = (row.key.len() + row.value.len()) as u64;
        sized.push((row.key, bytes));
    }
    sized
}

/// Proves cleanup batches resume within every limit and block on a lone row
/// no batch can hold.
///
/// A namespace with more physical rows than one batch's operations resumes at
/// its next batch. A partition mapping larger than the input ceiling, or an
/// emptied partition whose mapping delete exceeds the output ceiling, blocks.
/// An aborted build's delta and applied-state rows are deleted in bounded
/// batches that block on a row no batch can hold.
pub(super) async fn cleanup_batches_resume_and_block_on_oversized_rows() {
    let scope = DataScope::LegacyUnscoped;
    let driver = driver();
    let generous = 16 * 1024 * 1024;

    let db = test_db("vector-driver-cleanup-physical-batches").await;
    let unpartitioned = definition(None);
    put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], None)).await;
    put_source(&db, scope, 1, &properties([3.0, 2.0, 1.0], None)).await;
    let (build_id, _, _) = create_build(&db, scope, &unpartitioned, 1).await;
    let mut claim_sequence = 1;
    assert_eq!(
        drive_to_terminal(&db, &driver, build_id, &mut claim_sequence).await,
        CommittedOperationStep::Completed
    );
    let drop_id = accepted_drop(
        drop_index_operation(&db, scope, &unpartitioned)
            .await
            .expect("unpartitioned drop enqueues"),
    );
    let operation = read_operation(&db, scope, drop_id).await;
    let record = read_index(&db, scope, &unpartitioned).await;
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert!(matches!(
        cleanup_result(
            &transaction,
            &operation,
            &record,
            &unpartitioned,
            VectorCleanupProgress::DeletePhysical(PrefixScanProgress {
                cursor: None,
                counters: OperationCounters::default(),
            }),
            false,
            batch_limits(1024, 1024 * 1024, 1, generous),
        )
        .await,
        Ok(IndexOperationStepResult::Progressed(
            IndexOperationProgress::VectorCleanup(VectorCleanupProgress::DeletePhysical(
                PrefixScanProgress { cursor: None, .. }
            ))
        ))
    ));
    drop(transaction);
    db.close().await.expect("vector test database closes");

    let db = test_db("vector-driver-cleanup-mapping-limits").await;
    let partitioned = definition(Some("account_id"));
    put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], Some(10))).await;
    put_source(&db, scope, 1, &properties([4.0, 5.0, 6.0], Some(20))).await;
    let (build_id, index_id, generation) = create_build(&db, scope, &partitioned, 1).await;
    assert_eq!(
        drive_to_terminal(&db, &driver, build_id, &mut claim_sequence).await,
        CommittedOperationStep::Completed
    );
    let drop_id = accepted_drop(
        drop_index_operation(&db, scope, &partitioned)
            .await
            .expect("partitioned drop enqueues"),
    );
    let operation = read_operation(&db, scope, drop_id).await;
    let record = read_index(&db, scope, &partitioned).await;
    let delete = VectorCleanupProgress::DeletePhysical(PrefixScanProgress {
        cursor: None,
        counters: OperationCounters::default(),
    });
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert!(matches!(
        cleanup_result(
            &transaction,
            &operation,
            &record,
            &partitioned,
            delete.clone(),
            false,
            batch_limits(1024, 1, 1024, generous),
        )
        .await,
        Ok(IndexOperationStepResult::Blocked(
            IndexOperationBlocker::OversizedEntity { .. }
        ))
    ));
    // Emptying the first mapped namespace, in mapping key order, leaves only
    // its mapping delete, which a one-byte output ceiling cannot hold.
    let first = mapping_values(&db, scope, index_id, generation).await[0].physical_index_id;
    for lane in VectorStorageLane::ALL {
        for key in prefix_keys(
            &transaction,
            DataKey::data_prefix(scope, lane.prefix_key(first.get()).to_bytes()),
        )
        .await
        {
            transaction.delete(key).unwrap();
        }
    }
    assert!(matches!(
        cleanup_result(
            &transaction,
            &operation,
            &record,
            &partitioned,
            delete,
            false,
            batch_limits(1024, 1024 * 1024, 1024, 1),
        )
        .await,
        Ok(IndexOperationStepResult::Blocked(
            IndexOperationBlocker::OversizedEntity { .. }
        ))
    ));
    drop(transaction);
    db.close().await.expect("vector test database closes");

    let db = test_db("vector-driver-cleanup-delta-limits").await;
    for entity_id in 0..3 {
        put_source(&db, scope, entity_id, &properties([1.0, 2.0, 3.0], None)).await;
    }
    let (build_id, index_id, generation) = create_build(&db, scope, &unpartitioned, 2).await;
    assert_eq!(
        drive_one(
            &db,
            &driver,
            build_id,
            &mut claim_sequence,
            batch_limits(2, 1024 * 1024, 1024, generous),
        )
        .await,
        CommittedOperationStep::Progressed
    );
    assert!(matches!(
        drop_index_operation(&db, scope, &unpartitioned)
            .await
            .expect("building vector converts to abort cleanup"),
        IndexDdlReceipt::ExistingOperation { operation_id } if operation_id == build_id
    ));
    let operation = read_operation(&db, scope, build_id).await;
    let record = read_index(&db, scope, &unpartitioned).await;
    let applied = prefix_rows(
        &db,
        generation_prefix(scope, RecordKind::AppliedState, index_id, generation),
    )
    .await;
    assert_eq!(applied.len(), 2, "one scanned batch applied two entities");
    let deltas = VectorCleanupProgress::DeleteDeltas(PrefixScanProgress {
        cursor: None,
        counters: OperationCounters::default(),
    });
    // A lone applied-state row, or a pre-queue delta ahead of it, that no
    // batch can hold blocks with its entity.
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert!(matches!(
        cleanup_result(
            &transaction,
            &operation,
            &record,
            &unpartitioned,
            deltas.clone(),
            true,
            batch_limits(1024, 1, 1024, generous),
        )
        .await,
        Ok(IndexOperationStepResult::Blocked(IndexOperationBlocker::OversizedEntity {
            entity_id,
            ..
        })) if entity_id == IndexEntityId::new(0)
    ));
    let delta_entity = IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(2),
    };
    transaction
        .put(
            scoped_index_key(
                scope,
                ScopedKey::BuildDelta(IndexEntityStateKey {
                    index_id,
                    generation,
                    entity: delta_entity,
                }),
            ),
            encode_build_delta(&CoalescedBuildDeltaValue {
                index_id,
                generation,
                entity_kind: delta_entity.kind,
                entity_id: delta_entity.id,
                state: crate::index_lifecycle::work::CoalescedBuildDeltaState::Marker,
            }),
        )
        .unwrap();
    assert!(matches!(
        cleanup_result(
            &transaction,
            &operation,
            &record,
            &unpartitioned,
            deltas.clone(),
            true,
            batch_limits(1024, 1, 1024, generous),
        )
        .await,
        Ok(IndexOperationStepResult::Blocked(IndexOperationBlocker::OversizedEntity {
            entity_id,
            ..
        })) if entity_id == delta_entity.id
    ));
    drop(transaction);
    // One row per batch, or an input ceiling only the first row fits, leaves
    // the rest for the next step.
    for limits in [
        batch_limits(1, 1024 * 1024, 1024, generous),
        batch_limits(1024, applied[0].1, 1024, generous),
    ] {
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(matches!(
            cleanup_result(
                &transaction,
                &operation,
                &record,
                &unpartitioned,
                deltas.clone(),
                true,
                limits,
            )
            .await,
            Ok(IndexOperationStepResult::Progressed(
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(
                    VectorCleanupProgress::DeleteDeltas(_)
                ))
            ))
        ));
    }
    db.close().await.expect("vector test database closes");
}

/// Proves descriptor validation batches its applied-state and mapping lanes
/// within every limit, blocks on a row or a namespace creation no batch can
/// hold, and fails closed on foreign applied state.
pub(super) async fn descriptor_validation_batches_and_blocks() {
    let scope = DataScope::LegacyUnscoped;
    let generous = 16 * 1024 * 1024;
    let db = test_db("vector-driver-validation-batches").await;
    let definition = definition(Some("account_id"));
    for (entity_id, tenant) in [(0, 10), (1, 20), (2, 30)] {
        put_source(
            &db,
            scope,
            entity_id,
            &properties([1.0, 2.0, 3.0], Some(tenant)),
        )
        .await;
    }
    let (build_id, index_id, generation) = create_build(&db, scope, &definition, 2).await;
    let driver = driver();
    let mut claim_sequence = 1;
    assert_eq!(
        drive_one(
            &db,
            &driver,
            build_id,
            &mut claim_sequence,
            SearchIndexBackfillLimits::default().batch(),
        )
        .await,
        CommittedOperationStep::Progressed
    );
    let operation = read_operation(&db, scope, build_id).await;
    assert!(matches!(
        operation.progress(),
        IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
            VectorBuildStage::ValidateDescriptor(_)
        ))
    ));
    let record = read_index(&db, scope, &definition).await;
    let applied = prefix_rows(
        &db,
        generation_prefix(scope, RecordKind::AppliedState, index_id, generation),
    )
    .await;
    let mappings = prefix_rows(
        &db,
        generation_prefix(
            scope,
            RecordKind::VectorPartitionMapping,
            index_id,
            generation,
        ),
    )
    .await;
    assert_eq!((applied.len(), mappings.len()), (3, 3));
    let blocked = |result: Result<IndexOperationStepResult>| {
        matches!(
            result,
            Ok(IndexOperationStepResult::Blocked(
                IndexOperationBlocker::OversizedEntity { .. }
            ))
        )
    };
    let resumed = |result: Result<IndexOperationStepResult>| {
        matches!(
            result,
            Ok(IndexOperationStepResult::Progressed(
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                    VectorBuildStage::ValidateDescriptor(PrefixScanProgress {
                        cursor: Some(_),
                        ..
                    })
                ))
            ))
        )
    };
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    // The applied-state lane blocks on a row no batch holds and stops at
    // the input ceiling after its first row.
    assert!(blocked(
        validation_result(
            &db,
            &transaction,
            &operation,
            &record,
            &definition,
            None,
            batch_limits(1024, 1, 1024, generous)
        )
        .await
    ));
    assert!(resumed(
        validation_result(
            &db,
            &transaction,
            &operation,
            &record,
            &definition,
            None,
            batch_limits(1024, applied[0].1, 1024, generous)
        )
        .await
    ));
    // The mapping lane resumes after its cursor: it blocks on a row no batch
    // holds, stops at the input ceiling after one row, and stops at the
    // entity ceiling.
    let after_first = Some(mappings[0].0.clone());
    assert!(blocked(
        validation_result(
            &db,
            &transaction,
            &operation,
            &record,
            &definition,
            after_first.clone(),
            batch_limits(1024, 1, 1024, generous)
        )
        .await
    ));
    assert!(resumed(
        validation_result(
            &db,
            &transaction,
            &operation,
            &record,
            &definition,
            after_first.clone(),
            batch_limits(1024, mappings[1].1, 1024, generous)
        )
        .await
    ));
    assert!(resumed(
        validation_result(
            &db,
            &transaction,
            &operation,
            &record,
            &definition,
            after_first,
            batch_limits(1, 1024 * 1024, 1024, generous)
        )
        .await
    ));
    drop(transaction);

    // A mapped partition without physical metadata fails closed.
    let missing = mapping_values(&db, scope, index_id, generation).await[1].physical_index_id;
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .delete(
            DataKey::Data {
                scope,
                kind: DataKeyKind::Vector(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                    missing.get(),
                ))),
            }
            .to_bytes(),
        )
        .unwrap();
    assert!(matches!(
        validation_result(
            &db,
            &transaction,
            &operation,
            &record,
            &definition,
            Some(mappings[0].0.clone()),
            SearchIndexBackfillLimits::default().batch(),
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    drop(transaction);

    // Applied state that is empty, or names another element kind, is not
    // this build's.
    let empty = AppliedEntityStateValue {
        index_id,
        generation,
        entity_kind: IndexElementKind::Node,
        entity_id: IndexEntityId::new(0),
        state: AppliedFamilyState::Vector(None),
    };
    let edge = AppliedEntityStateValue {
        entity_kind: IndexElementKind::Edge,
        state: AppliedFamilyState::Vector(Some(TextPartition::Unpartitioned)),
        ..empty.clone()
    };
    for foreign in [empty, edge] {
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        for (key, _) in &applied {
            transaction.delete(key).unwrap();
        }
        transaction
            .put(
                applied_key(
                    scope,
                    index_id,
                    generation,
                    foreign.entity_kind,
                    foreign.entity_id,
                ),
                encode_applied_state(&foreign),
            )
            .unwrap();
        assert!(matches!(
            validation_result(
                &db,
                &transaction,
                &operation,
                &record,
                &definition,
                None,
                SearchIndexBackfillLimits::default().batch()
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
    }
    db.close().await.expect("vector test database closes");

    // An unpartitioned build over no source creates its namespace during
    // validation, which blocks when the creation cannot fit.
    let db = test_db("vector-driver-validation-create").await;
    let unpartitioned = self::definition(None);
    let (build_id, _, _) = create_build(&db, scope, &unpartitioned, 0).await;
    assert_eq!(
        drive_one(
            &db,
            &driver,
            build_id,
            &mut claim_sequence,
            SearchIndexBackfillLimits::default().batch(),
        )
        .await,
        CommittedOperationStep::Progressed
    );
    let operation = read_operation(&db, scope, build_id).await;
    let record = read_index(&db, scope, &unpartitioned).await;
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert!(blocked(
        validation_result(
            &db,
            &transaction,
            &operation,
            &record,
            &unpartitioned,
            None,
            batch_limits(1024, 1024 * 1024, 1024, 1),
        )
        .await
    ));
    drop(transaction);
    db.close().await.expect("vector test database closes");
}

/// Proves adoption, directory validation, activation, and planning fail
/// closed on a generation they do not own.
///
/// Legacy adoption and its directory validation reject a partitioned
/// generation, a missing or foreign reservation, and marker counters that
/// disagree; activation rejects another family's record, a reservation it
/// cannot activate, and adoption graph rows, and returns to validation while
/// applied state remains; planning rejects a record of another physical
/// family and a partition that contradicts the layout.
pub(super) async fn adoption_and_activation_guards_fail_closed() {
    let scope = DataScope::LegacyUnscoped;
    let db = test_db("vector-driver-ownership-guards").await;
    let unpartitioned = definition(None);
    let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &unpartitioned else {
        unreachable!("fixture definition is vector");
    };
    let (build_id, index_id, generation) = create_build(&db, scope, &unpartitioned, 0).await;
    let operation = read_operation(&db, scope, build_id).await;
    let record = read_index(&db, scope, &unpartitioned).await;
    let Some(PhysicalGeneration::Vector {
        layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
        ..
    }) = record.state().physical()
    else {
        panic!("the fixture build is unpartitioned");
    };
    let physical_index_id = *physical_index_id;
    let partitioned_db = test_db("vector-driver-ownership-guards-partitioned").await;
    let partitioned = definition(Some("account_id"));
    let (partitioned_id, _, _) = create_build(&partitioned_db, scope, &partitioned, 0).await;
    let partitioned_operation = read_operation(&partitioned_db, scope, partitioned_id).await;
    let partitioned_record = read_index(&partitioned_db, scope, &partitioned).await;
    let text_record = IndexRecordV2::building(
        IndexId::new(index_id.get() + 1).expect("fixture index ID is positive"),
        ValidatedDynamicIndexDefinition::Text(
            crate::index_lifecycle::ValidatedTextIndexDefinition::try_new(
                IndexElementKind::Node,
                "Document",
                "body",
                None::<String>,
                crate::config::TextAnalyzerKind::Standard,
                false,
            )
            .expect("text definition validates"),
        ),
        crate::index_lifecycle::IndexRevision::initial(),
        PhysicalGeneration::Text { generation },
        build_id,
    )
    .expect("text building record validates");
    let limits = SearchIndexBackfillLimits::default().batch();
    let counters = OperationCounters::default();
    let adopt = LegacyVectorValidationProgress {
        lane: LegacyVectorValidationLane::Core,
        cursor: None,
        counters,
    };
    let directory = |expected_markers| LegacyVectorDirectoryValidationProgress {
        cursor: None,
        expected_markers,
        verified_markers: 0,
        counters,
    };
    let reservation_key = IndexKey::Global {
        kind: GlobalKey::LegacyVectorPhysicalReservation(physical_index_id),
    }
    .to_bytes();
    let foreign = LegacyVectorPhysicalReservation::AdoptionBuilding {
        index_id,
        generation,
        operation_id: IndexOperationId::new_v4(),
    };
    let owned = LegacyVectorPhysicalReservation::AdoptionBuilding {
        index_id,
        generation,
        operation_id: build_id,
    };
    let corrupt = |result: Result<VectorStepResult>| {
        matches!(result, Err(HelixDbError::IndexCatalogCorruption(_)))
    };

    // A partitioned generation is never a legacy adoption.
    let transaction = partitioned_db
        .begin(IsolationLevel::Snapshot)
        .await
        .unwrap();
    assert!(corrupt(
        adopt_legacy::<vector::distance::Euclidean>(
            &transaction,
            scope,
            &partitioned_operation,
            &partitioned_record,
            vector_definition,
            &adopt,
            limits,
        )
        .await
    ));
    assert!(corrupt(
        validate_adopted_directory::<vector::distance::Euclidean>(
            &transaction,
            scope,
            &partitioned_operation,
            &partitioned_record,
            vector_definition,
            &directory(0),
            limits,
        )
        .await
    ));
    drop(transaction);
    partitioned_db
        .close()
        .await
        .expect("vector test database closes");

    // Adoption and directory validation require their own reservation and
    // consistent marker counters.
    for reservation in [None, Some(foreign)] {
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        if let Some(reservation) = reservation {
            transaction
                .put(
                    &reservation_key,
                    encode_metadata_value(&IndexV2MetadataValue::LegacyVectorPhysicalReservation(
                        reservation,
                    )),
                )
                .unwrap();
        }
        assert!(corrupt(
            adopt_legacy::<vector::distance::Euclidean>(
                &transaction,
                scope,
                &operation,
                &record,
                vector_definition,
                &adopt,
                limits,
            )
            .await
        ));
        assert!(corrupt(
            validate_adopted_directory::<vector::distance::Euclidean>(
                &transaction,
                scope,
                &operation,
                &record,
                vector_definition,
                &directory(0),
                limits,
            )
            .await
        ));
    }
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert!(corrupt(
        validate_adopted_directory::<vector::distance::Euclidean>(
            &transaction,
            scope,
            &operation,
            &record,
            vector_definition,
            &directory(1),
            limits,
        )
        .await
    ));
    drop(transaction);

    // Activation rejects another family's record, a reservation it cannot
    // activate, and graph rows beside an adoption, and returns to validation
    // while applied state remains.
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert!(corrupt(
        activation_result(
            &db,
            &transaction,
            &operation,
            &text_record,
            vector_definition
        )
        .await
    ));
    drop(transaction);
    let entity = IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(0),
    };
    for (reservation, delta) in [
        (LegacyVectorPhysicalReservation::LegacySource, false),
        (owned, true),
    ] {
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        transaction
            .put(
                &reservation_key,
                encode_metadata_value(&IndexV2MetadataValue::LegacyVectorPhysicalReservation(
                    reservation,
                )),
            )
            .unwrap();
        if delta {
            transaction
                .put(
                    scoped_index_key(
                        scope,
                        ScopedKey::BuildDelta(IndexEntityStateKey {
                            index_id,
                            generation,
                            entity,
                        }),
                    ),
                    encode_build_delta(&CoalescedBuildDeltaValue {
                        index_id,
                        generation,
                        entity_kind: entity.kind,
                        entity_id: entity.id,
                        state: crate::index_lifecycle::work::CoalescedBuildDeltaState::Marker,
                    }),
                )
                .unwrap();
        }
        assert!(corrupt(
            activation_result(&db, &transaction, &operation, &record, vector_definition).await
        ));
    }
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            applied_key(scope, index_id, generation, entity.kind, entity.id),
            encode_applied_state(&AppliedEntityStateValue {
                index_id,
                generation,
                entity_kind: entity.kind,
                entity_id: entity.id,
                state: AppliedFamilyState::Vector(Some(TextPartition::Unpartitioned)),
            }),
        )
        .unwrap();
    assert!(matches!(
        activation_result(&db, &transaction, &operation, &record, vector_definition)
            .await
            .map(|step| step.result),
        Ok(IndexOperationStepResult::Progressed(
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                VectorBuildStage::ValidateDescriptor(_)
            ))
        ))
    ));

    // Planning targets only a vector generation, whose layout its
    // partitions must match.
    assert!(matches!(
        VectorPlanTarget::build(scope, &operation, &text_record),
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    let target = VectorPlanTarget::build(scope, &operation, &record)
        .expect("an unpartitioned build is a planning target");
    let ValidatedDynamicIndexDefinition::Vector(tenant_definition) = &partitioned else {
        unreachable!("fixture definition is vector");
    };
    let tenant = vector_document(tenant_definition, &properties([1.0, 2.0, 3.0], Some(7)))
        .expect("tenant document validates")
        .expect("tenant document is indexed")
        .partition()
        .clone();
    assert!(matches!(
        resolve_build_physical(&transaction, &target, &tenant, true).await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    assert!(matches!(
        resolve_existing_build_physical(&transaction, &target, &tenant).await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    drop(transaction);
    db.close().await.expect("vector test database closes");
}
