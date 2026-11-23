//! Contracts for the vector lifecycle driver's step boundaries.
//!
//! This child of the vector lifecycle driver runs its build, cleanup, legacy
//! adoption, and planning scenarios against raw SlateDB databases through the
//! production outbox claim and step entry points. The driver's unit tests
//! share these fixtures and run every scenario. Every fixture row uses the
//! current key and value codecs; no row family or encoding is introduced.

use std::num::{NonZeroU64, NonZeroUsize};

use slatedb::object_store::memory::InMemory;

use super::*;
use crate::config::{SearchIndexBackfillLimits, VectorIndexDefinition};
use crate::encoding::property::property_value::PropertyValue;
use crate::encoding::property::Property;
use crate::encoding::v2::keys::NodePropertyKey;
use crate::encoding::v2::values::encode_build_delta;
use crate::index_lifecycle::failpoints::{self, IndexOutboxFailpoint};
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
    ActiveIndexHandle, ClaimSequence, DroppedGenerationValue, IndexDdlReceipt, IndexOperationId,
    IndexScopeGates, IndexStateV2, WriterEpoch,
};
use crate::migrations::startup::bootstrap_writer;
use crate::search::vector::{
    SimHasherRegistry, ValidatedVectorGenerationHandle, VectorCacheRegistry,
};

pub(super) const NOW_MILLIS: u64 = 1;

pub(super) async fn test_db(name: &str) -> Db {
    let db = Db::builder(name, Arc::new(InMemory::new()))
        .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
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
    crate::index_lifecycle::label_source::testing::put_source_row(
        db,
        scope,
        IndexElementKind::Node,
        entity_id,
        properties,
    )
    .await;
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
    driver: &dyn IndexOperationDriver,
    operation_id: IndexOperationId,
    claim_sequence: &mut u64,
    limits: SearchIndexBatchLimits,
) -> CommittedOperationStep {
    try_drive_one(db, driver, operation_id, claim_sequence, limits)
        .await
        .expect("vector step commits")
}

/// Claims `operation_id` and runs one step, which may fail.
async fn try_drive_one(
    db: &Db,
    driver: &dyn IndexOperationDriver,
    operation_id: IndexOperationId,
    claim_sequence: &mut u64,
    limits: SearchIndexBatchLimits,
) -> Result<CommittedOperationStep> {
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
    execute_claimed_step(db, &claimed, driver, limits, NOW_MILLIS).await
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
    let generations = mappings
        .iter()
        .map(|mapping| {
            ValidatedVectorGenerationHandle::try_from_active::<vector::distance::Euclidean>(
                &active_handle,
                mapping.physical_index_id,
            )
            .expect("partition mapping validates against the Active handle")
        })
        .collect::<Vec<_>>();
    let indexes = generations
        .iter()
        .map(VectorIndex::<vector::distance::Euclidean>::from_generation)
        .collect::<Vec<_>>();

    // A position in the first partition's namespace is no position in the
    // second: the scan of the second starts over and proves it empty.
    let one_operation = SearchIndexBatchLimits::try_new(
        NonZeroUsize::new(1024).unwrap(),
        NonZeroU64::new(1024 * 1024).unwrap(),
        NonZeroU64::MIN,
        NonZeroU64::new(16 * 1024 * 1024).unwrap(),
        NonZeroU64::new(16 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let Ok(PhysicalCleanupOutcome::Progress {
        resume: Some(first_partition_row),
        ..
    }) = delete_physical_namespace::<vector::distance::Euclidean>(
        &transaction,
        &generations[0],
        None,
        None,
        IndexElementKind::Node,
        OperationCounters::default(),
        one_operation,
    )
    .await
    else {
        panic!("one delete of the first partition hands on its position");
    };
    drop(transaction);
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let Ok(PhysicalCleanupOutcome::Progress {
        namespace_empty: true,
        resume: None,
        ..
    }) = delete_physical_namespace::<vector::distance::Euclidean>(
        &transaction,
        &generations[1],
        None,
        Some(&first_partition_row),
        IndexElementKind::Node,
        OperationCounters::default(),
        SearchIndexBackfillLimits::default().batch(),
    )
    .await
    else {
        panic!("a position in another namespace restarts the scan from the start");
    };
    drop(transaction);

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
            .cleanup_scan(&db, VectorCleanupFrom::Start)
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
                None,
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
            None,
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
            None,
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
            None,
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

/// A blocked step commits what it staged beside an unchanged cursor, and a
/// rescan cannot revisit an entity with applied state. So a `Scan` step that
/// admits rows and then meets an invalid one commits the admitted rows with
/// the cursor at the last of them, the next step blocks with nothing staged,
/// and once the row is repaired the retried build activates.
pub(super) async fn a_scan_step_ends_before_a_blocker_behind_admitted_rows() {
    let db = test_db("vector-driver-blocker-behind-admitted-rows").await;
    let scope = DataScope::LegacyUnscoped;
    let definition = definition(Some("account_id"));
    put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], Some(10))).await;
    put_source(&db, scope, 1, &properties([3.0, 2.0, 1.0], Some(10))).await;
    let wrong_dimension = vec![
        Property::new("$label", PropertyValue::String("Document".to_string())),
        Property::new("embedding", PropertyValue::F32Array(vec![1.0, 2.0])),
        Property::new("account_id", PropertyValue::I64(10)),
    ];
    put_source(&db, scope, 2, &wrong_dimension).await;
    let (operation_id, _, _) = create_build(&db, scope, &definition, 2).await;
    let scan = |operation: &IndexOperationRecord| {
        let IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
            VectorBuildStage::Scan(progress),
        )) = operation.progress()
        else {
            panic!("the build is still scanning: {:?}", operation.progress());
        };
        (progress.cursor.clone(), progress.counters.entities)
    };
    let driver = driver();
    let limits = SearchIndexBackfillLimits::default().batch();
    let mut claim_sequence = 1;
    let mut progressed = None;
    let blocked = loop {
        match drive_one(&db, &driver, operation_id, &mut claim_sequence, limits).await {
            CommittedOperationStep::Progressed => {
                progressed = Some(read_operation(&db, scope, operation_id).await);
            }
            CommittedOperationStep::Blocked => {
                break read_operation(&db, scope, operation_id).await
            }
            step @ (CommittedOperationStep::TransientFailure
            | CommittedOperationStep::Completed) => {
                panic!("the scan must block on the invalid row: {step:?}")
            }
        }
    };
    let admitted = progressed.expect("a progressed step commits the admitted rows");
    assert_eq!(scan(&admitted), (Some(source_cursor(scope, 1)), 2));
    assert!(
        matches!(
            blocked.execution_state(),
            crate::index_lifecycle::IndexOperationExecutionState::Blocked(
                IndexOperationBlocker::InvalidSourceData { entity_id, .. }
            ) if *entity_id == IndexEntityId::new(2)
        ),
        "{:?}",
        blocked.execution_state()
    );
    assert_eq!(
        scan(&blocked),
        scan(&admitted),
        "the blocker leaves the admitted rows' cursor"
    );

    put_source(&db, scope, 2, &properties([2.0, 2.0, 2.0], Some(10))).await;
    crate::index_lifecycle::outbox::retry_operation(&db, scope, operation_id)
        .await
        .expect("the blocked build retries");
    assert_eq!(
        drive_to_terminal(&db, &driver, operation_id, &mut claim_sequence).await,
        CommittedOperationStep::Completed,
        "the rescan resumes after the admitted rows"
    );
    assert!(matches!(
        read_index(&db, scope, &definition).await.state(),
        IndexStateV2::Active { .. }
    ));
    db.close().await.expect("vector test database closes");
}

/// An entity whose output the step's budget cannot admit behind admitted
/// entities ends the step like an invalid row: with an output budget of
/// exactly the first entity's output, the first step commits that entity
/// alone, and the build still activates under the default budget.
pub(super) async fn a_scan_step_ends_before_an_entity_its_output_budget_cannot_admit() {
    let scope = DataScope::LegacyUnscoped;
    let definition = definition(Some("account_id"));
    let seeded = |name: &'static str| {
        let definition = definition.clone();
        async move {
            let db = test_db(name).await;
            put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], Some(10))).await;
            put_source(&db, scope, 1, &properties([3.0, 2.0, 1.0], Some(10))).await;
            let (operation_id, _, _) = create_build(&db, scope, &definition, 1).await;
            (db, operation_id)
        }
    };
    let scan = |operation: &IndexOperationRecord| {
        let IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
            VectorBuildStage::Scan(progress),
        )) = operation.progress()
        else {
            panic!("the build is still scanning: {:?}", operation.progress());
        };
        (progress.cursor.clone(), progress.counters)
    };
    let driver = driver();

    // The first entity's exact output, measured by a one-entity step.
    let (db, operation_id) = seeded("vector-driver-first-entity-output").await;
    let mut claim_sequence = 1;
    assert_eq!(
        drive_one(
            &db,
            &driver,
            operation_id,
            &mut claim_sequence,
            batch_limits(1, 1024 * 1024, 1024, 16 * 1024 * 1024),
        )
        .await,
        CommittedOperationStep::Progressed
    );
    let (cursor, first) = scan(&read_operation(&db, scope, operation_id).await);
    assert_eq!((cursor, first.entities), (Some(source_cursor(scope, 0)), 1));
    db.close().await.expect("vector test database closes");

    let (db, operation_id) = seeded("vector-driver-output-budget-behind-admitted").await;
    let mut claim_sequence = 1;
    assert_eq!(
        drive_one(
            &db,
            &driver,
            operation_id,
            &mut claim_sequence,
            batch_limits(16, 1024 * 1024, 1024, first.output_bytes),
        )
        .await,
        CommittedOperationStep::Progressed
    );
    assert_eq!(
        scan(&read_operation(&db, scope, operation_id).await),
        (Some(source_cursor(scope, 0)), first),
        "the step ends before the entity its budget cannot admit"
    );
    assert_eq!(
        drive_to_terminal(&db, &driver, operation_id, &mut claim_sequence).await,
        CommittedOperationStep::Completed
    );
    assert!(matches!(
        read_index(&db, scope, &definition).await.state(),
        IndexStateV2::Active { .. }
    ));
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
                AppliedStateCheck::InTransaction,
                SearchIndexBackfillLimits::default().batch(),
                IndexLifecycleScanTuning::default(),
                registry,
                driver.batch_reads,
                &driver.build_cache,
                &driver.bulk,
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

    let prefix = crate::index_lifecycle::secondary::source_prefix(scope, IndexElementKind::Node);
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
    adoption_abort("vector-driver-adoption-abort", false).await;
    adoption_abort("vector-driver-admitted-adoption-abort", true).await;
}

/// Aborts a legacy adoption on a store that does or does not admit
/// dropped-generation entries. Either way the abort deletes its directory and
/// its build deltas itself, hands the legacy rows back untouched and leaves
/// no entry: this catalog did not allocate the namespace's physical id.
async fn adoption_abort(name: &str, admitted: bool) {
    let db = test_db(name).await;
    if admitted {
        dropped::admit_fixture(&db).await;
    }
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
    let aborted = dropped::DroppedGeneration {
        index_id: aborting.index_id(),
        generation: aborting.generation(),
    };
    put_build_delta(&db, aborted).await;
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
    assert_eq!(generation_keys(&db, aborted).await, Vec::<Bytes>::new());
    assert_eq!(dropped::load_entries(&db).await.unwrap(), vec![], "{name}");
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
        None,
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
        AppliedStateCheck::InTransaction,
        SearchIndexBackfillLimits::default().batch(),
        IndexLifecycleScanTuning::default(),
        Arc::clone(&driver.simhasher_registry),
        driver.batch_reads,
        &driver.build_cache,
        &driver.bulk,
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

/// Returns the keys of every physical row of one vector namespace, in the
/// order a cleanup scan visits them.
async fn physical_keys(
    read: &(impl slatedb::DbReadOps + Sync),
    scope: DataScope,
    physical_index_id: u64,
) -> Vec<Bytes> {
    let mut keys = Vec::new();
    for lane in VectorStorageLane::ALL {
        keys.extend(
            prefix_keys(
                read,
                DataKey::data_prefix(scope, lane.prefix_key(physical_index_id).to_bytes()),
            )
            .await,
        );
    }
    keys
}

/// Builds an unpartitioned index over six entities and enqueues its drop,
/// returning the drop, the namespace's physical ID, and the next claim
/// sequence.
async fn unpartitioned_drop(db: &Db, driver: &VectorIndexDriver) -> (IndexOperationId, u64, u64) {
    let scope = DataScope::LegacyUnscoped;
    let definition = definition(None);
    for entity_id in 0..6_u8 {
        put_source(
            db,
            scope,
            u64::from(entity_id),
            &properties([f32::from(entity_id), 2.0, 3.0], None),
        )
        .await;
    }
    let (build_id, _, _) = create_build(db, scope, &definition, 5).await;
    let mut claim_sequence = 1;
    assert_eq!(
        drive_to_terminal(db, driver, build_id, &mut claim_sequence).await,
        CommittedOperationStep::Completed
    );
    let active = read_index(db, scope, &definition).await;
    let IndexStateV2::Active {
        physical:
            PhysicalGeneration::Vector {
                layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
                ..
            },
        ..
    } = active.state()
    else {
        panic!("completed vector build is active and unpartitioned");
    };
    let drop_id = accepted_drop(
        drop_index_operation(db, scope, &definition)
            .await
            .expect("unpartitioned drop enqueues"),
    );
    (drop_id, physical_index_id.get(), claim_sequence)
}

/// Whether `operation` is a drop still deleting physical rows.
async fn deleting_physical(db: &Db, operation_id: IndexOperationId) -> bool {
    matches!(
        read_operation(db, DataScope::LegacyUnscoped, operation_id)
            .await
            .progress(),
        IndexOperationProgress::VectorCleanup(VectorCleanupProgress::DeletePhysical(
            PrefixScanProgress { cursor: None, .. }
        ))
    )
}

/// Proves each delete step resumes just past the last row the step before it
/// deleted, and that the stage is left only by a scan from the start.
///
/// A row put back behind the position stays through every resumed step: none
/// of them reads that part of the namespace again. The resumed step that
/// reaches the end hands on no position and stays in the stage, and the next
/// step, scanning from the start, deletes the row and alone leaves the stage.
/// The persisted cursor stays unset throughout.
pub(super) async fn unpartitioned_drop_resumes_past_deleted_rows_and_sweeps_before_leaving() {
    let db = test_db("vector-driver-cleanup-resume").await;
    let scope = DataScope::LegacyUnscoped;
    let driver = driver();
    let (drop_id, physical_index_id, mut claim_sequence) = unpartitioned_drop(&db, &driver).await;
    let keys = physical_keys(&db, scope, physical_index_id).await;
    assert!(keys.len() > 9, "the namespace spans several batches");
    let three_rows = batch_limits(1024, 1024 * 1024, 3, 16 * 1024 * 1024);
    let position = |driver: &VectorIndexDriver| driver.cleanup_resume.lock().get(&drop_id).cloned();

    // Retiring the cache deletes nothing and hands on no position.
    assert_eq!(
        drive_one(&db, &driver, drop_id, &mut claim_sequence, three_rows).await,
        CommittedOperationStep::Progressed
    );
    assert!(position(&driver).is_none());
    assert_eq!(physical_keys(&db, scope, physical_index_id).await, keys);

    // The first delete step scans from the start.
    assert_eq!(
        drive_one(&db, &driver, drop_id, &mut claim_sequence, three_rows).await,
        CommittedOperationStep::Progressed
    );
    assert_eq!(
        physical_keys(&db, scope, physical_index_id).await,
        keys[3..]
    );
    assert!(matches!(
        position(&driver),
        Some(VectorCleanupResume::Physical(_))
    ));

    // A row behind the position: the first key, which sorts before every
    // other row of the namespace.
    db.put(&keys[0], b"left behind")
        .await
        .expect("fixture row is written");
    let mut deleted = 3;
    while position(&driver).is_some() {
        assert!(
            deleted < keys.len(),
            "a resumed scan that reached the end still handed on a position"
        );
        assert_eq!(
            drive_one(&db, &driver, drop_id, &mut claim_sequence, three_rows).await,
            CommittedOperationStep::Progressed
        );
        deleted = (deleted + 3).min(keys.len());
        let mut left = vec![keys[0].clone()];
        left.extend_from_slice(&keys[deleted..]);
        assert_eq!(
            physical_keys(&db, scope, physical_index_id).await,
            left,
            "a resumed step deletes the next rows and none before its position"
        );
        assert!(
            deleting_physical(&db, drop_id).await,
            "a resumed step never leaves the stage"
        );
    }
    assert_eq!(deleted, keys.len(), "the resumed scans reached the end");

    // The scan from the start finds the row and leaves the stage.
    assert_eq!(
        drive_one(&db, &driver, drop_id, &mut claim_sequence, three_rows).await,
        CommittedOperationStep::Progressed
    );
    assert!(physical_keys(&db, scope, physical_index_id)
        .await
        .is_empty());
    assert!(matches!(
        read_operation(&db, scope, drop_id).await.progress(),
        IndexOperationProgress::VectorCleanup(VectorCleanupProgress::DeleteDeltas(_))
    ));
    assert_eq!(
        drive_to_terminal(&db, &driver, drop_id, &mut claim_sequence).await,
        CommittedOperationStep::Completed
    );
    assert!(matches!(
        read_index(&db, scope, &definition(None)).await.state(),
        IndexStateV2::Dropped { .. }
    ));
    assert!(
        position(&driver).is_none(),
        "a finished drop keeps no position"
    );
    db.close().await.expect("vector test database closes");
}

/// Proves a delete step reads the rows a drop already deleted outside its
/// transaction: its preparation reads the namespace from the start, and the
/// step begins at the row that read found.
///
/// A committed position is kept, so nothing is read from the start while
/// steps resume. With no position, the preparation hands on the first row or
/// that there is none, and a step handed "none" leaves the stage without
/// scanning. A start read for another namespace is no start.
pub(super) async fn delete_steps_start_at_the_row_their_preparation_read() {
    let db = test_db("vector-driver-cleanup-prepared-start").await;
    let scope = DataScope::LegacyUnscoped;
    let driver = driver();
    let (drop_id, physical_index_id, mut claim_sequence) = unpartitioned_drop(&db, &driver).await;
    let keys = physical_keys(&db, scope, physical_index_id).await;
    let three_rows = batch_limits(1024, 1024 * 1024, 3, 16 * 1024 * 1024);
    let position = |driver: &VectorIndexDriver| driver.cleanup_resume.lock().get(&drop_id).cloned();
    assert_eq!(
        drive_one(&db, &driver, drop_id, &mut claim_sequence, three_rows).await,
        CommittedOperationStep::Progressed
    );

    // No position yet: the preparation reads the first row from the start.
    let operation = read_operation(&db, scope, drop_id).await;
    let prepared = driver
        .prepare_step(&db, scope, &operation, three_rows)
        .await
        .expect("first delete step prepares");
    assert!(matches!(
        position(&driver),
        Some(VectorCleanupResume::FirstPhysical {
            physical_index_id: located,
            row: Some(_),
        }) if located.get() == physical_index_id
    ));
    drop(prepared);
    // A start read for another namespace is ignored: the step scans this
    // one from its start instead of taking it for empty.
    let record = read_index(&db, scope, &definition(None)).await;
    let ValidatedDynamicIndexDefinition::Vector(vector_definition) = record.definition() else {
        unreachable!("fixture definition is vector");
    };
    let IndexOperationProgress::VectorCleanup(progress) = operation.progress() else {
        panic!("a drop runs cleanup stages");
    };
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let foreign = VectorCleanupResume::FirstPhysical {
        physical_index_id: VectorPhysicalIndexId::new(physical_index_id + 1).unwrap(),
        row: None,
    };
    step_cleanup::<vector::distance::Euclidean>(
        &transaction,
        scope,
        &operation,
        &record,
        vector_definition,
        progress,
        Some(&foreign),
        false,
        three_rows,
        driver.cache_registry.as_ref(),
    )
    .await
    .expect("a foreign start is ignored");
    assert_eq!(
        physical_keys(&transaction, scope, physical_index_id).await,
        keys[3..]
    );
    drop(transaction);

    // A committed position is kept by the next preparation.
    assert_eq!(
        drive_one(&db, &driver, drop_id, &mut claim_sequence, three_rows).await,
        CommittedOperationStep::Progressed
    );
    assert!(matches!(
        position(&driver),
        Some(VectorCleanupResume::Physical(_))
    ));
    let operation = read_operation(&db, scope, drop_id).await;
    drop(
        driver
            .prepare_step(&db, scope, &operation, three_rows)
            .await
            .expect("resumed delete step prepares"),
    );
    assert!(matches!(
        position(&driver),
        Some(VectorCleanupResume::Physical(_))
    ));
    for _ in 0..64 {
        if position(&driver).is_none() {
            break;
        }
        assert_eq!(
            drive_one(&db, &driver, drop_id, &mut claim_sequence, three_rows).await,
            CommittedOperationStep::Progressed
        );
    }
    assert!(
        position(&driver).is_none(),
        "the resumed scans reached the end"
    );
    assert!(physical_keys(&db, scope, physical_index_id)
        .await
        .is_empty());

    // No position again: the preparation reads the whole namespace from the
    // start and finds nothing. A row put back after that read, which nothing
    // can do while the drop owns its generation, goes unseen: the step leaves
    // the stage without reading the namespace in its transaction.
    let operation = read_operation(&db, scope, drop_id).await;
    let prepared = driver
        .prepare_step(&db, scope, &operation, three_rows)
        .await
        .expect("closing delete step prepares");
    assert!(matches!(
        position(&driver),
        Some(VectorCleanupResume::FirstPhysical {
            physical_index_id: located,
            row: None,
        }) if located.get() == physical_index_id
    ));
    db.put(&keys[0], b"left behind")
        .await
        .expect("fixture row is written");
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    assert!(matches!(
        driver
            .step(&db, &transaction, scope, &operation, three_rows)
            .await
            .expect("prepared delete step stages")
            .result(),
        IndexOperationStepResult::Progressed(IndexOperationProgress::VectorCleanup(
            VectorCleanupProgress::DeleteDeltas(_)
        ))
    ));
    drop(transaction);
    drop(prepared);

    // Every step the outbox runs is prepared, so the row is read and deleted.
    assert_eq!(
        drive_one(&db, &driver, drop_id, &mut claim_sequence, three_rows).await,
        CommittedOperationStep::Progressed
    );
    assert!(physical_keys(&db, scope, physical_index_id)
        .await
        .is_empty());
    assert_eq!(
        drive_to_terminal(&db, &driver, drop_id, &mut claim_sequence).await,
        CommittedOperationStep::Completed
    );
    assert!(matches!(
        read_index(&db, scope, &definition(None)).await.state(),
        IndexStateV2::Dropped { .. }
    ));
    db.close().await.expect("vector test database closes");
}

/// Proves a drop that loses its position mid-way still removes every row.
///
/// A new driver, as after a restart, holds no position, so its first step
/// scans from the start: it deletes a row put back at the first key, which a
/// resumed step would have skipped. A drop that blocks drops its position
/// too. The drop then completes and leaves nothing.
pub(super) async fn a_drop_that_loses_its_position_rescans_from_the_start() {
    let db = test_db("vector-driver-cleanup-restart").await;
    let scope = DataScope::LegacyUnscoped;
    let driver = driver();
    let (drop_id, physical_index_id, mut claim_sequence) = unpartitioned_drop(&db, &driver).await;
    let keys = physical_keys(&db, scope, physical_index_id).await;
    assert!(keys.len() > 9, "the namespace spans several batches");
    let three_rows = batch_limits(1024, 1024 * 1024, 3, 16 * 1024 * 1024);
    for _ in 0..3 {
        assert_eq!(
            drive_one(&db, &driver, drop_id, &mut claim_sequence, three_rows).await,
            CommittedOperationStep::Progressed
        );
    }
    assert!(driver.cleanup_resume.lock().contains_key(&drop_id));
    assert_eq!(
        physical_keys(&db, scope, physical_index_id).await,
        keys[6..]
    );
    db.put(&keys[0], b"left behind")
        .await
        .expect("fixture row is written");

    let restarted = self::driver();
    assert_eq!(
        drive_one(&db, &restarted, drop_id, &mut claim_sequence, three_rows).await,
        CommittedOperationStep::Progressed
    );
    assert_eq!(
        physical_keys(&db, scope, physical_index_id).await,
        keys[8..],
        "the first step after a restart deletes from the start"
    );

    // A step that blocks, here on a row no batch can hold, drops the
    // position; the retried drop scans from the start again.
    assert!(restarted.cleanup_resume.lock().contains_key(&drop_id));
    assert_eq!(
        drive_one(
            &db,
            &restarted,
            drop_id,
            &mut claim_sequence,
            batch_limits(1024, 1, 3, 16 * 1024 * 1024),
        )
        .await,
        CommittedOperationStep::Blocked
    );
    assert!(!restarted.cleanup_resume.lock().contains_key(&drop_id));
    crate::index_lifecycle::outbox::retry_operation(&db, scope, drop_id)
        .await
        .expect("blocked drop is requeued");
    assert_eq!(
        drive_to_terminal(&db, &restarted, drop_id, &mut claim_sequence).await,
        CommittedOperationStep::Completed
    );
    assert!(physical_keys(&db, scope, physical_index_id)
        .await
        .is_empty());
    assert!(matches!(
        read_index(&db, scope, &definition(None)).await.state(),
        IndexStateV2::Dropped { .. }
    ));
    db.close().await.expect("vector test database closes");
}

/// Proves a partitioned drop resumes inside each namespace, never carries a
/// position from one namespace into the next, and deletes a mapping only
/// once a scan from the start proved its namespace empty.
pub(super) async fn partitioned_drop_resumes_within_each_namespace() {
    let db = test_db("vector-driver-partitioned-resume").await;
    let scope = DataScope::LegacyUnscoped;
    let definition = definition(Some("account_id"));
    for entity_id in 0..8_u8 {
        put_source(
            &db,
            scope,
            u64::from(entity_id),
            &properties(
                [f32::from(entity_id), 5.0, 6.0],
                Some(10 * (1 + i64::from(entity_id % 2))),
            ),
        )
        .await;
    }
    let (build_id, index_id, generation) = create_build(&db, scope, &definition, 7).await;
    let driver = driver();
    let mut claim_sequence = 1;
    assert_eq!(
        drive_to_terminal(&db, &driver, build_id, &mut claim_sequence).await,
        CommittedOperationStep::Completed
    );
    let active = read_index(&db, scope, &definition).await;
    let active_handle = ActiveIndexHandle::try_from_record(scope, &active)
        .expect("partitioned Active record projects a handle");
    // Mapping key order is cleanup order.
    let mappings = mapping_values(&db, scope, index_id, generation).await;
    assert_eq!(mappings.len(), 2);
    let (first, second) = (
        mappings[0].physical_index_id.get(),
        mappings[1].physical_index_id.get(),
    );
    let first_keys = physical_keys(&db, scope, first).await;
    let second_keys = physical_keys(&db, scope, second).await;
    assert!(first_keys.len() > 6 && second_keys.len() > 6);
    let first_generation = ValidatedVectorGenerationHandle::try_from_active::<
        vector::distance::Euclidean,
    >(&active_handle, mappings[0].physical_index_id)
    .expect("partition mapping validates against the Active handle");
    let first_index =
        VectorIndex::<vector::distance::Euclidean>::from_generation(&first_generation);
    let first_position = VectorCleanupResume::Physical(
        first_index
            .cleanup_scan(&db, VectorCleanupFrom::Start)
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .expect("the first namespace holds rows"),
    );

    let drop_id = accepted_drop(
        drop_index_operation(&db, scope, &definition)
            .await
            .expect("partitioned drop enqueues"),
    );
    let three_rows = batch_limits(1024, 1024 * 1024, 3, 16 * 1024 * 1024);
    let mut steps = 0;
    loop {
        let step = drive_one(&db, &driver, drop_id, &mut claim_sequence, three_rows).await;
        steps += 1;
        assert!(steps < 64, "partitioned drop exceeded its bounded steps");
        let first_left = physical_keys(&db, scope, first).await;
        let second_left = physical_keys(&db, scope, second).await;
        let mappings_left = mapping_values(&db, scope, index_id, generation).await.len();
        // Namespaces empty in mapping order, each from its first key on, and
        // a mapping outlives its namespace by the step that proves it empty.
        assert_eq!(
            first_left,
            first_keys[first_keys.len() - first_left.len()..]
        );
        assert_eq!(
            second_left,
            second_keys[second_keys.len() - second_left.len()..]
        );
        assert!(first_left.is_empty() || second_left == second_keys);
        assert!(
            mappings_left
                >= usize::from(!first_left.is_empty()) + usize::from(!second_left.is_empty())
        );
        if step == CommittedOperationStep::Completed {
            assert_eq!(mappings_left, 0);
            break;
        }
        assert_eq!(step, CommittedOperationStep::Progressed);

        // A position of the first namespace is no position in the second:
        // the step scans the second from its start.
        let operation = read_operation(&db, scope, drop_id).await;
        let IndexOperationProgress::VectorCleanup(
            progress @ VectorCleanupProgress::DeletePhysical(_),
        ) = operation.progress()
        else {
            continue;
        };
        if mappings_left != 1 || second_left.is_empty() {
            continue;
        }
        let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
            unreachable!("fixture definition is vector");
        };
        let record = read_index(&db, scope, &definition).await;
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let stepped = step_cleanup::<vector::distance::Euclidean>(
            &transaction,
            scope,
            &operation,
            &record,
            vector_definition,
            progress,
            Some(&first_position),
            false,
            three_rows,
            driver.cache_registry.as_ref(),
        )
        .await
        .expect("a foreign position is ignored");
        assert_eq!(
            physical_keys(&transaction, scope, second).await,
            second_left[3.min(second_left.len())..],
            "the step deleted the second namespace's first rows"
        );
        assert!(
            match &stepped.committed_state {
                Some(CommittedStepState::VectorCleanup(VectorCleanupResume::Physical(row))) =>
                    !first_index.owns_cleanup_row(row),
                Some(
                    CommittedStepState::VectorCleanup(
                        VectorCleanupResume::Work(_)
                        | VectorCleanupResume::FirstPhysical { .. }
                        | VectorCleanupResume::FirstWork(_),
                    )
                    | CommittedStepState::VectorBuild(_),
                ) => false,
                None => true,
            },
            "the position handed on is in the namespace the step deleted from"
        );
        drop(transaction);
    }
    assert!(steps > 6, "each namespace took several steps");
    assert!(matches!(
        read_index(&db, scope, &definition).await.state(),
        IndexStateV2::Dropped { .. }
    ));
    db.close().await.expect("vector test database closes");
}

/// Proves build-delta and applied-state cleanup resumes past the last row it
/// deleted, in that row's prefix, and reports the rows exhausted only from a
/// scan that started at the first prefix.
pub(super) async fn delta_cleanup_resumes_past_deleted_rows_and_sweeps_before_finalizing() {
    let db = test_db("vector-driver-cleanup-delta-resume").await;
    let scope = DataScope::LegacyUnscoped;
    let driver = driver();
    let unpartitioned = definition(None);
    for entity_id in 0..3 {
        put_source(&db, scope, entity_id, &properties([1.0, 2.0, 3.0], None)).await;
    }
    let (build_id, index_id, generation) = create_build(&db, scope, &unpartitioned, 2).await;
    let mut claim_sequence = 1;
    let generous = 16 * 1024 * 1024;
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
    let delta_entity = IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(2),
    };
    let delta_key = scoped_index_key(
        scope,
        ScopedKey::BuildDelta(IndexEntityStateKey {
            index_id,
            generation,
            entity: delta_entity,
        }),
    );
    db.put(
        &delta_key,
        encode_build_delta(&CoalescedBuildDeltaValue {
            index_id,
            generation,
            entity_kind: delta_entity.kind,
            entity_id: delta_entity.id,
            state: crate::index_lifecycle::work::CoalescedBuildDeltaState::Marker,
        }),
    )
    .await
    .expect("fixture delta is written");
    assert!(matches!(
        drop_index_operation(&db, scope, &unpartitioned)
            .await
            .expect("building vector converts to abort cleanup"),
        IndexDdlReceipt::ExistingOperation { operation_id } if operation_id == build_id
    ));
    let operation = read_operation(&db, scope, build_id).await;
    let applied = prefix_keys(
        &db,
        generation_prefix(scope, RecordKind::AppliedState, index_id, generation),
    )
    .await;
    assert_eq!(applied.len(), 2, "one scanned batch applied two entities");
    let one_row = batch_limits(1, 1024 * 1024, 1024, generous);
    let all_rows = batch_limits(1024, 1024 * 1024, 1024, generous);
    let outside = generation_prefix(
        scope,
        RecordKind::VectorPartitionMapping,
        index_id,
        generation,
    );
    let after = |key: &Bytes| Some(VectorCleanupResume::Work(key.clone()));
    let first = |key: Option<&Bytes>| Some(VectorCleanupResume::FirstWork(key.cloned()));
    // (start, limits) => (rows left, exhausted, position handed on): deltas
    // sort before applied state.
    for (resume, limits, left, exhausted, next) in [
        // From the start, one row: the delta goes, the rest waits.
        (None, one_row, applied.clone(), false, Some(&delta_key)),
        // From the start, everything: this reports the rows exhausted.
        (None, all_rows, Vec::new(), true, None),
        // Past the delta: the scan reaches the end without rereading it,
        // which proves nothing about the rows before it.
        (
            after(&delta_key),
            all_rows,
            vec![delta_key.clone()],
            false,
            None,
        ),
        (
            after(&delta_key),
            one_row,
            vec![delta_key.clone(), applied[1].clone()],
            false,
            Some(&applied[0]),
        ),
        // Past an applied row: the delta prefix is not scanned at all.
        (
            after(&applied[0]),
            all_rows,
            vec![delta_key.clone(), applied[0].clone()],
            false,
            None,
        ),
        (
            after(&applied[1]),
            all_rows,
            vec![delta_key.clone(), applied[0].clone(), applied[1].clone()],
            false,
            None,
        ),
        // A key of neither prefix is no position: the scan starts over.
        (after(&outside), all_rows, Vec::new(), true, None),
        // From the first row a preparation read: that row goes too, and
        // reaching the end reports the rows exhausted.
        (first(Some(&delta_key)), all_rows, Vec::new(), true, None),
        (
            first(Some(&delta_key)),
            one_row,
            applied.clone(),
            false,
            Some(&delta_key),
        ),
        (
            first(Some(&applied[0])),
            all_rows,
            vec![delta_key.clone()],
            true,
            None,
        ),
        (first(Some(&outside)), all_rows, Vec::new(), true, None),
        // A preparation that read no row: the step reads nothing either.
        (
            first(None),
            all_rows,
            vec![delta_key.clone(), applied[0].clone(), applied[1].clone()],
            true,
            None,
        ),
        // A start of the physical stage is none here.
        (
            Some(VectorCleanupResume::FirstPhysical {
                physical_index_id: VectorPhysicalIndexId::new(1).unwrap(),
                row: None,
            }),
            all_rows,
            Vec::new(),
            true,
            None,
        ),
    ] {
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let CleanupWorkOutcome::Progress {
            exhausted: reported,
            resume: handed_on,
            ..
        } = delete_delta_and_applied_rows(
            &transaction,
            scope,
            &operation,
            resume.as_ref(),
            OperationCounters::default(),
            limits,
        )
        .await
        .expect("delta cleanup batch plans")
        else {
            panic!("every fixture row fits the cleanup budgets");
        };
        let mut rows_left = prefix_keys(
            &transaction,
            generation_prefix(scope, RecordKind::BuildDelta, index_id, generation),
        )
        .await;
        rows_left.extend(
            prefix_keys(
                &transaction,
                generation_prefix(scope, RecordKind::AppliedState, index_id, generation),
            )
            .await,
        );
        assert_eq!(rows_left, left, "rows left starting from {resume:?}");
        assert_eq!(reported, exhausted, "exhaustion starting from {resume:?}");
        assert_eq!(handed_on.as_ref(), next, "position after {resume:?}");
    }

    // Through the outbox, one row per step: each step hands its row to the
    // next, and the abort leaves nothing.
    let mut positions = Vec::new();
    loop {
        let step = drive_one(&db, &driver, build_id, &mut claim_sequence, one_row).await;
        if step == CommittedOperationStep::Completed {
            break;
        }
        assert_eq!(step, CommittedOperationStep::Progressed);
        assert!(positions.len() < 64, "abort exceeded its bounded steps");
        positions.push(driver.cleanup_resume.lock().get(&build_id).cloned());
    }
    let work = positions
        .iter()
        .filter_map(|position| match position {
            Some(VectorCleanupResume::Work(key)) => Some(key.clone()),
            Some(
                VectorCleanupResume::Physical(_)
                | VectorCleanupResume::FirstPhysical { .. }
                | VectorCleanupResume::FirstWork(_),
            )
            | None => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(work, [delta_key, applied[0].clone(), applied[1].clone()]);
    assert!(driver.cleanup_resume.lock().is_empty());
    for kind in [RecordKind::BuildDelta, RecordKind::AppliedState] {
        assert!(
            prefix_keys(&db, generation_prefix(scope, kind, index_id, generation))
                .await
                .is_empty()
        );
    }
    assert!(matches!(
        read_index(&db, scope, &unpartitioned).await.state(),
        IndexStateV2::Dropped { .. }
    ));
    db.close().await.expect("vector test database closes");
}

/// Proves which steps fence graph writes: every abort step and a drop's
/// completion do, and a drop's cache retirement and delete steps do not.
///
/// A graph transaction holds the scope's shared mutation permit, so a fenced
/// step is one whose permit keeps that permit from being granted.
pub(super) async fn drop_delete_steps_hold_no_write_fence_and_aborts_keep_theirs() {
    let scope = DataScope::LegacyUnscoped;
    let gates = Arc::new(IndexScopeGates::default());
    let driver = VectorIndexDriver::new(
        Arc::clone(&gates),
        Arc::new(VectorCacheRegistry::default()),
        Arc::new(SimHasherRegistry::default()),
    );
    let fenced = async |db: &Db, operation_id: IndexOperationId| {
        let operation = read_operation(db, scope, operation_id).await;
        let permit = driver
            .acquire_step_permit(db, scope, &operation)
            .await
            .expect("step permit is granted");
        let write = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            gates.mutation_permit(scope),
        )
        .await;
        drop(permit);
        (operation.progress().clone(), write.is_err())
    };

    let db = test_db("vector-driver-drop-gates").await;
    let (drop_id, _, mut claim_sequence) = unpartitioned_drop(&db, &driver).await;
    let mut stages = Vec::new();
    loop {
        let (progress, write_fenced) = fenced(&db, drop_id).await;
        let IndexOperationProgress::VectorCleanup(stage) = progress else {
            panic!("a drop runs cleanup stages");
        };
        stages.push(match stage {
            VectorCleanupProgress::RetireCache(_) => ("retire_cache", write_fenced),
            VectorCleanupProgress::DeletePhysical(_) => ("delete_physical", write_fenced),
            VectorCleanupProgress::DeleteDeltas(_) => ("delete_deltas", write_fenced),
            VectorCleanupProgress::Finalize(_) => ("finalize", write_fenced),
        });
        let step = drive_one(
            &db,
            &driver,
            drop_id,
            &mut claim_sequence,
            batch_limits(1024, 1024 * 1024, 8, 16 * 1024 * 1024),
        )
        .await;
        if step == CommittedOperationStep::Completed {
            break;
        }
        assert_eq!(step, CommittedOperationStep::Progressed);
        assert!(stages.len() < 64, "drop exceeded its bounded steps");
    }
    stages.dedup();
    assert_eq!(
        stages,
        [
            ("retire_cache", false),
            ("delete_physical", false),
            ("delete_deltas", false),
            ("finalize", true),
        ]
    );
    db.close().await.expect("vector test database closes");

    let db = test_db("vector-driver-abort-gates").await;
    let unpartitioned = definition(None);
    for entity_id in 0..3 {
        put_source(&db, scope, entity_id, &properties([1.0, 2.0, 3.0], None)).await;
    }
    let (build_id, _, _) = create_build(&db, scope, &unpartitioned, 2).await;
    let mut claim_sequence = 1;
    assert_eq!(
        drive_one(
            &db,
            &driver,
            build_id,
            &mut claim_sequence,
            batch_limits(2, 1024 * 1024, 1024, 16 * 1024 * 1024),
        )
        .await,
        CommittedOperationStep::Progressed
    );
    drop_index_operation(&db, scope, &unpartitioned)
        .await
        .expect("building vector converts to abort cleanup");
    let mut stages = Vec::new();
    loop {
        let (progress, write_fenced) = fenced(&db, build_id).await;
        let IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(stage)) = progress
        else {
            panic!("an aborted build runs cleanup stages");
        };
        stages.push(match stage {
            VectorCleanupProgress::RetireCache(_) => ("retire_cache", write_fenced),
            VectorCleanupProgress::DeletePhysical(_) => ("delete_physical", write_fenced),
            VectorCleanupProgress::DeleteDeltas(_) => ("delete_deltas", write_fenced),
            VectorCleanupProgress::Finalize(_) => ("finalize", write_fenced),
        });
        let step = drive_one(
            &db,
            &driver,
            build_id,
            &mut claim_sequence,
            batch_limits(1, 1024 * 1024, 8, 16 * 1024 * 1024),
        )
        .await;
        if step == CommittedOperationStep::Completed {
            break;
        }
        assert_eq!(step, CommittedOperationStep::Progressed);
        assert!(stages.len() < 64, "abort exceeded its bounded steps");
    }
    stages.dedup();
    assert_eq!(
        stages,
        [
            ("retire_cache", true),
            ("delete_physical", true),
            ("delete_deltas", true),
            ("finalize", true),
        ]
    );
    db.close().await.expect("vector test database closes");
}

/// Proves activation looks for applied state before it takes its permit, not
/// again under it, and still goes back to validation when some is left.
///
/// The activation permit stops planning and every writer, and a scan of the
/// applied-state prefix steps over every row validation deleted, so the step
/// must not read the prefix while it holds the permit.
pub(super) async fn activation_looks_for_applied_state_before_its_permit() {
    let db = test_db("vector-driver-activation-applied-state").await;
    let scope = DataScope::LegacyUnscoped;
    let definition = definition(None);
    for entity_id in 0..3 {
        put_source(&db, scope, entity_id, &properties([1.0, 2.0, 3.0], None)).await;
    }
    let (build_id, index_id, generation) = create_build(&db, scope, &definition, 2).await;
    let driver = driver();
    let limits = SearchIndexBackfillLimits::default().batch();
    let mut claim_sequence = 1;
    let applied_prefix = generation_prefix(scope, RecordKind::AppliedState, index_id, generation);
    let activating = async |db: &Db| {
        matches!(
            read_operation(db, scope, build_id).await.progress(),
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                VectorBuildStage::Activate(_)
            ))
        )
    };
    // One applied-state row of the build, kept to put back at activation.
    let mut applied = None;
    for _ in 0..64 {
        if activating(&db).await {
            break;
        }
        assert_eq!(
            drive_one(&db, &driver, build_id, &mut claim_sequence, limits).await,
            CommittedOperationStep::Progressed
        );
        let mut rows = db.scan_prefix(&applied_prefix, ..).await.unwrap();
        if let Some(row) = rows.next().await.unwrap() {
            applied = Some((row.key, row.value));
        }
    }
    assert!(activating(&db).await, "the build reached activation");
    let (applied_key, applied_value) = applied.expect("the scan applied its entities");
    assert!(prefix_keys(&db, applied_prefix.clone()).await.is_empty());

    // Applied state left at activation: its preparation finds it, and the
    // step sends the build back to validation, which releases the row.
    db.put(&applied_key, &applied_value).await.unwrap();
    assert_eq!(
        drive_one(&db, &driver, build_id, &mut claim_sequence, limits).await,
        CommittedOperationStep::Progressed
    );
    assert!(matches!(
        read_operation(&db, scope, build_id).await.progress(),
        IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
            VectorBuildStage::ValidateDescriptor(_)
        ))
    ));
    assert!(driver.activations_without_applied_state.lock().is_empty());
    for _ in 0..64 {
        if activating(&db).await {
            break;
        }
        assert_eq!(
            drive_one(&db, &driver, build_id, &mut claim_sequence, limits).await,
            CommittedOperationStep::Progressed
        );
    }
    assert!(prefix_keys(&db, applied_prefix.clone()).await.is_empty());

    // None left: the preparation records that before taking the permit. A
    // row put back afterwards, which no writer can do while the build owns
    // its generation, goes unseen: the step does not read the prefix again.
    let operation = read_operation(&db, scope, build_id).await;
    let prepared = driver
        .prepare_step(&db, scope, &operation, limits)
        .await
        .expect("activation prepares");
    assert!(driver
        .activations_without_applied_state
        .lock()
        .contains(&build_id));
    db.put(&applied_key, &applied_value).await.unwrap();
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    assert!(matches!(
        driver
            .step(&db, &transaction, scope, &operation, limits)
            .await
            .expect("prepared activation stages")
            .result(),
        IndexOperationStepResult::Completed(IndexOperationOutcome::Build(
            BuildOperationOutcome::Succeeded
        ))
    ));
    drop(transaction);
    // The observation belonged to that step alone: one that was not prepared
    // with it scans in its transaction and finds the row.
    assert!(driver.activations_without_applied_state.lock().is_empty());
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    assert!(matches!(
        driver
            .step(&db, &transaction, scope, &operation, limits)
            .await
            .expect("unprepared activation stages")
            .result(),
        IndexOperationStepResult::Progressed(IndexOperationProgress::VectorBuild(
            VectorBuildProgress::Constructing(VectorBuildStage::ValidateDescriptor(_))
        ))
    ));
    drop(transaction);
    drop(prepared);

    db.delete(&applied_key).await.unwrap();
    assert_eq!(
        drive_to_terminal(&db, &driver, build_id, &mut claim_sequence).await,
        CommittedOperationStep::Completed
    );
    assert!(matches!(
        read_index(&db, scope, &definition).await.state(),
        IndexStateV2::Active { .. }
    ));
    assert!(driver.activations_without_applied_state.lock().is_empty());
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
    let mut transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
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
    // Every applied-state row of this fixture names another partition, and a
    // partition costs a metadata read: the lane stops at the entity ceiling's
    // worth of them, and at the output-operation ceiling after as many rows
    // as it may delete.
    let resumed_at = |result: Result<IndexOperationStepResult>| {
        let Ok(IndexOperationStepResult::Progressed(IndexOperationProgress::VectorBuild(
            VectorBuildProgress::Constructing(VectorBuildStage::ValidateDescriptor(
                PrefixScanProgress {
                    cursor: Some(cursor),
                    ..
                },
            )),
        ))) = result
        else {
            panic!("validation resumes at a cursor");
        };
        Bytes::copy_from_slice(cursor.as_bytes())
    };
    for (limits, last_deleted) in [
        (batch_limits(1024, generous, 2, generous), &applied[1].0),
        (batch_limits(1, generous, 1024, generous), &applied[0].0),
        (batch_limits(2, generous, 1024, generous), &applied[1].0),
    ] {
        drop(transaction);
        transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert_eq!(
            &resumed_at(
                validation_result(
                    &db,
                    &transaction,
                    &operation,
                    &record,
                    &definition,
                    None,
                    limits
                )
                .await
            ),
            last_deleted
        );
    }
    drop(transaction);
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
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

    // A mapped partition without physical metadata fails closed, in the
    // mapping lane and in the applied-state lane. The applied-state lane
    // checks each partition it meets, not only the first: a batch that may
    // delete two rows fails on the second entity, whose partition lost its
    // metadata, before it would hand on a cursor.
    let missing = mapping_values(&db, scope, index_id, generation).await[1].physical_index_id;
    for (cursor, limits) in [
        (
            Some(mappings[0].0.clone()),
            SearchIndexBackfillLimits::default().batch(),
        ),
        (None, batch_limits(1024, generous, 2, generous)),
    ] {
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        transaction
            .delete(
                DataKey::Data {
                    scope,
                    kind: DataKeyKind::Vector(VectorKey::IndexMetadata(
                        VectorIndexMetadataKey::new(missing.get()),
                    )),
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
                cursor,
                limits,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
    }

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

/// The generation an enqueued drop of the unpartitioned fixture removes.
async fn dropping_generation(db: &Db) -> dropped::DroppedGeneration {
    let record = read_index(db, DataScope::LegacyUnscoped, &definition(None)).await;
    assert!(matches!(record.state(), IndexStateV2::Dropping { .. }));
    dropped::DroppedGeneration {
        index_id: record.index_id(),
        generation: record.state().generation(),
    }
}

/// Writes one build delta of `entry`'s generation, as a build leaves behind
/// for a write it has not applied.
async fn put_build_delta(db: &Db, entry: dropped::DroppedGeneration) {
    let entity = IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(2),
    };
    db.put(
        scoped_index_key(
            DataScope::LegacyUnscoped,
            ScopedKey::BuildDelta(IndexEntityStateKey {
                index_id: entry.index_id,
                generation: entry.generation,
                entity,
            }),
        ),
        encode_build_delta(&CoalescedBuildDeltaValue {
            index_id: entry.index_id,
            generation: entry.generation,
            entity_kind: entity.kind,
            entity_id: entity.id,
            state: crate::index_lifecycle::work::CoalescedBuildDeltaState::Marker,
        }),
    )
    .await
    .expect("fixture build delta is written");
}

/// Returns the keys of every vector row keyed by `entry`'s generation.
async fn generation_keys(db: &Db, entry: dropped::DroppedGeneration) -> Vec<Bytes> {
    let mut keys = Vec::new();
    for kind in [RecordKind::BuildDelta, RecordKind::AppliedState] {
        keys.extend(
            prefix_keys(
                db,
                ScopedKey::generation_prefix(kind, entry.index_id, entry.generation),
            )
            .await,
        );
    }
    keys
}

/// Proves a drop on an admitted store deletes only the rows no compaction
/// filter may match and leaves its entry in the step that writes `Dropped`.
///
/// The drop takes three steps however many rows the generation has. Until
/// the last one commits the record is `Dropping` and no entry exists; the
/// last one writes both. Every hot, layer-0 and generation-keyed row stays
/// for compactions.
pub(super) async fn an_admitted_drop_deletes_only_unfiltered_rows_and_leaves_its_entry() {
    let db = test_db("vector-driver-dropped-entry").await;
    dropped::admit_fixture(&db).await;
    let scope = DataScope::LegacyUnscoped;
    let driver = driver();
    let (drop_id, physical_index_id, mut claim_sequence) = unpartitioned_drop(&db, &driver).await;
    let entry = dropping_generation(&db).await;
    // A build delta the build left behind: the drop leaves it to compactions.
    put_build_delta(&db, entry).await;
    let core = prefix_keys(
        &db,
        DataKey::data_prefix(
            scope,
            VectorStorageLane::Core
                .prefix_key(physical_index_id)
                .to_bytes(),
        ),
    )
    .await;
    assert!(!core.is_empty());
    let filtered = physical_keys(&db, scope, physical_index_id)
        .await
        .into_iter()
        .filter(|key| !core.contains(key))
        .collect::<Vec<_>>();
    assert!(
        filtered.len() > 9,
        "row-by-row cleanup would take several batches"
    );
    let generation_rows = generation_keys(&db, entry).await;
    assert!(!generation_rows.is_empty());
    // Too few operations a step for the namespace, enough for the core rows.
    let three_rows = batch_limits(1024, 1024 * 1024, 3, 16 * 1024 * 1024);
    assert!(core.len() <= 3);

    for step in ["retire the cache", "delete the unfiltered rows"] {
        assert_eq!(
            drive_one(&db, &driver, drop_id, &mut claim_sequence, three_rows).await,
            CommittedOperationStep::Progressed,
            "{step}"
        );
        assert!(matches!(
            read_index(&db, scope, &definition(None)).await.state(),
            IndexStateV2::Dropping { .. }
        ));
        assert_eq!(dropped::load_entries(&db).await.unwrap(), vec![], "{step}");
    }
    assert_eq!(physical_keys(&db, scope, physical_index_id).await, filtered);
    assert!(matches!(
        read_operation(&db, scope, drop_id).await.progress(),
        IndexOperationProgress::VectorCleanup(VectorCleanupProgress::Finalize(_))
    ));

    assert_eq!(
        drive_one(&db, &driver, drop_id, &mut claim_sequence, three_rows).await,
        CommittedOperationStep::Completed
    );
    assert!(matches!(
        read_index(&db, scope, &definition(None)).await.state(),
        IndexStateV2::Dropped { .. }
    ));
    assert_eq!(
        dropped::load_entries(&db).await.unwrap(),
        vec![(
            entry,
            DroppedGenerationValue {
                scope,
                rows: DroppedGenerationRows::UnpartitionedVector {
                    physical_index_id: VectorPhysicalIndexId::new(physical_index_id).unwrap(),
                },
            }
        )]
    );
    assert_eq!(physical_keys(&db, scope, physical_index_id).await, filtered);
    assert_eq!(generation_keys(&db, entry).await, generation_rows);
    db.close().await.expect("vector test database closes");
}

/// Proves a drop leaves no entry where compactions must not discard its
/// rows, and deletes every row itself.
///
/// A store below the admitting storage version can still be opened by a
/// binary that knows no entries. A generation with a legacy reservation was
/// adopted from legacy storage, whose physical id this catalog did not
/// allocate; the drop deletes the reservation with its last step.
pub(super) async fn a_drop_that_must_not_be_filtered_deletes_every_row_and_leaves_no_entry() {
    for (name, admitted, adopted) in [
        ("vector-driver-unadmitted-drop", false, false),
        ("vector-driver-adopted-drop", true, true),
        ("vector-driver-unadmitted-adopted-drop", false, true),
    ] {
        let db = test_db(name).await;
        if admitted {
            dropped::admit_fixture(&db).await;
        }
        let scope = DataScope::LegacyUnscoped;
        let driver = driver();
        let (drop_id, physical_index_id, mut claim_sequence) =
            unpartitioned_drop(&db, &driver).await;
        let entry = dropping_generation(&db).await;
        let reservation_key = IndexKey::Global {
            kind: GlobalKey::LegacyVectorPhysicalReservation(
                VectorPhysicalIndexId::new(physical_index_id).unwrap(),
            ),
        }
        .to_bytes();
        if adopted {
            db.put(
                &reservation_key,
                encode_metadata_value(&IndexV2MetadataValue::LegacyVectorPhysicalReservation(
                    LegacyVectorPhysicalReservation::AdoptedActive {
                        index_id: entry.index_id,
                        generation: entry.generation,
                    },
                )),
            )
            .await
            .expect("fixture reservation is written");
        }
        assert_eq!(
            drive_to_terminal(&db, &driver, drop_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed,
            "{name}"
        );
        assert!(matches!(
            read_index(&db, scope, &definition(None)).await.state(),
            IndexStateV2::Dropped { .. }
        ));
        assert_eq!(dropped::load_entries(&db).await.unwrap(), vec![], "{name}");
        assert_eq!(
            physical_keys(&db, scope, physical_index_id).await,
            Vec::<Bytes>::new(),
            "{name}"
        );
        assert_eq!(
            generation_keys(&db, entry).await,
            Vec::<Bytes>::new(),
            "{name}"
        );
        assert_eq!(db.get(&reservation_key).await.unwrap(), None, "{name}");
        db.close().await.expect("vector test database closes");
    }
}

/// Proves a drop whose unfiltered rows do not fit one step deletes them over
/// as many steps as they take, and nothing else: every row a compaction
/// filter matches is still left to its entry.
///
/// The counters of the earlier steps do not turn the drop into a row-by-row
/// one: the scan of the unfiltered rows resumes where the last step stopped.
pub(super) async fn a_drop_whose_unfiltered_rows_need_several_steps_leaves_the_rest() {
    let db = test_db("vector-driver-dropped-entry-fallback").await;
    dropped::admit_fixture(&db).await;
    let scope = DataScope::LegacyUnscoped;
    let driver = driver();
    let (drop_id, physical_index_id, mut claim_sequence) = unpartitioned_drop(&db, &driver).await;
    let entry = dropping_generation(&db).await;
    // A second core row beside the metadata: one step cannot hold both.
    db.put(
        DataKey::Data {
            scope,
            kind: DataKeyKind::Vector(
                crate::encoding::v2::keys::indexes::vector::VectorKey::TxnGuard(
                    crate::encoding::v2::legacy::vector::transaction_guard::LegacyVectorTxnGuardKey::new(
                        physical_index_id,
                    ),
                ),
            ),
        }
        .to_bytes(),
        b"fixture guard",
    )
    .await
    .expect("fixture core row is written");
    let core = core_keys(&db, scope, physical_index_id).await;
    assert_eq!(core.len(), 2);
    let filtered = physical_keys(&db, scope, physical_index_id)
        .await
        .into_iter()
        .filter(|key| !core.contains(key))
        .collect::<Vec<_>>();
    assert!(filtered.len() > 8, "the namespace has rows to leave");
    let one_row = batch_limits(1024, 1024 * 1024, 1, 16 * 1024 * 1024);

    // Retiring the cache, then one core row: one remains.
    for _ in 0..2 {
        assert_eq!(
            drive_one(&db, &driver, drop_id, &mut claim_sequence, one_row).await,
            CommittedOperationStep::Progressed
        );
    }
    assert_eq!(core_keys(&db, scope, physical_index_id).await.len(), 1);
    assert!(deleting_physical(&db, drop_id).await);

    let mut steps = 0;
    while drive_one(&db, &driver, drop_id, &mut claim_sequence, one_row).await
        == CommittedOperationStep::Progressed
    {
        steps += 1;
        assert!(steps < 8, "the filtered rows took steps of their own");
        assert_eq!(dropped::load_entries(&db).await.unwrap(), vec![]);
    }
    assert_eq!(physical_keys(&db, scope, physical_index_id).await, filtered);
    assert_eq!(
        dropped::load_entries(&db)
            .await
            .unwrap()
            .into_iter()
            .map(|(entry, _)| entry)
            .collect::<Vec<_>>(),
        vec![entry]
    );
    db.close().await.expect("vector test database closes");
}

/// Proves the entry and the `Dropped` record commit together or not at all:
/// a step that fails where the entry is staged blocks the operation, leaving
/// the record `Dropping` with no entry, and its retry writes both.
pub(super) async fn a_failed_final_step_leaves_neither_the_entry_nor_the_dropped_record() {
    let db = test_db("vector-driver-dropped-entry-failpoint").await;
    dropped::admit_fixture(&db).await;
    let scope = DataScope::LegacyUnscoped;
    let driver = driver();
    let (drop_id, _, mut claim_sequence) = unpartitioned_drop(&db, &driver).await;
    let entry = dropping_generation(&db).await;
    let limits = SearchIndexBackfillLimits::default().batch();
    for _ in 0..2 {
        assert_eq!(
            drive_one(&db, &driver, drop_id, &mut claim_sequence, limits).await,
            CommittedOperationStep::Progressed
        );
    }

    failpoints::inject_once(IndexOutboxFailpoint::DroppedEntryStagingBefore).unwrap();
    assert_eq!(
        drive_one(&db, &driver, drop_id, &mut claim_sequence, limits).await,
        CommittedOperationStep::Blocked
    );
    assert!(failpoints::was_triggered());
    assert!(matches!(
        read_index(&db, scope, &definition(None)).await.state(),
        IndexStateV2::Dropping { .. }
    ));
    assert!(matches!(
        read_operation(&db, scope, drop_id).await.progress(),
        IndexOperationProgress::VectorCleanup(VectorCleanupProgress::Finalize(_))
    ));
    assert_eq!(dropped::load_entries(&db).await.unwrap(), vec![]);

    crate::index_lifecycle::outbox::retry_operation(&db, scope, drop_id)
        .await
        .expect("the blocked drop is retried");
    assert_eq!(
        drive_one(&db, &driver, drop_id, &mut claim_sequence, limits).await,
        CommittedOperationStep::Completed
    );
    assert!(matches!(
        read_index(&db, scope, &definition(None)).await.state(),
        IndexStateV2::Dropped { .. }
    ));
    assert_eq!(
        dropped::load_entries(&db)
            .await
            .unwrap()
            .into_iter()
            .map(|(entry, _)| entry)
            .collect::<Vec<_>>(),
        vec![entry]
    );
    db.close().await.expect("vector test database closes");
}

/// Proves a store admitted while a drop is half-way through its rows hands
/// what is left to the drop's entry.
///
/// This is the store a binary that knows no entries leaves behind when it is
/// replaced mid-drop. The new binary deletes no row a compaction filter
/// matches, however many the old one had deleted and counted.
pub(super) async fn a_drop_admitted_half_way_is_handed_to_its_entry() {
    let db = test_db("vector-driver-dropped-entry-admitted-half-way").await;
    let scope = DataScope::LegacyUnscoped;
    let driver = driver();
    let (drop_id, physical_index_id, mut claim_sequence) = unpartitioned_drop(&db, &driver).await;
    let entry = dropping_generation(&db).await;
    let rows = physical_keys(&db, scope, physical_index_id).await.len();
    let one_row = batch_limits(1024, 1024 * 1024, 1, 16 * 1024 * 1024);
    // Retiring the cache, then two rows, one by one.
    for _ in 0..3 {
        assert_eq!(
            drive_one(&db, &driver, drop_id, &mut claim_sequence, one_row).await,
            CommittedOperationStep::Progressed
        );
    }
    let left = physical_keys(&db, scope, physical_index_id).await;
    assert_eq!(left.len(), rows - 2);
    // The core rows went first: only rows a filter matches are left.
    assert_eq!(
        core_keys(&db, scope, physical_index_id).await,
        Vec::<Bytes>::new()
    );

    dropped::admit_fixture(&db).await;
    let mut steps = 0;
    while drive_one(&db, &driver, drop_id, &mut claim_sequence, one_row).await
        == CommittedOperationStep::Progressed
    {
        steps += 1;
        assert!(steps < 8, "the rows left took steps of their own");
        assert_eq!(dropped::load_entries(&db).await.unwrap(), vec![]);
    }
    assert_eq!(physical_keys(&db, scope, physical_index_id).await, left);
    assert_eq!(
        dropped::load_entries(&db)
            .await
            .unwrap()
            .into_iter()
            .map(|(entry, _)| entry)
            .collect::<Vec<_>>(),
        vec![entry]
    );
    db.close().await.expect("vector test database closes");
}

/// Builds the partitioned fixture over two tenants of four entities each and
/// enqueues its drop. Returns the drop, the generation it removes, the
/// physical ids of its partitions in mapping order and the next claim
/// sequence.
async fn partitioned_drop(
    db: &Db,
    driver: &VectorIndexDriver,
) -> (IndexOperationId, dropped::DroppedGeneration, [u64; 2], u64) {
    let scope = DataScope::LegacyUnscoped;
    let definition = definition(Some("account_id"));
    for entity_id in 0..8_u8 {
        put_source(
            db,
            scope,
            u64::from(entity_id),
            &properties(
                [f32::from(entity_id), 5.0, 6.0],
                Some(10 * (1 + i64::from(entity_id % 2))),
            ),
        )
        .await;
    }
    let (build_id, index_id, generation) = create_build(db, scope, &definition, 7).await;
    let mut claim_sequence = 1;
    assert_eq!(
        drive_to_terminal(db, driver, build_id, &mut claim_sequence).await,
        CommittedOperationStep::Completed
    );
    let mappings = mapping_values(db, scope, index_id, generation).await;
    let [first, second] = &mappings[..] else {
        panic!("the fixture has two partitions");
    };
    let drop_id = accepted_drop(
        drop_index_operation(db, scope, &definition)
            .await
            .expect("partitioned drop enqueues"),
    );
    (
        drop_id,
        dropped::DroppedGeneration {
            index_id,
            generation,
        },
        [
            first.physical_index_id.get(),
            second.physical_index_id.get(),
        ],
        claim_sequence,
    )
}

/// Returns the keys of one vector namespace in the lane no compaction filter
/// may match.
async fn core_keys(db: &Db, scope: DataScope, physical_index_id: u64) -> Vec<Bytes> {
    prefix_keys(
        db,
        DataKey::data_prefix(
            scope,
            VectorStorageLane::Core
                .prefix_key(physical_index_id)
                .to_bytes(),
        ),
    )
    .await
}

/// The physical ids the partition mappings of `entry` name, in mapping order.
async fn mapped_physical_ids(db: &Db, entry: dropped::DroppedGeneration) -> Vec<u64> {
    mapping_values(
        db,
        DataScope::LegacyUnscoped,
        entry.index_id,
        entry.generation,
    )
    .await
    .iter()
    .map(|mapping| mapping.physical_index_id.get())
    .collect()
}

/// Proves a partitioned drop on an admitted store takes one step per
/// partition, deletes only the rows no compaction filter may match, keeps
/// every partition mapping and leaves its entry in the step that writes
/// `Dropped`.
///
/// The mappings are the only list of the physical ids the entry gives up, so
/// they stay until a reclaim deletes them with the entry. Until the last
/// step commits the record is `Dropping`, no entry exists and a refresh arms
/// nothing; afterwards a refresh arms the generation and one physical index
/// for each mapping.
pub(super) async fn an_admitted_partitioned_drop_keeps_its_mappings_and_leaves_its_entry() {
    let db = test_db("vector-driver-dropped-partitioned-entry").await;
    dropped::admit_fixture(&db).await;
    let scope = DataScope::LegacyUnscoped;
    let driver = driver();
    let (drop_id, entry, partitions, mut claim_sequence) = partitioned_drop(&db, &driver).await;
    let definition = definition(Some("account_id"));
    let mut all = Vec::new();
    let mut filtered = Vec::new();
    for physical_index_id in partitions {
        let core = core_keys(&db, scope, physical_index_id).await;
        assert!(!core.is_empty() && core.len() <= 3);
        let keys = physical_keys(&db, scope, physical_index_id).await;
        filtered.push(
            keys.iter()
                .filter(|key| !core.contains(key))
                .cloned()
                .collect::<Vec<_>>(),
        );
        assert!(
            filtered.last().is_some_and(|rows| rows.len() > 3),
            "row-by-row cleanup would take several batches"
        );
        all.push(keys);
    }
    let generation_rows = generation_keys(&db, entry).await;
    // Too few operations a step for a namespace, enough for its core rows.
    let three_rows = batch_limits(1024, 1024 * 1024, 3, 16 * 1024 * 1024);
    let registry = dropped::Registry::default();

    for (step, core_deleted) in [
        ("retire the cache", [false, false]),
        ("the first partition", [true, false]),
        ("the second partition", [true, true]),
    ] {
        assert_eq!(
            drive_one(&db, &driver, drop_id, &mut claim_sequence, three_rows).await,
            CommittedOperationStep::Progressed,
            "{step}"
        );
        for partition in 0..2 {
            assert_eq!(
                physical_keys(&db, scope, partitions[partition]).await,
                if core_deleted[partition] {
                    filtered[partition].clone()
                } else {
                    all[partition].clone()
                },
                "{step}"
            );
        }
        assert_eq!(mapped_physical_ids(&db, entry).await, partitions, "{step}");
        assert!(matches!(
            read_index(&db, scope, &definition).await.state(),
            IndexStateV2::Dropping { .. }
        ));
        assert_eq!(dropped::load_entries(&db).await.unwrap(), vec![], "{step}");
        registry
            .refresh(&db)
            .await
            .expect("a store without entries arms nothing");
        assert_eq!(registry.armed(), vec![], "{step}");
    }
    assert!(matches!(
        read_operation(&db, scope, drop_id).await.progress(),
        IndexOperationProgress::VectorCleanup(VectorCleanupProgress::Finalize(_))
    ));

    assert_eq!(
        drive_one(&db, &driver, drop_id, &mut claim_sequence, three_rows).await,
        CommittedOperationStep::Completed
    );
    assert!(matches!(
        read_index(&db, scope, &definition).await.state(),
        IndexStateV2::Dropped { .. }
    ));
    assert_eq!(
        dropped::load_entries(&db).await.unwrap(),
        vec![(
            entry,
            DroppedGenerationValue {
                scope,
                rows: DroppedGenerationRows::PartitionedVector,
            }
        )]
    );
    for partition in 0..2 {
        assert_eq!(
            physical_keys(&db, scope, partitions[partition]).await,
            filtered[partition]
        );
    }
    assert_eq!(mapped_physical_ids(&db, entry).await, partitions);
    assert_eq!(generation_keys(&db, entry).await, generation_rows);

    registry.refresh(&db).await.expect("the drop's entry arms");
    let armed = registry.armed();
    let [(armed_entry, armed)] = &armed[..] else {
        panic!("one entry is armed");
    };
    assert_eq!(*armed_entry, entry);
    assert_eq!(armed.owners.len(), 3);
    assert_eq!(
        armed
            .owners
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>(),
        std::iter::once(dropped::ownership::OwnerKey::Generation {
            scope,
            index_id: entry.index_id.get(),
            generation: entry.generation.get(),
        })
        .chain(partitions.into_iter().map(|physical_index_id| {
            dropped::ownership::OwnerKey::PhysicalIndex {
                scope,
                physical_index_id,
            }
        }))
        .collect::<std::collections::HashSet<_>>()
    );
    db.close().await.expect("vector test database closes");
}

/// Proves a partitioned drop that a store is admitted under half-way is
/// handed to its entry, wherever the admission finds it.
///
/// A row-by-row drop deletes each mapping with every row of its partition
/// before it moves on, so the mappings left at admission name exactly the
/// partitions that still have rows. From there the drop deletes only the
/// rows no filter matches and keeps those mappings, and its entry gives up
/// the generation and the partitions they name.
pub(super) async fn a_partitioned_drop_admitted_half_way_is_handed_to_its_entry() {
    for (name, mappings_left_at_admission) in [
        ("vector-driver-dropped-partitioned-half-way-first", 2),
        ("vector-driver-dropped-partitioned-half-way-second", 1),
    ] {
        let db = test_db(name).await;
        let scope = DataScope::LegacyUnscoped;
        let driver = driver();
        let (drop_id, entry, partitions, mut claim_sequence) = partitioned_drop(&db, &driver).await;
        let three_rows = batch_limits(1024, 1024 * 1024, 3, 16 * 1024 * 1024);
        // Retiring the cache and three rows of the first namespace, then on
        // until the mappings left say where the drop is.
        let mut steps = 0;
        while steps < 2 || mapped_physical_ids(&db, entry).await.len() > mappings_left_at_admission
        {
            assert_eq!(
                drive_one(&db, &driver, drop_id, &mut claim_sequence, three_rows).await,
                CommittedOperationStep::Progressed,
                "{name}"
            );
            steps += 1;
            assert!(steps < 64, "{name}: the drop exceeded its bounded steps");
        }
        let mapped = mapped_physical_ids(&db, entry).await;
        assert_eq!(
            mapped,
            partitions[2 - mappings_left_at_admission..],
            "{name}"
        );
        // What a filter matches in each partition at admission.
        let mut filtered = Vec::new();
        for physical_index_id in partitions {
            let core = core_keys(&db, scope, physical_index_id).await;
            filtered.push(
                physical_keys(&db, scope, physical_index_id)
                    .await
                    .into_iter()
                    .filter(|key| !core.contains(key))
                    .collect::<Vec<_>>(),
            );
        }
        assert!(
            filtered[1].len() > 3,
            "{name}: the second namespace has rows left"
        );
        assert_eq!(
            filtered[0].is_empty(),
            mappings_left_at_admission == 1,
            "{name}"
        );

        dropped::admit_fixture(&db).await;
        let mut after = 0;
        while drive_one(&db, &driver, drop_id, &mut claim_sequence, three_rows).await
            == CommittedOperationStep::Progressed
        {
            after += 1;
            assert!(after < 8, "{name}: the rows left took steps of their own");
            assert_eq!(dropped::load_entries(&db).await.unwrap(), vec![], "{name}");
        }
        for (physical_index_id, filtered) in partitions.into_iter().zip(&filtered) {
            assert_eq!(
                &physical_keys(&db, scope, physical_index_id).await,
                filtered,
                "{name}"
            );
        }
        assert_eq!(mapped_physical_ids(&db, entry).await, mapped, "{name}");
        assert_eq!(
            dropped::load_entries(&db).await.unwrap(),
            vec![(
                entry,
                DroppedGenerationValue {
                    scope,
                    rows: DroppedGenerationRows::PartitionedVector,
                }
            )],
            "{name}"
        );
        let registry = dropped::Registry::default();
        registry.refresh(&db).await.expect("the entry arms");
        assert_eq!(
            registry.armed()[0].1.owners.len(),
            1 + mappings_left_at_admission,
            "{name}: the generation and the partitions still mapped"
        );
        db.close().await.expect("vector test database closes");
    }
}

/// Proves what a partitioned drop does when the unfiltered rows of one
/// partition do not fit one step, the first partition or a later one: the
/// cursor stays on the partition until a scan from its start finds no
/// unfiltered row, and the drop still leaves every other row and every
/// mapping to its entry.
pub(super) async fn a_partition_whose_unfiltered_rows_need_several_steps() {
    for (name, slow_partition) in [
        ("vector-driver-dropped-partitioned-slow-first", 0),
        ("vector-driver-dropped-partitioned-slow-second", 1),
    ] {
        let db = test_db(name).await;
        dropped::admit_fixture(&db).await;
        let scope = DataScope::LegacyUnscoped;
        let driver = driver();
        let (drop_id, entry, partitions, mut claim_sequence) = partitioned_drop(&db, &driver).await;
        // A second core row beside the metadata: one step cannot hold both.
        db.put(
            DataKey::Data {
                scope,
                kind: DataKeyKind::Vector(
                    crate::encoding::v2::keys::indexes::vector::VectorKey::TxnGuard(
                        crate::encoding::v2::legacy::vector::transaction_guard::LegacyVectorTxnGuardKey::new(
                            partitions[slow_partition],
                        ),
                    ),
                ),
            }
            .to_bytes(),
            b"fixture guard",
        )
        .await
        .expect("fixture core row is written");
        let mut filtered = Vec::new();
        for physical_index_id in partitions {
            let core = core_keys(&db, scope, physical_index_id).await;
            filtered.push(
                physical_keys(&db, scope, physical_index_id)
                    .await
                    .into_iter()
                    .filter(|key| !core.contains(key))
                    .collect::<Vec<_>>(),
            );
        }
        let one_row = batch_limits(1024, 1024 * 1024, 1, 16 * 1024 * 1024);

        let mut steps = 0;
        while drive_one(&db, &driver, drop_id, &mut claim_sequence, one_row).await
            == CommittedOperationStep::Progressed
        {
            steps += 1;
            assert!(steps < 512, "{name}: the drop exceeded its bounded steps");
            assert_eq!(dropped::load_entries(&db).await.unwrap(), vec![], "{name}");
        }
        for partition in 0..2 {
            assert_eq!(
                physical_keys(&db, scope, partitions[partition]).await,
                filtered[partition],
                "{name}"
            );
        }
        assert_eq!(
            mapped_physical_ids(&db, entry).await,
            partitions.to_vec(),
            "{name}"
        );
        // The cache, one row, two rows over three steps, the last step.
        assert_eq!(steps, 5, "{name}");
        assert_eq!(
            dropped::load_entries(&db).await.unwrap(),
            vec![(
                entry,
                DroppedGenerationValue {
                    scope,
                    rows: DroppedGenerationRows::PartitionedVector,
                }
            )],
            "{name}"
        );
        db.close().await.expect("vector test database closes");
    }
}

/// Writes `value` as the entry of `entry`, as only a damaged store holds it.
async fn put_entry(db: &Db, entry: dropped::DroppedGeneration, value: DroppedGenerationValue) {
    db.put(
        IndexKey::Global {
            kind: GlobalKey::DroppedGeneration {
                index_id: entry.index_id,
                generation: entry.generation,
            },
        }
        .to_bytes(),
        encode_metadata_value(&IndexV2MetadataValue::DroppedGeneration(value)),
    )
    .await
    .expect("fixture entry is written");
}

/// Drops the unpartitioned fixture on an admitted store. Returns the entry
/// the drop left, its value, the physical id it names and the next claim
/// sequence.
async fn completed_drop(
    db: &Db,
    driver: &VectorIndexDriver,
) -> (
    dropped::DroppedGeneration,
    DroppedGenerationValue,
    VectorPhysicalIndexId,
    u64,
) {
    dropped::admit_fixture(db).await;
    let (drop_id, _, mut claim_sequence) = unpartitioned_drop(db, driver).await;
    let entry = dropping_generation(db).await;
    assert_eq!(
        drive_to_terminal(db, driver, drop_id, &mut claim_sequence).await,
        CommittedOperationStep::Completed
    );
    let entries = dropped::load_entries(db).await.unwrap();
    let [(stored, value)] = entries[..] else {
        panic!("the drop leaves one entry");
    };
    assert_eq!(stored, entry);
    let DroppedGenerationRows::UnpartitionedVector { physical_index_id } = value.rows else {
        panic!("an unpartitioned drop names its physical id");
    };
    (entry, value, physical_index_id, claim_sequence)
}

/// Builds the unpartitioned fixture again after its drop and returns the new
/// generation and its physical id.
async fn recreated(
    db: &Db,
    driver: &VectorIndexDriver,
    claim_sequence: &mut u64,
) -> (IndexGenerationId, VectorPhysicalIndexId) {
    let scope = DataScope::LegacyUnscoped;
    let (build_id, _, generation) = create_build(db, scope, &definition(None), 5).await;
    assert_eq!(
        drive_to_terminal(db, driver, build_id, claim_sequence).await,
        CommittedOperationStep::Completed
    );
    let record = read_index(db, scope, &definition(None)).await;
    let IndexStateV2::Active {
        physical:
            PhysicalGeneration::Vector {
                layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
                ..
            },
        ..
    } = record.state()
    else {
        panic!("the recreated index is active and unpartitioned");
    };
    (generation, *physical_index_id)
}

fn metadata_key(scope: DataScope, physical_index_id: VectorPhysicalIndexId) -> Bytes {
    DataKey::Data {
        scope,
        kind: DataKeyKind::Vector(
            crate::encoding::v2::keys::indexes::vector::VectorKey::IndexMetadata(
                crate::encoding::v2::keys::indexes::vector::VectorIndexMetadataKey::new(
                    physical_index_id.get(),
                ),
            ),
        ),
    }
    .to_bytes()
}

/// Proves a refresh arms a completed drop's entry with every owner its rows
/// have, behind a gate that no view of the `Dropping` record reaches, and
/// that a later refresh leaves an armed entry as it is.
pub(super) async fn a_refresh_arms_a_completed_drop_behind_its_gate() {
    let db = test_db("vector-driver-dropped-refresh").await;
    dropped::admit_fixture(&db).await;
    let scope = DataScope::LegacyUnscoped;
    let driver = driver();
    let (drop_id, physical_index_id, mut claim_sequence) = unpartitioned_drop(&db, &driver).await;
    let entry = dropping_generation(&db).await;
    let registry = dropped::Registry::default();
    let limits = SearchIndexBackfillLimits::default().batch();
    for _ in 0..2 {
        assert_eq!(
            drive_one(&db, &driver, drop_id, &mut claim_sequence, limits).await,
            CommittedOperationStep::Progressed
        );
    }
    // The last view that reads the generation as `Dropping`.
    let dropping = db.snapshot().await.unwrap();
    registry
        .refresh(&db)
        .await
        .expect("a store without entries arms nothing");
    assert_eq!(registry.armed(), vec![]);
    assert_eq!(
        drive_one(&db, &driver, drop_id, &mut claim_sequence, limits).await,
        CommittedOperationStep::Completed
    );

    registry.refresh(&db).await.expect("the drop's entry arms");
    let armed = registry.armed();
    let [(armed_entry, armed)] = &armed[..] else {
        panic!("one entry is armed");
    };
    assert_eq!(*armed_entry, entry);
    assert!(
        armed.gate > dropping.seq(),
        "a view of the Dropping record is older than the gate"
    );
    assert_eq!(armed.owners.len(), 2);
    assert_eq!(
        armed
            .owners
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>(),
        std::collections::HashSet::from([
            dropped::ownership::OwnerKey::Generation {
                scope,
                index_id: entry.index_id.get(),
                generation: entry.generation.get(),
            },
            dropped::ownership::OwnerKey::PhysicalIndex {
                scope,
                physical_index_id,
            },
        ])
    );

    put_source(&db, scope, 99, &properties([9.0, 2.0, 3.0], None)).await;
    registry
        .refresh(&db)
        .await
        .expect("a second refresh passes");
    assert_eq!(registry.armed(), vec![(entry, armed.clone())]);
    drop(dropping);
    db.close().await.expect("vector test database closes");
}

/// One way a stored entry contradicts the catalog or the id watermarks.
#[derive(Debug, Clone, Copy)]
enum Contradiction {
    IndexAboveWatermark,
    GenerationNotReached,
    AnotherFamily,
    AnotherScope,
    PhysicalIndexAboveWatermark,
    MetadataRowPresent,
    MappedByAnotherGeneration,
    MappedByItsOwnAndAnotherGeneration,
    MappingOfAnotherGeneration,
    PartitionsShareAPhysicalIndex,
    GenerationStillLive,
    PhysicalIndexStillLive,
}

/// Writes one partition mapping of `tenant` under `keyed`'s generation
/// prefix whose value names `valued`'s generation and `physical_index_id`.
async fn put_mapping(
    db: &Db,
    scope: DataScope,
    keyed: dropped::DroppedGeneration,
    valued: dropped::DroppedGeneration,
    tenant: &'static [u8],
    physical_index_id: VectorPhysicalIndexId,
) {
    let partition = VectorTenantPartition::try_new(Bytes::from_static(tenant)).unwrap();
    db.put(
        scoped_index_key(
            scope,
            ScopedKey::VectorPartitionMapping(
                crate::encoding::v2::keys::VectorPartitionMappingKey {
                    index_id: keyed.index_id,
                    generation: keyed.generation,
                    partition: partition.fingerprint(),
                },
            ),
        ),
        encode_partition_mapping(&crate::index_lifecycle::work::VectorPartitionMappingValue {
            index_id: valued.index_id,
            generation: valued.generation,
            partition,
            physical_index_id,
        }),
    )
    .await
    .expect("fixture mapping is written");
}

/// Proves an entry that names rows a live generation owns, or ids that were
/// never allocated, is quarantined: it is never armed, the refresh succeeds,
/// and every other entry is armed as if the damaged one were not there.
///
/// Each case starts from a completed drop, whose own entry passes, and
/// damages the store in one way: by a second entry, or by making the drop's
/// own entry wrong. A registry that reads the damaged store for the first
/// time, as an open does, quarantines the wrong entry with the reason and
/// keeps doing so. A registry that armed the drop's entry before the damage
/// quarantines a second entry too; it does not look again at an entry it
/// has armed.
pub(super) async fn a_refresh_quarantines_an_entry_the_catalog_contradicts() {
    for (name, contradiction, reason, adds_an_entry) in [
        (
            "index-above-watermark",
            Contradiction::IndexAboveWatermark,
            "names an index id above the watermark",
            true,
        ),
        (
            "generation-not-reached",
            Contradiction::GenerationNotReached,
            "names a generation its index has not reached",
            true,
        ),
        (
            "another-family",
            Contradiction::AnotherFamily,
            "names an index of another family",
            false,
        ),
        (
            "another-scope",
            Contradiction::AnotherScope,
            "names an index its scope has no record of",
            false,
        ),
        (
            "physical-above-watermark",
            Contradiction::PhysicalIndexAboveWatermark,
            "names a vector physical index id above the watermark",
            false,
        ),
        (
            "metadata-row-present",
            Contradiction::MetadataRowPresent,
            "names a vector physical index that has a metadata row",
            false,
        ),
        (
            "mapped-by-another-generation",
            Contradiction::MappedByAnotherGeneration,
            "names a vector physical index another generation's partition maps to",
            false,
        ),
        (
            "mapped-by-its-own-and-another-generation",
            Contradiction::MappedByItsOwnAndAnotherGeneration,
            "names a vector physical index another generation's partition maps to",
            false,
        ),
        (
            "mapping-of-another-generation",
            Contradiction::MappingOfAnotherGeneration,
            "has a partition mapping of another generation",
            false,
        ),
        (
            "partitions-share-a-physical-index",
            Contradiction::PartitionsShareAPhysicalIndex,
            "has two partitions mapped to one vector physical index",
            false,
        ),
        (
            "generation-still-live",
            Contradiction::GenerationStillLive,
            "names a generation its index record still owns",
            true,
        ),
        (
            "physical-still-live",
            Contradiction::PhysicalIndexStillLive,
            "names a vector physical index an index record still owns",
            false,
        ),
    ] {
        let db = test_db(&format!("vector-driver-dropped-contradiction-{name}")).await;
        let scope = DataScope::LegacyUnscoped;
        let driver = driver();
        let (entry, value, physical_index_id, mut claim_sequence) =
            completed_drop(&db, &driver).await;
        let running = dropped::Registry::default();
        running.refresh(&db).await.expect("the drop's entry passes");
        assert_eq!(running.armed().len(), 1, "{name}");

        match contradiction {
            Contradiction::IndexAboveWatermark => {
                let unallocated =
                    crate::index_lifecycle::repository::load_logical_index_watermark(&db)
                        .await
                        .unwrap()
                        .next_id;
                put_entry(
                    &db,
                    dropped::DroppedGeneration {
                        index_id: unallocated,
                        ..entry
                    },
                    value,
                )
                .await;
            }
            Contradiction::GenerationNotReached => {
                put_entry(
                    &db,
                    dropped::DroppedGeneration {
                        generation: entry.generation.checked_next().unwrap(),
                        ..entry
                    },
                    value,
                )
                .await;
            }
            Contradiction::AnotherFamily => {
                put_entry(
                    &db,
                    entry,
                    DroppedGenerationValue {
                        rows: DroppedGenerationRows::Secondary,
                        ..value
                    },
                )
                .await;
            }
            Contradiction::AnotherScope => {
                put_entry(
                    &db,
                    entry,
                    DroppedGenerationValue {
                        scope: DataScope::Tenant(
                            crate::encoding::v2::keys::scope::TenantId::from_u128(9),
                        ),
                        ..value
                    },
                )
                .await;
            }
            Contradiction::PhysicalIndexAboveWatermark => {
                put_entry(
                    &db,
                    entry,
                    DroppedGenerationValue {
                        rows: DroppedGenerationRows::UnpartitionedVector {
                            physical_index_id: peek_vector_physical_id(&db).await.unwrap(),
                        },
                        ..value
                    },
                )
                .await;
            }
            Contradiction::MetadataRowPresent => {
                db.put(metadata_key(scope, physical_index_id), b"metadata")
                    .await
                    .unwrap();
            }
            Contradiction::MappedByAnotherGeneration => {
                let later = dropped::DroppedGeneration {
                    generation: entry.generation.checked_next().unwrap(),
                    ..entry
                };
                put_mapping(&db, scope, later, later, b"tenant", physical_index_id).await;
            }
            // The entry's own mapping sorts first, and does not vouch for
            // the one after it.
            Contradiction::MappedByItsOwnAndAnotherGeneration => {
                let later = dropped::DroppedGeneration {
                    generation: entry.generation.checked_next().unwrap(),
                    ..entry
                };
                put_mapping(&db, scope, entry, entry, b"own", physical_index_id).await;
                put_mapping(&db, scope, later, later, b"tenant", physical_index_id).await;
            }
            // The two below list the entry's physical ids through mappings,
            // as a partitioned generation's entry does.
            Contradiction::MappingOfAnotherGeneration => {
                let later = dropped::DroppedGeneration {
                    generation: entry.generation.checked_next().unwrap(),
                    ..entry
                };
                put_mapping(&db, scope, entry, later, b"tenant", physical_index_id).await;
                put_entry(
                    &db,
                    entry,
                    DroppedGenerationValue {
                        rows: DroppedGenerationRows::PartitionedVector,
                        ..value
                    },
                )
                .await;
            }
            Contradiction::PartitionsShareAPhysicalIndex => {
                for tenant in [&b"one"[..], &b"two"[..]] {
                    put_mapping(&db, scope, entry, entry, tenant, physical_index_id).await;
                }
                put_entry(
                    &db,
                    entry,
                    DroppedGenerationValue {
                        rows: DroppedGenerationRows::PartitionedVector,
                        ..value
                    },
                )
                .await;
            }
            Contradiction::GenerationStillLive => {
                let (generation, _) = recreated(&db, &driver, &mut claim_sequence).await;
                dropped::Registry::default()
                    .refresh(&db)
                    .await
                    .expect("the drop's entry passes beside the recreated generation");
                // The layout names no physical id, so only the generation
                // contradicts the record.
                put_entry(
                    &db,
                    dropped::DroppedGeneration {
                        generation,
                        ..entry
                    },
                    DroppedGenerationValue {
                        rows: DroppedGenerationRows::PartitionedVector,
                        ..value
                    },
                )
                .await;
            }
            Contradiction::PhysicalIndexStillLive => {
                let (_, live) = recreated(&db, &driver, &mut claim_sequence).await;
                // Without its metadata row only the record says the id is live.
                db.delete(metadata_key(scope, live)).await.unwrap();
                put_entry(
                    &db,
                    entry,
                    DroppedGenerationValue {
                        rows: DroppedGenerationRows::UnpartitionedVector {
                            physical_index_id: live,
                        },
                        ..value
                    },
                )
                .await;
            }
        }

        let stored = dropped::load_entries(&db).await.unwrap();
        assert_eq!(stored.len(), 1 + usize::from(adds_an_entry), "{name}");
        let opened = dropped::Registry::default();
        // Twice: a quarantined entry is checked again and fails again.
        for _ in 0..2 {
            opened.refresh(&db).await.expect(name);
            let armed = opened
                .armed()
                .into_iter()
                .map(|(entry, _)| entry)
                .collect::<Vec<_>>();
            assert_eq!(
                armed,
                if adds_an_entry { vec![entry] } else { vec![] },
                "{name}: only the drop's own entry, while it is right, is armed"
            );
            let storage = opened.storage();
            let [quarantined] = &storage.quarantined[..] else {
                panic!("{name}: one entry is wrong: {storage:?}");
            };
            assert_eq!(quarantined.contradiction.to_string(), reason, "{name}");
            assert!(
                !armed
                    .iter()
                    .any(|armed| armed.index_id.get() == quarantined.index_id
                        && armed.generation.get() == quarantined.generation),
                "{name}: a quarantined entry is not armed"
            );
            assert_eq!(storage.entries(), stored.len(), "{name}");
        }

        running.refresh(&db).await.expect(name);
        assert_eq!(
            running
                .armed()
                .into_iter()
                .map(|(entry, _)| entry)
                .collect::<Vec<_>>(),
            vec![entry],
            "{name}: an armed entry stays armed"
        );
        assert_eq!(
            running.storage().quarantined.len(),
            usize::from(adds_an_entry),
            "{name}: a registry does not look again at an entry it has armed"
        );
        db.close().await.expect("vector test database closes");
    }
}

/// Proves an entry beside a record that is still `Dropping` is quarantined:
/// the entry and the `Dropped` record are written together, so one without
/// the other names rows a drop is still deleting.
pub(super) async fn a_refresh_quarantines_an_entry_whose_drop_has_not_completed() {
    let db = test_db("vector-driver-dropped-contradiction-dropping").await;
    dropped::admit_fixture(&db).await;
    let scope = DataScope::LegacyUnscoped;
    let driver = driver();
    let (drop_id, physical_index_id, mut claim_sequence) = unpartitioned_drop(&db, &driver).await;
    let physical_index_id = VectorPhysicalIndexId::new(physical_index_id).unwrap();
    let entry = dropping_generation(&db).await;
    let limits = SearchIndexBackfillLimits::default().batch();
    for _ in 0..2 {
        assert_eq!(
            drive_one(&db, &driver, drop_id, &mut claim_sequence, limits).await,
            CommittedOperationStep::Progressed
        );
    }
    assert_eq!(
        db.get(metadata_key(scope, physical_index_id))
            .await
            .unwrap(),
        None,
        "the drop has deleted the metadata row"
    );
    put_entry(
        &db,
        entry,
        DroppedGenerationValue {
            scope,
            rows: DroppedGenerationRows::UnpartitionedVector { physical_index_id },
        },
    )
    .await;
    let registry = dropped::Registry::default();
    registry.refresh(&db).await.expect("the refresh reads");
    assert_eq!(registry.armed(), vec![]);
    let storage = registry.storage();
    let [quarantined] = &storage.quarantined[..] else {
        panic!("the entry is wrong: {storage:?}");
    };
    assert_eq!(
        (
            quarantined.index_id,
            quarantined.generation,
            quarantined.scope,
            quarantined.rows,
            quarantined.contradiction,
        ),
        (
            entry.index_id.get(),
            entry.generation.get(),
            scope,
            crate::DroppedEntryRows::UnpartitionedVector {
                physical_index_id: physical_index_id.get(),
            },
            crate::DroppedEntryContradiction::GenerationStillOwned,
        )
    );
    assert_eq!(storage.waiting_since, None);
    db.close().await.expect("vector test database closes");
}

/// Proves a refresh that fails before its flush arms nothing, and that its
/// retry arms the entry.
pub(super) async fn a_refresh_that_fails_before_its_flush_arms_nothing() {
    let db = test_db("vector-driver-dropped-refresh-failpoint").await;
    let driver = driver();
    let (entry, _, _, _) = completed_drop(&db, &driver).await;
    let registry = dropped::Registry::default();
    failpoints::inject_once(IndexOutboxFailpoint::DroppedEntryArmBefore).unwrap();
    assert!(registry.refresh(&db).await.is_err());
    assert!(failpoints::was_triggered());
    assert_eq!(registry.armed(), vec![]);
    registry.refresh(&db).await.expect("the retry arms");
    assert_eq!(
        registry
            .armed()
            .into_iter()
            .map(|(entry, _)| entry)
            .collect::<Vec<_>>(),
        vec![entry]
    );
    db.close().await.expect("vector test database closes");
}

/// Proves no logical id an entry names is allocated, and that no entry fails
/// an allocation: a new index takes the first index id, and a recreated
/// index the first generation, that no stored entry names, however many
/// entries it skips. An entry that names a skipped id can then claim no row:
/// none is ever written under it.
///
/// A vector physical id is not skipped when it is allocated. An entry that
/// names an unallocated one is quarantined instead, and stays quarantined
/// once a build has taken the id: the build's record owns it.
pub(super) async fn an_id_an_entry_names_is_never_allocated_or_never_discarded() {
    let scope = DataScope::LegacyUnscoped;
    let contradictions = |registry: &dropped::Registry| {
        registry
            .storage()
            .quarantined
            .iter()
            .map(|entry| (entry.index_id, entry.generation, entry.contradiction))
            .collect::<Vec<_>>()
    };

    let db = test_db("vector-driver-dropped-unallocated-index").await;
    dropped::admit_fixture(&db).await;
    let next = crate::index_lifecycle::repository::load_logical_index_watermark(&db)
        .await
        .unwrap()
        .next_id
        .get();
    // Entries name two generations of the next id and of the one after,
    // and the id three on; the id between is free.
    let text = DroppedGenerationValue {
        scope: DataScope::Tenant(crate::encoding::v2::keys::scope::TenantId::from_u128(9)),
        rows: DroppedGenerationRows::Text,
    };
    for (index_id, generation) in [
        (next, 1),
        (next, 3),
        (next + 1, 1),
        (next + 1, 4),
        (next + 3, 2),
    ] {
        put_entry(
            &db,
            dropped::DroppedGeneration {
                index_id: IndexId::new(index_id).unwrap(),
                generation: IndexGenerationId::new(generation).unwrap(),
            },
            text,
        )
        .await;
    }
    let registry = dropped::Registry::default();
    registry.refresh(&db).await.expect("the refresh reads");
    assert_eq!(registry.armed(), vec![]);
    let above = crate::DroppedEntryContradiction::IndexAboveWatermark;
    assert_eq!(
        contradictions(&registry),
        vec![
            (next, 1, above),
            (next, 3, above),
            (next + 1, 1, above),
            (next + 1, 4, above),
            (next + 3, 2, above)
        ]
    );
    create_unpartitioned(&db)
        .await
        .expect("an index is created past the ids entries name");
    let created = read_index(&db, scope, &definition(None)).await;
    assert_eq!(created.index_id().get(), next + 2);
    assert_eq!(
        crate::index_lifecycle::repository::load_logical_index_watermark(&db)
            .await
            .unwrap()
            .next_id
            .get(),
        next + 3
    );
    // The skipped ids were never given to an index, so their entries stay
    // quarantined, and no entry names the index that was created.
    registry.refresh(&db).await.expect("the refresh reads");
    assert_eq!(registry.armed(), vec![]);
    let unrecorded = crate::DroppedEntryContradiction::IndexWithoutRecord;
    assert_eq!(
        contradictions(&registry),
        vec![
            (next, 1, unrecorded),
            (next, 3, unrecorded),
            (next + 1, 1, unrecorded),
            (next + 1, 4, unrecorded),
            (next + 3, 2, above)
        ]
    );
    db.close().await.expect("vector test database closes");

    let db = test_db("vector-driver-dropped-unallocated-generation").await;
    let driver = driver();
    let (entry, value, _, _) = completed_drop(&db, &driver).await;
    let last = entry.generation.get();
    // Entries name the next two generations and the one four on.
    for generation in [last + 1, last + 2, last + 4] {
        put_entry(
            &db,
            dropped::DroppedGeneration {
                generation: IndexGenerationId::new(generation).unwrap(),
                ..entry
            },
            value,
        )
        .await;
    }
    let registry = dropped::Registry::default();
    registry.refresh(&db).await.expect("the refresh reads");
    let unreached = crate::DroppedEntryContradiction::GenerationNotReached;
    let index_id = entry.index_id.get();
    assert_eq!(
        contradictions(&registry),
        vec![
            (index_id, last + 1, unreached),
            (index_id, last + 2, unreached),
            (index_id, last + 4, unreached)
        ]
    );
    create_unpartitioned(&db)
        .await
        .expect("the index is recreated past the generations entries name");
    let recreated = read_index(&db, scope, &definition(None)).await;
    assert_eq!(recreated.index_id(), entry.index_id);
    let IndexStateV2::Building { physical, .. } = recreated.state() else {
        panic!("the index is building again");
    };
    assert_eq!(physical.generation().get(), last + 3);
    // The generations the build skipped were never built and never will be,
    // so their entries now name rows nothing owns and are armed; the one
    // above the build's generation is one the live index has yet to pass.
    registry.refresh(&db).await.expect("the refresh reads");
    assert_eq!(
        contradictions(&registry),
        vec![(
            index_id,
            last + 4,
            crate::DroppedEntryContradiction::GenerationStillOwned
        )]
    );
    let mut armed = registry
        .armed()
        .into_iter()
        .map(|(entry, _)| entry.generation.get())
        .collect::<Vec<_>>();
    armed.sort_unstable();
    assert_eq!(armed, vec![last, last + 1, last + 2]);
    db.close().await.expect("vector test database closes");

    let db = test_db("vector-driver-dropped-unallocated-physical").await;
    let (entry, value, _, _) = completed_drop(&db, &driver).await;
    let unallocated = peek_vector_physical_id(&db).await.unwrap();
    put_entry(
        &db,
        entry,
        DroppedGenerationValue {
            rows: DroppedGenerationRows::UnpartitionedVector {
                physical_index_id: unallocated,
            },
            ..value
        },
    )
    .await;
    let quarantined = |registry: &dropped::Registry| {
        let storage = registry.storage();
        assert_eq!(registry.armed(), vec![]);
        let [quarantined] = &storage.quarantined[..] else {
            panic!("the entry is wrong: {storage:?}");
        };
        quarantined.contradiction
    };
    let registry = dropped::Registry::default();
    registry.refresh(&db).await.expect("the refresh reads");
    assert_eq!(
        quarantined(&registry),
        crate::DroppedEntryContradiction::PhysicalIndexAboveWatermark
    );
    create_unpartitioned(&db)
        .await
        .expect("the build takes the id the entry names");
    let building = read_index(&db, scope, &definition(None)).await;
    let IndexStateV2::Building {
        physical:
            PhysicalGeneration::Vector {
                layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
                ..
            },
        ..
    } = building.state()
    else {
        panic!("the index is building again");
    };
    assert_eq!(*physical_index_id, unallocated);
    // The registry that saw the id unallocated, and one that opens now.
    for registry in [registry, dropped::Registry::default()] {
        registry.refresh(&db).await.expect("the refresh reads");
        assert_eq!(
            quarantined(&registry),
            crate::DroppedEntryContradiction::PhysicalIndexStillOwned
        );
    }
    db.close().await.expect("vector test database closes");
}

/// Enqueues a build of the unpartitioned fixture definition.
async fn create_unpartitioned(db: &Db) -> Result<IndexDdlReceipt> {
    let scope = DataScope::LegacyUnscoped;
    create_index_operation(
        db,
        scope,
        definition(None),
        helix_planner::ir::IndexCreateMode::ErrorIfExists,
        InitialBuildProgress::vector(source_cursor(scope, 5)),
    )
    .await
}

/// Builds the fixture index over eight entities, in two tenants when
/// `tenant_property` partitions it, up to activation and aborts it there.
/// Returns the build, now aborting, the generation it removes, the physical
/// ids of its namespaces in mapping order and the next claim sequence.
async fn aborting_build(
    db: &Db,
    driver: &VectorIndexDriver,
    tenant_property: Option<&str>,
) -> (IndexOperationId, dropped::DroppedGeneration, Vec<u64>, u64) {
    let scope = DataScope::LegacyUnscoped;
    let definition = definition(tenant_property);
    for entity_id in 0..8_u8 {
        put_source(
            db,
            scope,
            u64::from(entity_id),
            &properties(
                [f32::from(entity_id), 5.0, 6.0],
                tenant_property.map(|_| 10 * (1 + i64::from(entity_id % 2))),
            ),
        )
        .await;
    }
    let (build_id, index_id, generation) = create_build(db, scope, &definition, 7).await;
    let mut claim_sequence = 1;
    for _ in 0..64 {
        if matches!(
            read_operation(db, scope, build_id).await.progress(),
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                VectorBuildStage::Activate(_)
            ))
        ) {
            break;
        }
        assert_eq!(
            drive_one(
                db,
                driver,
                build_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Progressed
        );
    }
    let building = read_index(db, scope, &definition).await;
    let IndexStateV2::Building {
        physical: PhysicalGeneration::Vector { layout, .. },
        ..
    } = building.state()
    else {
        panic!("the build has not activated");
    };
    let physical_ids = match layout {
        VectorPhysicalLayout::Unpartitioned { physical_index_id } => {
            vec![physical_index_id.get()]
        }
        VectorPhysicalLayout::Partitioned => mapping_values(db, scope, index_id, generation)
            .await
            .iter()
            .map(|mapping| mapping.physical_index_id.get())
            .collect(),
    };
    let receipt = drop_index_operation(db, scope, &definition)
        .await
        .expect("a building vector index converts to abort cleanup");
    assert!(matches!(
        receipt,
        IndexDdlReceipt::ExistingOperation { operation_id } if operation_id == build_id
    ));
    (
        build_id,
        dropped::DroppedGeneration {
            index_id,
            generation,
        },
        physical_ids,
        claim_sequence,
    )
}

/// Proves an aborted build on an admitted store is removed as a drop is: it
/// deletes only the rows no compaction filter may match, keeps its partition
/// mappings and leaves its entry in the step that writes `Dropped`.
///
/// Both layouts take one step to retire the cache and one for each namespace.
/// Until the last step commits the record is `Aborting` and no entry exists.
/// A fresh registry, as an open builds, arms the entry with the generation
/// and every namespace as owners, and still does once the index is built
/// again as a later generation, whose build touches none of the rows the
/// entry gives up.
pub(super) async fn an_admitted_abort_deletes_only_unfiltered_rows_and_leaves_its_entry() {
    for (name, tenant_property, namespaces) in [
        ("vector-driver-aborted-entry", None, 1),
        (
            "vector-driver-aborted-partitioned-entry",
            Some("account_id"),
            2,
        ),
    ] {
        let db = test_db(name).await;
        dropped::admit_fixture(&db).await;
        let scope = DataScope::LegacyUnscoped;
        let driver = driver();
        let definition = definition(tenant_property);
        let (build_id, entry, physical_ids, mut claim_sequence) =
            aborting_build(&db, &driver, tenant_property).await;
        assert_eq!(physical_ids.len(), namespaces, "{name}");
        put_build_delta(&db, entry).await;
        let mut filtered = Vec::new();
        for &physical_index_id in &physical_ids {
            let core = core_keys(&db, scope, physical_index_id).await;
            assert!(!core.is_empty() && core.len() <= 3, "{name}");
            filtered.push(
                physical_keys(&db, scope, physical_index_id)
                    .await
                    .into_iter()
                    .filter(|key| !core.contains(key))
                    .collect::<Vec<_>>(),
            );
            assert!(
                filtered.last().is_some_and(|rows| rows.len() > 3),
                "{name}: row-by-row cleanup would take several batches"
            );
        }
        let generation_rows = generation_keys(&db, entry).await;
        assert!(!generation_rows.is_empty(), "{name}");
        let mappings = mapped_physical_ids(&db, entry).await;
        // Too few operations a step for a namespace, enough for its core rows.
        let three_rows = batch_limits(1024, 1024 * 1024, 3, 16 * 1024 * 1024);

        for step in 0..=namespaces {
            assert_eq!(
                drive_one(&db, &driver, build_id, &mut claim_sequence, three_rows).await,
                CommittedOperationStep::Progressed,
                "{name}: step {step}"
            );
            assert!(matches!(
                read_index(&db, scope, &definition).await.state(),
                IndexStateV2::Aborting { .. }
            ));
            assert_eq!(dropped::load_entries(&db).await.unwrap(), vec![], "{name}");
        }
        assert!(matches!(
            read_operation(&db, scope, build_id).await.progress(),
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(
                VectorCleanupProgress::Finalize(_)
            ))
        ));

        assert_eq!(
            drive_one(&db, &driver, build_id, &mut claim_sequence, three_rows).await,
            CommittedOperationStep::Completed,
            "{name}"
        );
        assert!(matches!(
            read_index(&db, scope, &definition).await.state(),
            IndexStateV2::Dropped { .. }
        ));
        assert_eq!(
            dropped::load_entries(&db).await.unwrap(),
            vec![(
                entry,
                DroppedGenerationValue {
                    scope,
                    rows: match tenant_property {
                        None => DroppedGenerationRows::UnpartitionedVector {
                            physical_index_id: VectorPhysicalIndexId::new(physical_ids[0]).unwrap(),
                        },
                        Some(_) => DroppedGenerationRows::PartitionedVector,
                    },
                }
            )],
            "{name}"
        );
        let owners = std::iter::once(dropped::ownership::OwnerKey::Generation {
            scope,
            index_id: entry.index_id.get(),
            generation: entry.generation.get(),
        })
        .chain(physical_ids.iter().map(|&physical_index_id| {
            dropped::ownership::OwnerKey::PhysicalIndex {
                scope,
                physical_index_id,
            }
        }))
        .collect::<std::collections::HashSet<_>>();

        for stage in ["aborted", "built again"] {
            for (physical_index_id, filtered) in physical_ids.iter().zip(&filtered) {
                assert_eq!(
                    physical_keys(&db, scope, *physical_index_id).await,
                    *filtered,
                    "{name}: {stage}"
                );
            }
            assert_eq!(
                mapped_physical_ids(&db, entry).await,
                mappings,
                "{name}: {stage}"
            );
            assert_eq!(
                generation_keys(&db, entry).await,
                generation_rows,
                "{name}: {stage}"
            );
            let registry = dropped::Registry::default();
            registry.refresh(&db).await.expect("the abort's entry arms");
            let armed = registry.armed();
            let [(armed_entry, armed)] = &armed[..] else {
                panic!("{name}: one entry is armed once {stage}");
            };
            assert_eq!(*armed_entry, entry);
            assert_eq!(armed.owners.len(), 1 + namespaces);
            assert_eq!(
                armed
                    .owners
                    .iter()
                    .copied()
                    .collect::<std::collections::HashSet<_>>(),
                owners,
                "{name}: {stage}"
            );
            if stage == "aborted" {
                let (rebuild_id, _, generation) = create_build(&db, scope, &definition, 7).await;
                assert!(generation > entry.generation);
                assert_eq!(
                    drive_to_terminal(&db, &driver, rebuild_id, &mut claim_sequence).await,
                    CommittedOperationStep::Completed,
                    "{name}"
                );
                assert!(matches!(
                    read_index(&db, scope, &definition).await.state(),
                    IndexStateV2::Active { .. }
                ));
            }
        }
        db.close().await.expect("vector test database closes");
    }
}

/// Proves a store admitted while an abort is half-way through its rows hands
/// what is left to the abort's entry, as it does a drop's.
pub(super) async fn an_abort_admitted_half_way_is_handed_to_its_entry() {
    let db = test_db("vector-driver-aborted-entry-admitted-half-way").await;
    let scope = DataScope::LegacyUnscoped;
    let driver = driver();
    let (build_id, entry, physical_ids, mut claim_sequence) =
        aborting_build(&db, &driver, None).await;
    let [physical_index_id] = physical_ids[..] else {
        panic!("an unpartitioned build has one namespace");
    };
    put_build_delta(&db, entry).await;
    let rows = physical_keys(&db, scope, physical_index_id).await.len();
    let one_row = batch_limits(1024, 1024 * 1024, 1, 16 * 1024 * 1024);
    // Retiring the cache, then two rows.
    for _ in 0..3 {
        assert_eq!(
            drive_one(&db, &driver, build_id, &mut claim_sequence, one_row).await,
            CommittedOperationStep::Progressed
        );
    }
    let left = physical_keys(&db, scope, physical_index_id).await;
    assert_eq!(left.len(), rows - 2);
    assert_eq!(
        core_keys(&db, scope, physical_index_id).await,
        Vec::<Bytes>::new()
    );
    let deltas = generation_keys(&db, entry).await;
    assert!(!deltas.is_empty());

    dropped::admit_fixture(&db).await;
    let mut steps = 0;
    while drive_one(&db, &driver, build_id, &mut claim_sequence, one_row).await
        == CommittedOperationStep::Progressed
    {
        steps += 1;
        assert!(steps < 8, "the rows left took steps of their own");
        assert_eq!(dropped::load_entries(&db).await.unwrap(), vec![]);
    }
    // Neither a physical row nor a generation-keyed one was deleted since.
    assert_eq!(physical_keys(&db, scope, physical_index_id).await, left);
    assert_eq!(generation_keys(&db, entry).await, deltas);
    assert_eq!(
        dropped::load_entries(&db)
            .await
            .unwrap()
            .into_iter()
            .map(|(entry, _)| entry)
            .collect::<Vec<_>>(),
        vec![entry]
    );
    db.close().await.expect("vector test database closes");
}

/// Proves an abort's entry and its `Dropped` record commit together or not
/// at all: a step that fails where the entry is staged blocks the build,
/// leaving the record `Aborting` with no entry, and its retry writes both.
pub(super) async fn a_failed_final_step_of_an_abort_leaves_neither_the_entry_nor_the_dropped_record(
) {
    let db = test_db("vector-driver-aborted-entry-failpoint").await;
    dropped::admit_fixture(&db).await;
    let scope = DataScope::LegacyUnscoped;
    let driver = driver();
    let (build_id, entry, _, mut claim_sequence) = aborting_build(&db, &driver, None).await;
    let limits = SearchIndexBackfillLimits::default().batch();
    for _ in 0..2 {
        assert_eq!(
            drive_one(&db, &driver, build_id, &mut claim_sequence, limits).await,
            CommittedOperationStep::Progressed
        );
    }

    failpoints::inject_once(IndexOutboxFailpoint::DroppedEntryStagingBefore).unwrap();
    assert_eq!(
        drive_one(&db, &driver, build_id, &mut claim_sequence, limits).await,
        CommittedOperationStep::Blocked
    );
    assert!(failpoints::was_triggered());
    assert!(matches!(
        read_index(&db, scope, &definition(None)).await.state(),
        IndexStateV2::Aborting { .. }
    ));
    assert!(matches!(
        read_operation(&db, scope, build_id).await.progress(),
        IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(
            VectorCleanupProgress::Finalize(_)
        ))
    ));
    assert_eq!(dropped::load_entries(&db).await.unwrap(), vec![]);

    crate::index_lifecycle::outbox::retry_operation(&db, scope, build_id)
        .await
        .expect("the blocked abort is retried");
    assert_eq!(
        drive_one(&db, &driver, build_id, &mut claim_sequence, limits).await,
        CommittedOperationStep::Completed
    );
    assert!(matches!(
        read_index(&db, scope, &definition(None)).await.state(),
        IndexStateV2::Dropped { .. }
    ));
    assert_eq!(
        dropped::load_entries(&db)
            .await
            .unwrap()
            .into_iter()
            .map(|(entry, _)| entry)
            .collect::<Vec<_>>(),
        vec![entry]
    );
    db.close().await.expect("vector test database closes");
}
