use std::sync::Arc;

use slatedb::object_store::memory::InMemory;
use slatedb::{Db, IsolationLevel};

use super::*;
use crate::index_lifecycle::text::test_support;
use crate::index_lifecycle::{
    IndexElementKind, IndexEntityId, TextLogicalVersion, TextManifestRevision,
};

fn definition() -> ValidatedTextIndexDefinition {
    ValidatedTextIndexDefinition::try_from_runtime(
        &crate::config::TextIndexDefinition::new_node("Document", "body").unwrap(),
    )
    .unwrap()
}

fn root_key(
    scope: DataScope,
    operation: &IndexOperationRecord,
    partition: &TextPartition,
) -> (index_keys::TextManifestRootKey, Bytes) {
    let typed = index_keys::TextManifestRootKey {
        index_id: operation.index_id(),
        generation: operation.generation(),
        partition: partition.fingerprint(),
    };
    (
        typed,
        scoped_key(scope, index_keys::ScopedKey::TextManifestRoot(typed)),
    )
}

fn root_value(
    operation: &IndexOperationRecord,
    partition: TextPartition,
    page_count: u32,
    split_count: u64,
) -> Bytes {
    let revision = TextManifestRevision::new(u64::from(page_count).saturating_add(1)).unwrap();
    index_values::encode_manifest_root(
        &work::TextManifestRootValue::try_new(
            operation.index_id(),
            operation.generation(),
            partition,
            revision,
            page_count,
            split_count,
        )
        .unwrap(),
    )
}

fn root_progress(counters: OperationCounters) -> TextManifestValidationProgress {
    TextManifestValidationProgress::Roots(PrefixScanProgress {
        cursor: None,
        counters,
    })
}

fn entity_progress(counters: OperationCounters) -> TextManifestValidationProgress {
    TextManifestValidationProgress::EntityStates(PrefixScanProgress {
        cursor: None,
        counters,
    })
}

fn build_delta_row(
    scope: DataScope,
    operation: &IndexOperationRecord,
    entity: index_keys::IndexEntity,
) -> (Bytes, Bytes) {
    let key = scoped_key(
        scope,
        index_keys::ScopedKey::BuildDelta(index_keys::IndexEntityStateKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            entity,
        }),
    );
    let value = index_values::encode_build_delta(&work::CoalescedBuildDeltaValue {
        index_id: operation.index_id(),
        generation: operation.generation(),
        entity_kind: entity.kind,
        entity_id: entity.id,
    });
    (key, value)
}

fn assert_catch_up_with_counters(result: IndexOperationStepResult, expected: OperationCounters) {
    let IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
        TextBuildProgress::Constructing(TextBuildStage::CatchUp(progress)),
    )) = result
    else {
        panic!("pending build delta must preempt manifest validation")
    };
    assert!(progress.cursor.is_none());
    assert_eq!(progress.counters, expected);
}

#[tokio::test]
async fn pending_delta_preempts_every_validation_lane_and_preserves_counters() {
    let db = Db::open(
        "text-validation-pending-delta-lanes",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let operation = test_support::operation();
    let definition = definition();
    let scope = DataScope::LegacyUnscoped;
    let counters = OperationCounters {
        entities: 7,
        input_bytes: 11,
        output_operations: 13,
        output_bytes: 17,
    };
    let entity = index_keys::IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(19),
    };
    let (delta_key, delta_value) = build_delta_row(scope, &operation, entity);
    transaction.put(delta_key, delta_value).unwrap();

    for progress in [
        TextManifestValidationProgress::Pages(TextManifestPageValidationProgress::initial(
            counters,
        )),
        root_progress(counters),
        entity_progress(counters),
    ] {
        let ValidationSelection::Database(prepared) = select(
            &transaction,
            scope,
            &operation,
            &definition,
            &progress,
            test_support::batch_limits(u64::MAX, u64::MAX, u64::MAX),
        )
        .await
        .unwrap() else {
            panic!("pending build delta selects a database-only catch-up transition")
        };
        assert_catch_up_with_counters(prepared.stage(&transaction).await.unwrap(), counters);
    }
}

#[tokio::test]
async fn live_state_with_absent_marker_catches_up_only_when_a_delta_explains_it() {
    let db = Db::open(
        "text-validation-live-absent-delta",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let operation = test_support::operation();
    let definition = definition();
    let scope = DataScope::LegacyUnscoped;
    let partition = TextPartition::Unpartitioned;
    let (root_typed, root_key) = root_key(scope, &operation, &partition);
    transaction
        .put(root_key, root_value(&operation, partition.clone(), 1, 1))
        .unwrap();
    let entity = index_keys::IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(23),
    };
    let state_key = scoped_key(
        scope,
        index_keys::ScopedKey::TextEntityState(index_keys::TextEntityStateKey {
            root: root_typed,
            entity,
        }),
    );
    let state = work::TextEntityStateValue {
        index_id: operation.index_id(),
        generation: operation.generation(),
        partition,
        entity_kind: entity.kind,
        entity_id: entity.id,
        logical_version: TextLogicalVersion::initial(),
        live: true,
    };
    transaction
        .put(state_key, index_values::encode_text_entity_state(&state))
        .unwrap();
    let marker_key = scoped_key(
        scope,
        index_keys::ScopedKey::TextStatisticsEntity(index_keys::TextStatisticsEntityKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            entity,
        }),
    );
    let marker = work::TextStatisticsEntityValue {
        index_id: operation.index_id(),
        generation: operation.generation(),
        entity_kind: entity.kind,
        entity_id: entity.id,
        contribution: work::TextStatisticsContribution::Absent,
    };
    transaction
        .put(marker_key, index_values::encode_statistics_entity(&marker))
        .unwrap();
    let progress = entity_progress(OperationCounters::default());
    let limits = test_support::batch_limits(u64::MAX, u64::MAX, u64::MAX);

    let ValidationSelection::Database(unexplained) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &progress,
        limits,
    )
    .await
    .unwrap() else {
        panic!("entity-state validation is database-only")
    };
    assert!(matches!(
        unexplained.stage(&transaction).await.unwrap(),
        IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation)
    ));

    let (delta_key, delta_value) = build_delta_row(scope, &operation, entity);
    transaction.put(delta_key, delta_value).unwrap();
    let ValidationSelection::Database(explained) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &progress,
        limits,
    )
    .await
    .unwrap() else {
        panic!("explained marker mismatch selects database-only catch-up")
    };
    assert_catch_up_with_counters(
        explained.stage(&transaction).await.unwrap(),
        OperationCounters::default(),
    );
}

#[tokio::test]
async fn delta_inserted_after_preparation_invalidates_database_selection() {
    let db = Db::open(
        "text-validation-concurrent-delta-fence",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let operation = test_support::operation();
    let definition = definition();
    let scope = DataScope::LegacyUnscoped;
    let limits = test_support::batch_limits(u64::MAX, u64::MAX, u64::MAX);

    let ValidationSelection::Database(database) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &TextManifestValidationProgress::Pages(TextManifestPageValidationProgress::initial(
            OperationCounters::default(),
        )),
        limits,
    )
    .await
    .unwrap() else {
        panic!("empty page lane selects a database transition")
    };

    let entity = index_keys::IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(37),
    };
    let (delta_key, delta_value) = build_delta_row(scope, &operation, entity);
    transaction.put(delta_key, delta_value).unwrap();
    assert!(matches!(
        database.stage(&transaction).await.unwrap(),
        IndexOperationStepResult::TransientFailure
    ));
}

#[tokio::test]
async fn delta_inserted_after_preparation_invalidates_page_selection() {
    let db = Db::open(
        "text-validation-concurrent-delta-page-fence",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let operation = test_support::operation();
    let definition = definition();
    let scope = DataScope::LegacyUnscoped;
    let limits = test_support::batch_limits(u64::MAX, u64::MAX, u64::MAX);
    let partition = TextPartition::Unpartitioned;
    let (root_typed, root_key) = root_key(scope, &operation, &partition);
    let page_key = scoped_key(
        scope,
        index_keys::ScopedKey::TextManifestPage(index_keys::TextManifestPageKey {
            root: root_typed,
            page: 0,
        }),
    );
    let page = work::TextManifestPageValue::try_new(
        operation.index_id(),
        operation.generation(),
        partition.clone(),
        0,
        vec![test_support::split(31, 128)],
    )
    .unwrap();
    transaction
        .put(root_key, root_value(&operation, partition, 1, 1))
        .unwrap();
    transaction
        .put(page_key, index_values::encode_manifest_page(&page))
        .unwrap();
    let ValidationSelection::Page(page) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &TextManifestValidationProgress::Pages(TextManifestPageValidationProgress::initial(
            OperationCounters::default(),
        )),
        limits,
    )
    .await
    .unwrap() else {
        panic!("valid manifest page selects external validation")
    };

    let entity = index_keys::IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(37),
    };
    let (delta_key, delta_value) = build_delta_row(scope, &operation, entity);
    transaction.put(delta_key, delta_value).unwrap();
    assert!(matches!(
        page.stage(&transaction).await.unwrap(),
        IndexOperationStepResult::TransientFailure
    ));
}

#[tokio::test]
async fn prepared_database_revalidation_rejects_missing_changed_and_appended_rows() {
    let db = Db::open("text-validation-stale-ranges", Arc::new(InMemory::new()))
        .await
        .unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let prefix = Bytes::from_static(b"range/");
    let first_key = Bytes::from_static(b"range/a");
    let second_key = Bytes::from_static(b"range/b");
    let first_value = Bytes::from_static(b"one");
    let second_value = Bytes::from_static(b"two");
    transaction
        .put(first_key.clone(), first_value.clone())
        .unwrap();
    transaction
        .put(second_key.clone(), second_value.clone())
        .unwrap();
    let prepared = PreparedDatabaseValidation {
        ranges: vec![PreparedValidationRange {
            prefix,
            start: Bound::Unbounded,
            end: Bound::Included(Bytes::from_static(b"b")),
            rows: vec![
                (first_key.clone(), first_value.clone()),
                (second_key.clone(), second_value.clone()),
            ],
        }],
        observations: Vec::new(),
        result: IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation),
    };
    assert!(matches!(
        prepared.stage(&transaction).await.unwrap(),
        IndexOperationStepResult::Blocked(_)
    ));

    transaction.delete(second_key.clone()).unwrap();
    assert!(matches!(
        prepared.stage(&transaction).await.unwrap(),
        IndexOperationStepResult::TransientFailure
    ));
    transaction.put(second_key, second_value).unwrap();
    transaction
        .put(first_key, Bytes::from_static(b"changed"))
        .unwrap();
    assert!(matches!(
        prepared.stage(&transaction).await.unwrap(),
        IndexOperationStepResult::TransientFailure
    ));
}

#[tokio::test]
async fn page_validation_rejects_malformed_missing_mismatched_and_duplicate_inputs() {
    let db = Db::open(
        "text-validation-page-invalid-matrix",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let operation = test_support::operation();
    let definition = definition();
    let scope = DataScope::LegacyUnscoped;
    let partition = TextPartition::Unpartitioned;
    let (root_typed, root_key) = root_key(scope, &operation, &partition);
    let page_key = scoped_key(
        scope,
        index_keys::ScopedKey::TextManifestPage(index_keys::TextManifestPageKey {
            root: root_typed,
            page: 0,
        }),
    );
    let progress = TextManifestValidationProgress::Pages(
        TextManifestPageValidationProgress::initial(OperationCounters::default()),
    );
    let limits = test_support::batch_limits(u64::MAX, u64::MAX, u64::MAX);

    transaction
        .put(page_key.clone(), Bytes::from_static(b"malformed-page"))
        .unwrap();
    let ValidationSelection::Database(invalid_page) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &progress,
        limits,
    )
    .await
    .unwrap() else {
        panic!("malformed page is database-blocked");
    };
    assert!(matches!(
        invalid_page.stage(&transaction).await.unwrap(),
        IndexOperationStepResult::Blocked(_)
    ));

    let page = work::TextManifestPageValue::try_new(
        operation.index_id(),
        operation.generation(),
        partition.clone(),
        0,
        vec![test_support::split(1, 128)],
    )
    .unwrap();
    transaction
        .put(page_key.clone(), index_values::encode_manifest_page(&page))
        .unwrap();
    let ValidationSelection::Database(missing_root) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &progress,
        limits,
    )
    .await
    .unwrap() else {
        panic!("missing root is database-blocked");
    };
    assert!(matches!(
        missing_root.stage(&transaction).await.unwrap(),
        IndexOperationStepResult::Blocked(_)
    ));

    transaction
        .put(root_key.clone(), Bytes::from_static(b"malformed-root"))
        .unwrap();
    let ValidationSelection::Database(invalid_root) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &progress,
        limits,
    )
    .await
    .unwrap() else {
        panic!("malformed root is database-blocked");
    };
    assert!(matches!(
        invalid_root.stage(&transaction).await.unwrap(),
        IndexOperationStepResult::Blocked(_)
    ));

    transaction
        .put(
            root_key.clone(),
            root_value(&operation, partition.clone(), 1, 2),
        )
        .unwrap();
    let duplicate_page = work::TextManifestPageValue::try_new(
        operation.index_id(),
        operation.generation(),
        partition,
        0,
        vec![test_support::split(2, 128), test_support::split(2, 128)],
    )
    .unwrap();
    transaction
        .put(
            page_key,
            index_values::encode_manifest_page(&duplicate_page),
        )
        .unwrap();
    let ValidationSelection::Database(duplicate) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &progress,
        limits,
    )
    .await
    .unwrap() else {
        panic!("duplicate blob is database-blocked");
    };
    assert!(matches!(
        duplicate.stage(&transaction).await.unwrap(),
        IndexOperationStepResult::Blocked(_)
    ));
}

#[tokio::test]
async fn page_validation_checks_partition_progress_completion_and_counter_overflow() {
    let db = Db::open(
        "text-validation-page-progress-matrix",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let operation = test_support::operation();
    let scope = DataScope::LegacyUnscoped;
    let partition = TextPartition::Unpartitioned;
    let (root_typed, root_key) = root_key(scope, &operation, &partition);
    let page_key = scoped_key(
        scope,
        index_keys::ScopedKey::TextManifestPage(index_keys::TextManifestPageKey {
            root: root_typed,
            page: 0,
        }),
    );
    let page = work::TextManifestPageValue::try_new(
        operation.index_id(),
        operation.generation(),
        partition.clone(),
        0,
        vec![test_support::split(3, 128)],
    )
    .unwrap();
    transaction
        .put(root_key, root_value(&operation, partition, 1, 1))
        .unwrap();
    transaction
        .put(page_key.clone(), index_values::encode_manifest_page(&page))
        .unwrap();

    let foreign_partition = TextManifestPartitionValidation::try_new(
        [9; 32],
        TextManifestRevision::new(3).unwrap(),
        2,
        2,
        1,
        1,
    )
    .unwrap();
    let mismatched = TextManifestPageValidationProgress::try_new(
        Some(IndexCursor::try_new(page_key.clone()).unwrap()),
        Some(foreign_partition),
        OperationCounters::default(),
    )
    .unwrap();
    let ValidationSelection::Database(blocked) = select_page(
        &transaction,
        scope,
        &operation,
        &mismatched,
        test_support::batch_limits(u64::MAX, u64::MAX, u64::MAX),
    )
    .await
    .unwrap() else {
        panic!("mismatched partition progress is blocked");
    };
    assert!(matches!(
        blocked.stage(&transaction).await.unwrap(),
        IndexOperationStepResult::Blocked(_)
    ));

    let overflowing = TextManifestPageValidationProgress::initial(OperationCounters {
        input_bytes: u64::MAX,
        ..OperationCounters::default()
    });
    assert!(matches!(
        select_page(
            &transaction,
            scope,
            &operation,
            &overflowing,
            test_support::batch_limits(u64::MAX, u64::MAX, u64::MAX),
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
}

#[tokio::test]
async fn root_validation_covers_empty_nonempty_limits_and_stale_ranges() {
    let db = Db::open("text-validation-root-contracts", Arc::new(InMemory::new()))
        .await
        .unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let operation = test_support::operation();
    let definition = definition();
    let scope = DataScope::LegacyUnscoped;
    let partition = TextPartition::Unpartitioned;
    let (root_typed, root_key) = root_key(scope, &operation, &partition);
    let empty_root = root_value(&operation, partition.clone(), 0, 0);
    transaction
        .put(root_key.clone(), empty_root.clone())
        .unwrap();

    let progress = root_progress(OperationCounters::default());
    let ValidationSelection::Database(valid_empty) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &progress,
        test_support::batch_limits(u64::MAX, u64::MAX, u64::MAX),
    )
    .await
    .unwrap() else {
        panic!("empty root validation is database-only");
    };
    assert!(matches!(
        valid_empty.stage(&transaction).await.unwrap(),
        IndexOperationStepResult::Progressed(_)
    ));
    transaction.delete(root_key.clone()).unwrap();
    assert!(matches!(
        valid_empty.stage(&transaction).await.unwrap(),
        IndexOperationStepResult::TransientFailure
    ));

    let nonempty_root = root_value(&operation, partition.clone(), 1, 1);
    transaction.put(root_key.clone(), nonempty_root).unwrap();
    let ValidationSelection::Database(missing_corpus) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &progress,
        test_support::batch_limits(u64::MAX, u64::MAX, u64::MAX),
    )
    .await
    .unwrap() else {
        panic!("missing corpus is database-blocked");
    };
    assert!(matches!(
        missing_corpus.stage(&transaction).await.unwrap(),
        IndexOperationStepResult::Blocked(_)
    ));

    let corpus_key = super::super::statistics::corpus_key(
        scope,
        operation.index_id(),
        operation.generation(),
        &partition,
    );
    let corpus = work::TextCorpusStatisticsValue::try_new(
        operation.index_id(),
        operation.generation(),
        partition.clone(),
        1,
        1,
    )
    .unwrap();
    transaction
        .put(corpus_key, index_values::encode_corpus_statistics(&corpus))
        .unwrap();
    let ValidationSelection::Database(missing_page_zero) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &progress,
        test_support::batch_limits(u64::MAX, u64::MAX, u64::MAX),
    )
    .await
    .unwrap() else {
        panic!("missing page zero is database-blocked");
    };
    assert!(matches!(
        missing_page_zero.stage(&transaction).await.unwrap(),
        IndexOperationStepResult::Blocked(_)
    ));

    let page_key = scoped_key(
        scope,
        index_keys::ScopedKey::TextManifestPage(index_keys::TextManifestPageKey {
            root: root_typed,
            page: 0,
        }),
    );
    let page = work::TextManifestPageValue::try_new(
        operation.index_id(),
        operation.generation(),
        partition,
        0,
        vec![test_support::split(4, 128)],
    )
    .unwrap();
    transaction
        .put(page_key, index_values::encode_manifest_page(&page))
        .unwrap();
    let ValidationSelection::Database(low_limit) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &progress,
        test_support::batch_limits(1, u64::MAX, u64::MAX),
    )
    .await
    .unwrap() else {
        panic!("root limit is database-blocked");
    };
    assert!(matches!(
        low_limit.stage(&transaction).await.unwrap(),
        IndexOperationStepResult::Blocked(IndexOperationBlocker::ManifestLimit { .. })
    ));

    let overflow = root_progress(OperationCounters {
        input_bytes: u64::MAX,
        ..OperationCounters::default()
    });
    assert!(matches!(
        select(
            &transaction,
            scope,
            &operation,
            &definition,
            &overflow,
            test_support::batch_limits(u64::MAX, u64::MAX, u64::MAX),
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
}

#[tokio::test]
async fn entity_state_validation_covers_authority_markers_limits_and_overflow() {
    let db = Db::open(
        "text-validation-entity-contracts",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let operation = test_support::operation();
    let definition = definition();
    let scope = DataScope::LegacyUnscoped;
    let partition = TextPartition::Unpartitioned;
    let (root_typed, root_key) = root_key(scope, &operation, &partition);
    let entity = index_keys::IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(7),
    };
    let state_key = scoped_key(
        scope,
        index_keys::ScopedKey::TextEntityState(index_keys::TextEntityStateKey {
            root: root_typed,
            entity,
        }),
    );
    let state = work::TextEntityStateValue {
        index_id: operation.index_id(),
        generation: operation.generation(),
        partition: partition.clone(),
        entity_kind: entity.kind,
        entity_id: entity.id,
        logical_version: TextLogicalVersion::initial(),
        live: false,
    };
    transaction
        .put(state_key, index_values::encode_text_entity_state(&state))
        .unwrap();
    let progress = entity_progress(OperationCounters::default());
    let limits = test_support::batch_limits(u64::MAX, u64::MAX, u64::MAX);

    let ValidationSelection::Database(missing_root) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &progress,
        limits,
    )
    .await
    .unwrap() else {
        panic!("missing root is database-blocked");
    };
    assert!(matches!(
        missing_root.stage(&transaction).await.unwrap(),
        IndexOperationStepResult::Blocked(_)
    ));

    transaction
        .put(root_key.clone(), root_value(&operation, partition, 0, 0))
        .unwrap();
    let marker_key = scoped_key(
        scope,
        index_keys::ScopedKey::TextStatisticsEntity(index_keys::TextStatisticsEntityKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            entity,
        }),
    );
    let ValidationSelection::Database(missing_marker) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &progress,
        limits,
    )
    .await
    .unwrap() else {
        panic!("missing marker is database-blocked");
    };
    assert!(matches!(
        missing_marker.stage(&transaction).await.unwrap(),
        IndexOperationStepResult::Blocked(_)
    ));

    transaction
        .put(marker_key.clone(), Bytes::from_static(b"malformed-marker"))
        .unwrap();
    let ValidationSelection::Database(invalid_marker) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &progress,
        limits,
    )
    .await
    .unwrap() else {
        panic!("invalid marker is database-blocked");
    };
    assert!(matches!(
        invalid_marker.stage(&transaction).await.unwrap(),
        IndexOperationStepResult::Blocked(_)
    ));

    let marker = work::TextStatisticsEntityValue {
        index_id: operation.index_id(),
        generation: operation.generation(),
        entity_kind: entity.kind,
        entity_id: entity.id,
        contribution: work::TextStatisticsContribution::Absent,
    };
    transaction
        .put(marker_key, index_values::encode_statistics_entity(&marker))
        .unwrap();
    let ValidationSelection::Database(valid) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &progress,
        limits,
    )
    .await
    .unwrap() else {
        panic!("exact non-live state and absent marker are database-only");
    };
    assert!(matches!(
        valid.stage(&transaction).await.unwrap(),
        IndexOperationStepResult::Progressed(_)
    ));

    let ValidationSelection::Database(low_limit) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &progress,
        test_support::batch_limits(1, u64::MAX, u64::MAX),
    )
    .await
    .unwrap() else {
        panic!("entity-state limit is database-blocked");
    };
    assert!(matches!(
        low_limit.stage(&transaction).await.unwrap(),
        IndexOperationStepResult::Blocked(IndexOperationBlocker::ManifestLimit { .. })
    ));

    let overflow = entity_progress(OperationCounters {
        input_bytes: u64::MAX,
        ..OperationCounters::default()
    });
    assert!(matches!(
        select(
            &transaction,
            scope,
            &operation,
            &definition,
            &overflow,
            limits,
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
}

#[tokio::test]
async fn moved_non_live_entity_requires_the_exact_live_state_in_marker_partition() {
    let db = Db::open("text-validation-moved-entity", Arc::new(InMemory::new()))
        .await
        .unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let operation = test_support::operation();
    let definition = definition();
    let scope = DataScope::LegacyUnscoped;
    let old_partition = TextPartition::Unpartitioned;
    let old_fingerprint = old_partition.fingerprint();
    let new_partition = (0..=u16::MAX)
        .map(|seed| {
            TextPartition::try_tenant_value(Bytes::from(format!("new-partition-{seed}"))).unwrap()
        })
        .find(|partition| partition.fingerprint() > old_fingerprint)
        .expect("the fingerprint domain has a successor fixture");
    let entity = index_keys::IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(11),
    };
    let (old_root_typed, old_root_key) = root_key(scope, &operation, &old_partition);
    transaction
        .put(
            old_root_key,
            root_value(&operation, old_partition.clone(), 0, 0),
        )
        .unwrap();
    let old_state_key = scoped_key(
        scope,
        index_keys::ScopedKey::TextEntityState(index_keys::TextEntityStateKey {
            root: old_root_typed,
            entity,
        }),
    );
    let old_state = work::TextEntityStateValue {
        index_id: operation.index_id(),
        generation: operation.generation(),
        partition: old_partition,
        entity_kind: entity.kind,
        entity_id: entity.id,
        logical_version: TextLogicalVersion::initial(),
        live: false,
    };
    transaction
        .put(
            old_state_key,
            index_values::encode_text_entity_state(&old_state),
        )
        .unwrap();
    let marker_key = scoped_key(
        scope,
        index_keys::ScopedKey::TextStatisticsEntity(index_keys::TextStatisticsEntityKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            entity,
        }),
    );
    let contribution = work::TextStatisticsContribution::try_present(
        new_partition.clone(),
        [5; 32],
        1,
        vec![Bytes::from_static(b"term")],
    )
    .unwrap();
    let marker = work::TextStatisticsEntityValue {
        index_id: operation.index_id(),
        generation: operation.generation(),
        entity_kind: entity.kind,
        entity_id: entity.id,
        contribution,
    };
    transaction
        .put(marker_key, index_values::encode_statistics_entity(&marker))
        .unwrap();
    let (new_root_typed, _) = root_key(scope, &operation, &new_partition);
    let live_key = scoped_key(
        scope,
        index_keys::ScopedKey::TextEntityState(index_keys::TextEntityStateKey {
            root: new_root_typed,
            entity,
        }),
    );
    let live_state = work::TextEntityStateValue {
        index_id: operation.index_id(),
        generation: operation.generation(),
        partition: new_partition,
        entity_kind: entity.kind,
        entity_id: entity.id,
        logical_version: TextLogicalVersion::new(2).unwrap(),
        live: true,
    };
    transaction
        .put(
            live_key,
            index_values::encode_text_entity_state(&live_state),
        )
        .unwrap();

    let ValidationSelection::Database(valid) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &entity_progress(OperationCounters::default()),
        test_support::batch_limits(u64::MAX, u64::MAX, u64::MAX),
    )
    .await
    .unwrap() else {
        panic!("moved non-live entity is database-only");
    };
    assert!(matches!(
        valid.stage(&transaction).await.unwrap(),
        IndexOperationStepResult::Progressed(_)
    ));
}

#[tokio::test]
async fn selection_rejects_a_cursor_from_another_validation_lane() {
    let db = Db::open("text-validation-cursor-contract", Arc::new(InMemory::new()))
        .await
        .unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let operation = test_support::operation();
    let definition = definition();
    let progress = TextManifestValidationProgress::Roots(PrefixScanProgress {
        cursor: Some(IndexCursor::try_new(Bytes::from_static(b"foreign-lane")).unwrap()),
        counters: OperationCounters::default(),
    });
    assert!(matches!(
        select(
            &transaction,
            DataScope::LegacyUnscoped,
            &operation,
            &definition,
            &progress,
            test_support::batch_limits(u64::MAX, u64::MAX, u64::MAX),
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
}
