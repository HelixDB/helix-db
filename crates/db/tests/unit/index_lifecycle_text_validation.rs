use std::num::{NonZeroU64, NonZeroUsize};
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

fn validation_batch_limits(max_entities: usize, max_input_bytes: u64) -> SearchIndexBatchLimits {
    SearchIndexBatchLimits::try_new(
        NonZeroUsize::new(max_entities).unwrap(),
        NonZeroU64::new(max_input_bytes).unwrap(),
        NonZeroU64::new(u64::MAX).unwrap(),
        NonZeroU64::new(u64::MAX).unwrap(),
        NonZeroU64::new(u64::MAX).unwrap(),
    )
    .unwrap()
}

fn entity_state_progress(result: IndexOperationStepResult) -> PrefixScanProgress {
    let IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
        TextBuildProgress::Constructing(TextBuildStage::ValidateManifests(
            TextManifestValidationProgress::EntityStates(progress),
        )),
    )) = result
    else {
        panic!("entity-state batch must remain in its validation lane")
    };
    progress
}

#[tokio::test]
async fn entity_state_validation_admits_1025_rows_in_three_default_sized_batches() {
    const ENTITY_COUNT: u64 = 1_025;
    const BATCH_ENTITIES: usize = 512;

    let db = Db::open(
        "text-validation-entity-default-batches",
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
    let root_value = root_value(&operation, partition.clone(), 0, 0);
    transaction.put(root_key, root_value).unwrap();
    let mut state_keys = Vec::new();
    for entity_id in 0..ENTITY_COUNT {
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(entity_id),
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
            .put(
                state_key.clone(),
                index_values::encode_text_entity_state(&state),
            )
            .unwrap();
        state_keys.push(state_key);
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
    }

    let limits = validation_batch_limits(BATCH_ENTITIES, u64::MAX);
    let mut progress = PrefixScanProgress {
        cursor: None,
        counters: OperationCounters::default(),
    };
    for expected_last in [511, 1023, 1024] {
        let ValidationSelection::Database(prepared) = select(
            &transaction,
            scope,
            &operation,
            &definition,
            &TextManifestValidationProgress::EntityStates(progress),
            limits,
        )
        .await
        .unwrap() else {
            panic!("entity-state validation is database-only")
        };
        progress = entity_state_progress(prepared.stage(&transaction).await.unwrap());
        assert_eq!(
            progress.cursor.as_ref().unwrap().as_bytes(),
            state_keys[expected_last].as_ref()
        );
    }

    let ValidationSelection::Database(exhausted) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &TextManifestValidationProgress::EntityStates(progress),
        limits,
    )
    .await
    .unwrap() else {
        panic!("entity-state exhaustion is database-only")
    };
    assert!(matches!(
        exhausted.stage(&transaction).await.unwrap(),
        IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
            TextBuildProgress::Constructing(TextBuildStage::Activate(_))
        ))
    ));
}

#[tokio::test]
async fn entity_state_batch_stops_before_the_first_row_exceeding_its_byte_budget() {
    let db = Db::open(
        "text-validation-entity-byte-batches",
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
    let root_value = root_value(&operation, partition.clone(), 0, 0);
    transaction
        .put(root_key.clone(), root_value.clone())
        .unwrap();
    let mut state_keys = Vec::new();
    let mut one_entity_bytes = None;
    for entity_id in 0..3 {
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(entity_id),
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
        let state_value = index_values::encode_text_entity_state(&state);
        transaction
            .put(state_key.clone(), state_value.clone())
            .unwrap();
        state_keys.push(state_key.clone());
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
        let marker_value = index_values::encode_statistics_entity(&marker);
        transaction
            .put(marker_key.clone(), marker_value.clone())
            .unwrap();
        let bytes = row_bytes(&state_key, Some(&state_value))
            .saturating_add(row_bytes(&root_key, Some(&root_value)))
            .saturating_add(row_bytes(&marker_key, Some(&marker_value)));
        assert!(one_entity_bytes.is_none_or(|expected| expected == bytes));
        one_entity_bytes = Some(bytes);
    }
    let one_entity_bytes = one_entity_bytes.unwrap();
    let limits = validation_batch_limits(8, one_entity_bytes.saturating_mul(2));

    let ValidationSelection::Database(first) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &entity_progress(OperationCounters::default()),
        limits,
    )
    .await
    .unwrap() else {
        panic!("entity-state validation is database-only")
    };
    let first = entity_state_progress(first.stage(&transaction).await.unwrap());
    assert_eq!(first.cursor.as_ref().unwrap().as_bytes(), &state_keys[1]);
    assert_eq!(first.counters.input_bytes, one_entity_bytes * 2);

    let ValidationSelection::Database(second) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &TextManifestValidationProgress::EntityStates(first),
        limits,
    )
    .await
    .unwrap() else {
        panic!("remaining entity-state validation is database-only")
    };
    let second = entity_state_progress(second.stage(&transaction).await.unwrap());
    assert_eq!(second.cursor.as_ref().unwrap().as_bytes(), &state_keys[2]);
    assert_eq!(second.counters.input_bytes, one_entity_bytes * 3);
}

#[tokio::test]
async fn page_validation_batches_contiguous_pages_and_preserves_partition_progress() {
    let db = Db::open("text-validation-page-batches", Arc::new(InMemory::new()))
        .await
        .unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let operation = test_support::operation();
    let definition = definition();
    let scope = DataScope::LegacyUnscoped;
    let partition = TextPartition::Unpartitioned;
    let (root_typed, root_key) = root_key(scope, &operation, &partition);
    transaction
        .put(root_key, root_value(&operation, partition.clone(), 3, 3))
        .unwrap();
    let mut page_keys = Vec::new();
    for page in 0..3 {
        let page_key = scoped_key(
            scope,
            index_keys::ScopedKey::TextManifestPage(index_keys::TextManifestPageKey {
                root: root_typed,
                page,
            }),
        );
        let value = work::TextManifestPageValue::try_new(
            operation.index_id(),
            operation.generation(),
            partition.clone(),
            page,
            vec![test_support::split(u8::try_from(page + 1).unwrap(), 128)],
        )
        .unwrap();
        transaction
            .put(page_key.clone(), index_values::encode_manifest_page(&value))
            .unwrap();
        page_keys.push(page_key);
    }
    let limits = validation_batch_limits(2, u64::MAX);

    let ValidationSelection::Page(first) = select(
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
        panic!("valid pages select external validation")
    };
    assert_eq!(first.blobs().len(), 2);
    let IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
        TextBuildProgress::Constructing(TextBuildStage::ValidateManifests(
            TextManifestValidationProgress::Pages(first_progress),
        )),
    )) = first.stage(&transaction).await.unwrap()
    else {
        panic!("partial page batch remains in page validation")
    };
    assert_eq!(first_progress.cursor().unwrap().as_bytes(), &page_keys[1]);
    assert_eq!(first_progress.partition().unwrap().next_page(), 2);

    let ValidationSelection::Page(second) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &TextManifestValidationProgress::Pages(first_progress),
        limits,
    )
    .await
    .unwrap() else {
        panic!("remaining valid page selects external validation")
    };
    assert_eq!(second.blobs().len(), 1);
    let IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
        TextBuildProgress::Constructing(TextBuildStage::ValidateManifests(
            TextManifestValidationProgress::Pages(second_progress),
        )),
    )) = second.stage(&transaction).await.unwrap()
    else {
        panic!("final page batch records page-lane exhaustion cursor")
    };
    assert_eq!(second_progress.cursor().unwrap().as_bytes(), &page_keys[2]);
    assert!(second_progress.partition().is_none());
}

#[tokio::test]
async fn page_batch_defers_on_bytes_deduplicates_heads_and_fences_every_page() {
    let db = Db::open(
        "text-validation-page-batch-contracts",
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
    let root_value = root_value(&operation, partition.clone(), 2, 2);
    transaction
        .put(root_key.clone(), root_value.clone())
        .unwrap();
    let shared_split = test_support::split(8, 128);
    let mut pages = Vec::new();
    for page in 0..2 {
        let key = scoped_key(
            scope,
            index_keys::ScopedKey::TextManifestPage(index_keys::TextManifestPageKey {
                root: root_typed,
                page,
            }),
        );
        let value = index_values::encode_manifest_page(
            &work::TextManifestPageValue::try_new(
                operation.index_id(),
                operation.generation(),
                partition.clone(),
                page,
                vec![shared_split],
            )
            .unwrap(),
        );
        transaction.put(key.clone(), value.clone()).unwrap();
        pages.push((key, value));
    }
    let one_page_bytes = row_bytes(&pages[0].0, Some(&pages[0].1))
        .saturating_add(row_bytes(&root_key, Some(&root_value)));
    assert_eq!(
        one_page_bytes,
        row_bytes(&pages[1].0, Some(&pages[1].1))
            .saturating_add(row_bytes(&root_key, Some(&root_value)))
    );

    let ValidationSelection::Page(first) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &TextManifestValidationProgress::Pages(TextManifestPageValidationProgress::initial(
            OperationCounters::default(),
        )),
        validation_batch_limits(8, one_page_bytes),
    )
    .await
    .unwrap() else {
        panic!("one page fits the exact byte budget")
    };
    let IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
        TextBuildProgress::Constructing(TextBuildStage::ValidateManifests(
            TextManifestValidationProgress::Pages(first_progress),
        )),
    )) = first.stage(&transaction).await.unwrap()
    else {
        panic!("the deferred second page remains pending")
    };
    assert_eq!(first_progress.cursor().unwrap().as_bytes(), &pages[0].0);
    assert_eq!(first_progress.counters().input_bytes, one_page_bytes);

    let ValidationSelection::Page(batch) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &TextManifestValidationProgress::Pages(TextManifestPageValidationProgress::initial(
            OperationCounters::default(),
        )),
        validation_batch_limits(8, u64::MAX),
    )
    .await
    .unwrap() else {
        panic!("both pages form one externally validated batch")
    };
    assert_eq!(batch.blobs(), &[shared_split.blob()]);
    transaction
        .put(pages[1].0.clone(), Bytes::from_static(b"changed"))
        .unwrap();
    assert!(matches!(
        batch.stage(&transaction).await.unwrap(),
        IndexOperationStepResult::TransientFailure
    ));
}

#[tokio::test]
async fn root_validation_batches_tenant_partitions_in_key_order() {
    let db = Db::open("text-validation-root-batches", Arc::new(InMemory::new()))
        .await
        .unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let operation = test_support::operation();
    let definition = ValidatedTextIndexDefinition::try_from_runtime(
        &crate::config::TextIndexDefinition::new_node("Document", "body")
            .unwrap()
            .with_tenant_property("tenant")
            .unwrap(),
    )
    .unwrap();
    let scope = DataScope::LegacyUnscoped;
    let mut root_keys = Vec::new();
    for tenant in ["alpha", "beta", "gamma"] {
        let partition = TextPartition::try_tenant_value(Bytes::from(tenant.to_string())).unwrap();
        let (_, key) = root_key(scope, &operation, &partition);
        transaction
            .put(key.clone(), root_value(&operation, partition, 0, 0))
            .unwrap();
        root_keys.push(key);
    }
    root_keys.sort();
    let limits = validation_batch_limits(2, u64::MAX);

    let ValidationSelection::Database(first) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &root_progress(OperationCounters::default()),
        limits,
    )
    .await
    .unwrap() else {
        panic!("root validation is database-only")
    };
    let IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
        TextBuildProgress::Constructing(TextBuildStage::ValidateManifests(
            TextManifestValidationProgress::Roots(first_progress),
        )),
    )) = first.stage(&transaction).await.unwrap()
    else {
        panic!("partial root batch remains in root validation")
    };
    assert_eq!(
        first_progress.cursor.as_ref().unwrap().as_bytes(),
        &root_keys[1]
    );

    let ValidationSelection::Database(second) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &TextManifestValidationProgress::Roots(first_progress),
        limits,
    )
    .await
    .unwrap() else {
        panic!("remaining root validation is database-only")
    };
    let IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
        TextBuildProgress::Constructing(TextBuildStage::ValidateManifests(
            TextManifestValidationProgress::Roots(second_progress),
        )),
    )) = second.stage(&transaction).await.unwrap()
    else {
        panic!("final root row remains in root validation until exhaustion is proved")
    };
    assert_eq!(
        second_progress.cursor.as_ref().unwrap().as_bytes(),
        &root_keys[2]
    );
}

#[tokio::test]
async fn root_batch_defers_on_bytes_and_fences_every_admitted_root() {
    let db = Db::open(
        "text-validation-root-batch-contracts",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let operation = test_support::operation();
    let definition = ValidatedTextIndexDefinition::try_from_runtime(
        &crate::config::TextIndexDefinition::new_node("Document", "body")
            .unwrap()
            .with_tenant_property("tenant")
            .unwrap(),
    )
    .unwrap();
    let scope = DataScope::LegacyUnscoped;
    let mut roots = Vec::new();
    for tenant in ["alpha", "bravo"] {
        let partition = TextPartition::try_tenant_value(Bytes::from(tenant.to_string())).unwrap();
        let (_, key) = root_key(scope, &operation, &partition);
        let value = root_value(&operation, partition.clone(), 0, 0);
        let corpus_key = super::super::statistics::corpus_key(
            scope,
            operation.index_id(),
            operation.generation(),
            &partition,
        );
        transaction.put(key.clone(), value.clone()).unwrap();
        roots.push((key, value, corpus_key));
    }
    roots.sort_by(|left, right| left.0.cmp(&right.0));
    let one_root_bytes =
        row_bytes(&roots[0].0, Some(&roots[0].1)).saturating_add(row_bytes(&roots[0].2, None));
    assert_eq!(
        one_root_bytes,
        row_bytes(&roots[1].0, Some(&roots[1].1)).saturating_add(row_bytes(&roots[1].2, None))
    );

    let ValidationSelection::Database(first) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &root_progress(OperationCounters::default()),
        validation_batch_limits(8, one_root_bytes),
    )
    .await
    .unwrap() else {
        panic!("root validation is database-only")
    };
    let IndexOperationStepResult::Progressed(IndexOperationProgress::TextBuild(
        TextBuildProgress::Constructing(TextBuildStage::ValidateManifests(
            TextManifestValidationProgress::Roots(first_progress),
        )),
    )) = first.stage(&transaction).await.unwrap()
    else {
        panic!("the deferred second root remains pending")
    };
    assert_eq!(
        first_progress.cursor.as_ref().unwrap().as_bytes(),
        &roots[0].0
    );
    assert_eq!(first_progress.counters.input_bytes, one_root_bytes);

    let ValidationSelection::Database(batch) = select(
        &transaction,
        scope,
        &operation,
        &definition,
        &root_progress(OperationCounters::default()),
        validation_batch_limits(8, u64::MAX),
    )
    .await
    .unwrap() else {
        panic!("root validation is database-only")
    };
    transaction.delete(roots[1].0.clone()).unwrap();
    assert!(matches!(
        batch.stage(&transaction).await.unwrap(),
        IndexOperationStepResult::TransientFailure
    ));
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
    let (new_root_typed, new_root_key) = root_key(scope, &operation, &new_partition);
    transaction
        .put(
            new_root_key,
            root_value(&operation, new_partition.clone(), 1, 1),
        )
        .unwrap();
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
