use std::collections::BTreeMap;
use std::sync::Arc;

use slatedb::object_store::memory::InMemory;
use slatedb::{Db, IsolationLevel};

use super::*;
use crate::index_lifecycle::{IndexElementKind, IndexEntityId, IndexGenerationId, IndexId};

#[test]
fn signed_statistics_arithmetic_and_delta_accumulators_fail_closed() {
    assert_eq!(apply_signed(7, -3).unwrap(), 4);
    assert!(apply_signed(0, -1).is_err());
    assert!(apply_signed(u64::MAX, 1).is_err());
    assert!(apply_signed(u64::MAX, i128::MAX).is_err());

    let partition = work::TextPartition::Unpartitioned;
    let mut partitions = BTreeMap::from([(partition.clone(), (i8::MAX, i128::MAX))]);
    assert!(add_partition_delta(&mut partitions, partition.clone(), 1, 0).is_err());
    assert!(add_partition_delta(&mut partitions, partition.clone(), 0, 1).is_err());

    let term = Bytes::from_static(b"term");
    let mut terms = BTreeMap::from([((partition, term.clone()), i8::MAX)]);
    assert!(add_term_delta(&mut terms, work::TextPartition::Unpartitioned, term, 1).is_err());
}

#[test]
fn prepared_batches_require_contiguous_observations_and_measure_final_writes() {
    let first = PreparedTextStatisticsTransition {
        rows: vec![
            PreparedStatisticsRow {
                key: Bytes::from_static(b"delete"),
                observed: Some(Bytes::from_static(b"old")),
                replacement: None,
            },
            PreparedStatisticsRow {
                key: Bytes::from_static(b"put"),
                observed: None,
                replacement: Some(Bytes::from_static(b"middle")),
            },
            PreparedStatisticsRow {
                key: Bytes::from_static(b"same"),
                observed: Some(Bytes::from_static(b"same-value")),
                replacement: Some(Bytes::from_static(b"same-value")),
            },
        ],
    };
    let (input, operations, output) = first.measurements();
    assert!(input > 0);
    assert_eq!(operations, 2);
    assert!(output > 0);
    assert_eq!(first.rows().len(), 3);

    let mut batch = PreparedTextStatisticsBatch::default();
    batch.push(first).unwrap();
    batch
        .push(PreparedTextStatisticsTransition {
            rows: vec![PreparedStatisticsRow {
                key: Bytes::from_static(b"put"),
                observed: Some(Bytes::from_static(b"middle")),
                replacement: Some(Bytes::from_static(b"final")),
            }],
        })
        .unwrap();
    assert_eq!(
        batch.effective_value(b"put"),
        Some(Some(Bytes::from_static(b"final")))
    );
    assert_eq!(batch.effective_value(b"absent"), None);
    let (_, operations, _) = batch.measurements();
    assert_eq!(operations, 2);

    assert!(batch
        .push(PreparedTextStatisticsTransition {
            rows: vec![PreparedStatisticsRow {
                key: Bytes::from_static(b"put"),
                observed: Some(Bytes::from_static(b"not-final")),
                replacement: None,
            }],
        })
        .is_err());
}

#[tokio::test]
async fn build_active_source_and_query_statistics_share_one_exact_persisted_model() {
    let db = Db::open(
        "text-statistics-external-contracts",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let scope = DataScope::LegacyUnscoped;
    let index_id = IndexId::initial();
    let generation = IndexGenerationId::initial();
    let entity = index_keys::IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::initial(),
    };
    let partition = work::TextPartition::Unpartitioned;
    let present = present_contribution(
        TextAnalyzerKind::StandardStemEn,
        partition.clone(),
        "one searchable document",
    )
    .unwrap();

    assert!(matches!(
        load_query_statistics(
            &db,
            scope,
            index_id,
            generation,
            &partition,
            TextAnalyzerKind::StandardStemEn,
            ""
        )
        .await
        .unwrap(),
        LoadedTextQueryStatistics::EmptyQuery
    ));
    assert!(matches!(
        load_query_statistics(
            &db,
            scope,
            index_id,
            generation,
            &partition,
            TextAnalyzerKind::StandardStemEn,
            "searchable"
        )
        .await
        .unwrap(),
        LoadedTextQueryStatistics::EmptyCorpus
    ));

    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let prepared = prepare_build_mutation(
        &transaction,
        scope,
        index_id,
        generation,
        entity,
        present.clone(),
    )
    .await
    .unwrap();
    validate(&transaction, &prepared).await.unwrap();
    stage_validated(&transaction, &prepared).unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(
        load_entity_contribution(&db, scope, index_id, generation, entity)
            .await
            .unwrap(),
        Some(present.clone())
    );

    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let mut batch = PreparedTextStatisticsBatch::default();
    let removal = prepare_active_in_batch(
        &transaction,
        &batch,
        ActiveTextStatisticsMutation::new(
            scope,
            index_id,
            generation,
            entity,
            present,
            work::TextStatisticsContribution::Absent,
        ),
    )
    .await
    .unwrap();
    batch.push(removal).unwrap();
    batch.validate(&transaction).await.unwrap();
    batch.stage_validated(&transaction).unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(
        load_entity_contribution(&db, scope, index_id, generation, entity)
            .await
            .unwrap(),
        Some(work::TextStatisticsContribution::Absent)
    );

    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    assert!(prepare_source_scan_in_batch(
        &transaction,
        &PreparedTextStatisticsBatch::default(),
        scope,
        index_id,
        generation,
        entity,
        work::TextStatisticsContribution::Absent,
    )
    .await
    .unwrap()
    .is_none());
    transaction.rollback();
    db.close().await.unwrap();
}
