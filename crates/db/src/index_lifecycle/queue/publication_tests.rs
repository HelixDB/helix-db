//! Vector publication through the supervisor-owned publisher and real storage.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use helix_ast::{
    batch,
    graph::{EdgeRef, NodeRef},
    query::{QueryRequest, SearchConsistency},
    traversal,
    value::PropertyInput,
    value::PropertyValue,
};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use tokio::sync::oneshot;

use super::backlog::OperationCharge;
use super::overlay_tests::{delete, hits, vector_search, write};
use super::publication::{
    select_batch, HeldEntity, NextTarget, PublicationOutcome, QueuePublisher,
};
use super::storage::StoredQueue;
use super::tests::{
    add_doc, all_keys, open, publisher_with_limits, queue, queued, release_within_operand_bound,
    rows, target,
};
use super::QueueTarget;
use crate::batch_reads::BatchReads;
use crate::config::{
    CacheConfig, CacheMode, DbConfig, IndexOperationQueueTuning, QueueLayout,
    SearchIndexBatchLimits, TextIndexDefinition, VectorIndexDefinition,
};
use crate::encoding::v2::keys::indexes::vector::{
    VectorIndexMetadataKey, VectorKey, VectorStorageLane,
};
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{
    DataKey, DataKeyKind, IndexEntity, ManagedIndexKey, RecordKind, ScopedKey,
};
use crate::encoding::v2::values::indexes::operation_queue::{
    QueueFamily, QueueOperand, QueuedOperation, QueuedOperationId, QueuedPayload, QueuedTextPayload,
};
use crate::index_lifecycle::work::{TextPartition, TextStatisticsContribution};
use crate::index_lifecycle::{
    ActiveIndexHandle, IndexElementKind, IndexEntityId, ValidatedDynamicIndexDefinition,
    VectorPhysicalLayout,
};
use crate::search::vector::gated_wal::{GatedWalStore, WalUploads};
use crate::search::vector::{
    ValidatedVectorGenerationHandle, VectorDistanceMetric, VectorMemoryStore,
};
use crate::HelixDB;

pub(super) fn publisher(db: &HelixDB) -> &Arc<QueuePublisher> {
    db.index_queue_publisher()
        .expect("writer runs an automatic publisher")
}

/// Batch limits with `max_input_bytes` and `max_output_operations`; every
/// other limit is generous.
pub(super) fn batch_limits(
    max_input_bytes: u64,
    max_output_operations: u64,
) -> SearchIndexBatchLimits {
    SearchIndexBatchLimits::try_new(
        NonZeroUsize::new(512).unwrap(),
        NonZeroU64::new(max_input_bytes).unwrap(),
        NonZeroU64::new(max_output_operations).unwrap(),
        NonZeroU64::new(8 * 1024 * 1024).unwrap(),
        NonZeroU64::new(8 * 1024 * 1024).unwrap(),
    )
    .unwrap()
}

async fn queued_operations(db: &HelixDB) -> usize {
    queue(db, QueueFamily::Vector)
        .await
        .map_or(0, |queue| queue.operations().len())
}

/// Physical search: a zero eventual budget overlays no pending entity.
async fn physical(db: &HelixDB, query: [f32; 2]) -> Vec<(u64, f64)> {
    vector_search(db, query, 10, None, SearchConsistency::Eventual)
        .await
        .into_iter()
        .map(|(id, distance)| (id, f64::from_bits(distance)))
        .collect()
}

pub(super) async fn install_vector(db: &HelixDB, tenant: Option<&str>) {
    let definition =
        VectorIndexDefinition::new_node("Doc", "embedding", 2, VectorDistanceMetric::Euclidean)
            .unwrap();
    let definition = match tenant {
        Some(tenant) => definition.with_tenant_property(tenant).unwrap(),
        None => definition,
    };
    db.install_index_for_tests(ValidatedDynamicIndexDefinition::try_from(definition).unwrap())
        .await
        .unwrap();
}

/// Publishes until the generation queue is empty, returning published counts.
async fn drain(db: &HelixDB, target: QueueTarget) -> u64 {
    let mut published = 0;
    for _ in 0..1_000 {
        match publisher(db).publish_once(target).await.unwrap() {
            PublicationOutcome::Published { operations, .. } => published += operations,
            PublicationOutcome::Trimmed => {}
            PublicationOutcome::Empty => return published,
            outcome @ (PublicationOutcome::Discarded { .. }
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry
            | PublicationOutcome::Blocked
            | PublicationOutcome::Stalled) => {
                panic!("publication did not progress: {outcome:?}")
            }
        }
    }
    panic!("publication did not drain")
}

async fn search(db: &HelixDB, query: Vec<f32>, k: usize, tenant: Option<&str>) -> Vec<u64> {
    let result = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "hits",
                    traversal::g().vector_search_nodes(
                        "Doc",
                        "embedding",
                        query,
                        k,
                        tenant.map(PropertyValue::from),
                    ),
                )
                .returning(["hits"]),
        ))
        .await
        .unwrap();
    // An absent tenant partition (never materialized or reclaimed) is null.
    if result["hits"].is_null() {
        return Vec::new();
    }
    result["hits"]
        .as_array()
        .unwrap_or_else(|| panic!("search returned {result}"))
        .iter()
        .map(|hit| hit["$id"].as_u64().unwrap())
        .collect()
}

async fn set_embedding(db: &HelixDB, id: u64, vector: Vec<f32>) {
    db.query(QueryRequest::write(
        batch::write_batch().var_as(
            "updated",
            traversal::g()
                .n(NodeRef::from(id))
                .set_property("embedding", vector),
        ),
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn published_vectors_match_exact_search_and_drain_the_queue() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "publish-exact",
        store,
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector(&db, None).await;
    let mut points = Vec::new();
    for index in 0..40_u16 {
        let vector = vec![f32::from(index % 7), f32::from(index / 7)];
        let id = add_doc(&db, vector.clone(), "doc").await.unwrap();
        points.push((id, vector));
    }
    let target = target(&db, QueueFamily::Vector).await;
    let queries = [vec![0.0_f32, 0.0], vec![3.2, 2.9], vec![6.0, 5.0]];
    let expected = queries
        .iter()
        .map(|query| {
            let mut exact = points
                .iter()
                .map(|(id, vector)| {
                    let distance = (vector[0] - query[0]).powi(2) + (vector[1] - query[1]).powi(2);
                    (distance, *id)
                })
                .collect::<Vec<_>>();
            exact.sort_by(|left, right| left.partial_cmp(right).unwrap());
            exact.iter().take(5).map(|(_, id)| *id).collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    // Strong search overlays every pending vector before publication.
    for (query, expected) in queries.iter().zip(&expected) {
        assert_eq!(&search(&db, query.clone(), 5, None).await, expected);
    }
    assert_eq!(drain(&db, target).await, 40);
    assert!(
        queue(&db, QueueFamily::Vector).await.is_none(),
        "an empty queue is absent"
    );
    assert!(db
        .index_operation_backlog()
        .outstanding_targets()
        .is_empty());
    for (query, expected) in queries.iter().zip(&expected) {
        assert_eq!(&search(&db, query.clone(), 5, None).await, expected);
    }
    assert_eq!(
        publisher(&db)
            .metrics()
            .published_operations
            .load(Ordering::Relaxed),
        40
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn ordered_updates_deletes_and_recreation_converge_to_final_state() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "publish-order",
        store,
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector(&db, None).await;
    let moved = add_doc(&db, vec![0.0, 0.0], "a").await.unwrap();
    set_embedding(&db, moved, vec![5.0, 5.0]).await;
    set_embedding(&db, moved, vec![9.0, 9.0]).await;
    let deleted = add_doc(&db, vec![1.0, 1.0], "b").await.unwrap();
    db.query(QueryRequest::write(batch::write_batch().var_as(
        "dropped",
        traversal::g().n(NodeRef::from(deleted)).drop(),
    )))
    .await
    .unwrap();
    let recreated = add_doc(&db, vec![2.0, 2.0], "c").await.unwrap();
    db.query(QueryRequest::write(
        batch::write_batch().var_as(
            "removed",
            traversal::g()
                .n(NodeRef::from(recreated))
                .remove_property("embedding"),
        ),
    ))
    .await
    .unwrap();
    set_embedding(&db, recreated, vec![3.0, 3.0]).await;
    let target = target(&db, QueueFamily::Vector).await;
    assert_eq!(
        queue(&db, QueueFamily::Vector)
            .await
            .unwrap()
            .operations()
            .len(),
        8
    );
    assert_eq!(drain(&db, target).await, 8);

    assert_eq!(search(&db, vec![9.0, 9.0], 1, None).await, vec![moved]);
    assert_eq!(search(&db, vec![3.0, 3.0], 1, None).await, vec![recreated]);
    let all = search(&db, vec![0.0, 0.0], 10, None).await;
    assert_eq!(
        all,
        vec![recreated, moved],
        "deleted and superseded states are gone"
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn acknowledging_a_prefix_leaves_newer_operations_queued() {
    // Two text-free vector operations fit exactly: each retains 34 bytes
    // (mode 1, id 16, body_len 1, body: kind 1, id 1, previous 1-2,
    // replacement 1 + partition 1 + dimension 1 + 8).
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "publish-prefix",
        store,
        queued(IndexOperationQueueTuning::default().with_eventual_search_budget_for_tests(0)),
    )
    .await;
    install_vector(&db, None).await;
    let first = add_doc(&db, vec![0.0, 0.0], "a").await.unwrap();
    set_embedding(&db, first, vec![1.0, 1.0]).await;
    set_embedding(&db, first, vec![2.0, 2.0]).await;
    let second = add_doc(&db, vec![7.0, 7.0], "b").await.unwrap();
    let target = target(&db, QueueFamily::Vector).await;
    let operations = queue(&db, QueueFamily::Vector)
        .await
        .unwrap()
        .into_operations();
    let prefix_bytes = operations[0].retained_bytes() + operations[1].retained_bytes();
    assert!(
        physical(&db, [1.0, 1.0]).await.is_empty(),
        "nothing is published yet"
    );

    // A publisher with an input budget of exactly two operations publishes the
    // ordered prefix of the first entity and nothing newer.
    let narrow = publisher_with_limits(
        &db,
        batch_limits(prefix_bytes, 32_768),
        DbConfig::new()
            .search_index_backfill()
            .active_text_mutation(),
    );
    assert_eq!(
        narrow.publish_once(target).await.unwrap(),
        PublicationOutcome::Published {
            operations: 2,
            entities: 1
        }
    );
    let remaining = queue(&db, QueueFamily::Vector)
        .await
        .unwrap()
        .into_operations();
    assert_eq!(
        remaining
            .iter()
            .map(|operation| operation.id())
            .collect::<Vec<_>>(),
        vec![operations[2].id(), operations[3].id()],
        "the older acknowledgement removed exactly the published prefix"
    );
    // Physically, `first` holds the prefix's final vector [1, 1] and `second`
    // is absent; strong search overlays both pending entities instead.
    assert_eq!(physical(&db, [1.0, 1.0]).await, [(first, 0.0)]);
    assert_eq!(search(&db, vec![2.0, 2.0], 2, None).await, [first, second]);
    assert_eq!(
        db.index_operation_backlog()
            .usage(DataScope::LegacyUnscoped, target.index_id)
            .operations,
        2
    );
    assert_eq!(drain(&db, target).await, 2);
    assert_eq!(
        physical(&db, [2.0, 2.0]).await.first(),
        Some(&(first, 0.0)),
        "the rest of the chain replaced the prefix's vector"
    );
    assert_eq!(
        search(&db, vec![2.0, 2.0], 2, None).await,
        vec![first, second]
    );
    db.close().await.unwrap();
}

/// Raises the output-operation ceiling from one until an attempt publishes,
/// returning that publisher and its outcome.
///
/// Below the ceiling at which the first selected entity fits, attempts halve
/// the selection's operations until one operation alone crosses it: blocked,
/// nothing written. A trimmed attempt committed nothing, so every attempt
/// after a candidate's first reuses the queue that one read, or the one an
/// earlier publisher's commit retained, which the candidate then never
/// reads.
async fn smallest_publishing_ceiling(
    db: &HelixDB,
    target: QueueTarget,
    queued: usize,
) -> (Arc<QueuePublisher>, u64, PublicationOutcome) {
    let text = DbConfig::new()
        .search_index_backfill()
        .active_text_mutation();
    for max_output_operations in 1..1_000 {
        let candidate = publisher_with_limits(
            db,
            batch_limits(8 * 1024 * 1024, max_output_operations),
            text,
        );
        let retained = db.index_queue_store().retained().retained_bytes() > 0;
        let outcome = loop {
            match candidate.publish_once(target).await.unwrap() {
                PublicationOutcome::Trimmed => {}
                outcome @ (PublicationOutcome::Published { .. }
                | PublicationOutcome::Discarded { .. }
                | PublicationOutcome::Empty
                | PublicationOutcome::Deferred
                | PublicationOutcome::Retry
                | PublicationOutcome::Blocked
                | PublicationOutcome::Stalled) => break outcome,
            }
        };
        assert_eq!(
            candidate.metrics().queue_reads.load(Ordering::Relaxed),
            u64::from(!retained),
            "{max_output_operations} operations: trimmed attempts keep their queue"
        );
        match outcome {
            PublicationOutcome::Blocked => {
                assert_eq!(
                    candidate.metrics().blocked_attempts.load(Ordering::Relaxed),
                    1
                );
                assert!(
                    queued == 1 || candidate.metrics().output_retries.load(Ordering::Relaxed) > 0,
                    "several operations trim before one blocks"
                );
                assert_eq!(
                    queued_operations(db).await,
                    queued,
                    "trimmed and blocked attempts write nothing"
                );
            }
            published @ PublicationOutcome::Published { .. } => {
                assert!(
                    max_output_operations > 1,
                    "the acknowledgement alone fills a one-operation ceiling"
                );
                return (candidate, max_output_operations, published);
            }
            outcome @ (PublicationOutcome::Discarded { .. }
            | PublicationOutcome::Empty
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry
            | PublicationOutcome::Trimmed
            | PublicationOutcome::Stalled) => {
                panic!("{max_output_operations} operations gave {outcome:?}")
            }
        }
    }
    panic!("no ceiling fits one insert");
}

#[tokio::test]
async fn a_narrow_budget_publishes_the_fitting_prefix_without_retrying() {
    let db = open(
        "publish-output-budget",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default().with_eventual_search_budget_for_tests(0)),
    )
    .await;
    install_vector(&db, None).await;
    let mut ids = Vec::new();
    for index in 0..3_u8 {
        ids.push(
            add_doc(&db, vec![f32::from(index), 0.0], "doc")
                .await
                .unwrap(),
        );
    }
    let target = target(&db, QueueFamily::Vector).await;
    let operations = queue(&db, QueueFamily::Vector)
        .await
        .unwrap()
        .into_operations();

    // At the smallest fitting ceiling, the first entity fits and the second,
    // which also relinks the first, does not: the attempt commits exactly the
    // first entity, planned once and never retried.
    let (narrow, _, published) = smallest_publishing_ceiling(&db, target, 3).await;
    assert_eq!(
        published,
        PublicationOutcome::Published {
            operations: 1,
            entities: 1
        }
    );
    assert_eq!(narrow.metrics().output_retries.load(Ordering::Relaxed), 0);
    assert_eq!(narrow.metrics().blocked_attempts.load(Ordering::Relaxed), 0);
    assert_eq!(narrow.metrics().attempts.load(Ordering::Relaxed), 1);
    assert_eq!(
        queue(&db, QueueFamily::Vector)
            .await
            .unwrap()
            .into_operations()
            .iter()
            .map(|operation| operation.id())
            .collect::<Vec<_>>(),
        [operations[1].id(), operations[2].id()],
        "only the published prefix was acknowledged"
    );
    assert_eq!(
        db.index_operation_backlog()
            .usage(DataScope::LegacyUnscoped, target.index_id)
            .operations,
        2
    );
    assert_eq!(physical(&db, [0.0, 0.0]).await, [(ids[0], 0.0)]);
    assert_eq!(drain(&db, target).await, 2);
    assert_eq!(search(&db, vec![0.0, 0.0], 3, None).await, ids);
    db.close().await.unwrap();
}

#[tokio::test]
async fn an_entity_whose_output_alone_exceeds_the_budget_is_blocked() {
    let db = open(
        "publish-oversized-entity",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector(&db, None).await;
    let id = add_doc(&db, vec![0.0, 0.0], "a").await.unwrap();
    let target = target(&db, QueueFamily::Vector).await;
    let before = all_keys(&db).await;
    // The acknowledgement fits, but one insertion's vector output exceeds the
    // single-vector ceiling.
    let narrow = publisher_with_limits(
        &db,
        SearchIndexBatchLimits::try_new(
            NonZeroUsize::new(512).unwrap(),
            NonZeroU64::new(8 * 1024 * 1024).unwrap(),
            NonZeroU64::new(32_768).unwrap(),
            NonZeroU64::new(8 * 1024 * 1024).unwrap(),
            NonZeroU64::new(1).unwrap(),
        )
        .unwrap(),
        DbConfig::new()
            .search_index_backfill()
            .active_text_mutation(),
    );
    assert_eq!(
        narrow.publish_once(target).await.unwrap(),
        PublicationOutcome::Blocked
    );
    assert_eq!(narrow.metrics().output_retries.load(Ordering::Relaxed), 0);
    assert_eq!(narrow.metrics().blocked_attempts.load(Ordering::Relaxed), 1);
    assert_eq!(
        all_keys(&db).await,
        before,
        "a blocked attempt writes nothing"
    );
    assert_eq!(drain(&db, target).await, 1);
    assert_eq!(search(&db, vec![0.0, 0.0], 1, None).await, vec![id]);
    db.close().await.unwrap();
}

async fn add_tenant_doc(db: &HelixDB, vector: Vec<f32>, tenant: &str) -> u64 {
    let result = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "created",
                    traversal::g().add_n(
                        "Doc",
                        vec![
                            ("embedding", PropertyInput::from(vector)),
                            ("tenant", PropertyInput::from(tenant.to_string())),
                        ],
                    ),
                )
                .returning(["created"]),
        ))
        .await
        .unwrap();
    result["created"][0]["$id"].as_u64().unwrap()
}

#[tokio::test]
async fn a_prefix_advances_the_cursor_past_its_last_published_entity() {
    let db = open(
        "publish-prefix-cursor",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector(&db, Some("tenant")).await;
    // Each entity is the first of its own tenant partition, so each stages a
    // new mapping and costs about the same output.
    let mut ids = Vec::new();
    for (index, tenant) in (0_u8..).zip(["a", "b", "c", "d", "e"]) {
        ids.push(add_tenant_doc(&db, vec![f32::from(index), 0.0], tenant).await);
    }
    let target = target(&db, QueueFamily::Vector).await;
    let operations = queue(&db, QueueFamily::Vector)
        .await
        .unwrap()
        .into_operations();
    let (_, ceiling, published) = smallest_publishing_ceiling(&db, target, 5).await;
    assert_eq!(
        published,
        PublicationOutcome::Published {
            operations: 1,
            entities: 1
        }
    );

    // Each selection names three entities, of which only the first fits: the
    // slack absorbs one entity drawing a higher HNSW layer, not a second one.
    let rotating = publisher_with_limits(
        &db,
        batch_limits(
            operations[1..4]
                .iter()
                .map(|operation| operation.retained_bytes())
                .sum(),
            ceiling + 2,
        ),
        DbConfig::new()
            .search_index_backfill()
            .active_text_mutation(),
    );
    for _ in 0..2 {
        assert_eq!(
            rotating.publish_once(target).await.unwrap(),
            PublicationOutcome::Published {
                operations: 1,
                entities: 1
            }
        );
    }
    // The second selection started after the published entity, not after
    // the last selected one, so no selected entity waits a whole rotation.
    assert_eq!(
        queue(&db, QueueFamily::Vector)
            .await
            .unwrap()
            .into_operations()
            .iter()
            .map(|operation| operation.entity().id.get())
            .collect::<Vec<_>>(),
        ids[3..]
    );
    assert_eq!(rotating.metrics().output_retries.load(Ordering::Relaxed), 0);
    assert_eq!(drain(&db, target).await, 2);
    for (id, tenant) in ids.iter().zip(["a", "b", "c", "d", "e"]) {
        assert_eq!(
            search(&db, vec![0.0, 0.0], 5, Some(tenant)).await,
            vec![*id]
        );
    }
    db.close().await.unwrap();
}

/// Returns the physical namespace of every tenant partition mapping.
pub(super) async fn mapped_partitions(db: &HelixDB) -> Vec<u64> {
    let prefix = ManagedIndexKey::data_prefix(
        DataScope::LegacyUnscoped,
        ScopedKey::logical_prefix(RecordKind::VectorPartitionMapping),
    );
    let storage = db.inner_db();
    let mut rows = storage.scan_prefix(&prefix, ..).await.unwrap();
    let mut physical = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        physical.push(
            crate::encoding::v2::values::decode_partition_mapping(&row.value)
                .unwrap()
                .physical_index_id
                .get(),
        );
    }
    physical
}

/// Returns every physical row of one vector namespace, in key order.
pub(super) async fn physical_rows(
    db: &HelixDB,
    physical_index_id: u64,
) -> Vec<(bytes::Bytes, bytes::Bytes)> {
    let storage = db.inner_db();
    let mut rows = Vec::new();
    for lane in VectorStorageLane::ALL {
        let prefix = DataKey::data_prefix(
            DataScope::LegacyUnscoped,
            lane.prefix_key(physical_index_id).to_bytes(),
        );
        let mut scan = storage.scan_prefix(prefix, ..).await.unwrap();
        while let Some(row) = scan.next().await.unwrap() {
            rows.push((row.key, row.value));
        }
    }
    rows
}

#[tokio::test]
async fn one_batch_reclaims_a_partition_and_recreates_it_in_a_fresh_namespace() {
    let db = open(
        "publish-reclaim-recreate",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector(&db, Some("tenant")).await;
    let moved = add_tenant_doc(&db, vec![1.0, 1.0], "a").await;
    let target = target(&db, QueueFamily::Vector).await;
    assert_eq!(drain(&db, target).await, 1);
    let [reclaimed] = mapped_partitions(&db).await[..] else {
        panic!("one tenant partition is mapped");
    };
    assert!(!physical_rows(&db, reclaimed).await.is_empty());

    // Moving the only entity out of `a` empties it, and a later entity of
    // the same batch creates `a` again.
    db.query(QueryRequest::write(
        batch::write_batch().var_as(
            "moved",
            traversal::g()
                .n(NodeRef::from(moved))
                .set_property("tenant", "b".to_string()),
        ),
    ))
    .await
    .unwrap();
    let recreated = add_tenant_doc(&db, vec![2.0, 2.0], "a").await;
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Published {
            operations: 2,
            entities: 2
        }
    );
    assert_eq!(
        search(&db, vec![1.0, 1.0], 5, Some("a")).await,
        vec![recreated]
    );
    assert_eq!(search(&db, vec![1.0, 1.0], 5, Some("b")).await, vec![moved]);
    let partitions = mapped_partitions(&db).await;
    assert_eq!(partitions.len(), 2);
    assert!(
        !partitions.contains(&reclaimed),
        "the recreated partition has a fresh physical namespace"
    );
    assert!(
        physical_rows(&db, reclaimed).await.is_empty(),
        "the reclaimed namespace keeps no row"
    );
    db.close().await.unwrap();
}

/// Returns every physical row of the default scope's one unpartitioned
/// Active vector generation.
pub(super) async fn unpartitioned_vector_rows(db: &HelixDB) -> Vec<(bytes::Bytes, bytes::Bytes)> {
    let active = db
        .active_index_handles_loaded(DataScope::LegacyUnscoped)
        .into_iter()
        .find(|handle| matches!(handle, ActiveIndexHandle::Vector { .. }))
        .expect("one Active vector generation");
    let ActiveIndexHandle::Vector {
        layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
        ..
    } = active
    else {
        panic!("the vector generation is unpartitioned");
    };
    physical_rows(db, physical_index_id.get()).await
}

#[tokio::test]
async fn publication_builds_the_graph_a_build_of_the_same_inserts_builds() {
    let vectors = (0_u16..120)
        .map(|index| vec![f32::from(index % 11), f32::from(index / 11) * 1.5])
        .collect::<Vec<_>>();

    let published = open(
        "publish-build-parity-queued",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector(&published, None).await;
    let mut ids = Vec::new();
    for vector in &vectors {
        ids.push(add_doc(&published, vector.clone(), "doc").await.unwrap());
    }
    let target = target(&published, QueueFamily::Vector).await;
    // A narrow budget commits the queue as several admitted prefixes.
    let narrow = publisher_with_limits(
        &published,
        batch_limits(8 * 1024 * 1024, 512),
        DbConfig::new()
            .search_index_backfill()
            .active_text_mutation(),
    );
    let mut batches = 0;
    loop {
        match narrow.publish_once(target).await.unwrap() {
            PublicationOutcome::Published { .. } => batches += 1,
            PublicationOutcome::Empty => break,
            outcome @ (PublicationOutcome::Discarded { .. }
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry
            | PublicationOutcome::Trimmed
            | PublicationOutcome::Blocked
            | PublicationOutcome::Stalled) => {
                panic!("publication did not progress: {outcome:?}")
            }
        }
    }
    assert!(batches > 1, "the queue drained in {batches} batch");
    assert_eq!(narrow.metrics().output_retries.load(Ordering::Relaxed), 0);

    let built = open(
        "publish-build-parity-built",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    let mut built_ids = Vec::new();
    for vector in &vectors {
        built_ids.push(add_doc(&built, vector.clone(), "doc").await.unwrap());
    }
    assert_eq!(built_ids, ids);
    install_vector(&built, None).await;

    // Layers depend only on the index, generation, entity, and partition, so
    // publishing the inserts in order stages the build's exact rows.
    let published_rows = unpartitioned_vector_rows(&published).await;
    assert!(!published_rows.is_empty());
    assert!(
        published_rows == unpartitioned_vector_rows(&built).await,
        "publication and build graphs differ"
    );
    published.close().await.unwrap();
    built.close().await.unwrap();
}

#[tokio::test]
async fn a_concurrent_attempt_on_the_same_target_is_refused_without_touching_it() {
    let db = Arc::new(
        open(
            "publish-exclusive",
            Arc::new(InMemory::new()),
            queued(IndexOperationQueueTuning::default()),
        )
        .await,
    );
    install_vector(&db, None).await;
    add_doc(&db, vec![0.0, 0.0], "a").await.unwrap();
    let target = target(&db, QueueFamily::Vector).await;
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    *publisher(&db).hooks().before_commit.lock() = Some((reached_tx, release_rx));
    let publishing = {
        let db = Arc::clone(&db);
        tokio::spawn(async move { publisher(&db).publish_once(target).await })
    };
    reached_rx.await.unwrap();
    let attempts = publisher(&db).metrics().attempts.load(Ordering::Relaxed);
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Retry,
        "a second attempt never runs beside the first"
    );
    assert_eq!(
        publisher(&db).metrics().attempts.load(Ordering::Relaxed),
        attempts,
        "the refused call is not an attempt"
    );
    release_tx.send(()).unwrap();
    assert_eq!(
        publishing.await.unwrap().unwrap(),
        PublicationOutcome::Published {
            operations: 1,
            entities: 1
        }
    );
    // The claim was released with the first attempt.
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Empty
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn publication_commits_despite_concurrent_updates_to_selected_entities() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        open(
            "publish-concurrent",
            store,
            queued(IndexOperationQueueTuning::default()),
        )
        .await,
    );
    install_vector(&db, None).await;
    let first = add_doc(&db, vec![0.0, 0.0], "a").await.unwrap();
    let target = target(&db, QueueFamily::Vector).await;
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    *publisher(&db).hooks().before_commit.lock() = Some((reached_tx, release_rx));
    let publishing = {
        let db = Arc::clone(&db);
        tokio::spawn(async move { publisher(&db).publish_once(target).await })
    };
    reached_rx.await.unwrap();
    // While the publication transaction holds staged HNSW rows and the ACK of
    // operation 1, a foreground update of the same entity and a new entity
    // both commit: neither shares a token nor a read with the publication.
    set_embedding(&db, first, vec![4.0, 4.0]).await;
    let second = add_doc(&db, vec![8.0, 8.0], "b").await.unwrap();
    release_tx.send(()).unwrap();
    assert_eq!(
        publishing.await.unwrap().unwrap(),
        PublicationOutcome::Published {
            operations: 1,
            entities: 1
        },
        "the publication has no source-row or whole-queue dependency"
    );
    let remaining = queue(&db, QueueFamily::Vector)
        .await
        .unwrap()
        .into_operations();
    assert_eq!(
        remaining
            .iter()
            .map(|operation| operation.entity().id.get())
            .collect::<Vec<_>>(),
        vec![first, second],
        "the ACK of the older operation left both newer operations queued"
    );
    assert_eq!(drain(&db, target).await, 2);
    assert_eq!(
        search(&db, vec![4.0, 4.0], 2, None).await,
        vec![first, second]
    );
    Arc::into_inner(db).unwrap().close().await.unwrap();
}

#[tokio::test]
async fn failed_publication_leaves_operations_accounting_and_physical_rows_untouched() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "publish-failure",
        store,
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector(&db, None).await;
    add_doc(&db, vec![0.0, 0.0], "a").await.unwrap();
    add_doc(&db, vec![1.0, 0.0], "b").await.unwrap();
    let target = target(&db, QueueFamily::Vector).await;
    let before_keys = super::tests::all_keys(&db).await;
    let before_usage = db
        .index_operation_backlog()
        .usage(DataScope::LegacyUnscoped, target.index_id);
    publisher(&db)
        .hooks()
        .fail_before_commit
        .store(true, Ordering::SeqCst);
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Retry
    );
    assert_eq!(
        super::tests::all_keys(&db).await,
        before_keys,
        "nothing committed"
    );
    assert_eq!(
        db.index_operation_backlog()
            .usage(DataScope::LegacyUnscoped, target.index_id),
        before_usage
    );
    assert_eq!(drain(&db, target).await, 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn publication_uses_queued_payloads_without_reading_source_graph_rows() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "publish-payload",
        store,
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector(&db, None).await;
    let id = add_doc(&db, vec![6.0, 1.0], "a").await.unwrap();
    // Remove the graph property row out of band. A worker that consulted the
    // source graph would find nothing to index; the queued payload suffices.
    let property_key = crate::index_lifecycle::graph_mutation::GraphEntity::node(id)
        .property_key(DataScope::LegacyUnscoped);
    db.inner_db().delete(&property_key).await.unwrap();
    let target = target(&db, QueueFamily::Vector).await;
    assert_eq!(drain(&db, target).await, 1);
    let physical = super::tests::all_keys(&db)
        .await
        .into_iter()
        .filter(|key| key.first() == Some(&0xF0))
        .count();
    assert!(
        physical > 0,
        "the payload alone produced physical HNSW rows"
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn uncertain_acknowledgements_reconcile_after_a_flushed_read() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "publish-uncertain",
        store,
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector(&db, None).await;
    add_doc(&db, vec![0.0, 0.0], "a").await.unwrap();
    add_doc(&db, vec![1.0, 1.0], "b").await.unwrap();
    let target = target(&db, QueueFamily::Vector).await;
    let operations = queue(&db, QueueFamily::Vector)
        .await
        .unwrap()
        .into_operations();
    // Simulate a publication whose ACK of operation 0 committed but whose
    // response was lost, plus an uncertain ACK of operation 1 that never
    // committed.
    let (bytes, tokens) = QueueOperand::acknowledge(QueueFamily::Vector, [operations[0].id()])
        .unwrap()
        .into_parts();
    let transaction = db
        .inner_db()
        .begin(slatedb::IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    transaction
        .merge_disjoint_tokens(target.key(), tokens, bytes)
        .unwrap();
    transaction.commit().await.unwrap();
    db.index_operation_backlog()
        .mark_acknowledgement_uncertain([operations[0].id(), operations[1].id()]);
    let usage = db
        .index_operation_backlog()
        .usage(DataScope::LegacyUnscoped, target.index_id);
    assert_eq!((usage.operations, usage.uncertain_operations), (2, 2));
    // The next attempt reconciles before publishing: the absent ID is
    // released, the present one becomes durable and is then published.
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Published {
            operations: 1,
            entities: 1
        }
    );
    assert_eq!(
        db.index_operation_backlog()
            .usage(DataScope::LegacyUnscoped, target.index_id),
        Default::default()
    );
    // Both operations were committed here and acknowledged exactly once:
    // neither is rediscovered and both keep a measured lag.
    let stats = db.index_operation_queue_stats();
    assert_eq!(
        (
            stats.committed_operations,
            stats.discovered_operations,
            stats.acknowledged_operations,
            stats.censored_acknowledgements,
            stats.uncertain_operations,
        ),
        (2, 0, 2, 0, 0)
    );
    assert_eq!(db.index_operation_publication_lag().count(), 2);
    assert_eq!(
        db.index_operation_queue_stats().queue_reads,
        2,
        "the reconciliation read and the publication read are both counted"
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn restart_and_the_recovery_sweep_resume_publication_without_notifications() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "publish-restart",
        Arc::clone(&store),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector(&db, None).await;
    for index in 0..5_u8 {
        add_doc(&db, vec![f32::from(index), 0.0], "a")
            .await
            .unwrap();
    }
    db.close().await.unwrap();

    // Reopen with automatic publication running and a short sweep.
    let config = DbConfig::new().with_index_operation_queue_tuning(
        IndexOperationQueueTuning::default()
            .with_recovery_sweep_interval(Duration::from_millis(20))
            .unwrap(),
    );
    let reopened = open("publish-restart", store, config).await;
    tokio::time::timeout(Duration::from_secs(20), async {
        while !reopened
            .index_operation_backlog()
            .outstanding_targets()
            .is_empty()
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the supervisor published the recovered queue");
    assert_eq!(search(&reopened, vec![4.0, 0.0], 1, None).await.len(), 1);

    // Pause, enqueue, and resume without any notification: the sweep finds it.
    publisher(&reopened)
        .hooks()
        .paused
        .store(true, Ordering::SeqCst);
    add_doc(&reopened, vec![9.0, 9.0], "late").await.unwrap();
    publisher(&reopened)
        .hooks()
        .paused
        .store(false, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(20), async {
        while !reopened
            .index_operation_backlog()
            .outstanding_targets()
            .is_empty()
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the recovery sweep published work whose wake was consumed while paused");
    reopened.close().await.unwrap();
}

/// Two queued writes that return a published entity to its vector collapse
/// into one replay: both are acknowledged, as one entity, and no vector row
/// changes.
#[tokio::test]
async fn a_chain_back_to_the_published_vector_acknowledges_both_writes_and_changes_no_row() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "publish-replay-chain",
        store,
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector(&db, None).await;
    let mut ids = Vec::new();
    for index in 0..40_u16 {
        let vector = vec![f32::from(index % 7), f32::from(index / 7)];
        ids.push(add_doc(&db, vector, "doc").await.unwrap());
    }
    let target = target(&db, QueueFamily::Vector).await;
    assert_eq!(drain(&db, target).await, 40);
    let replayed = ids[17];
    set_embedding(&db, replayed, vec![9.5, 9.5]).await;
    set_embedding(&db, replayed, vec![3.0, 2.0]).await;
    let published = rows(&db, VectorKey::is_vector_keyspace).await;

    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Published {
            operations: 2,
            entities: 1
        }
    );
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Empty
    );
    assert!(queue(&db, QueueFamily::Vector).await.is_none());
    assert!(db
        .index_operation_backlog()
        .outstanding_targets()
        .is_empty());
    assert_eq!(rows(&db, VectorKey::is_vector_keyspace).await, published);
    assert_eq!(search(&db, vec![3.0, 2.0], 1, None).await, vec![replayed]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn tenant_moves_publish_into_pending_only_partitions_and_reclaim_empty_ones() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "publish-tenant",
        store,
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector(&db, Some("tenant")).await;
    let result = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "created",
                    traversal::g().add_n(
                        "Doc",
                        vec![
                            ("embedding", PropertyInput::from(vec![1.0_f32, 1.0])),
                            ("tenant", PropertyInput::from("a".to_string())),
                        ],
                    ),
                )
                .returning(["created"]),
        ))
        .await
        .unwrap();
    let id = result["created"][0]["$id"].as_u64().unwrap();
    let target = target(&db, QueueFamily::Vector).await;
    assert_eq!(drain(&db, target).await, 1);
    assert_eq!(search(&db, vec![1.0, 1.0], 1, Some("a")).await, vec![id]);
    db.query(QueryRequest::write(
        batch::write_batch().var_as(
            "moved",
            traversal::g()
                .n(NodeRef::from(id))
                .set_property("tenant", "b".to_string()),
        ),
    ))
    .await
    .unwrap();
    assert_eq!(drain(&db, target).await, 1);
    assert!(search(&db, vec![1.0, 1.0], 1, Some("a")).await.is_empty());
    assert_eq!(search(&db, vec![1.0, 1.0], 1, Some("b")).await, vec![id]);
    // The emptied source partition was reclaimed: its mapping row is gone.
    let mappings = super::tests::all_keys(&db)
        .await
        .into_iter()
        .filter(|key| {
            matches!(
                crate::encoding::v2::keys::ManagedIndexKey::parse_data_from_slice(key),
                Ok(crate::encoding::v2::keys::ManagedIndexKey::Data {
                    kind: crate::encoding::v2::keys::ScopedKey::VectorPartitionMapping(_),
                    ..
                })
            )
        })
        .count();
    assert_eq!(mappings, 1);
    db.close().await.unwrap();
}

/// Operand bound admitting 63 acknowledged IDs: a 5-byte header plus 16
/// bytes per ID.
const SMALL_OPERAND_BYTES: u64 = 1024;

#[tokio::test]
async fn vector_acknowledgements_fit_the_producer_operand_bound() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "publish-ack-bound",
        store,
        queued(
            IndexOperationQueueTuning::default()
                .with_max_operand_bytes(NonZeroU64::new(SMALL_OPERAND_BYTES).unwrap()),
        ),
    )
    .await;
    install_vector(&db, None).await;
    for index in 0..100_u16 {
        add_doc(&db, vec![f32::from(index), 0.0], "doc")
            .await
            .unwrap();
    }
    let target = target(&db, QueueFamily::Vector).await;
    assert_eq!(
        release_within_operand_bound(&db, target, QueueFamily::Vector).await,
        100
    );
    assert!(queue(&db, QueueFamily::Vector).await.is_none());
    assert_eq!(search(&db, vec![99.0, 0.0], 1, None).await.len(), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn row_acknowledgements_leave_room_for_their_effects() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "publish-rows-ack",
        store,
        queued(IndexOperationQueueTuning::default().with_layout(QueueLayout::Rows)),
    )
    .await;
    install_vector(&db, None).await;
    let hot = add_doc(&db, vec![0.0, 0.0], "hot").await.unwrap();
    for revision in 1..200_u16 {
        set_embedding(&db, hot, vec![f32::from(revision), 1.0]).await;
    }
    let target = target(&db, QueueFamily::Vector).await;
    // Row acknowledgements delete one row per operation, so the hot
    // entity's 200 operations alone exceed a 128-write transaction. Each
    // selection names at most half of it, leaving the collapsed effect room,
    // so no attempt is spent on an acknowledgement that fills the budget.
    let limits = SearchIndexBatchLimits::try_new(
        NonZeroUsize::new(512).unwrap(),
        NonZeroU64::new(8 * 1024 * 1024).unwrap(),
        NonZeroU64::new(128).unwrap(),
        NonZeroU64::new(8 * 1024 * 1024).unwrap(),
        NonZeroU64::new(8 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    let narrow = publisher_with_limits(
        &db,
        limits,
        DbConfig::new()
            .search_index_backfill()
            .active_text_mutation(),
    );
    let mut published = Vec::new();
    for _ in 0..1_000 {
        match narrow.publish_once(target).await.unwrap() {
            PublicationOutcome::Published { operations, .. } => published.push(operations),
            PublicationOutcome::Empty => break,
            outcome @ (PublicationOutcome::Discarded { .. }
            | PublicationOutcome::Trimmed
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry
            | PublicationOutcome::Blocked
            | PublicationOutcome::Stalled) => {
                panic!("row acknowledgements wasted an attempt: {outcome:?}")
            }
        }
    }
    assert_eq!(published, [64, 64, 64, 8]);
    assert_eq!(search(&db, vec![199.0, 1.0], 1, None).await, vec![hot]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn one_operation_that_cannot_fit_blocks_without_trimming() {
    for (name, layout) in [
        ("publish-blocked-map", QueueLayout::Map),
        ("publish-blocked-rows", QueueLayout::Rows),
    ] {
        let db = open(
            name,
            Arc::new(InMemory::new()),
            queued(IndexOperationQueueTuning::default().with_layout(layout)),
        )
        .await;
        install_vector(&db, None).await;
        let id = add_doc(&db, vec![0.0, 0.0], "a").await.unwrap();
        let target = target(&db, QueueFamily::Vector).await;
        let before = all_keys(&db).await;
        // Its acknowledgement alone fills a one-write transaction, so its
        // effect has no room however the selection shrinks.
        let narrow = publisher_with_limits(
            &db,
            batch_limits(8 * 1024 * 1024, 1),
            DbConfig::new()
                .search_index_backfill()
                .active_text_mutation(),
        );
        // Every backoff deadline lies after this instant, however long the
        // attempts take.
        let attempted = Instant::now();
        assert_eq!(
            narrow.publish_once(target).await.unwrap(),
            PublicationOutcome::Blocked,
            "{layout:?}"
        );
        assert!(
            matches!(
                narrow.next_target(&std::collections::HashSet::new(), attempted),
                NextTarget::Ready(ready) if ready == target
            ),
            "{layout:?}: holding the entity back lets the rest publish at once"
        );
        let held = narrow.blocked_entities();
        assert_eq!(
            held.iter()
                .map(|(target, entity)| (*target, entity.id.get()))
                .collect::<Vec<_>>(),
            [(target, id)],
            "{layout:?}"
        );
        // With only the held-back entity queued, attempts stall without
        // blocking again.
        for _ in 0..2 {
            assert_eq!(
                narrow.publish_once(target).await.unwrap(),
                PublicationOutcome::Stalled,
                "{layout:?}"
            );
        }
        assert_eq!(narrow.metrics().blocked_attempts.load(Ordering::Relaxed), 1);
        assert_eq!(narrow.blocked_entities(), held);
        assert_eq!(
            narrow.metrics().output_retries.load(Ordering::Relaxed),
            0,
            "{layout:?}: a blocked operation is never retried as a trim"
        );
        assert!(
            matches!(
                narrow.next_target(&std::collections::HashSet::new(), attempted),
                NextTarget::Delayed(_)
            ),
            "{layout:?}: stalled work backs off"
        );
        assert_eq!(
            all_keys(&db).await,
            before,
            "blocked attempts write nothing"
        );
        assert_eq!(drain(&db, target).await, 1);
        assert_eq!(search(&db, vec![0.0, 0.0], 1, None).await, vec![id]);
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn an_empty_queue_with_charged_work_backs_off() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "publish-empty-charged",
        store,
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector(&db, None).await;
    add_doc(&db, vec![0.0, 0.0], "a").await.unwrap();
    let target = target(&db, QueueFamily::Vector).await;
    assert_eq!(drain(&db, target).await, 1);
    // A foreground commit that reserved capacity is not visible yet, for
    // example while SlateDB write backpressure stalls it.
    let reservation = db
        .index_operation_backlog()
        .reserve(
            &[OperationCharge {
                target,
                entity: IndexEntity {
                    kind: IndexElementKind::Node,
                    id: IndexEntityId::new(1),
                },
                id: QueuedOperationId::generate(),
                encoded_bytes: 64,
            }],
            &[],
        )
        .unwrap();
    let publisher = publisher_with_limits(
        &db,
        DbConfig::new().search_index_backfill().batch(),
        DbConfig::new()
            .search_index_backfill()
            .active_text_mutation(),
    );
    // Backoff deadlines lie after the instant their attempt started, however
    // long the attempt takes.
    let attempted = Instant::now();
    assert_eq!(
        publisher.publish_once(target).await.unwrap(),
        PublicationOutcome::Empty
    );
    assert!(
        matches!(
            publisher.next_target(&std::collections::HashSet::new(), attempted),
            NextTarget::Delayed(_)
        ),
        "charged work that reads empty waits instead of being re-dispatched at once"
    );
    // It waits on another actor's commit, not on a failure, so its backoff
    // is capped like a hidden build's: once the commit lands, the operation
    // publishes within a second.
    let attempted = Instant::now();
    for _ in 0..12 {
        assert_eq!(
            publisher.publish_once(target).await.unwrap(),
            PublicationOutcome::Empty
        );
    }
    let settled = Instant::now();
    let NextTarget::Delayed(not_before) =
        publisher.next_target(&std::collections::HashSet::new(), attempted)
    else {
        panic!("charged work that reads empty stays backed off");
    };
    assert!(not_before <= settled + Duration::from_secs(1));
    drop(reservation);
    assert_eq!(
        publisher.next_target(&std::collections::HashSet::new(), Instant::now()),
        NextTarget::Idle
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_failure_after_the_acknowledgement_commits_releases_exactly_its_charges() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "publish-post-commit-failure",
        store,
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector(&db, None).await;
    add_doc(&db, vec![0.0, 0.0], "a").await.unwrap();
    add_doc(&db, vec![1.0, 0.0], "b").await.unwrap();
    let target = target(&db, QueueFamily::Vector).await;
    publisher(&db)
        .hooks()
        .fail_after_commit
        .store(true, Ordering::SeqCst);
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Retry
    );
    assert!(
        queue(&db, QueueFamily::Vector).await.is_none(),
        "the acknowledgement committed"
    );
    assert_eq!(
        db.index_operation_backlog()
            .usage(DataScope::LegacyUnscoped, target.index_id),
        Default::default(),
        "the committed acknowledgement released its charges"
    );
    assert!(db
        .index_operation_backlog()
        .outstanding_targets()
        .is_empty());
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Empty
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn an_active_namespace_without_metadata_fails_closed_and_writes_nothing() {
    let db = open(
        "publish-missing-metadata",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector(&db, None).await;
    add_doc(&db, vec![0.0, 0.0], "a").await.unwrap();
    add_doc(&db, vec![1.0, 1.0], "b").await.unwrap();
    let target = target(&db, QueueFamily::Vector).await;
    assert_eq!(drain(&db, target).await, 2);
    let Some(ActiveIndexHandle::Vector {
        layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
        ..
    }) = db
        .active_index_handles_loaded(DataScope::LegacyUnscoped)
        .into_iter()
        .find(|handle| matches!(handle, ActiveIndexHandle::Vector { .. }))
    else {
        panic!("one Active unpartitioned vector generation");
    };
    db.inner_db()
        .delete(
            DataKey::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::Vector(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                    physical_index_id.get(),
                ))),
            }
            .to_bytes(),
        )
        .await
        .unwrap();
    let inserted = add_doc(&db, vec![2.0, 2.0], "c").await.unwrap();
    let keys = all_keys(&db).await;
    let rows = unpartitioned_vector_rows(&db).await;

    // Only a build creates a missing namespace, so publication never
    // recreates it over rows search can no longer reach: planning the insert
    // fails, which holds it back.
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Blocked
    );
    assert_eq!(
        publisher(&db)
            .blocked_entities()
            .into_iter()
            .map(|(_, entity)| entity.id.get())
            .collect::<Vec<_>>(),
        [inserted]
    );
    assert_eq!(
        publisher(&db)
            .metrics()
            .error_retries
            .load(Ordering::Relaxed),
        0,
        "a missing namespace fails planning, not storage"
    );
    assert_eq!(
        all_keys(&db).await,
        keys,
        "the failed attempt writes nothing"
    );
    assert!(unpartitioned_vector_rows(&db).await == rows);
    assert_eq!(queued_operations(&db).await, 1);
    db.close().await.unwrap();
}

/// Rewrites namespace `physical_index_id`'s metadata under another property.
///
/// The index name stays, so the row still reads as valid metadata and only
/// the comparison with the canonical definition can reject it.
async fn contradict_metadata(db: &HelixDB, physical_index_id: u64) {
    let key = DataKey::Data {
        scope: DataScope::LegacyUnscoped,
        kind: DataKeyKind::Vector(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
            physical_index_id,
        ))),
    }
    .to_bytes();
    let stored = db
        .inner_db()
        .get(&key)
        .await
        .unwrap()
        .expect("the namespace has metadata");
    let mut metadata = crate::search::vector::decode_metadata(&stored).unwrap();
    metadata.config.property_name = "contradicting_embedding".to_string();
    db.inner_db()
        .put(
            &key,
            crate::search::vector::encode_metadata(&metadata).as_slice(),
        )
        .await
        .unwrap();
}

/// Asserts `target`'s next attempt fails closed on the contradicting metadata
/// of namespace `physical_index_id`: planning fails, holding back `entity`,
/// whose one queued operation stays queued, and the attempt writes nothing
/// and retains no planning session.
async fn assert_contradicting_metadata_fails_closed(
    db: &HelixDB,
    target: QueueTarget,
    physical_index_id: u64,
    entity: u64,
) {
    let keys = all_keys(db).await;
    let rows = physical_rows(db, physical_index_id).await;
    assert_eq!(
        publisher(db).publish_once(target).await.unwrap(),
        PublicationOutcome::Blocked
    );
    assert_eq!(
        publisher(db)
            .blocked_entities()
            .into_iter()
            .map(|(_, held)| held.id.get())
            .collect::<Vec<_>>(),
        [entity]
    );
    assert_eq!(
        publisher(db)
            .metrics()
            .error_retries
            .load(Ordering::Relaxed),
        0,
        "contradicting metadata fails planning, not storage"
    );
    assert_eq!(
        all_keys(db).await,
        keys,
        "the failed attempt writes nothing"
    );
    assert!(
        physical_rows(db, physical_index_id).await == rows,
        "every physical row is unchanged"
    );
    assert_eq!(queued_operations(db).await, 1);
    assert!(publisher(db)
        .planning_cache()
        .retained_publication(target)
        .is_none());
}

#[tokio::test]
async fn an_upsert_into_contradicting_metadata_fails_closed_and_writes_nothing() {
    let db = open(
        "publish-contradicting-upsert",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector(&db, None).await;
    add_doc(&db, vec![0.0, 0.0], "a").await.unwrap();
    let target = target(&db, QueueFamily::Vector).await;
    assert_eq!(drain(&db, target).await, 1);
    let Some(ActiveIndexHandle::Vector {
        layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
        ..
    }) = db
        .active_index_handles_loaded(DataScope::LegacyUnscoped)
        .into_iter()
        .find(|handle| matches!(handle, ActiveIndexHandle::Vector { .. }))
    else {
        panic!("one Active unpartitioned vector generation");
    };
    // The drain ended with Empty; one more commit leaves a warm session,
    // which the failed attempt then drops.
    add_doc(&db, vec![1.0, 1.0], "b").await.unwrap();
    assert!(matches!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Published { .. }
    ));
    contradict_metadata(&db, physical_index_id.get()).await;
    let inserted = add_doc(&db, vec![2.0, 2.0], "c").await.unwrap();
    assert!(publisher(&db)
        .planning_cache()
        .retained_publication(target)
        .is_some());
    assert_contradicting_metadata_fails_closed(&db, target, physical_index_id.get(), inserted)
        .await;
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_removal_from_contradicting_metadata_fails_closed_and_writes_nothing() {
    let db = open(
        "publish-contradicting-removal",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector(&db, Some("tenant")).await;
    let removed = add_tenant_doc(&db, vec![1.0, 1.0], "a").await;
    add_tenant_doc(&db, vec![2.0, 2.0], "a").await;
    let target = target(&db, QueueFamily::Vector).await;
    assert_eq!(drain(&db, target).await, 2);
    let [partition] = mapped_partitions(&db).await[..] else {
        panic!("one tenant partition is mapped");
    };
    contradict_metadata(&db, partition).await;

    // The partition keeps another entity, so only the removal reads its
    // metadata; no reclamation runs.
    super::overlay_tests::delete(&db, removed).await;
    assert_contradicting_metadata_fails_closed(&db, target, partition, removed).await;
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_physical_id_allocated_before_planning_retries_as_a_conflict() {
    let db = Arc::new(
        open(
            "publish-allocation-race",
            Arc::new(InMemory::new()),
            queued(IndexOperationQueueTuning::default()),
        )
        .await,
    );
    install_vector(&db, Some("tenant")).await;
    db.install_index_for_tests(
        ValidatedDynamicIndexDefinition::try_from(
            VectorIndexDefinition::new_node("Pic", "embedding", 2, VectorDistanceMetric::Euclidean)
                .unwrap()
                .with_tenant_property("tenant")
                .unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let document = add_tenant_doc(&db, vec![1.0, 1.0], "a").await;
    let [documents] = db.index_operation_backlog().outstanding_targets()[..] else {
        panic!("one queued document generation");
    };
    db.query(QueryRequest::write(batch::write_batch().var_as(
        "created",
        traversal::g().add_n(
            "Pic",
            vec![
                ("embedding", PropertyInput::from(vec![2.0_f32, 2.0])),
                ("tenant", PropertyInput::from("x".to_string())),
            ],
        ),
    )))
    .await
    .unwrap();
    let pictures = db
        .index_operation_backlog()
        .outstanding_targets()
        .into_iter()
        .find(|target| *target != documents)
        .expect("one queued picture generation");
    let (reached_tx, reached_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    *publisher(&db).hooks().before_planning.lock() = Some((reached_tx, release_rx));
    let publishing = {
        let db = Arc::clone(&db);
        tokio::spawn(async move { publisher(&db).publish_once(documents).await })
    };
    reached_rx.await.unwrap();

    // The picture partition takes the physical ID the open document
    // transaction still reads as free, and creates its namespace there.
    assert_eq!(
        publisher(&db).publish_once(pictures).await.unwrap(),
        PublicationOutcome::Published {
            operations: 1,
            entities: 1
        }
    );
    let conflicts = publisher(&db)
        .metrics()
        .commit_conflicts
        .load(Ordering::Relaxed);
    release_tx.send(()).unwrap();
    assert_eq!(
        publishing.await.unwrap().unwrap(),
        PublicationOutcome::Retry
    );
    assert_eq!(
        publisher(&db)
            .metrics()
            .commit_conflicts
            .load(Ordering::Relaxed),
        conflicts + 1
    );
    assert_eq!(
        publisher(&db)
            .metrics()
            .error_retries
            .load(Ordering::Relaxed),
        0,
        "the collision is a conflict, not an error"
    );
    assert_eq!(drain(&db, documents).await, 1);
    assert_eq!(
        search(&db, vec![1.0, 1.0], 1, Some("a")).await,
        vec![document]
    );
    Arc::into_inner(db).unwrap().close().await.unwrap();
}

/// Installs a second vector index over `Pic` nodes.
async fn install_pictures(db: &HelixDB) {
    let definition =
        VectorIndexDefinition::new_node("Pic", "embedding", 2, VectorDistanceMetric::Euclidean)
            .unwrap();
    db.install_index_for_tests(ValidatedDynamicIndexDefinition::try_from(definition).unwrap())
        .await
        .unwrap();
}

async fn add_picture(db: &HelixDB) {
    db.query(QueryRequest::write(batch::write_batch().var_as(
        "created",
        traversal::g().add_n(
            "Pic",
            vec![("embedding", PropertyInput::from(vec![1.0_f32, 1.0]))],
        ),
    )))
    .await
    .unwrap();
}

/// Queues one picture and one document, returning their generations.
async fn queue_pictures_then_documents(db: &HelixDB) -> (QueueTarget, QueueTarget) {
    install_vector(db, None).await;
    install_pictures(db).await;
    add_picture(db).await;
    let [pictures] = db.index_operation_backlog().outstanding_targets()[..] else {
        panic!("one queued picture generation");
    };
    add_doc(db, vec![0.0, 0.0], "a").await.unwrap();
    let documents = db
        .index_operation_backlog()
        .outstanding_targets()
        .into_iter()
        .find(|target| *target != pictures)
        .expect("one queued document generation");
    (pictures, documents)
}

async fn wait_until_published(db: &HelixDB, target: QueueTarget, why: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while db
            .index_operation_backlog()
            .usage(target.scope, target.index_id)
            .operations
            > 0
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect(why);
}

#[tokio::test]
async fn backed_off_publication_retries_while_another_task_is_in_flight() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "publish-in-flight-backoff",
        store,
        queued(
            IndexOperationQueueTuning::default()
                .with_recovery_sweep_interval(Duration::from_secs(600))
                .unwrap(),
        ),
    )
    .await;
    let (pictures, documents) = queue_pictures_then_documents(&db).await;
    // Five failures back the documents off for 320 ms: far longer than one
    // dispatch pass, so only a timer can run their retry.
    for _ in 0..5 {
        publisher(&db)
            .hooks()
            .fail_before_commit
            .store(true, Ordering::SeqCst);
        assert_eq!(
            publisher(&db).publish_once(documents).await.unwrap(),
            PublicationOutcome::Retry
        );
    }
    // The picture publication task waits on ownership held here, so it stays
    // in flight while the documents back off; nothing wakes the worker again.
    let ownership = db
        .inner
        .index_scope_gates
        .publication_permit(pictures)
        .await;
    publisher(&db).hooks().paused.store(false, Ordering::SeqCst);
    add_picture(&db).await;
    wait_until_published(
        &db,
        documents,
        "the document retry ran after its backoff while another task was in flight",
    )
    .await;
    assert_eq!(
        db.index_operation_backlog()
            .usage(pictures.scope, pictures.index_id)
            .operations,
        2
    );
    drop(ownership);
    wait_until_published(&db, pictures, "the picture publication finished").await;
    db.close().await.unwrap();
}

#[tokio::test]
async fn the_recovery_sweep_runs_while_another_task_is_in_flight() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "publish-in-flight-sweep",
        store,
        queued(
            IndexOperationQueueTuning::default()
                .with_recovery_sweep_interval(Duration::from_millis(50))
                .unwrap(),
        ),
    )
    .await;
    let (pictures, documents) = queue_pictures_then_documents(&db).await;
    // The picture publication task waits on ownership held here, so it stays
    // in flight while the documents publish.
    let ownership = db
        .inner
        .index_scope_gates
        .publication_permit(pictures)
        .await;
    publisher(&db).hooks().paused.store(false, Ordering::SeqCst);
    add_doc(&db, vec![1.0, 0.0], "b").await.unwrap();
    wait_until_published(&db, documents, "the woken worker published the documents").await;
    // The next document's notification arrives while publication is paused,
    // and resuming sends none: only the sweep can find it.
    publisher(&db).hooks().paused.store(true, Ordering::SeqCst);
    add_doc(&db, vec![2.0, 0.0], "c").await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    publisher(&db).hooks().paused.store(false, Ordering::SeqCst);
    wait_until_published(
        &db,
        documents,
        "the sweep dispatched queued work while another task was in flight",
    )
    .await;
    drop(ownership);
    wait_until_published(&db, pictures, "the picture publication finished").await;
    db.close().await.unwrap();
}

/// A writer over a gated WAL whose commit-fenced cache holds two published
/// entities, one of which has a queued move.
struct FencedPublication {
    gate: Arc<GatedWalStore>,
    db: HelixDB,
    target: QueueTarget,
    handle: ValidatedVectorGenerationHandle,
    /// Published at `[0, 0]`; its queued operation moves it to `[5, 5]`.
    moved: u64,
    /// Published at `[9, 9]` and never written again.
    untouched: u64,
}

impl FencedPublication {
    /// Publishes both entities, hydrates the cache, then queues the move.
    async fn open(name: &str) -> Self {
        let gate = Arc::new(GatedWalStore::new());
        let db = open(
            name,
            Arc::clone(&gate) as Arc<dyn ObjectStore>,
            queued(IndexOperationQueueTuning::default()),
        )
        .await;
        // Only the explicit refresh below publishes a resident store.
        db.inner
            .caches
            .vector_memory
            .refresh_task
            .lock()
            .await
            .take()
            .expect("the writer owns a vector refresh task")
            .stop()
            .await;
        install_vector(&db, None).await;
        let moved = add_doc(&db, vec![0.0, 0.0], "moved").await.unwrap();
        let untouched = add_doc(&db, vec![9.0, 9.0], "untouched").await.unwrap();
        let target = target(&db, QueueFamily::Vector).await;
        assert_eq!(drain(&db, target).await, 2);
        db.refresh_vector_memory_cache()
            .await
            .expect("the writer hydrates its vector cache");
        let active = db
            .active_index_handles_loaded(DataScope::LegacyUnscoped)
            .into_iter()
            .find(|handle| matches!(handle, ActiveIndexHandle::Vector { .. }))
            .expect("the fixture owns one Active vector generation");
        let ActiveIndexHandle::Vector {
            layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
            ..
        } = &active
        else {
            panic!("the fixture vector index is unpartitioned");
        };
        let handle =
            ValidatedVectorGenerationHandle::try_from_active_current(&active, *physical_index_id)
                .expect("the Active generation validates");
        let fixture = Self {
            gate,
            db,
            target,
            handle,
            moved,
            untouched,
        };
        let store = fixture.resident();
        assert!(
            store.get_simhash(moved).is_some() && store.get_simhash(untouched).is_some(),
            "the hydrated store caches both published entities"
        );
        set_embedding(&fixture.db, moved, vec![5.0, 5.0]).await;
        fixture
    }

    /// Returns the published store without proving it current.
    fn resident(&self) -> Arc<VectorMemoryStore> {
        Arc::clone(
            self.db
                .vector_cache_registry()
                .resident_guard_for(&self.handle)
                .expect("the Active generation is hydrated")
                .store(),
        )
    }

    /// Returns whether a commit on the generation still holds its fence.
    fn commit_pending(&self) -> bool {
        self.db
            .vector_cache_registry()
            .resident_guard_for(&self.handle)
            .expect("the Active generation is hydrated")
            .pending_dirty()
            .has_pending_commits()
    }

    /// Asserts that searches at the latest snapshot attach the store and that
    /// only the moved entity's rows were evicted from it.
    async fn assert_only_moved_evicted(&self) {
        let applied_seq = self.db.inner_db().snapshot().await.unwrap().seq();
        let guard = self
            .db
            .vector_cache_registry()
            .read_guard_for(&self.handle, applied_seq)
            .expect("the evicted store is current for the applied snapshot");
        assert!(
            guard.store().get_simhash(self.moved).is_none(),
            "the published entity's rows were evicted"
        );
        assert!(
            guard.store().get_simhash(self.untouched).is_some(),
            "rows the publication never wrote stay resident"
        );
    }

    /// Returns the strong nearest hit to the moved entity's new vector.
    ///
    /// From its stale `[0, 0]` rows the untouched entity would be nearer.
    async fn nearest_to_move(&self) -> (u64, f64) {
        let hits = vector_search(&self.db, [5.0, 5.0], 1, None, SearchConsistency::Strong).await;
        let [(id, distance)] = hits[..] else {
            panic!("one nearest hit, got {hits:?}");
        };
        (id, f64::from_bits(distance))
    }
}

#[tokio::test]
async fn publication_fences_resolve_once_applied_before_the_wal_is_durable() {
    let fixture = FencedPublication::open("publish-fence-durable").await;
    fixture.gate.uploads.send_replace(WalUploads::Held);
    let mut publishing = Box::pin(publisher(&fixture.db).publish_once(fixture.target));
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! {
                biased;
                outcome = &mut publishing => panic!("a held WAL upload cannot finish: {outcome:?}"),
                () = tokio::time::sleep(Duration::from_millis(5)) => {}
            }
            if fixture.resident().get_simhash(fixture.moved).is_none() && !fixture.commit_pending()
            {
                break;
            }
        }
    })
    .await
    .expect("the fence resolves while the WAL upload is still held");
    fixture.assert_only_moved_evicted().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut publishing)
            .await
            .is_err(),
        "the attempt still waits for its acknowledgement to become durable"
    );

    fixture.gate.uploads.send_replace(WalUploads::Open);
    assert_eq!(
        publishing.await.unwrap(),
        PublicationOutcome::Published {
            operations: 1,
            entities: 1
        }
    );
    assert_eq!(fixture.nearest_to_move().await, (fixture.moved, 0.0));
    fixture.db.close().await.unwrap();
}

#[tokio::test]
async fn a_publication_conflict_retries_without_evicting_the_resident_store() {
    let fixture = FencedPublication::open("publish-fence-conflict").await;
    let (reached_tx, reached_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    *publisher(&fixture.db).hooks().before_commit.lock() = Some((reached_tx, release_rx));
    let conflicting = async {
        reached_rx.await.unwrap();
        // Rewriting the index record with its own bytes commits inside the
        // range the publication read its ownership from.
        let storage = fixture.db.inner_db();
        let prefix = ManagedIndexKey::data_prefix(
            DataScope::LegacyUnscoped,
            ScopedKey::logical_prefix(RecordKind::IndexRecord),
        );
        let mut rows = storage.scan_prefix(&prefix, ..).await.unwrap();
        let record = rows
            .next()
            .await
            .unwrap()
            .expect("the vector index record exists");
        storage.put(&record.key, &record.value).await.unwrap();
        release_tx.send(()).unwrap();
    };
    let (outcome, ()) = tokio::join!(
        publisher(&fixture.db).publish_once(fixture.target),
        conflicting
    );
    assert_eq!(outcome.unwrap(), PublicationOutcome::Retry);
    assert_eq!(
        publisher(&fixture.db)
            .metrics()
            .commit_conflicts
            .load(Ordering::Relaxed),
        1
    );
    assert!(
        !fixture.commit_pending(),
        "the rejected commit released its fence"
    );
    let store = fixture.resident();
    assert!(
        store.get_simhash(fixture.moved).is_some()
            && store.get_simhash(fixture.untouched).is_some(),
        "a rejected batch evicts nothing"
    );
    let seq = fixture.db.inner_db().snapshot().await.unwrap().seq();
    fixture
        .db
        .vector_cache_registry()
        .read_guard_for(&fixture.handle, seq)
        .expect("a rejected commit leaves the store current");
    assert_eq!(
        queued_operations(&fixture.db).await,
        1,
        "nothing was acknowledged"
    );

    assert_eq!(drain(&fixture.db, fixture.target).await, 1);
    fixture.assert_only_moved_evicted().await;
    assert_eq!(fixture.nearest_to_move().await, (fixture.moved, 0.0));
    fixture.db.close().await.unwrap();
}

#[tokio::test]
async fn a_failure_after_the_batch_applies_evicts_its_rows_and_leaves_the_acknowledgement_uncertain(
) {
    let fixture = FencedPublication::open("publish-fence-post-apply").await;
    // A committed move leaves a retained session, and a hydrated store caches
    // the moved entity again before its next move.
    assert_eq!(
        publisher(&fixture.db)
            .publish_once(fixture.target)
            .await
            .unwrap(),
        PublicationOutcome::Published {
            operations: 1,
            entities: 1
        }
    );
    fixture
        .db
        .refresh_vector_memory_cache()
        .await
        .expect("the writer hydrates its vector cache");
    assert!(fixture.resident().get_simhash(fixture.moved).is_some());
    set_embedding(&fixture.db, fixture.moved, vec![6.0, 6.0]).await;
    assert!(publisher(&fixture.db)
        .planning_cache()
        .retained_publication(fixture.target)
        .is_some());
    fixture.gate.uploads.send_replace(WalUploads::Failing);
    assert_eq!(
        publisher(&fixture.db)
            .publish_once(fixture.target)
            .await
            .unwrap(),
        PublicationOutcome::Retry
    );
    assert!(
        publisher(&fixture.db)
            .planning_cache()
            .retained_publication(fixture.target)
            .is_none(),
        "an uncertain commit forgets the session"
    );
    assert_eq!(
        publisher(&fixture.db)
            .metrics()
            .uncertain_commits
            .load(Ordering::Relaxed),
        1
    );
    let usage = fixture
        .db
        .index_operation_backlog()
        .usage(DataScope::LegacyUnscoped, fixture.target.index_id);
    assert_eq!(
        (usage.operations, usage.uncertain_operations),
        (1, 1),
        "the acknowledgement may have committed, so its charge awaits reconciliation"
    );
    assert!(!fixture.commit_pending());
    let store = fixture.resident();
    assert!(
        store.get_simhash(fixture.moved).is_none(),
        "the batch may be visible, so its rows are evicted"
    );
    assert!(store.get_simhash(fixture.untouched).is_some());
    assert!(
        fixture
            .db
            .vector_cache_registry()
            .read_guard_for(&fixture.handle, u64::MAX)
            .is_ok(),
        "the evicted store stays attachable"
    );
    // The failed WAL upload closed the writer, so it is dropped unclosed.
}

#[tokio::test]
async fn a_dropped_publication_still_resolves_its_fence() {
    let fixture = FencedPublication::open("publish-fence-dropped").await;
    fixture.gate.uploads.send_replace(WalUploads::Held);
    let mut publishing = Box::pin(publisher(&fixture.db).publish_once(fixture.target));
    // The attempt spawns its commit in the poll that takes the fence. This
    // current-thread test does not yield between that poll and the drop, so
    // the commit task cannot resolve the fence before the attempt is gone.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            assert!(
                futures::poll!(publishing.as_mut()).is_pending(),
                "a held WAL upload cannot finish"
            );
            if fixture.commit_pending() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the attempt reaches its fenced commit");
    drop(publishing);

    tokio::time::timeout(Duration::from_secs(10), async {
        while fixture.commit_pending() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the detached commit resolves its fence after the attempt is gone");
    // A fence dropped unresolved would leave the store unattachable instead.
    fixture.assert_only_moved_evicted().await;
    fixture.gate.uploads.send_replace(WalUploads::Open);
    fixture.db.inner_db().flush().await.unwrap();
    assert_eq!(
        queued_operations(&fixture.db).await,
        0,
        "the acknowledgement committed"
    );
    // The attempt never released its charge, so it lasts until reopen
    // rebuilds the ledger from the queue. The worker never strands a charge
    // this way: graceful shutdown joins every in-flight publication instead
    // of aborting it. Only runtime teardown drops an attempt mid-commit, and
    // the ledger lives in memory, so that charge never outlives the process.
    // A supervisor panic would also drop its task set, but then no worker
    // publishes again until reopen rebuilds the ledger anyway.
    assert_eq!(
        fixture
            .db
            .index_operation_backlog()
            .usage(DataScope::LegacyUnscoped, fixture.target.index_id)
            .operations,
        1
    );
    let FencedPublication {
        gate,
        db,
        target,
        moved,
        ..
    } = fixture;
    db.close().await.unwrap();

    let reopened = open(
        "publish-fence-dropped",
        gate,
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    assert_eq!(
        reopened
            .index_operation_backlog()
            .usage(DataScope::LegacyUnscoped, target.index_id),
        Default::default()
    );
    let hits = vector_search(&reopened, [5.0, 5.0], 1, None, SearchConsistency::Strong).await;
    assert_eq!(
        hits.iter()
            .map(|(id, distance)| (*id, f64::from_bits(*distance)))
            .collect::<Vec<_>>(),
        [(moved, 0.0)]
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn no_fence_is_outstanding_while_publication_waits_before_its_commit() {
    let fixture = FencedPublication::open("publish-fence-barrier").await;
    let (reached_tx, reached_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    *publisher(&fixture.db).hooks().before_commit.lock() = Some((reached_tx, release_rx));
    let observing = async {
        reached_rx.await.unwrap();
        assert!(
            !fixture.commit_pending(),
            "fences are taken only once nothing but the commit remains"
        );
        let seq = fixture.db.inner_db().snapshot().await.unwrap().seq();
        let guard = fixture
            .db
            .vector_cache_registry()
            .read_guard_for(&fixture.handle, seq)
            .expect("searches keep attaching the store while publication is staged");
        assert!(
            guard.store().get_simhash(fixture.moved).is_some(),
            "nothing is evicted before the commit"
        );
        drop(guard);
        assert_eq!(
            fixture.nearest_to_move().await,
            (fixture.moved, 0.0),
            "strong search overlays the queued move on the cached rows"
        );
        release_tx.send(()).unwrap();
    };
    let (outcome, ()) = tokio::join!(
        publisher(&fixture.db).publish_once(fixture.target),
        observing
    );
    assert_eq!(
        outcome.unwrap(),
        PublicationOutcome::Published {
            operations: 1,
            entities: 1
        }
    );
    assert_eq!(queued_operations(&fixture.db).await, 0);
    fixture.assert_only_moved_evicted().await;
    assert_eq!(
        fixture.nearest_to_move().await,
        (fixture.moved, 0.0),
        "with the queue empty, the published rows answer"
    );
    fixture.db.close().await.unwrap();
}

#[tokio::test]
async fn searches_see_each_commit_planned_by_a_retained_session() {
    let fixture = FencedPublication::open("publish-fence-retained").await;
    let publisher = publisher(&fixture.db);
    // The fixture's drain ended with an empty queue, which forgot the session.
    let mut previous = publisher
        .planning_cache()
        .retained_publication(fixture.target);
    for round in 0..3_u8 {
        assert_eq!(previous.is_some(), round > 0, "round {round}");
        assert_eq!(
            publisher.publish_once(fixture.target).await.unwrap(),
            PublicationOutcome::Published {
                operations: 1,
                entities: 1
            }
        );
        let retained = publisher
            .planning_cache()
            .retained_publication(fixture.target)
            .expect("the commit retains its session");
        assert!(previous.is_none_or(|previous| retained.commit > previous.commit));
        previous = Some(retained);
        assert!(!fixture.commit_pending(), "the commit resolved its fence");
        fixture.assert_only_moved_evicted().await;
        let moved_to = [5.0 + f32::from(round); 2];
        for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
            let hits = vector_search(&fixture.db, moved_to, 1, None, consistency)
                .await
                .into_iter()
                .map(|(id, distance)| (id, f64::from_bits(distance)))
                .collect::<Vec<_>>();
            assert_eq!(
                hits,
                [(fixture.moved, 0.0)],
                "round {round}: {consistency:?} search right after the commit"
            );
        }
        set_embedding(&fixture.db, fixture.moved, vec![6.0 + f32::from(round); 2]).await;
    }
    fixture.db.close().await.unwrap();
}

#[tokio::test]
async fn publication_reads_rows_with_the_database_batch_policy() {
    for (name, cache, expected) in [
        (
            "publish-batch-reads-memory",
            CacheConfig::default(),
            BatchReads::Concurrent,
        ),
        (
            "publish-batch-reads-vector-memory-only",
            CacheConfig::default().with_mode(CacheMode::VectorMemoryOnly),
            BatchReads::Single,
        ),
    ] {
        let db = open(
            name,
            Arc::new(InMemory::new()),
            queued(IndexOperationQueueTuning::default()).with_cache(cache),
        )
        .await;
        assert_eq!(db.batch_reads(), expected, "{name}");
        install_vector(&db, None).await;
        add_doc(&db, vec![0.0, 0.0], "a").await.unwrap();
        let target = target(&db, QueueFamily::Vector).await;
        assert_eq!(drain(&db, target).await, 1);
        assert_eq!(
            *publisher(&db).hooks().batch_reads.lock(),
            Some(expected),
            "{name}: publication follows the cache mode"
        );
        db.close().await.unwrap();
    }
}

/// Publishes 40 grid points, then queues a move of a central point far
/// outside the grid when `update` is set, then two inserts inside it.
///
/// Returns the database, its vector target, the moved point, and the inserts.
async fn queue_behind_a_moved_point(
    name: &str,
    update: bool,
) -> (HelixDB, QueueTarget, u64, [u64; 2]) {
    let db = open(
        name,
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default().with_eventual_search_budget_for_tests(0)),
    )
    .await;
    install_vector(&db, None).await;
    let mut ids = Vec::new();
    for index in 0..40_u16 {
        let vector = vec![f32::from(index % 7), f32::from(index / 7)];
        ids.push(add_doc(&db, vector, "doc").await.unwrap());
    }
    let target = target(&db, QueueFamily::Vector).await;
    assert_eq!(drain(&db, target).await, 40);
    if update {
        set_embedding(&db, ids[17], vec![40.0, 40.0]).await;
    }
    let inserts = [
        add_doc(&db, vec![2.5, 2.5], "doc").await.unwrap(),
        add_doc(&db, vec![4.5, 1.5], "doc").await.unwrap(),
    ];
    (db, target, ids[17], inserts)
}

/// A queued update whose relink and reinsertion cannot fit a narrowed
/// publication blocks only its own entity: inserts queued after it, each of
/// which fits alone, still publish.
#[tokio::test]
async fn blocked_vector_head_does_not_stall_later_entities() {
    // Without the update the same inserts plan against the same graph, so
    // the smallest ceiling publishing each alone fits both here too.
    let (twin, target, _, _) = queue_behind_a_moved_point("publish-blocked-head-twin", false).await;
    let (_, first, published) = smallest_publishing_ceiling(&twin, target, 2).await;
    let ceiling = if published
        == (PublicationOutcome::Published {
            operations: 2,
            entities: 2,
        }) {
        first
    } else {
        first.max(smallest_publishing_ceiling(&twin, target, 1).await.1)
    };
    twin.close().await.unwrap();

    let (db, target, moved, inserts) =
        queue_behind_a_moved_point("publish-blocked-head", true).await;
    let narrow = publisher_with_limits(
        &db,
        batch_limits(8 * 1024 * 1024, ceiling),
        DbConfig::new()
            .search_index_backfill()
            .active_text_mutation(),
    );
    let mut outcomes = Vec::new();
    for _ in 0..16 {
        match narrow.publish_once(target).await.unwrap() {
            PublicationOutcome::Empty => break,
            outcome @ (PublicationOutcome::Published { .. }
            | PublicationOutcome::Discarded { .. }
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry
            | PublicationOutcome::Trimmed
            | PublicationOutcome::Blocked
            | PublicationOutcome::Stalled) => outcomes.push(outcome),
        }
    }
    assert!(
        narrow.metrics().blocked_attempts.load(Ordering::Relaxed) > 0,
        "the update cannot fit {ceiling} operations: {outcomes:?}"
    );
    assert_eq!(search(&db, vec![40.0, 40.0], 1, None).await, [moved]);
    assert_eq!(
        queue(&db, QueueFamily::Vector)
            .await
            .map_or_else(Vec::new, |queue| queue
                .operations()
                .iter()
                .map(|operation| operation.entity().id.get())
                .collect::<Vec<_>>()),
        [moved],
        "only the blocked update stays queued after {outcomes:?}"
    );
    for (insert, point) in inserts.into_iter().zip([[2.5, 2.5], [4.5, 1.5]]) {
        assert_eq!(physical(&db, point).await.first(), Some(&(insert, 0.0)));
    }
    assert_eq!(drain(&db, target).await, 1);
    assert_eq!(search(&db, vec![40.0, 40.0], 1, None).await, [moved]);
    db.close().await.unwrap();
}

/// Queues [`queue_behind_a_moved_point`]'s move behind a publisher whose
/// output ceiling fits each insert alone but not the move, and whose input
/// budget the move alone fills, then publishes until only the held-back move
/// is left.
///
/// Returns the database, its vector target, the narrowed publisher, and the
/// held-back point.
async fn hold_back_a_moved_point(name: &str) -> (HelixDB, QueueTarget, Arc<QueuePublisher>, u64) {
    let (twin, target, _, _) = queue_behind_a_moved_point(&format!("{name}-twin"), false).await;
    let (_, first, published) = smallest_publishing_ceiling(&twin, target, 2).await;
    let ceiling = if published
        == (PublicationOutcome::Published {
            operations: 2,
            entities: 2,
        }) {
        first
    } else {
        first.max(smallest_publishing_ceiling(&twin, target, 1).await.1)
    };
    twin.close().await.unwrap();

    let (db, target, moved, inserts) = queue_behind_a_moved_point(name, true).await;
    let queued = queue(&db, QueueFamily::Vector).await.unwrap();
    assert_eq!(queued.operations()[0].entity().id.get(), moved);
    // Every batch holds one entity: the first operation fills the budget.
    let narrow = publisher_with_limits(
        &db,
        batch_limits(queued.operations()[0].retained_bytes(), ceiling),
        DbConfig::new()
            .search_index_backfill()
            .active_text_mutation(),
    );
    let mut outcomes = Vec::new();
    for _ in 0..16 {
        match narrow.publish_once(target).await.unwrap() {
            PublicationOutcome::Stalled => break,
            outcome @ (PublicationOutcome::Published { .. }
            | PublicationOutcome::Discarded { .. }
            | PublicationOutcome::Empty
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry
            | PublicationOutcome::Trimmed
            | PublicationOutcome::Blocked) => outcomes.push(outcome),
        }
    }
    assert_eq!(
        narrow
            .blocked_entities()
            .into_iter()
            .map(|(_, entity)| entity.id.get())
            .collect::<Vec<_>>(),
        [moved],
        "{outcomes:?}"
    );
    for (insert, point) in inserts.into_iter().zip([[2.5, 2.5], [4.5, 1.5]]) {
        assert_eq!(physical(&db, point).await.first(), Some(&(insert, 0.0)));
    }
    (db, target, narrow, moved)
}

/// A held-back entity whose repair queues behind its blocked operation is
/// retried first, alone, and past the input budget: the repair publishes even
/// though the blocked operation alone fills that budget.
#[tokio::test]
async fn a_repair_publishes_a_held_back_vector_past_the_input_budget() {
    let (db, target, narrow, moved) = hold_back_a_moved_point("publish-repair-input").await;

    // Moving the point back restores its published vector.
    set_embedding(&db, moved, vec![3.0, 2.0]).await;
    assert_eq!(
        narrow.publish_once(target).await.unwrap(),
        PublicationOutcome::Published {
            operations: 2,
            entities: 1
        }
    );
    assert!(narrow.blocked_entities().is_empty());
    assert_eq!(
        narrow.publish_once(target).await.unwrap(),
        PublicationOutcome::Empty
    );
    assert_eq!(physical(&db, [3.0, 2.0]).await.first(), Some(&(moved, 0.0)));
    assert_ne!(
        physical(&db, [40.0, 40.0]).await.first(),
        Some(&(moved, 0.0))
    );
    db.close().await.unwrap();
}

/// Deleting a held-back point repairs it only once removing it fits one
/// publication. Removal relinks the point's neighbors, which here needs more
/// than the narrowed ceiling, so the delete is held back too: strong search
/// serves it at once while the published graph keeps the point, until a
/// publisher with the default limits removes it.
#[tokio::test]
async fn a_deleted_held_back_vector_publishes_once_its_removal_fits() {
    let (db, target, narrow, moved) = hold_back_a_moved_point("publish-repair-delete").await;
    // Whether the published graph still holds the point's vector row; a
    // deleted node is never served, whatever the graph holds.
    let published = async || {
        rows(&db, VectorKey::is_vector_keyspace)
            .await
            .keys()
            .any(|key| {
                matches!(
                    DataKey::parse_from_slice(DataScope::LegacyUnscoped, key),
                    Ok(DataKey::Data {
                        kind: DataKeyKind::Vector(VectorKey::Vector(item)),
                        ..
                    }) if item.node_id() == moved
                )
            })
    };
    assert!(published().await);
    delete(&db, moved).await;
    assert_eq!(
        narrow.publish_once(target).await.unwrap(),
        PublicationOutcome::Blocked
    );
    assert_eq!(
        narrow
            .blocked_entities()
            .into_iter()
            .map(|(_, entity)| entity.id.get())
            .collect::<Vec<_>>(),
        [moved]
    );
    assert!(!search(&db, vec![3.0, 2.0], 10, None).await.contains(&moved));
    assert!(published().await);

    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Published {
            operations: 2,
            entities: 1
        }
    );
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Empty
    );
    assert!(publisher(&db).blocked_entities().is_empty());
    assert!(!published().await);
    db.close().await.unwrap();
}

/// One edge's expected index state.
#[derive(Debug, Clone, Copy)]
struct Link {
    tenant: &'static str,
    embedding: Option<[f32; 2]>,
    body: Option<&'static str>,
}

/// Asserts tenant-scoped edge searches with `consistency` find exactly the
/// live `links`: every embedded edge of the tenant in exact distance order,
/// and every edge of the tenant whose body holds the term.
async fn assert_edge_searches(
    db: &HelixDB,
    links: &BTreeMap<u64, Link>,
    consistency: SearchConsistency,
    step: &str,
) {
    let found = async |search: traversal::Traversal<traversal::OnEdges>| {
        let request = QueryRequest::read(
            batch::read_batch()
                .var_as("hits", search)
                .returning(["hits"]),
        )
        .with_search_consistency(consistency)
        .unwrap();
        hits(&Box::pin(db.query(request)).await.unwrap(), "hits")
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>()
    };
    for tenant in ["a", "b", "c"] {
        for query in [[0.1_f32, 0.35], [3.3, 1.7]] {
            let mut exact = links
                .iter()
                .filter(|(_, link)| link.tenant == tenant)
                .filter_map(|(id, link)| {
                    let vector = link.embedding?;
                    Some((
                        (vector[0] - query[0]).powi(2) + (vector[1] - query[1]).powi(2),
                        *id,
                    ))
                })
                .collect::<Vec<_>>();
            exact.sort_by(|left, right| left.partial_cmp(right).unwrap());
            assert_eq!(
                found(traversal::g().vector_search_edges(
                    "LINK",
                    "embedding",
                    query.to_vec(),
                    8,
                    Some(PropertyValue::from(tenant)),
                ))
                .await,
                exact.into_iter().map(|(_, id)| id).collect::<Vec<_>>(),
                "{step}: tenant {tenant} vector {query:?} with {consistency:?} search"
            );
        }
        for term in [
            "shared", "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "omega", "rebirth",
        ] {
            let expected = links
                .iter()
                .filter(|(_, link)| {
                    link.tenant == tenant
                        && link
                            .body
                            .is_some_and(|body| body.split(' ').any(|word| word == term))
                })
                .map(|(id, _)| *id)
                .collect::<Vec<_>>();
            let mut text = found(traversal::g().text_search_edges(
                "LINK",
                "body",
                term,
                8,
                Some(PropertyValue::from(tenant)),
            ))
            .await;
            text.sort_unstable();
            assert_eq!(
                text, expected,
                "{step}: tenant {tenant} text {term:?} with {consistency:?} search"
            );
        }
    }
}

/// Every edge mutation (inserts, updates, tenant moves, property removals,
/// edge drops, cascading node drops, and re-adding a dropped edge's
/// endpoints) reaches edge vector and text indexes, whether its operations
/// collapse in the queue or each publishes before the next.
///
/// Edge hydration only checks that an edge still exists, so a leftover
/// physical row is invisible to search: the final rows are checked directly.
#[tokio::test]
async fn edge_vector_and_text_indexes_publish_every_edge_mutation() {
    for publish_each_step in [false, true] {
        let db = open(
            &format!("publish-edge-mutations-{publish_each_step}"),
            Arc::new(InMemory::new()),
            queued(IndexOperationQueueTuning::default()),
        )
        .await;
        for definition in [
            ValidatedDynamicIndexDefinition::try_from(
                VectorIndexDefinition::new_edge(
                    "LINK",
                    "embedding",
                    2,
                    VectorDistanceMetric::Euclidean,
                )
                .unwrap()
                .with_tenant_property("tenant")
                .unwrap(),
            )
            .unwrap(),
            ValidatedDynamicIndexDefinition::try_from(
                TextIndexDefinition::new_edge("LINK", "body")
                    .unwrap()
                    .with_tenant_property("tenant")
                    .unwrap(),
            )
            .unwrap(),
        ] {
            db.install_index_for_tests(definition).await.unwrap();
        }
        let targets = [
            target(&db, QueueFamily::Vector).await,
            target(&db, QueueFamily::Text).await,
        ];
        let nodes = write(&db, || {
            QueryRequest::write(
                (0..4)
                    .fold(batch::write_batch(), |nodes, node| {
                        nodes.var_as(
                            &format!("n{node}"),
                            traversal::g().add_n("Node", Vec::<(&str, PropertyInput)>::new()),
                        )
                    })
                    .returning(["n0", "n1", "n2", "n3"]),
            )
        })
        .await;
        let nodes = ["n0", "n1", "n2", "n3"].map(|name| nodes[name][0]["$id"].as_u64().unwrap());
        let mut links = BTreeMap::<u64, Link>::new();
        // After each step strong search sees every queued mutation; when
        // publishing each step, both consistencies see published rows.
        let checkpoint = async |links: &BTreeMap<u64, Link>, step: &str| {
            assert_edge_searches(&db, links, SearchConsistency::Strong, step).await;
            if publish_each_step {
                for target in targets {
                    drain(&db, target).await;
                }
                for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
                    assert_edge_searches(&db, links, consistency, step).await;
                }
            }
        };
        let mutate =
            async |change: traversal::Traversal<traversal::OnEdges, traversal::WriteEnabled>| {
                write(&db, || {
                    QueryRequest::write(batch::write_batch().var_as("changed", change.clone()))
                })
                .await;
            };

        let mut ids = Vec::new();
        for (from, to, embedding, body, tenant) in [
            (0, 1, [0.0, 0.0], "alpha shared", "a"),
            (1, 2, [1.0, 0.25], "beta shared", "a"),
            (2, 3, [2.0, 0.5], "gamma shared", "a"),
            (1, 3, [0.5, 2.0], "delta shared", "b"),
            (2, 1, [3.0, 3.0], "alpha epsilon", "b"),
            (0, 2, [4.0, 1.0], "beta zeta", "c"),
        ] {
            let created = write(&db, || {
                QueryRequest::write(
                    batch::write_batch()
                        .var_as(
                            "created",
                            traversal::g()
                                .n(NodeRef::from(nodes[from]))
                                .add_e(
                                    "LINK",
                                    NodeRef::from(nodes[to]),
                                    vec![
                                        ("embedding", PropertyInput::from(embedding.to_vec())),
                                        ("body", PropertyInput::from(body.to_string())),
                                        ("tenant", PropertyInput::from(tenant.to_string())),
                                    ],
                                )
                                .id(),
                        )
                        .returning(["created"]),
                )
            })
            .await;
            let id = created["created"][0].as_u64().unwrap();
            ids.push(id);
            links.insert(
                id,
                Link {
                    tenant,
                    embedding: Some(embedding),
                    body: Some(body),
                },
            );
            checkpoint(&links, &format!("add edge {}", ids.len())).await;
        }

        mutate(
            traversal::g()
                .e(EdgeRef::id(ids[1]))
                .set_property("embedding", vec![1.5_f32, 1.5])
                .set_property("body", "omega shared".to_string()),
        )
        .await;
        let link = links.get_mut(&ids[1]).unwrap();
        (link.embedding, link.body) = (Some([1.5, 1.5]), Some("omega shared"));
        checkpoint(&links, "update embedding and body").await;

        mutate(
            traversal::g()
                .e(EdgeRef::id(ids[2]))
                .set_property("tenant", "b".to_string()),
        )
        .await;
        links.get_mut(&ids[2]).unwrap().tenant = "b";
        checkpoint(&links, "tenant move").await;

        mutate(
            traversal::g()
                .e(EdgeRef::id(ids[4]))
                .remove_property("embedding"),
        )
        .await;
        links.get_mut(&ids[4]).unwrap().embedding = None;
        checkpoint(&links, "remove the embedding").await;
        mutate(
            traversal::g()
                .e(EdgeRef::id(ids[3]))
                .remove_property("body"),
        )
        .await;
        links.get_mut(&ids[3]).unwrap().body = None;
        checkpoint(&links, "remove the body").await;

        write(&db, || {
            QueryRequest::write(batch::write_batch().var_as(
                "dropped",
                traversal::g().drop_edge_by_id(EdgeRef::id(ids[1])),
            ))
        })
        .await;
        links.remove(&ids[1]);
        checkpoint(&links, "drop one edge").await;

        // Dropping the source node cascades to both edges it starts.
        write(&db, || {
            QueryRequest::write(
                batch::write_batch()
                    .var_as("dropped", traversal::g().n(NodeRef::from(nodes[0])).drop()),
            )
        })
        .await;
        links.remove(&ids[0]);
        links.remove(&ids[5]);
        checkpoint(&links, "drop a source node").await;

        // The dropped edge's endpoints are linked again under a new ID.
        let created = write(&db, || {
            QueryRequest::write(
                batch::write_batch()
                    .var_as(
                        "created",
                        traversal::g()
                            .n(NodeRef::from(nodes[1]))
                            .add_e(
                                "LINK",
                                NodeRef::from(nodes[2]),
                                vec![
                                    ("embedding", PropertyInput::from(vec![0.25_f32, 0.25])),
                                    ("body", PropertyInput::from("alpha rebirth".to_string())),
                                    ("tenant", PropertyInput::from("a".to_string())),
                                ],
                            )
                            .id(),
                    )
                    .returning(["created"]),
            )
        })
        .await;
        let readded = created["created"][0].as_u64().unwrap();
        assert!(!ids.contains(&readded));
        links.insert(
            readded,
            Link {
                tenant: "a",
                embedding: Some([0.25, 0.25]),
                body: Some("alpha rebirth"),
            },
        );
        checkpoint(&links, "re-add an edge").await;

        for target in targets {
            drain(&db, target).await;
        }
        for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
            assert_edge_searches(&db, &links, consistency, "published").await;
        }
        assert_eq!(db.index_operation_queue_stats().pending_operations, 0);

        // Vector rows: only live embedded edges are placed, once each, in
        // their tenant's partition, and no row or link names any other edge.
        let mut placed = BTreeMap::<u64, BTreeSet<u64>>::new();
        let mut counts = BTreeMap::<u64, u64>::new();
        let mut named = BTreeSet::<u64>::new();
        for (key, value) in rows(&db, VectorKey::is_vector_keyspace).await {
            let Ok(DataKey::Data {
                kind: DataKeyKind::Vector(key),
                ..
            }) = DataKey::parse_from_slice(DataScope::LegacyUnscoped, &key)
            else {
                panic!("unparsed vector row {key:?}");
            };
            match key {
                VectorKey::Vector(item) => {
                    assert!(
                        placed
                            .entry(key.index_id())
                            .or_default()
                            .insert(item.node_id()),
                        "edge {} placed twice in one partition",
                        item.node_id()
                    );
                    named.insert(item.node_id());
                }
                VectorKey::UpperVector(item) => {
                    named.insert(item.node_id());
                }
                VectorKey::SimHash(item) => {
                    named.insert(item.node_id());
                }
                VectorKey::SimHashDirectory(item) => {
                    named.insert(item.node_id());
                }
                VectorKey::EntryCandidateSorted(item) => {
                    named.insert(item.node_id());
                }
                VectorKey::EntryCandidateNode(item) => {
                    named.insert(item.node_id());
                }
                VectorKey::Layer0Neighbors(item) => {
                    named.insert(item.node_id());
                    named.extend(
                        crate::encoding::v2::values::indexes::vector::decode_layer0_neighbors(
                            &value,
                        )
                        .unwrap(),
                    );
                }
                VectorKey::UpperNeighbors(item) => {
                    named.insert(item.node_id());
                    named.extend(
                        crate::encoding::v2::values::indexes::vector::neighbors::decode_upper_neighbors(
                            &value,
                        )
                        .unwrap(),
                    );
                }
                VectorKey::ReverseEdge(item) => {
                    named.insert(item.target_node_id());
                    named.insert(item.source_node_id());
                }
                VectorKey::IndexMetadata(item) => {
                    let metadata = crate::search::vector::decode_metadata(&value).unwrap();
                    counts.insert(item.index_id(), metadata.count);
                    named.extend(metadata.entry_point);
                }
                VectorKey::IndexPrefix(_)
                | VectorKey::TxnGuard(_)
                | VectorKey::VectorPrefix(_)
                | VectorKey::SimHashDirectoryPrefix(_)
                | VectorKey::EntryCandidatePrefix(_)
                | VectorKey::MemoryPrefix(_)
                | VectorKey::L0Prefix(_)
                | VectorKey::ReverseEdgePrefix(_) => {}
            }
        }
        let embedded = |tenant| {
            links
                .iter()
                .filter(|(_, link)| link.tenant == tenant && link.embedding.is_some())
                .map(|(id, _)| *id)
                .collect::<BTreeSet<_>>()
        };
        assert_eq!(
            placed.values().cloned().collect::<BTreeSet<_>>(),
            BTreeSet::from([embedded("a"), embedded("b")]),
            "publish each step {publish_each_step}"
        );
        let live = embedded("a")
            .union(&embedded("b"))
            .copied()
            .collect::<BTreeSet<_>>();
        assert!(
            named.is_subset(&live),
            "publish each step {publish_each_step}: vector rows name dead edges {:?}",
            named.difference(&live).collect::<Vec<_>>()
        );
        // The emptied partition `c` was reclaimed, and every remaining
        // partition's metadata counts exactly its live edges.
        let mut partitions = mapped_partitions(&db).await;
        partitions.sort_unstable();
        assert_eq!(
            partitions,
            placed.keys().copied().collect::<Vec<_>>(),
            "publish each step {publish_each_step}"
        );
        for (partition, members) in &placed {
            assert_eq!(
                counts.get(partition).copied(),
                Some(members.len() as u64),
                "publish each step {publish_each_step}: partition {partition} metadata count"
            );
        }

        // Text markers: live bodies are accounted once per tenant partition,
        // and corpus statistics count exactly them. Removing a published
        // document tombstones its marker as absent; a document never
        // published leaves none.
        let mut accounted = BTreeMap::<TextPartition, BTreeSet<u64>>::new();
        let mut tombstoned = BTreeSet::new();
        let mut corpus = BTreeMap::<TextPartition, u64>::new();
        let is_corpus = |key: &[u8]| {
            matches!(
                ManagedIndexKey::parse_data_from_slice(key),
                Ok(ManagedIndexKey::Data {
                    kind: ScopedKey::TextCorpusStatistics(_),
                    ..
                })
            )
        };
        for (_, value) in rows(&db, is_corpus).await {
            let statistics = crate::encoding::v2::values::decode_corpus_statistics(&value).unwrap();
            if statistics.document_count > 0 {
                corpus.insert(statistics.partition, statistics.document_count);
            }
        }
        let is_marker = |key: &[u8]| {
            matches!(
                ManagedIndexKey::parse_data_from_slice(key),
                Ok(ManagedIndexKey::Data {
                    kind: ScopedKey::TextStatisticsEntity(_),
                    ..
                })
            )
        };
        for (_, value) in rows(&db, is_marker).await {
            let marker = crate::encoding::v2::values::decode_statistics_entity(&value).unwrap();
            match marker.contribution {
                TextStatisticsContribution::Present { partition, .. } => {
                    accounted
                        .entry(partition)
                        .or_default()
                        .insert(marker.entity_id.get());
                }
                TextStatisticsContribution::Absent => {
                    tombstoned.insert(marker.entity_id.get());
                }
            }
        }
        let bodies = |tenant| {
            links
                .iter()
                .filter(|(_, link)| link.tenant == tenant && link.body.is_some())
                .map(|(id, _)| *id)
                .collect::<BTreeSet<_>>()
        };
        assert_eq!(
            accounted.values().cloned().collect::<BTreeSet<_>>(),
            BTreeSet::from([bodies("a"), bodies("b")]),
            "publish each step {publish_each_step}"
        );
        assert_eq!(
            corpus,
            accounted
                .iter()
                .map(|(partition, members)| (partition.clone(), members.len() as u64))
                .collect::<BTreeMap<_, _>>(),
            "publish each step {publish_each_step}: corpus document counts"
        );
        let removed = BTreeSet::from([ids[0], ids[1], ids[3], ids[5]]);
        if publish_each_step {
            assert_eq!(tombstoned, removed);
        } else {
            assert!(tombstoned.is_empty(), "{tombstoned:?}");
        }
        db.close().await.unwrap();
    }
}

/// A held entity whose repair the rotation reaches after other entities ends
/// that batch, so the next batch, which starts after its last entity, repairs
/// it. Skipping it instead kept wrapping past it: with two entities rewritten
/// in a fixed order between publications, every batch ended on the same
/// entity and the repair never ran.
#[test]
fn a_repair_is_reached_however_its_generation_is_rewritten() {
    let entity = |id| IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(id),
    };
    let (held, alpha, bravo) = (entity(1), entity(2), entity(3));
    let operation = |id, entity| {
        QueuedOperation::new(
            QueuedOperationId::try_from_u128(id).unwrap(),
            entity,
            QueuedPayload::Text(QueuedTextPayload { replacement: None }),
        )
    };
    // `held` blocked alone, which moved the rotation past it; `bravo` and
    // then `alpha` were written behind it.
    let mut queue = vec![operation(1, held), operation(2, bravo), operation(3, alpha)];
    let holds = HashMap::from([(
        held,
        HeldEntity::Waiting {
            through: queue[0].id(),
        },
    )]);
    let mut cursor = held;
    let mut batches = Vec::new();
    for round in 0..8_u128 {
        let selected = select_batch(
            &StoredQueue::new(QueueFamily::Text, queue.clone(), HashMap::new(), 0)
                .expect("the queue holds operations"),
            Some(cursor),
            &holds,
            512,
            NonZeroUsize::MAX,
            NonZeroUsize::MAX,
            u64::MAX,
        )
        .iter()
        .map(|selected| {
            (
                selected.entity.id.get(),
                selected.operations.len() + selected.superseding.len(),
            )
        })
        .collect::<Vec<_>>();
        // Publishing acknowledges the batch, and the next one starts after
        // its last entity.
        queue.retain(|queued| {
            selected
                .iter()
                .all(|(entity, _)| queued.entity().id.get() != *entity)
        });
        cursor = entity(selected.last().expect("a repair is selectable").0);
        batches.push(selected);
        if cursor == held {
            break;
        }
        if round == 0 {
            // The repair, then `alpha` and `bravo`, rewritten in that order
            // before every later attempt.
            queue.push(operation(4, held));
        }
        queue.extend([
            operation(10 + 2 * round, alpha),
            operation(11 + 2 * round, bravo),
        ]);
    }
    assert_eq!(
        batches,
        [vec![(3, 1), (2, 1)], vec![(3, 1)], vec![(1, 2)]],
        "the repair runs once the batch the rotation carried up to it publishes"
    );
}
