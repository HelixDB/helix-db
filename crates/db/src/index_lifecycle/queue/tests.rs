//! Queued foreground maintenance through the public query boundary.

use std::collections::BTreeSet;
use std::num::NonZeroU64;
use std::sync::Arc;

use helix_ast::{batch, graph::NodeRef, query::QueryRequest, traversal, value::PropertyInput};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

use super::publication::{PublicationOutcome, QueuePublisher};
use super::recovery::read_queue;
use super::QueueTarget;
use crate::config::{
    ActiveTextMutationLimits, DbConfig, IndexOperationQueueTuning, SearchIndexBatchLimits,
    TextIndexDefinition, VectorIndexDefinition,
};
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{ManagedIndexKey, RecordKind, ScopedKey};
use crate::encoding::v2::values::decode_index_record;
use crate::encoding::v2::values::indexes::operation_queue::{
    OperationQueue, QueueFamily, QueueOperand, QueuedOperation, QueuedPayload,
};
use crate::error::{HelixDbError, IndexBackpressureResource, IndexOperationBatchResource};
use crate::index_lifecycle::work::TextPartition;
use crate::index_lifecycle::ValidatedDynamicIndexDefinition;
use crate::search::vector::VectorDistanceMetric;
use crate::HelixDB;

/// Automatic publication paused, so queues stay observable.
pub(super) fn queued(tuning: IndexOperationQueueTuning) -> DbConfig {
    DbConfig::new().with_index_operation_queue_tuning(tuning.with_publication_paused_for_tests())
}

pub(super) async fn open(name: &str, store: Arc<dyn ObjectStore>, config: DbConfig) -> HelixDB {
    HelixDB::open_with_object_store_and_config(name, store, config)
        .await
        .expect("queued database opens")
}

pub(super) async fn install_vector_and_text(db: &HelixDB) {
    db.install_index_for_tests(
        ValidatedDynamicIndexDefinition::try_from(
            VectorIndexDefinition::new_node("Doc", "embedding", 2, VectorDistanceMetric::Euclidean)
                .unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    db.install_index_for_tests(
        ValidatedDynamicIndexDefinition::try_from(
            TextIndexDefinition::new_node("Doc", "body").unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
}

/// Returns the queue target of the only index with `family` in the default scope.
pub(crate) async fn target(db: &HelixDB, family: QueueFamily) -> QueueTarget {
    let prefix = ManagedIndexKey::data_prefix(
        DataScope::LegacyUnscoped,
        ScopedKey::logical_prefix(RecordKind::IndexRecord),
    );
    let storage = db.inner_db();
    let mut rows = storage.scan_prefix(&prefix, ..).await.unwrap();
    while let Some(row) = rows.next().await.unwrap() {
        let record = decode_index_record(&row.value).unwrap();
        let matches = matches!(
            (record.definition(), family),
            (
                ValidatedDynamicIndexDefinition::Vector(_),
                QueueFamily::Vector
            ) | (ValidatedDynamicIndexDefinition::Text(_), QueueFamily::Text)
        );
        if matches {
            return QueueTarget::new(
                DataScope::LegacyUnscoped,
                record.index_id(),
                record.state().generation(),
            );
        }
    }
    panic!("index family {family:?} is installed");
}

pub(super) async fn all_keys(db: &HelixDB) -> std::collections::BTreeSet<bytes::Bytes> {
    let storage = db.inner_db();
    let mut rows = storage.scan::<std::ops::RangeFull>(..).await.unwrap();
    let mut keys = std::collections::BTreeSet::new();
    while let Some(row) = rows.next().await.unwrap() {
        keys.insert(row.key);
    }
    keys
}

/// Rows of `db` whose key `keep` accepts, by key.
pub(super) async fn rows(
    db: &HelixDB,
    keep: impl Fn(&[u8]) -> bool,
) -> std::collections::BTreeMap<bytes::Bytes, bytes::Bytes> {
    let mut rows = db.inner_db().scan::<std::ops::RangeFull>(..).await.unwrap();
    let mut kept = std::collections::BTreeMap::new();
    while let Some(row) = rows.next().await.unwrap() {
        if keep(&row.key) {
            kept.insert(row.key, row.value);
        }
    }
    kept
}

pub(super) async fn queue(db: &HelixDB, family: QueueFamily) -> Option<OperationQueue> {
    read_queue(db.inner_db().as_ref(), target(db, family).await, family)
        .await
        .unwrap()
}

pub(super) fn vector_of(payload: &QueuedPayload) -> (Option<TextPartition>, Option<Vec<f32>>) {
    let QueuedPayload::Vector(payload) = payload else {
        panic!("vector queue holds vector payloads");
    };
    (
        payload.previous.clone(),
        payload
            .replacement
            .as_ref()
            .map(|replacement| replacement.vector().to_vec()),
    )
}

pub(super) fn text_of(payload: &QueuedPayload) -> Option<String> {
    let QueuedPayload::Text(payload) = payload else {
        panic!("text queue holds text payloads");
    };
    payload
        .replacement
        .as_ref()
        .map(|replacement| replacement.text().to_string())
}

pub(super) async fn add_doc(db: &HelixDB, embedding: Vec<f32>, body: &str) -> crate::Result<u64> {
    let result = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "created",
                    traversal::g().add_n(
                        "Doc",
                        vec![
                            ("embedding", PropertyInput::from(embedding)),
                            ("body", PropertyInput::from(body.to_string())),
                        ],
                    ),
                )
                .returning(["created"]),
        ))
        .await?;
    Ok(result["created"][0]["$id"]
        .as_u64()
        .expect("created node id"))
}

/// A publisher over `db`'s storage and caches with its own limits and schedule.
///
/// It shares the writer publisher's planning cache, as every publisher of one
/// writer must: each attempt then takes its target's retained session out
/// before it plans, whichever publisher committed last.
pub(super) fn publisher_with_limits(
    db: &HelixDB,
    limits: SearchIndexBatchLimits,
    text: ActiveTextMutationLimits,
) -> Arc<QueuePublisher> {
    QueuePublisher::new(
        db.inner_db(),
        Arc::clone(db.index_operation_backlog()),
        Arc::clone(&db.inner.index_queue_store),
        Arc::clone(&db.inner.index_scope_gates),
        super::publication::VectorPublicationResources {
            cache_registry: Arc::clone(&db.inner.caches.vector_memory.registry),
            simhasher_registry: Arc::clone(db.simhasher_registry()),
            batch_reads: db.batch_reads(),
            planning_cache: Arc::clone(
                db.index_queue_publisher()
                    .expect("the writer runs a publisher")
                    .planning_cache(),
            ),
        },
        limits,
        super::publication::TextPublicationResources {
            object_store: Arc::clone(db.object_store()),
            database: db.path().to_string(),
            limits: text,
        },
    )
}

/// Makes `publisher`'s next staged attempt conflict at commit.
///
/// The returned future waits until that attempt reaches its commit, rewrites
/// every default-scope index record with its own bytes (inside the range the
/// attempt read its ownership from), and then releases the attempt. Run it
/// concurrently with the attempt.
pub(super) fn conflict_next_commit(
    db: &HelixDB,
    publisher: &QueuePublisher,
) -> impl std::future::Future<Output = ()> {
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    *publisher.hooks().before_commit.lock() = Some((reached_tx, release_rx));
    async move {
        reached_rx.await.expect("the attempt reaches its commit");
        let storage = db.inner_db();
        let prefix = ManagedIndexKey::data_prefix(
            DataScope::LegacyUnscoped,
            ScopedKey::logical_prefix(RecordKind::IndexRecord),
        );
        let mut rows = storage.scan_prefix(&prefix, ..).await.unwrap();
        let mut records = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            records.push(row);
        }
        assert!(!records.is_empty(), "an index is installed");
        for record in records {
            storage.put(&record.key, &record.value).await.unwrap();
        }
        release_tx
            .send(())
            .expect("the attempt waits for its release");
    }
}

/// Publishes or discards `target` until it is empty, asserting that every
/// committed acknowledgement operand fits the producer operand bound (the
/// WAL entry limit); returns the operations released.
pub(super) async fn release_within_operand_bound(
    db: &HelixDB,
    target: QueueTarget,
    family: QueueFamily,
) -> u64 {
    let publisher = db.index_queue_publisher().expect("writer runs a publisher");
    let queued_ids = || async {
        read_queue(db.inner_db().as_ref(), target, family)
            .await
            .unwrap()
            .map_or_else(BTreeSet::new, |queue| {
                queue
                    .operations()
                    .iter()
                    .map(QueuedOperation::id)
                    .collect::<BTreeSet<_>>()
            })
    };
    let mut released = 0;
    for _ in 0..1_000 {
        let before = queued_ids().await;
        let operations = match publisher.publish_once(target).await.unwrap() {
            PublicationOutcome::Published { operations, .. }
            | PublicationOutcome::Discarded { operations } => operations,
            PublicationOutcome::Trimmed => continue,
            PublicationOutcome::Empty => return released,
            outcome @ (PublicationOutcome::Deferred
            | PublicationOutcome::Retry
            | PublicationOutcome::Blocked) => panic!("publication did not progress: {outcome:?}"),
        };
        let acknowledged = before
            .difference(&queued_ids().await)
            .copied()
            .collect::<Vec<_>>();
        assert_eq!(acknowledged.len() as u64, operations);
        let operand = QueueOperand::acknowledge(family, acknowledged).unwrap();
        assert!(
            operand.bytes().len() as u64 <= db.index_operand_limit(),
            "a {}-byte acknowledgement exceeds the {}-byte operand bound",
            operand.bytes().len(),
            db.index_operand_limit()
        );
        released += operations;
    }
    panic!("publication did not drain")
}

pub(super) async fn node_count(db: &HelixDB) -> u64 {
    db.query(QueryRequest::read(
        batch::read_batch()
            .var_as("count", traversal::g().n(NodeRef::all()).count())
            .returning(["count"]),
    ))
    .await
    .unwrap()["count"]
        .as_u64()
        .unwrap()
}

#[tokio::test]
async fn graph_rows_and_complete_operations_commit_together_without_physical_rows() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "queue-atomic",
        store,
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector_and_text(&db).await;

    let before = all_keys(&db).await;
    let id = add_doc(&db, vec![1.0, 2.0], "hello queued world")
        .await
        .unwrap();
    // Only graph rows and queue keys appeared: no HNSW, manifest, entity-state,
    // statistics, or build-delta rows were staged in the foreground.
    for key in all_keys(&db).await.difference(&before) {
        assert_ne!(key.first(), Some(&0xF0), "physical vector row {key:?}");
        if let Ok(ManagedIndexKey::Data { kind, .. }) = ManagedIndexKey::parse_data_from_slice(key)
        {
            assert!(
                matches!(kind, ScopedKey::IndexOperationQueue(_)),
                "unexpected lifecycle row {kind:?}"
            );
        }
    }
    let vectors = queue(&db, QueueFamily::Vector)
        .await
        .expect("vector work queued");
    assert_eq!(vectors.operations().len(), 1);
    let operation = &vectors.operations()[0];
    assert_eq!(operation.entity().id.get(), id);
    assert_eq!(
        vector_of(operation.payload()),
        (None, Some(vec![1.0, 2.0])),
        "an insert has no previous routing and carries the exact vector"
    );
    let texts = queue(&db, QueueFamily::Text)
        .await
        .expect("text work queued");
    assert_eq!(texts.operations().len(), 1);
    assert_eq!(
        text_of(texts.operations()[0].payload()).as_deref(),
        Some("hello queued world")
    );
    assert_eq!(node_count(&db).await, 1);

    let vector_target = target(&db, QueueFamily::Vector).await;
    let usage = db
        .index_operation_backlog()
        .usage(DataScope::LegacyUnscoped, vector_target.index_id);
    assert_eq!(usage.operations, 1);
    assert_eq!(usage.members, 1);
    assert_eq!(usage.retained_bytes, operation.retained_bytes());
    db.close().await.unwrap();
}

#[tokio::test]
async fn one_transaction_collapses_but_committed_updates_stay_ordered() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "queue-order",
        store,
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector_and_text(&db).await;

    // Create and update in one transaction: one operation with the final state.
    let result = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "created",
                    traversal::g().add_n(
                        "Doc",
                        vec![
                            ("embedding", PropertyInput::from(vec![1.0_f32, 0.0])),
                            ("body", PropertyInput::from("first".to_string())),
                        ],
                    ),
                )
                .var_as(
                    "updated",
                    traversal::g()
                        .n(NodeRef::Var("created".to_string()))
                        .set_property("embedding", vec![0.0_f32, 1.0])
                        .set_property("body", "second".to_string()),
                )
                .returning(["created"]),
        ))
        .await
        .unwrap();
    let id = result["created"][0]["$id"].as_u64().unwrap();
    let vectors = queue(&db, QueueFamily::Vector).await.unwrap();
    assert_eq!(vectors.operations().len(), 1);
    assert_eq!(
        vector_of(vectors.operations()[0].payload()),
        (None, Some(vec![0.0, 1.0]))
    );

    // Separate committed updates retain every operation in commit order.
    for (vector, body) in [(vec![2.0_f32, 2.0], "third"), (vec![2.0, 2.0], "fourth")] {
        db.query(QueryRequest::write(
            batch::write_batch().var_as(
                "updated",
                traversal::g()
                    .n(NodeRef::from(id))
                    .set_property("embedding", vector)
                    .set_property("body", body.to_string()),
            ),
        ))
        .await
        .unwrap();
    }
    let vectors = queue(&db, QueueFamily::Vector).await.unwrap();
    // The identical-vector update produced no new vector operation.
    assert_eq!(
        vectors
            .operations()
            .iter()
            .map(|operation| vector_of(operation.payload()))
            .collect::<Vec<_>>(),
        vec![
            (None, Some(vec![0.0, 1.0])),
            (Some(TextPartition::Unpartitioned), Some(vec![2.0, 2.0])),
        ]
    );
    let texts = queue(&db, QueueFamily::Text).await.unwrap();
    assert_eq!(
        texts
            .operations()
            .iter()
            .map(|operation| text_of(operation.payload()))
            .collect::<Vec<_>>(),
        vec![
            Some("second".to_string()),
            Some("third".to_string()),
            Some("fourth".to_string())
        ]
    );

    // Removing the property and deleting the entity both enqueue deletions.
    db.query(QueryRequest::write(
        batch::write_batch().var_as(
            "removed",
            traversal::g()
                .n(NodeRef::from(id))
                .remove_property("embedding"),
        ),
    ))
    .await
    .unwrap();
    db.query(QueryRequest::write(
        batch::write_batch().var_as("dropped", traversal::g().n(NodeRef::from(id)).drop()),
    ))
    .await
    .unwrap();
    let vectors = queue(&db, QueueFamily::Vector).await.unwrap();
    assert_eq!(
        vector_of(vectors.operations().last().unwrap().payload()),
        (Some(TextPartition::Unpartitioned), None)
    );
    assert_eq!(vectors.operations().len(), 3, "the drop saw no vector left");
    let texts = queue(&db, QueueFamily::Text).await.unwrap();
    assert_eq!(text_of(texts.operations().last().unwrap().payload()), None);
    assert_eq!(texts.operations().len(), 4);
    // Every retained operation still counts toward the byte limit.
    let text_target = target(&db, QueueFamily::Text).await;
    let usage = db
        .index_operation_backlog()
        .usage(DataScope::LegacyUnscoped, text_target.index_id);
    assert_eq!(usage.operations, 4);
    assert_eq!(usage.members, 1);
    assert_eq!(
        usage.retained_bytes,
        texts
            .operations()
            .iter()
            .map(|operation| operation.retained_bytes())
            .sum::<u64>()
    );
    db.close().await.unwrap();
}

pub(super) async fn install_text(db: &HelixDB) {
    db.install_index_for_tests(
        ValidatedDynamicIndexDefinition::try_from(
            TextIndexDefinition::new_node("Doc", "body").unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
}

pub(super) async fn add_text(db: &HelixDB, body: &str) -> crate::Result<serde_json::Value> {
    db.query(QueryRequest::write(batch::write_batch().var_as(
        "created",
        traversal::g().add_n("Doc", vec![("body", PropertyInput::from(body.to_string()))]),
    )))
    .await
}

#[tokio::test]
async fn member_backpressure_aborts_the_whole_write_at_the_exact_boundary() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let tuning = IndexOperationQueueTuning::default().with_max_members(NonZeroU64::new(2).unwrap());
    let db = open("queue-members", store, queued(tuning)).await;
    install_text(&db).await;
    add_text(&db, "one").await.unwrap();
    add_text(&db, "two")
        .await
        .expect("the limit itself is admitted");
    let error = add_text(&db, "three")
        .await
        .expect_err("one member too many");
    assert!(matches!(
        error,
        HelixDbError::IndexBackpressure {
            resource: IndexBackpressureResource::PendingMembers,
            requested: 3,
            limit: 2,
            ..
        }
    ));
    assert!(error.is_index_backpressure());
    assert_eq!(
        node_count(&db).await,
        2,
        "the rejected graph write rolled back"
    );
    assert_eq!(
        queue(&db, QueueFamily::Text)
            .await
            .unwrap()
            .operations()
            .len(),
        2
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_write_over_the_member_limit_on_its_own_fails_without_retry() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let tuning = IndexOperationQueueTuning::default().with_max_members(NonZeroU64::new(2).unwrap());
    let db = open("queue-members-alone", store, queued(tuning)).await;
    install_text(&db).await;
    let write = ["one", "two", "three"]
        .into_iter()
        .fold(batch::write_batch(), |write, body| {
            write.var_as(
                body,
                traversal::g().add_n("Doc", vec![("body", PropertyInput::from(body.to_string()))]),
            )
        });
    let error = db
        .query(QueryRequest::write(write))
        .await
        .expect_err("three members never fit a limit of two");
    assert!(matches!(
        error,
        HelixDbError::IndexOperationBatchTooLarge {
            resource: IndexOperationBatchResource::PendingMembers,
            observed: 3,
            limit: 2,
            ..
        }
    ));
    assert!(!error.is_index_backpressure(), "{error}");
    assert!(error.is_invalid_input(), "{error}");
    assert_eq!(node_count(&db).await, 0, "the graph write rolled back");
    // Nothing stayed charged: writes within the limit are admitted.
    add_text(&db, "one").await.unwrap();
    add_text(&db, "two").await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn byte_backpressure_accepts_the_limit_and_rejects_one_byte_more() {
    // Node IDs 0 and 1 with a one-character text each retain 24 bytes:
    // mode(1) + id(16) + body_len(1) + body(kind 1, id 1, some 1,
    // partition 1, len 1, text 1).
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let tuning = IndexOperationQueueTuning::default()
        .with_max_retained_bytes(NonZeroU64::new(48).unwrap())
        .unwrap();
    let db = open("queue-bytes", store, queued(tuning)).await;
    install_text(&db).await;
    add_text(&db, "a").await.unwrap();
    add_text(&db, "b")
        .await
        .expect("exactly 48 bytes is admitted");
    let text_target = target(&db, QueueFamily::Text).await;
    assert_eq!(
        db.index_operation_backlog()
            .usage(DataScope::LegacyUnscoped, text_target.index_id)
            .retained_bytes,
        48
    );
    let error = add_text(&db, "c").await.expect_err("above the byte limit");
    assert!(matches!(
        error,
        HelixDbError::IndexBackpressure {
            resource: IndexBackpressureResource::RetainedBytes,
            requested: 72,
            limit: 48,
            ..
        }
    ));
    assert_eq!(node_count(&db).await, 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn oversized_operands_fail_before_commit_without_leaking_capacity() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let tuning =
        IndexOperationQueueTuning::default().with_max_operand_bytes(NonZeroU64::new(64).unwrap());
    let db = open("queue-operand", store, queued(tuning)).await;
    install_text(&db).await;
    let error = add_text(&db, &"x".repeat(128))
        .await
        .expect_err("operand too large");
    assert!(matches!(
        error,
        HelixDbError::IndexOperationBatchTooLarge { limit: 64, .. }
    ));
    assert!(error.is_invalid_input());
    assert_eq!(node_count(&db).await, 0);
    let text_target = target(&db, QueueFamily::Text).await;
    assert_eq!(
        db.index_operation_backlog()
            .usage(DataScope::LegacyUnscoped, text_target.index_id)
            .operations,
        0
    );
    add_text(&db, "small").await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn reopening_rebuilds_exact_accounting_before_graph_writes() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let tuning = IndexOperationQueueTuning::default().with_max_members(NonZeroU64::new(3).unwrap());
    let db = open("queue-restart", Arc::clone(&store), queued(tuning)).await;
    install_vector_and_text(&db).await;
    add_doc(&db, vec![1.0, 0.0], "a").await.unwrap();
    let second = add_doc(&db, vec![0.0, 1.0], "b").await.unwrap();
    db.query(QueryRequest::write(
        batch::write_batch().var_as(
            "updated",
            traversal::g()
                .n(NodeRef::from(second))
                .set_property("body", "b2".to_string()),
        ),
    ))
    .await
    .unwrap();
    let text_target = target(&db, QueueFamily::Text).await;
    let before = db
        .index_operation_backlog()
        .usage(DataScope::LegacyUnscoped, text_target.index_id);
    assert_eq!(before.operations, 3);
    assert_eq!(before.members, 2);
    db.close().await.unwrap();

    let reopened = open("queue-restart", store, queued(tuning)).await;
    assert_eq!(
        reopened
            .index_operation_backlog()
            .usage(DataScope::LegacyUnscoped, text_target.index_id),
        before
    );
    // The rebuilt member count still enforces the limit exactly.
    add_doc(&reopened, vec![1.0, 1.0], "c").await.unwrap();
    assert!(add_doc(&reopened, vec![1.0, 1.0], "d")
        .await
        .unwrap_err()
        .is_index_backpressure());
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn concurrent_writers_to_distinct_entities_commit_without_queue_conflicts() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        open(
            "queue-concurrent",
            store,
            queued(IndexOperationQueueTuning::default()),
        )
        .await,
    );
    install_vector_and_text(&db).await;
    let mut tasks = tokio::task::JoinSet::new();
    for writer in 0..16_u8 {
        let db = Arc::clone(&db);
        tasks.spawn(async move {
            add_doc(
                &db,
                vec![f32::from(writer), 1.0],
                &format!("writer {writer}"),
            )
            .await
        });
    }
    while let Some(result) = tasks.join_next().await {
        result
            .unwrap()
            .expect("distinct entities never conflict on the queue");
    }
    assert_eq!(
        queue(&db, QueueFamily::Vector)
            .await
            .unwrap()
            .operations()
            .len(),
        16
    );
    assert_eq!(
        queue(&db, QueueFamily::Text)
            .await
            .unwrap()
            .operations()
            .len(),
        16
    );
    Arc::into_inner(db).unwrap().close().await.unwrap();
}
