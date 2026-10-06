//! Strong and eventual pending-data search overlays through public queries.

use std::num::NonZeroU64;
use std::sync::Arc;

use helix_ast::{
    batch, expr,
    graph::NodeRef,
    query::{QueryRequest, SearchConsistency},
    traversal,
    value::{PropertyInput, PropertyValue},
};
use helix_planner::{context, exec, planning};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

use super::publication::PublicationOutcome;
use super::tests::{open, publisher_with_limits, queue, queued, target};
use super::QueueTarget;
use crate::config::{
    DbConfig, IndexOperationQueueTuning, SearchIndexBackfillLimits, SearchIndexBatchLimits,
    SecondaryIndexDefinition, TextBackfillCompactionLimits, TextIndexDefinition,
    VectorIndexDefinition,
};
use crate::encoding::v2::values::indexes::operation_queue::{
    OperationQueue, QueueFamily, QUEUE_VALUE_KIND,
};
use crate::index_lifecycle::ValidatedDynamicIndexDefinition;
use crate::search::vector::VectorDistanceMetric;
use crate::HelixDB;

pub(super) async fn install(db: &HelixDB, tenant: Option<&str>) {
    let vector =
        VectorIndexDefinition::new_node("Doc", "embedding", 2, VectorDistanceMetric::Euclidean)
            .unwrap();
    let text = TextIndexDefinition::new_node("Doc", "body").unwrap();
    let (vector, text) = match tenant {
        Some(tenant) => (
            vector.with_tenant_property(tenant).unwrap(),
            text.with_tenant_property(tenant).unwrap(),
        ),
        None => (vector, text),
    };
    for definition in [
        ValidatedDynamicIndexDefinition::try_from(vector).unwrap(),
        ValidatedDynamicIndexDefinition::try_from(text).unwrap(),
    ] {
        db.install_index_for_tests(definition).await.unwrap();
    }
}

pub(super) async fn write(db: &HelixDB, request: impl Fn() -> QueryRequest) -> serde_json::Value {
    for _ in 0..100 {
        match Box::pin(db.query(request())).await {
            Ok(result) => return result,
            Err(error) if error.is_transaction_conflict() => {}
            Err(error) => panic!("write failed: {error}"),
        }
    }
    panic!("write kept conflicting")
}

pub(super) async fn add(
    db: &HelixDB,
    embedding: [f32; 2],
    body: &str,
    tenant: Option<&str>,
) -> u64 {
    let result = write(db, || {
        let mut properties = vec![
            ("embedding", PropertyInput::from(embedding.to_vec())),
            ("body", PropertyInput::from(body.to_string())),
        ];
        if let Some(tenant) = tenant {
            properties.push(("tenant", PropertyInput::from(tenant.to_string())));
        }
        QueryRequest::write(
            batch::write_batch()
                .var_as("created", traversal::g().add_n("Doc", properties))
                .returning(["created"]),
        )
    })
    .await;
    result["created"][0]["$id"].as_u64().unwrap()
}

pub(super) async fn update(db: &HelixDB, id: u64, embedding: [f32; 2], body: &str) {
    write(db, || {
        QueryRequest::write(
            batch::write_batch().var_as(
                "updated",
                traversal::g()
                    .n(NodeRef::from(id))
                    .set_property("embedding", embedding.to_vec())
                    .set_property("body", body.to_string()),
            ),
        )
    })
    .await;
}

pub(super) async fn delete(db: &HelixDB, id: u64) {
    write(db, || {
        QueryRequest::write(
            batch::write_batch().var_as("dropped", traversal::g().n(NodeRef::from(id)).drop()),
        )
    })
    .await;
}

pub(super) fn hits(result: &serde_json::Value, name: &str) -> Vec<(u64, u64)> {
    if result[name].is_null() {
        return Vec::new();
    }
    result[name]
        .as_array()
        .unwrap_or_else(|| panic!("search returned {result}"))
        .iter()
        .map(|hit| {
            (
                hit["$id"].as_u64().unwrap(),
                hit["$score"]
                    .as_f64()
                    .or_else(|| hit["$distance"].as_f64())
                    .map_or(0, f64::to_bits),
            )
        })
        .collect()
}

pub(super) async fn vector_search(
    db: &HelixDB,
    query: [f32; 2],
    k: usize,
    tenant: Option<&str>,
    consistency: SearchConsistency,
) -> Vec<(u64, u64)> {
    let request = QueryRequest::read(
        batch::read_batch()
            .var_as(
                "hits",
                traversal::g().vector_search_nodes(
                    "Doc",
                    "embedding",
                    query.to_vec(),
                    k,
                    tenant.map(PropertyValue::from),
                ),
            )
            .returning(["hits"]),
    )
    .with_search_consistency(consistency)
    .unwrap();
    hits(&Box::pin(db.query(request)).await.unwrap(), "hits")
}

pub(super) async fn text_search(
    db: &HelixDB,
    query: &str,
    k: usize,
    tenant: Option<&str>,
    consistency: SearchConsistency,
) -> Vec<(u64, u64)> {
    let request = QueryRequest::read(
        batch::read_batch()
            .var_as(
                "hits",
                traversal::g().text_search_nodes(
                    "Doc",
                    "body",
                    query,
                    k,
                    tenant.map(PropertyValue::from),
                ),
            )
            .returning(["hits"]),
    )
    .with_search_consistency(consistency)
    .unwrap();
    hits(&Box::pin(db.query(request)).await.unwrap(), "hits")
}

pub(super) async fn drain(db: &HelixDB, target: QueueTarget) {
    let publisher = db.index_queue_publisher().unwrap();
    for _ in 0..1_000 {
        match publisher.publish_once(target).await.unwrap() {
            PublicationOutcome::Empty => return,
            PublicationOutcome::Published { .. } | PublicationOutcome::Trimmed => {}
            outcome @ (PublicationOutcome::Discarded { .. }
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry
            | PublicationOutcome::Blocked
            | PublicationOutcome::Stalled) => panic!("publication stalled: {outcome:?}"),
        }
    }
}

fn ordinals(found: Vec<(u64, u64)>, ids: &[u64]) -> Vec<(usize, u64)> {
    found
        .into_iter()
        .map(|(id, score)| {
            (
                ids.iter()
                    .position(|candidate| *candidate == id)
                    .expect("hit is a workload entity"),
                score,
            )
        })
        .collect()
}

/// A deterministic workload of inserts, replacements that stop matching,
/// moves, empty text, and deletions.
async fn workload(db: &HelixDB) -> Vec<u64> {
    let mut ids = Vec::new();
    for (index, body) in [
        "rust storage engine",
        "rust planner",
        "graph storage storage",
        "",
        "vector search engine",
        "text search with rust",
        "rust rust rust",
        "unrelated words",
    ]
    .into_iter()
    .enumerate()
    {
        let index = index as f32;
        ids.push(add(db, [index, index * 0.5], body, None).await);
    }
    update(db, ids[1], [9.0, 9.0], "python planner").await;
    update(db, ids[3], [0.1, 0.1], "late rust arrival").await;
    delete(db, ids[4]).await;
    update(db, ids[6], [3.0, 3.1], "rust").await;
    ids
}

const TEXT_QUERIES: [&str; 5] = ["rust", "storage", "planner", "search engine", "arrival"];
const VECTOR_QUERIES: [[f32; 2]; 3] = [[0.0, 0.0], [3.0, 3.0], [9.0, 9.0]];

async fn assert_matches_reference(
    db: &HelixDB,
    ids: &[u64],
    reference: &HelixDB,
    reference_ids: &[u64],
) {
    for query in TEXT_QUERIES {
        assert_eq!(
            ordinals(
                text_search(db, query, 10, None, SearchConsistency::Strong).await,
                ids
            ),
            ordinals(
                text_search(reference, query, 10, None, SearchConsistency::Strong).await,
                reference_ids
            ),
            "text query {query:?}"
        );
    }
    for query in VECTOR_QUERIES {
        assert_eq!(
            ordinals(
                vector_search(db, query, 10, None, SearchConsistency::Strong).await,
                ids
            ),
            ordinals(
                vector_search(reference, query, 10, None, SearchConsistency::Strong).await,
                reference_ids
            ),
            "vector query {query:?}"
        );
    }
}

#[tokio::test]
async fn strong_search_equals_an_independent_build_before_and_after_partial_publication() {
    // Reference: an initial build over the workload's final graph state, so
    // its index rows never pass through the queue.
    let reference = open(
        "overlay-reference",
        Arc::new(InMemory::new()),
        DbConfig::new(),
    )
    .await;
    let reference_ids = workload(&reference).await;
    install(&reference, None).await;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "overlay-strong",
        store,
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&db, None).await;
    let ids = workload(&db).await;

    // Nothing published: every result comes from the pending overlay, scored
    // against statistics derived from pending documents alone.
    Box::pin(assert_matches_reference(
        &db,
        &ids,
        &reference,
        &reference_ids,
    ))
    .await;

    // Publish the vector queue only; text stays pending and vectors mix.
    drain(&db, target(&db, QueueFamily::Vector).await).await;
    Box::pin(assert_matches_reference(
        &db,
        &ids,
        &reference,
        &reference_ids,
    ))
    .await;

    // Further committed changes after publication are overlaid on physical data.
    update(&db, ids[0], [8.0, 8.0], "storage moved").await;
    update(&reference, reference_ids[0], [8.0, 8.0], "storage moved").await;
    delete(&db, ids[2]).await;
    delete(&reference, reference_ids[2]).await;
    drain(&db, target(&db, QueueFamily::Text).await).await;
    update(&db, ids[5], [5.0, 5.0], "rust search").await;
    update(&reference, reference_ids[5], [5.0, 5.0], "rust search").await;
    Box::pin(assert_matches_reference(
        &db,
        &ids,
        &reference,
        &reference_ids,
    ))
    .await;
    reference.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn eventual_search_selects_complete_entities_within_its_budget() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    // Probe the exact retained size of one small text operation, then allow
    // exactly one pending entity per eventual search.
    let db = open(
        "overlay-eventual",
        store,
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&db, None).await;
    let first = add(&db, [0.0, 0.0], "alpha", None).await;
    let text_bytes = queue(&db, QueueFamily::Text).await.unwrap().operations()[0].retained_bytes();
    let vector_bytes =
        queue(&db, QueueFamily::Vector).await.unwrap().operations()[0].retained_bytes();
    db.close().await.unwrap();

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let tuning = IndexOperationQueueTuning::default()
        .with_eventual_search_budget_for_tests(text_bytes.max(vector_bytes));
    let db = open("overlay-eventual-bounded", store, queued(tuning)).await;
    install(&db, None).await;
    let first_id = add(&db, [0.0, 0.0], "alpha", None).await;
    assert_eq!(first_id, first);
    let second = add(&db, [0.1, 0.0], "alpha", None).await;
    let strong = text_search(&db, "alpha", 10, None, SearchConsistency::Strong).await;
    assert_eq!(strong.len(), 2);
    // Eventual selects the oldest complete entity and leaves the second
    // subject to eventual visibility.
    let eventual = text_search(&db, "alpha", 10, None, SearchConsistency::Eventual).await;
    assert_eq!(
        eventual.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![first_id]
    );
    let eventual = vector_search(&db, [0.0, 0.0], 10, None, SearchConsistency::Eventual).await;
    assert_eq!(
        eventual.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![first_id]
    );

    // Each search in a multi-search request gets its own budget.
    let request = QueryRequest::read(
        batch::read_batch()
            .var_as(
                "text",
                traversal::g().text_search_nodes("Doc", "body", "alpha", 10, None),
            )
            .var_as(
                "vector",
                traversal::g().vector_search_nodes("Doc", "embedding", vec![0.0, 0.0], 10, None),
            )
            .returning(["text", "vector"]),
    )
    .with_search_consistency(SearchConsistency::Eventual)
    .unwrap();
    let result = db.query(request).await.unwrap();
    assert_eq!(hits(&result, "text").len(), 1);
    assert_eq!(hits(&result, "vector").len(), 1);

    // Once published, eventual sees everything physically.
    drain(&db, target(&db, QueueFamily::Text).await).await;
    drain(&db, target(&db, QueueFamily::Vector).await).await;
    let eventual = text_search(&db, "alpha", 10, None, SearchConsistency::Eventual).await;
    assert_eq!(eventual.len(), 2);
    assert!(eventual.iter().any(|(id, _)| *id == second));
    db.close().await.unwrap();
}

#[tokio::test]
async fn write_batch_searches_see_their_own_uncommitted_changes() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "overlay-local",
        store,
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&db, None).await;
    let committed = add(&db, [5.0, 5.0], "committed words", None).await;
    let result = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "created",
                    traversal::g().add_n(
                        "Doc",
                        vec![
                            ("embedding", PropertyInput::from(vec![0.0_f32, 0.0])),
                            ("body", PropertyInput::from("local words".to_string())),
                        ],
                    ),
                )
                .var_as(
                    "moved",
                    traversal::g()
                        .n(NodeRef::from(committed))
                        .set_property("embedding", vec![0.5_f32, 0.5])
                        .set_property("body", "moved elsewhere".to_string()),
                )
                .var_as(
                    "vector_hits",
                    traversal::g().vector_search_nodes(
                        "Doc",
                        "embedding",
                        vec![0.0, 0.0],
                        10,
                        None,
                    ),
                )
                .var_as(
                    "text_hits",
                    traversal::g().text_search_nodes("Doc", "body", "words", 10, None),
                )
                .returning(["created", "vector_hits", "text_hits"]),
        ))
        .await
        .unwrap();
    let created = result["created"][0]["$id"].as_u64().unwrap();
    assert_eq!(
        hits(&result, "vector_hits")
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>(),
        vec![created, committed],
        "the uncommitted insert and update are ranked in the same transaction"
    );
    assert_eq!(
        hits(&result, "text_hits")
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>(),
        vec![created],
        "the replacement that stopped matching is suppressed"
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn pending_only_tenant_partitions_and_traversal_restrictions_apply() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "overlay-tenant",
        store,
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&db, Some("tenant")).await;
    let a = add(&db, [1.0, 1.0], "shared words", Some("a")).await;
    let b = add(&db, [1.0, 1.1], "shared words", Some("b")).await;
    let a2 = add(&db, [2.0, 2.0], "shared other", Some("a")).await;
    // No physical partition exists yet; strong search serves the pending one.
    assert_eq!(
        vector_search(&db, [1.0, 1.0], 10, Some("a"), SearchConsistency::Strong)
            .await
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>(),
        vec![a, a2]
    );
    assert_eq!(
        text_search(&db, "shared", 10, Some("b"), SearchConsistency::Strong)
            .await
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>(),
        vec![b]
    );
    // A traversal-scoped search only ranks pending entities it may see.
    let result = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "hits",
                    traversal::g().n(NodeRef::from(a2)).vector_search(
                        "Doc",
                        "embedding",
                        vec![1.0, 1.0],
                        10,
                        Some(PropertyValue::from("a")),
                    ),
                )
                .returning(["hits"]),
        ))
        .await
        .unwrap();
    assert_eq!(
        hits(&result, "hits")
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>(),
        vec![a2]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn eventual_consistency_is_rejected_for_write_requests() {
    let write = QueryRequest::write(
        batch::write_batch().var_as("created", traversal::g().add_n("Doc", vec![("x", "y")])),
    );
    assert!(write
        .with_search_consistency(SearchConsistency::Eventual)
        .is_err());
    let json = r#"{"request_type":"write","query_name":null,"query":{"write":{"entries":[],"returns":[]}},"search_consistency":"eventual"}"#;
    assert!(sonic_rs::from_str::<QueryRequest>(json).is_err());
    let json = r#"{"request_type":"read","query_name":null,"query":{"read":{"entries":[],"returns":[]}},"search_consistency":"strong"}"#;
    let parsed = sonic_rs::from_str::<QueryRequest>(json).unwrap();
    assert_eq!(parsed.search_consistency(), SearchConsistency::Strong);
    assert!(!parsed
        .to_json_string()
        .unwrap()
        .contains("search_consistency"));
}

#[tokio::test]
async fn readers_overlay_pending_operations_from_their_own_snapshot() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = open(
        "queue-reader-overlay",
        Arc::clone(&store),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&writer, None).await;
    let first = add(&writer, [1.0, 1.0], "reader visible", None).await;
    add(&writer, [2.0, 2.0], "reader other", None).await;
    let reader = HelixDB::open_reader_with_object_store_for_tests(
        "queue-reader-overlay",
        Arc::clone(&store),
    )
    .await
    .unwrap();
    // The writer's own overlays are the reference for the reader's.
    let agree = |label: &'static str| {
        let (writer, reader) = (&writer, &reader);
        async move {
            for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
                for query in [[1.0, 1.0], [5.0, 5.0]] {
                    assert_eq!(
                        vector_search(reader, query, 10, None, consistency).await,
                        vector_search(writer, query, 10, None, consistency).await,
                        "{label}: vector {query:?} {consistency:?}"
                    );
                }
                for term in ["reader", "visible", "moved"] {
                    assert_eq!(
                        text_search(reader, term, 10, None, consistency).await,
                        text_search(writer, term, 10, None, consistency).await,
                        "{label}: text {term} {consistency:?}"
                    );
                }
            }
        }
    };
    let converge = |label: &'static str| {
        let (writer, reader) = (&writer, &reader);
        async move {
            // The reader replays the writer's WAL on its own schedule.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while vector_search(reader, [5.0, 5.0], 10, None, SearchConsistency::Strong).await
                != vector_search(writer, [5.0, 5.0], 10, None, SearchConsistency::Strong).await
                || text_search(reader, "moved", 10, None, SearchConsistency::Strong).await
                    != text_search(writer, "moved", 10, None, SearchConsistency::Strong).await
            {
                assert!(
                    std::time::Instant::now() < deadline,
                    "{label}: reader never caught up"
                );
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
    };

    // Everything is pending: both overlays come from the reader's snapshot.
    assert_eq!(writer.index_operation_queue_stats().pending_operations, 4);
    agree("pending").await;
    assert_eq!(
        vector_search(&reader, [1.0, 1.0], 10, None, SearchConsistency::Strong)
            .await
            .len(),
        2
    );
    let stats = reader.index_operation_queue_stats();
    assert_eq!(
        (stats.pending_operations, stats.committed_operations),
        (0, 0),
        "readers own no admission ledger"
    );

    // Published work is served physically, never twice.
    drain(&writer, target(&writer, QueueFamily::Vector).await).await;
    drain(&writer, target(&writer, QueueFamily::Text).await).await;
    writer.flush_writer().await.unwrap();
    converge("published").await;
    agree("published").await;
    assert_eq!(
        vector_search(&reader, [1.0, 1.0], 10, None, SearchConsistency::Strong)
            .await
            .len(),
        2
    );

    // A pending update suppresses the published representation.
    update(&writer, first, [5.0, 5.0], "moved").await;
    writer.flush_writer().await.unwrap();
    converge("updated").await;
    agree("updated").await;
    let near = vector_search(&reader, [5.0, 5.0], 1, None, SearchConsistency::Strong).await;
    assert_eq!(near.iter().map(|(id, _)| *id).collect::<Vec<_>>(), [first]);
    assert!(
        text_search(&reader, "visible", 10, None, SearchConsistency::Strong)
            .await
            .is_empty(),
        "the replaced body no longer matches"
    );
    reader.close().await.unwrap();
    writer.close().await.unwrap();
}

/// Adds one `Doc` per embedding (all bodies `"alpha"`) in batched writes and
/// returns their IDs in input order.
pub(super) async fn add_many(
    db: &HelixDB,
    embeddings: impl IntoIterator<Item = [f32; 2]>,
) -> Vec<u64> {
    let embeddings = embeddings.into_iter().collect::<Vec<_>>();
    let mut ids = Vec::with_capacity(embeddings.len());
    for chunk in embeddings.chunks(100) {
        let result = write(db, || {
            let batch =
                chunk
                    .iter()
                    .enumerate()
                    .fold(batch::write_batch(), |batch, (index, embedding)| {
                        batch.var_as(
                            &format!("d{index}"),
                            traversal::g().add_n(
                                "Doc",
                                vec![
                                    ("embedding", PropertyInput::from(embedding.to_vec())),
                                    ("body", PropertyInput::from("alpha".to_string())),
                                ],
                            ),
                        )
                    });
            QueryRequest::write(batch.returning((0..chunk.len()).map(|index| format!("d{index}"))))
        })
        .await;
        ids.extend(
            (0..chunk.len()).map(|index| result[format!("d{index}")][0]["$id"].as_u64().unwrap()),
        );
    }
    ids
}

/// Deletes `ids` in batched writes.
async fn delete_many(db: &HelixDB, ids: &[u64]) {
    for chunk in ids.chunks(100) {
        write(db, || {
            QueryRequest::write(chunk.iter().enumerate().fold(
                batch::write_batch(),
                |batch, (index, id)| {
                    batch.var_as(
                        &format!("d{index}"),
                        traversal::g().n(NodeRef::from(*id)).drop(),
                    )
                },
            ))
        })
        .await;
    }
}

#[tokio::test]
async fn restricted_search_at_its_result_cap_excludes_superseded_candidates() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "overlay-restricted-cap",
        store,
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&db, None).await;
    // 801 traversal candidates on a line; the restricted cap is 800 results.
    let ids = add_many(&db, (0..801).map(|index| [index as f32, 0.0])).await;
    drain(&db, target(&db, QueueFamily::Vector).await).await;
    // One pending re-embedding supersedes the nearest physical row.
    update(&db, ids[0], [0.5, 0.0], "alpha").await;

    let request = |k: usize| {
        QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "hits",
                    traversal::g().n(NodeRef::from(ids.clone())).vector_search(
                        "Doc",
                        "embedding",
                        vec![0.0, 0.0],
                        k,
                        None,
                    ),
                )
                .returning(["hits"]),
        )
    };
    let found = hits(
        &db.query(request(800))
            .await
            .expect("a restricted search within its cap succeeds while work is pending"),
        "hits",
    );
    assert_eq!(
        found.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        ids[..800],
        "the farthest candidate is the only one left out"
    );
    assert_eq!(
        found[0].1,
        0.25_f64.to_bits(),
        "the pending vector, not the superseded row, is scored"
    );
    // Excluding pending entities never lifts the cap of the caller's own
    // candidate set.
    let error = db
        .query(request(801))
        .await
        .expect_err("801 effective results exceed the restricted cap");
    assert!(
        error
            .to_string()
            .contains("restricted vector search result count must be at most 800, got 801"),
        "{error}"
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn overlay_widening_is_bounded_and_fails_explicitly_past_its_suppression_limit() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "overlay-widening-bound",
        store,
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&db, None).await;
    // Identical bodies tie on BM25, so text ranks them by ID like the line
    // ranks them by distance.
    let ids = add_many(&db, (0..802).map(|index| [index as f32, 0.0])).await;
    drain(&db, target(&db, QueueFamily::Vector).await).await;
    drain(&db, target(&db, QueueFamily::Text).await).await;
    let nearest = |found: Vec<(u64, u64)>| found.into_iter().map(|(id, _)| id).collect::<Vec<_>>();

    // Re-embedded and rewritten neighbours are answered by their exact
    // pending scores, so superseding more results than the bound does not
    // widen past it. The repeated term outranks every physical hit.
    for (chunk_index, chunk) in ids[..801].chunks(100).enumerate() {
        write(&db, || {
            QueryRequest::write(chunk.iter().enumerate().fold(
                batch::write_batch(),
                |batch, (index, id)| {
                    let position = (chunk_index * 100 + index) as f32 + 0.25;
                    batch.var_as(
                        &format!("d{index}"),
                        traversal::g()
                            .n(NodeRef::from(*id))
                            .set_property("embedding", vec![position, 0.0])
                            .set_property("body", "alpha alpha".to_string()),
                    )
                },
            ))
        })
        .await;
    }
    let pending_vector = vector_search(&db, [0.0, 0.0], 1, None, SearchConsistency::Strong).await;
    let pending_text = text_search(&db, "alpha", 1, None, SearchConsistency::Strong).await;
    assert_eq!(nearest(pending_vector.clone()), [ids[0]]);
    assert_eq!(nearest(pending_text.clone()), [ids[0]]);
    drain(&db, target(&db, QueueFamily::Vector).await).await;
    drain(&db, target(&db, QueueFamily::Text).await).await;
    // The pending scores are exactly the published ones.
    assert_eq!(
        vector_search(&db, [0.0, 0.0], 1, None, SearchConsistency::Strong).await,
        pending_vector
    );
    assert_eq!(
        text_search(&db, "alpha", 1, None, SearchConsistency::Strong).await,
        pending_text
    );

    // Exactly the bound of superseded results ahead of the answer: the
    // widened search still completes.
    delete_many(&db, &ids[..800]).await;
    assert_eq!(
        nearest(vector_search(&db, [0.0, 0.0], 1, None, SearchConsistency::Strong).await),
        [ids[800]]
    );
    assert_eq!(
        nearest(text_search(&db, "alpha", 1, None, SearchConsistency::Strong).await),
        [ids[800]]
    );

    // One more superseded result ahead: the search fails with retryable
    // backpressure instead of widening with the backlog.
    delete(&db, ids[800]).await;
    for request in [
        QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "hits",
                    traversal::g().vector_search_nodes("Doc", "embedding", vec![0.0, 0.0], 1, None),
                )
                .returning(["hits"]),
        ),
        QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "hits",
                    traversal::g().text_search_nodes("Doc", "body", "alpha", 1, None),
                )
                .returning(["hits"]),
        ),
    ] {
        let error = db
            .query(request)
            .await
            .expect_err("more superseded results than the bound must not widen silently");
        assert!(error.is_index_backpressure(), "unexpected error: {error}");
    }
    // Eventual search overlays only the oldest 800 deletes instead. The newest
    // keeps its published row, whose deleted node is not returned.
    assert_eq!(
        nearest(vector_search(&db, [0.0, 0.0], 2, None, SearchConsistency::Eventual).await),
        [ids[801]]
    );
    assert_eq!(
        nearest(text_search(&db, "alpha", 2, None, SearchConsistency::Eventual).await),
        [ids[801]]
    );

    // Publication drains the backlog and the same searches succeed.
    drain(&db, target(&db, QueueFamily::Vector).await).await;
    drain(&db, target(&db, QueueFamily::Text).await).await;
    assert_eq!(
        nearest(vector_search(&db, [0.0, 0.0], 1, None, SearchConsistency::Strong).await),
        [ids[801]]
    );
    assert_eq!(
        nearest(text_search(&db, "alpha", 1, None, SearchConsistency::Strong).await),
        [ids[801]]
    );
    db.close().await.unwrap();
}

/// Rewrites each of `ids` to a far embedding and a body without `"alpha"`, in
/// batched writes.
async fn move_away(db: &HelixDB, ids: &[u64]) {
    for (chunk_index, chunk) in ids.chunks(100).enumerate() {
        write(db, || {
            QueryRequest::write(chunk.iter().enumerate().fold(
                batch::write_batch(),
                |batch, (index, id)| {
                    let position = 10_000.0 + (chunk_index * 100 + index) as f32;
                    batch.var_as(
                        &format!("d{index}"),
                        traversal::g()
                            .n(NodeRef::from(*id))
                            .set_property("embedding", vec![position, 0.0])
                            .set_property("body", "beta".to_string()),
                    )
                },
            ))
        })
        .await;
    }
}

#[tokio::test]
async fn past_the_suppression_limit_strong_search_fails_and_eventual_search_degrades() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "overlay-suppression-consistency",
        store,
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&db, None).await;
    let ids = add_many(&db, (0..802).map(|index| [index as f32, 0.0])).await;
    drain(&db, target(&db, QueueFamily::Vector).await).await;
    drain(&db, target(&db, QueueFamily::Text).await).await;
    // 801 pending rewrites move the nearest documents away and stop them
    // matching, so each one's published result lies ahead of the answer.
    move_away(&db, &ids[..801]).await;
    let nearest = |found: Vec<(u64, u64)>| found.into_iter().map(|(id, _)| id).collect::<Vec<_>>();

    // Strong search never answers without every committed change.
    for request in [
        QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "hits",
                    traversal::g().vector_search_nodes("Doc", "embedding", vec![0.0, 0.0], 1, None),
                )
                .returning(["hits"]),
        ),
        QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "hits",
                    traversal::g().text_search_nodes("Doc", "body", "alpha", 1, None),
                )
                .returning(["hits"]),
        ),
    ] {
        let error = db
            .query(request)
            .await
            .expect_err("strong search must not skip the limit's worth of superseded results");
        assert!(error.is_index_backpressure(), "unexpected error: {error}");
    }

    // Eventual search overlays the oldest 800 rewrites and serves the newest
    // from its published row, exactly as before that rewrite.
    assert_eq!(
        vector_search(&db, [0.0, 0.0], 1, None, SearchConsistency::Eventual).await,
        [(ids[800], 640_000.0_f64.to_bits())]
    );
    assert_eq!(
        nearest(text_search(&db, "alpha", 1, None, SearchConsistency::Eventual).await),
        [ids[800]]
    );
    // A search whose answer needs no widening still overlays every rewrite.
    assert_eq!(
        nearest(vector_search(&db, [10_800.0, 0.0], 1, None, SearchConsistency::Eventual).await),
        [ids[800]]
    );

    // Once published, both consistencies agree on the fresh answer.
    drain(&db, target(&db, QueueFamily::Vector).await).await;
    drain(&db, target(&db, QueueFamily::Text).await).await;
    for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
        assert_eq!(
            nearest(vector_search(&db, [0.0, 0.0], 1, None, consistency).await),
            [ids[801]],
            "{consistency:?}"
        );
        assert_eq!(
            nearest(text_search(&db, "alpha", 1, None, consistency).await),
            [ids[801]],
            "{consistency:?}"
        );
    }
    db.close().await.unwrap();
}

/// How a write batch changes the documents ahead of the origin.
#[derive(Debug, Clone, Copy)]
enum AheadChange {
    Delete,
    /// Re-embeds far away, as [`move_away`] does, and rewrites the body to
    /// stop matching `"alpha"`.
    MoveAway,
}

/// One write batch that applies `change` to `changed` and then ranks the
/// document nearest the origin through each index of `families`.
fn change_and_search(
    changed: &[u64],
    change: AheadChange,
    families: &[QueueFamily],
) -> QueryRequest {
    let batch = match change {
        AheadChange::Delete => batch::write_batch().var_as(
            "changed",
            traversal::g().n(NodeRef::from(changed.to_vec())).drop(),
        ),
        AheadChange::MoveAway => {
            changed
                .iter()
                .enumerate()
                .fold(batch::write_batch(), |batch, (index, id)| {
                    batch.var_as(
                        &format!("changed{index}"),
                        traversal::g()
                            .n(NodeRef::from(*id))
                            .set_property("embedding", vec![10_000.0 + index as f32, 0.0])
                            .set_property("body", "beta".to_string()),
                    )
                })
        }
    };
    let batch = families.iter().fold(batch, |batch, family| match family {
        QueueFamily::Vector => batch.var_as(
            "vector_hits",
            traversal::g().vector_search_nodes("Doc", "embedding", vec![0.0, 0.0], 1, None),
        ),
        QueueFamily::Text => batch.var_as(
            "text_hits",
            traversal::g().text_search_nodes("Doc", "body", "alpha", 1, None),
        ),
    });
    QueryRequest::write(batch.returning(families.iter().map(|family| match family {
        QueueFamily::Vector => "vector_hits",
        QueueFamily::Text => "text_hits",
    })))
}

#[test]
fn write_batch_searches_widen_past_every_change_of_their_own() {
    // The move-away write batch has 902 statements, so the planner's
    // recursive reachability walk runs over 2,000 frames deep and exceeds the
    // default debug test thread stack; production sizing is unchanged.
    std::thread::Builder::new()
        .name("overlay-local-suppression".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("overlay test runtime builds")
                .block_on(write_batch_searches_widen_past_every_change_of_their_own_contract());
        })
        .expect("overlay test thread starts")
        .join()
        .expect("overlay test completes");
}

async fn write_batch_searches_widen_past_every_change_of_their_own_contract() {
    for change in [AheadChange::Delete, AheadChange::MoveAway] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = open(
            &format!("overlay-local-suppression-{change:?}"),
            store,
            queued(IndexOperationQueueTuning::default()),
        )
        .await;
        install(&db, None).await;
        let ids = add_many(&db, (0..901).map(|index| [index as f32, 0.0])).await;
        drain(&db, target(&db, QueueFamily::Vector).await).await;
        drain(&db, target(&db, QueueFamily::Text).await).await;

        // The batch's own changes put 900 published results ahead of the
        // answer. No publication can clear them, so they never count toward
        // the suppression limit.
        let result = db
            .query(change_and_search(
                &ids[..900],
                change,
                &[QueueFamily::Vector, QueueFamily::Text],
            ))
            .await
            .unwrap_or_else(|error| panic!("{change:?}: {error}"));
        let local_vector = hits(&result, "vector_hits");
        let local_text = hits(&result, "text_hits");
        assert_eq!(
            local_vector,
            [(ids[900], 810_000.0_f64.to_bits())],
            "{change:?}"
        );
        assert_eq!(
            local_text.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            [ids[900]],
            "{change:?}"
        );

        // Committed and unpublished, the same changes do count.
        for request in [
            QueryRequest::read(
                batch::read_batch()
                    .var_as(
                        "hits",
                        traversal::g().vector_search_nodes(
                            "Doc",
                            "embedding",
                            vec![0.0, 0.0],
                            1,
                            None,
                        ),
                    )
                    .returning(["hits"]),
            ),
            QueryRequest::read(
                batch::read_batch()
                    .var_as(
                        "hits",
                        traversal::g().text_search_nodes("Doc", "body", "alpha", 1, None),
                    )
                    .returning(["hits"]),
            ),
        ] {
            let error = db
                .query(request)
                .await
                .expect_err("a committed backlog past the limit fails a strong search");
            assert!(error.is_index_backpressure(), "{change:?}: {error}");
        }

        // Published, they rank exactly as the batch ranked them.
        drain(&db, target(&db, QueueFamily::Vector).await).await;
        drain(&db, target(&db, QueueFamily::Text).await).await;
        assert_eq!(
            vector_search(&db, [0.0, 0.0], 1, None, SearchConsistency::Strong).await,
            local_vector,
            "{change:?}"
        );
        assert_eq!(
            text_search(&db, "alpha", 1, None, SearchConsistency::Strong).await,
            local_text,
            "{change:?}"
        );
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn write_batch_searches_fail_with_backpressure_only_past_committed_work() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "overlay-local-committed-suppression",
        store,
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&db, None).await;
    let ids = add_many(&db, (0..902).map(|index| [index as f32, 0.0])).await;
    drain(&db, target(&db, QueueFamily::Vector).await).await;
    drain(&db, target(&db, QueueFamily::Text).await).await;
    // 801 committed rewrites ahead of the answer, then 100 of the batch's own
    // deletes behind them.
    move_away(&db, &ids[..801]).await;
    let local = &ids[801..901];

    for family in [QueueFamily::Vector, QueueFamily::Text] {
        let error = db
            .query(change_and_search(local, AheadChange::Delete, &[family]))
            .await
            .expect_err("committed work past the limit still fails the search");
        assert!(error.is_index_backpressure(), "{family:?}: {error}");
        assert!(
            error
                .to_string()
                .contains("suppressed_search_results would reach 801, limit 800"),
            "{family:?}: only committed results count: {error}"
        );
    }

    // Once that work is published the unchanged batch succeeds, widening past
    // its own deletes.
    drain(&db, target(&db, QueueFamily::Vector).await).await;
    drain(&db, target(&db, QueueFamily::Text).await).await;
    let result = db
        .query(change_and_search(
            local,
            AheadChange::Delete,
            &[QueueFamily::Vector, QueueFamily::Text],
        ))
        .await
        .unwrap();
    assert_eq!(
        hits(&result, "vector_hits"),
        [(ids[901], 811_801.0_f64.to_bits())]
    );
    assert_eq!(
        hits(&result, "text_hits")
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>(),
        [ids[901]]
    );
    db.close().await.unwrap();
}

/// Ranks `"rust"` over the traversal candidates `ids`.
async fn restricted_text_search(
    db: &HelixDB,
    ids: &[u64],
    consistency: SearchConsistency,
) -> Vec<(u64, u64)> {
    let request = QueryRequest::read(
        batch::read_batch()
            .var_as(
                "hits",
                traversal::g()
                    .n(NodeRef::from(ids.to_vec()))
                    .text_search("Doc", "body", "rust", 10, None),
            )
            .returning(["hits"]),
    )
    .with_search_consistency(consistency)
    .unwrap();
    hits(&Box::pin(db.query(request)).await.unwrap(), "hits")
}

#[tokio::test]
async fn traversal_scoped_text_search_scores_superseded_candidates_exactly() {
    let bodies = [
        "rust storage",
        "rust planner",
        "rust rust",
        "graph storage",
        "rust unchanged",
        "rust outside",
    ];
    // Pending changes to the first four candidates: a rewrite that stops
    // matching, a delete, a rewrite that starts matching, and a rewrite that
    // still matches. The fifth candidate stays published; the last document
    // matches outside every candidate set.
    let changes = async |db: &HelixDB, ids: &[u64]| {
        update(db, ids[1], [1.0, 0.0], "python planner").await;
        delete(db, ids[2]).await;
        update(db, ids[3], [3.0, 0.0], "rust graph").await;
        update(db, ids[0], [0.0, 0.0], "rust rust storage").await;
    };
    // Reference: an initial build over the final graph state.
    let reference = open(
        "overlay-restricted-text-reference",
        Arc::new(InMemory::new()),
        DbConfig::new(),
    )
    .await;
    let mut reference_ids = Vec::new();
    for (index, body) in bodies.into_iter().enumerate() {
        reference_ids.push(add(&reference, [index as f32, 0.0], body, None).await);
    }
    changes(&reference, &reference_ids).await;
    install(&reference, None).await;

    let db = open(
        "overlay-restricted-text",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&db, None).await;
    let mut ids = Vec::new();
    for (index, body) in bodies.into_iter().enumerate() {
        ids.push(add(&db, [index as f32, 0.0], body, None).await);
    }
    drain(&db, target(&db, QueueFamily::Text).await).await;
    changes(&db, &ids).await;

    // Four candidates are all superseded; five keep one published match.
    let mut expected = Vec::new();
    for (candidates, ranked) in [(4, &[0, 3][..]), (5, &[0, 3, 4][..])] {
        let reference_hits = ordinals(
            restricted_text_search(
                &reference,
                &reference_ids[..candidates],
                SearchConsistency::Strong,
            )
            .await,
            &reference_ids,
        );
        assert_eq!(
            reference_hits
                .iter()
                .map(|(ordinal, _)| *ordinal)
                .collect::<Vec<_>>(),
            ranked,
            "only candidates that match after the changes rank"
        );
        for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
            assert_eq!(
                ordinals(
                    restricted_text_search(&db, &ids[..candidates], consistency).await,
                    &ids
                ),
                reference_hits,
                "{candidates} candidates, {consistency:?}"
            );
        }
        expected.push((candidates, reference_hits));
    }
    // Published, the same searches read only physical splits.
    drain(&db, target(&db, QueueFamily::Text).await).await;
    for (candidates, reference_hits) in expected {
        assert_eq!(
            ordinals(
                restricted_text_search(&db, &ids[..candidates], SearchConsistency::Strong).await,
                &ids
            ),
            reference_hits,
            "{candidates} candidates, published"
        );
    }
    reference.close().await.unwrap();
    db.close().await.unwrap();
}

/// A full-text disk tier admits a split on its second successful use.
fn with_full_text_disk_tier(config: DbConfig, root: &std::path::Path) -> DbConfig {
    use crate::config::{
        CacheConfig, CacheMode, FtsHybridCacheConfig, FtsWarmConfig, ObjectStoreWarmLevel,
        SlateHybridCacheConfig, SlateObjectStoreCacheSettings, SlateWarmConfig,
        VectorMemorySettings,
    };
    config.with_cache(CacheConfig::new(
        VectorMemorySettings::default(),
        CacheMode::Hybrid {
            slate_db: SlateHybridCacheConfig::try_new(
                1024 * 1024,
                root.join("foyer"),
                16 * 1024 * 1024,
            )
            .unwrap(),
            object_store: SlateObjectStoreCacheSettings::try_new(
                root.join("object-store"),
                Some(1024 * 1024),
                4096,
                false,
                ObjectStoreWarmLevel::Off,
                None,
                1,
            )
            .unwrap(),
            slate_warm: SlateWarmConfig::Off,
            fts: Some(
                FtsHybridCacheConfig::try_new(
                    1024 * 1024,
                    root.join("fts"),
                    1024 * 1024,
                    FtsWarmConfig::Off,
                    1,
                )
                .unwrap(),
            ),
        },
    ))
}

#[tokio::test]
async fn a_widened_text_search_records_one_use_of_each_split() {
    let root = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "overlay-text-split-demand",
        store,
        with_full_text_disk_tier(queued(IndexOperationQueueTuning::default()), root.path()),
    )
    .await;
    db.wait_for_startup_cache_warm().await;
    install(&db, None).await;
    // Identical bodies tie on BM25, so the search ranks them by ID.
    let ids = add_many(&db, (0..3).map(|index| [index as f32, 0.0])).await;
    drain(&db, target(&db, QueueFamily::Text).await).await;
    // The top result no longer matches once its rewrite publishes, so the
    // strong search skips it and widens with a second physical search.
    update(&db, ids[0], [0.0, 0.0], "beta").await;
    let attempts = || async { db.fts_cache_state().await.unwrap().hydration_attempts };

    let found = text_search(&db, "alpha", 1, None, SearchConsistency::Strong).await;
    assert_eq!(
        found.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        [ids[1]]
    );
    // A hydration the search spawned counts its attempt when first polled.
    tokio::task::yield_now().await;
    assert_eq!(
        attempts().await,
        0,
        "one widened search is one use of each split, which admits nothing"
    );

    let again = text_search(&db, "alpha", 1, None, SearchConsistency::Strong).await;
    assert_eq!(again, found);
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while attempts().await == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a second search admits the splits it used");
    db.close().await.unwrap();
}

#[tokio::test]
async fn restricted_searches_skip_the_physical_search_when_every_candidate_is_superseded() {
    let db = open(
        "overlay-restricted-superseded",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&db, None).await;
    let ids = add_many(&db, (0..4).map(|index| [index as f32, 0.0])).await;
    drain(&db, target(&db, QueueFamily::Vector).await).await;
    drain(&db, target(&db, QueueFamily::Text).await).await;
    // Pending rewrites supersede the first three docs in both indexes; the
    // last stays published.
    for (index, id) in ids[..3].iter().enumerate() {
        update(&db, *id, [index as f32 + 0.5, 0.0], "alpha rewritten").await;
    }
    let vector = |candidates: &[u64], consistency| {
        QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "hits",
                    traversal::g()
                        .n(NodeRef::from(candidates.to_vec()))
                        .vector_search("Doc", "embedding", vec![0.0, 0.0], 10, None),
                )
                .returning(["hits"]),
        )
        .with_search_consistency(consistency)
        .unwrap()
    };
    let text = |candidates: &[u64], consistency| {
        QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "hits",
                    traversal::g()
                        .n(NodeRef::from(candidates.to_vec()))
                        .text_search("Doc", "body", "alpha", 10, None),
                )
                .returning(["hits"]),
        )
        .with_search_consistency(consistency)
        .unwrap()
    };

    let mut overlaid = Vec::new();
    for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
        // Only the pending vectors can rank, and no physical search runs.
        let (result, stats) = crate::search::vector::observe_restricted_search(Box::pin(
            db.query(vector(&ids[..3], consistency)),
        ))
        .await;
        let vector_hits = hits(&result.unwrap(), "hits");
        assert_eq!(
            vector_hits,
            [
                (ids[0], 0.25_f64.to_bits()),
                (ids[1], 2.25_f64.to_bits()),
                (ids[2], 6.25_f64.to_bits()),
            ],
            "{consistency:?}"
        );
        assert!(stats.is_none(), "{consistency:?}: {stats:?}");
        // A published candidate still reaches the physical search.
        let (result, stats) = crate::search::vector::observe_restricted_search(Box::pin(
            db.query(vector(&ids, consistency)),
        ))
        .await;
        assert_eq!(
            hits(&result.unwrap(), "hits").last(),
            Some(&(ids[3], 9.0_f64.to_bits())),
            "{consistency:?}"
        );
        assert!(
            stats.as_ref().is_some_and(|stats| stats.strategy.is_some()),
            "{consistency:?}: {stats:?}"
        );

        // Text likewise scores only the pending documents and loads no
        // manifest.
        let (result, loads) = crate::index_lifecycle::text::serving::observe_manifest_root_loads(
            Box::pin(db.query(text(&ids[..3], consistency))),
        )
        .await;
        let text_hits = hits(&result.unwrap(), "hits");
        assert_eq!(
            text_hits.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            ids[..3],
            "{consistency:?}"
        );
        assert_eq!(loads, 0, "{consistency:?}");
        let (result, loads) = crate::index_lifecycle::text::serving::observe_manifest_root_loads(
            Box::pin(db.query(text(&ids, consistency))),
        )
        .await;
        assert_eq!(hits(&result.unwrap(), "hits").len(), 4, "{consistency:?}");
        assert_eq!(loads, 1, "{consistency:?}");
        overlaid.push((vector_hits, text_hits));
    }

    // Published, the same searches read only physical rows and agree.
    drain(&db, target(&db, QueueFamily::Vector).await).await;
    drain(&db, target(&db, QueueFamily::Text).await).await;
    for (vector_hits, text_hits) in overlaid {
        assert_eq!(
            hits(
                &Box::pin(db.query(vector(&ids[..3], SearchConsistency::Strong)))
                    .await
                    .unwrap(),
                "hits"
            ),
            vector_hits
        );
        assert_eq!(
            hits(
                &Box::pin(db.query(text(&ids[..3], SearchConsistency::Strong)))
                    .await
                    .unwrap(),
                "hits"
            ),
            text_hits
        );
    }
    db.close().await.unwrap();
}

/// Writes `docs`, each linked from the hub and, but for the first, to the
/// doc written before it, then `search` as `result`.
///
/// Doc `d` embeds at `d * 7 % 320` on one axis, so each of the first 320 docs
/// has its own vector distance, and every third doc has kind `B`.
fn docs_batch(
    docs: std::ops::Range<usize>,
    search: Option<traversal::Traversal<traversal::OnNodes>>,
) -> batch::WriteBatch {
    let first = docs.start;
    let names = docs
        .clone()
        .map(|doc| format!("d{doc}"))
        .collect::<Vec<_>>();
    let written = docs
        .zip(&names)
        .fold(batch::write_batch(), |written, (doc, name)| {
            let written = written
                .var_as(
                    name,
                    traversal::g().add_n(
                        "Doc",
                        vec![
                            (
                                "embedding",
                                PropertyInput::from(vec![(doc * 7 % 320) as f32, 0.0]),
                            ),
                            (
                                "body",
                                PropertyInput::from(if doc % 2 == 0 {
                                    "rust storage"
                                } else {
                                    "rust planner engine"
                                }),
                            ),
                            (
                                "kind",
                                PropertyInput::from(if doc % 3 == 0 { "B" } else { "A" }),
                            ),
                        ],
                    ),
                )
                .var_as(
                    &format!("{name}_linked"),
                    traversal::g().n_with_label("Hub").add_e(
                        "HAS",
                        NodeRef::var(name),
                        Vec::<(&str, PropertyInput)>::new(),
                    ),
                );
            if doc == first {
                return written;
            }
            written.var_as(
                &format!("{name}_next"),
                traversal::g().n(NodeRef::var(name)).add_e(
                    "NEXT",
                    NodeRef::var(format!("d{}", doc - 1)),
                    Vec::<(&str, PropertyInput)>::new(),
                ),
            )
        });
    match search {
        Some(search) => written
            .var_as("result", search)
            .returning(names.into_iter().chain(["result".to_string()])),
        None => written.returning(names),
    }
}

/// Filters kind-`B` docs after an overlaid search, directly or behind an
/// expansion. Each filter sees more than one record batch of node rows, so
/// index membership resolves its set instead of evaluating every row.
///
/// Without statistics the planner estimates a search's rows by its `k`, so
/// each search asks for the 800-result cap to make index membership pay.
///
/// `label` scopes the filter to `Doc`: `$label = Doc` plans index
/// membership, while the per-row oracle's negated inequality is not a finite
/// `$label` domain, so it plans a true per-row filter.
fn membership_shapes(
    label: &expr::Predicate,
) -> [(&'static str, traversal::Traversal<traversal::OnNodes>); 4] {
    let kind_b = || expr::Predicate::and(vec![label.clone(), expr::Predicate::eq("kind", "B")]);
    [
        (
            "vector search then expansion",
            traversal::g()
                .vector_search_nodes("Doc", "embedding", vec![0.0, 0.0], 800, None)
                .out(Some("NEXT"))
                .where_(kind_b()),
        ),
        (
            "expansion then vector search",
            traversal::g()
                .n_with_label("Hub")
                .out(Some("HAS"))
                .vector_search("Doc", "embedding", vec![0.0, 0.0], 800, None)
                .where_(kind_b()),
        ),
        (
            "text search then expansion",
            traversal::g()
                .text_search_nodes("Doc", "body", "rust", 800, None)
                .out(Some("NEXT"))
                .where_(kind_b()),
        ),
        (
            "expansion then text search",
            traversal::g()
                .n_with_label("Hub")
                .out(Some("HAS"))
                .text_search("Doc", "body", "rust", 800, None)
                .where_(kind_b()),
        ),
    ]
}

/// Index membership after vector and text searches needs no search-index
/// flush: while the queue holds every doc unpublished, it keeps exactly the
/// rows the per-row filter keeps, in order, for strong and eventual reads and
/// inside a write batch that adds its own docs.
#[tokio::test]
async fn index_membership_after_overlaid_searches_matches_the_per_row_filter() {
    // Both databases hold the same unpublished docs. Only the first indexes
    // `kind`, and only its filters state the label as `$label = Doc`, so only
    // they plan index membership.
    let fixture = async |name: &str, indexed: bool| {
        let db = open(
            name,
            Arc::new(InMemory::new()),
            queued(IndexOperationQueueTuning::default()),
        )
        .await;
        install(&db, None).await;
        if indexed {
            db.install_index_for_tests(
                SecondaryIndexDefinition::node_equality("Doc", "kind")
                    .unwrap()
                    .try_into()
                    .unwrap(),
            )
            .await
            .unwrap();
        }
        write(&db, || {
            QueryRequest::write(batch::write_batch().var_as(
                "hub",
                traversal::g().add_n("Hub", Vec::<(&str, PropertyInput)>::new()),
            ))
        })
        .await;
        let mut ids = Vec::new();
        for start in (0..300).step_by(100) {
            let written = docs_batch(start..start + 100, None);
            let result = write(&db, || QueryRequest::write(written.clone())).await;
            ids.extend(
                (start..start + 100)
                    .map(|doc| result[format!("d{doc}")][0]["$id"].as_u64().unwrap()),
            );
        }
        (db, ids)
    };
    let (indexed, mut indexed_ids) = fixture("overlay-membership-indexed", true).await;
    let (per_row, mut per_row_ids) = fixture("overlay-membership-per-row", false).await;
    assert_eq!(
        indexed.index_operation_queue_stats().pending_members,
        600,
        "every doc's vector and text insert is unpublished"
    );
    let membership_steps = |plan: exec::ExecutablePlan| {
        plan.steps()
            .iter()
            .filter(|step| matches!(step.op, exec::ExecOp::IndexMembership { .. }))
            .count()
    };
    let resolved = || {
        indexed
            .inner
            .resolved_index_memberships
            .load(std::sync::atomic::Ordering::Relaxed)
    };

    let indexed_label = expr::Predicate::eq("$label", "Doc");
    let per_row_label = expr::Predicate::not(expr::Predicate::neq("$label", "Doc"));
    let shapes = || {
        membership_shapes(&indexed_label)
            .into_iter()
            .zip(membership_shapes(&per_row_label))
    };

    for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
        for ((shape, search), (_, oracle)) in shapes() {
            let read = |search| {
                batch::read_batch()
                    .var_as("result", search)
                    .returning(["result"])
            };
            let (read, oracle) = (read(search), read(oracle));
            let plan = |db: &HelixDB, read: &batch::ReadBatch| {
                planning::plan_read_batch(
                    read,
                    &db.planner_context(context::ParamBindings::default()),
                )
                .unwrap()
            };
            assert_eq!(membership_steps(plan(&indexed, &read)), 1, "{shape}");
            assert_eq!(membership_steps(plan(&per_row, &oracle)), 0, "{shape}");
            let run = async |db: &HelixDB, read: &batch::ReadBatch, ids: &[u64]| {
                let request = QueryRequest::read(read.clone())
                    .with_search_consistency(consistency)
                    .unwrap();
                ordinals(
                    hits(&Box::pin(db.query(request)).await.unwrap(), "result"),
                    ids,
                )
            };
            let before = resolved();
            let found = run(&indexed, &read, &indexed_ids).await;
            assert_eq!(
                resolved(),
                before + 1,
                "{shape}, {consistency:?}: membership reads its index set"
            );
            let expected = run(&per_row, &oracle, &per_row_ids).await;
            assert!(!expected.is_empty(), "{shape}, {consistency:?}");
            assert_eq!(found, expected, "{shape}, {consistency:?}");
        }
    }

    // A write batch adds docs, their vectors, text, and hub links, then
    // filters its own search.
    for (index, ((shape, search), (_, oracle))) in shapes().enumerate() {
        let docs = 300 + index * 5..305 + index * 5;
        let written = docs_batch(docs.clone(), Some(search));
        let oracle = docs_batch(docs.clone(), Some(oracle));
        let plan = |db: &HelixDB, written: &batch::WriteBatch| {
            planning::plan_write_batch(
                written,
                &db.planner_context(context::ParamBindings::default()),
            )
            .unwrap()
        };
        assert_eq!(membership_steps(plan(&indexed, &written)), 1, "{shape}");
        assert_eq!(membership_steps(plan(&per_row, &oracle)), 0, "{shape}");
        let run = async |db: &HelixDB, written: &batch::WriteBatch, ids: &mut Vec<u64>| {
            let result = write(db, || QueryRequest::write(written.clone())).await;
            ids.extend(
                docs.clone()
                    .map(|doc| result[format!("d{doc}")][0]["$id"].as_u64().unwrap()),
            );
            ordinals(hits(&result, "result"), ids)
        };
        let before = resolved();
        let found = run(&indexed, &written, &mut indexed_ids).await;
        assert_eq!(
            resolved(),
            before + 1,
            "{shape} in a write batch: membership reads its index set"
        );
        let expected = run(&per_row, &oracle, &mut per_row_ids).await;
        assert!(
            expected.iter().any(|(ordinal, _)| *ordinal >= 300),
            "{shape} in a write batch keeps some of its own docs"
        );
        assert_eq!(found, expected, "{shape} in a write batch");
    }
    indexed.close().await.unwrap();
    per_row.close().await.unwrap();
}

/// Hit IDs in ascending order.
fn sorted_ids(found: Vec<(u64, u64)>) -> Vec<u64> {
    let mut ids = found.into_iter().map(|(id, _)| id).collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

/// Opens a database whose eventual budget covers exactly one small
/// operation, adds one `alpha` document at the origin, and rewrites each
/// family's resolved queue value to follow its operation with a malformed one
/// larger than the rest of the budget. Returns the database and the document.
async fn open_with_a_corrupt_queue_tail(name: &str) -> (HelixDB, u64) {
    // Probe one small operation's retained size per family.
    let probe = open(
        &format!("{name}-probe"),
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&probe, None).await;
    add(&probe, [0.0, 0.0], "alpha", None).await;
    let mut budget = 0;
    for family in [QueueFamily::Text, QueueFamily::Vector] {
        budget = budget.max(queue(&probe, family).await.unwrap().operations()[0].retained_bytes());
    }
    probe.close().await.unwrap();

    let tuning = IndexOperationQueueTuning::default().with_eventual_search_budget_for_tests(budget);
    let db = open(name, Arc::new(InMemory::new()), queued(tuning)).await;
    install(&db, None).await;
    let first = add(&db, [0.0, 0.0], "alpha", None).await;
    let storage = db.inner_db();
    for family in [QueueFamily::Text, QueueFamily::Vector] {
        // Version, kind, and family bytes, then no removals and one insert.
        const HEADER: usize = 3;
        let key = target(&db, family).await.key();
        let stored = storage.get(&key).await.unwrap().unwrap();
        assert_eq!(stored[1], QUEUE_VALUE_KIND);
        assert_eq!(&stored[HEADER..HEADER + 2], &[0x00, 0x01]);
        let mut value = stored[..HEADER].to_vec();
        value.extend_from_slice(&[0x00, 0x02]);
        value.extend_from_slice(&stored[HEADER + 2..]);
        // A later operation whose body names an unknown entity kind and is
        // larger than anything left of the budget after the first.
        value.push(0x01);
        value.extend_from_slice(&(u128::MAX >> 1).to_be_bytes());
        value.push(100);
        value.extend_from_slice(&[0x7F; 100]);
        let decoded = OperationQueue::decode(&value);
        assert!(
            decoded.is_err(),
            "{family:?}: the later operation is malformed"
        );
        storage.put(&key, value).await.unwrap();
    }
    (db, first)
}

/// Named text (`alpha`) and vector (origin) searches of the corrupt-tail
/// fixture at `consistency`.
fn corrupt_tail_searches(consistency: SearchConsistency) -> [(&'static str, QueryRequest); 2] {
    let search = |traversal| {
        QueryRequest::read(
            batch::read_batch()
                .var_as("hits", traversal)
                .returning(["hits"]),
        )
        .with_search_consistency(consistency)
        .unwrap()
    };
    [
        (
            "text",
            search(traversal::g().text_search_nodes("Doc", "body", "alpha", 10, None)),
        ),
        (
            "vector",
            search(traversal::g().vector_search_nodes(
                "Doc",
                "embedding",
                vec![0.0, 0.0],
                10,
                None,
            )),
        ),
    ]
}

#[tokio::test]
async fn eventual_search_decodes_only_its_budget() {
    let (db, first) = open_with_a_corrupt_queue_tail("overlay-decode-budget").await;
    // Eventual search decodes only the operations its budget can select.
    for (name, request) in corrupt_tail_searches(SearchConsistency::Eventual) {
        let result = Box::pin(db.query(request)).await.unwrap_or_else(|error| {
            panic!("eventual {name} search decoded past its budget: {error}")
        });
        assert_eq!(sorted_ids(hits(&result, "hits")), [first], "{name}");
    }
    // Strong search needs every operation and still fails closed.
    for (name, request) in corrupt_tail_searches(SearchConsistency::Strong) {
        assert!(
            Box::pin(db.query(request)).await.is_err(),
            "strong {name} search must not answer past a corrupt operation"
        );
    }
    db.close().await.unwrap();
}

/// Known limitation, pinned so that lifting it shows up here; not a contract.
///
/// The eventual budget bounds which operations a search decodes, not the
/// queue read. In the map layout a read fetches the whole value, and while a
/// merge operand is pending above it (the normal state of an index taking
/// writes or being drained) SlateDB first resolves that operand against all
/// of it, parsing, validating, and re-encoding every record. Such a search
/// therefore costs the whole backlog, up to `max_retained_bytes`, and fails
/// on a corrupt record its budget never selects. Even a resolved value is
/// walked record by record, since an entity's latest operation may be its
/// last record. Bounding the read by the budget needs a queue layout whose
/// reads can stop there and still find each entity's latest operation; once
/// one exists, replace this with a bound on the bytes each eventual search
/// reads and merges.
#[tokio::test]
async fn known_limitation_eventual_search_resolves_the_whole_queue_below_a_pending_operand() {
    let (db, _) = open_with_a_corrupt_queue_tail("overlay-pending-operand").await;
    // The flush first makes the corrupt value a base that no flush merges the
    // new operand into.
    db.inner_db()
        .flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .unwrap();
    add(&db, [1.0, 1.0], "beta", None).await;
    for (name, request) in corrupt_tail_searches(SearchConsistency::Eventual) {
        let Err(error) = Box::pin(db.query(request)).await else {
            panic!("eventual {name} search no longer resolves the whole queue");
        };
        assert!(
            format!("{error:?}").contains("unknown queued entity kind"),
            "{name}: {error:?}"
        );
    }
    db.close().await.unwrap();
}

/// An eventual search shows a pending entity at its latest state or not at
/// all. An earlier state that alone fits the budget is never shown: the
/// entity's physical representation may already hold the latest state.
#[tokio::test]
async fn eventual_search_shows_an_entity_at_its_latest_state_or_not_at_all() {
    // Probe one small operation's retained size per family.
    let probe = open(
        "overlay-latest-probe",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&probe, None).await;
    add(&probe, [0.0, 0.0], "alpha", None).await;
    let mut budget = 0;
    for family in [QueueFamily::Text, QueueFamily::Vector] {
        budget = budget.max(queue(&probe, family).await.unwrap().operations()[0].retained_bytes());
    }
    probe.close().await.unwrap();

    // The budget covers the add but not the larger update that follows it.
    let tuning = IndexOperationQueueTuning::default().with_eventual_search_budget_for_tests(budget);
    let db = open(
        "overlay-latest-state",
        Arc::new(InMemory::new()),
        queued(tuning),
    )
    .await;
    install(&db, None).await;
    let id = add(&db, [0.0, 0.0], "alpha", None).await;
    let omega = "omega replaces the first body with one longer than the eventual budget";
    update(&db, id, [3.0, 4.0], omega).await;
    for family in [QueueFamily::Text, QueueFamily::Vector] {
        let operations = queue(&db, family).await.unwrap().into_operations();
        assert_eq!(operations.len(), 2, "{family:?}");
        assert!(operations[0].retained_bytes() <= budget, "{family:?}");
    }
    assert!(queue(&db, QueueFamily::Text).await.unwrap().operations()[1].retained_bytes() > budget);
    let nearest = |found: Vec<(u64, u64)>| {
        let [(found, bits)] = found.try_into().expect("one hit");
        assert_eq!(found, id);
        f64::from_bits(bits)
    };

    // Strong search reads every operation and sees the update.
    assert_eq!(
        sorted_ids(text_search(&db, "omega", 10, None, SearchConsistency::Strong).await),
        [id]
    );
    assert!(
        text_search(&db, "alpha", 10, None, SearchConsistency::Strong)
            .await
            .is_empty()
    );
    assert_eq!(
        nearest(vector_search(&db, [3.0, 4.0], 10, None, SearchConsistency::Strong).await),
        0.0
    );
    // The text update does not fit the eventual budget, so the entity keeps
    // its (absent) physical representation; its add, which alone fits, is
    // never shown.
    for query in ["alpha", "omega"] {
        assert!(
            text_search(&db, query, 10, None, SearchConsistency::Eventual)
                .await
                .is_empty(),
            "{query}"
        );
    }
    // The vector update fits, so the entity is shown at its latest state.
    assert_eq!(
        nearest(vector_search(&db, [3.0, 4.0], 10, None, SearchConsistency::Eventual).await),
        0.0
    );
    db.close().await.unwrap();
}

/// Backfill limits whose text publications analyze at most `budget` bytes.
fn analysis_budget(budget: u64) -> SearchIndexBackfillLimits {
    let defaults = SearchIndexBackfillLimits::default();
    let compaction = defaults.text_compaction();
    SearchIndexBackfillLimits::try_new(
        defaults.batch(),
        defaults.edge_property_read_batch(),
        defaults.text_artifacts(),
        TextBackfillCompactionLimits::new(
            compaction.max_fan_in(),
            NonZeroU64::new(budget).unwrap(),
            compaction.max_temporary_disk_bytes(),
            compaction.max_output_blob_bytes(),
            NonZeroU64::new(8 * 1024).unwrap(),
        ),
    )
    .unwrap()
}

/// What one text publication's analysis charges for `text` under the default
/// analyzer.
fn analysis_charge(text: &str) -> u64 {
    crate::search::text::analyze_text_within_budget(
        TextIndexDefinition::new_node("Doc", "body")
            .unwrap()
            .analyzer(),
        text,
        &mut crate::search::text::TextAnalysisMemoryBudget::new(NonZeroU64::MAX),
    )
    .unwrap()
    .1
    .analysis_bytes
}

/// Analysis bytes each pending-document analysis of `label` charged.
fn analyzed(label: &str) -> Vec<u64> {
    crate::search::text::PENDING_ANALYSIS_BYTES
        .lock()
        .unwrap()
        .iter()
        .filter(|(analyzed, _)| analyzed == label)
        .map(|(_, bytes)| *bytes)
        .collect()
}

/// Paused queue tuning whose strong text searches analyze at most `bound`
/// bytes.
fn strong_text_bound(bound: u64) -> IndexOperationQueueTuning {
    IndexOperationQueueTuning::default()
        .with_strong_text_search_max_analysis_bytes(NonZeroU64::new(bound).unwrap())
}

/// Opens a queued database whose strong text searches and text publications
/// both analyze at most `budget` bytes, with a `label` text index on `body`.
async fn open_with_text_index(name: &str, label: &str, budget: u64) -> HelixDB {
    let db = open(
        name,
        Arc::new(InMemory::new()),
        queued(strong_text_bound(budget))
            .with_search_index_backfill_limits(analysis_budget(budget)),
    )
    .await;
    db.install_index_for_tests(
        ValidatedDynamicIndexDefinition::try_from(
            TextIndexDefinition::new_node(label, "body").unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    db
}

/// Creates one `label` node per body, each in its own write.
async fn add_bodies(db: &HelixDB, label: &str, bodies: &[String]) -> Vec<u64> {
    let mut ids = Vec::new();
    for body in bodies {
        let created = write(db, || {
            QueryRequest::write(
                batch::write_batch()
                    .var_as(
                        "created",
                        traversal::g()
                            .add_n(label, vec![("body", PropertyInput::from(body.clone()))]),
                    )
                    .returning(["created"]),
            )
        })
        .await;
        ids.push(created["created"][0]["$id"].as_u64().unwrap());
    }
    ids
}

fn text_search_request(
    label: &str,
    term: &str,
    k: usize,
    consistency: SearchConsistency,
) -> QueryRequest {
    QueryRequest::read(
        batch::read_batch()
            .var_as(
                "hits",
                traversal::g().text_search_nodes(label, "body", term, k, None),
            )
            .returning(["hits"]),
    )
    .with_search_consistency(consistency)
    .unwrap()
}

#[tokio::test]
async fn strong_text_overlay_is_bounded_by_its_analysis_bound() {
    // A label no other test indexes keys this test's analysis log entries.
    const LABEL: &str = "OverlayBudgetDoc";
    const BUDGET: u64 = 64 * 1024;
    const DOCS: usize = 40;
    let db = open_with_text_index("overlay-text-budget", LABEL, BUDGET).await;
    // One term padded to 4 KiB, so each document alone fits the per-document
    // admission allowances and only their sum exceeds the budget.
    let body = format!("alpha{}", " ".repeat(4 * 1024 - "alpha".len()));
    add_bodies(&db, LABEL, &vec![body.clone(); DOCS]).await;
    assert_eq!(
        queue(&db, QueueFamily::Text)
            .await
            .unwrap()
            .operations()
            .len(),
        DOCS
    );
    let charge = analysis_charge(&body);
    assert!(DOCS as u64 * charge > 2 * BUDGET);

    let error = Box::pin(db.query(text_search_request(
        LABEL,
        "alpha",
        DOCS,
        SearchConsistency::Strong,
    )))
    .await
    .expect_err("strong search fails rather than analyze past the budget");
    assert!(error.is_index_backpressure(), "{error}");
    assert!(analyzed(LABEL).is_empty(), "nothing was analyzed in memory");
    assert_eq!(
        db.index_operation_queue_stats()
            .strong_text_search_rejections,
        1
    );
    // Eventual search degrades within the budget instead of failing.
    let result = Box::pin(db.query(text_search_request(
        LABEL,
        "alpha",
        DOCS,
        SearchConsistency::Eventual,
    )))
    .await
    .expect("eventual search never fails for backlog");
    assert_eq!(
        db.index_operation_queue_stats()
            .strong_text_search_rejections,
        1,
        "eventual searches are never refused"
    );
    let calls = analyzed(LABEL);
    let found = hits(&result, "hits").len() as u64;
    assert_eq!(found, BUDGET / charge);
    assert!(
        calls.len() == 1 && calls.iter().all(|bytes| *bytes <= BUDGET),
        "eventual overlay charged {calls:?} bytes; the publication budget is {BUDGET}"
    );
    db.close().await.unwrap();
}

/// The strong text bound is not what one publication drains: a publication
/// that selects less input than the bound leaves a strong search past it
/// failing until enough publications bring the backlog back within it, here
/// one per document past the bound.
#[tokio::test]
async fn a_strong_text_search_past_the_bound_waits_for_every_publication_it_needs() {
    const LABEL: &str = "OverlayNarrowInputDoc";
    const BUDGET: u64 = 64 * 1024;
    const PAST: usize = 3;
    let limits = analysis_budget(BUDGET);
    let db = open_with_text_index("overlay-text-narrow-input", LABEL, BUDGET).await;
    let body = format!("alpha{}", " ".repeat(4 * 1024 - "alpha".len()));
    let within = usize::try_from(BUDGET / analysis_charge(&body)).unwrap();
    add_bodies(&db, LABEL, &vec![body; within + PAST]).await;
    let target = target(&db, QueueFamily::Text).await;
    // Each publication's input budget is one document's, so it selects one.
    let document = queue(&db, QueueFamily::Text).await.unwrap().operations()[0].retained_bytes();
    assert!(document < BUDGET);
    let batch = limits.batch();
    let narrow = publisher_with_limits(
        &db,
        SearchIndexBatchLimits::try_new(
            batch.max_entities(),
            NonZeroU64::new(document).unwrap(),
            batch.max_output_operations(),
            batch.max_output_bytes(),
            batch.max_single_vector_output_bytes(),
        )
        .unwrap(),
        limits.active_text_mutation(),
    );
    let strong = || {
        Box::pin(db.query(text_search_request(
            LABEL,
            "alpha",
            within + PAST,
            SearchConsistency::Strong,
        )))
    };
    for published in 0..PAST {
        let error = strong()
            .await
            .expect_err("the backlog is still past the bound");
        assert!(
            error.to_string().contains("pending_text_analysis_bytes"),
            "after {published} publications: {error}"
        );
        assert_eq!(
            narrow.publish_once(target).await.unwrap(),
            PublicationOutcome::Published {
                operations: 1,
                entities: 1
            }
        );
    }
    let result = strong()
        .await
        .expect("the backlog is back within the bound");
    assert_eq!(hits(&result, "hits").len(), within + PAST);
    db.close().await.unwrap();
}

/// Short dense tokens cost far more analysis than their text: documents
/// whose text together is well within the budget still exceed it once each
/// token is charged, so strong search fails and eventual search keeps only
/// the documents whose analysis fits.
#[tokio::test]
async fn dense_token_text_is_bounded_by_its_analysis_not_its_length() {
    const LABEL: &str = "OverlayDenseDoc";
    const BUDGET: u64 = 64 * 1024;
    const DOCS: usize = 3;
    let db = open_with_text_index("overlay-text-dense", LABEL, BUDGET).await;
    let body = "a ".repeat(100);
    let ids = add_bodies(&db, LABEL, &vec![body.clone(); DOCS]).await;
    let charge = analysis_charge(&body);
    assert!(
        (DOCS * body.len()) as u64 * 100 < BUDGET && DOCS as u64 * charge > BUDGET,
        "{DOCS} documents of {} text bytes charge {charge} each",
        body.len()
    );
    let error = Box::pin(db.query(text_search_request(
        LABEL,
        "a",
        DOCS,
        SearchConsistency::Strong,
    )))
    .await
    .expect_err("dense text past the analysis budget fails strong search");
    assert!(error.is_index_backpressure(), "{error}");
    assert!(
        error
            .to_string()
            .contains("pending_text_analysis_bytes would reach "),
        "{error}"
    );
    assert!(
        error.to_string().contains(&format!("limit {BUDGET}")),
        "{error}"
    );
    assert!(analyzed(LABEL).is_empty());

    let result = Box::pin(db.query(text_search_request(
        LABEL,
        "a",
        DOCS,
        SearchConsistency::Eventual,
    )))
    .await
    .unwrap();
    let within = usize::try_from(BUDGET / charge).unwrap();
    assert!(within < DOCS);
    let mut found = hits(&result, "hits")
        .into_iter()
        .map(|(id, _)| id)
        .collect::<Vec<_>>();
    found.sort_unstable();
    assert_eq!(found, ids[..within], "eventual search keeps the oldest");
    assert_eq!(analyzed(LABEL), [within as u64 * charge]);

    // Once published, nothing is left to analyze and strong search is exact.
    drain(&db, target(&db, QueueFamily::Text).await).await;
    let result = Box::pin(db.query(text_search_request(
        LABEL,
        "a",
        DOCS,
        SearchConsistency::Strong,
    )))
    .await
    .unwrap();
    assert_eq!(hits(&result, "hits").len(), DOCS);
    db.close().await.unwrap();
}

/// Prefiltered text searches analyze every pending document of their
/// partition for corpus statistics, so the analysis bound covers them too. A
/// write batch's own documents count first: past the bound alone they fail it
/// for good, since no publication clears them.
#[tokio::test]
async fn text_analysis_bound_covers_prefiltered_searches_and_a_write_batchs_own_text() {
    const LABEL: &str = "OverlayBoundDoc";
    const BUDGET: u64 = 64 * 1024;
    const DOCS: usize = 20;
    let db = open_with_text_index("overlay-text-bound-scopes", LABEL, BUDGET).await;
    let body = format!("alpha{}", " ".repeat(4 * 1024 - "alpha".len()));
    let add = || traversal::g().add_n(LABEL, vec![("body", PropertyInput::from(body.clone()))]);
    let ids = add_bodies(&db, LABEL, &vec![body.clone(); DOCS]).await;
    let charge = analysis_charge(&body);
    let within = usize::try_from(BUDGET / charge).unwrap();
    // The first document past the bound crosses it reserving its text.
    let reached = within as u64 * charge + body.len() as u64;
    assert!(within < DOCS - 1 && reached > BUDGET);
    let past_the_bound =
        format!("pending_text_analysis_bytes would reach {reached}, limit {BUDGET}");

    // Two candidates, but statistics would analyze all twenty documents.
    let candidates = [ids[0], ids[DOCS - 1]];
    let prefiltered = |consistency| {
        QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "hits",
                    traversal::g()
                        .n(NodeRef::from(candidates.to_vec()))
                        .text_search(LABEL, "body", "alpha", 10, None),
                )
                .returning(["hits"]),
        )
        .with_search_consistency(consistency)
        .unwrap()
    };
    let error = Box::pin(db.query(prefiltered(SearchConsistency::Strong)))
        .await
        .expect_err("a prefiltered strong search analyzes its whole partition");
    assert!(error.to_string().contains(&past_the_bound), "{error}");
    // Eventual search overlays the oldest documents within the bound: the
    // first candidate is among them, the last is not yet published.
    let result = Box::pin(db.query(prefiltered(SearchConsistency::Eventual)))
        .await
        .unwrap();
    assert_eq!(
        hits(&result, "hits")
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>(),
        [ids[0]]
    );

    // A write batch searches strongly, so committed text past the bound
    // beside its own document fails it retryably, and nothing commits.
    let own_search = |documents: usize| {
        let batch = (0..documents).fold(batch::write_batch(), |batch, index| {
            batch.var_as(&format!("created{index}"), add())
        });
        QueryRequest::write(
            batch
                .var_as(
                    "hits",
                    traversal::g().text_search_nodes(LABEL, "body", "alpha", 2 * DOCS, None),
                )
                .returning(["hits"]),
        )
    };
    let error = Box::pin(db.query(own_search(1)))
        .await
        .expect_err("a write batch's search analyzes the committed backlog");
    assert!(error.is_index_backpressure(), "{error}");
    assert!(error.to_string().contains(&past_the_bound), "{error}");
    assert_eq!(
        queue(&db, QueueFamily::Text)
            .await
            .unwrap()
            .operations()
            .len(),
        DOCS,
        "nothing from the failed batch commits"
    );

    // Published work leaves only a write batch's own documents: within the
    // bound the batch searches them all strongly; past it, it can never
    // search them, so it fails for good and commits nothing.
    drain(&db, target(&db, QueueFamily::Text).await).await;
    let result = Box::pin(db.query(own_search(within))).await.unwrap();
    assert_eq!(hits(&result, "hits").len(), DOCS + within);
    drain(&db, target(&db, QueueFamily::Text).await).await;
    let error = Box::pin(db.query(own_search(within + 1)))
        .await
        .expect_err("a write batch's own text past the bound fails it");
    assert!(
        matches!(
            error,
            crate::error::HelixDbError::IndexOperationBatchTooLarge {
                resource: crate::error::IndexOperationBatchResource::PendingTextAnalysisBytes,
                observed,
                limit: BUDGET,
                ..
            } if observed == reached
        ),
        "{error}"
    );
    assert!(queue(&db, QueueFamily::Text).await.is_none());
    // The prefiltered read and the write batch past committed work were
    // refused retryably; a write's own text past the bound is not counted.
    assert_eq!(
        db.index_operation_queue_stats()
            .strong_text_search_rejections,
        2
    );
    db.close().await.unwrap();
}

/// The analysis bound counts only the searched tenant's partition: a tenant
/// past it fails strong text searches and degrades eventual ones, while a
/// tenant within it stays exact.
#[tokio::test]
async fn text_analysis_bound_counts_only_the_searched_tenant_partition() {
    const BUDGET: u64 = 64 * 1024;
    const DOCS: usize = 20;
    let db = open(
        "overlay-text-bound-tenants",
        Arc::new(InMemory::new()),
        queued(strong_text_bound(BUDGET))
            .with_search_index_backfill_limits(analysis_budget(BUDGET)),
    )
    .await;
    install(&db, Some("tenant")).await;
    let body = format!("alpha{}", " ".repeat(4 * 1024 - "alpha".len()));
    let mut crowded = Vec::new();
    for _ in 0..DOCS {
        crowded.push(add(&db, [0.0, 0.0], &body, Some("crowded")).await);
    }
    let quiet = add(&db, [0.0, 0.0], &body, Some("quiet")).await;
    let charge = analysis_charge(&body);
    let within = usize::try_from(BUDGET / charge).unwrap();
    // The first document past the bound crosses it reserving its text.
    let reached = within as u64 * charge + body.len() as u64;
    assert!(within < DOCS && reached > BUDGET);

    let strong = QueryRequest::read(
        batch::read_batch()
            .var_as(
                "hits",
                traversal::g().text_search_nodes(
                    "Doc",
                    "body",
                    "alpha",
                    2 * DOCS,
                    Some(PropertyValue::from("crowded")),
                ),
            )
            .returning(["hits"]),
    );
    let error = Box::pin(db.query(strong))
        .await
        .expect_err("the crowded tenant's backlog exceeds the bound");
    assert!(
        error.to_string().contains(&format!(
            "pending_text_analysis_bytes would reach {reached}, limit {BUDGET}"
        )),
        "{error}"
    );
    assert_eq!(
        text_search(
            &db,
            "alpha",
            2 * DOCS,
            Some("quiet"),
            SearchConsistency::Strong
        )
        .await
        .into_iter()
        .map(|(id, _)| id)
        .collect::<Vec<_>>(),
        [quiet]
    );
    let mut found = text_search(
        &db,
        "alpha",
        2 * DOCS,
        Some("crowded"),
        SearchConsistency::Eventual,
    )
    .await
    .into_iter()
    .map(|(id, _)| id)
    .collect::<Vec<_>>();
    found.sort_unstable();
    let mut oldest = crowded[..within].to_vec();
    oldest.sort_unstable();
    assert_eq!(found, oldest, "eventual search keeps the oldest documents");
    db.close().await.unwrap();
}

/// Text the index worker holds back still counts toward the strong text
/// analysis bound, although no publication drains it: with publication
/// limits and the bound lowered below a queued document, strong text searches
/// of its partition fail with backpressure that no retry or publication
/// attempt clears, eventual ones serve the published index, and a rewrite
/// that publishes makes strong search exact again.
#[tokio::test]
async fn held_back_text_counts_toward_the_strong_text_analysis_bound() {
    const LABEL: &str = "OverlayHeldBackDoc";
    const BUDGET: u64 = 64 * 1024;
    let name = "overlay-text-held-back";
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        name,
        Arc::clone(&store),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    db.install_index_for_tests(
        ValidatedDynamicIndexDefinition::try_from(
            TextIndexDefinition::new_node(LABEL, "body").unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    // Admitted under the default limits, but its analysis alone exceeds the
    // lowered bound.
    let body = (0..400)
        .map(|term| format!("held{term}"))
        .collect::<Vec<_>>()
        .join(" ");
    assert!(analysis_charge(&body) > BUDGET);
    let [held]: [u64; 1] = add_bodies(&db, LABEL, &[body])
        .await
        .try_into()
        .expect("one document");
    db.close().await.unwrap();

    let db = open(
        name,
        store,
        queued(strong_text_bound(BUDGET))
            .with_search_index_backfill_limits(analysis_budget(BUDGET)),
    )
    .await;
    let target = target(&db, QueueFamily::Text).await;
    let publisher = db.index_queue_publisher().expect("writer runs a publisher");
    assert_eq!(
        publisher.publish_once(target).await.unwrap(),
        PublicationOutcome::Blocked
    );
    assert_eq!(db.blocked_index_entity_count(), 1);
    let search =
        |term, consistency| Box::pin(db.query(text_search_request(LABEL, term, 10, consistency)));
    for _ in 0..2 {
        let error = search("held0", SearchConsistency::Strong)
            .await
            .expect_err("held-back text alone exceeds the bound");
        assert!(
            error.is_index_backpressure()
                && error.to_string().contains("pending_text_analysis_bytes"),
            "{error}"
        );
        assert_eq!(
            publisher.publish_once(target).await.unwrap(),
            PublicationOutcome::Stalled
        );
    }
    let eventual = search("held0", SearchConsistency::Eventual)
        .await
        .expect("eventual search never fails for backlog");
    assert!(
        hits(&eventual, "hits").is_empty(),
        "eventual search serves the published index"
    );

    write(&db, || {
        QueryRequest::write(
            batch::write_batch().var_as(
                "updated",
                traversal::g()
                    .n(NodeRef::from(held))
                    .set_property("body", "tiny".to_string()),
            ),
        )
    })
    .await;
    assert_eq!(
        publisher.publish_once(target).await.unwrap(),
        PublicationOutcome::Published {
            operations: 2,
            entities: 1
        }
    );
    assert_eq!(db.blocked_index_entity_count(), 0);
    let repaired = search("tiny", SearchConsistency::Strong).await.unwrap();
    assert_eq!(
        hits(&repaired, "hits")
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>(),
        [held]
    );
    let replaced = search("held0", SearchConsistency::Strong).await.unwrap();
    assert!(hits(&replaced, "hits").is_empty());
    db.close().await.unwrap();
}

/// The strong text bound is its own setting, not the publication budget: a
/// strong search stays exact past what one publication analyzes while
/// eventual search still overlays only that much. Its analyses are reused by
/// later searches, only documents holding a query term are indexed in
/// memory, and the results equal the published index's after publication
/// and a restart.
#[tokio::test]
async fn strong_text_search_stays_exact_past_the_publication_budget() {
    const LABEL: &str = "OverlayStrongBoundDoc";
    const BUDGET: u64 = 64 * 1024;
    const DOCS: usize = 40;
    let name = "overlay-text-strong-bound";
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = || {
        queued(IndexOperationQueueTuning::default())
            .with_search_index_backfill_limits(analysis_budget(BUDGET))
    };
    let db = open(name, Arc::clone(&store), config()).await;
    db.install_index_for_tests(
        ValidatedDynamicIndexDefinition::try_from(
            TextIndexDefinition::new_node(LABEL, "body").unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let body = format!("alpha{}", " ".repeat(4 * 1024 - "alpha".len()));
    let ids = add_bodies(&db, LABEL, &vec![body.clone(); DOCS]).await;
    let charge = analysis_charge(&body);
    assert!(DOCS as u64 * charge > 2 * BUDGET);
    async fn search(db: &HelixDB, term: &str, consistency: SearchConsistency) -> Vec<(u64, u64)> {
        let request = text_search_request(LABEL, term, DOCS, consistency);
        hits(&Box::pin(db.query(request)).await.unwrap(), "hits")
    }

    let pending = search(&db, "alpha", SearchConsistency::Strong).await;
    let mut found = pending.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    found.sort_unstable();
    assert_eq!(
        found, ids,
        "strong search is exact past the publication budget"
    );
    assert_eq!(
        db.pending_text_analyses().held_bytes(),
        DOCS as u64 * charge
    );
    assert_eq!(analyzed(LABEL), [DOCS as u64 * charge]);
    // Reused, not analyzed again: the cache is unchanged and the result too.
    assert_eq!(
        search(&db, "alpha", SearchConsistency::Strong).await,
        pending
    );
    assert_eq!(
        db.pending_text_analyses().held_bytes(),
        DOCS as u64 * charge
    );
    // No document holds the term, so nothing is indexed in memory.
    assert!(search(&db, "missing", SearchConsistency::Strong)
        .await
        .is_empty());
    assert_eq!(
        analyzed(LABEL).len(),
        2,
        "only matching documents are indexed"
    );
    // Eventual search overlays one publication's analysis and serves the
    // rest as published (nothing yet).
    let eventual = search(&db, "alpha", SearchConsistency::Eventual).await;
    assert_eq!(eventual.len() as u64, BUDGET / charge);
    assert_eq!(
        db.index_operation_queue_stats()
            .strong_text_search_rejections,
        0
    );

    // A restart empties the cache; the overlay is rebuilt identically.
    db.close().await.unwrap();
    let db = open(name, Arc::clone(&store), config()).await;
    assert_eq!(db.pending_text_analyses().held_bytes(), 0);
    assert_eq!(
        search(&db, "alpha", SearchConsistency::Strong).await,
        pending
    );

    // Publishing the whole queue drops the generation's cached analyses, and
    // the published documents score the same.
    drain(&db, target(&db, QueueFamily::Text).await).await;
    assert_eq!(db.pending_text_analyses().held_bytes(), 0);
    assert_eq!(
        search(&db, "alpha", SearchConsistency::Strong).await,
        pending
    );
    assert_eq!(db.pending_text_analyses().held_bytes(), 0);
    db.close().await.unwrap();
}

/// The strong text bound admits a backlog charged exactly the bound and
/// refuses one byte less, whether the search analyzes the backlog or reuses
/// analyses a refused search left, reporting the whole backlog's charge
/// either way. Eventual searches answer the same whatever the cache holds.
#[tokio::test]
async fn the_strong_text_bound_admits_exactly_its_charge_cold_and_warm() {
    const LABEL: &str = "OverlayBoundaryDoc";
    const DOCS: usize = 6;
    let name = "overlay-text-boundary";
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let bodies = (0..DOCS)
        .map(|ordinal| format!("alpha term{ordinal}{}", " beta".repeat(ordinal)))
        .collect::<Vec<_>>();
    let total = bodies.iter().map(|body| analysis_charge(body)).sum::<u64>();
    let open_bounded =
        |bound: u64| open(name, Arc::clone(&store), queued(strong_text_bound(bound)));
    async fn search(
        db: &HelixDB,
        consistency: SearchConsistency,
    ) -> crate::Result<Vec<(u64, u64)>> {
        Box::pin(db.query(text_search_request(LABEL, "alpha", DOCS, consistency)))
            .await
            .map(|result| hits(&result, "hits"))
    }

    let db = open_bounded(total).await;
    db.install_index_for_tests(
        ValidatedDynamicIndexDefinition::try_from(
            TextIndexDefinition::new_node(LABEL, "body").unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    add_bodies(&db, LABEL, &bodies).await;
    let eventual = search(&db, SearchConsistency::Eventual).await.unwrap();
    assert_eq!(db.pending_text_analyses().held_bytes(), 0);
    let cold = search(&db, SearchConsistency::Strong).await.unwrap();
    assert_eq!(cold.len(), DOCS);
    assert_eq!(db.pending_text_analyses().held_bytes(), total);
    assert_eq!(search(&db, SearchConsistency::Strong).await.unwrap(), cold);
    assert_eq!(
        search(&db, SearchConsistency::Eventual).await.unwrap(),
        eventual,
        "a warm cache leaves eventual results unchanged"
    );
    assert_eq!(eventual, cold, "everything fits the eventual budget too");
    db.close().await.unwrap();

    let db = open_bounded(total - 1).await;
    for attempt in ["analyzed", "reused"] {
        let error = search(&db, SearchConsistency::Strong)
            .await
            .expect_err("one byte past the bound");
        assert!(
            matches!(
                error,
                crate::error::HelixDbError::IndexBackpressure {
                    resource: crate::error::IndexBackpressureResource::PendingTextAnalysisBytes,
                    requested,
                    limit,
                    ..
                } if requested == total && limit == total - 1
            ),
            "{attempt}: {error}"
        );
        // The refused search kept every analysis it reached for its retry.
        let reached = bodies[..DOCS - 1]
            .iter()
            .map(|body| analysis_charge(body))
            .sum::<u64>();
        assert_eq!(
            db.pending_text_analyses().held_bytes(),
            reached,
            "{attempt}"
        );
    }
    assert_eq!(
        db.index_operation_queue_stats()
            .strong_text_search_rejections,
        2
    );
    assert_eq!(
        search(&db, SearchConsistency::Eventual).await.unwrap(),
        eventual
    );
    db.close().await.unwrap();
}

/// A write batch's text search reads its own staged documents, never an
/// analysis cached for the committed document it replaced, and caches none
/// of its own: the committed operation it superseded is dropped, the rest
/// are kept, and scores equal a search that analyzes everything afresh.
#[tokio::test]
async fn a_warm_cache_never_hides_a_write_batchs_own_text() {
    let db = open(
        "overlay-text-cache-local",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&db, None).await;
    let first = add(&db, [0.0, 0.0], "alpha one", None).await;
    let second = add(&db, [1.0, 0.0], "alpha two", None).await;
    let text_target = target(&db, QueueFamily::Text).await;
    let cached = || {
        db.pending_text_analyses().cached(
            text_target,
            &crate::index_lifecycle::work::TextPartition::Unpartitioned,
        )
    };
    assert_eq!(
        sorted_ids(text_search(&db, "alpha", 10, None, SearchConsistency::Strong).await),
        [first, second]
    );
    assert_eq!(cached(), 2);

    let result = write(&db, || {
        QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "replaced",
                    traversal::g()
                        .n(NodeRef::from(first))
                        .set_property("body", "beta replaced".to_string()),
                )
                .var_as(
                    "created",
                    traversal::g().add_n(
                        "Doc",
                        vec![
                            ("embedding", PropertyInput::from(vec![2.0_f32, 0.0])),
                            ("body", PropertyInput::from("alpha local".to_string())),
                        ],
                    ),
                )
                .var_as(
                    "alpha",
                    traversal::g().text_search_nodes("Doc", "body", "alpha", 10, None),
                )
                .var_as(
                    "beta",
                    traversal::g().text_search_nodes("Doc", "body", "beta", 10, None),
                )
                .returning(["created", "alpha", "beta"]),
        )
    })
    .await;
    let created = result["created"][0]["$id"].as_u64().unwrap();
    assert_eq!(sorted_ids(hits(&result, "alpha")), [second, created]);
    assert_eq!(sorted_ids(hits(&result, "beta")), [first]);
    assert_eq!(cached(), 1, "only the committed operation it left in place");

    let warm = text_search(&db, "alpha beta", 10, None, SearchConsistency::Strong).await;
    assert_eq!(sorted_ids(warm.clone()), [first, second, created]);
    assert_eq!(cached(), 3);
    db.pending_text_analyses().forget(text_target);
    assert_eq!(
        text_search(&db, "alpha beta", 10, None, SearchConsistency::Strong).await,
        warm,
        "reused analyses score exactly as fresh ones"
    );
    db.close().await.unwrap();
}

/// The index worker drops a generation's cached analyses once nothing of it
/// is queued, without another search: a publication that leaves work queued
/// keeps them for the searches that still need them.
#[tokio::test]
async fn publication_releases_cached_analyses_once_the_queue_drains() {
    let db = open(
        "overlay-text-cache-drain",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    db.install_index_for_tests(
        ValidatedDynamicIndexDefinition::try_from(
            TextIndexDefinition::new_node("Doc", "body").unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    for ordinal in 0..3 {
        add(&db, [0.0, 0.0], &format!("alpha {ordinal}"), None).await;
    }
    let text_target = target(&db, QueueFamily::Text).await;
    let found = text_search(&db, "alpha", 10, None, SearchConsistency::Strong).await;
    assert_eq!(found.len(), 3);
    let held = db.pending_text_analyses().held_bytes();
    assert!(held > 0);

    // An input budget below any operation: one operation per publication.
    let narrow = publisher_with_limits(
        &db,
        super::publication_tests::batch_limits(1, 32_768),
        DbConfig::new()
            .search_index_backfill()
            .active_text_mutation(),
    );
    assert_eq!(
        narrow.publish_once(text_target).await.unwrap(),
        PublicationOutcome::Published {
            operations: 1,
            entities: 1
        }
    );
    assert_eq!(
        db.pending_text_analyses().held_bytes(),
        held,
        "work stays queued"
    );
    drain(&db, text_target).await;
    assert_eq!(db.pending_text_analyses().held_bytes(), 0);
    assert_eq!(
        text_search(&db, "alpha", 10, None, SearchConsistency::Strong).await,
        found
    );
    db.close().await.unwrap();
}

/// A strong search that finds its generation's queue empty drops what is
/// still cached for that generation: here what a search pinned to a view
/// from before the drain cached again after the index worker released it.
/// Eventual searches keep it, and other generations' entries stay.
#[tokio::test]
async fn a_strong_search_of_a_drained_queue_releases_its_generations_analyses() {
    let db = open(
        "overlay-text-cache-empty-queue",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&db, None).await;
    for ordinal in 0..3 {
        add(&db, [0.0, 0.0], &format!("alpha {ordinal}"), None).await;
    }
    let text_target = target(&db, QueueFamily::Text).await;
    let next_generation = QueueTarget::new(
        text_target.scope,
        text_target.index_id,
        crate::index_lifecycle::IndexGenerationId::new(text_target.generation.get() + 1).unwrap(),
    );
    let all = crate::index_lifecycle::work::TextPartition::Unpartitioned;
    let cache = db.pending_text_analyses();
    let found = text_search(&db, "alpha", 10, None, SearchConsistency::Strong).await;
    assert_eq!(found.len(), 3);
    let stale = cache
        .get(text_target, &all)
        .expect("the strong search cached its selection");
    let held = cache.held_bytes();
    drain(&db, text_target).await;
    assert!(queue(&db, QueueFamily::Text).await.is_none());
    assert_eq!(cache.held_bytes(), 0);
    let recache = || cache.replace(text_target, &all, (*stale).clone());
    recache();
    cache.replace(next_generation, &all, (*stale).clone());
    assert_eq!(cache.held_bytes(), 2 * held);

    assert_eq!(
        text_search(&db, "alpha", 10, None, SearchConsistency::Eventual).await,
        found
    );
    assert_eq!(
        cache.held_bytes(),
        2 * held,
        "eventual searches never release"
    );
    assert_eq!(
        text_search(&db, "alpha", 10, None, SearchConsistency::Strong).await,
        found
    );
    assert_eq!(cache.cached(text_target, &all), 0);
    assert_eq!(cache.cached(next_generation, &all), 3, "another generation");
    assert_eq!(cache.held_bytes(), held);

    // A write batch's search is strong and releases the same way.
    recache();
    let result = write(&db, || {
        QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "hits",
                    traversal::g().text_search_nodes("Doc", "body", "alpha", 10, None),
                )
                .returning(["hits"]),
        )
    })
    .await;
    assert_eq!(hits(&result, "hits"), found);
    assert_eq!(cache.cached(text_target, &all), 0);
    assert_eq!(cache.held_bytes(), held);
    db.close().await.unwrap();
}

/// A reader handle has no index worker, so a strong search that finds the
/// queue drained is what releases the analyses its earlier strong searches
/// cached.
#[tokio::test]
async fn a_reader_releases_cached_analyses_once_it_reads_the_queue_drained() {
    let name = "overlay-text-cache-reader";
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = open(
        name,
        Arc::clone(&store),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&writer, None).await;
    for ordinal in 0..3 {
        add(&writer, [0.0, 0.0], &format!("alpha {ordinal}"), None).await;
    }
    let text_target = target(&writer, QueueFamily::Text).await;
    let reader = HelixDB::open_reader_with_object_store_for_tests(name, Arc::clone(&store))
        .await
        .unwrap();
    let all = crate::index_lifecycle::work::TextPartition::Unpartitioned;
    let found = text_search(&reader, "alpha", 10, None, SearchConsistency::Strong).await;
    assert_eq!(
        found,
        text_search(&writer, "alpha", 10, None, SearchConsistency::Strong).await
    );
    assert_eq!(reader.pending_text_analyses().cached(text_target, &all), 3);
    let held = reader.pending_text_analyses().held_bytes();

    drain(&writer, text_target).await;
    // The reader replays the writer's WAL in order, so once it sees a later
    // vector-only write it also sees the drained text queue.
    let marker = write(&writer, || {
        QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "created",
                    traversal::g().add_n(
                        "Doc",
                        vec![("embedding", PropertyInput::from(vec![9.0_f32, 9.0]))],
                    ),
                )
                .returning(["created"]),
        )
    })
    .await["created"][0]["$id"]
        .as_u64()
        .unwrap();
    assert!(queue(&writer, QueueFamily::Text).await.is_none());
    writer.flush_writer().await.unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !vector_search(&reader, [9.0, 9.0], 1, None, SearchConsistency::Strong)
        .await
        .iter()
        .any(|(id, _)| *id == marker)
    {
        assert!(
            std::time::Instant::now() < deadline,
            "reader never caught up"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    assert_eq!(
        text_search(&reader, "alpha", 10, None, SearchConsistency::Eventual).await,
        found
    );
    assert_eq!(reader.pending_text_analyses().held_bytes(), held);
    assert_eq!(
        text_search(&reader, "alpha", 10, None, SearchConsistency::Strong).await,
        found
    );
    assert_eq!(reader.pending_text_analyses().held_bytes(), 0);
    reader.close().await.unwrap();
    writer.close().await.unwrap();
}

/// Updates, deletes, and re-inserts after the cache is warm replace cached
/// analyses by operation: every strong search answers from each entity's
/// latest committed document, scored exactly as publication scores it.
#[tokio::test]
async fn cached_text_analyses_follow_updates_deletes_and_reinserts() {
    let db = open(
        "overlay-text-cache-churn",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&db, None).await;
    let strong = |term: &'static str| text_search(&db, term, 10, None, SearchConsistency::Strong);
    let ids_of = |found: Vec<(u64, u64)>| {
        let mut ids = found.into_iter().map(|(id, _)| id).collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    };
    let text_target = target(&db, QueueFamily::Text).await;
    let cached = || {
        db.pending_text_analyses().cached(
            text_target,
            &crate::index_lifecycle::work::TextPartition::Unpartitioned,
        )
    };
    let first = add(&db, [0.0, 0.0], "alpha", None).await;
    let second = add(&db, [1.0, 0.0], "alpha beta", None).await;
    assert_eq!(ids_of(strong("alpha").await), [first, second]);
    assert_eq!(cached(), 2);

    update(&db, first, [0.0, 0.0], "gamma").await;
    assert_eq!(ids_of(strong("alpha").await), [second]);
    assert_eq!(ids_of(strong("gamma").await), [first]);
    assert_eq!(
        cached(),
        2,
        "the superseded operation's analysis is dropped"
    );

    delete(&db, second).await;
    assert!(strong("alpha").await.is_empty());
    assert_eq!(cached(), 1, "a deleted entity has nothing to analyze");

    let third = add(&db, [2.0, 0.0], "alpha delta", None).await;
    let before = strong("alpha").await;
    assert_eq!(ids_of(before.clone()), [third]);
    let gamma = strong("gamma").await;
    drain(&db, text_target).await;
    assert_eq!(
        strong("alpha").await,
        before,
        "publication keeps every score"
    );
    assert_eq!(strong("gamma").await, gamma);
    assert_eq!(cached(), 0);
    db.close().await.unwrap();
}

/// Strong text searches racing writes and automatic publication share the
/// cache: each one sees every document committed before it started,
/// whichever search last replaced the cached analyses.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_strong_text_searches_stay_exact_while_publication_runs() {
    const WRITES: usize = 60;
    const SEARCHERS: usize = 3;
    let db = Arc::new(
        open(
            "overlay-text-cache-concurrent",
            Arc::new(InMemory::new()),
            DbConfig::new(),
        )
        .await,
    );
    install(&db, None).await;
    let committed = Arc::new(std::sync::Mutex::new(Vec::<(u64, String)>::new()));
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let searchers = (0..SEARCHERS)
        .map(|searcher| {
            let (db, committed, done) =
                (Arc::clone(&db), Arc::clone(&committed), Arc::clone(&done));
            tokio::spawn(async move {
                let mut checked = 0;
                while !done.load(std::sync::atomic::Ordering::SeqCst) {
                    let visible = committed.lock().unwrap().clone();
                    let Some((id, term)) = visible.get((checked + searcher) % visible.len().max(1))
                    else {
                        tokio::task::yield_now().await;
                        continue;
                    };
                    let found = text_search(&db, term, 10, None, SearchConsistency::Strong).await;
                    assert_eq!(
                        found.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                        [*id],
                        "{term} was committed before the search"
                    );
                    let shared = text_search(&db, "shared", 100, None, SearchConsistency::Strong)
                        .await
                        .len();
                    assert!(shared >= visible.len(), "{shared} < {}", visible.len());
                    checked += 1;
                }
                checked
            })
        })
        .collect::<Vec<_>>();
    for ordinal in 0..WRITES {
        let term = format!("unique{ordinal}");
        let id = add(&db, [ordinal as f32, 0.0], &format!("shared {term}"), None).await;
        committed.lock().unwrap().push((id, term));
        tokio::task::yield_now().await;
    }
    done.store(true, std::sync::atomic::Ordering::SeqCst);
    for searcher in searchers {
        assert!(
            searcher.await.unwrap() > 0,
            "every searcher checked a write"
        );
    }
    db.close().await.unwrap();
}
