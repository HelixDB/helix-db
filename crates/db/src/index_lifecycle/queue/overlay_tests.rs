//! Strong and eventual pending-data search overlays through public queries.

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
use super::tests::{open, queue, queued, target};
use super::QueueTarget;
use crate::config::{
    DbConfig, IndexOperationQueueTuning, SecondaryIndexDefinition, TextIndexDefinition,
    VectorIndexDefinition,
};
use crate::encoding::v2::values::indexes::operation_queue::QueueFamily;
use crate::index_lifecycle::ValidatedDynamicIndexDefinition;
use crate::search::vector::VectorDistanceMetric;
use crate::HelixDB;

async fn install(db: &HelixDB, tenant: Option<&str>) {
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
            | PublicationOutcome::Blocked) => panic!("publication stalled: {outcome:?}"),
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
async fn add_many(db: &HelixDB, embeddings: impl IntoIterator<Item = [f32; 2]>) -> Vec<u64> {
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
