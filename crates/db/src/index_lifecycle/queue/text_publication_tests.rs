//! Text publication from queued payloads against an independent BM25 reference:
//! an initial build over the same final documents.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use helix_ast::{
    batch, graph::NodeRef, query::QueryRequest, traversal, value::PropertyInput,
    value::PropertyValue,
};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

use super::publication::{PublicationOutcome, QueuePublisher};
use super::tests::{
    add_text, all_keys, node_count, open, publisher_with_limits, queue, queued,
    release_within_operand_bound, target,
};
use super::QueueTarget;
use crate::config::{
    ActiveTextMutationLimits, DbConfig, IndexOperationQueueTuning, QueueLayout,
    SearchIndexBackfillLimits, SearchIndexBatchLimits, TextBackfillCompactionLimits,
    TextBuildArtifactLimits, TextIndexDefinition,
};
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{ManagedIndexKey, ScopedKey};
use crate::encoding::v2::values::indexes::operation_queue::QueueFamily;
use crate::error::{ActiveTextMutationResource, HelixDbError};
use crate::index_lifecycle::ValidatedDynamicIndexDefinition;
use crate::HelixDB;

fn publisher(db: &HelixDB) -> &Arc<QueuePublisher> {
    db.index_queue_publisher()
        .expect("writer runs an automatic publisher")
}

async fn install(db: &HelixDB, tenant: Option<&str>) {
    let definition = TextIndexDefinition::new_node("Doc", "body").unwrap();
    let definition = match tenant {
        Some(tenant) => definition.with_tenant_property(tenant).unwrap(),
        None => definition,
    };
    db.install_index_for_tests(ValidatedDynamicIndexDefinition::try_from(definition).unwrap())
        .await
        .unwrap();
}

async fn drain(db: &HelixDB, target: QueueTarget) -> (u64, u64) {
    let (mut operations_total, mut batches) = (0, 0);
    for _ in 0..1_000 {
        match publisher(db).publish_once(target).await.unwrap() {
            PublicationOutcome::Published { operations, .. } => {
                operations_total += operations;
                batches += 1;
            }
            PublicationOutcome::Trimmed => {}
            PublicationOutcome::Empty => return (operations_total, batches),
            outcome @ (PublicationOutcome::Discarded { .. }
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry
            | PublicationOutcome::Blocked) => panic!("publication did not progress: {outcome:?}"),
        }
    }
    panic!("publication did not drain")
}

/// Ordered `(id, score bits)` hits; score bits make BM25 parity exact.
async fn search(db: &HelixDB, text: &str, k: usize, tenant: Option<&str>) -> Vec<(u64, u64)> {
    let result = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "hits",
                    traversal::g().text_search_nodes(
                        "Doc",
                        "body",
                        text,
                        k,
                        tenant.map(PropertyValue::from),
                    ),
                )
                .returning(["hits"]),
        ))
        .await
        .unwrap();
    if result["hits"].is_null() {
        return Vec::new();
    }
    result["hits"]
        .as_array()
        .unwrap_or_else(|| panic!("text search returned {result}"))
        .iter()
        .map(|hit| {
            (
                hit["$id"].as_u64().unwrap(),
                hit["$score"].as_f64().map_or(0, f64::to_bits),
            )
        })
        .collect()
}

/// Executes a write, retrying retryable transaction conflicts like a client.
async fn write(db: &HelixDB, request: impl Fn() -> QueryRequest) -> serde_json::Value {
    for _ in 0..100 {
        match db.query(request()).await {
            Ok(result) => return result,
            Err(error) if error.is_transaction_conflict() => {}
            Err(error) => panic!("write failed: {error}"),
        }
    }
    panic!("write kept conflicting")
}

async fn add(db: &HelixDB, body: &str, tenant: Option<&str>) -> u64 {
    let result = write(db, || {
        let mut properties = vec![("body", PropertyInput::from(body.to_string()))];
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

async fn set(db: &HelixDB, id: u64, property: &str, value: &str) {
    write(db, || {
        QueryRequest::write(
            batch::write_batch().var_as(
                "updated",
                traversal::g()
                    .n(NodeRef::from(id))
                    .set_property(property, value.to_string()),
            ),
        )
    })
    .await;
}

async fn drop_node(db: &HelixDB, id: u64) {
    write(db, || {
        QueryRequest::write(
            batch::write_batch().var_as("dropped", traversal::g().n(NodeRef::from(id)).drop()),
        )
    })
    .await;
}

/// Maps node IDs to workload insertion ordinals so databases compare by
/// document rather than by raw ID.
fn ordinals(hits: Vec<(u64, u64)>, ids: &[u64]) -> Vec<(usize, u64)> {
    hits.into_iter()
        .map(|(id, score)| {
            (
                ids.iter()
                    .position(|candidate| *candidate == id)
                    .expect("every hit is a workload document"),
                score,
            )
        })
        .collect()
}

/// Applies one deterministic workload covering inserts, replacements that stop
/// matching, empty text, deletes, and a hot document.
async fn workload(db: &HelixDB) -> Vec<u64> {
    let mut ids = Vec::new();
    for body in [
        "rust storage engine",
        "rust planner",
        "graph storage storage",
        "",
        "vector search engine",
        "text search with rust",
    ] {
        ids.push(add(db, body, None).await);
    }
    set(db, ids[1], "body", "python planner").await;
    set(db, ids[3], "body", "late rust arrival").await;
    drop_node(db, ids[4]).await;
    for revision in 0..6 {
        set(
            db,
            ids[2],
            "body",
            &format!("hot storage revision {revision} rust"),
        )
        .await;
    }
    set(db, ids[5], "body", "").await;
    ids
}

const QUERIES: [&str; 6] = ["rust", "storage", "planner", "search", "revision", "engine"];

#[tokio::test]
async fn published_text_matches_an_independent_build_bm25_exactly() {
    let reference = open("text-reference", Arc::new(InMemory::new()), DbConfig::new()).await;
    let reference_ids = workload(&reference).await;
    install(&reference, None).await;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "text-queued",
        store,
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&db, None).await;
    let ids = workload(&db).await;
    let target = target(&db, QueueFamily::Text).await;
    assert_eq!(
        queue(&db, QueueFamily::Text)
            .await
            .unwrap()
            .operations()
            .len(),
        16
    );
    let (published, batches) = drain(&db, target).await;
    assert_eq!(published, 16);
    assert_eq!(batches, 1, "one bounded epoch published every entity");

    for query in QUERIES {
        assert_eq!(
            ordinals(search(&db, query, 10, None).await, &ids),
            ordinals(search(&reference, query, 10, None).await, &reference_ids),
            "query {query:?} ranks and scores identically"
        );
    }
    // The replacement that stopped matching, the deleted node, and the
    // emptied document never surface.
    let rust = search(&db, "rust", 10, None)
        .await
        .into_iter()
        .map(|(id, _)| id)
        .collect::<Vec<_>>();
    assert!(!rust.contains(&ids[1]) && !rust.contains(&ids[5]));
    assert!(search(&db, "vector", 10, None).await.is_empty());
    reference.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn one_epoch_builds_one_split_per_partition() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "text-batching",
        store,
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&db, None).await;
    for index in 0..50 {
        add(&db, &format!("document number {index} shared"), None).await;
    }
    let target = target(&db, QueueFamily::Text).await;
    let before = all_keys(&db).await;
    assert_eq!(drain(&db, target).await, (50, 1));
    let pages = all_keys(&db)
        .await
        .difference(&before)
        .filter(|key| {
            matches!(
                ManagedIndexKey::parse_data_from_slice(key),
                Ok(ManagedIndexKey::Data {
                    kind: ScopedKey::TextManifestPage(_),
                    ..
                })
            )
        })
        .count();
    assert_eq!(pages, 1, "fifty documents share one appended split page");
    assert_eq!(search(&db, "shared", 100, None).await.len(), 50);
    db.close().await.unwrap();
}

#[tokio::test]
async fn tenant_moves_retire_the_old_partition_and_update_statistics() {
    let reference = open(
        "text-tenant-reference",
        Arc::new(InMemory::new()),
        DbConfig::new(),
    )
    .await;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "text-tenant",
        store,
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&db, Some("tenant")).await;
    let mut ids = Vec::new();
    for database in [&reference, &db] {
        let moved = add(database, "shared moving words", Some("a")).await;
        let resident = add(database, "shared resident words", Some("a")).await;
        let other = add(database, "shared other words", Some("b")).await;
        set(database, moved, "tenant", "b").await;
        set(database, moved, "body", "shared moved words updated").await;
        ids.push(vec![moved, resident, other]);
    }
    // The reference indexes the final rows through an initial build.
    install(&reference, Some("tenant")).await;
    let target = target(&db, QueueFamily::Text).await;
    drain(&db, target).await;
    for tenant in ["a", "b"] {
        for query in ["shared", "moving", "moved", "words"] {
            assert_eq!(
                ordinals(search(&db, query, 10, Some(tenant)).await, &ids[1]),
                ordinals(search(&reference, query, 10, Some(tenant)).await, &ids[0]),
                "tenant {tenant} query {query}"
            );
        }
    }
    reference.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn failed_text_publication_commits_no_statistics_manifest_or_acknowledgement() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "text-failure",
        store,
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&db, None).await;
    add(&db, "alpha beta", None).await;
    add(&db, "beta gamma", None).await;
    let target = target(&db, QueueFamily::Text).await;
    let before = all_keys(&db).await;
    publisher(&db)
        .hooks()
        .fail_before_commit
        .store(true, Ordering::SeqCst);
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Retry
    );
    assert_eq!(
        all_keys(&db).await,
        before,
        "no row committed without its ACK"
    );
    assert_eq!(
        queue(&db, QueueFamily::Text)
            .await
            .unwrap()
            .operations()
            .len(),
        2
    );
    assert_eq!(
        db.index_operation_backlog()
            .usage(DataScope::LegacyUnscoped, target.index_id)
            .operations,
        2
    );
    assert_eq!(drain(&db, target).await.0, 2);
    assert_eq!(search(&db, "beta", 10, None).await.len(), 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn text_acknowledgements_fit_the_producer_operand_bound() {
    // A 1 KiB operand bound admits 63 acknowledged IDs: a 5-byte header plus
    // 16 bytes per ID.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "text-ack-bound",
        store,
        queued(
            IndexOperationQueueTuning::default()
                .with_max_operand_bytes(NonZeroU64::new(1024).unwrap()),
        ),
    )
    .await;
    install(&db, None).await;
    for index in 0..100 {
        add(&db, &format!("document {index}"), None).await;
    }
    let target = target(&db, QueueFamily::Text).await;
    assert_eq!(
        release_within_operand_bound(&db, target, QueueFamily::Text).await,
        100
    );
    assert_eq!(search(&db, "document", 200, None).await.len(), 100);
    db.close().await.unwrap();
}

#[tokio::test]
async fn text_publication_counts_acknowledgement_writes_in_its_budget() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "text-ack-budget",
        store,
        queued(IndexOperationQueueTuning::default().with_layout(QueueLayout::Rows)),
    )
    .await;
    install(&db, None).await;
    let hot = add(&db, "hot revision 0", None).await;
    for revision in 1..100 {
        set(&db, hot, "body", &format!("hot revision {revision}")).await;
    }
    let target = target(&db, QueueFamily::Text).await;
    // Row acknowledgements delete one row per operation: 100 of them exceed
    // a 64-write transaction however few rows the collapsed epoch writes.
    // Each selection names at most half of it, leaving the epoch room, so no
    // attempt is spent on an acknowledgement that fills the budget.
    let defaults = SearchIndexBackfillLimits::default();
    let batch = SearchIndexBatchLimits::try_new(
        NonZeroUsize::new(512).unwrap(),
        NonZeroU64::new(8 * 1024 * 1024).unwrap(),
        NonZeroU64::new(64).unwrap(),
        NonZeroU64::new(8 * 1024 * 1024).unwrap(),
        NonZeroU64::new(8 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    let narrow = publisher_with_limits(
        &db,
        batch,
        ActiveTextMutationLimits::from_backfill(
            SearchIndexBackfillLimits::try_new(
                batch,
                defaults.edge_property_read_batch(),
                defaults.text_artifacts(),
                defaults.text_compaction(),
            )
            .unwrap(),
        ),
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
            | PublicationOutcome::Blocked) => panic!("publication wasted an attempt: {outcome:?}"),
        }
    }
    assert_eq!(published, [32, 32, 32, 4]);
    assert_eq!(search(&db, "hot", 10, None).await.len(), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn one_text_operation_that_cannot_fit_blocks_without_trimming() {
    for (name, layout) in [
        ("text-blocked-map", QueueLayout::Map),
        ("text-blocked-rows", QueueLayout::Rows),
    ] {
        let db = open(
            name,
            Arc::new(InMemory::new()),
            queued(IndexOperationQueueTuning::default().with_layout(layout)),
        )
        .await;
        install(&db, None).await;
        add(&db, "alpha", None).await;
        let target = target(&db, QueueFamily::Text).await;
        let before = all_keys(&db).await;
        // Its acknowledgement alone fills a one-write transaction, so its
        // epoch has no room however the selection shrinks.
        let batch = SearchIndexBatchLimits::try_new(
            NonZeroUsize::new(512).unwrap(),
            NonZeroU64::new(8 * 1024 * 1024).unwrap(),
            NonZeroU64::MIN,
            NonZeroU64::new(8 * 1024 * 1024).unwrap(),
            NonZeroU64::new(8 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        let narrow = publisher_with_limits(
            &db,
            batch,
            ActiveTextMutationLimits::unchecked_for_tests(
                batch,
                SearchIndexBackfillLimits::default()
                    .text_compaction()
                    .max_input_bytes(),
                SearchIndexBackfillLimits::default()
                    .text_compaction()
                    .max_output_blob_bytes(),
                SearchIndexBackfillLimits::default()
                    .text_compaction()
                    .max_manifest_bytes(),
            ),
        );
        for attempt in 1..=3_u64 {
            assert_eq!(
                narrow.publish_once(target).await.unwrap(),
                PublicationOutcome::Blocked,
                "{layout:?}"
            );
            assert_eq!(
                narrow.metrics().blocked_attempts.load(Ordering::Relaxed),
                attempt
            );
        }
        assert_eq!(
            narrow.metrics().output_retries.load(Ordering::Relaxed),
            0,
            "{layout:?}: a blocked operation is never retried as a trim"
        );
        assert_eq!(
            all_keys(&db).await,
            before,
            "blocked attempts write nothing"
        );
        assert_eq!(drain(&db, target).await, (1, 1));
        assert_eq!(search(&db, "alpha", 10, None).await.len(), 1);
        db.close().await.unwrap();
    }
}

/// Queued mode with a 64-operation, 64 KiB publication budget and 16 KiB
/// manifest pages, so one document may hold at most 26 unique terms (half of
/// 64 operations, less its six corpus, marker, root, page, state, and pointer
/// rows) and roughly 24 KiB of statistics rows.
pub(super) fn tight_publication_config() -> DbConfig {
    let defaults = SearchIndexBackfillLimits::default();
    let batch = defaults.batch();
    let compaction = defaults.text_compaction();
    let limits = SearchIndexBackfillLimits::try_new(
        SearchIndexBatchLimits::try_new(
            batch.max_entities(),
            batch.max_input_bytes(),
            NonZeroU64::new(64).unwrap(),
            NonZeroU64::new(64 * 1024).unwrap(),
            NonZeroU64::new(64 * 1024).unwrap(),
        )
        .unwrap(),
        defaults.edge_property_read_batch(),
        TextBuildArtifactLimits::new(
            NonZeroUsize::new(64).unwrap(),
            NonZeroU64::new(16 * 1024).unwrap(),
        ),
        TextBackfillCompactionLimits::new(
            compaction.max_fan_in(),
            compaction.max_input_bytes(),
            compaction.max_temporary_disk_bytes(),
            compaction.max_output_blob_bytes(),
            NonZeroU64::new(16 * 1024).unwrap(),
        ),
    )
    .unwrap();
    queued(IndexOperationQueueTuning::default()).with_search_index_backfill_limits(limits)
}

/// `count` distinct terms, each `width` bytes long and prefixed by `tag`.
pub(super) fn distinct_terms(tag: &str, count: usize, width: usize) -> String {
    (0..count)
        .map(|index| {
            let term = format!("{tag}x{index}");
            format!("{term}{}", "z".repeat(width.saturating_sub(term.len())))
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Writes one document, returning `None` when admission rejected it.
async fn try_write(db: &HelixDB, request: impl Fn() -> QueryRequest) -> Option<serde_json::Value> {
    for _ in 0..100 {
        match db.query(request()).await {
            Ok(result) => return Some(result),
            Err(HelixDbError::ActiveTextMutationLimitExceeded { .. }) => return None,
            Err(error) if error.is_transaction_conflict() => {}
            Err(error) => panic!("write failed: {error}"),
        }
    }
    panic!("write kept conflicting")
}

#[tokio::test]
async fn oversized_text_is_rejected_before_commit_instead_of_blocking_publication() {
    let db = open(
        "text-oversized",
        Arc::new(InMemory::new()),
        tight_publication_config(),
    )
    .await;
    install(&db, None).await;
    let error = add_text(&db, &distinct_terms("wide", 40, 4))
        .await
        .expect_err("40 unique terms exceed one document's publication share");
    assert!(
        matches!(
            error,
            HelixDbError::ActiveTextMutationLimitExceeded {
                resource: ActiveTextMutationResource::OutputOperations,
                observed: 46,
                limit: 32,
            }
        ),
        "{error:?}"
    );
    assert!(error.is_invalid_input());
    assert_eq!(
        node_count(&db).await,
        0,
        "the rejected graph write rolled back"
    );
    assert!(queue(&db, QueueFamily::Text).await.is_none());
    // The largest admitted document is exactly half the operation budget.
    add_text(&db, &distinct_terms("edge", 27, 4))
        .await
        .expect_err("27 unique terms need 33 operations");
    add_text(&db, &distinct_terms("edge", 26, 4))
        .await
        .expect("26 unique terms need exactly 32 operations");
    let target = target(&db, QueueFamily::Text).await;
    assert_eq!(drain(&db, target).await.0, 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn text_beyond_the_analysis_budget_is_rejected_before_commit() {
    let db = open(
        "text-analysis-budget",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&db, None).await;
    // 240,000 four-byte tokens reserve about 70 MB of the 64 MiB analysis
    // budget while the 1.2 MB operand still fits the queue.
    let error = add_text(&db, &"word ".repeat(240_000))
        .await
        .expect_err("the analysis budget rejects the write");
    assert!(
        matches!(
            error,
            HelixDbError::ActiveTextMutationLimitExceeded {
                resource: ActiveTextMutationResource::AnalysisBytes,
                ..
            }
        ),
        "{error:?}"
    );
    assert_eq!(node_count(&db).await, 0);
    assert!(queue(&db, QueueFamily::Text).await.is_none());
    db.close().await.unwrap();
}

/// Every admitted document publishes as an insert, a replacement of any other
/// admitted document (collapsed or not), and a tenant move: none can block.
#[tokio::test]
async fn admitted_text_never_blocks_publication_as_a_replacement() {
    let db = open(
        "text-admitted-pairs",
        Arc::new(InMemory::new()),
        tight_publication_config(),
    )
    .await;
    install(&db, Some("tenant")).await;
    let target = target(&db, QueueFamily::Text).await;
    // Wide documents stress operations; long terms stress output bytes.
    let candidates = [
        (20, 4),
        (26, 4),
        (27, 4),
        (40, 4),
        (4, 1_000),
        (6, 1_000),
        (12, 1_000),
    ];
    let document = |round: usize, (count, width): (usize, usize)| {
        distinct_terms(&format!("r{round}c{count}w{width}"), count, width)
    };
    let mut ids = Vec::new();
    for (position, candidate) in candidates.into_iter().enumerate() {
        let body = document(0, candidate);
        let created = try_write(&db, || {
            QueryRequest::write(
                batch::write_batch()
                    .var_as(
                        "created",
                        traversal::g().add_n(
                            "Doc",
                            vec![
                                ("body", PropertyInput::from(body.clone())),
                                ("tenant", PropertyInput::from("a".to_string())),
                            ],
                        ),
                    )
                    .returning(["created"]),
            )
        })
        .await;
        let Some(created) = created else {
            continue;
        };
        ids.push((created["created"][0]["$id"].as_u64().unwrap(), position));
    }
    assert_eq!(
        ids.iter()
            .map(|(_, position)| *position)
            .collect::<Vec<_>>(),
        vec![0, 1, 4, 5],
        "20/26 short-term and 4/6 long-term documents fit"
    );
    drain(&db, target).await;
    // Rounds replace every document with every other candidate, alone, as a
    // collapsed chain, and together with a tenant move.
    for round in 1..=candidates.len() {
        for (id, position) in &ids {
            let next = candidates[(position + round) % candidates.len()];
            let tenant = if round % 2 == 0 { "a" } else { "b" };
            for revision in 0..=(round % 3) {
                let body = document(round * 10 + revision, next);
                try_write(&db, || {
                    QueryRequest::write(
                        batch::write_batch().var_as(
                            "updated",
                            traversal::g()
                                .n(NodeRef::from(*id))
                                .set_property("body", body.clone())
                                .set_property("tenant", tenant.to_string()),
                        ),
                    )
                })
                .await;
            }
        }
        drain(&db, target).await;
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn term_frequency_changes_republish_the_document() {
    let reference = open(
        "text-frequency-reference",
        Arc::new(InMemory::new()),
        DbConfig::new(),
    )
    .await;
    let mut reference_ids = Vec::new();
    for body in ["error warning warning", "error info"] {
        reference_ids.push(add(&reference, body, None).await);
    }
    install(&reference, None).await;

    let db = open(
        "text-frequency",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&db, None).await;
    let target = target(&db, QueueFamily::Text).await;
    let mut ids = Vec::new();
    for body in ["error error warning", "error info"] {
        ids.push(add(&db, body, None).await);
    }
    drain(&db, target).await;
    // Same token count and unique terms, different term frequencies.
    set(&db, ids[0], "body", "error warning warning").await;
    drain(&db, target).await;
    for query in ["error", "warning"] {
        assert_eq!(
            ordinals(search(&db, query, 10, None).await, &ids),
            ordinals(search(&reference, query, 10, None).await, &reference_ids),
            "query {query:?} scores the republished frequencies"
        );
    }
    reference.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn text_output_limits_halve_the_batch_then_block_one_entity() {
    let db = open(
        "text-output-limits",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&db, None).await;
    for body in ["alpha one", "beta two", "gamma three", "delta four"] {
        add(&db, body, None).await;
    }
    let target = target(&db, QueueFamily::Text).await;
    let before = all_keys(&db).await;
    // A split ceiling lowered below every split after admission: no split
    // fits one byte, so even a single document exceeds the limits. No
    // validated policy admits a document whose lone split cannot fit.
    let defaults = SearchIndexBackfillLimits::default();
    let compaction = defaults.text_compaction();
    let tiny_splits = ActiveTextMutationLimits::unchecked_for_tests(
        defaults.batch(),
        compaction.max_input_bytes(),
        NonZeroU64::MIN,
        compaction.max_manifest_bytes(),
    );
    let narrow = publisher_with_limits(&db, defaults.batch(), tiny_splits);
    // Four entities halve to two, then one, which alone still exceeds.
    let mut observed = Vec::new();
    for _ in 0..3 {
        observed.push(narrow.publish_once(target).await.unwrap());
    }
    assert_eq!(
        observed,
        [
            PublicationOutcome::Trimmed,
            PublicationOutcome::Trimmed,
            PublicationOutcome::Blocked
        ]
    );
    assert_eq!(narrow.metrics().output_retries.load(Ordering::Relaxed), 2);
    assert_eq!(narrow.metrics().blocked_attempts.load(Ordering::Relaxed), 1);
    assert_eq!(all_keys(&db).await, before, "no attempt wrote anything");
    assert_eq!(
        queue(&db, QueueFamily::Text)
            .await
            .unwrap()
            .operations()
            .len(),
        4
    );
    assert_eq!(drain(&db, target).await, (4, 1));
    assert_eq!(search(&db, "beta", 10, None).await.len(), 1);
    db.close().await.unwrap();
}
