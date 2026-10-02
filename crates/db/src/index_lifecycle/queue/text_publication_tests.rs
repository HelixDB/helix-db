//! Text publication from queued payloads against an independent BM25 reference:
//! an initial build over the same final documents.

use std::collections::HashSet;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use helix_ast::{
    batch, graph::NodeRef, query::QueryRequest, traversal, value::PropertyInput,
    value::PropertyValue,
};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

use super::publication::{NextTarget, PublicationOutcome, QueuePublisher};
use super::publication_tests::batch_limits;
use super::tests::{
    add_text, all_keys, conflict_next_commit, node_count, open, publisher_with_limits, queue,
    queued, release_within_operand_bound, target, text_of,
};
use super::QueueTarget;
use crate::config::{
    ActiveTextMutationLimits, DbConfig, IndexOperationQueueTuning, QueueLayout,
    SearchIndexBackfillLimits, SearchIndexBatchLimits, TextBackfillCompactionLimits,
    TextBuildArtifactLimits, TextIndexDefinition,
};
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{ManagedIndexKey, ScopedKey};
use crate::encoding::v2::values::indexes::operation_queue::{
    QueueFamily, QueuedOperation, QueuedOperationId,
};
use crate::error::{ActiveTextMutationResource, HelixDbError};
use crate::index_lifecycle::ValidatedDynamicIndexDefinition;
use crate::search::vector::gated_wal::{GatedWalStore, WalUploads};
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
            | PublicationOutcome::Blocked
            | PublicationOutcome::Stalled) => panic!("publication did not progress: {outcome:?}"),
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
            | PublicationOutcome::Blocked
            | PublicationOutcome::Stalled) => panic!("publication wasted an attempt: {outcome:?}"),
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
        assert_eq!(
            narrow.publish_once(target).await.unwrap(),
            PublicationOutcome::Blocked,
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

/// A drain whose epochs a split ceiling binds alternates trimmed and
/// published attempts: each published epoch doubles the next selection past
/// the ceiling again. A trimmed attempt committed nothing, so it keeps the
/// queue it took, and the drain reads storage only for its first attempt and
/// for the one that finds the queue empty.
#[tokio::test]
async fn trimmed_text_attempts_keep_their_queue() {
    const DOCS: usize = 200;
    let db = open(
        "text-trimmed-retention",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&db, None).await;
    for index in 0..DOCS {
        add(&db, &distinct_terms(&format!("d{index}"), 20, 12), None).await;
    }
    let target = target(&db, QueueFamily::Text).await;
    let defaults = SearchIndexBackfillLimits::default();
    let compaction = defaults.text_compaction();
    let narrow = publisher_with_limits(
        &db,
        defaults.batch(),
        ActiveTextMutationLimits::unchecked_for_tests(
            defaults.batch(),
            compaction.max_input_bytes(),
            NonZeroU64::new(8 * 1024).unwrap(),
            compaction.max_manifest_bytes(),
        ),
    );
    let mut published = 0;
    let mut trimmed_after_publishing = false;
    for attempt in 0.. {
        assert!(attempt < 1_000, "the drain does not converge");
        match narrow.publish_once(target).await.unwrap() {
            PublicationOutcome::Empty => break,
            PublicationOutcome::Published { operations, .. } => published += operations,
            PublicationOutcome::Trimmed => trimmed_after_publishing |= published > 0,
            outcome @ (PublicationOutcome::Discarded { .. }
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry
            | PublicationOutcome::Blocked
            | PublicationOutcome::Stalled) => panic!("publication did not progress: {outcome:?}"),
        }
    }
    assert_eq!(published, DOCS as u64);
    assert!(
        trimmed_after_publishing,
        "a published epoch's doubled selection crossed the ceiling again"
    );
    assert_eq!(
        narrow.metrics().queue_reads.load(Ordering::Relaxed),
        2,
        "after {} trims",
        narrow.metrics().output_retries.load(Ordering::Relaxed)
    );
    assert_eq!(
        search(&db, &distinct_terms("d7", 1, 12), 10, None)
            .await
            .len(),
        1
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn text_publication_continues_from_its_last_commit_and_rereads_after_an_error() {
    let db = open(
        "text-retained",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&db, None).await;
    let mut ids = Vec::new();
    for body in [
        "apple shared",
        "banana shared",
        "cherry shared",
        "damson shared",
    ] {
        ids.push(add(&db, body, None).await);
    }
    let target = target(&db, QueueFamily::Text).await;
    // An input budget below any operation: one operation per batch.
    let narrow = publisher_with_limits(
        &db,
        batch_limits(1, 32_768),
        DbConfig::new()
            .search_index_backfill()
            .active_text_mutation(),
    );
    let reads = || narrow.metrics().queue_reads.load(Ordering::Relaxed);
    let retained = || db.index_queue_store().retained().retained_bytes();
    let stored = || async {
        queue(&db, QueueFamily::Text)
            .await
            .map_or_else(Vec::new, |queue| queue.into_operations())
    };
    let published = |outcome| match outcome {
        PublicationOutcome::Published { operations, .. } => operations,
        outcome @ (PublicationOutcome::Discarded { .. }
        | PublicationOutcome::Empty
        | PublicationOutcome::Deferred
        | PublicationOutcome::Retry
        | PublicationOutcome::Trimmed
        | PublicationOutcome::Blocked
        | PublicationOutcome::Stalled) => panic!("publication did not commit: {outcome:?}"),
    };
    let hits = |query| {
        let db = &db;
        async move {
            let mut found = search(db, query, 10, None)
                .await
                .into_iter()
                .map(|(id, _)| id)
                .collect::<Vec<_>>();
            found.sort_unstable();
            found
        }
    };

    assert_eq!(published(narrow.publish_once(target).await.unwrap()), 1);
    assert_eq!(reads(), 1);
    assert_eq!(
        retained(),
        stored()
            .await
            .iter()
            .map(QueuedOperation::retained_bytes)
            .sum::<u64>(),
        "the rest is retained"
    );

    // Work committed after the read waits for the retained operations: the
    // next batch continues from them without reading storage and publishes
    // the second document's original body, leaving its update queued.
    set(&db, ids[1], "body", "plum moved").await;
    let late = add(&db, "fig shared", None).await;
    assert_eq!(published(narrow.publish_once(target).await.unwrap()), 1);
    assert_eq!(reads(), 1);
    assert!(retained() > 0);
    assert_eq!(
        stored()
            .await
            .iter()
            .map(|operation| text_of(operation.payload()))
            .collect::<Vec<_>>(),
        [
            Some("cherry shared".to_string()),
            Some("damson shared".to_string()),
            Some("plum moved".to_string()),
            Some("fig shared".to_string()),
        ]
    );
    assert_eq!(hits("plum").await, [ids[1]], "strong search sees the move");
    assert!(hits("banana").await.is_empty());

    // An attempt that fails takes the retained queue and drops it: the next
    // one reads storage again, newer work included.
    narrow
        .hooks()
        .fail_before_commit
        .store(true, Ordering::SeqCst);
    assert_eq!(
        narrow.publish_once(target).await.unwrap(),
        PublicationOutcome::Retry
    );
    assert_eq!(retained(), 0);
    let mut drained = 0;
    loop {
        let outcome = narrow.publish_once(target).await.unwrap();
        if outcome == PublicationOutcome::Empty {
            break;
        }
        drained += published(outcome);
    }
    assert_eq!(drained, 4, "the rest, the move, and the late add");
    assert_eq!(retained(), 0);
    assert!(stored().await.is_empty());
    assert_eq!(
        reads(),
        3,
        "the read after the failure and the read that finds the queue empty"
    );
    // The move was published after the retained insert it follows.
    assert_eq!(hits("plum").await, [ids[1]]);
    assert!(hits("banana").await.is_empty());
    assert_eq!(hits("fig").await, [late]);
    let mut shared = vec![ids[0], ids[2], ids[3], late];
    shared.sort_unstable();
    assert_eq!(hits("shared").await, shared);
    db.close().await.unwrap();
}

#[tokio::test]
async fn text_publication_keeps_its_queue_after_a_conflict_and_rereads_after_an_uncertain_commit() {
    let gate = Arc::new(GatedWalStore::new());
    let db = open(
        "text-retained-uncommitted",
        Arc::clone(&gate) as Arc<dyn ObjectStore>,
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&db, None).await;
    // One document's add and two rewrites, published one operation per
    // batch.
    let entity = add(&db, "apple", None).await;
    set(&db, entity, "body", "plum").await;
    set(&db, entity, "body", "kiwi").await;
    let target = target(&db, QueueFamily::Text).await;
    let queued_operations = || async {
        queue(&db, QueueFamily::Text)
            .await
            .map_or(0, |queue| queue.operations().len())
    };
    assert_eq!(queued_operations().await, 3);
    let queued_bytes = queue(&db, QueueFamily::Text)
        .await
        .unwrap()
        .operations()
        .iter()
        .map(QueuedOperation::retained_bytes)
        .sum::<u64>();
    let narrow = publisher_with_limits(
        &db,
        batch_limits(1, 32_768),
        DbConfig::new()
            .search_index_backfill()
            .active_text_mutation(),
    );
    let reads = || narrow.metrics().queue_reads.load(Ordering::Relaxed);
    let retained = || db.index_queue_store().retained().retained_bytes();

    // A conflicting commit acknowledged nothing, so its attempt retains the
    // whole queue unchanged, never the rest of it, which would publish the
    // rewrites ahead of the add.
    let (outcome, ()) = tokio::join!(
        narrow.publish_once(target),
        conflict_next_commit(&db, &narrow)
    );
    assert_eq!(outcome.unwrap(), PublicationOutcome::Retry);
    assert_eq!(narrow.metrics().commit_conflicts.load(Ordering::Relaxed), 1);
    assert_eq!((reads(), retained()), (1, queued_bytes));
    assert_eq!(queued_operations().await, 3, "nothing was acknowledged");
    let mut published = 0;
    loop {
        match narrow.publish_once(target).await.unwrap() {
            PublicationOutcome::Empty => break,
            PublicationOutcome::Published { operations, .. } => published += operations,
            outcome @ (PublicationOutcome::Discarded { .. }
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry
            | PublicationOutcome::Trimmed
            | PublicationOutcome::Blocked
            | PublicationOutcome::Stalled) => panic!("publication stalled: {outcome:?}"),
        }
    }
    assert_eq!(published, 3);
    assert_eq!(
        reads(),
        2,
        "the kept queue drains without reading storage until it finds it empty"
    );
    let ids = |hits: Vec<(u64, u64)>| hits.into_iter().map(|(id, _)| id).collect::<Vec<_>>();
    assert_eq!(
        ids(search(&db, "kiwi", 10, None).await),
        [entity],
        "the last rewrite is published last"
    );
    assert!(search(&db, "apple", 10, None).await.is_empty());
    assert!(search(&db, "plum", 10, None).await.is_empty());

    // An uncertain commit may have acknowledged its batch, so its attempt
    // keeps no queue.
    set(&db, entity, "body", "fig").await;
    set(&db, entity, "body", "lime").await;
    gate.uploads.send_replace(WalUploads::Failing);
    assert_eq!(
        narrow.publish_once(target).await.unwrap(),
        PublicationOutcome::Retry
    );
    assert_eq!(
        narrow.metrics().uncertain_commits.load(Ordering::Relaxed),
        1
    );
    assert_eq!(retained(), 0);
    // The failed WAL upload closed the writer, so it is dropped unclosed.
}

/// IDs of `hits`, in rank order.
fn ids(hits: Vec<(u64, u64)>) -> Vec<u64> {
    hits.into_iter().map(|(id, _)| id).collect()
}

/// Entity IDs of the queued text operations, in queue order, in either queue
/// layout.
async fn queued_entities(db: &HelixDB) -> Vec<u64> {
    let target = target(db, QueueFamily::Text).await;
    db.inner
        .index_queue_store
        .read(db.inner_db().as_ref(), target)
        .await
        .unwrap()
        .map_or_else(Vec::new, |stored| {
            stored
                .queue()
                .operations()
                .iter()
                .map(|operation| operation.entity().id.get())
                .collect()
        })
}

/// One queued replacement that can never fit a publication stays queued
/// alone: entities queued after it still publish, and strong search keeps
/// overlaying it.
#[tokio::test]
async fn blocked_text_head_does_not_stall_later_entities() {
    let db = open(
        "text-blocked-head",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&db, None).await;
    let wide = add(&db, &distinct_terms("wide", 120, 4), None).await;
    let target = target(&db, QueueFamily::Text).await;
    assert_eq!(drain(&db, target).await, (1, 1));
    set(&db, wide, "body", "small").await;
    let later = add(&db, "fresh words", None).await;
    // Replacing the 120 accounted terms needs more than a 64-operation
    // publication, while the two-term insert needs a handful.
    let defaults = SearchIndexBackfillLimits::default();
    let batch = SearchIndexBatchLimits::try_new(
        defaults.batch().max_entities(),
        defaults.batch().max_input_bytes(),
        NonZeroU64::new(64).unwrap(),
        NonZeroU64::new(64 * 1024).unwrap(),
        NonZeroU64::new(64 * 1024).unwrap(),
    )
    .unwrap();
    let narrow = publisher_with_limits(
        &db,
        batch,
        ActiveTextMutationLimits::unchecked_for_tests(
            batch,
            defaults.text_compaction().max_input_bytes(),
            defaults.text_compaction().max_output_blob_bytes(),
            NonZeroU64::new(16 * 1024).unwrap(),
        ),
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
        "the head's replacement cannot fit the narrowed publication: {outcomes:?}"
    );
    assert_eq!(ids(search(&db, "small", 10, None).await), [wide]);
    assert!(search(&db, "widex7", 10, None).await.is_empty());
    assert_eq!(ids(search(&db, "fresh", 10, None).await), [later]);
    assert_eq!(
        queued_entities(&db).await,
        [wide],
        "only the blocked head stays queued after {outcomes:?}"
    );
    // The writer's own publisher still fits the head.
    assert_eq!(drain(&db, target).await, (1, 1));
    assert_eq!(ids(search(&db, "small", 10, None).await), [wide]);
    db.close().await.unwrap();
}

/// Publishes a 24,000-term document under twice today's output limits, then
/// reopens `store` under today's: one publication can then never replace that
/// document with any other wide document today's producer admits.
async fn reopen_after_lowered_limits(name: &str) -> (HelixDB, u64, QueueTarget) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let defaults = SearchIndexBackfillLimits::default();
    let batch = defaults.batch();
    let doubled = SearchIndexBackfillLimits::try_new(
        SearchIndexBatchLimits::try_new(
            batch.max_entities(),
            batch.max_input_bytes(),
            NonZeroU64::new(2 * batch.max_output_operations().get()).unwrap(),
            NonZeroU64::new(2 * batch.max_output_bytes().get()).unwrap(),
            batch.max_single_vector_output_bytes(),
        )
        .unwrap(),
        defaults.edge_property_read_batch(),
        defaults.text_artifacts(),
        defaults.text_compaction(),
    )
    .unwrap();
    let db = open(
        name,
        Arc::clone(&store),
        queued(IndexOperationQueueTuning::default()).with_search_index_backfill_limits(doubled),
    )
    .await;
    install(&db, None).await;
    let wide = add(&db, &distinct_terms("old", 24_000, 4), None).await;
    let target = target(&db, QueueFamily::Text).await;
    assert_eq!(drain(&db, target).await, (1, 1));
    db.close().await.unwrap();

    let db = open(name, store, queued(IndexOperationQueueTuning::default())).await;
    let error = add_text(&db, &distinct_terms("old", 24_000, 4))
        .await
        .expect_err("today's producer refuses the published document");
    assert!(
        matches!(
            error,
            HelixDbError::ActiveTextMutationLimitExceeded {
                resource: ActiveTextMutationResource::OutputOperations,
                ..
            }
        ),
        "{error:?}"
    );
    assert!(queue(&db, QueueFamily::Text).await.is_none());
    (db, wide, target)
}

/// After lowered limits, a replacement today's producer admits can still
/// never replace the published document in one publication. Later entities
/// publish, strong search overlays the blocked replacement, the stats report
/// it, and a repair to a small document publishes.
#[tokio::test]
async fn blocked_text_head_after_lowered_limits_does_not_stall_later_entities() {
    let (db, wide, target) = reopen_after_lowered_limits("text-lowered-head").await;
    // Each replacement alone is admitted; the published document plus
    // either exceeds every publication. Two of them trim the head's
    // operation ceiling before it blocks.
    set(&db, wide, "body", &distinct_terms("first", 10_000, 4)).await;
    set(&db, wide, "body", &distinct_terms("second", 10_000, 4)).await;
    let later = add(&db, "later words", None).await;
    let mut outcomes = Vec::new();
    for _ in 0..16 {
        match publisher(&db).publish_once(target).await.unwrap() {
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
        db.index_operation_queue_stats().blocked_attempts > 0,
        "the stats report the blocked head after {outcomes:?}"
    );
    assert_eq!(ids(search(&db, "secondx9999", 10, None).await), [wide]);
    assert!(search(&db, "firstx0", 10, None).await.is_empty());
    assert!(search(&db, "oldx23999", 10, None).await.is_empty());
    assert_eq!(ids(search(&db, "later", 10, None).await), [later]);
    assert_eq!(
        queued_entities(&db).await,
        [wide, wide],
        "only the blocked head stays queued after {outcomes:?}"
    );
    let blocked = db.blocked_index_entities();
    assert_eq!(
        blocked
            .iter()
            .map(|entity| (entity.index_id, entity.generation, entity.id.get()))
            .collect::<Vec<_>>(),
        [(target.index_id, target.generation, wide)]
    );
    assert_eq!(db.index_operation_queue_stats().blocked_entities, 1);

    set(&db, wide, "body", "tiny").await;
    let mut repaired = Vec::new();
    for _ in 0..16 {
        match publisher(&db).publish_once(target).await.unwrap() {
            PublicationOutcome::Empty => break,
            outcome @ (PublicationOutcome::Published { .. }
            | PublicationOutcome::Discarded { .. }
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry
            | PublicationOutcome::Trimmed
            | PublicationOutcome::Blocked
            | PublicationOutcome::Stalled) => repaired.push(outcome),
        }
    }
    assert!(
        queue(&db, QueueFamily::Text).await.is_none(),
        "the repaired head publishes: {repaired:?}"
    );
    assert_eq!(ids(search(&db, "tiny", 10, None).await), [wide]);
    assert!(search(&db, "secondx0", 10, None).await.is_empty());
    assert!(search(&db, "oldx0", 10, None).await.is_empty());
    assert_eq!(ids(search(&db, "later", 10, None).await), [later]);
    assert!(db.blocked_index_entities().is_empty());
    assert_eq!(db.index_operation_queue_stats().blocked_entities, 0);
    db.close().await.unwrap();
}

/// A head whose prefix was trimmed to one operation and then blocked still
/// publishes once a later operation of the same entity repairs it: the
/// trimmed operation ceiling must not pin the selection to the blocked
/// prefix forever.
#[tokio::test]
async fn a_repair_publishes_a_head_blocked_after_its_operation_ceiling_was_trimmed() {
    let (db, wide, target) = reopen_after_lowered_limits("text-lowered-repair").await;
    set(&db, wide, "body", &distinct_terms("first", 10_000, 4)).await;
    set(&db, wide, "body", &distinct_terms("second", 10_000, 4)).await;
    let mut outcomes = Vec::new();
    for _ in 0..4 {
        outcomes.push(publisher(&db).publish_once(target).await.unwrap());
    }
    // Two operations trim to one, which alone still cannot fit.
    assert_eq!(
        outcomes[..2],
        [PublicationOutcome::Trimmed, PublicationOutcome::Blocked],
        "{outcomes:?}"
    );
    assert!(
        outcomes
            .iter()
            .all(|outcome| !matches!(outcome, PublicationOutcome::Published { .. })),
        "{outcomes:?}"
    );
    set(&db, wide, "body", "tiny").await;
    let mut repaired = Vec::new();
    for _ in 0..16 {
        match publisher(&db).publish_once(target).await.unwrap() {
            PublicationOutcome::Empty => break,
            outcome @ (PublicationOutcome::Published { .. }
            | PublicationOutcome::Discarded { .. }
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry
            | PublicationOutcome::Trimmed
            | PublicationOutcome::Blocked
            | PublicationOutcome::Stalled) => repaired.push(outcome),
        }
    }
    assert!(
        queue(&db, QueueFamily::Text).await.is_none(),
        "the repaired head publishes: {repaired:?}"
    );
    assert_eq!(ids(search(&db, "tiny", 10, None).await), [wide]);
    assert!(search(&db, "secondx0", 10, None).await.is_empty());
    db.close().await.unwrap();
}

/// Deleting a held-back document repairs it: the delete supersedes the
/// replacement that could not fit and publishes in one repair, removing the
/// published document from the index and its corpus.
#[tokio::test]
async fn deleting_a_held_back_document_repairs_it() {
    let (db, wide, target) = reopen_after_lowered_limits("text-held-deleted").await;
    let documents = async || {
        let is_corpus = |key: &[u8]| {
            matches!(
                ManagedIndexKey::parse_data_from_slice(key),
                Ok(ManagedIndexKey::Data {
                    kind: ScopedKey::TextCorpusStatistics(_),
                    ..
                })
            )
        };
        super::tests::rows(&db, is_corpus)
            .await
            .values()
            .map(|value| {
                crate::encoding::v2::values::decode_corpus_statistics(value)
                    .unwrap()
                    .document_count
            })
            .sum::<u64>()
    };
    assert_eq!(documents().await, 1);
    set(&db, wide, "body", &distinct_terms("first", 10_000, 4)).await;
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Blocked
    );
    assert_eq!(db.blocked_index_entities().len(), 1);
    drop_node(&db, wide).await;
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Published {
            operations: 2,
            entities: 1
        }
    );
    assert!(queue(&db, QueueFamily::Text).await.is_none());
    assert!(db.blocked_index_entities().is_empty());
    assert_eq!(documents().await, 0);
    assert!(search(&db, "oldx0", 10, None).await.is_empty());
    assert!(search(&db, "firstx0", 10, None).await.is_empty());
    db.close().await.unwrap();
}

/// Waits until `target`'s logical index retains exactly `operations`.
async fn wait_for_operations(db: &HelixDB, target: QueueTarget, operations: u64, why: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while db
            .index_operation_backlog()
            .usage(target.scope, target.index_id)
            .operations
            != operations
        {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect(why);
}

/// The automatic worker holds a blocked entity back, keeps publishing the
/// rest of its generation, waits for a write while only that entity is left,
/// and publishes its repair without being driven.
#[tokio::test]
async fn the_worker_publishes_around_a_held_back_entity_and_then_its_repair() {
    let (db, wide, target) = reopen_after_lowered_limits("text-worker-held").await;
    set(&db, wide, "body", &distinct_terms("first", 10_000, 4)).await;
    let later = add(&db, "later words", None).await;
    publisher(&db).hooks().paused.store(false, Ordering::SeqCst);
    wait_for_operations(&db, target, 1, "later entities publish around the head").await;
    assert_eq!(ids(search(&db, "later", 10, None).await), [later]);
    assert_eq!(queued_entities(&db).await, [wide]);
    assert_eq!(
        db.blocked_index_entities()
            .iter()
            .map(|entity| entity.id.get())
            .collect::<Vec<_>>(),
        [wide]
    );
    // While only the held-back entity is queued, the worker waits for a
    // write instead of polling: at most the publication that drained the
    // rest and the attempt that found only the held entity finish after it.
    let attempts = db.index_operation_queue_stats().publication_attempts;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        db.index_operation_queue_stats().publication_attempts - attempts <= 2,
        "a stalled generation waits for a write"
    );

    set(&db, wide, "body", "tiny").await;
    wait_for_operations(&db, target, 0, "the repair publishes").await;
    assert_eq!(ids(search(&db, "tiny", 10, None).await), [wide]);
    assert!(search(&db, "oldx0", 10, None).await.is_empty());
    assert!(db.blocked_index_entities().is_empty());
    db.close().await.unwrap();
}

/// A write that arrives while a repair halves is not swallowed by it: once
/// halving reaches the operation known not to publish, the entity waits only
/// through the newest operation the repair tried at full width, so the later
/// write starts a new full-width repair, which publishes.
#[tokio::test]
async fn a_write_queued_while_a_repair_halves_starts_a_new_repair() {
    let (db, wide, target) = reopen_after_lowered_limits("text-repair-mid-halving").await;
    let publisher = publisher(&db);
    set(&db, wide, "body", &distinct_terms("first", 10_000, 4)).await;
    assert_eq!(
        publisher.publish_once(target).await.unwrap(),
        PublicationOutcome::Blocked
    );
    set(&db, wide, "body", &distinct_terms("second", 10_000, 4)).await;
    set(&db, wide, "body", &distinct_terms("third", 10_000, 4)).await;
    // All three do not fit, so the repair halves past the blocked first.
    assert_eq!(
        publisher.publish_once(target).await.unwrap(),
        PublicationOutcome::Trimmed
    );
    set(&db, wide, "body", "tiny").await;
    // The first two do not fit either, and nothing is left to halve.
    assert_eq!(
        publisher.publish_once(target).await.unwrap(),
        PublicationOutcome::Blocked
    );
    assert_eq!(
        publisher.publish_once(target).await.unwrap(),
        PublicationOutcome::Published {
            operations: 4,
            entities: 1
        }
    );
    assert!(queue(&db, QueueFamily::Text).await.is_none());
    assert!(db.blocked_index_entities().is_empty());
    assert_eq!(ids(search(&db, "tiny", 10, None).await), [wide]);
    assert!(search(&db, "thirdx0", 10, None).await.is_empty());
    db.close().await.unwrap();
}

/// A held entity written before every attempt is repaired only when the
/// rotation reaches it and is passed once held back again, so each document
/// queued behind it still publishes within three attempts, and the entity
/// publishes once a write fits.
#[tokio::test]
async fn a_held_entity_written_before_every_attempt_does_not_starve_its_generation() {
    let (db, wide, target) = reopen_after_lowered_limits("text-hot-held").await;
    let publisher = publisher(&db);
    let hot = |write: usize| distinct_terms(&format!("hot{write}y"), 10_000, 4);
    let mut writes = 0;
    set(&db, wide, "body", &hot(writes)).await;
    assert_eq!(
        publisher.publish_once(target).await.unwrap(),
        PublicationOutcome::Blocked
    );
    let mut repairs = 0;
    for round in 0..4 {
        let later = add(&db, &format!("later{round}"), None).await;
        let mut attempts = Vec::new();
        while queued_entities(&db).await.contains(&later) {
            assert!(
                attempts.len() < 3,
                "round {round}: the held entity starved {later}: {attempts:?}"
            );
            writes += 1;
            set(&db, wide, "body", &hot(writes)).await;
            attempts.push(publisher.publish_once(target).await.unwrap());
        }
        assert_eq!(
            attempts.last(),
            Some(&PublicationOutcome::Published {
                operations: 1,
                entities: 1
            }),
            "round {round}"
        );
        repairs += attempts.len() - 1;
        assert_eq!(
            ids(search(&db, &format!("later{round}"), 10, None).await),
            [later]
        );
    }
    assert!(repairs > 0, "the held entity was repaired between rounds");
    assert_eq!(
        db.blocked_index_entities()
            .iter()
            .map(|entity| entity.id.get())
            .collect::<Vec<_>>(),
        [wide]
    );
    set(&db, wide, "body", "tiny").await;
    let mut repaired = Vec::new();
    while queue(&db, QueueFamily::Text).await.is_some() {
        assert!(repaired.len() < 4, "the repair publishes: {repaired:?}");
        repaired.push(publisher.publish_once(target).await.unwrap());
    }
    assert_eq!(ids(search(&db, "tiny", 10, None).await), [wide]);
    assert!(db.blocked_index_entities().is_empty());
    db.close().await.unwrap();
}

/// A held entity's repair is not starved by entities rewritten in a fixed
/// order between publications: the batch the rotation carries up to it ends
/// there, so its repair runs in the next attempt.
#[tokio::test]
async fn a_repair_runs_although_other_entities_are_rewritten_between_publications() {
    let (db, wide, target) = reopen_after_lowered_limits("text-repair-rotation").await;
    let publisher = publisher(&db);
    set(&db, wide, "body", &distinct_terms("first", 10_000, 4)).await;
    assert_eq!(
        publisher.publish_once(target).await.unwrap(),
        PublicationOutcome::Blocked
    );
    let bravo = add(&db, "bravo0", None).await;
    let alpha = add(&db, "alpha0", None).await;
    assert_eq!(
        publisher.publish_once(target).await.unwrap(),
        PublicationOutcome::Published {
            operations: 2,
            entities: 2
        }
    );
    set(&db, wide, "body", "tiny").await;
    let mut outcomes = Vec::new();
    for round in 1..=4 {
        set(&db, alpha, "body", &format!("alpha{round}")).await;
        set(&db, bravo, "body", &format!("bravo{round}")).await;
        outcomes.push(publisher.publish_once(target).await.unwrap());
        if !queued_entities(&db).await.contains(&wide) {
            break;
        }
    }
    // The batch ends at the held entity, then its repair publishes alone.
    assert_eq!(
        outcomes,
        [
            PublicationOutcome::Published {
                operations: 1,
                entities: 1
            },
            PublicationOutcome::Published {
                operations: 2,
                entities: 1
            }
        ]
    );
    assert_eq!(ids(search(&db, "tiny", 10, None).await), [wide]);
    assert!(db.blocked_index_entities().is_empty());
    db.close().await.unwrap();
}

/// A generation whose every queued entity is held back waits for new work
/// instead of polling its queue: no backoff makes it eligible again, a write
/// to it does at once, and only a long deadline, which bounds how late a
/// retirement is discovered, rechecks it without one.
#[tokio::test]
async fn a_stalled_generation_waits_for_a_write_instead_of_polling() {
    let (db, wide, target) = reopen_after_lowered_limits("text-stalled-waits").await;
    // The writer's own publisher never schedules in tests; one with the same
    // limits does.
    let defaults = DbConfig::new().search_index_backfill();
    let scheduler = publisher_with_limits(&db, defaults.batch(), defaults.active_text_mutation());
    set(&db, wide, "body", &distinct_terms("first", 10_000, 4)).await;
    assert_eq!(
        scheduler.publish_once(target).await.unwrap(),
        PublicationOutcome::Blocked
    );
    let stalled = Instant::now();
    assert_eq!(
        scheduler.publish_once(target).await.unwrap(),
        PublicationOutcome::Stalled
    );
    let none = HashSet::new();
    let NextTarget::Delayed(deadline) =
        scheduler.next_target(&none, stalled + Duration::from_secs(30))
    else {
        panic!("a stalled generation waits past every retry backoff");
    };
    assert!(deadline >= stalled + Duration::from_secs(60));
    assert_eq!(
        scheduler.next_target(&none, deadline),
        NextTarget::Ready(target)
    );
    let later = add(&db, "later words", None).await;
    assert_eq!(
        scheduler.next_target(&none, stalled),
        NextTarget::Ready(target),
        "a write makes it eligible at once"
    );
    assert_eq!(
        scheduler.publish_once(target).await.unwrap(),
        PublicationOutcome::Published {
            operations: 1,
            entities: 1
        }
    );
    assert_eq!(ids(search(&db, "later", 10, None).await), [later]);
    assert_eq!(
        scheduler.publish_once(target).await.unwrap(),
        PublicationOutcome::Stalled
    );
    assert!(matches!(
        scheduler.next_target(&none, Instant::now() + Duration::from_secs(30)),
        NextTarget::Delayed(_)
    ));
    db.close().await.unwrap();
}

/// A queue retained after a commit lacks writes admitted since its read.
/// When every entity left in it is held back, the attempt reads storage again
/// instead of stalling, so a repair written after that read publishes at
/// once rather than after the stalled deadline.
#[tokio::test]
async fn a_repair_behind_a_retained_queue_publishes_without_stalling() {
    let (db, wide, target) = reopen_after_lowered_limits("text-retained-repair").await;
    let defaults = DbConfig::new().search_index_backfill();
    let scheduler = publisher_with_limits(&db, defaults.batch(), defaults.active_text_mutation());
    let reads = || scheduler.metrics().queue_reads.load(Ordering::Relaxed);
    let retained = || db.index_queue_store().retained().retained_bytes();
    set(&db, wide, "body", &distinct_terms("first", 10_000, 4)).await;
    assert_eq!(
        scheduler.publish_once(target).await.unwrap(),
        PublicationOutcome::Blocked
    );
    let later = add(&db, "later words", None).await;
    assert_eq!(
        scheduler.publish_once(target).await.unwrap(),
        PublicationOutcome::Published {
            operations: 1,
            entities: 1
        }
    );
    assert_eq!(ids(search(&db, "later", 10, None).await), [later]);
    assert!(retained() > 0, "the held entity's operation is retained");
    set(&db, wide, "body", "tiny").await;
    let before = reads();
    assert_eq!(
        scheduler.publish_once(target).await.unwrap(),
        PublicationOutcome::Published {
            operations: 2,
            entities: 1
        },
        "the retained queue holds only the held entity; storage holds its repair"
    );
    assert_eq!(reads(), before + 1);
    assert_eq!(ids(search(&db, "tiny", 10, None).await), [wide]);
    assert!(db.blocked_index_entities().is_empty());
    assert!(queue(&db, QueueFamily::Text).await.is_none());
    db.close().await.unwrap();
}

/// A write charged before a stalled attempt reads its queue, but whose commit
/// returns only after that read, makes the generation eligible again when the
/// commit returns rather than at the stalled deadline.
#[tokio::test]
async fn an_enqueue_returning_after_a_stalled_read_makes_its_generation_eligible() {
    let (db, wide, target) = reopen_after_lowered_limits("text-stalled-commit-race").await;
    let defaults = DbConfig::new().search_index_backfill();
    let scheduler = publisher_with_limits(&db, defaults.batch(), defaults.active_text_mutation());
    set(&db, wide, "body", &distinct_terms("first", 10_000, 4)).await;
    assert_eq!(
        scheduler.publish_once(target).await.unwrap(),
        PublicationOutcome::Blocked
    );
    // A producer's write to the held entity, charged but not yet committed.
    let operation = QueuedOperationId::try_from_u128(1).unwrap();
    let mut reservation = db
        .index_operation_backlog()
        .reserve(
            &[super::backlog::OperationCharge {
                target,
                entity: crate::encoding::v2::keys::IndexEntity {
                    kind: crate::index_lifecycle::IndexElementKind::Node,
                    id: crate::index_lifecycle::IndexEntityId::new(wide),
                },
                id: operation,
                bytes: 1,
            }],
            &[],
        )
        .unwrap();
    reservation.begin_commit();
    assert_eq!(
        scheduler.publish_once(target).await.unwrap(),
        PublicationOutcome::Stalled,
        "the queue read misses the uncommitted write"
    );
    let none = HashSet::new();
    assert!(matches!(
        scheduler.next_target(&none, Instant::now()),
        NextTarget::Delayed(_)
    ));
    reservation.committed();
    assert_eq!(
        scheduler.next_target(&none, Instant::now()),
        NextTarget::Ready(target),
        "the returned commit may be readable now"
    );
    db.index_operation_backlog().acknowledge([operation]);
    db.close().await.unwrap();
}

/// The worker retries a stalled generation as soon as an enqueue it may have
/// missed returns, even when the producer learns no outcome: a request
/// cancelled mid-commit wakes it, rather than the stalled deadline.
#[tokio::test]
async fn the_worker_retries_a_stalled_generation_when_an_enqueue_is_cancelled_mid_commit() {
    let name = "text-worker-cancelled-enqueue";
    let (db, wide, target) = reopen_after_lowered_limits(name).await;
    let store = Arc::clone(db.object_store());
    db.close().await.unwrap();
    // Past the stalled deadline, so only a wake can retry the generation
    // sooner.
    let db = open(
        name,
        store,
        queued(
            IndexOperationQueueTuning::default()
                .with_recovery_sweep_interval(Duration::from_secs(600))
                .unwrap(),
        ),
    )
    .await;
    set(&db, wide, "body", &distinct_terms("first", 10_000, 4)).await;
    // A producer's write to the held entity, charged and submitted.
    let operation = QueuedOperationId::try_from_u128(1).unwrap();
    let mut reservation = db
        .index_operation_backlog()
        .reserve(
            &[super::backlog::OperationCharge {
                target,
                entity: crate::encoding::v2::keys::IndexEntity {
                    kind: crate::index_lifecycle::IndexElementKind::Node,
                    id: crate::index_lifecycle::IndexEntityId::new(wide),
                },
                id: operation,
                bytes: 1,
            }],
            &[],
        )
        .unwrap();
    reservation.begin_commit();
    publisher(&db).hooks().paused.store(false, Ordering::SeqCst);
    db.wake_index_worker().await;
    let none = HashSet::new();
    tokio::time::timeout(Duration::from_secs(30), async {
        while !matches!(
            publisher(&db).next_target(&none, Instant::now()),
            NextTarget::Delayed(deadline) if deadline > Instant::now() + Duration::from_secs(30)
        ) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the worker holds the entity back and stalls");
    assert_eq!(
        db.blocked_index_entities()
            .iter()
            .map(|entity| entity.id.get())
            .collect::<Vec<_>>(),
        [wide]
    );
    let attempts = db.index_operation_queue_stats().publication_attempts;
    drop(reservation);
    tokio::time::timeout(Duration::from_secs(10), async {
        while db.index_operation_queue_stats().publication_attempts == attempts {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the cancelled enqueue wakes the worker before the stalled deadline");
    db.index_operation_backlog().acknowledge([operation]);
    db.close().await.unwrap();
}

/// A restarted publisher rediscovers unpublishable heads with an empty held
/// set. Isolating the first trims the selection to one entity, and the trim
/// outlives the block, so every further head is held back in one attempt and
/// the document queued behind them publishes right after.
#[tokio::test]
async fn rediscovered_unpublishable_heads_are_held_back_in_one_attempt_each() {
    const HEADS: usize = 8;
    let db = open(
        "text-rediscovered-heads",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install(&db, None).await;
    let mut heads = Vec::new();
    for head in 0..HEADS {
        heads.push(add(&db, &distinct_terms(&format!("wide{head}y"), 120, 4), None).await);
    }
    let target = target(&db, QueueFamily::Text).await;
    assert_eq!(drain(&db, target).await.0, HEADS as u64);
    for head in &heads {
        set(&db, *head, "body", "small").await;
    }
    let later = add(&db, "fresh words", None).await;
    // Replacing 120 accounted terms needs more than a 64-operation
    // publication, while the two-term insert needs a handful.
    let defaults = SearchIndexBackfillLimits::default();
    let batch = SearchIndexBatchLimits::try_new(
        defaults.batch().max_entities(),
        defaults.batch().max_input_bytes(),
        NonZeroU64::new(64).unwrap(),
        NonZeroU64::new(64 * 1024).unwrap(),
        NonZeroU64::new(64 * 1024).unwrap(),
    )
    .unwrap();
    let restarted = publisher_with_limits(
        &db,
        batch,
        ActiveTextMutationLimits::unchecked_for_tests(
            batch,
            defaults.text_compaction().max_input_bytes(),
            defaults.text_compaction().max_output_blob_bytes(),
            NonZeroU64::new(16 * 1024).unwrap(),
        ),
    );
    let mut outcomes = Vec::new();
    while queued_entities(&db).await.contains(&later) {
        assert!(outcomes.len() < 2 * HEADS, "{outcomes:?}");
        outcomes.push(restarted.publish_once(target).await.unwrap());
    }
    // Nine entities halve to four, two, then one: the first head blocks,
    // then each other head blocks alone.
    assert_eq!(
        outcomes,
        [PublicationOutcome::Trimmed; 3]
            .into_iter()
            .chain([PublicationOutcome::Blocked; HEADS])
            .chain([PublicationOutcome::Published {
                operations: 1,
                entities: 1
            }])
            .collect::<Vec<_>>()
    );
    assert_eq!(queued_entities(&db).await, heads);
    assert_eq!(restarted.blocked_entities().len(), HEADS);
    assert_eq!(ids(search(&db, "fresh", 10, None).await), [later]);
    db.close().await.unwrap();
}

/// A publisher whose 64-operation publications fit replacing one term with
/// another, but not replacing or retiring 120 terms.
fn narrow_publisher(db: &HelixDB) -> Arc<QueuePublisher> {
    let defaults = SearchIndexBackfillLimits::default();
    let batch = SearchIndexBatchLimits::try_new(
        defaults.batch().max_entities(),
        defaults.batch().max_input_bytes(),
        NonZeroU64::new(64).unwrap(),
        NonZeroU64::new(64 * 1024).unwrap(),
        NonZeroU64::new(64 * 1024).unwrap(),
    )
    .unwrap();
    publisher_with_limits(
        db,
        batch,
        ActiveTextMutationLimits::unchecked_for_tests(
            batch,
            defaults.text_compaction().max_input_bytes(),
            defaults.text_compaction().max_output_blob_bytes(),
            NonZeroU64::new(16 * 1024).unwrap(),
        ),
    )
}

/// Eventual text search for `text` in a database whose eventual search
/// budget is zero, so it overlays no unpublished work: the published index
/// alone.
async fn published(db: &HelixDB, text: &str) -> Vec<u64> {
    let result = db
        .query(
            QueryRequest::read(
                batch::read_batch()
                    .var_as(
                        "hits",
                        traversal::g().text_search_nodes("Doc", "body", text, 10, None),
                    )
                    .returning(["hits"]),
            )
            .with_search_consistency(helix_ast::query::SearchConsistency::Eventual)
            .unwrap(),
        )
        .await
        .unwrap();
    result["hits"].as_array().map_or_else(Vec::new, |hits| {
        hits.iter()
            .map(|hit| hit["$id"].as_u64().unwrap())
            .collect()
    })
}

/// Publishes `small` for one document, then queues 40 replacements of 120
/// terms each behind it, more than one acknowledgement of
/// [`narrow_publisher`] can carry, none of which it can publish.
///
/// Returns the database, the document, its target, the narrow publisher, and
/// its acknowledgement capacity.
async fn held_past_one_acknowledgement(
    name: &str,
) -> (HelixDB, u64, QueueTarget, Arc<QueuePublisher>, usize) {
    const WIDE: usize = 40;
    let db = open(
        name,
        Arc::new(InMemory::new()),
        queued(
            IndexOperationQueueTuning::default()
                .with_layout(QueueLayout::Rows)
                .with_eventual_search_budget_for_tests(0),
        ),
    )
    .await;
    install(&db, None).await;
    let held = add(&db, "small", None).await;
    let target = target(&db, QueueFamily::Text).await;
    assert_eq!(drain(&db, target).await, (1, 1));
    for write in 0..WIDE {
        set(
            &db,
            held,
            "body",
            &distinct_terms(&format!("wide{write}y"), 120, 4),
        )
        .await;
    }
    let narrow = narrow_publisher(&db);
    // Row acknowledgements leave each narrowed publication 32 IDs, fewer
    // than the entity's queued operations, and every one of those 32 states
    // replaces one term with 120.
    let capacity = db.inner.index_queue_store.acknowledgement_capacity(
        target,
        super::OutputBudget {
            max_operations: 32,
            max_bytes: 32 * 1024,
        },
    );
    assert_eq!(capacity.get(), 32);
    assert!(queued_entities(&db).await.len() > capacity.get());
    (db, held, target, narrow, capacity.get())
}

/// Publishes with `narrow` until at most `left` operations stay queued,
/// returning every outcome.
async fn publish_until(
    db: &HelixDB,
    narrow: &QueuePublisher,
    target: QueueTarget,
    left: usize,
) -> Vec<PublicationOutcome> {
    let mut outcomes = Vec::new();
    while queued_entities(db).await.len() > left {
        assert!(outcomes.len() < 16, "the repair stalled: {outcomes:?}");
        outcomes.push(narrow.publish_once(target).await.unwrap());
    }
    outcomes
}

/// A held entity with more queued operations than one acknowledgement can
/// carry still publishes a later write: the repair publishes the newest
/// queued state while acknowledging what it can, and the entity drains,
/// republishing that state, until every operation is acknowledged. A write
/// that cannot fit while it drains holds it back at once, without halving
/// back to a state older than the one it serves.
#[tokio::test]
async fn a_repair_publishes_its_newest_state_past_one_acknowledgement() {
    let (db, held, target, narrow, capacity) =
        held_past_one_acknowledgement("text-repair-past-acknowledgement").await;
    set(&db, held, "body", "tiny").await;
    let queued = queued_entities(&db).await.len();
    let outcomes = publish_until(&db, &narrow, target, queued - capacity).await;
    assert_eq!(
        outcomes[outcomes.len() - 2..],
        [
            PublicationOutcome::Blocked,
            PublicationOutcome::Published {
                operations: capacity as u64,
                entities: 1
            }
        ],
        "{outcomes:?}"
    );
    // The newest state is published, and the entity is draining rather than
    // blocked.
    assert_eq!(published(&db, "tiny").await, [held]);
    assert!(published(&db, "small").await.is_empty());
    assert!(narrow.blocked_entities().is_empty());

    // The repair retained the entity's other operations, which lack any later
    // write (see `a_draining_entity_republishes_its_retained_state_first`);
    // without them the next attempt reads storage and sees the write.
    assert!(db.index_queue_store().retained().take(target).is_some());
    set(&db, held, "body", &distinct_terms("late", 120, 4)).await;
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
        [held]
    );
    assert_eq!(published(&db, "tiny").await, [held]);

    set(&db, held, "body", "final").await;
    assert_eq!(
        narrow.publish_once(target).await.unwrap(),
        PublicationOutcome::Published {
            operations: (queued - capacity + 2) as u64,
            entities: 1
        }
    );
    assert_eq!(
        narrow.publish_once(target).await.unwrap(),
        PublicationOutcome::Empty
    );
    assert!(queued_entities(&db).await.is_empty());
    assert!(narrow.blocked_entities().is_empty());
    assert_eq!(published(&db, "final").await, [held]);
    assert!(published(&db, "tiny").await.is_empty());
    assert!(published(&db, "latex0").await.is_empty());
    db.close().await.unwrap();
}

/// A draining entity's retained operations lack writes queued after the read
/// they came from, so its next attempt republishes the newest state it holds
/// while acknowledging the rest. The later write, which cannot fit, is read
/// and held back once the retained operations are gone, and its own repair
/// publishes.
#[tokio::test]
async fn a_draining_entity_republishes_its_retained_state_first() {
    let (db, held, target, narrow, capacity) =
        held_past_one_acknowledgement("text-draining-retained").await;
    set(&db, held, "body", "tiny").await;
    let queued = queued_entities(&db).await.len();
    publish_until(&db, &narrow, target, queued - capacity).await;
    assert_eq!(published(&db, "tiny").await, [held]);
    set(&db, held, "body", &distinct_terms("late", 120, 4)).await;
    let reads = || narrow.metrics().queue_reads.load(Ordering::Relaxed);
    let before = reads();
    assert_eq!(
        narrow.publish_once(target).await.unwrap(),
        PublicationOutcome::Published {
            operations: (queued - capacity) as u64,
            entities: 1
        }
    );
    assert_eq!(
        reads(),
        before,
        "the drain continues from its retained queue"
    );
    assert_eq!(published(&db, "tiny").await, [held]);
    assert_eq!(
        narrow.publish_once(target).await.unwrap(),
        PublicationOutcome::Blocked
    );
    assert_eq!(reads(), before + 1);
    assert_eq!(published(&db, "tiny").await, [held]);
    set(&db, held, "body", "final").await;
    assert_eq!(
        narrow.publish_once(target).await.unwrap(),
        PublicationOutcome::Published {
            operations: 2,
            entities: 1
        }
    );
    assert!(queued_entities(&db).await.is_empty());
    assert!(narrow.blocked_entities().is_empty());
    assert_eq!(published(&db, "final").await, [held]);
    assert!(published(&db, "latex0").await.is_empty());
    db.close().await.unwrap();
}

/// Only the publisher's memory knows that an entity drains: one restarted
/// mid-drain publishes the remaining operations in regular batches, each
/// collapsing to the newest operation it acknowledges, so the published state
/// steps back to an older queued state and forward again until the last is
/// acknowledged. Strong search overlays the newest state throughout.
#[tokio::test]
async fn a_publisher_restarted_while_an_entity_drains_republishes_older_states() {
    let (db, held, target, narrow, capacity) =
        held_past_one_acknowledgement("text-drain-restart").await;
    // Every state after the 40 wide ones fits, so only the repair is held.
    for write in 0..2 * capacity {
        set(&db, held, "body", &format!("small{write}")).await;
    }
    let queued = queued_entities(&db).await.len();
    let outcomes = publish_until(&db, &narrow, target, queued - capacity).await;
    assert_eq!(
        outcomes.last(),
        Some(&PublicationOutcome::Published {
            operations: capacity as u64,
            entities: 1
        }),
        "{outcomes:?}"
    );
    let newest = format!("small{}", 2 * capacity - 1);
    assert_eq!(published(&db, &newest).await, [held]);

    let restarted = narrow_publisher(&db);
    // Operations 33 to 64, 65 to 96, then the last eight: each batch serves
    // its newest state, the first older than the one already served.
    let left = queued - capacity;
    for (operations, state) in [
        (capacity, capacity - 9),
        (capacity, 2 * capacity - 9),
        (left - 2 * capacity, 2 * capacity - 1),
    ] {
        assert_eq!(
            restarted.publish_once(target).await.unwrap(),
            PublicationOutcome::Published {
                operations: operations as u64,
                entities: 1
            }
        );
        assert_eq!(published(&db, &format!("small{state}")).await, [held]);
        assert_eq!(ids(search(&db, &newest, 10, None).await), [held]);
    }
    assert!(published(&db, &format!("small{}", capacity - 9))
        .await
        .is_empty());
    assert_eq!(
        restarted.publish_once(target).await.unwrap(),
        PublicationOutcome::Empty
    );
    db.close().await.unwrap();
}

/// A delete repairs a held entity past one acknowledgement too: it publishes
/// at once, and the entity drains its remaining operations without serving
/// any of their states.
#[tokio::test]
async fn a_delete_repairs_a_held_entity_past_one_acknowledgement() {
    let (db, held, target, narrow, capacity) =
        held_past_one_acknowledgement("text-delete-past-acknowledgement").await;
    drop_node(&db, held).await;
    let queued = queued_entities(&db).await.len();
    let outcomes = publish_until(&db, &narrow, target, queued - capacity).await;
    assert_eq!(
        outcomes.last(),
        Some(&PublicationOutcome::Published {
            operations: capacity as u64,
            entities: 1
        }),
        "{outcomes:?}"
    );
    assert!(published(&db, "small").await.is_empty());
    assert!(narrow.blocked_entities().is_empty());
    assert_eq!(
        narrow.publish_once(target).await.unwrap(),
        PublicationOutcome::Published {
            operations: (queued - capacity) as u64,
            entities: 1
        }
    );
    assert_eq!(
        narrow.publish_once(target).await.unwrap(),
        PublicationOutcome::Empty
    );
    assert!(queued_entities(&db).await.is_empty());
    assert!(published(&db, "small").await.is_empty());
    assert!(published(&db, "wide39yx0").await.is_empty());
    db.close().await.unwrap();
}

/// Dropping an index releases the entities its publisher held back: the
/// discard empties the retired queue, after which no attempt ever visits the
/// generation again.
#[tokio::test]
async fn dropping_an_index_releases_its_held_back_entities() {
    let (db, wide, target) = reopen_after_lowered_limits("text-held-dropped").await;
    set(&db, wide, "body", &distinct_terms("first", 10_000, 4)).await;
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Blocked
    );
    assert_eq!(db.blocked_index_entities().len(), 1);
    assert_eq!(db.index_operation_queue_stats().blocked_entities, 1);
    let dropped = super::lifecycle_tests::drop_index(
        &db,
        helix_ast::index::IndexSpec::node_text("Doc", "body", None::<&str>),
    )
    .await
    .expect("dropping an active index runs an operation");
    assert_eq!(
        super::lifecycle_tests::wait_terminal(&db, &dropped).await,
        "succeeded"
    );
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Discarded { operations: 1 }
    );
    assert!(db
        .index_operation_backlog()
        .outstanding_targets()
        .is_empty());
    assert!(db.blocked_index_entities().is_empty());
    assert_eq!(db.index_operation_queue_stats().blocked_entities, 0);
    db.close().await.unwrap();
}

/// A repair whose commit outcome is uncertain no longer holds its entity
/// back, since it may have published; here it did, and reconciliation
/// releases its operations.
#[tokio::test]
async fn an_uncertain_repair_commit_releases_its_hold() {
    let (db, wide, target) = reopen_after_lowered_limits("text-held-uncertain").await;
    let publisher = publisher(&db);
    set(&db, wide, "body", &distinct_terms("first", 10_000, 4)).await;
    assert_eq!(
        publisher.publish_once(target).await.unwrap(),
        PublicationOutcome::Blocked
    );
    assert_eq!(db.blocked_index_entities().len(), 1);
    set(&db, wide, "body", "tiny").await;
    publisher
        .hooks()
        .uncertain_after_commit
        .store(true, Ordering::SeqCst);
    assert_eq!(
        publisher.publish_once(target).await.unwrap(),
        PublicationOutcome::Retry
    );
    assert_eq!(db.index_operation_queue_stats().uncertain_commits, 1);
    assert!(db.blocked_index_entities().is_empty());
    assert_eq!(db.index_operation_queue_stats().blocked_entities, 0);
    assert_eq!(
        publisher.publish_once(target).await.unwrap(),
        PublicationOutcome::Empty
    );
    assert!(queue(&db, QueueFamily::Text).await.is_none());
    assert_eq!(ids(search(&db, "tiny", 10, None).await), [wide]);
    assert!(search(&db, "firstx0", 10, None).await.is_empty());
    db.close().await.unwrap();
}
