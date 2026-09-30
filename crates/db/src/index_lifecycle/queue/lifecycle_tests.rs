//! Builds, activation, retirement, and restart with queued vector/text work.
//!
//! Tests pause a build between steps by holding its generation's publication
//! ownership, which every lifecycle step acquires first, so writes land on
//! both sides of the scan cursor deterministically.

use std::collections::BTreeMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use helix_ast::{
    batch, graph::NodeRef, index::IndexSpec, index::VectorDistanceMetric, query::QueryRequest,
    query::SearchConsistency, traversal, value::PropertyInput,
};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

use super::backlog::OperationCharge;
use super::overlay_tests::{add, delete, drain, text_search, update, vector_search, write};
use super::publication::PublicationOutcome;
use super::tests::{
    open, publisher_with_limits, queue, queued, release_within_operand_bound, rows, target,
};
use super::text_publication_tests::{distinct_terms, tight_publication_config};
use super::QueueTarget;
use crate::config::{
    DbConfig, IndexOperationQueueTuning, QueueLayout, SearchIndexBackfillLimits,
    SearchIndexBatchLimits, TextIndexDefinition,
};
use crate::encoding::v2::keys::indexes::vector::VectorKey;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{DataKey, DataKeyKind, IndexEntity};
use crate::encoding::v2::values::indexes::operation_queue::{QueueFamily, QueuedOperationId};
use crate::index_lifecycle::outbox::step_pause::{self, StepPause};
use crate::index_lifecycle::{
    IndexElementKind, IndexEntityId, IndexGenerationPublicationPermit, IndexOperationProgress,
    SourceScanProgress, TextBuildProgress, TextBuildStage, ValidatedDynamicIndexDefinition,
    VectorBuildProgress, VectorBuildStage,
};
use crate::HelixDB;

/// Queued mode, paused publication, and four-entity build batches.
pub(super) fn config() -> DbConfig {
    let defaults = SearchIndexBackfillLimits::default();
    let batch = defaults.batch();
    let limits = SearchIndexBackfillLimits::try_new(
        SearchIndexBatchLimits::try_new(
            NonZeroUsize::new(4).unwrap(),
            batch.max_input_bytes(),
            batch.max_output_operations(),
            batch.max_output_bytes(),
            batch.max_single_vector_output_bytes(),
        )
        .unwrap(),
        defaults.edge_property_read_batch(),
        defaults.text_artifacts(),
        defaults.text_compaction(),
    )
    .unwrap();
    queued(IndexOperationQueueTuning::default()).with_search_index_backfill_limits(limits)
}

pub(super) fn vector_spec() -> IndexSpec {
    IndexSpec::node_vector(
        "Doc",
        "embedding",
        NonZeroUsize::new(2).unwrap(),
        VectorDistanceMetric::Euclidean,
        Some("tenant"),
    )
}

fn text_spec() -> IndexSpec {
    IndexSpec::node_text("Doc", "body", None::<&str>)
}

/// Returns the operation ID named anywhere in a DDL receipt.
fn operation_id(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Object(object) => object
            .get("operation_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .or_else(|| object.values().find_map(operation_id)),
        serde_json::Value::Array(values) => values.iter().find_map(operation_id),
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => None,
    }
}

pub(super) async fn create(db: &HelixDB, spec: IndexSpec) -> String {
    let receipt = write(db, || {
        QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "created",
                    traversal::g().create_index_if_not_exists(spec.clone()),
                )
                .returning(["created"]),
        )
    })
    .await;
    operation_id(&receipt).unwrap_or_else(|| panic!("create accepted a build: {receipt}"))
}

pub(super) async fn drop_index(db: &HelixDB, spec: IndexSpec) -> Option<String> {
    let receipt = write(db, || {
        QueryRequest::write(
            batch::write_batch()
                .var_as("dropped", traversal::g().drop_index(spec.clone()))
                .returning(["dropped"]),
        )
    })
    .await;
    operation_id(&receipt)
}

async fn status(db: &HelixDB, operation: &str) -> serde_json::Value {
    let result = Box::pin(
        db.query(QueryRequest::read(
            batch::read_batch()
                .var_as("status", traversal::g().get_index_operation(operation))
                .returning(["status"]),
        )),
    )
    .await
    .unwrap();
    result["status"].clone()
}

fn scanned(status: &serde_json::Value) -> u64 {
    status["progress"]["entities"]
        .as_str()
        .and_then(|entities| entities.parse().ok())
        .unwrap_or_else(|| panic!("status reports progress: {status}"))
}

pub(super) async fn wait_terminal(db: &HelixDB, operation: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let current = status(db, operation).await;
        match current["status"].as_str() {
            Some("queued" | "running") => {}
            Some(terminal) => return terminal.to_string(),
            None => panic!("malformed operation status {current}"),
        }
        assert!(Instant::now() < deadline, "operation stalled: {current}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Lets the build take steps one ownership handoff at a time until `ready`
/// holds, then returns with ownership held so no further step runs.
async fn pause_when(
    db: &HelixDB,
    operation: &str,
    target: QueueTarget,
    ready: impl Fn(&serde_json::Value) -> bool,
) -> IndexGenerationPublicationPermit {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let permit = db.inner.index_scope_gates.publication_permit(target).await;
        let current = status(db, operation).await;
        if ready(&current) {
            return permit;
        }
        assert!(
            matches!(current["status"].as_str(), Some("queued" | "running")),
            "build finished before pausing: {current}"
        );
        assert!(
            Instant::now() < deadline,
            "build did not progress: {current}"
        );
        drop(permit);
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

pub(super) async fn set_tenant(db: &HelixDB, id: u64, tenant: &str) {
    write(db, || {
        QueryRequest::write(
            batch::write_batch().var_as(
                "moved",
                traversal::g()
                    .n(NodeRef::from(id))
                    .set_property("tenant", PropertyInput::from(tenant.to_string())),
            ),
        )
    })
    .await;
}

/// Final graph state of vector documents: tenant and embedding by node ID.
type VectorState = BTreeMap<u64, (&'static str, [f32; 2])>;

/// Asserts strong and eventual searches equal exact search over `state`.
async fn assert_exact_vectors(db: &HelixDB, state: &VectorState, consistency: SearchConsistency) {
    for tenant in ["a", "b", "c"] {
        for query in [[3.31_f32, 2.77], [9.4, 0.6], [20.2, 3.1], [0.2, 10.8]] {
            let mut exact = state
                .iter()
                .filter(|(_, (owner, _))| *owner == tenant)
                .map(|(id, (_, vector))| {
                    let distance = (vector[0] - query[0]).powi(2) + (vector[1] - query[1]).powi(2);
                    (distance, *id)
                })
                .collect::<Vec<_>>();
            exact.sort_by(|left, right| left.partial_cmp(right).unwrap());
            let expected = exact.iter().take(8).map(|(_, id)| *id).collect::<Vec<_>>();
            let found = vector_search(db, query, 8, Some(tenant), consistency)
                .await
                .into_iter()
                .map(|(id, _)| id)
                .collect::<Vec<_>>();
            assert_eq!(
                found, expected,
                "tenant {tenant} query {query:?} with {consistency:?} search"
            );
        }
    }
}

async fn seed_vectors(db: &HelixDB, count: u16) -> (Vec<u64>, VectorState) {
    let mut ids = Vec::new();
    let mut state = VectorState::new();
    for index in 0..count {
        let tenant = if index % 2 == 0 { "a" } else { "b" };
        let embedding = [f32::from(index % 11), f32::from(index / 11)];
        let id = add(db, embedding, "doc", Some(tenant)).await;
        ids.push(id);
        state.insert(id, (tenant, embedding));
    }
    (ids, state)
}

/// Applies writes on both sides of the build cursor: updates, tenant moves,
/// deletes, and new entities above the build's source watermark.
async fn concurrent_vector_writes(db: &HelixDB, ids: &[u64], state: &mut VectorState, round: u16) {
    let shift = f32::from(round) * 0.25;
    for position in [2_usize, 70] {
        let embedding = [20.5 + shift, 3.0 - shift];
        update(db, ids[position], embedding, "doc").await;
        state.get_mut(&ids[position]).unwrap().1 = embedding;
    }
    for (position, tenant) in [(3_usize, "a"), (71, "c")] {
        set_tenant(db, ids[position], tenant).await;
        state.get_mut(&ids[position]).unwrap().0 = tenant;
    }
    for position in [5_usize + usize::from(round), 73 + usize::from(round)] {
        delete(db, ids[position]).await;
        state.remove(&ids[position]);
    }
    for tenant in ["a", "c"] {
        let embedding = [9.0 + shift, 0.5 + shift];
        let id = add(db, embedding, "doc", Some(tenant)).await;
        state.insert(id, (tenant, embedding));
    }
}

#[tokio::test]
async fn vector_build_with_concurrent_writes_matches_exact_search_through_publication() {
    let db = open("build-vector", Arc::new(InMemory::new()), config()).await;
    let (ids, mut state) = seed_vectors(&db, 120).await;
    let operation = create(&db, vector_spec()).await;
    let target = target(&db, QueueFamily::Vector).await;

    let paused = pause_when(&db, &operation, target, |status| scanned(status) >= 40).await;
    assert!(
        scanned(&status(&db, &operation).await) < 120,
        "paused mid-scan"
    );
    concurrent_vector_writes(&db, &ids, &mut state, 0).await;
    // Moves an entity the scan has not reached yet; the scan then applies
    // this intermediate state before the entity moves again below.
    set_tenant(&db, ids[101], "a").await;
    state.get_mut(&ids[101]).unwrap().0 = "a";

    // The paused build cannot activate, so the canonical record still reads
    // Building: publication defers (classifying before it would wait for
    // ownership) without acknowledging or discarding the build-time
    // operations.
    let queued_before = queue(&db, QueueFamily::Vector)
        .await
        .unwrap()
        .operations()
        .len();
    assert_eq!(
        db.index_queue_publisher()
            .unwrap()
            .publish_once(target)
            .await
            .unwrap(),
        PublicationOutcome::Deferred
    );
    assert_eq!(
        queue(&db, QueueFamily::Vector)
            .await
            .unwrap()
            .operations()
            .len(),
        queued_before
    );
    assert_eq!(db.index_operation_queue_stats().deferred_attempts, 1);
    drop(paused);

    let paused = pause_when(&db, &operation, target, |status| scanned(status) >= 110).await;
    concurrent_vector_writes(&db, &ids, &mut state, 1).await;
    set_tenant(&db, ids[101], "c").await;
    state.get_mut(&ids[101]).unwrap().0 = "c";
    drop(paused);

    assert_eq!(wait_terminal(&db, &operation).await, "succeeded");
    assert!(
        queue(&db, QueueFamily::Vector).await.is_some(),
        "build-time operations stay queued until published"
    );
    assert_exact_vectors(&db, &state, SearchConsistency::Strong).await;

    drain(&db, target).await;
    assert!(queue(&db, QueueFamily::Vector).await.is_none());
    assert!(db
        .index_operation_backlog()
        .outstanding_targets()
        .is_empty());
    assert_exact_vectors(&db, &state, SearchConsistency::Strong).await;
    assert_exact_vectors(&db, &state, SearchConsistency::Eventual).await;
    db.close().await.unwrap();
}

#[tokio::test]
async fn restart_during_a_build_and_after_activation_recovers_queued_operations() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open("build-restart", Arc::clone(&store), config()).await;
    let (ids, mut state) = seed_vectors(&db, 120).await;
    let operation = create(&db, vector_spec()).await;
    let target = target(&db, QueueFamily::Vector).await;
    let paused = pause_when(&db, &operation, target, |status| scanned(status) >= 40).await;
    concurrent_vector_writes(&db, &ids, &mut state, 0).await;
    drop(paused);
    db.close().await.unwrap();

    // The build resumes from its persisted cursor and the ledger reloads the
    // Building generation's queue.
    let db = open("build-restart", Arc::clone(&store), config()).await;
    assert!(db
        .index_operation_backlog()
        .outstanding_targets()
        .contains(&target));
    assert_eq!(wait_terminal(&db, &operation).await, "succeeded");
    db.close().await.unwrap();

    // Reopening right after activation, before any publication, serves the
    // complete generation from physical rows plus the reloaded queue.
    let db = open("build-restart", Arc::clone(&store), config()).await;
    assert_exact_vectors(&db, &state, SearchConsistency::Strong).await;
    drain(&db, target).await;
    assert_exact_vectors(&db, &state, SearchConsistency::Eventual).await;
    db.close().await.unwrap();
}

/// Final text documents by node ID.
type TextState = BTreeMap<u64, String>;

/// Hits as `(score bits, body)` sorted by descending score then body, so
/// databases that assigned different node IDs compare by document.
async fn text_hits(
    db: &HelixDB,
    state: &TextState,
    query: &str,
    consistency: SearchConsistency,
) -> Vec<(u64, String)> {
    let mut hits = text_search(db, query, 200, None, consistency)
        .await
        .into_iter()
        .map(|(id, score)| (score, state[&id].clone()))
        .collect::<Vec<_>>();
    hits.sort_by(|left, right| {
        f64::from_bits(right.0)
            .total_cmp(&f64::from_bits(left.0))
            .then_with(|| left.1.cmp(&right.1))
    });
    hits
}

const TEXT_QUERIES: [&str; 5] = ["rust", "storage", "engine", "graph", "revision"];

#[tokio::test]
async fn text_build_with_concurrent_writes_matches_a_fresh_build_through_publication() {
    let db = open("build-text", Arc::new(InMemory::new()), config()).await;
    let vocabulary = [
        "rust storage",
        "graph engine",
        "rust graph storage",
        "engine",
    ];
    let mut ids = Vec::new();
    let mut state = TextState::new();
    for index in 0..80_usize {
        let body = format!("{} doc{index}", vocabulary[index % vocabulary.len()]);
        let id = add(&db, [0.0, 0.0], &body, None).await;
        ids.push(id);
        state.insert(id, body);
    }
    let operation = create(&db, text_spec()).await;
    let target = target(&db, QueueFamily::Text).await;

    for (round, threshold) in [(0_usize, 24_u64), (1, 64)] {
        let paused = pause_when(&db, &operation, target, |status| {
            scanned(status) >= threshold
        })
        .await;
        for position in [1 + round, 50 + round] {
            let body = format!("revision {round} rust engine rewritten");
            update(&db, ids[position], [0.0, 0.0], &body).await;
            state.insert(ids[position], body);
        }
        for position in [10 + round, 60 + round] {
            delete(&db, ids[position]).await;
            state.remove(&ids[position]);
        }
        // Replacements that stop matching, empty text, and a hot document.
        update(&db, ids[20 + round], [0.0, 0.0], "").await;
        state.insert(ids[20 + round], String::new());
        for revision in 0..3 {
            let body = format!("hot graph revision {revision}");
            update(&db, ids[70], [0.0, 0.0], &body).await;
            state.insert(ids[70], body);
        }
        let body = format!("fresh storage graph arrival {round}");
        let id = add(&db, [0.0, 0.0], &body, None).await;
        state.insert(id, body);
        drop(paused);
    }
    assert_eq!(wait_terminal(&db, &operation).await, "succeeded");

    // Reference: an independent initial build over the final documents.
    let reference = open("build-text-reference", Arc::new(InMemory::new()), config()).await;
    let mut reference_state = TextState::new();
    for body in state.values() {
        let id = add(&reference, [0.0, 0.0], body, None).await;
        reference_state.insert(id, body.clone());
    }
    reference
        .install_index_for_tests(
            ValidatedDynamicIndexDefinition::try_from(
                TextIndexDefinition::new_node("Doc", "body").unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();

    for published in [false, true] {
        if published {
            drain(&db, target).await;
            assert!(queue(&db, QueueFamily::Text).await.is_none());
        }
        for query in TEXT_QUERIES {
            let expected = text_hits(
                &reference,
                &reference_state,
                query,
                SearchConsistency::Strong,
            )
            .await;
            assert_eq!(
                text_hits(&db, &state, query, SearchConsistency::Strong).await,
                expected,
                "strong {query:?} (published: {published})"
            );
            if published {
                assert_eq!(
                    text_hits(&db, &state, query, SearchConsistency::Eventual).await,
                    expected,
                    "eventual {query:?}"
                );
            }
        }
    }
    reference.close().await.unwrap();
    db.close().await.unwrap();
}

const VOCABULARY: [&str; 4] = [
    "rust storage",
    "graph engine",
    "rust graph storage",
    "engine",
];

/// Live source documents by node ID, as vector and text index state.
#[derive(Default)]
struct Documents {
    live: Vec<u64>,
    vectors: VectorState,
    texts: TextState,
}

/// One document's tenant, embedding, and body.
type Document = (&'static str, [f32; 2], String);

/// Seeded document `index`: tenant "a" or "b", an embedding on the integer
/// grid, and a body from [`VOCABULARY`].
fn seeded(index: u16) -> Document {
    let tenant = if index.is_multiple_of(2) { "a" } else { "b" };
    let embedding = [f32::from(index % 11), f32::from(index / 11)];
    let body = format!(
        "{} doc{index}",
        VOCABULARY[usize::from(index) % VOCABULARY.len()]
    );
    (tenant, embedding, body)
}

impl Documents {
    /// Adds `documents` in one write.
    async fn add(&mut self, db: &HelixDB, documents: Vec<Document>) {
        let name = |ordinal: usize| format!("created{ordinal}");
        let result = write(db, || {
            let batch = documents.iter().enumerate().fold(
                batch::write_batch(),
                |batch, (ordinal, (tenant, embedding, body))| {
                    batch.var_as(
                        &name(ordinal),
                        traversal::g().add_n(
                            "Doc",
                            vec![
                                ("embedding", PropertyInput::from(embedding.to_vec())),
                                ("body", PropertyInput::from(body.clone())),
                                ("tenant", PropertyInput::from(tenant.to_string())),
                            ],
                        ),
                    )
                },
            );
            QueryRequest::write(batch.returning((0..documents.len()).map(name)))
        })
        .await;
        for (ordinal, (tenant, embedding, body)) in documents.into_iter().enumerate() {
            let id = result[name(ordinal)][0]["$id"].as_u64().unwrap();
            self.live.push(id);
            self.vectors.insert(id, (tenant, embedding));
            self.texts.insert(id, body);
        }
    }

    /// Applies change `round % 8` to live document `id`: an update (0-3), a
    /// tenant move (4, to "a" when `round % 16 == 4`, else to "c"), an insert
    /// of a new document (5), a delete (6), or an update to empty text (7).
    /// `serial` numbers the write among all writers, so every written
    /// embedding is distinct and off the seeded grid, and exact search has
    /// no ties.
    async fn change(&mut self, db: &HelixDB, id: u64, round: u64, serial: u64) {
        let embedding = [
            (serial % 20) as f32 + 0.5 + (serial / 20) as f32 * 0.001,
            (serial % 7) as f32 + 0.25,
        ];
        let body = format!(
            "revision {} {serial}",
            VOCABULARY[usize::try_from(serial).unwrap() % VOCABULARY.len()]
        );
        match round % 8 {
            0..=3 => {
                update(db, id, embedding, &body).await;
                self.vectors.get_mut(&id).unwrap().1 = embedding;
                self.texts.insert(id, body);
            }
            4 => {
                let tenant = if round % 16 == 4 { "a" } else { "c" };
                set_tenant(db, id, tenant).await;
                self.vectors.get_mut(&id).unwrap().0 = tenant;
            }
            5 => self.add(db, vec![("c", embedding, body)]).await,
            6 => {
                delete(db, id).await;
                self.live.retain(|live| *live != id);
                self.vectors.remove(&id);
                self.texts.remove(&id);
            }
            _ => {
                update(db, id, embedding, "").await;
                self.vectors.get_mut(&id).unwrap().1 = embedding;
                self.texts.insert(id, String::new());
            }
        }
    }

    /// Writes to these documents until `building` clears, spreading writes
    /// over both sides of every build cursor and above the source watermark.
    /// Returns the number of writes.
    async fn write_while(
        &mut self,
        db: &HelixDB,
        building: &AtomicBool,
        serial: &AtomicU64,
    ) -> u64 {
        let mut round = 0_u64;
        while building.load(Ordering::SeqCst) {
            let id = self.live[usize::try_from(round * 37).unwrap() % self.live.len()];
            self.change(db, id, round, serial.fetch_add(1, Ordering::SeqCst))
                .await;
            round += 1;
        }
        round
    }
}

/// Publishes both queues of `db`, then asserts its vector and text searches
/// equal exact search over `documents` and a cold build of them.
async fn assert_matches_cold_builds(db: &HelixDB, documents: &Documents, reference: &str) {
    for family in [QueueFamily::Vector, QueueFamily::Text] {
        assert!(
            queue(db, family).await.is_some(),
            "{family:?} writes during the build stay queued until published"
        );
        drain(db, target(db, family).await).await;
        assert!(queue(db, family).await.is_none());
    }

    // Reference: cold builds over the final documents, in default batches.
    let reference = open(
        reference,
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    let mut cold = Documents::default();
    cold.add(
        &reference,
        documents
            .live
            .iter()
            .map(|id| {
                let (tenant, embedding) = documents.vectors[id];
                (tenant, embedding, documents.texts[id].clone())
            })
            .collect(),
    )
    .await;
    for spec in [vector_spec(), text_spec()] {
        let operation = create(&reference, spec).await;
        assert_eq!(wait_terminal(&reference, &operation).await, "succeeded");
    }

    assert_exact_vectors(&reference, &cold.vectors, SearchConsistency::Strong).await;
    for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
        assert_exact_vectors(db, &documents.vectors, consistency).await;
        for query in TEXT_QUERIES {
            assert_eq!(
                text_hits(db, &documents.texts, query, consistency).await,
                text_hits(&reference, &cold.texts, query, SearchConsistency::Strong).await,
                "{consistency:?} {query:?}"
            );
        }
    }
    reference.close().await.unwrap();
}

/// The first vector `Scan` step, then the first text `ScanSource` step, is
/// held after it stages while queued writes commit: updates, a tenant move,
/// a delete, and empty text on rows the step read and on rows ahead of it,
/// and an insert above its source watermark. Each step still commits on its
/// first attempt, and once the queue drains both indexes equal cold builds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn first_scan_steps_commit_through_queued_writes_in_their_window() {
    let db = open(
        "build-step-window-writes",
        Arc::new(InMemory::new()),
        config(),
    )
    .await;
    let mut documents = Documents::default();
    documents.add(&db, (0..24).map(seeded).collect()).await;
    let first_scans: [(IndexSpec, step_pause::StepMatcher); 2] = [
        (vector_spec(), |progress| {
            matches!(
                progress,
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                    VectorBuildStage::Scan(SourceScanProgress { cursor: None, .. })
                ))
            )
        }),
        (text_spec(), |progress| {
            matches!(
                progress,
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::ScanSource(SourceScanProgress { cursor: None, .. })
                ))
            )
        }),
    ];
    let mut serial = 0;
    let mut builds = Vec::new();
    for (spec, first_scan) in first_scans {
        let pause = StepPause::arm(db.inner_db().as_ref(), first_scan);
        let operation = create(&db, spec).await;
        tokio::time::timeout(Duration::from_secs(60), pause.reached())
            .await
            .expect("the first scan step stages");
        // The held step read one four-entity batch: the lowest live IDs.
        let mut live = documents.live.clone();
        live.sort_unstable();
        let (read, ahead) = live.split_at(4);
        for (id, round) in [
            (read[0], 0),
            (read[1], 4),
            (read[2], 6),
            (read[3], 7),
            (ahead[0], 0),
            (ahead[1], 12),
            (ahead[2], 6),
            (ahead[3], 7),
            // Round 5 inserts a new document.
            (ahead[4], 5),
        ] {
            documents.change(&db, id, round, serial).await;
            serial += 1;
        }
        pause.release();
        builds.push((operation, pause));
    }
    for (operation, pause) in builds {
        assert_eq!(wait_terminal(&db, &operation).await, "succeeded");
        assert_eq!(
            pause.stagings(),
            1,
            "the held step committed on its first attempt"
        );
    }
    assert_matches_cold_builds(&db, &documents, "build-step-window-writes-reference").await;
    db.close().await.unwrap();
}

/// Concurrent writers, each over its own documents.
const WRITERS: u16 = 16;

/// Writers update, move, delete, and insert documents concurrently for as
/// long as unpaused vector and text builds run, so writes commit inside
/// nearly every build step. Both builds activate, and once the queue drains,
/// searches equal exact search and a cold build of the final documents.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn builds_under_continuous_writes_match_cold_builds_once_the_queue_drains() {
    let db = open(
        "build-continuous-writes",
        Arc::new(InMemory::new()),
        config(),
    )
    .await;
    let mut shards = Vec::new();
    for writer in 0..WRITERS {
        let mut shard = Documents::default();
        shard
            .add(&db, (writer * 8..(writer + 1) * 8).map(seeded).collect())
            .await;
        shards.push(shard);
    }
    let vector_operation = create(&db, vector_spec()).await;
    let text_operation = create(&db, text_spec()).await;
    let building = AtomicBool::new(true);
    let serial = AtomicU64::new(0);
    let (outcomes, writes) = tokio::join!(
        async {
            let outcomes = [
                wait_terminal(&db, &vector_operation).await,
                wait_terminal(&db, &text_operation).await,
            ];
            building.store(false, Ordering::SeqCst);
            outcomes
        },
        futures::future::join_all(
            shards
                .iter_mut()
                .map(|shard| shard.write_while(&db, &building, &serial)),
        ),
    );
    assert_eq!(outcomes, ["succeeded", "succeeded"]);
    assert!(
        writes.iter().all(|count| *count > 0),
        "every writer ran during the builds: {writes:?}"
    );
    let documents = shards
        .into_iter()
        .fold(Documents::default(), |mut documents, shard| {
            documents.live.extend(shard.live);
            documents.vectors.extend(shard.vectors);
            documents.texts.extend(shard.texts);
            documents
        });
    assert_matches_cold_builds(&db, &documents, "build-continuous-writes-reference").await;
    db.close().await.unwrap();
}

/// Seeds 120 documents and creates the vector index, holding its first
/// `Scan` step once it staged. That step read the four lowest IDs, so a write
/// to any later document, or an insert under the source watermark, commits
/// before the build reads it and later publishes as a replay.
async fn hold_first_vector_scan(db: &HelixDB) -> (Vec<u64>, VectorState, String, StepPause) {
    let (ids, state) = seed_vectors(db, 120).await;
    let pause = StepPause::arm(db.inner_db().as_ref(), |progress| {
        matches!(
            progress,
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                VectorBuildStage::Scan(SourceScanProgress { cursor: None, .. })
            ))
        )
    });
    let operation = create(db, vector_spec()).await;
    tokio::time::timeout(Duration::from_secs(60), pause.reached())
        .await
        .expect("the first scan step stages");
    (ids, state, operation, pause)
}

/// Writes documents the held first scan step has not read: updates, a
/// two-write chain, tenant moves, and inserts under the source watermark.
async fn write_ahead_of_the_scan(db: &HelixDB, ids: &[u64], state: &mut VectorState) {
    for (position, embedding) in [(10, [20.5, 3.0]), (60, [9.25, 0.75]), (60, [0.5, 10.5])] {
        update(db, ids[position], embedding, "doc").await;
        state.get_mut(&ids[position]).unwrap().1 = embedding;
    }
    for (position, tenant) in [(11, "c"), (70, "b")] {
        set_tenant(db, ids[position], tenant).await;
        state.get_mut(&ids[position]).unwrap().0 = tenant;
    }
    for (tenant, embedding) in [("a", [3.5, 2.5]), ("c", [15.5, 1.5])] {
        let id = add(db, embedding, "doc", Some(tenant)).await;
        state.insert(id, (tenant, embedding));
    }
}

/// Whether `key` is a node's property row or a vector row that places a
/// node: its vector, SimHash, or layer, but not its links or index metadata.
fn places_or_documents_a_node(key: &[u8]) -> bool {
    matches!(
        DataKey::parse_from_slice(DataScope::LegacyUnscoped, key),
        Ok(DataKey::Data {
            kind: DataKeyKind::NodeProperty(_)
                | DataKeyKind::Vector(
                    VectorKey::Vector(_)
                        | VectorKey::UpperVector(_)
                        | VectorKey::SimHash(_)
                        | VectorKey::SimHashDirectory(_)
                        | VectorKey::EntryCandidateSorted(_)
                        | VectorKey::EntryCandidateNode(_)
                ),
            ..
        })
    )
}

/// Every queued write replays a state the build read, so publication
/// acknowledges each chain without changing a single vector row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn draining_replays_of_a_build_changes_no_vector_row() {
    let db = open("build-replays", Arc::new(InMemory::new()), config()).await;
    let (ids, mut state, operation, pause) = hold_first_vector_scan(&db).await;
    write_ahead_of_the_scan(&db, &ids, &mut state).await;
    pause.release();
    assert_eq!(wait_terminal(&db, &operation).await, "succeeded");
    let built = rows(&db, VectorKey::is_vector_keyspace).await;

    drain(&db, target(&db, QueueFamily::Vector).await).await;
    assert!(queue(&db, QueueFamily::Vector).await.is_none());
    assert!(db
        .index_operation_backlog()
        .outstanding_targets()
        .is_empty());
    assert_eq!(rows(&db, VectorKey::is_vector_keyspace).await, built);
    assert_exact_vectors(&db, &state, SearchConsistency::Strong).await;
    assert_exact_vectors(&db, &state, SearchConsistency::Eventual).await;
    db.close().await.unwrap();
}

/// A build under replays and real changes, drained once with replays
/// skipped and once through the full path, holds the same documents and the
/// same vector, SimHash, and layer of every node; only links differ.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn build_then_drain_places_every_node_as_the_unskipped_run_does() {
    let mut runs = Vec::new();
    for replace in [false, true] {
        let name = if replace {
            "build-replays-replaced"
        } else {
            "build-replays-skipped"
        };
        let db = open(name, Arc::new(InMemory::new()), config()).await;
        let (ids, mut state, operation, pause) = hold_first_vector_scan(&db).await;
        // Real changes to documents the held step read, and a chain back to
        // the value it read.
        update(&db, ids[0], [20.25, 3.25], "doc").await;
        state.get_mut(&ids[0]).unwrap().1 = [20.25, 3.25];
        set_tenant(&db, ids[1], "c").await;
        state.get_mut(&ids[1]).unwrap().0 = "c";
        delete(&db, ids[2]).await;
        state.remove(&ids[2]);
        update(&db, ids[3], [7.5, 7.5], "doc").await;
        update(&db, ids[3], state[&ids[3]].1, "doc").await;
        write_ahead_of_the_scan(&db, &ids, &mut state).await;
        pause.release();
        assert_eq!(wait_terminal(&db, &operation).await, "succeeded");
        let built = rows(&db, VectorKey::is_vector_keyspace).await;

        let target = target(&db, QueueFamily::Vector).await;
        if replace {
            crate::search::vector::REPLACE_REPLAYS
                .scope((), drain(&db, target))
                .await;
        } else {
            drain(&db, target).await;
        }
        assert!(queue(&db, QueueFamily::Vector).await.is_none());
        for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
            assert_exact_vectors(&db, &state, consistency).await;
        }
        runs.push((
            built,
            rows(&db, VectorKey::is_vector_keyspace).await,
            rows(&db, places_or_documents_a_node).await,
        ));
        db.close().await.unwrap();
    }
    let [(skipped_built, skipped, skipped_nodes), (replaced_built, replaced, replaced_nodes)] =
        <[_; 2]>::try_from(runs).unwrap();
    assert_eq!(
        skipped_built, replaced_built,
        "both builds wrote the same rows"
    );
    assert_eq!(skipped_nodes, replaced_nodes);
    assert_ne!(skipped, replaced, "the full path relinked replayed nodes");
}

async fn discard_all(db: &HelixDB, target: QueueTarget) -> u64 {
    let publisher = db.index_queue_publisher().unwrap();
    let mut discarded = 0;
    loop {
        match publisher.publish_once(target).await.unwrap() {
            PublicationOutcome::Discarded { operations } => discarded += operations,
            PublicationOutcome::Empty => return discarded,
            outcome @ (PublicationOutcome::Published { .. }
            | PublicationOutcome::Trimmed
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry
            | PublicationOutcome::Blocked) => {
                panic!("retired queue was not discarded: {outcome:?}")
            }
        }
    }
}

#[tokio::test]
async fn dropping_an_index_discards_its_queued_operations_even_after_restart() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open("drop-discard", Arc::clone(&store), config()).await;
    let operation = create(&db, vector_spec()).await;
    assert_eq!(wait_terminal(&db, &operation).await, "succeeded");
    let target = target(&db, QueueFamily::Vector).await;
    for index in 0..5_u8 {
        add(&db, [f32::from(index), 1.0], "doc", Some("a")).await;
    }
    let drop_operation = drop_index(&db, vector_spec()).await.unwrap();
    assert_eq!(wait_terminal(&db, &drop_operation).await, "succeeded");
    assert_eq!(
        db.index_operation_backlog()
            .usage(target.scope, target.index_id)
            .operations,
        5,
        "retired work stays charged until discarded"
    );
    db.close().await.unwrap();

    let db = open("drop-discard", Arc::clone(&store), config()).await;
    assert!(db
        .index_operation_backlog()
        .outstanding_targets()
        .contains(&target));
    assert_eq!(discard_all(&db, target).await, 5);
    assert!(db
        .index_operation_backlog()
        .outstanding_targets()
        .is_empty());
    assert!(db.inner_db().get(target.key()).await.unwrap().is_none());
    let stats = db.index_operation_queue_stats();
    assert_eq!(stats.discarded_operations, 5);
    assert_eq!(stats.queue_reads, 1, "the discard read is counted");
    db.close().await.unwrap();
}

#[tokio::test]
async fn aborting_a_build_discards_operations_written_during_it() {
    let db = open("abort-discard", Arc::new(InMemory::new()), config()).await;
    let (ids, _) = seed_vectors(&db, 60).await;
    let operation = create(&db, vector_spec()).await;
    let target = target(&db, QueueFamily::Vector).await;
    let paused = pause_when(&db, &operation, target, |status| scanned(status) >= 8).await;
    update(&db, ids[1], [4.0, 4.0], "doc").await;
    update(&db, ids[50], [5.0, 5.0], "doc").await;
    add(&db, [6.0, 6.0], "doc", Some("a")).await;
    let abort = drop_index(&db, vector_spec())
        .await
        .unwrap_or_else(|| operation.clone());
    drop(paused);
    assert_eq!(wait_terminal(&db, &operation).await, "aborted");
    if abort != operation {
        assert_eq!(wait_terminal(&db, &abort).await, "succeeded");
    }
    assert_eq!(discard_all(&db, target).await, 3);
    assert!(db.inner_db().get(target.key()).await.unwrap().is_none());
    assert!(db
        .index_operation_backlog()
        .outstanding_targets()
        .is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn discard_acknowledgements_fit_the_producer_operand_bound() {
    // A 1 KiB operand bound admits 63 acknowledged IDs: a 5-byte header plus
    // 16 bytes per ID.
    let config = config().with_index_operation_queue_tuning(
        IndexOperationQueueTuning::default()
            .with_max_operand_bytes(NonZeroU64::new(1024).unwrap())
            .with_publication_paused_for_tests(),
    );
    let db = open("discard-ack-bound", Arc::new(InMemory::new()), config).await;
    let operation = create(&db, vector_spec()).await;
    assert_eq!(wait_terminal(&db, &operation).await, "succeeded");
    let target = target(&db, QueueFamily::Vector).await;
    for index in 0..100_u8 {
        add(&db, [f32::from(index), 1.0], "doc", Some("a")).await;
    }
    let drop_operation = drop_index(&db, vector_spec()).await.unwrap();
    assert_eq!(wait_terminal(&db, &drop_operation).await, "succeeded");
    assert_eq!(
        release_within_operand_bound(&db, target, QueueFamily::Vector).await,
        100
    );
    assert!(db
        .index_operation_backlog()
        .outstanding_targets()
        .is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_building_generation_defers_without_waiting_for_its_build_step() {
    let db = open("build-defer", Arc::new(InMemory::new()), config()).await;
    let (ids, _) = seed_vectors(&db, 60).await;
    let operation = create(&db, vector_spec()).await;
    let target = target(&db, QueueFamily::Vector).await;
    let paused = pause_when(&db, &operation, target, |status| scanned(status) >= 8).await;
    update(&db, ids[1], [4.0, 4.0], "doc").await;
    // The paused build step holds the generation's ownership. Deferring a
    // hidden build must not occupy a worker task until that step ends.
    let deferred = tokio::time::timeout(
        Duration::from_secs(5),
        db.index_queue_publisher().unwrap().publish_once(target),
    )
    .await
    .expect("a hidden build defers without waiting for its step");
    assert_eq!(deferred.unwrap(), PublicationOutcome::Deferred);
    drop(paused);
    assert_eq!(wait_terminal(&db, &operation).await, "succeeded");
    drain(&db, target).await;
    assert!(db
        .index_operation_backlog()
        .outstanding_targets()
        .is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn uncertain_charges_of_a_hidden_build_reconcile_while_it_runs() {
    let db = open("build-reconcile", Arc::new(InMemory::new()), config()).await;
    let (ids, _) = seed_vectors(&db, 60).await;
    let operation = create(&db, vector_spec()).await;
    let target = target(&db, QueueFamily::Vector).await;
    let paused = pause_when(&db, &operation, target, |status| scanned(status) >= 8).await;
    update(&db, ids[1], [4.0, 4.0], "doc").await;
    // A request cancelled during its commit leaves an uncertain charge; this
    // one never committed.
    let mut lost = db
        .index_operation_backlog()
        .reserve(vec![OperationCharge {
            target,
            entity: IndexEntity {
                kind: IndexElementKind::Node,
                id: IndexEntityId::new(ids[2]),
            },
            id: QueuedOperationId::generate(),
            bytes: 64,
        }])
        .unwrap();
    lost.begin_commit();
    drop(lost);
    let usage = || {
        let usage = db
            .index_operation_backlog()
            .usage(target.scope, target.index_id);
        (usage.operations, usage.uncertain_operations)
    };
    assert_eq!(usage(), (2, 1));
    // Limits count the whole logical index, so the phantom charge would
    // throttle writes for as long as the build runs. Reconciling only reads
    // the queue, so it must not wait for the paused step's ownership.
    let deferred = tokio::time::timeout(
        Duration::from_secs(5),
        db.index_queue_publisher().unwrap().publish_once(target),
    )
    .await
    .expect("a hidden build defers without waiting for its step");
    assert_eq!(deferred.unwrap(), PublicationOutcome::Deferred);
    assert_eq!(usage(), (1, 0), "the lost enqueue was released");
    drop(paused);
    assert_eq!(wait_terminal(&db, &operation).await, "succeeded");
    drain(&db, target).await;
    assert_eq!(usage(), (0, 0));
    db.close().await.unwrap();
}

#[tokio::test]
async fn uncertain_charges_of_an_active_generation_reconcile_while_its_ownership_is_held() {
    let db = open("active-reconcile", Arc::new(InMemory::new()), config()).await;
    let (ids, _) = seed_vectors(&db, 3).await;
    let operation = create(&db, vector_spec()).await;
    assert_eq!(wait_terminal(&db, &operation).await, "succeeded");
    let target = target(&db, QueueFamily::Vector).await;
    update(&db, ids[1], [4.0, 4.0], "doc").await;
    // A request cancelled during its commit leaves an uncertain charge; this
    // one never committed.
    let mut lost = db
        .index_operation_backlog()
        .reserve(vec![OperationCharge {
            target,
            entity: IndexEntity {
                kind: IndexElementKind::Node,
                id: IndexEntityId::new(ids[2]),
            },
            id: QueuedOperationId::generate(),
            bytes: 64,
        }])
        .unwrap();
    lost.begin_commit();
    drop(lost);
    let usage = || {
        let usage = db
            .index_operation_backlog()
            .usage(target.scope, target.index_id);
        (usage.operations, usage.uncertain_operations)
    };
    assert_eq!(usage(), (2, 1));
    // Compaction or a lifecycle step holds the Active generation's ownership
    // for a whole step. Reconciling only reads the queue, so the phantom
    // charge is released before the attempt waits for that ownership.
    let owned = db.inner.index_scope_gates.publication_permit(target).await;
    let publisher = Arc::clone(db.index_queue_publisher().unwrap());
    let waiting = tokio::spawn(async move { publisher.publish_once(target).await });
    let deadline = Instant::now() + Duration::from_secs(60);
    while db
        .inner
        .index_scope_gates
        .publication_permit_holders(target)
        < 2
    {
        assert!(
            Instant::now() < deadline,
            "publication never awaited ownership"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(usage(), (1, 0), "the lost enqueue was released");
    drop(owned);
    assert_eq!(
        waiting.await.unwrap().unwrap(),
        PublicationOutcome::Published {
            operations: 1,
            entities: 1
        }
    );
    assert_eq!(usage(), (0, 0));
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_retired_generation_discards_without_waiting_for_its_cleanup_step() {
    let db = open("abort-discard-unowned", Arc::new(InMemory::new()), config()).await;
    let (ids, _) = seed_vectors(&db, 60).await;
    let operation = create(&db, vector_spec()).await;
    let target = target(&db, QueueFamily::Vector).await;
    let paused = pause_when(&db, &operation, target, |status| scanned(status) >= 8).await;
    update(&db, ids[1], [4.0, 4.0], "doc").await;
    add(&db, [6.0, 6.0], "doc", Some("a")).await;
    let abort = drop_index(&db, vector_spec())
        .await
        .unwrap_or_else(|| operation.clone());
    // The paused step holds the generation's ownership, as every abort and
    // cleanup step does. A discard touches only the queue, so it must not
    // occupy a worker task until that step ends.
    let discarded = tokio::time::timeout(Duration::from_secs(5), discard_all(&db, target))
        .await
        .expect("a retired generation discards without waiting for its step");
    assert_eq!(discarded, 2);
    drop(paused);
    assert_eq!(wait_terminal(&db, &operation).await, "aborted");
    if abort != operation {
        assert_eq!(wait_terminal(&db, &abort).await, "succeeded");
    }
    assert!(db.inner_db().get(target.key()).await.unwrap().is_none());
    assert!(db
        .index_operation_backlog()
        .outstanding_targets()
        .is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_generation_retired_while_awaiting_ownership_retries_then_discards() {
    let db = open("retire-awaiting", Arc::new(InMemory::new()), config()).await;
    let operation = create(&db, vector_spec()).await;
    assert_eq!(wait_terminal(&db, &operation).await, "succeeded");
    let target = target(&db, QueueFamily::Vector).await;
    add(&db, [1.0, 1.0], "doc", Some("a")).await;
    // Compaction or a lifecycle step holds the Active generation's ownership.
    let owned = db.inner.index_scope_gates.publication_permit(target).await;
    let publisher = Arc::clone(db.index_queue_publisher().unwrap());
    let waiting = tokio::spawn(async move { publisher.publish_once(target).await });
    let deadline = Instant::now() + Duration::from_secs(60);
    while db
        .inner
        .index_scope_gates
        .publication_permit_holders(target)
        < 2
    {
        assert!(
            Instant::now() < deadline,
            "publication never awaited ownership"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let drop_operation = drop_index(&db, vector_spec()).await.unwrap();
    drop(owned);
    // Its transaction re-reads the record under ownership and retries
    // instead of publishing into the retired generation.
    assert_eq!(waiting.await.unwrap().unwrap(), PublicationOutcome::Retry);
    assert_eq!(discard_all(&db, target).await, 1);
    assert_eq!(wait_terminal(&db, &drop_operation).await, "succeeded");
    assert!(db
        .index_operation_backlog()
        .outstanding_targets()
        .is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn row_discards_fill_the_output_operation_budget() {
    let config = config().with_index_operation_queue_tuning(
        IndexOperationQueueTuning::default()
            .with_layout(QueueLayout::Rows)
            .with_publication_paused_for_tests(),
    );
    let db = open("discard-rows-budget", Arc::new(InMemory::new()), config).await;
    let operation = create(&db, vector_spec()).await;
    assert_eq!(wait_terminal(&db, &operation).await, "succeeded");
    let target = target(&db, QueueFamily::Vector).await;
    for index in 0..20_u8 {
        add(&db, [f32::from(index), 1.0], "doc", Some("a")).await;
    }
    let drop_operation = drop_index(&db, vector_spec()).await.unwrap();
    assert_eq!(wait_terminal(&db, &drop_operation).await, "succeeded");
    // A row discard deletes one row per operation and stages nothing else,
    // so each fills, and never exceeds, an eight-write transaction.
    let defaults = SearchIndexBackfillLimits::default();
    let batch = defaults.batch();
    let narrow = publisher_with_limits(
        &db,
        SearchIndexBatchLimits::try_new(
            batch.max_entities(),
            batch.max_input_bytes(),
            NonZeroU64::new(8).unwrap(),
            batch.max_output_bytes(),
            batch.max_single_vector_output_bytes(),
        )
        .unwrap(),
        defaults.active_text_mutation(),
    );
    let mut discarded = Vec::new();
    loop {
        match narrow.publish_once(target).await.unwrap() {
            PublicationOutcome::Discarded { operations } => discarded.push(operations),
            PublicationOutcome::Empty => break,
            outcome @ (PublicationOutcome::Published { .. }
            | PublicationOutcome::Trimmed
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry
            | PublicationOutcome::Blocked) => {
                panic!("retired queue was not discarded: {outcome:?}")
            }
        }
    }
    assert_eq!(discarded, [8, 8, 4]);
    assert!(db
        .index_operation_backlog()
        .outstanding_targets()
        .is_empty());
    db.close().await.unwrap();
}

async fn retry(db: &HelixDB, operation: &str) {
    write(db, || {
        QueryRequest::write(
            batch::write_batch()
                .var_as("retried", traversal::g().retry_index_operation(operation))
                .returning(["retried"]),
        )
    })
    .await;
}

fn text_ids(hits: Vec<(u64, u64)>) -> Vec<u64> {
    hits.into_iter().map(|(id, _)| id).collect()
}

/// A build admits a document only when any later replacement can publish, so
/// it blocks on an oversized source row until that row is repaired.
#[tokio::test]
async fn text_build_blocks_on_a_document_publication_could_not_replace() {
    let db = open(
        "build-text-oversized",
        Arc::new(InMemory::new()),
        tight_publication_config(),
    )
    .await;
    let resident = add(&db, [0.0, 0.0], "small resident", None).await;
    let wide = add(&db, [0.0, 0.0], &distinct_terms("wide", 40, 4), None).await;
    let operation = create(&db, text_spec()).await;
    assert_eq!(wait_terminal(&db, &operation).await, "blocked");
    assert_eq!(
        status(&db, &operation).await["blocker_code"],
        "oversized_entity"
    );
    // The oversized row is only the previous document of the repair.
    update(&db, wide, [0.0, 0.0], "narrow repaired").await;
    retry(&db, &operation).await;
    assert_eq!(wait_terminal(&db, &operation).await, "succeeded");
    let target = target(&db, QueueFamily::Text).await;
    drain(&db, target).await;
    for (query, expected) in [("repaired", wide), ("resident", resident)] {
        assert_eq!(
            text_ids(text_search(&db, query, 10, None, SearchConsistency::Eventual).await),
            vec![expected],
            "{query}"
        );
    }
    db.close().await.unwrap();
}

/// Rows a blocked build rejects as invalid can be repaired or deleted while
/// the build still routes writes to its hidden generation.
#[tokio::test]
async fn blocked_text_build_accepts_repairs_of_invalid_source_rows() {
    let db = open("build-text-invalid", Arc::new(InMemory::new()), config()).await;
    let mut invalid = Vec::new();
    for value in [7_i64, 8] {
        let created = write(&db, || {
            QueryRequest::write(
                batch::write_batch()
                    .var_as(
                        "created",
                        traversal::g().add_n("Doc", vec![("body", PropertyInput::from(value))]),
                    )
                    .returning(["created"]),
            )
        })
        .await;
        invalid.push(created["created"][0]["$id"].as_u64().unwrap());
    }
    let resident = add(&db, [0.0, 0.0], "valid resident", None).await;
    let operation = create(&db, text_spec()).await;
    assert_eq!(wait_terminal(&db, &operation).await, "blocked");
    assert_eq!(
        status(&db, &operation).await["blocker_code"],
        "invalid_source_data"
    );
    for (id, repair) in invalid.iter().zip([
        QueryRequest::write(
            batch::write_batch().var_as(
                "repaired",
                traversal::g()
                    .n(NodeRef::from(invalid[0]))
                    .set_property("body", "repaired words".to_string()),
            ),
        ),
        QueryRequest::write(batch::write_batch().var_as(
            "deleted",
            traversal::g().n(NodeRef::from(invalid[1])).drop(),
        )),
    ]) {
        db.query(repair)
            .await
            .unwrap_or_else(|error| panic!("repairing invalid row {id} failed: {error}"));
    }
    retry(&db, &operation).await;
    assert_eq!(wait_terminal(&db, &operation).await, "succeeded");
    let target = target(&db, QueueFamily::Text).await;
    drain(&db, target).await;
    for (query, expected) in [("repaired", invalid[0]), ("resident", resident)] {
        assert_eq!(
            text_ids(text_search(&db, query, 10, None, SearchConsistency::Eventual).await),
            vec![expected],
            "{query}"
        );
    }
    db.close().await.unwrap();
}

/// Vector rows a blocked build rejects can be repaired while the build still
/// routes writes to its hidden generation.
#[tokio::test]
async fn blocked_vector_build_accepts_repairs_of_invalid_source_rows() {
    let db = open("build-vector-invalid", Arc::new(InMemory::new()), config()).await;
    let created = write(&db, || {
        QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "created",
                    traversal::g().add_n(
                        "Doc",
                        vec![
                            ("embedding", PropertyInput::from(vec![1.0_f32, 2.0, 3.0])),
                            ("tenant", PropertyInput::from("a".to_string())),
                        ],
                    ),
                )
                .returning(["created"]),
        )
    })
    .await;
    let invalid = created["created"][0]["$id"].as_u64().unwrap();
    let operation = create(&db, vector_spec()).await;
    assert_eq!(wait_terminal(&db, &operation).await, "blocked");
    assert_eq!(
        status(&db, &operation).await["blocker_code"],
        "invalid_source_data"
    );
    db.query(QueryRequest::write(
        batch::write_batch().var_as(
            "repaired",
            traversal::g()
                .n(NodeRef::from(invalid))
                .set_property("embedding", vec![0.5_f32, 0.5]),
        ),
    ))
    .await
    .unwrap_or_else(|error| panic!("repairing the wrong-dimension row failed: {error}"));
    retry(&db, &operation).await;
    assert_eq!(wait_terminal(&db, &operation).await, "succeeded");
    let target = target(&db, QueueFamily::Vector).await;
    drain(&db, target).await;
    let hits = vector_search(&db, [0.5, 0.5], 10, Some("a"), SearchConsistency::Eventual).await;
    assert_eq!(
        hits.into_iter().map(|(id, _)| id).collect::<Vec<_>>(),
        vec![invalid]
    );
    db.close().await.unwrap();
}
