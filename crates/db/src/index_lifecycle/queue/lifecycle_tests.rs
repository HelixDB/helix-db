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
use super::publication_tests::batch_limits;
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

pub(super) fn scanned(status: &serde_json::Value) -> u64 {
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
pub(super) async fn pause_when(
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
const WRITERS: u16 = 4;

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

/// A build reads every entity at its latest state, so the chains its scan
/// raced stay queued as replays of states no newer than the rows it wrote.
/// Whatever the eventual budget cuts, an eventual search must show such an
/// entity at that latest state or through its built row, never at an earlier
/// state of its chain: here, a document updated twice ahead of the scan and
/// one that moves to a tenant whose name makes its latest operation far
/// larger than its first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eventual_searches_after_a_build_never_show_a_state_older_than_its_rows() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open("build-eventual-budgets", Arc::clone(&store), config()).await;
    let (ids, mut state, operation, pause) = hold_first_vector_scan(&db).await;
    write_ahead_of_the_scan(&db, &ids, &mut state).await;
    let far: &'static str = Box::leak("f".repeat(300).into_boxed_str());
    update(&db, ids[100], [50.0, 50.0], "doc").await;
    write(&db, || {
        QueryRequest::write(
            batch::write_batch().var_as(
                "moved",
                traversal::g()
                    .n(NodeRef::from(ids[100]))
                    .set_property("embedding", vec![-50.0_f32, -50.0])
                    .set_property("tenant", PropertyInput::from(far.to_string())),
            ),
        )
    })
    .await;
    state.insert(ids[100], (far, [-50.0, -50.0]));
    pause.release();
    assert_eq!(wait_terminal(&db, &operation).await, "succeeded");
    let sizes = queue(&db, QueueFamily::Vector)
        .await
        .expect("the raced chains stay queued as replays")
        .operations()
        .iter()
        .map(|operation| operation.retained_bytes())
        .collect::<Vec<_>>();
    db.close().await.unwrap();

    // No budget, every budget that ends between two operations, and all.
    let budgets = std::iter::once(0).chain(sizes.iter().scan(0, |total, size| {
        *total += size;
        Some(*total)
    }));
    for budget in budgets {
        let base = config();
        let tuning = base
            .index_operation_queue()
            .with_eventual_search_budget_for_tests(budget);
        let reopened = base.with_index_operation_queue_tuning(tuning);
        let db = open("build-eventual-budgets", Arc::clone(&store), reopened).await;
        assert_eq!(
            queue(&db, QueueFamily::Vector)
                .await
                .map_or(0, |queue| queue.operations().len()),
            sizes.len(),
            "publication stays paused"
        );
        for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
            assert_exact_vectors(&db, &state, consistency).await;
            let label = format!("{consistency:?} search within {budget}");
            let moved = vector_search(&db, [-50.0, -50.0], 1, Some(far), consistency).await;
            assert_eq!(
                moved
                    .iter()
                    .map(|(id, bits)| (*id, f64::from_bits(*bits)))
                    .collect::<Vec<_>>(),
                [(ids[100], 0.0)],
                "{label} finds the move"
            );
            assert!(
                !vector_search(&db, [50.0, 50.0], 8, Some("a"), consistency)
                    .await
                    .iter()
                    .any(|(id, _)| *id == ids[100]),
                "{label} shows the moved document at its first update"
            );
        }
        db.close().await.unwrap();
    }
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
            | PublicationOutcome::Blocked
            | PublicationOutcome::Stalled) => {
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
    assert_eq!(
        stats.queue_reads, 2,
        "the discard read and the read that finds the queue empty are counted"
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_queue_retained_before_its_index_drops_is_discarded_and_never_published() {
    let db = open("drop-retained", Arc::new(InMemory::new()), config()).await;
    let operation = create(&db, vector_spec()).await;
    assert_eq!(wait_terminal(&db, &operation).await, "succeeded");
    let target = target(&db, QueueFamily::Vector).await;
    for index in 0..5_u8 {
        add(&db, [f32::from(index), 1.0], "doc", Some("a")).await;
    }
    // One operation per batch, so the first commit retains the other four.
    let operations = queue(&db, QueueFamily::Vector)
        .await
        .unwrap()
        .into_operations();
    let narrow = publisher_with_limits(
        &db,
        batch_limits(operations[0].retained_bytes(), 32_768),
        DbConfig::new()
            .search_index_backfill()
            .active_text_mutation(),
    );
    assert_eq!(
        narrow.publish_once(target).await.unwrap(),
        PublicationOutcome::Published {
            operations: 1,
            entities: 1
        }
    );
    assert!(db.index_queue_store().retained().retained_bytes() > 0);

    // The retained remainder belongs to a generation that retires before the
    // next attempt: that attempt drops it and discards from storage.
    let drop_operation = drop_index(&db, vector_spec()).await.unwrap();
    assert_eq!(wait_terminal(&db, &drop_operation).await, "succeeded");
    assert_eq!(
        narrow.publish_once(target).await.unwrap(),
        PublicationOutcome::Discarded { operations: 4 }
    );
    assert_eq!(db.index_queue_store().retained().retained_bytes(), 0);
    assert_eq!(
        narrow.publish_once(target).await.unwrap(),
        PublicationOutcome::Empty
    );
    assert_eq!(
        narrow
            .metrics()
            .published_operations
            .load(Ordering::Relaxed),
        1
    );
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
        .reserve(
            &[OperationCharge {
                target,
                entity: IndexEntity {
                    kind: IndexElementKind::Node,
                    id: IndexEntityId::new(ids[2]),
                },
                id: QueuedOperationId::generate(),
                bytes: 64,
            }],
            &[],
        )
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
        .reserve(
            &[OperationCharge {
                target,
                entity: IndexEntity {
                    kind: IndexElementKind::Node,
                    id: IndexEntityId::new(ids[2]),
                },
                id: QueuedOperationId::generate(),
                bytes: 64,
            }],
            &[],
        )
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
            | PublicationOutcome::Blocked
            | PublicationOutcome::Stalled) => {
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
/// it blocks on an oversized source row until that row is repaired. Nothing
/// publishes the blocked build's queued work, so once writes fill its member
/// cap, the repair is still admitted and an unrelated insert is refused
/// without retryable backpressure.
#[tokio::test]
async fn text_build_blocks_on_a_document_publication_could_not_replace() {
    let db = open(
        "build-text-oversized",
        Arc::new(InMemory::new()),
        tight_publication_config().with_index_operation_queue_tuning(
            IndexOperationQueueTuning::default()
                .with_max_members(NonZeroU64::new(2).unwrap())
                .with_publication_paused_for_tests(),
        ),
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
    let mut fillers = Vec::new();
    for body in ["alpha filler", "beta filler"] {
        fillers.push(add(&db, [0.0, 0.0], body, None).await);
    }
    assert_eq!(db.index_operation_queue_stats().pending_members, 2);
    // The oversized row is only the previous document of the repair.
    db.query(QueryRequest::write(
        batch::write_batch().var_as(
            "repaired",
            traversal::g()
                .n(NodeRef::from(wide))
                .set_property("body", "narrow repaired".to_string()),
        ),
    ))
    .await
    .expect("the oversized document's repair is admitted beyond the member cap");
    assert_build_blocked(
        db.query(QueryRequest::write(batch::write_batch().var_as(
            "created",
            traversal::g().add_n(
                "Doc",
                vec![("body", PropertyInput::from("gamma".to_string()))],
            ),
        )))
        .await,
        &operation,
        crate::error::IndexBackpressureResource::PendingMembers,
        "an unrelated insert",
    );
    retry(&db, &operation).await;
    assert_eq!(wait_terminal(&db, &operation).await, "succeeded");
    let target = target(&db, QueueFamily::Text).await;
    drain(&db, target).await;
    for (query, expected) in [
        ("repaired", vec![wide]),
        ("resident", vec![resident]),
        ("filler", fillers),
    ] {
        let mut found =
            text_ids(text_search(&db, query, 10, None, SearchConsistency::Eventual).await);
        found.sort_unstable();
        assert_eq!(found, expected, "{query}");
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

/// Creates one `Doc` holding `properties`, which may be invalid for an index.
async fn add_raw(db: &HelixDB, properties: Vec<(&'static str, PropertyInput)>) -> u64 {
    let created = write(db, || {
        QueryRequest::write(
            batch::write_batch()
                .var_as("created", traversal::g().add_n("Doc", properties.clone()))
                .returning(["created"]),
        )
    })
    .await;
    created["created"][0]["$id"].as_u64().unwrap()
}

async fn set_property(db: &HelixDB, id: u64, property: &str, value: PropertyInput) {
    write(db, || {
        QueryRequest::write(
            batch::write_batch().var_as(
                "repaired",
                traversal::g()
                    .n(NodeRef::from(id))
                    .set_property(property, value.clone()),
            ),
        )
    })
    .await;
}

/// [`config`] with every entity's own vector output capped at `limit` bytes.
fn single_vector_output_config(limit: u64) -> DbConfig {
    let defaults = SearchIndexBackfillLimits::default();
    let batch = defaults.batch();
    let limits = SearchIndexBackfillLimits::try_new(
        SearchIndexBatchLimits::try_new(
            NonZeroUsize::new(4).unwrap(),
            batch.max_input_bytes(),
            batch.max_output_operations(),
            batch.max_output_bytes(),
            NonZeroU64::new(limit).unwrap(),
        )
        .unwrap(),
        defaults.edge_property_read_batch(),
        defaults.text_artifacts(),
        defaults.text_compaction(),
    )
    .unwrap();
    config().with_search_index_backfill_limits(limits)
}

/// Whether a vector build over `embeddings` (tenant "a", in ID order)
/// succeeds when no entity's own output may exceed `limit` bytes; a build
/// that does not fit blocks on an oversized entity.
async fn vector_build_fits(embeddings: &[[f32; 2]], limit: u64) -> bool {
    let db = open(
        &format!("build-vector-single-output-{}-{limit}", embeddings.len()),
        Arc::new(InMemory::new()),
        single_vector_output_config(limit),
    )
    .await;
    for embedding in embeddings {
        add(&db, *embedding, "doc", Some("a")).await;
    }
    let operation = create(&db, vector_spec()).await;
    let fits = match wait_terminal(&db, &operation).await.as_str() {
        "succeeded" => true,
        "blocked" => {
            assert_eq!(
                status(&db, &operation).await["blocker_code"],
                "oversized_entity"
            );
            false
        }
        other => panic!("probe build ended {other}"),
    };
    db.close().await.unwrap();
    fits
}

/// A vector `Scan` step that admits valid rows and then reaches a row it
/// cannot index commits the valid rows and leaves the blocker to the next
/// step. Repairing that row and retrying must build every row: no staged
/// work for the earlier rows may make the retried scan fail.
#[tokio::test]
async fn blocked_vector_build_repairs_invalid_row_after_valid_rows_in_batch() {
    // An invalid (three-dimensional) embedding after two valid rows of one
    // four-entity batch.
    let db = open(
        "build-vector-invalid-mid-batch",
        Arc::new(InMemory::new()),
        config(),
    )
    .await;
    let mut state = VectorState::new();
    for embedding in [[1.0, 1.0], [2.0, 2.0]] {
        let id = add(&db, embedding, "doc", Some("a")).await;
        state.insert(id, ("a", embedding));
    }
    let invalid = add_raw(
        &db,
        vec![
            ("embedding", PropertyInput::from(vec![1.0_f32, 2.0, 3.0])),
            ("tenant", PropertyInput::from("a".to_string())),
        ],
    )
    .await;
    assert!(state.keys().all(|valid| *valid < invalid), "scan order");
    let operation = create(&db, vector_spec()).await;
    assert_eq!(wait_terminal(&db, &operation).await, "blocked");
    let blocked = status(&db, &operation).await;
    assert_eq!(blocked["blocker_code"], "invalid_source_data");
    assert_eq!(
        scanned(&blocked),
        2,
        "the valid rows ahead of the blocker commit with their cursor: {blocked}"
    );
    set_property(
        &db,
        invalid,
        "embedding",
        PropertyInput::from(vec![0.5_f32, 0.5]),
    )
    .await;
    state.insert(invalid, ("a", [0.5, 0.5]));
    retry(&db, &operation).await;
    let retried = wait_terminal(&db, &operation).await;
    assert_eq!(
        retried,
        "succeeded",
        "retry after repair: {}",
        status(&db, &operation).await
    );
    drain(&db, target(&db, QueueFamily::Vector).await).await;
    assert_exact_vectors(&db, &state, SearchConsistency::Strong).await;
    assert_eq!(
        vector_search(&db, [0.5, 0.5], 10, Some("a"), SearchConsistency::Strong)
            .await
            .len(),
        3
    );
    db.close().await.unwrap();
}

/// The oversized-entity blocker inside `plan_and_apply`: an entity whose own
/// output exceeds the single-vector limit after earlier entities of its
/// batch were admitted. Reopening with the default limit and retrying must
/// build every row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocked_vector_build_retries_oversized_entity_after_valid_rows_in_batch() {
    let embeddings = [[1.0_f32, 1.0], [2.0, 2.0], [3.0, 3.0]];
    // The smallest single-vector limit the whole build fits in is the
    // largest entity's own output. Probes are independent databases, so each
    // round runs concurrently and narrows the range ninefold.
    let probe = |limits: Vec<u64>| async move {
        let fits = futures::future::join_all(
            limits
                .iter()
                .map(|limit| vector_build_fits(&embeddings, *limit)),
        )
        .await;
        limits.into_iter().zip(fits).collect::<Vec<_>>()
    };
    let doubling = probe((10..=22).map(|shift| 1_u64 << shift).collect()).await;
    let mut fits = doubling
        .iter()
        .find(|(_, fit)| *fit)
        .map(|(limit, _)| *limit)
        .expect("the probe build fits under 4 MiB");
    let mut blocks = doubling
        .iter()
        .filter(|(limit, fit)| !fit && *limit < fits)
        .map(|(limit, _)| *limit)
        .max()
        .unwrap_or(0);
    while fits - blocks > 1 {
        let step = ((fits - blocks) / 9).max(1);
        let round = probe(
            (1..=8)
                .map(|ordinal| blocks + ordinal * step)
                .filter(|limit| *limit < fits)
                .collect(),
        )
        .await;
        if let Some((limit, _)) = round.iter().find(|(_, fit)| *fit) {
            fits = *limit;
        }
        blocks = round
            .iter()
            .filter(|(limit, fit)| !fit && *limit < fits)
            .map(|(limit, _)| *limit)
            .fold(blocks, u64::max);
    }
    // One byte less blocks the build, but not on its first entity: a later
    // entity of the same batch is the one that exceeds the limit.
    let limit = fits - 1;
    assert!(
        vector_build_fits(&embeddings[..1], limit).await,
        "the first entity's output is the largest; no mid-batch case at {limit} bytes"
    );

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let name = "build-vector-oversized-mid-batch";
    let db = open(name, Arc::clone(&store), single_vector_output_config(limit)).await;
    let mut state = VectorState::new();
    for embedding in embeddings {
        let id = add(&db, embedding, "doc", Some("a")).await;
        state.insert(id, ("a", embedding));
    }
    let operation = create(&db, vector_spec()).await;
    assert_eq!(wait_terminal(&db, &operation).await, "blocked");
    let blocked = status(&db, &operation).await;
    assert_eq!(blocked["blocker_code"], "oversized_entity");
    assert!(
        (1..embeddings.len() as u64).contains(&scanned(&blocked)),
        "the entities ahead of the oversized one commit with their cursor: {blocked}"
    );
    db.close().await.unwrap();

    let db = open(name, Arc::clone(&store), config()).await;
    retry(&db, &operation).await;
    let retried = wait_terminal(&db, &operation).await;
    assert_eq!(
        retried,
        "succeeded",
        "retry under the default limit: {}",
        status(&db, &operation).await
    );
    drain(&db, target(&db, QueueFamily::Vector).await).await;
    assert_exact_vectors(&db, &state, SearchConsistency::Strong).await;
    db.close().await.unwrap();
}

/// Text control: an invalid body after two valid documents of one batch
/// blocks, and the repaired row builds.
#[tokio::test]
async fn blocked_text_build_repairs_invalid_row_after_valid_rows_in_batch() {
    let db = open(
        "build-text-invalid-mid-batch",
        Arc::new(InMemory::new()),
        config(),
    )
    .await;
    let mut residents = Vec::new();
    for body in ["alpha resident", "beta resident"] {
        residents.push(add(&db, [0.0, 0.0], body, None).await);
    }
    let invalid = add_raw(&db, vec![("body", PropertyInput::from(7_i64))]).await;
    assert!(residents.iter().all(|valid| *valid < invalid), "scan order");
    let operation = create(&db, text_spec()).await;
    assert_eq!(wait_terminal(&db, &operation).await, "blocked");
    let blocked = status(&db, &operation).await;
    assert_eq!(blocked["blocker_code"], "invalid_source_data");
    assert_eq!(
        scanned(&blocked),
        0,
        "blocked in the first scan step: {blocked}"
    );
    set_property(
        &db,
        invalid,
        "body",
        PropertyInput::from("repaired words".to_string()),
    )
    .await;
    retry(&db, &operation).await;
    assert_eq!(
        wait_terminal(&db, &operation).await,
        "succeeded",
        "retry after repair: {}",
        status(&db, &operation).await
    );
    drain(&db, target(&db, QueueFamily::Text).await).await;
    for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
        let mut found = text_ids(text_search(&db, "resident", 10, None, consistency).await);
        found.sort_unstable();
        assert_eq!(found, residents, "{consistency:?}");
        assert_eq!(
            text_ids(text_search(&db, "repaired", 10, None, consistency).await),
            vec![invalid],
            "{consistency:?}"
        );
    }
    db.close().await.unwrap();
}

/// A build blocked on an invalid row still routes writes to its hidden
/// generation, whose queue cannot publish until the build activates. Once
/// ordinary writes fill the member cap, the repair the blocker asks for must
/// still be admitted, and an unrelated insert must not fail with a
/// retryable backpressure error that can never clear.
#[tokio::test]
async fn blocked_build_at_member_cap_still_admits_its_repair() {
    let mut refused = Vec::new();
    for family in [QueueFamily::Vector, QueueFamily::Text] {
        let config = config().with_index_operation_queue_tuning(
            IndexOperationQueueTuning::default()
                .with_max_members(NonZeroU64::new(2).unwrap())
                .with_publication_paused_for_tests(),
        );
        let db = open(
            &format!("blocked-build-member-cap-{family:?}"),
            Arc::new(InMemory::new()),
            config,
        )
        .await;
        let (spec, property, invalid_value, repaired_value) = match family {
            QueueFamily::Vector => (
                vector_spec(),
                "embedding",
                PropertyInput::from(vec![1.0_f32, 2.0, 3.0]),
                PropertyInput::from(vec![0.5_f32, 0.5]),
            ),
            QueueFamily::Text => (
                text_spec(),
                "body",
                PropertyInput::from(7_i64),
                PropertyInput::from("repaired words".to_string()),
            ),
        };
        let invalid = add_raw(
            &db,
            vec![
                (property, invalid_value),
                ("tenant", PropertyInput::from("a".to_string())),
            ],
        )
        .await;
        let operation = create(&db, spec).await;
        assert_eq!(
            wait_terminal(&db, &operation).await,
            "blocked",
            "{family:?}"
        );
        assert_eq!(
            status(&db, &operation).await["blocker_code"],
            "invalid_source_data",
            "{family:?}"
        );
        let mut state = VectorState::from([(invalid, ("a", [0.5, 0.5]))]);
        let mut bodies = BTreeMap::from([(invalid, "repaired words".to_string())]);
        for (embedding, body) in [([1.0, 1.0], "alpha"), ([2.0, 2.0], "beta")] {
            let id = add(&db, embedding, body, Some("a")).await;
            state.insert(id, ("a", embedding));
            bodies.insert(id, body.to_string());
        }
        assert_eq!(
            db.index_operation_queue_stats().pending_members,
            2,
            "{family:?}: ordinary writes filled the member cap"
        );

        let repair = db
            .query(QueryRequest::write(
                batch::write_batch().var_as(
                    "repaired",
                    traversal::g()
                        .n(NodeRef::from(invalid))
                        .set_property(property, repaired_value),
                ),
            ))
            .await;
        let insert = db
            .query(QueryRequest::write(
                batch::write_batch()
                    .var_as(
                        "created",
                        traversal::g().add_n(
                            "Doc",
                            vec![
                                ("embedding", PropertyInput::from(vec![3.0_f32, 3.0])),
                                ("body", PropertyInput::from("gamma".to_string())),
                                ("tenant", PropertyInput::from("a".to_string())),
                            ],
                        ),
                    )
                    .returning(["created"]),
            ))
            .await;
        let insert_settles = match &insert {
            Ok(_) => true,
            Err(error) => !error.is_index_backpressure() && error.to_string().contains(&operation),
        };
        if !(repair.is_ok() && insert_settles) {
            // Both families are reported before the test fails.
            refused.push(format!(
                "{family:?}: the repair must be admitted ({repair:?}) and an unrelated \
                 insert must not be refused with backpressure that never clears ({insert:?})"
            ));
            db.close().await.unwrap();
            continue;
        }
        if let Ok(created) = &insert {
            let id = created["created"][0]["$id"].as_u64().unwrap();
            state.insert(id, ("a", [3.0, 3.0]));
            bodies.insert(id, "gamma".to_string());
        }

        retry(&db, &operation).await;
        assert_eq!(
            wait_terminal(&db, &operation).await,
            "succeeded",
            "{family:?}: {}",
            status(&db, &operation).await
        );
        drain(&db, target(&db, family).await).await;
        match family {
            QueueFamily::Vector => {
                for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
                    assert_exact_vectors(&db, &state, consistency).await;
                }
            }
            QueueFamily::Text => {
                for (id, body) in &bodies {
                    let term = body.split(' ').next().unwrap();
                    assert_eq!(
                        text_ids(
                            text_search(&db, term, 10, None, SearchConsistency::Eventual).await
                        ),
                        vec![*id],
                        "{term}"
                    );
                }
            }
        }
        db.close().await.unwrap();
    }
    assert!(refused.is_empty(), "{refused:#?}");
}

/// Asserts that `result` is the non-retryable refusal of a write that would
/// saturate `expected` of the queued work of the blocked build `operation`.
fn assert_build_blocked(
    result: crate::Result<serde_json::Value>,
    operation: &str,
    expected: crate::error::IndexBackpressureResource,
    write: &str,
) {
    let error = result.expect_err(write);
    assert!(
        matches!(
            error,
            crate::error::HelixDbError::IndexBuildBlocked { resource, .. } if resource == expected
        ),
        "{write}: {error:?}"
    );
    assert_eq!(
        error.error_code(),
        helix_ast::error_code::QueryErrorCode::IndexBuildBlocked,
        "{write}"
    );
    assert!(!error.is_index_backpressure(), "{write}: {error}");
    assert!(error.to_string().contains(operation), "{write}: {error}");
}

/// Beyond the member cap a blocked build admits no new member but its
/// blocker's first repair: an unrelated insert and the repair bundled with
/// another entity are refused. Writes to pending entities add no member, so
/// a resident's update and a second write of the repaired entity stay
/// admitted, before and after the retry. Once the retried build runs, a new
/// member is ordinary retryable backpressure again and clears as publication
/// drains.
#[tokio::test]
async fn blocked_build_beyond_the_member_cap_refuses_only_new_members() {
    let config = config().with_index_operation_queue_tuning(
        IndexOperationQueueTuning::default()
            .with_max_members(NonZeroU64::new(2).unwrap())
            .with_publication_paused_for_tests(),
    );
    let db = open("blocked-build-bounded", Arc::new(InMemory::new()), config).await;
    let invalid = add_raw(
        &db,
        vec![
            ("embedding", PropertyInput::from(vec![1.0_f32, 2.0, 3.0])),
            ("tenant", PropertyInput::from("a".to_string())),
        ],
    )
    .await;
    let operation = create(&db, vector_spec()).await;
    assert_eq!(wait_terminal(&db, &operation).await, "blocked");
    let mut state = VectorState::from([(invalid, ("a", [0.5, 0.5]))]);
    let mut residents = Vec::new();
    for embedding in [[1.0, 1.0], [2.0, 2.0]] {
        let id = add(&db, embedding, "doc", Some("a")).await;
        state.insert(id, ("a", embedding));
        residents.push(id);
    }
    let insert = || {
        db.query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "created",
                    traversal::g().add_n(
                        "Doc",
                        vec![
                            ("embedding", PropertyInput::from(vec![3.0_f32, 3.0])),
                            ("tenant", PropertyInput::from("a".to_string())),
                        ],
                    ),
                )
                .returning(["created"]),
        ))
    };
    let repair = |embedding: [f32; 2]| {
        traversal::g()
            .n(NodeRef::from(invalid))
            .set_property("embedding", PropertyInput::from(embedding.to_vec()))
    };
    let pending = || {
        let stats = db.index_operation_queue_stats();
        (stats.pending_members, stats.pending_operations)
    };
    assert_eq!(pending(), (2, 2));

    let members = crate::error::IndexBackpressureResource::PendingMembers;
    assert_build_blocked(insert().await, &operation, members, "an unrelated insert");
    assert_build_blocked(
        db.query(QueryRequest::write(
            batch::write_batch()
                .var_as("repaired", repair([0.5, 0.5]))
                .var_as(
                    "moved",
                    traversal::g()
                        .n(NodeRef::from(residents[0]))
                        .set_property("embedding", PropertyInput::from(vec![1.5_f32, 1.5])),
                ),
        ))
        .await,
        &operation,
        members,
        "the repair bundled with another entity",
    );
    assert_eq!(pending(), (2, 2), "refused writes queue nothing");
    db.query(QueryRequest::write(
        batch::write_batch().var_as("repaired", repair([0.5, 0.5])),
    ))
    .await
    .expect("the blocker's first repair is admitted beyond the cap");
    assert_eq!(pending(), (3, 3));
    db.query(QueryRequest::write(
        batch::write_batch().var_as("repaired", repair([0.25, 0.25])),
    ))
    .await
    .expect("a second write of the repaired entity adds no member");
    state.insert(invalid, ("a", [0.25, 0.25]));
    update(&db, residents[0], [1.5, 1.5], "doc").await;
    state.insert(residents[0], ("a", [1.5, 1.5]));
    assert_build_blocked(
        insert().await,
        &operation,
        members,
        "an insert after the repair",
    );
    assert_eq!(pending(), (3, 5));

    retry(&db, &operation).await;
    assert_eq!(wait_terminal(&db, &operation).await, "succeeded");
    assert!(
        insert()
            .await
            .expect_err("still saturated")
            .is_index_backpressure(),
        "an Active generation's saturation is retryable"
    );
    update(&db, residents[1], [2.5, 2.5], "doc").await;
    state.insert(residents[1], ("a", [2.5, 2.5]));
    assert_eq!(pending(), (3, 6), "a resident's update adds no member");
    let target = target(&db, QueueFamily::Vector).await;
    drain(&db, target).await;
    let created = insert().await.expect("publication cleared the backlog");
    state.insert(
        created["created"][0]["$id"].as_u64().unwrap(),
        ("a", [3.0, 3.0]),
    );
    drain(&db, target).await;
    for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
        assert_exact_vectors(&db, &state, consistency).await;
    }
    db.close().await.unwrap();
}

/// Only the build measures an entity's vector output, so the first queued
/// write of an oversized entity may leave it oversized. Once that write has
/// taken the queue past its byte limit, every further write is refused,
/// including the entity's own and a resident's update, except removing the
/// entity from the index, which clears the blocker.
#[tokio::test]
async fn blocked_build_admits_removing_an_entity_its_first_write_left_oversized() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let name = "blocked-build-oversized-removal";
    // Every entity's own output exceeds one byte.
    let tight = |tuning: IndexOperationQueueTuning| {
        single_vector_output_config(1)
            .with_index_operation_queue_tuning(tuning.with_publication_paused_for_tests())
    };
    let db = open(
        name,
        Arc::clone(&store),
        tight(IndexOperationQueueTuning::default()),
    )
    .await;
    let oversized = add(&db, [1.0, 1.0], "doc", Some("a")).await;
    let operation = create(&db, vector_spec()).await;
    assert_eq!(wait_terminal(&db, &operation).await, "blocked");
    assert_eq!(
        status(&db, &operation).await["blocker_code"],
        "oversized_entity"
    );
    let resident = add(&db, [2.0, 2.0], "doc", Some("a")).await;
    assert!(oversized < resident, "scan order");
    // Room for the resident's operation and half of another, so any further
    // write is above the byte limit while no single one is too large.
    let resident_bytes = db.index_operation_queue_stats().retained_bytes;
    db.close().await.unwrap();
    let db = open(
        name,
        Arc::clone(&store),
        tight(
            IndexOperationQueueTuning::default()
                .with_max_retained_bytes(NonZeroU64::new(resident_bytes * 3 / 2).unwrap())
                .unwrap(),
        ),
    )
    .await;
    let embed = |id: u64, embedding: [f32; 2]| {
        db.query(QueryRequest::write(
            batch::write_batch().var_as(
                "moved",
                traversal::g()
                    .n(NodeRef::from(id))
                    .set_property("embedding", PropertyInput::from(embedding.to_vec())),
            ),
        ))
    };
    embed(oversized, [1.5, 1.5])
        .await
        .expect("the blocker's first write is admitted beyond the byte limit");
    retry(&db, &operation).await;
    assert_eq!(wait_terminal(&db, &operation).await, "blocked");
    assert_eq!(
        status(&db, &operation).await["blocker_code"],
        "oversized_entity",
        "the first write left the first entity oversized"
    );
    let bytes = crate::error::IndexBackpressureResource::RetainedBytes;
    assert_build_blocked(
        embed(oversized, [1.25, 1.25]).await,
        &operation,
        bytes,
        "a second write of the oversized entity",
    );
    assert_build_blocked(
        embed(resident, [2.5, 2.5]).await,
        &operation,
        bytes,
        "a resident's update",
    );
    assert_build_blocked(
        db.query(QueryRequest::write(batch::write_batch().var_as(
            "created",
            traversal::g().add_n(
                "Doc",
                vec![
                    ("embedding", PropertyInput::from(vec![3.0_f32, 3.0])),
                    ("tenant", PropertyInput::from("a".to_string())),
                ],
            ),
        )))
        .await,
        &operation,
        bytes,
        "an unrelated insert",
    );
    db.query(QueryRequest::write(batch::write_batch().var_as(
        "dropped",
        traversal::g().n(NodeRef::from(oversized)).drop(),
    )))
    .await
    .expect("removing the oversized entity is admitted beyond the byte limit");
    let stats = db.index_operation_queue_stats();
    assert_eq!((stats.pending_members, stats.pending_operations), (2, 3));
    db.close().await.unwrap();

    // Under the default limits the retried build and the drained queue index
    // the resident alone.
    let db = open(name, Arc::clone(&store), config()).await;
    retry(&db, &operation).await;
    assert_eq!(
        wait_terminal(&db, &operation).await,
        "succeeded",
        "{}",
        status(&db, &operation).await
    );
    drain(&db, target(&db, QueueFamily::Vector).await).await;
    let state = VectorState::from([(resident, ("a", [2.0, 2.0]))]);
    for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
        assert_exact_vectors(&db, &state, consistency).await;
    }
    db.close().await.unwrap();
}

/// A row invalid for both a vector and a text index blocks both builds. With
/// both saturated, one write repairing both properties charges two blocked
/// generations, and each must admit it as its blocker's repair, while an
/// unrelated insert is refused by a blocked build.
#[tokio::test]
async fn one_write_repairs_a_row_two_saturated_blocked_builds_block_on() {
    let config = config().with_index_operation_queue_tuning(
        IndexOperationQueueTuning::default()
            .with_max_members(NonZeroU64::new(2).unwrap())
            .with_publication_paused_for_tests(),
    );
    let db = open(
        "blocked-builds-shared-repair",
        Arc::new(InMemory::new()),
        config,
    )
    .await;
    let invalid = add_raw(
        &db,
        vec![
            ("embedding", PropertyInput::from(vec![1.0_f32, 2.0, 3.0])),
            ("body", PropertyInput::from(7_i64)),
            ("tenant", PropertyInput::from("a".to_string())),
        ],
    )
    .await;
    let operations = [
        create(&db, vector_spec()).await,
        create(&db, text_spec()).await,
    ];
    for operation in &operations {
        assert_eq!(wait_terminal(&db, operation).await, "blocked");
        assert_eq!(
            status(&db, operation).await["blocker_code"],
            "invalid_source_data"
        );
    }
    let mut state = VectorState::from([(invalid, ("a", [0.5, 0.5]))]);
    let mut bodies = BTreeMap::from([(invalid, "repaired words".to_string())]);
    for (embedding, body) in [([1.0, 1.0], "alpha"), ([2.0, 2.0], "beta")] {
        let id = add(&db, embedding, body, Some("a")).await;
        state.insert(id, ("a", embedding));
        bodies.insert(id, body.to_string());
    }
    let pending = || {
        let stats = db.index_operation_queue_stats();
        (stats.pending_members, stats.pending_operations)
    };
    assert_eq!(pending(), (4, 4), "both indexes are at their member cap");

    db.query(QueryRequest::write(
        batch::write_batch().var_as(
            "repaired",
            traversal::g()
                .n(NodeRef::from(invalid))
                .set_property("embedding", vec![0.5_f32, 0.5])
                .set_property("body", "repaired words".to_string()),
        ),
    ))
    .await
    .expect("both blocked builds admit the shared repair");
    assert_eq!(pending(), (6, 6));
    let error = db
        .query(QueryRequest::write(batch::write_batch().var_as(
            "created",
            traversal::g().add_n(
                "Doc",
                vec![
                    ("embedding", PropertyInput::from(vec![3.0_f32, 3.0])),
                    ("body", PropertyInput::from("gamma".to_string())),
                    ("tenant", PropertyInput::from("a".to_string())),
                ],
            ),
        )))
        .await
        .expect_err("an unrelated insert is refused");
    assert!(
        matches!(error, crate::error::HelixDbError::IndexBuildBlocked { .. })
            && operations
                .iter()
                .any(|operation| error.to_string().contains(operation)),
        "{error:?}"
    );
    assert_eq!(pending(), (6, 6));

    for operation in &operations {
        retry(&db, operation).await;
        assert_eq!(
            wait_terminal(&db, operation).await,
            "succeeded",
            "{}",
            status(&db, operation).await
        );
    }
    for family in [QueueFamily::Vector, QueueFamily::Text] {
        drain(&db, target(&db, family).await).await;
    }
    for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
        assert_exact_vectors(&db, &state, consistency).await;
    }
    for (id, body) in &bodies {
        let term = body.split(' ').next().unwrap();
        assert_eq!(
            text_ids(text_search(&db, term, 10, None, SearchConsistency::Eventual).await),
            vec![*id],
            "{term}"
        );
    }
    db.close().await.unwrap();
}

/// A running build's saturated generation clears once the build activates
/// and publication drains it, so its refusal stays retryable backpressure.
#[tokio::test]
async fn running_build_beyond_the_member_cap_is_retryable_backpressure() {
    let config = config().with_index_operation_queue_tuning(
        IndexOperationQueueTuning::default()
            .with_max_members(NonZeroU64::new(2).unwrap())
            .with_publication_paused_for_tests(),
    );
    let db = open(
        "running-build-backpressure",
        Arc::new(InMemory::new()),
        config,
    )
    .await;
    let (ids, mut state) = seed_vectors(&db, 12).await;
    let operation = create(&db, vector_spec()).await;
    let target = target(&db, QueueFamily::Vector).await;
    let paused = pause_when(&db, &operation, target, |status| scanned(status) >= 4).await;
    for position in [1_usize, 2] {
        update(&db, ids[position], [7.0, 7.0], "doc").await;
        state.get_mut(&ids[position]).unwrap().1 = [7.0, 7.0];
    }
    let error = db
        .query(QueryRequest::write(
            batch::write_batch().var_as(
                "moved",
                traversal::g()
                    .n(NodeRef::from(ids[3]))
                    .set_property("embedding", PropertyInput::from(vec![8.0_f32, 8.0])),
            ),
        ))
        .await
        .expect_err("a third member is above the cap");
    assert!(
        matches!(
            error,
            crate::error::HelixDbError::IndexBackpressure {
                resource: crate::error::IndexBackpressureResource::PendingMembers,
                ..
            }
        ),
        "{error:?}"
    );
    drop(paused);
    assert_eq!(wait_terminal(&db, &operation).await, "succeeded");
    drain(&db, target).await;
    update(&db, ids[3], [8.0, 8.0], "doc").await;
    state.get_mut(&ids[3]).unwrap().1 = [8.0, 8.0];
    drain(&db, target).await;
    assert_exact_vectors(&db, &state, SearchConsistency::Strong).await;
    db.close().await.unwrap();
}

/// How an operator resolves a blocked build while a write races it.
#[derive(Debug, Clone, Copy)]
enum BlockedBuildResolution {
    Retry,
    Abort,
}

/// A saturated blocked build admits its blocker's repair from the writing
/// transaction's own read of the build's operation. A retry or abort of the
/// build that commits between that read and the write's commit must fail the
/// write with a transaction conflict, so the repair never lands above the
/// limits in a running or retired generation, and releasing the aborted
/// reservation leaves the ledger as it was.
///
/// The retry rewrites only the operation row and its pointer, so it conflicts
/// solely through the admission read; the abort also rewrites the canonical
/// index row the transaction's mutation catalog read.
#[tokio::test]
async fn a_retry_or_abort_committed_after_a_blocker_repair_reserves_fails_the_repair() {
    for resolution in [BlockedBuildResolution::Retry, BlockedBuildResolution::Abort] {
        let config = config().with_index_operation_queue_tuning(
            IndexOperationQueueTuning::default()
                .with_max_members(NonZeroU64::new(2).unwrap())
                .with_publication_paused_for_tests(),
        );
        let db = open(
            &format!("blocked-build-stale-blocker-{resolution:?}"),
            Arc::new(InMemory::new()),
            config,
        )
        .await;
        let scope = DataScope::LegacyUnscoped;
        let invalid = add_raw(
            &db,
            vec![
                ("embedding", PropertyInput::from(vec![1.0_f32, 2.0, 3.0])),
                ("tenant", PropertyInput::from("a".to_string())),
            ],
        )
        .await;
        let operation = create(&db, vector_spec()).await;
        assert_eq!(wait_terminal(&db, &operation).await, "blocked");
        let mut state = VectorState::from([(invalid, ("a", [0.5, 0.5]))]);
        for embedding in [[1.0, 1.0], [2.0, 2.0]] {
            let id = add(&db, embedding, "doc", Some("a")).await;
            state.insert(id, ("a", embedding));
        }
        let target = target(&db, QueueFamily::Vector).await;
        let ledger = || {
            db.index_operation_backlog()
                .usage(target.scope, target.index_id)
        };
        let saturated = ledger();
        assert_eq!((saturated.members, saturated.operations), (2, 2));
        // No build step runs while its generation's ownership is held, so
        // only the resolution's own commit races the repair.
        let held = db.inner.index_scope_gates.publication_permit(target).await;

        // The repair, staged exactly as a graph write stages it.
        let scope_permit = db.index_mutation_scope_permit(scope).await;
        let transaction = db
            .inner_db()
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let (_, _, vector, text, routes) =
            crate::index_lifecycle::mutation_catalog::MutationIndexCatalog::load(
                &transaction,
                scope,
            )
            .await
            .unwrap()
            .into_components();
        let entity = crate::index_lifecycle::graph_mutation::GraphEntity::node(invalid);
        let before = crate::index_lifecycle::graph_mutation::CanonicalPropertyRow::decode(
            transaction
                .get(entity.property_key(scope))
                .await
                .unwrap()
                .expect("the blocker's row exists"),
        )
        .unwrap();
        let crate::index_lifecycle::graph_mutation::PropertyEditOutcome::Changed(transition) =
            crate::index_lifecycle::graph_mutation::GraphMutationTransition::edit(
                scope,
                entity,
                before,
                crate::index_lifecycle::graph_mutation::PropertyEdit::set(
                    crate::encoding::v2::values::property::Property::new(
                        "embedding",
                        crate::encoding::v2::values::property::property_value::PropertyValue::F32Array(
                            vec![0.5, 0.5],
                        ),
                    ),
                ),
            )
        else {
            panic!("the repair changes the blocker's row");
        };
        let mut collector = super::producer::QueuedMutationCollector::new(scope);
        collector
            .collect(
                &vector,
                &text,
                &routes.targets_for(&transition),
                &transition,
            )
            .unwrap();
        let staged = collector
            .finalize(db.index_operand_limit(), db.active_text_mutation_limits())
            .unwrap();
        let mut reservation = staged
            .reserve(db.index_operation_backlog(), &transaction)
            .await
            .expect("the blocker's first repair is admitted beyond the member cap");
        let admitted = ledger();
        assert_eq!((admitted.members, admitted.operations), (3, 3));
        for staged in staged.operands {
            db.index_queue_store()
                .stage_enqueue(
                    &transaction,
                    staged.target,
                    staged.operand,
                    &staged.operations,
                )
                .unwrap();
        }
        transaction
            .put(
                transition.graph_key(),
                transition.after().unwrap().encoded().clone(),
            )
            .unwrap();

        let operation_id = crate::index_lifecycle::IndexOperationId::new(
            uuid::Uuid::parse_str(&operation).unwrap(),
        )
        .unwrap();
        match resolution {
            BlockedBuildResolution::Retry => {
                db.retry_index_operation(scope, operation_id).await.unwrap();
            }
            BlockedBuildResolution::Abort => {
                db.abort_index_operation(scope, operation_id).await.unwrap();
            }
        }
        reservation.begin_commit();
        let error = transaction
            .commit()
            .await
            .expect_err("the build changed after the repair read it blocked");
        assert_eq!(
            error.kind(),
            slatedb::ErrorKind::Transaction,
            "{resolution:?}: {error}"
        );
        reservation.aborted();
        drop(scope_permit);
        assert_eq!(ledger(), saturated, "{resolution:?}: nothing stays charged");

        let repair = || {
            db.query(QueryRequest::write(
                batch::write_batch().var_as(
                    "repaired",
                    traversal::g()
                        .n(NodeRef::from(invalid))
                        .set_property("embedding", PropertyInput::from(vec![0.5_f32, 0.5])),
                ),
            ))
        };
        match resolution {
            BlockedBuildResolution::Retry => {
                assert!(
                    repair()
                        .await
                        .expect_err("a runnable build admits no repair above the cap")
                        .is_index_backpressure(),
                    "a runnable build's saturation is retryable"
                );
                assert_eq!(ledger(), saturated);
                drop(held);
                assert_eq!(
                    wait_terminal(&db, &operation).await,
                    "blocked",
                    "the refused repairs left the row invalid"
                );
                repair()
                    .await
                    .expect("the blocker's first repair is admitted once it blocks again");
                retry(&db, &operation).await;
                assert_eq!(wait_terminal(&db, &operation).await, "succeeded");
                drain(&db, target).await;
                for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
                    assert_exact_vectors(&db, &state, consistency).await;
                }
            }
            BlockedBuildResolution::Abort => {
                repair()
                    .await
                    .expect("an aborting build routes no queued work");
                assert_eq!(ledger(), saturated, "the retired generation is not charged");
                drop(held);
                assert_eq!(wait_terminal(&db, &operation).await, "aborted");
            }
        }
        db.close().await.unwrap();
    }
}
