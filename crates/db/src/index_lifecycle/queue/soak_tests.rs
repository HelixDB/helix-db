//! Concurrent soak: foreground writers, readers, index builds, and the
//! background publisher while SlateDB flushes and compacts continuously and
//! the writer restarts.
//!
//! Each seed opens a database whose publication runs automatically with small
//! fixed limits, seeds documents, then runs [`CYCLES`] phases of eight writers
//! and four readers. The first phase creates tenant-partitioned vector and
//! text indexes under writes; once the family chosen for dropping has served
//! a strong read, a later phase drops it and creates it again. Every phase
//! ends by quiescing, closing, and reopening the writer.
//!
//! Readers derive their oracle from the same response as their searches: a
//! `value_map` of every `Doc` read in the searches' snapshot. Strong results
//! must equal exact search over that snapshot; eventual results must name
//! distinct documents of it. Once every queue drains, the run asserts exact
//! results, empty ledgers, physical index rows that match the live documents,
//! and that compaction rewrote queues while they held work.
//!
//! `SOAK_SEEDS` (default 1), `SOAK_SEED` (first seed), and `SOAK_PHASE_SECS`
//! (default 8) scale the run. Its coverage assertions need builds to finish
//! within the phases, which a debug build on a loaded host does not, so the
//! soak is ignored and runs in release in the nightly workflow.

use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use helix_ast::{
    batch,
    graph::NodeRef,
    index::IndexSpec,
    query::{QueryRequest, SearchConsistency},
    traversal,
    value::{PropertyInput, PropertyValue},
};
use slatedb::config::{
    CompactionWorkerOptions, CompactorOptions, Settings, SizeTieredCompactionSchedulerOptions,
};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::DbReadOps;
use tokio::sync::RwLock;
use tokio::task::JoinSet;

use super::lifecycle_tests::vector_spec;
use super::overlay_tests::{add, delete, drain, hits, update, vector_search};
use super::publication_tests::install_vector;
use super::tests::{open, queued, rows, target};
use crate::config::{
    DbConfig, IndexOperationQueueTuning, SearchIndexBackfillLimits, SearchIndexBatchLimits,
    TextAnalyzerKind, TextBackfillCompactionLimits, TextBuildArtifactLimits,
};
use crate::encoding::v2::keys::indexes::vector::{VectorKey, VectorStorageLane};
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{DataKey, DataKeyKind, ManagedIndexKey, RecordKind, ScopedKey};
use crate::encoding::v2::values::indexes::operation_queue::QueueFamily;
use crate::encoding::v2::values::indexes::vector::decode_layer0_neighbors;
use crate::encoding::v2::values::indexes::vector::neighbors::decode_upper_neighbors;
use crate::encoding::v2::values::property::encode_index_partition_value;
use crate::encoding::v2::values::property::property_value::PropertyValue as StoredValue;
use crate::encoding::v2::values::{decode_partition_mapping, decode_statistics_entity};
use crate::index_lifecycle::text::statistics::present_contribution;
use crate::index_lifecycle::work::{TextPartition, TextStatisticsContribution};
use crate::index_lifecycle::{ActiveIndexHandle, VectorPhysicalLayout};
use crate::HelixDB;

const TENANTS: [&str; 4] = ["ta", "tb", "tc", "td"];
const VOCABULARY: [&str; 8] = [
    "amber", "birch", "cedar", "delta", "ember", "fjord", "glade", "heron",
];
const WRITERS: u64 = 8;
const READERS: u64 = 4;
const SEEDED_DOCS: usize = 200;
const TARGET_DOCS: usize = 300;
const CYCLES: usize = 3;
const VECTOR_K: usize = 6;
/// Larger than every partition, so text results list every match.
const TEXT_K: usize = 512;

/// SplitMix64: a dependency-free generator, so a seed replays its schedule.
pub(super) struct Rng(pub(super) u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut mixed = self.0;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        mixed ^ (mixed >> 31)
    }

    pub(super) fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }

    fn fraction(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1_u64 << 53) as f64
    }

    fn tenant(&mut self) -> &'static str {
        TENANTS[self.below(TENANTS.len())]
    }

    /// A point on a quarter grid: squared distances to [`Rng::query`]
    /// points are exact in `f32`, so `$distance` bits are predictable.
    fn embedding(&mut self) -> [f32; 2] {
        [self.below(64) as f32 * 0.25, self.below(64) as f32 * 0.25]
    }

    /// A point on an eighth grid, off the embedding grid to thin out ties.
    fn query(&mut self) -> [f32; 2] {
        [
            self.below(128) as f32 * 0.125,
            self.below(128) as f32 * 0.125,
        ]
    }

    /// One to four vocabulary words, repeats allowed.
    fn body(&mut self) -> String {
        let words = 1 + self.below(4);
        (0..words)
            .map(|_| VOCABULARY[self.below(VOCABULARY.len())])
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// Writer operations, each counted to prove the run exercised it.
#[derive(Debug, Clone, Copy)]
enum Op {
    Insert,
    InsertOther,
    Embedding,
    Body,
    IdenticalRewrite,
    RemoveProperty,
    RestoreProperty,
    Delete,
    TenantMove,
    Relabel,
    Chain,
}

const OPS: [Op; 11] = [
    Op::Insert,
    Op::InsertOther,
    Op::Embedding,
    Op::Body,
    Op::IdenticalRewrite,
    Op::RemoveProperty,
    Op::RestoreProperty,
    Op::Delete,
    Op::TenantMove,
    Op::Relabel,
    Op::Chain,
];

/// Shared counters proving each path of the scenario ran.
#[derive(Default)]
struct Counters {
    ops: [AtomicU64; OPS.len()],
    chains: [AtomicU64; 6],
    write_conflicts: AtomicU64,
    write_backpressure: AtomicU64,
    strong_vector_checks: AtomicU64,
    strong_text_checks: AtomicU64,
    eventual_checks: AtomicU64,
    graph_checks: AtomicU64,
    read_backpressure: AtomicU64,
    l0_compactions: AtomicU64,
    l0_compactions_while_pending: AtomicU64,
    last_run_compactions: AtomicU64,
    last_run_compactions_while_pending: AtomicU64,
}

impl Counters {
    fn count(&self, op: Op) {
        self.ops[op as usize].fetch_add(1, Ordering::Relaxed);
    }

    fn get(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }
}

/// Which indexes readers may search: only fully Active ones.
#[derive(Debug, Clone, Copy, Default)]
struct Serving {
    vector: bool,
    text: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Family {
    Vector,
    Text,
}

impl Family {
    fn spec(self) -> IndexSpec {
        match self {
            Self::Vector => vector_spec(),
            Self::Text => IndexSpec::node_text("Doc", "body", Some("tenant")),
        }
    }

    fn serve(self, serving: &mut Serving, active: bool) {
        match self {
            Self::Vector => serving.vector = active,
            Self::Text => serving.text = active,
        }
    }
}

/// One index as the controller drives it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Lifecycle {
    Absent,
    Building(String),
    Active,
    Dropping(String),
}

struct Index {
    family: Family,
    state: Lifecycle,
    /// Builds that reached Active.
    builds: u32,
    /// When the current build or cleanup started.
    since: Instant,
    /// Wall time of every finished build or cleanup.
    took: Vec<(&'static str, Duration)>,
}

impl Index {
    fn new(family: Family) -> Self {
        Self {
            family,
            state: Lifecycle::Absent,
            builds: 0,
            since: Instant::now(),
            took: Vec::new(),
        }
    }

    fn start(&mut self, state: Lifecycle) {
        self.state = state;
        self.since = Instant::now();
    }
}

/// Small queued publication limits, fixed for the whole run: four entities
/// per attempt and a 1,024-operation, 128 KiB output budget.
fn config() -> DbConfig {
    let defaults = SearchIndexBackfillLimits::default();
    let compaction = defaults.text_compaction();
    let limits = SearchIndexBackfillLimits::try_new(
        SearchIndexBatchLimits::try_new(
            NonZeroUsize::new(4).unwrap(),
            defaults.batch().max_input_bytes(),
            NonZeroU64::new(1_024).unwrap(),
            NonZeroU64::new(128 * 1024).unwrap(),
            NonZeroU64::new(128 * 1024).unwrap(),
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
    // Tiny L0 files and a fast compactor that merges any two sources, so
    // queue merges resolve over partial operand sets throughout the run.
    let slate = Settings {
        flush_interval: Some(Duration::from_millis(5)),
        l0_sst_size_bytes: 16 * 1024,
        max_unflushed_bytes: 256 * 1024,
        manifest_poll_interval: Duration::from_millis(10),
        compactor_options: Some(CompactorOptions {
            poll_interval: Duration::from_millis(10),
            commit_compacted_interval: Duration::from_millis(10),
            scheduler_options: SizeTieredCompactionSchedulerOptions {
                min_compaction_sources: 2,
                ..Default::default()
            }
            .into(),
            worker: Some(CompactionWorkerOptions {
                compactions_poll_interval: Duration::from_millis(10),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    DbConfig::new()
        .with_slate_settings(slate)
        .with_search_index_backfill_limits(limits)
}

pub(super) fn env_or(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .map(|value| value.parse().unwrap_or_else(|_| panic!("{name}={value}")))
        .unwrap_or(default)
}

/// Executes `request` as a client would: conflicts and index backpressure
/// retry; anything else fails the soak.
async fn execute(
    db: &HelixDB,
    counters: &Counters,
    request: impl Fn() -> QueryRequest,
) -> serde_json::Value {
    for _ in 0..10_000 {
        match Box::pin(db.query(request())).await {
            Ok(result) => return result,
            Err(error) if error.is_transaction_conflict() => {
                counters.write_conflicts.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) if error.is_index_backpressure() => {
                counters.write_backpressure.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(error) => panic!("request failed: {error}"),
        }
    }
    panic!("request kept retrying")
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

/// Starts a build (`create`) or a drop's cleanup and returns its operation.
async fn ddl(db: &HelixDB, counters: &Counters, family: Family, create: bool) -> String {
    let receipt = execute(db, counters, || {
        let entry = if create {
            batch::write_batch().var_as(
                "ddl",
                traversal::g().create_index_if_not_exists(family.spec()),
            )
        } else {
            batch::write_batch().var_as("ddl", traversal::g().drop_index(family.spec()))
        };
        QueryRequest::write(entry.returning(["ddl"]))
    })
    .await;
    operation_id(&receipt)
        .unwrap_or_else(|| panic!("{family:?} create={create} started no operation: {receipt}"))
}

async fn operation_status(db: &HelixDB, operation: &str) -> serde_json::Value {
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

/// Moves `index` on once its build or cleanup operation finishes.
async fn advance(db: &HelixDB, counters: &Counters, index: &mut Index, serving: &RwLock<Serving>) {
    let (operation, building) = match &index.state {
        Lifecycle::Building(operation) => (operation.clone(), true),
        Lifecycle::Dropping(operation) => (operation.clone(), false),
        Lifecycle::Absent | Lifecycle::Active => return,
    };
    let status = operation_status(db, &operation).await;
    match status["status"].as_str() {
        Some("queued" | "running") => {}
        Some("succeeded") if building => {
            index.took.push(("build", index.since.elapsed()));
            index.state = Lifecycle::Active;
            index.builds += 1;
            index.family.serve(&mut *serving.write().await, true);
        }
        Some("succeeded") => {
            index.took.push(("cleanup", index.since.elapsed()));
            index.start(Lifecycle::Building(
                ddl(db, counters, index.family, true).await,
            ));
        }
        _ => panic!("{:?} operation {operation} ended: {status}", index.family),
    }
}

/// Takes `index` out of service, then drops it.
async fn start_drop(
    db: &HelixDB,
    counters: &Counters,
    index: &mut Index,
    serving: &RwLock<Serving>,
) {
    assert_eq!(index.state, Lifecycle::Active);
    // Waits out every in-flight read of the index first.
    index.family.serve(&mut *serving.write().await, false);
    index.start(Lifecycle::Dropping(
        ddl(db, counters, index.family, false).await,
    ));
}

fn node(id: u64) -> traversal::Traversal<traversal::OnNodes> {
    traversal::g().n(NodeRef::from(id))
}

fn doc_properties(
    embedding: [f32; 2],
    body: &str,
    tenant: &str,
) -> Vec<(&'static str, PropertyInput)> {
    vec![
        ("embedding", PropertyInput::from(embedding.to_vec())),
        ("body", PropertyInput::from(body.to_string())),
        ("tenant", PropertyInput::from(tenant.to_string())),
    ]
}

/// A multi-operation write batch with precomputed values, so retries
/// replay it exactly.
#[derive(Debug, Clone)]
enum Chain {
    /// Several replacements of one document; the last of each wins.
    Rewrites {
        id: u64,
        first: [f32; 2],
        body: String,
        tenant: &'static str,
        last: [f32; 2],
    },
    /// Leaves and rejoins the indexed label around a re-embedding.
    Relabel { id: u64, embedding: [f32; 2] },
    /// Removes the embedding, then sets a new one.
    RemoveRestore { id: u64, embedding: [f32; 2] },
    /// Creates a document and edits it before commit.
    CreateEdit {
        embedding: [f32; 2],
        body: String,
        tenant: &'static str,
        moved: &'static str,
        last: [f32; 2],
    },
    /// Creates a document and drops it before commit.
    CreateDrop {
        embedding: [f32; 2],
        body: String,
        tenant: &'static str,
    },
    /// Exchanges the tenants and embeddings of two documents.
    Swap {
        left: u64,
        right: u64,
        left_value: (&'static str, [f32; 2]),
        right_value: (&'static str, [f32; 2]),
    },
}

impl Chain {
    fn random(rng: &mut Rng, id: u64, other: u64) -> Self {
        match rng.below(6) {
            0 => Self::Rewrites {
                id,
                first: rng.embedding(),
                body: rng.body(),
                tenant: rng.tenant(),
                last: rng.embedding(),
            },
            1 => Self::Relabel {
                id,
                embedding: rng.embedding(),
            },
            2 => Self::RemoveRestore {
                id,
                embedding: rng.embedding(),
            },
            3 => Self::CreateEdit {
                embedding: rng.embedding(),
                body: rng.body(),
                tenant: rng.tenant(),
                moved: rng.tenant(),
                last: rng.embedding(),
            },
            4 => Self::CreateDrop {
                embedding: rng.embedding(),
                body: rng.body(),
                tenant: rng.tenant(),
            },
            _ => Self::Swap {
                left: id,
                right: other,
                left_value: (rng.tenant(), rng.embedding()),
                right_value: (rng.tenant(), rng.embedding()),
            },
        }
    }

    /// Existing documents the chain writes.
    fn ids(&self) -> Vec<u64> {
        match self {
            Self::Rewrites { id, .. }
            | Self::Relabel { id, .. }
            | Self::RemoveRestore { id, .. } => vec![*id],
            Self::Swap { left, right, .. } => vec![*left, *right],
            Self::CreateEdit { .. } | Self::CreateDrop { .. } => Vec::new(),
        }
    }

    const fn kind(&self) -> usize {
        match self {
            Self::Rewrites { .. } => 0,
            Self::Relabel { .. } => 1,
            Self::RemoveRestore { .. } => 2,
            Self::CreateEdit { .. } => 3,
            Self::CreateDrop { .. } => 4,
            Self::Swap { .. } => 5,
        }
    }

    fn request(&self) -> QueryRequest {
        let write = batch::write_batch();
        let created = || traversal::g().n(NodeRef::Var("created".to_string()));
        let write = match self {
            Self::Rewrites {
                id,
                first,
                body,
                tenant,
                last,
            } => write
                .var_as("a", node(*id).set_property("embedding", first.to_vec()))
                .var_as("b", node(*id).set_property("body", body.clone()))
                .var_as("c", node(*id).set_property("tenant", tenant.to_string()))
                .var_as("d", node(*id).set_property("embedding", last.to_vec())),
            Self::Relabel { id, embedding } => write
                .var_as("a", node(*id).set_property("$label", "Other".to_string()))
                .var_as("b", node(*id).set_property("embedding", embedding.to_vec()))
                .var_as("c", node(*id).set_property("$label", "Doc".to_string())),
            Self::RemoveRestore { id, embedding } => write
                .var_as("a", node(*id).remove_property("embedding"))
                .var_as("b", node(*id).set_property("embedding", embedding.to_vec())),
            Self::CreateEdit {
                embedding,
                body,
                tenant,
                moved,
                last,
            } => write
                .var_as(
                    "created",
                    traversal::g().add_n("Doc", doc_properties(*embedding, body, tenant)),
                )
                .var_as("a", created().set_property("tenant", moved.to_string()))
                .var_as("b", created().set_property("embedding", last.to_vec())),
            Self::CreateDrop {
                embedding,
                body,
                tenant,
            } => write
                .var_as(
                    "created",
                    traversal::g().add_n("Doc", doc_properties(*embedding, body, tenant)),
                )
                .var_as("a", created().drop()),
            Self::Swap {
                left,
                right,
                left_value,
                right_value,
            } => write
                .var_as(
                    "a",
                    node(*left)
                        .set_property("tenant", right_value.0.to_string())
                        .set_property("embedding", right_value.1.to_vec()),
                )
                .var_as(
                    "b",
                    node(*right)
                        .set_property("tenant", left_value.0.to_string())
                        .set_property("embedding", left_value.1.to_vec()),
                ),
        };
        match self {
            Self::CreateEdit { .. } => QueryRequest::write(write.returning(["created"])),
            Self::Rewrites { .. }
            | Self::Relabel { .. }
            | Self::RemoveRestore { .. }
            | Self::CreateDrop { .. }
            | Self::Swap { .. } => QueryRequest::write(write),
        }
    }
}

async fn insert(
    db: &HelixDB,
    counters: &Counters,
    label: &'static str,
    (embedding, body, tenant): ([f32; 2], String, &'static str),
) -> u64 {
    let result = execute(db, counters, || {
        QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "created",
                    traversal::g().add_n(label, doc_properties(embedding, &body, tenant)),
                )
                .returning(["created"]),
        )
    })
    .await;
    result["created"][0]["$id"].as_u64().unwrap()
}

async fn set(db: &HelixDB, counters: &Counters, id: u64, property: &str, value: PropertyInput) {
    execute(db, counters, || {
        QueryRequest::write(
            batch::write_batch().var_as("set", node(id).set_property(property, value.clone())),
        )
    })
    .await;
}

/// Writes a document's current values back unchanged; returns whether the
/// document still existed.
async fn rewrite_identically(db: &HelixDB, counters: &Counters, id: u64) -> bool {
    let current = execute(db, counters, || {
        QueryRequest::read(
            batch::read_batch()
                .var_as("current", node(id).value_map(None::<Vec<String>>))
                .returning(["current"]),
        )
    })
    .await;
    let Some(current) = current["current"].as_array().and_then(|rows| rows.first()) else {
        return false;
    };
    let current = current.clone();
    execute(db, counters, || {
        let tenant = current["tenant"]
            .as_str()
            .expect("every node keeps a tenant");
        let mut target = node(id).set_property("tenant", tenant.to_string());
        if let Some(embedding) = current["embedding"].as_array() {
            target = target.set_property(
                "embedding",
                embedding
                    .iter()
                    .map(|value| value.as_f64().unwrap() as f32)
                    .collect::<Vec<_>>(),
            );
        }
        for property in ["body", "$label"] {
            if let Some(value) = current[property].as_str() {
                target = target.set_property(property, value.to_string());
            }
        }
        QueryRequest::write(batch::write_batch().var_as("rewritten", target))
    })
    .await;
    true
}

/// Every committed write by node, so a failure can show how a node got
/// into the state that broke an invariant.
pub(super) struct History {
    started: Instant,
    writes: Mutex<Vec<(Duration, u64, String)>>,
}

impl History {
    pub(super) fn new() -> Self {
        Self {
            started: Instant::now(),
            writes: Mutex::new(Vec::new()),
        }
    }

    pub(super) fn record(&self, id: u64, write: String) {
        let at = self.started.elapsed();
        self.writes.lock().unwrap().push((at, id, write));
    }

    /// The writes of `id`, oldest first.
    fn of(&self, id: u64) -> Vec<String> {
        self.writes
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, written, _)| *written == id)
            .map(|(at, _, write)| format!("{:.3}s {write}", at.as_secs_f64()))
            .collect()
    }
}

/// One writer: random operations over the shared document pool until `stop`.
async fn writer(
    db: Arc<HelixDB>,
    mut rng: Rng,
    pool: Arc<Mutex<Vec<u64>>>,
    stop: Arc<AtomicBool>,
    counters: Arc<Counters>,
    history: Arc<History>,
) {
    let mut removed: Option<(u64, &'static str)> = None;
    while !stop.load(Ordering::Relaxed) {
        if let Some((id, property)) = removed.take() {
            let value = match property {
                "embedding" => PropertyInput::from(rng.embedding().to_vec()),
                _ => PropertyInput::from(rng.body()),
            };
            history.record(id, format!("restore {property} {value:?}"));
            set(&db, &counters, id, property, value).await;
            counters.count(Op::RestoreProperty);
            continue;
        }
        let (id, other, size) = {
            let pool = pool.lock().unwrap();
            (
                pool[rng.below(pool.len())],
                pool[rng.below(pool.len())],
                pool.len(),
            )
        };
        let roll = rng.below(100);
        let op = match roll {
            0..14 if size < TARGET_DOCS => Op::Insert,
            0..14 => Op::Delete,
            14..17 => Op::InsertOther,
            17..29 => Op::Embedding,
            29..39 => Op::Body,
            39..47 => Op::IdenticalRewrite,
            47..53 => Op::RemoveProperty,
            53..61 if size > TARGET_DOCS / 2 => Op::Delete,
            53..61 => Op::Insert,
            61..71 => Op::TenantMove,
            71..79 => Op::Relabel,
            _ => Op::Chain,
        };
        // Writes are recorded before they commit, so a reader of the
        // history never misses one that already took effect.
        match op {
            Op::Insert | Op::InsertOther => {
                let label = if matches!(op, Op::InsertOther) {
                    "Other"
                } else {
                    "Doc"
                };
                let values = (rng.embedding(), rng.body(), rng.tenant());
                let description = format!("insert {label} {values:?}");
                let created = insert(&db, &counters, label, values).await;
                history.record(created, description);
                pool.lock().unwrap().push(created);
            }
            Op::Embedding | Op::Body | Op::TenantMove | Op::Relabel => {
                let (property, value) = match op {
                    Op::Embedding => ("embedding", PropertyInput::from(rng.embedding().to_vec())),
                    Op::Body => ("body", PropertyInput::from(rng.body())),
                    Op::TenantMove => ("tenant", PropertyInput::from(rng.tenant().to_string())),
                    Op::Relabel => {
                        let label = if rng.below(2) == 0 { "Other" } else { "Doc" };
                        ("$label", PropertyInput::from(label.to_string()))
                    }
                    Op::Insert
                    | Op::InsertOther
                    | Op::IdenticalRewrite
                    | Op::RemoveProperty
                    | Op::RestoreProperty
                    | Op::Delete
                    | Op::Chain => unreachable!("only property sets reach here"),
                };
                history.record(id, format!("set {property} {value:?}"));
                set(&db, &counters, id, property, value).await;
            }
            Op::IdenticalRewrite => {
                history.record(id, "identical rewrite".to_string());
                if !rewrite_identically(&db, &counters, id).await {
                    continue;
                }
            }
            Op::RemoveProperty => {
                let property = if rng.below(2) == 0 {
                    "embedding"
                } else {
                    "body"
                };
                history.record(id, format!("remove {property}"));
                execute(&db, &counters, || {
                    QueryRequest::write(
                        batch::write_batch().var_as("removed", node(id).remove_property(property)),
                    )
                })
                .await;
                removed = Some((id, property));
            }
            Op::Delete => {
                history.record(id, "delete".to_string());
                execute(&db, &counters, || {
                    QueryRequest::write(batch::write_batch().var_as("dropped", node(id).drop()))
                })
                .await;
                pool.lock().unwrap().retain(|candidate| *candidate != id);
            }
            Op::Chain => {
                let chain = Chain::random(&mut rng, id, other);
                for touched in chain.ids() {
                    history.record(touched, format!("{chain:?}"));
                }
                let result = execute(&db, &counters, || chain.request()).await;
                if let Chain::CreateEdit { .. } = chain {
                    let created = result["created"][0]["$id"].as_u64().unwrap();
                    history.record(created, format!("{chain:?}"));
                    pool.lock().unwrap().push(created);
                }
                counters.chains[chain.kind()].fetch_add(1, Ordering::Relaxed);
            }
            Op::RestoreProperty => unreachable!("restores follow removals"),
        }
        counters.count(op);
    }
}

/// A live `Doc` in one read snapshot.
#[derive(Debug, Clone)]
struct Doc {
    tenant: String,
    embedding: Option<[f32; 2]>,
    body: Option<String>,
}

fn snapshot(result: &serde_json::Value) -> BTreeMap<u64, Doc> {
    let Some(rows) = result["docs"].as_array() else {
        assert!(result["docs"].is_null(), "docs read returned {result}");
        return BTreeMap::new();
    };
    rows.iter()
        .map(|row| {
            let embedding = row["embedding"].as_array().map(|values| {
                assert_eq!(values.len(), 2, "{row}");
                [
                    values[0].as_f64().unwrap() as f32,
                    values[1].as_f64().unwrap() as f32,
                ]
            });
            let doc = Doc {
                tenant: row["tenant"]
                    .as_str()
                    .unwrap_or_else(|| panic!("every document keeps a tenant: {row}"))
                    .to_string(),
                embedding,
                body: row["body"].as_str().map(str::to_string),
            };
            (row["$id"].as_u64().unwrap(), doc)
        })
        .collect()
}

/// Squared Euclidean distance bits as search reports them; exact in `f32`
/// for grid points.
fn distance(embedding: [f32; 2], query: [f32; 2]) -> u64 {
    let (dx, dy) = (embedding[0] - query[0], embedding[1] - query[1]);
    f64::from(dx * dx + dy * dy).to_bits()
}

/// Tantivy BM25 (`k1` 1.2, `b` 0.75) over one partition's exact statistics.
fn bm25(term_frequency: f64, length: f64, documents: f64, tokens: f64, frequency: f64) -> f64 {
    let idf = (1.0 + (documents - frequency + 0.5) / (frequency + 0.5)).ln();
    let norm = 1.2 * (1.0 - 0.75 + 0.75 * length / (tokens / documents));
    idf * 2.2 * term_frequency / (term_frequency + norm)
}

/// Searches one read batch runs next to its own `Doc` snapshot.
struct ReadPlan {
    vector: Vec<(&'static str, [f32; 2])>,
    text: Vec<(&'static str, &'static str)>,
}

impl ReadPlan {
    fn random(rng: &mut Rng, serving: Serving) -> Self {
        Self {
            vector: if serving.vector {
                TENANTS
                    .iter()
                    .map(|tenant| (*tenant, rng.query()))
                    .collect()
            } else {
                Vec::new()
            },
            text: if serving.text {
                TENANTS
                    .iter()
                    .map(|tenant| (*tenant, VOCABULARY[rng.below(VOCABULARY.len())]))
                    .collect()
            } else {
                Vec::new()
            },
        }
    }

    /// Every tenant against fixed points and every vocabulary word.
    fn exhaustive() -> Self {
        let points = [[0.125, 0.125], [8.0, 8.0], [15.875, 3.5], [4.375, 12.625]];
        Self {
            vector: TENANTS
                .iter()
                .flat_map(|tenant| points.iter().map(|point| (*tenant, *point)))
                .collect(),
            text: TENANTS
                .iter()
                .flat_map(|tenant| VOCABULARY.iter().map(|word| (*tenant, *word)))
                .collect(),
        }
    }

    fn request(&self, consistency: SearchConsistency) -> QueryRequest {
        let mut read = batch::read_batch().var_as(
            "docs",
            traversal::g()
                .n_with_label("Doc")
                .value_map(None::<Vec<String>>),
        );
        let mut names = vec!["docs".to_string()];
        for (index, (tenant, query)) in self.vector.iter().enumerate() {
            let name = format!("v{index}");
            read = read.var_as(
                &name,
                traversal::g().vector_search_nodes(
                    "Doc",
                    "embedding",
                    query.to_vec(),
                    VECTOR_K,
                    Some(PropertyValue::from(*tenant)),
                ),
            );
            names.push(name);
        }
        for (index, (tenant, word)) in self.text.iter().enumerate() {
            let name = format!("t{index}");
            read = read.var_as(
                &name,
                traversal::g().text_search_nodes(
                    "Doc",
                    "body",
                    *word,
                    TEXT_K,
                    Some(PropertyValue::from(*tenant)),
                ),
            );
            names.push(name);
        }
        QueryRequest::read(read.returning(names))
            .with_search_consistency(consistency)
            .unwrap()
    }

    /// Runs the batch, retrying index backpressure, and returns the response.
    async fn run(
        &self,
        db: &HelixDB,
        consistency: SearchConsistency,
        counters: &Counters,
        history: &History,
    ) -> serde_json::Value {
        for _ in 0..2_000 {
            match Box::pin(db.query(self.request(consistency))).await {
                Ok(result) => return result,
                Err(error) if error.is_index_backpressure() => {
                    counters.read_backpressure.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(error) => {
                    // Name the history of every node the error mentions.
                    let message = error.to_string();
                    let nodes = message
                        .split("node ")
                        .skip(1)
                        .filter_map(|rest| {
                            let digits = rest
                                .chars()
                                .take_while(char::is_ascii_digit)
                                .collect::<String>();
                            digits.parse::<u64>().ok()
                        })
                        .map(|id| (id, history.of(id)))
                        .collect::<Vec<_>>();
                    panic!("{consistency:?} read failed: {message}; writes: {nodes:?}")
                }
            }
        }
        panic!("{consistency:?} read stayed backpressured")
    }

    /// Asserts every search equals exact search over the response's own
    /// snapshot.
    fn assert_exact(&self, result: &serde_json::Value, context: &str) {
        let docs = snapshot(result);
        for (index, (tenant, query)) in self.vector.iter().enumerate() {
            let found = hits(result, &format!("v{index}"));
            let context = format!("{context}: vector {tenant} {query:?} found {found:?}");
            let mut exact = docs
                .iter()
                .filter(|(_, doc)| doc.tenant == *tenant)
                .filter_map(|(id, doc)| doc.embedding.map(|vector| (distance(vector, *query), *id)))
                .collect::<Vec<_>>();
            exact.sort_unstable();
            assert_distinct(&found, &context);
            for (id, bits) in &found {
                let doc = docs
                    .get(id)
                    .unwrap_or_else(|| panic!("{context}: {id} is not a live Doc"));
                assert_eq!(doc.tenant, *tenant, "{context}: {id} is in another tenant");
                let vector = doc
                    .embedding
                    .unwrap_or_else(|| panic!("{context}: {id} has no embedding"));
                assert_eq!(
                    *bits,
                    distance(vector, *query),
                    "{context}: {id}'s distance"
                );
            }
            // With each hit's distance exact, equal distance lists mean the
            // hits are a top-k (ties at the boundary may pick any member).
            assert_eq!(
                found.iter().map(|(_, bits)| *bits).collect::<Vec<_>>(),
                exact
                    .iter()
                    .take(VECTOR_K)
                    .map(|(bits, _)| *bits)
                    .collect::<Vec<_>>(),
                "{context}: exact {:?}",
                &exact[..exact.len().min(VECTOR_K + 2)]
            );
        }
        for (index, (tenant, word)) in self.text.iter().enumerate() {
            let found = hits(result, &format!("t{index}"));
            let context = format!("{context}: text {tenant} {word:?} found {found:?}");
            let partition = docs
                .iter()
                .filter(|(_, doc)| doc.tenant == *tenant)
                .filter_map(|(id, doc)| {
                    doc.body
                        .as_deref()
                        .map(|body| (*id, body.split(' ').collect::<Vec<_>>()))
                })
                .collect::<BTreeMap<_, _>>();
            let documents = partition.len() as f64;
            let tokens = partition.values().map(Vec::len).sum::<usize>() as f64;
            let matching = partition
                .iter()
                .filter_map(|(id, words)| {
                    let frequency = words.iter().filter(|candidate| *candidate == word).count();
                    (frequency > 0).then_some((*id, (frequency as f64, words.len() as f64)))
                })
                .collect::<BTreeMap<_, _>>();
            assert_distinct(&found, &context);
            assert_eq!(
                found.iter().map(|(id, _)| *id).collect::<BTreeSet<_>>(),
                matching.keys().copied().collect::<BTreeSet<_>>(),
                "{context}: every match and nothing else"
            );
            let frequency = matching.len() as f64;
            for (id, bits) in &found {
                let (term_frequency, length) = matching[id];
                let expected = bm25(term_frequency, length, documents, tokens, frequency);
                let score = f64::from_bits(*bits);
                assert!(
                    (score - expected).abs() <= expected.abs() * 1e-4,
                    "{context}: {id} scored {score}, exact statistics give {expected} \
                     (documents {documents}, tokens {tokens}, frequency {frequency})"
                );
            }
            assert!(
                found
                    .windows(2)
                    .all(|pair| f64::from_bits(pair[0].1) >= f64::from_bits(pair[1].1)),
                "{context}: scores descend"
            );
        }
    }

    /// Asserts every search names distinct documents of the snapshot.
    fn assert_eventual(&self, result: &serde_json::Value, context: &str) {
        let docs = snapshot(result);
        let names = (0..self.vector.len())
            .map(|index| format!("v{index}"))
            .chain((0..self.text.len()).map(|index| format!("t{index}")));
        for name in names {
            let found = hits(result, &name);
            let context = format!("{context}: eventual {name} found {found:?}");
            assert_distinct(&found, &context);
            for (id, _) in &found {
                assert!(docs.contains_key(id), "{context}: {id} is not a live Doc");
            }
        }
    }
}

fn assert_distinct(found: &[(u64, u64)], context: &str) {
    assert_eq!(
        found
            .iter()
            .map(|(id, _)| id)
            .collect::<BTreeSet<_>>()
            .len(),
        found.len(),
        "{context}: duplicate hits"
    );
}

/// One reader: strong and eventual batches over the Active indexes.
async fn reader(
    db: Arc<HelixDB>,
    mut rng: Rng,
    stop: Arc<AtomicBool>,
    serving: Arc<RwLock<Serving>>,
    counters: Arc<Counters>,
    history: Arc<History>,
    context: String,
) {
    while !stop.load(Ordering::Relaxed) {
        let guard = serving.read().await;
        if !guard.vector && !guard.text {
            drop(guard);
            tokio::time::sleep(Duration::from_millis(5)).await;
            continue;
        }
        let plan = ReadPlan::random(&mut rng, *guard);
        let strong = rng.below(3) != 0;
        let consistency = if strong {
            SearchConsistency::Strong
        } else {
            SearchConsistency::Eventual
        };
        let result = plan.run(&db, consistency, &counters, &history).await;
        drop(guard);
        if strong {
            plan.assert_exact(&result, &context);
            counters
                .strong_vector_checks
                .fetch_add(plan.vector.len() as u64, Ordering::Relaxed);
            counters
                .strong_text_checks
                .fetch_add(plan.text.len() as u64, Ordering::Relaxed);
        } else {
            plan.assert_eventual(&result, &context);
            counters.eventual_checks.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// One manifest observation: its L0 view IDs, the last sorted run's ID and
/// view IDs, and whether queues held work when it was read.
type ManifestSample<View> = (BTreeSet<View>, Option<(u32, Vec<View>)>, bool);

/// Polls the manifest, counting L0 compactions and rewrites of the oldest
/// sorted run, and whether queues held work on both sides of each.
async fn watch_compactions(
    db: Arc<HelixDB>,
    store: Arc<dyn ObjectStore>,
    stop: Arc<AtomicBool>,
    counters: Arc<Counters>,
) {
    let admin = slatedb::admin::Admin::builder(db.path().to_string(), store).build();
    let mut previous: Option<ManifestSample<_>> = None;
    while !stop.load(Ordering::Relaxed) {
        let pending = db.index_operation_queue_stats().pending_operations > 0;
        if let Ok(Some(manifest)) = admin.read_manifest(None).await {
            let l0 = manifest
                .l0()
                .iter()
                .map(|view| view.id)
                .collect::<BTreeSet<_>>();
            // Compaction writes L0 into a fresh, higher sorted-run ID and
            // merges runs into the oldest one, so the lowest ID is the last
            // run.
            let last = manifest
                .compacted()
                .iter()
                .min_by_key(|run| run.id)
                .map(|run| {
                    (
                        run.id,
                        run.sst_views.iter().map(|view| view.id).collect::<Vec<_>>(),
                    )
                });
            if let Some((before_l0, before_last, was_pending)) = &previous {
                let busy = *was_pending && pending;
                let l0_compacted = before_l0.difference(&l0).next().is_some();
                let last_rewritten = before_last.is_some() && *before_last != last;
                for (happened, total, while_pending) in [
                    (
                        l0_compacted,
                        &counters.l0_compactions,
                        &counters.l0_compactions_while_pending,
                    ),
                    (
                        last_rewritten,
                        &counters.last_run_compactions,
                        &counters.last_run_compactions_while_pending,
                    ),
                ] {
                    if happened {
                        total.fetch_add(1, Ordering::Relaxed);
                        if busy {
                            while_pending.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
            previous = Some((l0, last, pending));
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Waits until publication acknowledged every operation.
async fn wait_until_published(db: &HelixDB, history: &History, context: &str) {
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        let stats = db.index_operation_queue_stats();
        if stats.pending_operations == 0 && stats.uncertain_operations == 0 {
            return;
        }
        if Instant::now() >= deadline {
            // A stuck queue usually follows a corrupt graph; name it first.
            assert_vector_graph(db, history, context).await;
            panic!(
                "{context}: publication did not drain: {stats:?}, targets {:?}",
                db.index_operation_backlog().outstanding_targets()
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The partition a tenant's documents are indexed under.
pub(super) fn partition(tenant: &str) -> TextPartition {
    TextPartition::try_tenant_value(encode_index_partition_value(&StoredValue::String(
        tenant.to_string(),
    )))
    .unwrap()
}

/// One vector namespace's rows, classified by node.
#[derive(Debug, Default)]
pub(super) struct Namespace {
    count: Option<u64>,
    entry_point: Option<u64>,
    max_layer: Option<u16>,
    items: BTreeSet<u64>,
    simhashes: BTreeSet<u64>,
    directory: BTreeSet<u64>,
    uppers: BTreeSet<u64>,
    owners: BTreeSet<u64>,
    candidates: BTreeSet<u64>,
    /// Layer of every node by its entry-candidate node row.
    layers: BTreeMap<u64, u16>,
    /// `(layer, node)` of every sorted entry-candidate row.
    sorted: BTreeSet<(u16, u64)>,
    /// `(layer, owner)` of every neighbour-list row.
    rows: BTreeSet<(u16, u64)>,
    /// Every node a neighbour list or reverse edge names, with how.
    named: BTreeMap<u64, BTreeSet<String>>,
    /// `(layer, source, target)` of every neighbour-list entry.
    links: BTreeSet<(u16, u64, u64)>,
    /// `(layer, source, target)` of every reverse-edge locator.
    locators: BTreeSet<(u16, u64, u64)>,
}

impl Namespace {
    /// The nodes holding an item.
    pub(super) const fn items(&self) -> &BTreeSet<u64> {
        &self.items
    }

    /// Reads every row of the unscoped namespace `physical`.
    pub(super) async fn read(read: &(impl DbReadOps + Sync), physical: u64) -> Self {
        let mut namespace = Self::default();
        for lane in VectorStorageLane::ALL {
            let prefix = DataKey::data_prefix(
                DataScope::LegacyUnscoped,
                lane.prefix_key(physical).to_bytes(),
            );
            let mut rows = read.scan_prefix(&prefix, ..).await.unwrap();
            while let Some(row) = rows.next().await.unwrap() {
                namespace.classify(&row.key, &row.value);
            }
        }
        namespace
    }

    fn classify(&mut self, key: &[u8], value: &[u8]) {
        let Ok(DataKey::Data {
            kind: DataKeyKind::Vector(vector_key),
            ..
        }) = DataKey::parse_from_slice(DataScope::LegacyUnscoped, key)
        else {
            panic!("unparseable vector row {key:?}");
        };
        let mut name = |named: u64, by: String| {
            self.named.entry(named).or_default().insert(by);
        };
        match vector_key {
            VectorKey::IndexMetadata(_) => {
                let metadata = crate::search::vector::decode_metadata(value).unwrap();
                self.count = Some(metadata.count);
                self.entry_point = metadata.entry_point;
                self.max_layer = Some(metadata.max_layer);
            }
            VectorKey::Vector(key) => {
                self.items.insert(key.node_id());
            }
            VectorKey::SimHash(key) => {
                self.simhashes.insert(key.node_id());
            }
            VectorKey::SimHashDirectory(key) => {
                self.directory.insert(key.node_id());
            }
            VectorKey::UpperVector(key) => {
                self.uppers.insert(key.node_id());
            }
            VectorKey::Layer0Neighbors(key) => {
                for neighbour in decode_layer0_neighbors(value).unwrap() {
                    name(neighbour, format!("L0 list of {}", key.node_id()));
                    self.links.insert((0, key.node_id(), neighbour));
                }
                self.owners.insert(key.node_id());
                self.rows.insert((0, key.node_id()));
            }
            VectorKey::UpperNeighbors(key) => {
                for neighbour in decode_upper_neighbors(value).unwrap() {
                    name(
                        neighbour,
                        format!("L{} list of {}", key.layer(), key.node_id()),
                    );
                    self.links.insert((key.layer(), key.node_id(), neighbour));
                }
                self.owners.insert(key.node_id());
                self.rows.insert((key.layer(), key.node_id()));
            }
            VectorKey::ReverseEdge(key) => {
                let (target, layer, source) =
                    (key.target_node_id(), key.layer(), key.source_node_id());
                name(target, format!("L{layer} locator from {source}"));
                name(source, format!("L{layer} locator to {target}"));
                self.locators.insert((layer, source, target));
            }
            VectorKey::EntryCandidateSorted(key) => {
                self.candidates.insert(key.node_id());
                self.sorted.insert((key.layer(), key.node_id()));
            }
            VectorKey::EntryCandidateNode(key) => {
                self.candidates.insert(key.node_id());
                self.layers.insert(
                    key.node_id(),
                    crate::search::vector::decode_entry_candidate_layer(value).unwrap(),
                );
            }
            VectorKey::TxnGuard(_) => {}
            key @ (VectorKey::IndexPrefix(_)
            | VectorKey::VectorPrefix(_)
            | VectorKey::SimHashDirectoryPrefix(_)
            | VectorKey::EntryCandidatePrefix(_)
            | VectorKey::MemoryPrefix(_)
            | VectorKey::L0Prefix(_)
            | VectorKey::ReverseEdgePrefix(_)) => panic!("a row parsed as prefix {key:?}"),
        }
    }

    /// Returns every way the namespace contradicts itself: rows of a node
    /// without an item, links naming one, a link without exactly one
    /// locator, rows or links off their node's layers, or stale metadata.
    /// Holds for every committed state, whatever publication still owes.
    pub(super) fn violations(&self, history: &History) -> Vec<String> {
        let mut violations = Vec::new();
        if self.count != Some(self.items.len() as u64) {
            violations.push(format!(
                "metadata count {:?} for {} items",
                self.count,
                self.items.len()
            ));
        }
        if self.simhashes != self.items {
            violations.push(format!(
                "SimHash rows differ from items: {:?}",
                self.simhashes
                    .symmetric_difference(&self.items)
                    .collect::<Vec<_>>()
            ));
        }
        if !self.directory.is_empty() && self.directory != self.items {
            violations.push("SimHash directory differs from items".to_string());
        }
        // Only nodes above layer 0 have upper rows.
        for (label, set) in [
            ("upper vector", &self.uppers),
            ("neighbour-list owner", &self.owners),
            ("entry candidate", &self.candidates),
        ] {
            let dangling = set.difference(&self.items).collect::<Vec<_>>();
            if !dangling.is_empty() {
                violations.push(format!("{label} rows without an item: {dangling:?}"));
            }
        }
        for (dangling, by) in &self.named {
            if !self.items.contains(dangling) {
                violations.push(format!(
                    "links name {dangling}, which has no item; named by {by:?}; \
                     its writes: {:?}",
                    history.of(*dangling)
                ));
            }
        }
        // Deletion finds a node's incoming links through its locators.
        for (label, missing) in [
            (
                "links without a locator",
                self.links.difference(&self.locators),
            ),
            (
                "locators without a link",
                self.locators.difference(&self.links),
            ),
        ] {
            let missing = missing.collect::<Vec<_>>();
            if !missing.is_empty() {
                let targets = missing
                    .iter()
                    .map(|(_, _, target)| *target)
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .take(3)
                    .map(|target| {
                        format!(
                            "{target} (item here: {}): {:?}",
                            self.items.contains(&target),
                            history.of(target)
                        )
                    })
                    .collect::<Vec<_>>();
                violations.push(format!(
                    "{label} (layer, source, target): {missing:?}; targets {targets:#?}"
                ));
            }
        }
        match self.entry_point {
            Some(entry) if !self.items.contains(&entry) => {
                violations.push(format!("entry point {entry} has no item"));
            }
            None if !self.items.is_empty() => {
                violations.push("a populated namespace has no entry point".to_string());
            }
            Some(_) | None => {}
        }
        violations.extend(self.layer_violations());
        violations
    }

    /// Returns every way rows contradict their node's layer: each item has
    /// one layer, recorded by both entry-candidate rows; exactly the items
    /// above layer 0 hold an upper vector; every item holds one neighbour
    /// list per layer up to its own and no other; every link names a node
    /// on its layer; and the entry point sits on the highest layer, which
    /// metadata records.
    fn layer_violations(&self) -> Vec<String> {
        let mut violations = Vec::new();
        let layered = self.layers.keys().copied().collect::<BTreeSet<_>>();
        if layered != self.items {
            violations.push(format!(
                "entry-candidate layers differ from items: {:?}",
                layered
                    .symmetric_difference(&self.items)
                    .collect::<Vec<_>>()
            ));
        }
        let sorted = self
            .layers
            .iter()
            .map(|(node, layer)| (*layer, *node))
            .collect::<BTreeSet<_>>();
        if sorted != self.sorted {
            violations.push(format!(
                "sorted entry candidates differ from node layers: {:?}",
                sorted
                    .symmetric_difference(&self.sorted)
                    .collect::<Vec<_>>()
            ));
        }
        let uppers = self
            .layers
            .iter()
            .filter(|(_, layer)| **layer > 0)
            .map(|(node, _)| *node)
            .collect::<BTreeSet<_>>();
        if uppers != self.uppers {
            violations.push(format!(
                "upper vectors differ from items above layer 0: {:?}",
                uppers
                    .symmetric_difference(&self.uppers)
                    .collect::<Vec<_>>()
            ));
        }
        let rows = self
            .layers
            .iter()
            .filter(|(node, _)| self.items.contains(node))
            .flat_map(|(node, layer)| (0..=*layer).map(move |row| (row, *node)))
            .collect::<BTreeSet<_>>();
        if rows != self.rows {
            violations.push(format!(
                "neighbour-list rows (layer, owner) differ from item layers: {:?}",
                rows.symmetric_difference(&self.rows).collect::<Vec<_>>()
            ));
        }
        let off_layer = self
            .links
            .iter()
            .filter(|(layer, _, target)| {
                self.layers
                    .get(target)
                    .is_none_or(|target_layer| target_layer < layer)
            })
            .collect::<Vec<_>>();
        if !off_layer.is_empty() {
            violations.push(format!(
                "links (layer, source, target) above their target's layer: {off_layer:?}"
            ));
        }
        let highest = self.layers.values().max().copied();
        let entry_layer = self
            .entry_point
            .and_then(|entry| self.layers.get(&entry).copied());
        if let Some(entry) = self.entry_point
            && (entry_layer != highest || self.max_layer != highest)
        {
            violations.push(format!(
                "entry point {entry} on layer {entry_layer:?} with metadata max layer {:?}, \
                 but the highest layer is {highest:?}",
                self.max_layer
            ));
        }
        violations
    }
}

/// Reads the Active vector generation's namespaces from `read`, keyed by
/// partition (an unpartitioned generation's one under
/// [`TextPartition::Unpartitioned`]), or `None` while no vector generation is
/// Active.
pub(super) async fn vector_namespaces(
    db: &HelixDB,
    read: &(impl DbReadOps + Sync),
) -> Option<BTreeMap<TextPartition, (u64, Namespace)>> {
    let ActiveIndexHandle::Vector {
        index_id,
        generation,
        layout,
        ..
    } = db
        .active_index_handles_loaded(DataScope::LegacyUnscoped)
        .into_iter()
        .find(|handle| matches!(handle, ActiveIndexHandle::Vector { .. }))?
    else {
        unreachable!("the handle is a vector handle")
    };
    let mut physical = Vec::new();
    match layout {
        VectorPhysicalLayout::Unpartitioned { physical_index_id } => {
            physical.push((TextPartition::Unpartitioned, physical_index_id.get()));
        }
        VectorPhysicalLayout::Partitioned => {
            let prefix = ManagedIndexKey::data_prefix(
                DataScope::LegacyUnscoped,
                ScopedKey::logical_prefix(RecordKind::VectorPartitionMapping),
            );
            let mut mappings = read.scan_prefix(&prefix, ..).await.unwrap();
            while let Some(row) = mappings.next().await.unwrap() {
                let mapping = decode_partition_mapping(&row.value).unwrap();
                if mapping.index_id == index_id && mapping.generation == generation {
                    physical.push((
                        mapping.partition.as_partition().clone(),
                        mapping.physical_index_id.get(),
                    ));
                }
            }
        }
    }
    let mut namespaces = BTreeMap::new();
    for (partition, namespace) in physical {
        let rows = Namespace::read(read, namespace).await;
        namespaces.insert(partition, (namespace, rows));
    }
    Some(namespaces)
}

/// Asserts every namespace of the Active vector generation in one snapshot
/// is self-consistent.
pub(super) async fn assert_vector_graph(db: &HelixDB, history: &History, context: &str) {
    let snapshot = db.inner_db().snapshot().await.unwrap();
    let Some(namespaces) = vector_namespaces(db, snapshot.as_ref()).await else {
        return;
    };
    for (partition, (namespace, rows)) in &namespaces {
        let violations = rows.violations(history);
        assert!(
            violations.is_empty(),
            "{context}: vector namespace {namespace} ({partition:?}) is inconsistent: \
             {violations:#?}"
        );
    }
}

/// Checks the Active vector graph's consistency while the workload runs;
/// holding `serving` keeps the generation from being dropped meanwhile.
async fn watch_vector_graph(
    db: Arc<HelixDB>,
    stop: Arc<AtomicBool>,
    serving: Arc<RwLock<Serving>>,
    history: Arc<History>,
    counters: Arc<Counters>,
    context: String,
) {
    while !stop.load(Ordering::Relaxed) {
        let guard = serving.read().await;
        if guard.vector {
            assert_vector_graph(&db, &history, &context).await;
            counters.graph_checks.fetch_add(1, Ordering::Relaxed);
        }
        drop(guard);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Asserts the Active vector generation holds exactly the live embedded
/// documents of each tenant, in that tenant's namespace, consistently.
async fn assert_vector_rows(
    db: &HelixDB,
    docs: &BTreeMap<u64, Doc>,
    history: &History,
    context: &str,
) {
    let snapshot = db.inner_db().snapshot().await.unwrap();
    let namespaces = vector_namespaces(db, snapshot.as_ref())
        .await
        .unwrap_or_else(|| panic!("{context}: the vector index is Active"));
    let mut live = BTreeMap::<TextPartition, BTreeSet<u64>>::new();
    for (id, doc) in docs {
        if doc.embedding.is_some() {
            live.entry(partition(&doc.tenant)).or_default().insert(*id);
        }
    }
    assert_eq!(
        namespaces.keys().collect::<Vec<_>>(),
        live.keys().collect::<Vec<_>>(),
        "{context}: exactly the populated tenants own a namespace"
    );
    for (tenant_partition, ids) in &live {
        let (namespace, rows) = &namespaces[tenant_partition];
        let context = format!("{context}: namespace {namespace} ({tenant_partition:?})");
        assert_eq!(&rows.items, ids, "{context}: vector rows");
        let violations = rows.violations(history);
        assert!(violations.is_empty(), "{context}: {violations:#?}");
    }
}

/// Asserts the Active text generation's statistics markers account exactly
/// the live documents with a body: a present marker equals the document's
/// contribution, and only documents without one may keep an absent marker.
async fn assert_text_markers(db: &HelixDB, docs: &BTreeMap<u64, Doc>, context: &str) {
    let Some(ActiveIndexHandle::Text {
        index_id,
        generation,
        ..
    }) = db
        .active_index_handles_loaded(DataScope::LegacyUnscoped)
        .into_iter()
        .find(|handle| matches!(handle, ActiveIndexHandle::Text { .. }))
    else {
        panic!("{context}: the text index is Active");
    };
    let prefix = ManagedIndexKey::data_prefix(
        DataScope::LegacyUnscoped,
        ScopedKey::generation_prefix(RecordKind::TextStatisticsEntity, index_id, generation),
    );
    let storage = db.inner_db();
    let mut scan = storage.scan_prefix(&prefix, ..).await.unwrap();
    let mut present = BTreeMap::new();
    while let Some(row) = scan.next().await.unwrap() {
        let Ok(ManagedIndexKey::Data {
            kind: ScopedKey::TextStatisticsEntity(key),
            ..
        }) = ManagedIndexKey::parse_data_from_slice(&row.key)
        else {
            panic!("{context}: unparseable marker {:?}", row.key);
        };
        let marker = decode_statistics_entity(&row.value).unwrap();
        assert_eq!(marker.entity_id, key.entity.id, "{context}: marker owner");
        match marker.contribution {
            contribution @ TextStatisticsContribution::Present { .. } => {
                present.insert(key.entity.id.get(), contribution);
            }
            TextStatisticsContribution::Absent => {}
        }
    }
    let expected = docs
        .iter()
        .filter_map(|(id, doc)| {
            let body = doc.body.as_deref()?;
            let contribution =
                present_contribution(TextAnalyzerKind::Standard, partition(&doc.tenant), body)
                    .unwrap();
            Some((*id, contribution))
        })
        .collect::<BTreeMap<_, _>>();
    let wrong = expected
        .keys()
        .chain(present.keys())
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|id| present.get(id) != expected.get(id))
        .map(|id| (id, present.get(&id), expected.get(&id)))
        .collect::<Vec<_>>();
    assert!(
        wrong.is_empty(),
        "{context}: {} statistics markers disagree with live documents \
         (id, stored, live): {:?}",
        wrong.len(),
        wrong.iter().take(5).collect::<Vec<_>>()
    );
}

/// With writers stopped: drains publication, then asserts ledgers, exact
/// results under both consistencies, and physical rows.
async fn assert_quiescent(db: &HelixDB, counters: &Counters, history: &History, context: &str) {
    wait_until_published(db, history, context).await;
    let stats = db.index_operation_queue_stats();
    assert_eq!(
        (
            stats.retained_bytes,
            stats.pending_members,
            stats.pending_operations,
            stats.uncertain_operations
        ),
        (0, 0, 0, 0),
        "{context}: ledger totals"
    );
    assert!(
        db.index_operation_backlog()
            .outstanding_targets()
            .is_empty(),
        "{context}: no target keeps a charge"
    );
    let queues = rows(db, |key| {
        matches!(
            ManagedIndexKey::parse_data_from_slice(key),
            Ok(ManagedIndexKey::Data {
                kind: ScopedKey::IndexOperationQueue(_),
                ..
            })
        )
    })
    .await;
    assert!(
        queues.is_empty(),
        "{context}: drained queues resolve to no row"
    );
    let plan = ReadPlan::exhaustive();
    let mut docs = BTreeMap::new();
    for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
        let result = plan.run(db, consistency, counters, history).await;
        plan.assert_exact(&result, &format!("{context}: final {consistency:?}"));
        docs = snapshot(&result);
    }
    assert_vector_rows(db, &docs, history, context).await;
    assert_text_markers(db, &docs, context).await;
}

/// Publication counters of one writer handle, summed across restarts.
#[derive(Debug, Default)]
struct Totals {
    published: u64,
    deferred: u64,
    discarded: u64,
    batches: u64,
    output_retries: u64,
    commit_conflicts: u64,
    error_retries: u64,
}

impl Totals {
    fn add(&mut self, db: &HelixDB) {
        let stats = db.index_operation_queue_stats();
        self.published += stats.published_operations;
        self.deferred += stats.deferred_attempts;
        self.discarded += stats.discarded_operations;
        self.batches += stats.committed_batches;
        self.output_retries += stats.output_retries;
        self.commit_conflicts += stats.commit_conflicts;
        self.error_retries += stats.publication_error_retries;
    }
}

async fn soak(seed: u64, phase: Duration) {
    let context = format!("seed {seed}");
    println!("{context}: start, {CYCLES} phases of {phase:?}");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let name = format!("queue-soak-{seed}");
    let mut rng = Rng(seed);
    // Builds under load take several seconds, so both start early enough to
    // serve readers before the run ends.
    let create_at = [0.05 + 0.25 * rng.fraction(), 0.05 + 0.25 * rng.fraction()];
    let drop_at = 0.2 + 0.5 * rng.fraction();
    let dropped_family = if rng.below(2) == 0 {
        Family::Vector
    } else {
        Family::Text
    };
    let counters = Arc::new(Counters::default());
    let serving = Arc::new(RwLock::new(Serving::default()));
    let mut indexes = [Index::new(Family::Vector), Index::new(Family::Text)];
    let mut dropped = false;
    let mut totals = Totals::default();

    let mut db = Arc::new(open(&name, Arc::clone(&store), config()).await);
    let mut seeded = Vec::with_capacity(SEEDED_DOCS);
    for _ in 0..SEEDED_DOCS {
        let values = (rng.embedding(), rng.body(), rng.tenant());
        seeded.push(insert(&db, &counters, "Doc", values).await);
    }
    let pool = Arc::new(Mutex::new(seeded));
    let history = Arc::new(History::new());

    for cycle in 0..CYCLES {
        let stop = Arc::new(AtomicBool::new(false));
        let mut tasks = JoinSet::new();
        for writer_index in 0..WRITERS {
            tasks.spawn(writer(
                Arc::clone(&db),
                Rng(rng.next() ^ writer_index),
                Arc::clone(&pool),
                Arc::clone(&stop),
                Arc::clone(&counters),
                Arc::clone(&history),
            ));
        }
        for reader_index in 0..READERS {
            tasks.spawn(reader(
                Arc::clone(&db),
                Rng(rng.next() ^ reader_index),
                Arc::clone(&stop),
                Arc::clone(&serving),
                Arc::clone(&counters),
                Arc::clone(&history),
                format!("{context} cycle {cycle} reader {reader_index}"),
            ));
        }
        tasks.spawn(watch_compactions(
            Arc::clone(&db),
            Arc::clone(&store),
            Arc::clone(&stop),
            Arc::clone(&counters),
        ));
        tasks.spawn(watch_vector_graph(
            Arc::clone(&db),
            Arc::clone(&stop),
            Arc::clone(&serving),
            Arc::clone(&history),
            Arc::clone(&counters),
            format!("{context} cycle {cycle}"),
        ));

        let spawned = tasks.len();
        let started = Instant::now();
        while started.elapsed() < phase {
            let progress = started.elapsed().as_secs_f64() / phase.as_secs_f64();
            for (index, at) in indexes.iter_mut().zip(create_at) {
                if cycle == 0 && progress >= at && index.state == Lifecycle::Absent {
                    index.start(Lifecycle::Building(
                        ddl(&db, &counters, index.family, true).await,
                    ));
                }
            }
            let target = indexes
                .iter_mut()
                .find(|index| index.family == dropped_family)
                .unwrap();
            let served = match dropped_family {
                Family::Vector => &counters.strong_vector_checks,
                Family::Text => &counters.strong_text_checks,
            };
            if !dropped
                && ((cycle == 1 && progress >= drop_at) || cycle > 1)
                && target.state == Lifecycle::Active
                && Counters::get(served) > 0
            {
                start_drop(&db, &counters, target, &serving).await;
                dropped = true;
            }
            for index in &mut indexes {
                advance(&db, &counters, index, &serving).await;
            }
            // A failed task ends the phase at once.
            if tasks.len() < spawned {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        stop.store(true, Ordering::Relaxed);
        // Every task stops before a failure is reported, and the writer
        // closes, so the runtime can shut down behind the panic.
        let mut failures = Vec::new();
        while let Some(result) = tasks.join_next().await {
            if let Err(error) = result {
                failures.push(error.to_string());
            }
        }
        if !failures.is_empty() {
            db.close().await.unwrap();
            panic!("{context}: cycle {cycle} tasks failed: {failures:#?}");
        }
        if indexes[0].state == Lifecycle::Active {
            assert_vector_graph(&db, &history, &format!("{context} cycle {cycle} end")).await;
        }
        totals.add(&db);
        let mut states = Vec::new();
        for index in &indexes {
            let progress = match &index.state {
                Lifecycle::Building(operation) | Lifecycle::Dropping(operation) => {
                    operation_status(&db, operation).await["progress"].clone()
                }
                Lifecycle::Absent | Lifecycle::Active => serde_json::Value::Null,
            };
            states.push((index.family, index.state.clone(), progress));
        }
        db.close().await.unwrap();
        drop(db);
        println!(
            "{context}: cycle {cycle} closed with {} docs; indexes {states:?}",
            pool.lock().unwrap().len(),
        );
        db = Arc::new(open(&name, Arc::clone(&store), config()).await);
    }

    // Writers are stopped; finish outstanding builds, the drop, and its
    // recreation.
    let deadline = Instant::now() + Duration::from_secs(180);
    while !(dropped && indexes.iter().all(|index| index.state == Lifecycle::Active)) {
        assert!(Instant::now() < deadline, "{context}: lifecycle stalled");
        let target = indexes
            .iter_mut()
            .find(|index| index.family == dropped_family)
            .unwrap();
        if !dropped && target.state == Lifecycle::Active {
            start_drop(&db, &counters, target, &serving).await;
            dropped = true;
        }
        for index in &mut indexes {
            advance(&db, &counters, index, &serving).await;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_quiescent(&db, &counters, &history, &context).await;
    totals.add(&db);
    db.close().await.unwrap();
    drop(db);
    // A reopened writer rebuilds the same empty ledger over the same rows.
    let db = open(&name, Arc::clone(&store), config()).await;
    assert_quiescent(&db, &counters, &history, &format!("{context} reopened")).await;
    db.close().await.unwrap();

    let ops = OPS
        .iter()
        .map(|op| (*op, Counters::get(&counters.ops[*op as usize])))
        .collect::<Vec<_>>();
    let chains = counters
        .chains
        .iter()
        .map(Counters::get)
        .collect::<Vec<_>>();
    println!(
        "{context}: ops {ops:?} chains {chains:?} write conflicts {} write 429s {} \
         strong vector checks {} strong text checks {} eventual checks {} read 429s {} \
         graph checks {} \
         L0 compactions {} ({} while pending) last-run compactions {} ({} while pending) \
         builds {:?} publication {totals:?}",
        Counters::get(&counters.write_conflicts),
        Counters::get(&counters.write_backpressure),
        Counters::get(&counters.strong_vector_checks),
        Counters::get(&counters.strong_text_checks),
        Counters::get(&counters.eventual_checks),
        Counters::get(&counters.read_backpressure),
        Counters::get(&counters.graph_checks),
        Counters::get(&counters.l0_compactions),
        Counters::get(&counters.l0_compactions_while_pending),
        Counters::get(&counters.last_run_compactions),
        Counters::get(&counters.last_run_compactions_while_pending),
        indexes
            .iter()
            .map(|index| (index.family, index.builds, &index.took))
            .collect::<Vec<_>>(),
    );
    // The scenario ran every path it claims to.
    for (op, count) in &ops {
        assert!(*count > 0, "{context}: no {op:?} ran");
    }
    assert!(
        chains.iter().all(|count| *count > 0),
        "{context}: chains {chains:?}"
    );
    for index in &indexes {
        let expected = if index.family == dropped_family { 2 } else { 1 };
        assert_eq!(
            index.builds, expected,
            "{context}: {:?} builds",
            index.family
        );
    }
    assert!(
        Counters::get(&counters.strong_vector_checks) > 0,
        "{context}"
    );
    assert!(Counters::get(&counters.strong_text_checks) > 0, "{context}");
    assert!(Counters::get(&counters.eventual_checks) > 0, "{context}");
    assert!(Counters::get(&counters.graph_checks) > 0, "{context}");
    assert!(
        totals.published > 0 && totals.batches > 0,
        "{context}: {totals:?}"
    );
    assert!(
        totals.deferred > 0,
        "{context}: no write landed on a building generation: {totals:?}"
    );
    assert!(
        Counters::get(&counters.l0_compactions_while_pending) > 0,
        "{context}: no L0 compaction while queues held work"
    );
    assert!(
        Counters::get(&counters.last_run_compactions_while_pending) > 0,
        "{context}: no last-run compaction while queues held work"
    );
}

/// Prints warnings and errors, such as publication failures that only
/// retry, so a failing seed shows its cause.
struct Warnings;

impl tracing::Subscriber for Warnings {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() <= tracing::Level::WARN
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        struct Fields(String);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.push_str(&format!(" {}={value:?}", field.name()));
            }
        }
        let mut fields = Fields(String::new());
        event.record(&mut fields);
        eprintln!(
            "[{}] {}{}",
            event.metadata().level(),
            event.metadata().target(),
            fields.0
        );
    }

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "wall-clock soak; the nightly workflow runs it in release"]
async fn concurrent_writes_builds_and_publication_stay_exact_under_compaction() {
    let _ = tracing::subscriber::set_global_default(Warnings);
    let seeds = env_or("SOAK_SEEDS", 1);
    let first = env_or("SOAK_SEED", 0x5EED);
    let phase = Duration::from_secs(env_or("SOAK_PHASE_SECS", 8));
    for seed in first..first + seeds {
        soak(seed, phase).await;
    }
}

/// The deterministic core of what the soak finds: publication upserts a
/// re-embedded node by deleting it, which removes every locator naming it
/// directly, and inserting it again. Rows that named the node before and
/// relink to it again end unchanged against their cached originals, so their
/// flush restages no locator; a later delete of the node cannot find those
/// rows and leaves links to a node without an item.
#[tokio::test]
async fn a_vector_update_keeps_a_locator_for_every_link_it_restores() {
    let history = History::new();
    let db = open(
        "soak-update-locators",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector(&db, Some("tenant")).await;
    let mut ids = Vec::new();
    for index in 0..48_u16 {
        let embedding = [f32::from(index % 8), f32::from(index / 8)];
        ids.push(add(&db, embedding, "doc", Some("t")).await);
    }
    let target = target(&db, QueueFamily::Vector).await;
    drain(&db, target).await;
    assert_vector_graph(&db, &history, "built").await;
    // A small move keeps most of the node's neighbourhood.
    update(&db, ids[27], [3.25, 3.25], "doc").await;
    drain(&db, target).await;
    assert_vector_graph(&db, &history, "after the update").await;
    delete(&db, ids[27]).await;
    drain(&db, target).await;
    assert_vector_graph(&db, &history, "after the delete").await;
    for query in [[3.0, 3.0], [0.0, 0.0], [7.0, 5.0]] {
        vector_search(&db, query, 10, Some("t"), SearchConsistency::Strong).await;
    }
    db.close().await.unwrap();
}
