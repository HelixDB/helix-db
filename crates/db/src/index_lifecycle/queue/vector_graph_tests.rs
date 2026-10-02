//! Random queued vector write sequences keep every HNSW namespace valid.
//!
//! Each run seeds tenant-partitioned documents, builds a vector index over
//! them with random writes applied mid-scan, then applies rounds of random
//! inserts, near and far re-embeddings, replays, update chains, deletes,
//! removed and restored embeddings, and tenant moves, each round published by
//! a publisher with tiny batch limits that retains or forgets its planning
//! session at random. After the build and after every committed attempt,
//! every namespace of the Active generation must be a valid graph (see
//! [`Namespace::violations`]); after every drain, each partition must index
//! exactly its live documents, and strong and eventual searches over it must
//! return exactly them. Odd seeds plan under a planning-cache budget that
//! evicts, and output budgets small enough that full batches discard planned
//! entities, which the next attempt plans again through the retained
//! session. Both happen over a run, though not in every seed.
//!
//! `GRAPH_SEEDS` (default 4 per metric) and `GRAPH_SEED` (first seed) scale
//! the run.
//!
//! The direct runs apply the same invariants to every way a vector write is
//! planned below the publisher: the retained build session under budgets
//! that evict, the session with a discarded entity, and a one-off cache per
//! operation. Random upserts that move a node or change its layer, replays,
//! deletes, removals of absent nodes, deletes followed by a reinsertion in
//! one transaction, and moves between two namespaces are each planned in a
//! disposable transaction and applied to the committing one, as publication
//! applies an admitted entity. Odd seeds evict between entities, over a run
//! though not in every seed. `DIRECT_SEEDS` (default 24 per metric) and
//! `DIRECT_SEED` scale them.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU64, NonZeroUsize};

use helix_ast::{
    batch,
    graph::NodeRef,
    index::{IndexSpec, VectorDistanceMetric},
    query::{QueryRequest, SearchConsistency},
    traversal,
};
use slatedb::object_store::memory::InMemory;
use std::sync::Arc;

use slatedb::IsolationLevel;

use super::lifecycle_tests::{create, pause_when, scanned, set_tenant, wait_terminal};
use super::overlay_tests::{add, delete, update, vector_search, write};
use super::publication::{PublicationOutcome, QueuePublisher};
use super::soak_tests::{
    assert_vector_graph, env_or, partition, vector_namespaces, History, Namespace, Rng,
};
use super::tests::{open, publisher_with_limits, queue, queued, target};
use super::QueueTarget;
use crate::config::{
    DbConfig, IndexOperationQueueTuning, SearchIndexBackfillLimits, SearchIndexBatchLimits,
};
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::values::indexes::operation_queue::QueueFamily;
use crate::index_lifecycle::IndexElementKind;
use crate::search::vector::{
    self, Distance, MeasuredVectorTransaction, SearchParams, VectorBuildSession, VectorDimension,
    VectorIndex, VectorIndexConfig, VectorWriteRecorder,
};
use crate::HelixDB;

const TENANTS: [&str; 3] = ["ta", "tb", "tc"];
/// Documents seeded before the build, spread over every tenant.
const SEEDED: usize = 18;
const ROUNDS: usize = 8;
/// Larger than any partition, so a search lists every document it indexes.
const SEARCH_K: usize = 64;
/// Publication output-operation budgets: each fits any one entity beside its
/// acknowledgement, and most fill with a few.
const OUTPUT_OPERATIONS: [u64; 3] = [64, 128, 256];

/// One random write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Op {
    Insert,
    /// A small move that keeps most of the node's neighbourhood.
    Nudge,
    Jump,
    /// The node's own embedding again.
    Replay,
    /// Two embeddings in a row, of which the queue publishes the last.
    Chain,
    Delete,
    RemoveEmbedding,
    /// An embedding for a document without one: the node is inserted again.
    RestoreEmbedding,
    TenantMove,
}

const OPS: [Op; 9] = [
    Op::Insert,
    Op::Nudge,
    Op::Jump,
    Op::Replay,
    Op::Chain,
    Op::Delete,
    Op::RemoveEmbedding,
    Op::RestoreEmbedding,
    Op::TenantMove,
];

/// One live document: its tenant and its embedding, if it has one.
#[derive(Debug, Clone, Copy)]
struct Doc {
    tenant: &'static str,
    embedding: Option<[f32; 2]>,
}

/// The live documents a run has written, and what it applied.
struct Model {
    docs: BTreeMap<u64, Doc>,
    history: History,
    applied: BTreeMap<Op, u64>,
}

impl Rng {
    /// A point of a quarter grid away from the origin, so cosine accepts it;
    /// repeats and shared directions tie distances often.
    fn point(&mut self) -> [f32; 2] {
        [
            1.0 + self.below(16) as f32 * 0.25,
            1.0 + self.below(16) as f32 * 0.25,
        ]
    }

    fn pick<T: Copy>(&mut self, from: &[T]) -> T {
        from[self.below(from.len())]
    }
}

impl Model {
    /// IDs of documents that `keep` accepts.
    fn ids(&self, keep: impl Fn(&Doc) -> bool) -> Vec<u64> {
        self.docs
            .iter()
            .filter(|(_, doc)| keep(doc))
            .map(|(id, _)| *id)
            .collect()
    }

    /// Embedded documents of `tenant`.
    fn indexed(&self, tenant: &str) -> BTreeSet<u64> {
        self.docs
            .iter()
            .filter(|(_, doc)| doc.tenant == tenant && doc.embedding.is_some())
            .map(|(id, _)| *id)
            .collect()
    }

    async fn insert(&mut self, db: &HelixDB, rng: &mut Rng) {
        let (tenant, embedding) = (rng.pick(&TENANTS), rng.point());
        let id = add(db, embedding, "doc", Some(tenant)).await;
        self.docs.insert(
            id,
            Doc {
                tenant,
                embedding: Some(embedding),
            },
        );
        self.history
            .record(id, format!("insert {embedding:?} in {tenant}"));
        *self.applied.entry(Op::Insert).or_default() += 1;
    }

    async fn embed(&mut self, db: &HelixDB, id: u64, embedding: [f32; 2], op: Op) {
        update(db, id, embedding, "doc").await;
        self.docs.get_mut(&id).unwrap().embedding = Some(embedding);
        self.history.record(id, format!("{op:?} to {embedding:?}"));
    }

    /// Applies one random write, or an insert when no document fits it.
    async fn apply(&mut self, db: &HelixDB, rng: &mut Rng) {
        let op = rng.pick(&OPS);
        let embedded = self.ids(|doc| doc.embedding.is_some());
        let bare = self.ids(|doc| doc.embedding.is_none());
        let candidates = match op {
            Op::Insert => Vec::new(),
            Op::Nudge | Op::Jump | Op::Replay | Op::Chain | Op::RemoveEmbedding => embedded,
            Op::RestoreEmbedding => bare,
            Op::Delete | Op::TenantMove => self.ids(|_| true),
        };
        if candidates.is_empty() {
            return self.insert(db, rng).await;
        }
        let id = rng.pick(&candidates);
        let doc = self.docs[&id];
        match op {
            Op::Insert => unreachable!("inserts pick no document"),
            Op::Nudge => {
                let [x, y] = doc.embedding.unwrap();
                let step = if rng.below(2) == 0 { 0.25 } else { -0.25 };
                let nudged = if rng.below(2) == 0 {
                    [(x + step).max(1.0), y]
                } else {
                    [x, (y + step).max(1.0)]
                };
                self.embed(db, id, nudged, op).await;
            }
            Op::Jump => self.embed(db, id, rng.point(), op).await,
            Op::Replay => self.embed(db, id, doc.embedding.unwrap(), op).await,
            Op::Chain => {
                let first = rng.point();
                update(db, id, first, "doc").await;
                self.history.record(id, format!("chain through {first:?}"));
                self.embed(db, id, rng.point(), op).await;
            }
            Op::Delete => {
                delete(db, id).await;
                self.docs.remove(&id);
                self.history.record(id, "delete".to_string());
            }
            Op::RemoveEmbedding => {
                write(db, || {
                    QueryRequest::write(
                        batch::write_batch().var_as(
                            "removed",
                            traversal::g()
                                .n(NodeRef::from(id))
                                .remove_property("embedding"),
                        ),
                    )
                })
                .await;
                self.docs.get_mut(&id).unwrap().embedding = None;
                self.history.record(id, "remove embedding".to_string());
            }
            Op::RestoreEmbedding => self.embed(db, id, rng.point(), op).await,
            Op::TenantMove => {
                let others = TENANTS
                    .into_iter()
                    .filter(|tenant| *tenant != doc.tenant)
                    .collect::<Vec<_>>();
                let tenant = rng.pick(&others);
                set_tenant(db, id, tenant).await;
                self.docs.get_mut(&id).unwrap().tenant = tenant;
                self.history.record(id, format!("move to {tenant}"));
            }
        }
        *self.applied.entry(op).or_default() += 1;
    }
}

/// Queued mode, paused publication, four-entity build steps, and, when
/// `evicting`, a planning-cache budget of a few dozen rows.
fn config(evicting: bool) -> DbConfig {
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
    let limits = if evicting {
        limits.with_vector_build_cache_bytes(NonZeroU64::new(24 * 1024).unwrap())
    } else {
        limits
    };
    queued(IndexOperationQueueTuning::default()).with_search_index_backfill_limits(limits)
}

/// A publisher planning one to three entities per attempt under an output
/// budget that one or two upserts fill, so full batches discard entities.
fn tiny_publisher(db: &HelixDB, rng: &mut Rng) -> Arc<QueuePublisher> {
    let limits = SearchIndexBatchLimits::try_new(
        NonZeroUsize::new(1 + rng.below(3)).unwrap(),
        NonZeroU64::new(8 * 1024 * 1024).unwrap(),
        NonZeroU64::new(rng.pick(&OUTPUT_OPERATIONS)).unwrap(),
        NonZeroU64::new(8 * 1024 * 1024).unwrap(),
        NonZeroU64::new(8 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    publisher_with_limits(
        db,
        limits,
        DbConfig::new()
            .search_index_backfill()
            .active_text_mutation(),
    )
}

/// Publishes `target` until its queue is empty, forgetting the retained
/// session after a random quarter of attempts, and checks every namespace
/// after each commit. Returns the commits.
async fn publish(
    db: &HelixDB,
    publisher: &QueuePublisher,
    target: QueueTarget,
    rng: &mut Rng,
    model: &Model,
    context: &str,
) -> u64 {
    let mut commits = 0;
    for _ in 0..10_000 {
        let outcome = publisher.publish_once(target).await.unwrap();
        if rng.below(4) == 0 {
            publisher.planning_cache().forget_publication(target).await;
        }
        match outcome {
            PublicationOutcome::Trimmed => {}
            PublicationOutcome::Published { .. } => {
                commits += 1;
                assert_vector_graph(db, &model.history, &format!("{context}, commit {commits}"))
                    .await;
            }
            PublicationOutcome::Empty => return commits,
            outcome @ (PublicationOutcome::Discarded { .. }
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry
            | PublicationOutcome::Blocked) => {
                panic!("{context}: publication did not progress: {outcome:?}")
            }
        }
    }
    panic!("{context}: publication did not drain")
}

/// Asserts each partition indexes exactly its tenant's embedded documents,
/// and strong and eventual searches over it return exactly them.
async fn assert_published(db: &HelixDB, model: &Model, rng: &mut Rng, context: &str) {
    assert!(queue(db, QueueFamily::Vector).await.is_none(), "{context}");
    let snapshot = db.inner_db().snapshot().await.unwrap();
    let namespaces = vector_namespaces(db, snapshot.as_ref())
        .await
        .expect("the vector index is Active");
    for tenant in TENANTS {
        let expected = model.indexed(tenant);
        let items = namespaces
            .get(&partition(tenant))
            .map(|(_, namespace)| namespace.items().clone())
            .unwrap_or_default();
        assert_eq!(items, expected, "{context}: items of tenant {tenant}");
        for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
            let query = rng.point();
            let found = vector_search(db, query, SEARCH_K, Some(tenant), consistency).await;
            let ids = found.iter().map(|(id, _)| *id).collect::<BTreeSet<_>>();
            assert_eq!(ids.len(), found.len(), "{context}: distinct hits {found:?}");
            assert_eq!(
                ids, expected,
                "{context}: {consistency:?} search of tenant {tenant} at {query:?}"
            );
        }
    }
}

/// Builds the index over seeded documents with writes mid-scan, then
/// publishes random rounds, checking the graph throughout. Returns the
/// commits and the entries planning evicted.
async fn run(
    metric: VectorDistanceMetric,
    seed: u64,
    applied: &mut BTreeMap<Op, u64>,
) -> (u64, u64) {
    let mut rng = Rng(seed);
    let evicting = seed % 2 == 1;
    let name = format!("vector-graph-{metric:?}-{seed}");
    let db = open(&name, Arc::new(InMemory::new()), config(evicting)).await;
    let mut model = Model {
        docs: BTreeMap::new(),
        history: History::new(),
        applied: BTreeMap::new(),
    };
    for _ in 0..SEEDED {
        model.insert(&db, &mut rng).await;
    }

    let spec = IndexSpec::node_vector(
        "Doc",
        "embedding",
        NonZeroUsize::new(2).unwrap(),
        metric,
        Some("tenant"),
    );
    let operation = create(&db, spec).await;
    let target = target(&db, QueueFamily::Vector).await;
    let paused = pause_when(&db, &operation, target, |status| scanned(status) >= 6).await;
    for _ in 0..6 {
        model.apply(&db, &mut rng).await;
    }
    drop(paused);
    assert_eq!(wait_terminal(&db, &operation).await, "succeeded", "{name}");
    assert_vector_graph(&db, &model.history, &format!("{name}: built")).await;

    let publisher = tiny_publisher(&db, &mut rng);
    let mut commits = 0;
    for round in 0..ROUNDS {
        let context = format!("{name}, round {round}");
        for _ in 0..4 + rng.below(9) {
            model.apply(&db, &mut rng).await;
        }
        commits += publish(&db, &publisher, target, &mut rng, &model, &context).await;
        assert_published(&db, &model, &mut rng, &context).await;
    }
    for (op, count) in &model.applied {
        *applied.entry(*op).or_default() += count;
    }
    let evictions = publisher.planning_cache().publication_evictions();
    db.close().await.unwrap();
    (commits, evictions)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn random_queued_vector_writes_keep_every_graph_valid() {
    let seeds = env_or("GRAPH_SEEDS", 4);
    let first = env_or("GRAPH_SEED", 0x6EA9);
    let mut applied = BTreeMap::new();
    let (mut commits, mut evictions) = (0, 0);
    // Publication plans in this task, so the scope counts its discards and
    // not those of builds, which run in their own.
    let discards = vector::DISCARDED_ENTITIES
        .scope(Cell::new(0), async {
            for metric in [
                VectorDistanceMetric::Euclidean,
                VectorDistanceMetric::Cosine,
            ] {
                for seed in first..first + seeds {
                    let (run_commits, run_evictions) = run(metric, seed, &mut applied).await;
                    commits += run_commits;
                    if !seed.is_multiple_of(2) {
                        evictions += run_evictions;
                    }
                }
            }
            vector::DISCARDED_ENTITIES.with(Cell::get)
        })
        .await;
    assert!(
        OPS.iter()
            .all(|op| applied.get(op).is_some_and(|count| *count > 0)),
        "every write kind ran: {applied:?}"
    );
    assert!(
        commits > 2 * seeds * ROUNDS as u64,
        "rounds took several attempts: {commits} commits"
    );
    assert!(
        (first..first + seeds).all(|seed| seed.is_multiple_of(2)) || evictions > 0,
        "the small budget evicted while planning"
    );
    assert!(discards > 0, "full batches discarded planned entities");
}

/// Physical namespaces of a direct run, standing in for two partitions.
const DIRECT_PHYSICAL: [u64; 2] = [91, 92];
/// Node IDs a direct run draws from.
const DIRECT_NODES: usize = 20;
const DIRECT_ATTEMPTS: usize = 40;

/// How one direct attempt plans its entities.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Planner {
    /// The retained session, as publication plans an admitted batch.
    Session,
    /// The retained session, discarding the attempt's last entity as a full
    /// batch does.
    Discarding,
    /// A cache per operation, as direct index writes plan. It is another
    /// writer to the session, which is forgotten afterwards.
    OneOff,
}

/// One direct entity change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Change {
    /// A new vector at a random layer, inserting the node when absent.
    Upsert,
    /// [`Change::Upsert`] after removing the node from the namespace it is
    /// absent from, as publication clears stale partitions.
    UpsertAfterStaleRemoval,
    /// The node's own vector at its own layer, which stages nothing.
    Replay,
    Delete,
    /// The node deleted and inserted again in one transaction.
    Reinsert,
    /// The node deleted from its namespace and inserted into the other.
    Move,
}

const CHANGES: [Change; 6] = [
    Change::Upsert,
    Change::UpsertAfterStaleRemoval,
    Change::Replay,
    Change::Delete,
    Change::Reinsert,
    Change::Move,
];

/// A node's namespace, vector, and layer.
type Placement = (usize, [f32; 2], u16);

/// One direct run's namespaces and planning state.
struct Direct<D: Distance> {
    indexes: [VectorIndex<D>; 2],
    session: VectorBuildSession<D>,
    budget: NonZeroU64,
}

impl<D: Distance> Direct<D> {
    /// Upserts `node` into namespace `slot`, through the session unless the
    /// planner is one-off.
    async fn upsert(
        &mut self,
        write: &MeasuredVectorTransaction<'_>,
        planner: Planner,
        slot: usize,
        node: u64,
        (vector, layer): ([f32; 2], u16),
    ) {
        let index = &self.indexes[slot];
        match planner {
            Planner::OneOff => {
                index
                    .stage_upsert_at_layer(write, node, &vector, layer)
                    .await
            }
            Planner::Session | Planner::Discarding => {
                index
                    .stage_upsert_at_layer_with_session(
                        write,
                        node,
                        &vector,
                        layer,
                        &mut self.session,
                    )
                    .await
            }
        }
        .unwrap();
    }

    /// Deletes `node` from namespace `slot`, through the session unless the
    /// planner is one-off.
    async fn remove(
        &mut self,
        write: &MeasuredVectorTransaction<'_>,
        planner: Planner,
        slot: usize,
        node: u64,
    ) {
        let index = &self.indexes[slot];
        match planner {
            Planner::OneOff => index.stage_delete(write, node).await,
            Planner::Session | Planner::Discarding => {
                index
                    .stage_delete_with_build_session(write, node, &mut self.session)
                    .await
            }
        }
        .unwrap();
    }

    /// Plans `change` of `node`, returning its placement afterwards.
    async fn plan(
        &mut self,
        write: &MeasuredVectorTransaction<'_>,
        planner: Planner,
        rng: &mut Rng,
        change: Change,
        node: u64,
        placed: Option<Placement>,
    ) -> Option<Placement> {
        let fresh = (rng.point(), [0_u16, 0, 0, 0, 1, 1, 2][rng.below(7)]);
        let Some((slot, vector, layer)) = placed else {
            let slot = rng.below(2);
            if change == Change::UpsertAfterStaleRemoval {
                self.remove(write, planner, 1 - slot, node).await;
            }
            self.upsert(write, planner, slot, node, fresh).await;
            return Some((slot, fresh.0, fresh.1));
        };
        match change {
            Change::Upsert | Change::UpsertAfterStaleRemoval => {
                if change == Change::UpsertAfterStaleRemoval {
                    self.remove(write, planner, 1 - slot, node).await;
                }
                self.upsert(write, planner, slot, node, fresh).await;
                Some((slot, fresh.0, fresh.1))
            }
            Change::Replay => {
                self.upsert(write, planner, slot, node, (vector, layer))
                    .await;
                placed
            }
            Change::Delete => {
                self.remove(write, planner, slot, node).await;
                None
            }
            Change::Reinsert => {
                self.remove(write, planner, slot, node).await;
                self.upsert(write, planner, slot, node, fresh).await;
                Some((slot, fresh.0, fresh.1))
            }
            Change::Move => {
                self.remove(write, planner, slot, node).await;
                self.upsert(write, planner, 1 - slot, node, fresh).await;
                Some((1 - slot, fresh.0, fresh.1))
            }
        }
    }
}

/// Applies random attempts to two namespaces below the publisher, checking
/// both after every commit. Returns the rows sessions evicted.
async fn direct_run<D: Distance>(seed: u64, applied: &mut BTreeMap<String, u64>) -> u64 {
    let mut rng = Rng(seed);
    let name = format!("direct-graph-{}-{seed}", D::name());
    let db = slatedb::Db::open(name.as_str(), Arc::new(InMemory::new()))
        .await
        .unwrap();
    let indexes = DIRECT_PHYSICAL.map(|physical| {
        let identity = vector::VectorGenerationIdentity::try_new(
            DataScope::LegacyUnscoped,
            7,
            format!("{name}-{physical}"),
            physical,
            NonZeroU64::new(3).unwrap(),
            11,
            IndexElementKind::Node,
            VectorDimension::try_new(2).unwrap(),
        )
        .unwrap();
        VectorIndex::<D>::from_generation(
            &vector::ValidatedVectorGenerationHandle::create_current::<D>(identity).unwrap(),
        )
    });
    let create = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&create);
    for index in &indexes {
        index
            .stage_create(
                &measured,
                VectorIndexConfig::new(index.name(), "embedding", 2)
                    .with_m(2)
                    .with_m0(4)
                    .with_ef_construction(8),
            )
            .await
            .unwrap();
    }
    create.commit().await.unwrap();
    // Even seeds keep every row; odd ones evict between entities.
    let budget = if seed.is_multiple_of(2) {
        1 << 20
    } else {
        12 * 1024
    };
    let budget = NonZeroU64::new(budget).unwrap();
    let mut direct = Direct {
        indexes,
        session: VectorBuildSession::new(budget),
        budget,
    };
    let mut evictions = 0;
    let history = History::new();
    let mut placed = BTreeMap::<u64, Placement>::new();
    for attempt in 0..DIRECT_ATTEMPTS {
        let planner = match rng.below(6) {
            0 => Planner::OneOff,
            1 | 2 => Planner::Discarding,
            _ => Planner::Session,
        };
        let planning = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let commit = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let recorder = VectorWriteRecorder::new();
        let write = recorder.bind(&planning);
        let entities = 1 + rng.below(3);
        for entity in 0..entities {
            let node = rng.below(DIRECT_NODES) as u64;
            let change = CHANGES[rng.below(CHANGES.len())];
            let checkpoint = write.checkpoint();
            let next = direct
                .plan(
                    &write,
                    planner,
                    &mut rng,
                    change,
                    node,
                    placed.get(&node).copied(),
                )
                .await;
            if planner != Planner::OneOff {
                direct.session.flush_all(&write).unwrap();
                direct.session.enforce_limits(&write).unwrap();
            }
            if planner == Planner::Discarding && entity + 1 == entities {
                direct.session.discard_entity();
                *applied.entry("discarded".to_string()).or_default() += 1;
                break;
            }
            write
                .plan_since(checkpoint)
                .unwrap()
                .apply_to(&commit)
                .unwrap();
            if planner != Planner::OneOff {
                direct.session.admit_entity();
            }
            let kind = match placed.get(&node) {
                Some(_) => format!("{change:?} by {planner:?}"),
                None => format!("insert by {planner:?}"),
            };
            history.record(node, format!("{kind} to {next:?}"));
            *applied.entry(kind).or_default() += 1;
            match next {
                Some(placement) => placed.insert(node, placement),
                None => placed.remove(&node),
            };
        }
        commit.commit().await.unwrap();
        if planner == Planner::OneOff || rng.below(4) == 0 {
            evictions += direct.session.stats().neighbor_evictions();
            direct.session = VectorBuildSession::new(direct.budget);
        }

        let context = format!("{name}, attempt {attempt}");
        let snapshot = db.snapshot().await.unwrap();
        for (slot, index) in direct.indexes.iter().enumerate() {
            let namespace = Namespace::read(snapshot.as_ref(), DIRECT_PHYSICAL[slot]).await;
            let violations = namespace.violations(&history);
            assert!(
                violations.is_empty(),
                "{context}: namespace {slot} is inconsistent: {violations:#?}"
            );
            let expected = placed
                .iter()
                .filter(|(_, (placed, ..))| *placed == slot)
                .map(|(node, _)| *node)
                .collect::<BTreeSet<_>>();
            assert_eq!(namespace.items(), &expected, "{context}: namespace {slot}");
            let query = rng.point();
            let found = index
                .search(
                    snapshot.as_ref(),
                    &query,
                    &SearchParams::new(DIRECT_NODES).unwrap(),
                )
                .await
                .unwrap_or_else(|error| {
                    panic!("{context}: search of namespace {slot} at {query:?}: {error}")
                });
            let ids = found
                .iter()
                .map(|result| result.entity_id())
                .collect::<BTreeSet<_>>();
            assert_eq!(ids.len(), found.len(), "{context}: distinct hits");
            assert!(
                ids.is_subset(&expected),
                "{context}: {ids:?} of {expected:?}"
            );
        }
    }
    evictions += direct.session.stats().neighbor_evictions();
    assert!(
        evictions == 0 || budget.get() < 1 << 20,
        "{name}: only the small budget evicts ({evictions} evictions)"
    );
    evictions
}

#[tokio::test]
async fn random_direct_vector_writes_keep_one_locator_per_link() {
    let seeds = env_or("DIRECT_SEEDS", 24);
    let first = env_or("DIRECT_SEED", 0xD1EC);
    let mut applied = BTreeMap::new();
    let mut evictions = 0;
    for seed in first..first + seeds {
        evictions += direct_run::<vector::distance::Euclidean>(seed, &mut applied).await;
        evictions += direct_run::<vector::distance::Cosine>(seed, &mut applied).await;
    }
    assert!(
        (first..first + seeds).all(|seed| seed.is_multiple_of(2)) || evictions > 0,
        "the small budget evicted between entities"
    );
    let missing = CHANGES
        .iter()
        .flat_map(|change| {
            [Planner::Session, Planner::Discarding, Planner::OneOff]
                .map(|planner| format!("{change:?} by {planner:?}"))
        })
        .chain(["discarded".to_string()])
        .filter(|kind| !applied.contains_key(kind))
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "every change ran: {missing:?} in {applied:?}"
    );
}
