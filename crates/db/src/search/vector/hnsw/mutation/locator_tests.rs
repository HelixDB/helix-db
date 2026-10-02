//! Deletes and upserts leave exactly one reverse locator per link, whichever
//! cache plans them.
//!
//! [`stray_locators_never_survive_a_delete_or_upsert`] commits locators
//! naming the node about to change that no row backs: from a source that does
//! not link it, from a source without an item, and on a layer above every
//! row. No cached baseline owns them, so the delete must remove them as
//! residue, through a retained session and through one-off caches.
//!
//! [`one_off_caches_at_their_bound_keep_one_locator_per_link`] fills each
//! one-off cache to its bound before the operation, so every further load
//! flushes and evicts the oldest dirty row mid-operation.
//!
//! [`links_released_versions_left_without_locators_outlive_their_target`]
//! starts from links committed without a locator, as released versions
//! write them, and pins that no operation here repairs them.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use slatedb::object_store::memory::InMemory;
use slatedb::{DbReadOps, IsolationLevel};

use super::*;
use crate::encoding::v2::keys::indexes::vector::VectorKey;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::values::indexes::vector::decode_layer0_neighbors;
use crate::encoding::v2::values::indexes::vector::neighbors::decode_upper_neighbors;
use crate::index_lifecycle::IndexElementKind;
use crate::search::vector::distance::{Cosine, Euclidean};
use crate::search::vector::{SearchParams, ValidatedVectorGenerationHandle, VectorIndexConfig};

/// A node ID no vector takes.
const GHOST: NodeId = 10_000;
/// A layer above every row: [`random_layer`] picks at most 2.
const ABOVE: u16 = 5;

/// `(layer, source, target)` of one neighbor-row entry or reverse locator.
type Link = (u16, NodeId, NodeId);

/// Every item, neighbor row, link, and locator of one namespace.
#[derive(Default)]
struct Graph {
    items: BTreeSet<NodeId>,
    /// `(layer, node)` of every neighbor row.
    rows: BTreeSet<(u16, NodeId)>,
    /// Every neighbor-row entry.
    links: BTreeSet<Link>,
    /// Every reverse locator.
    locators: BTreeSet<Link>,
}

impl Graph {
    async fn read<D: Distance>(
        index: &VectorIndex<D>,
        read: &(impl DbReadOps + Send + Sync),
    ) -> Self {
        let mut graph = Self::default();
        let mut rows = read.scan(..).await.unwrap();
        while let Some(row) = rows.next().await.unwrap() {
            let Ok(logical) = index.row_keyspace().strip_physical_key(&row.key) else {
                continue;
            };
            let Ok(key) = VectorKey::parse_from_slice(logical) else {
                continue;
            };
            let (layer, node, neighbors) = match key {
                VectorKey::Vector(key) => {
                    graph.items.insert(key.node_id());
                    continue;
                }
                VectorKey::ReverseEdge(key) => {
                    graph.locators.insert((
                        key.layer(),
                        key.source_node_id(),
                        key.target_node_id(),
                    ));
                    continue;
                }
                VectorKey::Layer0Neighbors(key) => (
                    0,
                    key.node_id(),
                    decode_layer0_neighbors(row.value.as_ref()).unwrap(),
                ),
                VectorKey::UpperNeighbors(key) => (
                    key.layer(),
                    key.node_id(),
                    decode_upper_neighbors(row.value.as_ref()).unwrap(),
                ),
                VectorKey::IndexMetadata(_)
                | VectorKey::IndexPrefix(_)
                | VectorKey::TxnGuard(_)
                | VectorKey::VectorPrefix(_)
                | VectorKey::SimHashDirectoryPrefix(_)
                | VectorKey::SimHashDirectory(_)
                | VectorKey::EntryCandidatePrefix(_)
                | VectorKey::EntryCandidateSorted(_)
                | VectorKey::EntryCandidateNode(_)
                | VectorKey::MemoryPrefix(_)
                | VectorKey::L0Prefix(_)
                | VectorKey::SimHash(_)
                | VectorKey::UpperVector(_)
                | VectorKey::ReverseEdgePrefix(_) => continue,
            };
            graph.rows.insert((layer, node));
            graph
                .links
                .extend(neighbors.into_iter().map(|target| (layer, node, target)));
        }
        graph
    }

    /// Asserts one locator per link and none other, links and rows only of
    /// nodes with an item, and items of exactly `live`.
    fn assert_valid(&self, live: &BTreeSet<NodeId>, context: &str) {
        self.assert_faults(live, &BTreeSet::new(), &BTreeSet::new(), context);
    }

    /// Asserts [`Self::assert_valid`] except that exactly `unlocated` links
    /// have no locator and exactly `dangling` links name a node without an
    /// item.
    fn assert_faults(
        &self,
        live: &BTreeSet<NodeId>,
        unlocated: &BTreeSet<Link>,
        dangling: &BTreeSet<Link>,
        context: &str,
    ) {
        let actual_unlocated = self
            .links
            .difference(&self.locators)
            .copied()
            .collect::<BTreeSet<_>>();
        let stray = self.locators.difference(&self.links).collect::<Vec<_>>();
        let actual_dangling = self
            .links
            .iter()
            .filter(|(_, _, target)| !self.items.contains(target))
            .copied()
            .collect::<BTreeSet<_>>();
        let orphaned = self
            .rows
            .iter()
            .filter(|(_, node)| !self.items.contains(node))
            .collect::<Vec<_>>();
        assert!(
            actual_unlocated == *unlocated
                && stray.is_empty()
                && actual_dangling == *dangling
                && orphaned.is_empty(),
            "{context}: links without a locator {actual_unlocated:?} (expected {unlocated:?}), \
             locators without a link {stray:?}, links to itemless nodes {actual_dangling:?} \
             (expected {dangling:?}), rows of itemless nodes {orphaned:?}"
        );
        assert_eq!(&self.items, live, "{context}: items");
    }
}

/// A point of a quarter grid away from the origin, so cosine accepts it.
fn random_point(rng: &mut StdRng) -> [f32; 2] {
    [
        1.0 + rng.random_range(0..64_u32) as f32 * 0.25,
        1.0 + rng.random_range(0..64_u32) as f32 * 0.25,
    ]
}

/// A layer of at most 2, mostly 0.
fn random_layer(rng: &mut StdRng) -> u16 {
    [0, 0, 0, 0, 0, 1, 1, 2][rng.random_range(0..8_usize)]
}

/// Opens a database holding one empty generation namespace of small degree.
async fn create<D: Distance>(name: &str) -> (slatedb::Db, VectorIndex<D>) {
    let db = slatedb::Db::open(name, Arc::new(InMemory::new()))
        .await
        .unwrap();
    let identity = VectorGenerationIdentity::try_new(
        DataScope::LegacyUnscoped,
        7,
        name.to_string(),
        91,
        NonZeroU64::new(3).unwrap(),
        11,
        IndexElementKind::Node,
        VectorDimension::try_new(2).unwrap(),
    )
    .unwrap();
    let index = VectorIndex::<D>::from_generation(
        &ValidatedVectorGenerationHandle::create_current::<D>(identity).unwrap(),
    );
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    index
        .stage_create(
            &MeasuredVectorTransaction::new(&txn),
            VectorIndexConfig::new(index.name(), "embedding", 2)
                .with_m(2)
                .with_m0(4)
                .with_ef_construction(16),
        )
        .await
        .unwrap();
    txn.commit().await.unwrap();
    (db, index)
}

/// Inserts nodes `1..=nodes` at random points and layers, returning their
/// points.
async fn build<D: Distance>(
    db: &slatedb::Db,
    index: &VectorIndex<D>,
    rng: &mut StdRng,
    nodes: NodeId,
) -> BTreeMap<NodeId, [f32; 2]> {
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let mut session = VectorBuildSession::<D>::new(NonZeroU64::new(1 << 34).unwrap());
    let mut points = BTreeMap::new();
    for node in 1..=nodes {
        let point = random_point(rng);
        index
            .stage_upsert_at_layer_with_session(
                &measured,
                node,
                &point,
                random_layer(rng),
                &mut session,
            )
            .await
            .unwrap();
        session.flush_all(&measured).unwrap();
        session.admit_entity();
        points.insert(node, point);
    }
    txn.commit().await.unwrap();
    points
}

/// Asserts the committed namespace is valid and holds exactly `points`, and
/// that a search over it succeeds.
async fn check<D: Distance>(
    db: &slatedb::Db,
    index: &VectorIndex<D>,
    points: &BTreeMap<NodeId, [f32; 2]>,
    rng: &mut StdRng,
    context: &str,
) {
    let snapshot = db.snapshot().await.unwrap();
    Graph::read(index, snapshot.as_ref())
        .await
        .assert_valid(&points.keys().copied().collect(), context);
    index
        .search(
            snapshot.as_ref(),
            &random_point(rng),
            &SearchParams::new(20).unwrap(),
        )
        .await
        .unwrap_or_else(|error| panic!("{context}: search failed: {error}"));
}

/// How one stray-locator run plans each operation.
#[derive(Debug, Clone, Copy)]
enum Planner {
    /// One build session retained across every operation, as publication
    /// retains one between commits.
    Session,
    /// A cache per operation, as direct index writes plan.
    OneOff,
}

/// Before each operation on a live node, commits stray locators naming it,
/// then deletes it or moves it to a new point through `planner`. A deleted
/// node an operation picks is inserted again instead.
async fn stray_run<D: Distance>(seed: u64, planner: Planner) {
    const NODES: NodeId = 160;
    const OPS: usize = 60;
    let mut rng = StdRng::seed_from_u64(seed);
    let name = format!("stray-locators-{}-{planner:?}-{seed}", D::name());
    let (db, index) = create::<D>(&name).await;
    let mut points = build(&db, &index, &mut rng, NODES).await;
    check(&db, &index, &points, &mut rng, &format!("{name}: built")).await;
    let mut session = VectorBuildSession::<D>::new(NonZeroU64::new(1 << 20).unwrap());
    for op in 0..OPS {
        let node = rng.random_range(1..=NODES);
        let current = points.get(&node).copied();
        let strays = match current {
            Some(_) => {
                let snapshot = db.snapshot().await.unwrap();
                let graph = Graph::read(&index, snapshot.as_ref()).await;
                let others = points
                    .keys()
                    .copied()
                    .filter(|other| *other != node)
                    .collect::<Vec<_>>();
                let unlinked = loop {
                    let source = others[rng.random_range(0..others.len())];
                    let layer = rng.random_range(0..=2_u16);
                    if !graph.links.contains(&(layer, source, node)) {
                        break (layer, source);
                    }
                };
                let strays = vec![unlinked, (0, GHOST), (ABOVE, others[0])];
                let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
                let measured = MeasuredVectorTransaction::new(&txn);
                let rows = VectorWriteRows::new(&measured, index.row_keyspace());
                for (layer, source) in &strays {
                    rows.put_reverse_locator(node, *layer, *source).unwrap();
                }
                txn.commit().await.unwrap();
                strays
            }
            None => Vec::new(),
        };
        let delete = current.is_some() && rng.random_range(0..3_u8) == 0;
        let point = loop {
            let point = random_point(&mut rng);
            if Some(point) != current {
                break point;
            }
        };
        let layer = random_layer(&mut rng);
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&txn);
        match (planner, delete) {
            (Planner::Session, true) => index
                .stage_delete_with_build_session(&measured, node, &mut session)
                .await
                .unwrap(),
            (Planner::Session, false) => index
                .stage_upsert_at_layer_with_session(&measured, node, &point, layer, &mut session)
                .await
                .unwrap(),
            (Planner::OneOff, true) => index.stage_delete(&measured, node).await.unwrap(),
            (Planner::OneOff, false) => index
                .stage_upsert_at_layer(&measured, node, &point, layer)
                .await
                .unwrap(),
        }
        if matches!(planner, Planner::Session) {
            session.flush_all(&measured).unwrap();
            session.enforce_limits(&measured).unwrap();
            session.admit_entity();
        }
        txn.commit().await.unwrap();
        let change = if delete {
            points.remove(&node);
            format!("delete {node}")
        } else {
            points.insert(node, point);
            format!("upsert {node} to {point:?} at layer {layer}")
        };
        check(
            &db,
            &index,
            &points,
            &mut rng,
            &format!("{name}: op {op} ({change}, strays {strays:?})"),
        )
        .await;
    }
}

#[tokio::test]
async fn stray_locators_never_survive_a_delete_or_upsert() {
    for seed in 0..2 {
        for planner in [Planner::Session, Planner::OneOff] {
            stray_run::<Euclidean>(seed, planner).await;
            stray_run::<Cosine>(seed, planner).await;
        }
    }
}

/// Plans each operation in a one-off cache first filled to its bound with
/// other nodes' rows, so every further load flushes and evicts the oldest
/// dirty row. Returns how many neighbor rows upserts and deletes staged
/// before their final flush, which only those evicting flushes write.
async fn bounded_one_off_run<D: Distance>(seed: u64) -> [usize; 2] {
    const NODES: NodeId = VECTOR_BUILD_NEIGHBOR_CACHE_LIMIT as NodeId + 64;
    const OPS: usize = 30;
    let mut rng = StdRng::seed_from_u64(seed);
    let name = format!("bounded-one-off-{}-{seed}", D::name());
    let (db, index) = create::<D>(&name).await;
    let mut points = build(&db, &index, &mut rng, NODES).await;
    let mut deleted = BTreeSet::new();
    let mut evicted = [0, 0];
    for op in 0..OPS {
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&txn);
        let mut metadata = index.get_metadata(&measured).await.unwrap().unwrap();
        let degree_limits = MutationDegreeLimits::try_from_metadata(&metadata).unwrap();
        let mut cache = MutationOpCache::<D>::with_degree_limits(
            degree_limits.layer0.get(),
            degree_limits.upper.get(),
        )
        .unwrap();
        let live = points.keys().copied().collect::<Vec<_>>();
        let choice = rng.random_range(0..4_u8);
        let node = match deleted.first() {
            Some(&dead) if choice == 3 => dead,
            _ => live[rng.random_range(0..live.len())],
        };
        let start = rng.random_range(0..live.len());
        for other in live
            .iter()
            .cycle()
            .skip(start)
            .filter(|other| **other != node)
            .take(VECTOR_BUILD_NEIGHBOR_CACHE_LIMIT)
        {
            index
                .load_neighbors_for_mutation(&measured, 0, *other, &mut cache)
                .await
                .unwrap();
        }
        assert_eq!(cache.neighbor_count(), VECTOR_BUILD_NEIGHBOR_CACHE_LIMIT);
        let checkpoint = measured.checkpoint();
        let change = if choice == 2 {
            index
                .stage_delete_with_metadata(&measured, node, &mut metadata, &mut cache)
                .await
                .unwrap();
            points.remove(&node);
            deleted.insert(node);
            format!("delete {node}")
        } else {
            // A nudge keeps most of the neighbourhood; a reinsertion or a
            // random point replaces it.
            let point = match points.get(&node) {
                Some([x, y]) if choice == 1 => [x + 0.25, *y],
                _ => random_point(&mut rng),
            };
            let semantics = ActiveVectorSemantics::for_distance::<D>().unwrap();
            let vector = ValidatedMetricVector::try_new(
                UnalignedVector::<D::VectorCodec>::from_slice(&point),
                semantics.distance_metric(),
                VectorDimension::try_new(2).unwrap(),
            )
            .unwrap();
            let layer = random_layer(&mut rng);
            index
                .insert_with_mutation_cache(
                    &measured,
                    node,
                    &vector,
                    VectorInsertContract::Upsert,
                    Some(layer),
                    &mut metadata,
                    &mut cache,
                    false,
                )
                .await
                .unwrap();
            points.insert(node, point);
            deleted.remove(&node);
            format!("upsert {node} to {point:?} at layer {layer}")
        };
        evicted[usize::from(choice == 2)] += measured
            .plan_since(checkpoint)
            .unwrap()
            .keys()
            .filter(|key| {
                index
                    .row_keyspace()
                    .strip_physical_key(key)
                    .ok()
                    .and_then(|logical| VectorKey::parse_from_slice(logical).ok())
                    .is_some_and(|key| {
                        matches!(
                            key,
                            VectorKey::Layer0Neighbors(_) | VectorKey::UpperNeighbors(_)
                        )
                    })
            })
            .count();
        index
            .flush_mutation_cache(&measured, &mut cache)
            .await
            .unwrap();
        txn.commit().await.unwrap();
        check(
            &db,
            &index,
            &points,
            &mut rng,
            &format!("{name}: op {op} ({change})"),
        )
        .await;
    }
    evicted
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_off_caches_at_their_bound_keep_one_locator_per_link() {
    let mut evicted = [0, 0];
    for seed in 0..2 {
        for run in [
            bounded_one_off_run::<Euclidean>(seed).await,
            bounded_one_off_run::<Cosine>(seed).await,
        ] {
            evicted = [evicted[0] + run[0], evicted[1] + run[1]];
        }
    }
    assert!(
        evicted.iter().all(|rows| *rows > 0),
        "upserts and deletes each flushed and evicted dirty rows mid-operation: {evicted:?}"
    );
}

/// Commits `links` without their locators, as released versions left them.
async fn commit_without_locators<D: Distance>(
    db: &slatedb::Db,
    index: &VectorIndex<D>,
    links: &BTreeSet<Link>,
) {
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let rows = VectorWriteRows::new(&measured, index.row_keyspace());
    links
        .iter()
        .try_for_each(|(layer, source, target)| {
            rows.delete_reverse_locator(*target, *layer, *source)
        })
        .unwrap();
    txn.commit().await.unwrap();
}

/// Moves `node` a step along x at its top layer in `graph` through a fresh
/// session, as publication plans it, committing only a planned move.
async fn re_embed<D: Distance>(
    db: &slatedb::Db,
    index: &VectorIndex<D>,
    graph: &Graph,
    points: &mut BTreeMap<NodeId, [f32; 2]>,
    node: NodeId,
) -> Result<(), HelixDbError> {
    let [x, y] = points[&node];
    let point = [x + 0.25, y];
    let layer = graph
        .rows
        .iter()
        .filter(|(_, row)| *row == node)
        .map(|(layer, _)| *layer)
        .max()
        .unwrap();
    let mut session = VectorBuildSession::<D>::new(NonZeroU64::new(1 << 20).unwrap());
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    index
        .stage_upsert_at_layer_with_session(&measured, node, &point, layer, &mut session)
        .await?;
    session.flush_all(&measured).unwrap();
    txn.commit().await.unwrap();
    points.insert(node, point);
    Ok(())
}

/// Builds a namespace and deletes every fifth node, then drops the locators
/// of every link to `mirrored`, which links back every source, and
/// re-embeds it, then drops those of every link to `target`, which has the
/// most one-way incoming links, and re-embeds and deletes it.
async fn released_unlocated_run<D: Distance>() {
    let mut rng = StdRng::seed_from_u64(0);
    let name = format!("released-unlocated-{}", D::name());
    let (db, index) = create::<D>(&name).await;
    let mut points = build(&db, &index, &mut rng, 160).await;
    // This build leaves no one-way link; the relinks of deletes do.
    let mut session = VectorBuildSession::<D>::new(NonZeroU64::new(1 << 20).unwrap());
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    for node in (5..=160).step_by(5) {
        index
            .stage_delete_with_build_session(&measured, node, &mut session)
            .await
            .unwrap();
        session.flush_all(&measured).unwrap();
        session.admit_entity();
        points.remove(&node);
    }
    txn.commit().await.unwrap();
    let built = Graph::read(&index, db.snapshot().await.unwrap().as_ref()).await;
    built.assert_valid(&points.keys().copied().collect(), &format!("{name}: built"));
    let incoming = |graph: &Graph, node: NodeId| {
        graph
            .links
            .iter()
            .filter(|(_, _, target)| *target == node)
            .copied()
            .collect::<BTreeSet<_>>()
    };
    let one_way = |graph: &Graph, node: NodeId| {
        incoming(graph, node)
            .into_iter()
            .filter(|(layer, source, _)| !graph.links.contains(&(*layer, node, *source)))
            .collect::<BTreeSet<_>>()
    };

    let mirrored = points
        .keys()
        .copied()
        .find(|node| !incoming(&built, *node).is_empty() && one_way(&built, *node).is_empty())
        .unwrap();
    let released = incoming(&built, mirrored);
    commit_without_locators(&db, &index, &released).await;
    re_embed(&db, &index, &built, &mut points, mirrored)
        .await
        .unwrap();
    let moved = Graph::read(&index, db.snapshot().await.unwrap().as_ref()).await;
    // The re-embedding's delete reaches every source through the node's own
    // rows, and each link its insert restores is back at its baseline, so
    // the flush stages no locator for it.
    let unlocated = released
        .intersection(&moved.links)
        .copied()
        .collect::<BTreeSet<_>>();
    assert!(
        !unlocated.is_empty(),
        "{name}: re-embedding {mirrored} restores released links"
    );
    moved.assert_faults(
        &points.keys().copied().collect(),
        &unlocated,
        &BTreeSet::new(),
        &format!("{name}: re-embedded {mirrored}"),
    );

    let target = points
        .keys()
        .copied()
        .filter(|node| *node != mirrored)
        .max_by_key(|node| one_way(&moved, *node).len())
        .unwrap();
    let released = incoming(&moved, target);
    let unreachable = one_way(&moved, target);
    assert!(
        !unreachable.is_empty(),
        "{name}: a node has a one-way incoming link"
    );
    commit_without_locators(&db, &index, &released).await;
    let unlocated = unlocated.union(&released).copied().collect::<BTreeSet<_>>();
    // The re-embedding's delete misses each one-way source, so its insert
    // searches through one back to the node and links the node to itself.
    let error = re_embed(&db, &index, &moved, &mut points, target)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains(&format!("neighbor set contains its owner {target}")),
        "{name}: re-embedding {target}: {error}"
    );

    let mut session = VectorBuildSession::<D>::new(NonZeroU64::new(1 << 20).unwrap());
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    index
        .stage_delete_with_build_session(&measured, target, &mut session)
        .await
        .unwrap();
    session.flush_all(&measured).unwrap();
    txn.commit().await.unwrap();
    points.remove(&target);
    let snapshot = db.snapshot().await.unwrap();
    let deleted = Graph::read(&index, snapshot.as_ref()).await;
    // A relink's pruning may drop a one-way link incidentally; every other
    // link to the node goes with the delete.
    let dangling = released
        .intersection(&deleted.links)
        .copied()
        .collect::<BTreeSet<_>>();
    assert!(
        !dangling.is_empty() && dangling.is_subset(&unreachable),
        "{name}: only one-way released links outlive {target}: {dangling:?}"
    );
    deleted.assert_faults(
        &points.keys().copied().collect(),
        &unlocated.intersection(&deleted.links).copied().collect(),
        &dangling,
        &format!("{name}: deleted {target}"),
    );
    let Some((_, source, _)) = dangling.iter().find(|(layer, _, _)| *layer == 0) else {
        panic!("{name}: a layer-0 released link outlives {target}");
    };
    let Err(error) = index
        .search(
            snapshot.as_ref(),
            &points[source],
            &SearchParams::new(20).unwrap(),
        )
        .await
    else {
        panic!("{name}: a search from {source} reaches its link to deleted {target}");
    };
    assert!(
        error
            .to_string()
            .contains(&format!("missing simhash for node {target} ")),
        "{name}: {error}"
    );
}

/// Pins what this code does with links released versions committed without
/// a locator; a locator rebuild of released namespaces must turn it around.
///
/// v3.1.0 through v3.4.2 (Docker images through v0.0.9) re-embed a node as a
/// delete and an insert in one cache. The delete removed every locator naming
/// the node directly, and the boundary staged locators only for rows whose
/// value changed, so each link the insert restored kept no locator. A delete
/// finds a link's source only through that locator or the node's own rows, so
/// nothing here repairs such a link. Once the node stops linking back, nothing
/// reaches the link: re-embedding the node fails, as its insert searches
/// through the link back to the node and links the node to itself, and deleting
/// the node leaves the link naming a node without an item, which fails searches
/// that reach it. Every other link and locator stays exact.
#[tokio::test]
async fn links_released_versions_left_without_locators_outlive_their_target() {
    released_unlocated_run::<Euclidean>().await;
    released_unlocated_run::<Cosine>().await;
}
