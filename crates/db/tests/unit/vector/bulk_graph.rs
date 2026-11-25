//! Contracts for parallel in-memory HNSW construction and its row writer.
//!
//! The fixtures build graphs on real threads and store them through the
//! production row writers into isolated in-memory databases. A one-thread
//! bulk build is compared row for row with the serial builder; concurrent
//! builds and unlinked graphs are checked against the structural contract the
//! stored reverse locators and searches depend on.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;
use std::sync::Arc;

use proptest::prelude::*;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use slatedb::object_store::memory::InMemory;
use slatedb::IsolationLevel;

use super::*;
use crate::encoding::v2::keys::scope::DataScope;
use crate::index_lifecycle::IndexElementKind;
use crate::search::vector::distance::{Cosine, Euclidean, Manhattan};
use crate::search::vector::hnsw::mutation::{
    FreshVectorBuildProof, MutationOpCache, MutationScratch,
};
use crate::search::vector::storage::VectorRows;
use crate::search::vector::{
    self, Candidate, SearchParams, ValidatedVectorGenerationHandle, VectorBuildSession,
    VectorGenerationIdentity,
};

const THREADS: NonZeroUsize = NonZeroUsize::new(8).unwrap();
const ONE_THREAD: NonZeroUsize = NonZeroUsize::MIN;

/// Deterministic source: `count` vectors of `dimension` components with
/// entity IDs `10, 20, ..` and layers drawn as the lifecycle draws them.
fn source(seed: u64, count: usize, dimension: usize, ml: f32) -> Vec<(NodeId, Vec<f32>, u16)> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..count)
        .map(|position| {
            let vector = (0..dimension)
                .map(|_| rng.random_range(-1.0_f32..1.0))
                .collect();
            let layer = vector::select_layer(ml, &mut rng);
            ((position as NodeId + 1) * 10, vector, layer)
        })
        .collect()
}

fn vectors<D: Distance>(source: &[(NodeId, Vec<f32>, u16)]) -> BulkVectors<D> {
    let mut vectors = BulkVectors::new(source[0].1.len());
    source
        .iter()
        .for_each(|(entity_id, vector, layer)| vectors.push(*entity_id, vector, *layer));
    vectors
}

fn config(dimension: usize, m: usize) -> VectorIndexConfig {
    VectorIndexConfig::new("bulk-graph", "embedding", dimension)
        .with_m(m)
        .with_m0(m * 2)
}

fn built<D: Distance>(
    source: &[(NodeId, Vec<f32>, u16)],
    config: &VectorIndexConfig,
    threads: NonZeroUsize,
) -> BulkGraph<D> {
    let linked = AtomicU64::new(0);
    let BulkConstruction::Built(graph) = BulkGraph::construct(
        vectors(source),
        BulkParameters::try_from_config(config).unwrap(),
        threads,
        &AtomicBool::new(false),
        &linked,
    )
    .unwrap() else {
        panic!("an uncancelled construction builds");
    };
    assert_eq!(
        linked.load(Ordering::Relaxed),
        source.len() as u64,
        "the construction counts every vector it inserted"
    );
    *graph
}

/// Every link of every linked node, by layer and source ordinal.
fn links<D: Distance>(graph: &BulkGraph<D>) -> BTreeMap<(u8, u32), BTreeSet<u32>> {
    (0..graph.len() as u32)
        .filter(|node| !graph.is_unlinked(*node))
        .flat_map(|node| {
            (0..=graph.vectors.levels[node as usize])
                .map(move |layer| ((layer, node), graph.row(layer, node).iter().collect()))
        })
        .collect()
}

/// Asserts the structural contract of a constructed or unlinked graph.
fn assert_graph_contract<D: Distance>(graph: &BulkGraph<D>) {
    let links = links(graph);
    for (&(layer, node), row) in &links {
        assert!(row.len() <= graph.parameters.degree(layer), "degree limit");
        assert_eq!(
            row.len(),
            graph.row(layer, node).len(),
            "a row holds a node once"
        );
        for &neighbor in row {
            assert_ne!(neighbor, node, "no self link");
            assert!(!graph.is_unlinked(neighbor), "no link to an unlinked node");
            assert!(
                links[&(layer, neighbor)].contains(&node),
                "link {node}->{neighbor} on layer {layer} has its reverse"
            );
        }
    }
    (0..graph.len() as u32)
        .filter(|node| graph.is_unlinked(*node))
        .for_each(|node| {
            (0..=graph.vectors.levels[node as usize])
                .for_each(|layer| assert!(graph.row(layer, node).is_empty()))
        });
    let linked = (0..graph.len() as u32)
        .filter(|node| !graph.is_unlinked(*node))
        .collect::<Vec<_>>();
    assert_eq!(linked.len(), graph.linked_len());
    if linked.len() > 1 {
        linked.iter().for_each(|node| {
            assert!(
                !links[&(0, *node)].is_empty(),
                "node {node} has a layer-0 link"
            )
        });
    }
    let top = linked
        .iter()
        .map(|node| graph.vectors.levels[*node as usize])
        .max();
    let first_on_top = linked
        .iter()
        .find(|node| Some(graph.vectors.levels[**node as usize]) == top);
    assert_eq!(
        graph.entry_point(),
        first_on_top.map(|node| (graph.entity_id(*node), u16::from(top.unwrap())))
    );
}

fn identity(dimension: usize) -> VectorGenerationIdentity {
    VectorGenerationIdentity::try_new(
        DataScope::LegacyUnscoped,
        7,
        "bulk-graph".to_string(),
        91,
        NonZeroU64::new(3).unwrap(),
        11,
        IndexElementKind::Node,
        VectorDimension::try_new(dimension).unwrap(),
    )
    .unwrap()
}

fn index<D: Distance>(dimension: usize) -> VectorIndex<D> {
    VectorIndex::from_generation(
        &ValidatedVectorGenerationHandle::create_current::<D>(identity(dimension)).unwrap(),
    )
}

async fn database(name: &str) -> Arc<slatedb::Db> {
    Arc::new(
        slatedb::Db::open(name, Arc::new(InMemory::new()))
            .await
            .unwrap(),
    )
}

async fn committed_rows(db: &slatedb::Db) -> BTreeMap<bytes::Bytes, bytes::Bytes> {
    let mut rows = db.scan::<std::ops::RangeFull>(..).await.unwrap();
    let mut committed = BTreeMap::new();
    while let Some(row) = rows.next().await.unwrap() {
        committed.insert(row.key, row.value);
    }
    committed
}

/// Stores every linked node of `graph` and its metadata in one commit.
async fn store<D: Distance>(
    db: &slatedb::Db,
    index: &VectorIndex<D>,
    graph: &BulkGraph<D>,
    source: &[(NodeId, Vec<f32>, u16)],
    config: &VectorIndexConfig,
) {
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&transaction);
    (0..graph.len() as u32)
        .filter(|node| !graph.is_unlinked(*node))
        .for_each(|node| {
            index
                .stage_bulk_node(&measured, graph, node, &source[node as usize].1)
                .unwrap()
        });
    index
        .stage_bulk_metadata(&measured, graph, config.clone())
        .await
        .unwrap();
    transaction.commit().await.unwrap();
}

/// Stores `source` through the serial builder in source order.
async fn store_serially<D: Distance>(
    db: &slatedb::Db,
    index: &VectorIndex<D>,
    source: &[(NodeId, Vec<f32>, u16)],
    config: &VectorIndexConfig,
) {
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&transaction);
    index.stage_create(&measured, config.clone()).await.unwrap();
    let mut session = VectorBuildSession::new(NonZeroU64::new(1 << 26).unwrap());
    for (entity_id, vector, layer) in source {
        index
            .stage_known_fresh_at_layer_with_session(
                &measured,
                *entity_id,
                vector,
                *layer,
                FreshVectorBuildProof::for_test(),
                &mut session,
            )
            .await
            .unwrap();
        session.flush_all(&measured).unwrap();
        session.admit_entity();
    }
    transaction.commit().await.unwrap();
}

fn exact_top(source: &[(NodeId, Vec<f32>, u16)], query: &[f32], k: usize) -> BTreeSet<NodeId> {
    let mut ranked = source
        .iter()
        .map(|(entity_id, vector, _)| {
            let distance = vector
                .iter()
                .zip(query)
                .map(|(left, right)| (left - right) * (left - right))
                .sum::<f32>();
            (DistanceScore::try_new(distance).unwrap(), *entity_id)
        })
        .collect::<Vec<_>>();
    ranked.sort_unstable();
    ranked
        .into_iter()
        .take(k)
        .map(|(_, entity_id)| entity_id)
        .collect()
}

proptest! {
    /// The ordinal selection keeps exactly the nodes the serial selection keeps.
    #[test]
    fn diverse_selection_matches_the_serial_selection(
        points in prop::collection::vec((-8_i8..8, -8_i8..8), 1..40),
        degree in 1_usize..12,
    ) {
        let items = points
            .iter()
            .map(|(x, y)| {
                let point = [f32::from(*x), f32::from(*y)];
                let vector = UnalignedVector::<f32>::from_slice(&point);
                Item::<Euclidean> {
                    header: Euclidean::new_header(&vector),
                    vector: Cow::Owned(vector.into_owned()),
                }
            })
            .collect::<Vec<_>>();
        let reference = &items[0];
        let mut scored = (1..items.len() as u32)
            .map(|node| {
                let distance = Euclidean::distance(reference, &items[node as usize]);
                (DistanceScore::try_new(distance).unwrap(), node)
            })
            .collect::<Vec<Scored>>();
        scored.sort_unstable();
        let candidates = scored
            .iter()
            .map(|(distance, node)| Candidate::try_new(NodeId::from(*node), distance.get()).unwrap())
            .collect::<Vec<_>>();
        let serial = vector::select_diverse(
            reference,
            candidates
                .iter()
                .map(|candidate| (*candidate, items.get(candidate.node_id as usize))),
            degree,
        )
        .unwrap();

        let mut selected = vec![u32::MAX];
        select_diverse(
            &scored,
            degree,
            |left, right| {
                Ok(DistanceScore::try_new(Euclidean::distance(
                    &items[left as usize],
                    &items[right as usize],
                ))
                .unwrap())
            },
            &mut selected,
        )
        .unwrap();
        prop_assert_eq!(
            selected.into_iter().map(NodeId::from).collect::<Vec<_>>(),
            serial
        );
    }
}

/// How the vectors of a serial-equality source tie.
#[derive(Clone, Copy)]
enum Ties {
    /// Random vectors: no two distances are equal.
    None,
    /// Every vector twice: the two copies tie at every distance, so only
    /// source order ranks them, in the searches and in the selection. (Many
    /// copies of one point would not compare: the serial build leaves the
    /// later ones without a link, which the bulk build mends.)
    Pairs,
    /// A square lattice searched with a narrow beam: most distances repeat,
    /// so the beam is full of ties at the distance where it stops. No two
    /// points lie on one line through the origin, where their cosine
    /// distance would make them copies.
    Lattice,
}

/// Asserts that a one-thread bulk build under `D` stores the rows the serial
/// builder stores: the same keys, values and edges.
async fn assert_one_thread_build_stores_the_serial_build_rows<D: Distance>() {
    for (seed, count, dimension, m, ties) in [
        (1, 700, 6, 4, Ties::None),
        (2, 500, 3, 16, Ties::None),
        (3, 1, 3, 16, Ties::None),
        (4, 400, 3, 4, Ties::Pairs),
        (5, 400, 2, 4, Ties::Lattice),
    ] {
        let config = config(dimension, m);
        let mut source = source(seed, count, dimension, config.ml);
        let config = match ties {
            Ties::None => config,
            Ties::Pairs => {
                (1..source.len())
                    .step_by(2)
                    .for_each(|copy| source[copy].1 = source[copy - 1].1.clone());
                config
            }
            Ties::Lattice => {
                source.iter_mut().enumerate().for_each(|(node, row)| {
                    row.1 = vec![(node % 20) as f32 + 1.0, (node / 20) as f32 + 21.0]
                });
                config.with_ef_construction(8)
            }
        };
        let index = index::<D>(dimension);
        let config = VectorIndexConfig {
            index_name: index.name().to_string(),
            ..config
        };

        let serial = database("bulk-graph-serial").await;
        store_serially(&serial, &index, &source, &config).await;
        let bulk = database("bulk-graph-one-thread").await;
        let graph = built::<D>(&source, &config, ONE_THREAD);
        assert_graph_contract(&graph);
        store(&bulk, &index, &graph, &source, &config).await;

        let serial = committed_rows(&serial).await;
        let bulk = committed_rows(&bulk).await;
        assert_eq!(
            serial.keys().collect::<Vec<_>>(),
            bulk.keys().collect::<Vec<_>>(),
            "{} seed {seed}: same row keys",
            D::name()
        );
        assert!(serial == bulk, "{} seed {seed}: same row values", D::name());
    }
}

/// Built on one thread, the bulk path applies the serial insert's rules in the
/// serial order under every full-precision metric, each with its own vector
/// header and score.
#[tokio::test]
async fn one_thread_build_stores_the_serial_build_rows() {
    assert_one_thread_build_stores_the_serial_build_rows::<Euclidean>().await;
    assert_one_thread_build_stores_the_serial_build_rows::<Cosine>().await;
    assert_one_thread_build_stores_the_serial_build_rows::<Manhattan>().await;
}

/// A concurrent build keeps the structural contract, and its stored form has
/// one reverse locator per link and answers searches as well as the serial
/// build of the same source.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_build_is_symmetric_linked_and_searchable() {
    let dimension = 12;
    let config = config(dimension, 8);
    let source = source(7, 6_000, dimension, config.ml);
    let index = index::<Euclidean>(dimension);
    let config = VectorIndexConfig {
        index_name: index.name().to_string(),
        ..config
    };
    let graph = built::<Euclidean>(&source, &config, THREADS);
    assert_graph_contract(&graph);

    let bulk = database("bulk-graph-concurrent").await;
    store(&bulk, &index, &graph, &source, &config).await;
    let serial = database("bulk-graph-concurrent-serial").await;
    store_serially(&serial, &index, &source, &config).await;

    let metadata = index.get_metadata(bulk.as_ref()).await.unwrap().unwrap();
    assert_eq!(metadata.count, 6_000);
    assert_eq!(
        metadata
            .entry_point
            .map(|entry| (entry, metadata.max_layer)),
        graph.entry_point()
    );

    let rows = VectorRows::new(bulk.as_ref(), index.row_keyspace());
    for node in (0..graph.len() as u32).step_by(37) {
        let entity_id = graph.entity_id(node);
        let level = graph.vectors.levels[node as usize];
        let locators = rows.reverse_sources_for_target(entity_id).await.unwrap();
        for layer in 0..=level {
            let stored = match layer {
                0 => rows.layer0_neighbors(entity_id).await.unwrap(),
                layer => rows
                    .upper_neighbors(u16::from(layer), entity_id)
                    .await
                    .unwrap()
                    .expect("a row per occupied layer"),
            };
            assert_eq!(stored, graph.links(layer, node));
            // The graph is symmetric, so a node's sources are its links.
            assert_eq!(locators.sources_at(u16::from(layer)), stored);
        }
        assert_eq!(
            locators.sources_by_layer().len(),
            (0..=level)
                .filter(|layer| !graph.links(*layer, node).is_empty())
                .count()
        );
    }

    let queries = self::source(8, 200, dimension, config.ml);
    let params = SearchParams::new(10).unwrap();
    let mut found = [0_usize; 2];
    for (_, query, _) in &queries {
        let exact = exact_top(&source, query, 10);
        for (database, found) in [&bulk, &serial].into_iter().zip(found.iter_mut()) {
            *found += index
                .search(database.as_ref(), query, &params)
                .await
                .unwrap()
                .iter()
                .filter(|result| exact.contains(&result.entity_id()))
                .count();
        }
    }
    let [bulk_found, serial_found] = found;
    assert!(
        bulk_found * 100 >= serial_found * 99,
        "bulk recall {bulk_found} of 2000, serial {serial_found}"
    );
    assert!(
        serial_found >= 1_900,
        "serial recall {serial_found} of 2000"
    );
}

/// A link whose reverse is missing, as two concurrent inserts can leave, is
/// removed before the graph is written, and every link with its reverse is
/// kept.
#[test]
fn links_without_their_reverse_are_removed() {
    let config = config(3, 4);
    let source = source(31, 300, 3, config.ml);
    let graph = built::<Euclidean>(&source, &config, ONE_THREAD);
    let before = links(&graph);
    // A layer-0 row with a free slot gains a link its target does not return.
    let nodes = 0..graph.len() as u32;
    let (linking, target) = nodes
        .clone()
        .find_map(|node| {
            let row = &before[&(0, node)];
            (row.len() < graph.parameters.degree(0))
                .then(|| {
                    nodes
                        .clone()
                        .find(|other| *other != node && !row.contains(other))
                })
                .flatten()
                .map(|other| (node, other))
        })
        .expect("some layer-0 row has a free slot");
    graph.row(0, linking).push(target);
    assert!(graph.row(0, linking).contains(target));
    assert!(!graph.row(0, target).contains(linking));

    let mut scratch = Scratch::default();
    nodes.for_each(|node| graph.drop_unreciprocated(node, &mut scratch));
    assert!(links(&graph) == before);
    assert_graph_contract(&graph);
}

/// Duplicate vectors, alone or among distinct ones, are linked like any
/// other: within the degree limits, symmetrically, and none left without a
/// layer-0 link, on one thread and on several. A row keeps its lowest
/// copies, so later copies lose every link they chose and are linked again.
#[test]
fn duplicate_vectors_are_linked_like_any_other() {
    let config = config(3, 4);
    let mut mixed = source(21, 1_200, 3, config.ml);
    mixed
        .iter_mut()
        .step_by(2)
        .for_each(|(_, vector, _)| *vector = vec![0.25, -0.5, 0.75]);
    let mut identical = source(22, 500, 3, config.ml);
    identical
        .iter_mut()
        .for_each(|(_, vector, _)| *vector = vec![1.0, 1.0, 1.0]);
    for source in [&mixed, &identical] {
        for threads in [ONE_THREAD, THREADS] {
            assert_graph_contract(&built::<Euclidean>(source, &config, threads));
            assert_graph_contract(&built::<Cosine>(source, &config, threads));
        }
    }
}

/// A beam enters each start once, never visits the node it excludes, keeps
/// at most `ef` of the nearest, and ranks starts and visited nodes alike.
#[test]
fn a_beam_enters_each_start_once_and_keeps_the_nearest() {
    let config = config(2, 2);
    // Points on a line, so distances to the query are known: node `i` lies
    // `i` from the query at 0, and every two nodes tie at no distance.
    let source = (0..40)
        .map(|position| ((position + 1) * 10, vec![position as f32, 0.0], 0))
        .collect::<Vec<_>>();
    let graph = built::<Euclidean>(&source, &config, ONE_THREAD);
    let query = graph.vectors.item(0);
    let mut scratch = Scratch::default();
    let ranked = |scratch: &Scratch| {
        scratch
            .ranked
            .iter()
            .map(|(_, node)| *node)
            .collect::<Vec<_>>()
    };

    // Repeated starts are scored once.
    graph
        .beam(&query, 0, &[30, 30, 20, 30], 0, 64, &mut scratch)
        .unwrap();
    let all = ranked(&scratch);
    assert_eq!(all.len(), 39, "every node but the excluded one, once");
    assert!(
        all.windows(2).all(|pair| pair[0] < pair[1]),
        "nearest first"
    );

    // More starts than the beam holds: the nearest `ef` stay.
    graph
        .beam(&query, 0, &[39, 38, 37, 3, 36], 0, 2, &mut scratch)
        .unwrap();
    assert_eq!(ranked(&scratch), [1, 2]);
}

/// A beam never starts at the node it excludes.
#[test]
#[should_panic(expected = "a beam starts at other nodes")]
fn a_beam_rejects_the_excluded_node_as_a_start() {
    let config = config(2, 2);
    let source = source(3, 20, 2, config.ml);
    let graph = built::<Euclidean>(&source, &config, ONE_THREAD);
    let query = graph.vectors.item(4);
    graph
        .beam(&query, 4, &[1, 4], 0, 8, &mut Scratch::default())
        .unwrap();
}

/// A beam needs a start.
#[test]
#[should_panic(expected = "a beam starts at other nodes")]
fn a_beam_rejects_no_start() {
    let config = config(2, 2);
    let source = source(3, 20, 2, config.ml);
    let graph = built::<Euclidean>(&source, &config, ONE_THREAD);
    let query = graph.vectors.item(4);
    graph
        .beam(&query, 4, &[], 0, 8, &mut Scratch::default())
        .unwrap();
}

/// The serial insert's beam enters its entry points as the bulk beam does:
/// each once and in order, never the inserting node, at most `ef` of them,
/// and a missing later one not at all.
#[tokio::test]
async fn a_serial_beam_enters_each_entry_point_once() {
    let config = config(2, 2);
    let source = (0..40)
        .map(|position| ((position + 1) * 10, vec![position as f32, 0.0], 0))
        .collect::<Vec<_>>();
    let index = index::<Euclidean>(2);
    let config = VectorIndexConfig {
        index_name: index.name().to_string(),
        ..config
    };
    let db = database("bulk-graph-serial-beam-starts").await;
    store_serially(&db, &index, &source, &config).await;
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&transaction);
    let origin = UnalignedVector::<f32>::from_slice(&[0.0, 0.0]);
    let query = Item::<Euclidean> {
        header: Euclidean::new_header(&origin),
        vector: Cow::Owned(origin.into_owned()),
    };
    let limits = MutationDegreeLimits::try_from_config(&config).unwrap();
    let beam = |entry_points: Vec<NodeId>, ef: usize| {
        let (index, measured, query) = (&index, &measured, &query);
        async move {
            let mut cache = MutationOpCache::<Euclidean>::with_degree_limits(
                limits.layer0.get(),
                limits.upper.get(),
            )
            .unwrap();
            index
                .search_layer_beam(
                    measured,
                    query,
                    &entry_points,
                    0,
                    ef,
                    10,
                    &mut cache,
                    &mut MutationScratch::default(),
                )
                .await
                .unwrap()
                .into_iter()
                .map(|candidate| candidate.node_id)
                .collect::<Vec<_>>()
        }
    };

    let all = beam(vec![300, 300, 200, 300, 10], 64).await;
    assert_eq!(all.len(), 39, "every node but the inserting one, once");
    assert!(
        all.windows(2).all(|pair| pair[0] < pair[1]),
        "nearest first"
    );
    assert_eq!(beam(vec![400, 390, 380, 40, 370], 2).await, [20, 30]);
    // A missing later entry point is skipped.
    assert_eq!(beam(vec![400, 9_999], 64).await.len(), 39);
    transaction.rollback();
}

/// Two clusters of normally distributed points `separation` apart on every
/// axis, alternating in source order, and joined by a thin filament: about
/// one point in 33 lies anywhere between them.
fn two_clusters(count: usize, dimension: usize, ml: f32) -> Vec<(NodeId, Vec<f32>, u16)> {
    let separation = 5.0_f32;
    let mut rng = StdRng::seed_from_u64(1);
    let points = (0..count)
        .map(|position| {
            let centre = match (rng.random_range(0.0_f32..1.0) < 0.03, position % 2) {
                (true, _) => rng.random_range(0.0..separation),
                (false, 0) => 0.0,
                (false, _) => separation,
            };
            (0..dimension)
                .map(|_| {
                    // Box-Muller: a standard normal from two uniforms.
                    let (radius, angle): (f32, f32) = (
                        rng.random_range(f32::EPSILON..1.0),
                        rng.random_range(0.0..1.0),
                    );
                    centre + (-2.0 * radius.ln()).sqrt() * (std::f32::consts::TAU * angle).cos()
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut layers = StdRng::seed_from_u64(7);
    points
        .into_iter()
        .enumerate()
        .map(|(position, vector)| {
            (
                position as NodeId + 1,
                vector,
                vector::select_layer(ml, &mut layers),
            )
        })
        .collect()
}

/// Two clusters joined by a filament, built by both builders at a degree
/// low enough to show at six hundred points what SIFT shows at a million.
///
/// When every layer-0 beam of an insert started from the one node a greedy
/// walk of layer 1 reached, the beam stayed in that node's cluster, so the
/// links along the filament were pruned and never formed again: layer 0
/// split into the two clusters, and a node linked into the cluster opposite
/// to where a search for it descends was never found, even by its own
/// vector. Both builds now store the same graph, whose layer 0 is one
/// component reachable from the entry point, so a beam as wide as the graph
/// finds every node.
#[tokio::test(flavor = "multi_thread")]
async fn clusters_joined_by_a_filament_stay_linked_and_every_node_is_found() {
    let dimension = 16;
    let config = config(dimension, 4);
    let source = two_clusters(600, dimension, config.ml);
    let index = index::<Euclidean>(dimension);
    let config = VectorIndexConfig {
        index_name: index.name().to_string(),
        ..config
    };
    let graph = built::<Euclidean>(&source, &config, ONE_THREAD);
    let bulk = database("bulk-graph-filament").await;
    store(&bulk, &index, &graph, &source, &config).await;
    let serial = database("bulk-graph-filament-serial").await;
    store_serially(&serial, &index, &source, &config).await;
    assert!(
        committed_rows(&bulk).await == committed_rows(&serial).await,
        "a one-thread bulk build stores the serial build's rows"
    );

    let params = SearchParams::new(1)
        .unwrap()
        .with_ef(source.len())
        .unwrap()
        .with_simhash_mode(vector::SimHashMode::Off)
        .with_pre_simhash_sampling_ratio(1.0)
        .unwrap();
    for (name, db) in [("bulk", &bulk), ("serial", &serial)] {
        let rows = VectorRows::new(db.as_ref(), index.row_keyspace());
        let entry = rows.metadata().await.unwrap().unwrap().entry_point.unwrap();
        let mut reached = BTreeSet::from([entry]);
        let mut frontier = vec![entry];
        while let Some(node) = frontier.pop() {
            for neighbor in rows.layer0_neighbors(node).await.unwrap() {
                if reached.insert(neighbor) {
                    frontier.push(neighbor);
                }
            }
        }
        assert_eq!(
            reached.len(),
            source.len(),
            "{name}: layer 0 is one component"
        );

        let mut missed = Vec::new();
        for (entity, vector, _) in &source {
            let results = index.search(db.as_ref(), vector, &params).await.unwrap();
            if results.first().map(|result| result.entity_id()) != Some(*entity) {
                missed.push(*entity);
            }
        }
        assert!(
            missed.is_empty(),
            "{name}: not found by their own vectors: {missed:?}"
        );
    }
}

/// Small degrees make later inserts prune every link of earlier nodes; the
/// build links each of them again and stays symmetric.
#[test]
fn nodes_pruned_of_every_link_are_linked_again() {
    for seed in 0..8 {
        let config = config(2, 2);
        let source = source(seed, 3_000, 2, config.ml);
        assert_graph_contract(&built::<Euclidean>(&source, &config, THREADS));
    }
}

/// Stored rows restaged from the reported link changes equal a store of the
/// graph as it is after unlinking, so the journal names every changed row and
/// locator.
#[tokio::test]
async fn link_changes_restage_stored_rows_to_the_unlinked_graph() {
    let dimension = 3;
    let config = config(dimension, 2);
    let source = source(11, 400, dimension, config.ml);
    let index = index::<Euclidean>(dimension);
    let config = VectorIndexConfig {
        index_name: index.name().to_string(),
        ..config
    };
    let mut graph = built::<Euclidean>(&source, &config, THREADS);

    // As pass 3 does: nodes are visited in source order and either stored or
    // unlinked, and a change is restaged only for a source already stored.
    let restaged = database("bulk-graph-restaged").await;
    for node in 0..graph.len() as u32 {
        let transaction = restaged.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&transaction);
        if node % 3 == 1 {
            let mut changes = Vec::new();
            graph.unlink(node, &mut changes).unwrap();
            assert_graph_contract(&graph);
            assert!(changes
                .iter()
                .all(|change| !graph.is_unlinked(change.source)));
            changes
                .into_iter()
                .filter(|change| change.source < node)
                .for_each(|change| {
                    index
                        .stage_bulk_link_change(&measured, &graph, change)
                        .unwrap()
                });
            let mut again = Vec::new();
            graph.unlink(node, &mut again).unwrap();
            assert!(again.is_empty(), "unlinking twice changes nothing");
        } else {
            index
                .stage_bulk_node(&measured, &graph, node, &source[node as usize].1)
                .unwrap();
        }
        transaction
            .commit_with_options(&slatedb::config::WriteOptions {
                await_durable: false,
                ..slatedb::config::WriteOptions::default()
            })
            .await
            .unwrap();
    }
    let unlinked = (0..graph.len() as u32).filter(|node| node % 3 == 1).count();
    let transaction = restaged.begin(IsolationLevel::Snapshot).await.unwrap();
    index
        .stage_bulk_metadata(
            &MeasuredVectorTransaction::new(&transaction),
            &graph,
            config.clone(),
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    let direct = database("bulk-graph-direct").await;
    store(&direct, &index, &graph, &source, &config).await;
    assert!(committed_rows(&restaged).await == committed_rows(&direct).await);
    assert_eq!(graph.linked_len(), 400 - unlinked);
}

/// Unlinking in any order keeps the contract down to the last node, moves the
/// entry point by rule, and leaves nothing to activate once all are gone.
#[test]
fn unlinking_every_node_keeps_the_contract_until_none_is_left() {
    let config = config(2, 2);
    let source = source(5, 300, 2, config.ml);
    let mut graph = built::<Euclidean>(&source, &config, THREADS);
    let mut order = (0..graph.len() as u32).collect::<Vec<_>>();
    let mut rng = StdRng::seed_from_u64(9);
    // Entry points first, so the rule is exercised while the graph is dense.
    order.sort_by_cached_key(|node| {
        (
            Reverse(graph.vectors.levels[*node as usize]),
            rng.random::<u32>(),
        )
    });
    let mut changes = Vec::new();
    for node in order {
        graph.unlink(node, &mut changes).unwrap();
        assert_graph_contract(&graph);
    }
    assert_eq!(graph.linked_len(), 0);
    assert_eq!(graph.entry_point(), None);
}

/// A graph of full rows has no free slot for a node that lost its links, so
/// re-linking trades the furthest link of its closest neighbor that can
/// spare one, and never leaves that neighbor's old partner without a link.
#[test]
fn relinking_into_full_rows_trades_a_spare_link() {
    // Nodes 0..=4 form a clique on a line: with a layer-0 degree of four,
    // every row is full. Node 5 sits beyond node 4 and holds no link.
    let config = config(1, 2);
    let source = (0..6_u8)
        .map(|position| (NodeId::from(position) + 1, vec![f32::from(position)], 0))
        .collect::<Vec<_>>();
    let mut graph = built::<Euclidean>(&source[..5], &config, ONE_THREAD);
    assert!((0..5).all(|node| graph.row(0, node).len() == 4));
    graph = BulkGraph {
        layer0: LinkRows::new(6, 4),
        upper: LinkRows::new(0, 2),
        upper_slots: vec![0; 6],
        entry: RwLock::new(Entry { node: 0, layer: 0 }),
        unlinked: vec![false; 6],
        linked: 6,
        vectors: vectors::<Euclidean>(&source),
        parameters: graph.parameters,
    };
    (0..5_u32).for_each(|node| {
        graph
            .row(0, node)
            .replace(&(0..5).filter(|other| *other != node).collect::<Vec<_>>())
    });

    let mut changes = Vec::new();
    assert!(graph
        .relink_orphan(5, &mut Scratch::default(), &mut changes)
        .unwrap());
    assert_graph_contract(&graph);
    // Node 4 is closest; its furthest neighbor, node 0, keeps three links.
    let link = |source, target, kind| LinkChange {
        layer: 0,
        source,
        target,
        kind,
    };
    assert_eq!(
        changes,
        [
            link(4, 0, LinkChangeKind::Removed),
            link(0, 4, LinkChangeKind::Removed),
            link(4, 5, LinkChangeKind::Added),
            link(5, 4, LinkChangeKind::Added),
        ]
    );
}

/// Two nodes whose only link was the unlinked node are each linked again
/// once. Relinking the first links it to the second, its only candidate,
/// which then holds the link and gains nothing: the pair is not linked
/// twice, and neither is left for the orphan search.
#[test]
fn neighbors_orphaned_together_are_linked_once() {
    // Node 0 links nodes 1 and 2, which link nothing else. Node 2 is the
    // entry point: the only node on layer 1.
    let config = config(1, 2);
    let source = [(1, 0.0, 0), (2, 1.0, 0), (3, 2.0, 1)]
        .map(|(entity_id, position, layer)| (entity_id, vec![position], layer));
    let mut graph = BulkGraph {
        layer0: LinkRows::new(3, 4),
        upper: LinkRows::new(1, 2),
        upper_slots: vec![0; 3],
        entry: RwLock::new(Entry { node: 2, layer: 1 }),
        unlinked: vec![false; 3],
        linked: 3,
        vectors: vectors::<Euclidean>(&source),
        parameters: BulkParameters::try_from_config(&config).unwrap(),
    };
    graph.row(0, 0).replace(&[1, 2]);
    graph.row(0, 1).replace(&[0]);
    graph.row(0, 2).replace(&[0]);
    assert_graph_contract(&graph);

    let mut changes = Vec::new();
    graph.unlink(0, &mut changes).unwrap();
    assert_graph_contract(&graph);
    let link = |source, target, kind| LinkChange {
        layer: 0,
        source,
        target,
        kind,
    };
    assert_eq!(
        changes,
        [
            link(1, 0, LinkChangeKind::Removed),
            link(2, 0, LinkChangeKind::Removed),
            link(1, 2, LinkChangeKind::Added),
            link(2, 1, LinkChangeKind::Added),
        ]
    );
}

/// A graph at an upper degree of two with a node per point on a line, each
/// at its position and reaching its layer, holding `links` (layer and both
/// ends) both ways, with its entry point by rule.
fn line_graph(points: &[(f32, u8)], links: &[(u8, u32, u32)]) -> BulkGraph<Euclidean> {
    let source = points
        .iter()
        .enumerate()
        .map(|(node, (position, layer))| (node as NodeId + 1, vec![*position], u16::from(*layer)))
        .collect::<Vec<_>>();
    let parameters = BulkParameters::try_from_config(&config(1, 2)).unwrap();
    let upper_slots = points
        .iter()
        .scan(0_u32, |next, (_, layer)| {
            let slot = *next;
            *next += u32::from(*layer);
            Some(slot)
        })
        .collect();
    let upper_rows = points.iter().map(|(_, layer)| usize::from(*layer)).sum();
    let mut graph = BulkGraph {
        layer0: LinkRows::new(points.len(), parameters.degree(0)),
        upper: LinkRows::new(upper_rows, parameters.degree(1)),
        upper_slots,
        entry: RwLock::new(Entry { node: 0, layer: 0 }),
        unlinked: vec![false; points.len()],
        linked: points.len(),
        vectors: vectors::<Euclidean>(&source),
        parameters,
    };
    for &(layer, left, right) in links {
        graph.row(layer, left).push(right);
        graph.row(layer, right).push(left);
    }
    *graph.entry.get_mut() = graph.entry_by_rule().unwrap();
    graph
}

/// The links of two clusters on a line for [`line_graph`]: nodes `left` at 0,
/// 1 and 2, and nodes `right` at 10, 11 and 12, each a layer-0 clique that
/// links no node of the other. The first node of each cluster as given (at 0
/// and 12) alone reaches layer 1, where the two link each other.
fn two_cliques(left: [u32; 3], right: [u32; 3]) -> Vec<(u8, u32, u32)> {
    [left, right]
        .into_iter()
        .flat_map(|[a, b, c]| [(0, a, b), (0, a, c), (0, b, c)])
        .chain([(1, left[0], right[0])])
        .collect()
}

/// A node left without links between two clusters is linked into both.
///
/// Layer 0 holds the clusters apart. A greedy walk of layer 1 from the entry
/// point, at 0, moves to the node at 12, nearer the orphan at 6.4, and a
/// layer-0 beam from there alone finds the right cluster only, so every link
/// the orphan took stayed inside it. Seeded from both layer-1 nodes, the beam
/// finds both clusters, and the orphan links the two nearest nodes of each.
#[test]
fn an_orphan_between_two_clusters_is_linked_into_both() {
    // Node 3, at 6, links node 4, at 6.4, alone; unlinking node 3 leaves
    // node 4 without links and no neighbor to relink it from.
    let points = [
        (0.0, 1),
        (1.0, 0),
        (2.0, 0),
        (6.0, 0),
        (6.4, 0),
        (10.0, 0),
        (11.0, 0),
        (12.0, 1),
    ];
    let mut links = two_cliques([0, 1, 2], [7, 6, 5]);
    links.push((0, 3, 4));
    let mut graph = line_graph(&points, &links);
    assert_graph_contract(&graph);

    let mut changes = Vec::new();
    graph.unlink(3, &mut changes).unwrap();
    assert_graph_contract(&graph);
    assert_eq!(graph.row(0, 4).iter().collect::<Vec<_>>(), [1, 2, 5, 6]);
}

/// A node on layer 0 alone starts layer 0 from what a beam of layer 1 as wide
/// as the upper degree finds from the entry point, not from where a greedy
/// walk of layer 1 ends nor from a beam started there.
///
/// Seen from node 5, at 0, the entry point E at 10 links A at 5 and B at
/// -5.2; A links A2 at 5.5, and B links C at -1. A greedy walk moves to A
/// and stops: A2 is further. A beam of two from A keeps A and A2 and stops,
/// as E is further than both. From E, the beam keeps A and B, ahead of A2,
/// and B leads it to C.
#[test]
fn a_layer_0_insert_starts_from_a_layer_1_beam_from_the_entry() {
    let points = [
        (10.0, 1),
        (5.0, 1),
        (5.5, 1),
        (-5.2, 1),
        (-1.0, 1),
        (0.0, 0),
    ];
    let graph = line_graph(&points, &[(1, 0, 1), (1, 0, 3), (1, 1, 2), (1, 3, 4)]);
    let entry = *graph.entry.read();
    assert_eq!((entry.node, entry.layer), (0, 1));
    let mut seeds = Vec::new();
    graph
        .descend(
            &graph.vectors.item(5),
            5,
            0,
            entry,
            &mut seeds,
            &mut Scratch::default(),
        )
        .unwrap();
    assert_eq!(seeds, [4, 1], "C and A, nearest first");
}

/// An insert above layer 0 starts each lower layer's beam from the nearest
/// nodes the beam above found, not from the nearest alone.
///
/// Node 3, at 6.4 on layer 1, finds both layer-1 nodes, nearest the one at
/// 12. A layer-0 beam from that node alone finds only the right cluster;
/// from both, it finds both clusters and links the two nearest nodes of each.
#[test]
fn an_upper_insert_seeds_layer_0_from_every_node_layer_1_found() {
    let points = [
        (0.0, 1),
        (1.0, 0),
        (2.0, 0),
        (6.4, 1),
        (10.0, 0),
        (11.0, 0),
        (12.0, 1),
    ];
    let graph = line_graph(&points, &two_cliques([0, 1, 2], [6, 5, 4]));
    let entry = *graph.entry.read();
    assert_eq!((entry.node, entry.layer), (0, 1));
    graph.connect(3, 1, entry, &mut Scratch::default()).unwrap();
    assert_graph_contract(&graph);
    assert_eq!(graph.row(1, 3).iter().collect::<Vec<_>>(), [0, 6]);
    assert_eq!(graph.row(0, 3).iter().collect::<Vec<_>>(), [1, 2, 4, 5]);
}

/// Unlinking a tenth of a graph leaves the rest as densely linked as it was:
/// every neighbor that lost a link is linked again from the neighborhood the
/// unlinked node left, as the serial delete relinks it, where linking again
/// only the nodes left with no link at all would cost the graph a tenth of
/// its links for good. The same holds on the upper layers.
#[test]
fn unlinking_a_tenth_of_the_graph_keeps_its_degree() {
    let config = config(8, 8);
    let source = source(21, 3_000, 8, config.ml);
    let mut graph = built::<Euclidean>(&source, &config, THREADS);
    // Mean links of a linked node on layer 0, and of one on layer 1.
    let degrees = |graph: &BulkGraph<Euclidean>| {
        [0_u8, 1].map(|layer| {
            let rows = links(graph)
                .into_iter()
                .filter(|((row_layer, _), _)| *row_layer == layer)
                .map(|(_, row)| row.len())
                .collect::<Vec<_>>();
            rows.iter().sum::<usize>() as f64 / rows.len() as f64
        })
    };
    let before = degrees(&graph);
    let mut changes = Vec::new();
    (0..graph.len() as u32)
        .filter(|node| node % 10 == 3)
        .for_each(|node| graph.unlink(node, &mut changes).unwrap());
    assert_graph_contract(&graph);
    assert_eq!(graph.linked_len(), 2_700);
    let after = degrees(&graph);
    for (layer, (before, after)) in before.into_iter().zip(after).enumerate() {
        assert!(
            after >= before * 0.98,
            "layer {layer}: {after} links a node after, {before} before"
        );
    }
}

/// A node linked again takes a link for every free slot of its row, not only
/// for the one the unlinked node left: a full row that lost two links, to
/// nodes that have room for it, gains both back, each with its reverse and
/// each reported both ways.
#[test]
fn relinking_fills_every_free_slot() {
    let config = config(8, 8);
    let source = source(22, 400, 8, config.ml);
    let graph = built::<Euclidean>(&source, &config, THREADS);
    let degree = graph.parameters.degree(0);
    let (node, lost) = (0..graph.len() as u32)
        .find_map(|node| {
            let row = graph.row(0, node).iter().collect::<Vec<_>>();
            (row.len() == degree).then(|| (node, [row[0], row[1]]))
        })
        .expect("a node with a full layer-0 row");
    for other in lost {
        assert!(graph.row(0, node).remove(other));
        assert!(graph.row(0, other).remove(node));
    }
    let mut changes = Vec::new();
    graph
        .relink(0, node, &lost, &mut Scratch::default(), &mut changes)
        .unwrap();
    assert_eq!(graph.row(0, node).len(), degree);
    let mut added = changes
        .iter()
        .map(|change| {
            assert_eq!((change.layer, change.kind), (0, LinkChangeKind::Added));
            (change.source, change.target)
        })
        .collect::<Vec<_>>();
    added.sort_unstable();
    let mut expected = lost
        .into_iter()
        .flat_map(|other| [(node, other), (other, node)])
        .collect::<Vec<_>>();
    expected.sort_unstable();
    assert_eq!(added, expected);
    assert_graph_contract(&graph);
}

/// A link row ascends whatever order its links arrive in, holds a node once,
/// and never exceeds its degree.
#[test]
fn link_rows_ascend_and_hold_a_node_once() {
    let rows = LinkRows::new(2, 4);
    let mut row = rows.lock(1);
    [7, 2, 9, 4].into_iter().for_each(|node| row.push(node));
    assert_eq!(row.iter().collect::<Vec<_>>(), [2, 4, 7, 9]);
    assert!(row.remove(4) && !row.remove(4));
    assert_eq!(row.iter().collect::<Vec<_>>(), [2, 7, 9]);
    row.push(0);
    assert_eq!(row.iter().collect::<Vec<_>>(), [0, 2, 7, 9]);
    assert!(row.remove(9) && row.remove(0));
    assert_eq!(row.iter().collect::<Vec<_>>(), [2, 7]);
    row.replace(&[1, 3, 5]);
    assert_eq!(
        (row.len(), row.contains(3), row.contains(2)),
        (3, true, false)
    );
    drop(row);
    assert!(rows.lock(0).is_empty());

    let panics = |change: fn(&mut LinkRow<'_>)| {
        let rows = LinkRows::new(1, 2);
        let mut row = rows.lock(0);
        row.replace(&[3, 5]);
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| change(&mut row))).is_err()
    };
    assert!(panics(|row| row.push(5)), "a held node");
    assert!(panics(|row| row.push(4)), "a full row");
    assert!(panics(|row| row.replace(&[5, 3])), "descending links");
    assert!(panics(|row| row.replace(&[3, 3])), "a repeated link");
    assert!(
        panics(|row| row.replace(&[1, 2, 3])),
        "more links than the degree"
    );
    assert!(!panics(|row| row.replace(&[])));
}

/// Vectors are held in chunks of at most [`CHUNK_BYTES`], or of one vector
/// when one is larger, and each is read back from the chunk and offset its
/// ordinal maps to.
#[test]
fn vectors_are_read_back_across_bounded_chunks() {
    for (dimension, count, chunks) in [(65_536, 20, 3), (600_000, 3, 3), (3, 20, 1)] {
        let vector = |node: usize| {
            (0..dimension)
                .map(|component| (node * 7 + component % 5) as f32)
                .collect::<Vec<_>>()
        };
        let mut vectors = BulkVectors::<Euclidean>::new(dimension);
        (0..count).for_each(|node| vectors.push(node as NodeId + 1, &vector(node), 0));
        assert_eq!(vectors.chunks.len(), chunks, "dimension {dimension}");
        assert!(vectors.chunks.iter().all(|chunk| {
            chunk.capacity() * size_of::<f32>() <= CHUNK_BYTES.max(dimension * size_of::<f32>())
        }));
        (0..count).for_each(|node| {
            assert!(
                vectors.item(node as u32).vector.to_vec() == vector(node),
                "dimension {dimension}, vector {node}"
            )
        });
    }
}

/// The per-vector estimate the memory budget uses covers what a build holds.
#[test]
fn memory_estimate_covers_the_held_bytes() {
    let dimension = 128;
    let config = config(dimension, 16);
    let source = source(13, 20_000, dimension, config.ml);
    let parameters = BulkParameters::try_from_config(&config).unwrap();
    let graph = built::<Euclidean>(&source, &config, THREADS);
    let held = graph
        .vectors
        .chunks
        .iter()
        .map(|chunk| chunk.capacity() * size_of::<f32>())
        .sum::<usize>()
        + graph.vectors.headers.len() * size_of::<<Euclidean as Distance>::Header>()
        + graph.vectors.entity_ids.len() * size_of::<NodeId>()
        + graph.vectors.source_hashes.len() * size_of::<u64>()
        + graph.vectors.levels.len()
        + graph.unlinked.len()
        + graph.upper_slots.len() * size_of::<u32>()
        + [&graph.layer0, &graph.upper]
            .into_iter()
            .map(|rows| {
                rows.slots.len() * size_of::<AtomicU32>()
                    + rows.locks.len() * size_of::<Mutex<()>>()
            })
            .sum::<usize>();
    let estimate = parameters.bytes_per_vector::<Euclidean>(dimension) * 20_000;
    // The last vector chunk is allocated whole, and upper rows follow the
    // drawn layers; both stay within two percent at this size.
    assert!(
        (held as u64).abs_diff(estimate) * 50 <= estimate,
        "held {held}, estimated {estimate}"
    );
    assert_eq!(
        parameters.bytes_per_vector::<Euclidean>(dimension),
        (dimension * 4
            + size_of::<<Euclidean as Distance>::Header>()
            + (8 + 8 + 1 + 1 + 4)
            + (32 * 4 + 1)
            + (16 * 4 + 1_usize).div_ceil(15)) as u64
    );
    // A layer multiplier that bounds no upper rows saturates, so no budget
    // admits the vector.
    assert_eq!(
        BulkParameters::try_from_config(&config.with_ml(f32::MAX))
            .unwrap()
            .bytes_per_vector::<Euclidean>(dimension),
        u64::MAX
    );
}

/// A raised cancellation flag ends construction without a graph.
#[test]
fn cancellation_ends_construction_without_a_graph() {
    let config = config(3, 16);
    let source = source(1, 500, 3, config.ml);
    assert!(matches!(
        BulkGraph::construct(
            vectors::<Euclidean>(&source),
            BulkParameters::try_from_config(&config).unwrap(),
            THREADS,
            &AtomicBool::new(true),
            &AtomicU64::new(0),
        ),
        Ok(BulkConstruction::Cancelled)
    ));
}

/// A distance the kernel cannot score fails the build as it fails an insert.
#[test]
fn an_invalid_distance_fails_construction() {
    let config = config(2, 16);
    let source = [
        (1, vec![f32::MAX, f32::MAX], 0),
        (2, vec![f32::MIN, f32::MIN], 0),
    ];
    assert!(matches!(
        BulkGraph::construct(
            vectors::<Euclidean>(&source),
            BulkParameters::try_from_config(&config).unwrap(),
            THREADS,
            &AtomicBool::new(false),
            &AtomicU64::new(0),
        ),
        Err(HelixDbError::InvariantViolation(message))
            if message.contains("invalid score")
    ));
}

/// Degree settings the serial insert rejects are rejected before a build.
#[test]
fn invalid_degree_settings_are_rejected() {
    assert!(matches!(
        BulkParameters::try_from_config(&config(3, 0)),
        Err(HelixDbError::InvalidVectorConfig(_))
    ));
}

/// Source hashes compare vectors byte for byte.
#[test]
fn source_hash_distinguishes_bytes() {
    let hash = BulkVectors::<Euclidean>::source_hash;
    assert_eq!(hash(&[1.0, 2.0]), hash(&[1.0, 2.0]));
    assert_ne!(hash(&[0.0, 2.0]), hash(&[-0.0, 2.0]));
    assert_ne!(hash(&[1.0, 2.0]), hash(&[2.0, 1.0]));
}

/// Positions resolve entity IDs, and the gap an absent ID falls into.
#[test]
fn positions_resolve_entities_and_gaps() {
    let config = config(3, 16);
    let graph = built::<Euclidean>(&source(1, 5, 3, config.ml), &config, ONE_THREAD);
    assert_eq!(graph.position(30), Ok(2));
    assert_eq!(graph.position(31), Err(3));
    assert_eq!(graph.position(1), Err(0));
    assert_eq!(graph.position(51), Err(5));
}

#[test]
#[should_panic(expected = "ascending entity order")]
fn vectors_out_of_source_order_are_rejected() {
    let mut vectors = BulkVectors::<Euclidean>::new(1);
    vectors.push(2, &[0.0], 0);
    vectors.push(2, &[1.0], 0);
}

/// A stored node carries the vector passed to the writer, validated as an
/// insert validates it.
#[tokio::test]
async fn staging_rejects_a_vector_of_another_dimension() {
    let config = config(3, 16);
    let source = source(1, 5, 3, config.ml);
    let graph = built::<Euclidean>(&source, &config, ONE_THREAD);
    let index = index::<Euclidean>(3);
    let db = database("bulk-graph-dimension").await;
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&transaction);
    assert!(index
        .stage_bulk_node(&measured, &graph, 0, &[1.0, 2.0])
        .is_err());
    assert!(index
        .stage_bulk_node(&measured, &graph, 0, &[1.0, f32::NAN, 2.0])
        .is_err());
}

/// A graph with every node unlinked stages no metadata row.
#[tokio::test]
async fn an_emptied_graph_stages_no_metadata() {
    let config = config(3, 16);
    let source = source(1, 2, 3, config.ml);
    let mut graph = built::<Euclidean>(&source, &config, ONE_THREAD);
    let mut changes = Vec::new();
    graph.unlink(0, &mut changes).unwrap();
    graph.unlink(1, &mut changes).unwrap();
    let index = index::<Euclidean>(3);
    let db = database("bulk-graph-emptied").await;
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&transaction);
    index
        .stage_bulk_metadata(&measured, &graph, config)
        .await
        .unwrap();
    assert_eq!(measured.measurement().unwrap().operations(), 0);
}

/// Entities a stored graph holds: each entity's vector and layer.
type Live = BTreeMap<NodeId, (Vec<f32>, u16)>;

/// Whether every stored link has its reverse. A build leaves a symmetric
/// graph; the serial delete repairs the rows that named the deleted node with
/// links their targets do not return.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Links {
    Symmetric,
    Repaired,
}

/// Asserts that the rows stored for `index` are a well-formed graph over
/// exactly `live`, read the way the serial insert, delete and search read
/// them: metadata that counts the entities and names an entry point on the top
/// layer, one neighbor row per occupied layer and none above, rows that ascend
/// within their degree limit and link only live nodes of their layer, exactly
/// one reverse locator per link, the entry-candidate and upper-vector rows of
/// each entity's layer, and nothing left of the entities in `gone`.
async fn assert_stored_graph_contract<D: Distance>(
    db: &slatedb::Db,
    index: &VectorIndex<D>,
    config: &VectorIndexConfig,
    live: &Live,
    gone: &[NodeId],
    expected: Links,
) {
    use crate::search::vector::storage::EntryCandidateLayerRow;

    let rows = VectorRows::new(db, index.row_keyspace());
    let parameters = BulkParameters::try_from_config(config).unwrap();
    let metadata = rows.metadata().await.unwrap().expect("a stored graph");
    let top = live.values().map(|(_, layer)| *layer).max().unwrap();
    assert_eq!(metadata.count, live.len() as u64);
    assert_eq!(metadata.max_layer, top);
    assert_eq!(live[&metadata.entry_point.unwrap()].1, top);

    let mut links = BTreeMap::<(u16, NodeId), Vec<NodeId>>::new();
    for (&entity, (_, level)) in live {
        assert!(rows.layer0_row_exists(entity).await.unwrap());
        for layer in 0..=*level {
            let row = match layer {
                0 => rows.layer0_neighbors(entity).await.unwrap(),
                layer => rows
                    .upper_neighbors(layer, entity)
                    .await
                    .unwrap()
                    .expect("a row per occupied layer"),
            };
            links.insert((layer, entity), row);
        }
        assert!(rows
            .upper_neighbors(level + 1, entity)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            rows.entry_candidate_layer(entity).await.unwrap(),
            EntryCandidateLayerRow::Present(*level)
        );
        assert_eq!(
            rows.upper_vector_row(entity).await.unwrap().is_some(),
            *level > 0
        );
    }
    // Sources of every link, by layer and target, ascending as locators are.
    let mut sources = BTreeMap::<(u16, NodeId), Vec<NodeId>>::new();
    for (&(layer, entity), row) in &links {
        assert!(
            row.len() <= parameters.degree(layer as u8),
            "degree of {entity} on layer {layer}"
        );
        assert!(row.windows(2).all(|pair| pair[0] < pair[1]), "row ascends");
        for neighbor in row {
            assert_ne!(*neighbor, entity, "no self link");
            let reverse = links.get(&(layer, *neighbor)).unwrap_or_else(|| {
                panic!("link {entity}->{neighbor} on layer {layer} names a live node")
            });
            assert!(
                expected == Links::Repaired || reverse.contains(&entity),
                "link {entity}->{neighbor} on layer {layer} has its reverse"
            );
            sources.entry((layer, *neighbor)).or_default().push(entity);
        }
    }
    for (&entity, (_, level)) in live {
        let locators = rows.reverse_sources_for_target(entity).await.unwrap();
        (0..=*level).for_each(|layer| {
            assert_eq!(
                locators.sources_at(layer),
                sources.get(&(layer, entity)).map_or(&[][..], Vec::as_slice),
                "locators of {entity} on layer {layer}"
            )
        });
        assert!(locators
            .sources_by_layer()
            .iter()
            .all(|(layer, sources)| layer <= level && !sources.is_empty()));
    }
    for &entity in gone {
        assert!(!rows.layer0_row_exists(entity).await.unwrap());
        assert_eq!(
            rows.entry_candidate_layer(entity).await.unwrap(),
            EntryCandidateLayerRow::Missing
        );
        assert!(rows.upper_vector_row(entity).await.unwrap().is_none());
        assert!(rows
            .reverse_sources_for_target(entity)
            .await
            .unwrap()
            .sources_by_layer()
            .is_empty());
    }
}

/// One write queue publication applies to an active index.
enum SerialWrite {
    Upsert(NodeId, Vec<f32>, u16),
    Delete(NodeId),
}

/// Applies `writes` in order through the serial insert and delete, as queue
/// publication does after activation, in one commit.
async fn write_serially<D: Distance>(
    db: &slatedb::Db,
    index: &VectorIndex<D>,
    writes: &[SerialWrite],
) {
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&transaction);
    let mut session = VectorBuildSession::new(NonZeroU64::new(1 << 26).unwrap());
    for write in writes {
        match write {
            SerialWrite::Upsert(entity, vector, layer) => index
                .stage_upsert_at_layer_with_session(
                    &measured,
                    *entity,
                    vector,
                    *layer,
                    &mut session,
                )
                .await
                .unwrap(),
            SerialWrite::Delete(entity) => index
                .stage_delete_with_build_session(&measured, *entity, &mut session)
                .await
                .unwrap(),
        }
        session.flush_all(&measured).unwrap();
        session.admit_entity();
    }
    transaction.commit().await.unwrap();
}

/// Entities of `live` a search for their own vector returns among ten.
async fn found_by_own_vector<D: Distance>(
    db: &slatedb::Db,
    index: &VectorIndex<D>,
    live: &Live,
) -> usize {
    let params = SearchParams::new(10).unwrap();
    let mut found = 0;
    for (entity, (vector, _)) in live {
        found += usize::from(
            index
                .search(db, vector, &params)
                .await
                .unwrap()
                .iter()
                .any(|result| result.entity_id() == *entity),
        );
    }
    found
}

/// The serial insert and delete that run after activation keep a graph built
/// concurrently as well formed as one they built themselves. Both stores
/// start symmetric; after the same inserts between and beyond the built
/// entities, replacements, deletes of the entry point and every other
/// top-layer node, and reinsertions, both hold a graph the serial reader
/// accepts, with no link to a deleted entity and one locator per link, and
/// the bulk-built one finds its entities as well as the serial one.
#[tokio::test(flavor = "multi_thread")]
async fn serial_writes_keep_a_concurrently_built_graph_well_formed() {
    let dimension = 8;
    let config = config(dimension, 6);
    let mut source = source(41, 4_000, dimension, config.ml);
    // Exact copies among the built entities.
    (0..source.len())
        .step_by(10)
        .skip(1)
        .for_each(|copy| source[copy].1 = source[copy - 7].1.clone());
    let index = index::<Euclidean>(dimension);
    let config = VectorIndexConfig {
        index_name: index.name().to_string(),
        ..config
    };
    let graph = built::<Euclidean>(&source, &config, THREADS);
    assert_graph_contract(&graph);
    let bulk = database("bulk-graph-host").await;
    store(&bulk, &index, &graph, &source, &config).await;
    let serial = database("bulk-graph-host-serial").await;
    store_serially(&serial, &index, &source, &config).await;

    let mut live = source
        .iter()
        .map(|(entity, vector, layer)| (*entity, (vector.clone(), *layer)))
        .collect::<Live>();
    for database in [&bulk, &serial] {
        assert_stored_graph_contract(database, &index, &config, &live, &[], Links::Symmetric).await;
    }

    let mut rng = StdRng::seed_from_u64(43);
    let vector = |rng: &mut StdRng| {
        (0..dimension)
            .map(|_| rng.random_range(-1.0_f32..1.0))
            .collect::<Vec<_>>()
    };
    let top = source.iter().map(|(_, _, layer)| *layer).max().unwrap();
    // New entities between the built ones and above them.
    let inserted = (0..600_u64)
        .map(|new| {
            let layer = vector::select_layer(config.ml, &mut rng);
            SerialWrite::Upsert(new * 71 + 5, vector(&mut rng), layer)
        })
        .collect::<Vec<_>>();
    let replaced = source
        .iter()
        .skip(3)
        .step_by(17)
        .map(|(entity, _, layer)| SerialWrite::Upsert(*entity, vector(&mut rng), *layer))
        .collect::<Vec<_>>();
    let deleted = source
        .iter()
        .enumerate()
        .filter(|(position, (_, _, layer))| *layer == top || position % 9 == 4)
        .map(|(_, (entity, _, _))| *entity)
        .collect::<Vec<_>>();
    let reinserted = deleted
        .iter()
        .step_by(5)
        .map(|entity| {
            let layer = live[entity].1;
            SerialWrite::Upsert(*entity, vector(&mut rng), layer)
        })
        .collect::<Vec<_>>();
    let mut writes = inserted
        .into_iter()
        .chain(replaced)
        .chain(deleted.iter().map(|entity| SerialWrite::Delete(*entity)))
        .collect::<Vec<_>>();
    writes.sort_by_cached_key(|_| rng.random::<u32>());
    writes.extend(reinserted);

    let mut gone = BTreeSet::new();
    for write in &writes {
        match write {
            SerialWrite::Upsert(entity, vector, layer) => {
                live.insert(*entity, (vector.clone(), *layer));
                gone.remove(entity);
            }
            SerialWrite::Delete(entity) => {
                live.remove(entity);
                gone.insert(*entity);
            }
        }
    }
    let gone = gone.into_iter().collect::<Vec<_>>();
    assert!(deleted.len() > 400 && gone.len() < deleted.len());

    let mut found = [0_usize; 2];
    for (database, found) in [&bulk, &serial].into_iter().zip(found.iter_mut()) {
        write_serially(database, &index, &writes).await;
        assert_stored_graph_contract(database, &index, &config, &live, &gone, Links::Repaired)
            .await;
        *found = found_by_own_vector(database, &index, &live).await;
    }
    let [bulk_found, serial_found] = found;
    assert!(
        bulk_found * 200 >= serial_found * 199,
        "bulk-built host finds {bulk_found} of {}, serial-built {serial_found}",
        live.len()
    );
    assert!(
        serial_found * 100 >= live.len() * 98,
        "serial-built host finds {serial_found} of {}",
        live.len()
    );
}

/// Sources of a few nodes, with fewer distinct vectors than nodes, more
/// threads than nodes, and the smallest degrees, keep the contract under
/// every metric: these are the builds in which every insert races the first
/// ones, most candidates tie, and pruning leaves the most nodes to re-link.
/// Unlinking each graph to nothing keeps it at every step.
#[test]
fn small_sources_of_copies_built_on_many_threads_keep_the_contract() {
    fn check<D: Distance>(
        source: &[(NodeId, Vec<f32>, u16)],
        config: &VectorIndexConfig,
        seed: u64,
    ) {
        for threads in [2, 8, 32] {
            let mut graph = built::<D>(source, config, NonZeroUsize::new(threads).unwrap());
            assert_graph_contract(&graph);
            let mut rng = StdRng::seed_from_u64(seed);
            let mut order = (0..graph.len() as u32).collect::<Vec<_>>();
            order.sort_by_cached_key(|_| rng.random::<u32>());
            let mut changes = Vec::new();
            for node in order {
                graph.unlink(node, &mut changes).unwrap();
                assert_graph_contract(&graph);
            }
        }
    }
    for seed in 0..240_u64 {
        let count = 1 + seed as usize % 80;
        let dimension = 1 + seed as usize % 3;
        let config = config(dimension, 2 + seed as usize % 3);
        let mut rng = StdRng::seed_from_u64(seed);
        let mut source = source(seed, count, dimension, config.ml);
        // Components drawn from three positive values: copies, ties, and for
        // cosine, distinct vectors at distance zero.
        source.iter_mut().for_each(|(_, vector, _)| {
            vector
                .iter_mut()
                .for_each(|component| *component = f32::from(rng.random_range(1_u8..4)))
        });
        check::<Euclidean>(&source, &config, seed);
        check::<Cosine>(&source, &config, seed);
    }
}
