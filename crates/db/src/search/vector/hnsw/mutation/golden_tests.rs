//! Golden digests of seeded graph mutation workloads.
//!
//! Each scenario plans a seeded mix of fresh inserts, upserts that move a
//! node, exact replays, layer changes, duplicated vectors, and deletes of
//! present and absent nodes, committing in batches. After every commit it
//! digests every committed row (items, SimHashes, neighbor rows, reverse
//! locators, entry candidates, metadata) and the results of fixed searches.
//! The digests were recorded before mutation planning reused scratch buffers,
//! so any change to the rows a workload stores, or to what its graph returns,
//! fails here.
//!
//! Vectors hold small integers, so every distance is exact in `f32` whatever
//! order a kernel sums in (a cosine norm always rounds to its fixed scalar
//! reference's bits, and its dot product is exact), and SimHash projections
//! are scalar: the digests hold on every architecture and under
//! `force-vector-scalar-kernel`.
//!
//! A workload may run several namespaces in one database, interleaving their
//! operations through one shared build session, so the scratch a session lends
//! moves between namespaces on every operation.

use std::collections::BTreeMap;
use std::sync::Arc;

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use sha2::{Digest, Sha256};
use slatedb::object_store::memory::InMemory;
use slatedb::IsolationLevel;

use super::*;
use crate::encoding::v2::keys::scope::DataScope;
use crate::index_lifecycle::IndexElementKind;
use crate::search::vector::distance::{Cosine, Euclidean, Manhattan};
use crate::search::vector::{
    SearchParams, SimHashMode, ValidatedVectorGenerationHandle, VectorIndexConfig,
};

/// Node IDs a workload draws from, so upserts and deletes revisit nodes.
const NODES: NodeId = 120;
/// Operations one workload plans.
const OPERATIONS: usize = 480;
/// Operations committed together.
const OPERATIONS_PER_COMMIT: usize = 24;
/// Not a multiple of any SIMD width, so kernels take their remainder paths.
const DIMENSION: usize = 12;

/// How one workload plans its operations.
#[derive(Debug, Clone, Copy)]
enum Planner {
    /// One build session retained across commits, as publication retains one,
    /// bounded so tightly that most entries are evicted between entities.
    TinySession,
    /// One build session whose budget never evicts.
    LargeSession,
    /// A cache per operation, as direct index writes plan.
    OneOff,
    /// Fresh inserts only, through one bounded session, as backfill plans.
    Backfill,
}

impl Planner {
    fn session<D: Distance>(self) -> VectorBuildSession<D> {
        match self {
            Self::TinySession | Self::Backfill => {
                VectorBuildSession::with_test_limits(NonZeroU64::new(1 << 15).unwrap(), 32, 24, 32)
            }
            Self::LargeSession | Self::OneOff => {
                VectorBuildSession::new(NonZeroU64::new(1 << 34).unwrap())
            }
        }
    }
}

/// One planned mutation.
#[derive(Debug, Clone)]
enum Operation {
    Upsert {
        node: NodeId,
        vector: Vec<f32>,
        layer: u16,
    },
    Delete(NodeId),
}

fn random_vector(rng: &mut StdRng) -> Vec<f32> {
    (0..DIMENSION)
        .map(|_| f32::from(rng.random_range(-3..=3_i8)))
        .collect()
}

/// A layer of at most 3, mostly 0.
fn random_layer(rng: &mut StdRng) -> u16 {
    [0, 0, 0, 0, 0, 0, 1, 1, 2, 3][rng.random_range(0..10_usize)]
}

/// Draws the next operation against the `live` nodes and applies it there.
fn next_operation(
    rng: &mut StdRng,
    live: &mut BTreeMap<NodeId, (Vec<f32>, u16)>,
    fresh_only: bool,
) -> Operation {
    let pick_live = |rng: &mut StdRng, live: &BTreeMap<NodeId, (Vec<f32>, u16)>| {
        let nodes = live.keys().copied().collect::<Vec<_>>();
        (!nodes.is_empty()).then(|| nodes[rng.random_range(0..nodes.len())])
    };
    let operation = if fresh_only {
        let node = NodeId::try_from(live.len()).unwrap() + 1;
        Operation::Upsert {
            node,
            vector: random_vector(rng),
            layer: random_layer(rng),
        }
    } else {
        match (rng.random_range(0..20_u8), pick_live(rng, live)) {
            (11..=12, Some(node)) => {
                let (vector, layer) = live[&node].clone();
                Operation::Upsert {
                    node,
                    vector,
                    layer,
                }
            }
            (13, Some(node)) => {
                let (vector, layer) = live[&node].clone();
                Operation::Upsert {
                    node,
                    vector,
                    layer: (layer + 1) % 4,
                }
            }
            (14..=16, _) => Operation::Delete(rng.random_range(1..=NODES)),
            (17, Some(source)) => Operation::Upsert {
                node: rng.random_range(1..=NODES),
                vector: live[&source].0.clone(),
                layer: random_layer(rng),
            },
            _ => Operation::Upsert {
                node: rng.random_range(1..=NODES),
                vector: random_vector(rng),
                layer: random_layer(rng),
            },
        }
    };
    match &operation {
        Operation::Upsert {
            node,
            vector,
            layer,
        } => {
            live.insert(*node, (vector.clone(), *layer));
        }
        Operation::Delete(node) => {
            live.remove(node);
        }
    }
    operation
}

/// Creates the empty generation namespace `name`, of small degree, in `db`.
///
/// `ordinal` distinguishes the namespaces of one database.
async fn create<D: Distance>(db: &slatedb::Db, name: &str, ordinal: u64) -> VectorIndex<D> {
    let identity = VectorGenerationIdentity::try_new(
        DataScope::LegacyUnscoped,
        7 + ordinal,
        name.to_string(),
        91 + ordinal,
        NonZeroU64::new(3).unwrap(),
        11,
        IndexElementKind::Node,
        VectorDimension::try_new(DIMENSION).unwrap(),
    )
    .unwrap();
    let index = VectorIndex::<D>::from_generation(
        &ValidatedVectorGenerationHandle::create_current::<D>(identity).unwrap(),
    );
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    index
        .stage_create(
            &MeasuredVectorTransaction::new(&txn),
            VectorIndexConfig::new(index.name(), "embedding", DIMENSION)
                .with_m(4)
                .with_m0(8)
                .with_ef_construction(24),
        )
        .await
        .unwrap();
    txn.commit().await.unwrap();
    index
}

/// Folds every committed row, then each namespace's fixed search results,
/// into `digest`.
async fn digest_state<D: Distance>(
    db: &slatedb::Db,
    namespaces: &[Namespace<D>],
    digest: &mut Sha256,
) {
    let snapshot = db.snapshot().await.unwrap();
    let mut rows = snapshot.scan(..).await.unwrap();
    while let Some(row) = rows.next().await.unwrap() {
        digest.update(u64::try_from(row.key.len()).unwrap().to_be_bytes());
        digest.update(&row.key);
        digest.update(u64::try_from(row.value.len()).unwrap().to_be_bytes());
        digest.update(&row.value);
    }
    let parameters = SearchParams::new(8)
        .unwrap()
        .with_ef(32)
        .unwrap()
        .with_simhash_mode(SimHashMode::Off);
    for namespace in namespaces {
        for query in &namespace.queries {
            let results = namespace
                .index
                .search(snapshot.as_ref(), query, &parameters)
                .await
                .unwrap();
            digest.update(u64::try_from(results.len()).unwrap().to_be_bytes());
            for result in results {
                digest.update(result.entity_id().to_be_bytes());
                digest.update(result.score().get().to_bits().to_be_bytes());
            }
        }
    }
}

/// Folds a session's telemetry and retained footprint into `digest`.
fn digest_session<D: Distance>(session: &VectorBuildSession<D>, digest: &mut Sha256) {
    let stats = session.stats();
    for counter in [
        stats.item_hits(),
        stats.item_misses(),
        stats.neighbor_hits(),
        stats.neighbor_misses(),
        stats.simhash_hits(),
        stats.simhash_misses(),
        stats.item_evictions(),
        stats.neighbor_evictions(),
        stats.simhash_evictions(),
        stats.dirty_neighbor_flushes(),
        stats.max_retained_payload_bytes(),
    ]
    .into_iter()
    .chain(
        [
            session.item_count(),
            session.neighbor_count(),
            session.simhash_count(),
            session.retained_bytes().unwrap(),
        ]
        .map(|count| u64::try_from(count).unwrap()),
    ) {
        digest.update(counter.to_be_bytes());
    }
}

/// Hex digests of one workload: its committed rows and search results, and
/// its build session's cache behavior after every operation.
struct Digests {
    rows: String,
    cache: String,
}

fn hex(digest: Sha256) -> String {
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// One namespace of a workload, driven by its own seeded operations.
struct Namespace<D: Distance> {
    index: VectorIndex<D>,
    rng: StdRng,
    queries: Vec<Vec<f32>>,
    live: BTreeMap<NodeId, (Vec<f32>, u16)>,
}

/// Runs one seeded workload of a namespace per seed in one database, and
/// returns its digests. Each step plans one operation per namespace, in seed
/// order, through one build session shared by every namespace.
async fn run<D: Distance>(seeds: &[u64], planner: Planner) -> Digests {
    // Planners of one seed share a namespace, so their digests compare.
    let name = |seed: u64| format!("golden-{}-{seed}", D::name());
    let db = slatedb::Db::open(name(seeds[0]), Arc::new(InMemory::new()))
        .await
        .unwrap();
    let mut namespaces = Vec::new();
    for (ordinal, &seed) in (0..).zip(seeds) {
        let index = create::<D>(&db, &name(seed), ordinal).await;
        let mut rng = StdRng::seed_from_u64(seed);
        let queries = (0..6).map(|_| random_vector(&mut rng)).collect();
        namespaces.push(Namespace {
            index,
            rng,
            queries,
            live: BTreeMap::new(),
        });
    }
    let mut session = planner.session::<D>();
    let mut digest = Sha256::new();
    let mut cache_digest = Sha256::new();
    let fresh_only = matches!(planner, Planner::Backfill);
    let operations = if fresh_only {
        usize::try_from(NODES).unwrap()
    } else {
        OPERATIONS
    };
    for batch in 0..operations.div_ceil(OPERATIONS_PER_COMMIT) {
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&txn);
        let batch_end = operations.min((batch + 1) * OPERATIONS_PER_COMMIT);
        for _ in batch * OPERATIONS_PER_COMMIT..batch_end {
            for Namespace {
                index, rng, live, ..
            } in &mut namespaces
            {
                let operation = next_operation(rng, live, fresh_only);
                match (planner, operation) {
                    (
                        Planner::Backfill,
                        Operation::Upsert {
                            node,
                            vector,
                            layer,
                        },
                    ) => index
                        .stage_known_fresh_at_layer_with_session(
                            &measured,
                            node,
                            &vector,
                            layer,
                            FreshVectorBuildProof::for_test(),
                            &mut session,
                        )
                        .await
                        .unwrap(),
                    (
                        Planner::OneOff,
                        Operation::Upsert {
                            node,
                            vector,
                            layer,
                        },
                    ) => index
                        .stage_upsert_at_layer(&measured, node, &vector, layer)
                        .await
                        .unwrap(),
                    (Planner::OneOff, Operation::Delete(node)) => {
                        index.stage_delete(&measured, node).await.unwrap();
                    }
                    (
                        _,
                        Operation::Upsert {
                            node,
                            vector,
                            layer,
                        },
                    ) => index
                        .stage_upsert_at_layer_with_session(
                            &measured,
                            node,
                            &vector,
                            layer,
                            &mut session,
                        )
                        .await
                        .unwrap(),
                    (_, Operation::Delete(node)) => index
                        .stage_delete_with_build_session(&measured, node, &mut session)
                        .await
                        .unwrap(),
                }
                session.flush_all(&measured).unwrap();
                session.enforce_limits(&measured).unwrap();
                session.admit_entity();
                digest_session(&session, &mut cache_digest);
            }
        }
        txn.commit().await.unwrap();
        digest_state(&db, &namespaces, &mut digest).await;
    }
    db.close().await.unwrap();
    Digests {
        rows: hex(digest),
        cache: hex(cache_digest),
    }
}

/// Asserts every `(label, actual)` digest equals its recorded golden value,
/// reporting all of them at once so a deliberate change can update them.
fn assert_golden(kind: &str, actual: &[(&str, String)], expected: &[(&str, &str)]) {
    assert_eq!(
        actual.iter().map(|(label, _)| *label).collect::<Vec<_>>(),
        expected.iter().map(|(label, _)| *label).collect::<Vec<_>>()
    );
    assert!(
        actual
            .iter()
            .zip(expected)
            .all(|((_, digest), (_, expected))| digest == expected),
        "{kind} digests changed: {actual:#?}"
    );
}

/// Runs every `(label, seeds, planner)` workload and asserts its row digest,
/// and its session cache digest when `cache` lists the label.
///
/// Only a cache that never evicts, or a workload that never deletes, has a
/// deterministic cache digest: deletion relinking reads candidates in hash
/// order, which orders their cache touches and so what a tight budget evicts
/// (never what is stored, which the row digests pin).
async fn assert_workloads<D: Distance>(
    workloads: &[(&str, &[u64], Planner)],
    rows: &[(&str, &str)],
    cache: &[(&str, &str)],
) {
    let mut actual_rows = Vec::new();
    let mut actual_cache = Vec::new();
    for &(label, seeds, planner) in workloads {
        let digests = run::<D>(seeds, planner).await;
        actual_rows.push((label, digests.rows));
        if cache.iter().any(|(cached, _)| *cached == label) {
            actual_cache.push((label, digests.cache));
        }
    }
    assert_golden("row", &actual_rows, rows);
    assert_golden("cache", &actual_cache, cache);
}

/// Every planner of one seed stores the same rows: eviction and cache
/// ownership never change what a mutation plans.
const EUCLIDEAN_SEED_1: &str = "8a8268fc073bdc4a82d573624a62767ba4275dd83cab24b312160a49f2b66090";
const MANHATTAN_SEED_4: &str = "f0d2b874474a7b5e4c1df05e2b97447ee257eec8aaeb918d7cc42030c7f9d485";
const COSINE_SEED_6: &str = "0d715c730a137e4d0c15e255e965c3f367a27c8e0100f7dcb34451ff0cc2d680";
const EUCLIDEAN_SEEDS_1_8: &str =
    "2d11a658df45606174a74f6ff28e43ed3e0c829e23e036a430afa459c8c1890f";

/// Session cache behavior (hits, misses, evictions, flushes, and retained
/// footprint after every operation) is pinned too, where it is deterministic:
/// buffer reuse must not change what is cached, touched, or evicted.
#[tokio::test(flavor = "multi_thread")]
async fn seeded_euclidean_workloads_keep_golden_rows_and_cache_behavior() {
    assert_workloads::<Euclidean>(
        &[
            ("tiny", &[1], Planner::TinySession),
            ("large", &[1], Planner::LargeSession),
            ("one-off", &[1], Planner::OneOff),
            ("backfill", &[2], Planner::Backfill),
            ("tiny-3", &[3], Planner::TinySession),
        ],
        &[
            ("tiny", EUCLIDEAN_SEED_1),
            ("large", EUCLIDEAN_SEED_1),
            ("one-off", EUCLIDEAN_SEED_1),
            (
                "backfill",
                "a03743504040462fad25621372320c0c20530d3669d87eb2d7c7c9ed9d1a86e7",
            ),
            (
                "tiny-3",
                "262debfb7b3ffd696a8381955784d1a0a92554b38806c53f37a35368904d215d",
            ),
        ],
        &[
            (
                "large",
                "d6463777480b42ee26e3bbcfa0f4f3d6bf6984623ff87d1b24b855c1ef042ccf",
            ),
            (
                "backfill",
                "aa05ee91553659bfe819929f34ebe00cb74cb6776d6949a1bbe80abd93332fcc",
            ),
        ],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn seeded_manhattan_workloads_keep_golden_rows_and_cache_behavior() {
    assert_workloads::<Manhattan>(
        &[
            ("tiny", &[4], Planner::TinySession),
            ("one-off", &[4], Planner::OneOff),
            ("backfill", &[5], Planner::Backfill),
        ],
        &[
            ("tiny", MANHATTAN_SEED_4),
            ("one-off", MANHATTAN_SEED_4),
            (
                "backfill",
                "62acf336d1752c0f2ab1fa659716895a7333a00cdc36144258e5ab544501643c",
            ),
        ],
        &[(
            "backfill",
            "c6e2550acb0d3f934cc249d789605fd5eb96f6fcd7e5fbaefc5c8546425605eb",
        )],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn seeded_cosine_workloads_keep_golden_rows_and_cache_behavior() {
    assert_workloads::<Cosine>(
        &[
            ("tiny", &[6], Planner::TinySession),
            ("large", &[6], Planner::LargeSession),
            ("one-off", &[6], Planner::OneOff),
            ("backfill", &[7], Planner::Backfill),
        ],
        &[
            ("tiny", COSINE_SEED_6),
            ("large", COSINE_SEED_6),
            ("one-off", COSINE_SEED_6),
            (
                "backfill",
                "10417445e00bba9cafaa87d7138f624fd922e634a2e116a232c2b8bd44ea2e12",
            ),
        ],
        &[
            (
                "large",
                "6839fdfe85b773d1eeb29e7d138429ccca187028b760cf450f0946cd4b7b474f",
            ),
            (
                "backfill",
                "5d0982ce195feee4bf592b15c67512da59eefd84f942acd508ec1fbd0ff9c732",
            ),
        ],
    )
    .await;
}

/// Namespaces interleaved through one session store what they store when
/// every operation plans through its own cache: the scratch a session lends
/// moving between namespaces never changes what either plans.
#[tokio::test(flavor = "multi_thread")]
async fn namespaces_sharing_a_session_keep_golden_rows_and_cache_behavior() {
    assert_workloads::<Euclidean>(
        &[
            ("tiny", &[1, 8], Planner::TinySession),
            ("large", &[1, 8], Planner::LargeSession),
            ("one-off", &[1, 8], Planner::OneOff),
            ("backfill", &[2, 9], Planner::Backfill),
        ],
        &[
            ("tiny", EUCLIDEAN_SEEDS_1_8),
            ("large", EUCLIDEAN_SEEDS_1_8),
            ("one-off", EUCLIDEAN_SEEDS_1_8),
            (
                "backfill",
                "53c1bdd03b731e3f50b155b431287c1a3ed44cee199388a9c77ca52de870bd8d",
            ),
        ],
        &[
            (
                "large",
                "5043efc97d1aa1906f2c618e5b8556864880928b3879aa2adaa1d3365b4049a0",
            ),
            (
                "backfill",
                "b211ab321baf0a5a8f9e6f3f1a1f2fc02d03e0674307e6b053956bb3460cef12",
            ),
        ],
    )
    .await;
}
