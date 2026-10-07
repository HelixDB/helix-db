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
//! order a kernel sums in, and SimHash projections are scalar: the digests
//! hold on every architecture and under `force-vector-scalar-kernel`.

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
use crate::search::vector::distance::{Euclidean, Manhattan};
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
    (db, index)
}

/// Folds every committed row and fixed search results into `digest`.
async fn digest_state<D: Distance>(
    db: &slatedb::Db,
    index: &VectorIndex<D>,
    queries: &[Vec<f32>],
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
    for query in queries {
        let results = index
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

/// Runs one seeded workload and returns its hex digest.
async fn run<D: Distance>(seed: u64, planner: Planner) -> String {
    let mut rng = StdRng::seed_from_u64(seed);
    // Planners of one seed share a namespace, so their digests compare.
    let name = format!("golden-{}-{seed}", D::name());
    let (db, index) = create::<D>(&name).await;
    let queries = (0..6).map(|_| random_vector(&mut rng)).collect::<Vec<_>>();
    let mut live = BTreeMap::new();
    let mut session = planner.session::<D>();
    let mut digest = Sha256::new();
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
            let operation = next_operation(&mut rng, &mut live, fresh_only);
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
        }
        txn.commit().await.unwrap();
        digest_state(&db, &index, &queries, &mut digest).await;
    }
    db.close().await.unwrap();
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Asserts every `(label, actual)` digest equals its recorded golden value,
/// reporting all of them at once so a deliberate change can update them.
fn assert_golden(actual: &[(&str, String)], expected: &[(&str, &str)]) {
    let mismatched = actual
        .iter()
        .zip(expected)
        .filter(|((label, digest), (expected_label, expected_digest))| {
            assert_eq!(label, expected_label);
            digest != expected_digest
        })
        .collect::<Vec<_>>();
    assert!(
        mismatched.is_empty(),
        "graph mutation digests changed: {actual:#?}"
    );
}

/// Every planner of one seed stores the same rows: eviction and cache
/// ownership never change what a mutation plans.
const EUCLIDEAN_SEED_1: &str = "8a8268fc073bdc4a82d573624a62767ba4275dd83cab24b312160a49f2b66090";
const MANHATTAN_SEED_4: &str = "f0d2b874474a7b5e4c1df05e2b97447ee257eec8aaeb918d7cc42030c7f9d485";

#[tokio::test(flavor = "multi_thread")]
async fn seeded_euclidean_workloads_store_golden_rows() {
    let actual = vec![
        ("tiny", run::<Euclidean>(1, Planner::TinySession).await),
        ("large", run::<Euclidean>(1, Planner::LargeSession).await),
        ("one-off", run::<Euclidean>(1, Planner::OneOff).await),
        ("backfill", run::<Euclidean>(2, Planner::Backfill).await),
        ("tiny-3", run::<Euclidean>(3, Planner::TinySession).await),
    ];
    assert_golden(
        &actual,
        &[
            ("tiny", EUCLIDEAN_SEED_1),
            ("large", EUCLIDEAN_SEED_1),
            ("one-off", EUCLIDEAN_SEED_1),
            ("backfill", "a03743504040462fad25621372320c0c20530d3669d87eb2d7c7c9ed9d1a86e7"),
            ("tiny-3", "262debfb7b3ffd696a8381955784d1a0a92554b38806c53f37a35368904d215d"),
        ],
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn seeded_manhattan_workloads_store_golden_rows() {
    let actual = vec![
        ("tiny", run::<Manhattan>(4, Planner::TinySession).await),
        ("one-off", run::<Manhattan>(4, Planner::OneOff).await),
        ("backfill", run::<Manhattan>(5, Planner::Backfill).await),
    ];
    assert_golden(
        &actual,
        &[
            ("tiny", MANHATTAN_SEED_4),
            ("one-off", MANHATTAN_SEED_4),
            ("backfill", "62acf336d1752c0f2ab1fa659716895a7333a00cdc36144258e5ab544501643c"),
        ],
    );
}
