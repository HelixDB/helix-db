//! Behavioural oracle for [`VectorMemoryStore`].
//!
//! These tests pin what callers observe through the store boundary: lookup
//! bytes for every row kind and layer, upsert and removal semantics, hydration
//! admission order, and safety of concurrent lookups during removal. They use
//! only the public store surface so they hold for any resident layout.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use proptest::prelude::*;
use slatedb::object_store::memory::InMemory;
use slatedb::{Db, IsolationLevel};

use super::*;
use crate::encoding::v2::keys::indexes::vector::{
    VectorLayer0NeighborsKey, VectorSimHashKey, VectorUpperNeighborsKey, VectorUpperVectorKey,
};
use crate::encoding::v2::keys::scope::TenantId;

/// Highest layer the generated fixtures use; lookups probe one layer above it.
const MAX_LAYER: u16 = 4;

/// One supported cache row together with the exact persisted key it came from.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ExpectedRow {
    Neighbors {
        layer: u16,
        node_id: NodeId,
        value: Bytes,
    },
    SimHash {
        node_id: NodeId,
        bits: u64,
    },
    Vector {
        node_id: NodeId,
        value: Bytes,
    },
}

/// Deterministic SplitMix64 stream so fixtures are reproducible without seeds.
struct SplitMix(u64);

impl SplitMix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn bytes(&mut self, len: usize) -> Bytes {
        (0..len)
            .map(|_| self.next().to_le_bytes()[0])
            .collect::<Vec<u8>>()
            .into()
    }
}

/// Reference model of every row a store should currently return.
#[derive(Debug, Default)]
struct Model {
    vectors: HashMap<NodeId, Bytes>,
    neighbors: HashMap<(u16, NodeId), Bytes>,
    simhashes: HashMap<NodeId, u64>,
}

impl Model {
    fn admit(&mut self, row: &ExpectedRow) {
        match row {
            ExpectedRow::Neighbors {
                layer,
                node_id,
                value,
            } => {
                self.neighbors.insert((*layer, *node_id), value.clone());
            }
            ExpectedRow::SimHash { node_id, bits } => {
                self.simhashes.insert(*node_id, *bits);
            }
            ExpectedRow::Vector { node_id, value } => {
                self.vectors.insert(*node_id, value.clone());
            }
        }
    }

    fn remove_node(&mut self, node_id: NodeId) {
        self.vectors.remove(&node_id);
        self.simhashes.remove(&node_id);
        self.neighbors.retain(|(_, node), _| *node != node_id);
    }

    /// Asserts every lookup for `nodes` and layers `0..=MAX_LAYER + 1`.
    fn assert_matches(&self, store: &VectorMemoryStore, nodes: impl IntoIterator<Item = NodeId>) {
        for node_id in nodes {
            assert_eq!(
                store.get_upper_vector(node_id),
                self.vectors.get(&node_id).cloned(),
                "upper vector for node {node_id}"
            );
            assert_eq!(
                store.get_simhash(node_id).map(|hash| hash.bits()),
                self.simhashes.get(&node_id).copied(),
                "simhash for node {node_id}"
            );
            for layer in 0..=MAX_LAYER + 1 {
                assert_eq!(
                    store.get_upper_neighbors_bytes(layer, node_id),
                    self.neighbors.get(&(layer, node_id)).cloned(),
                    "upper neighbors for node {node_id} layer {layer}"
                );
            }
        }
    }
}

/// Builds the persisted key bytes for one expected row in `scope`.
fn row_key(scope: DataScope, index_id: u64, row: &ExpectedRow) -> Bytes {
    let key = match row {
        ExpectedRow::Neighbors { layer, node_id, .. } => {
            VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(index_id, *layer, *node_id))
        }
        ExpectedRow::SimHash { node_id, .. } => {
            VectorKey::SimHash(VectorSimHashKey::new(index_id, *node_id))
        }
        ExpectedRow::Vector { node_id, .. } => {
            VectorKey::UpperVector(VectorUpperVectorKey::new(index_id, *node_id))
        }
    };
    DataKey::Data {
        scope,
        kind: DataKeyKind::Vector(key),
    }
    .to_bytes()
}

/// Persisted value bytes for one expected row.
fn row_value(row: &ExpectedRow) -> Bytes {
    match row {
        ExpectedRow::Neighbors { value, .. } | ExpectedRow::Vector { value, .. } => value.clone(),
        ExpectedRow::SimHash { bits, .. } => Bytes::copy_from_slice(&encode_simhash(*bits)),
    }
}

/// An HNSW-shaped fixture: every node has a SimHash, roughly one in eight
/// nodes has upper layers, and upper rows vary from empty to multi-MiB.
struct HydrationFixture {
    db: Db,
    scope: DataScope,
    index_id: u64,
    node_count: NodeId,
    /// Supported rows in physical scan (key) order.
    rows: Vec<ExpectedRow>,
}

impl HydrationFixture {
    async fn build(name: &str, node_count: NodeId) -> Self {
        let scope = DataScope::Tenant(TenantId::from_u128(0xC0FFEE));
        let index_id = crate::search::vector::index_id_from_name(name);
        let db = Db::builder(name, Arc::new(InMemory::new()))
            .build()
            .await
            .expect("fixture db opens");
        let mut random = SplitMix(index_id);
        let mut rows = Vec::new();
        for node_id in 0..node_count {
            rows.push(ExpectedRow::SimHash {
                node_id,
                bits: random.next(),
            });
            let level = match node_id {
                id if id % 512 == 0 => MAX_LAYER,
                id if id % 64 == 0 => 2,
                id if id % 8 == 0 => 1,
                _ => 0,
            };
            for layer in 1..=level {
                let len = usize::try_from(random.next() % 200).unwrap();
                rows.push(ExpectedRow::Neighbors {
                    layer,
                    node_id,
                    value: random.bytes(len),
                });
            }
            if level > 0 {
                // Node 8 carries a row larger than any plausible chunk, node
                // 16 an empty row; the rest are vector-sized.
                let len = match node_id {
                    8 => 3 * 1024 * 1024 + 7,
                    16 => 0,
                    _ => 64 + usize::try_from(random.next() % 1200).unwrap(),
                };
                rows.push(ExpectedRow::Vector {
                    node_id,
                    value: random.bytes(len),
                });
            }
        }
        rows.sort_by_key(|row| row_key(scope, index_id, row));

        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        for row in &rows {
            transaction
                .put(row_key(scope, index_id, row), row_value(row))
                .unwrap();
        }
        // Rows hydration must skip: layer-0 neighbors in the same prefix, a
        // different index, and the same index in a different scope.
        for node_id in 0..node_count {
            transaction
                .put(
                    DataKey::Data {
                        scope,
                        kind: DataKeyKind::Vector(VectorKey::Layer0Neighbors(
                            VectorLayer0NeighborsKey::new(index_id, node_id),
                        )),
                    }
                    .to_bytes(),
                    random.bytes(48),
                )
                .unwrap();
        }
        let foreign_vector = ExpectedRow::Vector {
            node_id: 8,
            value: Bytes::from_static(b"foreign"),
        };
        transaction
            .put(
                row_key(scope, index_id.wrapping_add(1), &foreign_vector),
                row_value(&foreign_vector),
            )
            .unwrap();
        transaction
            .put(
                row_key(
                    DataScope::Tenant(TenantId::from_u128(0xBEEF)),
                    index_id,
                    &foreign_vector,
                ),
                row_value(&foreign_vector),
            )
            .unwrap();
        transaction.commit().await.unwrap();

        Self {
            db,
            scope,
            index_id,
            node_count,
            rows,
        }
    }

    fn store(&self) -> VectorMemoryStore {
        VectorMemoryStore::new(self.scope, self.index_id, u64::MAX)
    }

    async fn load(
        &self,
        budget: VectorMemoryAdmissionBudget,
    ) -> (VectorMemoryStore, VectorMemoryStoreLoadSummary) {
        let store = self.store();
        let summary = store
            .load_descriptor_bound_with_budget(&self.db, budget, None)
            .await
            .expect("fixture hydration succeeds");
        (store, summary)
    }

    /// Model containing exactly the first `admitted` rows in scan order.
    fn prefix_model(&self, admitted: usize) -> Model {
        let mut model = Model::default();
        for row in &self.rows[..admitted] {
            model.admit(row);
        }
        model
    }
}

/// Unbounded hydration returns byte-identical rows for every kind and layer,
/// skips foreign and layer-0 rows, and charges what it reports.
#[tokio::test]
async fn unbounded_hydration_returns_every_row_byte_for_byte() {
    let fixture = HydrationFixture::build("store_oracle_unbounded", 2_048).await;
    let (store, summary) = fixture.load(VectorMemoryAdmissionBudget::Unbounded).await;

    assert_eq!(summary.loaded_entries, fixture.rows.len());
    assert_eq!(
        summary.completion,
        VectorMemoryStoreLoadCompletion::Complete
    );
    assert_eq!(store.estimated_bytes(), summary.estimated_bytes);
    let total_value_bytes: usize = fixture.rows.iter().map(|row| row_value(row).len()).sum();
    assert!(
        summary.estimated_bytes >= u64::try_from(total_value_bytes).unwrap(),
        "the charge covers at least every admitted value byte"
    );
    fixture
        .prefix_model(fixture.rows.len())
        .assert_matches(&store, 0..fixture.node_count + 1);
}

/// Every bounded hydration admits an exact scan-order prefix and never charges
/// more than its budget; a budget of the unbounded charge admits every row.
#[tokio::test]
async fn bounded_hydration_admits_an_exact_scan_prefix_within_budget() {
    let fixture = HydrationFixture::build("store_oracle_bounded", 1_024).await;
    let (_, full) = fixture.load(VectorMemoryAdmissionBudget::Unbounded).await;

    let generous = full.estimated_bytes * 4 + 8 * 1024 * 1024;
    let budgets = [
        0,
        1,
        63,
        64,
        4_096,
        65_536,
        full.estimated_bytes / 3,
        full.estimated_bytes / 2,
        full.estimated_bytes - 1,
        full.estimated_bytes,
        generous,
    ];
    for budget in budgets {
        let (store, summary) = fixture
            .load(VectorMemoryAdmissionBudget::Bounded(budget))
            .await;
        assert!(
            summary.estimated_bytes <= budget,
            "budget {budget} charged {}",
            summary.estimated_bytes
        );
        assert_eq!(store.estimated_bytes(), summary.estimated_bytes);
        let expected_completion = if summary.loaded_entries == fixture.rows.len() {
            VectorMemoryStoreLoadCompletion::Complete
        } else {
            VectorMemoryStoreLoadCompletion::BudgetExhausted
        };
        assert_eq!(summary.completion, expected_completion, "budget {budget}");
        if budget == 0 {
            assert_eq!(summary.loaded_entries, 0);
        }
        if budget >= full.estimated_bytes {
            assert_eq!(
                summary.loaded_entries,
                fixture.rows.len(),
                "budget {budget}"
            );
            assert_eq!(summary.estimated_bytes, full.estimated_bytes);
        }
        fixture
            .prefix_model(summary.loaded_entries)
            .assert_matches(&store, 0..fixture.node_count);
    }
}

/// Commit eviction after hydration removes exactly the named rows.
#[tokio::test]
async fn removal_after_hydration_matches_the_model() {
    let fixture = HydrationFixture::build("store_oracle_removal", 1_024).await;
    let (store, _) = fixture.load(VectorMemoryAdmissionBudget::Unbounded).await;
    let mut model = fixture.prefix_model(fixture.rows.len());
    let estimated = store.estimated_bytes();

    for node_id in (0..fixture.node_count).step_by(24) {
        store.remove_node(node_id);
        model.remove_node(node_id);
    }
    for node_id in (0..fixture.node_count).step_by(40) {
        store.remove_upper_neighbors(1, node_id);
        model.neighbors.remove(&(1, node_id));
    }
    // Removing the lowest layer of a multi-layer node keeps the others.
    store.remove_upper_neighbors(2, 512);
    model.neighbors.remove(&(2, 512));
    store.remove_upper_vector(64);
    model.vectors.remove(&64);
    store.remove_simhash(3);
    model.simhashes.remove(&3);
    // Removing every neighbor layer keeps the node's vector.
    for layer in 0..=MAX_LAYER {
        store.remove_upper_neighbors(layer, 1_000);
    }
    model.neighbors.retain(|(_, node), _| *node != 1_000);
    model.assert_matches(&store, 0..fixture.node_count);

    assert_eq!(
        store.estimated_bytes(),
        estimated,
        "evicted rows stay charged until the store is replaced"
    );

    store.clear();
    assert_eq!(store.estimated_bytes(), 0);
    Model::default().assert_matches(&store, 0..fixture.node_count);
}

/// Returned rows stay valid after removal, clear, and dropping the store.
#[tokio::test]
async fn returned_rows_outlive_removal_clear_and_the_store() {
    let fixture = HydrationFixture::build("store_oracle_lifetime", 256).await;
    let (store, _) = fixture.load(VectorMemoryAdmissionBudget::Unbounded).await;
    let model = fixture.prefix_model(fixture.rows.len());

    let vector = store.get_upper_vector(8).expect("large vector cached");
    let neighbors = store
        .get_upper_neighbors_bytes(1, 64)
        .expect("neighbors cached");
    store.remove_node(8);
    store.clear();
    drop(store);

    assert_eq!(Some(&vector), model.vectors.get(&8));
    assert_eq!(Some(&neighbors), model.neighbors.get(&(1, 64)));
}

/// Concurrent lookups during removal and clear only ever see the hydrated
/// bytes or a miss, and a removed row never reappears.
#[test]
fn concurrent_lookups_during_removal_see_hydrated_bytes_or_miss() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let fixture = runtime.block_on(HydrationFixture::build("store_oracle_concurrent", 2_048));
    let (store, _) = runtime.block_on(fixture.load(VectorMemoryAdmissionBudget::Unbounded));
    let store = Arc::new(store);
    let model = Arc::new(fixture.prefix_model(fixture.rows.len()));
    let node_count = fixture.node_count;
    let done = Arc::new(AtomicBool::new(false));

    std::thread::scope(|scope| {
        for reader in 0..6u64 {
            let store = Arc::clone(&store);
            let model = Arc::clone(&model);
            let done = Arc::clone(&done);
            scope.spawn(move || {
                let mut evicted = BTreeMap::<(u8, u16, NodeId), ()>::new();
                let mut check = |kind: u8,
                                 layer: u16,
                                 node_id: NodeId,
                                 got: Option<Bytes>,
                                 expected: Option<&Bytes>| {
                    match got {
                        Some(bytes) => {
                            assert_eq!(
                                Some(&bytes),
                                expected,
                                "node {node_id} kind {kind} layer {layer}"
                            );
                            assert!(
                                !evicted.contains_key(&(kind, layer, node_id)),
                                "removed row reappeared for node {node_id}"
                            );
                        }
                        None => {
                            if expected.is_some() {
                                evicted.insert((kind, layer, node_id), ());
                            }
                        }
                    }
                };
                while !done.load(Ordering::Acquire) {
                    for offset in 0..node_count {
                        let node_id = (offset + reader * 97) % node_count;
                        check(
                            0,
                            0,
                            node_id,
                            store.get_upper_vector(node_id),
                            model.vectors.get(&node_id),
                        );
                        for layer in 1..=MAX_LAYER {
                            check(
                                1,
                                layer,
                                node_id,
                                store.get_upper_neighbors_bytes(layer, node_id),
                                model.neighbors.get(&(layer, node_id)),
                            );
                        }
                    }
                }
            });
        }
        for node_id in (0..node_count).rev() {
            store.remove_node(node_id);
            if node_id % 256 == 0 {
                std::thread::yield_now();
            }
        }
        store.clear();
        done.store(true, Ordering::Release);
    });
    Model::default().assert_matches(&store, 0..node_count);
}

/// One mutation applied to both the store and the model.
#[derive(Debug, Clone)]
enum Operation {
    InsertVector(NodeId, Vec<u8>),
    InsertNeighbors(u16, NodeId, Vec<u8>),
    InsertSimHash(NodeId, u64),
    RemoveVector(NodeId),
    RemoveNeighbors(u16, NodeId),
    RemoveNodeNeighbors(NodeId),
    RemoveSimHash(NodeId),
    RemoveNode(NodeId),
    Clear,
}

fn operation() -> impl Strategy<Value = Operation> {
    let node = 0..6u64;
    let layer = 0..=MAX_LAYER;
    let value = prop_oneof![
        8 => proptest::collection::vec(any::<u8>(), 0..48),
        1 => proptest::collection::vec(any::<u8>(), 4_000..9_000),
    ];
    prop_oneof![
        4 => (node.clone(), value.clone()).prop_map(|(n, v)| Operation::InsertVector(n, v)),
        4 => (layer.clone(), node.clone(), value).prop_map(|(l, n, v)| Operation::InsertNeighbors(l, n, v)),
        2 => (node.clone(), any::<u64>()).prop_map(|(n, b)| Operation::InsertSimHash(n, b)),
        1 => node.clone().prop_map(Operation::RemoveVector),
        2 => (layer, node.clone()).prop_map(|(l, n)| Operation::RemoveNeighbors(l, n)),
        1 => node.clone().prop_map(Operation::RemoveNodeNeighbors),
        1 => node.clone().prop_map(Operation::RemoveSimHash),
        1 => node.prop_map(Operation::RemoveNode),
        1 => Just(Operation::Clear),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Every interleaving of upserts and removals matches a plain map model.
    #[test]
    fn mutation_sequences_match_the_model(operations in proptest::collection::vec(operation(), 1..80)) {
        let store = VectorMemoryStore::new(DataScope::LegacyUnscoped, 7, u64::MAX);
        let mut model = Model::default();
        for operation in operations {
            match operation {
                Operation::InsertVector(node_id, value) => {
                    let value = Bytes::from(value);
                    store.insert_upper_vector(node_id, value.clone());
                    model.vectors.insert(node_id, value);
                }
                Operation::InsertNeighbors(layer, node_id, value) => {
                    let value = Bytes::from(value);
                    store.insert_upper_neighbors_bytes(layer, node_id, value.clone());
                    model.neighbors.insert((layer, node_id), value);
                }
                Operation::InsertSimHash(node_id, bits) => {
                    store.insert_simhash(node_id, SimHash::from_bits(bits));
                    model.simhashes.insert(node_id, bits);
                }
                Operation::RemoveVector(node_id) => {
                    store.remove_upper_vector(node_id);
                    model.vectors.remove(&node_id);
                }
                Operation::RemoveNeighbors(layer, node_id) => {
                    store.remove_upper_neighbors(layer, node_id);
                    model.neighbors.remove(&(layer, node_id));
                }
                Operation::RemoveNodeNeighbors(node_id) => {
                    for layer in 0..=MAX_LAYER {
                        store.remove_upper_neighbors(layer, node_id);
                    }
                    model.neighbors.retain(|(_, node), _| *node != node_id);
                }
                Operation::RemoveSimHash(node_id) => {
                    store.remove_simhash(node_id);
                    model.simhashes.remove(&node_id);
                }
                Operation::RemoveNode(node_id) => {
                    store.remove_node(node_id);
                    model.remove_node(node_id);
                }
                Operation::Clear => {
                    store.clear();
                    model = Model::default();
                }
            }
            model.assert_matches(&store, 0..7);
        }
    }
}
