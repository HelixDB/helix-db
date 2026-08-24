//! Transaction-local coalescing for current-format graph topology rows.
//!
//! Add-only bitmap and adjacency rows need no observation: one current-format
//! merge operand represents the complete union. Any key whose final mutation
//! removes membership participates in one sorted `multi_get`, after which this
//! runtime stages one final current-format put/delete. No runtime state is
//! serialized.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use roaring::RoaringTreemap;
use slatedb::DbTransaction;

use crate::encoding::indexes::label::{EdgeLabelKey, EdgeLabelNeighborKey};
use crate::encoding::indexes::{
    hash_property_name, hash_property_value, EdgeDirection, PropertyIndexKey,
};
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{AdjacencyKey, DataKey, DataKeyKind, EdgePairIndexKey};
use crate::encoding::v2::values::adjacency::Edges;
use crate::encoding::v2::values::{adjacency as edges, indexes as secondary};
use crate::{HelixDbError, Result};

/// A final membership operation for one ID within the current epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MembershipMutation {
    Present,
    Absent,
}

/// Direction local to adjacency rows; `Both` is represented by two entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum AdjacencyDirection {
    Out,
    In,
}

#[derive(Debug, Default)]
struct TopologyMutationBatch {
    bitmaps: BTreeMap<Bytes, BTreeMap<u64, MembershipMutation>>,
    adjacency: BTreeMap<Bytes, BTreeMap<(AdjacencyDirection, u64), MembershipMutation>>,
}

/// Transaction-owned topology mutations with a terminal prepared state.
#[derive(Debug, Default)]
pub(in crate::execution::interpreter) struct TopologyMutationRuntime {
    state: TopologyMutationRuntimeState,
    staged_keys: BTreeSet<Bytes>,
}

#[derive(Debug, Default)]
enum TopologyMutationRuntimeState {
    #[default]
    Collecting,
    Pending(TopologyMutationBatch),
    Prepared,
}

impl TopologyMutationRuntime {
    /// Reads topology rows while preserving transaction-local staged overlays.
    pub(in crate::execution::interpreter) async fn observe(
        &self,
        transaction: &DbTransaction,
        keys: &[Bytes],
    ) -> Result<Vec<Option<Bytes>>> {
        let mut values = vec![None; keys.len()];
        let mut snapshot_positions = Vec::new();
        let mut snapshot_keys = Vec::new();
        for (position, key) in keys.iter().enumerate() {
            if self.staged_keys.contains(key) {
                values[position] = transaction.get(key).await?;
            } else {
                snapshot_positions.push(position);
                snapshot_keys.push(key.clone());
            }
        }
        if !snapshot_keys.is_empty() {
            for (position, value) in snapshot_positions
                .into_iter()
                .zip(transaction.multi_get(&snapshot_keys).await?)
            {
                values[position] = value;
            }
        }
        Ok(values)
    }

    /// Adds one node-label membership through the canonical typed key.
    pub(in crate::execution::interpreter) fn add_node_label(
        &mut self,
        scope: DataScope,
        label: &str,
        node_id: u64,
    ) -> Result<()> {
        self.record_bitmap(
            node_label_key(scope, label),
            node_id,
            MembershipMutation::Present,
        )
    }

    /// Removes one node-label membership through the canonical typed key.
    pub(in crate::execution::interpreter) fn remove_node_label(
        &mut self,
        scope: DataScope,
        label: &str,
        node_id: u64,
    ) -> Result<()> {
        self.record_bitmap(
            node_label_key(scope, label),
            node_id,
            MembershipMutation::Absent,
        )
    }

    /// Adds one exact multigraph pair membership.
    pub(in crate::execution::interpreter) fn add_edge_pair(
        &mut self,
        scope: DataScope,
        from: u64,
        to: u64,
        edge_id: u64,
    ) -> Result<()> {
        self.record_bitmap(
            edge_pair_key(scope, from, to),
            edge_id,
            MembershipMutation::Present,
        )
    }

    /// Removes one exact multigraph pair membership.
    pub(in crate::execution::interpreter) fn remove_edge_pair(
        &mut self,
        scope: DataScope,
        from: u64,
        to: u64,
        edge_id: u64,
    ) -> Result<()> {
        self.record_bitmap(
            edge_pair_key(scope, from, to),
            edge_id,
            MembershipMutation::Absent,
        )
    }

    /// Adds outgoing, incoming, and global edge-label memberships.
    pub(in crate::execution::interpreter) fn add_edge_label(
        &mut self,
        scope: DataScope,
        from: u64,
        to: u64,
        label: &str,
        edge_id: u64,
    ) -> Result<()> {
        self.record_bitmap(
            edge_label_neighbor_key(scope, EdgeDirection::Out, from, label),
            to,
            MembershipMutation::Present,
        )?;
        self.record_bitmap(
            edge_label_neighbor_key(scope, EdgeDirection::In, to, label),
            from,
            MembershipMutation::Present,
        )?;
        self.record_bitmap(
            global_edge_label_key(scope, label),
            edge_id,
            MembershipMutation::Present,
        )
    }

    /// Removes the global edge-label membership for one physical edge.
    pub(in crate::execution::interpreter) fn remove_global_edge_label(
        &mut self,
        scope: DataScope,
        label: &str,
        edge_id: u64,
    ) -> Result<()> {
        self.record_bitmap(
            global_edge_label_key(scope, label),
            edge_id,
            MembershipMutation::Absent,
        )
    }

    /// Removes pair-level outgoing and incoming edge-label memberships.
    pub(in crate::execution::interpreter) fn remove_edge_label_neighbors(
        &mut self,
        scope: DataScope,
        from: u64,
        to: u64,
        label: &str,
    ) -> Result<()> {
        self.record_bitmap(
            edge_label_neighbor_key(scope, EdgeDirection::Out, from, label),
            to,
            MembershipMutation::Absent,
        )?;
        self.record_bitmap(
            edge_label_neighbor_key(scope, EdgeDirection::In, to, label),
            from,
            MembershipMutation::Absent,
        )
    }

    /// Adds one directed adjacency membership.
    pub(in crate::execution::interpreter) fn add_adjacency(
        &mut self,
        scope: DataScope,
        node: u64,
        neighbor: u64,
        direction: helix_planner::ir::ExpandDirection,
    ) -> Result<()> {
        self.record_adjacency(
            scope,
            node,
            neighbor,
            direction,
            MembershipMutation::Present,
        )
    }

    /// Removes one directed adjacency membership.
    pub(in crate::execution::interpreter) fn remove_adjacency(
        &mut self,
        scope: DataScope,
        node: u64,
        neighbor: u64,
        direction: helix_planner::ir::ExpandDirection,
    ) -> Result<()> {
        self.record_adjacency(scope, node, neighbor, direction, MembershipMutation::Absent)
    }

    fn record_bitmap(&mut self, key: Bytes, id: u64, mutation: MembershipMutation) -> Result<()> {
        let batch = self.collecting_batch()?;
        batch.bitmaps.entry(key).or_default().insert(id, mutation);
        Ok(())
    }

    fn record_adjacency(
        &mut self,
        scope: DataScope,
        node: u64,
        neighbor: u64,
        direction: helix_planner::ir::ExpandDirection,
        mutation: MembershipMutation,
    ) -> Result<()> {
        let key = DataKey::Data {
            scope,
            kind: DataKeyKind::Adjacency(AdjacencyKey::new(node)),
        }
        .to_bytes();
        let row = self.collecting_batch()?.adjacency.entry(key).or_default();
        match direction {
            helix_planner::ir::ExpandDirection::Out => {
                row.insert((AdjacencyDirection::Out, neighbor), mutation);
            }
            helix_planner::ir::ExpandDirection::In => {
                row.insert((AdjacencyDirection::In, neighbor), mutation);
            }
            helix_planner::ir::ExpandDirection::Both => {
                row.insert((AdjacencyDirection::Out, neighbor), mutation);
                row.insert((AdjacencyDirection::In, neighbor), mutation);
            }
        }
        Ok(())
    }

    fn collecting_batch(&mut self) -> Result<&mut TopologyMutationBatch> {
        if matches!(self.state, TopologyMutationRuntimeState::Collecting) {
            self.state = TopologyMutationRuntimeState::Pending(TopologyMutationBatch::default());
        }
        match &mut self.state {
            TopologyMutationRuntimeState::Pending(batch) => Ok(batch),
            TopologyMutationRuntimeState::Collecting => {
                unreachable!("collecting state transitions to a pending batch")
            }
            TopologyMutationRuntimeState::Prepared => Err(HelixDbError::InvariantViolation(
                "prepared topology mutation runtime cannot collect another mutation".to_string(),
            )),
        }
    }

    /// Flushes one coalesced epoch and returns to the collecting state.
    pub(in crate::execution::interpreter) async fn flush(
        &mut self,
        transaction: &DbTransaction,
    ) -> Result<()> {
        let state = std::mem::take(&mut self.state);
        let batch = match state {
            TopologyMutationRuntimeState::Collecting => return Ok(()),
            TopologyMutationRuntimeState::Pending(batch) => batch,
            TopologyMutationRuntimeState::Prepared => {
                self.state = TopologyMutationRuntimeState::Prepared;
                return Err(HelixDbError::InvariantViolation(
                    "prepared topology mutation runtime cannot flush another epoch".to_string(),
                ));
            }
        };
        #[cfg(feature = "production-coverage")]
        super::benchmark_telemetry::record_topology_flush();

        let mut observation_keys = batch
            .bitmaps
            .iter()
            .filter(|(_, mutations)| {
                mutations
                    .values()
                    .any(|mutation| *mutation == MembershipMutation::Absent)
            })
            .map(|(key, _)| key.clone())
            .chain(
                batch
                    .adjacency
                    .iter()
                    .filter(|(_, mutations)| {
                        mutations
                            .values()
                            .any(|mutation| *mutation == MembershipMutation::Absent)
                    })
                    .map(|(key, _)| key.clone()),
            )
            .collect::<Vec<_>>();
        observation_keys.sort_unstable();
        observation_keys.dedup();
        let (staged_keys, snapshot_keys): (Vec<_>, Vec<_>) = observation_keys
            .into_iter()
            .partition(|key| self.staged_keys.contains(key));
        const OBSERVATION_KEYS_PER_CHUNK: usize = 512;

        let mut observations = BTreeMap::new();
        for chunk in snapshot_keys.chunks(OBSERVATION_KEYS_PER_CHUNK) {
            observations.extend(
                chunk
                    .iter()
                    .cloned()
                    .zip(transaction.multi_get(chunk).await?),
            );
        }
        for key in staged_keys {
            observations.insert(key.clone(), transaction.get(&key).await?);
        }

        for (key, mutations) in batch.bitmaps {
            if mutations
                .values()
                .all(|mutation| *mutation == MembershipMutation::Present)
            {
                let additions = mutations.keys().copied().collect::<RoaringTreemap>();
                transaction.merge_commutative(
                    &key,
                    secondary::SecondaryEqualityValue::encode_ids(&additions),
                )?;
                self.staged_keys.insert(key);
                continue;
            }
            let mut bitmap = observations
                .get(&key)
                .cloned()
                .flatten()
                .map(|value| {
                    secondary::SecondaryEqualityValue::decode(&value).map(|value| value.into_ids())
                })
                .transpose()?
                .unwrap_or_default();
            for (id, mutation) in mutations {
                match mutation {
                    MembershipMutation::Present => {
                        bitmap.insert(id);
                    }
                    MembershipMutation::Absent => {
                        bitmap.remove(id);
                    }
                }
            }
            if bitmap.is_empty() {
                transaction.delete(&key)?;
            } else {
                transaction.put(&key, secondary::SecondaryEqualityValue::encode_ids(&bitmap))?;
            }
            self.staged_keys.insert(key);
        }

        for (key, mutations) in batch.adjacency {
            if mutations
                .values()
                .all(|mutation| *mutation == MembershipMutation::Present)
            {
                let mut additions = Edges::new();
                for ((direction, neighbor), _) in mutations {
                    match direction {
                        AdjacencyDirection::Out => additions.add_out(neighbor),
                        AdjacencyDirection::In => additions.add_in(neighbor),
                    }
                }
                transaction.merge_commutative(&key, edges::encode_edges(&additions))?;
                self.staged_keys.insert(key);
                continue;
            }
            let mut adjacency = observations
                .get(&key)
                .cloned()
                .flatten()
                .map(|value| edges::decode_edges(&value))
                .transpose()?
                .unwrap_or_default();
            for ((direction, neighbor), mutation) in mutations {
                match (direction, mutation) {
                    (AdjacencyDirection::Out, MembershipMutation::Present) => {
                        adjacency.add_out(neighbor);
                    }
                    (AdjacencyDirection::In, MembershipMutation::Present) => {
                        adjacency.add_in(neighbor);
                    }
                    (AdjacencyDirection::Out, MembershipMutation::Absent) => {
                        adjacency.remove_out(neighbor);
                    }
                    (AdjacencyDirection::In, MembershipMutation::Absent) => {
                        adjacency.remove_in(neighbor);
                    }
                }
            }
            if adjacency.is_empty() {
                transaction.delete(&key)?;
            } else {
                transaction.put(&key, edges::encode_edges(&adjacency))?;
            }
            self.staged_keys.insert(key);
        }
        Ok(())
    }

    /// Flushes the final epoch and seals the runtime for commit.
    pub(in crate::execution::interpreter) async fn prepare(
        &mut self,
        transaction: &DbTransaction,
    ) -> Result<()> {
        self.flush(transaction).await?;
        self.state = TopologyMutationRuntimeState::Prepared;
        Ok(())
    }

    /// Consumes only a runtime that completed final preparation.
    pub(in crate::execution::interpreter) fn consume_prepared(self) -> Result<()> {
        match self.state {
            TopologyMutationRuntimeState::Prepared => Ok(()),
            TopologyMutationRuntimeState::Collecting | TopologyMutationRuntimeState::Pending(_) => {
                Err(HelixDbError::InvariantViolation(
                    "topology mutation runtime reached commit before prepare".to_string(),
                ))
            }
        }
    }
}

fn node_label_key(scope: DataScope, label: &str) -> Bytes {
    DataKey::Data {
        scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::Equality(
            crate::encoding::indexes::equality::EqualityIndexKey::new(
                hash_property_name("$label"),
                hash_property_value(label),
            ),
        )),
    }
    .to_bytes()
}

fn edge_pair_key(scope: DataScope, from: u64, to: u64) -> Bytes {
    DataKey::Data {
        scope,
        kind: DataKeyKind::EdgePairIndex(EdgePairIndexKey::new(from, to)),
    }
    .to_bytes()
}

fn edge_label_neighbor_key(
    scope: DataScope,
    direction: EdgeDirection,
    node: u64,
    label: &str,
) -> Bytes {
    DataKey::Data {
        scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabelNeighbor(
            EdgeLabelNeighborKey::new(direction, node, hash_property_value(label)),
        )),
    }
    .to_bytes()
}

fn global_edge_label_key(scope: DataScope, label: &str) -> Bytes {
    DataKey::Data {
        scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabel(EdgeLabelKey::new(
            hash_property_value(label),
        ))),
    }
    .to_bytes()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, IsolationLevel};

    use super::*;

    async fn database(name: &str) -> Db {
        Db::builder(
            format!("topology-mutation-runtime/{name}"),
            Arc::new(InMemory::new()),
        )
        .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
        .build()
        .await
        .expect("topology test database opens")
    }

    async fn transaction(name: &str) -> DbTransaction {
        database(name)
            .await
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("topology test transaction opens")
    }

    #[derive(Debug, Clone, Copy)]
    enum RaceRow {
        NodeLabel(&'static str),
        OutAdjacency(u64),
    }

    impl RaceRow {
        fn stage(
            self,
            runtime: &mut TopologyMutationRuntime,
            scope: DataScope,
            id: u64,
            mutation: MembershipMutation,
        ) {
            match (self, mutation) {
                (Self::NodeLabel(label), MembershipMutation::Present) => {
                    runtime.add_node_label(scope, label, id).unwrap();
                }
                (Self::NodeLabel(label), MembershipMutation::Absent) => {
                    runtime.remove_node_label(scope, label, id).unwrap();
                }
                (Self::OutAdjacency(node), MembershipMutation::Present) => {
                    runtime
                        .add_adjacency(scope, node, id, helix_planner::ir::ExpandDirection::Out)
                        .unwrap();
                }
                (Self::OutAdjacency(node), MembershipMutation::Absent) => {
                    runtime
                        .remove_adjacency(scope, node, id, helix_planner::ir::ExpandDirection::Out)
                        .unwrap();
                }
            }
        }

        async fn ids(self, db: &Db, scope: DataScope) -> Vec<u64> {
            match self {
                Self::NodeLabel(label) => secondary::SecondaryEqualityValue::decode(
                    &db.get(node_label_key(scope, label))
                        .await
                        .unwrap()
                        .expect("node label race row exists"),
                )
                .unwrap()
                .into_ids()
                .iter()
                .collect(),
                Self::OutAdjacency(node) => edges::decode_edges(
                    &db.get(
                        DataKey::Data {
                            scope,
                            kind: DataKeyKind::Adjacency(AdjacencyKey::new(node)),
                        }
                        .to_bytes(),
                    )
                    .await
                    .unwrap()
                    .expect("adjacency race row exists"),
                )
                .unwrap()
                .iter_out()
                .collect(),
            }
        }
    }

    #[tokio::test]
    async fn concurrent_add_only_rows_commit_and_preserve_every_topology_membership() {
        let db = database("concurrent-add-only").await;
        let scope = DataScope::LegacyUnscoped;
        let left = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let right = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let mut left_runtime = TopologyMutationRuntime::default();
        let mut right_runtime = TopologyMutationRuntime::default();

        left_runtime.add_node_label(scope, "User", 1).unwrap();
        right_runtime.add_node_label(scope, "User", 2).unwrap();
        left_runtime.add_edge_pair(scope, 10, 20, 100).unwrap();
        right_runtime.add_edge_pair(scope, 10, 20, 101).unwrap();
        left_runtime
            .add_edge_label(scope, 1, 9, "FOLLOWS", 100)
            .unwrap();
        right_runtime
            .add_edge_label(scope, 1, 10, "FOLLOWS", 101)
            .unwrap();
        left_runtime
            .add_edge_label(scope, 11, 2, "FOLLOWS", 102)
            .unwrap();
        right_runtime
            .add_edge_label(scope, 12, 2, "FOLLOWS", 103)
            .unwrap();
        left_runtime
            .add_adjacency(scope, 50, 60, helix_planner::ir::ExpandDirection::Both)
            .unwrap();
        right_runtime
            .add_adjacency(scope, 50, 61, helix_planner::ir::ExpandDirection::Both)
            .unwrap();

        left_runtime.prepare(&left).await.unwrap();
        right_runtime.prepare(&right).await.unwrap();
        left_runtime.consume_prepared().unwrap();
        right_runtime.consume_prepared().unwrap();
        left.commit().await.unwrap();
        right.commit().await.unwrap();

        let bitmap_ids = |key| async {
            secondary::SecondaryEqualityValue::decode(
                &db.get(key).await.unwrap().expect("topology bitmap exists"),
            )
            .unwrap()
            .into_ids()
            .iter()
            .collect::<Vec<_>>()
        };
        assert_eq!(bitmap_ids(node_label_key(scope, "User")).await, vec![1, 2]);
        assert_eq!(
            bitmap_ids(edge_pair_key(scope, 10, 20)).await,
            vec![100, 101]
        );
        assert_eq!(
            bitmap_ids(edge_label_neighbor_key(
                scope,
                EdgeDirection::Out,
                1,
                "FOLLOWS",
            ))
            .await,
            vec![9, 10]
        );
        assert_eq!(
            bitmap_ids(edge_label_neighbor_key(
                scope,
                EdgeDirection::In,
                2,
                "FOLLOWS",
            ))
            .await,
            vec![11, 12]
        );
        assert_eq!(
            bitmap_ids(global_edge_label_key(scope, "FOLLOWS")).await,
            vec![100, 101, 102, 103]
        );
        let adjacency = edges::decode_edges(
            &db.get(
                DataKey::Data {
                    scope,
                    kind: DataKeyKind::Adjacency(AdjacencyKey::new(50)),
                }
                .to_bytes(),
            )
            .await
            .unwrap()
            .expect("adjacency row exists"),
        )
        .unwrap();
        assert_eq!(adjacency.iter_out().collect::<Vec<_>>(), vec![60, 61]);
        assert_eq!(adjacency.iter_in().collect::<Vec<_>>(), vec![60, 61]);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn topology_insert_remove_races_conflict_in_both_commit_orders() {
        let db = database("insert-remove-races").await;
        let scope = DataScope::LegacyUnscoped;
        let cases = [
            (RaceRow::NodeLabel("insert-first"), true),
            (RaceRow::NodeLabel("remove-first"), false),
            (RaceRow::OutAdjacency(100), true),
            (RaceRow::OutAdjacency(101), false),
        ];

        for (row, insert_commits_first) in cases {
            let seed = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .unwrap();
            let mut seed_runtime = TopologyMutationRuntime::default();
            row.stage(&mut seed_runtime, scope, 1, MembershipMutation::Present);
            seed_runtime.prepare(&seed).await.unwrap();
            seed_runtime.consume_prepared().unwrap();
            seed.commit().await.unwrap();

            let insert = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .unwrap();
            let remove = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .unwrap();
            let mut insert_runtime = TopologyMutationRuntime::default();
            row.stage(&mut insert_runtime, scope, 2, MembershipMutation::Present);
            insert_runtime.prepare(&insert).await.unwrap();
            insert_runtime.consume_prepared().unwrap();
            let mut remove_runtime = TopologyMutationRuntime::default();
            row.stage(&mut remove_runtime, scope, 1, MembershipMutation::Absent);
            remove_runtime.prepare(&remove).await.unwrap();
            remove_runtime.consume_prepared().unwrap();

            let retry_mutation = if insert_commits_first {
                insert.commit().await.unwrap();
                let error = remove.commit().await.expect_err("remove must conflict");
                assert_eq!(error.kind(), slatedb::ErrorKind::Transaction);
                MembershipMutation::Absent
            } else {
                remove.commit().await.unwrap();
                let error = insert.commit().await.expect_err("insert must conflict");
                assert_eq!(error.kind(), slatedb::ErrorKind::Transaction);
                MembershipMutation::Present
            };

            let retry = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .unwrap();
            let mut retry_runtime = TopologyMutationRuntime::default();
            row.stage(
                &mut retry_runtime,
                scope,
                if insert_commits_first { 1 } else { 2 },
                retry_mutation,
            );
            retry_runtime.prepare(&retry).await.unwrap();
            retry_runtime.consume_prepared().unwrap();
            retry.commit().await.unwrap();
            assert_eq!(row.ids(&db, scope).await, vec![2]);
        }

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn add_only_rows_merge_once_and_later_removals_observe_the_staged_overlay() {
        let transaction = transaction("add-remove-overlay").await;
        let scope = DataScope::LegacyUnscoped;
        let mut runtime = TopologyMutationRuntime::default();

        for node_id in [1, 2] {
            runtime.add_node_label(scope, "User", node_id).unwrap();
        }
        for edge_id in [10, 11] {
            runtime.add_edge_pair(scope, 1, 2, edge_id).unwrap();
        }
        runtime
            .add_adjacency(scope, 1, 2, helix_planner::ir::ExpandDirection::Out)
            .unwrap();
        runtime
            .add_adjacency(scope, 1, 3, helix_planner::ir::ExpandDirection::Out)
            .unwrap();
        runtime.add_edge_label(scope, 1, 2, "FOLLOWS", 10).unwrap();
        runtime.flush(&transaction).await.unwrap();

        runtime.remove_node_label(scope, "User", 1).unwrap();
        runtime.remove_edge_pair(scope, 1, 2, 10).unwrap();
        runtime
            .remove_adjacency(scope, 1, 2, helix_planner::ir::ExpandDirection::Out)
            .unwrap();
        runtime
            .remove_global_edge_label(scope, "FOLLOWS", 10)
            .unwrap();
        runtime.flush(&transaction).await.unwrap();

        let labels = secondary::SecondaryEqualityValue::decode(
            &transaction
                .get(node_label_key(scope, "User"))
                .await
                .unwrap()
                .expect("remaining node label row exists"),
        )
        .unwrap()
        .into_ids();
        assert_eq!(labels.iter().collect::<Vec<_>>(), vec![2]);
        let pairs = secondary::SecondaryEqualityValue::decode(
            &transaction
                .get(edge_pair_key(scope, 1, 2))
                .await
                .unwrap()
                .expect("remaining pair row exists"),
        )
        .unwrap()
        .into_ids();
        assert_eq!(pairs.iter().collect::<Vec<_>>(), vec![11]);
        let adjacency = edges::decode_edges(
            &transaction
                .get(
                    DataKey::Data {
                        scope,
                        kind: DataKeyKind::Adjacency(AdjacencyKey::new(1)),
                    }
                    .to_bytes(),
                )
                .await
                .unwrap()
                .expect("remaining adjacency row exists"),
        )
        .unwrap();
        assert_eq!(adjacency.iter_out().collect::<Vec<_>>(), vec![3]);

        runtime.prepare(&transaction).await.unwrap();
        runtime.consume_prepared().unwrap();
    }

    #[tokio::test]
    async fn prepared_runtime_rejects_further_collection() {
        let transaction = transaction("prepared-rejects-collection").await;
        let mut runtime = TopologyMutationRuntime::default();
        runtime.prepare(&transaction).await.unwrap();

        assert!(matches!(
            runtime.add_node_label(DataScope::LegacyUnscoped, "User", 1),
            Err(HelixDbError::InvariantViolation(_))
        ));
    }
}
