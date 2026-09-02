//! Transaction-local coalescing for current-format graph topology rows.
//!
//! Bitmap and adjacency rows are staged as associative membership deltas. The
//! transaction conflict metadata names each logical member, so mutations to
//! disjoint members of one physical row can commit concurrently without a
//! read/modify/write cycle.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use slatedb::DbTransaction;

use crate::encoding::indexes::label::{EdgeLabelKey, EdgeLabelNeighborKey, NodeLabelKey};
use crate::encoding::indexes::{CanonicalLabel, EdgeDirection, PropertyIndexKey};
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{AdjacencyKey, DataKey, DataKeyKind, EdgePairIndexKey};
use crate::encoding::v2::values::{adjacency as edges, indexes as secondary};
use crate::{HelixDbError, MembershipDeltaWriteMode, Result};

/// A final membership operation for one ID within the current epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MembershipMutation {
    Present,
    Absent,
}

/// Logical identity for one shared bitmap row. Physical keys are encoded once
/// when the coalesced epoch is flushed.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum BitmapRow {
    NodeLabel {
        scope: DataScope,
        label: CanonicalLabel,
    },
    EdgePair {
        scope: DataScope,
        from: u64,
        to: u64,
    },
    EdgeLabelNeighbor {
        scope: DataScope,
        direction: EdgeDirection,
        node: u64,
        label: CanonicalLabel,
    },
    GlobalEdgeLabel {
        scope: DataScope,
        label: CanonicalLabel,
    },
}

impl BitmapRow {
    fn to_bytes(&self) -> Bytes {
        match self {
            Self::NodeLabel { scope, label } => DataKey::Data {
                scope: *scope,
                kind: DataKeyKind::PropertyIndex(PropertyIndexKey::NodeLabel(
                    NodeLabelKey::from_canonical(label.clone()),
                )),
            }
            .to_bytes(),
            Self::EdgePair { scope, from, to } => DataKey::Data {
                scope: *scope,
                kind: DataKeyKind::EdgePairIndex(EdgePairIndexKey::new(*from, *to)),
            }
            .to_bytes(),
            Self::EdgeLabelNeighbor {
                scope,
                direction,
                node,
                label,
            } => DataKey::Data {
                scope: *scope,
                kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabelNeighbor(
                    EdgeLabelNeighborKey::from_canonical(*direction, *node, label.clone()),
                )),
            }
            .to_bytes(),
            Self::GlobalEdgeLabel { scope, label } => DataKey::Data {
                scope: *scope,
                kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabel(
                    EdgeLabelKey::from_canonical(label.clone()),
                )),
            }
            .to_bytes(),
        }
    }
}

#[derive(Debug, Default)]
struct TopologyMutationBatch {
    bitmaps: BTreeMap<BitmapRow, secondary::BitmapMembershipDelta>,
    adjacency: BTreeMap<(DataScope, u64), edges::AdjacencyMembershipDelta>,
}

/// Transaction-owned topology mutations with a terminal prepared state.
#[derive(Debug, Default)]
pub(in crate::execution::interpreter) struct TopologyMutationRuntime {
    state: TopologyMutationRuntimeState,
    staged_keys: BTreeSet<Bytes>,
    #[cfg(test)]
    unchecked_disjoint_for_benchmark: bool,
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
            BitmapRow::NodeLabel {
                scope,
                label: CanonicalLabel::from_label(label)?,
            },
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
            BitmapRow::NodeLabel {
                scope,
                label: CanonicalLabel::from_label(label)?,
            },
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
            BitmapRow::EdgePair { scope, from, to },
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
            BitmapRow::EdgePair { scope, from, to },
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
        let label = CanonicalLabel::from_label(label)?;
        self.record_bitmap(
            BitmapRow::EdgeLabelNeighbor {
                scope,
                direction: EdgeDirection::Out,
                node: from,
                label: label.clone(),
            },
            to,
            MembershipMutation::Present,
        )?;
        self.record_bitmap(
            BitmapRow::EdgeLabelNeighbor {
                scope,
                direction: EdgeDirection::In,
                node: to,
                label: label.clone(),
            },
            from,
            MembershipMutation::Present,
        )?;
        self.record_bitmap(
            BitmapRow::GlobalEdgeLabel { scope, label },
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
            BitmapRow::GlobalEdgeLabel {
                scope,
                label: CanonicalLabel::from_label(label)?,
            },
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
        let label = CanonicalLabel::from_label(label)?;
        self.record_bitmap(
            BitmapRow::EdgeLabelNeighbor {
                scope,
                direction: EdgeDirection::Out,
                node: from,
                label: label.clone(),
            },
            to,
            MembershipMutation::Absent,
        )?;
        self.record_bitmap(
            BitmapRow::EdgeLabelNeighbor {
                scope,
                direction: EdgeDirection::In,
                node: to,
                label,
            },
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

    fn record_bitmap(
        &mut self,
        row: BitmapRow,
        id: u64,
        mutation: MembershipMutation,
    ) -> Result<()> {
        let delta = self.collecting_batch()?.bitmaps.entry(row).or_default();
        match mutation {
            MembershipMutation::Present => delta.add(id),
            MembershipMutation::Absent => delta.remove(id),
        }
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
        let delta = self
            .collecting_batch()?
            .adjacency
            .entry((scope, node))
            .or_default();
        match (direction, mutation) {
            (helix_planner::ir::ExpandDirection::Out, MembershipMutation::Present) => {
                delta.add_out(neighbor);
            }
            (helix_planner::ir::ExpandDirection::In, MembershipMutation::Present) => {
                delta.add_in(neighbor);
            }
            (helix_planner::ir::ExpandDirection::Both, MembershipMutation::Present) => {
                delta.add_out(neighbor);
                delta.add_in(neighbor);
            }
            (helix_planner::ir::ExpandDirection::Out, MembershipMutation::Absent) => {
                delta.remove_out(neighbor);
            }
            (helix_planner::ir::ExpandDirection::In, MembershipMutation::Absent) => {
                delta.remove_in(neighbor);
            }
            (helix_planner::ir::ExpandDirection::Both, MembershipMutation::Absent) => {
                delta.remove_out(neighbor);
                delta.remove_in(neighbor);
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

        match crate::membership_delta::transaction_write_mode(transaction).await? {
            MembershipDeltaWriteMode::LegacyExclusive => {
                for (row, delta) in batch.bitmaps {
                    let key = row.to_bytes();
                    let mut bitmap = transaction
                        .get(&key)
                        .await?
                        .map(|value| {
                            secondary::SecondaryEqualityBitmapValue::decode(&value)
                                .map(secondary::SecondaryEqualityBitmapValue::into_ids)
                        })
                        .transpose()?
                        .unwrap_or_default();
                    delta.apply_to(&mut bitmap);
                    if bitmap.is_empty() {
                        transaction.delete(&key)?;
                    } else {
                        transaction.put_with_options(
                            &key,
                            secondary::SecondaryEqualityBitmapValue::new(bitmap).encode(),
                            &slatedb::config::PutOptions {
                                ttl: slatedb::config::Ttl::NoExpiry,
                            },
                        )?;
                    }
                    self.staged_keys.insert(key);
                }

                for ((scope, node), delta) in batch.adjacency {
                    let key = DataKey::Data {
                        scope,
                        kind: DataKeyKind::Adjacency(AdjacencyKey::new(node)),
                    }
                    .to_bytes();
                    let mut adjacency = transaction
                        .get(&key)
                        .await?
                        .map(|value| edges::decode_edges(&value))
                        .transpose()?
                        .unwrap_or_default();
                    delta.apply_to(&mut adjacency);
                    if adjacency.is_empty() {
                        transaction.delete(&key)?;
                    } else {
                        transaction.put_with_options(
                            &key,
                            edges::encode_edges(&adjacency),
                            &slatedb::config::PutOptions {
                                ttl: slatedb::config::Ttl::NoExpiry,
                            },
                        )?;
                    }
                    self.staged_keys.insert(key);
                }
            }
            MembershipDeltaWriteMode::DisjointV2 => {
                let mut merges = Vec::with_capacity(batch.bitmaps.len() + batch.adjacency.len());
                let mut staged_keys = Vec::with_capacity(merges.capacity());
                for (row, delta) in batch.bitmaps {
                    let key = row.to_bytes();
                    #[cfg(test)]
                    if self.unchecked_disjoint_for_benchmark {
                        transaction.merge_disjoint_tokens(
                            &key,
                            delta.members().map(u128::from),
                            delta.encode(),
                        )?;
                        self.staged_keys.insert(key);
                        continue;
                    }
                    staged_keys.push(key.clone());
                    merges.push(slatedb::DisjointMergeBatchEntry::from_tokens(
                        key,
                        delta.members().map(u128::from),
                        delta.encode(),
                    ));
                }

                for ((scope, node), delta) in batch.adjacency {
                    const INCOMING_DIRECTION_TOKEN: u128 = 1_u128 << u64::BITS;

                    let key = DataKey::Data {
                        scope,
                        kind: DataKeyKind::Adjacency(AdjacencyKey::new(node)),
                    }
                    .to_bytes();
                    let outgoing = delta.outgoing_members().map(u128::from);
                    let incoming = delta
                        .incoming_members()
                        .map(|neighbor| INCOMING_DIRECTION_TOKEN | u128::from(neighbor));
                    #[cfg(test)]
                    if self.unchecked_disjoint_for_benchmark {
                        transaction.merge_disjoint_tokens(
                            &key,
                            outgoing.chain(incoming),
                            delta.encode(),
                        )?;
                        self.staged_keys.insert(key);
                        continue;
                    }
                    staged_keys.push(key.clone());
                    merges.push(slatedb::DisjointMergeBatchEntry::from_tokens(
                        key,
                        outgoing.chain(incoming),
                        delta.encode(),
                    ));
                }

                #[cfg(test)]
                if self.unchecked_disjoint_for_benchmark {
                    return Ok(());
                }
                transaction.merge_disjoint_checked_batch(merges).await?;
                self.staged_keys.extend(staged_keys);
            }
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

#[cfg(test)]
pub(super) fn node_label_key(scope: DataScope, label: &str) -> Bytes {
    DataKey::Data {
        scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::NodeLabel(
            NodeLabelKey::from_label(label).expect("test labels are valid canonical labels"),
        )),
    }
    .to_bytes()
}

#[cfg(test)]
pub(super) fn edge_pair_key(scope: DataScope, from: u64, to: u64) -> Bytes {
    DataKey::Data {
        scope,
        kind: DataKeyKind::EdgePairIndex(EdgePairIndexKey::new(from, to)),
    }
    .to_bytes()
}

#[cfg(test)]
pub(super) fn edge_label_neighbor_key(
    scope: DataScope,
    direction: EdgeDirection,
    node: u64,
    label: &str,
) -> Bytes {
    DataKey::Data {
        scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabelNeighbor(
            EdgeLabelNeighborKey::from_label(direction, node, label)
                .expect("test labels are valid canonical labels"),
        )),
    }
    .to_bytes()
}

#[cfg(test)]
pub(super) fn global_edge_label_key(scope: DataScope, label: &str) -> Bytes {
    DataKey::Data {
        scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabel(
            EdgeLabelKey::from_label(label).expect("test labels are valid canonical labels"),
        )),
    }
    .to_bytes()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use roaring::RoaringTreemap;
    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, IsolationLevel};

    use super::*;

    async fn database_with_mode(name: &str, mode: MembershipDeltaWriteMode) -> Db {
        let db = Db::builder(
            format!("topology-mutation-runtime/{name}"),
            Arc::new(InMemory::new()),
        )
        .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
        .build()
        .await
        .expect("topology test database opens");
        crate::migrations::startup::bootstrap_writer(&db)
            .await
            .expect("topology test database bootstraps storage metadata");
        if mode == MembershipDeltaWriteMode::DisjointV2 {
            crate::membership_delta::activate(&db)
                .await
                .expect("topology test database activates V2 deltas");
        }
        db
    }

    async fn database(name: &str) -> Db {
        database_with_mode(name, MembershipDeltaWriteMode::DisjointV2).await
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
                Self::NodeLabel(label) => db
                    .get(node_label_key(scope, label))
                    .await
                    .unwrap()
                    .map(|value| {
                        secondary::SecondaryEqualityValue::decode(&value)
                            .unwrap()
                            .into_ids()
                            .iter()
                            .collect()
                    })
                    .unwrap_or_default(),
                Self::OutAdjacency(node) => db
                    .get(
                        DataKey::Data {
                            scope,
                            kind: DataKeyKind::Adjacency(AdjacencyKey::new(node)),
                        }
                        .to_bytes(),
                    )
                    .await
                    .unwrap()
                    .map(|value| edges::decode_edges(&value).unwrap().iter_out().collect())
                    .unwrap_or_default(),
            }
        }
    }

    fn stage_linked_event(
        runtime: &mut TopologyMutationRuntime,
        scope: DataScope,
        parent_id: u64,
        event_id: u64,
        edge_id: u64,
        node_label: &str,
        edge_label: &str,
        mutation: MembershipMutation,
    ) {
        match mutation {
            MembershipMutation::Present => {
                runtime.add_node_label(scope, node_label, event_id).unwrap();
                runtime
                    .add_edge_pair(scope, parent_id, event_id, edge_id)
                    .unwrap();
                runtime
                    .add_edge_label(scope, parent_id, event_id, edge_label, edge_id)
                    .unwrap();
                runtime
                    .add_adjacency(
                        scope,
                        parent_id,
                        event_id,
                        helix_planner::ir::ExpandDirection::Out,
                    )
                    .unwrap();
                runtime
                    .add_adjacency(
                        scope,
                        event_id,
                        parent_id,
                        helix_planner::ir::ExpandDirection::In,
                    )
                    .unwrap();
            }
            MembershipMutation::Absent => {
                runtime
                    .remove_node_label(scope, node_label, event_id)
                    .unwrap();
                runtime
                    .remove_edge_pair(scope, parent_id, event_id, edge_id)
                    .unwrap();
                runtime
                    .remove_global_edge_label(scope, edge_label, edge_id)
                    .unwrap();
                runtime
                    .remove_edge_label_neighbors(scope, parent_id, event_id, edge_label)
                    .unwrap();
                runtime
                    .remove_adjacency(
                        scope,
                        parent_id,
                        event_id,
                        helix_planner::ir::ExpandDirection::Out,
                    )
                    .unwrap();
                runtime
                    .remove_adjacency(
                        scope,
                        event_id,
                        parent_id,
                        helix_planner::ir::ExpandDirection::In,
                    )
                    .unwrap();
            }
        }
    }

    #[tokio::test]
    async fn corrupt_topology_rows_fail_closed_in_every_write_mode() {
        let scope = DataScope::LegacyUnscoped;
        for mode in [
            MembershipDeltaWriteMode::LegacyExclusive,
            MembershipDeltaWriteMode::DisjointV2,
        ] {
            for row in [
                "node-label",
                "edge-pair",
                "edge-neighbor-out",
                "edge-neighbor-in",
                "global-edge-label",
                "adjacency",
            ] {
                let db = database_with_mode(&format!("corrupt-{mode:?}-{row}"), mode).await;
                let key = match row {
                    "node-label" => node_label_key(scope, "Corrupt"),
                    "edge-pair" => edge_pair_key(scope, 10, 11),
                    "edge-neighbor-out" => {
                        edge_label_neighbor_key(scope, EdgeDirection::Out, 10, "CORRUPT")
                    }
                    "edge-neighbor-in" => {
                        edge_label_neighbor_key(scope, EdgeDirection::In, 11, "CORRUPT")
                    }
                    "global-edge-label" => global_edge_label_key(scope, "CORRUPT"),
                    "adjacency" => DataKey::Data {
                        scope,
                        kind: DataKeyKind::Adjacency(AdjacencyKey::new(10)),
                    }
                    .to_bytes(),
                    _ => unreachable!("the fixture lists every topology row"),
                };
                let corrupt = Bytes::from_static(b"corrupt-topology-value");
                db.put(&key, corrupt.clone()).await.unwrap();

                let transaction = db
                    .begin(IsolationLevel::SerializableSnapshot)
                    .await
                    .unwrap();
                let mut runtime = TopologyMutationRuntime::default();
                match row {
                    "node-label" => runtime.remove_node_label(scope, "Corrupt", 1).unwrap(),
                    "edge-pair" => runtime.remove_edge_pair(scope, 10, 11, 1).unwrap(),
                    "edge-neighbor-out" | "edge-neighbor-in" => runtime
                        .remove_edge_label_neighbors(scope, 10, 11, "CORRUPT")
                        .unwrap(),
                    "global-edge-label" => runtime
                        .remove_global_edge_label(scope, "CORRUPT", 1)
                        .unwrap(),
                    "adjacency" => runtime
                        .remove_adjacency(scope, 10, 11, helix_planner::ir::ExpandDirection::Out)
                        .unwrap(),
                    _ => unreachable!("the fixture lists every topology row"),
                }
                assert!(runtime.flush(&transaction).await.is_err(), "{mode:?} {row}");
                transaction.rollback();
                assert_eq!(db.get(&key).await.unwrap(), Some(corrupt), "{mode:?} {row}");
                db.close().await.unwrap();
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
    async fn topology_insert_remove_races_on_disjoint_members_commit_in_both_orders() {
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

            if insert_commits_first {
                insert.commit().await.unwrap();
                remove.commit().await.unwrap();
            } else {
                remove.commit().await.unwrap();
                insert.commit().await.unwrap();
            }
            assert_eq!(row.ids(&db, scope).await, vec![2]);
        }

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn incoming_and_outgoing_tokens_do_not_alias_at_the_id_boundary() {
        const NODE: u64 = 300;
        const NEIGHBOR: u64 = u64::MAX;

        for outgoing_commits_first in [false, true] {
            let db = database(&format!(
                "adjacency-direction-token-boundary-{outgoing_commits_first}"
            ))
            .await;
            let scope = DataScope::LegacyUnscoped;
            let seed = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .unwrap();
            let mut seed_runtime = TopologyMutationRuntime::default();
            seed_runtime
                .add_adjacency(
                    scope,
                    NODE,
                    NEIGHBOR,
                    helix_planner::ir::ExpandDirection::In,
                )
                .unwrap();
            seed_runtime.prepare(&seed).await.unwrap();
            seed_runtime.consume_prepared().unwrap();
            seed.commit().await.unwrap();

            let outgoing = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .unwrap();
            let incoming = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .unwrap();
            let mut outgoing_runtime = TopologyMutationRuntime::default();
            outgoing_runtime
                .add_adjacency(
                    scope,
                    NODE,
                    NEIGHBOR,
                    helix_planner::ir::ExpandDirection::Out,
                )
                .unwrap();
            outgoing_runtime.prepare(&outgoing).await.unwrap();
            outgoing_runtime.consume_prepared().unwrap();
            let mut incoming_runtime = TopologyMutationRuntime::default();
            incoming_runtime
                .remove_adjacency(
                    scope,
                    NODE,
                    NEIGHBOR,
                    helix_planner::ir::ExpandDirection::In,
                )
                .unwrap();
            incoming_runtime.prepare(&incoming).await.unwrap();
            incoming_runtime.consume_prepared().unwrap();

            if outgoing_commits_first {
                outgoing.commit().await.unwrap();
                incoming.commit().await.unwrap();
            } else {
                incoming.commit().await.unwrap();
                outgoing.commit().await.unwrap();
            }

            let adjacency = edges::decode_edges(
                &db.get(
                    DataKey::Data {
                        scope,
                        kind: DataKeyKind::Adjacency(AdjacencyKey::new(NODE)),
                    }
                    .to_bytes(),
                )
                .await
                .unwrap()
                .expect("adjacency row remains present"),
            )
            .unwrap();
            assert_eq!(adjacency.iter_out().collect::<Vec<_>>(), vec![NEIGHBOR]);
            assert_eq!(adjacency.iter_in().collect::<Vec<_>>(), Vec::<u64>::new());
            db.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn topology_insert_remove_races_on_same_member_conflict_in_both_orders() {
        let db = database("same-member-insert-remove-races").await;
        let scope = DataScope::LegacyUnscoped;
        let cases = [
            (RaceRow::NodeLabel("insert-first"), true),
            (RaceRow::NodeLabel("remove-first"), false),
            (RaceRow::OutAdjacency(200), true),
            (RaceRow::OutAdjacency(201), false),
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
            row.stage(&mut insert_runtime, scope, 1, MembershipMutation::Present);
            insert_runtime.prepare(&insert).await.unwrap();
            insert_runtime.consume_prepared().unwrap();
            let mut remove_runtime = TopologyMutationRuntime::default();
            row.stage(&mut remove_runtime, scope, 1, MembershipMutation::Absent);
            remove_runtime.prepare(&remove).await.unwrap();
            remove_runtime.consume_prepared().unwrap();

            if insert_commits_first {
                insert.commit().await.unwrap();
                let error = remove.commit().await.expect_err("remove must conflict");
                assert_eq!(error.kind(), slatedb::ErrorKind::Transaction);
                assert_eq!(row.ids(&db, scope).await, vec![1]);
            } else {
                remove.commit().await.unwrap();
                let error = insert.commit().await.expect_err("insert must conflict");
                assert_eq!(error.kind(), slatedb::ErrorKind::Transaction);
                assert_eq!(row.ids(&db, scope).await, Vec::<u64>::new());
            }
        }

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn linked_event_insert_and_expiry_preserve_every_shared_topology_row() {
        let db = database("linked-event-insert-expiry-races").await;
        let scope = DataScope::LegacyUnscoped;

        for (case, insert_commits_first) in [(0_u64, false), (1, true)] {
            let parent_id = 1_000 + case * 100;
            let expired_event_id = parent_id + 1;
            let inserted_event_id = parent_id + 2;
            let expired_edge_id = parent_id + 10;
            let inserted_edge_id = parent_id + 11;
            let node_label = format!("Event-{case}");
            let edge_label = format!("HAS_EVENT-{case}");

            let seed = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .unwrap();
            let mut seed_runtime = TopologyMutationRuntime::default();
            stage_linked_event(
                &mut seed_runtime,
                scope,
                parent_id,
                expired_event_id,
                expired_edge_id,
                &node_label,
                &edge_label,
                MembershipMutation::Present,
            );
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
            stage_linked_event(
                &mut insert_runtime,
                scope,
                parent_id,
                inserted_event_id,
                inserted_edge_id,
                &node_label,
                &edge_label,
                MembershipMutation::Present,
            );
            insert_runtime.prepare(&insert).await.unwrap();
            insert_runtime.consume_prepared().unwrap();
            let mut remove_runtime = TopologyMutationRuntime::default();
            stage_linked_event(
                &mut remove_runtime,
                scope,
                parent_id,
                expired_event_id,
                expired_edge_id,
                &node_label,
                &edge_label,
                MembershipMutation::Absent,
            );
            remove_runtime.prepare(&remove).await.unwrap();
            remove_runtime.consume_prepared().unwrap();

            if insert_commits_first {
                insert.commit().await.unwrap();
                remove.commit().await.unwrap();
            } else {
                remove.commit().await.unwrap();
                insert.commit().await.unwrap();
            }

            let bitmap_ids = |key| async {
                db.get(key)
                    .await
                    .unwrap()
                    .map(|value| {
                        secondary::SecondaryEqualityValue::decode(&value)
                            .unwrap()
                            .into_ids()
                            .iter()
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            };
            assert_eq!(
                bitmap_ids(node_label_key(scope, &node_label)).await,
                vec![inserted_event_id]
            );
            assert_eq!(
                bitmap_ids(edge_label_neighbor_key(
                    scope,
                    EdgeDirection::Out,
                    parent_id,
                    &edge_label,
                ))
                .await,
                vec![inserted_event_id]
            );
            assert_eq!(
                bitmap_ids(global_edge_label_key(scope, &edge_label)).await,
                vec![inserted_edge_id]
            );
            assert_eq!(
                bitmap_ids(edge_pair_key(scope, parent_id, expired_event_id)).await,
                Vec::<u64>::new()
            );
            assert_eq!(
                bitmap_ids(edge_pair_key(scope, parent_id, inserted_event_id)).await,
                vec![inserted_edge_id]
            );
            let parent_adjacency = edges::decode_edges(
                &db.get(
                    DataKey::Data {
                        scope,
                        kind: DataKeyKind::Adjacency(AdjacencyKey::new(parent_id)),
                    }
                    .to_bytes(),
                )
                .await
                .unwrap()
                .expect("parent adjacency remains"),
            )
            .unwrap();
            assert_eq!(
                parent_adjacency.iter_out().collect::<Vec<_>>(),
                vec![inserted_event_id]
            );
            let expired_adjacency = db
                .get(
                    DataKey::Data {
                        scope,
                        kind: DataKeyKind::Adjacency(AdjacencyKey::new(expired_event_id)),
                    }
                    .to_bytes(),
                )
                .await
                .unwrap()
                .map(|value| edges::decode_edges(&value).unwrap())
                .unwrap_or_default();
            assert!(expired_adjacency.is_empty());
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
    async fn coalesced_membership_deltas_keep_the_last_mutation_per_member() {
        let db = database("coalesced-last-membership-mutation").await;
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let scope = DataScope::LegacyUnscoped;
        let mut runtime = TopologyMutationRuntime::default();

        runtime.add_node_label(scope, "User", 1).unwrap();
        runtime.remove_node_label(scope, "User", 1).unwrap();
        runtime.remove_node_label(scope, "User", 2).unwrap();
        runtime.add_node_label(scope, "User", 2).unwrap();
        runtime
            .add_adjacency(scope, 9, 1, helix_planner::ir::ExpandDirection::Both)
            .unwrap();
        runtime
            .remove_adjacency(scope, 9, 1, helix_planner::ir::ExpandDirection::Out)
            .unwrap();
        runtime
            .remove_adjacency(scope, 9, 2, helix_planner::ir::ExpandDirection::Both)
            .unwrap();
        runtime
            .add_adjacency(scope, 9, 2, helix_planner::ir::ExpandDirection::Out)
            .unwrap();
        runtime.prepare(&transaction).await.unwrap();
        runtime.consume_prepared().unwrap();
        transaction.commit().await.unwrap();

        assert_eq!(
            secondary::SecondaryEqualityValue::decode(
                &db.get(node_label_key(scope, "User"))
                    .await
                    .unwrap()
                    .expect("coalesced node-label row exists"),
            )
            .unwrap()
            .into_ids()
            .iter()
            .collect::<Vec<_>>(),
            vec![2]
        );
        let adjacency = edges::decode_edges(
            &db.get(
                DataKey::Data {
                    scope,
                    kind: DataKeyKind::Adjacency(AdjacencyKey::new(9)),
                }
                .to_bytes(),
            )
            .await
            .unwrap()
            .expect("coalesced adjacency row exists"),
        )
        .unwrap();
        assert_eq!(adjacency.iter_out().collect::<Vec<_>>(), vec![2]);
        assert_eq!(adjacency.iter_in().collect::<Vec<_>>(), vec![1]);

        db.close().await.unwrap();
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

    #[derive(Debug, Clone, Copy)]
    enum SweepBenchmarkStrategy {
        ExclusiveReadModifyWrite,
        UncheckedDisjointDelta,
        DisjointDelta,
    }

    #[derive(Debug)]
    struct SweepConflictBenchmarkResult {
        writer_conflicts: usize,
        sweeper_conflicts: usize,
        writer_commits: usize,
        sweeper_commits: usize,
    }

    #[derive(Debug)]
    struct SweepPreparationBenchmarkResult {
        minimum: Duration,
        median: Duration,
        maximum: Duration,
        samples: usize,
    }

    async fn stage_benchmark_memberships(
        db: &Db,
        strategy: SweepBenchmarkStrategy,
        ids: std::ops::Range<u64>,
        present: bool,
    ) -> DbTransaction {
        const PARENT_ID: u64 = u64::MAX - 1;
        const LABEL: &str = "KubernetesEvent";

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("benchmark transaction begins");
        match strategy {
            SweepBenchmarkStrategy::ExclusiveReadModifyWrite if present => {
                let additions = ids.clone().collect::<RoaringTreemap>();
                transaction
                    .merge_commutative(
                        node_label_key(DataScope::LegacyUnscoped, LABEL),
                        secondary::SecondaryEqualityValue::encode_ids(&additions),
                    )
                    .expect("exclusive benchmark label additions stage");
                let mut adjacency = edges::Edges::new();
                for id in ids {
                    adjacency.add_out(id);
                }
                transaction
                    .merge_commutative(
                        DataKey::Data {
                            scope: DataScope::LegacyUnscoped,
                            kind: DataKeyKind::Adjacency(AdjacencyKey::new(PARENT_ID)),
                        }
                        .to_bytes(),
                        edges::encode_edges(&adjacency),
                    )
                    .expect("exclusive benchmark adjacency additions stage");
            }
            SweepBenchmarkStrategy::ExclusiveReadModifyWrite => {
                let label_key = node_label_key(DataScope::LegacyUnscoped, LABEL);
                let adjacency_key = DataKey::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::Adjacency(AdjacencyKey::new(PARENT_ID)),
                }
                .to_bytes();
                let values = transaction
                    .multi_get(&[label_key.clone(), adjacency_key.clone()])
                    .await
                    .expect("exclusive benchmark rows read");
                let mut labels = values[0]
                    .as_deref()
                    .map(secondary::SecondaryEqualityValue::decode)
                    .transpose()
                    .expect("exclusive benchmark labels decode")
                    .map(secondary::SecondaryEqualityValue::into_ids)
                    .unwrap_or_default();
                let mut adjacency = values[1]
                    .as_deref()
                    .map(edges::decode_edges)
                    .transpose()
                    .expect("exclusive benchmark adjacency decodes")
                    .unwrap_or_default();
                for id in ids {
                    labels.remove(id);
                    adjacency.remove_out(id);
                }
                if labels.is_empty() {
                    transaction
                        .delete(label_key)
                        .expect("exclusive benchmark empty label row deletes");
                } else {
                    transaction
                        .put(
                            label_key,
                            secondary::SecondaryEqualityValue::encode_ids(&labels),
                        )
                        .expect("exclusive benchmark label replacement stages");
                }
                if adjacency.is_empty() {
                    transaction
                        .delete(adjacency_key)
                        .expect("exclusive benchmark empty adjacency row deletes");
                } else {
                    transaction
                        .put(adjacency_key, edges::encode_edges(&adjacency))
                        .expect("exclusive benchmark adjacency replacement stages");
                }
            }
            SweepBenchmarkStrategy::UncheckedDisjointDelta
            | SweepBenchmarkStrategy::DisjointDelta => {
                let mut runtime = TopologyMutationRuntime::default();
                runtime.unchecked_disjoint_for_benchmark =
                    matches!(strategy, SweepBenchmarkStrategy::UncheckedDisjointDelta);
                for id in ids {
                    if present {
                        runtime
                            .add_node_label(DataScope::LegacyUnscoped, LABEL, id)
                            .unwrap();
                        runtime
                            .add_adjacency(
                                DataScope::LegacyUnscoped,
                                PARENT_ID,
                                id,
                                helix_planner::ir::ExpandDirection::Out,
                            )
                            .unwrap();
                    } else {
                        runtime
                            .remove_node_label(DataScope::LegacyUnscoped, LABEL, id)
                            .unwrap();
                        runtime
                            .remove_adjacency(
                                DataScope::LegacyUnscoped,
                                PARENT_ID,
                                id,
                                helix_planner::ir::ExpandDirection::Out,
                            )
                            .unwrap();
                    }
                }
                runtime.prepare(&transaction).await.unwrap();
                runtime.consume_prepared().unwrap();
            }
        }
        transaction
    }

    async fn run_sweep_conflict_benchmark(
        strategy: SweepBenchmarkStrategy,
        expired_members: u64,
        inserted_members: u64,
        sweep_chunk: u64,
        writer_chunk: u64,
    ) -> SweepConflictBenchmarkResult {
        const PARENT_ID: u64 = u64::MAX - 1;
        const LABEL: &str = "KubernetesEvent";

        let db = Arc::new(
            database(match strategy {
                SweepBenchmarkStrategy::ExclusiveReadModifyWrite => "benchmark-exclusive",
                SweepBenchmarkStrategy::UncheckedDisjointDelta => "benchmark-unchecked-disjoint",
                SweepBenchmarkStrategy::DisjointDelta => "benchmark-disjoint",
            })
            .await,
        );
        if !matches!(strategy, SweepBenchmarkStrategy::ExclusiveReadModifyWrite) {
            crate::membership_delta::activate(db.as_ref())
                .await
                .expect("disjoint conflict benchmark activates V2 membership deltas");
        }
        let label_key = node_label_key(DataScope::LegacyUnscoped, LABEL);
        db.put(
            &label_key,
            secondary::SecondaryEqualityValue::encode_ids(
                &(0..expired_members).collect::<RoaringTreemap>(),
            ),
        )
        .await
        .expect("benchmark label seed persists");
        let mut adjacency = edges::Edges::new();
        for id in 0..expired_members {
            adjacency.add_out(id);
        }
        db.put(
            DataKey::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::Adjacency(AdjacencyKey::new(PARENT_ID)),
            }
            .to_bytes(),
            edges::encode_edges(&adjacency),
        )
        .await
        .expect("benchmark adjacency seed persists");

        let writer_chunks = inserted_members.div_ceil(writer_chunk) as usize;
        let sweeper_chunks = expired_members.div_ceil(sweep_chunk) as usize;
        let synchronized_chunks = writer_chunks.min(sweeper_chunks);
        let barriers = (0..synchronized_chunks)
            .map(|_| Arc::new(tokio::sync::Barrier::new(2)))
            .collect::<Vec<_>>();
        let writer_progress = Arc::new(AtomicUsize::new(0));
        let writer_notify = Arc::new(tokio::sync::Notify::new());
        let writer = {
            let db = Arc::clone(&db);
            let barriers = barriers.clone();
            let writer_progress = Arc::clone(&writer_progress);
            let writer_notify = Arc::clone(&writer_notify);
            tokio::spawn(async move {
                let mut conflicts = 0;
                for chunk in 0..writer_chunks {
                    let start = expired_members + chunk as u64 * writer_chunk;
                    let end = (start + writer_chunk).min(expired_members + inserted_members);
                    let mut first_attempt = true;
                    loop {
                        let transaction =
                            stage_benchmark_memberships(&db, strategy, start..end, true).await;
                        if first_attempt && chunk < synchronized_chunks {
                            barriers[chunk].wait().await;
                        }
                        match transaction.commit().await {
                            Ok(_) => break,
                            Err(error) if error.kind() == slatedb::ErrorKind::Transaction => {
                                conflicts += 1;
                                first_attempt = false;
                            }
                            Err(error) => panic!("benchmark writer commit failed: {error}"),
                        }
                    }
                    writer_progress.store(chunk + 1, Ordering::Release);
                    writer_notify.notify_waiters();
                }
                conflicts
            })
        };
        let sweeper = {
            let db = Arc::clone(&db);
            let barriers = barriers.clone();
            let writer_progress = Arc::clone(&writer_progress);
            let writer_notify = Arc::clone(&writer_notify);
            tokio::spawn(async move {
                let mut conflicts = 0;
                for chunk in 0..sweeper_chunks {
                    let start = chunk as u64 * sweep_chunk;
                    let end = (start + sweep_chunk).min(expired_members);
                    let mut first_attempt = true;
                    loop {
                        let transaction =
                            stage_benchmark_memberships(&db, strategy, start..end, false).await;
                        if first_attempt && chunk < synchronized_chunks {
                            barriers[chunk].wait().await;
                            loop {
                                let notified = writer_notify.notified();
                                if writer_progress.load(Ordering::Acquire) > chunk {
                                    break;
                                }
                                notified.await;
                            }
                        }
                        match transaction.commit().await {
                            Ok(_) => break,
                            Err(error) if error.kind() == slatedb::ErrorKind::Transaction => {
                                conflicts += 1;
                                first_attempt = false;
                            }
                            Err(error) => panic!("benchmark sweeper commit failed: {error}"),
                        }
                    }
                }
                conflicts
            })
        };
        let writer_conflicts = writer.await.expect("benchmark writer joins");
        let sweeper_conflicts = sweeper.await.expect("benchmark sweeper joins");
        let labels = secondary::SecondaryEqualityValue::decode(
            &db.get(&label_key)
                .await
                .expect("benchmark result reads")
                .expect("inserted memberships remain"),
        )
        .expect("benchmark result decodes")
        .into_ids();
        assert_eq!(labels.len(), inserted_members);
        assert_eq!(labels.min(), Some(expired_members));
        assert_eq!(labels.max(), Some(expired_members + inserted_members - 1));
        let adjacency = edges::decode_edges(
            &db.get(
                DataKey::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::Adjacency(AdjacencyKey::new(PARENT_ID)),
                }
                .to_bytes(),
            )
            .await
            .expect("benchmark adjacency result reads")
            .expect("inserted adjacency memberships remain"),
        )
        .expect("benchmark adjacency result decodes");
        let outgoing = adjacency.iter_out().collect::<RoaringTreemap>();
        assert_eq!(outgoing.len(), inserted_members);
        assert_eq!(outgoing.min(), Some(expired_members));
        assert_eq!(outgoing.max(), Some(expired_members + inserted_members - 1));
        db.close().await.expect("benchmark database closes");

        SweepConflictBenchmarkResult {
            writer_conflicts,
            sweeper_conflicts,
            writer_commits: writer_chunks,
            sweeper_commits: sweeper_chunks,
        }
    }

    async fn run_sweep_preparation_benchmark(
        strategy: SweepBenchmarkStrategy,
        expired_members: u64,
        sweep_chunk: u64,
        samples: usize,
    ) -> SweepPreparationBenchmarkResult {
        const PARENT_ID: u64 = u64::MAX - 1;
        const LABEL: &str = "KubernetesEvent";

        let db = database(match strategy {
            SweepBenchmarkStrategy::ExclusiveReadModifyWrite => "prepare-exclusive",
            SweepBenchmarkStrategy::UncheckedDisjointDelta => "prepare-unchecked-disjoint",
            SweepBenchmarkStrategy::DisjointDelta => "prepare-disjoint",
        })
        .await;
        if !matches!(strategy, SweepBenchmarkStrategy::ExclusiveReadModifyWrite) {
            crate::membership_delta::activate(&db)
                .await
                .expect("disjoint preparation benchmark activates V2 membership deltas");
        }
        db.put(
            node_label_key(DataScope::LegacyUnscoped, LABEL),
            secondary::SecondaryEqualityValue::encode_ids(
                &(0..expired_members).collect::<RoaringTreemap>(),
            ),
        )
        .await
        .expect("preparation benchmark label seed persists");
        let mut adjacency = edges::Edges::new();
        for id in 0..expired_members {
            adjacency.add_out(id);
        }
        db.put(
            DataKey::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::Adjacency(AdjacencyKey::new(PARENT_ID)),
            }
            .to_bytes(),
            edges::encode_edges(&adjacency),
        )
        .await
        .expect("preparation benchmark adjacency seed persists");

        // Warm storage and codec paths before collecting paired samples. The
        // transaction is deliberately not committed so every sample stages a
        // sweep against the same full hot rows.
        drop(
            stage_benchmark_memberships(&db, strategy, 0..sweep_chunk.min(expired_members), false)
                .await,
        );

        let mut elapsed = Vec::with_capacity(samples);
        for _ in 0..samples {
            let started = Instant::now();
            for start in (0..expired_members).step_by(sweep_chunk as usize) {
                drop(
                    stage_benchmark_memberships(
                        &db,
                        strategy,
                        start..(start + sweep_chunk).min(expired_members),
                        false,
                    )
                    .await,
                );
            }
            elapsed.push(started.elapsed());
        }
        elapsed.sort_unstable();
        db.close()
            .await
            .expect("preparation benchmark database closes");

        SweepPreparationBenchmarkResult {
            minimum: elapsed[0],
            median: elapsed[elapsed.len() / 2],
            maximum: elapsed[elapsed.len() - 1],
            samples: elapsed.len(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "manual 100K-member conflict and preparation-performance comparison"]
    async fn benchmark_exclusive_vs_disjoint_linked_event_sweep() {
        const MAX_CHECKED_PREPARATION_REGRESSION: f64 = 1.10;

        let expired_members = std::env::var("HELIX_SWEEP_BENCH_EXPIRED")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(100_000);
        let inserted_members = std::env::var("HELIX_SWEEP_BENCH_INSERTED")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(10_000);
        let sweep_chunk = std::env::var("HELIX_SWEEP_BENCH_SWEEP_CHUNK")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1_000);
        let writer_chunk = std::env::var("HELIX_SWEEP_BENCH_WRITER_CHUNK")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(250);
        let performance_samples = std::env::var("HELIX_SWEEP_BENCH_SAMPLES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(9);
        assert!(expired_members > 0);
        assert!(inserted_members > 0);
        assert!(sweep_chunk > 0);
        assert!(writer_chunk > 0);
        assert!(performance_samples >= 3);
        assert!(performance_samples % 2 == 1);

        let before_conflicts = run_sweep_conflict_benchmark(
            SweepBenchmarkStrategy::ExclusiveReadModifyWrite,
            expired_members,
            inserted_members,
            sweep_chunk,
            writer_chunk,
        )
        .await;
        let after_conflicts = run_sweep_conflict_benchmark(
            SweepBenchmarkStrategy::DisjointDelta,
            expired_members,
            inserted_members,
            sweep_chunk,
            writer_chunk,
        )
        .await;
        let before_sweep_conflict_rate =
            before_conflicts.sweeper_conflicts as f64 / before_conflicts.sweeper_commits as f64;
        let after_sweep_conflict_rate =
            after_conflicts.sweeper_conflicts as f64 / after_conflicts.sweeper_commits as f64;

        let before_preparation = run_sweep_preparation_benchmark(
            SweepBenchmarkStrategy::ExclusiveReadModifyWrite,
            expired_members,
            sweep_chunk,
            performance_samples,
        )
        .await;
        let after_preparation = run_sweep_preparation_benchmark(
            SweepBenchmarkStrategy::DisjointDelta,
            expired_members,
            sweep_chunk,
            performance_samples,
        )
        .await;
        let unchecked_preparation = run_sweep_preparation_benchmark(
            SweepBenchmarkStrategy::UncheckedDisjointDelta,
            expired_members,
            sweep_chunk,
            performance_samples,
        )
        .await;
        let before_preparation_throughput =
            expired_members as f64 / before_preparation.median.as_secs_f64();
        let after_preparation_throughput =
            expired_members as f64 / after_preparation.median.as_secs_f64();
        let unchecked_preparation_throughput =
            expired_members as f64 / unchecked_preparation.median.as_secs_f64();
        let exclusive_preparation_time_ratio =
            after_preparation.median.as_secs_f64() / before_preparation.median.as_secs_f64();
        let checked_preparation_time_ratio =
            after_preparation.median.as_secs_f64() / unchecked_preparation.median.as_secs_f64();

        println!(
            "SWEEP_CONFLICT before sweep_commits={} sweep_conflicts={} sweep_conflict_rate={:.4} writer_commits={} writer_conflicts={}",
            before_conflicts.sweeper_commits,
            before_conflicts.sweeper_conflicts,
            before_sweep_conflict_rate,
            before_conflicts.writer_commits,
            before_conflicts.writer_conflicts,
        );
        println!(
            "SWEEP_CONFLICT after sweep_commits={} sweep_conflicts={} sweep_conflict_rate={:.4} writer_commits={} writer_conflicts={}",
            after_conflicts.sweeper_commits,
            after_conflicts.sweeper_conflicts,
            after_sweep_conflict_rate,
            after_conflicts.writer_commits,
            after_conflicts.writer_conflicts,
        );
        println!(
            "SWEEP_PREP before samples={} min_ms={} median_ms={} max_ms={} throughput_members_per_s={:.0}",
            before_preparation.samples,
            before_preparation.minimum.as_millis(),
            before_preparation.median.as_millis(),
            before_preparation.maximum.as_millis(),
            before_preparation_throughput,
        );
        println!(
            "SWEEP_PREP unchecked samples={} min_ms={} median_ms={} max_ms={} throughput_members_per_s={:.0}",
            unchecked_preparation.samples,
            unchecked_preparation.minimum.as_millis(),
            unchecked_preparation.median.as_millis(),
            unchecked_preparation.maximum.as_millis(),
            unchecked_preparation_throughput,
        );
        println!(
            "SWEEP_PREP after samples={} min_ms={} median_ms={} max_ms={} throughput_members_per_s={:.0}",
            after_preparation.samples,
            after_preparation.minimum.as_millis(),
            after_preparation.median.as_millis(),
            after_preparation.maximum.as_millis(),
            after_preparation_throughput,
        );
        println!(
            "SWEEP_DELTA conflict_rate_points={:.2} exclusive_preparation_time_ratio={:.3} checked_preparation_time_ratio={:.3}",
            (after_sweep_conflict_rate - before_sweep_conflict_rate) * 100.0,
            exclusive_preparation_time_ratio,
            checked_preparation_time_ratio,
        );

        assert!(before_conflicts.sweeper_conflicts > 0);
        assert_eq!(after_conflicts.sweeper_conflicts, 0);
        assert_eq!(after_conflicts.writer_conflicts, 0);
        assert!(
            checked_preparation_time_ratio <= MAX_CHECKED_PREPARATION_REGRESSION,
            "checked validation exceeded the {:.0}% preparation regression budget: unchecked={:?}, checked={:?}",
            (MAX_CHECKED_PREPARATION_REGRESSION - 1.0) * 100.0,
            unchecked_preparation,
            after_preparation,
        );
    }
}
