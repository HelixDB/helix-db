//! Node storage mutation contracts.
//!
//! Each helper stages the authoritative node row together with every V2
//! secondary/vector/text action captured by the request transaction. Active
//! secondary/vector generations receive physical entry
//! changes; hidden building generations receive one coalesced entity delta in
//! that same transaction.

use std::collections::{BTreeMap, BTreeSet};

use super::contracts::label_of;
use super::edge::PairMembers;
use super::MutationIndexContext;
use super::*;
use crate::index_lifecycle::graph_mutation::{
    CanonicalPropertyRow, GraphEntity, GraphMutationTransition, PropertyEdit, PropertyEditOutcome,
};

/// Nodes read and staged together. Each batch ends with a topology flush,
/// because the next batch's reads must see it. A delete's peak memory follows its largest
/// batch, so a batch also ends at the node reaching [`DELETE_BATCH_PAIRS`], and
/// shrinks when the request's budget refuses it.
const NODE_DELETE_BATCH: usize = 512;

/// Neighbor pairs that end a batch. Each pair loads a pair row and its edges.
const DELETE_BATCH_PAIRS: usize = 4096;

/// Topology rows a delete collects before flushing them. A flush builds one
/// merge per row, so this bounds its working memory, and the deltas waiting
/// for it, at what deleting a few nodes needs. Staging uses the batch's
/// observations, so a flush in mid-batch changes no result.
pub(in crate::execution::interpreter) const DELETE_FLUSH_ROWS: usize = 512;

/// One batch's node rows and the neighbor pairs attached to them.
struct ObservedNodeDeletions {
    rows: BTreeMap<u64, CanonicalPropertyRow>,
    pairs: BTreeSet<(u64, u64)>,
    /// Requested nodes covered, in ID order. The rest wait for a later batch.
    covered: usize,
    _memory: Option<crate::query_resources::Reservation>,
}

/// Sorted, deduplicated node-row observations with an ordered mutation overlay.
pub(super) struct ObservedNodeRows {
    rows: BTreeMap<u64, Option<CanonicalPropertyRow>>,
    _memory: Option<crate::query_resources::Reservation>,
}

/// Distinct node-existence observations used by batched endpoint validation.
pub(super) struct ObservedNodeExistence {
    present: BTreeSet<u64>,
}

impl ObservedNodeExistence {
    pub(super) fn contains(&self, node_id: u64) -> bool {
        self.present.contains(&node_id)
    }

    pub(super) fn require(&self, node_id: u64) -> Result<()> {
        if self.present.contains(&node_id) {
            Ok(())
        } else {
            Err(HelixDbError::NodeNotFound(node_id))
        }
    }
}

impl ObservedNodeRows {
    pub(super) fn observed(&self, node_id: u64) -> Option<CanonicalPropertyRow> {
        self.rows.get(&node_id).cloned().flatten()
    }

    pub(super) fn replace(&mut self, node_id: u64, row: Option<CanonicalPropertyRow>) {
        assert!(
            self.rows.insert(node_id, row).is_some(),
            "a node observation overlay only replaces requested entities"
        );
    }
}

impl<'db> ExecutionContext<'db> {
    pub(super) async fn store_node(
        &self,
        txn: &impl crate::transaction::Mutation,
        node_id: u64,
        properties: Vec<Property>,
        index_context: &mut MutationIndexContext,
    ) -> Result<()> {
        let transition = GraphMutationTransition::create(
            self.tenant_scope,
            GraphEntity::node(node_id),
            CanonicalPropertyRow::new_with_budget(properties, self.row_memory.as_ref())?,
        );
        let properties = transition
            .after()
            .expect("a create transition has an after row")
            .properties();
        if let Some(label) = label_of(properties) {
            index_context
                .topology_mutations()
                .add_node_label(self.tenant_scope, label, node_id)?;
        }
        let encoded = transition
            .after()
            .expect("a create transition has an after row")
            .write_payload();
        index_context.maintain_graph_indexes(transition, self.row_memory.as_ref())?;
        index_context.property_writes.stage(
            txn,
            self.tenant_scope,
            GraphEntity::node(node_id),
            Some(encoded),
            self.row_memory.as_ref(),
        )?;
        Ok(())
    }

    #[cfg(test)]
    pub(super) async fn set_node_property(
        &self,
        txn: &impl crate::transaction::Mutation,
        node_id: u64,
        property: Property,
        index_context: &mut MutationIndexContext,
    ) -> Result<()> {
        if property.name == "$label" && property.value.as_str().is_none() {
            return Err(HelixDbError::Query(
                "node `$label` mutations require a string value".to_string(),
            ));
        }
        let key = self.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
            node_id,
        )));
        let observed = txn
            .get(&key)
            .await?
            .map(|encoded| {
                CanonicalPropertyRow::decode_with_budget(encoded, self.row_memory.as_ref())
            })
            .transpose()?;
        let _ = self
            .set_node_property_observed(txn, node_id, property, observed, index_context)
            .await?;
        index_context.flush_topology(txn)?;
        Ok(())
    }

    pub(super) async fn set_node_property_observed(
        &self,
        txn: &impl crate::transaction::Mutation,
        node_id: u64,
        property: Property,
        observed: Option<CanonicalPropertyRow>,
        index_context: &mut MutationIndexContext,
    ) -> Result<CanonicalPropertyRow> {
        if property.name == "$label" && property.value.as_str().is_none() {
            return Err(HelixDbError::Query(
                "node `$label` mutations require a string value".to_string(),
            ));
        }
        let Some(before) = observed else {
            return Err(HelixDbError::InvariantViolation(
                "Active text graph source disagrees with its supplied before state".to_string(),
            ));
        };
        let outcome = GraphMutationTransition::edit_with_budget(
            self.tenant_scope,
            GraphEntity::node(node_id),
            before,
            PropertyEdit::set(property),
            self.row_memory.as_ref(),
        )?;
        let PropertyEditOutcome::Changed(transition) = outcome else {
            let PropertyEditOutcome::Unchanged(row) = outcome else {
                unreachable!("property edit outcomes are closed")
            };
            return Ok(row);
        };
        let old_properties = transition
            .before()
            .expect("a replacement transition has a before row")
            .properties();
        let properties = transition
            .after()
            .expect("a replacement transition has an after row")
            .properties();
        let old_label = label_of(old_properties);
        if transition
            .changed()
            .expect("a replacement transition has changed properties")
            .contains("$label")
        {
            let Some(new_label) = label_of(properties) else {
                return Err(HelixDbError::InvariantViolation(
                    "validated node label lost its string value".to_string(),
                ));
            };
            if old_label != Some(new_label) {
                old_label
                    .map(|old_label| {
                        index_context.topology_mutations().remove_node_label(
                            self.tenant_scope,
                            old_label,
                            node_id,
                        )
                    })
                    .transpose()?;
                index_context.topology_mutations().add_node_label(
                    self.tenant_scope,
                    new_label,
                    node_id,
                )?;
            }
        }
        let encoded = transition
            .after()
            .expect("a replacement transition has an after row")
            .write_payload();
        let final_row = transition
            .after()
            .expect("a replacement transition has an after row")
            .clone();
        index_context.maintain_graph_indexes(transition, self.row_memory.as_ref())?;
        index_context.property_writes.stage(
            txn,
            self.tenant_scope,
            GraphEntity::node(node_id),
            Some(encoded),
            self.row_memory.as_ref(),
        )?;
        Ok(final_row)
    }

    #[cfg(test)]
    pub(super) async fn remove_node_property(
        &self,
        txn: &impl crate::transaction::Mutation,
        node_id: u64,
        name: &ir::NonEmptyString,
        index_context: &mut MutationIndexContext,
    ) -> Result<()> {
        if name.as_ref() == "$label" {
            return Err(HelixDbError::Query(
                "node `$label` cannot be removed".to_string(),
            ));
        }
        let key = self.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
            node_id,
        )));
        let observed = txn
            .get(&key)
            .await?
            .map(|encoded| {
                CanonicalPropertyRow::decode_with_budget(encoded, self.row_memory.as_ref())
            })
            .transpose()?;
        let _ = self
            .remove_node_property_observed(txn, node_id, name, observed, index_context)
            .await?;
        Ok(())
    }

    pub(super) async fn remove_node_property_observed(
        &self,
        txn: &impl crate::transaction::Mutation,
        node_id: u64,
        name: &ir::NonEmptyString,
        observed: Option<CanonicalPropertyRow>,
        index_context: &mut MutationIndexContext,
    ) -> Result<Option<CanonicalPropertyRow>> {
        if name.as_ref() == "$label" {
            return Err(HelixDbError::Query(
                "node `$label` cannot be removed".to_string(),
            ));
        }
        let Some(before) = observed else {
            return Ok(None);
        };
        let outcome = GraphMutationTransition::edit_with_budget(
            self.tenant_scope,
            GraphEntity::node(node_id),
            before,
            PropertyEdit::remove(name.as_ref()),
            self.row_memory.as_ref(),
        )?;
        let PropertyEditOutcome::Changed(transition) = outcome else {
            let PropertyEditOutcome::Unchanged(row) = outcome else {
                unreachable!("property edit outcomes are closed")
            };
            return Ok(Some(row));
        };
        let encoded = transition
            .after()
            .expect("a replacement transition has an after row")
            .write_payload();
        let final_row = transition
            .after()
            .expect("a replacement transition has an after row")
            .clone();
        index_context.maintain_graph_indexes(transition, self.row_memory.as_ref())?;
        index_context.property_writes.stage(
            txn,
            self.tenant_scope,
            GraphEntity::node(node_id),
            Some(encoded),
            self.row_memory.as_ref(),
        )?;
        Ok(Some(final_row))
    }

    pub(super) async fn observe_node_rows(
        &self,
        txn: &impl crate::transaction::Mutation,
        node_ids: impl IntoIterator<Item = u64>,
    ) -> Result<ObservedNodeRows> {
        let requested = super::observations::RowKeys::new(
            node_ids,
            super::observations::Kind::Nodes,
            self.tenant_scope,
            self.row_memory.as_ref(),
        )?;
        if requested.ids.is_empty() {
            return Ok(ObservedNodeRows {
                rows: BTreeMap::new(),
                _memory: None,
            });
        }
        let memory = self
            .row_memory
            .as_ref()
            .map(|budget| {
                budget.reserve(helix_planner::relational::allocation::btree_bytes::<
                    u64,
                    Option<CanonicalPropertyRow>,
                >(requested.ids.len()))
            })
            .transpose()?;
        let keys = &requested.keys;
        let request = crate::query_resources::properties::ReadRequest::new(
            keys.len(),
            self.row_memory.as_ref(),
        )?;
        self.row_memory.iter().for_each(|budget| {
            budget.record_reads(crate::query_resources::StorageReadUsage {
                multi_get_batches: 1,
                multi_get_keys: keys.len(),
                ..Default::default()
            })
        });
        let values = txn.multi_get(keys).await?;
        let mut decoded = requested
            .ids
            .iter()
            .copied()
            .zip(request.attach(values)?)
            .map(|(node_id, value)| {
                value
                    .map(CanonicalPropertyRow::decode_read)
                    .transpose()
                    .map(|row| (node_id, row))
            });
        let rows = if self.row_memory.is_some() {
            // Avoid FromIterator's extra sorting vector while all raw reads
            // and already decoded snapshots are live.
            decoded.try_fold(BTreeMap::new(), |mut rows, entry| {
                let (id, row) = entry?;
                rows.insert(id, row);
                Ok::<_, HelixDbError>(rows)
            })?
        } else {
            decoded.collect::<Result<BTreeMap<_, _>>>()?
        };
        Ok(ObservedNodeRows {
            rows,
            _memory: memory,
        })
    }

    /// One read of which `node_ids` exist in the transaction's view, its own
    /// writes included, taking no read dependency on their rows. The caller
    /// must stage an adjacency merge on each node it relies on, or record the
    /// read so commit restores the dependency: see `ExistenceReads`.
    pub(super) async fn observe_node_existence(
        &self,
        txn: &impl crate::transaction::Mutation,
        node_ids: impl IntoIterator<Item = u64>,
    ) -> Result<ObservedNodeExistence> {
        let requested = super::observations::RowKeys::new(
            node_ids,
            super::observations::Kind::Nodes,
            self.tenant_scope,
            self.row_memory.as_ref(),
        )?;
        if requested.ids.is_empty() {
            return Ok(ObservedNodeExistence {
                present: BTreeSet::new(),
            });
        }
        let request = crate::query_resources::properties::ReadRequest::new(
            requested.keys.len(),
            self.row_memory.as_ref(),
        )?;
        self.row_memory.iter().for_each(|budget| {
            budget.record_reads(crate::query_resources::StorageReadUsage {
                multi_get_batches: 1,
                multi_get_keys: requested.keys.len(),
                ..Default::default()
            })
        });
        let values = request.attach(txn.read_without_dependency(&requested.keys).await?)?;
        Ok(ObservedNodeExistence {
            present: requested
                .ids
                .iter()
                .copied()
                .zip(values)
                .filter_map(|(id, value)| value.map(|_| id))
                .collect(),
        })
    }

    /// One read of each node's row and adjacency row, stopping at the node
    /// that reaches [`DELETE_BATCH_PAIRS`].
    async fn observe_node_deletions(
        &self,
        txn: &impl crate::transaction::Mutation,
        node_ids: &[u64],
    ) -> Result<ObservedNodeDeletions> {
        let requested = super::observations::RowKeys::new(
            node_ids.iter().copied(),
            super::observations::Kind::NodeDeletions,
            self.tenant_scope,
            self.row_memory.as_ref(),
        )?;
        if requested.ids.is_empty() {
            return Ok(ObservedNodeDeletions {
                rows: BTreeMap::new(),
                pairs: BTreeSet::new(),
                covered: 0,
                _memory: None,
            });
        }
        let row_bytes = helix_planner::relational::allocation::btree_bytes::<
            u64,
            CanonicalPropertyRow,
        >(requested.ids.len());
        let mut memory = self
            .row_memory
            .as_ref()
            .map(|budget| budget.reserve(row_bytes))
            .transpose()?;
        let request = crate::query_resources::properties::ReadRequest::new(
            requested.keys.len(),
            self.row_memory.as_ref(),
        )?;
        self.row_memory.iter().for_each(|budget| {
            budget.record_reads(crate::query_resources::StorageReadUsage {
                multi_get_batches: 1,
                multi_get_keys: requested.keys.len(),
                ..Default::default()
            })
        });
        let mut values = request.attach(txn.multi_get(&requested.keys).await?)?;
        let mut rows = BTreeMap::new();
        let mut pairs = BTreeSet::new();
        let mut covered = 0;
        for node_id in requested.ids.iter().copied() {
            self.check_execution_deadline()?;
            let row = values.next().expect("each node has one row result");
            let adjacency = values.next().expect("each node has one adjacency result");
            covered += 1;
            let Some(row) = row else {
                continue;
            };
            rows.insert(node_id, CanonicalPropertyRow::decode_read(row)?);
            let Some(adjacency) = adjacency else {
                continue;
            };
            let edges = values::adjacency::decode_edges(adjacency.bytes())?;
            let added = usize::try_from(edges.nxts_out.len().saturating_add(edges.nxts_in.len()))
                .map_err(|_| HelixDbError::QueryMemoryLimitExceeded)?;
            memory
                .as_mut()
                .map(|memory| {
                    memory.resize(row_bytes.saturating_add(
                        helix_planner::relational::allocation::btree_bytes::<(u64, u64), ()>(
                            pairs.len().saturating_add(added),
                        ),
                    ))
                })
                .transpose()?;
            pairs.extend(
                edges
                    .iter_out()
                    .map(|to| (node_id, to))
                    .chain(edges.iter_in().map(|from| (from, node_id))),
            );
            if pairs.len() >= DELETE_BATCH_PAIRS {
                break;
            }
        }
        Ok(ObservedNodeDeletions {
            rows,
            pairs,
            covered,
            _memory: memory,
        })
    }

    /// Deletes `edges`, then `nodes`, both strictly ascending. A node without
    /// a row is skipped. Each batch makes one multi-get per dependency level,
    /// however many nodes it holds: node and adjacency rows, then pair rows,
    /// then edge rows, with no row read twice. It flushes topology once per
    /// [`DELETE_FLUSH_ROWS`] collected rows and once at its end.
    ///
    /// `check_attached` sees each batch's pairs before anything of that batch
    /// is staged. Edges of `edges` count as pair members until their batch
    /// deletes them. A refusal may follow staged earlier batches, so the
    /// caller must drop the transaction.
    ///
    /// Observation only reads, so a batch the memory budget refuses retries
    /// with half its nodes. One node may need all the memory it ever did.
    pub(super) async fn delete_elements<E: From<HelixDbError>>(
        &self,
        txn: &impl crate::transaction::Mutation,
        edges: &[u64],
        nodes: &[u64],
        check_attached: impl Fn(&PairMembers) -> std::result::Result<(), E>,
        index_context: &mut MutationIndexContext,
    ) -> std::result::Result<(), E> {
        assert!(
            edges.is_sorted_by(|a, b| a < b) && nodes.is_sorted_by(|a, b| a < b),
            "deletion targets are strictly ascending"
        );
        // Prior clauses may still own coalesced pair or adjacency changes,
        // and every observation below must include them.
        index_context.flush_topology(txn)?;
        let mut selected = edges;
        let mut remaining = nodes;
        let mut batch_size = NODE_DELETE_BATCH;
        loop {
            let batch = &remaining[..batch_size.min(remaining.len())];
            let refused = |error: &HelixDbError| {
                matches!(error, HelixDbError::QueryMemoryLimitExceeded) && batch.len() > 1
            };
            let observed = async {
                let ObservedNodeDeletions {
                    rows,
                    pairs,
                    covered,
                    _memory,
                } = self.observe_node_deletions(txn, batch).await?;
                let mut members = PairMembers::default();
                self.observe_pair_members(txn, pairs.iter().copied(), &mut members, index_context)
                    .await?;
                Ok::<_, HelixDbError>((rows, members, covered, _memory))
            }
            .await;
            let (rows, members, covered, _memory) = match observed {
                Err(error) if refused(&error) => {
                    batch_size = batch.len() / 2;
                    continue;
                }
                observed => observed?,
            };
            check_attached(&members)?;
            let mut observed_edges = match self
                .observe_edge_deletions(txn, selected.iter().copied(), members, index_context)
                .await
            {
                Err(error) if refused(&error) => {
                    batch_size = batch.len() / 2;
                    continue;
                }
                observed => observed?,
            };
            selected = &[];
            for edge_id in observed_edges.edge_ids() {
                self.check_execution_deadline()?;
                self.delete_edge_observed(
                    txn,
                    edge_id,
                    observed_edges.take(edge_id)?,
                    index_context,
                )
                .await?;
                if index_context.pending_topology_rows() >= DELETE_FLUSH_ROWS {
                    index_context.flush_topology(txn)?;
                }
            }
            let staged = rows.keys().copied().collect::<Vec<_>>();
            for (node_id, row) in rows {
                self.check_execution_deadline()?;
                let transition = GraphMutationTransition::delete(
                    self.tenant_scope,
                    GraphEntity::node(node_id),
                    row,
                );
                let properties = transition
                    .before()
                    .expect("a delete transition has a before row")
                    .properties();
                label_of(properties)
                    .map(|label| {
                        index_context.topology_mutations().remove_node_label(
                            self.tenant_scope,
                            label,
                            node_id,
                        )
                    })
                    .transpose()?;
                index_context.maintain_graph_indexes(transition, self.row_memory.as_ref())?;
                index_context.property_writes.stage(
                    txn,
                    self.tenant_scope,
                    GraphEntity::node(node_id),
                    None,
                    self.row_memory.as_ref(),
                )?;
                if index_context.pending_topology_rows() >= DELETE_FLUSH_ROWS {
                    index_context.flush_topology(txn)?;
                }
            }
            // The tombstone is the last write to each adjacency row, so it
            // replaces this batch's deltas in the write batch.
            index_context.flush_topology(txn)?;
            staged
                .into_iter()
                .try_for_each(|node_id| {
                    txn.delete(self.storage_key(keys::DataKeyKind::Adjacency(
                        keys::AdjacencyKey::new(node_id),
                    )))
                })
                .map_err(HelixDbError::from)?;
            remaining = &remaining[covered..];
            if remaining.is_empty() {
                return Ok(());
            }
            // A cut batch shows what these nodes cost: start the next one
            // there instead of reading rows it would drop.
            batch_size = if covered < batch.len() {
                covered
            } else {
                batch_size.saturating_mul(2).min(NODE_DELETE_BATCH)
            };
        }
    }

    pub(super) async fn node_targets(&self, plan: &ir::NodeTargetPlan) -> Result<Vec<u64>> {
        match plan {
            ir::NodeTargetPlan::All => {
                self.scan_element_ids(exec::ElementKeyspace::NodeProperty, None)
                    .await
            }
            ir::NodeTargetPlan::Empty => Ok(Vec::new()),
            ir::NodeTargetPlan::PointIds { ids } => Ok(ids.as_ref().to_vec()),
            ir::NodeTargetPlan::FromParam { param } => self.param_ids(param),
            ir::NodeTargetPlan::FromVar { variable } => self.variable_nodes(variable),
        }
    }

    #[cfg(test)]
    pub(super) async fn ensure_node_exists(&self, node_id: u64) -> Result<()> {
        let key = keys::DataKey::Data {
            scope: self.tenant_scope,
            kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(node_id)),
        }
        .to_bytes();
        if self.get_raw(&key).await?.is_some() {
            Ok(())
        } else {
            Err(HelixDbError::NodeNotFound(node_id))
        }
    }

    #[cfg(test)]
    pub(super) async fn ensure_node_exists_in_tx(
        &self,
        txn: &impl crate::transaction::Mutation,
        node_id: u64,
    ) -> Result<()> {
        let key = self.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
            node_id,
        )));
        if txn.get(&key).await?.is_some() {
            Ok(())
        } else {
            Err(HelixDbError::NodeNotFound(node_id))
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use helix_ast::value::PropertyValue as AstPropertyValue;
    use helix_planner::context;
    use slatedb::IsolationLevel;

    use super::super::super::test_support;
    use super::*;

    async fn all_rows(db: &HelixDB) -> BTreeMap<Bytes, Bytes> {
        let mut scan = db.inner_db().scan(..).await.unwrap();
        let mut rows = BTreeMap::new();
        while let Some(row) = scan.next().await.unwrap() {
            rows.insert(row.key, row.value);
        }
        rows
    }

    /// One membership row a cascade delete changes.
    #[derive(Debug, Clone, Copy)]
    enum CorruptRow {
        NodeLabel,
        EdgePair,
        EdgeLabelOut,
        EdgeLabelIn,
        GlobalEdgeLabel,
        Adjacency,
        NodeEquality,
        EdgeEquality,
    }

    /// What a cascade delete does when one of its rows was corrupted outside
    /// Helix.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum CascadeOutcome {
        /// The delete only stages a removal delta for the row, which never
        /// reads it: every record goes and the delta commits.
        Deletes,
        /// The delete reads the row to find incident edges, so it fails
        /// closed and writes nothing.
        RejectsOnRead,
    }

    /// A membership row corrupted outside Helix never fails a memtable flush.
    ///
    /// Staging a removal never reads the row it changes, so a cascade delete
    /// commits unless it reads the corrupt row itself, in which case it
    /// rejects without writing anything. Either way the next flush builds.
    #[tokio::test]
    async fn cascade_delete_over_a_corrupt_row_is_atomic_and_never_fails_a_flush() {
        for (corrupt, expected) in [
            (None, CascadeOutcome::Deletes),
            (Some(CorruptRow::NodeLabel), CascadeOutcome::Deletes),
            (Some(CorruptRow::EdgePair), CascadeOutcome::RejectsOnRead),
            (Some(CorruptRow::EdgeLabelOut), CascadeOutcome::Deletes),
            (Some(CorruptRow::EdgeLabelIn), CascadeOutcome::Deletes),
            (Some(CorruptRow::GlobalEdgeLabel), CascadeOutcome::Deletes),
            (Some(CorruptRow::Adjacency), CascadeOutcome::RejectsOnRead),
            (Some(CorruptRow::NodeEquality), CascadeOutcome::Deletes),
            (Some(CorruptRow::EdgeEquality), CascadeOutcome::Deletes),
        ] {
            cascade_delete_over_a_corrupt_row(corrupt, expected).await;
        }
    }

    async fn cascade_delete_over_a_corrupt_row(
        corrupt: Option<CorruptRow>,
        expected: CascadeOutcome,
    ) {
        let scope = keys::scope::DataScope::LegacyUnscoped;
        let config =
            test_support::in_memory_config(&format!("mutation-corrupt-cascade-delete-{corrupt:?}"))
                .with_equality_index("User", "status")
                .with_edge_equality_index("FOLLOWS", "status");
        let db = test_support::open_db_with_config(config).await;
        let target = test_support::add_node_with_properties(
            &db,
            "User",
            vec![("status", AstPropertyValue::from("shared"))],
        )
        .await;
        let neighbor = test_support::add_node_with_properties(
            &db,
            "User",
            vec![("status", AstPropertyValue::from("shared"))],
        )
        .await;
        let outgoing = test_support::add_edge_with_properties(
            &db,
            target,
            neighbor,
            "FOLLOWS",
            vec![("status", AstPropertyValue::from("shared"))],
        )
        .await;
        let incoming = test_support::add_edge_with_properties(
            &db,
            neighbor,
            target,
            "FOLLOWS",
            vec![("status", AstPropertyValue::from("shared"))],
        )
        .await;

        let handles = db.active_index_handles_loaded(scope);
        let node_equality = handles
            .iter()
            .find(|handle| {
                handle.secondary_definition().is_some_and(|definition| {
                    definition.element_kind() == crate::index_lifecycle::IndexElementKind::Node
                })
            })
            .expect("the node equality generation is active");
        let edge_equality = handles
            .iter()
            .find(|handle| {
                handle.secondary_definition().is_some_and(|definition| {
                    definition.element_kind() == crate::index_lifecycle::IndexElementKind::Edge
                })
            })
            .expect("the edge equality generation is active");
        let corrupt_key = corrupt.map(|row| match row {
            CorruptRow::NodeLabel => super::super::topology::node_label_key(scope, "User"),
            CorruptRow::EdgePair => super::super::topology::edge_pair_key(scope, target, neighbor),
            CorruptRow::EdgeLabelOut => super::super::topology::edge_label_neighbor_key(
                scope,
                crate::encoding::indexes::EdgeDirection::Out,
                target,
                "FOLLOWS",
            ),
            CorruptRow::EdgeLabelIn => super::super::topology::edge_label_neighbor_key(
                scope,
                crate::encoding::indexes::EdgeDirection::In,
                target,
                "FOLLOWS",
            ),
            CorruptRow::GlobalEdgeLabel => {
                super::super::topology::global_edge_label_key(scope, "FOLLOWS")
            }
            CorruptRow::Adjacency => keys::DataKey::Data {
                scope,
                kind: keys::DataKeyKind::Adjacency(keys::AdjacencyKey::new(target)),
            }
            .to_bytes(),
            CorruptRow::NodeEquality => {
                crate::index_lifecycle::secondary::equality_bitmap_key_for_test(
                    node_equality,
                    "shared",
                )
            }
            CorruptRow::EdgeEquality => {
                crate::index_lifecycle::secondary::equality_bitmap_key_for_test(
                    edge_equality,
                    "shared",
                )
            }
        });
        let node_key = keys::DataKey::Data {
            scope,
            kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(target)),
        }
        .to_bytes();
        let outgoing_key = keys::DataKey::Data {
            scope,
            kind: keys::DataKeyKind::EdgePropertyById(keys::EdgePropertyByIdKey::new(outgoing)),
        }
        .to_bytes();
        let incoming_key = keys::DataKey::Data {
            scope,
            kind: keys::DataKeyKind::EdgePropertyById(keys::EdgePropertyByIdKey::new(incoming)),
        }
        .to_bytes();
        let node_param = test_support::name("node");
        let access_id = exec::ExecStepId::new(1).unwrap();
        let drop_node = test_support::executable(
            ir::PlanKind::Write,
            vec![
                test_support::step(
                    1,
                    Vec::new(),
                    exec::ExecOp::Access {
                        plan: Box::new(exec::ExecAccessPlan::Node(
                            exec::ExecNodeAccessPlan::FromParam {
                                param: node_param.clone(),
                            },
                        )),
                    },
                ),
                test_support::step(
                    2,
                    vec![access_id],
                    exec::ExecOp::Mutation {
                        plan: exec::ExecMutationPlan::Drop,
                    },
                ),
            ],
            2,
        );

        let memtable = slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        };
        // Corrupt the row outside Helix and flush it below the next memtable.
        async {
            let Some(key) = &corrupt_key else {
                return;
            };
            assert!(
                db.inner_db().get(key).await.unwrap().is_some(),
                "{corrupt:?} fixture row exists"
            );
            db.inner_db()
                .put(key, Bytes::from_static(b"corrupt-membership-row"))
                .await
                .unwrap();
            db.inner_db()
                .flush_with_options(memtable.clone())
                .await
                .unwrap();
        }
        .await;
        let before = all_rows(&db).await;
        let outcome = db
            .execute(
                &drop_node,
                context::ParamBindings::default()
                    .with_value(node_param, AstPropertyValue::I64(target as i64)),
            )
            .await
            .map(drop);
        match (expected, outcome) {
            (CascadeOutcome::Deletes, Ok(_)) => {}
            (CascadeOutcome::RejectsOnRead, Err(_)) => {
                assert_eq!(all_rows(&db).await, before, "{corrupt:?}");
            }
            (expected, outcome) => panic!("{corrupt:?}: expected {expected:?}, got {outcome:?}"),
        }
        for record_key in [&node_key, &outgoing_key, &incoming_key] {
            assert_eq!(
                db.inner_db().get(record_key).await.unwrap().is_none(),
                expected == CascadeOutcome::Deletes,
                "{corrupt:?}"
            );
        }
        db.inner_db()
            .flush_with_options(memtable)
            .await
            .unwrap_or_else(|error| panic!("{corrupt:?} failed a memtable flush: {error}"));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn label_updates_validate_values_and_move_label_indexes() {
        let db = test_support::open_db("mutation-node-label-updates").await;
        let node_id = test_support::add_user(&db, "alice").await;
        let context = ExecutionContext::new(&db, context::ParamBindings::default());
        let mut index_context = MutationIndexContext::for_configured_index_test();
        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("snapshot transaction begins");
        let key = context.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
            node_id,
        )));
        let encoded_before = txn
            .get(&key)
            .await
            .expect("node row reads")
            .expect("node row exists");
        let error = context
            .set_node_property(
                &txn,
                node_id,
                Property::new("$label", DbPropertyValue::I64(7)),
                &mut index_context,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("require a string value"));

        context
            .set_node_property(
                &txn,
                node_id,
                Property::string("$label", "User"),
                &mut index_context,
            )
            .await
            .expect("setting the same label preserves indexes");
        assert_eq!(
            txn.get(&key).await.expect("node row re-reads"),
            Some(encoded_before),
            "a no-op set retains the exact canonical bytes"
        );
        assert!(
            index_context
                .finalize_queued(
                    u64::MAX,
                    crate::config::SearchIndexBackfillLimits::default().active_text_mutation(),
                )
                .unwrap()
                .is_empty(),
            "a no-op set creates no downstream text work"
        );
        context
            .set_node_property(
                &txn,
                node_id,
                Property::string("$label", "Admin"),
                &mut index_context,
            )
            .await
            .expect("changing the label moves indexes");
        assert!(
            index_context
                .finalize_queued(
                    u64::MAX,
                    crate::config::SearchIndexBackfillLimits::default().active_text_mutation(),
                )
                .unwrap()
                .is_empty(),
            "an empty text catalog queues no operation"
        );

        assert!(
            crate::search::lookup_equality_index(&txn, "$label", "User",)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            crate::search::lookup_equality_index(&txn, "$label", "Admin",)
                .await
                .unwrap(),
            vec![node_id]
        );

        context
            .remove_node_property(
                &txn,
                node_id,
                &test_support::name("missing"),
                &mut index_context,
            )
            .await
            .expect("removing an absent node property is idempotent");
    }

    #[tokio::test]
    async fn deleting_a_node_removes_incoming_edges_and_missing_deletes_are_idempotent() {
        let db = test_support::open_db("mutation-delete-node-incoming-edge").await;
        let from = test_support::add_user(&db, "alice").await;
        let to = test_support::add_user(&db, "bob").await;
        let edge_id = test_support::add_edge(&db, from, to, "FOLLOWS").await;
        let context = ExecutionContext::new(&db, context::ParamBindings::default());
        let mut index_context = MutationIndexContext::for_configured_index_test();
        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("snapshot transaction begins");
        context
            .delete_elements::<HelixDbError>(&txn, &[], &[to], |_| Ok(()), &mut index_context)
            .await
            .expect("target node and incoming edge are deleted");
        assert_eq!(
            crate::search::get_edge_endpoints(&txn, edge_id)
                .await
                .unwrap(),
            None
        );
        assert!(matches!(
            context.ensure_node_exists_in_tx(&txn, to).await,
            Err(HelixDbError::NodeNotFound(id)) if id == to
        ));

        context
            .delete_elements::<HelixDbError>(&txn, &[], &[99], |_| Ok(()), &mut index_context)
            .await
            .expect("deleting a missing node is idempotent");
    }

    #[tokio::test]
    async fn node_targets_and_existence_checks_cover_reader_and_transaction_storage() {
        let db = test_support::open_db("mutation-node-targets-and-existence").await;
        let alice = test_support::add_user(&db, "alice").await;
        let bob = test_support::add_user(&db, "bob").await;
        let context = ExecutionContext::new(&db, context::ParamBindings::default());

        assert_eq!(
            context
                .node_targets(&ir::NodeTargetPlan::All)
                .await
                .unwrap(),
            vec![alice, bob]
        );
        context
            .ensure_node_exists(alice)
            .await
            .expect("reader finds existing node");
        assert!(matches!(
            context.ensure_node_exists(99).await,
            Err(HelixDbError::NodeNotFound(99))
        ));

        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("snapshot transaction begins");
        context
            .ensure_node_exists_in_tx(&txn, bob)
            .await
            .expect("transaction finds existing node");
        assert!(matches!(
            context.ensure_node_exists_in_tx(&txn, 99).await,
            Err(HelixDbError::NodeNotFound(99))
        ));
    }
}
