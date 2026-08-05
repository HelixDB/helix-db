//! Node storage mutation contracts.
//!
//! Each helper stages the authoritative node row together with every V2
//! secondary/vector/text action captured by the request transaction. Active
//! secondary/vector generations receive physical entry
//! changes; hidden building generations receive one coalesced entity delta in
//! that same transaction.

use std::collections::{BTreeMap, BTreeSet};

use slatedb::DbTransaction;

use super::contracts::{decode_stored_edges, label_of};
use super::MutationIndexContext;
use super::*;
use crate::index_lifecycle::graph_mutation::{
    CanonicalPropertyRow, GraphEntity, GraphMutationTransition, PropertyEdit, PropertyEditOutcome,
};

/// Sorted, deduplicated node-row observations with an ordered mutation overlay.
pub(super) struct ObservedNodeRows {
    rows: BTreeMap<u64, Option<CanonicalPropertyRow>>,
}

/// Distinct node-existence observations used by batched endpoint validation.
pub(super) struct ObservedNodeExistence {
    present: BTreeSet<u64>,
}

/// Existing node property rows captured by one deletion observation epoch.
pub(super) struct ObservedNodeDeletionBatch {
    rows: BTreeMap<u64, CanonicalPropertyRow>,
}

/// A complete node-cascade closure whose pair rows were each observed once.
pub(super) struct PreparedTopologyDeletion {
    nodes: ObservedNodeDeletionBatch,
    edges: super::edge::ObservedEdgeDeletionBatch,
}

impl ObservedNodeExistence {
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
        txn: &DbTransaction,
        node_id: u64,
        properties: Vec<Property>,
        index_context: &mut MutationIndexContext,
    ) -> Result<()> {
        let transition = GraphMutationTransition::create(
            self.tenant_scope,
            GraphEntity::node(node_id),
            CanonicalPropertyRow::new(properties),
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
            .encoded()
            .clone();
        index_context
            .maintain_graph_indexes(
                txn,
                transition,
                self.db
                    .config()
                    .db()
                    .search_index_backfill()
                    .active_text_mutation(),
                self.db.object_store(),
                self.db.path(),
            )
            .await?;
        let key = self.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
            node_id,
        )));
        txn.put(&key, encoded)?;
        Ok(())
    }

    #[cfg(test)]
    pub(super) async fn set_node_property(
        &self,
        txn: &DbTransaction,
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
            .map(CanonicalPropertyRow::decode)
            .transpose()?;
        let _ = self
            .set_node_property_observed(txn, node_id, property, observed, index_context)
            .await?;
        index_context.flush_topology(txn).await?;
        Ok(())
    }

    pub(super) async fn set_node_property_observed(
        &self,
        txn: &DbTransaction,
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
        let outcome = GraphMutationTransition::edit(
            self.tenant_scope,
            GraphEntity::node(node_id),
            before,
            PropertyEdit::set(property),
        );
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
        let old_label = label_of(old_properties).map(str::to_string);
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
            if old_label.as_deref() != Some(new_label) {
                if let Some(old_label) = old_label.as_deref() {
                    index_context.topology_mutations().remove_node_label(
                        self.tenant_scope,
                        old_label,
                        node_id,
                    )?;
                }
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
            .encoded()
            .clone();
        let final_row = transition
            .after()
            .expect("a replacement transition has an after row")
            .clone();
        index_context
            .maintain_graph_indexes(
                txn,
                transition,
                self.db
                    .config()
                    .db()
                    .search_index_backfill()
                    .active_text_mutation(),
                self.db.object_store(),
                self.db.path(),
            )
            .await?;
        txn.put(
            self.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
                node_id,
            ))),
            encoded,
        )?;
        Ok(final_row)
    }

    #[cfg(test)]
    pub(super) async fn remove_node_property(
        &self,
        txn: &DbTransaction,
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
            .map(CanonicalPropertyRow::decode)
            .transpose()?;
        let _ = self
            .remove_node_property_observed(txn, node_id, name, observed, index_context)
            .await?;
        Ok(())
    }

    pub(super) async fn remove_node_property_observed(
        &self,
        txn: &DbTransaction,
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
        let outcome = GraphMutationTransition::edit(
            self.tenant_scope,
            GraphEntity::node(node_id),
            before,
            PropertyEdit::remove(name.as_ref()),
        );
        let PropertyEditOutcome::Changed(transition) = outcome else {
            let PropertyEditOutcome::Unchanged(row) = outcome else {
                unreachable!("property edit outcomes are closed")
            };
            return Ok(Some(row));
        };
        let encoded = transition
            .after()
            .expect("a replacement transition has an after row")
            .encoded()
            .clone();
        let final_row = transition
            .after()
            .expect("a replacement transition has an after row")
            .clone();
        index_context
            .maintain_graph_indexes(
                txn,
                transition,
                self.db
                    .config()
                    .db()
                    .search_index_backfill()
                    .active_text_mutation(),
                self.db.object_store(),
                self.db.path(),
            )
            .await?;
        txn.put(
            self.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
                node_id,
            ))),
            encoded,
        )?;
        Ok(Some(final_row))
    }

    pub(super) async fn observe_node_rows(
        &self,
        txn: &DbTransaction,
        node_ids: impl IntoIterator<Item = u64>,
    ) -> Result<ObservedNodeRows> {
        let node_ids = node_ids.into_iter().collect::<BTreeSet<_>>();
        let keys = node_ids
            .iter()
            .map(|node_id| {
                self.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
                    *node_id,
                )))
            })
            .collect::<Vec<_>>();
        let values = if keys.is_empty() {
            Vec::new()
        } else {
            txn.multi_get(&keys).await?
        };
        let rows = node_ids
            .into_iter()
            .zip(values)
            .map(|(node_id, value)| {
                value
                    .map(CanonicalPropertyRow::decode)
                    .transpose()
                    .map(|row| (node_id, row))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        Ok(ObservedNodeRows { rows })
    }

    pub(super) async fn observe_node_existence(
        &self,
        txn: &DbTransaction,
        node_ids: impl IntoIterator<Item = u64>,
    ) -> Result<ObservedNodeExistence> {
        let node_ids = node_ids.into_iter().collect::<BTreeSet<_>>();
        let keys = node_ids
            .iter()
            .map(|node_id| {
                self.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
                    *node_id,
                )))
            })
            .collect::<Vec<_>>();
        let values = if keys.is_empty() {
            Vec::new()
        } else {
            txn.multi_get(&keys).await?
        };
        let mut present = BTreeSet::new();
        for ((node_id, key), value) in node_ids.into_iter().zip(keys).zip(values) {
            if value.is_some() || txn.get(&key).await?.is_some() {
                present.insert(node_id);
            }
        }
        Ok(ObservedNodeExistence { present })
    }

    #[cfg(test)]
    pub(super) async fn delete_node(
        &self,
        txn: &DbTransaction,
        node_id: u64,
        index_context: &mut MutationIndexContext,
    ) -> Result<()> {
        self.delete_nodes(txn, std::iter::once(node_id), index_context)
            .await
    }

    /// Observes and stages one complete node-deletion cascade as a single epoch.
    ///
    /// The closure owns sorted unique nodes, pairs, and incident edges, so a
    /// self-loop or edge shared by multiple deleted nodes is processed once.
    pub(super) async fn delete_nodes(
        &self,
        txn: &DbTransaction,
        node_ids: impl IntoIterator<Item = u64>,
        index_context: &mut MutationIndexContext,
    ) -> Result<()> {
        let PreparedTopologyDeletion { nodes, mut edges } = self
            .observe_node_deletion_batch(txn, node_ids, index_context)
            .await?;
        let edge_ids = edges.edge_ids().collect::<Vec<_>>();
        for edge_id in edge_ids {
            self.check_execution_deadline()?;
            self.delete_edge_observed(txn, edge_id, edges.take(edge_id)?, index_context)
                .await?;
        }
        for (node_id, property_row) in nodes.rows {
            self.check_execution_deadline()?;
            let transition = GraphMutationTransition::delete(
                self.tenant_scope,
                GraphEntity::node(node_id),
                property_row,
            );
            let properties = transition
                .before()
                .expect("a delete transition has a before row")
                .properties();
            if let Some(label) = label_of(properties) {
                index_context.topology_mutations().remove_node_label(
                    self.tenant_scope,
                    label,
                    node_id,
                )?;
            }
            index_context
                .maintain_graph_indexes(
                    txn,
                    transition,
                    self.db
                        .config()
                        .db()
                        .search_index_backfill()
                        .active_text_mutation(),
                    self.db.object_store(),
                    self.db.path(),
                )
                .await?;
            txn.delete(self.storage_key(keys::DataKeyKind::NodeProperty(
                keys::NodePropertyKey::new(node_id),
            )))?;
            txn.delete(
                self.storage_key(keys::DataKeyKind::Adjacency(keys::AdjacencyKey::new(
                    node_id,
                ))),
            )?;
        }
        Ok(())
    }

    /// Builds one fail-closed incident-edge closure from bounded graph reads.
    async fn observe_node_deletion_batch(
        &self,
        txn: &DbTransaction,
        node_ids: impl IntoIterator<Item = u64>,
        index_context: &mut MutationIndexContext,
    ) -> Result<PreparedTopologyDeletion> {
        const NODES_PER_CHUNK: usize = 256;

        let node_ids = node_ids.into_iter().collect::<BTreeSet<_>>();
        index_context.flush_topology(txn).await?;
        let node_ids = node_ids.into_iter().collect::<Vec<_>>();
        let mut nodes = BTreeMap::new();
        let mut pairs = BTreeSet::new();
        for chunk in node_ids.chunks(NODES_PER_CHUNK) {
            self.check_execution_deadline()?;
            let property_keys = chunk
                .iter()
                .map(|node_id| {
                    self.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
                        *node_id,
                    )))
                })
                .collect::<Vec<_>>();
            let adjacency_keys = chunk
                .iter()
                .map(|node_id| {
                    self.storage_key(keys::DataKeyKind::Adjacency(keys::AdjacencyKey::new(
                        *node_id,
                    )))
                })
                .collect::<Vec<_>>();
            let property_values = txn.multi_get(&property_keys).await?;
            let adjacency_values = index_context.observe_topology(txn, &adjacency_keys).await?;
            for (((node_id, property_key), property_value), adjacency_value) in chunk
                .iter()
                .copied()
                .zip(property_keys)
                .zip(property_values)
                .zip(adjacency_values)
            {
                let property_value = match property_value {
                    Some(value) => Some(value),
                    None => txn.get(&property_key).await?,
                };
                let property_row = property_value
                    .map(CanonicalPropertyRow::decode)
                    .transpose()?;
                if property_row.is_none() && adjacency_value.is_some() {
                    return Err(HelixDbError::InvariantViolation(format!(
                        "node {node_id} has adjacency without a canonical property row"
                    )));
                }
                let adjacency = decode_stored_edges(adjacency_value)?;
                pairs.extend(adjacency.iter_out().map(|to| (node_id, to)));
                pairs.extend(adjacency.iter_in().map(|from| (from, node_id)));
                if let Some(property_row) = property_row {
                    nodes.insert(node_id, property_row);
                }
            }
        }

        let pair_values = self
            .observe_edge_pair_values(txn, pairs, index_context)
            .await?;
        let mut incident_edges = BTreeSet::new();
        for value in pair_values.values().filter_map(Option::as_ref) {
            incident_edges
                .extend(values::secondary::SecondaryEqualityValue::decode(value)?.into_ids());
        }
        let edges = self
            .observe_edge_deletions_from_pairs(txn, incident_edges, pair_values)
            .await?;
        #[cfg(feature = "production-coverage")]
        super::benchmark_telemetry::record_cascade(
            nodes.len(),
            edges.edge_ids().count(),
            edges.pair_count(),
        );
        Ok(PreparedTopologyDeletion {
            nodes: ObservedNodeDeletionBatch { rows: nodes },
            edges,
        })
    }

    /// Intersects directed adjacency from the smaller endpoint set.
    ///
    /// This preserves pair-drop semantics without materializing the Cartesian
    /// product of every source and target.
    pub(super) async fn edge_pairs_between(
        &self,
        txn: &DbTransaction,
        sources: &BTreeSet<u64>,
        targets: &BTreeSet<u64>,
        index_context: &MutationIndexContext,
    ) -> Result<BTreeSet<(u64, u64)>> {
        const NODES_PER_CHUNK: usize = 512;

        #[derive(Clone, Copy)]
        enum ObservationSide<'a> {
            Sources(&'a BTreeSet<u64>),
            Targets(&'a BTreeSet<u64>),
        }
        let side = if sources.len() <= targets.len() {
            ObservationSide::Sources(sources)
        } else {
            ObservationSide::Targets(targets)
        };
        let nodes = match side {
            ObservationSide::Sources(nodes) | ObservationSide::Targets(nodes) => {
                nodes.iter().copied().collect::<Vec<_>>()
            }
        };
        let mut pairs = BTreeSet::new();
        for chunk in nodes.chunks(NODES_PER_CHUNK) {
            self.check_execution_deadline()?;
            let keys = chunk
                .iter()
                .map(|node_id| {
                    self.storage_key(keys::DataKeyKind::Adjacency(keys::AdjacencyKey::new(
                        *node_id,
                    )))
                })
                .collect::<Vec<_>>();
            let observed = index_context.observe_topology(txn, &keys).await?;
            for (node_id, value) in chunk.iter().copied().zip(observed) {
                let adjacency = decode_stored_edges(value)?;
                match side {
                    ObservationSide::Sources(_) => pairs.extend(
                        adjacency
                            .iter_out()
                            .filter(|target| targets.contains(target))
                            .map(|target| (node_id, target)),
                    ),
                    ObservationSide::Targets(_) => pairs.extend(
                        adjacency
                            .iter_in()
                            .filter(|source| sources.contains(source))
                            .map(|source| (source, node_id)),
                    ),
                }
            }
        }
        Ok(pairs)
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
        let key = keys::Key::Data {
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
        txn: &DbTransaction,
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
    use std::sync::atomic::{AtomicU64, Ordering};

    use helix_ast::prelude::*;
    use helix_ast::query::QueryRequest;
    use helix_planner::context;
    use proptest::prelude::*;
    use slatedb::IsolationLevel;

    use super::super::super::test_support;
    use super::*;

    fn index_context(db: &HelixDB) -> MutationIndexContext {
        MutationIndexContext::for_configured_index_test(std::sync::Arc::clone(
            db.simhasher_registry(),
        ))
    }

    static PROPERTY_DATABASE_ID: AtomicU64 = AtomicU64::new(0);

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(24))]

        #[test]
        fn batch_node_deletion_matches_a_directed_multigraph_model(
            node_count in 1usize..8,
            generated_edges in prop::collection::vec((0usize..8, 0usize..8, 0u8..3), 0..24),
            generated_deletions in prop::collection::vec(0usize..8, 1..12),
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("property-test runtime starts");
            runtime.block_on(async move {
                let database_id = PROPERTY_DATABASE_ID.fetch_add(1, Ordering::Relaxed);
                let db = test_support::open_db(&format!(
                    "mutation-node-batch-property-{database_id}"
                ))
                .await;
                let mut nodes = Vec::with_capacity(node_count);
                for ordinal in 0..node_count {
                    nodes.push(test_support::add_user(&db, &format!("user-{ordinal}")).await);
                }

                let mut edges = Vec::with_capacity(generated_edges.len());
                for (from, to, label) in generated_edges {
                    let from = nodes[from % node_count];
                    let to = nodes[to % node_count];
                    let label = match label {
                        0 => "LIKES",
                        1 => "KNOWS",
                        _ => "FOLLOWS",
                    };
                    let edge_id = test_support::add_edge(&db, from, to, label).await;
                    edges.push((edge_id, from, to));
                }

                let deleted = generated_deletions
                    .into_iter()
                    .map(|ordinal| nodes[ordinal % node_count])
                    .collect::<BTreeSet<_>>();
                let deletion = write_batch().var_as(
                    "deleted",
                    g().n(NodeRef::ids(deleted.iter().copied())).drop(),
                );
                let plan = helix_planner::planning::plan_write_batch(
                    &deletion,
                    &db.planner_context(context::ParamBindings::default()),
                )
                .expect("property-test deletion plans");
                db.execute(&plan, context::ParamBindings::default())
                    .await
                    .expect("property-test deletion commits");

                let context = ExecutionContext::new(&db, context::ParamBindings::default());
                let remaining_nodes = context
                    .node_targets(&ir::NodeTargetPlan::All)
                    .await
                    .expect("remaining nodes scan");
                let expected_nodes = nodes
                    .iter()
                    .copied()
                    .filter(|node_id| !deleted.contains(node_id))
                    .collect::<Vec<_>>();
                assert_eq!(remaining_nodes, expected_nodes);

                let transaction = db
                    .inner_db()
                    .begin(IsolationLevel::Snapshot)
                    .await
                    .expect("verification transaction begins");
                for (edge_id, from, to) in edges {
                    let expected = (!deleted.contains(&from) && !deleted.contains(&to))
                        .then_some((from, to));
                    assert_eq!(
                        crate::search::get_edge_endpoints(&transaction, edge_id)
                            .await
                            .expect("edge endpoints read"),
                        expected,
                        "edge {edge_id} visibility must match the reference model"
                    );
                }
                db.close().await.expect("property-test database closes");
            });
        }
    }

    #[tokio::test]
    async fn label_updates_validate_values_and_move_label_indexes() {
        let db = test_support::open_db("mutation-node-label-updates").await;
        let node_id = test_support::add_user(&db, "alice").await;
        let context = ExecutionContext::new(&db, context::ParamBindings::default());
        let mut index_context = index_context(&db);
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
        assert_eq!(
            index_context.pending_active_text_entities(),
            0,
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
        assert_eq!(
            index_context.pending_active_text_entities(),
            0,
            "an empty text catalog retains no graph transition"
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
        let mut index_context = index_context(&db);
        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("snapshot transaction begins");
        context
            .delete_node(&txn, to, &mut index_context)
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
            .delete_node(&txn, 99, &mut index_context)
            .await
            .expect("deleting a missing node is idempotent");
    }

    #[tokio::test]
    async fn same_transaction_create_then_batch_delete_observes_staged_topology() {
        let db = test_support::open_db("mutation-node-create-delete-staged-topology").await;
        let context = ExecutionContext::new(&db, context::ParamBindings::default());
        let mut index_context = index_context(&db);
        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("snapshot transaction begins");
        for node_id in [10, 11] {
            context
                .store_node(
                    &txn,
                    node_id,
                    vec![Property::string("$label", "User")],
                    &mut index_context,
                )
                .await
                .expect("node create stages");
        }
        context
            .store_edge(
                &txn,
                super::super::contracts::EdgeMutationTarget::new(20, 10, 11),
                &test_support::name("FOLLOWS"),
                &CanonicalPropertyRow::new(vec![Property::string("$label", "FOLLOWS")]),
                &mut index_context,
            )
            .await
            .expect("edge create stages");

        context
            .delete_nodes(&txn, [10, 11, 10], &mut index_context)
            .await
            .expect("batch deletion observes the staged edge once");
        index_context
            .flush_topology(&txn)
            .await
            .expect("deletion topology flushes");

        assert_eq!(
            crate::search::get_edge_endpoints(&txn, 20)
                .await
                .expect("edge endpoint reads"),
            None
        );
        for node_id in [10, 11] {
            assert!(matches!(
                context.ensure_node_exists_in_tx(&txn, node_id).await,
                Err(HelixDbError::NodeNotFound(id)) if id == node_id
            ));
        }
        let pair_key = context.storage_key(keys::DataKeyKind::EdgePairIndex(
            keys::EdgePairIndexKey::new(10, 11),
        ));
        assert_eq!(txn.get(pair_key).await.expect("pair row reads"), None);
    }

    #[tokio::test]
    async fn active_text_deletion_drains_beyond_one_epoch_and_stays_absent_after_reopen() {
        const DOCUMENT_COUNT: usize = 513;
        const DATABASE: &str = "mutation-node-text-bounded-deletion";

        let config =
            test_support::in_memory_config(DATABASE).with_node_text_index("Document", "body");
        let object_store = config.object_store();
        let db = test_support::open_db_with_config(config).await;
        let mut create = write_batch();
        for ordinal in 0..DOCUMENT_COUNT {
            create = create.var_as(
                &format!("document_{ordinal}"),
                g().add_n(
                    "Document",
                    vec![("body", PropertyInput::from("bounded deletion token"))],
                ),
            );
        }
        db.query(QueryRequest::write(create.returning(Vec::<String>::new())))
            .await
            .expect("513 indexed documents commit through bounded create epochs");

        let deletion = write_batch()
            .var_as("deleted", g().n(NodeRef::all()).drop())
            .var_as(
                "remaining_hits",
                g().text_search_nodes("Document", "body", "bounded", 8, None)
                    .id(),
            )
            .returning(["remaining_hits"]);
        assert_eq!(
            db.query(QueryRequest::write(deletion))
                .await
                .expect("513 indexed deletions drain and remain invisible to same-request search"),
            serde_json::json!({ "remaining_hits": [] })
        );
        db.close().await.expect("bounded text writer closes");

        let reopened = test_support::open_db_with_object_store(DATABASE, object_store).await;
        let verification = read_batch()
            .var_as("nodes", g().n(NodeRef::all()).count())
            .var_as(
                "hits",
                g().text_search_nodes("Document", "body", "bounded", 8, None)
                    .id(),
            )
            .returning(["nodes", "hits"]);
        assert_eq!(
            reopened
                .query(QueryRequest::read(verification))
                .await
                .expect("reopened graph and text index remain deletion-clean"),
            serde_json::json!({ "nodes": 0, "hits": [] })
        );
        reopened.close().await.expect("bounded text reader closes");
    }

    #[tokio::test]
    async fn batch_deletion_fails_closed_on_adjacency_without_a_node_row() {
        let db = test_support::open_db("mutation-node-delete-malformed-adjacency").await;
        let context = ExecutionContext::new(&db, context::ParamBindings::default());
        let mut index_context = index_context(&db);
        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("snapshot transaction begins");
        index_context
            .topology_mutations()
            .add_adjacency(
                crate::encoding::v1::keys::tenant::DataScope::LegacyUnscoped,
                10,
                11,
                ir::ExpandDirection::Out,
            )
            .expect("malformed adjacency collects");

        let error = context
            .delete_nodes(&txn, [10], &mut index_context)
            .await
            .expect_err("orphan adjacency must fail closed");

        assert!(matches!(error, HelixDbError::InvariantViolation(_)));
        assert!(error.to_string().contains("adjacency without"));
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
