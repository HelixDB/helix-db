//! Edge storage mutation contracts.
//!
//! Edge helpers update endpoints, adjacency, and every V2 secondary/vector/text
//! action in one caller-owned transaction. Active
//! secondary/vector generations receive physical entry changes; hidden
//! building generations receive one coalesced entity delta in that same
//! transaction.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use slatedb::DbTransaction;

use super::contracts::{label_of, EdgeMutationTarget};
use super::MutationIndexContext;
use super::*;
use crate::index_lifecycle::graph_mutation::{
    CanonicalPropertyRow, GraphEntity, GraphMutationTransition, PropertyEdit, PropertyEditOutcome,
};

/// One edge's endpoint and property observations from the same `multi_get`.
#[derive(Clone)]
pub(super) struct ObservedEdgeRow {
    endpoints: Option<(u64, u64)>,
    properties: Option<CanonicalPropertyRow>,
}

/// Sorted, deduplicated edge observations with an ordered property overlay.
pub(super) struct ObservedEdgeRows {
    rows: BTreeMap<u64, ObservedEdgeRow>,
}

struct ObservedPairState {
    edge_ids: roaring::RoaringTreemap,
    label_counts: BTreeMap<String, usize>,
}

/// One edge deletion plus the exact pair state after removing that edge.
pub(super) struct ObservedEdgeDeletion {
    row: ObservedEdgeRow,
    pair_becomes_empty: bool,
    label_remains: bool,
}

/// Batch-owned edge and pair observations advanced in deletion order.
pub(super) struct ObservedEdgeDeletionBatch {
    rows: ObservedEdgeRows,
    pairs: BTreeMap<(u64, u64), ObservedPairState>,
}

impl ObservedEdgeRows {
    pub(super) fn observed(&self, edge_id: u64) -> ObservedEdgeRow {
        self.rows
            .get(&edge_id)
            .cloned()
            .expect("an edge observation exists for every requested entity")
    }

    pub(super) fn replace_properties(
        &mut self,
        edge_id: u64,
        properties: Option<CanonicalPropertyRow>,
    ) {
        self.rows
            .get_mut(&edge_id)
            .expect("an edge observation overlay only replaces requested entities")
            .properties = properties;
    }
}

impl ObservedEdgeDeletionBatch {
    /// Returns the sorted edge IDs owned by this observation batch.
    pub(super) fn edge_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.rows.rows.keys().copied()
    }

    /// Returns the number of distinct physical pair rows owned by the batch.
    pub(super) fn pair_count(&self) -> usize {
        self.pairs.len()
    }

    pub(super) fn matches_label(
        &self,
        edge_id: u64,
        expected: Option<&ir::NonEmptyString>,
    ) -> bool {
        let Some(expected) = expected else {
            return true;
        };
        self.rows
            .rows
            .get(&edge_id)
            .and_then(|row| row.properties.as_ref())
            .and_then(|properties| label_of(properties.properties()))
            == Some(expected.as_ref())
    }

    pub(super) fn take(&mut self, edge_id: u64) -> Result<ObservedEdgeDeletion> {
        let row = self.rows.observed(edge_id);
        let Some((from, to)) = row.endpoints else {
            return Ok(ObservedEdgeDeletion {
                row,
                pair_becomes_empty: false,
                label_remains: false,
            });
        };
        let Some(pair) = self.pairs.get_mut(&(from, to)) else {
            return Err(HelixDbError::InvariantViolation(format!(
                "edge {edge_id} endpoints have no observed pair state"
            )));
        };
        if !pair.edge_ids.remove(edge_id) {
            return Err(HelixDbError::InvariantViolation(format!(
                "edge pair index was missing edge {edge_id} for {from} -> {to}"
            )));
        }
        let label = row
            .properties
            .as_ref()
            .and_then(|properties| label_of(properties.properties()));
        if let Some(label) = label {
            let Some(remaining) = pair.label_counts.get_mut(label) else {
                return Err(HelixDbError::InvariantViolation(format!(
                    "edge {edge_id} label was missing from its pair label state"
                )));
            };
            *remaining = remaining.saturating_sub(1);
        }
        let label_remains = label.is_some_and(|label| pair.label_counts[label] != 0);
        Ok(ObservedEdgeDeletion {
            row,
            pair_becomes_empty: pair.edge_ids.is_empty(),
            label_remains,
        })
    }
}

impl<'db> ExecutionContext<'db> {
    pub(super) async fn store_edge(
        &self,
        txn: &DbTransaction,
        edge: EdgeMutationTarget,
        label: &ir::NonEmptyString,
        property_row: &CanonicalPropertyRow,
        index_context: &mut MutationIndexContext,
    ) -> Result<()> {
        let transition = GraphMutationTransition::create(
            self.tenant_scope,
            GraphEntity::edge(edge.edge_id),
            property_row.clone(),
        );
        crate::search::store_edge_endpoints_scoped(
            txn,
            edge.edge_id,
            edge.from,
            edge.to,
            self.tenant_scope,
        )
        .await?;
        index_context.topology_mutations().add_edge_pair(
            self.tenant_scope,
            edge.from,
            edge.to,
            edge.edge_id,
        )?;
        index_context.topology_mutations().add_adjacency(
            self.tenant_scope,
            edge.from,
            edge.to,
            ir::ExpandDirection::Out,
        )?;
        index_context.topology_mutations().add_adjacency(
            self.tenant_scope,
            edge.to,
            edge.from,
            ir::ExpandDirection::In,
        )?;
        index_context.topology_mutations().add_edge_label(
            self.tenant_scope,
            edge.from,
            edge.to,
            label.as_ref(),
            edge.edge_id,
        )?;
        let key = transition.graph_key();
        let encoded = property_row.encoded().clone();
        index_context
            .maintain_graph_indexes(
                txn,
                transition,
                self.db
                    .config()
                    .db()
                    .search_index_backfill()
                    .active_text_mutation(),
            )
            .await?;
        txn.put(key, encoded)?;
        Ok(())
    }

    #[cfg(test)]
    pub(super) async fn set_edge_property(
        &self,
        txn: &DbTransaction,
        edge_id: u64,
        property: Property,
        index_context: &mut MutationIndexContext,
    ) -> Result<()> {
        let observed = self
            .observe_edge_rows(txn, std::iter::once(edge_id))
            .await?;
        let _ = self
            .set_edge_property_observed(
                txn,
                edge_id,
                property,
                observed.observed(edge_id),
                index_context,
            )
            .await?;
        Ok(())
    }

    pub(super) async fn set_edge_property_observed(
        &self,
        txn: &DbTransaction,
        edge_id: u64,
        property: Property,
        observed: ObservedEdgeRow,
        index_context: &mut MutationIndexContext,
    ) -> Result<CanonicalPropertyRow> {
        let Some(_) = observed.endpoints else {
            return Err(HelixDbError::Query(format!(
                "edge {edge_id} does not exist"
            )));
        };
        let Some(before) = observed.properties else {
            return Err(HelixDbError::InvariantViolation(
                "Active text graph source disagrees with its supplied before state".to_string(),
            ));
        };
        let outcome = GraphMutationTransition::edit(
            self.tenant_scope,
            GraphEntity::edge(edge_id),
            before,
            PropertyEdit::set(property),
        );
        let PropertyEditOutcome::Changed(transition) = outcome else {
            let PropertyEditOutcome::Unchanged(row) = outcome else {
                unreachable!("property edit outcomes are closed")
            };
            return Ok(row);
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
            )
            .await?;
        txn.put(
            self.storage_key(keys::DataKeyKind::EdgePropertyById(
                keys::EdgePropertyByIdKey::new(edge_id),
            )),
            encoded,
        )?;
        Ok(final_row)
    }

    #[cfg(test)]
    pub(super) async fn remove_edge_property(
        &self,
        txn: &DbTransaction,
        edge_id: u64,
        name: &ir::NonEmptyString,
        index_context: &mut MutationIndexContext,
    ) -> Result<()> {
        let observed = self
            .observe_edge_rows(txn, std::iter::once(edge_id))
            .await?
            .observed(edge_id);
        let _ = self
            .remove_edge_property_observed(txn, edge_id, name, observed, index_context)
            .await?;
        Ok(())
    }

    pub(super) async fn remove_edge_property_observed(
        &self,
        txn: &DbTransaction,
        edge_id: u64,
        name: &ir::NonEmptyString,
        observed: ObservedEdgeRow,
        index_context: &mut MutationIndexContext,
    ) -> Result<Option<CanonicalPropertyRow>> {
        if observed.endpoints.is_none() {
            return Ok(None);
        }
        let Some(before) = observed.properties else {
            return Ok(None);
        };
        let outcome = GraphMutationTransition::edit(
            self.tenant_scope,
            GraphEntity::edge(edge_id),
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
            )
            .await?;
        txn.put(
            self.storage_key(keys::DataKeyKind::EdgePropertyById(
                keys::EdgePropertyByIdKey::new(edge_id),
            )),
            encoded,
        )?;
        Ok(Some(final_row))
    }

    pub(super) async fn observe_edge_rows(
        &self,
        txn: &DbTransaction,
        edge_ids: impl IntoIterator<Item = u64>,
    ) -> Result<ObservedEdgeRows> {
        const EDGES_PER_CHUNK: usize = 256;

        let edge_ids = edge_ids.into_iter().collect::<BTreeSet<_>>();
        let edge_ids = edge_ids.into_iter().collect::<Vec<_>>();
        let mut rows = BTreeMap::new();
        for chunk in edge_ids.chunks(EDGES_PER_CHUNK) {
            self.check_execution_deadline()?;
            let mut keys = Vec::with_capacity(chunk.len().saturating_mul(2));
            for edge_id in chunk {
                keys.push(self.storage_key(keys::DataKeyKind::EdgeEndpoints(
                    keys::EdgeEndpointsKey::new(*edge_id),
                )));
                keys.push(self.storage_key(keys::DataKeyKind::EdgePropertyById(
                    keys::EdgePropertyByIdKey::new(*edge_id),
                )));
            }
            let values = txn.multi_get(&keys).await?;
            let mut observed = keys.into_iter().zip(values);
            for edge_id in chunk {
                let (endpoint_key, endpoint_value) = observed
                    .next()
                    .expect("each observed edge has one endpoint result");
                let endpoint_value = match endpoint_value {
                    Some(value) => Some(value),
                    None => txn.get(&endpoint_key).await?,
                };
                let endpoints = endpoint_value
                    .map(|value| {
                        crate::encoding::v1::values::edge_endpoints::EdgeEndpointsValue::decode(
                            &value,
                        )
                        .map(|endpoints| (endpoints.source(), endpoints.target()))
                    })
                    .transpose()?;
                let (property_key, property_value) = observed
                    .next()
                    .expect("each observed edge has one property result");
                let property_value = match property_value {
                    Some(value) => Some(value),
                    None => txn.get(&property_key).await?,
                };
                let properties = property_value
                    .map(CanonicalPropertyRow::decode)
                    .transpose()?;
                rows.insert(
                    *edge_id,
                    ObservedEdgeRow {
                        endpoints,
                        properties,
                    },
                );
            }
            assert!(observed.next().is_none());
        }
        Ok(ObservedEdgeRows { rows })
    }

    pub(super) async fn observe_edge_deletions(
        &self,
        txn: &DbTransaction,
        edge_ids: impl IntoIterator<Item = u64>,
        index_context: &MutationIndexContext,
    ) -> Result<ObservedEdgeDeletionBatch> {
        let rows = self.observe_edge_rows(txn, edge_ids).await?;
        let pairs = rows
            .rows
            .values()
            .filter_map(|row| row.endpoints)
            .collect::<BTreeSet<_>>();
        let pair_values = self
            .observe_edge_pair_values(txn, pairs, index_context)
            .await?;
        self.finish_edge_deletion_observation(txn, rows, pair_values)
            .await
    }

    /// Observes each directed pair bitmap once in bounded, deadline-aware chunks.
    pub(super) async fn observe_edge_pair_values(
        &self,
        txn: &DbTransaction,
        pairs: BTreeSet<(u64, u64)>,
        index_context: &MutationIndexContext,
    ) -> Result<BTreeMap<(u64, u64), Option<Bytes>>> {
        const PAIRS_PER_CHUNK: usize = 512;

        let pairs = pairs.into_iter().collect::<Vec<_>>();
        let mut observed = BTreeMap::new();
        for chunk in pairs.chunks(PAIRS_PER_CHUNK) {
            self.check_execution_deadline()?;
            let keys = chunk
                .iter()
                .map(|(from, to)| {
                    keys::Key::Data {
                        scope: self.tenant_scope,
                        kind: keys::DataKeyKind::EdgePairIndex(keys::EdgePairIndexKey::new(
                            *from, *to,
                        )),
                    }
                    .to_bytes()
                })
                .collect::<Vec<_>>();
            let values = index_context.observe_topology(txn, &keys).await?;
            observed.extend(chunk.iter().copied().zip(values));
        }
        Ok(observed)
    }

    /// Completes an edge-deletion batch from already observed pair bitmaps.
    ///
    /// Reusing the pair observations is what guarantees node cascades do not
    /// reread a pair shared by multiple deleted nodes.
    pub(super) async fn observe_edge_deletions_from_pairs(
        &self,
        txn: &DbTransaction,
        edge_ids: impl IntoIterator<Item = u64>,
        pair_values: BTreeMap<(u64, u64), Option<Bytes>>,
    ) -> Result<ObservedEdgeDeletionBatch> {
        let rows = self.observe_edge_rows(txn, edge_ids).await?;
        self.finish_edge_deletion_observation(txn, rows, pair_values)
            .await
    }

    async fn finish_edge_deletion_observation(
        &self,
        txn: &DbTransaction,
        rows: ObservedEdgeRows,
        pair_values: BTreeMap<(u64, u64), Option<Bytes>>,
    ) -> Result<ObservedEdgeDeletionBatch> {
        const PROPERTIES_PER_CHUNK: usize = 512;

        let mut pair_states = pair_values
            .into_iter()
            .map(|(pair, value)| {
                let edge_ids = value
                    .map(|value| {
                        values::secondary::SecondaryEqualityValue::decode(&value)
                            .map(|value| value.into_ids())
                    })
                    .transpose()?
                    .unwrap_or_default();
                Ok((
                    pair,
                    ObservedPairState {
                        edge_ids,
                        label_counts: BTreeMap::new(),
                    },
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;

        for (edge_id, row) in &rows.rows {
            let Some((from, to)) = row.endpoints else {
                if pair_states
                    .values()
                    .any(|pair| pair.edge_ids.contains(*edge_id))
                {
                    return Err(HelixDbError::InvariantViolation(format!(
                        "edge pair index references edge {edge_id} without endpoints"
                    )));
                }
                continue;
            };
            let Some(pair) = pair_states.get(&(from, to)) else {
                return Err(HelixDbError::InvariantViolation(format!(
                    "edge {edge_id} endpoints have no observed pair state"
                )));
            };
            if !pair.edge_ids.contains(*edge_id) {
                return Err(HelixDbError::InvariantViolation(format!(
                    "edge pair index was missing edge {edge_id} for {from} -> {to}"
                )));
            }
        }

        let pair_edge_ids = pair_states
            .values()
            .flat_map(|pair| pair.edge_ids.iter())
            .collect::<BTreeSet<_>>();
        let pair_edge_ids = pair_edge_ids.into_iter().collect::<Vec<_>>();
        let mut labels = BTreeMap::new();
        for chunk in pair_edge_ids.chunks(PROPERTIES_PER_CHUNK) {
            self.check_execution_deadline()?;
            let property_keys = chunk
                .iter()
                .map(|edge_id| {
                    keys::Key::Data {
                        scope: self.tenant_scope,
                        kind: keys::DataKeyKind::EdgePropertyById(keys::EdgePropertyByIdKey::new(
                            *edge_id,
                        )),
                    }
                    .to_bytes()
                })
                .collect::<Vec<_>>();
            let property_values = txn.multi_get(&property_keys).await?;
            for ((edge_id, key), value) in chunk
                .iter()
                .copied()
                .zip(property_keys)
                .zip(property_values)
            {
                let value = match value {
                    Some(value) => Some(value),
                    None => txn.get(&key).await?,
                };
                let label = value
                    .map(|value| decode_properties(&value))
                    .transpose()?
                    .and_then(|properties| label_of(&properties).map(str::to_owned));
                labels.insert(edge_id, label);
            }
        }
        for pair in pair_states.values_mut() {
            for label in pair
                .edge_ids
                .iter()
                .filter_map(|edge_id| labels.get(&edge_id).and_then(Option::as_deref))
            {
                *pair.label_counts.entry(label.to_owned()).or_default() += 1;
            }
        }
        Ok(ObservedEdgeDeletionBatch {
            rows,
            pairs: pair_states,
        })
    }

    #[cfg(test)]
    pub(super) async fn delete_edge(
        &self,
        txn: &DbTransaction,
        edge_id: u64,
        index_context: &mut MutationIndexContext,
    ) -> Result<()> {
        index_context.flush_topology(txn).await?;
        let mut observed = self
            .observe_edge_deletions(txn, std::iter::once(edge_id), index_context)
            .await?;
        self.delete_edge_observed(txn, edge_id, observed.take(edge_id)?, index_context)
            .await?;
        index_context.flush_topology(txn).await
    }

    pub(super) async fn delete_edge_observed(
        &self,
        txn: &DbTransaction,
        edge_id: u64,
        observed: ObservedEdgeDeletion,
        index_context: &mut MutationIndexContext,
    ) -> Result<()> {
        let Some((from, to)) = observed.row.endpoints else {
            return Ok(());
        };
        let property_key = self.storage_key(keys::DataKeyKind::EdgePropertyById(
            keys::EdgePropertyByIdKey::new(edge_id),
        ));
        let Some(properties) = observed.row.properties else {
            return Err(HelixDbError::InvariantViolation(
                "Active text graph source disagrees with its supplied before state".to_string(),
            ));
        };
        let transition = GraphMutationTransition::delete(
            self.tenant_scope,
            GraphEntity::edge(edge_id),
            properties,
        );
        let properties = transition
            .before()
            .expect("a delete transition has a before row")
            .properties();
        let label = label_of(properties).map(str::to_string);
        if let Some(label) = label.as_deref() {
            index_context
                .topology_mutations()
                .remove_global_edge_label(self.tenant_scope, label, edge_id)?;
            if !observed.label_remains {
                index_context
                    .topology_mutations()
                    .remove_edge_label_neighbors(self.tenant_scope, from, to, label)?;
            }
        }
        index_context.topology_mutations().remove_edge_pair(
            self.tenant_scope,
            from,
            to,
            edge_id,
        )?;
        if observed.pair_becomes_empty {
            index_context.topology_mutations().remove_adjacency(
                self.tenant_scope,
                from,
                to,
                ir::ExpandDirection::Out,
            )?;
            index_context.topology_mutations().remove_adjacency(
                self.tenant_scope,
                to,
                from,
                ir::ExpandDirection::In,
            )?;
        }
        crate::search::delete_edge_endpoints_scoped(txn, edge_id, self.tenant_scope).await?;
        index_context
            .maintain_graph_indexes(
                txn,
                transition,
                self.db
                    .config()
                    .db()
                    .search_index_backfill()
                    .active_text_mutation(),
            )
            .await?;
        txn.delete(property_key)?;
        Ok(())
    }

    #[cfg(test)]
    pub(super) async fn edge_matches_label(
        &self,
        txn: &DbTransaction,
        edge_id: u64,
        expected: Option<&ir::NonEmptyString>,
    ) -> Result<bool> {
        let Some(expected) = expected else {
            return Ok(true);
        };
        let properties =
            crate::search::get_edge_properties_by_id_scoped(txn, edge_id, self.tenant_scope)
                .await?;
        Ok(label_of(&properties) == Some(expected.as_ref()))
    }

    pub(super) fn edge_targets(&self, plan: &ir::EdgeTargetPlan) -> Result<Vec<u64>> {
        match plan {
            ir::EdgeTargetPlan::Empty => Ok(Vec::new()),
            ir::EdgeTargetPlan::PointIds { ids } => Ok(ids.as_ref().to_vec()),
            ir::EdgeTargetPlan::FromParam { param } => self.param_ids(param),
            ir::EdgeTargetPlan::FromVar { variable } => self.variable_edges(variable),
        }
    }
}

#[cfg(test)]
mod tests {
    use helix_ast::value::PropertyValue as AstPropertyValue;
    use helix_planner::context;
    use slatedb::IsolationLevel;

    use super::super::super::test_support;
    use super::*;

    fn index_context(db: &HelixDB) -> MutationIndexContext {
        MutationIndexContext::for_configured_index_test(std::sync::Arc::clone(
            db.simhasher_registry(),
        ))
    }

    #[tokio::test]
    async fn missing_edges_have_explicit_property_and_delete_contracts() {
        let db = test_support::open_db("mutation-missing-edge-contracts").await;
        let context = ExecutionContext::new(&db, context::ParamBindings::default());
        let mut index_context = index_context(&db);
        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("snapshot transaction begins");
        let error = context
            .set_edge_property(
                &txn,
                99,
                Property::new("weight", DbPropertyValue::I64(1)),
                &mut index_context,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("edge 99 does not exist"));

        context
            .remove_edge_property(&txn, 99, &test_support::name("weight"), &mut index_context)
            .await
            .expect("removing a property from a missing edge is idempotent");
        context
            .delete_edge(&txn, 99, &mut index_context)
            .await
            .expect("deleting a missing edge is idempotent");
    }

    #[tokio::test]
    async fn removing_an_absent_edge_property_preserves_existing_properties() {
        let db = test_support::open_db("mutation-absent-edge-property").await;
        let from = test_support::add_user(&db, "alice").await;
        let to = test_support::add_user(&db, "bob").await;
        let edge_id = test_support::add_edge_with_properties(
            &db,
            from,
            to,
            "FOLLOWS",
            vec![("weight", AstPropertyValue::I64(3))],
        )
        .await;
        let context = ExecutionContext::new(&db, context::ParamBindings::default());
        let mut index_context = index_context(&db);
        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("snapshot transaction begins");

        context
            .remove_edge_property(
                &txn,
                edge_id,
                &test_support::name("missing"),
                &mut index_context,
            )
            .await
            .expect("removing an absent property is idempotent");

        let properties = crate::search::get_edge_properties_by_id(&txn, edge_id)
            .await
            .unwrap();
        assert!(properties.iter().any(|property| {
            property.name == "weight" && property.value == DbPropertyValue::I64(3)
        }));
    }

    #[tokio::test]
    async fn edge_property_and_delete_contracts_preserve_shared_pair_indexes() {
        let config = test_support::in_memory_config("mutation-edge-property-delete-contracts")
            .with_edge_equality_index("FOLLOWS", "status")
            .with_edge_range_index("FOLLOWS", "weight");
        let db = test_support::open_db_with_config(config).await;
        let from = test_support::add_user(&db, "alice").await;
        let to = test_support::add_user(&db, "bob").await;
        let first = test_support::add_edge_with_properties(
            &db,
            from,
            to,
            "FOLLOWS",
            vec![("weight", AstPropertyValue::I64(3))],
        )
        .await;
        let second = test_support::add_edge_with_properties(
            &db,
            from,
            to,
            "FOLLOWS",
            vec![("weight", AstPropertyValue::I64(5))],
        )
        .await;
        let context = ExecutionContext::new(&db, context::ParamBindings::default());
        let mut index_context = index_context(&db);
        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("snapshot transaction begins");
        context
            .set_edge_property(
                &txn,
                first,
                Property::string("status", "active"),
                &mut index_context,
            )
            .await
            .expect("setting an indexed edge property succeeds");
        context
            .remove_edge_property(
                &txn,
                first,
                &test_support::name("weight"),
                &mut index_context,
            )
            .await
            .expect("removing an indexed edge property succeeds");
        let properties = crate::search::get_edge_properties_by_id(&txn, first)
            .await
            .unwrap();
        assert!(properties.iter().any(|property| {
            property.name == "status"
                && property.value == DbPropertyValue::String("active".to_string())
        }));
        assert!(!properties.iter().any(|property| property.name == "weight"));
        assert!(context.edge_matches_label(&txn, first, None).await.unwrap());
        assert!(context
            .edge_matches_label(&txn, first, Some(&test_support::name("FOLLOWS")))
            .await
            .unwrap());
        assert!(!context
            .edge_matches_label(&txn, first, Some(&test_support::name("LIKES")))
            .await
            .unwrap());

        context
            .delete_edge(&txn, first, &mut index_context)
            .await
            .expect("deleting one same-label parallel edge succeeds");
        assert_eq!(
            crate::search::lookup_edge_pair_index(&txn, from, to)
                .await
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![second]
        );
        context
            .delete_edge(&txn, second, &mut index_context)
            .await
            .expect("deleting the last pair edge succeeds");
        assert!(crate::search::lookup_edge_pair_index(&txn, from, to)
            .await
            .unwrap()
            .is_empty());
        txn.commit().await.expect("edge changes commit atomically");
    }

    #[tokio::test]
    async fn edge_targets_resolve_parameter_and_variable_sources() {
        let db = test_support::open_db("mutation-edge-target-sources").await;
        let param = test_support::name("ids");
        let variable = test_support::name("edges");
        let mut context = ExecutionContext::new(
            &db,
            context::ParamBindings::default()
                .with_value(param.clone(), AstPropertyValue::I64Array(vec![4, 5])),
        );
        context.variables.insert(
            variable.clone(),
            ExecutionValue::Stream(vec![
                ExecutionRow::current(ElementRef::Edge(8)),
                ExecutionRow::current(ElementRef::Node(9)),
                ExecutionRow::current(ElementRef::Edge(10)),
            ]),
        );

        assert_eq!(
            context
                .edge_targets(&ir::EdgeTargetPlan::FromParam { param })
                .unwrap(),
            vec![4, 5]
        );
        assert_eq!(
            context
                .edge_targets(&ir::EdgeTargetPlan::FromVar { variable })
                .unwrap(),
            vec![8, 10]
        );
    }
}
