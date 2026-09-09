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

    pub(super) async fn delete_node(
        &self,
        txn: &DbTransaction,
        node_id: u64,
        index_context: &mut MutationIndexContext,
    ) -> Result<()> {
        let key = self.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
            node_id,
        )));
        let Some(stored) = txn.get(&key).await? else {
            return Ok(());
        };
        let transition = GraphMutationTransition::delete(
            self.tenant_scope,
            GraphEntity::node(node_id),
            CanonicalPropertyRow::decode(stored)?,
        );
        let properties = transition
            .before()
            .expect("a delete transition has a before row")
            .properties();
        index_context.flush_topology(txn).await?;
        let incident_edges = self.incident_edge_ids(txn, node_id, index_context).await?;
        let mut observed_edges = self
            .observe_edge_deletions(txn, incident_edges.iter().copied(), index_context)
            .await?;
        for edge_id in incident_edges {
            self.check_execution_deadline()?;
            self.delete_edge_observed(txn, edge_id, observed_edges.take(edge_id)?, index_context)
                .await?;
        }
        index_context.flush_topology(txn).await?;
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
            )
            .await?;
        txn.delete(&key)?;
        txn.delete(
            self.storage_key(keys::DataKeyKind::Adjacency(keys::AdjacencyKey::new(
                node_id,
            ))),
        )?;
        Ok(())
    }

    async fn incident_edge_ids(
        &self,
        txn: &DbTransaction,
        node_id: u64,
        index_context: &MutationIndexContext,
    ) -> Result<BTreeSet<u64>> {
        let key = self.storage_key(keys::DataKeyKind::Adjacency(keys::AdjacencyKey::new(
            node_id,
        )));
        let edges = decode_stored_edges(txn.get(&key).await?)?;
        let pairs = edges
            .iter_out()
            .map(|to| (node_id, to))
            .chain(edges.iter_in().map(|from| (from, node_id)))
            .collect::<BTreeSet<_>>();
        let keys = pairs
            .into_iter()
            .map(|(from, to)| {
                self.storage_key(keys::DataKeyKind::EdgePairIndex(
                    keys::EdgePairIndexKey::new(from, to),
                ))
            })
            .collect::<Vec<_>>();
        let mut edge_ids = BTreeSet::new();
        for value in index_context.observe_topology(txn, &keys).await? {
            self.check_execution_deadline()?;
            let Some(value) = value else {
                continue;
            };
            edge_ids.extend(values::indexes::SecondaryEqualityValue::decode(&value)?.into_ids());
        }
        Ok(edge_ids)
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
    use bytes::Bytes;
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

    async fn all_rows(db: &HelixDB) -> BTreeMap<Bytes, Bytes> {
        let mut scan = db.inner_db().scan(..).await.unwrap();
        let mut rows = BTreeMap::new();
        while let Some(row) = scan.next().await.unwrap() {
            rows.insert(row.key, row.value);
        }
        rows
    }

    #[tokio::test]
    async fn rejected_cascade_delete_preserves_every_graph_and_index_row() {
        let scope = keys::scope::DataScope::LegacyUnscoped;
        let config = test_support::in_memory_config("mutation-corrupt-cascade-delete")
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
        let cases = [
            (
                "node-label",
                super::super::topology::node_label_key(scope, "User"),
            ),
            (
                "edge-pair",
                super::super::topology::edge_pair_key(scope, target, neighbor),
            ),
            (
                "edge-label-out",
                super::super::topology::edge_label_neighbor_key(
                    scope,
                    crate::encoding::indexes::EdgeDirection::Out,
                    target,
                    "FOLLOWS",
                ),
            ),
            (
                "edge-label-in",
                super::super::topology::edge_label_neighbor_key(
                    scope,
                    crate::encoding::indexes::EdgeDirection::In,
                    target,
                    "FOLLOWS",
                ),
            ),
            (
                "global-edge-label",
                super::super::topology::global_edge_label_key(scope, "FOLLOWS"),
            ),
            (
                "adjacency",
                keys::DataKey::Data {
                    scope,
                    kind: keys::DataKeyKind::Adjacency(keys::AdjacencyKey::new(target)),
                }
                .to_bytes(),
            ),
            (
                "node-equality",
                crate::index_lifecycle::secondary::equality_bitmap_key_for_test(
                    node_equality,
                    "shared",
                ),
            ),
            (
                "edge-equality",
                crate::index_lifecycle::secondary::equality_bitmap_key_for_test(
                    edge_equality,
                    "shared",
                ),
            ),
        ];
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

        for (name, key) in cases {
            let original = db
                .inner_db()
                .get(&key)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("{name} fixture row exists"));
            db.inner_db()
                .put(&key, Bytes::from_static(b"corrupt-membership-row"))
                .await
                .unwrap();
            let before = all_rows(&db).await;
            db.execute(
                &drop_node,
                context::ParamBindings::default()
                    .with_value(node_param.clone(), AstPropertyValue::I64(target as i64)),
            )
            .await
            .expect_err("corrupt membership rows reject the cascade delete");
            assert_eq!(all_rows(&db).await, before, "{name}");
            for record_key in [&node_key, &outgoing_key, &incoming_key] {
                assert!(
                    db.inner_db().get(record_key).await.unwrap().is_some(),
                    "{name} preserves node and edge records"
                );
            }
            db.inner_db().put(&key, original).await.unwrap();
        }

        db.execute(
            &drop_node,
            context::ParamBindings::default()
                .with_value(node_param, AstPropertyValue::I64(target as i64)),
        )
        .await
        .expect("the repaired fixture still permits a complete cascade delete");
        for record_key in [&node_key, &outgoing_key, &incoming_key] {
            assert!(db.inner_db().get(record_key).await.unwrap().is_none());
        }
        db.close().await.unwrap();
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
