//! Node storage mutation contracts.
//!
//! Each helper stages the authoritative node row together with configured
//! indexes and every V2 secondary/vector/text action captured by the request
//! transaction. Active secondary/vector generations receive physical entry
//! changes; hidden building generations receive one coalesced entity delta in
//! that same transaction.

use std::collections::BTreeSet;

use slatedb::DbTransaction;

use super::super::search_index::TextIndexMaintenanceOutcome;
use super::contracts::{
    decode_stored_edges, decode_stored_properties, label_of, remove_property_by_name,
    upsert_property,
};
use super::MutationIndexContext;
use super::*;
use crate::encoding::property;

impl<'db> ExecutionContext<'db> {
    pub(super) async fn store_node(
        &self,
        txn: &DbTransaction,
        node_id: u64,
        properties: &[Property],
        indexes: &crate::config::RuntimeIndexCatalog,
        index_context: &mut MutationIndexContext,
    ) -> Result<TextIndexMaintenanceOutcome> {
        self.stage_text_graph_mutation(
            txn,
            index_context,
            crate::index_v2::text::active_request::ActiveTextGraphMutation::create(
                self.tenant_scope,
                crate::encoding::v1::keys::index_v2::IndexEntity {
                    kind: crate::index_v2::IndexElementKind::Node,
                    id: crate::index_v2::IndexEntityId::new(node_id),
                },
                properties,
            ),
        )
        .await?;
        crate::index_v2::secondary::maintain_entity(
            txn,
            self.tenant_scope,
            index_context.secondary(),
            crate::index_v2::IndexElementKind::Node,
            node_id,
            &[],
            properties,
        )
        .await?;
        crate::search::update_indexes_for_properties_scoped(
            txn,
            node_id,
            properties,
            indexes,
            self.tenant_scope,
        )
        .await?;
        self.maintain_node_vector_indexes_on_create(
            txn,
            node_id,
            properties,
            indexes,
            index_context,
        )
        .await?;
        let text_indexes = self
            .maintain_node_text_indexes_on_create(txn, node_id, properties, indexes)
            .await?;
        Ok(text_indexes)
    }

    pub(super) async fn set_node_property(
        &self,
        txn: &DbTransaction,
        node_id: u64,
        property: Property,
        indexes: &crate::config::RuntimeIndexCatalog,
        index_context: &mut MutationIndexContext,
    ) -> Result<TextIndexMaintenanceOutcome> {
        let key = self.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
            node_id,
        )));
        let mut properties = decode_stored_properties(txn.get(&key).await?)?;
        let old_properties = properties.clone();
        let label = label_of(&properties).map(str::to_string);
        let old = upsert_property(&mut properties, property.clone());
        if property.name == "$label" && property.value.as_str().is_none() {
            return Err(HelixDbError::Query(
                "node `$label` mutations require a string value".to_string(),
            ));
        }
        self.stage_text_graph_mutation(
            txn,
            index_context,
            crate::index_v2::text::active_request::ActiveTextGraphMutation::replace(
                self.tenant_scope,
                crate::encoding::v1::keys::index_v2::IndexEntity {
                    kind: crate::index_v2::IndexElementKind::Node,
                    id: crate::index_v2::IndexEntityId::new(node_id),
                },
                &old_properties,
                &properties,
            ),
        )
        .await?;
        let update_indexes = crate::search::NodePropertyIndexUpdateCatalog::new(indexes);
        if property.name == "$label" {
            let Some(new_label) = property.value.as_str() else {
                return Err(HelixDbError::InvariantViolation(
                    "validated node label lost its string value".to_string(),
                ));
            };
            if label.as_deref() == Some(new_label) {
                crate::search::update_indexes_for_property(
                    crate::search::NodePropertyIndexUpdateRequest::new_scoped(
                        txn,
                        node_id,
                        label.as_deref(),
                        &property,
                        old.as_ref().map(|property| &property.value),
                        update_indexes,
                        self.tenant_scope,
                    ),
                )
                .await?;
            } else {
                for indexed in properties
                    .iter()
                    .filter(|property| property.name != "$label")
                {
                    crate::search::validate_unique_node_equality_candidate_scoped(
                        txn,
                        node_id,
                        new_label,
                        &indexed.name,
                        &indexed.value,
                        indexes,
                        self.tenant_scope,
                    )
                    .await?;
                }
                crate::search::remove_all_indexes_for_node_scoped(
                    txn,
                    node_id,
                    &old_properties,
                    indexes,
                    self.tenant_scope,
                )
                .await?;
                crate::search::update_indexes_for_properties_scoped(
                    txn,
                    node_id,
                    &properties,
                    indexes,
                    self.tenant_scope,
                )
                .await?;
            }
        } else {
            crate::search::update_indexes_for_property(
                crate::search::NodePropertyIndexUpdateRequest::new_scoped(
                    txn,
                    node_id,
                    label.as_deref(),
                    &property,
                    old.as_ref().map(|property| &property.value),
                    update_indexes,
                    self.tenant_scope,
                ),
            )
            .await?;
        }
        self.maintain_node_vector_indexes_on_update(
            txn,
            node_id,
            super::super::search_index::VectorPropertyUpdate::new(
                &old_properties,
                &properties,
                &property.name,
            ),
            indexes,
            index_context,
        )
        .await?;
        let text_indexes = self
            .maintain_node_text_indexes_on_update(
                txn,
                node_id,
                super::super::search_index::TextPropertyUpdate::new(
                    &old_properties,
                    &properties,
                    &property.name,
                ),
                indexes,
            )
            .await?;
        crate::index_v2::secondary::maintain_entity(
            txn,
            self.tenant_scope,
            index_context.secondary(),
            crate::index_v2::IndexElementKind::Node,
            node_id,
            &old_properties,
            &properties,
        )
        .await?;
        Ok(text_indexes)
    }

    pub(super) async fn remove_node_property(
        &self,
        txn: &DbTransaction,
        node_id: u64,
        name: &ir::NonEmptyString,
        indexes: &crate::config::RuntimeIndexCatalog,
        index_context: &mut MutationIndexContext,
    ) -> Result<TextIndexMaintenanceOutcome> {
        let key = self.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
            node_id,
        )));
        let mut properties = decode_stored_properties(txn.get(&key).await?)?;
        let old_properties = properties.clone();
        let label = label_of(&properties).map(str::to_string);
        let Some(removed) = remove_property_by_name(&mut properties, name.as_ref()) else {
            return Ok(TextIndexMaintenanceOutcome::default());
        };
        self.stage_text_graph_mutation(
            txn,
            index_context,
            crate::index_v2::text::active_request::ActiveTextGraphMutation::replace(
                self.tenant_scope,
                crate::encoding::v1::keys::index_v2::IndexEntity {
                    kind: crate::index_v2::IndexElementKind::Node,
                    id: crate::index_v2::IndexEntityId::new(node_id),
                },
                &old_properties,
                &properties,
            ),
        )
        .await?;
        crate::search::remove_indexes_for_property(
            crate::search::NodePropertyIndexRemovalRequest::new_scoped(
                txn,
                node_id,
                label.as_deref(),
                name.as_ref(),
                &removed.value,
                crate::search::NodePropertyIndexRemovalCatalog::new(indexes),
                self.tenant_scope,
            ),
        )
        .await?;
        self.maintain_node_vector_indexes_on_update(
            txn,
            node_id,
            super::super::search_index::VectorPropertyUpdate::new(
                &old_properties,
                &properties,
                name.as_ref(),
            ),
            indexes,
            index_context,
        )
        .await?;
        let text_indexes = self
            .maintain_node_text_indexes_on_update(
                txn,
                node_id,
                super::super::search_index::TextPropertyUpdate::new(
                    &old_properties,
                    &properties,
                    name.as_ref(),
                ),
                indexes,
            )
            .await?;
        crate::index_v2::secondary::maintain_entity(
            txn,
            self.tenant_scope,
            index_context.secondary(),
            crate::index_v2::IndexElementKind::Node,
            node_id,
            &old_properties,
            &properties,
        )
        .await?;
        Ok(text_indexes)
    }

    pub(super) async fn delete_node(
        &self,
        txn: &DbTransaction,
        node_id: u64,
        indexes: &crate::config::RuntimeIndexCatalog,
        index_context: &mut MutationIndexContext,
    ) -> Result<TextIndexMaintenanceOutcome> {
        let key = self.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
            node_id,
        )));
        let Some(stored) = txn.get(&key).await? else {
            return Ok(TextIndexMaintenanceOutcome::default());
        };
        let properties = property::decode_properties(&stored)?;
        let incident_edges = self.incident_edge_ids(txn, node_id).await?;
        self.stage_text_graph_mutation(
            txn,
            index_context,
            crate::index_v2::text::active_request::ActiveTextGraphMutation::delete(
                self.tenant_scope,
                crate::encoding::v1::keys::index_v2::IndexEntity {
                    kind: crate::index_v2::IndexElementKind::Node,
                    id: crate::index_v2::IndexEntityId::new(node_id),
                },
                &properties,
            ),
        )
        .await?;
        let mut text_indexes = TextIndexMaintenanceOutcome::default();
        for edge_id in incident_edges {
            self.delete_edge(txn, edge_id, indexes, index_context)
                .await?
                .merge_into(&mut text_indexes);
        }
        crate::search::remove_all_indexes_for_node_scoped(
            txn,
            node_id,
            &properties,
            indexes,
            self.tenant_scope,
        )
        .await?;
        self.maintain_node_vector_indexes_on_delete(
            txn,
            node_id,
            &properties,
            indexes,
            index_context,
        )
        .await?;
        self.maintain_node_text_indexes_on_delete(txn, node_id, &properties, indexes)
            .await?
            .merge_into(&mut text_indexes);
        crate::index_v2::secondary::maintain_entity(
            txn,
            self.tenant_scope,
            index_context.secondary(),
            crate::index_v2::IndexElementKind::Node,
            node_id,
            &properties,
            &[],
        )
        .await?;
        txn.delete(
            self.storage_key(keys::DataKeyKind::Adjacency(keys::AdjacencyKey::new(
                node_id,
            ))),
        )?;
        Ok(text_indexes)
    }

    async fn incident_edge_ids(&self, txn: &DbTransaction, node_id: u64) -> Result<BTreeSet<u64>> {
        let key = self.storage_key(keys::DataKeyKind::Adjacency(keys::AdjacencyKey::new(
            node_id,
        )));
        let edges = decode_stored_edges(txn.get(&key).await?)?;
        let mut edge_ids = BTreeSet::new();
        for to in edges.iter_out() {
            edge_ids.extend(
                crate::search::lookup_edge_pair_index_scoped(txn, node_id, to, self.tenant_scope)
                    .await?,
            );
        }
        for from in edges.iter_in() {
            edge_ids.extend(
                crate::search::lookup_edge_pair_index_scoped(txn, from, node_id, self.tenant_scope)
                    .await?,
            );
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
    async fn label_updates_validate_values_and_move_secondary_indexes() {
        let config = test_support::in_memory_config("mutation-node-label-updates")
            .with_equality_index("User", "name")
            .with_equality_index("Admin", "name");
        let db = test_support::open_db_with_config(config).await;
        let node_id = test_support::add_user(&db, "alice").await;
        let context = ExecutionContext::new(&db, context::ParamBindings::default());
        let mut index_context = index_context(&db);
        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("snapshot transaction begins");
        let indexes = db.runtime_config_snapshot_loaded(
            crate::encoding::keys::tenant::DataScope::LegacyUnscoped,
        );

        let error = context
            .set_node_property(
                &txn,
                node_id,
                Property::new("$label", DbPropertyValue::I64(7)),
                &indexes,
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
                &indexes,
                &mut index_context,
            )
            .await
            .expect("setting the same label preserves indexes");
        context
            .set_node_property(
                &txn,
                node_id,
                Property::string("$label", "Admin"),
                &indexes,
                &mut index_context,
            )
            .await
            .expect("changing the label moves indexes");

        assert!(crate::search::lookup_equality_index(
            &txn,
            &crate::config::scoped_secondary_index_property("User", "name"),
            "alice",
        )
        .await
        .unwrap()
        .is_empty());
        assert_eq!(
            crate::search::lookup_equality_index(
                &txn,
                &crate::config::scoped_secondary_index_property("Admin", "name"),
                "alice",
            )
            .await
            .unwrap(),
            vec![node_id]
        );

        context
            .remove_node_property(
                &txn,
                node_id,
                &test_support::name("missing"),
                &indexes,
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
        let indexes = crate::config::RuntimeIndexCatalog::default();

        context
            .delete_node(&txn, to, &indexes, &mut index_context)
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
            .delete_node(&txn, 99, &indexes, &mut index_context)
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
