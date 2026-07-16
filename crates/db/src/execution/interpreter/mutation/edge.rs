//! Edge storage mutation contracts.
//!
//! Edge helpers update endpoints, adjacency, configured indexes, and every V2
//! secondary/vector/text action in one caller-owned transaction. Active
//! secondary/vector generations receive physical entry changes; hidden
//! building generations receive one coalesced entity delta in that same
//! transaction.

use slatedb::DbTransaction;

use super::super::search_index::TextIndexMaintenanceOutcome;
use super::contracts::{
    decode_stored_properties, label_of, remove_edge_property_indexes, remove_property_by_name,
    upsert_property, EdgeMutationTarget,
};
use super::MutationIndexContext;
use super::*;

impl<'db> ExecutionContext<'db> {
    pub(super) async fn store_edge(
        &self,
        txn: &DbTransaction,
        edge: EdgeMutationTarget,
        label: &ir::NonEmptyString,
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
                    kind: crate::index_v2::IndexElementKind::Edge,
                    id: crate::index_v2::IndexEntityId::new(edge.edge_id),
                },
                properties,
            ),
        )
        .await?;
        crate::search::store_edge_endpoints_scoped(
            txn,
            edge.edge_id,
            edge.from,
            edge.to,
            self.tenant_scope,
        )
        .await?;
        crate::search::add_to_edge_pair_index_scoped(
            txn,
            edge.from,
            edge.to,
            edge.edge_id,
            self.tenant_scope,
        )
        .await?;
        self.add_adjacency(txn, edge.from, edge.to, ir::ExpandDirection::Out)
            .await?;
        self.add_adjacency(txn, edge.to, edge.from, ir::ExpandDirection::In)
            .await?;
        crate::search::add_to_edge_label_index_scoped(
            txn,
            edge.from,
            edge.to,
            label.as_ref(),
            self.tenant_scope,
        )
        .await?;
        crate::search::add_to_global_edge_label_index_scoped(
            txn,
            label.as_ref(),
            edge.edge_id,
            self.tenant_scope,
        )
        .await?;
        for property in properties {
            crate::search::update_edge_indexes_for_property(
                crate::search::EdgePropertyIndexUpdateRequest::new(
                    txn,
                    crate::search::EdgeIndexTarget::new(edge.from, edge.to, edge.edge_id),
                    Some(label.as_ref()),
                    &property.name,
                    &property.value,
                    None,
                    crate::search::EdgePropertyIndexCatalog::new(indexes),
                )
                .with_tenant_scope(self.tenant_scope),
            )
            .await?;
        }
        crate::index_v2::secondary::maintain_entity(
            txn,
            self.tenant_scope,
            index_context.secondary(),
            crate::index_v2::IndexElementKind::Edge,
            edge.edge_id,
            &[],
            properties,
        )
        .await?;
        self.maintain_edge_vector_indexes_on_create(
            txn,
            edge.edge_id,
            properties,
            indexes,
            index_context,
        )
        .await?;
        let text_indexes = self
            .maintain_edge_text_indexes_on_create(txn, edge.edge_id, properties, indexes)
            .await?;
        Ok(text_indexes)
    }

    pub(super) async fn set_edge_property(
        &self,
        txn: &DbTransaction,
        edge_id: u64,
        property: Property,
        indexes: &crate::config::RuntimeIndexCatalog,
        index_context: &mut MutationIndexContext,
    ) -> Result<TextIndexMaintenanceOutcome> {
        let Some((from, to)) =
            crate::search::get_edge_endpoints_scoped(txn, edge_id, self.tenant_scope).await?
        else {
            return Err(HelixDbError::Query(format!(
                "edge {edge_id} does not exist"
            )));
        };
        let key = self.storage_key(keys::DataKeyKind::EdgePropertyById(
            keys::EdgePropertyByIdKey::new(edge_id),
        ));
        let mut properties = decode_stored_properties(txn.get(&key).await?)?;
        let old_properties = properties.clone();
        let label = label_of(&properties).map(str::to_string);
        let old = upsert_property(&mut properties, property.clone());
        self.stage_text_graph_mutation(
            txn,
            index_context,
            crate::index_v2::text::active_request::ActiveTextGraphMutation::replace(
                self.tenant_scope,
                crate::encoding::v1::keys::index_v2::IndexEntity {
                    kind: crate::index_v2::IndexElementKind::Edge,
                    id: crate::index_v2::IndexEntityId::new(edge_id),
                },
                &old_properties,
                &properties,
            ),
        )
        .await?;
        crate::search::update_edge_indexes_for_property(
            crate::search::EdgePropertyIndexUpdateRequest::new(
                txn,
                crate::search::EdgeIndexTarget::new(from, to, edge_id),
                label.as_deref(),
                &property.name,
                &property.value,
                old.as_ref().map(|property| &property.value),
                crate::search::EdgePropertyIndexCatalog::new(indexes),
            )
            .with_tenant_scope(self.tenant_scope),
        )
        .await?;
        self.maintain_edge_vector_indexes_on_update(
            txn,
            edge_id,
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
            .maintain_edge_text_indexes_on_update(
                txn,
                edge_id,
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
            crate::index_v2::IndexElementKind::Edge,
            edge_id,
            &old_properties,
            &properties,
        )
        .await?;
        Ok(text_indexes)
    }

    pub(super) async fn remove_edge_property(
        &self,
        txn: &DbTransaction,
        edge_id: u64,
        name: &ir::NonEmptyString,
        indexes: &crate::config::RuntimeIndexCatalog,
        index_context: &mut MutationIndexContext,
    ) -> Result<TextIndexMaintenanceOutcome> {
        let Some((from, to)) =
            crate::search::get_edge_endpoints_scoped(txn, edge_id, self.tenant_scope).await?
        else {
            return Ok(TextIndexMaintenanceOutcome::default());
        };
        let key = self.storage_key(keys::DataKeyKind::EdgePropertyById(
            keys::EdgePropertyByIdKey::new(edge_id),
        ));
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
                    kind: crate::index_v2::IndexElementKind::Edge,
                    id: crate::index_v2::IndexEntityId::new(edge_id),
                },
                &old_properties,
                &properties,
            ),
        )
        .await?;
        let edge = EdgeMutationTarget::new(edge_id, from, to);
        remove_edge_property_indexes(
            txn,
            edge,
            label.as_deref(),
            name.as_ref(),
            &removed.value,
            indexes,
            self.tenant_scope,
        )
        .await?;
        self.maintain_edge_vector_indexes_on_update(
            txn,
            edge_id,
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
            .maintain_edge_text_indexes_on_update(
                txn,
                edge_id,
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
            crate::index_v2::IndexElementKind::Edge,
            edge_id,
            &old_properties,
            &properties,
        )
        .await?;
        Ok(text_indexes)
    }

    pub(super) async fn delete_edge(
        &self,
        txn: &DbTransaction,
        edge_id: u64,
        indexes: &crate::config::RuntimeIndexCatalog,
        index_context: &mut MutationIndexContext,
    ) -> Result<TextIndexMaintenanceOutcome> {
        let Some((from, to)) =
            crate::search::get_edge_endpoints_scoped(txn, edge_id, self.tenant_scope).await?
        else {
            return Ok(TextIndexMaintenanceOutcome::default());
        };
        let properties =
            crate::search::get_edge_properties_by_id_scoped(txn, edge_id, self.tenant_scope)
                .await?;
        let label = label_of(&properties).map(str::to_string);
        let mut remaining_pair =
            crate::search::lookup_edge_pair_index_scoped(txn, from, to, self.tenant_scope).await?;
        debug_assert!(
            remaining_pair.contains(edge_id),
            "edge pair index was missing edge {edge_id} for {from} -> {to}"
        );
        remaining_pair.remove(edge_id);

        self.stage_text_graph_mutation(
            txn,
            index_context,
            crate::index_v2::text::active_request::ActiveTextGraphMutation::delete(
                self.tenant_scope,
                crate::encoding::v1::keys::index_v2::IndexEntity {
                    kind: crate::index_v2::IndexElementKind::Edge,
                    id: crate::index_v2::IndexEntityId::new(edge_id),
                },
                &properties,
            ),
        )
        .await?;

        crate::search::remove_all_edge_indexes(
            crate::search::EdgePropertyIndexRemovalRequest::new_scoped(
                txn,
                crate::search::EdgeIndexTarget::new(from, to, edge_id),
                &properties,
                crate::search::EdgePropertyIndexCatalog::new(indexes),
                self.tenant_scope,
            ),
        )
        .await?;
        self.maintain_edge_vector_indexes_on_delete(
            txn,
            edge_id,
            &properties,
            indexes,
            index_context,
        )
        .await?;
        let text_indexes = self
            .maintain_edge_text_indexes_on_delete(txn, edge_id, &properties, indexes)
            .await?;
        crate::index_v2::secondary::maintain_entity(
            txn,
            self.tenant_scope,
            index_context.secondary(),
            crate::index_v2::IndexElementKind::Edge,
            edge_id,
            &properties,
            &[],
        )
        .await?;
        if let Some(label) = label.as_deref() {
            crate::search::remove_from_global_edge_label_index_scoped(
                txn,
                label,
                edge_id,
                self.tenant_scope,
            )
            .await?;
            let remaining_ids = remaining_pair.iter().collect::<Vec<_>>();
            if !Self::edge_ids_contain_label(txn, remaining_ids, label, self.tenant_scope).await? {
                crate::search::remove_from_edge_label_index_scoped(
                    txn,
                    from,
                    to,
                    label,
                    self.tenant_scope,
                )
                .await?;
            }
        }
        crate::search::remove_from_edge_pair_index_scoped(
            txn,
            from,
            to,
            edge_id,
            self.tenant_scope,
        )
        .await?;
        if remaining_pair.is_empty() {
            self.remove_adjacency(txn, from, to, ir::ExpandDirection::Out)
                .await?;
            self.remove_adjacency(txn, to, from, ir::ExpandDirection::In)
                .await?;
        }
        crate::search::delete_edge_endpoints_scoped(txn, edge_id, self.tenant_scope).await?;
        Ok(text_indexes)
    }

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

    async fn edge_ids_contain_label(
        txn: &DbTransaction,
        edge_ids: Vec<u64>,
        label: &str,
        tenant_scope: crate::encoding::keys::tenant::DataScope,
    ) -> Result<bool> {
        for edge_id in edge_ids {
            let properties =
                crate::search::get_edge_properties_by_id_scoped(txn, edge_id, tenant_scope).await?;
            if label_of(&properties) == Some(label) {
                return Ok(true);
            }
        }
        Ok(false)
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
        let indexes = crate::config::RuntimeIndexCatalog::default();

        let error = context
            .set_edge_property(
                &txn,
                99,
                Property::new("weight", DbPropertyValue::I64(1)),
                &indexes,
                &mut index_context,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("edge 99 does not exist"));

        context
            .remove_edge_property(
                &txn,
                99,
                &test_support::name("weight"),
                &indexes,
                &mut index_context,
            )
            .await
            .expect("removing a property from a missing edge is idempotent");
        context
            .delete_edge(&txn, 99, &indexes, &mut index_context)
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
                &crate::config::RuntimeIndexCatalog::default(),
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
