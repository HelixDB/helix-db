//! Common row mutations share native transaction and index-maintenance contracts.
use super::{super::ExecutionContext, contracts::EdgeMutationTarget};
use crate::{
    cypher,
    encoding::v2::values::property::{property_value::PropertyValue, Property},
    index_lifecycle::graph_mutation::CanonicalPropertyRow,
};
use helix_planner::{ir, relational as r};
use std::collections::BTreeSet;

impl ExecutionContext<'_> {
    pub(in crate::execution::interpreter) async fn row_create_node(
        &mut self,
        label: &str,
        mut properties: Vec<Property>,
    ) -> cypher::Result<u64> {
        let id = self.writer()?.node_ids().allocate().await?;
        properties.push(Property::string("$label", label));
        let mut scope = self.take_or_begin_write_scope().await?;
        self.store_node(&scope.txn, id, properties, &mut scope.index_context)
            .await?;
        self.finish_write_scope(scope).await?;
        Ok(id)
    }

    pub(in crate::execution::interpreter) async fn row_create_edge(
        &mut self,
        from: u64,
        to: u64,
        label: &str,
        mut properties: Vec<Property>,
    ) -> cypher::Result<u64> {
        let label = ir::NonEmptyString::new(label.to_owned()).ok_or_else(|| {
            r::QueryError::runtime(
                "SyntaxError",
                "NoRelationshipType",
                "a relationship requires a nonempty type",
            )
        })?;
        let id = self.writer()?.edge_ids().allocate().await?;
        properties.push(Property::string("$label", label.as_ref()));
        let mut scope = self.take_or_begin_write_scope().await?;
        let endpoints = self.observe_node_existence(&scope.txn, [from, to]).await?;
        endpoints.require(from)?;
        endpoints.require(to)?;
        self.store_edge(
            &scope.txn,
            EdgeMutationTarget::new(id, from, to),
            &label,
            &CanonicalPropertyRow::new(properties),
            &mut scope.index_context,
        )
        .await?;
        self.finish_write_scope(scope).await?;
        Ok(id)
    }

    /// `None` removes a property; native DSL null storage remains unchanged.
    pub(in crate::execution::interpreter) async fn row_edit_property(
        &mut self,
        entity: r::Entity,
        key: &str,
        value: Option<PropertyValue>,
    ) -> cypher::Result<()> {
        if key.starts_with('$') {
            return Err(r::QueryError::runtime(
                "UnsupportedFeature",
                "ReservedPropertyName",
                "internal metadata cannot be assigned",
            )
            .into());
        }
        let name = ir::NonEmptyString::new(key.to_owned()).ok_or_else(|| {
            r::QueryError::runtime(
                "UnsupportedFeature",
                "EmptyPropertyName",
                "storage requires nonempty property names",
            )
        })?;
        let mut scope = self.take_or_begin_write_scope().await?;
        match entity {
            r::Entity::Node(id) => {
                let observed = self.observe_node_rows(&scope.txn, [id]).await?.observed(id);
                match value {
                    Some(value) => {
                        self.set_node_property_observed(
                            &scope.txn,
                            id,
                            Property::new(key, value),
                            observed,
                            &mut scope.index_context,
                        )
                        .await?;
                    }
                    None => {
                        self.remove_node_property_observed(
                            &scope.txn,
                            id,
                            &name,
                            observed,
                            &mut scope.index_context,
                        )
                        .await?;
                    }
                }
            }
            r::Entity::Relationship(id) => {
                let observed = self.observe_edge_rows(&scope.txn, [id]).await?.observed(id);
                match value {
                    Some(value) => {
                        self.set_edge_property_observed(
                            &scope.txn,
                            id,
                            Property::new(key, value),
                            observed,
                            &mut scope.index_context,
                        )
                        .await?;
                    }
                    None => {
                        self.remove_edge_property_observed(
                            &scope.txn,
                            id,
                            &name,
                            observed,
                            &mut scope.index_context,
                        )
                        .await?;
                    }
                }
            }
        }
        self.finish_write_scope(scope).await?;
        Ok(())
    }

    /// Delete all explicitly selected relationships before checking node attachment.
    /// A failed check drops the statement's transaction, including prior clauses.
    pub(in crate::execution::interpreter) async fn row_delete_entities(
        &mut self,
        entities: BTreeSet<r::Entity>,
        detach: bool,
    ) -> cypher::Result<()> {
        let mut scope = self.take_or_begin_write_scope().await?;
        // Prior CREATE clauses may still own coalesced pair/adjacency changes.
        // Deletion observations must include them, just like native deletion.
        scope.index_context.flush_topology(&scope.txn).await?;
        let edges = entities
            .iter()
            .filter_map(|e| match e {
                r::Entity::Relationship(id) => Some(*id),
                r::Entity::Node(_) => None,
            })
            .collect::<Vec<_>>();
        let mut observed = self
            .observe_edge_deletions(&scope.txn, edges.iter().copied(), &scope.index_context)
            .await?;
        for id in edges {
            self.check_execution_deadline()?;
            self.delete_edge_observed(&scope.txn, id, observed.take(id)?, &mut scope.index_context)
                .await?;
        }
        scope.index_context.flush_topology(&scope.txn).await?;
        for entity in entities {
            let r::Entity::Node(id) = entity else {
                continue;
            };
            self.check_execution_deadline()?;
            if !detach
                && !self
                    .incident_edge_ids(&scope.txn, id, &scope.index_context)
                    .await?
                    .is_empty()
            {
                return Err(r::QueryError::runtime(
                    "ConstraintVerificationFailed",
                    "DeleteConnectedNode",
                    "cannot delete a node with relationships; use DETACH DELETE",
                )
                .into());
            }
            self.delete_node(&scope.txn, id, &mut scope.index_context)
                .await?;
        }
        self.finish_write_scope(scope).await?;
        Ok(())
    }
}
