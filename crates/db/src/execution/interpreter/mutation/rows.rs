//! Common row mutations share native transaction and index-maintenance contracts.
use super::{super::ExecutionContext, contracts::EdgeMutationTarget};
use crate::{
    cypher,
    encoding::v2::values::property::{property_value::PropertyValue, Property},
    index_lifecycle::graph_mutation::{self, CanonicalPropertyRow},
};
use helix_planner::{ir, relational as r};
use std::collections::BTreeSet;

/// Deduplicated mutation targets and their construction reservation move
/// together into the write barrier. No insertion can bypass admission.
pub(in crate::execution::interpreter) struct DeletionTargets {
    entities: BTreeSet<r::Entity>,
    memory: crate::query_resources::Reservation,
}

impl DeletionTargets {
    pub(in crate::execution::interpreter) fn new(
        budget: &crate::query_resources::Budget,
    ) -> cypher::Result<Self> {
        Ok(Self {
            entities: BTreeSet::new(),
            memory: budget.reserve(0)?,
        })
    }

    pub(in crate::execution::interpreter) fn insert(
        &mut self,
        entity: r::Entity,
    ) -> cypher::Result<()> {
        if !self.entities.contains(&entity) {
            // A sparse B-tree node holds eleven 16-byte entities, twelve child
            // pointers and bookkeeping. This bound per unique entry also covers
            // transient node splits; duplicates consume no additional budget.
            self.memory
                .resize(self.entities.len().saturating_add(1).saturating_mul(512))?;
            self.entities.insert(entity);
        }
        Ok(())
    }
}

impl ExecutionContext<'_> {
    /// An evaluated map observes, rewrites and stages its target exactly once.
    /// The map contract preserves labels, so topology routes remain valid.
    pub(in crate::execution::interpreter) async fn row_edit_map(
        &mut self,
        entity: r::Entity,
        edit: graph_mutation::map::Edit,
    ) -> cypher::Result<()> {
        let mut scope = self.take_or_begin_write_scope().await?;
        let (entity, before) = match entity {
            r::Entity::Node(id) => (
                graph_mutation::GraphEntity::node(id),
                self.observe_node_rows(&scope.txn, [id])
                    .await?
                    .observed(id)
                    .ok_or_else(|| {
                        crate::HelixDbError::InvariantViolation(
                            "Active text graph source disagrees with its supplied before state"
                                .to_string(),
                        )
                    })?,
            ),
            r::Entity::Relationship(id) => (
                graph_mutation::GraphEntity::edge(id),
                self.observe_edge_rows(&scope.txn, [id])
                    .await?
                    .observed(id)
                    .require_properties(id)?,
            ),
        };
        match edit.apply(self.tenant_scope, entity, before, self.row_memory.as_ref())? {
            graph_mutation::PropertyEditOutcome::Unchanged(_) => {}
            graph_mutation::PropertyEditOutcome::Changed(transition) => {
                let encoded = transition
                    .after()
                    .expect("a map replacement has an after row")
                    .write_payload();
                scope
                    .index_context
                    .maintain_graph_indexes(
                        &scope.txn,
                        transition,
                        self.db
                            .config()
                            .db()
                            .search_index_backfill()
                            .active_text_mutation(),
                        self.row_memory.as_ref(),
                    )
                    .await?;
                scope.index_context.property_writes.stage(
                    &scope.txn,
                    self.tenant_scope,
                    entity,
                    Some(encoded),
                    self.row_memory.as_ref(),
                )?;
            }
        }
        self.finish_write_scope(scope).await?;
        Ok(())
    }

    pub(in crate::execution::interpreter) async fn row_create_node(
        &mut self,
        label: &str,
        mut properties: Vec<Property>,
    ) -> cypher::Result<u64> {
        // The caller owns the input vector. If push reallocates it, admit the
        // new capacity alongside that input, plus both metadata strings, before
        // either allocation. Canonical row retention takes over before return.
        let growth = if properties.len() == properties.capacity() {
            properties
                .capacity()
                .saturating_mul(2)
                .max(4)
                .saturating_mul(size_of::<Property>())
        } else {
            0
        };
        let _metadata_memory = self
            .row_memory
            .as_ref()
            .map(|budget| {
                budget.reserve(
                    growth
                        .saturating_add("$label".len())
                        .saturating_add(label.len()),
                )
            })
            .transpose()?;
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
        if label.is_empty() {
            return Err(r::QueryError::runtime(
                "SyntaxError",
                "NoRelationshipType",
                "a relationship requires a nonempty type",
            )
            .into());
        }
        let growth = if properties.len() == properties.capacity() {
            properties
                .capacity()
                .saturating_mul(2)
                .max(4)
                .saturating_mul(size_of::<Property>())
        } else {
            0
        };
        // The validated type and the stored label each own a string.
        let _metadata_memory = self
            .row_memory
            .as_ref()
            .map(|budget| {
                budget.reserve(
                    growth
                        .saturating_add("$label".len())
                        .saturating_add(label.len().saturating_mul(2)),
                )
            })
            .transpose()?;
        let label = ir::NonEmptyString::new(label.to_owned())
            .expect("relationship type is checked before admission");
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
            &CanonicalPropertyRow::new_with_budget(properties, self.row_memory.as_ref())?,
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
        targets: DeletionTargets,
        detach: bool,
    ) -> cypher::Result<()> {
        let DeletionTargets {
            entities,
            memory: _targets_memory,
        } = targets;
        let mut scope = self.take_or_begin_write_scope().await?;
        // Prior CREATE clauses may still own coalesced pair/adjacency changes.
        // Deletion observations must include them, just like native deletion.
        scope.index_context.flush_topology(&scope.txn).await?;
        let edge_count = entities
            .iter()
            .filter(|e| matches!(e, r::Entity::Relationship(_)))
            .count();
        let _edges_memory = self
            .row_budget()
            .reserve(edge_count.saturating_mul(size_of::<u64>()))?;
        let mut edges = Vec::with_capacity(edge_count);
        edges.extend(entities.iter().filter_map(|e| match e {
            r::Entity::Relationship(id) => Some(*id),
            r::Entity::Node(_) => None,
        }));
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

#[cfg(test)]
mod target_tests {
    use super::*;

    #[test]
    fn deletion_targets_admit_unique_entries_and_release_on_failure_or_drop() {
        let budget = crate::query_resources::Budget::new(512 * 2000);
        let mut targets = DeletionTargets::new(&budget).unwrap();
        let mut expected = BTreeSet::new();
        for i in 0..2000 {
            let id = (i * 997) % 2000;
            let entity = if i % 2 == 0 {
                r::Entity::Node(id)
            } else {
                r::Entity::Relationship(id)
            };
            targets.insert(entity).unwrap();
            expected.insert(entity);
        }
        assert_eq!(targets.entities, expected);
        assert_eq!(budget.available(), 0);
        for entity in expected {
            targets.insert(entity).unwrap();
        }
        for entity in [r::Entity::Node(u64::MAX), r::Entity::Relationship(u64::MAX)] {
            assert!(
                matches!(targets.insert(entity), Err(cypher::Error::Query(error)) if error.detail == "MemoryLimit")
            );
            assert!(!targets.entities.contains(&entity));
            assert_eq!(targets.entities.len(), 2000);
        }
        drop(targets);
        assert_eq!(budget.available(), 512 * 2000);
        let budget = crate::query_resources::Budget::new(511);
        let mut targets = DeletionTargets::new(&budget).unwrap();
        assert!(targets.insert(r::Entity::Node(1)).is_err());
        assert!(targets.entities.is_empty());
        assert_eq!(budget.available(), 511);
    }
}
