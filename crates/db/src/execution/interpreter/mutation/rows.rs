//! Common row mutations share native transaction and index-maintenance contracts.
use super::{super::ExecutionContext, contracts::EdgeMutationTarget};
use crate::{
    cypher,
    encoding::v2::values::property::Property,
    index_lifecycle::graph_mutation::{self, CanonicalPropertyRow},
};
use helix_planner::relational::{category, detail};
use helix_planner::{ir, relational as r};
use std::collections::{BTreeMap, BTreeSet};

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

/// What one update asks of its target.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::execution::interpreter) enum EditKind {
    /// Only removals, which have nothing to do on a target whose row is gone.
    Removal,
    /// Sets keys, leaving others alone.
    Extension,
    /// Replaces every user property.
    Replacement,
}

impl EditKind {
    const fn mode(self) -> graph_mutation::map::Mode {
        match self {
            Self::Removal | Self::Extension => graph_mutation::map::Mode::Extend,
            Self::Replacement => graph_mutation::map::Mode::ReplaceUserProperties,
        }
    }
}

struct EntityEdit {
    edit: graph_mutation::map::Edit,
    /// Whether every folded change was a removal.
    skips_missing_row: bool,
}

/// Evaluated edits folded per entity. Applying them costs one read and one
/// write however many keys reached each entity.
///
/// Folding must match applying each update alone. Callers apply pending edits
/// first when a read needs them ([`Self::changes`]) or a change cannot join
/// them ([`Self::blocks`]). Transitions keep first-edit order, and an entity
/// only extends the most recent edit, because a unique index claims values in
/// transition order: a value one entity releases can only be claimed after.
pub(in crate::execution::interpreter) struct EntityEdits {
    entities: BTreeMap<r::Entity, EntityEdit>,
    order: Vec<r::Entity>,
    structure: crate::query_resources::Reservation,
    changes: crate::query_resources::Reservation,
}

impl EntityEdits {
    pub(in crate::execution::interpreter) fn new(
        budget: &crate::query_resources::Budget,
    ) -> cypher::Result<Self> {
        Ok(Self {
            entities: BTreeMap::new(),
            order: Vec::new(),
            structure: budget.reserve(0)?,
            changes: budget.reserve(0)?,
        })
    }

    /// Whether a read of `requirement` on `entity` needs a pending edit applied.
    pub(in crate::execution::interpreter) fn changes(
        &self,
        entity: r::Entity,
        requirement: r::PropertyRequirement<'_>,
    ) -> bool {
        self.entities
            .get(&entity)
            .is_some_and(|pending| pending.edit.changes(requirement))
    }

    /// Whether a change to `names` must wait for this entity's pending edit.
    pub(in crate::execution::interpreter) fn blocks<'a>(
        &self,
        entity: r::Entity,
        kind: EditKind,
        names: impl Iterator<Item = &'a str>,
    ) -> bool {
        self.entities.get(&entity).is_some_and(|pending| {
            kind == EditKind::Replacement
                || self.order.last() != Some(&entity)
                || !pending.edit.folds(names)
        })
    }

    /// Bytes pending edits hold.
    pub(in crate::execution::interpreter) const fn pending_bytes(&self) -> usize {
        self.changes.bytes()
    }

    /// The edit that collects `entity`'s next change, keeping `memory` until
    /// it is applied. The caller has applied anything [`Self::blocks`] names.
    pub(in crate::execution::interpreter) fn edit(
        &mut self,
        entity: r::Entity,
        kind: EditKind,
        memory: crate::query_resources::Reservation,
    ) -> cypher::Result<&mut graph_mutation::map::Edit> {
        self.changes.absorb(memory);
        if self.order.last() != Some(&entity) {
            assert!(
                !self.entities.contains_key(&entity),
                "a pending edit only extends the most recent entity"
            );
            let count = self.entities.len().saturating_add(1);
            // Doubling growth briefly holds the old and the new vector.
            self.structure.resize(
                r::allocation::btree_bytes::<r::Entity, EntityEdit>(count)
                    .saturating_add(count.saturating_mul(3 * size_of::<r::Entity>())),
            )?;
            self.order.push(entity);
        }
        let pending = self.entities.entry(entity).or_insert(EntityEdit {
            edit: graph_mutation::map::Edit::new(kind.mode()),
            skips_missing_row: true,
        });
        pending.skips_missing_row &= kind == EditKind::Removal;
        Ok(&mut pending.edit)
    }
}

#[cfg(test)]
impl EntityEdits {
    /// Installs one prepared edit for tests of the apply contract.
    pub(in crate::execution::interpreter) fn prepared(
        budget: &crate::query_resources::Budget,
        entity: r::Entity,
        edit: graph_mutation::map::Edit,
        skips_missing_row: bool,
    ) -> cypher::Result<Self> {
        let mut edits = Self::new(budget)?;
        edits.entities.insert(
            entity,
            EntityEdit {
                edit,
                skips_missing_row,
            },
        );
        edits.order.push(entity);
        Ok(edits)
    }
}

impl ExecutionContext<'_> {
    /// Apply every pending edit with one observation per entity kind and one
    /// transition per entity. Edits preserve labels, so topology routes stay
    /// valid. A rejected edit drops the statement's transaction.
    pub(in crate::execution::interpreter) async fn row_apply_edits(
        &mut self,
        edits: &mut EntityEdits,
    ) -> cypher::Result<()> {
        if edits.entities.is_empty() {
            return Ok(());
        }
        let mut scope = self.take_or_begin_write_scope().await?;
        let nodes = self
            .observe_node_rows(
                &scope.txn,
                edits.entities.keys().filter_map(|entity| match entity {
                    r::Entity::Node(id) => Some(*id),
                    r::Entity::Relationship(_) => None,
                }),
            )
            .await?;
        let relationships = self
            .observe_edge_rows(
                &scope.txn,
                edits.entities.keys().filter_map(|entity| match entity {
                    r::Entity::Relationship(id) => Some(*id),
                    r::Entity::Node(_) => None,
                }),
            )
            .await?;
        for entity in std::mem::take(&mut edits.order) {
            self.check_execution_deadline()?;
            let EntityEdit {
                edit,
                skips_missing_row,
            } = edits
                .entities
                .remove(&entity)
                .expect("every ordered entity has one pending edit");
            let (entity, before) = match entity {
                r::Entity::Node(id) => (
                    graph_mutation::GraphEntity::node(id),
                    nodes.observed(id).ok_or_else(|| {
                        crate::HelixDbError::InvariantViolation(
                            "Active text graph source disagrees with its supplied before state"
                                .to_string(),
                        )
                    }),
                ),
                r::Entity::Relationship(id) => (
                    graph_mutation::GraphEntity::edge(id),
                    relationships.observed(id).require_properties(id),
                ),
            };
            let before = match before {
                Ok(before) => before,
                Err(_) if skips_missing_row => continue,
                Err(error) => return Err(error.into()),
            };
            let graph_mutation::PropertyEditOutcome::Changed(transition) =
                edit.apply(self.tenant_scope, entity, before, self.row_memory.as_ref())?
            else {
                continue;
            };
            let encoded = transition
                .after()
                .expect("a map replacement has an after row")
                .write_payload();
            scope
                .index_context
                .maintain_graph_indexes(transition, self.row_memory.as_ref())?;
            scope.index_context.property_writes.stage(
                &scope.txn,
                self.tenant_scope,
                entity,
                Some(encoded),
                self.row_memory.as_ref(),
            )?;
        }
        edits.structure.shrink_to(0);
        edits.changes.shrink_to(0);
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

    /// Stages one relationship. The caller proves both endpoints exist with
    /// [`Self::row_require_nodes`] before the request commits.
    pub(in crate::execution::interpreter) async fn row_create_edge(
        &mut self,
        from: u64,
        to: u64,
        label: &str,
        mut properties: Vec<Property>,
    ) -> cypher::Result<u64> {
        if label.is_empty() {
            return Err(r::QueryError::runtime(
                category::SYNTAX_ERROR,
                detail::NO_RELATIONSHIP_TYPE,
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

    /// Reject the first of `ids` absent from the transaction's view, with one
    /// read however many relationships share them.
    pub(in crate::execution::interpreter) async fn row_require_nodes(
        &mut self,
        ids: &[u64],
    ) -> cypher::Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let scope = self.take_or_begin_write_scope().await?;
        let existing = self
            .observe_node_existence(&scope.txn, ids.iter().copied())
            .await?;
        ids.iter().try_for_each(|id| existing.require(*id))?;
        self.finish_write_scope(scope).await?;
        Ok(())
    }

    /// Delete the selected entities. Without `detach`, a node may only have
    /// selected relationships. A refusal drops the statement's transaction,
    /// including prior clauses and any batch already staged.
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
        let edge_count = entities
            .iter()
            .filter(|e| matches!(e, r::Entity::Relationship(_)))
            .count();
        let _ids_memory = self
            .row_budget()
            .reserve(entities.len().saturating_mul(size_of::<u64>()))?;
        let mut edges = Vec::with_capacity(edge_count);
        let mut nodes = Vec::with_capacity(entities.len() - edge_count);
        for entity in entities {
            match entity {
                r::Entity::Relationship(id) => edges.push(id),
                r::Entity::Node(id) => nodes.push(id),
            }
        }
        self.delete_elements(
            &scope.txn,
            &edges,
            &nodes,
            |members: &super::edge::PairMembers| {
                if detach
                    || members
                        .edge_ids()
                        .all(|id| edges.binary_search(&id).is_ok())
                {
                    return Ok(());
                }
                Err(cypher::Error::from(r::QueryError::runtime(
                    category::CONSTRAINT_VERIFICATION_FAILED,
                    detail::DELETE_CONNECTED_NODE,
                    "cannot delete a node with relationships; use DETACH DELETE",
                )))
            },
            &mut scope.index_context,
        )
        .await?;
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
                matches!(targets.insert(entity), Err(cypher::Error::Query(error)) if error.detail == "memory_limit")
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
