//! Node index footprint of request-transaction writes.
//!
//! Every node create, property replacement, and delete reaches the node's
//! `$label` bitmap and secondary indexes only through a
//! [`GraphMutationTransition`] passed to
//! [`MutationIndexContext::maintain_graph_indexes`](super::MutationIndexContext::maintain_graph_indexes),
//! so recording each transition there yields every node index state a write
//! may have changed without reading a record. A future write path that
//! changes node indexes without such a transition must record its own
//! footprint.

use std::collections::{BTreeMap, BTreeSet};

use crate::index_lifecycle::graph_mutation::{GraphEntity, GraphMutationTransition};

/// Node index state that one label's writes changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::execution::interpreter) enum LabelWrites {
    /// A node of the label was created, deleted, or relabelled: the label's
    /// `$label` bitmap and every index of the label may have changed.
    Nodes,
    /// Existing nodes of the label changed these properties, so only indexes
    /// on them may have changed.
    Properties(BTreeSet<Box<str>>),
}

/// Labels whose node indexes writes changed, with what changed in each.
///
/// The footprint is bounded by the schema, labels times properties, not by
/// the number of writes. [`LabelWrites::Nodes`] absorbs every property of its
/// label.
#[derive(Debug, Default)]
pub(in crate::execution::interpreter) struct NodeIndexWrites(BTreeMap<Box<str>, LabelWrites>);

impl NodeIndexWrites {
    /// What writes changed in `label`'s node indexes, if anything.
    pub(in crate::execution::interpreter) fn label(&self, label: &str) -> Option<&LabelWrites> {
        self.0.get(label)
    }

    /// Record the node index state `transition` may change.
    ///
    /// This mirrors the label and property scoping of secondary index routing
    /// (`MutationRouteCatalog::targets_for`) and of `$label` bitmap
    /// maintenance:
    ///
    /// * a create or delete changes its label's bitmap and every index;
    /// * a replacement that changes `$label` does so for the old and new
    ///   labels;
    /// * any other replacement changes only the indexes of the changed
    ///   properties of its label;
    /// * edge rows and label-less node rows enter no node index.
    pub(in crate::execution::interpreter) fn record(
        &mut self,
        transition: &GraphMutationTransition,
    ) {
        let GraphEntity::Node(_) = transition.entity() else {
            return;
        };
        let before = transition
            .before()
            .and_then(|row| super::contracts::label_of(row.properties()));
        let after = transition
            .after()
            .and_then(|row| super::contracts::label_of(row.properties()));
        match (transition.changed(), before == after) {
            (Some(changed), true) => after.into_iter().for_each(|label| {
                match self.0.get_mut(label) {
                    Some(LabelWrites::Nodes) => {}
                    Some(LabelWrites::Properties(properties)) => {
                        changed.iter().for_each(|property| {
                            // Allocate only for a property not seen yet.
                            if !properties.contains(property) {
                                properties.insert(property.into());
                            }
                        });
                    }
                    None => {
                        self.0.insert(
                            label.into(),
                            LabelWrites::Properties(changed.iter().map(Box::from).collect()),
                        );
                    }
                }
            }),
            (Some(_), false) | (None, _) => {
                before
                    .into_iter()
                    .chain(after)
                    .for_each(|label| match self.0.get_mut(label) {
                        Some(writes) => *writes = LabelWrites::Nodes,
                        None => {
                            self.0.insert(label.into(), LabelWrites::Nodes);
                        }
                    });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::v2::keys::scope::DataScope;
    use crate::encoding::v2::values::property::Property;
    use crate::index_lifecycle::graph_mutation::{
        CanonicalPropertyRow, PropertyEdit, PropertyEditOutcome,
    };

    const NODE: GraphEntity = GraphEntity::node(1);
    const EDGE: GraphEntity = GraphEntity::edge(1);

    fn row(label: Option<&str>) -> CanonicalPropertyRow {
        CanonicalPropertyRow::new(
            label
                .map(|label| Property::string("$label", label))
                .into_iter()
                .chain([Property::string("title", "t")])
                .collect(),
        )
    }

    fn create(entity: GraphEntity, label: Option<&str>) -> GraphMutationTransition {
        GraphMutationTransition::create(DataScope::LegacyUnscoped, entity, row(label))
    }

    fn delete(entity: GraphEntity, label: Option<&str>) -> GraphMutationTransition {
        GraphMutationTransition::delete(DataScope::LegacyUnscoped, entity, row(label))
    }

    fn set(
        entity: GraphEntity,
        label: Option<&str>,
        property: &str,
        value: &str,
    ) -> GraphMutationTransition {
        let PropertyEditOutcome::Changed(transition) = GraphMutationTransition::edit(
            DataScope::LegacyUnscoped,
            entity,
            row(label),
            PropertyEdit::set(Property::string(property, value)),
        ) else {
            panic!("the edit changes the row");
        };
        transition
    }

    fn properties(names: &[&str]) -> LabelWrites {
        LabelWrites::Properties(names.iter().map(|name| Box::from(*name)).collect())
    }

    fn recorded(transitions: &[GraphMutationTransition]) -> NodeIndexWrites {
        let mut writes = NodeIndexWrites::default();
        transitions
            .iter()
            .for_each(|transition| writes.record(transition));
        writes
    }

    #[test]
    fn creates_and_deletes_change_every_index_of_their_label() {
        for transition in [create(NODE, Some("Item")), delete(NODE, Some("Item"))] {
            let writes = recorded(&[transition]);
            assert_eq!(writes.label("Item"), Some(&LabelWrites::Nodes));
            assert_eq!(writes.label("Group"), None);
        }
        // Repeated node writes to one label keep one entry.
        let writes = recorded(&[create(NODE, Some("Item")), delete(NODE, Some("Item"))]);
        assert_eq!(writes.0.len(), 1);
    }

    #[test]
    fn replacements_change_only_their_properties() {
        let mut writes = recorded(&[set(NODE, Some("Item"), "title", "u")]);
        assert_eq!(writes.label("Item"), Some(&properties(&["title"])));
        writes.record(&set(NODE, Some("Item"), "title", "v"));
        assert_eq!(writes.label("Item"), Some(&properties(&["title"])));
        writes.record(&set(NODE, Some("Item"), "kind", "B"));
        assert_eq!(writes.label("Item"), Some(&properties(&["kind", "title"])));
        assert_eq!(writes.label("Group"), None);
    }

    #[test]
    fn node_writes_absorb_property_writes() {
        let writes = recorded(&[
            set(NODE, Some("Item"), "title", "u"),
            create(NODE, Some("Item")),
            set(NODE, Some("Item"), "kind", "B"),
        ]);
        assert_eq!(writes.label("Item"), Some(&LabelWrites::Nodes));
    }

    #[test]
    fn relabels_change_both_labels() {
        let writes = recorded(&[
            set(NODE, Some("Group"), "title", "u"),
            set(NODE, Some("Item"), "$label", "Group"),
        ]);
        assert_eq!(writes.label("Item"), Some(&LabelWrites::Nodes));
        assert_eq!(writes.label("Group"), Some(&LabelWrites::Nodes));
        // A node gaining its first label changes only that label.
        let writes = recorded(&[set(NODE, None, "$label", "Item")]);
        assert_eq!(writes.label("Item"), Some(&LabelWrites::Nodes));
        assert_eq!(writes.0.len(), 1);
    }

    #[test]
    fn edges_and_label_less_nodes_change_no_node_index() {
        let writes = recorded(&[
            create(EDGE, Some("LINK")),
            set(EDGE, Some("LINK"), "kind", "B"),
            delete(EDGE, Some("LINK")),
            create(NODE, None),
            set(NODE, None, "kind", "B"),
            delete(NODE, None),
        ]);
        assert!(writes.0.is_empty());
    }
}
