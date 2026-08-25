//! Canonical in-memory graph transitions shared by foreground index writers.
//!
//! These types are deliberately runtime-only. Property rows continue to use
//! the canonical value codec; retaining the encoded bytes beside the decoded values
//! prevents each index family from rebuilding the same payload.

use std::sync::Arc;

use bytes::Bytes;

use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::IndexEntity;
use crate::encoding::v2::keys::{DataKey, DataKeyKind};
use crate::encoding::v2::values::property::{self, Property};
use crate::error::Result;

use super::{IndexElementKind, IndexEntityId};

/// Graph identity whose element kind is encoded by its enum variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum GraphEntity {
    /// One node property row.
    Node(IndexEntityId),
    /// One edge property row.
    Edge(IndexEntityId),
}

impl GraphEntity {
    /// Binds a node ID to the node property keyspace.
    pub(crate) const fn node(id: u64) -> Self {
        Self::Node(IndexEntityId::new(id))
    }

    /// Binds an edge ID to the edge property-by-ID keyspace.
    pub(crate) const fn edge(id: u64) -> Self {
        Self::Edge(IndexEntityId::new(id))
    }

    /// Returns the index-facing entity representation.
    pub(crate) const fn index_entity(self) -> IndexEntity {
        match self {
            Self::Node(id) => IndexEntity {
                kind: IndexElementKind::Node,
                id,
            },
            Self::Edge(id) => IndexEntity {
                kind: IndexElementKind::Edge,
                id,
            },
        }
    }

    fn property_key_kind(self) -> DataKeyKind<'static> {
        match self {
            Self::Node(id) => {
                DataKeyKind::NodeProperty(crate::encoding::v2::keys::NodePropertyKey::new(id.get()))
            }
            Self::Edge(id) => DataKeyKind::EdgePropertyById(
                crate::encoding::v2::keys::EdgePropertyByIdKey::new(id.get()),
            ),
        }
    }

    /// Returns the canonical scoped property-row key.
    pub(crate) fn property_key(self, scope: DataScope) -> Bytes {
        DataKey::Data {
            scope,
            kind: self.property_key_kind(),
        }
        .to_bytes()
    }
}

/// One decoded property row paired with its canonical encoding.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CanonicalPropertyRow {
    properties: Arc<Vec<Property>>,
    encoded: Bytes,
}

impl CanonicalPropertyRow {
    /// Encodes an owned property set exactly once through the canonical codec.
    pub(crate) fn new(properties: Vec<Property>) -> Self {
        let encoded = property::encode_properties(&properties);
        Self {
            properties: Arc::new(properties),
            encoded,
        }
    }

    /// Decodes one existing canonical property row exactly once.
    pub(crate) fn decode(encoded: Bytes) -> Result<Self> {
        let properties = property::decode_properties(&encoded)?;
        Ok(Self {
            properties: Arc::new(properties),
            encoded,
        })
    }

    /// Borrows the decoded properties.
    pub(crate) fn properties(&self) -> &[Property] {
        self.properties.as_slice()
    }

    /// Borrows the exact canonical bytes for storage and validation.
    pub(crate) const fn encoded(&self) -> &Bytes {
        &self.encoded
    }

    /// Returns the encoded row length without rebuilding it.
    pub(crate) fn encoded_len(&self) -> usize {
        self.encoded.len()
    }
}

/// Non-empty property names changed by one replacement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChangedProperties {
    first: Box<str>,
    rest: Box<[Box<str>]>,
}

impl ChangedProperties {
    /// Creates the single-property replacement used by executable mutations.
    pub(crate) fn one(name: impl Into<Box<str>>) -> Self {
        Self {
            first: name.into(),
            rest: Box::new([]),
        }
    }

    /// Returns whether the replacement changed `name`.
    pub(crate) fn contains(&self, name: &str) -> bool {
        self.first.as_ref() == name || self.rest.iter().any(|changed| changed.as_ref() == name)
    }

    /// Iterates every changed property name.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.first.as_ref()).chain(self.rest.iter().map(Box::as_ref))
    }
}

/// A requested edit whose variants exclude an absent set value.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PropertyEdit {
    /// Insert or replace one named property.
    Set(Property),
    /// Remove one named property when present.
    Remove(Box<str>),
}

impl PropertyEdit {
    /// Creates a set edit from the already-evaluated property value.
    pub(crate) const fn set(property: Property) -> Self {
        Self::Set(property)
    }

    /// Creates a remove edit for one non-empty planner-validated name.
    pub(crate) fn remove(name: impl Into<Box<str>>) -> Self {
        Self::Remove(name.into())
    }

    fn name(&self) -> &str {
        match self {
            Self::Set(property) => property.name.as_str(),
            Self::Remove(name) => name,
        }
    }
}

/// Closed authoritative graph property-row transition.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum GraphMutationTransition {
    /// The canonical row must not exist before this transaction.
    Create {
        /// Storage scope of the new row.
        scope: DataScope,
        /// Typed graph identity of the new row.
        entity: GraphEntity,
        /// Complete new row.
        after: CanonicalPropertyRow,
    },
    /// One existing row changes at least one property.
    Replace {
        /// Storage scope of the row.
        scope: DataScope,
        /// Typed graph identity of the row.
        entity: GraphEntity,
        /// Complete transaction-visible state before replacement.
        before: CanonicalPropertyRow,
        /// Complete state after replacement.
        after: CanonicalPropertyRow,
        /// Non-empty set of changed property names.
        changed: ChangedProperties,
    },
    /// One existing row is removed without a replacement value.
    Delete {
        /// Storage scope of the removed row.
        scope: DataScope,
        /// Typed graph identity of the removed row.
        entity: GraphEntity,
        /// Complete transaction-visible state before deletion.
        before: CanonicalPropertyRow,
    },
}

/// Result of applying a property edit to one canonical row.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PropertyEditOutcome {
    /// The requested value was already authoritative; no write is needed.
    Unchanged(CanonicalPropertyRow),
    /// The edit produced a non-empty replacement transition.
    Changed(GraphMutationTransition),
}

impl GraphMutationTransition {
    /// Creates one typed row insertion.
    pub(crate) const fn create(
        scope: DataScope,
        entity: GraphEntity,
        after: CanonicalPropertyRow,
    ) -> Self {
        Self::Create {
            scope,
            entity,
            after,
        }
    }

    /// Applies one edit and returns a replacement only when bytes must change.
    pub(crate) fn edit(
        scope: DataScope,
        entity: GraphEntity,
        before: CanonicalPropertyRow,
        edit: PropertyEdit,
    ) -> PropertyEditOutcome {
        let name = edit.name();
        let position = before
            .properties()
            .iter()
            .position(|property| property.name == name);
        match (&edit, position) {
            (PropertyEdit::Set(property), Some(position))
                if before.properties()[position].same_v1_representation(property) =>
            {
                return PropertyEditOutcome::Unchanged(before);
            }
            (PropertyEdit::Remove(_), None) => {
                return PropertyEditOutcome::Unchanged(before);
            }
            (PropertyEdit::Set(_), _) | (PropertyEdit::Remove(_), Some(_)) => {}
        }

        let changed = ChangedProperties::one(name);
        let mut properties = before.properties().to_vec();
        match (edit, position) {
            (PropertyEdit::Set(property), Some(position)) => properties[position] = property,
            (PropertyEdit::Set(property), None) => properties.push(property),
            (PropertyEdit::Remove(_), Some(position)) => {
                properties.remove(position);
            }
            (PropertyEdit::Remove(_), None) => {
                unreachable!("an absent remove returns before constructing a transition")
            }
        }
        PropertyEditOutcome::Changed(Self::Replace {
            scope,
            entity,
            before,
            after: CanonicalPropertyRow::new(properties),
            changed,
        })
    }

    /// Creates one typed row deletion.
    pub(crate) const fn delete(
        scope: DataScope,
        entity: GraphEntity,
        before: CanonicalPropertyRow,
    ) -> Self {
        Self::Delete {
            scope,
            entity,
            before,
        }
    }

    /// Returns the row scope.
    pub(crate) const fn scope(&self) -> DataScope {
        match self {
            Self::Create { scope, .. }
            | Self::Replace { scope, .. }
            | Self::Delete { scope, .. } => *scope,
        }
    }

    /// Returns the typed graph entity.
    pub(crate) const fn entity(&self) -> GraphEntity {
        match self {
            Self::Create { entity, .. }
            | Self::Replace { entity, .. }
            | Self::Delete { entity, .. } => *entity,
        }
    }

    /// Returns the complete before row when one must exist.
    pub(crate) const fn before(&self) -> Option<&CanonicalPropertyRow> {
        match self {
            Self::Create { .. } => None,
            Self::Replace { before, .. } | Self::Delete { before, .. } => Some(before),
        }
    }

    /// Returns the complete after row when one remains.
    pub(crate) const fn after(&self) -> Option<&CanonicalPropertyRow> {
        match self {
            Self::Create { after, .. } | Self::Replace { after, .. } => Some(after),
            Self::Delete { .. } => None,
        }
    }

    /// Returns replacement names, or `None` for whole-row create/delete work.
    pub(crate) const fn changed(&self) -> Option<&ChangedProperties> {
        match self {
            Self::Replace { changed, .. } => Some(changed),
            Self::Create { .. } | Self::Delete { .. } => None,
        }
    }

    /// Returns the canonical scoped graph-row key.
    pub(crate) fn graph_key(&self) -> Bytes {
        self.entity().property_key(self.scope())
    }

    /// Consumes the transition into its coalescing components.
    pub(crate) fn into_states(
        self,
    ) -> (
        DataScope,
        GraphEntity,
        Option<CanonicalPropertyRow>,
        Option<CanonicalPropertyRow>,
    ) {
        match self {
            Self::Create {
                scope,
                entity,
                after,
            } => (scope, entity, None, Some(after)),
            Self::Replace {
                scope,
                entity,
                before,
                after,
                ..
            } => (scope, entity, Some(before), Some(after)),
            Self::Delete {
                scope,
                entity,
                before,
            } => (scope, entity, Some(before), None),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::encoding::v2::values::property::property_value::PropertyValue;

    fn row() -> CanonicalPropertyRow {
        CanonicalPropertyRow::new(vec![
            Property::string("$label", "User"),
            Property::string("name", "Ada"),
        ])
    }

    #[test]
    fn canonical_row_reuses_exact_v1_bytes() {
        let original = row();
        let decoded = CanonicalPropertyRow::decode(original.encoded().clone()).unwrap();

        assert_eq!(decoded, original);
        assert_eq!(decoded.encoded_len(), original.encoded_len());
    }

    #[test]
    fn property_edits_exclude_empty_replacements() {
        let scope = DataScope::LegacyUnscoped;
        let entity = GraphEntity::node(7);
        let unchanged = GraphMutationTransition::edit(
            scope,
            entity,
            row(),
            PropertyEdit::set(Property::string("name", "Ada")),
        );
        assert!(matches!(unchanged, PropertyEditOutcome::Unchanged(_)));

        let removed =
            GraphMutationTransition::edit(scope, entity, row(), PropertyEdit::remove("missing"));
        assert!(matches!(removed, PropertyEditOutcome::Unchanged(_)));

        let PropertyEditOutcome::Changed(changed) = GraphMutationTransition::edit(
            scope,
            entity,
            row(),
            PropertyEdit::set(Property::string("name", "Grace")),
        ) else {
            panic!("different values produce a replacement");
        };
        assert!(changed.changed().unwrap().contains("name"));
        assert_eq!(
            changed.changed().unwrap().iter().collect::<Vec<_>>(),
            ["name"]
        );
        assert_eq!(
            changed
                .after()
                .unwrap()
                .properties()
                .iter()
                .find(|property| property.name == "name")
                .and_then(|property| property.value.as_str()),
            Some("Grace")
        );
        let (changed_scope, changed_entity, before, after) = changed.into_states();
        assert_eq!(changed_scope, scope);
        assert_eq!(changed_entity, entity);
        assert_eq!(
            before.as_ref().map(CanonicalPropertyRow::properties),
            Some(row().properties())
        );
        assert_eq!(
            after
                .as_ref()
                .and_then(|row| row
                    .properties()
                    .iter()
                    .find(|property| property.name == "name"))
                .and_then(|property| property.value.as_str()),
            Some("Grace")
        );

        let PropertyEditOutcome::Changed(added) = GraphMutationTransition::edit(
            scope,
            entity,
            row(),
            PropertyEdit::set(Property::string("title", "Engineer")),
        ) else {
            panic!("a new property produces a replacement");
        };
        assert_eq!(
            added
                .after()
                .expect("an added property has an after row")
                .properties()
                .last(),
            Some(&Property::string("title", "Engineer"))
        );

        let PropertyEditOutcome::Changed(removed) =
            GraphMutationTransition::edit(scope, entity, row(), PropertyEdit::remove("name"))
        else {
            panic!("an existing property produces a replacement");
        };
        assert_eq!(
            removed.after().unwrap().properties(),
            [Property::string("$label", "User")]
        );
    }

    #[test]
    fn representation_distinct_floats_are_routed_as_index_changes() {
        let scope = DataScope::LegacyUnscoped;
        let entity = GraphEntity::node(7);
        let before = CanonicalPropertyRow::new(vec![Property::f64("score", -0.0)]);
        let before_bytes = before.encoded().clone();
        let PropertyEditOutcome::Changed(transition) = GraphMutationTransition::edit(
            scope,
            entity,
            before,
            PropertyEdit::set(Property::f64("score", 0.0)),
        ) else {
            panic!("signed-zero replacement must remain observable");
        };
        let after = transition.after().expect("replacement has an after row");
        assert_ne!(after.encoded(), &before_bytes);
        assert_eq!(
            after.encoded(),
            &property::encode_properties(&[Property::f64("score", 0.0)])
        );
        assert!(
            transition
                .changed()
                .expect("replacement has changed properties")
                .contains("score"),
            "every index family must observe the representation-distinct property"
        );
        let decoded = CanonicalPropertyRow::decode(after.encoded().clone()).unwrap();
        let PropertyValue::F64(value) = decoded.properties()[0].value else {
            panic!("score remains an f64");
        };
        assert_eq!(value.to_bits(), 0.0_f64.to_bits());

        let nan_bits = 0x7ff8_0000_0000_0042;
        let nan = f64::from_bits(nan_bits);
        let unchanged = GraphMutationTransition::edit(
            scope,
            entity,
            CanonicalPropertyRow::new(vec![Property::f64("score", nan)]),
            PropertyEdit::set(Property::f64("score", nan)),
        );
        let PropertyEditOutcome::Unchanged(unchanged) = unchanged else {
            panic!("bit-identical NaN must not create index or graph writes");
        };
        assert_eq!(
            unchanged.encoded(),
            &property::encode_properties(&[Property::f64("score", nan)])
        );

        let changed_nan = GraphMutationTransition::edit(
            scope,
            entity,
            unchanged,
            PropertyEdit::set(Property::f64(
                "score",
                f64::from_bits(nan_bits.saturating_add(1)),
            )),
        );
        assert!(matches!(changed_nan, PropertyEditOutcome::Changed(_)));
    }

    #[test]
    fn nested_float_representations_participate_in_no_op_detection() {
        let nested = |value| {
            Property::new(
                "metadata",
                PropertyValue::Object(BTreeMap::from([(
                    "values".to_string(),
                    PropertyValue::Array(vec![
                        PropertyValue::F64(value),
                        PropertyValue::F32Array(vec![value as f32]),
                    ]),
                )])),
            )
        };
        let before = CanonicalPropertyRow::new(vec![nested(-0.0)]);
        let before_bytes = before.encoded().clone();
        let PropertyEditOutcome::Changed(transition) = GraphMutationTransition::edit(
            DataScope::LegacyUnscoped,
            GraphEntity::node(9),
            before,
            PropertyEdit::set(nested(0.0)),
        ) else {
            panic!("nested signed-zero replacement must change the canonical row");
        };
        let after = transition.after().expect("replacement has an after row");
        assert_ne!(after.encoded(), &before_bytes);
        assert_eq!(
            after.encoded(),
            &property::encode_properties(&[nested(0.0)])
        );
    }

    #[test]
    fn transition_variants_preserve_typed_keys_and_presence() {
        let scope = DataScope::LegacyUnscoped;
        let node = GraphEntity::node(3);
        let edge = GraphEntity::edge(4);
        let create = GraphMutationTransition::create(scope, node, row());
        let delete = GraphMutationTransition::delete(scope, edge, row());

        assert!(matches!(
            node.index_entity(),
            IndexEntity {
                kind: IndexElementKind::Node,
                id,
            } if id == IndexEntityId::new(3)
        ));
        assert!(matches!(
            edge.index_entity(),
            IndexEntity {
                kind: IndexElementKind::Edge,
                id,
            } if id == IndexEntityId::new(4)
        ));
        assert_eq!(create.scope(), scope);
        assert_eq!(create.entity(), node);
        assert!(create.changed().is_none());
        assert!(create.before().is_none());
        assert!(create.after().is_some());
        assert_eq!(delete.scope(), scope);
        assert_eq!(delete.entity(), edge);
        assert!(delete.changed().is_none());
        assert!(delete.before().is_some());
        assert!(delete.after().is_none());
        assert!(matches!(
            DataKey::parse_from_slice(scope, &create.graph_key()).unwrap(),
            DataKey::Data {
                kind: DataKeyKind::NodeProperty(_),
                ..
            }
        ));
        assert!(matches!(
            DataKey::parse_from_slice(scope, &delete.graph_key()).unwrap(),
            DataKey::Data {
                kind: DataKeyKind::EdgePropertyById(_),
                ..
            }
        ));

        let (create_scope, create_entity, before, after) = create.into_states();
        assert_eq!(create_scope, scope);
        assert_eq!(create_entity, node);
        assert!(before.is_none());
        assert!(after.is_some());

        let (delete_scope, delete_entity, before, after) = delete.into_states();
        assert_eq!(delete_scope, scope);
        assert_eq!(delete_entity, edge);
        assert!(before.is_some());
        assert!(after.is_none());
    }
}
