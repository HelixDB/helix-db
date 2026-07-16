//! Configured static text-index maintenance for graph mutations.
//!
//! The mutation scope supplies a [`crate::index_v2::ConfiguredIndexCatalog`],
//! so definitions projected from canonical V2 `Active` records cannot enter
//! this retained physical format. Lifecycle-owned text effects are staged only
//! by [`crate::index_v2::text::active_request`].

use slatedb::DbTransaction;

use super::super::*;
use crate::config::TextElementType;

mod apply;
mod change;
mod document;
mod outcome;

pub(in crate::execution::interpreter) use self::outcome::TextIndexMaintenanceOutcome;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn maintain_node_text_indexes_on_create(
        &self,
        txn: &DbTransaction,
        node_id: u64,
        properties: &[Property],
        indexes: &crate::config::RuntimeIndexCatalog,
    ) -> Result<TextIndexMaintenanceOutcome> {
        self.maintain_text_indexes(
            txn,
            TextIndexMutation::Create {
                element_type: TextElementType::Node,
                entity_id: node_id,
                properties,
            },
            indexes,
        )
        .await
    }

    pub(in crate::execution::interpreter) async fn maintain_node_text_indexes_on_update(
        &self,
        txn: &DbTransaction,
        node_id: u64,
        update: TextPropertyUpdate<'_>,
        indexes: &crate::config::RuntimeIndexCatalog,
    ) -> Result<TextIndexMaintenanceOutcome> {
        self.maintain_text_indexes(
            txn,
            TextIndexMutation::Update {
                element_type: TextElementType::Node,
                entity_id: node_id,
                old_properties: update.old_properties,
                new_properties: update.new_properties,
                changed_property: update.changed_property,
            },
            indexes,
        )
        .await
    }

    pub(in crate::execution::interpreter) async fn maintain_node_text_indexes_on_delete(
        &self,
        txn: &DbTransaction,
        node_id: u64,
        properties: &[Property],
        indexes: &crate::config::RuntimeIndexCatalog,
    ) -> Result<TextIndexMaintenanceOutcome> {
        self.maintain_text_indexes(
            txn,
            TextIndexMutation::Delete {
                element_type: TextElementType::Node,
                entity_id: node_id,
                properties,
            },
            indexes,
        )
        .await
    }

    pub(in crate::execution::interpreter) async fn maintain_edge_text_indexes_on_create(
        &self,
        txn: &DbTransaction,
        edge_id: u64,
        properties: &[Property],
        indexes: &crate::config::RuntimeIndexCatalog,
    ) -> Result<TextIndexMaintenanceOutcome> {
        self.maintain_text_indexes(
            txn,
            TextIndexMutation::Create {
                element_type: TextElementType::Edge,
                entity_id: edge_id,
                properties,
            },
            indexes,
        )
        .await
    }

    pub(in crate::execution::interpreter) async fn maintain_edge_text_indexes_on_update(
        &self,
        txn: &DbTransaction,
        edge_id: u64,
        update: TextPropertyUpdate<'_>,
        indexes: &crate::config::RuntimeIndexCatalog,
    ) -> Result<TextIndexMaintenanceOutcome> {
        self.maintain_text_indexes(
            txn,
            TextIndexMutation::Update {
                element_type: TextElementType::Edge,
                entity_id: edge_id,
                old_properties: update.old_properties,
                new_properties: update.new_properties,
                changed_property: update.changed_property,
            },
            indexes,
        )
        .await
    }

    pub(in crate::execution::interpreter) async fn maintain_edge_text_indexes_on_delete(
        &self,
        txn: &DbTransaction,
        edge_id: u64,
        properties: &[Property],
        indexes: &crate::config::RuntimeIndexCatalog,
    ) -> Result<TextIndexMaintenanceOutcome> {
        self.maintain_text_indexes(
            txn,
            TextIndexMutation::Delete {
                element_type: TextElementType::Edge,
                entity_id: edge_id,
                properties,
            },
            indexes,
        )
        .await
    }

    async fn maintain_text_indexes(
        &self,
        txn: &DbTransaction,
        mutation: TextIndexMutation<'_>,
        indexes: &crate::config::RuntimeIndexCatalog,
    ) -> Result<TextIndexMaintenanceOutcome> {
        let element_type = mutation.element_type();
        let old_properties = mutation.old_properties();
        let new_properties = mutation.new_properties();
        let changed_property = mutation.changed_property();
        let mut outcome = outcome::TextIndexMaintenanceOutcome::default();
        for definition in indexes.text_indexes().filter(|definition| {
            document::text_definition_matches(
                definition,
                element_type,
                old_properties,
                new_properties,
                changed_property,
            )
        }) {
            let old_document = document::text_document_for_definition(
                definition,
                element_type,
                mutation.entity_id(),
                old_properties,
            )?
            .map(|document| document.map_index_name(|name| self.scoped_physical_index_name(name)));
            let new_document = document::text_document_for_definition(
                definition,
                element_type,
                mutation.entity_id(),
                new_properties,
            )?
            .map(|document| document.map_index_name(|name| self.scoped_physical_index_name(name)));
            self.apply_text_index_change(
                txn,
                definition,
                change::TextIndexChange::from_documents(old_document, new_document),
            )
            .await?
            .merge_into(&mut outcome);
        }
        Ok(outcome)
    }
}

/// Borrowed old/new property transition for one text-indexed entity.
pub(in crate::execution::interpreter) struct TextPropertyUpdate<'a> {
    old_properties: &'a [Property],
    new_properties: &'a [Property],
    changed_property: &'a str,
}

impl<'a> TextPropertyUpdate<'a> {
    /// Binds the complete property transition used for active and hidden indexes.
    pub(in crate::execution::interpreter) const fn new(
        old_properties: &'a [Property],
        new_properties: &'a [Property],
        changed_property: &'a str,
    ) -> Self {
        Self {
            old_properties,
            new_properties,
            changed_property,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum TextIndexMutation<'a> {
    Create {
        element_type: TextElementType,
        entity_id: u64,
        properties: &'a [Property],
    },
    Update {
        element_type: TextElementType,
        entity_id: u64,
        old_properties: &'a [Property],
        new_properties: &'a [Property],
        changed_property: &'a str,
    },
    Delete {
        element_type: TextElementType,
        entity_id: u64,
        properties: &'a [Property],
    },
}

impl TextIndexMutation<'_> {
    fn element_type(&self) -> TextElementType {
        match self {
            Self::Create { element_type, .. }
            | Self::Update { element_type, .. }
            | Self::Delete { element_type, .. } => *element_type,
        }
    }

    fn entity_id(&self) -> u64 {
        match self {
            Self::Create { entity_id, .. }
            | Self::Update { entity_id, .. }
            | Self::Delete { entity_id, .. } => *entity_id,
        }
    }

    fn old_properties(&self) -> &[Property] {
        match self {
            Self::Create { .. } => &[],
            Self::Update { old_properties, .. }
            | Self::Delete {
                properties: old_properties,
                ..
            } => old_properties,
        }
    }

    fn new_properties(&self) -> &[Property] {
        match self {
            Self::Create {
                properties: new_properties,
                ..
            }
            | Self::Update { new_properties, .. } => new_properties,
            Self::Delete { .. } => &[],
        }
    }

    fn changed_property(&self) -> Option<&str> {
        match self {
            Self::Update {
                changed_property, ..
            } => Some(changed_property),
            Self::Create { .. } | Self::Delete { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use helix_planner::context;
    use slatedb::IsolationLevel;

    use super::super::super::test_support;
    use super::*;

    #[tokio::test]
    async fn text_maintenance_propagates_invalid_old_and_new_document_values() {
        let db = test_support::open_db("text-maintenance-invalid-documents").await;
        let context = ExecutionContext::new(&db, context::ParamBindings::default());
        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("snapshot transaction begins");
        let indexes = crate::config::RuntimeIndexCatalog::new().with_text_index(
            crate::config::TextIndexDefinition::new_node("Doc", "body")
                .expect("valid text definition"),
        );
        let invalid = vec![Property::string("$label", "Doc"), Property::i64("body", 7)];

        let old_error = context
            .maintain_node_text_indexes_on_update(
                &txn,
                1,
                TextPropertyUpdate::new(&invalid, &[], "body"),
                &indexes,
            )
            .await
            .unwrap_err();
        assert!(old_error
            .to_string()
            .contains("only support String and StringArray"));

        let new_error = context
            .maintain_node_text_indexes_on_create(&txn, 1, &invalid, &indexes)
            .await
            .unwrap_err();
        assert!(new_error
            .to_string()
            .contains("only support String and StringArray"));
    }
}
