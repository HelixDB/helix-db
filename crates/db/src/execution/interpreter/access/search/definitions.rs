//! Runtime search-index definition lookup contracts.

use helix_planner::ir;

use super::*;
use crate::config::{
    TextElementType, TextIndexDefinition, VectorElementType, VectorIndexDefinition,
};

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter::access::search) fn vector_definition(
        &self,
        element_type: VectorElementType,
        label: &ir::NonEmptyString,
        property: &ir::NonEmptyString,
    ) -> Result<VectorIndexDefinition> {
        self.db
            .runtime_config_snapshot_loaded(self.tenant_scope)
            .vector_indexes()
            .find(|definition| {
                definition.element_type() == element_type
                    && definition.label() == label.as_ref()
                    && definition.property() == property.as_ref()
            })
            .cloned()
            .ok_or_else(|| {
                HelixDbError::IndexNotFound(format!(
                    "physical vector index definition for {element_type:?}:{label}:{property}"
                ))
            })
    }

    pub(in crate::execution::interpreter::access::search) fn text_definition(
        &self,
        element_type: TextElementType,
        label: &ir::NonEmptyString,
        property: &ir::NonEmptyString,
    ) -> Result<TextIndexDefinition> {
        self.db
            .runtime_config_snapshot_loaded(self.tenant_scope)
            .text_indexes()
            .find(|definition| {
                definition.element_type() == element_type
                    && definition.label() == label.as_ref()
                    && definition.property() == property.as_ref()
            })
            .cloned()
            .ok_or_else(|| {
                HelixDbError::IndexNotFound(format!(
                    "physical text index definition for {element_type:?}:{label}:{property}"
                ))
            })
    }
}

#[cfg(test)]
mod tests {
    use helix_planner::context;

    use super::super::super::super::test_support;
    use super::*;

    #[tokio::test]
    async fn text_definition_reports_missing_physical_definition() {
        let db = test_support::open_db("search-missing-text-definition").await;
        let context = ExecutionContext::new(&db, context::ParamBindings::default());
        let label = test_support::name("Doc");
        let property = test_support::name("body");

        let error = context
            .text_definition(TextElementType::Node, &label, &property)
            .expect_err("missing text definition should fail");

        assert!(matches!(
            error,
            HelixDbError::IndexNotFound(name)
                if name == "physical text index definition for Node:Doc:body"
        ));
    }
}
