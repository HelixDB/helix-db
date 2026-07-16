//! Canonical V2 vector-index mutation routing.
//!
//! Graph writes enter the lifecycle-owned mutation set exactly once. Building
//! generations record deltas and active generations update descriptor-bound
//! physical rows. A configured definition without a matching V2 target fails
//! closed; this module never constructs or mutates a display-name-derived HNSW
//! namespace.

use slatedb::DbTransaction;

use super::super::mutation::MutationIndexContext;
use super::super::*;
use super::properties;
use crate::config::VectorElementType;
use crate::encoding::keys::tenant::DataScope;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn maintain_node_vector_indexes_on_create(
        &self,
        txn: &DbTransaction,
        node_id: u64,
        properties: &[Property],
        indexes: &crate::config::RuntimeIndexCatalog,
        index_context: &MutationIndexContext,
    ) -> Result<()> {
        maintain_vector_indexes(
            txn,
            VectorIndexMaintenance::new(VectorElementType::Node, node_id)
                .with_new_properties(properties),
            indexes,
            index_context,
            self.tenant_scope,
        )
        .await
    }

    pub(in crate::execution::interpreter) async fn maintain_node_vector_indexes_on_update(
        &self,
        txn: &DbTransaction,
        node_id: u64,
        update: VectorPropertyUpdate<'_>,
        indexes: &crate::config::RuntimeIndexCatalog,
        index_context: &MutationIndexContext,
    ) -> Result<()> {
        maintain_vector_indexes(
            txn,
            VectorIndexMaintenance::new(VectorElementType::Node, node_id)
                .with_old_properties(update.old_properties)
                .with_new_properties(update.new_properties)
                .with_changed_property(update.changed_property),
            indexes,
            index_context,
            self.tenant_scope,
        )
        .await
    }

    pub(in crate::execution::interpreter) async fn maintain_node_vector_indexes_on_delete(
        &self,
        txn: &DbTransaction,
        node_id: u64,
        properties: &[Property],
        indexes: &crate::config::RuntimeIndexCatalog,
        index_context: &MutationIndexContext,
    ) -> Result<()> {
        maintain_vector_indexes(
            txn,
            VectorIndexMaintenance::new(VectorElementType::Node, node_id)
                .with_old_properties(properties),
            indexes,
            index_context,
            self.tenant_scope,
        )
        .await
    }

    pub(in crate::execution::interpreter) async fn maintain_edge_vector_indexes_on_create(
        &self,
        txn: &DbTransaction,
        edge_id: u64,
        properties: &[Property],
        indexes: &crate::config::RuntimeIndexCatalog,
        index_context: &MutationIndexContext,
    ) -> Result<()> {
        maintain_vector_indexes(
            txn,
            VectorIndexMaintenance::new(VectorElementType::Edge, edge_id)
                .with_new_properties(properties),
            indexes,
            index_context,
            self.tenant_scope,
        )
        .await
    }

    pub(in crate::execution::interpreter) async fn maintain_edge_vector_indexes_on_update(
        &self,
        txn: &DbTransaction,
        edge_id: u64,
        update: VectorPropertyUpdate<'_>,
        indexes: &crate::config::RuntimeIndexCatalog,
        index_context: &MutationIndexContext,
    ) -> Result<()> {
        maintain_vector_indexes(
            txn,
            VectorIndexMaintenance::new(VectorElementType::Edge, edge_id)
                .with_old_properties(update.old_properties)
                .with_new_properties(update.new_properties)
                .with_changed_property(update.changed_property),
            indexes,
            index_context,
            self.tenant_scope,
        )
        .await
    }

    pub(in crate::execution::interpreter) async fn maintain_edge_vector_indexes_on_delete(
        &self,
        txn: &DbTransaction,
        edge_id: u64,
        properties: &[Property],
        indexes: &crate::config::RuntimeIndexCatalog,
        index_context: &MutationIndexContext,
    ) -> Result<()> {
        maintain_vector_indexes(
            txn,
            VectorIndexMaintenance::new(VectorElementType::Edge, edge_id)
                .with_old_properties(properties),
            indexes,
            index_context,
            self.tenant_scope,
        )
        .await
    }
}

/// Borrowed old/new property transition for one vector-indexed entity.
pub(in crate::execution::interpreter) struct VectorPropertyUpdate<'a> {
    old_properties: &'a [Property],
    new_properties: &'a [Property],
    changed_property: &'a str,
}

impl<'a> VectorPropertyUpdate<'a> {
    /// Binds the complete property transition used for index selection.
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

struct VectorIndexMaintenance<'a> {
    element_type: VectorElementType,
    entity_id: u64,
    old_properties: &'a [Property],
    new_properties: &'a [Property],
    changed_property: Option<&'a str>,
}

impl<'a> VectorIndexMaintenance<'a> {
    const fn new(element_type: VectorElementType, entity_id: u64) -> Self {
        Self {
            element_type,
            entity_id,
            old_properties: &[],
            new_properties: &[],
            changed_property: None,
        }
    }

    const fn with_old_properties(mut self, old_properties: &'a [Property]) -> Self {
        self.old_properties = old_properties;
        self
    }

    const fn with_new_properties(mut self, new_properties: &'a [Property]) -> Self {
        self.new_properties = new_properties;
        self
    }

    const fn with_changed_property(mut self, changed_property: &'a str) -> Self {
        self.changed_property = Some(changed_property);
        self
    }
}

async fn maintain_vector_indexes(
    txn: &DbTransaction,
    request: VectorIndexMaintenance<'_>,
    indexes: &crate::config::RuntimeIndexCatalog,
    index_context: &MutationIndexContext,
    tenant_scope: DataScope,
) -> Result<()> {
    let entity_kind = match request.element_type {
        VectorElementType::Node => crate::index_v2::IndexElementKind::Node,
        VectorElementType::Edge => crate::index_v2::IndexElementKind::Edge,
    };
    if indexes.vector_indexes().any(|definition| {
        !index_context.vector().owns_runtime_definition(definition)
            && definition.element_type() == request.element_type
            && (properties::label_value(request.old_properties) == Some(definition.label())
                || properties::label_value(request.new_properties) == Some(definition.label()))
            && request
                .changed_property
                .map(|property| {
                    property == "$label"
                        || definition.property() == property
                        || definition.tenant_property() == Some(property)
                })
                .unwrap_or(true)
    }) {
        return Err(HelixDbError::IndexLifecycleUnavailable {
            family: crate::error::IndexFamily::Vector,
            reason: crate::error::IndexLifecycleUnavailableReason::CanonicalStateUnavailable,
        });
    }
    crate::index_v2::vector::maintain_entity(
        txn,
        tenant_scope,
        index_context.vector(),
        index_context.vector_cache_writes(),
        crate::index_v2::vector::VectorEntityMutation::new(
            entity_kind,
            request.entity_id,
            request.old_properties,
            request.new_properties,
        ),
    )
    .await
}

#[cfg(test)]
mod tests {
    use slatedb::IsolationLevel;

    use super::super::super::test_support;
    use super::*;
    use crate::search::vector::VectorDistanceMetric;

    #[tokio::test]
    async fn configured_vector_without_canonical_target_fails_closed_only_when_affected() {
        let db = test_support::open_db("configured-vector-requires-v2-target").await;
        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("snapshot transaction begins");
        let indexes = crate::config::RuntimeIndexCatalog::new()
            .with_node_vector_index("Document", "embedding", 3, VectorDistanceMetric::Euclidean)
            .expect("valid configured vector index");
        let index_context = MutationIndexContext::for_configured_index_test(std::sync::Arc::clone(
            db.simhasher_registry(),
        ));
        let indexed = [
            Property::string("$label", "Document"),
            Property::new("embedding", DbPropertyValue::F32Array(vec![1.0, 2.0, 3.0])),
        ];

        let error = maintain_vector_indexes(
            &txn,
            VectorIndexMaintenance::new(VectorElementType::Node, 7).with_new_properties(&indexed),
            &indexes,
            &index_context,
            DataScope::LegacyUnscoped,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            HelixDbError::IndexLifecycleUnavailable {
                family: crate::error::IndexFamily::Vector,
                reason: crate::error::IndexLifecycleUnavailableReason::CanonicalStateUnavailable,
            }
        ));

        maintain_vector_indexes(
            &txn,
            VectorIndexMaintenance::new(VectorElementType::Node, 8)
                .with_old_properties(&indexed)
                .with_new_properties(&indexed)
                .with_changed_property("title"),
            &indexes,
            &index_context,
            DataScope::LegacyUnscoped,
        )
        .await
        .expect("an unrelated property change does not require vector authority");
        maintain_vector_indexes(
            &txn,
            VectorIndexMaintenance::new(VectorElementType::Edge, 9).with_new_properties(&indexed),
            &indexes,
            &index_context,
            DataScope::LegacyUnscoped,
        )
        .await
        .expect("a definition for another element kind is unrelated");
    }
}
