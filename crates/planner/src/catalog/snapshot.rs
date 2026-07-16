use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::ir::NonEmptyString;

use super::metadata::{
    EdgeEqualityIndexMeta, EdgeRangeIndexMeta, NodeEqualityIndexMeta, NodeRangeIndexMeta,
    TextIndexMeta, VectorIndexMeta,
};
use super::property::{ScopedPropertyDirectionKey, ScopedPropertyKey};
use super::search::{SearchIndexKey, SearchIndexScope};

/// Snapshot of known indexes.
///
/// Builder methods accept typed keys and scopes so empty labels, properties,
/// and tenant properties are rejected before the catalog boundary.
///
/// # Examples
///
/// ```
/// use helix_planner::catalog::{
///     ElementKind, IndexCatalogSnapshot, ScopedPropertyKey, SearchIndexKey,
///     SearchIndexScope,
/// };
///
/// let email = ScopedPropertyKey::try_new("User", "email").unwrap();
/// let embedding = SearchIndexKey::try_new(ElementKind::Node, "Doc", "embedding").unwrap();
///
/// let catalog = IndexCatalogSnapshot::default()
///     .with_node_eq(email.clone())
///     .with_vector(embedding.clone(), SearchIndexScope::Unscoped);
///
/// assert!(catalog.node_eq.contains_key(&email));
/// assert_eq!(catalog.vector[&embedding].scope, SearchIndexScope::Unscoped);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexCatalogSnapshot {
    /// Node equality indexes keyed by `(label, property)`.
    pub node_eq: HashMap<ScopedPropertyKey, NodeEqualityIndexMeta>,
    /// Node range indexes keyed by `(label, property, direction)`.
    pub node_range: HashMap<ScopedPropertyDirectionKey, NodeRangeIndexMeta>,
    /// Edge equality indexes keyed by `(label, property)`.
    pub edge_eq: HashMap<ScopedPropertyKey, EdgeEqualityIndexMeta>,
    /// Edge range indexes keyed by `(label, property, direction)`.
    pub edge_range: HashMap<ScopedPropertyDirectionKey, EdgeRangeIndexMeta>,
    /// Vector indexes.
    pub vector: HashMap<SearchIndexKey, VectorIndexMeta>,
    /// Text indexes.
    pub text: HashMap<SearchIndexKey, TextIndexMeta>,
}

impl IndexCatalogSnapshot {
    /// Add a node equality index.
    pub fn with_node_eq(mut self, key: ScopedPropertyKey) -> Self {
        self.node_eq.insert(
            key.clone(),
            NodeEqualityIndexMeta::new(NonEmptyString::from_prefixed_display("node_eq:", key)),
        );
        self
    }

    /// Add a node range index.
    pub fn with_node_range(mut self, key: ScopedPropertyDirectionKey) -> Self {
        self.node_range.insert(
            key.clone(),
            NodeRangeIndexMeta::new(NonEmptyString::from_prefixed_display("node_range:", key)),
        );
        self
    }

    /// Add an edge equality index.
    pub fn with_edge_eq(mut self, key: ScopedPropertyKey) -> Self {
        self.edge_eq.insert(
            key.clone(),
            EdgeEqualityIndexMeta::new(NonEmptyString::from_prefixed_display("edge_eq:", key)),
        );
        self
    }

    /// Add an edge range index.
    pub fn with_edge_range(mut self, key: ScopedPropertyDirectionKey) -> Self {
        self.edge_range.insert(
            key.clone(),
            EdgeRangeIndexMeta::new(NonEmptyString::from_prefixed_display("edge_range:", key)),
        );
        self
    }

    /// Add a vector index.
    pub fn with_vector(mut self, key: SearchIndexKey, scope: SearchIndexScope) -> Self {
        self.vector.insert(
            key.clone(),
            VectorIndexMeta::new(NonEmptyString::from_prefixed_display("vector:", key), scope),
        );
        self
    }

    /// Add a text index.
    pub fn with_text(mut self, key: SearchIndexKey, scope: SearchIndexScope) -> Self {
        self.text.insert(
            key.clone(),
            TextIndexMeta::new(NonEmptyString::from_prefixed_display("text:", key), scope),
        );
        self
    }
}
