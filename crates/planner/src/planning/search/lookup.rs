//! Catalog-backed search-index lookup.

use crate::{catalog, error};

pub(super) fn vector_index(
    indexes: &catalog::IndexCatalogSnapshot,
    key: &catalog::SearchIndexKey,
) -> Result<catalog::VectorIndexMeta, error::PlannerError> {
    indexes
        .vector
        .get(key)
        .cloned()
        .ok_or_else(|| missing_index(key, catalog::SearchIndexKind::Vector))
}

pub(super) fn text_index(
    indexes: &catalog::IndexCatalogSnapshot,
    key: &catalog::SearchIndexKey,
) -> Result<catalog::TextIndexMeta, error::PlannerError> {
    indexes
        .text
        .get(key)
        .cloned()
        .ok_or_else(|| missing_index(key, catalog::SearchIndexKind::Text))
}

fn missing_index(
    key: &catalog::SearchIndexKey,
    kind: catalog::SearchIndexKind,
) -> error::PlannerError {
    error::PlannerError::MissingSearchIndex {
        element: key.element,
        kind,
        label: key.label.clone(),
        property: key.property.clone(),
    }
}
