//! Edge search access builders.

use helix_ast::expr::StreamBound;
use helix_ast::value::PropertyInput;

use super::super::{input, lookup, non_empty};
use super::{contracts, metadata};
use crate::{catalog, error, ir};

/// Build an edge vector-search access plan.
pub fn edge_vector_search(
    indexes: &catalog::IndexCatalogSnapshot,
    label: &str,
    property: &str,
    tenant_value: Option<&PropertyInput>,
    query_vector: &PropertyInput,
    k: &StreamBound,
) -> Result<contracts::SearchAccessPlan<ir::EdgeAccessPlan>, error::PlannerError> {
    let key = edge_search_key(label, property)?;
    let index = lookup::vector_index(indexes, &catalog::SearchIndexKey::from(key.clone()))?;
    let index_id = index.index_id.clone();
    Ok(contracts::SearchAccessPlan {
        plan: ir::EdgeAccessPlan::VectorSearch {
            key,
            index: metadata::search_index_metadata(
                index.index_id,
                index.scope,
                tenant_value,
                catalog::SearchIndexKind::Vector,
            )?,
            query_vector: input::vector_query(query_vector)?,
            k: input::search_limit(catalog::SearchIndexKind::Vector, k)?,
        },
        index_id,
    })
}

/// Build an edge text-search access plan.
pub fn edge_text_search(
    indexes: &catalog::IndexCatalogSnapshot,
    label: &str,
    property: &str,
    tenant_value: Option<&PropertyInput>,
    query_text: &PropertyInput,
    k: &StreamBound,
) -> Result<contracts::SearchAccessPlan<ir::EdgeAccessPlan>, error::PlannerError> {
    let key = edge_search_key(label, property)?;
    let index = lookup::text_index(indexes, &catalog::SearchIndexKey::from(key.clone()))?;
    let index_id = index.index_id.clone();
    Ok(contracts::SearchAccessPlan {
        plan: ir::EdgeAccessPlan::TextSearch {
            key,
            index: metadata::search_index_metadata(
                index.index_id,
                index.scope,
                tenant_value,
                catalog::SearchIndexKind::Text,
            )?,
            query_text: input::text_query(query_text)?,
            k: input::search_limit(catalog::SearchIndexKind::Text, k)?,
        },
        index_id,
    })
}

fn edge_search_key(
    label: &str,
    property: &str,
) -> Result<catalog::EdgeSearchIndexKey, error::PlannerError> {
    Ok(catalog::EdgeSearchIndexKey::new(
        non_empty(label, ir::NameField::Label)?,
        non_empty(property, ir::NameField::Property)?,
    ))
}
