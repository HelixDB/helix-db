//! Search create-index contracts.

use helix_ast::index;
use std::num::NonZeroUsize;

use super::super::shared;
use crate::{error, ir};

pub(super) fn node_vector(
    label: &str,
    property: &str,
    dimension: NonZeroUsize,
    metric: index::VectorDistanceMetric,
    tenant_property: &Option<String>,
) -> Result<ir::IndexDdlCreateSpec, error::PlannerError> {
    Ok(ir::IndexDdlCreateSpec::NodeVector {
        key: shared::scoped_property_key(label, property)?,
        dimension: ir::VectorIndexDimension::from_non_zero(dimension),
        metric: shared::vector_index_metric(metric),
        scope: shared::search_scope(tenant_property)?,
    })
}

pub(super) fn node_text(
    label: &str,
    property: &str,
    tenant_property: &Option<String>,
) -> Result<ir::IndexDdlCreateSpec, error::PlannerError> {
    Ok(ir::IndexDdlCreateSpec::NodeText {
        key: shared::scoped_property_key(label, property)?,
        scope: shared::search_scope(tenant_property)?,
    })
}

pub(super) fn edge_vector(
    label: &str,
    property: &str,
    dimension: NonZeroUsize,
    metric: index::VectorDistanceMetric,
    tenant_property: &Option<String>,
) -> Result<ir::IndexDdlCreateSpec, error::PlannerError> {
    Ok(ir::IndexDdlCreateSpec::EdgeVector {
        key: shared::scoped_property_key(label, property)?,
        dimension: ir::VectorIndexDimension::from_non_zero(dimension),
        metric: shared::vector_index_metric(metric),
        scope: shared::search_scope(tenant_property)?,
    })
}

pub(super) fn edge_text(
    label: &str,
    property: &str,
    tenant_property: &Option<String>,
) -> Result<ir::IndexDdlCreateSpec, error::PlannerError> {
    Ok(ir::IndexDdlCreateSpec::EdgeText {
        key: shared::scoped_property_key(label, property)?,
        scope: shared::search_scope(tenant_property)?,
    })
}
