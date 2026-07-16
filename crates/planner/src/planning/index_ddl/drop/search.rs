//! Search drop-index contracts.

use super::super::shared;
use crate::{error, ir};

pub(super) fn node_vector(
    label: &str,
    property: &str,
) -> Result<ir::IndexDdlDropSpec, error::PlannerError> {
    Ok(ir::IndexDdlDropSpec::NodeVector {
        key: shared::scoped_property_key(label, property)?,
    })
}

pub(super) fn node_text(
    label: &str,
    property: &str,
) -> Result<ir::IndexDdlDropSpec, error::PlannerError> {
    Ok(ir::IndexDdlDropSpec::NodeText {
        key: shared::scoped_property_key(label, property)?,
    })
}

pub(super) fn edge_vector(
    label: &str,
    property: &str,
) -> Result<ir::IndexDdlDropSpec, error::PlannerError> {
    Ok(ir::IndexDdlDropSpec::EdgeVector {
        key: shared::scoped_property_key(label, property)?,
    })
}

pub(super) fn edge_text(
    label: &str,
    property: &str,
) -> Result<ir::IndexDdlDropSpec, error::PlannerError> {
    Ok(ir::IndexDdlDropSpec::EdgeText {
        key: shared::scoped_property_key(label, property)?,
    })
}
