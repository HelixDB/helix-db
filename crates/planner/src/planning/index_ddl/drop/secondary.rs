//! Secondary drop-index contracts.

use helix_ast::index;

use super::super::shared;
use crate::{error, ir};

pub(super) fn node_equality(
    label: &str,
    property: &str,
    unique: bool,
) -> Result<ir::IndexDdlDropSpec, error::PlannerError> {
    Ok(ir::IndexDdlDropSpec::NodeEquality {
        key: shared::scoped_property_key(label, property)?,
        uniqueness: if unique {
            crate::catalog::IndexUniqueness::Unique
        } else {
            crate::catalog::IndexUniqueness::NonUnique
        },
    })
}

pub(super) fn node_range(
    label: &str,
    property: &str,
    direction: index::RangeIndexDirection,
) -> Result<ir::IndexDdlDropSpec, error::PlannerError> {
    Ok(ir::IndexDdlDropSpec::NodeRange {
        key: shared::scoped_property_direction_key(label, property, direction)?,
    })
}

pub(super) fn edge_equality(
    label: &str,
    property: &str,
) -> Result<ir::IndexDdlDropSpec, error::PlannerError> {
    Ok(ir::IndexDdlDropSpec::EdgeEquality {
        key: shared::scoped_property_key(label, property)?,
    })
}

pub(super) fn edge_range(
    label: &str,
    property: &str,
    direction: index::RangeIndexDirection,
) -> Result<ir::IndexDdlDropSpec, error::PlannerError> {
    Ok(ir::IndexDdlDropSpec::EdgeRange {
        key: shared::scoped_property_direction_key(label, property, direction)?,
    })
}
