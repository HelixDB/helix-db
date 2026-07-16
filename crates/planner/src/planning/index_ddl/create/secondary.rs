//! Secondary create-index contracts.

use helix_ast::index;

use super::super::shared;
use crate::{catalog, error, ir};

pub(super) fn node_equality(
    label: &str,
    property: &str,
    unique: bool,
) -> Result<ir::IndexDdlCreateSpec, error::PlannerError> {
    Ok(ir::IndexDdlCreateSpec::NodeEquality {
        key: shared::scoped_property_key(label, property)?,
        uniqueness: index_uniqueness(unique),
    })
}

pub(super) fn node_range(
    label: &str,
    property: &str,
    direction: index::RangeIndexDirection,
) -> Result<ir::IndexDdlCreateSpec, error::PlannerError> {
    Ok(ir::IndexDdlCreateSpec::NodeRange {
        key: shared::scoped_property_direction_key(label, property, direction)?,
    })
}

pub(super) fn edge_equality(
    label: &str,
    property: &str,
) -> Result<ir::IndexDdlCreateSpec, error::PlannerError> {
    Ok(ir::IndexDdlCreateSpec::EdgeEquality {
        key: shared::scoped_property_key(label, property)?,
    })
}

pub(super) fn edge_range(
    label: &str,
    property: &str,
    direction: index::RangeIndexDirection,
) -> Result<ir::IndexDdlCreateSpec, error::PlannerError> {
    Ok(ir::IndexDdlCreateSpec::EdgeRange {
        key: shared::scoped_property_direction_key(label, property, direction)?,
    })
}

fn index_uniqueness(unique: bool) -> catalog::IndexUniqueness {
    if unique {
        catalog::IndexUniqueness::Unique
    } else {
        catalog::IndexUniqueness::NonUnique
    }
}
