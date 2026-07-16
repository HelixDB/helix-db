//! Property-selection projection payloads.

use super::super::names;
use crate::{error, ir};

/// Lower `values(...)` properties into a non-empty unique property list.
pub(in crate::planning::selected::native) fn values_properties(
    properties: &[String],
) -> Result<ir::PropertyNames, error::PlannerError> {
    let properties = property_names(properties)?;
    let properties = ir::AtLeast::<_, 1>::try_from_vec(properties).ok_or(
        error::PlannerError::InvalidProjectionArity {
            op: error::ProjectionOp::Values,
            min: 1,
            actual: 0,
        },
    )?;
    unique_property_names(properties)
}

/// Lower `value_map(...)` property selection.
pub(in crate::planning::selected::native) fn property_selection(
    properties: Option<&[String]>,
) -> Result<ir::PropertySelection, error::PlannerError> {
    let Some(properties) = properties else {
        return Ok(ir::PropertySelection::All);
    };
    match ir::AtLeast::<_, 1>::try_from_vec(property_names(properties)?) {
        Some(properties) => unique_property_names(properties).map(ir::PropertySelection::Selected),
        None => Ok(ir::PropertySelection::All),
    }
}

fn property_names(properties: &[String]) -> Result<Vec<ir::NonEmptyString>, error::PlannerError> {
    properties
        .iter()
        .map(|property| names::non_empty(property.as_str(), ir::NameField::Property))
        .collect()
}

fn unique_property_names(
    properties: ir::AtLeast<ir::NonEmptyString, 1>,
) -> Result<ir::PropertyNames, error::PlannerError> {
    ir::PropertyNames::new(properties).map_err(|err| match err {
        ir::PropertyNamesError::DuplicateName { name } => {
            error::PlannerError::DuplicatePropertySelection { property: name }
        }
    })
}
