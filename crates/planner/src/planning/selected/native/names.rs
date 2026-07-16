//! Native lowering name validation.

use crate::{error, ir};

/// Build a non-empty IR string at native AST lowering boundaries.
pub(super) fn non_empty(
    value: impl Into<String>,
    field: ir::NameField,
) -> Result<ir::NonEmptyString, error::PlannerError> {
    ir::NonEmptyString::new(value.into()).ok_or(error::PlannerError::InvalidEmptyName { field })
}
