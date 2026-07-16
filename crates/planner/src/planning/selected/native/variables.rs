//! Native stream variable operation validation.

use super::names;
use crate::{error, ir, logical};

/// Build a pure `within` stream variable operation.
pub(super) fn within(variable: &str) -> Result<logical::PureStreamVariableOp, error::PlannerError> {
    names::non_empty(variable, ir::NameField::Variable).map(logical::PureStreamVariableOp::Within)
}

/// Build a pure `without` stream variable operation.
pub(super) fn without(
    variable: &str,
) -> Result<logical::PureStreamVariableOp, error::PlannerError> {
    names::non_empty(variable, ir::NameField::Variable).map(logical::PureStreamVariableOp::Without)
}

/// Build a pure `select` stream variable operation.
pub(super) fn select(name: &str) -> Result<logical::PureStreamVariableOp, error::PlannerError> {
    names::non_empty(name, ir::NameField::Name).map(logical::PureStreamVariableOp::Select)
}

/// Build a pure `bind` stream variable operation.
pub(super) fn bind(name: &str) -> Result<logical::PureStreamVariableOp, error::PlannerError> {
    names::non_empty(name, ir::NameField::Name).map(logical::PureStreamVariableOp::Bind)
}

/// Build a pure input-rooted `inject` stream variable operation.
pub(super) fn inject(variable: &str) -> Result<logical::PureStreamVariableOp, error::PlannerError> {
    names::non_empty(variable, ir::NameField::Variable).map(logical::PureStreamVariableOp::Inject)
}

/// Build a state-writing `as` stream variable operation.
pub(super) fn as_write(name: &str) -> Result<logical::StreamVariableWriteOp, error::PlannerError> {
    names::non_empty(name, ir::NameField::Name).map(logical::StreamVariableWriteOp::As)
}

/// Build a state-writing `store` stream variable operation.
pub(super) fn store(name: &str) -> Result<logical::StreamVariableWriteOp, error::PlannerError> {
    names::non_empty(name, ir::NameField::Name).map(logical::StreamVariableWriteOp::Store)
}
