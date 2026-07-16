//! Native order-key validation.

use crate::{error, ir};

/// Lower one AST order key into the executable IR ordering contract.
pub(super) fn order_key(
    property: &str,
    order: helix_ast::traversal::Order,
) -> Result<ir::OrderKey, error::PlannerError> {
    Ok(ir::OrderKey {
        property: super::names::non_empty(property, ir::NameField::Property)?,
        order,
    })
}

/// Lower an AST order list into a non-empty duplicate-free ordering contract.
pub(super) fn order_keys(
    orderings: &[(String, helix_ast::traversal::Order)],
) -> Result<ir::OrderKeys, error::PlannerError> {
    let keys = orderings
        .iter()
        .map(|(property, order)| order_key(property, *order))
        .collect::<Result<Vec<_>, _>>()?;
    let keys =
        ir::AtLeast::<_, 1>::try_from_vec(keys).ok_or(error::PlannerError::InvalidOrderKeys)?;
    ir::OrderKeys::new(keys).map_err(|err| match err {
        ir::OrderKeysError::DuplicateProperty { property } => {
            error::PlannerError::DuplicateOrderKey { property }
        }
    })
}
