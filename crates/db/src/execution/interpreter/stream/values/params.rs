//! Runtime parameter lookup contracts.

use super::conversion::{ast_to_db_value, query_value_to_db_value};
use super::*;

pub(in crate::execution::interpreter::stream) fn param_value_from(
    params: &context::ParamBindings,
    name: &ir::NonEmptyString,
) -> Result<DbPropertyValue> {
    if let Some(value) = params.values.get(name) {
        return Ok(ast_to_db_value(value.clone()));
    }
    params
        .query_values
        .get(name)
        .cloned()
        .map(query_value_to_db_value)
        .ok_or_else(|| HelixDbError::Query(format!("parameter `{name}` is not bound")))
}
