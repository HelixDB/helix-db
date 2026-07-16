//! Expression parameter lookup contracts.

use super::super::values::param_value_from;
use super::*;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) fn param_value(
        &self,
        name: &ir::NonEmptyString,
    ) -> Result<DbPropertyValue> {
        param_value_from(&self.params, name)
    }
}
