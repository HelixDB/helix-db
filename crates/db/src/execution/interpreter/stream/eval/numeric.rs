//! Numeric expression coercion contracts.

use super::*;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter::stream::eval) fn numeric_binary_values(
        &self,
        left: DbPropertyValue,
        right: DbPropertyValue,
        op: impl FnOnce(f64, f64) -> f64,
    ) -> Result<DbPropertyValue> {
        let left = left
            .as_f64()
            .ok_or_else(|| HelixDbError::Query("left expression must be numeric".to_string()))?;
        let right = right
            .as_f64()
            .ok_or_else(|| HelixDbError::Query("right expression must be numeric".to_string()))?;
        Ok(DbPropertyValue::F64(op(left, right)))
    }
}
