//! Stream operation execution and typed value/projection boundaries.

use super::*;

mod aggregate;
mod bounds;
mod eval;
mod filter;
mod order;
mod projection;
mod sets;
mod values;

#[cfg(test)]
mod tests;

pub(super) use self::eval::property_value_is_in;
pub(super) use self::filter::{PreparedIndexMembership, RowDecision};
pub(super) use self::values::ast_to_db_value;

impl<'db> ExecutionContext<'db> {
    pub(super) fn stream_rows(
        &self,
        value: ExecutionValue,
        op: &'static str,
    ) -> Result<Vec<ExecutionRow>> {
        match value {
            ExecutionValue::Stream(rows) => Ok(rows),
            ExecutionValue::FoldedStream(_) => Err(HelixDbError::Query(format!(
                "{op} expected stream input, got folded stream; use unfold first"
            ))),
            other @ (ExecutionValue::Count(_)
            | ExecutionValue::Bool(_)
            | ExecutionValue::Scalars(_)
            | ExecutionValue::IndexDdlReceipt(_)
            | ExecutionValue::IndexOperationStatus(_)) => Err(HelixDbError::Query(format!(
                "{op} expected stream input, got {other:?}"
            ))),
        }
    }
}

pub(in crate::execution::interpreter) use bounds::eval_stream_bound;

pub(in crate::execution::interpreter) use sets::RowDistinctKey;
pub(in crate::execution::interpreter) use values::DistinctKey;
