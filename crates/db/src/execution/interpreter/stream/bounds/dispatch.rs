//! Limit, skip, and range input-shape dispatch.

use super::super::values::{limit_scalars, scalar_items, skip_scalars, slice_scalars};
use super::*;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) fn limit(
        &mut self,
        input: ExecutionValue,
        count: &ir::StreamBoundPlan,
    ) -> Result<ExecutionValue> {
        let count = self.stream_bound(count)?;
        match input {
            ExecutionValue::Stream(rows) => {
                Ok(ExecutionValue::Stream(rows::limit_rows(rows, count)))
            }
            ExecutionValue::FoldedStream(_) => Err(HelixDbError::Query(
                "limit expected stream input, got folded stream; use unfold first".to_string(),
            )),
            value @ (ExecutionValue::Count(_)
            | ExecutionValue::Bool(_)
            | ExecutionValue::Scalars(_)) => Ok(ExecutionValue::Scalars(limit_scalars(
                scalar_items(value),
                count,
            ))),
            ExecutionValue::IndexDdlReceipt(_) | ExecutionValue::IndexOperationStatus(_) => Err(
                HelixDbError::Query("limit cannot consume an index lifecycle value".to_string()),
            ),
        }
    }

    pub(in crate::execution::interpreter) fn skip(
        &mut self,
        input: ExecutionValue,
        count: &ir::StreamBoundPlan,
    ) -> Result<ExecutionValue> {
        let count = self.stream_bound(count)?;
        match input {
            ExecutionValue::Stream(rows) => {
                Ok(ExecutionValue::Stream(rows::skip_rows(rows, count)))
            }
            ExecutionValue::FoldedStream(_) => Err(HelixDbError::Query(
                "skip expected stream input, got folded stream; use unfold first".to_string(),
            )),
            value @ (ExecutionValue::Count(_)
            | ExecutionValue::Bool(_)
            | ExecutionValue::Scalars(_)) => Ok(ExecutionValue::Scalars(skip_scalars(
                scalar_items(value),
                count,
            ))),
            ExecutionValue::IndexDdlReceipt(_) | ExecutionValue::IndexOperationStatus(_) => Err(
                HelixDbError::Query("skip cannot consume an index lifecycle value".to_string()),
            ),
        }
    }

    pub(in crate::execution::interpreter) fn range(
        &mut self,
        input: ExecutionValue,
        range: &ir::StreamRangePlan,
    ) -> Result<ExecutionValue> {
        let (start, end) = self.stream_range(range)?;
        match input {
            ExecutionValue::Stream(rows) => {
                Ok(ExecutionValue::Stream(rows::slice_rows(rows, start, end)))
            }
            ExecutionValue::FoldedStream(_) => Err(HelixDbError::Query(
                "range expected stream input, got folded stream; use unfold first".to_string(),
            )),
            value @ (ExecutionValue::Count(_)
            | ExecutionValue::Bool(_)
            | ExecutionValue::Scalars(_)) => Ok(ExecutionValue::Scalars(slice_scalars(
                scalar_items(value),
                start,
                end,
            ))),
            ExecutionValue::IndexDdlReceipt(_) | ExecutionValue::IndexOperationStatus(_) => Err(
                HelixDbError::Query("range cannot consume an index lifecycle value".to_string()),
            ),
        }
    }
}
