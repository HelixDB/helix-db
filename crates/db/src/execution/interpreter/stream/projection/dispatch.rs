//! Projection input-shape dispatch.

use super::*;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn project(
        &mut self,
        input: ExecutionValue,
        projection: &ir::ProjectionPlan,
    ) -> Result<ExecutionValue> {
        match input {
            ExecutionValue::Stream(rows) => self.project_stream_rows(rows, projection).await,
            ExecutionValue::FoldedStream(_) => Err(HelixDbError::Query(
                "project expected stream input, got folded stream; use unfold first".to_string(),
            )),
            value @ (ExecutionValue::Count(_)
            | ExecutionValue::Bool(_)
            | ExecutionValue::Scalars(_)) => scalar::project_scalar_items(value, projection),
            ExecutionValue::IndexDdlReceipt(_) | ExecutionValue::IndexOperationStatus(_) => Err(
                HelixDbError::Query("project cannot consume an index lifecycle value".to_string()),
            ),
        }
    }
}
