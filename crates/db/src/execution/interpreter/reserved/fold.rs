//! Execution of reserved fold and unfold operations.

use super::super::{ExecutionContext, ExecutionValue, FoldedStream};
use crate::error::{HelixDbError, Result};

impl<'db> ExecutionContext<'db> {
    pub(super) fn fold(&self, input: ExecutionValue) -> Result<ExecutionValue> {
        Ok(ExecutionValue::FoldedStream(FoldedStream::new(
            self.stream_rows(input, "fold")?,
        )))
    }

    pub(super) fn unfold(&self, input: ExecutionValue) -> Result<ExecutionValue> {
        match input {
            ExecutionValue::FoldedStream(folded) => Ok(ExecutionValue::Stream(folded.into_rows())),
            ExecutionValue::Stream(rows) => Ok(ExecutionValue::Stream(rows)),
            ExecutionValue::Count(_)
            | ExecutionValue::Bool(_)
            | ExecutionValue::Scalars(_)
            | ExecutionValue::IndexDdlReceipt(_)
            | ExecutionValue::IndexOperationStatus(_) => Err(HelixDbError::Query(
                // Report the type contract without evaluating or exposing input values.
                "unfold expected stream or folded stream input".into(),
            )),
        }
    }
}
