use super::super::{ExecutionContext, ExecutionRow, ExecutionValue};
use crate::error::Result;

impl<'db> ExecutionContext<'db> {
    pub(super) fn path(&self, input: ExecutionValue) -> Result<ExecutionValue> {
        Ok(ExecutionValue::Stream(
            self.stream_rows(input, "path")?
                .into_iter()
                .map(ExecutionRow::mark_path_visible)
                .collect(),
        ))
    }

    pub(super) fn simple_path(&self, input: ExecutionValue) -> Result<ExecutionValue> {
        Ok(ExecutionValue::Stream(
            self.stream_rows(input, "simple_path")?
                .into_iter()
                .filter(ExecutionRow::has_simple_path)
                .collect(),
        ))
    }
}
