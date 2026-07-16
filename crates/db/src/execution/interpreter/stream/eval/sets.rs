//! Variable element-set extraction contracts.

use std::collections::BTreeSet;

use super::*;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter::stream) fn element_set(
        &self,
        value: &ExecutionValue,
    ) -> Result<BTreeSet<ElementRef>> {
        match value {
            ExecutionValue::Stream(rows) => Ok(rows
                .iter()
                .filter_map(|row| row.current.clone())
                .collect::<BTreeSet<_>>()),
            ExecutionValue::FoldedStream(folded) => Ok(folded
                .rows()
                .iter()
                .filter_map(|row| row.current.clone())
                .collect::<BTreeSet<_>>()),
            other @ (ExecutionValue::Count(_)
            | ExecutionValue::Bool(_)
            | ExecutionValue::Scalars(_)
            | ExecutionValue::IndexDdlReceipt(_)
            | ExecutionValue::IndexOperationStatus(_)) => Err(HelixDbError::Query(format!(
                "variable operation expected stream value, got {other:?}"
            ))),
        }
    }
}
