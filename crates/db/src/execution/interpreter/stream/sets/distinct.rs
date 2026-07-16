//! Distinct stream and scalar dispatch contracts.

use std::collections::BTreeSet;

use super::super::values::{distinct_scalars, scalar_items};
use super::*;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) fn distinct(
        &mut self,
        input: ExecutionValue,
    ) -> Result<ExecutionValue> {
        match input {
            ExecutionValue::Stream(rows) => Ok(ExecutionValue::Stream(distinct_rows(rows))),
            ExecutionValue::FoldedStream(_) => Err(HelixDbError::Query(
                "distinct expected stream input, got folded stream; use unfold first".to_string(),
            )),
            value @ (ExecutionValue::Count(_)
            | ExecutionValue::Bool(_)
            | ExecutionValue::Scalars(_)) => Ok(ExecutionValue::Scalars(distinct_scalars(
                scalar_items(value),
            ))),
            ExecutionValue::IndexDdlReceipt(_) | ExecutionValue::IndexOperationStatus(_) => Err(
                HelixDbError::Query("distinct cannot consume an index lifecycle value".to_string()),
            ),
        }
    }
}

pub(in crate::execution::interpreter::stream) fn distinct_rows(
    rows: Vec<ExecutionRow>,
) -> Vec<ExecutionRow> {
    let mut seen = BTreeSet::new();
    rows.into_iter()
        .filter(|row| seen.insert(RowDistinctKey::from(row)))
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum RowDistinctKey {
    Current(ElementRef),
    Empty {
        bindings: Vec<(ir::NonEmptyString, ElementRef)>,
        path: Option<RowPath>,
        sack: RowSack,
    },
}

impl From<&ExecutionRow> for RowDistinctKey {
    fn from(row: &ExecutionRow) -> Self {
        match row.current.clone() {
            Some(current) => Self::Current(current),
            None => Self::Empty {
                bindings: row.bindings.clone().into_iter().collect(),
                path: row.path_visible.then(|| row.path.clone()),
                sack: row.sack.clone(),
            },
        }
    }
}
