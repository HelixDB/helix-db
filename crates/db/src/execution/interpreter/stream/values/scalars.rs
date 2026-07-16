//! Scalar terminal sequence contracts.

use std::collections::BTreeSet;

use super::*;

pub(in crate::execution::interpreter::stream) fn scalar_items(
    value: ExecutionValue,
) -> Vec<ExecutionScalar> {
    match value {
        ExecutionValue::Count(count) => vec![count_scalar(count)],
        ExecutionValue::Bool(value) => vec![ExecutionScalar::Value(DbPropertyValue::Bool(value))],
        ExecutionValue::Scalars(values) => values,
        ExecutionValue::Stream(_)
        | ExecutionValue::FoldedStream(_)
        | ExecutionValue::IndexDdlReceipt(_)
        | ExecutionValue::IndexOperationStatus(_) => {
            unreachable!("scalar_items is only called for scalar execution values")
        }
    }
}

pub(in crate::execution::interpreter::stream) fn limit_scalars(
    mut values: Vec<ExecutionScalar>,
    count: usize,
) -> Vec<ExecutionScalar> {
    values.truncate(count);
    values
}

pub(in crate::execution::interpreter::stream) fn skip_scalars(
    values: Vec<ExecutionScalar>,
    count: usize,
) -> Vec<ExecutionScalar> {
    values.into_iter().skip(count).collect()
}

pub(in crate::execution::interpreter::stream) fn slice_scalars(
    values: Vec<ExecutionScalar>,
    start: usize,
    end: usize,
) -> Vec<ExecutionScalar> {
    values
        .into_iter()
        .skip(start)
        .take(end.saturating_sub(start))
        .collect()
}

pub(in crate::execution::interpreter::stream) fn distinct_scalars(
    values: Vec<ExecutionScalar>,
) -> Vec<ExecutionScalar> {
    let mut seen = BTreeSet::new();
    values
        .into_iter()
        .filter(|value| seen.insert(scalar_key(value)))
        .collect()
}

fn count_scalar(count: usize) -> ExecutionScalar {
    ExecutionScalar::Value(DbPropertyValue::I64(count.try_into().unwrap_or(i64::MAX)))
}

fn scalar_key(value: &ExecutionScalar) -> String {
    format!("{value:?}")
}
