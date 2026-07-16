//! Row-window helper contracts.

use super::*;

pub(in crate::execution::interpreter::stream) fn limit_rows(
    mut rows: Vec<ExecutionRow>,
    count: usize,
) -> Vec<ExecutionRow> {
    rows.truncate(count);
    rows
}

pub(in crate::execution::interpreter::stream) fn skip_rows(
    rows: Vec<ExecutionRow>,
    count: usize,
) -> Vec<ExecutionRow> {
    rows.into_iter().skip(count).collect()
}

pub(in crate::execution::interpreter::stream) fn slice_rows(
    rows: Vec<ExecutionRow>,
    start: usize,
    end: usize,
) -> Vec<ExecutionRow> {
    rows.into_iter()
        .skip(start)
        .take(end.saturating_sub(start))
        .collect()
}
