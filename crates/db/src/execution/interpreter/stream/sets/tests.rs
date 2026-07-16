//! Contract tests for stream set operations and variable assignment.

mod distinct;
mod merge;
mod variables;

use std::collections::BTreeSet;

use helix_planner::context;

use super::super::super::test_support;
use super::super::super::{
    ElementRef, ExecutionContext, ExecutionRow, ExecutionScalar, ExecutionValue, FoldedStream,
    RowVirtualProperties,
};
use super::*;
use super::{distinct as set_distinct, merge as set_merge, variables as set_variables};

fn row(id: u64) -> ExecutionRow {
    ExecutionRow::current(ElementRef::Node(id))
}

fn rows(ids: &[u64]) -> Vec<ExecutionRow> {
    ids.iter().copied().map(row).collect()
}

fn row_ids(rows: Vec<ExecutionRow>) -> Vec<u64> {
    rows.into_iter()
        .map(|row| match row.current.expect("row current element") {
            ElementRef::Node(id) => id,
            ElementRef::Edge(id) => panic!("expected node row, got edge {id}"),
        })
        .collect()
}

fn visible_path_row(start: u64, next: u64) -> ExecutionRow {
    let mut row = ExecutionRow::current(ElementRef::Node(start));
    row.set_current(ElementRef::Node(next));
    row.mark_path_visible()
}

fn name(value: &str) -> ir::NonEmptyString {
    ir::NonEmptyString::new(value).expect("valid test binding")
}

fn stream(ids: &[u64]) -> ExecutionValue {
    ExecutionValue::Stream(rows(ids))
}

fn scalars(ids: &[u64]) -> ExecutionValue {
    ExecutionValue::Scalars(ids.iter().copied().map(ExecutionScalar::NodeId).collect())
}

fn expect_stream(value: ExecutionValue, label: &str) -> Vec<ExecutionRow> {
    match value {
        ExecutionValue::Stream(rows) => rows,
        ExecutionValue::FoldedStream(value) => {
            panic!("expected {label} stream, got folded stream {value:?}")
        }
        ExecutionValue::Count(value) => panic!("expected {label} stream, got count {value}"),
        ExecutionValue::Bool(value) => panic!("expected {label} stream, got bool {value}"),
        ExecutionValue::Scalars(value) => panic!("expected {label} stream, got scalars {value:?}"),
        ExecutionValue::IndexDdlReceipt(value) => {
            panic!("expected {label} stream, got index DDL receipt {value:?}")
        }
        ExecutionValue::IndexOperationStatus(value) => {
            panic!("expected {label} stream, got index operation status {value:?}")
        }
    }
}

fn error_message<T>(result: Result<T>) -> String {
    match result {
        Ok(_) => panic!("operation should fail"),
        Err(err) => err.to_string(),
    }
}
