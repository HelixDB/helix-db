mod dispatch;
mod eval;
mod rows;

use helix_ast::expr::{Expr, StreamBound};
use helix_ast::query::QueryValue;
use helix_ast::value::PropertyValue as AstPropertyValue;

use super::super::super::test_support;
use super::super::super::{ElementRef, ExecutionContext, ExecutionScalar, FoldedStream};
use super::*;
use super::{eval as bound_eval, rows as row_bounds};

fn name(value: &str) -> ir::NonEmptyString {
    test_support::name(value)
}

fn rows(ids: &[u64]) -> Vec<ExecutionRow> {
    ids.iter()
        .copied()
        .map(|id| ExecutionRow::current(ElementRef::Node(id)))
        .collect()
}

fn row_ids(value: ExecutionValue) -> Vec<u64> {
    let ExecutionValue::Stream(rows) = value else {
        panic!("expected stream value");
    };
    rows.into_iter()
        .map(|row| match row.current.expect("row current element") {
            ElementRef::Node(id) => id,
            ElementRef::Edge(id) => panic!("expected node row, got edge {id}"),
        })
        .collect()
}

fn scalars(values: Vec<u64>) -> ExecutionValue {
    ExecutionValue::Scalars(values.into_iter().map(ExecutionScalar::NodeId).collect())
}

fn error_message<T>(result: Result<T>) -> String {
    match result {
        Ok(_) => panic!("operation should fail"),
        Err(err) => err.to_string(),
    }
}
