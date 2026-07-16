mod expr;
mod predicate;
mod property;
mod sets;

use helix_ast::expr::{CompareOp, Expr, Predicate};
use helix_ast::value::PropertyValue;
use helix_planner::context;

use super::super::super::test_support;
use super::*;

fn name(value: &str) -> ir::NonEmptyString {
    ir::NonEmptyString::new(value).expect("valid test name")
}

fn current_node(id: u64) -> ExecutionRow {
    ExecutionRow::current(ElementRef::Node(id))
}

fn current_edge(id: u64) -> ExecutionRow {
    ExecutionRow::current(ElementRef::Edge(id))
}
