//! Memo-child extraction for access-backed stream expressions.
//!
//! Access wrappers own their residual-free source and physical access prefix.
//! They do not produce selected child roots; the embedded access path remains
//! part of the parent logical payload and memo identity.

use super::*;

pub(super) fn filter_children(_filter: &AccessFilter) -> Vec<LogicalExpr> {
    Vec::new()
}

pub(super) fn window_children(_window: &AccessWindow) -> Vec<LogicalExpr> {
    Vec::new()
}

pub(super) fn order_children(_order: &AccessOrder) -> Vec<LogicalExpr> {
    Vec::new()
}

pub(super) fn distinct_children(_distinct: &AccessDistinct) -> Vec<LogicalExpr> {
    Vec::new()
}

pub(super) fn pipeline_children(_pipeline: &AccessPipeline) -> Vec<LogicalExpr> {
    Vec::new()
}
