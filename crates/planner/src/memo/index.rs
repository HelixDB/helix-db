//! Request-local indexes over validated memo records.
//!
//! `Memo` owns the serialized records, while this module owns the derived
//! lookup contract. Keeping the index private lets the memo enforce dense,
//! one-based IDs once, then use direct indexing on hot optimizer paths.

use super::ids::{MemoExprId, MemoGroupId};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct MemoIndexes {
    expr_locations: Vec<MemoExprLocation>,
}

impl MemoIndexes {
    pub(super) fn new(expr_capacity: usize) -> Self {
        Self {
            expr_locations: Vec::with_capacity(expr_capacity),
        }
    }

    pub(super) fn group_index(group: MemoGroupId) -> usize {
        group.get() - 1
    }

    pub(super) fn push_expr(&mut self, expr: MemoExprId, location: MemoExprLocation) {
        debug_assert_eq!(
            expr.get(),
            self.expr_locations.len() + 1,
            "memo expression IDs must remain dense and one-based"
        );
        self.expr_locations.push(location);
    }

    pub(super) fn expr_location(&self, expr: MemoExprId) -> Option<MemoExprLocation> {
        self.expr_locations.get(expr.get() - 1).copied()
    }

    pub(super) fn expr_count(&self) -> usize {
        self.expr_locations.len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct MemoExprLocation {
    group_index: usize,
    expr_index: usize,
}

impl MemoExprLocation {
    pub(super) const fn new(group_index: usize, expr_index: usize) -> Self {
        Self {
            group_index,
            expr_index,
        }
    }

    pub(super) const fn group_index(self) -> usize {
        self.group_index
    }

    pub(super) const fn expr_index(self) -> usize {
        self.expr_index
    }
}
