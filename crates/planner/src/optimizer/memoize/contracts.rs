//! Memoization queue and insertion-result contracts.

use crate::{logical, memo};

#[derive(Debug, Clone, PartialEq)]
pub(in crate::optimizer) struct QueuedMemoExpr {
    pub(in crate::optimizer) group: memo::MemoGroupId,
    pub(in crate::optimizer) expr: memo::MemoExprId,
    pub(in crate::optimizer) logical: logical::LogicalExpr,
}

#[derive(Debug, Clone)]
pub(in crate::optimizer) struct MemoizedExpr {
    pub(in crate::optimizer) group: memo::MemoGroupId,
}
