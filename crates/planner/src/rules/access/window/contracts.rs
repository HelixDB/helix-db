//! Shared access-window rewrite contracts.

use crate::ir;

#[derive(Debug, Clone, PartialEq)]
pub(super) enum AccessWindowSourceRewrite<T> {
    Rewritten(T),
    NotApplicable(AccessWindowSourceRejection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AccessWindowSourceRejection {
    UnsupportedSource,
    PointIdsUnchanged,
    Search(SearchLimitRewriteRejection),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ElementIdsWindowRewrite {
    Sliced(ir::ElementIds),
    Unchanged,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum SearchLimitRewrite {
    Tightened(ir::SearchLimitPlan),
    NotApplicable(SearchLimitRewriteRejection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SearchLimitRewriteRejection {
    NonPrefixWindow,
    OpenEndedWindow,
    ExistingLimitTighterOrEqual,
    DynamicLimit,
}
