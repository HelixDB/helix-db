//! Outcome contracts for range-index direction rewrites.

use crate::{catalog, ir, logical, optimizer, rules};

/// Range-index direction rewrite outcome at the access-order rule boundary.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::rules::access) enum AccessOrderRangeDirectionRewrite {
    /// No opposite-direction range index can satisfy the order request.
    NotApplicable,
    /// The order can be satisfied by switching to the returned access path.
    Rewritten(logical::AccessPath),
}

impl AccessOrderRangeDirectionRewrite {
    pub(in crate::rules::access) const fn is_rewritten(&self) -> bool {
        matches!(self, Self::Rewritten(_))
    }

    pub(in crate::rules::access) fn into_rule_result(self) -> optimizer::RuleResult {
        match self {
            Self::NotApplicable => optimizer::RuleResult::NotApplicable,
            Self::Rewritten(access) => rules::access_path_result(access),
        }
    }
}

pub(super) fn direction_application_rewrite<T>(
    application: RangeDirectionRewriteApplication<T>,
    access_path: impl FnOnce(T) -> logical::AccessPath,
) -> AccessOrderRangeDirectionRewrite {
    match application {
        RangeDirectionRewriteApplication::Rewritten(source) => {
            AccessOrderRangeDirectionRewrite::Rewritten(access_path(source))
        }
        RangeDirectionRewriteApplication::NotApplicable(_reason) => {
            AccessOrderRangeDirectionRewrite::NotApplicable
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum RangeDirectionRewriteApplication<T> {
    Rewritten(T),
    NotApplicable(RangeDirectionRewriteRejection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RangeDirectionRewriteRejection {
    MultiKeyOrdering,
    NotRangeIndex,
    PropertyMismatch,
    AlreadySatisfied,
    MissingIndex,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum RangeDirectionRewriteMatch<'a> {
    Matched {
        key: &'a catalog::ScopedPropertyDirectionKey,
        range: &'a ir::IndexRange,
        direction: helix_ast::index::RangeIndexDirection,
    },
    NotApplicable(RangeDirectionRewriteRejection),
}

pub(super) fn range_direction_for_order(
    order: helix_ast::traversal::Order,
) -> helix_ast::index::RangeIndexDirection {
    match order {
        helix_ast::traversal::Order::Asc => helix_ast::index::RangeIndexDirection::Asc,
        helix_ast::traversal::Order::Desc => helix_ast::index::RangeIndexDirection::Desc,
    }
}
