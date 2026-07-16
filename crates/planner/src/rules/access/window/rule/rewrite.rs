//! Access-window logical rewrite outcomes.

use super::super::super::super::{access_path_result, access_window_result};
use super::super::contracts::{AccessWindowSourceRejection, AccessWindowSourceRewrite};
use super::super::{edge, node};
use crate::{logical, optimizer};

#[derive(Debug, Clone, PartialEq)]
pub(super) enum AccessWindowRewrite {
    NotApplicable,
    FoldedAccess(logical::AccessPath),
    TightenedWindow(logical::AccessWindow),
}

impl AccessWindowRewrite {
    pub(super) const fn is_folded_access(&self) -> bool {
        matches!(self, Self::FoldedAccess(_))
    }

    pub(super) fn into_rule_result(self) -> optimizer::RuleResult {
        match self {
            Self::NotApplicable => optimizer::RuleResult::NotApplicable,
            Self::FoldedAccess(access) => access_path_result(access),
            Self::TightenedWindow(window) => access_window_result(window),
        }
    }
}

pub(super) fn rewrite_access_window(window: &logical::AccessWindow) -> AccessWindowRewrite {
    match fold_access_window(window) {
        AccessWindowFold::Folded(access) => return AccessWindowRewrite::FoldedAccess(access),
        AccessWindowFold::NotFolded => {}
    }
    match tighten_access_window_search_prefix(window) {
        AccessWindowPrefixRewrite::Tightened(window) => {
            AccessWindowRewrite::TightenedWindow(window)
        }
        AccessWindowPrefixRewrite::NotApplicable(_reason) => AccessWindowRewrite::NotApplicable,
    }
}

#[derive(Debug, Clone, PartialEq)]
enum AccessWindowFold {
    NotFolded,
    Folded(logical::AccessPath),
}

fn fold_access_window(window: &logical::AccessWindow) -> AccessWindowFold {
    match window.access() {
        logical::AccessPath::Node(path) => access_window_fold(
            node::simplify_access_window(path.source(), window.window()),
            |source| logical::AccessPath::Node(logical::NodeAccessPath::new(source)),
        ),
        logical::AccessPath::Edge(path) => access_window_fold(
            edge::simplify_access_window(path.source(), window.window()),
            |source| logical::AccessPath::Edge(logical::EdgeAccessPath::new(source)),
        ),
    }
}

fn access_window_fold<T>(
    rewrite: AccessWindowSourceRewrite<T>,
    access_path: impl FnOnce(T) -> logical::AccessPath,
) -> AccessWindowFold {
    match rewrite {
        AccessWindowSourceRewrite::Rewritten(source) => {
            AccessWindowFold::Folded(access_path(source))
        }
        AccessWindowSourceRewrite::NotApplicable(_reason) => AccessWindowFold::NotFolded,
    }
}

#[derive(Debug, Clone, PartialEq)]
enum AccessWindowPrefixRewrite {
    Tightened(logical::AccessWindow),
    NotApplicable(AccessWindowPrefixRejection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AccessWindowPrefixRejection {
    MissingEnd,
    Source(AccessWindowSourceRejection),
}

fn tighten_access_window_search_prefix(
    window: &logical::AccessWindow,
) -> AccessWindowPrefixRewrite {
    let Some(end) = window.window().end().and_then(std::num::NonZeroUsize::new) else {
        return AccessWindowPrefixRewrite::NotApplicable(AccessWindowPrefixRejection::MissingEnd);
    };
    match window.access() {
        logical::AccessPath::Node(path) => {
            prefix_rewrite(node::tighten_search_prefix(path.source(), end), |source| {
                logical::AccessWindow::new(
                    logical::AccessPath::Node(logical::NodeAccessPath::new(source)),
                    window.window(),
                )
            })
        }
        logical::AccessPath::Edge(path) => {
            prefix_rewrite(edge::tighten_search_prefix(path.source(), end), |source| {
                logical::AccessWindow::new(
                    logical::AccessPath::Edge(logical::EdgeAccessPath::new(source)),
                    window.window(),
                )
            })
        }
    }
}

fn prefix_rewrite<T>(
    rewrite: AccessWindowSourceRewrite<T>,
    window: impl FnOnce(T) -> logical::AccessWindow,
) -> AccessWindowPrefixRewrite {
    match rewrite {
        AccessWindowSourceRewrite::Rewritten(source) => {
            AccessWindowPrefixRewrite::Tightened(window(source))
        }
        AccessWindowSourceRewrite::NotApplicable(reason) => {
            AccessWindowPrefixRewrite::NotApplicable(AccessWindowPrefixRejection::Source(reason))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir;

    fn node_access(plan: ir::NodeAccessPlan) -> logical::AccessPath {
        logical::AccessPath::Node(logical::NodeAccessPath::new(
            ir::NodeAccessSourcePlan::from_unfiltered(plan),
        ))
    }

    #[test]
    fn access_window_rewrite_distinguishes_folded_access_from_no_rewrite() {
        let folded = logical::AccessWindow::new(
            node_access(ir::NodeAccessPlan::AllScan),
            logical::AccessWindowRange::new(0, None).unwrap(),
        );
        let unchanged = logical::AccessWindow::new(
            node_access(ir::NodeAccessPlan::AllScan),
            logical::AccessWindowRange::new(1, Some(3)).unwrap(),
        );

        assert!(rewrite_access_window(&folded).is_folded_access());
        assert_eq!(
            rewrite_access_window(&unchanged),
            AccessWindowRewrite::NotApplicable
        );
    }

    #[test]
    fn access_window_rewrite_result_conversion_preserves_not_applicable() {
        assert_eq!(
            AccessWindowRewrite::NotApplicable.into_rule_result(),
            optimizer::RuleResult::NotApplicable
        );
    }

    #[test]
    fn access_window_prefix_rewrite_reports_missing_end_and_source_rejections() {
        let open_ended = logical::AccessWindow::new(
            node_access(ir::NodeAccessPlan::AllScan),
            logical::AccessWindowRange::new(1, None).unwrap(),
        );
        assert_eq!(
            tighten_access_window_search_prefix(&open_ended),
            AccessWindowPrefixRewrite::NotApplicable(AccessWindowPrefixRejection::MissingEnd)
        );

        let unsupported = logical::AccessWindow::new(
            node_access(ir::NodeAccessPlan::AllScan),
            logical::AccessWindowRange::new(1, Some(3)).unwrap(),
        );
        assert_eq!(
            tighten_access_window_search_prefix(&unsupported),
            AccessWindowPrefixRewrite::NotApplicable(AccessWindowPrefixRejection::Source(
                AccessWindowSourceRejection::UnsupportedSource
            ))
        );
    }
}
