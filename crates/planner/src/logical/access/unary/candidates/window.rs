//! Access-window scheduler predicates.

use super::super::AccessWindow;
use crate::logical;

pub(in crate::logical::access::unary) fn window_has_rewrite_candidate(
    window: &AccessWindow,
) -> bool {
    let source_kind = window.access().source_kind();
    window.window().is_identity()
        || window.window().is_empty()
        || window
            .access()
            .hard_cardinality_upper_bound()
            .is_some_and(|upper| {
                window.window().start() >= upper
                    || window.window().fully_contains_bounded_prefix(upper)
            })
        || matches!(source_kind, logical::AccessSourceKind::PointIds)
        || matches!(source_kind, logical::AccessSourceKind::Search)
            && window.window().end().is_some_and(|end| end > 0)
}
