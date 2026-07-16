use std::num::NonZeroUsize;

use super::contracts::{ElementIdsWindowRewrite, SearchLimitRewrite, SearchLimitRewriteRejection};
use crate::{ir, logical};

pub(super) fn access_window_ids(
    ids: &ir::ElementIds,
    window: logical::AccessWindowRange,
) -> ElementIdsWindowRewrite {
    let len = ids.as_ref().len();
    let start = window.start().min(len);
    let end = window.end().map_or(len, |end| end.min(len).max(start));
    if start == 0 && end == len {
        return ElementIdsWindowRewrite::Unchanged;
    }
    match ids.slice(start..end) {
        Some(ids) => ElementIdsWindowRewrite::Sliced(ids),
        None => ElementIdsWindowRewrite::Unchanged,
    }
}

pub(super) fn tighten_search_window_limit(
    k: &ir::SearchLimitPlan,
    window: logical::AccessWindowRange,
) -> SearchLimitRewrite {
    if window.start() != 0 {
        return SearchLimitRewrite::NotApplicable(SearchLimitRewriteRejection::NonPrefixWindow);
    }
    let Some(end) = window.end() else {
        return SearchLimitRewrite::NotApplicable(SearchLimitRewriteRejection::OpenEndedWindow);
    };
    let Some(limit) = NonZeroUsize::new(end) else {
        return SearchLimitRewrite::NotApplicable(SearchLimitRewriteRejection::OpenEndedWindow);
    };
    tighten_search_limit(k, limit)
}

pub(super) fn tighten_search_limit(
    k: &ir::SearchLimitPlan,
    limit: NonZeroUsize,
) -> SearchLimitRewrite {
    match k {
        ir::SearchLimitPlan::Literal(current) if limit < *current => {
            SearchLimitRewrite::Tightened(ir::SearchLimitPlan::Literal(limit))
        }
        ir::SearchLimitPlan::Literal(_) => SearchLimitRewrite::NotApplicable(
            SearchLimitRewriteRejection::ExistingLimitTighterOrEqual,
        ),
        ir::SearchLimitPlan::Expr(_) => {
            SearchLimitRewrite::NotApplicable(SearchLimitRewriteRejection::DynamicLimit)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(values: Vec<u64>) -> ir::ElementIds {
        ir::ElementIds::new(ir::AtLeast::<_, 1>::try_from_vec(values).unwrap()).unwrap()
    }

    fn limit(value: usize) -> ir::SearchLimitPlan {
        ir::SearchLimitPlan::Literal(NonZeroUsize::new(value).unwrap())
    }

    #[test]
    fn access_window_ids_distinguishes_slices_from_unchanged_windows() {
        let ids = ids(vec![10, 20, 30]);

        assert_eq!(
            access_window_ids(&ids, logical::AccessWindowRange::new(0, Some(3)).unwrap()),
            ElementIdsWindowRewrite::Unchanged
        );
        assert!(matches!(
            access_window_ids(&ids, logical::AccessWindowRange::new(1, None).unwrap()),
            ElementIdsWindowRewrite::Sliced(ids) if ids.as_ref() == [20, 30]
        ));
    }

    #[test]
    fn search_window_limit_reports_prefix_and_bound_rejections() {
        assert_eq!(
            tighten_search_window_limit(
                &limit(10),
                logical::AccessWindowRange::new(1, Some(3)).unwrap()
            ),
            SearchLimitRewrite::NotApplicable(SearchLimitRewriteRejection::NonPrefixWindow)
        );
        assert_eq!(
            tighten_search_window_limit(
                &limit(10),
                logical::AccessWindowRange::new(0, None).unwrap()
            ),
            SearchLimitRewrite::NotApplicable(SearchLimitRewriteRejection::OpenEndedWindow)
        );
    }

    #[test]
    fn search_limit_tightening_reports_dynamic_and_non_tighter_limits() {
        let dynamic = ir::SearchLimitPlan::Expr(
            ir::SearchLimitExprPlan::new(helix_ast::expr::Expr::param("k")).unwrap(),
        );

        assert!(matches!(
            tighten_search_limit(&limit(10), NonZeroUsize::new(3).unwrap()),
            SearchLimitRewrite::Tightened(ir::SearchLimitPlan::Literal(limit)) if limit.get() == 3
        ));
        assert_eq!(
            tighten_search_limit(&limit(3), NonZeroUsize::new(10).unwrap()),
            SearchLimitRewrite::NotApplicable(
                SearchLimitRewriteRejection::ExistingLimitTighterOrEqual
            )
        );
        assert_eq!(
            tighten_search_limit(&dynamic, NonZeroUsize::new(3).unwrap()),
            SearchLimitRewrite::NotApplicable(SearchLimitRewriteRejection::DynamicLimit)
        );
    }
}
