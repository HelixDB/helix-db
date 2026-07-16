//! Shared access-window source rewrite flow.

use std::num::NonZeroUsize;

use super::contracts::{
    AccessWindowSourceRejection, AccessWindowSourceRewrite, ElementIdsWindowRewrite,
    SearchLimitRewrite,
};
use super::support;
use crate::{ir, logical};

pub(super) enum AccessSearchParts<'a, K> {
    Vector {
        key: &'a K,
        index: &'a ir::SearchIndexPlan,
        query_vector: &'a ir::VectorQueryInputPlan,
        k: &'a ir::SearchLimitPlan,
    },
    Text {
        key: &'a K,
        index: &'a ir::SearchIndexPlan,
        query_text: &'a ir::TextQueryInputPlan,
        k: &'a ir::SearchLimitPlan,
    },
}

pub(super) trait AccessWindowFamily {
    type Source: Clone;
    type SearchKey: Clone;

    fn hard_cardinality_upper_bound(source: &Self::Source) -> Option<usize>;
    fn empty_source() -> Self::Source;
    fn point_ids(source: &Self::Source) -> Option<&ir::ElementIds>;
    fn point_ids_source(ids: ir::ElementIds) -> Self::Source;
    fn search_parts(source: &Self::Source) -> Option<AccessSearchParts<'_, Self::SearchKey>>;
    fn vector_search_source(
        key: Self::SearchKey,
        index: ir::SearchIndexPlan,
        query_vector: ir::VectorQueryInputPlan,
        k: ir::SearchLimitPlan,
    ) -> Self::Source;
    fn text_search_source(
        key: Self::SearchKey,
        index: ir::SearchIndexPlan,
        query_text: ir::TextQueryInputPlan,
        k: ir::SearchLimitPlan,
    ) -> Self::Source;
}

pub(super) fn simplify_access_window<F>(
    source: &F::Source,
    window: logical::AccessWindowRange,
) -> AccessWindowSourceRewrite<F::Source>
where
    F: AccessWindowFamily,
{
    if window.is_identity() {
        return AccessWindowSourceRewrite::Rewritten(source.clone());
    }
    if window.is_empty() || access_window_exhausts_source::<F>(source, window) {
        return AccessWindowSourceRewrite::Rewritten(F::empty_source());
    }
    if access_window_covers_source::<F>(source, window) {
        return AccessWindowSourceRewrite::Rewritten(source.clone());
    }
    if let Some(ids) = F::point_ids(source) {
        return rewrite_point_ids_window::<F>(ids, window);
    }
    tighten_search_window::<F>(source, window)
}

pub(super) fn tighten_search_prefix<F>(
    source: &F::Source,
    end: NonZeroUsize,
) -> AccessWindowSourceRewrite<F::Source>
where
    F: AccessWindowFamily,
{
    let Some(parts) = F::search_parts(source) else {
        return AccessWindowSourceRewrite::NotApplicable(
            AccessWindowSourceRejection::UnsupportedSource,
        );
    };
    support::tighten_search_limit(search_limit(&parts), end)
        .map_rewritten(|k| search_source::<F>(parts, k))
}

fn access_window_exhausts_source<F>(source: &F::Source, window: logical::AccessWindowRange) -> bool
where
    F: AccessWindowFamily,
{
    F::hard_cardinality_upper_bound(source).is_some_and(|upper| window.start() >= upper)
}

fn access_window_covers_source<F>(source: &F::Source, window: logical::AccessWindowRange) -> bool
where
    F: AccessWindowFamily,
{
    F::hard_cardinality_upper_bound(source)
        .is_some_and(|upper| window.fully_contains_bounded_prefix(upper))
}

fn rewrite_point_ids_window<F>(
    ids: &ir::ElementIds,
    window: logical::AccessWindowRange,
) -> AccessWindowSourceRewrite<F::Source>
where
    F: AccessWindowFamily,
{
    match support::access_window_ids(ids, window) {
        ElementIdsWindowRewrite::Sliced(ids) => {
            AccessWindowSourceRewrite::Rewritten(F::point_ids_source(ids))
        }
        ElementIdsWindowRewrite::Unchanged => {
            AccessWindowSourceRewrite::NotApplicable(AccessWindowSourceRejection::PointIdsUnchanged)
        }
    }
}

fn tighten_search_window<F>(
    source: &F::Source,
    window: logical::AccessWindowRange,
) -> AccessWindowSourceRewrite<F::Source>
where
    F: AccessWindowFamily,
{
    let Some(parts) = F::search_parts(source) else {
        return AccessWindowSourceRewrite::NotApplicable(
            AccessWindowSourceRejection::UnsupportedSource,
        );
    };
    support::tighten_search_window_limit(search_limit(&parts), window)
        .map_rewritten(|k| search_source::<F>(parts, k))
}

fn search_limit<'a, K>(parts: &'a AccessSearchParts<'_, K>) -> &'a ir::SearchLimitPlan {
    match parts {
        AccessSearchParts::Vector { k, .. } | AccessSearchParts::Text { k, .. } => k,
    }
}

fn search_source<F>(parts: AccessSearchParts<'_, F::SearchKey>, k: ir::SearchLimitPlan) -> F::Source
where
    F: AccessWindowFamily,
{
    match parts {
        AccessSearchParts::Vector {
            key,
            index,
            query_vector,
            ..
        } => F::vector_search_source(key.clone(), index.clone(), query_vector.clone(), k),
        AccessSearchParts::Text {
            key,
            index,
            query_text,
            ..
        } => F::text_search_source(key.clone(), index.clone(), query_text.clone(), k),
    }
}

trait SearchLimitRewriteExt<T> {
    fn map_rewritten(
        self,
        f: impl FnOnce(ir::SearchLimitPlan) -> T,
    ) -> AccessWindowSourceRewrite<T>;
}

impl<T> SearchLimitRewriteExt<T> for SearchLimitRewrite {
    fn map_rewritten(
        self,
        f: impl FnOnce(ir::SearchLimitPlan) -> T,
    ) -> AccessWindowSourceRewrite<T> {
        match self {
            SearchLimitRewrite::Tightened(k) => AccessWindowSourceRewrite::Rewritten(f(k)),
            SearchLimitRewrite::NotApplicable(reason) => AccessWindowSourceRewrite::NotApplicable(
                AccessWindowSourceRejection::Search(reason),
            ),
        }
    }
}
