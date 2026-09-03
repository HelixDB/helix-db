//! Search read-limit composition contracts.

use helix_planner::{ir, properties};

use super::*;

#[derive(Debug, Clone, Copy)]
pub(in crate::execution::interpreter) struct SearchReadLimit<'a> {
    pub(in crate::execution::interpreter::access::search) search_limit: &'a ir::SearchLimitPlan,
    pub(in crate::execution::interpreter::access::search) access_limit:
        Option<properties::PositiveUsize>,
}

impl<'a> SearchReadLimit<'a> {
    pub(in crate::execution::interpreter) const fn new(
        search_limit: &'a ir::SearchLimitPlan,
        access_limit: Option<properties::PositiveUsize>,
    ) -> Self {
        Self {
            search_limit,
            access_limit,
        }
    }
}

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter::access::search) async fn effective_search_limit(
        &self,
        limit: SearchReadLimit<'_>,
    ) -> Result<usize> {
        Ok(limited_search_k(
            self.search_limit(limit.search_limit).await?,
            limit.access_limit,
        ))
    }
}

/// Hard ceiling applied to every search result count that has no narrower
/// schema-level `access_limit`.
///
/// A client-supplied `k` otherwise passes straight into `SearchParams`/the
/// text-manifest search with no upper bound: `ResultCount::try_new` only
/// rejects zero, and `effective_search_limit` only tightens `k` when the
/// index definition happens to configure an `access_limit`. For vector
/// search this drives the HNSW beam width (`ef = k.max(100)`) directly,
/// letting an unauthenticated caller force a full frontier expansion over
/// the entire index with a single large `k`. Mirrors the value the
/// restricted vector-search path already enforces unconditionally via
/// `MAX_RESTRICTED_RESULT_COUNT` in `crate::search::vector::restricted`.
const DEFAULT_MAX_SEARCH_RESULT_COUNT: usize = 800;

pub(in crate::execution::interpreter::access) fn limited_search_k(
    k: usize,
    limit: Option<properties::PositiveUsize>,
) -> usize {
    let k = k.min(DEFAULT_MAX_SEARCH_RESULT_COUNT);
    limit.map(|limit| k.min(limit.get())).unwrap_or(k)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_read_limit_preserves_search_limit_and_optional_access_cap() {
        let search_limit = ir::SearchLimitPlan::new(helix_ast::expr::StreamBound::Literal(10))
            .expect("positive search limit");
        let access_limit = properties::PositiveUsize::new(3);
        let limit = SearchReadLimit::new(&search_limit, access_limit);

        assert!(std::ptr::eq(limit.search_limit, &search_limit));
        assert_eq!(limit.access_limit.map(|value| value.get()), Some(3));
    }

    #[test]
    fn limited_search_k_tightens_only_when_access_cap_is_lower() {
        assert_eq!(limited_search_k(10, None), 10);
        assert_eq!(limited_search_k(10, properties::PositiveUsize::new(3)), 3);
        assert_eq!(limited_search_k(3, properties::PositiveUsize::new(10)), 3);
        assert_eq!(
            limited_search_k(1_000, properties::PositiveUsize::new(800)),
            800
        );
    }

    #[test]
    fn limited_search_k_applies_the_default_ceiling_with_no_access_limit() {
        assert_eq!(
            limited_search_k(DEFAULT_MAX_SEARCH_RESULT_COUNT, None),
            DEFAULT_MAX_SEARCH_RESULT_COUNT
        );
        assert_eq!(
            limited_search_k(DEFAULT_MAX_SEARCH_RESULT_COUNT + 1, None),
            DEFAULT_MAX_SEARCH_RESULT_COUNT
        );
        assert_eq!(
            limited_search_k(usize::MAX, None),
            DEFAULT_MAX_SEARCH_RESULT_COUNT
        );
    }

    #[test]
    fn limited_search_k_lets_a_narrower_access_limit_still_win_under_the_default_ceiling() {
        let narrower = properties::PositiveUsize::new(DEFAULT_MAX_SEARCH_RESULT_COUNT - 1)
            .expect("nonzero access limit");
        assert_eq!(
            limited_search_k(usize::MAX, Some(narrower)),
            DEFAULT_MAX_SEARCH_RESULT_COUNT - 1
        );
    }

    #[test]
    fn limited_search_k_ignores_an_access_limit_looser_than_the_default_ceiling() {
        let looser = properties::PositiveUsize::new(DEFAULT_MAX_SEARCH_RESULT_COUNT * 10)
            .expect("nonzero access limit");
        assert_eq!(
            limited_search_k(usize::MAX, Some(looser)),
            DEFAULT_MAX_SEARCH_RESULT_COUNT
        );
    }
}
