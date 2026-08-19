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

pub(in crate::execution::interpreter::access) fn limited_search_k(
    k: usize,
    limit: Option<properties::PositiveUsize>,
) -> usize {
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
}
