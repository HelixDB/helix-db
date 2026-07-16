use std::num::NonZeroUsize;

use super::contracts::AccessWindowSourceRewrite;
use super::shared;
use crate::{catalog, ir, logical};

pub(super) fn simplify_access_window(
    source: &ir::EdgeAccessSourcePlan,
    window: logical::AccessWindowRange,
) -> AccessWindowSourceRewrite<ir::EdgeAccessSourcePlan> {
    shared::simplify_access_window::<EdgeWindowFamily>(source, window)
}

pub(super) fn tighten_search_prefix(
    source: &ir::EdgeAccessSourcePlan,
    end: NonZeroUsize,
) -> AccessWindowSourceRewrite<ir::EdgeAccessSourcePlan> {
    shared::tighten_search_prefix::<EdgeWindowFamily>(source, end)
}

pub(super) struct EdgeWindowFamily;

impl shared::AccessWindowFamily for EdgeWindowFamily {
    type Source = ir::EdgeAccessSourcePlan;
    type SearchKey = catalog::EdgeSearchIndexKey;

    fn hard_cardinality_upper_bound(source: &Self::Source) -> Option<usize> {
        super::super::sources::edge_source_hard_cardinality_upper_bound(source)
    }

    fn empty_source() -> Self::Source {
        ir::EdgeAccessSourcePlan::from_unfiltered(ir::EdgeAccessPlan::Empty)
    }

    fn point_ids(source: &Self::Source) -> Option<&ir::ElementIds> {
        match source.as_ref() {
            ir::EdgeAccessPlan::PointIds { ids } => Some(ids),
            _ => None,
        }
    }

    fn point_ids_source(ids: ir::ElementIds) -> Self::Source {
        ir::EdgeAccessSourcePlan::from_unfiltered(ir::EdgeAccessPlan::PointIds { ids })
    }

    fn search_parts(
        source: &Self::Source,
    ) -> Option<shared::AccessSearchParts<'_, Self::SearchKey>> {
        match source.as_ref() {
            ir::EdgeAccessPlan::VectorSearch {
                key,
                index,
                query_vector,
                k,
            } => Some(shared::AccessSearchParts::Vector {
                key,
                index,
                query_vector,
                k,
            }),
            ir::EdgeAccessPlan::TextSearch {
                key,
                index,
                query_text,
                k,
            } => Some(shared::AccessSearchParts::Text {
                key,
                index,
                query_text,
                k,
            }),
            _ => None,
        }
    }

    fn vector_search_source(
        key: Self::SearchKey,
        index: ir::SearchIndexPlan,
        query_vector: ir::VectorQueryInputPlan,
        k: ir::SearchLimitPlan,
    ) -> Self::Source {
        ir::EdgeAccessSourcePlan::from_unfiltered(ir::EdgeAccessPlan::VectorSearch {
            key,
            index,
            query_vector,
            k,
        })
    }

    fn text_search_source(
        key: Self::SearchKey,
        index: ir::SearchIndexPlan,
        query_text: ir::TextQueryInputPlan,
        k: ir::SearchLimitPlan,
    ) -> Self::Source {
        ir::EdgeAccessSourcePlan::from_unfiltered(ir::EdgeAccessPlan::TextSearch {
            key,
            index,
            query_text,
            k,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::contracts::{
        AccessWindowSourceRejection, AccessWindowSourceRewrite, SearchLimitRewriteRejection,
    };
    use super::*;

    fn ids(values: Vec<u64>) -> ir::ElementIds {
        ir::ElementIds::new(ir::AtLeast::<_, 1>::try_from_vec(values).unwrap()).unwrap()
    }

    fn source(plan: ir::EdgeAccessPlan) -> ir::EdgeAccessSourcePlan {
        ir::EdgeAccessSourcePlan::from_unfiltered(plan)
    }

    fn limit(value: usize) -> ir::SearchLimitPlan {
        ir::SearchLimitPlan::Literal(NonZeroUsize::new(value).unwrap())
    }

    fn text(k: ir::SearchLimitPlan) -> ir::EdgeAccessPlan {
        ir::EdgeAccessPlan::TextSearch {
            key: catalog::EdgeSearchIndexKey::try_new("LIKES", "comment").unwrap(),
            index: ir::SearchIndexPlan {
                index_id: ir::NonEmptyString::new("likes_comment").unwrap(),
                tenant: ir::SearchTenantPlan::Unscoped,
            },
            query_text: ir::TextQueryInputPlan::new(helix_ast::value::PropertyInput::from(
                helix_ast::value::PropertyValue::from("great"),
            ))
            .unwrap(),
            k,
        }
    }

    #[test]
    fn edge_window_simplification_reports_point_id_and_unsupported_outcomes() {
        assert!(matches!(
            simplify_access_window(
                &source(ir::EdgeAccessPlan::PointIds {
                    ids: ids(vec![10, 20])
                }),
                logical::AccessWindowRange::new(0, Some(2)).unwrap(),
            ),
            AccessWindowSourceRewrite::Rewritten(source)
                if matches!(source.as_ref(), ir::EdgeAccessPlan::PointIds { ids } if ids.as_ref() == [10, 20])
        ));
        assert!(matches!(
            simplify_access_window(
                &source(ir::EdgeAccessPlan::PointIds {
                    ids: ids(vec![10, 20])
                }),
                logical::AccessWindowRange::new(1, Some(2)).unwrap(),
            ),
            AccessWindowSourceRewrite::Rewritten(source)
                if matches!(source.as_ref(), ir::EdgeAccessPlan::PointIds { ids } if ids.as_ref() == [20])
        ));
        assert_eq!(
            simplify_access_window(
                &source(ir::EdgeAccessPlan::LabelScan {
                    label: ir::NonEmptyString::new("LIKES").unwrap()
                }),
                logical::AccessWindowRange::new(1, Some(2)).unwrap(),
            ),
            AccessWindowSourceRewrite::NotApplicable(
                AccessWindowSourceRejection::UnsupportedSource
            )
        );
    }

    #[test]
    fn edge_search_prefix_tightening_reports_rejections_and_rewrites() {
        assert!(matches!(
            tighten_search_prefix(&source(text(limit(10))), NonZeroUsize::new(3).unwrap()),
            AccessWindowSourceRewrite::Rewritten(source)
                if matches!(
                    source.as_ref(),
                    ir::EdgeAccessPlan::TextSearch {
                        k: ir::SearchLimitPlan::Literal(k),
                        ..
                    } if k.get() == 3
                )
        ));
        assert_eq!(
            tighten_search_prefix(&source(text(limit(2))), NonZeroUsize::new(3).unwrap()),
            AccessWindowSourceRewrite::NotApplicable(AccessWindowSourceRejection::Search(
                SearchLimitRewriteRejection::ExistingLimitTighterOrEqual
            ))
        );
        assert_eq!(
            tighten_search_prefix(
                &source(ir::EdgeAccessPlan::AllScan),
                NonZeroUsize::new(3).unwrap()
            ),
            AccessWindowSourceRewrite::NotApplicable(
                AccessWindowSourceRejection::UnsupportedSource
            )
        );
    }
}
