use std::num::NonZeroUsize;

use super::contracts::AccessWindowSourceRewrite;
use super::shared;
use crate::{catalog, ir, logical};

pub(super) fn simplify_access_window(
    source: &ir::NodeAccessSourcePlan,
    window: logical::AccessWindowRange,
) -> AccessWindowSourceRewrite<ir::NodeAccessSourcePlan> {
    shared::simplify_access_window::<NodeWindowFamily>(source, window)
}

pub(super) fn tighten_search_prefix(
    source: &ir::NodeAccessSourcePlan,
    end: NonZeroUsize,
) -> AccessWindowSourceRewrite<ir::NodeAccessSourcePlan> {
    shared::tighten_search_prefix::<NodeWindowFamily>(source, end)
}

pub(super) struct NodeWindowFamily;

impl shared::AccessWindowFamily for NodeWindowFamily {
    type Source = ir::NodeAccessSourcePlan;
    type SearchKey = catalog::NodeSearchIndexKey;

    fn hard_cardinality_upper_bound(source: &Self::Source) -> Option<usize> {
        super::super::sources::node_source_hard_cardinality_upper_bound(source)
    }

    fn empty_source() -> Self::Source {
        ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::Empty)
    }

    fn point_ids(source: &Self::Source) -> Option<&ir::ElementIds> {
        match source.as_ref() {
            ir::NodeAccessPlan::PointIds { ids } => Some(ids),
            _ => None,
        }
    }

    fn point_ids_source(ids: ir::ElementIds) -> Self::Source {
        ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::PointIds { ids })
    }

    fn search_parts(
        source: &Self::Source,
    ) -> Option<shared::AccessSearchParts<'_, Self::SearchKey>> {
        match source.as_ref() {
            ir::NodeAccessPlan::VectorSearch {
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
            ir::NodeAccessPlan::TextSearch {
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
        ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::VectorSearch {
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
        ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::TextSearch {
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

    fn source(plan: ir::NodeAccessPlan) -> ir::NodeAccessSourcePlan {
        ir::NodeAccessSourcePlan::from_unfiltered(plan)
    }

    fn limit(value: usize) -> ir::SearchLimitPlan {
        ir::SearchLimitPlan::Literal(NonZeroUsize::new(value).unwrap())
    }

    fn vector(k: ir::SearchLimitPlan) -> ir::NodeAccessPlan {
        ir::NodeAccessPlan::VectorSearch {
            key: catalog::NodeSearchIndexKey::try_new("User", "embedding").unwrap(),
            index: ir::SearchIndexPlan {
                index_id: ir::NonEmptyString::new("user_embedding").unwrap(),
                tenant: ir::SearchTenantPlan::Unscoped,
            },
            query_vector: ir::VectorQueryInputPlan::new(helix_ast::value::PropertyInput::from(
                helix_ast::value::PropertyValue::F32Array(vec![0.5]),
            ))
            .unwrap(),
            k,
        }
    }

    #[test]
    fn node_window_simplification_reports_point_id_and_unsupported_outcomes() {
        assert!(matches!(
            simplify_access_window(
                &source(ir::NodeAccessPlan::PointIds {
                    ids: ids(vec![10, 20])
                }),
                logical::AccessWindowRange::new(0, Some(2)).unwrap(),
            ),
            AccessWindowSourceRewrite::Rewritten(source)
                if matches!(source.as_ref(), ir::NodeAccessPlan::PointIds { ids } if ids.as_ref() == [10, 20])
        ));
        assert!(matches!(
            simplify_access_window(
                &source(ir::NodeAccessPlan::PointIds {
                    ids: ids(vec![10, 20])
                }),
                logical::AccessWindowRange::new(1, Some(2)).unwrap(),
            ),
            AccessWindowSourceRewrite::Rewritten(source)
                if matches!(source.as_ref(), ir::NodeAccessPlan::PointIds { ids } if ids.as_ref() == [20])
        ));
        assert_eq!(
            simplify_access_window(
                &source(ir::NodeAccessPlan::LabelScan {
                    label: ir::NonEmptyString::new("User").unwrap()
                }),
                logical::AccessWindowRange::new(1, Some(2)).unwrap(),
            ),
            AccessWindowSourceRewrite::NotApplicable(
                AccessWindowSourceRejection::UnsupportedSource
            )
        );
    }

    #[test]
    fn node_search_prefix_tightening_reports_rejections_and_rewrites() {
        assert!(matches!(
            tighten_search_prefix(&source(vector(limit(10))), NonZeroUsize::new(3).unwrap()),
            AccessWindowSourceRewrite::Rewritten(source)
                if matches!(
                    source.as_ref(),
                    ir::NodeAccessPlan::VectorSearch {
                        k: ir::SearchLimitPlan::Literal(k),
                        ..
                    } if k.get() == 3
                )
        ));
        assert_eq!(
            tighten_search_prefix(&source(vector(limit(2))), NonZeroUsize::new(3).unwrap()),
            AccessWindowSourceRewrite::NotApplicable(AccessWindowSourceRejection::Search(
                SearchLimitRewriteRejection::ExistingLimitTighterOrEqual
            ))
        );
        assert_eq!(
            tighten_search_prefix(
                &source(ir::NodeAccessPlan::AllScan),
                NonZeroUsize::new(3).unwrap()
            ),
            AccessWindowSourceRewrite::NotApplicable(
                AccessWindowSourceRejection::UnsupportedSource
            )
        );
    }
}
