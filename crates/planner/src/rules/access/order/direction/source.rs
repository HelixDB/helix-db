//! Source-shape proofs for range-index direction rewrites.

use super::contracts;
use crate::{catalog, ir};

pub(super) trait RangeDirectionAccessPlan {
    fn range_index_parts(&self) -> RangeDirectionSource<'_>;
}

impl RangeDirectionAccessPlan for ir::NodeAccessPlan {
    fn range_index_parts(&self) -> RangeDirectionSource<'_> {
        match self {
            ir::NodeAccessPlan::RangeIndex { key, range, .. } => {
                RangeDirectionSource::RangeIndex { key, range }
            }
            _ => RangeDirectionSource::Other,
        }
    }
}

impl RangeDirectionAccessPlan for ir::EdgeAccessPlan {
    fn range_index_parts(&self) -> RangeDirectionSource<'_> {
        match self {
            ir::EdgeAccessPlan::RangeIndex { key, range, .. } => {
                RangeDirectionSource::RangeIndex { key, range }
            }
            _ => RangeDirectionSource::Other,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum RangeDirectionSource<'a> {
    RangeIndex {
        key: &'a catalog::ScopedPropertyDirectionKey,
        range: &'a ir::IndexRange,
    },
    Other,
}

pub(super) fn matchable_range_direction_source<'a>(
    source: &'a impl RangeDirectionAccessPlan,
    ordering: &ir::OrderKeys,
) -> contracts::RangeDirectionRewriteMatch<'a> {
    let [required] = ordering.as_ref() else {
        return contracts::RangeDirectionRewriteMatch::NotApplicable(
            contracts::RangeDirectionRewriteRejection::MultiKeyOrdering,
        );
    };
    let (key, range) = match source.range_index_parts() {
        RangeDirectionSource::RangeIndex { key, range } => (key, range),
        RangeDirectionSource::Other => {
            return contracts::RangeDirectionRewriteMatch::NotApplicable(
                contracts::RangeDirectionRewriteRejection::NotRangeIndex,
            );
        }
    };
    if key.property != required.property {
        return contracts::RangeDirectionRewriteMatch::NotApplicable(
            contracts::RangeDirectionRewriteRejection::PropertyMismatch,
        );
    }
    let direction = contracts::range_direction_for_order(required.order);
    if key.direction == direction {
        return contracts::RangeDirectionRewriteMatch::NotApplicable(
            contracts::RangeDirectionRewriteRejection::AlreadySatisfied,
        );
    }
    contracts::RangeDirectionRewriteMatch::Matched {
        key,
        range,
        direction,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(value: &str) -> ir::NonEmptyString {
        ir::NonEmptyString::new(value).unwrap()
    }

    fn lower_range(value: i64) -> ir::IndexRange {
        ir::IndexRange::Lower {
            lower: ir::IndexBound::Inclusive(
                ir::RangeIndexValue::literal(helix_ast::value::PropertyValue::from(value)).unwrap(),
            ),
        }
    }

    fn node_source(plan: ir::NodeAccessPlan) -> ir::NodeAccessSourcePlan {
        ir::NodeAccessSourcePlan::from_unfiltered(plan)
    }

    fn node_range_source(
        property: &str,
        direction: helix_ast::index::RangeIndexDirection,
    ) -> ir::NodeAccessSourcePlan {
        node_source(ir::NodeAccessPlan::RangeIndex {
            index: catalog::NodeRangeIndexMeta::try_new("node_range").unwrap(),
            key: catalog::ScopedPropertyDirectionKey::try_new("User", property, direction).unwrap(),
            range: lower_range(18),
        })
    }

    fn desc_age_ordering() -> ir::OrderKeys {
        ir::OrderKeys::from(ir::OrderKey {
            property: name("age"),
            order: helix_ast::traversal::Order::Desc,
        })
    }

    fn two_key_ordering() -> ir::OrderKeys {
        ir::OrderKeys::new(ir::AtLeast::<_, 1>::from_one_and_rest(
            ir::OrderKey {
                property: name("age"),
                order: helix_ast::traversal::Order::Desc,
            },
            vec![ir::OrderKey {
                property: name("name"),
                order: helix_ast::traversal::Order::Asc,
            }],
        ))
        .unwrap()
    }

    #[test]
    fn range_direction_source_match_reports_rejection_reasons() {
        assert_eq!(
            matchable_range_direction_source(
                node_range_source("age", helix_ast::index::RangeIndexDirection::Asc).as_ref(),
                &two_key_ordering(),
            ),
            contracts::RangeDirectionRewriteMatch::NotApplicable(
                contracts::RangeDirectionRewriteRejection::MultiKeyOrdering
            )
        );
        assert_eq!(
            matchable_range_direction_source(
                node_source(ir::NodeAccessPlan::AllScan).as_ref(),
                &desc_age_ordering(),
            ),
            contracts::RangeDirectionRewriteMatch::NotApplicable(
                contracts::RangeDirectionRewriteRejection::NotRangeIndex
            )
        );
        assert_eq!(
            matchable_range_direction_source(
                node_range_source("score", helix_ast::index::RangeIndexDirection::Asc).as_ref(),
                &desc_age_ordering(),
            ),
            contracts::RangeDirectionRewriteMatch::NotApplicable(
                contracts::RangeDirectionRewriteRejection::PropertyMismatch
            )
        );
        assert_eq!(
            matchable_range_direction_source(
                node_range_source("age", helix_ast::index::RangeIndexDirection::Desc).as_ref(),
                &desc_age_ordering(),
            ),
            contracts::RangeDirectionRewriteMatch::NotApplicable(
                contracts::RangeDirectionRewriteRejection::AlreadySatisfied
            )
        );
        assert!(matches!(
            matchable_range_direction_source(
                node_range_source("age", helix_ast::index::RangeIndexDirection::Asc).as_ref(),
                &desc_age_ordering(),
            ),
            contracts::RangeDirectionRewriteMatch::Matched {
                direction: helix_ast::index::RangeIndexDirection::Desc,
                ..
            }
        ));
    }
}
