//! Derived properties and set-shape predicates for edge access sources.

use crate::{catalog, ir};

use super::{EdgeAccessPlan, EdgeAccessSourcePlan};

pub(super) fn hard_cardinality_upper_bound(source: &EdgeAccessPlan) -> Option<usize> {
    match source {
        EdgeAccessPlan::Empty => Some(0),
        EdgeAccessPlan::PointIds { ids } => Some(ids.as_ref().len()),
        EdgeAccessPlan::VectorSearch { k, .. } | EdgeAccessPlan::TextSearch { k, .. } => {
            super::super::search_limit_hard_cardinality_upper_bound(k)
        }
        EdgeAccessPlan::Intersect(plans) => plans
            .iter()
            .filter_map(EdgeAccessSourcePlan::hard_cardinality_upper_bound)
            .min(),
        EdgeAccessPlan::Union(plans) => plans.iter().try_fold(0usize, |sum, plan| {
            plan.hard_cardinality_upper_bound()
                .map(|upper| sum.saturating_add(upper))
        }),
        EdgeAccessPlan::FromParam { .. }
        | EdgeAccessPlan::FromVar { .. }
        | EdgeAccessPlan::AllScan
        | EdgeAccessPlan::LabelScan { .. }
        | EdgeAccessPlan::EqualityIndex { .. }
        | EdgeAccessPlan::RangeIndex { .. }
        | EdgeAccessPlan::ScanThenFilter { .. } => None,
    }
}

pub(super) fn common_label(source: &EdgeAccessPlan) -> Option<&ir::NonEmptyString> {
    match source {
        EdgeAccessPlan::Intersect(plans) | EdgeAccessPlan::Union(plans) => {
            super::super::common_source_label(plans.iter().map(EdgeAccessSourcePlan::common_label))
        }
        plan => plan.direct_label(),
    }
}

pub(super) fn set_canonicalization_candidate(source: &EdgeAccessPlan) -> bool {
    match source {
        EdgeAccessPlan::Union(sources) => {
            sources.iter().any(|source| {
                matches!(
                    source.as_ref(),
                    EdgeAccessPlan::Empty | EdgeAccessPlan::Union(_)
                )
            }) || super::super::access_sources_have_duplicate(sources, "edge_access_source:v1")
        }
        EdgeAccessPlan::Intersect(sources) => {
            sources.iter().any(|source| {
                matches!(
                    source.as_ref(),
                    EdgeAccessPlan::Empty | EdgeAccessPlan::Intersect(_)
                )
            }) || super::super::access_sources_have_duplicate(sources, "edge_access_source:v1")
        }
        EdgeAccessPlan::Empty
        | EdgeAccessPlan::PointIds { .. }
        | EdgeAccessPlan::FromParam { .. }
        | EdgeAccessPlan::FromVar { .. }
        | EdgeAccessPlan::AllScan
        | EdgeAccessPlan::LabelScan { .. }
        | EdgeAccessPlan::EqualityIndex { .. }
        | EdgeAccessPlan::RangeIndex { .. }
        | EdgeAccessPlan::VectorSearch { .. }
        | EdgeAccessPlan::TextSearch { .. }
        | EdgeAccessPlan::ScanThenFilter { .. } => false,
    }
}

pub(super) fn set_subsumption_candidate(source: &EdgeAccessPlan) -> bool {
    match source {
        EdgeAccessPlan::Union(sources) => {
            super::super::union_has_subsumption_candidate(sources, EdgeAccessSourcePlan::subsumes)
        }
        EdgeAccessPlan::Intersect(sources) => super::super::intersection_has_subsumption_candidate(
            sources,
            EdgeAccessSourcePlan::subsumes,
        ),
        EdgeAccessPlan::Empty
        | EdgeAccessPlan::PointIds { .. }
        | EdgeAccessPlan::FromParam { .. }
        | EdgeAccessPlan::FromVar { .. }
        | EdgeAccessPlan::AllScan
        | EdgeAccessPlan::LabelScan { .. }
        | EdgeAccessPlan::EqualityIndex { .. }
        | EdgeAccessPlan::RangeIndex { .. }
        | EdgeAccessPlan::VectorSearch { .. }
        | EdgeAccessPlan::TextSearch { .. }
        | EdgeAccessPlan::ScanThenFilter { .. } => false,
    }
}

pub(super) fn subsumes(superset: &EdgeAccessPlan, subset: &EdgeAccessPlan) -> bool {
    if superset == subset {
        return true;
    }
    match (superset, subset) {
        (_, EdgeAccessPlan::Empty) => true,
        (EdgeAccessPlan::AllScan, _) => true,
        (superset, EdgeAccessPlan::Union(children)) => children
            .iter()
            .all(|child| subsumes(superset, child.as_ref())),
        (superset, EdgeAccessPlan::Intersect(children)) => children
            .iter()
            .any(|child| subsumes(superset, child.as_ref())),
        (EdgeAccessPlan::Union(children), subset) => children
            .iter()
            .any(|child| subsumes(child.as_ref(), subset)),
        (EdgeAccessPlan::Intersect(children), subset) => children
            .iter()
            .all(|child| subsumes(child.as_ref(), subset)),
        (EdgeAccessPlan::LabelScan { label }, subset) => subset
            .direct_label()
            .is_some_and(|subset_label| subset_label == label),
        (
            EdgeAccessPlan::RangeIndex {
                key: range_key,
                range,
                ..
            },
            EdgeAccessPlan::EqualityIndex {
                key: equality_key,
                value: ir::IndexValue::Literal(value),
                ..
            },
        ) => {
            range_key.label == equality_key.label
                && range_key.property == equality_key.property
                && range.contains_secondary_literal(value)
        }
        (
            EdgeAccessPlan::RangeIndex {
                key: superset_key,
                range: superset_range,
                ..
            },
            EdgeAccessPlan::RangeIndex {
                key: subset_key,
                range: subset_range,
                ..
            },
        ) => {
            superset_key.label == subset_key.label
                && superset_key.property == subset_key.property
                && superset_range.contains_range(subset_range)
        }
        _ => false,
    }
}

pub(super) fn direct_label(plan: &EdgeAccessPlan) -> Option<&ir::NonEmptyString> {
    match plan {
        EdgeAccessPlan::LabelScan { label }
        | EdgeAccessPlan::EqualityIndex {
            key: catalog::ScopedPropertyKey { label, .. },
            ..
        }
        | EdgeAccessPlan::RangeIndex {
            key: catalog::ScopedPropertyDirectionKey { label, .. },
            ..
        }
        | EdgeAccessPlan::VectorSearch {
            key: catalog::EdgeSearchIndexKey { label, .. },
            ..
        }
        | EdgeAccessPlan::TextSearch {
            key: catalog::EdgeSearchIndexKey { label, .. },
            ..
        } => Some(label),
        _ => None,
    }
}
