//! Derived properties and set-shape predicates for node access sources.

use crate::{catalog, ir};

use super::{NodeAccessPlan, NodeAccessSourcePlan};

pub(super) fn hard_cardinality_upper_bound(source: &NodeAccessPlan) -> Option<usize> {
    match source {
        NodeAccessPlan::Empty => Some(0),
        NodeAccessPlan::PointIds { ids } => Some(ids.as_ref().len()),
        NodeAccessPlan::EqualityIndex { index, .. }
            if matches!(index.uniqueness, catalog::IndexUniqueness::Unique) =>
        {
            Some(1)
        }
        NodeAccessPlan::VectorSearch { k, .. } | NodeAccessPlan::TextSearch { k, .. } => {
            super::super::search_limit_hard_cardinality_upper_bound(k)
        }
        NodeAccessPlan::Intersect(plans) => plans
            .iter()
            .filter_map(NodeAccessSourcePlan::hard_cardinality_upper_bound)
            .min(),
        NodeAccessPlan::Union(plans) => plans.iter().try_fold(0usize, |sum, plan| {
            plan.hard_cardinality_upper_bound()
                .map(|upper| sum.saturating_add(upper))
        }),
        NodeAccessPlan::FromParam { .. }
        | NodeAccessPlan::FromVar { .. }
        | NodeAccessPlan::AllScan
        | NodeAccessPlan::LabelScan { .. }
        | NodeAccessPlan::EqualityIndex { .. }
        | NodeAccessPlan::RangeIndex { .. }
        | NodeAccessPlan::ScanThenFilter { .. } => None,
    }
}

pub(super) fn common_label(source: &NodeAccessPlan) -> Option<&ir::NonEmptyString> {
    match source {
        NodeAccessPlan::Intersect(plans) | NodeAccessPlan::Union(plans) => {
            super::super::common_source_label(plans.iter().map(NodeAccessSourcePlan::common_label))
        }
        plan => plan.direct_label(),
    }
}

pub(super) fn set_canonicalization_candidate(source: &NodeAccessPlan) -> bool {
    match source {
        NodeAccessPlan::Union(sources) => {
            sources.iter().any(|source| {
                matches!(
                    source.as_ref(),
                    NodeAccessPlan::Empty | NodeAccessPlan::Union(_)
                )
            }) || super::super::access_sources_have_duplicate(sources, "node_access_source:v1")
        }
        NodeAccessPlan::Intersect(sources) => {
            sources.iter().any(|source| {
                matches!(
                    source.as_ref(),
                    NodeAccessPlan::Empty | NodeAccessPlan::Intersect(_)
                )
            }) || super::super::access_sources_have_duplicate(sources, "node_access_source:v1")
        }
        NodeAccessPlan::Empty
        | NodeAccessPlan::PointIds { .. }
        | NodeAccessPlan::FromParam { .. }
        | NodeAccessPlan::FromVar { .. }
        | NodeAccessPlan::AllScan
        | NodeAccessPlan::LabelScan { .. }
        | NodeAccessPlan::EqualityIndex { .. }
        | NodeAccessPlan::RangeIndex { .. }
        | NodeAccessPlan::VectorSearch { .. }
        | NodeAccessPlan::TextSearch { .. }
        | NodeAccessPlan::ScanThenFilter { .. } => false,
    }
}

pub(super) fn set_subsumption_candidate(source: &NodeAccessPlan) -> bool {
    match source {
        NodeAccessPlan::Union(sources) => {
            super::super::union_has_subsumption_candidate(sources, NodeAccessSourcePlan::subsumes)
        }
        NodeAccessPlan::Intersect(sources) => super::super::intersection_has_subsumption_candidate(
            sources,
            NodeAccessSourcePlan::subsumes,
        ),
        NodeAccessPlan::Empty
        | NodeAccessPlan::PointIds { .. }
        | NodeAccessPlan::FromParam { .. }
        | NodeAccessPlan::FromVar { .. }
        | NodeAccessPlan::AllScan
        | NodeAccessPlan::LabelScan { .. }
        | NodeAccessPlan::EqualityIndex { .. }
        | NodeAccessPlan::RangeIndex { .. }
        | NodeAccessPlan::VectorSearch { .. }
        | NodeAccessPlan::TextSearch { .. }
        | NodeAccessPlan::ScanThenFilter { .. } => false,
    }
}

pub(super) fn subsumes(superset: &NodeAccessPlan, subset: &NodeAccessPlan) -> bool {
    if superset == subset {
        return true;
    }
    match (superset, subset) {
        (_, NodeAccessPlan::Empty) => true,
        (NodeAccessPlan::AllScan, _) => true,
        (superset, NodeAccessPlan::Union(children)) => children
            .iter()
            .all(|child| subsumes(superset, child.as_ref())),
        (superset, NodeAccessPlan::Intersect(children)) => children
            .iter()
            .any(|child| subsumes(superset, child.as_ref())),
        (NodeAccessPlan::Union(children), subset) => children
            .iter()
            .any(|child| subsumes(child.as_ref(), subset)),
        (NodeAccessPlan::Intersect(children), subset) => children
            .iter()
            .all(|child| subsumes(child.as_ref(), subset)),
        (NodeAccessPlan::LabelScan { label }, subset) => subset
            .direct_label()
            .is_some_and(|subset_label| subset_label == label),
        (
            NodeAccessPlan::RangeIndex {
                key: range_key,
                range,
                ..
            },
            NodeAccessPlan::EqualityIndex {
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
            NodeAccessPlan::RangeIndex {
                key: superset_key,
                range: superset_range,
                ..
            },
            NodeAccessPlan::RangeIndex {
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

pub(super) fn direct_label(plan: &NodeAccessPlan) -> Option<&ir::NonEmptyString> {
    match plan {
        NodeAccessPlan::LabelScan { label }
        | NodeAccessPlan::EqualityIndex {
            key: catalog::ScopedPropertyKey { label, .. },
            ..
        }
        | NodeAccessPlan::RangeIndex {
            key: catalog::ScopedPropertyDirectionKey { label, .. },
            ..
        }
        | NodeAccessPlan::VectorSearch {
            key: catalog::NodeSearchIndexKey { label, .. },
            ..
        }
        | NodeAccessPlan::TextSearch {
            key: catalog::NodeSearchIndexKey { label, .. },
            ..
        } => Some(label),
        _ => None,
    }
}
