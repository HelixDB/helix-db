//! Edge same-key range-intersection tightening.

use super::super::*;
use super::merge;

pub(super) fn simplify(plan: &ir::EdgeAccessPlan) -> AccessSetPlanRewrite<ir::EdgeAccessPlan> {
    let ir::EdgeAccessPlan::Intersect(plans) = plan else {
        return AccessSetPlanRewrite::NotApplicable;
    };
    match merge::merge_intersection_sources(plans, range_key_digest, merge_range_sources) {
        merge::RangeIntersectionMerge::Merged(sources) => {
            AccessSetPlanRewrite::Rewritten(edge_intersection_from_sources(sources))
        }
        merge::RangeIntersectionMerge::Unchanged(_) => AccessSetPlanRewrite::NotApplicable,
    }
}

pub(super) fn has_candidate(plan: &ir::EdgeAccessPlan) -> bool {
    let ir::EdgeAccessPlan::Intersect(plans) = plan else {
        return false;
    };
    plans.iter().enumerate().any(|(index, left)| {
        plans
            .iter()
            .skip(index + 1)
            .any(|right| range_sources_can_merge(left, right))
    })
}

fn range_key_digest(source: &ir::EdgeAccessSourcePlan) -> merge::RangeMergeKey {
    match source.as_ref() {
        ir::EdgeAccessPlan::RangeIndex { key, .. } => merge::RangeMergeKey::Key(
            digest::PlanDigest::for_tagged_value("edge_range_intersection_key:v1", key),
        ),
        _ => merge::RangeMergeKey::NotRange,
    }
}

fn range_sources_can_merge(
    left: &ir::EdgeAccessSourcePlan,
    right: &ir::EdgeAccessSourcePlan,
) -> bool {
    match (left.as_ref(), right.as_ref()) {
        (
            ir::EdgeAccessPlan::RangeIndex {
                key,
                range: left_range,
                ..
            },
            ir::EdgeAccessPlan::RangeIndex {
                key: right_key,
                range: right_range,
                ..
            },
        ) if key == right_key => left_range.intersect(right_range).is_some(),
        _ => false,
    }
}

fn merge_range_sources(
    left: &ir::EdgeAccessSourcePlan,
    right: &ir::EdgeAccessSourcePlan,
) -> merge::RangeSourceMerge<ir::EdgeAccessSourcePlan> {
    match (left.as_ref(), right.as_ref()) {
        (
            ir::EdgeAccessPlan::RangeIndex {
                index,
                key,
                range: left_range,
            },
            ir::EdgeAccessPlan::RangeIndex {
                key: right_key,
                range: right_range,
                ..
            },
        ) if key == right_key => match left_range.intersect(right_range) {
            Some(range) => merge::RangeSourceMerge::Merged(
                ir::EdgeAccessSourcePlan::from_unfiltered(ir::EdgeAccessPlan::RangeIndex {
                    index: index.clone(),
                    key: key.clone(),
                    range,
                }),
            ),
            None => merge::RangeSourceMerge::NotMergeable,
        },
        _ => merge::RangeSourceMerge::NotMergeable,
    }
}
