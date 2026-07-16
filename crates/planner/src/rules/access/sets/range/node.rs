//! Node same-key range-intersection tightening.

use super::super::*;
use super::merge;

pub(super) fn simplify(plan: &ir::NodeAccessPlan) -> AccessSetPlanRewrite<ir::NodeAccessPlan> {
    let ir::NodeAccessPlan::Intersect(plans) = plan else {
        return AccessSetPlanRewrite::NotApplicable;
    };
    match merge::merge_intersection_sources(plans, range_key_digest, merge_range_sources) {
        merge::RangeIntersectionMerge::Merged(sources) => {
            AccessSetPlanRewrite::Rewritten(node_intersection_from_sources(sources))
        }
        merge::RangeIntersectionMerge::Unchanged(_) => AccessSetPlanRewrite::NotApplicable,
    }
}

pub(super) fn has_candidate(plan: &ir::NodeAccessPlan) -> bool {
    let ir::NodeAccessPlan::Intersect(plans) = plan else {
        return false;
    };
    plans.iter().enumerate().any(|(index, left)| {
        plans
            .iter()
            .skip(index + 1)
            .any(|right| range_sources_can_merge(left, right))
    })
}

fn range_key_digest(source: &ir::NodeAccessSourcePlan) -> merge::RangeMergeKey {
    match source.as_ref() {
        ir::NodeAccessPlan::RangeIndex { key, .. } => merge::RangeMergeKey::Key(
            digest::PlanDigest::for_tagged_value("node_range_intersection_key:v1", key),
        ),
        _ => merge::RangeMergeKey::NotRange,
    }
}

fn range_sources_can_merge(
    left: &ir::NodeAccessSourcePlan,
    right: &ir::NodeAccessSourcePlan,
) -> bool {
    match (left.as_ref(), right.as_ref()) {
        (
            ir::NodeAccessPlan::RangeIndex {
                key,
                range: left_range,
                ..
            },
            ir::NodeAccessPlan::RangeIndex {
                key: right_key,
                range: right_range,
                ..
            },
        ) if key == right_key => left_range.intersect(right_range).is_some(),
        _ => false,
    }
}

fn merge_range_sources(
    left: &ir::NodeAccessSourcePlan,
    right: &ir::NodeAccessSourcePlan,
) -> merge::RangeSourceMerge<ir::NodeAccessSourcePlan> {
    match (left.as_ref(), right.as_ref()) {
        (
            ir::NodeAccessPlan::RangeIndex {
                index,
                key,
                range: left_range,
            },
            ir::NodeAccessPlan::RangeIndex {
                key: right_key,
                range: right_range,
                ..
            },
        ) if key == right_key => match left_range.intersect(right_range) {
            Some(range) => merge::RangeSourceMerge::Merged(
                ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::RangeIndex {
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
