//! Node equality-union restriction by range intersections.

use std::collections::BTreeMap;

use super::super::super::AccessSetPlanRewrite;
use super::super::support::*;
use super::replacement;
use crate::{catalog, digest, ir};

pub(super) fn simplify(plan: &ir::NodeAccessPlan) -> AccessSetPlanRewrite<ir::NodeAccessPlan> {
    let ir::NodeAccessPlan::Intersect(plans) = plan else {
        return AccessSetPlanRewrite::NotApplicable;
    };
    let restriction = replacement::apply_intersection_restriction(
        plans,
        find_restriction,
        |source| matches!(source.as_ref(), ir::NodeAccessPlan::Empty),
        super::super::super::node_intersection_from_sources,
        || ir::NodeAccessPlan::Empty,
    );
    match restriction {
        replacement::IntersectionRestriction::Rewritten(plan) => {
            AccessSetPlanRewrite::Rewritten(plan)
        }
        replacement::IntersectionRestriction::Unchanged => AccessSetPlanRewrite::NotApplicable,
    }
}

pub(super) fn has_candidate(plan: &ir::NodeAccessPlan) -> bool {
    let ir::NodeAccessPlan::Intersect(plans) = plan else {
        return false;
    };
    let mut range_buckets: BTreeMap<digest::PlanDigest, Vec<NodeRangeBucketEntry<'_>>> =
        BTreeMap::new();
    plans.iter().enumerate().for_each(|(index, source)| {
        if let Some((digest, entry)) =
            node_range_bucket_entry("node_equality_range_property:v1", index, source)
        {
            range_buckets.entry(digest).or_default().push(entry);
        }
    });
    plans.iter().any(|source| {
        let ir::NodeAccessPlan::Union(children) = source.as_ref() else {
            return false;
        };
        let Some(union_key) = equality_union_key(children) else {
            return false;
        };
        let digest = scoped_property_digest(
            "node_equality_range_property:v1",
            &union_key.label,
            &union_key.property,
        );
        range_buckets.get(&digest).is_some_and(|range_entries| {
            range_entries.iter().any(|entry| {
                equality_union_can_be_restricted_by_range(children, entry.key, entry.range)
            })
        })
    })
}

fn find_restriction(
    slots: &[Option<ir::NodeAccessSourcePlan>],
) -> replacement::RangeRestrictionMatch<ir::NodeAccessSourcePlan> {
    let mut range_buckets: BTreeMap<digest::PlanDigest, Vec<NodeRangeBucketEntry<'_>>> =
        BTreeMap::new();
    slots.iter().enumerate().for_each(|(index, source)| {
        if let Some((digest, entry)) = source.as_ref().and_then(|source| {
            node_range_bucket_entry("node_equality_range_property:v1", index, source)
        }) {
            range_buckets.entry(digest).or_default().push(entry);
        }
    });
    for (union_index, union) in slots.iter().enumerate().filter_map(|(index, source)| {
        let ir::NodeAccessPlan::Union(children) = source.as_ref()?.as_ref() else {
            return None;
        };
        Some((index, children))
    }) {
        let Some(union_key) = equality_union_key(union) else {
            continue;
        };
        let digest = scoped_property_digest(
            "node_equality_range_property:v1",
            &union_key.label,
            &union_key.property,
        );
        let Some(range_entries) = range_buckets.get(&digest) else {
            continue;
        };
        for entry in range_entries {
            match restrict_equality_union_by_range(union, entry.key, entry.range) {
                replacement::EqualityUnionRangeRestriction::Restricted(replacement) => {
                    return replacement::RangeRestrictionMatch::Found(
                        replacement::RangeRestrictedUnion::new(
                            union_index,
                            entry.index,
                            replacement,
                        ),
                    );
                }
                replacement::EqualityUnionRangeRestriction::NotApplicable => {}
            }
        }
    }
    replacement::RangeRestrictionMatch::NotFound
}

fn equality_union_can_be_restricted_by_range(
    children: &ir::AtLeast<ir::NodeAccessSourcePlan, 2>,
    range_key: &catalog::ScopedPropertyDirectionKey,
    range: &ir::IndexRange,
) -> bool {
    children.iter().all(|child| {
        let Some((equality_key, value)) = node_literal_equality_parts(child) else {
            return false;
        };
        equality_key.label == range_key.label
            && equality_key.property == range_key.property
            && (range.contains_secondary_literal(value) || range.excludes_secondary_literal(value))
    })
}

fn equality_union_key(
    children: &ir::AtLeast<ir::NodeAccessSourcePlan, 2>,
) -> Option<&catalog::ScopedPropertyKey> {
    let (first_child, rest) = children.as_ref().split_first()?;
    let (first, _) = node_literal_equality_parts(first_child)?;
    rest.iter()
        .all(|child| node_literal_equality_parts(child).is_some_and(|(key, _)| key == first))
        .then_some(first)
}

fn restrict_equality_union_by_range(
    children: &ir::AtLeast<ir::NodeAccessSourcePlan, 2>,
    range_key: &catalog::ScopedPropertyDirectionKey,
    range: &ir::IndexRange,
) -> replacement::EqualityUnionRangeRestriction<ir::NodeAccessSourcePlan> {
    let mut retained = Vec::new();
    for child in children {
        let Some((equality_key, value)) = node_literal_equality_parts(child) else {
            return replacement::EqualityUnionRangeRestriction::NotApplicable;
        };
        if equality_key.label != range_key.label || equality_key.property != range_key.property {
            return replacement::EqualityUnionRangeRestriction::NotApplicable;
        }
        if range.contains_secondary_literal(value) {
            retained.push(child.clone());
        } else if !range.excludes_secondary_literal(value) {
            return replacement::EqualityUnionRangeRestriction::NotApplicable;
        }
    }
    replacement::EqualityUnionRangeRestriction::Restricted(node_source_from_union_candidates(
        retained,
    ))
}
