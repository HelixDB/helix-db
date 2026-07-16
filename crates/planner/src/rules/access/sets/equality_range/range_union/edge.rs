//! Edge equality-union branch removal covered by range union branches.

use std::collections::BTreeMap;

use super::super::super::AccessSetPlanRewrite;
use super::super::support::*;
use super::removal;
use crate::{digest, ir};

pub(super) fn simplify(plan: &ir::EdgeAccessPlan) -> AccessSetPlanRewrite<ir::EdgeAccessPlan> {
    let ir::EdgeAccessPlan::Union(plans) = plan else {
        return AccessSetPlanRewrite::NotApplicable;
    };
    match removal::remove_covered_sources(plans, range_buckets, equality_covered_by_any_range) {
        removal::CoveredSourceRemoval::Removed(sources) => {
            AccessSetPlanRewrite::Rewritten(super::super::super::edge_union_from_sources(sources))
        }
        removal::CoveredSourceRemoval::Unchanged(_) => AccessSetPlanRewrite::NotApplicable,
    }
}

pub(super) fn has_candidate(plan: &ir::EdgeAccessPlan) -> bool {
    let ir::EdgeAccessPlan::Union(plans) = plan else {
        return false;
    };
    let buckets = range_buckets(plans);
    plans
        .iter()
        .any(|source| equality_covered_by_any_range(source, &buckets))
}

fn range_buckets<'a>(
    plans: &'a ir::AtLeast<ir::EdgeAccessSourcePlan, 2>,
) -> BTreeMap<digest::PlanDigest, Vec<EdgeRangeBucketEntry<'a>>> {
    let mut buckets: BTreeMap<digest::PlanDigest, Vec<EdgeRangeBucketEntry<'a>>> = BTreeMap::new();
    plans.iter().enumerate().for_each(|(index, source)| {
        if let Some((digest, entry)) =
            edge_range_bucket_entry("edge_equality_range_union_property:v1", index, source)
        {
            buckets.entry(digest).or_default().push(entry);
        }
    });
    buckets
}

fn equality_covered_by_any_range(
    source: &ir::EdgeAccessSourcePlan,
    range_buckets: &BTreeMap<digest::PlanDigest, Vec<EdgeRangeBucketEntry<'_>>>,
) -> bool {
    let Some((key, value)) = edge_literal_equality_parts(source) else {
        return false;
    };
    let digest = scoped_property_digest(
        "edge_equality_range_union_property:v1",
        &key.label,
        &key.property,
    );
    range_buckets.get(&digest).is_some_and(|range_indexes| {
        range_indexes.iter().any(|entry| {
            entry.key.label == key.label
                && entry.key.property == key.property
                && entry.range.contains_secondary_literal(value)
        })
    })
}
