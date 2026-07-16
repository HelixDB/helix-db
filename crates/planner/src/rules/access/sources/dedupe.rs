use std::collections::BTreeMap;

use serde::Serialize;

use crate::{digest, ir};

pub(in crate::rules::access) fn dedupe_node_sources(
    plans: &mut Vec<ir::NodeAccessSourcePlan>,
    changed: &mut bool,
) {
    dedupe_access_sources(plans, changed, "node_access_source:v1");
}

pub(in crate::rules::access) fn dedupe_edge_sources(
    plans: &mut Vec<ir::EdgeAccessSourcePlan>,
    changed: &mut bool,
) {
    dedupe_access_sources(plans, changed, "edge_access_source:v1");
}

fn dedupe_access_sources<T>(plans: &mut Vec<T>, changed: &mut bool, digest_tag: &'static str)
where
    T: Clone + PartialEq + Serialize,
{
    let mut buckets: BTreeMap<digest::PlanDigest, Vec<T>> = BTreeMap::new();
    let mut deduped = Vec::with_capacity(plans.len());
    plans.drain(..).for_each(|plan| {
        let bucket = buckets
            .entry(digest::PlanDigest::for_tagged_value(digest_tag, &plan))
            .or_default();
        if bucket.iter().any(|existing| existing == &plan) {
            *changed = true;
        } else {
            bucket.push(plan.clone());
            deduped.push(plan);
        }
    });
    *plans = deduped;
}
