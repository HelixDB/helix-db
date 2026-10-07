//! Graph access-plan contract facade.
//!
//! Node and edge access contracts live in separate element-family modules so
//! each residual-free source wrapper owns the serde and construction boundary
//! for its corresponding access-plan ADT.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::digest;

mod edge;
mod iteration;
mod node;

pub use self::{
    edge::{EdgeAccessPlan, EdgeAccessSourcePlan, EdgeResidualBranch, EdgeResidualBranches},
    iteration::RangeScanIteration,
    node::{
        NodeAccessPlan, NodeAccessSourcePlan, NodeIndexMembershipError, NodeIndexMembershipPlan,
        NodeMembershipOutsideLabel, NodeMembershipSet, NodeResidualBranch, NodeResidualBranches,
    },
};

fn search_limit_hard_cardinality_upper_bound(k: &super::SearchLimitPlan) -> Option<usize> {
    match k {
        super::SearchLimitPlan::Literal(k) => Some(k.get()),
        super::SearchLimitPlan::Expr(_) => None,
    }
}

fn common_source_label<'a>(
    mut labels: impl Iterator<Item = Option<&'a super::NonEmptyString>>,
) -> Option<&'a super::NonEmptyString> {
    let first = labels.next()??;
    labels
        .all(|label| label.is_some_and(|label| label == first))
        .then_some(first)
}

fn access_sources_have_duplicate<T>(
    sources: &super::AtLeast<T, 2>,
    digest_tag: &'static str,
) -> bool
where
    T: PartialEq + Serialize,
{
    let mut buckets: BTreeMap<digest::PlanDigest, Vec<&T>> = BTreeMap::new();
    sources.iter().any(|source| {
        let bucket = buckets
            .entry(digest::PlanDigest::for_tagged_value(digest_tag, source))
            .or_default();
        if bucket.contains(&source) {
            true
        } else {
            bucket.push(source);
            false
        }
    })
}

/// Sets wider than this compare digests before testing two sources for
/// equality; narrower ones compare directly, which costs less than
/// serializing every source.
const SUBSUMPTION_DIGEST_WIDTH: usize = 8;

/// `subsumes(superset, subset)` over source positions: equality, or
/// `structurally_subsumes`. In a wide set, equality is tested only between
/// sources whose digests match, so checking every pair stays cheap.
fn positional_subsumption<'s, T, F>(
    sources: &'s super::AtLeast<T, 2>,
    structurally_subsumes: F,
) -> impl Fn(usize, usize) -> bool + 's
where
    T: PartialEq + Serialize,
    F: Fn(&T, &T) -> bool + 's,
{
    let sources = sources.as_ref();
    let digests = (sources.len() > SUBSUMPTION_DIGEST_WIDTH).then(|| {
        sources
            .iter()
            .map(digest::PlanDigest::for_value)
            .collect::<Vec<_>>()
    });
    move |superset, subset| {
        (digests
            .as_ref()
            .is_none_or(|digests| digests[superset] == digests[subset])
            && sources[superset] == sources[subset])
            || structurally_subsumes(&sources[superset], &sources[subset])
    }
}

fn union_has_subsumption_candidate<T, F>(
    sources: &super::AtLeast<T, 2>,
    structurally_subsumes: F,
) -> bool
where
    T: PartialEq + Serialize,
    F: Fn(&T, &T) -> bool,
{
    let subsumes = positional_subsumption(sources, structurally_subsumes);
    (0..sources.len()).any(|index| {
        (0..sources.len()).any(|other| {
            other != index && subsumes(other, index) && (!subsumes(index, other) || other < index)
        })
    })
}

fn intersection_has_subsumption_candidate<T, F>(
    sources: &super::AtLeast<T, 2>,
    structurally_subsumes: F,
) -> bool
where
    T: PartialEq + Serialize,
    F: Fn(&T, &T) -> bool,
{
    let subsumes = positional_subsumption(sources, structurally_subsumes);
    (0..sources.len()).any(|index| {
        (0..sources.len()).any(|other| {
            other != index && subsumes(index, other) && (!subsumes(other, index) || other < index)
        })
    })
}
