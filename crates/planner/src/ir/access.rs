//! Graph access-plan contract facade.
//!
//! Node and edge access contracts live in separate element-family modules so
//! each residual-free source wrapper owns the serde and construction boundary
//! for its corresponding access-plan ADT.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::digest;

mod edge;
mod node;

pub use self::{
    edge::{EdgeAccessPlan, EdgeAccessSourcePlan},
    node::{NodeAccessPlan, NodeAccessSourcePlan},
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

fn union_has_subsumption_candidate<T, F>(sources: &super::AtLeast<T, 2>, subsumes: F) -> bool
where
    F: Fn(&T, &T) -> bool,
{
    sources.iter().enumerate().any(|(index, source)| {
        sources.iter().enumerate().any(|(other_index, other)| {
            other_index != index
                && subsumes(other, source)
                && (!subsumes(source, other) || other_index < index)
        })
    })
}

fn intersection_has_subsumption_candidate<T, F>(sources: &super::AtLeast<T, 2>, subsumes: F) -> bool
where
    F: Fn(&T, &T) -> bool,
{
    sources.iter().enumerate().any(|(index, source)| {
        sources.iter().enumerate().any(|(other_index, other)| {
            other_index != index
                && subsumes(source, other)
                && (!subsumes(other, source) || other_index < index)
        })
    })
}
