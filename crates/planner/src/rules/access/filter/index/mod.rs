//! Catalog-backed access-filter indexing.
//!
//! Static predicate pruning is shared here, while node and edge modules own
//! typed catalog lookup and source-combination contracts.

mod contracts;
mod edge;
mod label_domain;
mod membership;
mod node;
mod shared;
mod translate;

use self::contracts::{
    AccessFilterIndexApplication, AccessFilterIndexRejection, PartialIndexFilterApplication,
    PartialIndexFilterRejection,
};
use super::atoms::{access_filter_index_plan, AccessFilterIndexPlanMatch};
use super::AccessFilterRewrite;
use crate::{analysis, catalog, context, ir, logical};

pub(in crate::rules) use label_domain::has_candidate as label_domain_has_candidate;
pub(in crate::rules) use membership::index_membership_filter;

/// Every index rewrite of a source filter: [`required_index_access_filter`],
/// or else the `$label` bitmaps of a finite label domain.
///
/// Both intersect the source with index sets, which emits each element
/// once, so neither rewrites a source that may repeat elements (see
/// [`logical::AccessPath::may_repeat_elements`]): its filter stays
/// row-preserving.
pub(in crate::rules) fn index_access_filter(
    filter: &logical::AccessFilter,
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
) -> AccessFilterRewrite {
    required_index_access_filter(filter, indexes, planner_limits).or_else(|| {
        if filter.access().may_repeat_elements() {
            return AccessFilterRewrite::NotApplicable;
        }
        let Ok(analysis::PrunedPredicate::Feasible { predicate, .. }) =
            analysis::prune_borrowed(filter.predicate().as_ref())
        else {
            return AccessFilterRewrite::NotApplicable;
        };
        label_domain::rewrite(filter.access(), &predicate, planner_limits)
    })
}

/// The index rewrite a source filter must take because a property index
/// serves at least one of its conjuncts: the full rewrite when indexes answer
/// the whole predicate, otherwise its recursive translation (`AND` into
/// intersection, `OR` into union), in which each branch intersects its
/// index-served conjuncts and keeps only the rest as its own residual. Only a
/// disjunction with a branch no index narrows stays a per-row filter.
///
/// A filter the source already answers exactly, such as `a == 1` over the
/// `a == 1` equality lookup, is dropped: the result is the unchanged source,
/// never a per-row filter. The label-domain fallback is not part of this
/// contract, because the `$label` bitmaps are no property index.
///
/// The rewrite is idempotent: its residual holds only conjuncts no index
/// answers, so rewriting the output again yields
/// [`AccessFilterRewrite::NotApplicable`].
///
/// A source that may repeat elements is never rewritten, since intersecting
/// it with an index set would collapse its repeats. Index membership decides
/// such node filters instead, row by row from the set, keeping every repeat.
pub(in crate::rules) fn required_index_access_filter(
    filter: &logical::AccessFilter,
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
) -> AccessFilterRewrite {
    if filter.access().may_repeat_elements() {
        return AccessFilterRewrite::NotApplicable;
    }
    let pruned = match analysis::prune_borrowed(filter.predicate().as_ref()) {
        Ok(predicate) => predicate,
        Err(_) => return AccessFilterRewrite::NotApplicable,
    };
    let analysis::PrunedPredicate::Feasible { predicate, label } = pruned else {
        return AccessFilterRewrite::NotApplicable;
    };
    let full = match filter.access() {
        logical::AccessPath::Node(path) => index_application_rewrite(
            node::index_filter(path, &predicate, &label, indexes, planner_limits),
            logical::AccessPath::Node,
            filter.access(),
        ),
        logical::AccessPath::Edge(path) => index_application_rewrite(
            edge::index_filter(path, &predicate, &label, indexes, planner_limits),
            logical::AccessPath::Edge,
            filter.access(),
        ),
    };
    full.or_else(|| match filter.access() {
        logical::AccessPath::Node(path) => partial_index_application_rewrite(
            translate::translated_index_filter::<node::NodeIndexFamily>(
                path,
                &predicate,
                &label,
                indexes,
                planner_limits,
            ),
            |source| logical::AccessPath::Node(logical::NodeAccessPath::new(source)),
            filter.access(),
        ),
        logical::AccessPath::Edge(path) => partial_index_application_rewrite(
            translate::translated_index_filter::<edge::EdgeIndexFamily>(
                path,
                &predicate,
                &label,
                indexes,
                planner_limits,
            ),
            |source| logical::AccessPath::Edge(logical::EdgeAccessPath::new(source)),
            filter.access(),
        ),
    })
}

/// `unchanged` is the filtered source, kept as the whole result when the
/// source already answers the predicate exactly.
fn index_application_rewrite<T>(
    application: AccessFilterIndexApplication<T>,
    access_path: impl FnOnce(T) -> logical::AccessPath,
    unchanged: &logical::AccessPath,
) -> AccessFilterRewrite {
    match application {
        AccessFilterIndexApplication::Rewritten(path) => {
            AccessFilterRewrite::Rewritten(access_path(path))
        }
        AccessFilterIndexApplication::NotApplicable(
            AccessFilterIndexRejection::SourceUnchanged,
        ) => AccessFilterRewrite::Rewritten(unchanged.clone()),
        AccessFilterIndexApplication::NotApplicable(
            AccessFilterIndexRejection::NoLabel
            | AccessFilterIndexRejection::Predicate(_)
            | AccessFilterIndexRejection::MissingIndex(_),
        ) => AccessFilterRewrite::NotApplicable,
    }
}

/// `unchanged` is the filtered source, kept as the whole result when the
/// source already answers every conjunct exactly.
fn partial_index_application_rewrite<T>(
    application: PartialIndexFilterApplication<T>,
    access_path: impl FnOnce(T) -> logical::AccessPath,
    unchanged: &logical::AccessPath,
) -> AccessFilterRewrite {
    match application {
        PartialIndexFilterApplication::Rewritten { source, residual } => {
            let access = access_path(source);
            match residual {
                Some(predicate) => AccessFilterRewrite::RewrittenPipeline(
                    logical::AccessPipeline::new(
                        access,
                        ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Filter {
                            predicate,
                        }),
                    )
                    .expect("single residual filter is a valid access pipeline"),
                ),
                None => AccessFilterRewrite::Rewritten(access),
            }
        }
        PartialIndexFilterApplication::NotApplicable(
            PartialIndexFilterRejection::SourceUnchanged,
        ) => AccessFilterRewrite::Rewritten(unchanged.clone()),
        PartialIndexFilterApplication::NotApplicable(
            PartialIndexFilterRejection::NoLabel
            | PartialIndexFilterRejection::NotConjunction
            | PartialIndexFilterRejection::NoIndexedConjunct
            | PartialIndexFilterRejection::ResidualBranchesUnrepresentable,
        ) => AccessFilterRewrite::NotApplicable,
    }
}

fn index_plan(
    predicate: &helix_ast::expr::Predicate,
    label: &crate::ir::NonEmptyString,
    planner_limits: &context::PlannerLimits,
) -> AccessFilterIndexPlanMatch {
    access_filter_index_plan(predicate, label, planner_limits)
}
