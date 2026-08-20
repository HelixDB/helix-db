//! Catalog-backed access-filter indexing.
//!
//! Static predicate pruning is shared here, while node and edge modules own
//! typed catalog lookup and source-combination contracts.

mod contracts;
mod edge;
mod label_domain;
mod node;
mod shared;

use self::contracts::{AccessFilterIndexApplication, PartialIndexFilterApplication};
use super::atoms::{access_filter_index_plan, AccessFilterIndexPlanMatch};
use super::AccessFilterRewrite;
use crate::{analysis, catalog, context, ir, logical};

pub(in crate::rules) use label_domain::has_candidate as label_domain_has_candidate;

pub(in crate::rules) fn index_access_filter(
    filter: &logical::AccessFilter,
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
) -> AccessFilterRewrite {
    let pruned = match analysis::prune_statically_impossible_branches(filter.predicate().as_ref()) {
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
        ),
        logical::AccessPath::Edge(path) => index_application_rewrite(
            edge::index_filter(path, &predicate, &label, indexes, planner_limits),
            logical::AccessPath::Edge,
        ),
    };
    full.or_else(|| match filter.access() {
        logical::AccessPath::Node(path) => partial_index_application_rewrite(
            node::partial_index_filter(path, &predicate, &label, indexes, planner_limits),
            |source| logical::AccessPath::Node(logical::NodeAccessPath::new(source)),
        ),
        logical::AccessPath::Edge(path) => partial_index_application_rewrite(
            edge::partial_index_filter(path, &predicate, &label, indexes, planner_limits),
            |source| logical::AccessPath::Edge(logical::EdgeAccessPath::new(source)),
        ),
    })
    .or_else(|| label_domain::rewrite(filter.access(), &predicate, planner_limits))
}

fn index_application_rewrite<T>(
    application: AccessFilterIndexApplication<T>,
    access_path: impl FnOnce(T) -> logical::AccessPath,
) -> AccessFilterRewrite {
    match application {
        AccessFilterIndexApplication::Rewritten(path) => {
            AccessFilterRewrite::Rewritten(access_path(path))
        }
        AccessFilterIndexApplication::NotApplicable(_reason) => AccessFilterRewrite::NotApplicable,
    }
}

fn partial_index_application_rewrite<T>(
    application: PartialIndexFilterApplication<T>,
    access_path: impl FnOnce(T) -> logical::AccessPath,
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
        PartialIndexFilterApplication::NotApplicable(_reason) => AccessFilterRewrite::NotApplicable,
    }
}

fn index_plan(
    predicate: &helix_ast::expr::Predicate,
    label: &crate::ir::NonEmptyString,
    planner_limits: &context::PlannerLimits,
) -> AccessFilterIndexPlanMatch {
    access_filter_index_plan(predicate, label, planner_limits)
}
