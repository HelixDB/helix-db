//! Canonical union and intersection normalization.
//!
//! The rule facade delegates node and edge source details to separate modules.
//! `normalization` owns the generic set contract: flatten nested same-family
//! sets, elide union empties, short-circuit intersection empties, and rebuild
//! only when the source list actually changed.

mod edge;
mod node;
mod normalization;

use super::*;

pub(in crate::rules::access) fn simplify_access_set(
    access: &logical::AccessPath,
) -> AccessSetRewrite {
    match access {
        logical::AccessPath::Node(path) => {
            node_simplification_result(node::simplify(path.source().as_ref()))
        }
        logical::AccessPath::Edge(path) => {
            edge_simplification_result(edge::simplify(path.source().as_ref()))
        }
    }
}

fn node_simplification_result(
    rewrite: normalization::SourceSetSimplification<ir::NodeAccessPlan>,
) -> AccessSetRewrite {
    match rewrite {
        normalization::SourceSetSimplification::NotASet
        | normalization::SourceSetSimplification::Unchanged => AccessSetRewrite::NotApplicable,
        normalization::SourceSetSimplification::Rewritten(plan) => {
            AccessSetRewrite::from_node_plan(AccessSetPlanRewrite::Rewritten(plan))
        }
    }
}

fn edge_simplification_result(
    rewrite: normalization::SourceSetSimplification<ir::EdgeAccessPlan>,
) -> AccessSetRewrite {
    match rewrite {
        normalization::SourceSetSimplification::NotASet
        | normalization::SourceSetSimplification::Unchanged => AccessSetRewrite::NotApplicable,
        normalization::SourceSetSimplification::Rewritten(plan) => {
            AccessSetRewrite::from_edge_plan(AccessSetPlanRewrite::Rewritten(plan))
        }
    }
}
