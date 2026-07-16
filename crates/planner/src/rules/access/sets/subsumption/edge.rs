//! Edge access subsumption proofs.

use super::super::*;

pub(super) fn simplify(plan: &ir::EdgeAccessPlan) -> AccessSetPlanRewrite<ir::EdgeAccessPlan> {
    match plan {
        ir::EdgeAccessPlan::Union(plans) => {
            match super::remove_subsumed_union_sources(plans, source_subsumes) {
                super::AccessSourceRemoval::Removed(sources) => {
                    AccessSetPlanRewrite::Rewritten(edge_union_from_sources(sources))
                }
                super::AccessSourceRemoval::Unchanged => AccessSetPlanRewrite::NotApplicable,
            }
        }
        ir::EdgeAccessPlan::Intersect(plans) => {
            match super::remove_redundant_intersection_sources(plans, source_subsumes) {
                super::AccessSourceRemoval::Removed(sources) => {
                    AccessSetPlanRewrite::Rewritten(edge_intersection_from_sources(sources))
                }
                super::AccessSourceRemoval::Unchanged => AccessSetPlanRewrite::NotApplicable,
            }
        }
        _ => AccessSetPlanRewrite::NotApplicable,
    }
}

fn source_subsumes(superset: &ir::EdgeAccessSourcePlan, subset: &ir::EdgeAccessSourcePlan) -> bool {
    superset.subsumes(subset)
}
