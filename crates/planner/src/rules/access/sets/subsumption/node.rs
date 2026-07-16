//! Node access subsumption proofs.

use super::super::*;

pub(super) fn simplify(plan: &ir::NodeAccessPlan) -> AccessSetPlanRewrite<ir::NodeAccessPlan> {
    match plan {
        ir::NodeAccessPlan::Union(plans) => {
            match super::remove_subsumed_union_sources(plans, source_subsumes) {
                super::AccessSourceRemoval::Removed(sources) => {
                    AccessSetPlanRewrite::Rewritten(node_union_from_sources(sources))
                }
                super::AccessSourceRemoval::Unchanged => AccessSetPlanRewrite::NotApplicable,
            }
        }
        ir::NodeAccessPlan::Intersect(plans) => {
            match super::remove_redundant_intersection_sources(plans, source_subsumes) {
                super::AccessSourceRemoval::Removed(sources) => {
                    AccessSetPlanRewrite::Rewritten(node_intersection_from_sources(sources))
                }
                super::AccessSourceRemoval::Unchanged => AccessSetPlanRewrite::NotApplicable,
            }
        }
        _ => AccessSetPlanRewrite::NotApplicable,
    }
}

fn source_subsumes(superset: &ir::NodeAccessSourcePlan, subset: &ir::NodeAccessSourcePlan) -> bool {
    superset.subsumes(subset)
}
