//! Memo-child extraction for root control-flow expressions.

use super::*;

pub(super) fn branch_children(branch: &RootBranch) -> Vec<LogicalExpr> {
    let mut inputs = vec![branch.input().clone()];
    branch_plan_inputs(branch.plan(), &mut inputs);
    inputs
}

pub(super) fn repeat_children(repeat: &RootRepeat) -> Vec<LogicalExpr> {
    vec![repeat.input().clone(), repeat.plan().body.as_ref().clone()]
}

fn branch_plan_inputs(plan: &ir::BranchPlan<LogicalExpr>, inputs: &mut Vec<LogicalExpr>) {
    match plan {
        ir::BranchPlan::Union(plans) => inputs.extend(plans.iter().cloned()),
        ir::BranchPlan::Choose { then_plan, .. } => inputs.push(then_plan.as_ref().clone()),
        ir::BranchPlan::ChooseElse {
            then_plan,
            else_plan,
            ..
        } => {
            inputs.push(then_plan.as_ref().clone());
            inputs.push(else_plan.as_ref().clone());
        }
        ir::BranchPlan::Coalesce(plans) => inputs.extend(plans.iter().cloned()),
        ir::BranchPlan::Optional(plan) => inputs.push(plan.as_ref().clone()),
    }
}
