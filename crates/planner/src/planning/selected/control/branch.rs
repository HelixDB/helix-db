//! Branch payload reconstruction from selected child roots.

use crate::planning::selected::rejection;
use crate::{error, exec, ir, logical};

pub(in crate::planning::selected) fn collect_branch_plan_inputs<'a>(
    plan: &'a ir::BranchPlan<logical::LogicalExpr>,
    inputs: &mut Vec<&'a logical::LogicalExpr>,
) {
    match plan {
        ir::BranchPlan::Union(plans) => inputs.extend(plans.iter()),
        ir::BranchPlan::Choose { then_plan, .. } => inputs.push(then_plan),
        ir::BranchPlan::ChooseElse {
            then_plan,
            else_plan,
            ..
        } => {
            inputs.push(then_plan);
            inputs.push(else_plan);
        }
        ir::BranchPlan::Coalesce(plans) => inputs.extend(plans.iter()),
        ir::BranchPlan::Optional(plan) => inputs.push(plan),
    }
}

#[derive(Debug, Clone)]
pub(in crate::planning::selected) struct SelectedBranchRoots {
    roots: Vec<exec::SelectedExecutableRunRoot>,
}

impl SelectedBranchRoots {
    pub(in crate::planning::selected::control) fn new(
        plan: &ir::BranchPlan<logical::LogicalExpr>,
        roots: Vec<exec::SelectedExecutableRunRoot>,
    ) -> Result<Self, error::PlannerError> {
        if roots.len() != branch_plan_root_count(plan) {
            return Err(rejection::unsupported(
                rejection::Reason::BranchRootArityMismatch,
            ));
        }
        Ok(Self { roots })
    }

    fn into_iter(self) -> std::vec::IntoIter<exec::SelectedExecutableRunRoot> {
        self.roots.into_iter()
    }
}

pub(in crate::planning::selected) fn split_selected_branch_roots(
    plan: &ir::BranchPlan<logical::LogicalExpr>,
    selected: Vec<exec::SelectedExecutableRunRoot>,
) -> Result<(exec::SelectedExecutableRunRoot, SelectedBranchRoots), error::PlannerError> {
    let expected = branch_plan_root_count(plan)
        .checked_add(1)
        .ok_or_else(|| rejection::unsupported(rejection::Reason::BranchRootArityMismatch))?;
    if selected.len() != expected {
        return Err(rejection::unsupported(
            rejection::Reason::BranchRootArityMismatch,
        ));
    }
    let mut selected = selected.into_iter();
    let input = next_selected_branch_root(&mut selected)?;
    let roots = SelectedBranchRoots::new(plan, selected.collect())?;
    Ok((input, roots))
}

pub(in crate::planning::selected) fn selected_branch_plan_from_roots(
    plan: &ir::BranchPlan<logical::LogicalExpr>,
    selected: SelectedBranchRoots,
) -> Result<exec::SelectedBranchPlan, error::PlannerError> {
    let mut selected = selected.into_iter();
    let reconstructed = selected_branch_plan_from_exact_roots(plan, &mut selected)?;
    if selected.next().is_some() {
        return Err(rejection::unsupported(
            rejection::Reason::BranchPlanReconstructionMismatch,
        ));
    }
    Ok(reconstructed)
}

fn selected_branch_plan_from_exact_roots(
    plan: &ir::BranchPlan<logical::LogicalExpr>,
    selected: &mut impl Iterator<Item = exec::SelectedExecutableRunRoot>,
) -> Result<exec::SelectedBranchPlan, error::PlannerError> {
    match plan {
        ir::BranchPlan::Union(plans) => {
            let plans = plans.try_map_ref(|_| next_selected_branch_root(selected))?;
            Ok(exec::SelectedBranchPlan::Union(plans))
        }
        ir::BranchPlan::Choose { condition, .. } => Ok(exec::SelectedBranchPlan::Choose {
            condition: condition.clone(),
            then_plan: Box::new(next_selected_branch_root(selected)?),
        }),
        ir::BranchPlan::ChooseElse { condition, .. } => Ok(exec::SelectedBranchPlan::ChooseElse {
            condition: condition.clone(),
            then_plan: Box::new(next_selected_branch_root(selected)?),
            else_plan: Box::new(next_selected_branch_root(selected)?),
        }),
        ir::BranchPlan::Coalesce(plans) => {
            let plans = plans.try_map_ref(|_| next_selected_branch_root(selected))?;
            Ok(exec::SelectedBranchPlan::Coalesce(plans))
        }
        ir::BranchPlan::Optional(_) => Ok(exec::SelectedBranchPlan::Optional(Box::new(
            next_selected_branch_root(selected)?,
        ))),
    }
}

fn next_selected_branch_root(
    selected: &mut impl Iterator<Item = exec::SelectedExecutableRunRoot>,
) -> Result<exec::SelectedExecutableRunRoot, error::PlannerError> {
    selected
        .next()
        .ok_or_else(|| rejection::unsupported(rejection::Reason::BranchPlanReconstructionMismatch))
}

fn branch_plan_root_count(plan: &ir::BranchPlan<logical::LogicalExpr>) -> usize {
    match plan {
        ir::BranchPlan::Union(plans) => plans.len(),
        ir::BranchPlan::Choose { .. } => 1,
        ir::BranchPlan::ChooseElse { .. } => 2,
        ir::BranchPlan::Coalesce(plans) => plans.len(),
        ir::BranchPlan::Optional(_) => 1,
    }
}
