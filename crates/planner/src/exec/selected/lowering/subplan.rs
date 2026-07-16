use super::*;

pub(super) fn lower_selected_run_root_as_subplan(
    root: SelectedExecutableRunRoot,
    profile: &cost::StorageCostProfile,
) -> Result<ExecutableSubplan, ExecPlanError> {
    let mut lowering = ExecutableDagBuilder::new(profile);
    let root = lowering.push_selected_run_root(
        root,
        Vec::new(),
        ir::BatchOutputPlan::Discard,
        ExecCondition::Always,
    )?;
    lowering.finish_with_root(root, rejection::Reason::SelectedRunRootEmptyDag)
}

pub(super) fn lower_selected_branch_plan(
    plan: SelectedBranchPlan,
    profile: &cost::StorageCostProfile,
) -> Result<ExecBranchPlan, ExecPlanError> {
    match plan {
        SelectedBranchPlan::Union(plans) => {
            let plans = plans.try_map(|plan| lower_selected_run_root_as_subplan(plan, profile))?;
            Ok(ExecBranchPlan::Union(plans))
        }
        SelectedBranchPlan::Choose {
            condition,
            then_plan,
        } => Ok(ExecBranchPlan::Choose {
            condition,
            then_plan: Box::new(lower_selected_run_root_as_subplan(*then_plan, profile)?),
        }),
        SelectedBranchPlan::ChooseElse {
            condition,
            then_plan,
            else_plan,
        } => Ok(ExecBranchPlan::ChooseElse {
            condition,
            then_plan: Box::new(lower_selected_run_root_as_subplan(*then_plan, profile)?),
            else_plan: Box::new(lower_selected_run_root_as_subplan(*else_plan, profile)?),
        }),
        SelectedBranchPlan::Coalesce(plans) => {
            let plans = plans.try_map(|plan| lower_selected_run_root_as_subplan(plan, profile))?;
            Ok(ExecBranchPlan::Coalesce(plans))
        }
        SelectedBranchPlan::Optional(plan) => Ok(ExecBranchPlan::Optional(Box::new(
            lower_selected_run_root_as_subplan(*plan, profile)?,
        ))),
    }
}
