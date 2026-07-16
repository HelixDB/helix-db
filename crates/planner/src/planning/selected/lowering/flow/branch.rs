//! Selected branch reconstruction from memo-child plans.

use super::super::super::{control, SelectedCascadesPlanner};
use super::super::memo_children;
use crate::{error, exec, logical};

impl SelectedCascadesPlanner<'_> {
    pub(in crate::planning::selected::lowering) fn selected_branch_input_and_plan(
        &mut self,
        branch: &logical::RootBranch,
        child_plans: memo_children::MemoChildPlanContext<'_, '_>,
        metrics: &mut exec::PlannerMetrics,
    ) -> Result<(exec::SelectedExecutableRunRoot, exec::SelectedBranchPlan), error::PlannerError>
    {
        let mut inputs = vec![branch.input()];
        control::collect_branch_plan_inputs(branch.plan(), &mut inputs);
        let selected = self.selected_flow_children(inputs.len(), child_plans, metrics)?;
        let (input, branch_roots) = control::split_selected_branch_roots(branch.plan(), selected)?;
        let plan = control::selected_branch_plan_from_roots(branch.plan(), branch_roots)?;
        Ok((input, plan))
    }
}
