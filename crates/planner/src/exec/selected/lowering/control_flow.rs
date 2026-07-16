//! Selected control-flow root lowering.
//!
//! Branch and repeat roots are explicit selected contracts. They lower their
//! input run-root first, then emit the control-flow barrier or pipeline step
//! described by the selected physical alternative.

use super::contracts::*;
use super::*;

impl ExecutableDagBuilder<'_> {
    pub(in crate::exec::selected::lowering) fn push_selected_branch_root(
        &mut self,
        branch: SelectedRootBranch,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        let (alternative, _provenance, input, plan) = branch.into_parts();
        let exec_plan = lower_selected_branch_plan(plan, self.profile)?;
        let (delivered, cost) = alternative.clone_contract();
        let schedule = selected_control_schedule(&delivered);
        let input_id = self.push_selected_run_root(
            *input,
            dependencies,
            ir::BatchOutputPlan::Discard,
            condition.clone(),
        )?;
        self.push_step(StepDraft {
            dependencies: vec![input_id],
            output,
            condition,
            op: ExecOp::Branch { plan: exec_plan },
            schedule,
            delivered,
            cost,
        })
    }

    pub(in crate::exec::selected::lowering) fn push_selected_repeat_root(
        &mut self,
        repeat: SelectedRootRepeat,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        let (alternative, _provenance, input, plan) = repeat.into_parts();
        let (delivered, cost) = alternative.clone_contract();
        let schedule = selected_control_schedule(&delivered);
        let body = lower_selected_run_root_as_subplan(*plan.body, self.profile)?;
        let exec_plan = ExecRepeatPlan {
            body: Box::new(body),
            stop: plan.stop,
            emit: plan.emit,
            max_depth: plan.max_depth,
        };
        let input_id = self.push_selected_run_root(
            *input,
            dependencies,
            ir::BatchOutputPlan::Discard,
            condition.clone(),
        )?;
        self.push_step(StepDraft {
            dependencies: vec![input_id],
            output,
            condition,
            op: ExecOp::Repeat { plan: exec_plan },
            schedule,
            delivered,
            cost,
        })
    }
}
