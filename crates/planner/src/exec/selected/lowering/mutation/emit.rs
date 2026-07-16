//! Executable emission for classified selected mutations.

use super::super::contracts::*;
use super::super::*;
use super::plan::LoweredSelectedMutation;

impl ExecutableDagBuilder<'_> {
    pub(super) fn push_lowered_selected_mutation(
        &mut self,
        mutation: LoweredSelectedMutation,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
        delivered: properties::DeliveredProperties,
        cost: cost::CostVector,
    ) -> Result<ExecStepId, ExecPlanError> {
        match mutation {
            LoweredSelectedMutation::Source(plan) => self.push_selected_mutation_step(
                dependencies,
                output,
                condition,
                plan,
                delivered,
                cost,
            ),
            LoweredSelectedMutation::Input { input, plan } => self
                .push_selected_input_mutation_step(
                    input,
                    selected_mutation_step_draft(
                        dependencies,
                        output,
                        condition,
                        plan,
                        delivered,
                        cost,
                    ),
                ),
        }
    }

    fn push_selected_input_mutation_step(
        &mut self,
        input: SelectedExecutableRunRoot,
        mut root: StepDraft,
    ) -> Result<ExecStepId, ExecPlanError> {
        let dependencies = std::mem::take(&mut root.dependencies);
        let input_id = self.push_selected_run_root(
            input,
            dependencies,
            ir::BatchOutputPlan::Discard,
            root.condition.clone(),
        )?;
        root.dependencies = vec![input_id];
        self.push_step(root)
    }

    fn push_selected_mutation_step(
        &mut self,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
        plan: ExecMutationPlan,
        delivered: properties::DeliveredProperties,
        cost: cost::CostVector,
    ) -> Result<ExecStepId, ExecPlanError> {
        self.push_step(StepDraft {
            dependencies,
            output,
            condition,
            op: ExecOp::Mutation { plan },
            schedule: ExecSchedule::Barrier,
            delivered,
            cost,
        })
    }
}
