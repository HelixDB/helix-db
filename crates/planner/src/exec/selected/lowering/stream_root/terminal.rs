//! Selected root-terminal lowering contract.
//!
//! A selected terminal root must have exactly one physical terminal suffix. The
//! prefix, if any, is localized to the recursive root-stream input.

use super::super::*;

impl ExecutableDagBuilder<'_> {
    pub(in crate::exec::selected::lowering) fn push_selected_terminal_root(
        &mut self,
        terminal: SelectedRootTerminalPlan,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        let (alternative, _provenance, input_prefix, plan) = terminal.into_parts();

        let (delivered, cost) = alternative.clone_contract();
        match plan {
            SelectedRootTerminal::Project { input, projection } => {
                let input_id = self.push_selected_root_stream_input(
                    input,
                    input_prefix.as_slice(),
                    dependencies,
                    condition.clone(),
                )?;
                let schedule = project_schedule(&projection);
                self.push_step(StepDraft {
                    dependencies: vec![input_id],
                    output,
                    condition,
                    op: ExecOp::Project { projection },
                    schedule,
                    delivered,
                    cost,
                })
            }
            SelectedRootTerminal::Aggregate { input, aggregate } => {
                let input_id = self.push_selected_root_stream_input(
                    input,
                    input_prefix.as_slice(),
                    dependencies,
                    condition.clone(),
                )?;
                self.push_step(StepDraft {
                    dependencies: vec![input_id],
                    output,
                    condition,
                    op: ExecOp::Aggregate { aggregate },
                    schedule: ExecSchedule::Barrier,
                    delivered,
                    cost,
                })
            }
            SelectedRootTerminal::Reserved { input, op } => {
                let input_id = self.push_selected_root_stream_input(
                    input,
                    input_prefix.as_slice(),
                    dependencies,
                    condition.clone(),
                )?;
                let schedule = reserved_schedule(&op);
                self.push_step(StepDraft {
                    dependencies: vec![input_id],
                    output,
                    condition,
                    op: ExecOp::Reserved { op },
                    schedule,
                    delivered,
                    cost,
                })
            }
            SelectedRootTerminal::VariableWrite { input, op } => {
                let input_id = self.push_selected_root_stream_input(
                    input,
                    input_prefix.as_slice(),
                    dependencies,
                    condition.clone(),
                )?;
                let op = op.to_stream_op();
                self.push_step(StepDraft {
                    dependencies: vec![input_id],
                    output,
                    condition,
                    op: ExecOp::Variable {
                        op: ExecVariableOp::Stream(op),
                    },
                    schedule: ExecSchedule::Barrier,
                    delivered,
                    cost,
                })
            }
        }
    }
}
