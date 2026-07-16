//! Root-stream leaf selected lowering.
//!
//! Access and variable-source leaves are the only root-stream inputs whose
//! physical prefix is localized to the parent selected pipeline or terminal.
//! Recursive stream-producing roots cross `SelectedRootStreamInput` and lower
//! through selected run-root contracts instead.

use super::contracts::*;
use super::*;

impl ExecutableDagBuilder<'_> {
    pub(super) fn push_selected_variable_source_stream(
        &mut self,
        source: &logical::VariableSource,
        ops: &[physical::PhysicalPipelineOp],
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        if ops
            != [physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Variable,
            )]
        {
            return Err(unsupported_selected_alternative(
                rejection::Reason::RootStreamVariableSourceMismatch,
            ));
        }
        self.push_step(StepDraft {
            dependencies,
            output,
            condition,
            op: ExecOp::Variable {
                op: ExecVariableOp::SourceInject {
                    variable: source.variable().clone(),
                },
            },
            schedule: ExecSchedule::Pipeline,
            delivered: properties::DeliveredProperties::default(),
            cost: self.profile.source_inject(),
        })
    }
}
