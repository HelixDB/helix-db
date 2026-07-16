//! Selected root-pipeline lowering contract.
//!
//! A selected root pipeline is valid only when the selected physical pipeline
//! ends with the logical suffix owned by the root. Any prefix belongs to the
//! recursive root-stream input and is lowered through `input`.

use super::super::contracts::*;
use super::super::*;

impl ExecutableDagBuilder<'_> {
    pub(in crate::exec::selected::lowering) fn push_selected_pipeline_root(
        &mut self,
        pipeline: SelectedRootPipeline,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        let (alternative, _provenance, input, input_prefix, ops) = pipeline.into_parts();

        let delivered = selected_root_stream_input_delivered_properties(&input);
        let mut input_id = self.push_selected_root_stream_input(
            input,
            input_prefix.as_slice(),
            dependencies,
            condition.clone(),
        )?;
        let mut current_delivered = delivered;
        let last_index = ops.len().saturating_sub(1);
        for (index, op) in ops.as_ref().iter().enumerate() {
            let step_output = if index == last_index {
                output.clone()
            } else {
                ir::BatchOutputPlan::Discard
            };
            let step = self.push_selected_stream_pipeline_op(
                op,
                input_id,
                current_delivered.clone(),
                step_output,
                condition.clone(),
            )?;
            current_delivered =
                selected_stream_pipeline_delivered_properties(current_delivered, op);
            input_id = step;
        }
        let (delivered, cost) = alternative.clone_contract();
        self.override_step_contract(input_id, delivered, cost)?;
        Ok(input_id)
    }
}
