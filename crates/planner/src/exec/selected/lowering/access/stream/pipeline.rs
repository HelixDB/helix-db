//! Selected composed access-pipeline executable lowering.

use super::super::*;

impl ExecutableDagBuilder<'_> {
    pub(in crate::exec::selected::lowering) fn push_selected_access_pipeline(
        &mut self,
        access_pipeline: &logical::AccessPipeline,
        pipeline: &physical::PhysicalPipeline,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        let parts = match selected_access_pipeline_parts(access_pipeline.access(), pipeline) {
            SelectedAccessPipelineMatch::Matched(parts) => parts,
            SelectedAccessPipelineMatch::NotMatched(_) => {
                return Err(unsupported_selected_alternative(
                    rejection::Reason::AccessPipelineSourceMismatch,
                ));
            }
        };
        let (access, ops) = parts.into_parts();
        if !selected_stream_pipeline_ops_match(access_pipeline.ops(), ops) {
            return Err(unsupported_selected_alternative(
                rejection::Reason::AccessPipelinePhysicalSuffixMismatch,
            ));
        }

        let leading_window_plan = access_pipeline.ops().first().and_then(|op| match op {
            logical::StreamPipelineOp::Window { window } => {
                Some(WindowAccessReadPlan::for_window(access, *window))
            }
            logical::StreamPipelineOp::Filter { .. }
            | logical::StreamPipelineOp::Limit { .. }
            | logical::StreamPipelineOp::Skip { .. }
            | logical::StreamPipelineOp::Range { .. }
            | logical::StreamPipelineOp::Order { .. }
            | logical::StreamPipelineOp::Expand { .. }
            | logical::StreamPipelineOp::Variable { .. }
            | logical::StreamPipelineOp::VariableWrite { .. }
            | logical::StreamPipelineOp::Distinct => None,
        });
        let access = leading_window_plan
            .as_ref()
            .map_or_else(|| access.clone(), |plan| plan.access().clone());
        let read_limit = leading_window_plan
            .as_ref()
            .map(WindowAccessReadPlan::read_limit)
            .unwrap_or_default();
        let leading_window_satisfied_by_read_limit = leading_window_plan
            .as_ref()
            .is_some_and(|plan| plan.suffix() == WindowSuffix::ElidedByReadLimit);
        let emitted_op_count = access_pipeline
            .ops()
            .len()
            .saturating_sub(usize::from(leading_window_satisfied_by_read_limit));
        let access_output = if emitted_op_count == 0 {
            output.clone()
        } else {
            ir::BatchOutputPlan::Discard
        };
        let mut delivered = selected_access_path_delivered_properties(access_pipeline.access());
        let mut input_id = self.push_selected_access_path_with_read_limit(
            access_pipeline.access(),
            &access,
            read_limit,
            dependencies,
            access_output,
            condition.clone(),
        )?;

        let last_index = access_pipeline.ops().len().saturating_sub(1);
        for (index, op) in access_pipeline.ops().iter().enumerate() {
            if index == 0 && leading_window_satisfied_by_read_limit {
                delivered = selected_stream_pipeline_delivered_properties(delivered, op);
                continue;
            }
            let is_last = index == last_index;
            let step_output = if is_last {
                output.clone()
            } else {
                ir::BatchOutputPlan::Discard
            };
            let step = self.push_selected_stream_pipeline_op(
                op,
                input_id,
                delivered.clone(),
                step_output,
                condition.clone(),
            )?;
            delivered = selected_stream_pipeline_delivered_properties(delivered, op);
            input_id = step;
        }

        Ok(input_id)
    }
}
