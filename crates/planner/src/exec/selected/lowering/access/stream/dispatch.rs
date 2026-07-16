//! Dispatch from selected access-stream ADTs to wrapper-specific lowering.

use super::super::*;

impl ExecutableDagBuilder<'_> {
    pub(in crate::exec::selected::lowering) fn push_selected_access_stream(
        &mut self,
        input: &logical::AccessStream,
        ops: &[physical::PhysicalPipelineOp],
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        let pipeline = selected_pipeline_from_ops(ops)?;
        match input {
            logical::AccessStream::Path(access) => {
                let parts = match selected_access_pipeline_parts(access, &pipeline) {
                    SelectedAccessPipelineMatch::Matched(parts) => parts,
                    SelectedAccessPipelineMatch::NotMatched(_) => {
                        return Err(unsupported_selected_alternative(
                            rejection::Reason::AccessStreamPathSourceMismatch,
                        ));
                    }
                };
                let (physical_access, ops) = parts.into_parts();
                if !ops.is_empty() {
                    return Err(unsupported_selected_alternative(
                        rejection::Reason::AccessStreamPathSourceMismatch,
                    ));
                }
                self.push_selected_access_path(
                    access,
                    physical_access,
                    dependencies,
                    output,
                    condition,
                )
            }
            logical::AccessStream::Filter(filter) => {
                self.push_selected_access_filter(filter, &pipeline, dependencies, output, condition)
            }
            logical::AccessStream::Window(window) => {
                self.push_selected_access_window(window, &pipeline, dependencies, output, condition)
            }
            logical::AccessStream::Order(order) => {
                self.push_selected_access_order(order, &pipeline, dependencies, output, condition)
            }
            logical::AccessStream::Distinct(distinct) => self.push_selected_access_distinct(
                distinct,
                &pipeline,
                dependencies,
                output,
                condition,
            ),
            logical::AccessStream::Pipeline(access_pipeline) => self.push_selected_access_pipeline(
                access_pipeline,
                &pipeline,
                dependencies,
                output,
                condition,
            ),
        }
    }
}
