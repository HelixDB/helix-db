//! Selected access-filter executable lowering.

use super::super::*;

impl ExecutableDagBuilder<'_> {
    pub(in crate::exec::selected::lowering) fn push_selected_access_filter(
        &mut self,
        filter: &logical::AccessFilter,
        pipeline: &physical::PhysicalPipeline,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        let access = match selected_access_filter_pipeline_access(filter, pipeline) {
            SelectedAccessFilterPipelineMatch::Matched(access) => access,
            SelectedAccessFilterPipelineMatch::NotMatched(_) => {
                return Err(unsupported_selected_alternative(
                    rejection::Reason::AccessFilterSourceMismatch,
                ));
            }
        };
        let input_id = self.push_selected_access_path(
            filter.access(),
            access,
            dependencies,
            ir::BatchOutputPlan::Discard,
            condition.clone(),
        )?;
        self.push_step(StepDraft {
            dependencies: vec![input_id],
            output,
            condition,
            op: ExecOp::Filter {
                predicate: filter.predicate().clone(),
            },
            schedule: ExecSchedule::Pipeline,
            delivered: filtered_delivered_properties(selected_access_path_delivered_properties(
                filter.access(),
            )),
            cost: predicate_cost_for_rows(
                self.profile,
                selected_access_path_hard_upper_bound(filter.access()).map(|rows| rows as u64),
            ),
        })
    }
}
