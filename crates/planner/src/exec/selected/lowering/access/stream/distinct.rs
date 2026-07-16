//! Selected access-distinct executable lowering.

use super::super::*;

impl ExecutableDagBuilder<'_> {
    pub(in crate::exec::selected::lowering) fn push_selected_access_distinct(
        &mut self,
        distinct: &logical::AccessDistinct,
        pipeline: &physical::PhysicalPipeline,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        let parts = match selected_access_pipeline_parts(distinct.access(), pipeline) {
            SelectedAccessPipelineMatch::Matched(parts) => parts,
            SelectedAccessPipelineMatch::NotMatched(_) => {
                return Err(unsupported_selected_alternative(
                    rejection::Reason::AccessDistinctSourceMismatch,
                ));
            }
        };
        let (access, ops) = parts.into_parts();
        if !matches!(
            ops,
            [physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Distinct
            )]
        ) {
            return Err(unsupported_selected_alternative(
                rejection::Reason::AccessDistinctPhysicalSuffixMismatch,
            ));
        }
        let input_id = self.push_selected_access_path(
            distinct.access(),
            access,
            dependencies,
            ir::BatchOutputPlan::Discard,
            condition.clone(),
        )?;
        self.push_step(StepDraft {
            dependencies: vec![input_id],
            output,
            condition,
            op: ExecOp::Distinct,
            schedule: ExecSchedule::Barrier,
            delivered: materialized_delivered_properties(filtered_delivered_properties(
                selected_access_path_delivered_properties(distinct.access()),
            )),
            cost: self
                .profile
                .explicit_sort(selected_access_path_estimated_rows(
                    distinct.access(),
                    self.profile,
                )),
        })
    }
}
