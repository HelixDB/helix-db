//! Selected access-order executable lowering.

use super::super::*;

impl ExecutableDagBuilder<'_> {
    pub(in crate::exec::selected::lowering) fn push_selected_access_order(
        &mut self,
        order: &logical::AccessOrder,
        pipeline: &physical::PhysicalPipeline,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        if !matches!(
            selected_access_pipeline_parts(order.access(), pipeline),
            SelectedAccessPipelineMatch::Matched(_)
        ) {
            return Err(unsupported_selected_alternative(
                rejection::Reason::AccessOrderSourceMismatch,
            ));
        }
        self.push_selected_access_pipeline(
            &logical::AccessPipeline::new(
                order.access().clone(),
                ir::AtLeast::from_one(logical::StreamPipelineOp::Order {
                    ordering: order.ordering().clone(),
                }),
            )
            .expect("a single ordering is a canonical pipeline"),
            pipeline,
            dependencies,
            output,
            condition,
        )
    }
}
