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
        let parts = match selected_access_pipeline_parts(order.access(), pipeline) {
            SelectedAccessPipelineMatch::Matched(parts) => parts,
            SelectedAccessPipelineMatch::NotMatched(_) => {
                return Err(unsupported_selected_alternative(
                    rejection::Reason::AccessOrderSourceMismatch,
                ));
            }
        };
        let (access, ops) = parts.into_parts();
        if !matches!(ops, [physical::PhysicalPipelineOp::Sort]) {
            return Err(unsupported_selected_alternative(
                rejection::Reason::AccessOrderPhysicalSuffixMismatch,
            ));
        }
        let input_id = self.push_selected_access_path(
            order.access(),
            access,
            dependencies,
            ir::BatchOutputPlan::Discard,
            condition.clone(),
        )?;
        self.push_step(StepDraft {
            dependencies: vec![input_id],
            output,
            condition,
            op: ExecOp::Order {
                plan: ir::OrderPlan::ExplicitSort(order.ordering().clone()),
            },
            schedule: ExecSchedule::Barrier,
            delivered: ordered_delivered_properties(
                materialized_delivered_properties(selected_access_path_delivered_properties(
                    order.access(),
                )),
                properties::DeliveredOrdering::ByKeys(order.ordering().clone()),
            ),
            cost: self
                .profile
                .explicit_sort(selected_access_path_estimated_rows(
                    order.access(),
                    self.profile,
                )),
        })
    }
}
