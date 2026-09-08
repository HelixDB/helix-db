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

        if ops.iter().any(|op| {
            matches!(
                op,
                physical::PhysicalPipelineOp::OrderSatisfiedByAccess { .. }
                    | physical::PhysicalPipelineOp::AccessReadLimit { .. }
            )
        }) {
            return self.push_selected_range_pipeline(
                access_pipeline,
                access,
                ops,
                dependencies,
                output,
                condition,
            );
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
            | logical::StreamPipelineOp::VectorSearch { .. }
            | logical::StreamPipelineOp::TextSearch { .. }
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

impl ExecutableDagBuilder<'_> {
    #[allow(clippy::too_many_arguments)]
    fn push_selected_range_pipeline(
        &mut self,
        pipeline: &logical::AccessPipeline,
        access: &physical::PhysicalAccess,
        physical_ops: &[physical::PhysicalPipelineOp],
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        let elided = |op: &physical::PhysicalPipelineOp| {
            matches!(
                op,
                physical::PhysicalPipelineOp::OrderSatisfiedByAccess { .. }
                    | physical::PhysicalPipelineOp::AccessReadLimit { .. }
            )
        };
        let last_emitted = physical_ops.iter().rposition(|op| !elided(op));
        let mut input_id = self.push_selected_access_path(
            pipeline.access(),
            access,
            dependencies,
            if last_emitted.is_none() {
                output.clone()
            } else {
                ir::BatchOutputPlan::Discard
            },
            condition.clone(),
        )?;
        // These are final executable steps, not estimates or a previously
        // rewritten logical driver. Check them before removing any ordering.
        let step = self
            .steps
            .iter_mut()
            .find(|step| step.id == input_id)
            .expect("the access step was just allocated");
        let mut bounds = Vec::new();
        let mut current_delivered = selected_access_path_delivered_properties(pipeline.access());
        let mut can_push_limit = crate::exec::range_access_can_push_limit(pipeline.access());
        for (logical_op, op) in pipeline.ops().iter().zip(physical_ops) {
            match op {
                physical::PhysicalPipelineOp::OrderSatisfiedByAccess { ordering } => {
                    let required = properties::RequiredOrdering::ByKeys(ordering.clone());
                    let proven = current_delivered
                        .cardinality
                        .upper()
                        .is_some_and(|upper| upper <= 1)
                        || (current_delivered.ordering.satisfies(&required)
                            && matches!(&step.op, ExecOp::Access { plan }
                                if crate::exec::executable_range_ordering(plan).satisfies(&required)));
                    if !proven {
                        return Err(unsupported_selected_alternative(
                            rejection::Reason::AccessOrderPhysicalSuffixMismatch,
                        ));
                    }
                }
                physical::PhysicalPipelineOp::AccessReadLimit { count } if can_push_limit => {
                    bounds.push(crate::exec::ExecAccessLimit::from_stream_bound(count));
                }
                physical::PhysicalPipelineOp::AccessReadLimit { .. } => {
                    return Err(unsupported_selected_alternative(
                        rejection::Reason::AccessPipelinePhysicalSuffixMismatch,
                    ));
                }
                _ => {
                    // Preserve the existing static offset-window read cap. The
                    // offset operator remains; runtime bounds cannot cross it.
                    match logical_op {
                        logical::StreamPipelineOp::Window { window } if can_push_limit => {
                            if let crate::exec::ExecAccessReadLimit::Bounded(limit) =
                                WindowAccessReadPlan::for_window(access, *window).read_limit()
                            {
                                bounds.push(crate::exec::ExecAccessLimit::Static(limit));
                            }
                        }
                        _ => {}
                    }
                    can_push_limit = false;
                }
            }
            current_delivered =
                selected_stream_pipeline_delivered_properties(current_delivered, logical_op);
        }
        if !bounds.is_empty() {
            let ExecOp::Access { plan } = &mut step.op else {
                return Err(unsupported_selected_alternative(
                    rejection::Reason::AccessPipelinePhysicalSuffixMismatch,
                ));
            };
            **plan = bounds
                .into_iter()
                .fold((**plan).clone(), |source, bound| source.limited_by(bound));
        }
        let mut delivered = selected_access_path_delivered_properties(pipeline.access());
        for (index, (logical_op, physical_op)) in
            pipeline.ops().iter().zip(physical_ops).enumerate()
        {
            if !elided(physical_op) {
                input_id = self.push_selected_stream_pipeline_op(
                    logical_op,
                    input_id,
                    delivered.clone(),
                    if Some(index) == last_emitted {
                        output.clone()
                    } else {
                        ir::BatchOutputPlan::Discard
                    },
                    condition.clone(),
                )?;
            }
            delivered = selected_stream_pipeline_delivered_properties(delivered, logical_op);
        }
        Ok(input_id)
    }
}
