use super::*;

pub(in crate::exec::selected::lowering) fn selected_mutation_step_draft(
    dependencies: Vec<ExecStepId>,
    output: ir::BatchOutputPlan,
    condition: ExecCondition,
    plan: ExecMutationPlan,
    delivered: properties::DeliveredProperties,
    cost: cost::CostVector,
) -> StepDraft {
    StepDraft {
        dependencies,
        output,
        condition,
        op: ExecOp::Mutation { plan },
        schedule: ExecSchedule::Barrier,
        delivered,
        cost,
    }
}

pub(in crate::exec::selected::lowering) fn selected_access_window_step_draft(
    window: logical::AccessWindowRange,
    input_id: ExecStepId,
    delivered: properties::DeliveredProperties,
    rows: cost::EstimatedRows,
    output: ir::BatchOutputPlan,
    condition: ExecCondition,
    profile: &cost::StorageCostProfile,
) -> Result<StepDraft, ExecPlanError> {
    if let Some(range) = window.bounded_stream_range() {
        let width = range.end().saturating_sub(range.start()) as u64;
        return Ok(StepDraft {
            dependencies: vec![input_id],
            output,
            condition,
            op: ExecOp::Range {
                range: ir::StreamRangePlan::Literal(range),
            },
            schedule: ExecSchedule::Pipeline,
            delivered: range_delivered_properties(delivered, Some((range.start(), range.end()))),
            cost: profile.stream_operator(estimated_rows_bounded_by(rows, Some(width))),
        });
    }

    if window.start() > 0 {
        return Ok(StepDraft {
            dependencies: vec![input_id],
            output,
            condition,
            op: ExecOp::Skip {
                count: ir::StreamBoundPlan::Literal(window.start()),
            },
            schedule: ExecSchedule::Pipeline,
            delivered: skip_delivered_properties(delivered, Some(window.start())),
            cost: profile.stream_operator(rows),
        });
    }

    Err(unsupported_selected_alternative(
        rejection::Reason::AccessPipelineIdentityWindow,
    ))
}
