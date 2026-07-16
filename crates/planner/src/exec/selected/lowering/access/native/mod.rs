//! Selected access-plan executable allocation.
//!
//! This module owns recursive executable step allocation for selected access
//! plans. Simple leaf conversion remains a shared contract, while recursive
//! sets, point IDs, and residual filters stay inside this selected access
//! lowering boundary.

use super::*;
use crate::exec;

mod edge;
mod merge;
mod node;
mod point_ids;

fn compound_access_output(
    read_limit: exec::ExecAccessReadLimit,
    output: &ir::BatchOutputPlan,
) -> ir::BatchOutputPlan {
    match read_limit {
        exec::ExecAccessReadLimit::Unbounded => output.clone(),
        exec::ExecAccessReadLimit::Bounded(_) => ir::BatchOutputPlan::Discard,
    }
}

fn push_compound_access_read_limit(
    lowering: &mut ExecutableDagBuilder<'_>,
    input_id: ExecStepId,
    read_limit: exec::ExecAccessReadLimit,
    delivered: properties::DeliveredProperties,
    output: ir::BatchOutputPlan,
    condition: ExecCondition,
) -> Result<ExecStepId, ExecPlanError> {
    let exec::ExecAccessReadLimit::Bounded(limit) = read_limit else {
        return Ok(input_id);
    };
    let rows = selected_rows_for_delivered(&delivered, lowering.profile);
    let count = limit.get();
    lowering.push_step(StepDraft {
        dependencies: vec![input_id],
        output,
        condition,
        op: ExecOp::Limit {
            count: ir::StreamBoundPlan::Literal(count),
        },
        schedule: ExecSchedule::Pipeline,
        delivered: limit_delivered_properties(delivered, Some(count)),
        cost: lowering
            .profile
            .stream_operator(estimated_rows_bounded_by(rows, Some(count as u64))),
    })
}
