//! Native stream-bound and range payload validation.

use helix_ast::expr::StreamBound;

use crate::{error, ir};

pub(in crate::planning::selected::native) fn stream_bound_plan(
    bound: StreamBound,
) -> Result<ir::StreamBoundPlan, error::PlannerError> {
    ir::StreamBoundPlan::new(bound).map_err(|err| match err {
        ir::StreamBoundPlanError::StaticLiteral { expected } => {
            error::PlannerError::InvalidStreamBoundExpression { expected }
        }
        ir::StreamBoundPlanError::Expression(err) => err.into(),
    })
}

pub(in crate::planning::selected::native) fn stream_range_plan(
    start: StreamBound,
    end: StreamBound,
) -> Result<ir::StreamRangePlan, error::PlannerError> {
    ir::StreamRangePlan::new(start, end).map_err(|err| match err {
        ir::StreamRangePlanError::InvertedLiteralRange { start, end } => {
            error::PlannerError::InvalidStreamRange { start, end }
        }
        ir::StreamRangePlanError::Bound(ir::StreamBoundPlanError::StaticLiteral { expected }) => {
            error::PlannerError::InvalidStreamBoundExpression { expected }
        }
        ir::StreamRangePlanError::Bound(ir::StreamBoundPlanError::Expression(err)) => err.into(),
    })
}
