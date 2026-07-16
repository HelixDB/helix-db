//! Stream literal-bound extraction contracts.

use crate::ir;

pub(in crate::exec) fn stream_bound_literal(bound: &ir::StreamBoundPlan) -> Option<usize> {
    match bound {
        ir::StreamBoundPlan::Literal(value) => Some(*value),
        ir::StreamBoundPlan::Expr(_) => None,
    }
}

pub(in crate::exec) fn stream_range_literal_bounds(
    range: &ir::StreamRangePlan,
) -> Option<(usize, usize)> {
    match range {
        ir::StreamRangePlan::Literal(range) => Some((range.start(), range.end())),
        ir::StreamRangePlan::Dynamic(_) => None,
    }
}
