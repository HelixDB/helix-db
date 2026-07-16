//! Delivered properties for selected stream-pipeline operators.

use super::super::*;
use super::variable;

pub(in crate::exec::selected::lowering) fn selected_stream_pipeline_delivered_properties(
    delivered: properties::DeliveredProperties,
    op: &logical::StreamPipelineOp,
) -> properties::DeliveredProperties {
    match op {
        logical::StreamPipelineOp::Filter { .. } => filtered_delivered_properties(delivered),
        logical::StreamPipelineOp::Window { window } => match window.end() {
            Some(end) => range_delivered_properties(delivered, Some((window.start(), end))),
            None if window.start() > 0 => {
                skip_delivered_properties(delivered, Some(window.start()))
            }
            None => delivered,
        },
        logical::StreamPipelineOp::Limit { count } => {
            limit_delivered_properties(delivered, stream_bound_literal(count))
        }
        logical::StreamPipelineOp::Skip { count } => {
            skip_delivered_properties(delivered, stream_bound_literal(count))
        }
        logical::StreamPipelineOp::Range { range } => {
            range_delivered_properties(delivered, stream_range_literal_bounds(range))
        }
        logical::StreamPipelineOp::Order { ordering } => ordered_delivered_properties(
            materialized_delivered_properties(delivered),
            properties::DeliveredOrdering::ByKeys(ordering.clone()),
        ),
        logical::StreamPipelineOp::Expand { plan } => {
            preserve_barrier_effect(delivered, expand_delivered_properties(plan))
        }
        logical::StreamPipelineOp::Variable { op } => {
            variable::selected_stream_variable_delivered_properties(delivered, op)
        }
        logical::StreamPipelineOp::VariableWrite { op } => {
            variable::selected_stream_variable_write_delivered_properties(delivered, op)
        }
        logical::StreamPipelineOp::Distinct => {
            materialized_delivered_properties(filtered_delivered_properties(delivered))
        }
    }
}
