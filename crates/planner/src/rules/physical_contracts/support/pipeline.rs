use crate::{cost, ir, logical, physical, properties};

use super::cardinality::{
    estimated_rows_bounded_by, stream_bound_upper, stream_range_upper, with_cardinality,
};
use super::delivered::{
    access_expand_delivered_properties, preserve_barrier_effect,
    stream_variable_delivered_properties, stream_variable_write_delivered_properties,
};
use super::window::access_window_stream_contract;

pub(in crate::rules) fn physical_pipeline_from_first_and_rest(
    first: physical::PhysicalPipelineOp,
    rest: Vec<physical::PhysicalPipelineOp>,
) -> physical::PhysicalPipeline {
    physical::PhysicalPipeline::new(ir::AtLeast::<_, 1>::from_one_and_rest(first, rest))
}

pub(in crate::rules) fn physical_pipeline_from_prefix_and_required_tail(
    prefix: Vec<physical::PhysicalPipelineOp>,
    tail: physical::PhysicalPipelineOp,
) -> physical::PhysicalPipeline {
    let mut prefix = prefix.into_iter();
    match prefix.next() {
        Some(first) => {
            let mut rest = prefix.collect::<Vec<_>>();
            rest.push(tail);
            physical_pipeline_from_first_and_rest(first, rest)
        }
        None => physical::PhysicalPipeline::new(ir::AtLeast::<_, 1>::from_one(tail)),
    }
}

pub(in crate::rules) fn physical_pipeline_from_prefix_and_required_suffix(
    prefix: Vec<physical::PhysicalPipelineOp>,
    suffix: ir::AtLeast<physical::PhysicalPipelineOp, 1>,
) -> physical::PhysicalPipeline {
    let (suffix_first, suffix_rest) = suffix.into_first_and_rest();
    let mut prefix = prefix.into_iter();
    match prefix.next() {
        Some(first) => {
            let mut rest = prefix.collect::<Vec<_>>();
            rest.push(suffix_first);
            rest.extend(suffix_rest);
            physical_pipeline_from_first_and_rest(first, rest)
        }
        None => physical_pipeline_from_first_and_rest(suffix_first, suffix_rest),
    }
}

pub(in crate::rules) fn stream_pipeline_op_contract(
    op: &logical::StreamPipelineOp,
    delivered: properties::DeliveredProperties,
    rows: cost::EstimatedRows,
    storage: &cost::StorageCostProfile,
) -> (
    physical::PhysicalPipelineOp,
    properties::DeliveredProperties,
    cost::CostVector,
) {
    match op {
        logical::StreamPipelineOp::Filter { .. } => {
            let upper = delivered.cardinality.upper();
            (
                physical::PhysicalPipelineOp::ResidualFilter,
                with_cardinality(delivered, upper),
                storage.predicate_eval(rows),
            )
        }
        logical::StreamPipelineOp::Window { window } => {
            let (effect, delivered, cost) =
                access_window_stream_contract(delivered, *window, rows, storage);
            (effect.into_pipeline_op(), delivered, cost)
        }
        logical::StreamPipelineOp::Limit { count } => {
            let cardinality = match count {
                ir::StreamBoundPlan::Literal(count) => delivered.cardinality.after_limit(*count),
                ir::StreamBoundPlan::Expr(_) => delivered.cardinality,
            };
            (
                physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Limit),
                properties::DeliveredProperties {
                    cardinality,
                    ..delivered
                },
                storage.stream_operator(estimated_rows_bounded_by(rows, stream_bound_upper(count))),
            )
        }
        logical::StreamPipelineOp::Skip { count } => {
            let cardinality = match count {
                ir::StreamBoundPlan::Literal(count) => delivered.cardinality.after_skip(*count),
                ir::StreamBoundPlan::Expr(_) => delivered.cardinality,
            };
            (
                physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Skip),
                properties::DeliveredProperties {
                    cardinality,
                    ..delivered
                },
                storage.stream_operator(rows),
            )
        }
        logical::StreamPipelineOp::Range { range } => {
            let cardinality = match range {
                ir::StreamRangePlan::Literal(range) => delivered
                    .cardinality
                    .after_range(range.start()..range.end()),
                ir::StreamRangePlan::Dynamic(_) => delivered.cardinality,
            };
            (
                physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Range),
                properties::DeliveredProperties {
                    cardinality,
                    ..delivered
                },
                storage.stream_operator(estimated_rows_bounded_by(rows, stream_range_upper(range))),
            )
        }
        logical::StreamPipelineOp::Order { ordering } => (
            physical::PhysicalPipelineOp::Sort,
            properties::DeliveredProperties {
                ordering: properties::DeliveredOrdering::ByKeys(ordering.clone()),
                materialization: properties::Materialization::Materialized,
                ..delivered
            },
            storage.explicit_sort(rows),
        ),
        logical::StreamPipelineOp::Expand { plan } => (
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Expand),
            preserve_barrier_effect(delivered, access_expand_delivered_properties(plan)),
            storage.stream_operator(rows),
        ),
        logical::StreamPipelineOp::Variable { op } => (
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Variable),
            stream_variable_delivered_properties(delivered, op),
            storage.stream_operator(rows),
        ),
        logical::StreamPipelineOp::VariableWrite { op } => (
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Variable),
            stream_variable_write_delivered_properties(delivered, op),
            storage.stream_operator(rows),
        ),
        logical::StreamPipelineOp::Distinct => (
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Distinct),
            properties::DeliveredProperties {
                cardinality: properties::CardinalityBounds::zero_to(delivered.cardinality.upper()),
                materialization: properties::Materialization::Materialized,
                ..delivered
            },
            storage.explicit_sort(rows),
        ),
    }
}

pub(in crate::rules) fn access_pipeline_op(
    access_path: &logical::AccessPath,
    access: physical::PhysicalAccess,
) -> physical::PhysicalPipelineOp {
    physical::PhysicalPipelineOp::Access {
        element: access_path.element(),
        access,
    }
}
