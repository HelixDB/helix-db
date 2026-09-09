use crate::{context, cost, exec, ir, logical, physical, properties};

use super::super::support::{
    access_pipeline_op, access_window_stream_contract, estimated_pipeline_rows,
    physical_pipeline_from_first_and_rest, stream_pipeline_op_contract, with_cardinality,
};
use super::source::access_path_contract;

pub(in crate::rules) fn access_filter_pipeline_contract(
    filter: &logical::AccessFilter,
    storage: &cost::StorageCostProfile,
    stats: &context::StatsSnapshot,
) -> (
    physical::PhysicalPipeline,
    properties::DeliveredProperties,
    cost::CostVector,
) {
    let access = access_path_contract(filter.access(), storage, stats);
    let filtered = with_cardinality(
        access.delivered.clone(),
        access.delivered.cardinality.upper(),
    );
    let predicate_cost = storage.predicate_eval(access.estimated_rows);
    let pipeline = physical::PhysicalPipeline::new(ir::AtLeast::<_, 1>::from_one_and_rest(
        access_pipeline_op(filter.access(), access.access),
        vec![physical::PhysicalPipelineOp::ResidualFilter],
    ));
    (pipeline, filtered, access.cost.serial(predicate_cost))
}

pub(in crate::rules) fn access_window_pipeline_contract(
    window: &logical::AccessWindow,
    storage: &cost::StorageCostProfile,
    stats: &context::StatsSnapshot,
) -> (
    physical::PhysicalPipeline,
    properties::DeliveredProperties,
    cost::CostVector,
) {
    let access = access_path_contract(window.access(), storage, stats);
    let (effect, delivered, window_cost) = access_window_stream_contract(
        access.delivered.clone(),
        window.window(),
        access.estimated_rows,
        storage,
    );
    let mut rest = Vec::new();
    effect.push_onto(&mut rest);
    let pipeline = physical_pipeline_from_first_and_rest(
        access_pipeline_op(window.access(), access.access),
        rest,
    );
    (pipeline, delivered, access.cost.serial(window_cost))
}

pub(in crate::rules) fn access_order_pipeline_contract(
    order: &logical::AccessOrder,
    storage: &cost::StorageCostProfile,
    stats: &context::StatsSnapshot,
) -> (
    physical::PhysicalPipeline,
    properties::DeliveredProperties,
    cost::CostVector,
) {
    let access = access_path_contract(order.access(), storage, stats);
    let satisfied = exec::access_delivers_order(order.access(), order.ordering());
    let sort_cost = if satisfied {
        cost::CostVector::ZERO
    } else {
        storage.property_sort(access.estimated_rows)
    };
    let delivered = properties::DeliveredProperties {
        ordering: properties::DeliveredOrdering::ByKeys(order.ordering().clone()),
        materialization: properties::Materialization::Materialized,
        ..access.delivered.clone()
    };
    let pipeline = physical::PhysicalPipeline::new(ir::AtLeast::<_, 1>::from_one_and_rest(
        access_pipeline_op(order.access(), access.access),
        vec![if satisfied {
            physical::PhysicalPipelineOp::OrderSatisfiedByAccess {
                ordering: order.ordering().clone(),
            }
        } else {
            physical::PhysicalPipelineOp::Sort
        }],
    ));
    (pipeline, delivered, access.cost.serial(sort_cost))
}

pub(in crate::rules) fn access_distinct_pipeline_contract(
    distinct: &logical::AccessDistinct,
    storage: &cost::StorageCostProfile,
    stats: &context::StatsSnapshot,
) -> (
    physical::PhysicalPipeline,
    properties::DeliveredProperties,
    cost::CostVector,
) {
    let access = access_path_contract(distinct.access(), storage, stats);
    let distinct_cost = storage.explicit_sort(access.estimated_rows);
    let delivered = properties::DeliveredProperties {
        cardinality: properties::CardinalityBounds::zero_to(access.delivered.cardinality.upper()),
        materialization: properties::Materialization::Materialized,
        ..access.delivered.clone()
    };
    let pipeline = physical::PhysicalPipeline::new(ir::AtLeast::<_, 1>::from_one_and_rest(
        access_pipeline_op(distinct.access(), access.access),
        vec![physical::PhysicalPipelineOp::Stream(
            physical::PhysicalStreamOp::Distinct,
        )],
    ));
    (pipeline, delivered, access.cost.serial(distinct_cost))
}

pub(in crate::rules) fn access_pipeline_physical_contract(
    pipeline: &logical::AccessPipeline,
    storage: &cost::StorageCostProfile,
    stats: &context::StatsSnapshot,
) -> (
    physical::PhysicalPipeline,
    properties::DeliveredProperties,
    cost::CostVector,
) {
    let access = access_path_contract(pipeline.access(), storage, stats);
    let first = access_pipeline_op(pipeline.access(), access.access);
    let mut rest = Vec::new();
    let mut delivered = access.delivered;
    let mut rows = access.estimated_rows;
    let mut total_cost = access.cost;

    let mut push_limit = exec::range_access_can_push_limit(pipeline.access());
    for op in pipeline.ops() {
        let (mut physical_op, next_delivered, mut op_cost) =
            stream_pipeline_op_contract(op, delivered.clone(), rows, storage);
        match op {
            logical::StreamPipelineOp::Order { ordering }
                if exec::access_delivers_order(pipeline.access(), ordering)
                    && (delivered
                        .cardinality
                        .upper()
                        .is_some_and(|upper| upper <= 1)
                        || delivered.ordering.satisfies(
                            &properties::RequiredOrdering::ByKeys(ordering.clone()),
                        )) =>
            {
                physical_op = physical::PhysicalPipelineOp::OrderSatisfiedByAccess {
                    ordering: ordering.clone(),
                };
                op_cost = cost::CostVector::ZERO;
            }
            logical::StreamPipelineOp::Limit { count } if push_limit => {
                physical_op = physical::PhysicalPipelineOp::AccessReadLimit {
                    count: count.clone(),
                };
                op_cost = cost::CostVector::ZERO;
            }
            logical::StreamPipelineOp::Window { window }
                if push_limit && window.start() == 0 && window.end().is_some() =>
            {
                physical_op = physical::PhysicalPipelineOp::AccessReadLimit {
                    count: ir::StreamBoundPlan::Literal(window.end().expect("bounded window")),
                };
                op_cost = cost::CostVector::ZERO;
            }
            _ => push_limit = false,
        }
        rest.push(physical_op);
        delivered = next_delivered;
        rows = estimated_pipeline_rows(&delivered, rows);
        total_cost = total_cost.serial(op_cost);
    }

    (
        physical_pipeline_from_first_and_rest(first, rest),
        delivered,
        total_cost,
    )
}
