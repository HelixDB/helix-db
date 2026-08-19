//! Public root-stream physical-contract entrypoints.

use crate::{context, cost, logical, physical, properties};

use super::{access, delivered};
use crate::rules::physical_contracts::support::{
    aggregate_output_delivered, estimated_pipeline_rows,
    physical_pipeline_from_prefix_and_required_suffix,
    physical_pipeline_from_prefix_and_required_tail, project_output_delivered,
    reserved_output_delivered, stream_pipeline_op_contract,
    stream_variable_write_delivered_properties,
};

pub(in crate::rules) fn root_pipeline_physical_contract(
    pipeline: &logical::RootPipeline,
    storage: &cost::StorageCostProfile,
    stats: &context::StatsSnapshot,
) -> (
    physical::PhysicalPipeline,
    properties::DeliveredProperties,
    cost::CostVector,
) {
    let mut contract = root_stream_pipeline_contract(pipeline.input(), storage, stats);
    let mut rows = estimated_pipeline_rows(&contract.delivered, storage.default_unknown_scan_rows);

    let suffix = pipeline.ops_at_least().map_ref(|op| {
        let (physical_op, next_delivered, op_cost) =
            stream_pipeline_op_contract(op, contract.delivered.clone(), rows, storage);
        contract.delivered = next_delivered;
        rows = estimated_pipeline_rows(&contract.delivered, rows);
        contract.cost = contract.cost.serial(op_cost);
        physical_op
    });
    (
        physical_pipeline_from_prefix_and_required_suffix(contract.ops, suffix),
        contract.delivered,
        contract.cost,
    )
}

pub(in crate::rules) fn stream_project_pipeline_contract(
    project: &logical::StreamProject,
    storage: &cost::StorageCostProfile,
    stats: &context::StatsSnapshot,
) -> (
    physical::PhysicalPipeline,
    properties::DeliveredProperties,
    cost::CostVector,
) {
    let contract = root_stream_pipeline_contract(project.input(), storage, stats);
    let rows = estimated_pipeline_rows(&contract.delivered, storage.default_unknown_scan_rows);
    (
        physical_pipeline_from_prefix_and_required_tail(
            contract.ops,
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Project),
        ),
        project_output_delivered(contract.delivered, project.projection()),
        contract.cost.serial(storage.stream_operator(rows)),
    )
}

pub(in crate::rules) fn stream_aggregate_pipeline_contract(
    aggregate: &logical::StreamAggregate,
    storage: &cost::StorageCostProfile,
    stats: &context::StatsSnapshot,
) -> (
    physical::PhysicalPipeline,
    properties::DeliveredProperties,
    cost::CostVector,
) {
    let contract = root_stream_pipeline_contract(aggregate.input(), storage, stats);
    let rows = estimated_pipeline_rows(&contract.delivered, storage.default_unknown_scan_rows);
    (
        physical_pipeline_from_prefix_and_required_tail(
            contract.ops,
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Aggregate),
        ),
        aggregate_output_delivered(contract.delivered, aggregate.aggregate()),
        contract.cost.serial(storage.explicit_sort(rows)),
    )
}

pub(in crate::rules) fn stream_variable_write_pipeline_contract(
    write: &logical::StreamVariableWrite,
    storage: &cost::StorageCostProfile,
    stats: &context::StatsSnapshot,
) -> (
    physical::PhysicalPipeline,
    properties::DeliveredProperties,
    cost::CostVector,
) {
    let contract = root_stream_pipeline_contract(write.input(), storage, stats);
    let rows = estimated_pipeline_rows(&contract.delivered, storage.default_unknown_scan_rows);
    (
        physical_pipeline_from_prefix_and_required_tail(
            contract.ops,
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Variable),
        ),
        stream_variable_write_delivered_properties(contract.delivered, write.op()),
        contract.cost.serial(storage.stream_operator(rows)),
    )
}

pub(in crate::rules) fn stream_reserved_pipeline_contract(
    reserved: &logical::StreamReserved,
    storage: &cost::StorageCostProfile,
    stats: &context::StatsSnapshot,
) -> (
    physical::PhysicalPipeline,
    properties::DeliveredProperties,
    cost::CostVector,
) {
    let contract = root_stream_pipeline_contract(reserved.input(), storage, stats);
    let rows = estimated_pipeline_rows(&contract.delivered, storage.default_unknown_scan_rows);
    (
        physical_pipeline_from_prefix_and_required_tail(
            contract.ops,
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Reserved),
        ),
        reserved_output_delivered(contract.delivered, reserved.op()),
        contract.cost.serial(storage.stream_operator(rows)),
    )
}

pub(super) struct RootStreamPipelineContract {
    pub(super) ops: Vec<physical::PhysicalPipelineOp>,
    pub(super) delivered: properties::DeliveredProperties,
    pub(super) cost: cost::CostVector,
}

impl RootStreamPipelineContract {
    pub(super) fn new(
        ops: Vec<physical::PhysicalPipelineOp>,
        delivered: properties::DeliveredProperties,
        cost: cost::CostVector,
    ) -> Self {
        Self {
            ops,
            delivered,
            cost,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum RootStreamPipelineFamily<'a> {
    Access(&'a logical::AccessStream),
    VariableSource,
    Localized(&'a logical::RootStream),
}

impl<'a> RootStreamPipelineFamily<'a> {
    pub(super) fn classify(input: &'a logical::RootStream) -> Self {
        match input {
            logical::RootStream::Access(access) => Self::Access(access),
            logical::RootStream::VariableSource(_) => Self::VariableSource,
            logical::RootStream::Mutation(_)
            | logical::RootStream::Branch(_)
            | logical::RootStream::Repeat(_)
            | logical::RootStream::Pipeline(_)
            | logical::RootStream::Reserved(_)
            | logical::RootStream::Project(_)
            | logical::RootStream::Cardinality(_)
            | logical::RootStream::Aggregate(_)
            | logical::RootStream::VariableWrite(_) => Self::Localized(input),
        }
    }
}

fn root_stream_pipeline_contract(
    input: &logical::RootStream,
    storage: &cost::StorageCostProfile,
    stats: &context::StatsSnapshot,
) -> RootStreamPipelineContract {
    match RootStreamPipelineFamily::classify(input) {
        RootStreamPipelineFamily::Access(access) => {
            access::access_stream_pipeline_contract(access, storage, stats)
        }
        RootStreamPipelineFamily::VariableSource => RootStreamPipelineContract::new(
            vec![physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Variable,
            )],
            properties::DeliveredProperties::default(),
            storage.source_inject(),
        ),
        RootStreamPipelineFamily::Localized(input) => RootStreamPipelineContract::new(
            Vec::new(),
            delivered::root_stream_delivered_properties(input, storage, stats),
            cost::CostVector::ZERO,
        ),
    }
}
