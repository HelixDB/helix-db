//! Delivered-property recursion for root streams.

use crate::{context, cost, logical, properties};

use super::access;
use crate::rules::physical_contracts::support::{
    aggregate_output_delivered, estimated_pipeline_rows, project_output_delivered,
    reserved_output_delivered, stream_pipeline_op_contract,
    stream_variable_write_delivered_properties,
};

pub(super) fn root_stream_delivered_properties(
    input: &logical::RootStream,
    storage: &cost::StorageCostProfile,
    stats: &context::StatsSnapshot,
) -> properties::DeliveredProperties {
    match RootStreamDeliveredFamily::classify(input) {
        RootStreamDeliveredFamily::Access(access) => {
            access::access_stream_pipeline_contract(access, storage, stats).delivered
        }
        RootStreamDeliveredFamily::VariableSource => properties::DeliveredProperties::default(),
        RootStreamDeliveredFamily::Barrier => root_barrier_delivered(),
        RootStreamDeliveredFamily::Pipeline(pipeline) => {
            delivered_after_root_pipeline(pipeline, storage, stats)
        }
        RootStreamDeliveredFamily::Reserved(reserved) => reserved_output_delivered(
            root_stream_delivered_properties(reserved.input(), storage, stats),
            reserved.op(),
        ),
        RootStreamDeliveredFamily::Project(project) => project_output_delivered(
            root_stream_delivered_properties(project.input(), storage, stats),
            project.projection(),
        ),
        RootStreamDeliveredFamily::Aggregate(aggregate) => aggregate_output_delivered(
            root_stream_delivered_properties(aggregate.input(), storage, stats),
            aggregate.aggregate(),
        ),
        RootStreamDeliveredFamily::VariableWrite(write) => {
            stream_variable_write_delivered_properties(
                root_stream_delivered_properties(write.input(), storage, stats),
                write.op(),
            )
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum RootStreamDeliveredFamily<'a> {
    Access(&'a logical::AccessStream),
    VariableSource,
    Barrier,
    Pipeline(&'a logical::RootPipeline),
    Reserved(&'a logical::StreamReserved),
    Project(&'a logical::StreamProject),
    Aggregate(&'a logical::StreamAggregate),
    VariableWrite(&'a logical::StreamVariableWrite),
}

impl<'a> RootStreamDeliveredFamily<'a> {
    pub(super) fn classify(input: &'a logical::RootStream) -> Self {
        match input {
            logical::RootStream::Access(access) => Self::Access(access),
            logical::RootStream::VariableSource(_) => Self::VariableSource,
            logical::RootStream::Mutation(_)
            | logical::RootStream::Branch(_)
            | logical::RootStream::Repeat(_) => Self::Barrier,
            logical::RootStream::Pipeline(pipeline) => Self::Pipeline(pipeline),
            logical::RootStream::Reserved(reserved) => Self::Reserved(reserved),
            logical::RootStream::Project(project) => Self::Project(project),
            logical::RootStream::Aggregate(aggregate) => Self::Aggregate(aggregate),
            logical::RootStream::VariableWrite(write) => Self::VariableWrite(write),
        }
    }
}

fn delivered_after_root_pipeline(
    pipeline: &logical::RootPipeline,
    storage: &cost::StorageCostProfile,
    stats: &context::StatsSnapshot,
) -> properties::DeliveredProperties {
    pipeline.ops().iter().fold(
        root_stream_delivered_properties(pipeline.input(), storage, stats),
        |delivered, op| {
            let rows = estimated_pipeline_rows(&delivered, storage.default_unknown_scan_rows);
            let (_, next_delivered, _) = stream_pipeline_op_contract(op, delivered, rows, storage);
            next_delivered
        },
    )
}

fn root_barrier_delivered() -> properties::DeliveredProperties {
    properties::DeliveredProperties {
        materialization: properties::Materialization::Materialized,
        effect: properties::EffectKind::Barrier,
        ..properties::DeliveredProperties::default()
    }
}
