use crate::{cost, ir, logical, physical, properties};

use super::cardinality::{estimated_rows_bounded_by, StreamRowUpperBound};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::rules) enum AccessWindowPhysicalEffect {
    Identity,
    Op(physical::PhysicalPipelineOp),
}

impl AccessWindowPhysicalEffect {
    pub(in crate::rules) fn push_onto(self, ops: &mut Vec<physical::PhysicalPipelineOp>) {
        if let Self::Op(op) = self {
            ops.push(op);
        }
    }

    pub(in crate::rules) fn into_pipeline_op(self) -> physical::PhysicalPipelineOp {
        match self {
            Self::Identity => physical::PhysicalPipelineOp::NoOp,
            Self::Op(op) => op,
        }
    }
}

pub(in crate::rules) fn access_window_stream_contract(
    delivered: properties::DeliveredProperties,
    window: logical::AccessWindowRange,
    rows: cost::EstimatedRows,
    storage: &cost::StorageCostProfile,
) -> (
    AccessWindowPhysicalEffect,
    properties::DeliveredProperties,
    cost::CostVector,
) {
    if let Some(range) = window.bounded_stream_range() {
        let width = range.end().saturating_sub(range.start()) as u64;
        return (
            AccessWindowPhysicalEffect::Op(physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Range,
            )),
            access_window_range_delivered(delivered, range),
            storage.stream_operator(estimated_rows_bounded_by(
                rows,
                StreamRowUpperBound::known(width),
            )),
        );
    }

    if window.start() == 0 {
        return (
            AccessWindowPhysicalEffect::Identity,
            delivered,
            cost::CostVector::ZERO,
        );
    }

    (
        AccessWindowPhysicalEffect::Op(physical::PhysicalPipelineOp::Stream(
            physical::PhysicalStreamOp::Skip,
        )),
        access_window_skip_delivered(delivered, window.start()),
        storage.stream_operator(rows),
    )
}

pub(in crate::rules) fn access_window_range_delivered(
    delivered: properties::DeliveredProperties,
    range: ir::StreamLiteralRange,
) -> properties::DeliveredProperties {
    properties::DeliveredProperties {
        cardinality: delivered
            .cardinality
            .after_range(range.start()..range.end()),
        ..delivered
    }
}

pub(in crate::rules) fn access_window_skip_delivered(
    delivered: properties::DeliveredProperties,
    start: usize,
) -> properties::DeliveredProperties {
    properties::DeliveredProperties {
        cardinality: delivered.cardinality.after_skip(start),
        ..delivered
    }
}
