use crate::{cost, ir, physical};

use super::super::support::{estimated_rows_bounded_by, stream_bound_upper, stream_range_upper};

pub(super) enum PipelineStreamPureOp<'a> {
    Limit { count: &'a ir::StreamBoundPlan },
    Skip,
    Range { range: &'a ir::StreamRangePlan },
    Distinct,
    Expand,
    Project,
    Aggregate,
    Variable,
    Reserved,
}

impl PipelineStreamPureOp<'_> {
    pub(super) fn contract(
        self,
        storage: &cost::StorageCostProfile,
        rows: cost::EstimatedRows,
    ) -> (physical::PhysicalStreamOp, cost::CostVector) {
        match self {
            Self::Limit { count } => (
                physical::PhysicalStreamOp::Limit,
                storage.stream_operator(estimated_rows_bounded_by(rows, stream_bound_upper(count))),
            ),
            Self::Skip => (
                physical::PhysicalStreamOp::Skip,
                storage.stream_operator(rows),
            ),
            Self::Range { range } => (
                physical::PhysicalStreamOp::Range,
                storage.stream_operator(estimated_rows_bounded_by(rows, stream_range_upper(range))),
            ),
            Self::Distinct => (
                physical::PhysicalStreamOp::Distinct,
                storage.explicit_sort(rows),
            ),
            Self::Expand => (
                physical::PhysicalStreamOp::Expand,
                storage.stream_operator(rows),
            ),
            Self::Project => (
                physical::PhysicalStreamOp::Project,
                storage.stream_operator(rows),
            ),
            Self::Aggregate => (
                physical::PhysicalStreamOp::Aggregate,
                storage.explicit_sort(rows),
            ),
            Self::Variable => (
                physical::PhysicalStreamOp::Variable,
                storage.source_inject(),
            ),
            Self::Reserved => (
                physical::PhysicalStreamOp::Reserved,
                storage.stream_operator(rows),
            ),
        }
    }
}
