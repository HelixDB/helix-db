use super::stream_op::PipelineStreamPureOp;
use crate::{cost, exec, logical, physical};

use super::super::support::element_keyspace;

pub(super) fn physical_pipeline_op_contract(
    op: &logical::PureLogicalOp,
    storage: &cost::StorageCostProfile,
    rows: cost::EstimatedRows,
) -> (physical::PhysicalPipelineOp, cost::CostVector) {
    match op {
        logical::PureLogicalOp::NoOp => {
            (physical::PhysicalPipelineOp::NoOp, cost::CostVector::ZERO)
        }
        logical::PureLogicalOp::Empty => {
            (physical::PhysicalPipelineOp::Empty, cost::CostVector::ZERO)
        }
        logical::PureLogicalOp::Source { element } => (
            physical::PhysicalPipelineOp::Access {
                element: *element,
                access: physical::PhysicalAccess::Kv(exec::KvReadPlan::RangeScan {
                    keyspace: element_keyspace(*element),
                    start: exec::KvKeyBound::Unbounded,
                    end: exec::KvKeyBound::Unbounded,
                    limit: None,
                }),
            },
            storage.range_scan(storage.default_unknown_scan_rows),
        ),
        logical::PureLogicalOp::Filter { .. } => (
            physical::PhysicalPipelineOp::ResidualFilter,
            storage.predicate_eval(rows),
        ),
        logical::PureLogicalOp::Order { .. } => (
            physical::PhysicalPipelineOp::Sort,
            storage.explicit_sort(rows),
        ),
        logical::PureLogicalOp::Limit { count } => {
            let (op, cost) = PipelineStreamPureOp::Limit { count }.contract(storage, rows);
            (physical::PhysicalPipelineOp::Stream(op), cost)
        }
        logical::PureLogicalOp::Skip { .. } => {
            let (op, cost) = PipelineStreamPureOp::Skip.contract(storage, rows);
            (physical::PhysicalPipelineOp::Stream(op), cost)
        }
        logical::PureLogicalOp::Range { range } => {
            let (op, cost) = PipelineStreamPureOp::Range { range }.contract(storage, rows);
            (physical::PhysicalPipelineOp::Stream(op), cost)
        }
        logical::PureLogicalOp::Distinct => {
            let (op, cost) = PipelineStreamPureOp::Distinct.contract(storage, rows);
            (physical::PhysicalPipelineOp::Stream(op), cost)
        }
        logical::PureLogicalOp::Expand { .. } => {
            let (op, cost) = PipelineStreamPureOp::Expand.contract(storage, rows);
            (physical::PhysicalPipelineOp::Stream(op), cost)
        }
        logical::PureLogicalOp::Project => {
            let (op, cost) = PipelineStreamPureOp::Project.contract(storage, rows);
            (physical::PhysicalPipelineOp::Stream(op), cost)
        }
        logical::PureLogicalOp::Aggregate => {
            let (op, cost) = PipelineStreamPureOp::Aggregate.contract(storage, rows);
            (physical::PhysicalPipelineOp::Stream(op), cost)
        }
        logical::PureLogicalOp::Variable => {
            let (op, cost) = PipelineStreamPureOp::Variable.contract(storage, rows);
            (physical::PhysicalPipelineOp::Stream(op), cost)
        }
        logical::PureLogicalOp::Reserved => {
            let (op, cost) = PipelineStreamPureOp::Reserved.contract(storage, rows);
            (physical::PhysicalPipelineOp::Stream(op), cost)
        }
    }
}
