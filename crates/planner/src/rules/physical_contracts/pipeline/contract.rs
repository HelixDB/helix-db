use super::{delivered, op};
use crate::{cost, ir, logical, physical, properties};

use super::super::support::estimated_pipeline_rows;

pub(in crate::rules) fn physical_pipeline_contract(
    ops: &ir::AtLeast<logical::PureLogicalOp, 1>,
    storage: &cost::StorageCostProfile,
) -> (
    physical::PhysicalPipeline,
    properties::DeliveredProperties,
    cost::CostVector,
) {
    let mut delivered = properties::DeliveredProperties::default();
    let mut total_cost = cost::CostVector::ZERO;
    let physical_ops = ops.map_ref(|op| {
        let rows = estimated_pipeline_rows(&delivered, storage.default_unknown_scan_rows);
        let (physical_op, op_cost) = op::physical_pipeline_op_contract(op, storage, rows);
        delivered = delivered::delivered_after_pipeline_op(delivered.clone(), op);
        total_cost = total_cost.serial(op_cost);
        physical_op
    });

    (
        physical::PhysicalPipeline::new(physical_ops),
        delivered,
        total_cost,
    )
}
