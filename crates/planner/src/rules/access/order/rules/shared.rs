//! Shared access-order/distinct rule result helpers.

use super::super::super::super::physical_result;
use crate::{cost, optimizer, physical, properties};

pub(super) fn access_pipeline_result(
    (pipeline, delivered, cost): (
        physical::PhysicalPipeline,
        properties::DeliveredProperties,
        cost::CostVector,
    ),
) -> optimizer::RuleResult {
    physical_result(physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(pipeline),
        delivered,
        cost,
    ))
}
