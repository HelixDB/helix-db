//! Delivered order follows the actual secondary-set lowering driver.
use crate::{exec, ir, properties};
pub(super) fn range_ordering_from_node_access(
    plan: &ir::NodeAccessPlan,
) -> properties::DeliveredOrdering {
    exec::node_range_ordering(plan)
}
pub(super) fn range_ordering_from_edge_access(
    plan: &ir::EdgeAccessPlan,
) -> properties::DeliveredOrdering {
    exec::edge_range_ordering(plan)
}
