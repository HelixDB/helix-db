//! Access delivered-ordering inference.

use crate::{catalog, ir, properties};

pub(super) fn range_ordering_from_node_access(
    plan: &ir::NodeAccessPlan,
) -> properties::DeliveredOrdering {
    match plan {
        ir::NodeAccessPlan::RangeIndex { key, .. } => range_ordering(key),
        _ => properties::DeliveredOrdering::Unordered,
    }
}

pub(super) fn range_ordering_from_edge_access(
    plan: &ir::EdgeAccessPlan,
) -> properties::DeliveredOrdering {
    match plan {
        ir::EdgeAccessPlan::RangeIndex { key, .. } => range_ordering(key),
        _ => properties::DeliveredOrdering::Unordered,
    }
}

fn range_ordering(key: &catalog::ScopedPropertyDirectionKey) -> properties::DeliveredOrdering {
    let order = match key.direction {
        helix_ast::index::RangeIndexDirection::Asc => helix_ast::traversal::Order::Asc,
        helix_ast::index::RangeIndexDirection::Desc => helix_ast::traversal::Order::Desc,
    };
    properties::DeliveredOrdering::ByKeys(
        ir::OrderKey {
            property: key.property.clone(),
            order,
        }
        .into(),
    )
}
