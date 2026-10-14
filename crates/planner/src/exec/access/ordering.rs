//! Ordering proofs use the same executable driver selection as lowering.

use crate::{catalog, exec, ir, logical, properties};

pub(crate) fn range_ordering(
    key: &catalog::ScopedPropertyDirectionKey,
    iteration: ir::RangeScanIteration,
) -> properties::DeliveredOrdering {
    properties::DeliveredOrdering::ByKeys(ir::OrderKeys::from(ir::OrderKey {
        property: key.property.clone(),
        order: match iteration.effective_direction(key.direction) {
            helix_ast::index::RangeIndexDirection::Asc => helix_ast::traversal::Order::Asc,
            helix_ast::index::RangeIndexDirection::Desc => helix_ast::traversal::Order::Desc,
        },
    }))
}

pub(crate) fn node_range_ordering(plan: &ir::NodeAccessPlan) -> properties::DeliveredOrdering {
    match exec::node_secondary_set(plan) {
        Some(
            exec::ExecNodeSecondarySetPlan::Range(driver)
            | exec::ExecNodeSecondarySetPlan::OrderedIntersect { driver, .. },
        ) => range_ordering(&driver.key, driver.iteration),
        _ => properties::DeliveredOrdering::Unordered,
    }
}

pub(crate) fn edge_range_ordering(plan: &ir::EdgeAccessPlan) -> properties::DeliveredOrdering {
    match exec::edge_secondary_set(plan) {
        Some(
            exec::ExecEdgeSecondarySetPlan::Range(driver)
            | exec::ExecEdgeSecondarySetPlan::OrderedIntersect { driver, .. },
        ) => range_ordering(&driver.key, driver.iteration),
        _ => properties::DeliveredOrdering::Unordered,
    }
}

pub(crate) fn access_delivers_order(
    access: &logical::AccessPath,
    required: &ir::OrderKeys,
) -> bool {
    if access
        .hard_cardinality_upper_bound()
        .is_some_and(|upper| upper <= 1)
    {
        return true;
    }
    let ordering = match access {
        logical::AccessPath::Node(path) => node_range_ordering(path.source().as_ref()),
        logical::AccessPath::Edge(path) => edge_range_ordering(path.source().as_ref()),
    };
    ordering.satisfies(&properties::RequiredOrdering::ByKeys(required.clone()))
}

/// Runtime bound pushdown is deliberately restricted to ordered range access
/// whose remaining membership work is fully represented by index inputs.
pub(crate) fn range_access_can_push_limit(access: &logical::AccessPath) -> bool {
    match access {
        logical::AccessPath::Node(path) => match exec::node_secondary_set(path.source().as_ref()) {
            Some(exec::ExecNodeSecondarySetPlan::Range(_)) => true,
            Some(exec::ExecNodeSecondarySetPlan::OrderedIntersect { filters, .. }) => {
                filters.iter().all(node_index_membership)
            }
            _ => false,
        },
        logical::AccessPath::Edge(path) => match exec::edge_secondary_set(path.source().as_ref()) {
            Some(exec::ExecEdgeSecondarySetPlan::Range(_)) => true,
            Some(exec::ExecEdgeSecondarySetPlan::OrderedIntersect { filters, .. }) => {
                filters.iter().all(edge_index_membership)
            }
            _ => false,
        },
    }
}

fn node_index_membership(set: &exec::ExecNodeSecondarySetPlan) -> bool {
    match set {
        exec::ExecNodeSecondarySetPlan::AuthoritativeScan(_) => false,
        exec::ExecNodeSecondarySetPlan::Intersect { driver, rest }
        | exec::ExecNodeSecondarySetPlan::Union { driver, rest } => {
            node_index_membership(driver) && rest.iter().all(node_index_membership)
        }
        exec::ExecNodeSecondarySetPlan::OrderedIntersect { filters, .. } => {
            filters.iter().all(node_index_membership)
        }
        _ => true,
    }
}

fn edge_index_membership(set: &exec::ExecEdgeSecondarySetPlan) -> bool {
    match set {
        exec::ExecEdgeSecondarySetPlan::AuthoritativeScan(_) => false,
        exec::ExecEdgeSecondarySetPlan::Intersect { driver, rest }
        | exec::ExecEdgeSecondarySetPlan::Union { driver, rest } => {
            edge_index_membership(driver) && rest.iter().all(edge_index_membership)
        }
        exec::ExecEdgeSecondarySetPlan::OrderedIntersect { filters, .. } => {
            filters.iter().all(edge_index_membership)
        }
        _ => true,
    }
}

/// Recheck the actual executable driver after all source lowering and bounds.
pub(crate) fn executable_range_ordering(
    plan: &exec::ExecAccessPlan,
) -> properties::DeliveredOrdering {
    match plan {
        exec::ExecAccessPlan::Limited(limited) => executable_range_ordering(limited.source()),
        exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::RangeIndex {
            key, iteration, ..
        })
        | exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::RangeIndex {
            key, iteration, ..
        }) => range_ordering(key, *iteration),
        exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::SecondarySet {
            set:
                exec::ExecNodeSecondarySetPlan::Range(driver)
                | exec::ExecNodeSecondarySetPlan::OrderedIntersect { driver, .. },
        }) => range_ordering(&driver.key, driver.iteration),
        exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::SecondarySet {
            set:
                exec::ExecEdgeSecondarySetPlan::Range(driver)
                | exec::ExecEdgeSecondarySetPlan::OrderedIntersect { driver, .. },
        }) => range_ordering(&driver.key, driver.iteration),
        _ => properties::DeliveredOrdering::Unordered,
    }
}
