//! Access-order scheduler predicates.

use super::super::{AccessOrder, AccessPath};
use crate::{catalog, ir};

pub(in crate::logical::access::unary) fn order_has_order_elision_candidate(
    order: &AccessOrder,
) -> bool {
    order
        .access()
        .hard_cardinality_upper_bound()
        .is_some_and(|upper| upper <= 1)
        || access_order_satisfaction_candidate(order.access(), order.ordering())
}

pub(in crate::logical::access::unary) fn order_has_range_direction_candidate(
    order: &AccessOrder,
) -> bool {
    let [required] = order.ordering().as_ref() else {
        return false;
    };
    access_range_direction_candidate(order.access(), required)
}

fn access_range_direction_candidate(access: &AccessPath, required: &ir::OrderKey) -> bool {
    match access {
        AccessPath::Node(path) => match path.source().as_ref() {
            ir::NodeAccessPlan::RangeIndex { key, .. } => range_direction_candidate(key, required),
            _ => false,
        },
        AccessPath::Edge(path) => match path.source().as_ref() {
            ir::EdgeAccessPlan::RangeIndex { key, .. } => range_direction_candidate(key, required),
            _ => false,
        },
    }
}

fn access_order_satisfaction_candidate(access: &AccessPath, ordering: &ir::OrderKeys) -> bool {
    let [required] = ordering.as_ref() else {
        return false;
    };
    fn node_candidate(source: &ir::NodeAccessPlan, required: &ir::OrderKey) -> bool {
        match source {
            ir::NodeAccessPlan::RangeIndex { key, .. } => range_satisfies_order(key, required),
            ir::NodeAccessPlan::Intersect(children) if source.is_secondary_set_eligible() => {
                children
                    .iter()
                    .any(|child| node_candidate(child.as_ref(), required))
            }
            _ => false,
        }
    }
    fn edge_candidate(source: &ir::EdgeAccessPlan, required: &ir::OrderKey) -> bool {
        match source {
            ir::EdgeAccessPlan::RangeIndex { key, .. } => range_satisfies_order(key, required),
            ir::EdgeAccessPlan::Intersect(children) if source.is_secondary_set_eligible() => {
                children
                    .iter()
                    .any(|child| edge_candidate(child.as_ref(), required))
            }
            _ => false,
        }
    }
    match access {
        AccessPath::Node(path) => node_candidate(path.source().as_ref(), required),
        AccessPath::Edge(path) => edge_candidate(path.source().as_ref(), required),
    }
}

fn range_satisfies_order(
    key: &catalog::ScopedPropertyDirectionKey,
    required: &ir::OrderKey,
) -> bool {
    key.property == required.property
}

fn range_direction_candidate(
    key: &catalog::ScopedPropertyDirectionKey,
    required: &ir::OrderKey,
) -> bool {
    key.property == required.property && key.direction != range_direction_for_order(required.order)
}

fn range_direction_for_order(
    order: helix_ast::traversal::Order,
) -> helix_ast::index::RangeIndexDirection {
    match order {
        helix_ast::traversal::Order::Asc => helix_ast::index::RangeIndexDirection::Asc,
        helix_ast::traversal::Order::Desc => helix_ast::index::RangeIndexDirection::Desc,
    }
}
