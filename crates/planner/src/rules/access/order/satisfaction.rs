use super::super::sources::{
    edge_source_hard_cardinality_upper_bound, node_source_hard_cardinality_upper_bound,
};
use super::direction::order_for_range_direction;
use crate::{catalog, ir, logical};

pub(in crate::rules::access) fn access_satisfies_order(order: &logical::AccessOrder) -> bool {
    match order.access() {
        logical::AccessPath::Node(path) => {
            node_access_satisfies_order(path.source(), order.ordering())
        }
        logical::AccessPath::Edge(path) => {
            edge_access_satisfies_order(path.source(), order.ordering())
        }
    }
}

fn node_access_satisfies_order(
    source: &ir::NodeAccessSourcePlan,
    ordering: &ir::OrderKeys,
) -> bool {
    node_source_hard_cardinality_upper_bound(source).is_some_and(|upper| upper <= 1)
        || matches!(
            source.as_ref(),
            ir::NodeAccessPlan::RangeIndex { key, .. }
                if range_index_satisfies_order(key, ordering)
        )
}

fn edge_access_satisfies_order(
    source: &ir::EdgeAccessSourcePlan,
    ordering: &ir::OrderKeys,
) -> bool {
    edge_source_hard_cardinality_upper_bound(source).is_some_and(|upper| upper <= 1)
        || matches!(
            source.as_ref(),
            ir::EdgeAccessPlan::RangeIndex { key, .. }
                if range_index_satisfies_order(key, ordering)
        )
}

fn range_index_satisfies_order(
    key: &catalog::ScopedPropertyDirectionKey,
    ordering: &ir::OrderKeys,
) -> bool {
    matches!(
        ordering.as_ref(),
        [required]
            if required.property == key.property
                && required.order == order_for_range_direction(key.direction)
    )
}
