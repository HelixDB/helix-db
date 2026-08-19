use super::super::sources::{
    edge_source_hard_cardinality_upper_bound, node_source_hard_cardinality_upper_bound,
};
use super::direction::order_for_range_direction;
use crate::{catalog, ir, logical};

/// Result of proving that an access path delivers a requested ordering.
///
/// A successful proof owns the access path whose executable direct range
/// driver provides that ordering. This prevents order elision from becoming
/// detached from the driver selected during secondary-set lowering.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::rules::access) enum AccessOrderSatisfaction {
    NotSatisfied,
    Satisfied(logical::AccessPath),
}

impl AccessOrderSatisfaction {
    pub(in crate::rules::access) fn is_satisfied(&self) -> bool {
        matches!(self, Self::Satisfied(_))
    }
}

pub(in crate::rules::access) fn access_order_satisfaction(
    order: &logical::AccessOrder,
) -> AccessOrderSatisfaction {
    match order.access() {
        logical::AccessPath::Node(path) => node_access_order_satisfaction(path, order.ordering()),
        logical::AccessPath::Edge(path) => edge_access_order_satisfaction(path, order.ordering()),
    }
}

fn node_access_order_satisfaction(
    path: &logical::NodeAccessPath,
    ordering: &ir::OrderKeys,
) -> AccessOrderSatisfaction {
    if node_source_hard_cardinality_upper_bound(path.source()).is_some_and(|upper| upper <= 1) {
        return AccessOrderSatisfaction::Satisfied(logical::AccessPath::Node(path.clone()));
    }
    let source = path.source().as_ref();
    match source {
        ir::NodeAccessPlan::RangeIndex { key, .. }
            if range_index_satisfies_order(key, ordering) =>
        {
            AccessOrderSatisfaction::Satisfied(logical::AccessPath::Node(path.clone()))
        }
        ir::NodeAccessPlan::Intersect(children) if source.is_secondary_set_eligible() => {
            let Some(children) = promote_node_range_driver(children, ordering) else {
                return AccessOrderSatisfaction::NotSatisfied;
            };
            AccessOrderSatisfaction::Satisfied(logical::AccessPath::Node(
                logical::NodeAccessPath::new(ir::NodeAccessSourcePlan::from_unfiltered(
                    ir::NodeAccessPlan::Intersect(children),
                )),
            ))
        }
        _ => AccessOrderSatisfaction::NotSatisfied,
    }
}

fn edge_access_order_satisfaction(
    path: &logical::EdgeAccessPath,
    ordering: &ir::OrderKeys,
) -> AccessOrderSatisfaction {
    if edge_source_hard_cardinality_upper_bound(path.source()).is_some_and(|upper| upper <= 1) {
        return AccessOrderSatisfaction::Satisfied(logical::AccessPath::Edge(path.clone()));
    }
    let source = path.source().as_ref();
    match source {
        ir::EdgeAccessPlan::RangeIndex { key, .. }
            if range_index_satisfies_order(key, ordering) =>
        {
            AccessOrderSatisfaction::Satisfied(logical::AccessPath::Edge(path.clone()))
        }
        ir::EdgeAccessPlan::Intersect(children) if source.is_secondary_set_eligible() => {
            let Some(children) = promote_edge_range_driver(children, ordering) else {
                return AccessOrderSatisfaction::NotSatisfied;
            };
            AccessOrderSatisfaction::Satisfied(logical::AccessPath::Edge(
                logical::EdgeAccessPath::new(ir::EdgeAccessSourcePlan::from_unfiltered(
                    ir::EdgeAccessPlan::Intersect(children),
                )),
            ))
        }
        _ => AccessOrderSatisfaction::NotSatisfied,
    }
}

fn promote_node_range_driver(
    children: &ir::AtLeast<ir::NodeAccessSourcePlan, 2>,
    ordering: &ir::OrderKeys,
) -> Option<ir::AtLeast<ir::NodeAccessSourcePlan, 2>> {
    let first_range = children
        .iter()
        .position(|child| matches!(child.as_ref(), ir::NodeAccessPlan::RangeIndex { .. }))?;
    let selected = children.iter().position(|child| {
        matches!(
            child.as_ref(),
            ir::NodeAccessPlan::RangeIndex { key, .. }
                if range_index_satisfies_order(key, ordering)
        )
    })?;
    let mut promoted = children.clone().into_iter().collect::<Vec<_>>();
    if selected != first_range {
        let driver = promoted.remove(selected);
        promoted.insert(first_range, driver);
    }
    Some(
        ir::AtLeast::try_from_vec(promoted)
            .expect("driver promotion preserves node intersection cardinality"),
    )
}

fn promote_edge_range_driver(
    children: &ir::AtLeast<ir::EdgeAccessSourcePlan, 2>,
    ordering: &ir::OrderKeys,
) -> Option<ir::AtLeast<ir::EdgeAccessSourcePlan, 2>> {
    let first_range = children
        .iter()
        .position(|child| matches!(child.as_ref(), ir::EdgeAccessPlan::RangeIndex { .. }))?;
    let selected = children.iter().position(|child| {
        matches!(
            child.as_ref(),
            ir::EdgeAccessPlan::RangeIndex { key, .. }
                if range_index_satisfies_order(key, ordering)
        )
    })?;
    let mut promoted = children.clone().into_iter().collect::<Vec<_>>();
    if selected != first_range {
        let driver = promoted.remove(selected);
        promoted.insert(first_range, driver);
    }
    Some(
        ir::AtLeast::try_from_vec(promoted)
            .expect("driver promotion preserves edge intersection cardinality"),
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
