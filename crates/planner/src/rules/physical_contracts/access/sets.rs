use crate::{cost, physical, properties};

use super::{contract::AccessPhysicalContract, delivered::access_delivered_with};

pub(super) fn access_set_contract(
    element: properties::ElementKind,
    access: physical::PhysicalAccess,
    children: Vec<AccessPhysicalContract>,
    cardinality: fn(&[properties::DeliveredProperties]) -> properties::CardinalityBounds,
    estimated_rows: fn(&[cost::EstimatedRows]) -> cost::EstimatedRows,
    storage: &cost::StorageCostProfile,
) -> AccessPhysicalContract {
    let delivered_children = children
        .iter()
        .map(|child| child.delivered.clone())
        .collect::<Vec<_>>();
    let child_costs = children.iter().map(|child| child.cost).collect::<Vec<_>>();
    let child_estimates = children
        .iter()
        .map(|child| child.estimated_rows)
        .collect::<Vec<_>>();
    AccessPhysicalContract::new(
        access,
        access_delivered_with(element, cardinality(&delivered_children)),
        storage.parallel(&child_costs, storage.max_parallel_kv_reads),
        estimated_rows(&child_estimates),
    )
}

pub(super) fn set_intersection_cardinality(
    children: &[properties::DeliveredProperties],
) -> properties::CardinalityBounds {
    let upper = children
        .iter()
        .filter_map(|child| child.cardinality.upper())
        .min();
    properties::CardinalityBounds::zero_to(upper)
}

pub(super) fn set_union_cardinality(
    children: &[properties::DeliveredProperties],
) -> properties::CardinalityBounds {
    let upper = children.iter().try_fold(0usize, |sum, child| {
        child
            .cardinality
            .upper()
            .and_then(|upper| sum.checked_add(upper))
    });
    properties::CardinalityBounds::zero_to(upper)
}

pub(super) fn set_intersection_estimated_rows(
    children: &[cost::EstimatedRows],
) -> cost::EstimatedRows {
    children
        .iter()
        .map(|rows| rows.as_rows())
        .min()
        .map_or(cost::EstimatedRows::ZERO, cost::EstimatedRows::rows)
}

pub(super) fn set_union_estimated_rows(children: &[cost::EstimatedRows]) -> cost::EstimatedRows {
    cost::EstimatedRows::rows(
        children
            .iter()
            .map(|rows| rows.as_rows())
            .fold(0_u64, u64::saturating_add),
    )
}
