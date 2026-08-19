use crate::{cost, physical, properties};

use super::{
    contract::AccessPhysicalContract,
    delivered::{access_delivered_with, with_ordering},
};

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
    let child_estimates = children
        .iter()
        .map(|child| child.estimated_rows)
        .collect::<Vec<_>>();
    let rows = estimated_rows(&child_estimates);
    let mut delivered = access_delivered_with(element, cardinality(&delivered_children));
    let secondary_costs = children
        .iter()
        .map(AccessPhysicalContract::secondary_id_cost)
        .collect::<Option<Vec<_>>>();
    let Some(secondary_costs) = secondary_costs else {
        let child_costs = children.iter().map(|child| child.cost).collect::<Vec<_>>();
        return AccessPhysicalContract::new(
            access,
            delivered,
            storage.parallel(&child_costs, storage.max_parallel_kv_reads),
            rows,
        );
    };
    if access == physical::PhysicalAccess::SetIntersection {
        let ordering = children
            .iter()
            .find(|child| child.access == physical::PhysicalAccess::RangeIndex)
            .map(|child| child.delivered.ordering.clone())
            .unwrap_or(properties::DeliveredOrdering::Unordered);
        delivered = with_ordering(delivered, ordering);
    }
    let batchable_equality = (access == physical::PhysicalAccess::SetUnion)
        .then(|| {
            let first = children.first()?.batchable_equality_identity()?;
            children
                .iter()
                .all(|child| child.batchable_equality_identity() == Some(first))
                .then_some(())
        })
        .flatten()
        .is_some();
    let id_cost = if batchable_equality {
        storage.bitmap_equality_batch(
            properties::PositiveUsize::at_least_one(children.len()),
            rows,
        )
    } else {
        storage
            .parallel(&secondary_costs, storage.max_parallel_kv_reads)
            .serial(storage.secondary_set_operation(rows))
    };
    AccessPhysicalContract::new_secondary(
        access,
        delivered,
        id_cost,
        storage.secondary_row_materialization(rows),
        rows,
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
