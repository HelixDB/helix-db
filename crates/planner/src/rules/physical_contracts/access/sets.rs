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
                .then_some(first.2)
        })
        .flatten();
    let id_cost = if let Some(uniqueness) = batchable_equality {
        let values = properties::PositiveUsize::at_least_one(children.len());
        match uniqueness {
            crate::catalog::IndexUniqueness::Unique => storage.unique_equality_batch(values, rows),
            crate::catalog::IndexUniqueness::NonUnique => {
                storage.bitmap_equality_batch(values, rows)
            }
        }
    } else {
        let driver = (access == physical::PhysicalAccess::SetIntersection)
            .then(|| {
                children
                    .iter()
                    .position(|child| child.range_iteration().is_some())
            })
            .flatten();
        // Unordered set algebra consumes membership inputs, even when the
        // intersection is estimated empty. Output cardinality only controls
        // final row construction. Ordered drivers retain their visited budget.
        let scanned = driver.map_or_else(
            || set_union_estimated_rows(&child_estimates),
            |index| children[index].estimated_rows,
        );
        // The executor reads every non-driver child concurrently, then scans
        // the ordered driver against their combined set. Only the driver is
        // filtered before verification; other ranges are complete
        // membership inputs.
        let filters = secondary_costs
            .into_iter()
            .enumerate()
            .filter(|(index, _)| Some(*index) != driver)
            .map(|(_, cost)| cost)
            .collect::<Vec<_>>();
        let driver = driver.map_or(cost::CostVector::ZERO, |index| {
            let child = &children[index];
            storage
                .ordered_range_scan(
                    child.estimated_rows,
                    child.range_iteration().expect("selected range driver"),
                )
                .serial(storage.authoritative_verification(rows))
        });
        storage
            .parallel_reads(&filters)
            .serial(driver)
            .serial(storage.secondary_set_operation(scanned))
    };
    AccessPhysicalContract::new_secondary(
        access,
        delivered,
        id_cost,
        storage.secondary_row_materialization(rows),
        rows,
    )
}

/// Physical contract of a union of index-only branch sets, each filtered by
/// its own residual.
///
/// Branch sets are read concurrently. A residual branch reads the records of
/// its own set's rows only, and every accepted row is materialized once. No
/// row outside the branch sets is ever read, so the contract never prices a
/// scan.
pub(super) fn branch_residual_union_contract(
    element: properties::ElementKind,
    branches: Vec<(AccessPhysicalContract, Option<&crate::ir::PredicatePlan>)>,
    storage: &cost::StorageCostProfile,
) -> AccessPhysicalContract {
    let rows = set_union_estimated_rows(
        &branches
            .iter()
            .map(|(branch, _)| branch.estimated_rows)
            .collect::<Vec<_>>(),
    );
    let delivered = access_delivered_with(
        element,
        set_union_cardinality(
            &branches
                .iter()
                .map(|(branch, _)| branch.delivered.clone())
                .collect::<Vec<_>>(),
        ),
    );
    let reads = storage.parallel_reads(
        &branches
            .iter()
            .map(|(branch, _)| branch.secondary_id_cost().unwrap_or(branch.cost))
            .collect::<Vec<_>>(),
    );
    let residuals = branches
        .iter()
        .filter_map(|(branch, residual)| {
            residual
                .map(|residual| storage.residual_filter(residual.as_ref(), branch.estimated_rows))
        })
        .fold(cost::CostVector::ZERO, cost::CostVector::serial);
    AccessPhysicalContract::new(
        physical::PhysicalAccess::BranchResidualUnion,
        delivered,
        reads
            .serial(residuals)
            .serial(storage.secondary_row_materialization(rows)),
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
