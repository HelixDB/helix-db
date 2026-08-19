//! Executable lowering cost accounting.
//!
//! All costs in this module are computed from `StorageCostProfile` so they stay
//! tunable for experiments. The module exposes only the costs needed by
//! selected executable lowering.

use super::contracts;
use super::*;
use crate::{catalog, cost, exec, ir, properties};

pub(super) fn parallel_merge_cost(
    profile: &cost::StorageCostProfile,
    max_concurrency: properties::PositiveUsize,
) -> cost::CostVector {
    profile.parallel_task_overhead(max_concurrency)
}

pub(in crate::exec) fn subplan_cost(plan: &ExecutableSubplan) -> cost::CostVector {
    plan.steps()
        .iter()
        .map(|step| step.cost)
        .fold(cost::CostVector::ZERO, cost::CostVector::serial)
}

pub(in crate::exec) fn foreach_subplan_cost(
    plan: &ExecutableSubplan,
    profile: &cost::StorageCostProfile,
) -> cost::CostVector {
    profile.foreach_wrapper().serial(subplan_cost(plan))
}

pub(in crate::exec) fn node_access_cost(
    plan: &ir::NodeAccessPlan,
    profile: &cost::StorageCostProfile,
) -> cost::CostVector {
    if let Some(set) = super::node_secondary_set(plan) {
        let (id_cost, rows) = node_secondary_set_cost(&set, profile);
        return id_cost.serial(profile.secondary_row_materialization(rows));
    }
    match plan {
        ir::NodeAccessPlan::Empty
        | ir::NodeAccessPlan::EqualityIndex { .. }
        | ir::NodeAccessPlan::RangeIndex { .. } => {
            unreachable!("secondary node leaves have ID-set costs")
        }
        ir::NodeAccessPlan::PointIds { ids } => point_get_cost(profile, ids.as_ref().len()),
        ir::NodeAccessPlan::Intersect(plans) | ir::NodeAccessPlan::Union(plans) => plans
            .iter()
            .map(|plan| node_access_cost(plan, profile))
            .fold(cost::CostVector::ZERO, cost::CostVector::serial),
        ir::NodeAccessPlan::ScanThenFilter { source, .. } => node_access_cost(source, profile)
            .serial(predicate_cost_for_rows(
                profile,
                contracts::node_access_hard_upper_bound(source).map(|rows| rows as u64),
            )),
        ir::NodeAccessPlan::FromParam { .. }
        | ir::NodeAccessPlan::FromVar { .. }
        | ir::NodeAccessPlan::AllScan
        | ir::NodeAccessPlan::LabelScan { .. }
        | ir::NodeAccessPlan::VectorSearch { .. }
        | ir::NodeAccessPlan::TextSearch { .. } => scan_cost_for_rows(
            profile,
            contracts::node_access_hard_upper_bound(plan).map(|rows| rows as u64),
        ),
    }
}

pub(in crate::exec) fn edge_access_cost(
    plan: &ir::EdgeAccessPlan,
    profile: &cost::StorageCostProfile,
) -> cost::CostVector {
    if let Some(set) = super::edge_secondary_set(plan) {
        let (id_cost, rows) = edge_secondary_set_cost(&set, profile);
        return id_cost.serial(profile.secondary_row_materialization(rows));
    }
    match plan {
        ir::EdgeAccessPlan::Empty
        | ir::EdgeAccessPlan::EqualityIndex { .. }
        | ir::EdgeAccessPlan::RangeIndex { .. } => {
            unreachable!("secondary edge leaves have ID-set costs")
        }
        ir::EdgeAccessPlan::PointIds { ids } => point_get_cost(profile, ids.as_ref().len()),
        ir::EdgeAccessPlan::Intersect(plans) | ir::EdgeAccessPlan::Union(plans) => plans
            .iter()
            .map(|plan| edge_access_cost(plan, profile))
            .fold(cost::CostVector::ZERO, cost::CostVector::serial),
        ir::EdgeAccessPlan::ScanThenFilter { source, .. } => edge_access_cost(source, profile)
            .serial(predicate_cost_for_rows(
                profile,
                contracts::edge_access_hard_upper_bound(source).map(|rows| rows as u64),
            )),
        ir::EdgeAccessPlan::FromParam { .. }
        | ir::EdgeAccessPlan::FromVar { .. }
        | ir::EdgeAccessPlan::AllScan
        | ir::EdgeAccessPlan::LabelScan { .. }
        | ir::EdgeAccessPlan::VectorSearch { .. }
        | ir::EdgeAccessPlan::TextSearch { .. } => scan_cost_for_rows(
            profile,
            contracts::edge_access_hard_upper_bound(plan).map(|rows| rows as u64),
        ),
    }
}

fn point_get_cost(profile: &cost::StorageCostProfile, count: usize) -> cost::CostVector {
    properties::PositiveUsize::new(count)
        .map_or(cost::CostVector::ZERO, |count| profile.point_gets(count))
}

fn bitmap_expr_cost(
    values: usize,
    children: impl IntoIterator<Item = (cost::CostVector, cost::EstimatedRows)>,
    intersect: bool,
    profile: &cost::StorageCostProfile,
) -> (cost::CostVector, cost::EstimatedRows) {
    if values != 0 {
        let per_value = profile.equality_index_rows(None);
        let rows = cost::EstimatedRows::rows(per_value.as_rows().saturating_mul(values as u64));
        let cost = if values == 1 {
            profile.bitmap_equality_lookup(rows)
        } else {
            profile.bitmap_equality_batch(properties::PositiveUsize::at_least_one(values), rows)
        };
        return (cost, rows);
    }
    let children = children.into_iter().collect::<Vec<_>>();
    let rows = if intersect {
        children
            .iter()
            .map(|(_, rows)| *rows)
            .min()
            .expect("bitmap set operation has children")
    } else {
        cost::EstimatedRows::rows(
            children
                .iter()
                .map(|(_, rows)| rows.as_rows())
                .fold(0_u64, u64::saturating_add),
        )
    };
    let cost = children
        .into_iter()
        .map(|(cost, _)| cost)
        .fold(cost::CostVector::ZERO, cost::CostVector::serial)
        .serial(profile.secondary_set_operation(rows));
    (cost, rows)
}

fn node_bitmap_cost(
    bitmap: &exec::ExecNodeBitmapExpr,
    profile: &cost::StorageCostProfile,
) -> (cost::CostVector, cost::EstimatedRows) {
    match bitmap {
        exec::ExecNodeBitmapExpr::PointRead { .. } => {
            bitmap_expr_cost(1, core::iter::empty(), false, profile)
        }
        exec::ExecNodeBitmapExpr::BatchedUnionRead { values, .. } => {
            bitmap_expr_cost(values.len(), core::iter::empty(), false, profile)
        }
        exec::ExecNodeBitmapExpr::Union { driver, rest } => bitmap_expr_cost(
            0,
            core::iter::once(node_bitmap_cost(driver, profile))
                .chain(rest.iter().map(|child| node_bitmap_cost(child, profile))),
            false,
            profile,
        ),
        exec::ExecNodeBitmapExpr::Intersect { driver, rest } => bitmap_expr_cost(
            0,
            core::iter::once(node_bitmap_cost(driver, profile))
                .chain(rest.iter().map(|child| node_bitmap_cost(child, profile))),
            true,
            profile,
        ),
    }
}

fn edge_bitmap_cost(
    bitmap: &exec::ExecEdgeBitmapExpr,
    profile: &cost::StorageCostProfile,
) -> (cost::CostVector, cost::EstimatedRows) {
    match bitmap {
        exec::ExecEdgeBitmapExpr::PointRead { .. } => {
            bitmap_expr_cost(1, core::iter::empty(), false, profile)
        }
        exec::ExecEdgeBitmapExpr::BatchedUnionRead { values, .. } => {
            bitmap_expr_cost(values.len(), core::iter::empty(), false, profile)
        }
        exec::ExecEdgeBitmapExpr::Union { driver, rest } => bitmap_expr_cost(
            0,
            core::iter::once(edge_bitmap_cost(driver, profile))
                .chain(rest.iter().map(|child| edge_bitmap_cost(child, profile))),
            false,
            profile,
        ),
        exec::ExecEdgeBitmapExpr::Intersect { driver, rest } => bitmap_expr_cost(
            0,
            core::iter::once(edge_bitmap_cost(driver, profile))
                .chain(rest.iter().map(|child| edge_bitmap_cost(child, profile))),
            true,
            profile,
        ),
    }
}

fn node_secondary_set_cost(
    set: &exec::ExecNodeSecondarySetPlan,
    profile: &cost::StorageCostProfile,
) -> (cost::CostVector, cost::EstimatedRows) {
    match set {
        exec::ExecNodeSecondarySetPlan::Empty => {
            (cost::CostVector::ZERO, cost::EstimatedRows::ZERO)
        }
        exec::ExecNodeSecondarySetPlan::Bitmap(bitmap) => node_bitmap_cost(bitmap, profile),
        exec::ExecNodeSecondarySetPlan::Unique { .. } => {
            let rows = profile.unique_equality_rows(None);
            (profile.unique_equality_lookup(rows), rows)
        }
        exec::ExecNodeSecondarySetPlan::AuthoritativeScan(_) => {
            let rows = profile.default_unknown_scan_rows;
            (profile.null_equality_scan(rows), rows)
        }
        exec::ExecNodeSecondarySetPlan::DynamicEquality { index, .. } => {
            let rows = match index.uniqueness {
                catalog::IndexUniqueness::Unique => profile.unique_equality_rows(None),
                catalog::IndexUniqueness::NonUnique => profile.equality_index_rows(None),
            };
            let cost = match index.uniqueness {
                catalog::IndexUniqueness::Unique => profile.unique_equality_lookup(rows),
                catalog::IndexUniqueness::NonUnique => profile.bitmap_equality_lookup(rows),
            };
            (cost, rows)
        }
        exec::ExecNodeSecondarySetPlan::Range(_) => {
            let rows = profile.default_range_index_rows;
            (profile.secondary_range_lookup(rows), rows)
        }
        exec::ExecNodeSecondarySetPlan::Intersect { driver, rest } => {
            let children = core::iter::once(driver.as_ref())
                .chain(rest.iter())
                .map(|child| node_secondary_set_cost(child, profile))
                .collect::<Vec<_>>();
            let rows = children
                .iter()
                .map(|(_, rows)| *rows)
                .min()
                .expect("secondary intersection has children");
            let cost = children
                .into_iter()
                .map(|(cost, _)| cost)
                .fold(cost::CostVector::ZERO, cost::CostVector::serial)
                .serial(profile.secondary_set_operation(rows));
            (cost, rows)
        }
        exec::ExecNodeSecondarySetPlan::Union { driver, rest } => {
            let children = core::iter::once(driver.as_ref())
                .chain(rest.iter())
                .map(|child| node_secondary_set_cost(child, profile))
                .collect::<Vec<_>>();
            let rows = cost::EstimatedRows::rows(
                children
                    .iter()
                    .map(|(_, rows)| rows.as_rows())
                    .fold(0_u64, u64::saturating_add),
            );
            let cost = children
                .into_iter()
                .map(|(cost, _)| cost)
                .fold(cost::CostVector::ZERO, cost::CostVector::serial)
                .serial(profile.secondary_set_operation(rows));
            (cost, rows)
        }
        exec::ExecNodeSecondarySetPlan::OrderedIntersect { driver: _, filters } => {
            let driver_rows = profile.default_range_index_rows;
            let mut rows = driver_rows;
            let mut cost = profile.secondary_range_lookup(driver_rows);
            for filter in filters {
                let (filter_cost, filter_rows) = node_secondary_set_cost(filter, profile);
                cost = cost.serial(filter_cost);
                rows = rows.min(filter_rows);
            }
            (cost.serial(profile.secondary_set_operation(rows)), rows)
        }
    }
}

fn edge_secondary_set_cost(
    set: &exec::ExecEdgeSecondarySetPlan,
    profile: &cost::StorageCostProfile,
) -> (cost::CostVector, cost::EstimatedRows) {
    match set {
        exec::ExecEdgeSecondarySetPlan::Empty => {
            (cost::CostVector::ZERO, cost::EstimatedRows::ZERO)
        }
        exec::ExecEdgeSecondarySetPlan::Bitmap(bitmap) => edge_bitmap_cost(bitmap, profile),
        exec::ExecEdgeSecondarySetPlan::AuthoritativeScan(_) => {
            let rows = profile.default_unknown_scan_rows;
            (profile.null_equality_scan(rows), rows)
        }
        exec::ExecEdgeSecondarySetPlan::DynamicEquality { .. } => {
            let rows = profile.equality_index_rows(None);
            (profile.bitmap_equality_lookup(rows), rows)
        }
        exec::ExecEdgeSecondarySetPlan::Range(_) => {
            let rows = profile.default_range_index_rows;
            (profile.secondary_range_lookup(rows), rows)
        }
        exec::ExecEdgeSecondarySetPlan::Intersect { driver, rest } => {
            let children = core::iter::once(driver.as_ref())
                .chain(rest.iter())
                .map(|child| edge_secondary_set_cost(child, profile))
                .collect::<Vec<_>>();
            let rows = children
                .iter()
                .map(|(_, rows)| *rows)
                .min()
                .expect("secondary intersection has children");
            let cost = children
                .into_iter()
                .map(|(cost, _)| cost)
                .fold(cost::CostVector::ZERO, cost::CostVector::serial)
                .serial(profile.secondary_set_operation(rows));
            (cost, rows)
        }
        exec::ExecEdgeSecondarySetPlan::Union { driver, rest } => {
            let children = core::iter::once(driver.as_ref())
                .chain(rest.iter())
                .map(|child| edge_secondary_set_cost(child, profile))
                .collect::<Vec<_>>();
            let rows = cost::EstimatedRows::rows(
                children
                    .iter()
                    .map(|(_, rows)| rows.as_rows())
                    .fold(0_u64, u64::saturating_add),
            );
            let cost = children
                .into_iter()
                .map(|(cost, _)| cost)
                .fold(cost::CostVector::ZERO, cost::CostVector::serial)
                .serial(profile.secondary_set_operation(rows));
            (cost, rows)
        }
        exec::ExecEdgeSecondarySetPlan::OrderedIntersect { driver: _, filters } => {
            let driver_rows = profile.default_range_index_rows;
            let mut rows = driver_rows;
            let mut cost = profile.secondary_range_lookup(driver_rows);
            for filter in filters {
                let (filter_cost, filter_rows) = edge_secondary_set_cost(filter, profile);
                cost = cost.serial(filter_cost);
                rows = rows.min(filter_rows);
            }
            (cost.serial(profile.secondary_set_operation(rows)), rows)
        }
    }
}

fn scan_cost_for_rows(profile: &cost::StorageCostProfile, rows: Option<u64>) -> cost::CostVector {
    profile.range_scan(rows.map_or(profile.default_unknown_scan_rows, cost::EstimatedRows::rows))
}

pub(in crate::exec) fn predicate_cost_for_rows(
    profile: &cost::StorageCostProfile,
    rows: Option<u64>,
) -> cost::CostVector {
    let rows = rows.unwrap_or_else(|| profile.default_unknown_scan_rows.as_rows());
    profile.predicate_eval(cost::EstimatedRows::rows(rows))
}
