//! Executable lowering cost accounting.
//!
//! All costs in this module are computed from `StorageCostProfile` so they stay
//! tunable for experiments. The module exposes only the costs needed by
//! selected executable lowering.

use super::contracts;
use super::*;
use crate::{catalog, cost, ir, properties};

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
    match plan {
        ir::NodeAccessPlan::Empty => cost::CostVector::ZERO,
        ir::NodeAccessPlan::PointIds { ids } => point_get_cost(profile, ids.as_ref().len()),
        ir::NodeAccessPlan::EqualityIndex { index, .. } => node_equality_index_cost(profile, index),
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
        | ir::NodeAccessPlan::RangeIndex { .. }
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
    match plan {
        ir::EdgeAccessPlan::Empty => cost::CostVector::ZERO,
        ir::EdgeAccessPlan::PointIds { ids } => point_get_cost(profile, ids.as_ref().len()),
        ir::EdgeAccessPlan::EqualityIndex { .. } => {
            equality_index_cost(profile, profile.equality_index_rows(None))
        }
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
        | ir::EdgeAccessPlan::RangeIndex { .. }
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

fn node_equality_index_cost(
    profile: &cost::StorageCostProfile,
    index: &catalog::NodeEqualityIndexMeta,
) -> cost::CostVector {
    let rows = match index.uniqueness {
        catalog::IndexUniqueness::Unique => profile.unique_equality_rows(None),
        catalog::IndexUniqueness::NonUnique => profile.equality_index_rows(None),
    };
    equality_index_cost(profile, rows)
}

fn equality_index_cost(
    profile: &cost::StorageCostProfile,
    rows: cost::EstimatedRows,
) -> cost::CostVector {
    profile.equality_index_lookup(rows)
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
