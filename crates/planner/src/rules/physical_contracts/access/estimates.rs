use crate::{cost, ir, properties};

pub(super) fn stats_rows(
    cardinality: Option<u64>,
    storage: &cost::StorageCostProfile,
) -> cost::EstimatedRows {
    cardinality.map_or(storage.default_unknown_scan_rows, cost::EstimatedRows::rows)
}

pub(super) fn unique_equality_rows(
    cardinality: Option<u64>,
    storage: &cost::StorageCostProfile,
) -> cost::EstimatedRows {
    storage.unique_equality_rows(cardinality)
}

pub(super) fn equality_index_rows(
    cardinality: Option<u64>,
    storage: &cost::StorageCostProfile,
) -> cost::EstimatedRows {
    storage.equality_index_rows(cardinality)
}

pub(super) fn search_cardinality(k: &ir::SearchLimitPlan) -> properties::CardinalityBounds {
    properties::CardinalityBounds::zero_to(search_upper(k))
}

pub(super) fn search_estimated_rows(
    k: &ir::SearchLimitPlan,
    storage: &cost::StorageCostProfile,
) -> cost::EstimatedRows {
    search_upper(k)
        .map(|upper| cost::EstimatedRows::rows(upper as u64))
        .unwrap_or(storage.default_unknown_scan_rows)
}

fn search_upper(k: &ir::SearchLimitPlan) -> Option<usize> {
    match k {
        ir::SearchLimitPlan::Literal(limit) => Some(limit.get()),
        ir::SearchLimitPlan::Expr(_) => None,
    }
}
