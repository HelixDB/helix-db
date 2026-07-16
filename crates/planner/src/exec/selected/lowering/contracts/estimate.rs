use super::*;

pub(in crate::exec::selected::lowering) fn estimated_rows_bounded_by(
    rows: cost::EstimatedRows,
    upper: Option<u64>,
) -> cost::EstimatedRows {
    upper
        .map(|upper| cost::EstimatedRows::rows(rows.as_rows().min(upper)))
        .unwrap_or(rows)
}

pub(in crate::exec::selected::lowering) fn selected_rows_for_delivered(
    delivered: &properties::DeliveredProperties,
    profile: &cost::StorageCostProfile,
) -> cost::EstimatedRows {
    delivered
        .cardinality
        .upper()
        .map_or(profile.default_unknown_scan_rows, |rows| {
            cost::EstimatedRows::rows(rows as u64)
        })
}

pub(in crate::exec::selected::lowering) fn selected_access_path_hard_upper_bound(
    access: &logical::AccessPath,
) -> Option<usize> {
    match access {
        logical::AccessPath::Node(path) => node_access_hard_upper_bound(path.source().as_ref()),
        logical::AccessPath::Edge(path) => edge_access_hard_upper_bound(path.source().as_ref()),
    }
}

pub(in crate::exec::selected::lowering) fn selected_access_path_estimated_rows(
    access: &logical::AccessPath,
    profile: &cost::StorageCostProfile,
) -> cost::EstimatedRows {
    selected_access_path_hard_upper_bound(access)
        .map_or(profile.default_unknown_scan_rows, |rows| {
            cost::EstimatedRows::rows(rows as u64)
        })
}
