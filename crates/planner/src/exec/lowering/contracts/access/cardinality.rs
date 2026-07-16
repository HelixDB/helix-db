//! Access cardinality inference.

use crate::{catalog, ir};

pub(in crate::exec) fn node_access_hard_upper_bound(plan: &ir::NodeAccessPlan) -> Option<usize> {
    match plan {
        ir::NodeAccessPlan::Empty => Some(0),
        ir::NodeAccessPlan::PointIds { ids } => Some(ids.as_ref().len()),
        ir::NodeAccessPlan::EqualityIndex { index, .. }
            if matches!(index.uniqueness, catalog::IndexUniqueness::Unique) =>
        {
            Some(1)
        }
        ir::NodeAccessPlan::VectorSearch { k, .. } | ir::NodeAccessPlan::TextSearch { k, .. } => {
            search_limit_hard_upper_bound(k)
        }
        ir::NodeAccessPlan::Intersect(plans) => plans
            .iter()
            .filter_map(|plan| node_access_hard_upper_bound(plan))
            .min(),
        ir::NodeAccessPlan::Union(plans) => plans.iter().try_fold(0usize, |sum, plan| {
            node_access_hard_upper_bound(plan).map(|upper| sum.saturating_add(upper))
        }),
        ir::NodeAccessPlan::ScanThenFilter { source, .. } => node_access_hard_upper_bound(source),
        ir::NodeAccessPlan::FromParam { .. }
        | ir::NodeAccessPlan::FromVar { .. }
        | ir::NodeAccessPlan::AllScan
        | ir::NodeAccessPlan::LabelScan { .. }
        | ir::NodeAccessPlan::EqualityIndex { .. }
        | ir::NodeAccessPlan::RangeIndex { .. } => None,
    }
}

pub(in crate::exec) fn edge_access_hard_upper_bound(plan: &ir::EdgeAccessPlan) -> Option<usize> {
    match plan {
        ir::EdgeAccessPlan::Empty => Some(0),
        ir::EdgeAccessPlan::PointIds { ids } => Some(ids.as_ref().len()),
        ir::EdgeAccessPlan::VectorSearch { k, .. } | ir::EdgeAccessPlan::TextSearch { k, .. } => {
            search_limit_hard_upper_bound(k)
        }
        ir::EdgeAccessPlan::Intersect(plans) => plans
            .iter()
            .filter_map(|plan| edge_access_hard_upper_bound(plan))
            .min(),
        ir::EdgeAccessPlan::Union(plans) => plans.iter().try_fold(0usize, |sum, plan| {
            edge_access_hard_upper_bound(plan).map(|upper| sum.saturating_add(upper))
        }),
        ir::EdgeAccessPlan::ScanThenFilter { source, .. } => edge_access_hard_upper_bound(source),
        ir::EdgeAccessPlan::FromParam { .. }
        | ir::EdgeAccessPlan::FromVar { .. }
        | ir::EdgeAccessPlan::AllScan
        | ir::EdgeAccessPlan::LabelScan { .. }
        | ir::EdgeAccessPlan::EqualityIndex { .. }
        | ir::EdgeAccessPlan::RangeIndex { .. } => None,
    }
}

pub(super) fn node_access_exact_cardinality(plan: &ir::NodeAccessPlan) -> Option<usize> {
    match plan {
        ir::NodeAccessPlan::Empty => Some(0),
        ir::NodeAccessPlan::PointIds { ids } => Some(ids.as_ref().len()),
        ir::NodeAccessPlan::EqualityIndex { index, .. }
            if matches!(index.uniqueness, catalog::IndexUniqueness::Unique) =>
        {
            Some(1)
        }
        ir::NodeAccessPlan::FromParam { .. }
        | ir::NodeAccessPlan::FromVar { .. }
        | ir::NodeAccessPlan::AllScan
        | ir::NodeAccessPlan::LabelScan { .. }
        | ir::NodeAccessPlan::EqualityIndex { .. }
        | ir::NodeAccessPlan::RangeIndex { .. }
        | ir::NodeAccessPlan::VectorSearch { .. }
        | ir::NodeAccessPlan::TextSearch { .. }
        | ir::NodeAccessPlan::Intersect(_)
        | ir::NodeAccessPlan::Union(_)
        | ir::NodeAccessPlan::ScanThenFilter { .. } => None,
    }
}

pub(super) fn edge_access_exact_cardinality(plan: &ir::EdgeAccessPlan) -> Option<usize> {
    match plan {
        ir::EdgeAccessPlan::Empty => Some(0),
        ir::EdgeAccessPlan::PointIds { ids } => Some(ids.as_ref().len()),
        ir::EdgeAccessPlan::FromParam { .. }
        | ir::EdgeAccessPlan::FromVar { .. }
        | ir::EdgeAccessPlan::AllScan
        | ir::EdgeAccessPlan::LabelScan { .. }
        | ir::EdgeAccessPlan::EqualityIndex { .. }
        | ir::EdgeAccessPlan::RangeIndex { .. }
        | ir::EdgeAccessPlan::VectorSearch { .. }
        | ir::EdgeAccessPlan::TextSearch { .. }
        | ir::EdgeAccessPlan::Intersect(_)
        | ir::EdgeAccessPlan::Union(_)
        | ir::EdgeAccessPlan::ScanThenFilter { .. } => None,
    }
}

fn search_limit_hard_upper_bound(k: &ir::SearchLimitPlan) -> Option<usize> {
    match k {
        ir::SearchLimitPlan::Literal(k) => Some(k.get()),
        ir::SearchLimitPlan::Expr(_) => None,
    }
}
