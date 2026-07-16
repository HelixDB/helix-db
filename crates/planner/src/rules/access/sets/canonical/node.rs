//! Node access-set canonicalization.

use super::super::*;
use super::normalization;

pub(super) fn simplify(
    plan: &ir::NodeAccessPlan,
) -> normalization::SourceSetSimplification<ir::NodeAccessPlan> {
    match plan {
        ir::NodeAccessPlan::Union(plans) => simplify_union(plans).into_simplification(),
        ir::NodeAccessPlan::Intersect(plans) => simplify_intersection(plans).into_simplification(),
        ir::NodeAccessPlan::Empty
        | ir::NodeAccessPlan::PointIds { .. }
        | ir::NodeAccessPlan::FromParam { .. }
        | ir::NodeAccessPlan::FromVar { .. }
        | ir::NodeAccessPlan::AllScan
        | ir::NodeAccessPlan::LabelScan { .. }
        | ir::NodeAccessPlan::EqualityIndex { .. }
        | ir::NodeAccessPlan::RangeIndex { .. }
        | ir::NodeAccessPlan::VectorSearch { .. }
        | ir::NodeAccessPlan::TextSearch { .. }
        | ir::NodeAccessPlan::ScanThenFilter { .. } => {
            normalization::SourceSetSimplification::NotASet
        }
    }
}

fn simplify_union(
    plans: &ir::AtLeast<ir::NodeAccessSourcePlan, 2>,
) -> normalization::SourceSetRewrite<ir::NodeAccessPlan> {
    normalization::normalize_union(
        plans,
        is_empty,
        nested_union,
        dedupe_node_sources,
        node_union_from_sources,
    )
}

fn simplify_intersection(
    plans: &ir::AtLeast<ir::NodeAccessSourcePlan, 2>,
) -> normalization::SourceSetRewrite<ir::NodeAccessPlan> {
    normalization::normalize_intersection(
        plans,
        is_empty,
        nested_intersection,
        dedupe_node_sources,
        node_intersection_from_sources,
        || ir::NodeAccessPlan::Empty,
    )
}

fn is_empty(source: &ir::NodeAccessSourcePlan) -> bool {
    matches!(source.as_ref(), ir::NodeAccessPlan::Empty)
}

fn nested_union(
    source: &ir::NodeAccessSourcePlan,
) -> Option<&ir::AtLeast<ir::NodeAccessSourcePlan, 2>> {
    match source.as_ref() {
        ir::NodeAccessPlan::Union(children) => Some(children),
        ir::NodeAccessPlan::Empty
        | ir::NodeAccessPlan::PointIds { .. }
        | ir::NodeAccessPlan::FromParam { .. }
        | ir::NodeAccessPlan::FromVar { .. }
        | ir::NodeAccessPlan::AllScan
        | ir::NodeAccessPlan::LabelScan { .. }
        | ir::NodeAccessPlan::EqualityIndex { .. }
        | ir::NodeAccessPlan::RangeIndex { .. }
        | ir::NodeAccessPlan::VectorSearch { .. }
        | ir::NodeAccessPlan::TextSearch { .. }
        | ir::NodeAccessPlan::ScanThenFilter { .. }
        | ir::NodeAccessPlan::Intersect(_) => None,
    }
}

fn nested_intersection(
    source: &ir::NodeAccessSourcePlan,
) -> Option<&ir::AtLeast<ir::NodeAccessSourcePlan, 2>> {
    match source.as_ref() {
        ir::NodeAccessPlan::Intersect(children) => Some(children),
        ir::NodeAccessPlan::Empty
        | ir::NodeAccessPlan::PointIds { .. }
        | ir::NodeAccessPlan::FromParam { .. }
        | ir::NodeAccessPlan::FromVar { .. }
        | ir::NodeAccessPlan::AllScan
        | ir::NodeAccessPlan::LabelScan { .. }
        | ir::NodeAccessPlan::EqualityIndex { .. }
        | ir::NodeAccessPlan::RangeIndex { .. }
        | ir::NodeAccessPlan::VectorSearch { .. }
        | ir::NodeAccessPlan::TextSearch { .. }
        | ir::NodeAccessPlan::ScanThenFilter { .. }
        | ir::NodeAccessPlan::Union(_) => None,
    }
}
