//! Edge access-set canonicalization.

use super::super::*;
use super::normalization;

pub(super) fn simplify(
    plan: &ir::EdgeAccessPlan,
) -> normalization::SourceSetSimplification<ir::EdgeAccessPlan> {
    match plan {
        ir::EdgeAccessPlan::Union(plans) => simplify_union(plans).into_simplification(),
        ir::EdgeAccessPlan::Intersect(plans) => simplify_intersection(plans).into_simplification(),
        ir::EdgeAccessPlan::Empty
        | ir::EdgeAccessPlan::PointIds { .. }
        | ir::EdgeAccessPlan::FromParam { .. }
        | ir::EdgeAccessPlan::FromVar { .. }
        | ir::EdgeAccessPlan::AllScan
        | ir::EdgeAccessPlan::LabelScan { .. }
        | ir::EdgeAccessPlan::EqualityIndex { .. }
        | ir::EdgeAccessPlan::RangeIndex { .. }
        | ir::EdgeAccessPlan::VectorSearch { .. }
        | ir::EdgeAccessPlan::TextSearch { .. }
        | ir::EdgeAccessPlan::ScanThenFilter { .. } => {
            normalization::SourceSetSimplification::NotASet
        }
    }
}

fn simplify_union(
    plans: &ir::AtLeast<ir::EdgeAccessSourcePlan, 2>,
) -> normalization::SourceSetRewrite<ir::EdgeAccessPlan> {
    normalization::normalize_union(
        plans,
        is_empty,
        nested_union,
        dedupe_edge_sources,
        edge_union_from_sources,
    )
}

fn simplify_intersection(
    plans: &ir::AtLeast<ir::EdgeAccessSourcePlan, 2>,
) -> normalization::SourceSetRewrite<ir::EdgeAccessPlan> {
    normalization::normalize_intersection(
        plans,
        is_empty,
        nested_intersection,
        dedupe_edge_sources,
        edge_intersection_from_sources,
        || ir::EdgeAccessPlan::Empty,
    )
}

fn is_empty(source: &ir::EdgeAccessSourcePlan) -> bool {
    matches!(source.as_ref(), ir::EdgeAccessPlan::Empty)
}

fn nested_union(
    source: &ir::EdgeAccessSourcePlan,
) -> Option<&ir::AtLeast<ir::EdgeAccessSourcePlan, 2>> {
    match source.as_ref() {
        ir::EdgeAccessPlan::Union(children) => Some(children),
        ir::EdgeAccessPlan::Empty
        | ir::EdgeAccessPlan::PointIds { .. }
        | ir::EdgeAccessPlan::FromParam { .. }
        | ir::EdgeAccessPlan::FromVar { .. }
        | ir::EdgeAccessPlan::AllScan
        | ir::EdgeAccessPlan::LabelScan { .. }
        | ir::EdgeAccessPlan::EqualityIndex { .. }
        | ir::EdgeAccessPlan::RangeIndex { .. }
        | ir::EdgeAccessPlan::VectorSearch { .. }
        | ir::EdgeAccessPlan::TextSearch { .. }
        | ir::EdgeAccessPlan::ScanThenFilter { .. }
        | ir::EdgeAccessPlan::Intersect(_) => None,
    }
}

fn nested_intersection(
    source: &ir::EdgeAccessSourcePlan,
) -> Option<&ir::AtLeast<ir::EdgeAccessSourcePlan, 2>> {
    match source.as_ref() {
        ir::EdgeAccessPlan::Intersect(children) => Some(children),
        ir::EdgeAccessPlan::Empty
        | ir::EdgeAccessPlan::PointIds { .. }
        | ir::EdgeAccessPlan::FromParam { .. }
        | ir::EdgeAccessPlan::FromVar { .. }
        | ir::EdgeAccessPlan::AllScan
        | ir::EdgeAccessPlan::LabelScan { .. }
        | ir::EdgeAccessPlan::EqualityIndex { .. }
        | ir::EdgeAccessPlan::RangeIndex { .. }
        | ir::EdgeAccessPlan::VectorSearch { .. }
        | ir::EdgeAccessPlan::TextSearch { .. }
        | ir::EdgeAccessPlan::ScanThenFilter { .. }
        | ir::EdgeAccessPlan::Union(_) => None,
    }
}
