use super::super::sources::{
    edge_source_hard_cardinality_upper_bound, node_source_hard_cardinality_upper_bound,
};
use crate::{catalog, ir, logical};

pub(in crate::rules::access) fn access_distinct_is_noop(
    distinct: &logical::AccessDistinct,
) -> bool {
    match distinct.access() {
        logical::AccessPath::Node(path) => node_access_distinct_is_noop(path.source()),
        logical::AccessPath::Edge(path) => edge_access_distinct_is_noop(path.source()),
    }
}

fn node_access_distinct_is_noop(source: &ir::NodeAccessSourcePlan) -> bool {
    node_source_hard_cardinality_upper_bound(source).is_some_and(|upper| upper <= 1)
        || node_source_is_duplicate_free(source.as_ref())
}

/// Whether `source` yields each node at most once even when it may yield many.
///
/// A unique lookup resolves its literals, parameter or authoritative null scan
/// to a node ID set, and access intersections combine ID sets, so one
/// duplicate-free input bounds every node to one row. Other sources keep their
/// distinct unless their hard bound is at most one row.
fn node_source_is_duplicate_free(source: &ir::NodeAccessPlan) -> bool {
    match source {
        ir::NodeAccessPlan::Empty | ir::NodeAccessPlan::PointIds { .. } => true,
        ir::NodeAccessPlan::EqualityIndex { index, .. } => {
            matches!(index.uniqueness, catalog::IndexUniqueness::Unique)
        }
        ir::NodeAccessPlan::Intersect(plans) => plans
            .iter()
            .any(|plan| node_source_is_duplicate_free(plan.as_ref())),
        ir::NodeAccessPlan::FromParam { .. }
        | ir::NodeAccessPlan::FromVar { .. }
        | ir::NodeAccessPlan::AllScan
        | ir::NodeAccessPlan::LabelScan { .. }
        | ir::NodeAccessPlan::RangeIndex { .. }
        | ir::NodeAccessPlan::VectorSearch { .. }
        | ir::NodeAccessPlan::TextSearch { .. }
        | ir::NodeAccessPlan::Union(_)
        | ir::NodeAccessPlan::ScanThenFilter { .. }
        | ir::NodeAccessPlan::BranchResidualUnion(_) => false,
    }
}

fn edge_access_distinct_is_noop(source: &ir::EdgeAccessSourcePlan) -> bool {
    edge_source_hard_cardinality_upper_bound(source).is_some_and(|upper| upper <= 1)
        || matches!(source.as_ref(), ir::EdgeAccessPlan::PointIds { .. })
}
