use super::super::sources::{
    edge_source_hard_cardinality_upper_bound, node_source_hard_cardinality_upper_bound,
};
use crate::{ir, logical};

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
        || matches!(source.as_ref(), ir::NodeAccessPlan::PointIds { .. })
}

fn edge_access_distinct_is_noop(source: &ir::EdgeAccessSourcePlan) -> bool {
    edge_source_hard_cardinality_upper_bound(source).is_some_and(|upper| upper <= 1)
        || matches!(source.as_ref(), ir::EdgeAccessPlan::PointIds { .. })
}
