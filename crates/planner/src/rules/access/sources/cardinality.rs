use crate::ir;

pub(in crate::rules::access) fn node_source_hard_cardinality_upper_bound(
    source: &ir::NodeAccessSourcePlan,
) -> Option<usize> {
    source.hard_cardinality_upper_bound()
}

pub(in crate::rules::access) fn edge_source_hard_cardinality_upper_bound(
    source: &ir::EdgeAccessSourcePlan,
) -> Option<usize> {
    source.hard_cardinality_upper_bound()
}
