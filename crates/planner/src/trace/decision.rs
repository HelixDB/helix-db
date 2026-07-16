use serde::{Deserialize, Serialize};

/// Internal planner trace decision.
///
/// # Examples
///
/// ```
/// use helix_planner::trace::TraceDecision;
///
/// assert_eq!(TraceDecision::NodeAllScan.to_string(), "node_all_scan");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceDecision {
    /// Context source injection.
    Context,
    /// Edge all-scan access.
    EdgeAllScan,
    /// Edge empty ID access.
    EdgeEmptyIds,
    /// Edge impossible label scope.
    EdgeEmptyLabelScope,
    /// Edge impossible scalar predicate.
    EdgeEmptyPredicate,
    /// Edge equality index access.
    EdgeEqualityIndex,
    /// Edge full-scan access.
    EdgeFullScan,
    /// Edge indexed intersection.
    EdgeIntersect,
    /// Edge point lookup.
    EdgePointGet,
    /// Edge range index access.
    EdgeRangeIndex,
    /// Edge OR residual scan.
    EdgeScanOr,
    /// Edge indexed union.
    EdgeUnion,
    /// Explicit sort.
    ExplicitSort,
    /// Limit operation.
    Limit,
    /// Node all-scan access.
    NodeAllScan,
    /// Node empty ID access.
    NodeEmptyIds,
    /// Node impossible label scope.
    NodeEmptyLabelScope,
    /// Node impossible scalar predicate.
    NodeEmptyPredicate,
    /// Node equality index access.
    NodeEqualityIndex,
    /// Node full-scan access.
    NodeFullScan,
    /// Node indexed intersection.
    NodeIntersect,
    /// Node point lookup.
    NodePointGet,
    /// Node range index access.
    NodeRangeIndex,
    /// Node OR residual scan.
    NodeScanOr,
    /// Node indexed union.
    NodeUnion,
    /// Range index already provides requested order.
    RangeIndexOrder,
    /// Residual filter.
    ResidualFilter,
    /// Reserved operation.
    ReservedOperation,
    /// Native AST root handed to selected Cascades planning.
    NativeQueryRoot,
    /// Native batch `ForEach` wrapper handed to selected executable planning.
    NativeForEach,
    /// Selected executable run root.
    SelectedRunRoot,
    /// Optimizer rule that produced a selected executable root.
    SelectedOptimizerRule,
    /// Memo expression that produced a selected executable root.
    SelectedMemoExpression,
    /// Child memo group referenced by a selected executable root.
    SelectedMemoChild,
    /// Selected executable `ForEach` wrapper.
    SelectedForEach,
    /// Text search index.
    TextIndex,
    /// Variable filter.
    VariableFilter,
    /// Variable operation.
    VariableOp,
    /// Vector search index.
    VectorIndex,
}

impl std::fmt::Display for TraceDecision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Context => f.write_str("context"),
            Self::EdgeAllScan => f.write_str("edge_all_scan"),
            Self::EdgeEmptyIds => f.write_str("edge_empty_ids"),
            Self::EdgeEmptyLabelScope => f.write_str("edge_empty_label_scope"),
            Self::EdgeEmptyPredicate => f.write_str("edge_empty_predicate"),
            Self::EdgeEqualityIndex => f.write_str("edge_equality_index"),
            Self::EdgeFullScan => f.write_str("edge_full_scan"),
            Self::EdgeIntersect => f.write_str("edge_intersect"),
            Self::EdgePointGet => f.write_str("edge_point_get"),
            Self::EdgeRangeIndex => f.write_str("edge_range_index"),
            Self::EdgeScanOr => f.write_str("edge_scan_or"),
            Self::EdgeUnion => f.write_str("edge_union"),
            Self::ExplicitSort => f.write_str("explicit_sort"),
            Self::Limit => f.write_str("limit"),
            Self::NodeAllScan => f.write_str("node_all_scan"),
            Self::NodeEmptyIds => f.write_str("node_empty_ids"),
            Self::NodeEmptyLabelScope => f.write_str("node_empty_label_scope"),
            Self::NodeEmptyPredicate => f.write_str("node_empty_predicate"),
            Self::NodeEqualityIndex => f.write_str("node_equality_index"),
            Self::NodeFullScan => f.write_str("node_full_scan"),
            Self::NodeIntersect => f.write_str("node_intersect"),
            Self::NodePointGet => f.write_str("node_point_get"),
            Self::NodeRangeIndex => f.write_str("node_range_index"),
            Self::NodeScanOr => f.write_str("node_scan_or"),
            Self::NodeUnion => f.write_str("node_union"),
            Self::RangeIndexOrder => f.write_str("range_index_order"),
            Self::ResidualFilter => f.write_str("residual_filter"),
            Self::ReservedOperation => f.write_str("reserved_operation"),
            Self::NativeQueryRoot => f.write_str("native_query_root"),
            Self::NativeForEach => f.write_str("native_foreach"),
            Self::SelectedRunRoot => f.write_str("selected_run_root"),
            Self::SelectedOptimizerRule => f.write_str("selected_optimizer_rule"),
            Self::SelectedMemoExpression => f.write_str("selected_memo_expression"),
            Self::SelectedMemoChild => f.write_str("selected_memo_child"),
            Self::SelectedForEach => f.write_str("selected_foreach"),
            Self::TextIndex => f.write_str("text_index"),
            Self::VariableFilter => f.write_str("variable_filter"),
            Self::VariableOp => f.write_str("variable_op"),
            Self::VectorIndex => f.write_str("vector_index"),
        }
    }
}
