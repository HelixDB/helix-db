use serde::{Deserialize, Serialize};

/// Internal planner trace pass.
///
/// # Examples
///
/// ```
/// use helix_planner::trace::TracePass;
///
/// assert_eq!(TracePass::AccessPath.to_string(), "access_path");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TracePass {
    /// Access-path selection.
    AccessPath,
    /// Bound pushdown.
    BoundPushdown,
    /// Cardinality-based ordering.
    CardinalityOrder,
    /// Predicate indexability.
    PredicateIndex,
    /// Order pushdown.
    OrderPushdown,
    /// Reserved operation preservation.
    ReservedNoop,
    /// Native AST handoff into selected Cascades planning.
    NativeHandoff,
    /// Cascades-selected executable root provenance.
    SelectedHandoff,
    /// Sub-traversal planning.
    SubTraversal,
    /// Variable planning.
    Variable,
}

impl std::fmt::Display for TracePass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AccessPath => f.write_str("access_path"),
            Self::BoundPushdown => f.write_str("bound_pushdown"),
            Self::CardinalityOrder => f.write_str("cardinality_order"),
            Self::PredicateIndex => f.write_str("predicate_index"),
            Self::OrderPushdown => f.write_str("order_pushdown"),
            Self::ReservedNoop => f.write_str("reserved_noop"),
            Self::NativeHandoff => f.write_str("native_handoff"),
            Self::SelectedHandoff => f.write_str("selected_handoff"),
            Self::SubTraversal => f.write_str("sub_traversal"),
            Self::Variable => f.write_str("variable"),
        }
    }
}
