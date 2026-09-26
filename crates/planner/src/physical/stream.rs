use serde::{Deserialize, Serialize};

/// Physical stream operator family used by implementation rules before the
/// executable DAG schedule is selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhysicalStreamOp {
    /// Limit.
    Limit,
    /// Skip.
    Skip,
    /// Range.
    Range,
    /// Distinct.
    Distinct,
    /// Row-preserving node secondary-index membership.
    IndexMembership,
    /// Graph expansion.
    Expand,
    /// Traversal-scoped vector ranking.
    VectorSearch,
    /// Traversal-scoped BM25 ranking.
    TextSearch,
    /// Projection.
    Project,
    /// Aggregation.
    Aggregate,
    /// Variable read or stream-local operation.
    Variable,
    /// Reserved operation.
    Reserved,
}

/// Physical control-flow operator family.
///
/// The executable payload stays in the logical root contract; this enum records
/// only the selected physical control-flow shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhysicalControlOp {
    /// Branch control flow.
    Branch,
    /// Repeat control flow.
    Repeat,
}
