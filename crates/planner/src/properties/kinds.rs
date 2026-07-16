use serde::{Deserialize, Serialize};

/// Element family flowing through a plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ElementKind {
    /// Node stream.
    Node,
    /// Edge stream.
    Edge,
}

/// Whether a plan shape is free to reorder/drop work with no observable side
/// effects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectKind {
    /// Pure relational/graph work.
    Pure,
    /// Observable barrier such as mutation, DDL, or order-sensitive state.
    Barrier,
}

/// Whether an operator can pipeline rows or requires materialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Materialization {
    /// Rows can flow through without a full input barrier.
    Streaming,
    /// The operator materializes before continuing.
    Materialized,
}

/// Physical key locality for LSM/object-storage read planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyLocality {
    /// Locality is unknown; cost conservatively as sparse.
    Unknown,
    /// Keys share useful encoded-key locality.
    Close,
    /// Keys are spread across unrelated key ranges.
    Sparse,
}
