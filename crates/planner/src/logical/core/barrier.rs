//! Barrier logical operation contracts.

use serde::{Deserialize, Serialize};

/// Logical operation with observable side effects or execution-order
/// constraints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BarrierLogicalOp {
    /// Mutation barrier.
    Mutation,
    /// Index DDL barrier.
    IndexDdl,
    /// Observable variable/state write.
    StateWrite,
    /// Repeat/branch semantic barrier.
    TraversalControl,
}
