use serde::{Deserialize, Serialize};

use crate::{exec, properties};

/// Physical access family independent of node/edge duplication.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhysicalAccess {
    /// KV read.
    Kv(exec::KvReadPlan),
    /// Empty access path.
    Empty,
    /// Runtime parameter or variable input.
    RuntimeInput,
    /// Fully selected node access program.
    NodeExact(Box<exec::ExecNodeAccessPlan>),
    /// Fully selected edge access program.
    EdgeExact(Box<exec::ExecEdgeAccessPlan>),
    /// Point reads that are costed as split batches.
    PointReads {
        /// Locality used for batch costing.
        locality: properties::KeyLocality,
    },
    /// Label scan.
    LabelScan,
    /// Range-index scan.
    RangeIndex,
    /// Vector search.
    VectorSearch,
    /// Text search.
    TextSearch,
    /// Set intersection of access paths.
    SetIntersection,
    /// Set union of access paths.
    SetUnion,
    /// Graph expansion.
    Expand,
}
