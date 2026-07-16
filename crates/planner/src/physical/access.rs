use serde::{Deserialize, Serialize};

use crate::{exec, properties};

/// Physical access family independent of node/edge duplication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhysicalAccess {
    /// KV read.
    Kv(exec::KvReadPlan),
    /// Empty access path.
    Empty,
    /// Runtime parameter or variable input.
    RuntimeInput,
    /// Point reads that are costed as split batches.
    PointReads {
        /// Locality used for batch costing.
        locality: properties::KeyLocality,
    },
    /// Label scan.
    LabelScan,
    /// Equality-index lookup.
    EqualityIndex,
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
