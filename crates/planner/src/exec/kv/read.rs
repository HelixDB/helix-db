//! Interpreter-facing KV read operation contracts.

use serde::{Deserialize, Serialize};

use super::{batch, key};
use crate::{ir, properties};

/// KV read operation selected by the planner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KvReadPlan {
    /// Single-key read.
    Get { key: key::KvKey },
    /// Batched point reads.
    MultiGet(batch::KvMultiGetPlan),
    /// Ordered range scan.
    RangeScan {
        /// Keyspace.
        keyspace: key::ElementKeyspace,
        /// Start bound.
        start: key::KvKeyBound,
        /// End bound.
        end: key::KvKeyBound,
        /// Optional result limit.
        limit: Option<properties::PositiveUsize>,
    },
    /// Prefix scan.
    PrefixScan {
        /// Keyspace.
        keyspace: key::ElementKeyspace,
        /// Non-empty key prefix.
        prefix: ir::AtLeast<u8, 1>,
        /// Optional result limit.
        limit: Option<properties::PositiveUsize>,
    },
}
