//! Runtime side of the immutable vector/text index-operation queue.
//!
//! Foreground graph transactions construct complete operations
//! ([`producer`]), reserve retained capacity ([`backlog`]), and stage one blind
//! enqueue operand per `(scope, index, generation)` queue key in the same
//! transaction as the graph change. The publication worker later applies and
//! acknowledges exact operation IDs.

pub(crate) mod backlog;
pub(crate) mod lag;
pub(crate) mod recovery;
pub(crate) mod storage;

use bytes::Bytes;

use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{IndexOperationQueueKey, ManagedIndexKey, ScopedKey};

use super::{IndexGenerationId, IndexId};

/// Exact output ceilings for one publication transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OutputBudget {
    pub(crate) max_operations: u64,
    pub(crate) max_bytes: u64,
}

/// One directly addressable generation queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct QueueTarget {
    pub(crate) scope: DataScope,
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
}

impl QueueTarget {
    /// Binds one scope, logical index, and generation.
    pub(crate) const fn new(
        scope: DataScope,
        index_id: IndexId,
        generation: IndexGenerationId,
    ) -> Self {
        Self {
            scope,
            index_id,
            generation,
        }
    }

    /// Returns the logical index aggregating this generation's accounting.
    pub(crate) const fn logical_index(self) -> backlog::LogicalIndex {
        backlog::LogicalIndex {
            scope: self.scope,
            index_id: self.index_id,
        }
    }

    /// Returns the canonical physical queue key.
    pub(crate) fn key(self) -> Bytes {
        ManagedIndexKey::Data {
            scope: self.scope,
            kind: ScopedKey::IndexOperationQueue(IndexOperationQueueKey {
                index_id: self.index_id,
                generation: self.generation,
            }),
        }
        .to_bytes()
    }
}

#[cfg(test)]
mod codec_storage_tests;
