//! Runtime side of the immutable vector/text index-operation queue.
//!
//! Foreground graph transactions construct complete operations
//! ([`producer`]), reserve retained capacity ([`backlog`]), and stage one blind
//! enqueue operand per `(scope, index, generation)` queue key in the same
//! transaction as the graph change. The publication worker later applies and
//! acknowledges exact operation IDs.

pub(crate) mod backlog;
pub(crate) mod lag;
pub(crate) mod producer;
pub(crate) mod publication;
pub(crate) mod recovery;
pub(crate) mod storage;

use bytes::Bytes;

use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{IndexOperationQueueKey, ManagedIndexKey, ScopedKey};

use super::{IndexGenerationId, IndexId};

/// Point-in-time counters for asynchronous vector/text index publication.
///
/// Backlog fields describe work retained right now across every logical
/// index; publication fields are monotonic since this handle opened. A
/// reader handle owns neither the admission ledger nor a publisher, so every
/// field reads zero there.
///
/// Every durable operation is counted once, in `committed_operations` or
/// `discovered_operations`, and every durable exact-ID acknowledgement once in
/// `acknowledged_operations`. Each acknowledged operation is either timed (its
/// lag is in [`crate::HelixDB::index_operation_publication_lag`]) or
/// censored, so `acknowledged_operations - censored_acknowledgements` equals
/// the lag histogram's count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize)]
pub struct IndexOperationQueueStats {
    /// Encoded bytes of committed operations not yet acknowledged.
    pub retained_bytes: u64,
    /// Distinct pending `(generation, entity)` members.
    pub pending_members: u64,
    /// Committed operations not yet acknowledged.
    pub pending_operations: u64,
    /// Pending operations whose enqueue or acknowledgement commit outcome is
    /// still being reconciled; their capacity stays charged meanwhile.
    pub uncertain_operations: u64,
    /// Operations published and acknowledged by a commit this handle observed
    /// succeed.
    pub published_operations: u64,
    /// Entities whose collapsed effects were published.
    pub published_entities: u64,
    /// Publication transactions committed.
    pub committed_batches: u64,
    /// Publication transactions that lost a serializable conflict.
    pub commit_conflicts: u64,
    /// Publication commits with an unknown outcome.
    pub uncertain_commits: u64,
    /// Attempts retried with fewer entities or operations after exceeding an
    /// output budget.
    pub output_retries: u64,
    /// Attempts where one operation's effect and acknowledgement alone
    /// exceeded an output budget.
    pub blocked_attempts: u64,
    /// Operations of retired generations acknowledged without publication.
    pub discarded_operations: u64,
    /// Queues read and decoded by publication, including retired-generation
    /// discards and uncertain-commit reconciliation.
    pub queue_reads: u64,
    /// Stored key and value bytes those reads returned.
    pub queue_read_bytes: u64,
    /// Wall-clock microseconds of those reads: storage I/O (object-store
    /// fetches on a cache miss), the map layout's read-time merge resolution
    /// (also in [`crate::OperationQueueMergeStats::resolved`]), and decoding.
    pub queue_read_micros: u64,
    /// Enqueue commits this handle observed succeed (durable foreground
    /// operation commits).
    pub committed_operations: u64,
    /// Durable operations whose enqueue commit this handle did not observe
    /// succeed: found in storage at startup or by reconciliation, or proven
    /// durable by publication after an uncertain enqueue.
    pub discovered_operations: u64,
    /// Operations released by a durable exact-ID acknowledgement: published,
    /// discarded, or an uncertain acknowledgement a flushed read proved
    /// committed.
    pub acknowledged_operations: u64,
    /// Acknowledged operations whose lag is censored rather than measured:
    /// this handle did not observe their enqueue commit return before
    /// learning they were durable (found at startup or by reconciliation, or
    /// acknowledged, or their acknowledgement attempted, by publication
    /// before their producer's commit returned, even if it then returned
    /// success).
    pub censored_acknowledgements: u64,
    /// Age of the oldest pending operation committed through this handle, a
    /// lower bound on its eventual publication lag. Zero when none pends.
    pub oldest_pending_micros: u64,
    /// Publication attempts, whatever their outcome.
    pub publication_attempts: u64,
    /// Wall-clock microseconds spent in publication attempts.
    pub publication_attempt_micros: u64,
    /// Attempts that must rediscover and retry: commit conflicts, uncertain
    /// commits, retryable errors, and ownership changes after classification.
    pub publication_retries: u64,
    /// Retries caused by a retryable storage or decoding error.
    pub publication_error_retries: u64,
    /// Attempts deferred because a hidden build owns the generation.
    pub deferred_attempts: u64,
}

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
#[cfg(test)]
mod layout_tests;
#[cfg(test)]
mod lifecycle_tests;
#[cfg(test)]
mod overlay_tests;
#[cfg(test)]
mod publication_tests;
#[cfg(test)]
mod stats_tests;
#[cfg(test)]
pub(crate) mod tests;
#[cfg(test)]
mod text_publication_tests;
