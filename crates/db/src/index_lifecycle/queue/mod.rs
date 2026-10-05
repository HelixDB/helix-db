//! Runtime side of the immutable vector/text index-operation queue.
//!
//! Foreground graph transactions construct complete operations
//! ([`producer`]), reserve retained capacity ([`backlog`]), and stage one blind
//! enqueue operand per `(scope, index, generation)` queue key in the same
//! transaction as the graph change. The publication worker later applies and
//! acknowledges exact operation IDs.
//!
//! # Build source reads
//!
//! Vector and text builds read graph rows from a snapshot outside their step's
//! serializable transaction, so a write inside a step's read-to-commit window
//! does not abort the step. Every graph write that commits after a build is
//! created also queues a complete operation for its hidden `Building`
//! generation (a write whose serializable catalog read predates the creation
//! conflicts with it). Publication defers those operations until activation,
//! then applies them in commit order as idempotent replacements or deletes:
//!
//! - a row changed after the build read it is corrected by its operation;
//! - a row changed before the build read it is re-applied with the same value,
//!   which vector publication recognizes and leaves in place;
//! - a deleted row is removed by its operation;
//! - an entity above the build's source watermark arrives only from the queue.
//!
//! Once the queue drains, the generation therefore holds the same documents
//! whichever snapshot, taken after the build's creation, each step read. Text
//! `ScanPartitions` rereads graph rows the same way and realigns each entity's
//! statistics marker with the document it builds, which queued publication
//! diffs against. Rows the build owns (operation, canonical record, applied
//! and entity state, statistics, tenant mappings, physical rows) stay in the
//! step's serializable read set. Secondary builds have no queue and read
//! source rows serializably.
//!
//! The queue corrects documents, not blockers, and a blocker is durable. A
//! step that blocks on a graph row therefore also reads that row through its
//! transaction (`ScanPartitions` retains it as an expected read), so a write
//! that repairs the row before the step commits makes the step retry.

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
    /// output budget, or with fewer text entities after planning a text epoch
    /// failed deterministically.
    pub output_retries: u64,
    /// Attempts that held an entity back: one operation's effect and
    /// acknowledgement alone exceeded an output budget, or planning the
    /// entity failed deterministically.
    pub blocked_attempts: u64,
    /// Entities held back right now (a gauge, unlike the publication
    /// counters): see [`crate::HelixDB::blocked_index_entities`].
    pub blocked_entities: u64,
    /// Operations of retired generations acknowledged without publication.
    pub discarded_operations: u64,
    /// Storage reads of a generation queue by publication, including reads
    /// that find it empty, retired-generation discards, and uncertain-commit
    /// reconciliation. An attempt that continues from the queue its target's
    /// previous commit left reads nothing, so draining a backlog reads it
    /// once rather than once per batch.
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
    /// Retries caused by an error: a transient one, such as storage I/O, or a
    /// deterministic one that no single entity's planning raised.
    pub publication_error_retries: u64,
    /// Attempts deferred because a hidden build owns the generation.
    pub deferred_attempts: u64,
}

/// One entity whose queued vector/text work is held back because one of its
/// operations alone can never fit a publication under the current limits,
/// for example after they were lowered, or because planning its change fails
/// deterministically, for example on damaged index rows or a corrupt queued
/// payload.
///
/// It blocks only its own publication: the rest of its generation keeps
/// publishing. Its operations stay queued, so strong searches keep serving
/// its newest committed state, and each later write to the entity is retried,
/// alone and at full width, once the publisher's rotation reaches it; it
/// publishes once one publication fits its newest state. However many of its
/// operations are queued, that repair publishes its newest state; one
/// publication acknowledges at most one acknowledgement's worth of them, so
/// further repairs republish that state until every one is acknowledged.
/// Only the publisher's process memory knows that: after a restart, the rest
/// publish in regular batches, each serving the newest operation it
/// acknowledges, so the published state can step back to an older queued
/// state until they are all acknowledged. Strong searches overlay every
/// queued operation and never see that; eventual searches that do not reach
/// the entity within their budget can.
///
/// A rewrite or delete repairs it only once one publication fits the change
/// from its published state and planning that change succeeds: removing a
/// document published under larger limits, or relinking a deleted vector's
/// neighbors, can exceed the lowered limits too. Raising the limits again
/// lets it publish. An entity held back after its planning failed is also
/// planned again about once a minute without a write, so it publishes on its
/// own once what failed is repaired, for example restored metadata; a held
/// delete is never written again, and this is what publishes it.
///
/// Its queued operations, and each later write to it, keep counting toward
/// its index's retained-byte limit until it publishes, a limit the whole
/// index shares. An entity written over and over while its planning keeps
/// failing, for example on damaged index rows of its own, therefore fills
/// that limit, and then writes to every entity of the index fail with
/// `index_backpressure`. Stop writing it until it publishes.
///
/// Its queued text still counts toward the pending text a strong text search
/// may analyze in its partition (one text publication's analysis budget),
/// and no publication drains it: while held-back text alone exceeds that
/// budget, strong text searches of the partition fail with
/// `index_backpressure` (`pending_text_analysis_bytes`) that retrying cannot
/// clear. Raise the limits again, rewrite or delete the entity where that
/// fits, or search with eventual consistency.
///
/// [`crate::HelixDB::blocked_index_entities`] lists them, and the writer logs
/// an error naming each one's scope, index, generation, and entity when it is
/// held back. The server's unauthenticated health responses report only how
/// many there are, never which.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BlockedIndexEntity {
    /// Data scope owning the index.
    pub scope: DataScope,
    /// Logical index.
    pub index_id: IndexId,
    /// Physical generation whose queue holds the operations.
    pub generation: IndexGenerationId,
    /// Node or edge.
    pub kind: super::IndexElementKind,
    /// Graph ID of the node or edge.
    pub id: super::IndexEntityId,
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
mod isolation_tests;
#[cfg(test)]
mod layout_tests;
#[cfg(test)]
mod lifecycle_tests;
#[cfg(test)]
mod overlay_tests;
#[cfg(test)]
mod planning_session_tests;
#[cfg(test)]
mod publication_tests;
#[cfg(test)]
mod soak_tests;
#[cfg(test)]
mod stats_tests;
#[cfg(test)]
pub(crate) mod tests;
#[cfg(test)]
mod text_publication_tests;
#[cfg(test)]
mod vector_graph_tests;
