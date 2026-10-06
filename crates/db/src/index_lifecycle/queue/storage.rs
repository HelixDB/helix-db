//! Storage layouts for immutable index-operation queues.
//!
//! [`QueueLayout::Map`] keeps one merge-backed value per generation; producers
//! and acknowledgements stage blind merge operands. [`QueueLayout::Rows`]
//! stores one row per operation under a writer-allocated sequence and
//! acknowledges by deleting that row; it is the baseline for queue-layout
//! benchmarks. Both layouts decode into the same [`OperationQueue`], so the
//! producer, the publisher, recovery, and search overlays run identical logic
//! over either layout. Only test and benchmark builds can select the row
//! layout. A database always reopens with the layout that wrote its queues:
//! writer opens fail closed on the other layout's queues, and so do reader
//! opens in builds that can select it.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::Mutex;
use slatedb::{DbReadOps, DbTransaction};

use crate::config::QueueLayout;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{IndexOperationRowKey, ManagedIndexKey, RecordKind, ScopedKey};
use crate::encoding::v2::values::indexes::operation_queue::{
    LatestOperations, OperationQueue, QueueFamily, QueueOperand, QueueRow, QueuedOperation,
    QueuedOperationId, QueuedPayload,
};
use crate::error::{HelixDbError, Result};

use super::{OutputBudget, QueueTarget};

/// Exact writes one acknowledgement stages in a publication transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AcknowledgementOutput {
    pub(crate) operations: u64,
    pub(crate) bytes: u64,
}

/// One generation queue as read from storage.
#[derive(Debug)]
pub(crate) struct StoredQueue {
    queue: OperationQueue,
    /// Row key of each operation (row layout only).
    rows: HashMap<QueuedOperationId, Bytes>,
    /// Stored key and value bytes read to materialize the queue.
    encoded_bytes: u64,
}

impl StoredQueue {
    /// Returns the decoded queue in enqueue order.
    pub(crate) const fn queue(&self) -> &OperationQueue {
        &self.queue
    }

    /// Returns the stored key and value bytes read to materialize the queue.
    pub(crate) const fn encoded_bytes(&self) -> u64 {
        self.encoded_bytes
    }

    /// Returns what remains of this queue once `acknowledged` committed, or
    /// `None` when nothing remains; the remainder was read from no storage.
    fn without(self, acknowledged: &[QueuedOperationId]) -> Option<Self> {
        let acknowledged = acknowledged.iter().copied().collect::<HashSet<_>>();
        let family = self.queue.family();
        let mut operations = self.queue.into_operations();
        operations.retain(|operation| !acknowledged.contains(&operation.id()));
        let mut rows = self.rows;
        rows.retain(|id, _| !acknowledged.contains(id));
        OperationQueue::from_rows(family, operations).map(|queue| Self {
            queue,
            rows,
            encoded_bytes: 0,
        })
    }
}

/// One generation queue's stored bytes as a search read them, not yet
/// decoded.
#[derive(Debug)]
pub(crate) enum PendingQueueBytes {
    /// The merge-backed value, resolved by the read.
    Map(Bytes),
    /// Every operation row in sequence order; never empty.
    Rows(Vec<Bytes>),
}

impl PendingQueueBytes {
    /// Decodes each entity at its latest outstanding operation, selecting
    /// entities in the order of their oldest operation while their latest
    /// operations' retained bytes fit `budget` (see [`LatestOperations`]).
    ///
    /// Only decoding follows the budget: finding each entity's latest
    /// operation walks every record's framing. Everything decoded must be
    /// valid, otherwise the decode fails closed.
    pub(crate) fn decode_latest(&self, budget: u64) -> Result<LatestOperations> {
        match self {
            Self::Map(value) => Ok(LatestOperations::decode(value, budget)?),
            Self::Rows(values) => {
                LatestOperations::decode_rows(values.iter().map(Bytes::as_ref), budget)?.ok_or_else(
                    || {
                        HelixDbError::InvariantViolation(
                            "a search read an empty operation row set".to_string(),
                        )
                    },
                )
            }
        }
    }
}

/// Queues publication read from storage, each minus the operations its own
/// commits acknowledged since, kept for the target's next attempt so that
/// draining a backlog reads it once instead of once per batch.
///
/// # Contract
///
/// Only publication acknowledges a queue and only one attempt per target
/// runs at a time, so a retained queue is always a subset of the durable
/// queue in its storage order: every operation it holds is still
/// outstanding, and anything enqueued since committed after the read, so it
/// follows every retained operation of its entity. An attempt therefore
/// [takes](Self::take) its target's queue before it classifies the target
/// and [retains](Self::retain) only what an outcome it knows the durable
/// effect of left: the remainder after a commit that succeeded, or the
/// whole queue after an attempt that provably committed nothing (a trimmed
/// selection or a definite commit conflict). An outcome whose
/// acknowledgement may or may not have committed, an error, a blocked
/// operation, an ownership change, and a retired or hidden generation drop
/// it. Newer work becomes visible once the retained operations are
/// published and the next attempt reads storage again; a retained queue
/// whose every entity is held back is dropped and read again in the same
/// attempt, since only newer work can repair a held entity.
///
/// Every publisher of one writer shares one instance (it lives in the
/// writer's [`QueueStore`]), so the take-then-retain discipline holds
/// whichever publisher attempts a target. Retained queues are charged their
/// operations' retained bytes against one budget shared by every target. A
/// queue that does not fit beside those already held is dropped and read
/// again by its target's next attempt; held queues are never evicted for
/// it. Publication visits targets round-robin, so evicting the least
/// recently used queue would evict exactly the one attempted next, while a
/// held queue only shrinks as its target drains and so frees its share.
#[derive(Debug)]
pub(crate) struct RetainedQueues {
    /// Most retained bytes held across every target.
    budget: u64,
    state: Mutex<RetainedState>,
}

#[derive(Debug, Default)]
struct RetainedState {
    /// Retained bytes of every held queue.
    held: u64,
    queues: HashMap<QueueTarget, (StoredQueue, u64)>,
}

impl RetainedQueues {
    /// Holds at most `budget` retained bytes of operations across every
    /// target.
    pub(crate) fn new(budget: u64) -> Self {
        Self {
            budget,
            state: Mutex::new(RetainedState::default()),
        }
    }

    /// Removes and returns `target`'s retained queue, releasing its bytes.
    pub(crate) fn take(&self, target: QueueTarget) -> Option<StoredQueue> {
        let mut state = self.state.lock();
        let (stored, bytes) = state.queues.remove(&target)?;
        state.held -= bytes;
        Some(stored)
    }

    /// Retains what remains of `stored`, which `target`'s attempt read or
    /// took, once exactly `acknowledged` committed: empty when the attempt
    /// committed nothing. Drops it when it does not fit the budget.
    pub(crate) fn retain(
        &self,
        target: QueueTarget,
        stored: StoredQueue,
        acknowledged: &[QueuedOperationId],
    ) {
        let Some(remaining) = stored.without(acknowledged) else {
            return;
        };
        let bytes = remaining
            .queue
            .operations()
            .iter()
            .map(QueuedOperation::retained_bytes)
            .sum::<u64>();
        let mut state = self.state.lock();
        assert!(
            !state.queues.contains_key(&target),
            "an attempt retains only the queue it took"
        );
        if bytes > self.budget.saturating_sub(state.held) {
            return;
        }
        state.held += bytes;
        state.queues.insert(target, (remaining, bytes));
    }

    /// Returns the retained bytes of every held queue.
    #[cfg(test)]
    pub(crate) fn retained_bytes(&self) -> u64 {
        self.state.lock().held
    }
}

/// Queue storage bound to one database's layout and WAL entry bound.
#[derive(Debug)]
pub(crate) struct QueueStore {
    layout: QueueLayout,
    /// Largest merge operand one transaction may stage for one queue key.
    max_operand_bytes: u64,
    /// Next row sequence (row layout only); rows sort in enqueue order.
    next_sequence: AtomicU64,
    /// Queues every publisher of this writer retains between attempts.
    retained: RetainedQueues,
}

impl QueueStore {
    /// Creates storage for `layout` whose operands stay within
    /// `max_operand_bytes` and whose publishers retain at most
    /// `retained_budget` bytes of queued operations between attempts;
    /// recovery raises the row sequence past every retained row before the
    /// first write.
    pub(crate) fn new(layout: QueueLayout, max_operand_bytes: u64, retained_budget: u64) -> Self {
        Self {
            layout,
            max_operand_bytes,
            next_sequence: AtomicU64::new(0),
            retained: RetainedQueues::new(retained_budget),
        }
    }

    /// Returns the queues publication retains between attempts. Searches
    /// never read them: they read storage.
    pub(crate) const fn retained(&self) -> &RetainedQueues {
        &self.retained
    }

    /// Returns the per-transaction operand ceiling for one queue key: the
    /// configured ceiling clamped to the WAL replay entry bound.
    pub(crate) const fn max_operand_bytes(&self) -> u64 {
        self.max_operand_bytes
    }

    /// Returns the configured layout.
    #[cfg(test)]
    pub(crate) const fn layout(&self) -> QueueLayout {
        self.layout
    }

    /// Reads one generation queue through `read`.
    ///
    /// Absence is the empty queue. A present queue must decode completely
    /// and hold one family, otherwise the read fails closed.
    pub(crate) async fn read(
        &self,
        read: &(impl DbReadOps + Sync),
        target: QueueTarget,
    ) -> Result<Option<StoredQueue>> {
        match self.layout {
            QueueLayout::Map => {
                let Some(value) = read.get(target.key()).await? else {
                    return Ok(None);
                };
                Ok(Some(StoredQueue {
                    queue: OperationQueue::decode(&value)?,
                    rows: HashMap::new(),
                    encoded_bytes: (target.key().len() + value.len()) as u64,
                }))
            }
            QueueLayout::Rows => {
                let prefix = row_prefix(target);
                let mut scan = read.scan_prefix(&prefix, ..).await?;
                let mut family = None;
                let mut operations = Vec::new();
                let mut rows = HashMap::new();
                let mut encoded_bytes = 0_u64;
                while let Some(row) = scan.next().await? {
                    encoded_bytes += (row.key.len() + row.value.len()) as u64;
                    let (row_family, operation) = QueueRow::decode(&row.value)?;
                    if *family.get_or_insert(row_family) != row_family {
                        return Err(HelixDbError::IndexCatalogCorruption(format!(
                            "operation rows for index {} mix families",
                            target.index_id.get()
                        )));
                    }
                    rows.insert(operation.id(), row.key);
                    operations.push(operation);
                }
                let Some(family) = family else {
                    return Ok(None);
                };
                Ok(
                    OperationQueue::from_rows(family, operations).map(|queue| StoredQueue {
                        queue,
                        rows,
                        encoded_bytes,
                    }),
                )
            }
        }
    }

    /// Reads, for a search, the stored bytes of one generation queue; `None`
    /// when the queue is absent. Decode them with
    /// [`PendingQueueBytes::decode_latest`], off the async workers: that
    /// work follows the backlog.
    ///
    /// The map layout's read fetches the whole value: while merge operands
    /// are pending above its base, which is the normal state of an index
    /// taking writes, SlateDB first resolves them against all of it,
    /// validating and re-encoding every record, inside this read. Such a
    /// read therefore costs the backlog (up to the per-index
    /// `max_retained_bytes`) whatever a later decode selects, and fails on a
    /// corrupt record no decode would select. The row layout scans every row.
    pub(crate) async fn read_latest(
        &self,
        read: &(impl DbReadOps + Sync),
        target: QueueTarget,
    ) -> Result<Option<PendingQueueBytes>> {
        match self.layout {
            QueueLayout::Map => Ok(read.get(target.key()).await?.map(PendingQueueBytes::Map)),
            QueueLayout::Rows => {
                let prefix = row_prefix(target);
                let mut scan = read.scan_prefix(&prefix, ..).await?;
                let mut values = Vec::new();
                while let Some(row) = scan.next().await? {
                    values.push(row.value);
                }
                Ok((!values.is_empty()).then_some(PendingQueueBytes::Rows(values)))
            }
        }
    }

    /// Stages one transaction's operations for `target`.
    ///
    /// Map operands are blind merges; rows are fresh keys. Neither reads the
    /// queue, so concurrent producers and acknowledgements never conflict on
    /// it.
    pub(crate) fn stage_enqueue(
        &self,
        transaction: &DbTransaction,
        target: QueueTarget,
        operand: QueueOperand,
        operations: &[QueuedOperation],
    ) -> Result<()> {
        match self.layout {
            QueueLayout::Map => {
                let (bytes, tokens) = operand.into_parts();
                transaction.merge_disjoint_tokens(target.key(), tokens, bytes)?;
            }
            QueueLayout::Rows => {
                for operation in operations {
                    let family = match operation.payload() {
                        QueuedPayload::Vector(_) => QueueFamily::Vector,
                        QueuedPayload::Text(_) => QueueFamily::Text,
                    };
                    transaction.put(
                        row_key(target, self.next_sequence.fetch_add(1, Ordering::Relaxed)),
                        QueueRow::encode(family, operation),
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Stages acknowledgements of exact operation IDs read in `stored`.
    pub(crate) fn stage_acknowledge(
        &self,
        transaction: &DbTransaction,
        target: QueueTarget,
        stored: &StoredQueue,
        ids: &[QueuedOperationId],
    ) -> Result<()> {
        match self.layout {
            QueueLayout::Map => {
                let (bytes, tokens) =
                    QueueOperand::acknowledge(stored.queue.family(), ids.iter().copied())
                        .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?
                        .into_parts();
                transaction.merge_disjoint_tokens(target.key(), tokens, bytes)?;
            }
            QueueLayout::Rows => {
                for id in ids {
                    let Some(key) = stored.rows.get(id) else {
                        return Err(HelixDbError::InvariantViolation(
                            "acknowledged operation was not read from its row".to_string(),
                        ));
                    };
                    transaction.delete(key)?;
                }
            }
        }
        Ok(())
    }

    /// Returns the exact writes one acknowledgement of `ids` stages, so
    /// publication budgets include it.
    pub(crate) fn acknowledgement_output(
        &self,
        target: QueueTarget,
        stored: &StoredQueue,
        ids: &[QueuedOperationId],
    ) -> Result<AcknowledgementOutput> {
        match self.layout {
            QueueLayout::Map => {
                let operand = QueueOperand::acknowledge(stored.queue.family(), ids.iter().copied())
                    .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?;
                Ok(AcknowledgementOutput {
                    operations: 1,
                    bytes: (target.key().len() + operand.bytes().len()) as u64,
                })
            }
            QueueLayout::Rows => Ok(AcknowledgementOutput {
                operations: ids.len() as u64,
                bytes: ids
                    .iter()
                    .filter_map(|id| stored.rows.get(id))
                    .map(|key| key.len() as u64)
                    .sum(),
            }),
        }
    }

    /// Returns the most operation IDs one acknowledgement of `target` may
    /// name so that it alone fits `budget` and, for the map layout, its
    /// operand fits the same WAL entry bound producer operands respect.
    ///
    /// A map acknowledgement is one merge operand; a row acknowledgement
    /// deletes one fixed-width row key per ID. At least one ID is always
    /// allowed. Below one ID, the operand bound also rejects every producer
    /// operand, so no queue it could strand exists; an output budget does
    /// not, so publication measures the acknowledgement against it and
    /// reports [`super::publication::PublicationOutcome::Blocked`], while a
    /// discard, which stages nothing else, still names one ID so a retired
    /// queue always drains.
    pub(crate) fn acknowledgement_capacity(
        &self,
        target: QueueTarget,
        budget: OutputBudget,
    ) -> NonZeroUsize {
        let capacity = match self.layout {
            QueueLayout::Map => QueueOperand::acknowledgement_capacity(
                self.max_operand_bytes
                    .min(budget.max_bytes.saturating_sub(target.key().len() as u64)),
            ),
            QueueLayout::Rows => budget
                .max_operations
                .min(budget.max_bytes / row_key(target, 0).len() as u64),
        };
        NonZeroUsize::new(usize::try_from(capacity).unwrap_or(usize::MAX))
            .unwrap_or(NonZeroUsize::MIN)
    }

    /// Discovers every queue in `scope` for startup accounting.
    ///
    /// Rows of the other layout mean the database was written with a
    /// different layout, which fails closed rather than orphaning work.
    pub(crate) async fn discover(
        &self,
        read: &(impl DbReadOps + Sync),
        scope: DataScope,
    ) -> Result<Vec<(QueueTarget, OperationQueue)>> {
        require_scope_layout(read, self.layout, scope).await?;
        let own = match self.layout {
            QueueLayout::Map => RecordKind::IndexOperationQueue,
            QueueLayout::Rows => RecordKind::IndexOperationRow,
        };
        let prefix = ManagedIndexKey::data_prefix(scope, ScopedKey::logical_prefix(own));
        let mut scan = read.scan_prefix(&prefix, ..).await?;
        let mut queues = Vec::new();
        // Row keys sort by (index, generation, sequence), so one generation's
        // rows are contiguous and already in enqueue order.
        let mut pending: Option<(QueueTarget, QueueFamily, Vec<QueuedOperation>)> = None;
        while let Some(row) = scan.next().await? {
            match ManagedIndexKey::parse_data_from_slice(&row.key) {
                Ok(ManagedIndexKey::Data {
                    scope: key_scope,
                    kind: ScopedKey::IndexOperationQueue(key),
                }) if key_scope == scope => queues.push((
                    QueueTarget::new(scope, key.index_id, key.generation),
                    OperationQueue::decode(&row.value)?,
                )),
                Ok(ManagedIndexKey::Data {
                    scope: key_scope,
                    kind: ScopedKey::IndexOperationRow(key),
                }) if key_scope == scope => {
                    self.next_sequence
                        .fetch_max(key.sequence.saturating_add(1), Ordering::Relaxed);
                    let target = QueueTarget::new(scope, key.index_id, key.generation);
                    let (family, operation) = QueueRow::decode(&row.value)?;
                    match pending.as_mut() {
                        Some((current, current_family, operations)) if *current == target => {
                            if *current_family != family {
                                return Err(HelixDbError::IndexCatalogCorruption(format!(
                                    "operation rows for index {} mix families",
                                    target.index_id.get()
                                )));
                            }
                            operations.push(operation);
                        }
                        Some(_) | None => {
                            queues.extend(finish_rows(pending.take()));
                            pending = Some((target, family, vec![operation]));
                        }
                    }
                }
                Ok(_) | Err(_) => {
                    return Err(HelixDbError::IndexCatalogCorruption(
                        "operation queue prefix holds another key".to_string(),
                    ));
                }
            }
        }
        queues.extend(finish_rows(pending));
        Ok(queues)
    }
}

/// Fails closed when `scope` holds queues written with a layout other than
/// `layout`.
///
/// Recovery, publication, and search overlays read one layout only, so the
/// other layout's operations would otherwise be silently invisible.
pub(crate) async fn require_scope_layout(
    read: &(impl DbReadOps + Sync),
    layout: QueueLayout,
    scope: DataScope,
) -> Result<()> {
    let other = match layout {
        QueueLayout::Map => RecordKind::IndexOperationRow,
        QueueLayout::Rows => RecordKind::IndexOperationQueue,
    };
    let prefix = ManagedIndexKey::data_prefix(scope, ScopedKey::logical_prefix(other));
    if read.scan_prefix(&prefix, ..).await?.next().await?.is_some() {
        return Err(HelixDbError::Config(format!(
            "index operation queues were written with a layout other than {layout:?}"
        )));
    }
    Ok(())
}

/// Closes one generation's contiguous rows into a queue.
fn finish_rows(
    rows: Option<(QueueTarget, QueueFamily, Vec<QueuedOperation>)>,
) -> Option<(QueueTarget, OperationQueue)> {
    let (target, family, operations) = rows?;
    OperationQueue::from_rows(family, operations).map(|queue| (target, queue))
}

/// Prefix of every row of one generation queue (row layout).
fn row_prefix(target: QueueTarget) -> Bytes {
    ManagedIndexKey::data_prefix(
        target.scope,
        ScopedKey::generation_prefix(
            RecordKind::IndexOperationRow,
            target.index_id,
            target.generation,
        ),
    )
}

fn row_key(target: QueueTarget, sequence: u64) -> Bytes {
    ManagedIndexKey::Data {
        scope: target.scope,
        kind: ScopedKey::IndexOperationRow(IndexOperationRowKey {
            index_id: target.index_id,
            generation: target.generation,
            sequence,
        }),
    }
    .to_bytes()
}
