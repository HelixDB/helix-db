//! Storage layouts for immutable index-operation queues.
//!
//! [`QueueLayout::Map`] keeps one merge-backed value per generation; producers
//! and acknowledgements stage blind merge operands. [`QueueLayout::Rows`]
//! stores one row per operation under a writer-allocated sequence and
//! acknowledges by deleting that row; it is the baseline for queue-layout
//! benchmarks. Both layouts read into the same [`StoredQueue`] for
//! publication, [`LatestOperations`] for search overlays, and
//! [`OperationFrame`]s for startup accounting, so the publisher, recovery,
//! and search overlays run identical logic over either layout. Only test and
//! benchmark builds can select the row layout. A database always reopens
//! with the layout that wrote its queues: writer opens fail closed on the
//! other layout's queues, and so do reader opens in builds that can select
//! it.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroUsize;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::Mutex;
use slatedb::{DbReadOps, DbTransaction};

use crate::config::QueueLayout;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{
    IndexEntity, IndexOperationRowKey, ManagedIndexKey, RecordKind, ScopedKey,
};
use crate::encoding::v2::values::indexes::operation_queue::{
    LatestOperations, OperationFrame, OperationQueue, QueueFamily, QueueOperand, QueueRow,
    QueuedOperation, QueuedOperationId, QueuedPayload,
};
use crate::error::{HelixDbError, Result};

use super::backlog::{charged_bytes, Admission};
use super::{OutputBudget, QueueTarget};

/// Exact writes one acknowledgement stages in a publication transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AcknowledgementOutput {
    pub(crate) operations: u64,
    pub(crate) bytes: u64,
}

/// One generation queue as read from storage, grouped by entity.
///
/// Publication selects whole per-entity prefixes, visiting entities in the
/// order of each one's oldest outstanding operation, and acknowledges exactly
/// those prefixes. The queue therefore keeps each entity's operations as a
/// chain through the read's storage order, and its entities ordered by their
/// chains' oldest operations, both built once per read. Selecting a batch
/// visits only the entities it reaches ([`Self::rotation`]) and an
/// acknowledgement updates only the chains it names ([`Self::without`]), so
/// draining a backlog through retained queues costs one decode of it plus
/// work proportional to each batch, rather than a pass over the whole
/// backlog per batch.
///
/// ```text
/// read order   0:a1  1:b1  2:a2  3:c1  4:b2
/// chains       a: 0 -> 2   b: 1 -> 4   c: 3
/// order        {0: a, 1: b, 3: c}
/// without(a1)  a: 2        order {1: b, 2: a, 3: c}
/// ```
#[derive(Debug)]
pub(crate) struct StoredQueue {
    family: QueueFamily,
    /// Every operation read, in storage order; `None` once acknowledged.
    operations: Vec<Option<QueuedOperation>>,
    /// Read position of the same entity's next operation, by read position;
    /// an entity's newest operation points at itself.
    next: Vec<usize>,
    /// Each entity's outstanding operations; an entity is present only while
    /// it has one.
    chains: HashMap<IndexEntity, Chain>,
    /// Every chained entity, by the read position of its oldest outstanding
    /// operation.
    order: BTreeMap<usize, IndexEntity>,
    /// Every chained entity, by the ID of its oldest outstanding operation.
    oldest: HashMap<QueuedOperationId, IndexEntity>,
    /// Row key of each outstanding operation (row layout only).
    rows: HashMap<QueuedOperationId, Bytes>,
    /// [`charged_bytes`] summed over the outstanding operations.
    charged_bytes: u64,
    /// Outstanding operations; never zero.
    outstanding: NonZeroUsize,
    /// Stored key and value bytes read to materialize the queue.
    encoded_bytes: u64,
    /// The target's latest admission observed before the read, if recorded.
    admitted: Option<Admission>,
}

/// One entity's outstanding operations in a [`StoredQueue`].
#[derive(Debug, Clone, Copy)]
struct Chain {
    /// Read position of the oldest.
    oldest: usize,
    /// Read position of the newest.
    newest: usize,
    len: NonZeroUsize,
}

impl StoredQueue {
    /// Groups `family`'s `operations`, in storage order, read with the row
    /// keys `rows` (row layout only) from `encoded_bytes` stored bytes;
    /// `None` for the empty queue.
    pub(crate) fn new(
        family: QueueFamily,
        operations: Vec<QueuedOperation>,
        rows: HashMap<QueuedOperationId, Bytes>,
        encoded_bytes: u64,
    ) -> Option<Self> {
        let outstanding = NonZeroUsize::new(operations.len())?;
        let mut next = (0..operations.len()).collect::<Vec<_>>();
        let mut chains = HashMap::<IndexEntity, Chain>::new();
        let mut order = BTreeMap::new();
        let mut oldest = HashMap::new();
        let mut charged = 0_u64;
        for (position, operation) in operations.iter().enumerate() {
            charged += charged_bytes(operation.retained_bytes());
            match chains.entry(operation.entity()) {
                Entry::Occupied(mut chain) => {
                    let chain = chain.get_mut();
                    next[chain.newest] = position;
                    chain.newest = position;
                    chain.len = chain.len.saturating_add(1);
                }
                Entry::Vacant(chain) => {
                    chain.insert(Chain {
                        oldest: position,
                        newest: position,
                        len: NonZeroUsize::MIN,
                    });
                    order.insert(position, operation.entity());
                    oldest.insert(operation.id(), operation.entity());
                }
            }
        }
        Some(Self {
            family,
            operations: operations.into_iter().map(Some).collect(),
            next,
            chains,
            order,
            oldest,
            rows,
            charged_bytes: charged,
            outstanding,
            encoded_bytes,
            admitted: None,
        })
    }

    /// Records `admitted`, the target's latest admission observed before
    /// this queue was read: an operation admitted later may be missing.
    pub(crate) const fn observed_after(mut self, admitted: Option<Admission>) -> Self {
        self.admitted = admitted;
        self
    }

    /// Returns the latest admission [`Self::observed_after`] recorded.
    pub(crate) const fn admitted(&self) -> Option<Admission> {
        self.admitted
    }

    /// Returns the retained family.
    pub(crate) const fn family(&self) -> QueueFamily {
        self.family
    }

    /// Returns every outstanding operation in storage order.
    pub(crate) fn operations(&self) -> impl Iterator<Item = &QueuedOperation> {
        self.operations.iter().flatten()
    }

    /// Returns how many operations are outstanding.
    #[cfg(test)]
    pub(crate) const fn len(&self) -> NonZeroUsize {
        self.outstanding
    }

    /// Returns how many entities have an outstanding operation.
    pub(crate) fn entities(&self) -> usize {
        self.chains.len()
    }

    /// Returns whether `entity` has an outstanding operation.
    pub(crate) fn contains(&self, entity: IndexEntity) -> bool {
        self.chains.contains_key(&entity)
    }

    /// Returns what admission charges the outstanding operations
    /// ([`charged_bytes`]).
    #[cfg(test)]
    pub(crate) const fn charged_bytes(&self) -> u64 {
        self.charged_bytes
    }

    /// Returns the stored key and value bytes read to materialize the queue.
    pub(crate) const fn encoded_bytes(&self) -> u64 {
        self.encoded_bytes
    }

    /// Returns every entity's outstanding operations once, in the order of
    /// each entity's oldest outstanding operation, starting after `after`
    /// and wrapping around to end at it; from the first entity when `after`
    /// has no outstanding operation.
    ///
    /// Each entity it yields costs a lookup in the order, however many
    /// entities or operations the queue holds.
    pub(crate) fn rotation(
        &self,
        after: Option<IndexEntity>,
    ) -> impl Iterator<Item = EntityOperations<'_>> {
        let (first, second) = match after.and_then(|entity| self.chains.get(&entity)) {
            Some(chain) => (
                (Bound::Excluded(chain.oldest), Bound::Unbounded),
                (Bound::Unbounded, Bound::Included(chain.oldest)),
            ),
            None => (
                (Bound::Unbounded, Bound::Unbounded),
                (Bound::Unbounded, Bound::Excluded(0)),
            ),
        };
        self.order
            .range(first)
            .chain(self.order.range(second))
            .map(|(_, entity)| EntityOperations {
                queue: self,
                entity: *entity,
                chain: self.chains[entity],
            })
    }

    /// Returns what remains of this queue once `acknowledged` committed, or
    /// `None` when nothing remains; the remainder was read from no storage.
    ///
    /// Costs work proportional to `acknowledged`, never to the queue.
    ///
    /// # Panics
    ///
    /// Unless `acknowledged` names, entity by entity, each one's oldest
    /// outstanding operations in order, as every publication and discard
    /// acknowledges them.
    fn without(mut self, acknowledged: &[QueuedOperationId]) -> Option<Self> {
        for id in acknowledged {
            let Some(entity) = self.oldest.remove(id) else {
                panic!("an acknowledgement names each entity's oldest outstanding operations");
            };
            let Entry::Occupied(mut chain) = self.chains.entry(entity) else {
                unreachable!("an entity with an oldest operation is chained");
            };
            let position = chain.get().oldest;
            let operation = self.operations[position]
                .take()
                .expect("a chain names only outstanding operations");
            debug_assert_eq!(operation.id(), *id);
            self.order.remove(&position);
            self.rows.remove(id);
            self.charged_bytes -= charged_bytes(operation.retained_bytes());
            self.outstanding = NonZeroUsize::new(self.outstanding.get() - 1)?;
            let Some(len) = NonZeroUsize::new(chain.get().len.get() - 1) else {
                chain.remove();
                continue;
            };
            let next = self.next[position];
            let chain = chain.get_mut();
            chain.oldest = next;
            chain.len = len;
            self.order.insert(next, entity);
            self.oldest.insert(
                self.operations[next]
                    .as_ref()
                    .expect("a chain names only outstanding operations")
                    .id(),
                entity,
            );
        }
        Some(Self {
            encoded_bytes: 0,
            ..self
        })
    }
}

/// One entity's outstanding operations in a [`StoredQueue`], oldest first.
#[derive(Debug, Clone, Copy)]
pub(crate) struct EntityOperations<'a> {
    queue: &'a StoredQueue,
    entity: IndexEntity,
    chain: Chain,
}

impl<'a> EntityOperations<'a> {
    /// Returns the entity.
    pub(crate) const fn entity(&self) -> IndexEntity {
        self.entity
    }

    /// Returns how many operations of the entity are outstanding.
    pub(crate) const fn len(&self) -> NonZeroUsize {
        self.chain.len
    }

    /// Returns the entity's newest outstanding operation.
    pub(crate) fn newest(&self) -> &'a QueuedOperation {
        self.queue.operations[self.chain.newest]
            .as_ref()
            .expect("a chain names only outstanding operations")
    }

    /// Returns the entity's outstanding operations, oldest first.
    pub(crate) fn iter(self) -> impl Iterator<Item = &'a QueuedOperation> {
        let queue = self.queue;
        std::iter::successors(Some(self.chain.oldest), move |position| {
            Some(queue.next[*position])
        })
        .take(self.chain.len.get())
        .map(move |position| {
            queue.operations[position]
                .as_ref()
                .expect("a chain names only outstanding operations")
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
/// effect of left: the remainder after a commit that succeeded, publishing
/// or discarding, or the whole queue after an attempt that provably
/// committed nothing (a trimmed selection, an entity held back, or a
/// definite commit conflict). An outcome whose acknowledgement may or may
/// not have committed, an error, an ownership change, and a hidden
/// generation drop it; a retired generation discards from it. Newer work
/// becomes visible once the retained operations are published and the next
/// attempt reads storage again. A held-back entity is repaired only by newer
/// work or a due retry, so a retained queue whose every entity is held back
/// is read again in the same attempt when none has a repair to try, and one
/// whose every entity waits for newer work or a retry is read again as soon
/// as an operation was admitted since its read ([`StoredQueue::admitted`]).
///
/// Every publisher of one writer shares one instance (it lives in the
/// writer's [`QueueStore`]), so the take-then-retain discipline holds
/// whichever publisher attempts a target. Retained queues are charged what
/// admission charges their operations ([`charged_bytes`]) against one budget
/// shared by every target. A queue that does not fit beside those already
/// held is dropped and read again by its target's next attempt; held queues
/// are never evicted for it. Publication visits targets round-robin, so
/// evicting the least recently used queue would evict exactly the one
/// attempted next, while a held queue only shrinks as its target drains and
/// so frees its share.
#[derive(Debug)]
pub(crate) struct RetainedQueues {
    /// Most charged bytes held across every target.
    budget: u64,
    state: Mutex<RetainedState>,
}

#[derive(Debug, Default)]
struct RetainedState {
    /// Charged bytes of every held queue.
    held: u64,
    queues: HashMap<QueueTarget, (StoredQueue, u64)>,
}

impl RetainedQueues {
    /// Holds at most `budget` charged bytes of operations across every
    /// target.
    pub(crate) fn new(budget: u64) -> Self {
        Self {
            budget,
            state: Mutex::new(RetainedState::default()),
        }
    }

    /// Removes and returns `target`'s retained queue, releasing its charge.
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
        let bytes = remaining.charged_bytes;
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

    /// Returns the charged bytes of every held queue.
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
    /// `retained_budget` charged bytes of queued operations between attempts;
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
                let queue = OperationQueue::decode(&value)?;
                Ok(StoredQueue::new(
                    queue.family(),
                    queue.into_operations(),
                    HashMap::new(),
                    (target.key().len() + value.len()) as u64,
                ))
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
                Ok(StoredQueue::new(family, operations, rows, encoded_bytes))
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
        transaction: &impl crate::transaction::Mutation,
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
                let (bytes, tokens) = QueueOperand::acknowledge(stored.family, ids.iter().copied())
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
                let operand = QueueOperand::acknowledge(stored.family, ids.iter().copied())
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

    /// Groups the rows of [`discovery_range`]s, read in key order, into
    /// generation queues for startup accounting.
    pub(crate) const fn discovery(&self) -> QueueDiscovery<'_> {
        QueueDiscovery {
            store: self,
            pending: None,
        }
    }
}

/// Every queue key of `scope` in either layout.
///
/// The layouts' record kinds are adjacent, so one scan of this range both
/// finds this layout's queues and fails closed on the other's, without a
/// separate probe.
pub(crate) fn discovery_range(scope: DataScope) -> std::ops::Range<Bytes> {
    const _: () = assert!(
        RecordKind::IndexOperationRow.as_u8() == RecordKind::IndexOperationQueue.as_u8() + 1,
        "both queue layouts form one contiguous key range"
    );
    let start = ManagedIndexKey::data_prefix(
        scope,
        ScopedKey::logical_prefix(RecordKind::IndexOperationQueue),
    );
    let mut end = ManagedIndexKey::data_prefix(
        scope,
        ScopedKey::logical_prefix(RecordKind::IndexOperationRow),
    )
    .to_vec();
    *end.last_mut().expect("a logical prefix ends with its kind") += 1;
    start..Bytes::from(end)
}

/// One generation queue found at startup, as its operations' frames in
/// storage order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiscoveredQueue {
    pub(crate) target: QueueTarget,
    pub(crate) family: QueueFamily,
    /// Never empty.
    pub(crate) frames: Vec<OperationFrame>,
}

/// Turns the rows of [`discovery_range`]s into generation queues as a scan
/// yields them, so startup holds one queue at a time rather than all of a
/// scope's.
///
/// # Contract
///
/// Rows must arrive in key order: each map-layout row is a whole queue, and
/// one generation's row-layout rows are contiguous and already in enqueue
/// order, so a row-layout queue completes when the next generation's first
/// row arrives or at [`Self::finish`]. A row of the other layout fails
/// closed with [`HelixDbError::Config`], since the publisher and search
/// overlays would never see its operations; any other key, a queue that
/// does not decode, and one generation's rows of two families are
/// [`HelixDbError::IndexCatalogCorruption`] or encoding errors.
///
/// ```text
/// map  rows: [q(1,1)] [q(2,1)]              -> push: q(1,1), q(2,1); finish: -
/// rows rows: [r(1,1,0)] [r(1,1,1)] [r(2,1,2)] -> push: -, -, q(1,1); finish: q(2,1)
/// ```
#[derive(Debug)]
pub(crate) struct QueueDiscovery<'a> {
    store: &'a QueueStore,
    /// The row-layout generation whose rows are still arriving.
    pending: Option<DiscoveredQueue>,
}

impl QueueDiscovery<'_> {
    /// Accepts the next row of a discovery range and returns the queue it
    /// completes, if any.
    pub(crate) fn push(&mut self, key: &[u8], value: &[u8]) -> Result<Option<DiscoveredQueue>> {
        let Ok(ManagedIndexKey::Data { scope, kind }) = ManagedIndexKey::parse_data_from_slice(key)
        else {
            return Err(HelixDbError::IndexCatalogCorruption(
                "operation queue prefix holds another key".to_string(),
            ));
        };
        match (self.store.layout, kind) {
            (QueueLayout::Map, ScopedKey::IndexOperationQueue(key)) => {
                let (family, frames) = OperationFrame::decode_queue(value)?;
                Ok(Some(DiscoveredQueue {
                    target: QueueTarget::new(scope, key.index_id, key.generation),
                    family,
                    frames,
                }))
            }
            (QueueLayout::Rows, ScopedKey::IndexOperationRow(key)) => {
                self.store
                    .next_sequence
                    .fetch_max(key.sequence.saturating_add(1), Ordering::Relaxed);
                let target = QueueTarget::new(scope, key.index_id, key.generation);
                let (family, frame) = OperationFrame::decode_row(value)?;
                let Some(pending) = self
                    .pending
                    .as_mut()
                    .filter(|pending| pending.target == target)
                else {
                    return Ok(self.pending.replace(DiscoveredQueue {
                        target,
                        family,
                        frames: vec![frame],
                    }));
                };
                if pending.family != family {
                    return Err(HelixDbError::IndexCatalogCorruption(format!(
                        "operation rows for index {} mix families",
                        target.index_id.get()
                    )));
                }
                pending.frames.push(frame);
                Ok(None)
            }
            (layout, ScopedKey::IndexOperationQueue(_) | ScopedKey::IndexOperationRow(_)) => {
                Err(HelixDbError::Config(format!(
                    "index operation queues were written with a layout other than {layout:?}"
                )))
            }
            (_, _) => Err(HelixDbError::IndexCatalogCorruption(
                "operation queue prefix holds another key".to_string(),
            )),
        }
    }

    /// Returns the last queue once every row was pushed.
    pub(crate) fn finish(self) -> Option<DiscoveredQueue> {
        self.pending
    }
}

/// Fails closed when `scope` holds queues written with a layout other than
/// `layout`.
///
/// Search overlays read one layout only, so the other layout's operations
/// would otherwise be silently invisible to a reader. Writers check the
/// layout while discovering queues instead.
#[cfg(any(test, feature = "async-index-benchmark"))]
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
