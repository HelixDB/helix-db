//! Writer-wide shared views of generation queues for read-request searches.
//!
//! # Why
//!
//! A search overlays the pending operations its request snapshot sees.
//! Reading them from storage makes SlateDB resolve the whole merge-backed
//! queue value inside the read, up to the per-index `max_retained_bytes` of
//! records, which the search then decodes again. Every search paid that
//! separately, so concurrent searches of one backlog each held their own
//! copies of it: about 2 GB apiece at the default limit.
//!
//! # Design
//!
//! The writer keeps one [`View`] per searched queue target: every operation
//! visible since the view's base read, each with the sequence of the commit
//! that enqueued it and, once acknowledged, of the commit that acknowledged
//! it. Every queue commit stages its changes in [`QueueChanges`], registers
//! them as in flight before it commits ([`SharedQueues::begin_commit`]), and
//! records them at its commit sequence the moment the commit applies
//! ([`CommittingQueue::applied`]). A search at snapshot `S`:
//!
//! 1. waits for the queue commits on its target that were in flight when it
//!    looked, since any of them may have applied at or below `S`; a commit
//!    registered later starts after the snapshot, so it applies above `S`;
//! 2. selects the operations enqueued at or below `S` and not acknowledged at
//!    or below `S` ([`SharedRead::select`]).
//!
//! That is exactly the queue storage resolves at `S`, so searches at
//! different snapshots share one copy and still see exactly their own
//! pending operations. Selections clone the shared payloads (`Arc`s), never
//! copy them.
//!
//! ```text
//! base read at 10: a1 b1     enqueue a2 at 12     ack a1 at 14
//! slots: a1 [10, 14)  b1 [10, -)  a2 [12, -)
//! search at 11 -> a1 b1   at 13 -> a2 b1 (a at its latest)   at 14 -> b1 a2
//! ```
//!
//! # Lifecycle of a target's view
//!
//! ```text
//! Absent ──search──► Building ──base read──► Ready ──uncertain commit──► Unsettled
//!    ▲                  │                      │                            │
//!    ├──cancel, error───┘                      ├──inconsistent record──►────┤
//!    ├──drained, evicted, retired, forgotten───┘                            │
//!    └────────────────────────── publisher reconciled ◄─────────────────────┘
//! ```
//!
//! The first search of an `Absent` target builds it: it logs every commit
//! recorded from then on, reads the queue at a base snapshot, and replays the
//! log above the base. The base is the search's own snapshot when no commit
//! on the target was recorded above it before logging began, and otherwise a
//! fresh snapshot, which every commit recorded before logging began precedes.
//! The build decodes and installs the base on the blocking pool, and installs
//! it even when the search that started it is gone, so a search cancelled
//! during a slow build hands it to the next search instead of leaving a
//! decode behind and another to start. A commit whose outcome is unknown
//! makes the view `Unsettled` until the publisher reconciles the target
//! ([`SharedQueues::settle`]): it may still apply at a sequence nobody
//! learns, so only a base read after reconciliation proved its fate can hold
//! it.
//!
//! A search reads storage itself, as before, whenever the view cannot serve
//! its snapshot: below the view's base or below what compaction dropped,
//! while the view is unsettled, and inside a write transaction, whose queue
//! read must stay a serializable read dependency. Readers have no views, so
//! all of their searches read storage. Those reads and every build's base
//! read are charged against one byte budget for what they resolve
//! ([`super::resolutions`]).
//!
//! A build serves the search that started it from the view it installs,
//! captured at that search's snapshot before anything can free it; when the
//! view cannot serve that snapshot (its base is a fresh snapshot above it),
//! or the build installs nothing, the search reads storage. So a search
//! builds at most once.
//!
//! # Memory
//!
//! A view holds exactly the operations the admission ledger charges to its
//! target, plus acknowledged ones an open request snapshot or a commit in
//! flight on the target can still see, so the per-index retained-byte limit
//! bounds it once, however many searches read it. Open writer request
//! snapshots pin what compaction may drop ([`SnapshotPin`]). Compaction
//! drops the acknowledged operations at or below the floor once they reach
//! half the live operations by count or by charged bytes, or once nothing
//! outstanding is left, so a view holds at most about one and a half times
//! its live operations while snapshots keep up. It runs whenever a commit on
//! the target resolves or the floor rises because the oldest pin closed, so
//! memory follows the ledger even when the queue then stays idle. A request
//! that keeps its snapshot open pins every acknowledgement after it until it
//! ends.
//!
//! Views are freed when their queue drains and no pin can see what it held,
//! when the publisher finds their generation retired ([`SharedQueues::retire`],
//! after which no build of the generation installs), and, least recently
//! searched first, whenever the views together exceed the writer's view
//! budget: the same ceiling as the queues publication retains between
//! attempts, one logical index's retained-byte limit per publication task.
//! Each view is charged [`VIEW_OVERHEAD_BYTES`] beside its operations, so
//! many small views are bounded too.
//!
//! A search leases the view it reads ([`ViewLease`]) from its plan until it
//! no longer holds anything it captured or selected from it: its request
//! keeps the selected operations' payloads, which the view shares, until it
//! ends. A leased view is never evicted, and stays the view its target's
//! searches share, so no target ever has a second copy of its operations
//! decoded while a search holds the first, and what searches hold of views
//! stays charged to the budget. Eviction never frees the view whose growth
//! (a commit), install (a build), or last lease ending started it, so the
//! views exceed the budget only by leased views (at most one per target)
//! and by that one view; the last lease of a view ending evicts the others
//! down to the budget again. A view that drains or retires
//! while leased is freed anyway: a pinned snapshot sees none of a drained
//! view's operations, so its searches selected none, and no view of a
//! retired generation is built again. A view reset while leased (an
//! uncertain commit, or an inconsistent record) stays charged, and its
//! target's searches read storage instead of building, until its last lease
//! ends. A target that holds no view, no commit in flight, no reset view
//! still leased, and nothing unknown about its commits is not tracked at all.
//!
//! Freeing a view and compaction's rebuild of what it keeps run after the
//! shared lock is released; only bookkeeping proportional to what changed
//! (map entries of dropped operations) runs under it. When they free or
//! merge more than [`INLINE_AFTER_BYTES`], they run on the blocking pool, so
//! a commit or a closing request never frees a large view on an async
//! worker.
//!
//! # Deleting a queue key
//!
//! Recording relies on every write to a queue key going through
//! [`QueueChanges`]. A transaction that deletes a queue key instead of
//! merging a removal (an acknowledgement that drains its queue) records the
//! acknowledgement of every operation outstanding in the queue it deletes,
//! which is what [`super::storage::QueueStore::stage_acknowledge`] records
//! whatever write it stages: such a delete is staged only when its
//! acknowledgement names every operation outstanding in the queue it read,
//! the ledger charges the target nothing else, and any enqueue committed
//! since the transaction began aborts it, so the delete removes exactly the
//! operations it acknowledges, and an enqueue that commits after it lands
//! above it and is recorded at its own, higher sequence.
//!
//! A delete that tombstones already-empty queues while a writer opens
//! ([`super::recovery::LoadedBacklog::tombstone_drained`]) needs no record
//! for two reasons. It changes no snapshot's queue: the key resolves empty
//! when its transaction begins, nothing commits to it before the delete
//! (that would abort it), and absence is the empty queue. And it commits
//! before the writer returns from open, so before any search plans a read
//! and any view exists ([`SharedQueues::searched`], which the open asserts).
//!
//! Any other write that deletes or rewrites a queue key must stage the
//! equivalent changes.
//!
//! # What does not scale
//!
//! - Each search walks every operation of its target's view at its snapshot:
//!   O(backlog) CPU but no copies (a strong search over a backlog of 148,000
//!   1536-dimensional operations took 0.6 to 0.9 s at the median, end to
//!   end). A build reads and decodes the queue once.
//! - The first searches of a target whose queue is large while writes
//!   continue may predate the build's fresh snapshot; they read storage,
//!   as do write transactions' and readers' searches, so a workload of many
//!   such concurrent searches on large backlogs queues up behind the
//!   resolution budget instead of holding a copy each.
//! - A search waits for the queue commits in flight on its target when it
//!   starts, normally their apply latency, but as long as SlateDB stalls
//!   writers.
//! - Every writer read request pins and unpins under the shared lock; an
//!   unpin that raises the floor checks only the views compaction waits on.
//!   Recording a commit costs its operations' map entries under the lock,
//!   and compaction removes the dropped ones there, about once per 1,024
//!   acknowledged operations or 4 MiB of their charged bytes, or once they
//!   reach half the live operations.
//! - Evicting a view to stay within the budget scans the tracked targets for
//!   the least recently searched view no search leases. Searches that keep
//!   leasing every view hold the views above the budget for as long as they
//!   run; each holds what it selected from them anyway.
//! - A target tracked anew assumes the latest commit of every untracked
//!   target, so while other queues take writes, the first search of a target
//!   after its view was freed usually builds from a fresh snapshot, and reads
//!   storage itself if its own snapshot predates that.
//! - Every generation retired while the writer runs stays recorded (a set
//!   entry each), so no search builds a view of it again.

use std::collections::hash_map::Entry;
use std::collections::{btree_map, hash_map, BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::encoding::v2::keys::IndexEntity;
use crate::encoding::v2::values::indexes::operation_queue::{
    LatestOperations, QueueFamily, QueuedOperation, QueuedOperationId,
};

use super::backlog::{charged_bytes, OPERATION_OVERHEAD_BYTES};
use super::QueueTarget;

/// Acknowledgement sequence of a slot no recorded commit acknowledged.
const OUTSTANDING: u64 = u64::MAX;

/// Fewest droppable acknowledged slots that start a compaction by count, so
/// a small view is not rebuilt on every acknowledgement. Tests compact
/// eagerly.
const MIN_COMPACTION_SLOTS: usize = if cfg!(test) { 1 } else { 1_024 };

/// Fewest droppable charged bytes that start a compaction by bytes, so a few
/// large acknowledged operations are dropped without waiting for 1,024 of
/// them.
const MIN_COMPACTION_BYTES: u64 = if cfg!(test) { 1 } else { 4 << 20 };

/// Most chunks at or below the floor before compaction merges them, bounding
/// what a search captures while acknowledgements stall.
const MAX_CHUNKS: usize = if cfg!(test) { 4 } else { 1_024 };

/// Bytes each view is charged against the writer's view budget beside its
/// operations: more than its bookkeeping and its target's entry take, so
/// the budget bounds how many views exist as well as what they hold.
pub(crate) const VIEW_OVERHEAD_BYTES: u64 = 1_024;

/// Most charged bytes of operations that work left for after the shared
/// lock frees or merges on the thread that released it; more runs on the
/// blocking pool.
const INLINE_AFTER_BYTES: u64 = 1 << 20;

/// One transaction's queue changes, staged with its queue writes.
///
/// [`super::storage::QueueStore::stage_enqueue`] and
/// [`super::storage::QueueStore::stage_acknowledge`] take it, so no queue
/// write can be staged without being recorded once it commits.
#[derive(Debug, Default)]
pub(crate) struct QueueChanges {
    changes: Vec<(QueueTarget, Change)>,
}

/// One staged change to one target's queue.
#[derive(Debug)]
enum Change {
    /// Operations enqueued, in staging order.
    Enqueued(Vec<QueuedOperation>),
    /// Exact operation IDs acknowledged.
    Acknowledged(Vec<QueuedOperationId>),
}

impl QueueChanges {
    /// Stages `operations` enqueued to `target`, after any staged before.
    pub(crate) fn enqueue(&mut self, target: QueueTarget, operations: &[QueuedOperation]) {
        match self
            .changes
            .iter_mut()
            .find(|(staged, _)| *staged == target)
        {
            Some((_, Change::Enqueued(staged))) => staged.extend_from_slice(operations),
            Some((_, Change::Acknowledged(_))) | None => self
                .changes
                .push((target, Change::Enqueued(operations.to_vec()))),
        }
    }

    /// Stages the acknowledgement of `ids` in `target`.
    pub(crate) fn acknowledge(&mut self, target: QueueTarget, ids: &[QueuedOperationId]) {
        match self
            .changes
            .iter_mut()
            .find(|(staged, _)| *staged == target)
        {
            Some((_, Change::Acknowledged(staged))) => staged.extend_from_slice(ids),
            Some((_, Change::Enqueued(_))) | None => self
                .changes
                .push((target, Change::Acknowledged(ids.to_vec()))),
        }
    }
}

/// One operation in a view, shared by every chunk and capture holding it.
#[derive(Debug)]
struct Slot {
    operation: QueuedOperation,
    /// Sequence of the commit that acknowledged it, or [`OUTSTANDING`].
    ///
    /// Set under the shared lock; a search reads it without the lock once
    /// every acknowledgement at or below its snapshot was recorded, so a
    /// store it races with is above its snapshot and either value reads as
    /// visible. Compaction reads it off the lock only at or below its floor,
    /// which every later store is above.
    acknowledged: AtomicU64,
}

/// Operations one commit enqueued, or a compaction merged, in storage order.
///
/// Keyed in its view by the commit sequence of every slot: a merged chunk's
/// latest, which every snapshot the view serves is at or above.
#[derive(Debug)]
struct Chunk {
    slots: Box<[Arc<Slot>]>,
}

/// Acknowledged slots of one acknowledgement sequence.
#[derive(Debug, Default)]
struct Acknowledged {
    ids: Vec<QueuedOperationId>,
    /// [`charged_bytes`] of those slots.
    bytes: u64,
}

/// One target's operations, as described in the module documentation.
#[derive(Debug)]
struct View {
    /// The build that made it, so a compaction that outlived it changes no
    /// later view of the target.
    build: u64,
    family: Option<QueueFamily>,
    /// Snapshot the base was read at; later commits at or below it are in
    /// the base already.
    base: u64,
    /// Smallest snapshot the view serves: the base, raised by compaction.
    served_from: u64,
    /// Chunks keyed by their sequence, which no two share.
    chunks: BTreeMap<u64, Arc<Chunk>>,
    /// Every slot by operation ID.
    slots: HashMap<QueuedOperationId, Arc<Slot>>,
    /// Acknowledged slots by acknowledgement sequence, for compaction.
    acknowledged: BTreeMap<u64, Acknowledged>,
    acknowledged_slots: usize,
    acknowledged_bytes: u64,
    /// Acknowledgements recorded before their operation's enqueue, by
    /// operation: the publisher read and acknowledged the operation after its
    /// commit applied, before the producer recorded that commit. Each is
    /// applied when the enqueue is recorded; one left once no queue commit on
    /// the target is in flight makes the view inconsistent.
    early: HashMap<QueuedOperationId, u64>,
    /// [`charged_bytes`] of every slot held.
    charged: u64,
    /// A compaction holds its chunks at or below its floor off the lock.
    compacting: bool,
    /// When a search last read it, for eviction.
    used: u64,
    /// Live [`ViewLease`]s on it; it is not evicted while any are.
    leases: usize,
}

/// Why a record left a view unusable.
#[derive(Debug)]
struct Inconsistent;

/// What compaction can do for a view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Due {
    /// Nothing at any floor, or a compaction already runs.
    Nothing,
    /// Enough once the floor rises.
    Blocked,
    /// Enough at the floor now.
    Now,
}

impl View {
    fn new(base: u64, queue: Option<(QueueFamily, Vec<QueuedOperation>)>) -> Self {
        let mut view = Self {
            build: 0,
            family: queue.as_ref().map(|(family, _)| *family),
            base,
            served_from: base,
            chunks: BTreeMap::new(),
            slots: HashMap::new(),
            acknowledged: BTreeMap::new(),
            acknowledged_slots: 0,
            acknowledged_bytes: 0,
            early: HashMap::new(),
            charged: 0,
            compacting: false,
            used: 0,
            leases: 0,
        };
        if let Some((_, operations)) = queue {
            view.insert_chunk(base, operations);
        }
        view
    }

    fn insert_chunk(&mut self, sequence: u64, operations: Vec<QueuedOperation>) {
        if operations.is_empty() {
            return;
        }
        let slots = operations
            .into_iter()
            .map(|operation| {
                self.charged += charged_bytes(operation.retained_bytes());
                let slot = Arc::new(Slot {
                    operation,
                    acknowledged: AtomicU64::new(OUTSTANDING),
                });
                self.slots.insert(slot.operation.id(), Arc::clone(&slot));
                slot
            })
            .collect();
        let previous = self.chunks.insert(sequence, Arc::new(Chunk { slots }));
        debug_assert!(previous.is_none(), "no two chunks share a sequence");
    }

    /// Applies one commit's change at `sequence`; a change at or below the
    /// base is in the base already.
    fn apply(&mut self, sequence: u64, change: Change) -> Result<(), Inconsistent> {
        if sequence <= self.base {
            return Ok(());
        }
        match change {
            Change::Enqueued(operations) => {
                let families_agree = operations.iter().all(|operation| {
                    *self.family.get_or_insert(operation.payload().family())
                        == operation.payload().family()
                });
                let fresh = operations
                    .iter()
                    .all(|operation| !self.slots.contains_key(&operation.id()));
                if !families_agree || !fresh || self.chunks.contains_key(&sequence) {
                    return Err(Inconsistent);
                }
                let ids = operations
                    .iter()
                    .map(QueuedOperation::id)
                    .collect::<Vec<_>>();
                self.insert_chunk(sequence, operations);
                for id in ids {
                    let Some(acknowledged) = self.early.remove(&id) else {
                        continue;
                    };
                    if acknowledged <= sequence {
                        return Err(Inconsistent);
                    }
                    self.acknowledge(id, acknowledged)?;
                }
            }
            Change::Acknowledged(ids) => {
                for id in ids {
                    if self.slots.contains_key(&id) {
                        self.acknowledge(id, sequence)?;
                    } else if self.early.insert(id, sequence).is_some() {
                        return Err(Inconsistent);
                    }
                }
            }
        }
        Ok(())
    }

    /// Marks the held operation `id` acknowledged at `sequence`.
    fn acknowledge(&mut self, id: QueuedOperationId, sequence: u64) -> Result<(), Inconsistent> {
        let slot = &self.slots[&id];
        slot.acknowledged
            .compare_exchange(OUTSTANDING, sequence, Ordering::Release, Ordering::Relaxed)
            .map_err(|_| Inconsistent)?;
        let bytes = charged_bytes(slot.operation.retained_bytes());
        let acknowledged = self.acknowledged.entry(sequence).or_default();
        acknowledged.ids.push(id);
        acknowledged.bytes += bytes;
        self.acknowledged_slots += 1;
        self.acknowledged_bytes += bytes;
        Ok(())
    }

    /// Fails when an early acknowledgement is left although no queue commit
    /// on the target is in flight to enqueue its operation.
    fn settled(&self, in_flight: bool) -> Result<(), Inconsistent> {
        if self.early.is_empty() || in_flight {
            return Ok(());
        }
        Err(Inconsistent)
    }

    /// Whether every operation the view holds is acknowledged at or below
    /// `floor`: dropping them leaves nothing, so the view is freed instead.
    /// An early acknowledgement still waiting for its enqueue is dropped
    /// with it; the enqueue then finds no view, and the next build reads it.
    fn drains(&self, floor: u64) -> bool {
        self.slots.len() == self.acknowledged_slots
            && self
                .acknowledged
                .last_key_value()
                .is_some_and(|(sequence, _)| *sequence <= floor)
    }

    /// Whether compacting at `floor` drops enough: acknowledged slots at or
    /// below it reaching half the live slots by count or by charged bytes
    /// (and at least the minimums), every slot, or [`MAX_CHUNKS`] chunks to
    /// merge.
    ///
    /// `floor` must be at or below every open pinned snapshot and below the
    /// sequence of every queue commit on the target still in flight, so
    /// nothing later is recorded at or below it.
    fn due(&self, floor: u64) -> Due {
        if self.compacting {
            return Due::Nothing;
        }
        let live = self.slots.len() - self.acknowledged_slots;
        let live_bytes = self.charged - self.acknowledged_bytes;
        let enough = |slots: usize, bytes: u64| {
            slots >= MIN_COMPACTION_SLOTS.max(live / 2)
                || bytes >= MIN_COMPACTION_BYTES.max(live_bytes / 2)
        };
        let possible = (self.acknowledged_slots > 0
            && (live == 0 || enough(self.acknowledged_slots, self.acknowledged_bytes)))
            || self.chunks.len() >= MAX_CHUNKS;
        if !possible {
            return Due::Nothing;
        }
        let (slots, bytes) =
            self.acknowledged
                .range(..=floor)
                .fold((0, 0), |(slots, bytes), (_, acknowledged)| {
                    (slots + acknowledged.ids.len(), bytes + acknowledged.bytes)
                });
        let now = (slots > 0 && enough(slots, bytes))
            || self.drains(floor)
            || self.chunks.range(..=floor).count() >= MAX_CHUNKS;
        if now {
            Due::Now
        } else {
            Due::Blocked
        }
    }

    /// Every chunk, in storage order, for a search at `snapshot` of
    /// `target`, leasing the view until the search lets go of it.
    fn capture(
        &mut self,
        shared: &Arc<SharedQueues>,
        target: QueueTarget,
        snapshot: u64,
    ) -> SharedRead {
        self.leases += 1;
        SharedRead {
            family: self.family,
            chunks: self
                .chunks
                .range(..=snapshot)
                .map(|(_, chunk)| Arc::clone(chunk))
                .collect(),
            snapshot,
            lease: ViewLease {
                _lease: Arc::new(Lease {
                    shared: Arc::clone(shared),
                    target,
                    build: self.build,
                }),
            },
        }
    }
}

/// A view's state; see the module documentation.
#[derive(Debug)]
enum ViewState {
    Absent,
    /// A search is reading the base; commits recorded meanwhile are logged.
    Building {
        build: u64,
        /// Known once the builder chose its snapshot.
        base: Option<u64>,
        log: Vec<(u64, Change)>,
    },
    Ready(View),
    /// A commit's outcome is unknown; searches read storage until the
    /// publisher reconciles the target.
    Unsettled,
}

impl ViewState {
    /// What the view is charged against the writer's view budget.
    fn weight(&self) -> u64 {
        match self {
            Self::Ready(view) => view.charged + VIEW_OVERHEAD_BYTES,
            Self::Absent | Self::Building { .. } | Self::Unsettled => 0,
        }
    }
}

/// Highest sequence a target's recorded commits applied at, as far as a
/// builder's choice of base is concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Recorded {
    /// Every commit recorded on the target applied at or below this.
    Through(u64),
    /// A commit's sequence is unknown (uncertain, or a view was found
    /// inconsistent), so only a fresh snapshot is a safe base.
    Unknown,
}

#[derive(Debug)]
struct TargetState {
    /// Queue commits begun and not yet resolved: ticket -> the global
    /// recorded sequence when it began, below its own sequence.
    in_flight: BTreeMap<u64, u64>,
    recorded: Recorded,
    view: ViewState,
    /// [`State::unsettlements`] when the view last became unsettled, so a
    /// settlement read before an unsettling commit never ends it.
    unsettled_at: u64,
    /// A view reset while leased, until its last lease ends: the target
    /// builds no view meanwhile, so `view` is never ready or building.
    orphan: Option<Orphan>,
}

/// A view reset while searches leased it.
#[derive(Debug)]
struct Orphan {
    build: u64,
    leases: usize,
    /// Its [`ViewState::weight`], still in [`State::held`].
    weight: u64,
}

/// Work a change under the shared lock leaves for after it: compactions to
/// finish and memory to free.
#[derive(Debug, Default)]
#[must_use = "run it once the lock is released"]
struct After {
    compactions: Vec<Compaction>,
    views: Vec<ViewState>,
    chunks: Vec<BTreeMap<u64, Arc<Chunk>>>,
    slots: Vec<Arc<Slot>>,
    changes: Vec<Change>,
}

impl After {
    /// About how many bytes running it frees or merges: what the views it
    /// drops are charged, and for each compaction, what the operations it
    /// drops are charged plus an operation's overhead per slot it merges.
    ///
    /// Only views and compactions are left when it is handed off: the chunks
    /// and slots a compaction drops are added while it finishes. A commit's
    /// own changes are not counted: they are as large as the commit, whose
    /// producer frees its copies on its own thread anyway.
    fn weight(&self) -> u64 {
        let views = self.views.iter().map(ViewState::weight);
        let compactions = self.compactions.iter().map(|compaction| {
            let merged: usize = compaction.below.iter().map(|chunk| chunk.slots.len()).sum();
            (merged as u64)
                .saturating_mul(OPERATION_OVERHEAD_BYTES)
                .saturating_add(compaction.dropping)
        });
        views.chain(compactions).fold(0, u64::saturating_add)
    }
}

/// One compaction of a view at `floor`, its merge built off the lock.
#[derive(Debug)]
struct Compaction {
    target: QueueTarget,
    build: u64,
    floor: u64,
    /// The view's chunks at or below `floor` when it started.
    below: Vec<Arc<Chunk>>,
    /// Charged bytes of the acknowledged operations it drops.
    dropping: u64,
}

#[derive(Debug, Default)]
struct State {
    /// Highest sequence any recorded commit applied at.
    recorded: u64,
    /// Open pinned snapshots: the recorded sequence each pinned at -> count.
    pins: BTreeMap<u64, usize>,
    next_ticket: u64,
    next_build: u64,
    /// Views made unsettled so far, writer-wide.
    unsettlements: u64,
    targets: HashMap<QueueTarget, TargetState>,
    /// Highest sequence the commits of any target no longer tracked were
    /// recorded at: a target tracked anew starts there, so a build never
    /// takes a base below a commit recorded before it was untracked.
    untracked: u64,
    /// [`ViewState::weight`] of every view and [`Orphan::weight`] of every
    /// orphan.
    held: u64,
    /// Ready views compaction waits on a higher floor for.
    blocked: HashSet<QueueTarget>,
    /// Bumped by every search a view serves.
    clock: u64,
    /// Searches that read storage because no view could serve them.
    fallbacks: u64,
    /// Generations the publisher found retired: searches of them read
    /// storage, and no build of them installs.
    retired: HashSet<QueueTarget>,
}

impl State {
    /// Sequence below which nothing visible to an open pinned snapshot or
    /// still in flight on `target` can change.
    fn floor(&self, target: QueueTarget) -> u64 {
        let pinned = self.pins.keys().next().copied().unwrap_or(self.recorded);
        self.targets.get(&target).map_or(pinned, |tracked| {
            tracked.in_flight.values().copied().fold(pinned, u64::min)
        })
    }

    /// `target`'s state, tracked anew when it is not.
    fn track(&mut self, target: QueueTarget) -> &mut TargetState {
        let untracked = self.untracked;
        self.targets.entry(target).or_insert_with(|| TargetState {
            in_flight: BTreeMap::new(),
            recorded: Recorded::Through(untracked),
            view: ViewState::Absent,
            unsettled_at: 0,
            orphan: None,
        })
    }

    /// Replaces `target`'s view, keeping [`Self::held`], and returns the old.
    fn replace(&mut self, target: QueueTarget, view: ViewState) -> ViewState {
        let tracked = self
            .targets
            .get_mut(&target)
            .expect("a replaced view is tracked");
        let old = std::mem::replace(&mut tracked.view, view);
        self.held = self.held - old.weight() + tracked.view.weight();
        if !matches!(tracked.view, ViewState::Ready(_)) {
            self.blocked.remove(&target);
        }
        old
    }

    /// Makes `target`'s view unsettled; its old view is freed after the lock,
    /// and stays charged as an [`Orphan`] while leased.
    fn reset(&mut self, target: QueueTarget, after: &mut After) {
        let old = self.replace(target, ViewState::Unsettled);
        self.unsettlements += 1;
        let tracked = self
            .targets
            .get_mut(&target)
            .expect("a reset target is tracked");
        tracked.recorded = Recorded::Unknown;
        tracked.unsettled_at = self.unsettlements;
        match &old {
            ViewState::Ready(view) if view.leases > 0 => {
                assert!(
                    tracked.orphan.is_none(),
                    "an orphan's target builds no view"
                );
                let weight = old.weight();
                tracked.orphan = Some(Orphan {
                    build: view.build,
                    leases: view.leases,
                    weight,
                });
                self.held += weight;
            }
            ViewState::Ready(_)
            | ViewState::Absent
            | ViewState::Building { .. }
            | ViewState::Unsettled => {}
        }
        after.views.push(old);
    }

    /// Stops tracking `target` when it holds nothing: no view, no commit in
    /// flight, no orphan, and no unknown commit sequence.
    fn prune(&mut self, target: QueueTarget) {
        let hash_map::Entry::Occupied(entry) = self.targets.entry(target) else {
            return;
        };
        let tracked = entry.get();
        let (ViewState::Absent, true, None, Recorded::Through(through)) = (
            &tracked.view,
            tracked.in_flight.is_empty(),
            &tracked.orphan,
            tracked.recorded,
        ) else {
            return;
        };
        self.untracked = self.untracked.max(through);
        entry.remove();
    }

    /// Starts what `target`'s view is due at its floor: frees a drained view,
    /// or starts a compaction finished after the lock.
    fn maintain(&mut self, target: QueueTarget, after: &mut After) {
        let floor = self.floor(target);
        let Some(TargetState {
            view: ViewState::Ready(view),
            ..
        }) = self.targets.get_mut(&target)
        else {
            self.blocked.remove(&target);
            return;
        };
        match view.due(floor) {
            Due::Nothing => {
                self.blocked.remove(&target);
            }
            Due::Blocked => {
                self.blocked.insert(target);
            }
            Due::Now if view.drains(floor) => {
                after.views.push(self.replace(target, ViewState::Absent));
                self.prune(target);
            }
            Due::Now => {
                view.compacting = true;
                after.compactions.push(Compaction {
                    target,
                    build: view.build,
                    floor,
                    below: view
                        .chunks
                        .range(..=floor)
                        .map(|(_, chunk)| Arc::clone(chunk))
                        .collect(),
                    dropping: view
                        .acknowledged
                        .range(..=floor)
                        .map(|(_, acknowledged)| acknowledged.bytes)
                        .sum(),
                });
                self.blocked.remove(&target);
            }
        }
    }

    /// Ends the compaction of `target`'s view of `build` at `floor` with
    /// `merged`, the slots it kept, in storage order.
    ///
    /// Nothing at or below its floor changed since it started: no commit is
    /// recorded there, and the view admits one compaction at a time. A view
    /// replaced meanwhile is left alone.
    fn finish(
        &mut self,
        target: QueueTarget,
        build: u64,
        floor: u64,
        merged: Box<[Arc<Slot>]>,
        after: &mut After,
    ) {
        let Some(TargetState {
            view: ViewState::Ready(view),
            ..
        }) = self.targets.get_mut(&target)
        else {
            after.slots.extend(merged);
            return;
        };
        if view.build != build {
            after.slots.extend(merged);
            return;
        }
        assert!(view.compacting, "a compaction finishes once");
        view.compacting = false;
        let above = view.chunks.split_off(&floor.saturating_add(1));
        let below = std::mem::replace(&mut view.chunks, above);
        if let Some((&sequence, _)) = below.last_key_value()
            && !merged.is_empty()
        {
            view.chunks
                .insert(sequence, Arc::new(Chunk { slots: merged }));
        }
        after.chunks.push(below);
        let kept = view.acknowledged.split_off(&floor.saturating_add(1));
        for (_, acknowledged) in std::mem::replace(&mut view.acknowledged, kept) {
            view.acknowledged_slots -= acknowledged.ids.len();
            view.acknowledged_bytes -= acknowledged.bytes;
            view.charged -= acknowledged.bytes;
            self.held -= acknowledged.bytes;
            after.slots.extend(
                acknowledged
                    .ids
                    .iter()
                    .map(|id| view.slots.remove(id).expect("an acknowledged slot is held")),
            );
        }
        view.served_from = view.served_from.max(floor);
        self.maintain(target, after);
    }

    /// Frees the least recently searched views that no search leases, other
    /// than `keep`'s, while the views exceed `budget`.
    fn evict(&mut self, budget: u64, keep: Option<QueueTarget>, after: &mut After) {
        while self.held > budget {
            let Some((_, idle)) = self
                .targets
                .iter()
                .filter_map(|(target, tracked)| match &tracked.view {
                    ViewState::Ready(view) if view.leases == 0 && Some(*target) != keep => {
                        Some((view.used, *target))
                    }
                    ViewState::Ready(_)
                    | ViewState::Absent
                    | ViewState::Building { .. }
                    | ViewState::Unsettled => None,
                })
                .min()
            else {
                return;
            };
            after.views.push(self.replace(idle, ViewState::Absent));
            self.prune(idle);
        }
    }
}

/// Every view of one writer, the in-flight queue commits, and the pinned
/// snapshots; see the module documentation.
#[derive(Debug)]
pub(crate) struct SharedQueues {
    state: Mutex<State>,
    /// Bumped whenever a commit resolves or a view changes state.
    changes: tokio::sync::watch::Sender<u64>,
    /// Most bytes the views may hold together before idle ones are evicted.
    budget: u64,
    /// Blocks a build's decode until a test releases it: the build reports
    /// on the first channel, then waits on the second.
    #[cfg(test)]
    decode_gate: DecodeGate,
    /// The thread and weight of every run of work that freed a view or
    /// compacted one.
    #[cfg(test)]
    ran_after: Mutex<Vec<(std::thread::ThreadId, u64)>>,
}

#[cfg(test)]
type DecodeGate = Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>;

/// Keeps compaction from dropping anything an open request snapshot taken
/// after it was created can see.
///
/// Created before the snapshot is opened, it pins the highest sequence any
/// commit was recorded at by then, which the snapshot is at or above.
/// Dropping the oldest pin raises the floor, so the views compaction waited
/// on compact then.
#[derive(Debug)]
pub(crate) struct SnapshotPin {
    shared: Arc<SharedQueues>,
    pinned: u64,
}

impl Drop for SnapshotPin {
    fn drop(&mut self) {
        let mut after = After::default();
        let mut state = self.shared.state.lock();
        let oldest = state.pins.keys().next().copied();
        let btree_map::Entry::Occupied(mut count) = state.pins.entry(self.pinned) else {
            unreachable!("a pin is counted until dropped");
        };
        *count.get_mut() -= 1;
        if *count.get() == 0 {
            count.remove();
            if oldest == Some(self.pinned) {
                let blocked = state.blocked.iter().copied().collect::<Vec<_>>();
                for target in blocked {
                    state.maintain(target, &mut after);
                }
            }
        }
        drop(state);
        self.shared.run(after);
    }
}

/// How many views were made unsettled, writer-wide, when it was read; see
/// [`SharedQueues::settle`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Settlement(u64);

/// Commits `transaction` and records `queue` the moment the commit applies
/// (see [`record_applied`]), then waits until it is durable, so searches never
/// wait for a WAL upload to see an acknowledgement.
pub(crate) async fn commit_recorded(
    transaction: slatedb::DbTransaction,
    queue: CommittingQueue,
) -> Result<Option<slatedb::WriteHandle>, slatedb::Error> {
    let applied = transaction
        .commit_with_options(&crate::transaction::CommitDurability::Applied.write_options())
        .await;
    record_applied(queue, &applied);
    let Some(handle) = applied? else {
        return Ok(None);
    };
    handle.await_durable().await?;
    Ok(Some(handle))
}

/// Resolves `queue` from its commit's apply outcome: applied at the handle's
/// sequence, aborted by a conflict or an empty batch, and unknown otherwise.
pub(crate) fn record_applied(
    queue: CommittingQueue,
    applied: &Result<Option<slatedb::WriteHandle>, slatedb::Error>,
) {
    match applied {
        Ok(Some(handle)) => queue.applied(handle.seqnum()),
        Ok(None) => queue.aborted(),
        Err(error) if error.kind() == slatedb::ErrorKind::Transaction => queue.aborted(),
        Err(_) => queue.uncertain(),
    }
}

/// One transaction's queue changes registered as in flight; resolve it with
/// the commit's outcome. Dropping it unresolved counts as an unknown outcome.
#[derive(Debug)]
pub(crate) struct CommittingQueue {
    shared: Arc<SharedQueues>,
    ticket: u64,
    changes: Vec<(QueueTarget, Change)>,
    resolved: bool,
}

impl CommittingQueue {
    /// Records the changes at `sequence`, where the commit applied.
    pub(crate) fn applied(mut self, sequence: u64) {
        self.resolve(Some(sequence));
    }

    /// The commit definitely applied nothing.
    pub(crate) fn aborted(mut self) {
        self.resolve(None);
    }

    /// Whether the commit applied is unknown. Callers mark the ledger's
    /// charges uncertain first.
    pub(crate) fn uncertain(mut self) {
        self.unsettle();
    }

    fn unsettle(&mut self) {
        self.resolved = true;
        if self.changes.is_empty() {
            return;
        }
        let mut after = After::default();
        let mut state = self.shared.state.lock();
        for (target, change) in std::mem::take(&mut self.changes) {
            let tracked = state
                .targets
                .get_mut(&target)
                .expect("a begun target is tracked");
            tracked.in_flight.remove(&self.ticket);
            state.reset(target, &mut after);
            after.changes.push(change);
        }
        drop(state);
        self.shared.changes.send_modify(|version| *version += 1);
        self.shared.run(after);
    }

    fn resolve(&mut self, sequence: Option<u64>) {
        self.resolved = true;
        if self.changes.is_empty() {
            return;
        }
        let mut after = After::default();
        let mut guard = self.shared.state.lock();
        let state = &mut *guard;
        if let Some(sequence) = sequence {
            state.recorded = state.recorded.max(sequence);
        }
        for (target, change) in std::mem::take(&mut self.changes) {
            let tracked = state
                .targets
                .get_mut(&target)
                .expect("a begun target is tracked");
            tracked.in_flight.remove(&self.ticket);
            let Some(sequence) = sequence else {
                // Nothing applied, but the commit left the floor.
                after.changes.push(change);
                state.maintain(target, &mut after);
                state.prune(target);
                continue;
            };
            tracked.recorded = match tracked.recorded {
                Recorded::Through(through) => Recorded::Through(through.max(sequence)),
                Recorded::Unknown => Recorded::Unknown,
            };
            let in_flight = !tracked.in_flight.is_empty();
            let (grown, consistent) = match &mut tracked.view {
                ViewState::Absent | ViewState::Unsettled => {
                    after.changes.push(change);
                    (0, Ok(()))
                }
                ViewState::Building { log, .. } => {
                    log.push((sequence, change));
                    (0, Ok(()))
                }
                ViewState::Ready(view) => {
                    let before = view.charged;
                    let consistent = view
                        .apply(sequence, change)
                        .and_then(|()| view.settled(in_flight));
                    (view.charged - before, consistent)
                }
            };
            state.held += grown;
            if let Err(Inconsistent) = consistent {
                tracing::error!(
                    index_id = target.index_id.get(),
                    generation = target.generation.get(),
                    "a recorded queue change contradicts its shared view; searches read storage until it is rebuilt"
                );
                state.reset(target, &mut after);
                continue;
            }
            if grown > 0 {
                state.evict(self.shared.budget, Some(target), &mut after);
            }
            // The commit left the floor, so it may have risen.
            state.maintain(target, &mut after);
            state.prune(target);
        }
        drop(guard);
        self.shared.changes.send_modify(|version| *version += 1);
        self.shared.run(after);
    }
}

impl Drop for CommittingQueue {
    fn drop(&mut self) {
        if !self.resolved {
            self.unsettle();
        }
    }
}

/// A view's operations captured for one search at its snapshot.
#[derive(Debug, Clone)]
pub(crate) struct SharedRead {
    family: Option<QueueFamily>,
    chunks: Vec<Arc<Chunk>>,
    snapshot: u64,
    lease: ViewLease,
}

/// Keeps a view from eviction while a search holds what it captured or
/// selected from it; see the module documentation. Clones share one lease,
/// which ends when the last is dropped.
#[derive(Debug, Clone)]
pub(crate) struct ViewLease {
    _lease: Arc<Lease>,
}

/// One lease on `target`'s view of `build`.
#[derive(Debug)]
struct Lease {
    shared: Arc<SharedQueues>,
    target: QueueTarget,
    build: u64,
}

impl Drop for Lease {
    /// Ends the lease on the view or orphan it was taken on, if that still
    /// exists. The last lease of a view ending evicts the other views no
    /// search leases down to the budget, since they may have exceeded it
    /// only because the view was leased.
    fn drop(&mut self) {
        let mut after = After::default();
        let mut guard = self.shared.state.lock();
        let state = &mut *guard;
        let Some(tracked) = state.targets.get_mut(&self.target) else {
            // Its view drained or retired and the target was untracked.
            return;
        };
        let released = match (&mut tracked.view, &mut tracked.orphan) {
            (ViewState::Ready(view), _) if view.build == self.build => {
                view.leases -= 1;
                view.leases == 0
            }
            (_, Some(orphan)) if orphan.build == self.build => {
                orphan.leases -= 1;
                if orphan.leases == 0 {
                    state.held -= orphan.weight;
                    tracked.orphan = None;
                    state.prune(self.target);
                }
                false
            }
            // Its view drained or retired; another may be leased now.
            (
                ViewState::Ready(_)
                | ViewState::Absent
                | ViewState::Building { .. }
                | ViewState::Unsettled,
                _,
            ) => false,
        };
        if released && state.held > self.shared.budget {
            state.evict(self.shared.budget, Some(self.target), &mut after);
        }
        drop(guard);
        self.shared.run(after);
    }
}

impl SharedRead {
    /// The lease this read holds on its view, for whatever keeps payloads
    /// it selects beyond the read.
    pub(crate) fn lease(&self) -> ViewLease {
        self.lease.clone()
    }

    /// Selects each entity visible at the snapshot at its latest operation,
    /// in the order of its oldest, while their retained bytes fit `budget`,
    /// exactly as [`LatestOperations::decode`] selects from the value storage
    /// resolves at the snapshot; `None` when nothing is visible.
    ///
    /// The reach it reports when the budget leaves an entity out is exact,
    /// where a decode's may be a lower bound (see
    /// [`LatestOperations::refused`]).
    pub(crate) fn select(&self, budget: u64) -> Option<LatestOperations> {
        let visible = self
            .chunks
            .iter()
            .flat_map(|chunk| chunk.slots.iter())
            .filter(|slot| slot.acknowledged.load(Ordering::Acquire) > self.snapshot)
            .map(|slot| &slot.operation);
        let mut order = Vec::<&QueuedOperation>::new();
        let mut positions = HashMap::<IndexEntity, usize>::new();
        for operation in visible {
            match positions.entry(operation.entity()) {
                Entry::Occupied(position) => order[*position.get()] = operation,
                Entry::Vacant(position) => {
                    position.insert(order.len());
                    order.push(operation);
                }
            }
        }
        let family = self.family?;
        if order.is_empty() {
            return None;
        }
        let mut charged = 0_u64;
        let mut selected = Vec::with_capacity(order.len());
        for operation in order {
            let reached = charged.saturating_add(operation.retained_bytes());
            if reached > budget {
                return Some(LatestOperations::new(family, selected, Some(reached)));
            }
            charged = reached;
            selected.push(operation.clone());
        }
        Some(LatestOperations::new(family, selected, None))
    }
}

/// A view's base read and its slots, built off the shared lock (on the
/// blocking pool) before [`SharedQueues::install`].
#[derive(Debug)]
pub(crate) struct PreparedView(View);

impl PreparedView {
    /// Holds `queue`, the family and operations in storage order a storage
    /// read at snapshot `base` resolved, or `None` for the empty queue.
    pub(crate) fn new(base: u64, queue: Option<(QueueFamily, Vec<QueuedOperation>)>) -> Self {
        Self(View::new(base, queue))
    }
}

/// How a search reads its target's pending operations.
#[derive(Debug)]
pub(crate) enum Plan {
    /// Select from the shared view.
    Shared(SharedRead),
    /// Read storage at the search's own snapshot.
    Fallback,
    /// Build the view; see [`SharedQueues::install`].
    Build(Builder),
}

/// One search's exclusive right to build a target's view.
///
/// Dropping it before [`SharedQueues::install`] abandons the build: the view
/// becomes absent again and another search builds it.
#[derive(Debug)]
pub(crate) struct Builder {
    shared: Arc<SharedQueues>,
    target: QueueTarget,
    build: u64,
    /// The building search's snapshot, which the installed view serves when
    /// it can.
    snapshot: u64,
    /// The searching snapshot may serve as the base: no commit on the target
    /// was recorded above it before logging began.
    own_snapshot: bool,
    installed: bool,
}

impl Builder {
    /// Whether the base may be read at the building search's own snapshot;
    /// otherwise it must be a fresh snapshot taken after this call.
    pub(crate) const fn own_snapshot(&self) -> bool {
        self.own_snapshot
    }
}

impl Drop for Builder {
    fn drop(&mut self) {
        if self.installed {
            return;
        }
        let mut after = After::default();
        let mut state = self.shared.state.lock();
        if state.targets.get(&self.target).is_some_and(|tracked| {
            matches!(tracked.view, ViewState::Building { build, .. } if build == self.build)
        }) {
            after.views.push(state.replace(self.target, ViewState::Absent));
            state.prune(self.target);
        }
        drop(state);
        self.shared.changes.send_modify(|version| *version += 1);
        self.shared.run(after);
    }
}

impl SharedQueues {
    /// Views of one writer holding at most `budget` bytes together beside
    /// those in use (see the module documentation).
    pub(crate) fn new(budget: u64) -> Self {
        Self {
            state: Mutex::new(State::default()),
            changes: tokio::sync::watch::Sender::new(0),
            budget,
            #[cfg(test)]
            decode_gate: Mutex::new(None),
            #[cfg(test)]
            ran_after: Mutex::new(Vec::new()),
        }
    }

    /// The thread and weight of every run of work that freed a view or
    /// compacted one so far.
    #[cfg(test)]
    pub(crate) fn ran_after(&self) -> Vec<(std::thread::ThreadId, u64)> {
        self.ran_after.lock().clone()
    }

    /// Makes the next build's decode report on `reached` and wait for
    /// `release`.
    #[cfg(test)]
    pub(crate) fn gate_next_decode(
        &self,
        reached: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) {
        *self.decode_gate.lock() = Some((reached, release));
    }

    /// Runs on the blocking pool before a build decodes its base.
    #[cfg(test)]
    pub(crate) fn before_decode(&self) {
        let Some((reached, release)) = self.decode_gate.lock().take() else {
            return;
        };
        reached.send(()).expect("the test waits for the decode");
        release.recv().expect("the test releases the decode");
    }

    /// Builds started so far.
    #[cfg(test)]
    pub(crate) fn builds(&self) -> u64 {
        self.state.lock().next_build
    }

    /// Leases on `target`'s ready view, and on its orphan.
    #[cfg(test)]
    pub(crate) fn leases(&self, target: QueueTarget) -> (usize, usize) {
        self.state
            .lock()
            .targets
            .get(&target)
            .map_or((0, 0), |tracked| {
                (
                    match &tracked.view {
                        ViewState::Ready(view) => view.leases,
                        ViewState::Absent | ViewState::Building { .. } | ViewState::Unsettled => 0,
                    },
                    tracked.orphan.as_ref().map_or(0, |orphan| orphan.leases),
                )
            })
    }

    /// Evicts every view no search leases, as a budget of nothing would.
    #[cfg(test)]
    pub(crate) fn evict_idle(self: &Arc<Self>) {
        let mut after = After::default();
        self.state.lock().evict(0, None, &mut after);
        self.run(after);
    }

    /// Whether `target` has a ready view.
    #[cfg(test)]
    pub(crate) fn holds_view(&self, target: QueueTarget) -> bool {
        self.state
            .lock()
            .targets
            .get(&target)
            .is_some_and(|tracked| matches!(tracked.view, ViewState::Ready(_)))
    }

    /// Runs `after` off the lock: on the blocking pool when it is heavier
    /// than [`INLINE_AFTER_BYTES`] and a runtime is at hand, and otherwise
    /// right here.
    fn run(self: &Arc<Self>, after: After) {
        if after.weight() > INLINE_AFTER_BYTES
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            let shared = Arc::clone(self);
            // Detached: the work needs nobody to wait for it.
            drop(runtime.spawn_blocking(move || shared.run_here(after)));
            return;
        }
        self.run_here(after);
    }

    /// Finishes the compactions `after` holds, then frees what it holds,
    /// both off the lock, on this thread.
    fn run_here(&self, mut after: After) {
        #[cfg(test)]
        if after.weight() > 0 {
            self.ran_after
                .lock()
                .push((std::thread::current().id(), after.weight()));
        }
        while let Some(Compaction {
            target,
            build,
            floor,
            below,
            dropping: _,
        }) = after.compactions.pop()
        {
            let merged = below
                .iter()
                .flat_map(|chunk| chunk.slots.iter())
                .filter(|slot| slot.acknowledged.load(Ordering::Acquire) > floor)
                .cloned()
                .collect();
            // A view replaced meanwhile may have left these the last handles.
            drop(below);
            self.state
                .lock()
                .finish(target, build, floor, merged, &mut after);
        }
    }

    /// Pins what compaction may drop for a request snapshot about to open.
    pub(crate) fn pin(self: &Arc<Self>) -> SnapshotPin {
        let mut state = self.state.lock();
        let pinned = state.recorded;
        *state.pins.entry(pinned).or_default() += 1;
        SnapshotPin {
            shared: Arc::clone(self),
            pinned,
        }
    }

    /// Registers `changes` as in flight; call right before the commit.
    ///
    /// A transaction that changes no queue registers nothing and takes no
    /// lock.
    pub(crate) fn begin_commit(self: &Arc<Self>, changes: QueueChanges) -> CommittingQueue {
        if changes.changes.is_empty() {
            return CommittingQueue {
                shared: Arc::clone(self),
                ticket: 0,
                changes: Vec::new(),
                resolved: true,
            };
        }
        let mut guard = self.state.lock();
        let state = &mut *guard;
        let ticket = state.next_ticket;
        state.next_ticket += 1;
        let recorded = state.recorded;
        for (target, _) in &changes.changes {
            state.track(*target).in_flight.insert(ticket, recorded);
        }
        CommittingQueue {
            shared: Arc::clone(self),
            ticket,
            changes: changes.changes,
            resolved: false,
        }
    }

    /// Plans how a search at `snapshot` reads `target`, after waiting for the
    /// target's queue commits in flight when it looked.
    pub(crate) async fn plan(self: &Arc<Self>, target: QueueTarget, snapshot: u64) -> Plan {
        let mut changes = self.changes.subscribe();
        let horizon = self.state.lock().next_ticket;
        loop {
            {
                let mut guard = self.state.lock();
                let state = &mut *guard;
                if state.retired.contains(&target) {
                    return Plan::Fallback;
                }
                state.clock += 1;
                let clock = state.clock;
                let next_build = state.next_build;
                let tracked = state.track(target);
                if tracked.in_flight.range(..horizon).next().is_none() {
                    match &mut tracked.view {
                        ViewState::Ready(view) if snapshot >= view.served_from => {
                            view.used = clock;
                            return Plan::Shared(view.capture(self, target, snapshot));
                        }
                        ViewState::Building {
                            base: Some(base), ..
                        } if snapshot < *base => {
                            return Plan::Fallback;
                        }
                        ViewState::Ready(_) | ViewState::Unsettled => return Plan::Fallback,
                        ViewState::Absent if tracked.orphan.is_some() => return Plan::Fallback,
                        ViewState::Building { .. } => {}
                        ViewState::Absent => {
                            let own_snapshot = matches!(tracked.recorded, Recorded::Through(through) if through <= snapshot);
                            tracked.view = ViewState::Building {
                                build: next_build,
                                base: None,
                                log: Vec::new(),
                            };
                            state.next_build += 1;
                            return Plan::Build(Builder {
                                shared: Arc::clone(self),
                                target,
                                build: next_build,
                                snapshot,
                                own_snapshot,
                                installed: false,
                            });
                        }
                    }
                }
                changes.mark_unchanged();
            }
            if changes.changed().await.is_err() {
                unreachable!("the sender lives as long as the views");
            }
        }
    }

    /// Records the base snapshot `builder` reads at, so searches below it
    /// read storage instead of waiting for the build.
    pub(crate) fn base_chosen(&self, builder: &Builder, base: u64) {
        let mut state = self.state.lock();
        if let Some(tracked) = state.targets.get_mut(&builder.target)
            && let ViewState::Building {
                build,
                base: chosen,
                ..
            } = &mut tracked.view
            && *build == builder.build
        {
            *chosen = Some(base);
        }
        drop(state);
        self.changes.send_modify(|version| *version += 1);
    }

    /// Installs `prepared`, the view `builder` read, replaying the commits
    /// logged above its base, and returns it captured at the building
    /// search's snapshot when it serves that. A build that a reset or a
    /// retirement overtook installs nothing and returns `None`.
    ///
    /// Only the replay runs under the lock: [`PreparedView::new`] built the
    /// base's slots beforehand, off it. Call it on the blocking pool: what it
    /// frees, it frees on this thread.
    pub(crate) fn install(
        &self,
        mut builder: Builder,
        prepared: PreparedView,
    ) -> Option<SharedRead> {
        let PreparedView(mut view) = prepared;
        builder.installed = true;
        let target = builder.target;
        let mut after = After::default();
        let mut guard = self.state.lock();
        let state = &mut *guard;
        let building = state.targets.get(&target).is_some_and(|tracked| {
            matches!(tracked.view, ViewState::Building { build, .. } if build == builder.build)
        });
        if !building {
            drop(guard);
            drop(view);
            return None;
        }
        let in_flight = !state.targets[&target].in_flight.is_empty();
        let ViewState::Building { log, .. } = state.replace(target, ViewState::Absent) else {
            unreachable!("checked above");
        };
        let base = view.base;
        let latest = log
            .iter()
            .map(|(sequence, _)| *sequence)
            .fold(base, u64::max);
        // Replayed in completion order: chunks are keyed by sequence, and an
        // acknowledgement replayed before its enqueue waits for it as early.
        let consistent = log
            .into_iter()
            .try_for_each(|(sequence, change)| view.apply(sequence, change))
            .and_then(|()| view.settled(in_flight));
        let own = match consistent {
            Ok(()) => {
                state.clock += 1;
                view.build = builder.build;
                view.used = state.clock;
                // Captured, and so leased, before anything below can compact
                // or evict it.
                let own = (builder.snapshot >= view.served_from)
                    .then(|| view.capture(&builder.shared, target, builder.snapshot));
                state
                    .targets
                    .get_mut(&target)
                    .expect("checked above")
                    .recorded = Recorded::Through(latest);
                after
                    .views
                    .push(state.replace(target, ViewState::Ready(view)));
                state.maintain(target, &mut after);
                state.evict(self.budget, Some(target), &mut after);
                own
            }
            Err(Inconsistent) => {
                tracing::error!(
                    index_id = target.index_id.get(),
                    generation = target.generation.get(),
                    "a logged queue change contradicts its base; searches read storage until it is rebuilt"
                );
                after.views.push(ViewState::Ready(view));
                state.reset(target, &mut after);
                None
            }
        };
        drop(guard);
        self.changes.send_modify(|version| *version += 1);
        self.run_here(after);
        own
    }

    /// Ends `builder`'s build without a view, whose base did not decode: the
    /// target is unsettled, so its searches read storage, until the
    /// publisher next settles it and a search tries again. Call it on the
    /// blocking pool, like [`Self::install`].
    pub(crate) fn abandon(&self, mut builder: Builder) {
        builder.installed = true;
        let mut after = After::default();
        let mut state = self.state.lock();
        let building = state.targets.get(&builder.target).is_some_and(|tracked| {
            matches!(tracked.view, ViewState::Building { build, .. } if build == builder.build)
        });
        if building {
            state.reset(builder.target, &mut after);
        }
        drop(state);
        self.changes.send_modify(|version| *version += 1);
        self.run_here(after);
    }

    /// Counts one search that reads storage because no view serves it.
    pub(crate) fn fallback(&self) {
        self.state.lock().fallbacks += 1;
    }

    /// Returns how often views were unsettled so far, for [`Self::settle`].
    pub(crate) fn settlement(&self) -> Settlement {
        Settlement(self.state.lock().unsettlements)
    }

    /// Ends `target`'s unsettled state, unless a commit unsettled it again
    /// after `settlement` was read; the next search builds from a fresh
    /// snapshot.
    ///
    /// Only the publisher calls it, once the admission ledger holds no
    /// uncertain charge of `target` after `settlement` was read: every
    /// uncertain commit's fate is then proven by a flushed read, and a
    /// producer marks the ledger before it unsettles the view, so an
    /// uncertain enqueue the ledger did not hold yet unsettles the view after
    /// `settlement`. The publisher's own uncertain acknowledgements unsettle
    /// the view first and mark the ledger right after, in the attempt before
    /// the one that settles.
    pub(crate) fn settle(&self, target: QueueTarget, settlement: Settlement) {
        let mut state = self.state.lock();
        let Some(tracked) = state.targets.get_mut(&target) else {
            return;
        };
        if tracked.unsettled_at > settlement.0 || !matches!(tracked.view, ViewState::Unsettled) {
            return;
        }
        tracked.view = ViewState::Absent;
        drop(state);
        self.changes.send_modify(|version| *version += 1);
    }

    /// Retires `target`, a generation no new search reads: frees its view,
    /// makes a build in progress install nothing, and sends every later
    /// search of it to storage, so nothing holds a view of it again.
    pub(crate) fn retire(self: &Arc<Self>, target: QueueTarget) {
        let mut after = After::default();
        let mut state = self.state.lock();
        if !state.retired.insert(target) {
            return;
        }
        if state.targets.get(&target).is_some_and(|tracked| {
            matches!(
                tracked.view,
                ViewState::Ready(_) | ViewState::Building { .. }
            )
        }) {
            after.views.push(state.replace(target, ViewState::Absent));
            state.prune(target);
        }
        drop(state);
        // Searches waiting for its build read storage instead.
        self.changes.send_modify(|version| *version += 1);
        self.run(after);
    }

    /// Frees `target`'s ready view as eviction would; the next search builds
    /// it anew. The target's recorded commits still bound later builds'
    /// bases.
    #[cfg(test)]
    pub(crate) fn free(self: &Arc<Self>, target: QueueTarget) {
        let mut after = After::default();
        let mut state = self.state.lock();
        if state
            .targets
            .get(&target)
            .is_some_and(|tracked| matches!(tracked.view, ViewState::Ready(_)))
        {
            after.views.push(state.replace(target, ViewState::Absent));
            state.prune(target);
        }
        drop(state);
        self.run(after);
    }

    /// Whether any search has planned a queue read since these views were
    /// created, so whether any view can exist.
    pub(crate) fn searched(&self) -> bool {
        self.state.lock().clock > 0
    }

    /// Charged bytes of the operations every view holds, and how many
    /// searches read storage.
    pub(crate) fn stats(&self) -> (u64, u64) {
        let state = self.state.lock();
        let held = state
            .targets
            .values()
            .map(|tracked| match &tracked.view {
                ViewState::Ready(view) => view.charged,
                ViewState::Absent | ViewState::Building { .. } | ViewState::Unsettled => 0,
            })
            .sum();
        (held, state.fallbacks)
    }
}

#[cfg(test)]
impl Default for SharedQueues {
    /// Views with an unbounded budget.
    fn default() -> Self {
        Self::new(u64::MAX)
    }
}

#[cfg(test)]
#[path = "shared_tests.rs"]
mod tests;
