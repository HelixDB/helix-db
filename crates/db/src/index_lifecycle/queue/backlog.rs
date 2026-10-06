//! Runtime admission accounting for retained immutable index operations.
//!
//! The ledger charges every retained operation its exact encoded record size
//! plus [`OPERATION_OVERHEAD_BYTES`] (see "Charges" below) and counts a
//! `(generation, entity)` member once while it has at least one outstanding
//! operation. Limits aggregate per logical index across its generations. The
//! ledger is process memory only: it never adds a shared persisted counter to
//! foreground transactions, and it is rebuilt from the durable queues before
//! a writer accepts graph writes.
//!
//! # Charges
//!
//! An operation's encoded record understates the memory it costs while
//! retained, most for small operations: a delete encodes in about 24 bytes,
//! while its ledger entry (about 370 bytes), its decoded form in a queue a
//! publisher holds, grouped by entity (about 460), and its share of resolving
//! its queue's merge operands (about 190) cost about 45 times that. So every
//! operation is charged [`charged_bytes`]: its encoded size plus the fixed
//! [`OPERATION_OVERHEAD_BYTES`]. The payload adds about one copy of the encoded
//! payload to the decoded queue and one to a resolution, and the
//! payload-independent costs stay below twice the overhead, so for every
//! operation shape the heap one retained operation holds in the ledger, in one
//! decoded copy of its queue, and in one resolution of its merge operands is at
//! most twice its charge: from about 1.7 times for a delete to 2 times for a
//! large vector. A logical index's retained-byte limit therefore bounds that
//! heap to twice the limit, whatever mix of operations fills it. The
//! `production_queue_admission_memory` test measures every shape at the
//! allocator's chunk sizes and enforces the bound.
//!
//! The ledger applies the overhead itself, to the encoded size every path
//! reports: foreground admission (including blocker repairs admitted beyond
//! the limits), operations found at open, and operations reconciliation
//! discovers. A reopened writer therefore charges exactly what live admission
//! did. Publishers budget the decoded queues they retain between attempts in
//! the same unit, and charged bytes sum across logical indexes, so a
//! writer-wide ceiling can bound them together.
//!
//! Each charge moves through a closed lifecycle:
//!
//! ```text
//! reserve ──► Reserved ──commit ok──► Durable ──acknowledged──► released
//!                 │                      ▲
//!                 ├─definite abort──► released
//!                 └─uncertain──► Uncertain enqueue ──seen after flush──┘
//!                                    └─absent after flush──► released
//!
//! Durable ──acknowledgement uncertain──► Uncertain acknowledgement
//!    ▲                                       ├─absent after flush──► acknowledged
//!    └───────────────seen after flush────────┘
//! ```
//!
//! A response timeout or cancelled commit is uncertain, never an abort: its
//! capacity stays charged until a later flushed read proves the outcome.
//! Uncertain charges are indexed per queue target, so publication checks and
//! reconciles one target without visiting any other retained charge.
//!
//! Every durable operation is counted once, as committed here or discovered,
//! and every durable exact-ID acknowledgement once, whichever path proves it.
//! The ledger also measures publication lag by exact operation ID: a charge
//! whose durable commit this process observed before learning of it any other
//! way carries that instant, and its acknowledgement records the elapsed time.
//! Every other acknowledgement is counted as censored rather than assigned a
//! guessed lag, including one for an operation whose acknowledgement
//! publication committed or attempted before its producer's commit returned.
//!
//! # Blocked builds
//!
//! A hidden build's generation publishes nothing before the build activates,
//! so once its build blocks, a saturated limit cannot clear until an operator
//! retries or aborts the operation. Such a refusal is the non-retryable
//! [`HelixDbError::IndexBuildBlocked`] rather than backpressure, except for a
//! transaction whose only operation in that generation repairs the entity
//! the blocker names. That repair is admitted beyond the limits when it is
//! the entity's first queued operation, or when it removes the entity from
//! the index, so every such blocker stays repairable:
//!
//! - An invalid source row needs only its first write. Every write queued for
//!   a hidden build is validated against the build's own rules, so once the
//!   entity has a queued operation its row is valid and a retry rereads it.
//! - An oversized entity has no such check, so its first write may leave it
//!   oversized. Deleting the entity is then still admitted, as is clearing
//!   its indexed property, and writes to properties the index does not read
//!   queue nothing.
//!
//! A transaction is refused only for a limit it grows, and a write to an
//! entity already pending in the generation adds no member. So while repairs
//! hold the member count above its limit, writes to pending entities are
//! still admitted within the byte limit; only new entities are refused. A
//! removal leaves the entity a member, so writing it back is not exempt from
//! the byte limit: each entity a build blocks on adds at most one member and
//! two operations (its first write and a removal) beyond the limits.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;

use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::IndexEntity;
use crate::encoding::v2::values::indexes::operation_queue::QueuedOperationId;
use crate::error::{HelixDbError, IndexBackpressureResource, IndexOperationBatchResource, Result};
use crate::index_lifecycle::worker::IndexWorkerWakeHandle;
use crate::index_lifecycle::{IndexGenerationId, IndexId, IndexOperationId};

use super::lag::PublicationLagHistogram;
use super::QueueTarget;

/// Fixed bytes charged to every retained operation beyond its encoded record.
///
/// See "Charges" in the module documentation; the
/// `production_queue_admission_memory` test asserts that it covers the
/// measured payload-independent memory of every operation shape.
pub(crate) const OPERATION_OVERHEAD_BYTES: u64 = 576;

/// Returns the bytes an operation whose record encodes in `encoded_bytes`
/// counts toward its index's retained-byte limit: `encoded_bytes` plus
/// [`OPERATION_OVERHEAD_BYTES`].
pub(crate) const fn charged_bytes(encoded_bytes: u64) -> u64 {
    encoded_bytes.saturating_add(OPERATION_OVERHEAD_BYTES)
}

/// Per-logical-index admission ceilings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BacklogLimits {
    pub(crate) max_retained_bytes: u64,
    pub(crate) max_members: u64,
}

/// A hidden build stopped on a blocker, as the reserving transaction read it.
///
/// See "Blocked builds" in the module documentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlockedBuild {
    /// The build's hidden generation.
    pub(crate) target: QueueTarget,
    pub(crate) operation_id: IndexOperationId,
    /// The reserving transaction's operation on the entity the blocker names;
    /// `None` when the blocker names no entity or the transaction does not
    /// write it.
    pub(crate) repair: Option<BlockerRepair>,
}

/// A reserving transaction's operation on the entity a blocker names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BlockerRepair {
    /// Leaves the entity indexed; exempt only as its first queued operation.
    Replace(IndexEntity),
    /// Removes the entity from the index; always exempt.
    Remove(IndexEntity),
}

/// One logical index across all of its generations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct LogicalIndex {
    pub(crate) scope: DataScope,
    pub(crate) index_id: IndexId,
}

/// One operation's admission charge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OperationCharge {
    pub(crate) target: QueueTarget,
    pub(crate) entity: IndexEntity,
    pub(crate) id: QueuedOperationId,
    /// The operation's encoded record size,
    /// [`crate::encoding::v2::values::indexes::operation_queue::QueuedOperation::retained_bytes`];
    /// the ledger charges [`charged_bytes`] of it.
    pub(crate) encoded_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChargeState {
    /// Admitted; the enqueue commit has not returned.
    Reserved,
    /// Durably enqueued and awaiting acknowledgement.
    Durable(DurableOrigin),
    /// A commit outcome is unknown; `marked` orders it against
    /// reconciliations.
    Uncertain {
        commit: UncertainCommit,
        marked: u64,
    },
}

impl ChargeState {
    const fn is_uncertain(self) -> bool {
        matches!(self, Self::Uncertain { .. })
    }
}

/// The commit whose outcome is unknown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UncertainCommit {
    /// The foreground enqueue: the operation may never have committed.
    Enqueue,
    /// An acknowledgement of a durable operation.
    Acknowledgement {
        /// How the ledger learned that the operation is durable.
        origin: DurableOrigin,
        /// When the acknowledgement commit returned without an outcome; it
        /// committed before this instant if at all.
        returned: Instant,
    },
}

/// How the ledger learned that an operation is durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DurableOrigin {
    /// This process observed the enqueue commit return at this instant.
    Committed(Instant),
    /// Found in storage (startup, reconciliation, or publication) without an
    /// observed commit; commit time unknown.
    Discovered,
}

#[derive(Debug, Clone, Copy)]
struct Charge {
    target: QueueTarget,
    entity: IndexEntity,
    /// [`charged_bytes`] of the operation's encoded size.
    charged: u64,
    state: ChargeState,
}

/// Ledger-wide order of the events that can make queued work newly readable:
/// an operation's charge and the return of its enqueue commit.
///
/// A target's latest admission therefore changes when an operation is
/// charged to it and again when that operation's enqueue commit returns,
/// committed or uncertain. A queue read that begins after observing a
/// target's latest admission sees every operation whose commit returned
/// before; any other operation changes the admission later. So publication
/// can wait for new work on a target without reading its queue, and the
/// ledger wakes the index worker whenever a commit's return changes one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct Admission(u64);

/// Current retained work for one logical index.
#[derive(Debug, Default)]
struct IndexUsage {
    /// Charged bytes of every retained operation.
    retained_bytes: u64,
    /// Outstanding operation references per `(generation, entity)` member.
    members: HashMap<(IndexGenerationId, IndexEntity), u32>,
    /// Outstanding work per generation; a generation is present only while
    /// it retains an operation.
    generations: BTreeMap<IndexGenerationId, GenerationUsage>,
}

/// Outstanding work of one generation.
#[derive(Debug, Clone, Copy)]
struct GenerationUsage {
    /// Outstanding operations; never zero.
    operations: u64,
    /// The generation's latest [`Admission`].
    latest: Admission,
}

#[derive(Debug, Default)]
struct BacklogState {
    indexes: HashMap<LogicalIndex, IndexUsage>,
    /// The latest admission issued.
    admissions: u64,
    charges: HashMap<QueuedOperationId, Charge>,
    /// Exactly the IDs of uncertain charges, per queue target; a target is
    /// present only while it holds one.
    uncertain: HashMap<QueueTarget, HashSet<QueuedOperationId>>,
    /// Monotonic reconciliation clock for uncertain outcomes.
    reconcile_clock: u64,
    outcomes: LedgerOutcomes,
    lag: PublicationLagHistogram,
    /// Charges reconciliations examined, so tests prove that reconciling a
    /// target visits only its uncertain charges.
    #[cfg(test)]
    reconciliation_visits: u64,
}

/// Monotonic per-operation outcomes since this ledger was created.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct LedgerOutcomes {
    /// Enqueue commits this process observed succeed.
    pub(crate) committed: u64,
    /// Durable operations whose enqueue commit this process did not observe
    /// succeed: found in storage, or proven durable after an uncertain
    /// enqueue.
    pub(crate) discovered: u64,
    /// Operations released by a durable exact-ID acknowledgement, including
    /// uncertain acknowledgements a flushed read proved committed.
    pub(crate) acknowledged: u64,
    /// Acknowledged operations without an observed commit instant: found in
    /// storage, proven durable after an uncertain enqueue, or acknowledged
    /// (or their acknowledgement attempted) by publication before their
    /// producer's commit returned.
    pub(crate) acknowledged_censored: u64,
}

/// Observable usage for one logical index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct BacklogUsage {
    /// [`charged_bytes`] summed over retained operations.
    pub(crate) retained_bytes: u64,
    pub(crate) members: u64,
    pub(crate) operations: u64,
    /// Operations whose enqueue or acknowledgement outcome is unknown.
    pub(crate) uncertain_operations: u64,
}

/// Ledger-wide usage plus outcome counters and the age of pending work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct BacklogTotals {
    pub(crate) usage: BacklogUsage,
    pub(crate) outcomes: LedgerOutcomes,
    /// Age of the oldest pending operation with an observed commit, a lower
    /// bound on its eventual (censored) publication lag.
    pub(crate) oldest_committed_pending_micros: u64,
}

/// Process-wide retained-operation ledger.
#[derive(Debug)]
pub(crate) struct IndexOperationBacklog {
    limits: BacklogLimits,
    state: Mutex<BacklogState>,
    /// Woken whenever an enqueue commit's return changes a target's latest
    /// [`Admission`], however the producer learned the outcome.
    worker: IndexWorkerWakeHandle,
}

impl IndexOperationBacklog {
    /// Creates an empty ledger that wakes `worker`; open must
    /// [`Self::load_durable`] before writes.
    pub(crate) fn new(limits: BacklogLimits, worker: IndexWorkerWakeHandle) -> Arc<Self> {
        Arc::new(Self {
            limits,
            state: Mutex::new(BacklogState::default()),
            worker,
        })
    }

    /// Atomically reserves capacity for every operation one transaction stages.
    ///
    /// Either every charge is admitted or none is: limits are checked for all
    /// touched logical indexes before any state changes. Acceptance at exactly
    /// the limit succeeds; one byte or member above it fails. Only a limit the
    /// transaction grows can refuse it: an entity already pending in the
    /// generation adds no member, so its writes are admitted even while the
    /// member count is above the limit, as after a blocker repair (below) or
    /// a reopen with a lower limit. A transaction whose own charges exceed a
    /// limit could never be admitted, so it fails with the non-retryable
    /// [`HelixDbError::IndexOperationBatchTooLarge`] before any retryable
    /// [`HelixDbError::IndexBackpressure`] is considered.
    /// A transaction routes through one catalog snapshot, which holds a single
    /// generation per logical index, so charges for two generations of one
    /// index are an [`HelixDbError::InvariantViolation`].
    ///
    /// A saturated index whose generation is one of `blocked` fails with
    /// [`HelixDbError::IndexBuildBlocked`] instead of backpressure, unless the
    /// transaction's only charge for it is its blocker's repair: the entity's
    /// first operation, or one removing it from the index, which is admitted
    /// beyond the limits.
    pub(crate) fn reserve(
        self: &Arc<Self>,
        charges: &[OperationCharge],
        blocked: &[BlockedBuild],
    ) -> Result<BacklogReservation> {
        let mut state = self.state.lock();
        let mut staged: BTreeMap<LogicalIndex, (IndexGenerationId, u64, HashSet<IndexEntity>)> =
            BTreeMap::new();
        let mut ids = HashSet::with_capacity(charges.len());
        for charge in charges {
            if !ids.insert(charge.id) || state.charges.contains_key(&charge.id) {
                return Err(HelixDbError::InvariantViolation(
                    "queued operation ID was reserved twice".to_string(),
                ));
            }
            let (generation, bytes, entities) = staged
                .entry(charge.target.logical_index())
                .or_insert_with(|| (charge.target.generation, 0, HashSet::new()));
            if *generation != charge.target.generation {
                return Err(HelixDbError::InvariantViolation(
                    "one transaction staged operations for two generations of one index"
                        .to_string(),
                ));
            }
            *bytes = bytes.saturating_add(charged_bytes(charge.encoded_bytes));
            entities.insert(charge.entity);
        }
        for (index, (_, bytes, entities)) in &staged {
            let hard_limits = [
                (
                    IndexOperationBatchResource::RetainedBytes,
                    *bytes,
                    self.limits.max_retained_bytes,
                ),
                (
                    IndexOperationBatchResource::PendingMembers,
                    entities.len() as u64,
                    self.limits.max_members,
                ),
            ];
            let Some((resource, observed, limit)) = hard_limits
                .into_iter()
                .find(|(_, observed, limit)| observed > limit)
            else {
                continue;
            };
            return Err(HelixDbError::IndexOperationBatchTooLarge {
                index_id: index.index_id.get(),
                resource,
                observed,
                limit,
            });
        }
        for (index, (generation, bytes, entities)) in &staged {
            let usage = state.indexes.get(index);
            let (members, new_members) = usage.map_or((0, entities.len()), |usage| {
                let new = entities
                    .iter()
                    .filter(|entity| !usage.members.contains_key(&(*generation, **entity)))
                    .count();
                (usage.members.len(), new)
            });
            // Each resource as (current, added, limit); one the transaction
            // does not grow cannot refuse it.
            let Some((resource, requested, limit)) = [
                (
                    IndexBackpressureResource::RetainedBytes,
                    usage.map_or(0, |usage| usage.retained_bytes),
                    *bytes,
                    self.limits.max_retained_bytes,
                ),
                (
                    IndexBackpressureResource::PendingMembers,
                    members as u64,
                    new_members as u64,
                    self.limits.max_members,
                ),
            ]
            .into_iter()
            .filter(|(_, _, added, _)| *added > 0)
            .map(|(resource, current, added, limit)| {
                (resource, current.saturating_add(added), limit)
            })
            .find(|(_, requested, limit)| requested > limit) else {
                continue;
            };
            let target = QueueTarget::new(index.scope, index.index_id, *generation);
            let Some(build) = blocked.iter().find(|build| build.target == target) else {
                return Err(HelixDbError::IndexBackpressure {
                    scope: index.scope,
                    index_id: index.index_id.get(),
                    resource,
                    requested,
                    limit,
                });
            };
            let exempt = entities.len() == 1
                && build.repair.is_some_and(|repair| match repair {
                    BlockerRepair::Replace(entity) => {
                        entities.contains(&entity)
                            && !usage.is_some_and(|usage| {
                                usage.members.contains_key(&(*generation, entity))
                            })
                    }
                    BlockerRepair::Remove(entity) => entities.contains(&entity),
                });
            if !exempt {
                return Err(HelixDbError::IndexBuildBlocked {
                    scope: index.scope,
                    index_id: index.index_id.get(),
                    operation_id: build.operation_id.as_uuid().to_string(),
                    resource,
                    requested,
                    limit,
                });
            }
        }
        for charge in charges {
            state.insert(*charge, ChargeState::Reserved);
        }
        Ok(BacklogReservation {
            backlog: Arc::clone(self),
            ids: charges.iter().map(|charge| charge.id).collect(),
            phase: ReservationPhase::Staged,
        })
    }

    /// Releases durably acknowledged operations and records their lag.
    pub(crate) fn acknowledge(&self, ids: impl IntoIterator<Item = QueuedOperationId>) {
        let now = Instant::now();
        let mut state = self.state.lock();
        for id in ids {
            let Some(charge) = state.release(id) else {
                continue;
            };
            state.count_acknowledgement(charge.state, now);
        }
    }

    /// Returns the cumulative exact-operation publication lag.
    pub(crate) fn lag(&self) -> PublicationLagHistogram {
        self.state.lock().lag.clone()
    }

    /// Marks durable operations whose acknowledgement outcome is unknown.
    ///
    /// Their capacity stays charged until a flushed read shows whether the
    /// acknowledgement committed; each keeps how its durability was learned.
    pub(crate) fn mark_acknowledgement_uncertain(
        &self,
        ids: impl IntoIterator<Item = QueuedOperationId>,
    ) {
        let returned = Instant::now();
        let mut guard = self.state.lock();
        let state = &mut *guard;
        let marked = state.reconcile_clock;
        for id in ids {
            let previous = state.transition(id, |current| {
                let origin = match current {
                    ChargeState::Durable(origin)
                    | ChargeState::Uncertain {
                        commit: UncertainCommit::Acknowledgement { origin, .. },
                        ..
                    } => origin,
                    // Publication read it durable before its producer
                    // observed a commit outcome.
                    ChargeState::Reserved
                    | ChargeState::Uncertain {
                        commit: UncertainCommit::Enqueue,
                        ..
                    } => DurableOrigin::Discovered,
                };
                ChargeState::Uncertain {
                    commit: UncertainCommit::Acknowledgement { origin, returned },
                    marked,
                }
            });
            if matches!(
                previous,
                Some(ChargeState::Uncertain {
                    commit: UncertainCommit::Enqueue,
                    ..
                })
            ) {
                state.outcomes.discovered += 1;
            }
        }
    }

    /// Charges the durable operations of one queue found at open, each given
    /// as its ID, entity, and encoded size.
    ///
    /// Runs before any graph write, so every operation is discovered durable
    /// work; an ID already charged (the same queue loaded twice) is left
    /// unchanged. Each is charged exactly as live admission charged it.
    /// Reconciliation reads go through [`Self::finish_reconciliation`]
    /// instead.
    pub(crate) fn load_durable(
        &self,
        target: QueueTarget,
        operations: impl IntoIterator<Item = (QueuedOperationId, IndexEntity, u64)>,
    ) {
        let mut state = self.state.lock();
        for (id, entity, encoded_bytes) in operations {
            state.discover(OperationCharge {
                target,
                entity,
                id,
                encoded_bytes,
            });
        }
    }

    /// Starts a reconciliation: uncertain charges marked before this point may
    /// be settled by [`Self::finish_reconciliation`] after a writer flush.
    pub(crate) fn begin_reconciliation(&self) -> ReconciliationTicket {
        let mut state = self.state.lock();
        state.reconcile_clock = state.reconcile_clock.saturating_add(1);
        ReconciliationTicket {
            clock: state.reconcile_clock,
        }
    }

    /// Returns whether `target` holds any charge, whatever its state.
    pub(crate) fn has_charges(&self, target: QueueTarget) -> bool {
        self.state
            .lock()
            .indexes
            .get(&target.logical_index())
            .is_some_and(|usage| usage.generations.contains_key(&target.generation))
    }

    /// Returns whether `target` holds any uncertain charge, in constant time.
    pub(crate) fn has_uncertain(&self, target: QueueTarget) -> bool {
        self.state.lock().uncertain.contains_key(&target)
    }

    /// Returns queue targets holding uncertain charges.
    #[cfg(test)]
    pub(crate) fn uncertain_targets(&self) -> std::collections::BTreeSet<QueueTarget> {
        self.state.lock().uncertain.keys().copied().collect()
    }

    /// Settles `target`'s uncertain charges against a flushed read of its
    /// queue, returning how many were released.
    ///
    /// `present` must be every operation of `target`'s queue, each as its
    /// ID, entity, and encoded size, in a read taken after a successful
    /// writer flush that itself began after `ticket` was issued. Presence
    /// proves an enqueue durable whenever it was marked, so a present
    /// uncertain enqueue or unknown ID becomes a discovered durable charge,
    /// charged as live admission would have. Every other uncertain charge
    /// settles only if marked before `ticket`: an absent enqueue never
    /// committed and is released; an absent acknowledgement committed and
    /// counts as one; a present acknowledgement did not commit, so the charge
    /// is durable again. Work under the ledger lock scales with `present` and
    /// `target`'s uncertain charges, never with other targets' charges.
    pub(crate) fn finish_reconciliation(
        &self,
        ticket: ReconciliationTicket,
        target: QueueTarget,
        present: impl IntoIterator<Item = (QueuedOperationId, IndexEntity, u64)>,
    ) -> u64 {
        let now = Instant::now();
        let mut guard = self.state.lock();
        let state = &mut *guard;
        // Only present uncertain IDs are kept, so the set is bounded by the
        // target's uncertain charges rather than by its queue.
        let mut present_uncertain = HashSet::new();
        for (id, entity, encoded_bytes) in present {
            let Some(charge) = state.charges.get(&id) else {
                state.discover(OperationCharge {
                    target,
                    entity,
                    id,
                    encoded_bytes,
                });
                continue;
            };
            if charge.state.is_uncertain() {
                present_uncertain.insert(id);
            }
        }
        let Some(uncertain) = state.uncertain.get(&target) else {
            return 0;
        };
        let settled = uncertain
            .iter()
            .filter_map(|id| {
                #[cfg(test)]
                {
                    state.reconciliation_visits += 1;
                }
                let charge = state
                    .charges
                    .get(id)
                    .expect("an indexed uncertain charge is retained");
                let ChargeState::Uncertain { commit, marked } = charge.state else {
                    unreachable!("the uncertain index holds only uncertain charges");
                };
                let present = present_uncertain.contains(id);
                let proven = present && commit == UncertainCommit::Enqueue;
                (proven || marked < ticket.clock).then_some((*id, commit, present))
            })
            .collect::<Vec<_>>();
        let mut released = 0;
        for (id, commit, present) in settled {
            match (commit, present) {
                (UncertainCommit::Enqueue, true) => {
                    state.transition(id, |_| ChargeState::Durable(DurableOrigin::Discovered));
                    state.outcomes.discovered += 1;
                }
                (UncertainCommit::Enqueue, false) => {
                    state.release(id);
                    released += 1;
                }
                (UncertainCommit::Acknowledgement { origin, .. }, true) => {
                    state.transition(id, |_| ChargeState::Durable(origin));
                }
                (UncertainCommit::Acknowledgement { .. }, false) => {
                    let charge = state.release(id).expect("a settled charge is retained");
                    state.count_acknowledgement(charge.state, now);
                    released += 1;
                }
            }
        }
        released
    }

    /// Returns generations with outstanding charges, in a stable order.
    pub(crate) fn outstanding_targets(&self) -> Vec<QueueTarget> {
        self.outstanding_admissions()
            .into_iter()
            .map(|(target, _)| target)
            .collect()
    }

    /// Returns generations with outstanding charges, in a stable order, each
    /// with its latest [`Admission`].
    pub(crate) fn outstanding_admissions(&self) -> Vec<(QueueTarget, Admission)> {
        let state = self.state.lock();
        let mut targets = state
            .indexes
            .iter()
            .flat_map(|(index, usage)| {
                usage.generations.iter().map(move |(generation, work)| {
                    (
                        QueueTarget::new(index.scope, index.index_id, *generation),
                        work.latest,
                    )
                })
            })
            .collect::<Vec<_>>();
        targets.sort_unstable();
        targets
    }

    /// Returns `target`'s latest [`Admission`], or `None` while it retains
    /// no operation.
    pub(crate) fn latest_admission(&self, target: QueueTarget) -> Option<Admission> {
        self.state
            .lock()
            .indexes
            .get(&target.logical_index())
            .and_then(|usage| usage.generations.get(&target.generation))
            .map(|work| work.latest)
    }

    /// Returns usage summed across every logical index, outcome counters,
    /// and the age of the oldest pending operation committed here.
    ///
    /// Scans every retained charge under the ledger lock, so callers sample
    /// it periodically rather than per request.
    pub(crate) fn totals(&self) -> BacklogTotals {
        let now = Instant::now();
        let state = self.state.lock();
        let (retained_bytes, members) =
            state
                .indexes
                .values()
                .fold((0_u64, 0_u64), |(bytes, members), usage| {
                    (
                        bytes.saturating_add(usage.retained_bytes),
                        members.saturating_add(usage.members.len() as u64),
                    )
                });
        let oldest_committed = state
            .charges
            .values()
            .filter_map(|charge| match charge.state {
                ChargeState::Durable(DurableOrigin::Committed(at))
                | ChargeState::Uncertain {
                    commit:
                        UncertainCommit::Acknowledgement {
                            origin: DurableOrigin::Committed(at),
                            ..
                        },
                    ..
                } => Some(at),
                ChargeState::Reserved
                | ChargeState::Durable(DurableOrigin::Discovered)
                | ChargeState::Uncertain { .. } => None,
            })
            .min();
        BacklogTotals {
            usage: BacklogUsage {
                retained_bytes,
                members,
                operations: state.charges.len() as u64,
                uncertain_operations: state.uncertain.values().map(|ids| ids.len() as u64).sum(),
            },
            outcomes: state.outcomes,
            oldest_committed_pending_micros: oldest_committed.map_or(0, |at| {
                u64::try_from(now.saturating_duration_since(at).as_micros()).unwrap_or(u64::MAX)
            }),
        }
    }

    /// Returns every retained charge by operation ID, with its
    /// [`charged_bytes`].
    #[cfg(test)]
    pub(crate) fn charges(
        &self,
    ) -> std::collections::BTreeMap<QueuedOperationId, (QueueTarget, IndexEntity, u64)> {
        self.state
            .lock()
            .charges
            .iter()
            .map(|(id, charge)| (*id, (charge.target, charge.entity, charge.charged)))
            .collect()
    }

    /// Returns current usage for one logical index, asserting that the
    /// per-target uncertain index agrees with every retained charge.
    #[cfg(test)]
    pub(crate) fn usage(&self, scope: DataScope, index_id: IndexId) -> BacklogUsage {
        let state = self.state.lock();
        let index = LogicalIndex { scope, index_id };
        let (operations, uncertain) = state
            .charges
            .values()
            .filter(|charge| charge.target.logical_index() == index)
            .fold((0_u64, 0_u64), |(operations, uncertain), charge| {
                (
                    operations + 1,
                    uncertain + u64::from(charge.state.is_uncertain()),
                )
            });
        let indexed = state
            .uncertain
            .iter()
            .filter(|(target, _)| target.logical_index() == index)
            .map(|(_, ids)| ids.len() as u64)
            .sum::<u64>();
        assert_eq!(
            uncertain, indexed,
            "the uncertain index matches the charges"
        );
        let Some(usage) = state.indexes.get(&index) else {
            return BacklogUsage::default();
        };
        BacklogUsage {
            retained_bytes: usage.retained_bytes,
            members: usage.members.len() as u64,
            operations,
            uncertain_operations: uncertain,
        }
    }

    /// Records an enqueue commit's outcome and wakes the index worker if it
    /// readmitted an operation: a stalled attempt may have read the queue
    /// before the commit returned, and otherwise sleeps until its deadline.
    fn resolve(&self, ids: &[QueuedOperationId], outcome: ReservationOutcome) {
        let now = Instant::now();
        let mut guard = self.state.lock();
        let state = &mut *guard;
        let marked = state.reconcile_clock;
        let admissions = state.admissions;
        for id in ids {
            match outcome {
                ReservationOutcome::Committed => {
                    state.outcomes.committed += 1;
                    // Publication may already have acknowledged an operation
                    // whose commit raced ahead of this call (it stays
                    // released) or attempted to (it stays uncertain, or
                    // durable once reconciled, with a discovered origin).
                    // Either way the ledger learned of durability through
                    // publication, not this observation, so the lag is
                    // censored.
                    let previous = state.transition(*id, |current| match current {
                        ChargeState::Reserved => {
                            ChargeState::Durable(DurableOrigin::Committed(now))
                        }
                        seen @ (ChargeState::Durable(_) | ChargeState::Uncertain { .. }) => seen,
                    });
                    if previous == Some(ChargeState::Reserved) {
                        state.readmit(*id);
                    }
                }
                ReservationOutcome::Aborted => {
                    state.release(*id);
                }
                ReservationOutcome::Uncertain => {
                    let previous = state.transition(*id, |current| match current {
                        ChargeState::Reserved => ChargeState::Uncertain {
                            commit: UncertainCommit::Enqueue,
                            marked,
                        },
                        seen @ (ChargeState::Durable(_) | ChargeState::Uncertain { .. }) => seen,
                    });
                    // Publication already acknowledged it, or attempted to,
                    // so the enqueue is durable although this producer saw no
                    // outcome.
                    match previous {
                        Some(ChargeState::Reserved) => state.readmit(*id),
                        Some(ChargeState::Durable(_) | ChargeState::Uncertain { .. }) | None => {
                            state.outcomes.discovered += 1;
                        }
                    }
                }
            }
        }
        let readmitted = state.admissions != admissions;
        drop(guard);
        if readmitted {
            self.worker.wake();
        }
    }
}

impl BacklogState {
    fn insert(&mut self, charge: OperationCharge, state: ChargeState) {
        assert!(
            !state.is_uncertain(),
            "charges enter the ledger reserved or durable"
        );
        self.admissions += 1;
        let latest = Admission(self.admissions);
        let usage = self
            .indexes
            .entry(charge.target.logical_index())
            .or_default();
        let charged = charged_bytes(charge.encoded_bytes);
        usage.retained_bytes = usage.retained_bytes.saturating_add(charged);
        *usage
            .members
            .entry((charge.target.generation, charge.entity))
            .or_default() += 1;
        usage
            .generations
            .entry(charge.target.generation)
            .and_modify(|work| {
                work.operations += 1;
                work.latest = latest;
            })
            .or_insert(GenerationUsage {
                operations: 1,
                latest,
            });
        self.charges.insert(
            charge.id,
            Charge {
                target: charge.target,
                entity: charge.entity,
                charged,
                state,
            },
        );
    }

    /// Gives the generation of retained charge `id` a new latest admission
    /// once its enqueue commit returned: a queue read that began earlier may
    /// have missed the operation.
    fn readmit(&mut self, id: QueuedOperationId) {
        let target = self.charges[&id].target;
        self.admissions += 1;
        self.indexes
            .get_mut(&target.logical_index())
            .and_then(|usage| usage.generations.get_mut(&target.generation))
            .expect("a retained charge's generation is counted")
            .latest = Admission(self.admissions);
    }

    /// Charges a durable operation found in storage unless it is retained.
    fn discover(&mut self, charge: OperationCharge) {
        if self.charges.contains_key(&charge.id) {
            return;
        }
        self.insert(charge, ChargeState::Durable(DurableOrigin::Discovered));
        self.outcomes.discovered += 1;
    }

    /// Moves one retained charge to the state `next` derives from its
    /// current one, keeping the per-target uncertain index exact.
    ///
    /// Returns the previous state, or `None` for an ID no longer retained.
    fn transition(
        &mut self,
        id: QueuedOperationId,
        next: impl FnOnce(ChargeState) -> ChargeState,
    ) -> Option<ChargeState> {
        let charge = self.charges.get_mut(&id)?;
        let previous = charge.state;
        charge.state = next(previous);
        let (target, uncertain) = (charge.target, charge.state.is_uncertain());
        match (previous.is_uncertain(), uncertain) {
            (false, true) => {
                self.uncertain.entry(target).or_default().insert(id);
            }
            (true, false) => self.forget_uncertain(target, id),
            (false, false) | (true, true) => {}
        }
        Some(previous)
    }

    /// Removes one charge and its usage, returning it if it was retained.
    fn release(&mut self, id: QueuedOperationId) -> Option<Charge> {
        let charge = self.charges.remove(&id)?;
        if charge.state.is_uncertain() {
            self.forget_uncertain(charge.target, id);
        }
        let index = charge.target.logical_index();
        let usage = self
            .indexes
            .get_mut(&index)
            .expect("a retained charge's logical index has usage");
        usage.retained_bytes = usage.retained_bytes.saturating_sub(charge.charged);
        let member = (charge.target.generation, charge.entity);
        let references = usage
            .members
            .get_mut(&member)
            .expect("a retained charge's member is counted");
        *references -= 1;
        if *references == 0 {
            usage.members.remove(&member);
        }
        let work = usage
            .generations
            .get_mut(&charge.target.generation)
            .expect("a retained charge's generation is counted");
        work.operations -= 1;
        if work.operations == 0 {
            usage.generations.remove(&charge.target.generation);
        }
        if usage.generations.is_empty() {
            self.indexes.remove(&index);
        }
        Some(charge)
    }

    fn forget_uncertain(&mut self, target: QueueTarget, id: QueuedOperationId) {
        let Entry::Occupied(mut ids) = self.uncertain.entry(target) else {
            unreachable!("an uncertain charge's target is indexed");
        };
        assert!(
            ids.get_mut().remove(&id),
            "an uncertain charge is indexed under its target"
        );
        if ids.get().is_empty() {
            ids.remove();
        }
    }

    /// Counts one durable exact-ID acknowledgement of a released charge,
    /// timing it when this process observed the enqueue commit.
    fn count_acknowledgement(&mut self, released: ChargeState, now: Instant) {
        self.outcomes.acknowledged += 1;
        let (origin, acknowledged) = match released {
            ChargeState::Durable(origin) => (origin, now),
            ChargeState::Uncertain {
                commit: UncertainCommit::Acknowledgement { origin, returned },
                ..
            } => (origin, returned),
            // Acknowledged before its producer observed the commit return.
            ChargeState::Reserved => (DurableOrigin::Discovered, now),
            // The acknowledgement proves the uncertain enqueue committed.
            ChargeState::Uncertain {
                commit: UncertainCommit::Enqueue,
                ..
            } => {
                self.outcomes.discovered += 1;
                (DurableOrigin::Discovered, now)
            }
        };
        match origin {
            DurableOrigin::Committed(at) => {
                let micros = acknowledged.saturating_duration_since(at).as_micros();
                self.lag.record(u64::try_from(micros).unwrap_or(u64::MAX));
            }
            DurableOrigin::Discovered => self.outcomes.acknowledged_censored += 1,
        }
    }
}

/// Ordering proof for one uncertain-outcome reconciliation.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReconciliationTicket {
    clock: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReservationPhase {
    /// Staged but not submitted: dropping it is a definite abort.
    Staged,
    /// Commit submitted: dropping it leaves the outcome uncertain.
    Committing,
    /// Explicitly resolved.
    Resolved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReservationOutcome {
    Committed,
    Aborted,
    Uncertain,
}

/// Capacity reserved by one foreground transaction.
///
/// Dropping an unresolved reservation before commit submission releases it;
/// dropping one after submission (for example a cancelled request) keeps it
/// charged as uncertain until reconciliation proves the outcome.
#[derive(Debug)]
pub(crate) struct BacklogReservation {
    backlog: Arc<IndexOperationBacklog>,
    ids: Vec<QueuedOperationId>,
    phase: ReservationPhase,
}

impl BacklogReservation {
    /// Marks the durable commit as submitted.
    pub(crate) fn begin_commit(&mut self) {
        assert_eq!(
            self.phase,
            ReservationPhase::Staged,
            "a reservation begins commit exactly once"
        );
        self.phase = ReservationPhase::Committing;
    }

    /// Records a durable commit.
    pub(crate) fn committed(mut self) {
        self.finish(ReservationOutcome::Committed);
    }

    /// Records a definite abort: nothing was committed.
    pub(crate) fn aborted(mut self) {
        self.finish(ReservationOutcome::Aborted);
    }

    /// Records an unknown commit outcome.
    pub(crate) fn uncertain(mut self) {
        self.finish(ReservationOutcome::Uncertain);
    }

    fn finish(&mut self, outcome: ReservationOutcome) {
        assert_ne!(
            self.phase,
            ReservationPhase::Resolved,
            "a reservation resolves exactly once"
        );
        self.phase = ReservationPhase::Resolved;
        self.backlog.resolve(&self.ids, outcome);
    }
}

impl Drop for BacklogReservation {
    fn drop(&mut self) {
        match self.phase {
            ReservationPhase::Staged => self.finish(ReservationOutcome::Aborted),
            ReservationPhase::Committing => self.finish(ReservationOutcome::Uncertain),
            ReservationPhase::Resolved => {}
        }
    }
}

#[cfg(test)]
mod tests;
