//! Bounded publication of queued operations under generation ownership.
//!
//! One publication attempt for one generation:
//!
//! 1. Classify the generation without waiting for its ownership, which a
//!    build, abort, cleanup, or compaction step holds for a whole step.
//!    Reconcile uncertain charges, then defer a hidden build or discard a
//!    retired generation's queue: all three touch only the queue, never
//!    physical rows.
//! 2. Acquire exclusive process-local publication ownership of an Active
//!    generation.
//! 3. Take the queue the target's previous attempt left
//!    ([`super::storage::RetainedQueues`]), or read the resolved queue outside
//!    any transaction (no read dependency) and group it by entity once.
//! 4. Select a bounded batch: whole ordered per-entity prefixes, rotating the
//!    starting entity for fairness, never skipping an earlier operation of an
//!    entity, and never naming more IDs than an acknowledgement may carry
//!    beside its effects. An entity whose lone operation cannot fit one
//!    publication is held back, so it blocks only itself; once a newer
//!    operation of it is selectable, it is repaired: retried alone, at full
//!    width, when the rotation reaches it.
//! 5. Open a serializable publication transaction and re-read the canonical
//!    record through it, so a generation that retired while the attempt
//!    awaited ownership, or a concurrent lifecycle change, retries instead.
//! 6. Stage the longest prefix of entities whose exact output fits beside the
//!    acknowledgement: vectors through the build planner
//!    ([`crate::index_lifecycle::vector::publication`]), text as one epoch.
//!    When planning fails, [`FailureKind`] decides: an entity whose planning
//!    fails deterministically is held back like one that cannot fit, but is
//!    also retried on a timer, and the rest publish from the next attempt;
//!    anything else retries the batch.
//! 7. Stage one acknowledgement naming exactly the published IDs.
//! 8. Commit through the vector cache's commit fence, release accounting and
//!    retain the rest of the queue, then retire emptied partition caches and
//!    retain the vector planning session for the target's next attempt: in a
//!    fair share of the planning budget while the target has work left, in
//!    spare budget once drained. A drained target's schedule is dropped,
//!    keeping only its latest vector commit among the most recently drained.
//!
//! Only this attempt acknowledges its generation's queue: callers never run
//! two attempts for one generation at once (the worker skips in-flight
//! targets). Conflicts and uncertain outcomes discard all prepared work,
//! including the planning session; the next attempt rediscovers durable
//! state instead of reusing an acknowledgement. A trimmed selection, an
//! entity held back, or a definite conflict committed nothing, so it retains
//! the queue unchanged for the next attempt; an uncertain outcome drops it.
//! Every step after the read costs work proportional to its batch, so a
//! backlog drains in time linear in its size.

use std::collections::{HashMap, HashSet, VecDeque};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use slatedb::{DbReadOps, IsolationLevel};

use crate::config::{ActiveTextMutationLimits, SearchIndexBatchLimits};
use crate::encoding::v2::keys::{IndexEntity, ManagedIndexKey, RecordKind, ScopedKey};
use crate::encoding::v2::values::decode_index_record;
use crate::encoding::v2::values::indexes::operation_queue::{
    QueueFamily, QueuedOperation, QueuedOperationId, QueuedPayload,
};
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::vector::publication::{
    stage_active_effects, QueuedVectorEffect, StagedEffects,
};
use crate::index_lifecycle::vector::{
    PublicationBacklog, VectorBuildCache, MAX_RETAINED_PUBLICATIONS,
};
use crate::index_lifecycle::{
    ActiveIndexHandle, IndexGenerationPublicationPermit, IndexRecordV2, IndexScopeGates,
    IndexStateV2,
};
use crate::search::vector::{self, SimHasherRegistry, VectorCacheRegistry};

use super::backlog::{Admission, IndexOperationBacklog};
use super::storage::{QueueStore, StoredQueue};
use super::{OutputBudget, QueueTarget};

/// Minimum delay before retrying a generation that made no progress.
const RETRY_DELAY: Duration = Duration::from_millis(10);
/// Maximum backoff for a generation that repeatedly cannot publish.
const MAX_BACKOFF: Duration = Duration::from_secs(5);
/// Maximum backoff while queued work waits on another actor rather than a
/// failure: a hidden build's activation ([`PublicationOutcome::Deferred`]),
/// or the in-flight commit of charged work that reads empty. It bounds how
/// long that work waits once it becomes publishable.
const MAX_DEFERRED_BACKOFF: Duration = Duration::from_secs(1);
/// Longest a [`PublicationOutcome::Stalled`] generation waits without new
/// work, and how long an entity whose planning failed waits before it is
/// planned again ([`HeldEntity::Failed`]). A new operation, or a failed
/// entity's due retry, makes a stalled generation eligible at once; this
/// bounds how long a retirement, which only an attempt discovers, leaves its
/// held operations charged before they are discarded, and how soon a failed
/// entity publishes once what failed is repaired.
const MAX_STALLED_WAIT: Duration = Duration::from_secs(60);
/// Most operation IDs one discard transaction acknowledges (about a 1 MiB
/// map operand); the operand and output bounds may lower it further.
const MAX_DISCARDED_OPERATIONS: usize = 65_536;

/// Outcome of one bounded publication attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublicationOutcome {
    /// Exact operations were durably published and acknowledged.
    Published { operations: u64, entities: u64 },
    /// A retired generation's operations were acknowledged without applying
    /// them; lifecycle cleanup reclaims its physical rows.
    Discarded { operations: u64 },
    /// The queue was empty.
    Empty,
    /// A hidden build owns the generation; its operations wait for activation.
    Deferred,
    /// A serializable conflict or uncertain commit; rediscover and retry.
    Retry,
    /// Exact output crossed a budget before anything fit, or planning a text
    /// epoch failed deterministically; retry immediately with fewer text
    /// entities, or fewer operations.
    Trimmed,
    /// One operation's effect and acknowledgement cannot fit an output budget,
    /// or planning one entity failed deterministically, so its entity is held
    /// back until a later operation supersedes it (or, after a failure, until
    /// its retry is due); retry immediately with the generation's other
    /// entities, or after backoff once two entities in a row failed to plan
    /// without a publication between them.
    Blocked,
    /// Every queued entity is held back after blocking; nothing was attempted.
    /// The generation waits for a new operation rather than retrying on a
    /// timer.
    Stalled,
}

/// Monotonic counters for publication observability.
#[derive(Debug, Default)]
pub(crate) struct QueuePublicationMetrics {
    pub(crate) published_operations: AtomicU64,
    pub(crate) published_entities: AtomicU64,
    pub(crate) committed_batches: AtomicU64,
    pub(crate) commit_conflicts: AtomicU64,
    pub(crate) uncertain_commits: AtomicU64,
    pub(crate) output_retries: AtomicU64,
    pub(crate) blocked_attempts: AtomicU64,
    pub(crate) discarded_operations: AtomicU64,
    pub(crate) queue_reads: AtomicU64,
    pub(crate) queue_read_bytes: AtomicU64,
    pub(crate) queue_read_micros: AtomicU64,
    pub(crate) attempts: AtomicU64,
    pub(crate) attempt_micros: AtomicU64,
    pub(crate) retry_attempts: AtomicU64,
    pub(crate) error_retries: AtomicU64,
    pub(crate) deferred_attempts: AtomicU64,
}

/// Per-generation fairness and retry state.
#[derive(Debug, Clone)]
struct TargetSchedule {
    /// Last entity whose prefix was published, or that was held back; the
    /// next batch starts after it.
    cursor: Option<IndexEntity>,
    /// Current text entity ceiling, reduced when a text epoch crossed a
    /// budget. Vector batches end at the first entity that does not fit.
    /// Like the operation ceiling, it outlives a block, so the next of
    /// several unpublishable heads (a restarted publisher rediscovering
    /// them) is held back in one attempt; publications double both back.
    entity_limit: usize,
    /// Current operation ceiling, reduced when not even the first entity fit
    /// beside the selection's acknowledgement.
    operation_limit: NonZeroUsize,
    /// Publisher sequence number of the target's latest vector commit; a
    /// retained planning session is reused only at exactly this commit.
    last_vector_commit: Option<NonZeroU64>,
    /// When the target is eligible again.
    eligibility: Eligibility,
    /// Consecutive attempts without progress.
    failures: u32,
    /// Entities held back after failing to plan since the generation last
    /// published. A failure outside one entity's input, such as a partition's
    /// missing metadata, fails every entity in turn, so from the second such
    /// hold on, the next attempt backs off rather than holding back the whole
    /// queue one immediate attempt at a time.
    failed_holds: u32,
    /// Entities held back because a lone operation of theirs could not fit a
    /// publication or failed to plan, or draining after their repair. Process
    /// memory only: a restarted publisher rediscovers blocked entities by
    /// blocking again, and publishes a draining entity's remaining operations
    /// in regular batches (see [`HeldEntity`]).
    held: HashMap<IndexEntity, HeldEntity>,
}

/// One entity held back after one of its operations could not fit a
/// publication, or failed to plan deterministically
/// ([`QueuePublisher::isolate`]).
///
/// A held entity is selected only alone, as a repair, once the rotation
/// reaches it ahead of every entity that is not held back; a batch the
/// rotation carries up to it ends there, so its repair runs next. Holding it
/// back again moves the rotation past it: however often it is written, the
/// rest of its generation publishes between its repairs.
///
/// A repair takes the entity's ordered prefix whatever its input bytes, since
/// its effect materializes only the prefix's newest state. Its first attempt
/// takes every queued operation of the entity, and each attempt that does not
/// fit halves the operations past `through`, so it never retries a state
/// already known not to publish. A repair that ends without publishing waits
/// through the newest operation its first attempt took; any later operation
/// starts a new repair at full width.
///
/// A repair acknowledges at most one acknowledgement's worth of its oldest
/// operations, but publishes the state of all it took, so a later write
/// (such as a delete) repairs the entity however many operations are queued
/// before it. A repair that committed past its acknowledgement keeps the
/// entity draining: it is repaired again at full width, republishing that
/// state or a newer one, until every operation it took is acknowledged,
/// rather than publishing an older prefix of them in a regular batch. A
/// draining repair that does not fit is never halved, since every shorter
/// prefix is older than the state the entity serves; it waits through its
/// newest operation like any other blocked entity.
///
/// Nothing durable records that an entity is draining, and storing it would
/// add state to every repair for a transient effect. A restarted publisher
/// therefore publishes the remaining operations in regular batches, each
/// serving the newest operation it acknowledges, so the entity's published
/// state can step back to an older queued state and forward again until the
/// last one is acknowledged. Strong searches overlay every queued operation
/// and never observe it; eventual searches that do not reach the entity
/// within their budget can.
///
/// An entity whose planning failed is also repaired, at full width, once its
/// retry is due, without a newer operation: what failed may be repaired by
/// then, for example restored metadata, and a held delete is never written
/// again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HeldEntity {
    /// Skipped until an operation newer than `through` is queued.
    Waiting {
        /// Newest operation of the entity known not to publish.
        through: QueuedOperationId,
    },
    /// Planning failed deterministically ([`QueuePublisher::isolate`]):
    /// skipped until an operation newer than `through` is queued, or until
    /// `retry`.
    Failed {
        /// Newest operation of the entity known not to publish.
        through: QueuedOperationId,
        /// When the same operations are planned again.
        retry: Instant,
    },
    /// Published its newest selected state, but operations past the last one
    /// acknowledged, `through`, are still queued: repaired again at full
    /// width. Not blocked.
    Draining {
        /// Last operation the committed repair acknowledged.
        through: QueuedOperationId,
    },
    /// Retried alone with its `width` oldest operations.
    Repairing {
        /// Newest operation of the entity known not to publish; `width`
        /// always extends past it.
        through: QueuedOperationId,
        /// Newest operation the repair's first, full-width attempt took.
        tried: QueuedOperationId,
        /// Operations the next attempt takes.
        width: NonZeroUsize,
    },
}

impl HeldEntity {
    /// Operations the entity's next repair takes of its `queued` operations,
    /// the newest of which is `newest`, or `None` while it waits for an
    /// operation newer than every state known not to publish or, after
    /// failing to plan, for its retry to be due at `now`.
    ///
    /// Operations are queued in order and never reused, so a newer one is
    /// queued exactly when `newest` is not the one known not to publish.
    fn repair_width(
        self,
        queued: NonZeroUsize,
        newest: QueuedOperationId,
        now: Instant,
    ) -> Option<usize> {
        let (through, due) = match self {
            Self::Repairing { width, .. } => return Some(width.min(queued).get()),
            Self::Waiting { through } | Self::Draining { through } => (through, false),
            Self::Failed { through, retry } => (through, retry <= now),
        };
        (due || newest != through).then_some(queued.get())
    }

    /// Whether the entity is held back because a state of it could not fit
    /// or failed to plan.
    const fn is_blocked(self) -> bool {
        match self {
            Self::Waiting { .. } | Self::Failed { .. } | Self::Repairing { .. } => true,
            Self::Draining { .. } => false,
        }
    }
}

impl TargetSchedule {
    /// Starts at `entity_limit` with no cursor, backoff, operation ceiling,
    /// or held-back entity.
    fn new(entity_limit: usize) -> Self {
        Self {
            cursor: None,
            entity_limit,
            operation_limit: NonZeroUsize::MAX,
            last_vector_commit: None,
            eligibility: Eligibility::Now,
            failures: 0,
            failed_holds: 0,
            held: HashMap::new(),
        }
    }
}

/// When a target is eligible for its next attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Eligibility {
    /// At once.
    Now,
    /// Once the instant passes: the backoff of an attempt without progress.
    After(Instant),
    /// Every queued entity was held back ([`PublicationOutcome::Stalled`]):
    /// once the target's latest admission moves past `admitted`, its latest
    /// admission when the stalled attempt began, or at `deadline`.
    NewWork {
        admitted: Option<Admission>,
        deadline: Instant,
    },
}

impl Eligibility {
    /// The instant the target becomes eligible, given `latest`, its latest
    /// admission; `None` once it is.
    fn waits_until(self, latest: Admission) -> Option<Instant> {
        match self {
            Self::Now => None,
            Self::After(instant) => Some(instant),
            Self::NewWork { admitted, deadline } => (admitted == Some(latest)).then_some(deadline),
        }
    }
}

/// Shared vector caches that publication fences, evicts, and retires, the
/// row-read policy its planner uses, and the planning budget its attempts
/// share with builds.
#[derive(Clone)]
pub(crate) struct VectorPublicationResources {
    pub(crate) cache_registry: Arc<VectorCacheRegistry>,
    pub(crate) simhasher_registry: Arc<SimHasherRegistry>,
    pub(crate) batch_reads: crate::batch_reads::BatchReads,
    pub(crate) planning_cache: Arc<VectorBuildCache>,
}

/// Object storage and limits for immutable text split publication.
#[derive(Clone)]
pub(crate) struct TextPublicationResources {
    pub(crate) object_store: Arc<dyn slatedb::object_store::ObjectStore>,
    pub(crate) database: String,
    pub(crate) limits: ActiveTextMutationLimits,
}

/// Shared publication runtime owned by the lifecycle supervisor.
pub(crate) struct QueuePublisher {
    db: Arc<slatedb::Db>,
    backlog: Arc<IndexOperationBacklog>,
    store: Arc<QueueStore>,
    scope_gates: Arc<IndexScopeGates>,
    vector: VectorPublicationResources,
    limits: SearchIndexBatchLimits,
    text: TextPublicationResources,
    /// Schedules of targets with outstanding work, or whose last attempt
    /// left some: a target's schedule is removed once an attempt finds it
    /// drained.
    schedules: Mutex<HashMap<QueueTarget, TargetSchedule>>,
    /// Latest vector commit of the most recently drained targets, newest
    /// last: a drained target's planning session waits, in spare budget, for
    /// a trickle of later writes, and is reused only at that commit. The
    /// planning cache retains at most [`MAX_RETAINED_PUBLICATIONS`] sessions,
    /// so a target drained before as many others drained is assumed to have
    /// lost its session; forgetting a commit only makes the next attempt
    /// plan cold.
    drained_commits: Mutex<VecDeque<(QueueTarget, NonZeroU64)>>,
    cursor: Mutex<Option<QueueTarget>>,
    /// Targets with an attempt running: an attempt is its queue's only
    /// acknowledger, and reconciliation and discard rely on that.
    attempts: Mutex<HashSet<QueueTarget>>,
    /// Sequence numbers handed to vector commits, never reused.
    vector_commits: AtomicU64,
    metrics: QueuePublicationMetrics,
    #[cfg(test)]
    hooks: test_hooks::PublicationHooks,
}

/// One running attempt's exclusive claim on its target, released on drop.
struct AttemptClaim<'publisher> {
    attempts: &'publisher Mutex<HashSet<QueueTarget>>,
    target: QueueTarget,
}

impl Drop for AttemptClaim<'_> {
    fn drop(&mut self) {
        self.attempts.lock().remove(&self.target);
    }
}

#[cfg(test)]
pub(crate) mod test_hooks {
    //! Deterministic interleaving and failure seams for publication tests.

    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use parking_lot::Mutex;
    use tokio::sync::oneshot;

    use super::SelectedEntity;
    use crate::encoding::v2::values::indexes::operation_queue::{
        QueuedOperationId, QueuedVectorReplacement,
    };
    use crate::error::{HelixDbError, Result};
    use crate::index_lifecycle::text::active_batch::QueuedTextEffect;
    use crate::index_lifecycle::vector::publication::QueuedVectorEffect;
    use crate::index_lifecycle::work::TextPartition;
    use crate::index_lifecycle::IndexElementKind;

    /// Test-only controls installed on one publisher.
    #[derive(Debug, Default)]
    pub(crate) struct PublicationHooks {
        /// Stops automatic scheduling so tests drive publication explicitly.
        pub(crate) paused: AtomicBool,
        /// Signals `reached` once a vector attempt's transaction is open and
        /// waits for `release` before planning.
        pub(crate) before_planning: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
        /// Signals `reached` after staging and waits for `release` before commit.
        pub(crate) before_commit: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
        /// Fails the next attempt after staging, before commit.
        pub(crate) fail_before_commit: AtomicBool,
        /// Fails the next vector attempt after its commit, as a post-commit
        /// cache invariant violation would.
        pub(crate) fail_after_commit: AtomicBool,
        /// Reports the next text attempt's successful commit as uncertain, as
        /// a commit whose response was lost would be.
        pub(crate) uncertain_after_commit: AtomicBool,
        /// Row-batch fetch policy of the last staged vector attempt's
        /// mutation indexes.
        pub(crate) batch_reads: Mutex<Option<crate::batch_reads::BatchReads>>,
        /// Failures injected into planning an entity whose newest selected
        /// operation is the key, so a newer operation supersedes one.
        pub(crate) planning_failures: Mutex<HashMap<QueuedOperationId, InjectedPlanningFailure>>,
    }

    /// A failure injected into planning one entity.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum InjectedPlanningFailure {
        /// The entity plans as a corrupt payload would, so planning fails the
        /// same way on every attempt: a vector replacement of the wrong
        /// dimension, or a text effect naming the other element kind.
        Corrupt,
        /// Planning the entity fails with an object-store error, as an
        /// outage would.
        Unavailable,
    }

    impl PublicationHooks {
        /// The failure injected into planning each of `selection`'s
        /// entities, in order.
        pub(crate) fn planning_failures(
            &self,
            selection: &[SelectedEntity<'_>],
        ) -> Vec<Option<InjectedPlanningFailure>> {
            let failures = self.planning_failures.lock();
            selection
                .iter()
                .map(|selected| {
                    selected
                        .taken()
                        .last()
                        .and_then(|newest| failures.get(&newest.id()).copied())
                })
                .collect()
        }
    }

    /// Gives every effect `injected` corrupts a replacement of the wrong
    /// dimension.
    pub(crate) fn corrupt_vector_effects(
        effects: Vec<QueuedVectorEffect>,
        injected: &[Option<InjectedPlanningFailure>],
    ) -> Vec<QueuedVectorEffect> {
        effects
            .into_iter()
            .zip(injected)
            .map(|(effect, injected)| match injected {
                Some(InjectedPlanningFailure::Corrupt) => QueuedVectorEffect {
                    replacement: Some(
                        QueuedVectorReplacement::try_new(
                            TextPartition::Unpartitioned,
                            Arc::from([0.5_f32; 3]),
                        )
                        .expect("a finite vector is a valid replacement"),
                    ),
                    ..effect
                },
                Some(InjectedPlanningFailure::Unavailable) | None => effect,
            })
            .collect()
    }

    /// Names the other element kind in every effect `injected` corrupts.
    pub(crate) fn corrupt_text_effects(
        effects: Vec<QueuedTextEffect>,
        injected: &[Option<InjectedPlanningFailure>],
    ) -> Vec<QueuedTextEffect> {
        effects
            .into_iter()
            .zip(injected)
            .map(|(mut effect, injected)| {
                if *injected == Some(InjectedPlanningFailure::Corrupt) {
                    effect.entity.kind = match effect.entity.kind {
                        IndexElementKind::Node => IndexElementKind::Edge,
                        IndexElementKind::Edge => IndexElementKind::Node,
                    };
                }
                effect
            })
            .collect()
    }

    /// Fails planning with an object-store error at the first entity
    /// `injected` makes unavailable, if any.
    pub(crate) fn fail_unavailable<T>(
        planned: Result<T>,
        injected: &[Option<InjectedPlanningFailure>],
        failed: impl FnOnce(usize, HelixDbError) -> Result<T>,
    ) -> Result<T> {
        let Some(position) = injected
            .iter()
            .position(|injected| *injected == Some(InjectedPlanningFailure::Unavailable))
        else {
            return planned;
        };
        failed(
            position,
            HelixDbError::ObjectStore(slatedb::object_store::Error::Generic {
                store: "injected",
                source: "injected planning outage".into(),
            }),
        )
    }
}

impl std::fmt::Debug for QueuePublisher {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueuePublisher")
            .finish_non_exhaustive()
    }
}

impl QueuePublisher {
    /// Installs publication against the writer's storage and shared caches.
    pub(crate) fn new(
        db: Arc<slatedb::Db>,
        backlog: Arc<IndexOperationBacklog>,
        store: Arc<QueueStore>,
        scope_gates: Arc<IndexScopeGates>,
        vector: VectorPublicationResources,
        limits: SearchIndexBatchLimits,
        text: TextPublicationResources,
    ) -> Arc<Self> {
        Arc::new(Self {
            db,
            backlog,
            store,
            scope_gates,
            vector,
            limits,
            text,
            schedules: Mutex::new(HashMap::new()),
            drained_commits: Mutex::new(VecDeque::new()),
            cursor: Mutex::new(None),
            attempts: Mutex::new(HashSet::new()),
            vector_commits: AtomicU64::new(0),
            metrics: QueuePublicationMetrics::default(),
            #[cfg(test)]
            hooks: test_hooks::PublicationHooks::default(),
        })
    }

    /// Returns deterministic test controls.
    #[cfg(test)]
    pub(crate) const fn hooks(&self) -> &test_hooks::PublicationHooks {
        &self.hooks
    }

    /// Returns publication counters.
    pub(crate) const fn metrics(&self) -> &QueuePublicationMetrics {
        &self.metrics
    }

    /// Returns the planning budget this publisher shares with builds.
    #[cfg(test)]
    pub(crate) fn planning_cache(&self) -> &Arc<VectorBuildCache> {
        &self.vector.planning_cache
    }

    /// Returns whether any generation retains outstanding or uncertain work.
    pub(crate) fn has_outstanding_work(&self) -> bool {
        !self.backlog.outstanding_targets().is_empty()
    }

    /// Picks the next eligible generation after the round-robin cursor.
    ///
    /// Candidates come from the in-memory ledger, so selecting work performs
    /// no storage read. Returns the earliest backoff deadline when every
    /// candidate is delayed.
    pub(crate) fn next_target(&self, in_flight: &HashSet<QueueTarget>, now: Instant) -> NextTarget {
        #[cfg(test)]
        if self.hooks.paused.load(Ordering::SeqCst) {
            return NextTarget::Idle;
        }
        let targets = self.backlog.outstanding_admissions();
        if targets.is_empty() {
            return NextTarget::Idle;
        }
        let mut cursor = self.cursor.lock();
        let schedules = self.schedules.lock();
        let start = cursor
            .and_then(|previous| targets.iter().position(|(target, _)| *target > previous))
            .unwrap_or(0);
        let mut earliest = None::<Instant>;
        for offset in 0..targets.len() {
            let (target, latest) = targets[(start + offset) % targets.len()];
            if in_flight.contains(&target) {
                continue;
            }
            if let Some(not_before) = schedules
                .get(&target)
                .and_then(|schedule| schedule.eligibility.waits_until(latest))
                && not_before > now
            {
                earliest = Some(earliest.map_or(not_before, |current| current.min(not_before)));
                continue;
            }
            *cursor = Some(target);
            return NextTarget::Ready(target);
        }
        earliest.map_or(NextTarget::Idle, NextTarget::Delayed)
    }

    /// Runs one bounded publication attempt for `target`.
    ///
    /// At most one attempt per target runs at a time, whoever calls: an
    /// attempt is its queue's only acknowledger, and reconciliation and
    /// discard rely on that. A call while another attempt holds the target
    /// returns [`PublicationOutcome::Retry`] without reading the queue or
    /// touching its schedule. Failures are classified by [`FailureKind`]: one
    /// entity's deterministic planning failure holds that entity back within
    /// the attempt, any other failure that leaves the writer usable retries
    /// the generation after backoff, and only errors that make the writer
    /// unusable (closed or fenced storage) are returned.
    pub(crate) async fn publish_once(&self, target: QueueTarget) -> Result<PublicationOutcome> {
        if !self.attempts.lock().insert(target) {
            return Ok(PublicationOutcome::Retry);
        }
        let _claim = AttemptClaim {
            attempts: &self.attempts,
            target,
        };
        // Read before the queue, so an operation admitted, or whose enqueue
        // commit returns, while the attempt runs makes a stalled target
        // eligible again.
        let admitted = self.backlog.latest_admission(target);
        let started = Instant::now();
        let outcome = match self.try_publish(target, admitted).await {
            Ok(outcome) => outcome,
            Err(error) => match FailureKind::of(&error) {
                FailureKind::Fatal => return Err(error),
                // A deterministic failure that no one entity's planning
                // raised has no entity to hold back.
                kind @ (FailureKind::Transient | FailureKind::Deterministic) => {
                    self.metrics.error_retries.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        %error,
                        ?kind,
                        index_id = target.index_id.get(),
                        generation = target.generation.get(),
                        "queued index publication failed; retrying after backoff"
                    );
                    PublicationOutcome::Retry
                }
            },
        };
        self.metrics.attempts.fetch_add(1, Ordering::Relaxed);
        self.metrics.attempt_micros.fetch_add(
            u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        match outcome {
            PublicationOutcome::Retry => {
                self.metrics.retry_attempts.fetch_add(1, Ordering::Relaxed);
            }
            PublicationOutcome::Deferred => {
                self.metrics
                    .deferred_attempts
                    .fetch_add(1, Ordering::Relaxed);
            }
            PublicationOutcome::Published { .. }
            | PublicationOutcome::Discarded { .. }
            | PublicationOutcome::Empty
            | PublicationOutcome::Trimmed
            | PublicationOutcome::Blocked
            | PublicationOutcome::Stalled => {}
        }
        // Only a vector commit retains a planning session; every other outcome
        // forgets the target's, so no session outlives the attempt that could
        // have written past it.
        match outcome {
            PublicationOutcome::Published { .. } => {}
            PublicationOutcome::Discarded { .. }
            | PublicationOutcome::Empty
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry
            | PublicationOutcome::Trimmed
            | PublicationOutcome::Blocked
            | PublicationOutcome::Stalled => {
                self.vector.planning_cache.forget_publication(target).await;
            }
        }
        self.reschedule(target, outcome, admitted);
        Ok(outcome)
    }

    /// Updates `target`'s schedule after an attempt that began when the
    /// target's latest admission was `admitted`.
    fn reschedule(
        &self,
        target: QueueTarget,
        outcome: PublicationOutcome,
        admitted: Option<Admission>,
    ) {
        let charged = self.backlog.has_charges(target);
        // Charged work that reads empty is not visible yet, for example a
        // reservation whose commit is still in flight: it waits like a hidden
        // build instead of being dispatched again at once.
        let unsettled = outcome == PublicationOutcome::Empty && charged;
        // A target this attempt drained is not scheduled again until new
        // work arrives, so nothing of its schedule is needed but its latest
        // vector commit, which a retained planning session may still match.
        let drained = !charged
            && matches!(
                outcome,
                PublicationOutcome::Published { .. } | PublicationOutcome::Discarded { .. }
            );
        let mut schedules = self.schedules.lock();
        if drained {
            let commit = schedules
                .remove(&target)
                .and_then(|schedule| schedule.last_vector_commit);
            drop(schedules);
            let mut commits = self.drained_commits.lock();
            commits.retain(|(previous, _)| *previous != target);
            commits.extend(commit.map(|commit| (target, commit)));
            if commits.len() > MAX_RETAINED_PUBLICATIONS {
                commits.pop_front();
            }
            return;
        }
        let default_limit = self.limits.max_entities().get();
        let schedule = schedules
            .entry(target)
            .or_insert_with(|| TargetSchedule::new(default_limit));
        match outcome {
            PublicationOutcome::Published { .. } => {
                schedule.failures = 0;
                schedule.failed_holds = 0;
                schedule.eligibility = Eligibility::Now;
                schedule.entity_limit = schedule.entity_limit.saturating_mul(2).min(default_limit);
                schedule.operation_limit = schedule
                    .operation_limit
                    .saturating_add(schedule.operation_limit.get());
            }
            // A retired generation never publishes again.
            PublicationOutcome::Discarded { .. } => {
                schedule.failures = 0;
                schedule.failed_holds = 0;
                schedule.eligibility = Eligibility::Now;
                schedule.held.clear();
            }
            PublicationOutcome::Empty if !unsettled => {
                schedules.remove(&target);
            }
            // Holding back a blocked entity is progress: the generation's
            // other entities can publish at once. A second entity failing to
            // plan with no publication between suggests the failure is not
            // the entities' own, so the next one waits (see `failed_holds`).
            PublicationOutcome::Blocked if schedule.failed_holds > 1 => {
                let backoff = RETRY_DELAY
                    .saturating_mul(1_u32 << (schedule.failed_holds - 1).min(9))
                    .min(MAX_BACKOFF);
                schedule.eligibility = Eligibility::After(Instant::now() + backoff);
            }
            PublicationOutcome::Trimmed | PublicationOutcome::Blocked => {
                schedule.eligibility = Eligibility::Now;
            }
            // Only a write, or a failed entity's due retry, can give a
            // held-back entity a repair, so polling would reread and decode
            // the queue for nothing.
            PublicationOutcome::Stalled => {
                schedule.failures = schedule.failures.saturating_add(1);
                let deadline = schedule
                    .held
                    .values()
                    .filter_map(|hold| match hold {
                        HeldEntity::Failed { retry, .. } => Some(*retry),
                        HeldEntity::Waiting { .. }
                        | HeldEntity::Draining { .. }
                        | HeldEntity::Repairing { .. } => None,
                    })
                    .fold(Instant::now() + MAX_STALLED_WAIT, Instant::min);
                schedule.eligibility = Eligibility::NewWork { admitted, deadline };
            }
            PublicationOutcome::Empty
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry => {
                schedule.failures = schedule.failures.saturating_add(1);
                // Deferred and unsettled work waits on another actor: an
                // activation or an in-flight commit.
                let ceiling = if matches!(
                    outcome,
                    PublicationOutcome::Deferred | PublicationOutcome::Empty
                ) {
                    MAX_DEFERRED_BACKOFF
                } else {
                    MAX_BACKOFF
                };
                let backoff = RETRY_DELAY
                    .saturating_mul(1_u32 << schedule.failures.min(9))
                    .min(ceiling);
                schedule.eligibility = Eligibility::After(Instant::now() + backoff);
            }
        }
    }

    /// Runs one attempt for `target`, whose latest admission was `admitted`
    /// before the attempt read anything.
    async fn try_publish(
        &self,
        target: QueueTarget,
        admitted: Option<Admission>,
    ) -> Result<PublicationOutcome> {
        // Taken before anything can return, so only an outcome of this
        // attempt whose durable effect it knows retains a queue for the next.
        let retained = self.store.retained().take(target);
        // Build, abort, cleanup, and compaction steps hold the generation's
        // ownership for a whole step, so it is classified before waiting for
        // ownership. Reconciliation, deferral, and discards touch only the
        // queue, which no step writes and only this attempt acknowledges, so
        // none of them waits: none occupies a worker task until a step ends,
        // and uncertain charges reconcile while a step runs instead of
        // throttling their logical index until it ends.
        let owner = GenerationOwner::of(
            load_generation_record(self.db.as_ref(), target)
                .await?
                .as_ref(),
        );
        if self.backlog.has_uncertain(target) {
            self.reconcile(target).await?;
        }
        match owner {
            GenerationOwner::Building => return Ok(PublicationOutcome::Deferred),
            GenerationOwner::Retired => return self.discard(target, retained, admitted).await,
            GenerationOwner::Active => {}
        }
        // Retirement while awaiting ownership is caught by the publication
        // transaction's own read of the record.
        let ownership = self.scope_gates.publication_permit(target).await;
        // A retained queue lacks every operation admitted since its read, so
        // holding back all of its entities proves nothing: a write may have
        // queued a repair. Its other entities publish first, like any newer
        // work, as do a draining entity's retained operations and a repair
        // in progress. Once only entities waiting for a newer operation or a
        // retry are left, it is read again as soon as anything was admitted
        // since its read, so retries that keep falling due never keep
        // publication on it. Only a stall on a fresh read waits for new work
        // (see `Eligibility::NewWork`); the stalled retained queue is dropped.
        let retained = retained.filter(|stored| {
            stored.admitted() == admitted || {
                let schedules = self.schedules.lock();
                let waiting = schedules.get(&target).map_or(0, |schedule| {
                    schedule
                        .held
                        .iter()
                        .filter(|(entity, hold)| {
                            matches!(hold, HeldEntity::Waiting { .. } | HeldEntity::Failed { .. })
                                && stored.contains(**entity)
                        })
                        .count()
                });
                waiting < stored.entities()
            }
        });
        if let Some(stored) = retained {
            let outcome = match stored.family() {
                QueueFamily::Vector => self.publish_vector(&ownership, stored).await?,
                QueueFamily::Text => self.publish_text(target, stored).await?,
            };
            if outcome != PublicationOutcome::Stalled {
                return Ok(outcome);
            }
        }
        let Some(stored) = self.read_queue(target, admitted).await? else {
            return Ok(PublicationOutcome::Empty);
        };
        match stored.family() {
            QueueFamily::Vector => self.publish_vector(&ownership, stored).await,
            QueueFamily::Text => self.publish_text(target, stored).await,
        }
    }

    /// Reads `target`'s queue from storage outside any transaction, counting
    /// every read, including one that finds the queue empty. `admitted` is
    /// the target's latest admission observed before the read.
    async fn read_queue(
        &self,
        target: QueueTarget,
        admitted: Option<Admission>,
    ) -> Result<Option<StoredQueue>> {
        let started = Instant::now();
        let stored = self
            .store
            .read(self.db.as_ref(), target)
            .await?
            .map(|stored| stored.observed_after(admitted));
        self.metrics.queue_reads.fetch_add(1, Ordering::Relaxed);
        self.metrics.queue_read_bytes.fetch_add(
            stored.as_ref().map_or(0, StoredQueue::encoded_bytes),
            Ordering::Relaxed,
        );
        self.metrics.queue_read_micros.fetch_add(
            u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        Ok(stored)
    }

    /// Acknowledges a bounded batch of a retired generation's operations
    /// without applying them, from `retained`, the queue the target's
    /// previous attempt left, or else from a read observed after `admitted`.
    ///
    /// A retired generation never serves again and lifecycle cleanup reclaims
    /// its physical rows, so its queued work is released rather than
    /// orphaned. Retirement is terminal, producers never route to a retired
    /// generation, and cleanup never touches the queue, so the
    /// acknowledgement needs neither an ownership read nor ownership. It
    /// stages nothing else, so it may fill the whole output budget.
    ///
    /// Each batch's charges are released as it commits, and the rest of the
    /// queue is retained for the next batch, so a backlog of any size is
    /// read and decoded once: its charges, which count toward the limits of
    /// the logical index a recreated index reuses, are released at the rate
    /// acknowledgements commit.
    async fn discard(
        &self,
        target: QueueTarget,
        retained: Option<StoredQueue>,
        admitted: Option<Admission>,
    ) -> Result<PublicationOutcome> {
        let stored = match retained {
            Some(stored) => stored,
            None => match self.read_queue(target, admitted).await? {
                Some(stored) => stored,
                None => return Ok(PublicationOutcome::Empty),
            },
        };
        let capacity = self.store.acknowledgement_capacity(
            target,
            OutputBudget {
                max_operations: self.limits.max_output_operations().get(),
                max_bytes: self.limits.max_output_bytes().get(),
            },
        );
        // Whole entities' oldest operations first, as every acknowledgement
        // a retained queue continues from names them.
        let discarded = stored
            .rotation(None)
            .flat_map(|queued| queued.iter())
            .take(capacity.get().min(MAX_DISCARDED_OPERATIONS))
            .map(QueuedOperation::id)
            .collect::<Vec<_>>();
        let transaction = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        self.store
            .stage_acknowledge(&transaction, target, &stored, &discarded)?;
        match transaction.commit().await {
            Ok(_) => {}
            // A definite conflict committed nothing.
            Err(error) if error.kind() == slatedb::ErrorKind::Transaction => {
                self.metrics
                    .commit_conflicts
                    .fetch_add(1, Ordering::Relaxed);
                self.store.retained().retain(target, stored, &[]);
                return Ok(PublicationOutcome::Retry);
            }
            Err(error) => {
                let error = HelixDbError::from_storage_commit(error);
                if FailureKind::of(&error) == FailureKind::Fatal {
                    return Err(error);
                }
                self.metrics
                    .uncertain_commits
                    .fetch_add(1, Ordering::Relaxed);
                self.backlog
                    .mark_acknowledgement_uncertain(discarded.iter().copied());
                tracing::warn!(%error, "retired queue discard outcome is uncertain");
                return Ok(PublicationOutcome::Retry);
            }
        }
        self.backlog.acknowledge(discarded.iter().copied());
        self.store.retained().retain(target, stored, &discarded);
        let operations = discarded.len() as u64;
        self.metrics
            .discarded_operations
            .fetch_add(operations, Ordering::Relaxed);
        tracing::info!(
            index_id = target.index_id.get(),
            generation = target.generation.get(),
            operations,
            "discarded queued operations of a retired index generation"
        );
        Ok(PublicationOutcome::Discarded { operations })
    }

    /// Resolves uncertain charges for `target` against a flushed read.
    ///
    /// A writer flush after the uncertainty was recorded makes any commit that
    /// was in flight durable and visible. An uncertain enqueue absent from the
    /// following read never committed and is released; an uncertain
    /// acknowledgement whose ID is absent committed and is counted as one.
    ///
    /// It reads only the queue, so it needs no generation ownership. It does
    /// need this attempt to be the queue's only acknowledger: an operation
    /// acknowledged between the read and the ledger update would be charged
    /// again as rediscovered.
    async fn reconcile(&self, target: QueueTarget) -> Result<()> {
        let ticket = self.backlog.begin_reconciliation();
        self.db.flush().await?;
        let stored = self.read_queue(target, None).await?;
        let present = stored
            .iter()
            .flat_map(StoredQueue::operations)
            .map(|operation| {
                (
                    operation.id(),
                    operation.entity(),
                    operation.retained_bytes(),
                )
            });
        let released = self.backlog.finish_reconciliation(ticket, target, present);
        if released > 0 {
            tracing::info!(
                index_id = target.index_id.get(),
                generation = target.generation.get(),
                released,
                "released uncertain queued index capacity after reconciliation"
            );
        }
        Ok(())
    }

    /// Publishes one batch of `stored`, which the attempt took or read, and
    /// retains what is left of it (see [`super::storage::RetainedQueues`]).
    async fn publish_vector(
        &self,
        ownership: &IndexGenerationPublicationPermit,
        stored: StoredQueue,
    ) -> Result<PublicationOutcome> {
        let target = ownership.target();
        // Vector batches are sized by planning admission, so only the cursor,
        // a trimmed operation ceiling, and the latest commit carry over
        // between attempts.
        let TargetSchedule {
            cursor,
            operation_limit,
            last_vector_commit,
            held,
            ..
        } = self.schedule(target);
        // The acknowledgement may take at most half of each output budget,
        // leaving its effects the rest: one filling the budget would leave a
        // hot entity's selection no room.
        let capacity = self.store.acknowledgement_capacity(
            target,
            OutputBudget {
                max_operations: self.limits.max_output_operations().get() / 2,
                max_bytes: self.limits.max_output_bytes().get() / 2,
            },
        );
        let selection = select_batch(
            &stored,
            cursor,
            &held,
            self.limits.max_entities().get(),
            operation_limit,
            capacity,
            self.limits.max_input_bytes().get(),
        );
        if selection.is_empty() {
            return Ok(PublicationOutcome::Stalled);
        }
        let transaction = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let Some(handle) = load_generation_record(&transaction, target)
            .await?
            .and_then(|record| ActiveIndexHandle::try_from_record(target.scope, &record))
        else {
            // Ownership changed after classification; classify again.
            return Ok(PublicationOutcome::Retry);
        };
        #[cfg(test)]
        {
            let barrier = self.hooks.before_planning.lock().take();
            if let Some((reached, release)) = barrier {
                let _ = reached.send(());
                let _ = release.await;
            }
        }
        // Collapsing reads only the decoded queue, so it fails the same way
        // on every attempt.
        let effects = match selection
            .iter()
            .map(|selected| collapse_vector(selected).map_err(|error| (selected, error)))
            .collect::<std::result::Result<Vec<_>, _>>()
        {
            Ok(effects) => effects,
            // Nothing committed: the next selection, past the held entity,
            // reuses the queue.
            Err((failed, error)) => {
                let outcome = self.isolate(target, std::slice::from_ref(failed), &error);
                self.store.retained().retain(target, stored, &[]);
                return Ok(outcome);
            }
        };
        #[cfg(test)]
        let injected = self.hooks.planning_failures(&selection);
        #[cfg(test)]
        let effects = test_hooks::corrupt_vector_effects(effects, &injected);
        // Admission reserves the whole selection's acknowledgement, which
        // bounds that of any prefix.
        let reserved = self.store.acknowledgement_output(
            target,
            &stored,
            &selection
                .iter()
                .flat_map(|selected| selected.operations.iter().map(|operation| operation.id()))
                .collect::<Vec<_>>(),
        )?;
        let cache_writes = vector::VectorCacheWriteSet::default();
        let commit =
            NonZeroU64::MIN.saturating_add(self.vector_commits.fetch_add(1, Ordering::Relaxed));
        let staged = stage_active_effects(
            &self.db,
            &transaction,
            ownership,
            &handle,
            &effects,
            self.limits,
            reserved,
            &self.vector,
            &cache_writes,
            last_vector_commit,
            commit,
        )
        .await;
        #[cfg(test)]
        let staged = test_hooks::fail_unavailable(staged, &injected, |position, error| {
            Ok(StagedEffects::Failed { position, error })
        });
        let (staged, retained) = match staged {
            Ok(StagedEffects::Prefix { staged, retained }) => (staged.get(), retained),
            Ok(StagedEffects::NoneFits) => {
                let outcome = self.shrink(target, &selection, 0);
                if outcome == PublicationOutcome::Blocked {
                    tracing::error!(
                        scope = ?target.scope,
                        entity = ?selection.first().map(|selected| selected.entity),
                        index_id = target.index_id.get(),
                        generation = target.generation.get(),
                        "one queued vector operation exceeds the publication output budget; \
                         its entity is held back until a later write supersedes it"
                    );
                }
                // Nothing committed: the next, smaller selection, or the one
                // past the held entity, reuses the queue.
                self.store.retained().retain(target, stored, &[]);
                return Ok(outcome);
            }
            Ok(StagedEffects::Failed { position, error })
                if FailureKind::of(&error) == FailureKind::Deterministic =>
            {
                let outcome =
                    self.isolate(target, std::slice::from_ref(&selection[position]), &error);
                // Nothing committed: the next selection, past the held
                // entity, reuses the queue.
                self.store.retained().retain(target, stored, &[]);
                return Ok(outcome);
            }
            // Planning proved the transaction cannot commit.
            Ok(StagedEffects::Failed { error, .. }) | Err(error)
                if error.is_transaction_conflict() =>
            {
                self.metrics
                    .commit_conflicts
                    .fetch_add(1, Ordering::Relaxed);
                self.store.retained().retain(target, stored, &[]);
                return Ok(PublicationOutcome::Retry);
            }
            Ok(StagedEffects::Failed { error, .. }) | Err(error) => return Err(error),
        };
        // Exactly the staged prefix is acknowledged; later entities stay
        // queued for the next attempt.
        let selection = &selection[..staged];
        let acknowledged = selection
            .iter()
            .flat_map(|selected| selected.operations.iter().map(|operation| operation.id()))
            .collect::<Vec<_>>();
        self.store
            .stage_acknowledge(&transaction, target, &stored, &acknowledged)?;
        let cache_effects = cache_writes.entries();
        let retirements = cache_effects
            .iter()
            .filter_map(|write| write.retirement().cloned())
            .collect::<Vec<_>>();
        #[cfg(test)]
        {
            *self.hooks.batch_reads.lock() = Some(self.vector.batch_reads);
            let barrier = self.hooks.before_commit.lock().take();
            if let Some((reached, release)) = barrier {
                let _ = reached.send(());
                let _ = release.await;
            }
            if self.hooks.fail_before_commit.swap(false, Ordering::SeqCst) {
                return Err(HelixDbError::InvariantViolation(
                    "injected publication failure before commit".to_string(),
                ));
            }
        }
        // Numbered before committing, so whatever the outcome, only a session
        // retained at this commit matches the target's next attempt.
        self.schedules
            .lock()
            .entry(target)
            .or_insert_with(|| TargetSchedule::new(self.limits.max_entities().get()))
            .last_vector_commit = Some(commit);
        // Fences are taken only once nothing but the commit remains, so an
        // early return never leaves one outstanding. The fenced commit
        // resolves them from the storage outcome: a conflict releases them
        // unchanged and any other outcome evicts their dirty rows.
        let pending_cache = cache_effects
            .iter()
            .filter_map(|write| self.vector.cache_registry.prepare_commit(write))
            .collect::<Vec<_>>();
        let fenced = !pending_cache.is_empty();
        let committed = match vector::commit_fenced(transaction, pending_cache).await {
            Ok(committed) => committed,
            // A definite conflict committed nothing.
            Err(error) if error.kind() == slatedb::ErrorKind::Transaction => {
                self.metrics
                    .commit_conflicts
                    .fetch_add(1, Ordering::Relaxed);
                self.store.retained().retain(target, stored, &[]);
                return Ok(PublicationOutcome::Retry);
            }
            Err(error) => {
                let error = HelixDbError::from_storage_commit(error);
                if FailureKind::of(&error) == FailureKind::Fatal {
                    return Err(error);
                }
                self.record_uncertain_commit(target, selection, &acknowledged);
                tracing::warn!(%error, "queued vector publication outcome is uncertain");
                return Ok(PublicationOutcome::Retry);
            }
        };
        // The acknowledgement is durable: release exactly its charges before
        // a cache effect can fail, so a post-commit error never strands
        // capacity or keeps an emptied generation schedulable.
        self.backlog.acknowledge(acknowledged.iter().copied());
        let operations = acknowledged.len() as u64;
        let entities = selection.len() as u64;
        self.advance_past(target, selection);
        self.store.retained().retain(target, stored, &acknowledged);
        self.metrics
            .published_operations
            .fetch_add(operations, Ordering::Relaxed);
        self.metrics
            .published_entities
            .fetch_add(entities, Ordering::Relaxed);
        self.metrics
            .committed_batches
            .fetch_add(1, Ordering::Relaxed);
        #[cfg(test)]
        if self.hooks.fail_after_commit.swap(false, Ordering::SeqCst) {
            return Err(HelixDbError::InvariantViolation(
                "injected publication failure after commit".to_string(),
            ));
        }
        if fenced && committed.is_none() {
            return Err(HelixDbError::InvariantViolation(
                "dirty vector cache rows committed without a storage sequence".to_string(),
            ));
        }
        for handle in retirements {
            self.vector.cache_registry.retire(&handle).await;
            if !self.vector.cache_registry.forget_validated_closed(&handle) {
                return Err(HelixDbError::InvariantViolation(
                    "published vector partition retirement did not close its cache entry"
                        .to_string(),
                ));
            }
        }
        // The session now mirrors exactly the committed rows. A target this
        // commit drained is not scheduled again until new work arrives, so
        // its session keeps only budget nothing else claims.
        match retained {
            Some(retained) => {
                let backlog = if self.backlog.has_charges(target) {
                    PublicationBacklog::Pending
                } else {
                    PublicationBacklog::Drained
                };
                self.vector
                    .planning_cache
                    .retain_publication(ownership, *retained, backlog)
                    .await;
            }
            None => self.vector.planning_cache.forget_publication(target).await,
        }
        Ok(PublicationOutcome::Published {
            operations,
            entities,
        })
    }
}

impl QueuePublisher {
    /// Returns `target`'s schedule, or a new one's, which resumes from the
    /// target's latest vector commit if it drained recently.
    fn schedule(&self, target: QueueTarget) -> TargetSchedule {
        let live = self.schedules.lock().get(&target).cloned();
        live.unwrap_or_else(|| TargetSchedule {
            last_vector_commit: self
                .drained_commits
                .lock()
                .iter()
                .find(|(drained, _)| *drained == target)
                .map(|(_, commit)| *commit),
            ..TargetSchedule::new(self.limits.max_entities().get())
        })
    }

    /// Returns how many targets have a schedule, and how many drained
    /// targets' latest vector commits are remembered.
    #[cfg(test)]
    pub(crate) fn scheduled_targets(&self) -> (usize, usize) {
        (
            self.schedules.lock().len(),
            self.drained_commits.lock().len(),
        )
    }

    /// Returns how many entities are held back after blocking, without
    /// collecting them. An entity draining after its repair published is not
    /// blocked.
    pub(crate) fn blocked_entity_count(&self) -> usize {
        self.schedules
            .lock()
            .values()
            .map(|schedule| {
                schedule
                    .held
                    .values()
                    .filter(|hold| hold.is_blocked())
                    .count()
            })
            .sum()
    }

    /// Returns every entity held back after blocking, by generation. An
    /// entity draining after its repair published is not blocked.
    pub(crate) fn blocked_entities(&self) -> Vec<(QueueTarget, IndexEntity)> {
        self.schedules
            .lock()
            .iter()
            .flat_map(|(target, schedule)| {
                schedule
                    .held
                    .iter()
                    .filter(|(_, hold)| hold.is_blocked())
                    .map(|(entity, _)| (*target, *entity))
            })
            .collect()
    }

    /// Makes every entity held back after failing to plan due for its retry,
    /// as if [`MAX_STALLED_WAIT`] had passed.
    #[cfg(any(
        test,
        all(feature = "production-coverage", feature = "index-lifecycle-testing")
    ))]
    pub(crate) fn make_failed_retries_due(&self) {
        let now = Instant::now();
        self.schedules
            .lock()
            .values_mut()
            .flat_map(|schedule| schedule.held.values_mut())
            .for_each(|hold| {
                let HeldEntity::Failed { retry, .. } = hold else {
                    return;
                };
                *retry = now;
            });
    }

    /// Shrinks the next selection of `target` after `selection`'s exact
    /// output crossed a budget before anything was published.
    ///
    /// The first `fitting` entities of a text epoch fit, so the retry takes
    /// only those. When not even the first entity fit beside the selection's
    /// acknowledgement, the retry takes half the operations, which shrinks the
    /// acknowledgement too; that is the only vector trim, since a vector batch
    /// otherwise commits its fitting prefix. A repair halves only its own
    /// width past the operations known not to publish (see [`HeldEntity`]),
    /// leaving the trims of other entities.
    ///
    /// Only a single operation that cannot fit, or a repair with nothing left
    /// to halve, is blocked: its entity is held back and the rotation moves
    /// past it. The trims that isolated it are kept, so the next of several
    /// unpublishable heads is held back in one attempt.
    fn shrink(
        &self,
        target: QueueTarget,
        selection: &[SelectedEntity<'_>],
        fitting: usize,
    ) -> PublicationOutcome {
        let operations = selection
            .iter()
            .map(|selected| selected.operations.len())
            .sum::<usize>();
        let mut schedules = self.schedules.lock();
        let schedule = schedules
            .entry(target)
            .or_insert_with(|| TargetSchedule::new(self.limits.max_entities().get()));
        // A held entity is only ever selected alone, as a repair.
        let repair = match selection {
            [only] => schedule.held.get(&only.entity).map(|hold| (only, *hold)),
            _ => None,
        };
        let outcome = match (repair, NonZeroUsize::new(operations / 2)) {
            (Some((only, hold)), _) => {
                let taken = only.taken().collect::<Vec<_>>();
                let (through, tried) = match hold {
                    // A failed entity that now plans but cannot fit is
                    // blocked like any other.
                    HeldEntity::Waiting { through } | HeldEntity::Failed { through, .. } => {
                        (through, taken.last().map_or(through, |last| last.id()))
                    }
                    // A draining entity already serves a state newer than
                    // every shorter prefix of its queue, so nothing is
                    // halved: its newest state is known not to publish.
                    HeldEntity::Draining { through } => {
                        let newest = taken.last().map_or(through, |last| last.id());
                        (newest, newest)
                    }
                    HeldEntity::Repairing { through, tried, .. } => (through, tried),
                };
                let known = taken
                    .iter()
                    .position(|operation| operation.id() == through)
                    .map_or(0, |position| position + 1);
                let halved = known + taken.len().saturating_sub(known) / 2;
                let (next, outcome) = match NonZeroUsize::new(halved).filter(|_| halved > known) {
                    Some(width) => (
                        HeldEntity::Repairing {
                            through,
                            tried,
                            width,
                        },
                        PublicationOutcome::Trimmed,
                    ),
                    None => {
                        schedule.cursor = Some(only.entity);
                        (
                            HeldEntity::Waiting { through: tried },
                            PublicationOutcome::Blocked,
                        )
                    }
                };
                schedule.held.insert(only.entity, next);
                outcome
            }
            (None, _) if fitting > 0 => {
                schedule.entity_limit = fitting;
                PublicationOutcome::Trimmed
            }
            (None, Some(half)) => {
                schedule.operation_limit = half;
                PublicationOutcome::Trimmed
            }
            (None, None) => {
                debug_assert_eq!(operations, 1, "an attempt selects at least one operation");
                schedule.held.extend(selection.iter().flat_map(|selected| {
                    selected.operations.iter().map(|operation| {
                        (
                            selected.entity,
                            HeldEntity::Waiting {
                                through: operation.id(),
                            },
                        )
                    })
                }));
                schedule.cursor = selection
                    .last()
                    .map(|blocked| blocked.entity)
                    .or(schedule.cursor);
                PublicationOutcome::Blocked
            }
        };
        if outcome == PublicationOutcome::Blocked {
            &self.metrics.blocked_attempts
        } else {
            &self.metrics.output_retries
        }
        .fetch_add(1, Ordering::Relaxed);
        outcome
    }

    /// Narrows `target`'s next attempt toward the entity of `failed`, a
    /// selection whose planning failed deterministically with `error`.
    ///
    /// A failed selection of one entity holds it back as
    /// [`HeldEntity::Failed`]: it waits for an operation newer than every
    /// state of it known not to publish, which a later write supplies and its
    /// repair then plans, or for its retry [`MAX_STALLED_WAIT`] from now,
    /// which plans the same operations again in case what failed was
    /// repaired. The rotation moves past it, so the rest of its generation
    /// keeps publishing; from the second entity in a row that fails without a
    /// publication between, the next attempt backs off instead (see
    /// `TargetSchedule::failed_holds`). Nothing is acknowledged or discarded,
    /// and nothing durable records the hold: a restarted publisher plans the
    /// entity again and holds it back again if it still fails. A text epoch
    /// is planned as a whole, so its failure does not name an entity; a
    /// failed epoch of several entities halves the text entity ceiling
    /// instead, until a failing epoch is one entity.
    ///
    /// The caller retains its queue after a hold, as after any blocked
    /// operation, since nothing committed: the rest of its entities publish
    /// from it without reading or decoding the queue again. Once only
    /// entities waiting for a newer operation or a retry are left in it, the
    /// next attempt reads storage as soon as anything was admitted since
    /// that queue was read ([`super::storage::RetainedQueues`]), so held
    /// entities whose retries keep falling due never keep publication on it:
    /// newer writes, including one that repairs a held entity, still publish.
    fn isolate(
        &self,
        target: QueueTarget,
        failed: &[SelectedEntity<'_>],
        error: &HelixDbError,
    ) -> PublicationOutcome {
        let mut schedules = self.schedules.lock();
        let schedule = schedules
            .entry(target)
            .or_insert_with(|| TargetSchedule::new(self.limits.max_entities().get()));
        let [only] = failed else {
            debug_assert!(failed.len() > 1, "an attempt selects at least one entity");
            schedule.entity_limit = failed.len() / 2;
            self.metrics.output_retries.fetch_add(1, Ordering::Relaxed);
            return PublicationOutcome::Trimmed;
        };
        // A narrowed repair failing says nothing of the wider states its
        // first attempt already found not to fit.
        let through = match schedule.held.get(&only.entity) {
            Some(HeldEntity::Repairing { tried, .. }) => *tried,
            Some(
                HeldEntity::Waiting { .. }
                | HeldEntity::Failed { .. }
                | HeldEntity::Draining { .. },
            )
            | None => only
                .taken()
                .last()
                .expect("a selected entity has operations")
                .id(),
        };
        schedule.held.insert(
            only.entity,
            HeldEntity::Failed {
                through,
                retry: Instant::now() + MAX_STALLED_WAIT,
            },
        );
        schedule.cursor = Some(only.entity);
        schedule.failed_holds = schedule.failed_holds.saturating_add(1);
        self.metrics
            .blocked_attempts
            .fetch_add(1, Ordering::Relaxed);
        tracing::error!(
            %error,
            scope = ?target.scope,
            entity = ?only.entity,
            index_id = target.index_id.get(),
            generation = target.generation.get(),
            retry_after_secs = MAX_STALLED_WAIT.as_secs(),
            "planning a queued index operation failed deterministically; its entity is held \
             back until a later write supersedes it or its retry is due"
        );
        PublicationOutcome::Blocked
    }

    /// Records a publication commit of `selection` whose outcome is unknown.
    ///
    /// The acknowledgement may have committed, so its capacity stays charged
    /// until a flushed read proves which IDs remain. A repair that may have
    /// published no longer holds its entity back: rediscovery holds it again
    /// if it did not.
    fn record_uncertain_commit(
        &self,
        target: QueueTarget,
        selection: &[SelectedEntity<'_>],
        acknowledged: &[QueuedOperationId],
    ) {
        self.metrics
            .uncertain_commits
            .fetch_add(1, Ordering::Relaxed);
        self.backlog
            .mark_acknowledgement_uncertain(acknowledged.iter().copied());
        self.advance_past(target, selection);
    }

    /// Starts `target`'s next batch after `selection`, whose entities are no
    /// longer held back: their committed publication, or one whose outcome
    /// is uncertain, superseded every hold (rediscovery holds an entity again
    /// if it did not commit). A repair that took operations past its
    /// acknowledgement keeps its entity draining, so the rest are republished
    /// by full-width repairs rather than older prefixes; if its commit did
    /// not land, the next full-width repair retries it.
    fn advance_past(&self, target: QueueTarget, selection: &[SelectedEntity<'_>]) {
        let mut schedules = self.schedules.lock();
        let schedule = schedules
            .entry(target)
            .or_insert_with(|| TargetSchedule::new(self.limits.max_entities().get()));
        for selected in selection {
            let (false, Some(acknowledged)) =
                (selected.superseding.is_empty(), selected.operations.last())
            else {
                schedule.held.remove(&selected.entity);
                continue;
            };
            schedule.held.insert(
                selected.entity,
                HeldEntity::Draining {
                    through: acknowledged.id(),
                },
            );
        }
        schedule.cursor = selection.last().map(|last| last.entity).or(schedule.cursor);
    }

    /// Publishes one text epoch of `stored`, which the attempt took or read,
    /// and retains what is left of it (see
    /// [`super::storage::RetainedQueues`]).
    async fn publish_text(
        &self,
        target: QueueTarget,
        stored: StoredQueue,
    ) -> Result<PublicationOutcome> {
        let TargetSchedule {
            cursor,
            entity_limit,
            operation_limit,
            held,
            ..
        } = self.schedule(target);
        // As for vectors, the acknowledgement leaves its epoch half of each
        // budget.
        let capacity = self.store.acknowledgement_capacity(
            target,
            OutputBudget {
                max_operations: self.text.limits.max_output_operations().get() / 2,
                max_bytes: self.text.limits.max_output_bytes().get() / 2,
            },
        );
        let selection = select_batch(
            &stored,
            cursor,
            &held,
            entity_limit.min(self.text.limits.max_entities().get()),
            operation_limit,
            capacity,
            self.limits.max_input_bytes().get(),
        );
        if selection.is_empty() {
            return Ok(PublicationOutcome::Stalled);
        }
        let transaction = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let Some(handle) = load_generation_record(&transaction, target)
            .await?
            .and_then(|record| ActiveIndexHandle::try_from_record(target.scope, &record))
        else {
            // Ownership changed after classification; classify again.
            return Ok(PublicationOutcome::Retry);
        };
        // As for vectors, collapsing fails the same way on every attempt.
        let effects = match selection
            .iter()
            .map(|selected| collapse_text(selected).map_err(|error| (selected, error)))
            .collect::<std::result::Result<Vec<_>, _>>()
        {
            Ok(effects) => effects,
            // Nothing committed: the next selection, past the held entity,
            // reuses the queue.
            Err((failed, error)) => {
                let outcome = self.isolate(target, std::slice::from_ref(failed), &error);
                self.store.retained().retain(target, stored, &[]);
                return Ok(outcome);
            }
        };
        #[cfg(test)]
        let injected = self.hooks.planning_failures(&selection);
        #[cfg(test)]
        let effects = test_hooks::corrupt_text_effects(effects, &injected);
        let acknowledged = selection
            .iter()
            .flat_map(|selected| selected.operations.iter().map(|operation| operation.id()))
            .collect::<Vec<_>>();
        let acknowledgement = self
            .store
            .acknowledgement_output(target, &stored, &acknowledged)?;
        let prepared = crate::index_lifecycle::text::active_batch::prepare_queued_text_epoch(
            &transaction,
            &handle,
            effects,
            self.text.limits,
            acknowledgement,
        )
        .await;
        #[cfg(test)]
        let prepared = test_hooks::fail_unavailable(prepared, &injected, |_, error| Err(error));
        let prepared = match prepared {
            Ok(prepared) => prepared,
            // Producers and builds admit every document to half of each
            // per-entity budget and bound its lone split within the split
            // ceilings, so one operation only fails here when the limits
            // shrank after admission.
            Err(error @ HelixDbError::ActiveTextMutationLimitExceeded { .. }) => {
                let outcome = self.shrink(target, &selection, selection.len() / 2);
                if outcome == PublicationOutcome::Blocked {
                    tracing::error!(
                        scope = ?target.scope,
                        entity = ?selection.first().map(|selected| selected.entity),
                        %error,
                        index_id = target.index_id.get(),
                        generation = target.generation.get(),
                        "one queued text operation exceeds the publication budget; its entity \
                         is held back until a later write supersedes it"
                    );
                }
                // Nothing committed: the next, smaller epoch, or the one past
                // the held entity, reuses the queue.
                self.store.retained().retain(target, stored, &[]);
                return Ok(outcome);
            }
            Err(error) if FailureKind::of(&error) == FailureKind::Deterministic => {
                let outcome = self.isolate(target, &selection, &error);
                // Nothing committed: the next, narrower epoch, or the one
                // past the held entity, reuses the queue.
                self.store.retained().retain(target, stored, &[]);
                return Ok(outcome);
            }
            Err(error) => return Err(error),
        };
        // Immutable split artifacts become durable before any committed
        // metadata references them; a later abort leaves only unreachable blobs.
        let published =
            crate::index_lifecycle::text::active_publication::publish_active_text_epoch(
                &self.text.object_store,
                &self.text.database,
                prepared,
                self.text.limits,
            )
            .await?;
        crate::index_lifecycle::text::active_batch::stage_active_text_epoch(
            &transaction,
            &published,
        )?;
        self.store
            .stage_acknowledge(&transaction, target, &stored, &acknowledged)?;
        #[cfg(test)]
        {
            let barrier = self.hooks.before_commit.lock().take();
            if let Some((reached, release)) = barrier {
                let _ = reached.send(());
                let _ = release.await;
            }
            if self.hooks.fail_before_commit.swap(false, Ordering::SeqCst) {
                return Err(HelixDbError::InvariantViolation(
                    "injected publication failure before commit".to_string(),
                ));
            }
        }
        match transaction.commit().await {
            Ok(_) => {}
            // A definite conflict committed nothing.
            Err(error) if error.kind() == slatedb::ErrorKind::Transaction => {
                self.metrics
                    .commit_conflicts
                    .fetch_add(1, Ordering::Relaxed);
                self.store.retained().retain(target, stored, &[]);
                return Ok(PublicationOutcome::Retry);
            }
            Err(error) => {
                let error = HelixDbError::from_storage_commit(error);
                if FailureKind::of(&error) == FailureKind::Fatal {
                    return Err(error);
                }
                self.record_uncertain_commit(target, &selection, &acknowledged);
                tracing::warn!(%error, "queued text publication outcome is uncertain");
                return Ok(PublicationOutcome::Retry);
            }
        }
        #[cfg(test)]
        if self
            .hooks
            .uncertain_after_commit
            .swap(false, Ordering::SeqCst)
        {
            self.record_uncertain_commit(target, &selection, &acknowledged);
            return Ok(PublicationOutcome::Retry);
        }
        self.backlog.acknowledge(acknowledged.iter().copied());
        let operations = acknowledged.len() as u64;
        let entities = selection.len() as u64;
        self.advance_past(target, &selection);
        self.store.retained().retain(target, stored, &acknowledged);
        self.metrics
            .published_operations
            .fetch_add(operations, Ordering::Relaxed);
        self.metrics
            .published_entities
            .fetch_add(entities, Ordering::Relaxed);
        self.metrics
            .committed_batches
            .fetch_add(1, Ordering::Relaxed);
        Ok(PublicationOutcome::Published {
            operations,
            entities,
        })
    }
}

/// Collapses one entity's ordered text prefix into its final replacement.
fn collapse_text(
    selected: &SelectedEntity<'_>,
) -> Result<crate::index_lifecycle::text::active_batch::QueuedTextEffect> {
    let Some(last) = selected.taken().last() else {
        return Err(HelixDbError::InvariantViolation(
            "a selected entity has no operations".to_string(),
        ));
    };
    let QueuedPayload::Text(last) = last.payload() else {
        return Err(HelixDbError::IndexCatalogCorruption(
            "text queue contains a vector operation".to_string(),
        ));
    };
    Ok(
        crate::index_lifecycle::text::active_batch::QueuedTextEffect {
            entity: selected.entity,
            replacement: last.replacement.as_ref().map(|replacement| {
                (
                    replacement.partition().clone(),
                    replacement.text().to_string(),
                )
            }),
        },
    )
}

/// Scheduling decision for the next publication task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NextTarget {
    /// Publish this generation now.
    Ready(QueueTarget),
    /// Work exists but every generation is backed off until this instant.
    Delayed(Instant),
    /// No outstanding work.
    Idle,
}

/// One entity's selected ordered operation prefix.
#[derive(Debug)]
pub(crate) struct SelectedEntity<'a> {
    pub(crate) entity: IndexEntity,
    /// Oldest operations, which the publication acknowledges; never empty.
    pub(crate) operations: Vec<&'a QueuedOperation>,
    /// Newer operations a repair took past one acknowledgement's worth: its
    /// effect publishes their newest state too, but they stay queued and
    /// later repairs republish it. Empty for every other selection.
    pub(crate) superseding: Vec<&'a QueuedOperation>,
}

impl<'a> SelectedEntity<'a> {
    /// Every operation whose state the effect collapses, oldest first.
    pub(crate) fn taken(&self) -> impl Iterator<Item = &'a QueuedOperation> + '_ {
        self.operations.iter().chain(&self.superseding).copied()
    }
}

/// Selects whole ordered per-entity prefixes of `queue` within entity,
/// operation, and input limits.
///
/// Entities are visited in the order of each one's oldest queued operation,
/// starting after `after` ([`StoredQueue::rotation`]), so repeated batches
/// rotate across entities. An entity's operations are taken oldest first and
/// never skipped; only the final selected entity may be cut to an ordered
/// prefix when the operation or input budget ends inside it. The first
/// selected entity always contributes at least one operation. No selection
/// names more than `max_acknowledged` operations, the most one
/// acknowledgement may carry beside its effects.
///
/// An entity in `held` never joins a batch, so it blocks no other entity.
/// When the rotation reaches it before any other entity and it has a repair
/// to try ([`HeldEntity`]), it is selected alone, with its repair's oldest
/// operations whatever their input bytes or the trimmed operation ceiling:
/// its effect materializes only its newest selected state, and its
/// operations are already decoded. Only its first `max_acknowledged` are
/// acknowledged; the rest are `superseding`. When the rotation reaches it
/// after other entities, the batch ends there: the next batch starts after
/// this one's last published entity, so the rotation never carries past a
/// repair, however the rest of its generation is written. A failed entity's
/// retry is due once its instant has passed when the selection runs. The
/// result is empty only when every queued entity is held back without a
/// repair to try.
///
/// Work is proportional to the entities visited and the operations
/// selected, never to the rest of the queue: the queue is grouped once per
/// read, and a held entity is checked against its newest operation alone.
pub(crate) fn select_batch<'a>(
    queue: &'a StoredQueue,
    after: Option<IndexEntity>,
    held: &HashMap<IndexEntity, HeldEntity>,
    max_entities: usize,
    max_operations: NonZeroUsize,
    max_acknowledged: NonZeroUsize,
    max_input_bytes: u64,
) -> Vec<SelectedEntity<'a>> {
    let max_operations = max_operations.min(max_acknowledged).get();
    let now = Instant::now();
    let mut selected = Vec::new();
    let mut input_bytes = 0_u64;
    let mut selected_operations = 0_usize;
    for queued in queue.rotation(after) {
        if selected.len() >= max_entities.max(1) {
            break;
        }
        let entity = queued.entity();
        match held
            .get(&entity)
            .map(|hold| hold.repair_width(queued.len(), queued.newest().id(), now))
        {
            Some(None) => continue,
            Some(Some(_)) if !selected.is_empty() => break,
            Some(Some(width)) => {
                let mut taken = queued.iter().take(width);
                let operations = taken
                    .by_ref()
                    .take(width.min(max_acknowledged.get()))
                    .collect();
                return vec![SelectedEntity {
                    entity,
                    operations,
                    superseding: taken.collect(),
                }];
            }
            None => {}
        }
        let mut prefix = Vec::new();
        for operation in queued.iter() {
            let bytes = operation.retained_bytes();
            let first_of_batch = selected.is_empty() && prefix.is_empty();
            if selected_operations == max_operations
                || (!first_of_batch && input_bytes.saturating_add(bytes) > max_input_bytes)
            {
                break;
            }
            input_bytes = input_bytes.saturating_add(bytes);
            selected_operations += 1;
            prefix.push(operation);
        }
        let exhausted = prefix.is_empty();
        if !exhausted {
            selected.push(SelectedEntity {
                entity,
                operations: prefix,
                superseding: Vec::new(),
            });
        }
        if exhausted || input_bytes >= max_input_bytes || selected_operations == max_operations {
            break;
        }
    }
    selected
}

/// Collapses one entity's ordered vector prefix into its physical effect.
fn collapse_vector(selected: &SelectedEntity<'_>) -> Result<QueuedVectorEffect> {
    let payloads = selected
        .taken()
        .map(|operation| match operation.payload() {
            QueuedPayload::Vector(payload) => Ok(payload),
            QueuedPayload::Text(_) => Err(HelixDbError::IndexCatalogCorruption(
                "vector queue contains a text operation".to_string(),
            )),
        })
        .collect::<Result<Vec<_>>>()?;
    let Some(last) = payloads.last() else {
        return Err(HelixDbError::InvariantViolation(
            "a selected entity has no operations".to_string(),
        ));
    };
    let stale = payloads
        .iter()
        .filter_map(|payload| payload.previous.as_ref())
        .fold(Vec::new(), |mut stale, partition| {
            if !stale.contains(partition) {
                stale.push(partition.clone());
            }
            stale
        });
    Ok(QueuedVectorEffect {
        entity_id: selected.entity.id,
        stale,
        replacement: last.replacement.clone(),
    })
}

/// Lifecycle ownership of one queued generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GenerationOwner {
    /// A hidden build owns the generation; its operations wait for activation.
    Building,
    /// The generation serves searches; the publisher applies its operations.
    Active,
    /// Aborting, dropping, dropped, or superseded: operations are discarded.
    Retired,
}

impl GenerationOwner {
    /// Classifies the record that still names a generation, if any.
    fn of(record: Option<&IndexRecordV2>) -> Self {
        match record.map(IndexRecordV2::state) {
            Some(IndexStateV2::Building { .. }) => Self::Building,
            Some(IndexStateV2::Active { .. }) => Self::Active,
            Some(
                IndexStateV2::Aborting { .. }
                | IndexStateV2::Dropping { .. }
                | IndexStateV2::Dropped { .. },
            )
            | None => Self::Retired,
        }
    }
}

/// Reads the canonical record owning `target`, returning it only when it
/// still names that generation.
///
/// Through a publication transaction the read registers a dependency on the
/// scope's index records, so a concurrent activation or retirement aborts
/// the publication instead of racing it.
async fn load_generation_record(
    read: &(impl DbReadOps + Sync),
    target: QueueTarget,
) -> Result<Option<IndexRecordV2>> {
    let prefix = ManagedIndexKey::data_prefix(
        target.scope,
        ScopedKey::logical_prefix(RecordKind::IndexRecord),
    );
    let mut rows = read.scan_prefix(&prefix, ..).await?;
    while let Some(row) = rows.next().await? {
        let record = decode_index_record(&row.value)?;
        if record.index_id() != target.index_id {
            continue;
        }
        let current = match record.state() {
            IndexStateV2::Building { .. }
            | IndexStateV2::Active { .. }
            | IndexStateV2::Aborting { .. }
            | IndexStateV2::Dropping { .. } => record.state().generation() == target.generation,
            IndexStateV2::Dropped { .. } => false,
        };
        return Ok(current.then_some(record));
    }
    Ok(None)
}

#[cfg(all(
    feature = "production-coverage",
    feature = "index-lifecycle-testing",
    not(test)
))]
#[path = "../../../tests/production_support/queue_publication.rs"]
pub(crate) mod production_contracts;

/// What retrying a failed publication can change, which decides how the
/// failure is handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FailureKind {
    /// The writer can make no further durable progress (closed or fenced
    /// storage): the error is returned and publication stops.
    Fatal,
    /// Storage or object-store I/O, a conflict, a lifecycle race, or a
    /// cancellation: the same batch may publish later, so the generation
    /// backs off and retries it whole, holding nothing back.
    Transient,
    /// The input itself cannot be planned (corrupt or invariant-violating
    /// state, or a payload its index rejects), so retrying the same input
    /// fails the same way. An entity whose planning fails like this is held
    /// back ([`QueuePublisher::isolate`]) while the rest of its generation
    /// publishes; a failure outside one entity's planning has no entity to
    /// hold back and retries like a transient one.
    Deterministic,
}

impl FailureKind {
    /// Classifies `error` by its variant, never its message. Every variant
    /// is named, so a new one cannot go unclassified.
    pub(crate) fn of(error: &HelixDbError) -> Self {
        match error {
            HelixDbError::DatabaseClosed | HelixDbError::WriterFencedCommitOutcomeUnknown => {
                Self::Fatal
            }
            HelixDbError::Storage(error)
                if matches!(error.kind(), slatedb::ErrorKind::Closed(_)) =>
            {
                Self::Fatal
            }
            HelixDbError::Storage(_)
            | HelixDbError::ObjectStore(_)
            | HelixDbError::TransactionConflict(_)
            | HelixDbError::RequestReadViewChanged
            | HelixDbError::QueryDeadlineExceeded
            | HelixDbError::QueryCancelledByReaderRetirement
            | HelixDbError::StaleIndexGeneration { .. }
            | HelixDbError::IndexBusy { .. }
            | HelixDbError::IndexBackpressure { .. }
            | HelixDbError::IndexBuildBlocked { .. }
            | HelixDbError::IdentifierAllocationFailed { .. } => Self::Transient,
            HelixDbError::Encoding(_)
            | HelixDbError::InvalidNodeId(_)
            | HelixDbError::NodeNotFound(_)
            | HelixDbError::EdgeNotFound { .. }
            | HelixDbError::Config(_)
            | HelixDbError::IndexLifecycleUnavailable { .. }
            | HelixDbError::SecondaryLifecycleSteppingRequiresDisabledMode
            | HelixDbError::MigrationSteppingRequiresDisabledMode
            | HelixDbError::ActiveTextMutationLimitExceeded { .. }
            | HelixDbError::IndexOperationBatchTooLarge { .. }
            | HelixDbError::InvalidIndexSourceData { .. }
            | HelixDbError::InvalidIndexV2Model(_)
            | HelixDbError::SecondaryIndexValue(_)
            | HelixDbError::MigrationRequired { .. }
            | HelixDbError::WriterMigrationRequired { .. }
            | HelixDbError::UnsupportedIndexStorageVersion { .. }
            | HelixDbError::IdentifierExhausted(_)
            | HelixDbError::IndexCatalogCorruption(_)
            | HelixDbError::InvalidVectorConfig(_)
            | HelixDbError::InvalidVectorItem(_)
            | HelixDbError::Query(_)
            | HelixDbError::Planner(_)
            | HelixDbError::InvalidQueryJson(_)
            | HelixDbError::WriterModeRequired { .. }
            | HelixDbError::ReaderModeRequired { .. }
            | HelixDbError::IndexAlreadyExists(_)
            | HelixDbError::IndexDefinitionConflict { .. }
            | HelixDbError::IndexOperationNotFound { .. }
            | HelixDbError::IndexOperationNotAbortable { .. }
            | HelixDbError::IndexNotFound(_)
            | HelixDbError::UniqueConstraintViolation { .. }
            | HelixDbError::UnsupportedUniqueIndexValueType { .. }
            | HelixDbError::InvalidDimension { .. }
            | HelixDbError::InvalidVectorComponent { .. }
            | HelixDbError::VectorComponentMagnitudeExceeded { .. }
            | HelixDbError::ZeroNormCosineVector
            | HelixDbError::LegacyZeroNormCosineVector { .. }
            | HelixDbError::InvariantViolation(_) => Self::Deterministic,
        }
    }
}
