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
//! 3. Take the queue the target's previous commit left
//!    ([`super::storage::RetainedQueues`]), or read the resolved queue outside
//!    any transaction (no read dependency).
//! 4. Select a bounded batch: whole ordered per-entity prefixes, rotating the
//!    starting entity for fairness, never skipping an earlier operation, and
//!    never naming more IDs than an acknowledgement may carry beside its
//!    effects.
//! 5. Open a serializable publication transaction and re-read the canonical
//!    record through it, so a generation that retired while the attempt
//!    awaited ownership, or a concurrent lifecycle change, retries instead.
//! 6. Stage the longest prefix of entities whose exact output fits beside the
//!    acknowledgement: vectors through the build planner
//!    ([`crate::index_lifecycle::vector::publication`]), text as one epoch.
//! 7. Stage one acknowledgement naming exactly the published IDs.
//! 8. Commit through the vector cache's commit fence, release accounting and
//!    retain the rest of the queue, then retire emptied partition caches and
//!    retain the vector planning session for the target's next attempt: in a
//!    fair share of the planning budget while the target has work left, in
//!    spare budget once drained.
//!
//! Only this attempt acknowledges its generation's queue: callers never run
//! two attempts for one generation at once (the worker skips in-flight
//! targets). Conflicts and uncertain outcomes discard all prepared work,
//! including the planning session; the next attempt rediscovers durable
//! state instead of reusing an acknowledgement.

use std::collections::{HashMap, HashSet};
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
    QueueFamily, QueuedOperation, QueuedPayload,
};
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::vector::publication::{
    stage_active_effects, QueuedVectorEffect, StagedEffects,
};
use crate::index_lifecycle::vector::{PublicationBacklog, VectorBuildCache};
use crate::index_lifecycle::{
    ActiveIndexHandle, IndexGenerationPublicationPermit, IndexRecordV2, IndexScopeGates,
    IndexStateV2,
};
use crate::search::vector::{self, SimHasherRegistry, VectorCacheRegistry};

use super::backlog::IndexOperationBacklog;
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
    /// Exact output crossed a budget before anything fit; retry immediately
    /// with fewer text entities, or fewer operations.
    Trimmed,
    /// One operation's effect and acknowledgement cannot fit an output budget.
    Blocked,
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
#[derive(Debug, Clone, Copy)]
struct TargetSchedule {
    /// Last entity whose prefix was published; the next batch starts after it.
    cursor: Option<IndexEntity>,
    /// Current text entity ceiling, reduced when a text epoch crossed a
    /// budget. Vector batches end at the first entity that does not fit.
    entity_limit: usize,
    /// Current operation ceiling, reduced when not even the first entity fit
    /// beside the selection's acknowledgement.
    operation_limit: NonZeroUsize,
    /// Publisher sequence number of the target's latest vector commit; a
    /// retained planning session is reused only at exactly this commit.
    last_vector_commit: Option<NonZeroU64>,
    /// Earliest instant the target is eligible again.
    not_before: Option<Instant>,
    /// Consecutive attempts without progress.
    failures: u32,
}

impl TargetSchedule {
    /// Starts at `entity_limit` with no cursor, backoff, or operation ceiling.
    const fn new(entity_limit: usize) -> Self {
        Self {
            cursor: None,
            entity_limit,
            operation_limit: NonZeroUsize::MAX,
            last_vector_commit: None,
            not_before: None,
            failures: 0,
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
    schedules: Mutex<HashMap<QueueTarget, TargetSchedule>>,
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

    use std::sync::atomic::AtomicBool;

    use parking_lot::Mutex;
    use tokio::sync::oneshot;

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
        /// Row-batch fetch policy of the last staged vector attempt's
        /// mutation indexes.
        pub(crate) batch_reads: Mutex<Option<crate::batch_reads::BatchReads>>,
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
        let targets = self.backlog.outstanding_targets();
        if targets.is_empty() {
            return NextTarget::Idle;
        }
        let mut cursor = self.cursor.lock();
        let schedules = self.schedules.lock();
        let start = cursor
            .and_then(|previous| targets.iter().position(|target| *target > previous))
            .unwrap_or(0);
        let mut earliest = None::<Instant>;
        for offset in 0..targets.len() {
            let target = targets[(start + offset) % targets.len()];
            if in_flight.contains(&target) {
                continue;
            }
            if let Some(not_before) = schedules
                .get(&target)
                .and_then(|schedule| schedule.not_before)
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
    /// touching its schedule. Retryable storage outcomes are classified here;
    /// only errors that make the writer unusable (closed or fenced storage)
    /// are returned.
    pub(crate) async fn publish_once(&self, target: QueueTarget) -> Result<PublicationOutcome> {
        if !self.attempts.lock().insert(target) {
            return Ok(PublicationOutcome::Retry);
        }
        let _claim = AttemptClaim {
            attempts: &self.attempts,
            target,
        };
        let started = Instant::now();
        let outcome = match self.try_publish(target).await {
            Ok(outcome) => outcome,
            Err(error) if is_fatal(&error) => return Err(error),
            Err(error) => {
                self.metrics.error_retries.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    %error,
                    index_id = target.index_id.get(),
                    generation = target.generation.get(),
                    "queued index publication failed; retrying after backoff"
                );
                PublicationOutcome::Retry
            }
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
            | PublicationOutcome::Blocked => {}
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
            | PublicationOutcome::Blocked => {
                self.vector.planning_cache.forget_publication(target).await;
            }
        }
        self.reschedule(target, outcome);
        Ok(outcome)
    }

    fn reschedule(&self, target: QueueTarget, outcome: PublicationOutcome) {
        // Charged work that reads empty is not visible yet, for example a
        // reservation whose commit is still in flight: it waits like a hidden
        // build instead of being dispatched again at once.
        let unsettled = outcome == PublicationOutcome::Empty && self.backlog.has_charges(target);
        let mut schedules = self.schedules.lock();
        let default_limit = self.limits.max_entities().get();
        let schedule = schedules
            .entry(target)
            .or_insert(TargetSchedule::new(default_limit));
        match outcome {
            PublicationOutcome::Published { .. } => {
                schedule.failures = 0;
                schedule.not_before = None;
                schedule.entity_limit = schedule.entity_limit.saturating_mul(2).min(default_limit);
                schedule.operation_limit = schedule
                    .operation_limit
                    .saturating_add(schedule.operation_limit.get());
            }
            PublicationOutcome::Discarded { .. } => {
                schedule.failures = 0;
                schedule.not_before = None;
            }
            PublicationOutcome::Empty if !unsettled => {
                schedules.remove(&target);
            }
            PublicationOutcome::Trimmed => {
                schedule.not_before = None;
            }
            PublicationOutcome::Empty
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry
            | PublicationOutcome::Blocked => {
                schedule.failures = schedule.failures.saturating_add(1);
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
                schedule.not_before = Some(Instant::now() + backoff);
            }
        }
    }

    async fn try_publish(&self, target: QueueTarget) -> Result<PublicationOutcome> {
        // Taken before anything can return, so only this attempt's own
        // successful commit retains a queue for the next one.
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
            GenerationOwner::Retired => return self.discard(target).await,
            GenerationOwner::Active => {}
        }
        // Retirement while awaiting ownership is caught by the publication
        // transaction's own read of the record.
        let ownership = self.scope_gates.publication_permit(target).await;
        let stored = match retained {
            Some(stored) => stored,
            None => {
                let Some(stored) = self.read_queue(target).await? else {
                    return Ok(PublicationOutcome::Empty);
                };
                stored
            }
        };
        match stored.queue().family() {
            QueueFamily::Vector => self.publish_vector(&ownership, &stored).await,
            QueueFamily::Text => self.publish_text(target, &stored).await,
        }
    }

    /// Reads `target`'s queue from storage outside any transaction, counting
    /// every read, including one that finds the queue empty.
    async fn read_queue(&self, target: QueueTarget) -> Result<Option<StoredQueue>> {
        let started = Instant::now();
        let stored = self.store.read(self.db.as_ref(), target).await?;
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
    /// without applying them.
    ///
    /// A retired generation never serves again and lifecycle cleanup reclaims
    /// its physical rows, so its queued work is released rather than
    /// orphaned. Retirement is terminal, producers never route to a retired
    /// generation, and cleanup never touches the queue, so the
    /// acknowledgement needs neither an ownership read nor ownership. It
    /// stages nothing else, so it may fill the whole output budget.
    async fn discard(&self, target: QueueTarget) -> Result<PublicationOutcome> {
        let Some(stored) = self.read_queue(target).await? else {
            return Ok(PublicationOutcome::Empty);
        };
        let capacity = self.store.acknowledgement_capacity(
            target,
            OutputBudget {
                max_operations: self.limits.max_output_operations().get(),
                max_bytes: self.limits.max_output_bytes().get(),
            },
        );
        let discarded = stored
            .queue()
            .operations()
            .iter()
            .take(capacity.get().min(MAX_DISCARDED_OPERATIONS))
            .map(QueuedOperation::id)
            .collect::<Vec<_>>();
        let transaction = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        self.store
            .stage_acknowledge(&transaction, target, &stored, &discarded)?;
        match transaction.commit().await {
            Ok(_) => {}
            Err(error) if error.kind() == slatedb::ErrorKind::Transaction => {
                self.metrics
                    .commit_conflicts
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(PublicationOutcome::Retry);
            }
            Err(error) => {
                let error = HelixDbError::from_storage_commit(error);
                if is_fatal(&error) {
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
        let stored = self.read_queue(target).await?;
        let present = stored
            .iter()
            .flat_map(|stored| stored.queue().operations())
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

    async fn publish_vector(
        &self,
        ownership: &IndexGenerationPublicationPermit,
        stored: &StoredQueue,
    ) -> Result<PublicationOutcome> {
        let target = ownership.target();
        let queue = stored.queue();
        // Vector batches are sized by planning admission, so only the cursor,
        // a trimmed operation ceiling, and the latest commit carry over
        // between attempts.
        let TargetSchedule {
            cursor,
            operation_limit,
            last_vector_commit,
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
            queue.operations(),
            cursor,
            self.limits.max_entities().get(),
            operation_limit.min(capacity),
            self.limits.max_input_bytes().get(),
        );
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
        let effects = selection
            .iter()
            .map(|selected| collapse_vector(selected))
            .collect::<Result<Vec<_>>>()?;
        // Admission reserves the whole selection's acknowledgement, which
        // bounds that of any prefix.
        let reserved = self.store.acknowledgement_output(
            target,
            stored,
            &selection
                .iter()
                .flat_map(|selected| selected.operations.iter().map(|operation| operation.id()))
                .collect::<Vec<_>>(),
        )?;
        let cache_writes = vector::VectorCacheWriteSet::default();
        let commit =
            NonZeroU64::MIN.saturating_add(self.vector_commits.fetch_add(1, Ordering::Relaxed));
        let (staged, retained) = match stage_active_effects(
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
        .await
        {
            Ok(StagedEffects::Prefix { staged, retained }) => (staged.get(), retained),
            Ok(StagedEffects::NoneFits) => {
                let outcome = self.shrink(target, &selection, 0);
                if outcome == PublicationOutcome::Blocked {
                    tracing::error!(
                        index_id = target.index_id.get(),
                        generation = target.generation.get(),
                        "one queued vector operation exceeds the publication output budget"
                    );
                }
                return Ok(outcome);
            }
            // Planning proved the transaction cannot commit.
            Err(error) if error.is_transaction_conflict() => {
                self.metrics
                    .commit_conflicts
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(PublicationOutcome::Retry);
            }
            Err(error) => return Err(error),
        };
        // Exactly the staged prefix is acknowledged; later entities stay
        // queued for the next attempt.
        let selection = &selection[..staged];
        let acknowledged = selection
            .iter()
            .flat_map(|selected| selected.operations.iter().map(|operation| operation.id()))
            .collect::<Vec<_>>();
        self.store
            .stage_acknowledge(&transaction, target, stored, &acknowledged)?;
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
            .or_insert(TargetSchedule::new(self.limits.max_entities().get()))
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
            Err(error) if error.kind() == slatedb::ErrorKind::Transaction => {
                self.metrics
                    .commit_conflicts
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(PublicationOutcome::Retry);
            }
            Err(error) => {
                let error = HelixDbError::from_storage_commit(error);
                if is_fatal(&error) {
                    return Err(error);
                }
                // The acknowledgement may have committed: keep capacity
                // charged until a flushed read proves which IDs remain.
                self.metrics
                    .uncertain_commits
                    .fetch_add(1, Ordering::Relaxed);
                self.backlog
                    .mark_acknowledgement_uncertain(acknowledged.iter().copied());
                tracing::warn!(%error, "queued vector publication outcome is uncertain");
                return Ok(PublicationOutcome::Retry);
            }
        };
        // The acknowledgement is durable: release exactly its charges before
        // a cache effect can fail, so a post-commit error never strands
        // capacity or keeps an emptied generation schedulable.
        self.backlog.acknowledge(acknowledged.iter().copied());
        self.store.retained().retain(target, stored, &acknowledged);
        let operations = acknowledged.len() as u64;
        let entities = selection.len() as u64;
        self.metrics
            .published_operations
            .fetch_add(operations, Ordering::Relaxed);
        self.metrics
            .published_entities
            .fetch_add(entities, Ordering::Relaxed);
        self.metrics
            .committed_batches
            .fetch_add(1, Ordering::Relaxed);
        if let Some(last) = selection.last() {
            self.advance_cursor(target, last.entity);
        }
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
    /// Returns `target`'s schedule, or a new one's.
    fn schedule(&self, target: QueueTarget) -> TargetSchedule {
        self.schedules
            .lock()
            .get(&target)
            .copied()
            .unwrap_or(TargetSchedule::new(self.limits.max_entities().get()))
    }

    /// Shrinks the next selection of `target` after `selection`'s exact
    /// output crossed a budget before anything was published.
    ///
    /// The first `fitting` entities of a text epoch fit, so the retry takes
    /// only those. When not even the first entity fit beside the selection's
    /// acknowledgement, the retry takes half the operations, which shrinks the
    /// acknowledgement too; that is the only vector trim, since a vector batch
    /// otherwise commits its fitting prefix. Only a single operation that
    /// cannot fit is blocked.
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
            .or_insert(TargetSchedule::new(self.limits.max_entities().get()));
        if fitting == 0 {
            let Some(half) = NonZeroUsize::new(operations / 2) else {
                self.metrics
                    .blocked_attempts
                    .fetch_add(1, Ordering::Relaxed);
                return PublicationOutcome::Blocked;
            };
            schedule.operation_limit = half;
        } else {
            schedule.entity_limit = fitting;
        }
        self.metrics.output_retries.fetch_add(1, Ordering::Relaxed);
        PublicationOutcome::Trimmed
    }

    fn advance_cursor(&self, target: QueueTarget, last: IndexEntity) {
        self.schedules
            .lock()
            .entry(target)
            .or_insert(TargetSchedule::new(self.limits.max_entities().get()))
            .cursor = Some(last);
    }

    async fn publish_text(
        &self,
        target: QueueTarget,
        stored: &StoredQueue,
    ) -> Result<PublicationOutcome> {
        let queue = stored.queue();
        let TargetSchedule {
            cursor,
            entity_limit,
            operation_limit,
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
            queue.operations(),
            cursor,
            entity_limit.min(self.text.limits.max_entities().get()),
            operation_limit.min(capacity),
            self.limits.max_input_bytes().get(),
        );
        let transaction = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        let Some(handle) = load_generation_record(&transaction, target)
            .await?
            .and_then(|record| ActiveIndexHandle::try_from_record(target.scope, &record))
        else {
            // Ownership changed after classification; classify again.
            return Ok(PublicationOutcome::Retry);
        };
        let effects = selection
            .iter()
            .map(collapse_text)
            .collect::<Result<Vec<_>>>()?;
        let acknowledged = selection
            .iter()
            .flat_map(|selected| selected.operations.iter().map(|operation| operation.id()))
            .collect::<Vec<_>>();
        let acknowledgement = self
            .store
            .acknowledgement_output(target, stored, &acknowledged)?;
        let prepared = match crate::index_lifecycle::text::active_batch::prepare_queued_text_epoch(
            &transaction,
            &handle,
            effects,
            self.text.limits,
            acknowledgement,
        )
        .await
        {
            Ok(prepared) => prepared,
            // Producers and builds admit every document to half of each
            // per-entity budget and bound its lone split within the split
            // ceilings, so one operation only fails here when the limits
            // shrank after admission.
            Err(error @ HelixDbError::ActiveTextMutationLimitExceeded { .. }) => {
                let outcome = self.shrink(target, &selection, selection.len() / 2);
                if outcome == PublicationOutcome::Blocked {
                    tracing::error!(
                        entity = ?selection.first().map(|selected| selected.entity),
                        %error,
                        index_id = target.index_id.get(),
                        generation = target.generation.get(),
                        "one queued text operation exceeds the publication budget"
                    );
                }
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
            .stage_acknowledge(&transaction, target, stored, &acknowledged)?;
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
            Err(error) if error.kind() == slatedb::ErrorKind::Transaction => {
                self.metrics
                    .commit_conflicts
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(PublicationOutcome::Retry);
            }
            Err(error) => {
                let error = HelixDbError::from_storage_commit(error);
                if is_fatal(&error) {
                    return Err(error);
                }
                self.metrics
                    .uncertain_commits
                    .fetch_add(1, Ordering::Relaxed);
                self.backlog
                    .mark_acknowledgement_uncertain(acknowledged.iter().copied());
                tracing::warn!(%error, "queued text publication outcome is uncertain");
                return Ok(PublicationOutcome::Retry);
            }
        }
        self.backlog.acknowledge(acknowledged.iter().copied());
        self.store.retained().retain(target, stored, &acknowledged);
        let operations = acknowledged.len() as u64;
        let entities = selection.len() as u64;
        self.metrics
            .published_operations
            .fetch_add(operations, Ordering::Relaxed);
        self.metrics
            .published_entities
            .fetch_add(entities, Ordering::Relaxed);
        self.metrics
            .committed_batches
            .fetch_add(1, Ordering::Relaxed);
        if let Some(last) = selection.last() {
            self.advance_cursor(target, last.entity);
        }
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
    let Some(last) = selected.operations.last() else {
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
    pub(crate) operations: Vec<&'a QueuedOperation>,
}

/// Selects whole ordered per-entity prefixes within entity, operation, and
/// input limits.
///
/// Entities are visited in first-appearance order starting after `after`, so
/// repeated batches rotate across entities. An entity's operations are taken
/// oldest first and never skipped; only the final selected entity may be cut
/// to an ordered prefix when the operation or input budget ends inside it.
/// The first entity always contributes at least one operation.
pub(crate) fn select_batch(
    operations: &[QueuedOperation],
    after: Option<IndexEntity>,
    max_entities: usize,
    max_operations: NonZeroUsize,
    max_input_bytes: u64,
) -> Vec<SelectedEntity<'_>> {
    let mut order = Vec::new();
    let mut grouped: HashMap<IndexEntity, Vec<&QueuedOperation>> = HashMap::new();
    for operation in operations {
        grouped
            .entry(operation.entity())
            .or_insert_with(|| {
                order.push(operation.entity());
                Vec::new()
            })
            .push(operation);
    }
    let start = after
        .and_then(|entity| order.iter().position(|candidate| *candidate == entity))
        .map_or(0, |position| position + 1);
    let mut selected = Vec::new();
    let mut input_bytes = 0_u64;
    let mut selected_operations = 0_usize;
    for offset in 0..order.len() {
        if selected.len() >= max_entities.max(1) {
            break;
        }
        let entity = order[(start + offset) % order.len()];
        let mut prefix = Vec::new();
        for operation in grouped.remove(&entity).unwrap_or_default() {
            let bytes = operation.retained_bytes();
            let first_of_batch = selected.is_empty() && prefix.is_empty();
            if selected_operations == max_operations.get()
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
            });
        }
        if exhausted
            || input_bytes >= max_input_bytes
            || selected_operations == max_operations.get()
        {
            break;
        }
    }
    selected
}

/// Collapses one entity's ordered vector prefix into its physical effect.
fn collapse_vector(selected: &SelectedEntity<'_>) -> Result<QueuedVectorEffect> {
    let payloads = selected
        .operations
        .iter()
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

/// Errors after which this writer can no longer make durable progress.
fn is_fatal(error: &HelixDbError) -> bool {
    matches!(
        error,
        HelixDbError::DatabaseClosed | HelixDbError::WriterFencedCommitOutcomeUnknown
    ) || matches!(
        error,
        HelixDbError::Storage(error) if matches!(error.kind(), slatedb::ErrorKind::Closed(_))
    )
}
