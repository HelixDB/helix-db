//! Bounded outbox driver for hidden vector construction.
//!
//! Each source step plans deterministic HNSW writes in a disposable
//! transaction, admits the complete last-write-wins vector write set, and then
//! applies those captured writes in the outbox transaction. The outbox
//! transaction also owns tenant mappings, builder-applied state, and the next
//! durable checkpoint. Writes made during a build are queued for the hidden
//! generation and published only after activation, by the same planner
//! ([`plan_and_apply`], see [`super::publication`]).
//!
//! The decoded rows planning reads are kept in one bounded
//! [`VectorBuildSession`] per build, and one per queue publication target,
//! that outlives its step or attempt only after that commits: see
//! [`RetainedVectorBuild`] and [`VectorPublicationCheckpoint`] for why a
//! matching checkpoint proves the cached rows still equal the committed rows
//! of a generation its owner writes exclusively. Every retained and
//! checked-out session shares one budget ([`VectorBuildCache`]).
//!
//! No vector row codec is defined here. Physical reads and writes remain behind
//! [`crate::search::vector::VectorIndex`] and the typed `encoding/v2` boundary.

use std::any::Any;
use std::collections::HashMap;
use std::num::NonZeroU64;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use rand::{rngs::StdRng, SeedableRng};
use sha2::{Digest, Sha256};
use slatedb::{Db, DbTransaction, IsolationLevel};

use crate::config::{IndexLifecycleScanTuning, SearchIndexBackfillLimits, SearchIndexBatchLimits};
use crate::encoding::property::decode_properties;
use crate::encoding::v2::keys::indexes::vector::{
    VectorIndexMetadataKey, VectorKey, VectorStorageLane,
};
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::ManagedIndexKey as IndexKey;
use crate::encoding::v2::keys::{DataKey, DataKeyKind, KeyPrefix};
use crate::encoding::v2::keys::{
    GlobalKey, IndexEntity, IndexEntityStateKey, RecordKind, ScopedKey,
};
#[cfg(test)]
use crate::encoding::v2::values::encode_build_delta;
use crate::encoding::v2::values::{
    decode_applied_state, decode_build_delta, decode_index_record, decode_partition_mapping,
    encode_applied_state, encode_metadata_value, encode_partition_mapping,
};
use crate::error::{HelixDbError, Result};
use crate::search::vector::{
    self, Distance, MeasuredVectorTransaction, PlannedVectorMutation,
    ValidatedVectorBuildGenerationHandle, ValidatedVectorCleanupAuthority,
    ValidatedVectorGenerationHandle, VectorBuildSession, VectorBuildSessionStats, VectorCleanupRow,
    VectorDistanceMetric, VectorIndex, VectorIndexConfig, VectorWriteMeasurement,
    VectorWriteRecorder,
};

use super::{vector_document, VectorIndexedDocument};
use crate::index_lifecycle::outbox::{
    CommittedOperationStep, CommittedStepState, IndexOperationDriver, IndexOperationStepExecution,
    IndexOperationStepPermit, IndexOperationStepResult, PreparedIndexOperationStep,
    StepResourceUsage, VectorPlanningUsage,
};
use crate::index_lifecycle::queue::QueueTarget;
use crate::index_lifecycle::work::{
    AppliedEntityStateValue, AppliedFamilyState, CoalescedBuildDeltaValue, VectorTenantPartition,
};
use crate::index_lifecycle::{
    ActiveIndexHandle, BuildOperationOutcome, IndexCursor, IndexElementKind, IndexEntityId,
    IndexGenerationId, IndexGenerationPublicationPermit, IndexId, IndexOperationBlocker,
    IndexOperationFamily, IndexOperationId, IndexOperationOutcome, IndexOperationProgress,
    IndexOperationRecord, IndexRecordV2, IndexV2MetadataValue,
    LegacyVectorDirectoryValidationProgress, LegacyVectorPhysicalReservation,
    LegacyVectorValidationLane, LegacyVectorValidationProgress, NoCursorProgress,
    OperationCounters, PhysicalGeneration, PrefixScanProgress, SourceScanProgress, TextPartition,
    ValidatedDynamicIndexDefinition, ValidatedVectorIndexDefinition, VectorBuildProgress,
    VectorBuildStage, VectorCleanupProgress, VectorPhysicalIdWatermark, VectorPhysicalIndexId,
    VectorPhysicalLayout, VectorRoutingLayoutV2,
};

/// Vector lifecycle driver sharing scope gates and the bounded SimHash owner.
pub(crate) struct VectorIndexDriver {
    scope_gates: Arc<crate::index_lifecycle::IndexScopeGates>,
    cache_registry: Arc<vector::VectorCacheRegistry>,
    simhasher_registry: Arc<vector::SimHasherRegistry>,
    scan_tuning: IndexLifecycleScanTuning,
    /// How build handles fetch HNSW row batches, fixed by the database's cache mode.
    batch_reads: crate::batch_reads::BatchReads,
    /// Planning budget shared with queue publication.
    build_cache: Arc<VectorBuildCache>,
}

/// Exact durable checkpoint a retained build planning cache mirrors.
///
/// The operation progress is the one the next step must start from; the index
/// record revision and generation bind the exact physical descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
struct VectorBuildCheckpoint {
    operation_id: IndexOperationId,
    generation: IndexGenerationId,
    index_record_revision: crate::index_lifecycle::IndexRevision,
    progress: IndexOperationProgress,
}

impl VectorBuildCheckpoint {
    fn new(
        operation: &IndexOperationRecord,
        record: &IndexRecordV2,
        progress: IndexOperationProgress,
    ) -> Self {
        Self {
            operation_id: operation.operation_id(),
            generation: operation.generation(),
            index_record_revision: record.revision(),
            progress,
        }
    }
}

/// Exact committed state a retained queue publication session mirrors.
///
/// A publication attempt plans against rows it reads outside its serializable
/// transaction, so a session retained after its commit is sound only while
/// the publisher stays the sole writer of the Active generation's physical
/// rows. Every other writer of vector rows is excluded or invalidates:
///
/// - Foreground mutations only enqueue operations.
/// - A build writes only its own `Building` generation, which publication
///   defers; the generation becomes Active in the build's last step, before
///   any publication into it can retain a session. Legacy adoption transcodes
///   its namespace's metadata in that same step.
/// - Retirement (drop) rewrites the index record, so `index_record_revision`
///   no longer matches and the publication transaction's record read
///   conflicts. Publication then discards the generation's queue, and cleanup
///   deletes its rows under the generation's publication permit; either
///   forgets the session.
/// - Publication itself reclaims emptied tenant partitions inside its commit.
///   Physical IDs only advance, so a reclaimed namespace's cached rows are
///   never read again.
/// - Startup migrations (legacy vector conversion, SimHash-directory
///   adoption, legacy namespace retirement) run while the writer opens, before
///   the queue ledger loads, so no attempt runs and the new cache holds no
///   session. SimHash-directory publication also rewrites the record.
/// - One writer runs one publisher, and every publisher of a writer shares its
///   cache. One attempt per target runs at a time under the generation's
///   [`crate::index_lifecycle::IndexGenerationPublicationPermit`], which
///   build, abort, and cleanup steps take too.
///
/// Every attempt takes its target's retained session out of the cache before
/// it plans, and ends by retaining its own clean session after a successful
/// commit or by forgetting the target's session. No commit can therefore
/// leave an older session behind, and `commit`, the publisher's sequence
/// number of the target's latest commit, is a second check of that.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VectorPublicationCheckpoint {
    pub(crate) target: QueueTarget,
    pub(crate) index_record_revision: crate::index_lifecycle::IndexRevision,
    pub(crate) commit: NonZeroU64,
}

/// Owner of at most one retained planning session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VectorPlanningOwner {
    Build(IndexOperationId),
    Publication(QueueTarget),
}

/// Committed state a retained planning session mirrors.
#[derive(Debug, Clone, PartialEq, Eq)]
enum VectorPlanningCheckpoint {
    Build(VectorBuildCheckpoint),
    Publication(VectorPublicationCheckpoint),
}

impl VectorPlanningCheckpoint {
    const fn owner(&self) -> VectorPlanningOwner {
        match self {
            Self::Build(checkpoint) => VectorPlanningOwner::Build(checkpoint.operation_id),
            Self::Publication(checkpoint) => VectorPlanningOwner::Publication(checkpoint.target),
        }
    }
}

/// Planning cache proven equal to committed physical rows at a checkpoint.
///
/// Only the V2 builder writes a `Building` generation's physical vector rows:
/// foreground mutations of a building index enqueue their operations for that
/// generation, and queue publication defers a `Building` generation without
/// touching its rows until activation. The planning/apply contract of
/// [`crate::search::vector::PlannedVectorMutation::apply_to`] relies on the same
/// exclusivity. A build session is retained only through
/// [`CommittedStepState`], which the outbox releases after the step that
/// produced it committed. Every later commit that writes the generation's rows
/// is a builder step that starts from `checkpoint` and admits at least one
/// entity, advancing the persisted progress counters, so a checkpoint match
/// proves no other write intervened. [`VectorPublicationCheckpoint`] states
/// the same for queue publication into an Active generation.
struct RetainedVectorBuild {
    checkpoint: VectorPlanningCheckpoint,
    /// `VectorBuildSession<D>` for the index's distance metric.
    session: Box<dyn RetainedBuildSession>,
}

/// A clean session offered for retention while its owner commits.
///
/// It keeps its checkout's lease, lowered to the bytes it holds, until the
/// commit settles, so the budget counts it exactly once throughout: as a
/// lease until [`VectorBuildCache`] retains it, then as retained bytes. An
/// offer dropped because its commit failed releases its lease with it.
pub(crate) struct OfferedVectorBuild {
    retained: RetainedVectorBuild,
    lease: SessionLease,
}

impl OfferedVectorBuild {
    /// Offers a publication attempt's `session` for retention once its commit
    /// at `checkpoint` succeeds; a session holding unflushed rows is dropped.
    pub(super) fn publication<D: Distance>(
        checkpoint: VectorPublicationCheckpoint,
        session: CheckedOutSession<D>,
    ) -> Option<Self> {
        session.into_offer(VectorPlanningCheckpoint::Publication(checkpoint))
    }

    /// Offers `session` at `checkpoint` under a new lease of `cache`'s budget.
    #[cfg(any(test, feature = "production-coverage"))]
    fn for_tests(
        cache: &VectorBuildCache,
        checkpoint: VectorPlanningCheckpoint,
        session: Box<dyn RetainedBuildSession>,
    ) -> Self {
        Self {
            lease: SessionLease::new(
                &cache.leases,
                LeaseDemand::Bounded(session.retained_bytes()),
            ),
            retained: RetainedVectorBuild {
                checkpoint,
                session,
            },
        }
    }
}

impl RetainedVectorBuild {
    /// Returns the build checkpoint this session mirrors, if a build retained it.
    #[cfg(any(test, feature = "production-coverage"))]
    fn build_checkpoint(&self) -> Option<&VectorBuildCheckpoint> {
        match &self.checkpoint {
            VectorPlanningCheckpoint::Build(checkpoint) => Some(checkpoint),
            VectorPlanningCheckpoint::Publication(_) => None,
        }
    }
}

/// Build planning session retained between committed steps, erased over its metric.
trait RetainedBuildSession: Any + Send {
    /// Returns the bytes this session charges against the driver's budget.
    fn retained_bytes(&self) -> usize;

    /// Evicts least-recently-used entries until at most `max_bytes` are charged.
    ///
    /// The session's own byte budget and class caps keep applying, so
    /// `usize::MAX` evicts exactly what [`Self::exceeds_limits`] reports.
    fn shrink_to(&mut self, max_bytes: usize) -> Result<()>;

    /// Rebinds the session's byte budget and the class caps scaled from it,
    /// evicting nothing.
    fn set_max_retained_bytes(&mut self, max_bytes: NonZeroU64);

    /// Returns whether the session holds more than its byte budget or a class cap allows.
    fn exceeds_limits(&self) -> bool;
}

impl<D: Distance> RetainedBuildSession for VectorBuildSession<D> {
    fn retained_bytes(&self) -> usize {
        // An unmeasurable session counts as over any budget, so it is shrunk.
        VectorBuildSession::retained_bytes(self).unwrap_or(usize::MAX)
    }

    fn shrink_to(&mut self, max_bytes: usize) -> Result<()> {
        VectorBuildSession::shrink_to(self, max_bytes)
    }

    fn set_max_retained_bytes(&mut self, max_bytes: NonZeroU64) {
        VectorBuildSession::set_max_retained_bytes(self, max_bytes);
    }

    fn exceeds_limits(&self) -> bool {
        VectorBuildSession::exceeds_limits(self)
    }
}

/// Most build operations whose planning sessions one driver retains at once.
///
/// More concurrently interleaved builds than this evict the least recently
/// committed build session whole. Publication commits never evict one.
const MAX_RETAINED_VECTOR_BUILDS: usize = 16;

/// Most queue publication targets whose planning sessions one driver retains
/// at once.
///
/// A commit beyond it evicts the least recently retained drained session.
/// When every retained target still has queued work, the new session is
/// dropped instead: evicting one would only make that target's next attempt
/// cold in turn, so under round-robin over `N` targets this many stay warm
/// rather than none. It also bounds the per-session work of every rebalance.
const MAX_RETAINED_PUBLICATIONS: usize = 16;

/// Whether a publication target still had queued work after the commit that
/// retained its session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublicationBacklog {
    /// Its next attempt comes round, so the session shares the budget fairly.
    Pending,
    /// The commit drained its queue and no attempt is scheduled until new
    /// work arrives, so the session keeps only budget nothing else claims.
    Drained,
}

/// Retained planning sessions, each list least recently committed first.
///
/// An owner has at most one session across all lists.
#[derive(Default)]
struct RetainedSessions {
    /// Build sessions between committed Scan steps.
    builds: Vec<RetainedVectorBuild>,
    /// Sessions of publication targets with queued work left.
    pending: Vec<RetainedVectorBuild>,
    /// Sessions of publication targets whose commit drained their queue.
    drained: Vec<RetainedVectorBuild>,
}

impl RetainedSessions {
    /// Takes `owner`'s session out, if one is retained.
    fn take(&mut self, owner: VectorPlanningOwner) -> Option<RetainedVectorBuild> {
        [&mut self.builds, &mut self.pending, &mut self.drained]
            .into_iter()
            .find_map(|sessions| {
                let index = sessions
                    .iter()
                    .position(|retained| retained.checkpoint.owner() == owner)?;
                Some(sessions.remove(index))
            })
    }

    /// Retains a build step's session within [`MAX_RETAINED_VECTOR_BUILDS`].
    fn admit_build(&mut self, retained: RetainedVectorBuild) {
        self.builds.push(retained);
        if self.builds.len() > MAX_RETAINED_VECTOR_BUILDS {
            self.builds.remove(0);
        }
    }

    /// Retains a publication attempt's session within
    /// [`MAX_RETAINED_PUBLICATIONS`], or drops it when every retained target
    /// still has queued work.
    fn admit_publication(&mut self, retained: RetainedVectorBuild, backlog: PublicationBacklog) {
        if self.pending.len() + self.drained.len() >= MAX_RETAINED_PUBLICATIONS {
            if self.drained.is_empty() {
                return;
            }
            self.drained.remove(0);
        }
        match backlog {
            PublicationBacklog::Pending => self.pending.push(retained),
            PublicationBacklog::Drained => self.drained.push(retained),
        }
    }

    /// Returns every retained session.
    #[cfg(any(test, feature = "production-coverage"))]
    fn iter(&self) -> impl DoubleEndedIterator<Item = &RetainedVectorBuild> {
        self.builds.iter().chain(&self.pending).chain(&self.drained)
    }
}

/// Returns the max-min fair cap of sessions sharing `budget`, or `None` if all fit.
///
/// Sessions at or under their fair share keep every byte, and the larger ones
/// split what remains equally: each is capped at the returned level.
fn max_min_cap(budget: usize, sizes: impl IntoIterator<Item = usize>) -> Option<usize> {
    let mut sizes = sizes.into_iter().collect::<Vec<_>>();
    sizes.sort_unstable();
    let count = sizes.len();
    let mut remaining = budget;
    sizes.into_iter().enumerate().find_map(|(index, size)| {
        let share = remaining / (count - index);
        remaining = remaining.saturating_sub(size);
        (size > share).then_some(share)
    })
}

/// Vector planning sessions of build steps and queue publication attempts,
/// under one byte budget.
///
/// Every session is counted exactly once, and `budget` is split between them
/// max-min fairly:
///
/// - A retained session, kept between committed build steps or publication
///   attempts of a target with queued work left, demands its bytes.
/// - A build step's checked-out session demands without bound: it plans with
///   whatever share it is given.
/// - A publication attempt's checked-out session demands what it resumed
///   with plus the larger of that and its attempt's input allowance (at
///   least a [`MAX_RETAINED_PUBLICATIONS`]th of the budget), so a small
///   target takes little and a growing one at least doubles per attempt
///   until it reaches its fair share.
/// - A session offered for retention ([`OfferedVectorBuild`]) keeps its lease,
///   lowered to its bytes, until its owner's commit settles.
///
/// A session retained by a commit that drained its target's queue
/// ([`PublicationBacklog::Drained`]) demands nothing: it keeps, newest first,
/// only what the fair split leaves free, and is shrunk or dropped as soon as
/// a checkout claims that, such as any build step's. It serves only a trickle
/// of later writes to its target, which is not scheduled until one arrives.
///
/// At most one session per build operation or publication target is
/// retained, and at most [`MAX_RETAINED_VECTOR_BUILDS`] builds and
/// [`MAX_RETAINED_PUBLICATIONS`] targets, so publication never evicts a build.
///
/// Every checkout and retaining commit rebalances: retained sessions over
/// their new limit shrink, and each checked-out session takes its new share
/// at its next entity boundary ([`CheckedOutSession::rebind`]). A dropped
/// checkout or offer leaves the other shares as they are until the next
/// rebalance, so the split only errs low. A checked-out session holds up to
/// the share it was last bound to until that boundary, so when shares fall,
/// the sessions together exceed the budget transiently by that difference
/// plus what each plans for one entity.
///
/// No bulk eviction runs on the async executor. Rebalancing shrinks retained
/// sessions on the blocking pool while holding the lock, so no checkout
/// observes a session mid-trim, and a checked-out session over its share is
/// shrunk there too, at checkout or between entities. Per-entity
/// [`VectorBuildSession::enforce_limits`] then evicts only what one entity
/// adds. A dropped large session frees its entries on a background thread.
pub(crate) struct VectorBuildCache {
    budget: NonZeroU64,
    /// An async lock, because rebalancing holds it while its trim runs off the executor.
    retained: Arc<tokio::sync::Mutex<RetainedSessions>>,
    leases: Arc<SessionLeases>,
    /// Keys publication planning read through its planning transactions.
    #[cfg(test)]
    publication_reads: AtomicU64,
    /// Entries publication planning evicted from its checked-out sessions.
    #[cfg(test)]
    publication_evictions: AtomicU64,
}

/// What one leased session demands of the budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeaseDemand {
    /// A build step, which plans with whatever share it is given.
    Unbounded,
    /// A publication attempt's resumed bytes and headroom, or the bytes an
    /// offered session holds.
    Bounded(usize),
}

impl LeaseDemand {
    /// Returns the demand as a max-min size.
    const fn bytes(self) -> usize {
        match self {
            Self::Unbounded => usize::MAX,
            Self::Bounded(bytes) => bytes,
        }
    }
}

/// Every live lease's demand and share, by lease.
#[derive(Default)]
struct SessionLeases {
    next: AtomicU64,
    /// A sync lock, so a lease releases on drop without awaiting.
    live: parking_lot::Mutex<HashMap<u64, LiveLease>>,
}

/// One live lease's demand and the share the latest rebalance bound it to.
struct LiveLease {
    demand: LeaseDemand,
    share: NonZeroU64,
}

/// One session's claim on a share of the budget, released on drop.
struct SessionLease {
    leases: Arc<SessionLeases>,
    id: u64,
}

impl SessionLease {
    /// Registers `demand`; the next rebalance sets its share.
    fn new(leases: &Arc<SessionLeases>, demand: LeaseDemand) -> Self {
        let id = leases.next.fetch_add(1, Ordering::Relaxed);
        leases.live.lock().insert(
            id,
            LiveLease {
                demand,
                share: NonZeroU64::MIN,
            },
        );
        Self {
            leases: Arc::clone(leases),
            id,
        }
    }

    /// Returns the share the latest rebalance bound this lease to.
    fn share(&self) -> NonZeroU64 {
        self.leases
            .live
            .lock()
            .get(&self.id)
            .expect("a lease stays registered until it drops")
            .share
    }

    /// Demands exactly `bytes`, what a session that stopped planning holds.
    fn hold(&self, bytes: usize) {
        self.leases
            .live
            .lock()
            .get_mut(&self.id)
            .expect("a lease stays registered until it drops")
            .demand = LeaseDemand::Bounded(bytes);
    }
}

impl Drop for SessionLease {
    fn drop(&mut self) {
        self.leases.live.lock().remove(&self.id);
    }
}

/// A planning session checked out of a [`VectorBuildCache`], bound to a share
/// of its budget until it is dropped or handed back for retention.
pub(crate) struct CheckedOutSession<D: Distance> {
    session: VectorBuildSession<D>,
    /// Share the session is bound to.
    bound: NonZeroU64,
    lease: SessionLease,
}

impl<D: Distance> CheckedOutSession<D> {
    fn fresh(lease: SessionLease, share: NonZeroU64) -> Self {
        Self {
            session: VectorBuildSession::new(share),
            bound: share,
            lease,
        }
    }

    /// Binds the session to its latest share of the budget.
    ///
    /// Call between entities, with every dirty row flushed. When the share
    /// fell below what the session holds, the session shrinks on the blocking
    /// pool. One that cannot shrink is replaced by a fresh session, which
    /// plans alike because the planning transaction holds every flushed row.
    async fn rebind(&mut self) {
        let share = self.lease.share();
        if share == self.bound {
            return;
        }
        let fell = share < self.bound;
        self.bound = share;
        self.session.set_max_retained_bytes(share);
        if !fell || !self.session.exceeds_limits() {
            return;
        }
        let session = core::mem::replace(&mut self.session, VectorBuildSession::new(share));
        self.session = shrink_off_executor(session, |session| session.shrink_to(usize::MAX))
            .await
            .unwrap_or_else(|| VectorBuildSession::new(share));
    }

    /// Offers the session for retention at `checkpoint`, keeping its lease
    /// lowered to the bytes it holds; a session holding unflushed rows is
    /// dropped with its lease instead.
    fn into_offer(self, checkpoint: VectorPlanningCheckpoint) -> Option<OfferedVectorBuild> {
        let Self { session, lease, .. } = self;
        (!session.has_dirty_neighbors()).then(|| {
            lease.hold(RetainedBuildSession::retained_bytes(&session));
            OfferedVectorBuild {
                retained: RetainedVectorBuild {
                    checkpoint,
                    session: Box::new(session),
                },
                lease,
            }
        })
    }
}

impl<D: Distance> core::ops::Deref for CheckedOutSession<D> {
    type Target = VectorBuildSession<D>;

    fn deref(&self) -> &Self::Target {
        &self.session
    }
}

impl<D: Distance> core::ops::DerefMut for CheckedOutSession<D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.session
    }
}

/// Runs `shrink` on `session` on the blocking pool.
///
/// Returns `None` when the session could not shrink or runtime shutdown
/// cancelled the shrink; the session was then dropped on the blocking pool.
async fn shrink_off_executor<S: Send + 'static>(
    mut session: S,
    shrink: impl FnOnce(&mut S) -> Result<()> + Send + 'static,
) -> Option<S> {
    let shrunk = tokio::task::spawn_blocking(move || shrink(&mut session).map(|()| session)).await;
    match shrunk {
        Ok(shrunk) => shrunk.ok(),
        Err(error) => match error.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            Err(_) => None,
        },
    }
}

impl VectorBuildCache {
    pub(crate) fn new(budget: NonZeroU64) -> Self {
        Self {
            budget,
            retained: Arc::new(tokio::sync::Mutex::new(RetainedSessions::default())),
            leases: Arc::new(SessionLeases::default()),
            #[cfg(test)]
            publication_reads: AtomicU64::new(0),
            #[cfg(test)]
            publication_evictions: AtomicU64::new(0),
        }
    }

    /// Counts the keys one publication attempt's planning read from storage
    /// and the entries it evicted after checkout: per entity and at rebinds,
    /// not the checkout's own trim.
    #[cfg(test)]
    pub(super) fn record_publication_planning(&self, reads: u64, evictions: u64) {
        self.publication_reads.fetch_add(reads, Ordering::Relaxed);
        self.publication_evictions
            .fetch_add(evictions, Ordering::Relaxed);
    }

    /// Returns keys publication planning has read from storage.
    #[cfg(test)]
    pub(crate) fn publication_reads(&self) -> u64 {
        self.publication_reads.load(Ordering::Relaxed)
    }

    /// Returns entries publication planning has evicted.
    #[cfg(test)]
    pub(crate) fn publication_evictions(&self) -> u64 {
        self.publication_evictions.load(Ordering::Relaxed)
    }

    /// Checks out a fresh session of unbounded demand, as a build step
    /// would, without taking any retained one.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) async fn checkout_fresh<D: Distance>(&self) -> CheckedOutSession<D> {
        let (lease, _) = self
            .lease(None, |_| false, |_| LeaseDemand::Unbounded)
            .await;
        let share = lease.share();
        CheckedOutSession::fresh(lease, share)
    }

    /// Checks out a build step's retained session for exactly `checkpoint`,
    /// or a fresh one.
    async fn checkout<D: Distance>(
        &self,
        checkpoint: &VectorBuildCheckpoint,
    ) -> CheckedOutSession<D> {
        self.checkout_owned(
            VectorPlanningOwner::Build(checkpoint.operation_id),
            |retained| {
                matches!(
                    retained,
                    VectorPlanningCheckpoint::Build(retained) if retained == checkpoint
                )
            },
            |_| LeaseDemand::Unbounded,
        )
        .await
    }

    /// Checks out the session retained for `permit`'s target when it mirrors
    /// exactly `reuse`, or a fresh one, demanding what it resumes with plus
    /// the larger of that and `allowance`.
    ///
    /// The allowance is at least a [`MAX_RETAINED_PUBLICATIONS`]th of the
    /// budget, so a batch of little input still leaves its session room to
    /// grow across attempts.
    ///
    /// The target's retained session is taken out either way, so the attempt
    /// can never leave it behind its own commit ([`VectorPublicationCheckpoint`]).
    pub(crate) async fn checkout_publication<D: Distance>(
        &self,
        permit: &IndexGenerationPublicationPermit,
        reuse: Option<&VectorPublicationCheckpoint>,
        allowance: NonZeroU64,
    ) -> CheckedOutSession<D> {
        assert!(
            reuse.is_none_or(|reuse| reuse.target == permit.target()),
            "a publication checks out only its permitted target's session"
        );
        let allowance = usize::try_from(allowance.get()).unwrap_or(usize::MAX).max(
            usize::try_from(self.budget.get()).unwrap_or(usize::MAX) / MAX_RETAINED_PUBLICATIONS,
        );
        self.checkout_owned(
            VectorPlanningOwner::Publication(permit.target()),
            |retained| {
                matches!(
                    retained,
                    VectorPlanningCheckpoint::Publication(retained) if Some(retained) == reuse
                )
            },
            |resumed| LeaseDemand::Bounded(resumed.saturating_add(resumed.max(allowance))),
        )
        .await
    }

    /// Checks out `owner`'s retained session when `reusable` accepts its
    /// checkpoint, or a fresh one, under the lease `demand` sets from the
    /// bytes it resumes with.
    ///
    /// Any other session of `owner`, or one of another metric, is stale and
    /// dropped; other owners' sessions stay. The returned session is bound to
    /// its share: the whole budget when it is the only session and demands
    /// without bound.
    ///
    /// The returned session is within every limit of that share. A reused
    /// session can hold more: its bytes when the share fell since its commit,
    /// or entries of a class whose cap scales with the share, such as a session
    /// dense in SimHashes or low-dimension items, whose count caps bind before
    /// its bytes. That excess is evicted on the blocking pool before the
    /// session is returned, so the first [`VectorBuildSession::enforce_limits`]
    /// evicts only what the step or attempt itself adds. Like a commit's trim,
    /// it is not step telemetry.
    async fn checkout_owned<D: Distance>(
        &self,
        owner: VectorPlanningOwner,
        reusable: impl FnOnce(&VectorPlanningCheckpoint) -> bool,
        demand: impl FnOnce(usize) -> LeaseDemand,
    ) -> CheckedOutSession<D> {
        let (lease, own) = self.lease(Some(owner), reusable, demand).await;
        let share = lease.share();
        let Some(RetainedVectorBuild { mut session, .. }) = own else {
            return CheckedOutSession::fresh(lease, share);
        };
        session.set_max_retained_bytes(share);
        if session.exceeds_limits() {
            // Retained sessions are clean, so the shrink needs no transaction.
            let Some(shrunk) =
                shrink_off_executor(session, |session| session.shrink_to(usize::MAX)).await
            else {
                return CheckedOutSession::fresh(lease, share);
            };
            session = shrunk;
        }
        let session: Box<dyn Any> = session;
        let Ok(session) = session.downcast::<VectorBuildSession<D>>() else {
            return CheckedOutSession::fresh(lease, share);
        };
        let mut session = *session;
        session.reset_stats();
        CheckedOutSession {
            session,
            bound: share,
            lease,
        }
    }

    /// Leases a share of the budget for one checkout and rebalances.
    ///
    /// `owner`'s retained session is taken out first and returned when
    /// `reusable` accepts its checkpoint; `demand` sets the lease from the
    /// bytes of that session, or zero.
    async fn lease(
        &self,
        owner: Option<VectorPlanningOwner>,
        reusable: impl FnOnce(&VectorPlanningCheckpoint) -> bool,
        demand: impl FnOnce(usize) -> LeaseDemand,
    ) -> (SessionLease, Option<RetainedVectorBuild>) {
        let mut retained = Arc::clone(&self.retained).lock_owned().await;
        let own = owner
            .and_then(|owner| retained.take(owner))
            .filter(|own| reusable(&own.checkpoint));
        let lease = SessionLease::new(
            &self.leases,
            demand(own.as_ref().map_or(0, |own| own.session.retained_bytes())),
        );
        self.rebalance(retained).await;
        (lease, own)
    }

    /// Retains a committed step's session, or forgets the operation's session.
    async fn after_commit(
        &self,
        operation_id: IndexOperationId,
        committed: CommittedOperationStep,
        state: Option<CommittedStepState>,
    ) {
        let owner = VectorPlanningOwner::Build(operation_id);
        let (CommittedOperationStep::Progressed, Some(CommittedStepState::VectorBuild(next))) =
            (committed, state)
        else {
            return self.forget(owner).await;
        };
        self.retain(owner, *next, RetainedSessions::admit_build)
            .await;
    }

    /// Retains the clean session of a publication attempt that committed at
    /// its checkpoint, replacing its target's session; `backlog` is whether
    /// the target has queued work left.
    pub(crate) async fn retain_publication(
        &self,
        permit: &IndexGenerationPublicationPermit,
        offered: OfferedVectorBuild,
        backlog: PublicationBacklog,
    ) {
        self.retain(
            VectorPlanningOwner::Publication(permit.target()),
            offered,
            |sessions, retained| sessions.admit_publication(retained, backlog),
        )
        .await;
    }

    /// Forgets the session retained for `target`, if any.
    pub(crate) async fn forget_publication(&self, target: QueueTarget) {
        self.forget(VectorPlanningOwner::Publication(target)).await;
    }

    /// Forgets `owner`'s retained session, if any.
    async fn forget(&self, owner: VectorPlanningOwner) {
        drop(self.retained.lock().await.take(owner));
    }

    /// Replaces `owner`'s retained session with `offered` through `admit`,
    /// then rebalances the budget.
    ///
    /// The offer's lease is released only once `admit` ran, so no rebalance
    /// misses the session or counts it twice.
    async fn retain(
        &self,
        owner: VectorPlanningOwner,
        offered: OfferedVectorBuild,
        admit: impl FnOnce(&mut RetainedSessions, RetainedVectorBuild),
    ) {
        let OfferedVectorBuild { retained, lease } = offered;
        assert_eq!(
            retained.checkpoint.owner(),
            owner,
            "a session is retained only for the owner that planned with it"
        );
        let mut sessions = Arc::clone(&self.retained).lock_owned().await;
        drop(sessions.take(owner));
        admit(&mut sessions, retained);
        drop(lease);
        self.rebalance(sessions).await;
    }

    /// Returns the checkpoint of the session retained for `target`.
    #[cfg(test)]
    pub(crate) fn retained_publication(
        &self,
        target: QueueTarget,
    ) -> Option<VectorPublicationCheckpoint> {
        self.retained
            .try_lock()
            .expect("no rebalance is trimming the retained sessions")
            .iter()
            .find_map(|retained| match &retained.checkpoint {
                VectorPlanningCheckpoint::Publication(checkpoint)
                    if checkpoint.target == target =>
                {
                    Some(*checkpoint)
                }
                VectorPlanningCheckpoint::Publication(_) | VectorPlanningCheckpoint::Build(_) => {
                    None
                }
            })
    }

    /// Splits the budget max-min fairly between the retained build and
    /// pending publication sessions and every lease, binds each lease to its
    /// share, then fits the drained sessions, newest first, into what that
    /// split leaves free.
    ///
    /// Sessions and leases under their fair share keep every byte they
    /// demand, and the rest are capped at the one level that exactly fills
    /// what remains. Every retained session over its limit shrinks to it,
    /// except that a drained session left no room is dropped. Shrinking runs
    /// on the blocking pool with `sessions` still locked, so no checkout
    /// observes a session mid-trim. A session that cannot shrink is dropped.
    async fn rebalance(&self, mut sessions: tokio::sync::OwnedMutexGuard<RetainedSessions>) {
        let budget = usize::try_from(self.budget.get()).unwrap_or(usize::MAX);
        let (cap, free) = {
            let mut live = self.leases.live.lock();
            let demands = sessions
                .builds
                .iter()
                .chain(&sessions.pending)
                .map(|retained| retained.session.retained_bytes())
                .chain(live.values().map(|lease| lease.demand.bytes()))
                .collect::<Vec<_>>();
            let cap = max_min_cap(budget, demands.iter().copied());
            for lease in live.values_mut() {
                // Only an unbounded budget fits an unbounded demand.
                let share = cap.unwrap_or(budget).min(lease.demand.bytes());
                lease.share = NonZeroU64::new(u64::try_from(share).unwrap_or(u64::MAX))
                    .unwrap_or(NonZeroU64::MIN);
            }
            let granted = demands
                .into_iter()
                .map(|demand| cap.map_or(demand, |cap| demand.min(cap)))
                .fold(0_usize, usize::saturating_add);
            (cap.unwrap_or(usize::MAX), budget.saturating_sub(granted))
        };
        let mut drained_limits = sessions
            .drained
            .iter()
            .rev()
            .scan(free, |free, retained| {
                let kept = retained.session.retained_bytes().min(*free);
                *free -= kept;
                Some(kept)
            })
            .collect::<Vec<_>>();
        drained_limits.reverse();
        let over = |retained: &RetainedVectorBuild, limit: usize| {
            retained.session.retained_bytes() > limit
        };
        let trims = sessions
            .builds
            .iter()
            .chain(&sessions.pending)
            .any(|retained| over(retained, cap))
            || sessions
                .drained
                .iter()
                .zip(&drained_limits)
                .any(|(retained, limit)| over(retained, *limit));
        if !trims {
            return;
        }
        let trimmed = tokio::task::spawn_blocking(move || {
            let fits = |retained: &mut RetainedVectorBuild, limit: usize| {
                retained.session.retained_bytes() <= limit
                    || retained.session.shrink_to(limit).is_ok()
            };
            sessions.builds.retain_mut(|retained| fits(retained, cap));
            sessions.pending.retain_mut(|retained| fits(retained, cap));
            let mut limits = drained_limits.into_iter();
            sessions.drained.retain_mut(|retained| {
                let limit = limits.next().expect("one limit per drained session");
                retained.session.retained_bytes() <= limit
                    || (limit > 0 && retained.session.shrink_to(limit).is_ok())
            });
        })
        .await;
        // A trim cancelled by runtime shutdown leaves the sessions whole.
        let Err(error) = trimmed else {
            return;
        };
        let Ok(panic) = error.try_into_panic() else {
            return;
        };
        std::panic::resume_unwind(panic);
    }
}

impl core::fmt::Debug for VectorIndexDriver {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("VectorIndexDriver")
            .finish_non_exhaustive()
    }
}

impl VectorIndexDriver {
    /// Installs vector work against mutation and cache authorities.
    pub(crate) fn new(
        scope_gates: Arc<crate::index_lifecycle::IndexScopeGates>,
        cache_registry: Arc<vector::VectorCacheRegistry>,
        simhasher_registry: Arc<vector::SimHasherRegistry>,
    ) -> Self {
        Self {
            scope_gates,
            cache_registry,
            simhasher_registry,
            scan_tuning: IndexLifecycleScanTuning::default(),
            batch_reads: crate::batch_reads::BatchReads::Single,
            build_cache: Arc::new(VectorBuildCache::new(
                SearchIndexBackfillLimits::default().vector_build_cache_bytes(),
            )),
        }
    }

    /// Applies runtime source-scan prefetching without admitting blocks to cache.
    pub(crate) const fn with_scan_tuning(mut self, scan_tuning: IndexLifecycleScanTuning) -> Self {
        self.scan_tuning = scan_tuning;
        self
    }

    /// Applies the database's row-batch fetch policy to build handles.
    ///
    /// Builds issue one `multi_get` per batch until the database opts into
    /// concurrent chunks, which it does only when a SlateDB block cache
    /// deduplicates their SST filter and index reads.
    pub(crate) const fn with_batch_reads(
        mut self,
        batch_reads: crate::batch_reads::BatchReads,
    ) -> Self {
        self.batch_reads = batch_reads;
        self
    }

    /// Bounds the planning sessions of builds and of queue publication
    /// through [`Self::build_cache`] together.
    pub(crate) fn with_build_cache_bytes(mut self, budget: NonZeroU64) -> Self {
        self.build_cache = Arc::new(VectorBuildCache::new(budget));
        self
    }

    /// Returns the planning budget builds share with queue publication.
    pub(crate) fn build_cache(&self) -> Arc<VectorBuildCache> {
        Arc::clone(&self.build_cache)
    }
}

#[async_trait]
impl IndexOperationDriver for VectorIndexDriver {
    fn family(&self) -> IndexOperationFamily {
        IndexOperationFamily::Vector
    }

    async fn acquire_generation_ownership(
        &self,
        scope: DataScope,
        operation: &IndexOperationRecord,
    ) -> Box<dyn IndexOperationStepPermit> {
        Box::new(
            self.scope_gates
                .publication_permit(QueueTarget::new(
                    scope,
                    operation.index_id(),
                    operation.generation(),
                ))
                .await,
        )
    }

    async fn acquire_step_permit(
        &self,
        scope: DataScope,
        operation: &IndexOperationRecord,
    ) -> Result<Box<dyn IndexOperationStepPermit>> {
        let needs_exclusive = matches!(
            operation.progress(),
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                VectorBuildStage::AdoptLegacy(_)
                    | VectorBuildStage::ValidateAdoptedDirectory(_)
                    | VectorBuildStage::ValidateDescriptor(_)
                    | VectorBuildStage::Activate(_)
            )) | IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(_))
                | IndexOperationProgress::VectorCleanup(_)
        );
        if needs_exclusive {
            return Ok(Box::new(self.scope_gates.lifecycle_permit(scope).await));
        }
        Ok(Box::new(()))
    }

    async fn prepare_step(
        &self,
        _db: &Db,
        scope: DataScope,
        operation: &IndexOperationRecord,
        _limits: SearchIndexBatchLimits,
    ) -> Result<PreparedIndexOperationStep> {
        let permit = self.acquire_step_permit(scope, operation).await?;
        Ok(PreparedIndexOperationStep::driver_owned(
            self.family(),
            permit,
        ))
    }

    async fn step(
        &self,
        db: &Db,
        transaction: &DbTransaction,
        scope: DataScope,
        operation: &IndexOperationRecord,
        limits: SearchIndexBatchLimits,
    ) -> Result<IndexOperationStepExecution> {
        let record = load_operation_index(transaction, scope, operation).await?;
        let ValidatedDynamicIndexDefinition::Vector(definition) = record.definition() else {
            return Err(corruption("vector operation loaded another family"));
        };
        let step = match operation.progress() {
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(stage)) => {
                match definition.metric() {
                    VectorDistanceMetric::Cosine => {
                        step_build::<vector::distance::Cosine>(
                            db,
                            transaction,
                            scope,
                            operation,
                            &record,
                            definition,
                            stage,
                            limits,
                            self.scan_tuning,
                            Arc::clone(&self.simhasher_registry),
                            self.batch_reads,
                            &self.build_cache,
                        )
                        .await?
                    }
                    VectorDistanceMetric::Euclidean => {
                        step_build::<vector::distance::Euclidean>(
                            db,
                            transaction,
                            scope,
                            operation,
                            &record,
                            definition,
                            stage,
                            limits,
                            self.scan_tuning,
                            Arc::clone(&self.simhasher_registry),
                            self.batch_reads,
                            &self.build_cache,
                        )
                        .await?
                    }
                    VectorDistanceMetric::Manhattan => {
                        step_build::<vector::distance::Manhattan>(
                            db,
                            transaction,
                            scope,
                            operation,
                            &record,
                            definition,
                            stage,
                            limits,
                            self.scan_tuning,
                            Arc::clone(&self.simhasher_registry),
                            self.batch_reads,
                            &self.build_cache,
                        )
                        .await?
                    }
                }
            }
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(_))
            | IndexOperationProgress::VectorCleanup(_) => {
                let (progress, aborting) = match operation.progress() {
                    IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(
                        progress,
                    )) => (progress, true),
                    IndexOperationProgress::VectorCleanup(progress) => (progress, false),
                    IndexOperationProgress::SecondaryBuild(_)
                    | IndexOperationProgress::TextBuild(_)
                    | IndexOperationProgress::SecondaryCleanup(_)
                    | IndexOperationProgress::TextCleanup(_)
                    | IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(_)) => {
                        return Err(corruption(
                            "vector cleanup dispatch selected a non-cleanup progress state",
                        ));
                    }
                };
                match definition.metric() {
                    VectorDistanceMetric::Cosine => {
                        step_cleanup::<vector::distance::Cosine>(
                            transaction,
                            scope,
                            operation,
                            &record,
                            definition,
                            progress,
                            aborting,
                            limits,
                            &self.cache_registry,
                        )
                        .await?
                    }
                    VectorDistanceMetric::Euclidean => {
                        step_cleanup::<vector::distance::Euclidean>(
                            transaction,
                            scope,
                            operation,
                            &record,
                            definition,
                            progress,
                            aborting,
                            limits,
                            &self.cache_registry,
                        )
                        .await?
                    }
                    VectorDistanceMetric::Manhattan => {
                        step_cleanup::<vector::distance::Manhattan>(
                            transaction,
                            scope,
                            operation,
                            &record,
                            definition,
                            progress,
                            aborting,
                            limits,
                            &self.cache_registry,
                        )
                        .await?
                    }
                }
            }
            IndexOperationProgress::SecondaryBuild(_)
            | IndexOperationProgress::TextBuild(_)
            | IndexOperationProgress::SecondaryCleanup(_)
            | IndexOperationProgress::TextCleanup(_) => {
                return Err(corruption("vector driver received another family progress"));
            }
        };
        Ok(step.into_execution())
    }

    async fn after_commit(
        &self,
        scope: DataScope,
        index: &IndexRecordV2,
        operation: &IndexOperationRecord,
        committed: CommittedOperationStep,
        state: Option<CommittedStepState>,
    ) {
        self.build_cache
            .after_commit(operation.operation_id(), committed, state)
            .await;
        // Cleanup runs only on a retired generation, which publication never
        // writes again.
        if matches!(
            operation.progress(),
            IndexOperationProgress::VectorCleanup(_)
        ) {
            self.build_cache
                .forget_publication(QueueTarget::new(
                    scope,
                    operation.index_id(),
                    operation.generation(),
                ))
                .await;
        }
        if committed != CommittedOperationStep::Completed
            || !matches!(
                operation.progress(),
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(
                    VectorCleanupProgress::Finalize(_)
                )) | IndexOperationProgress::VectorCleanup(VectorCleanupProgress::Finalize(_))
            )
        {
            return;
        }
        let ValidatedDynamicIndexDefinition::Vector(definition) = index.definition() else {
            return;
        };
        let authority = match definition.metric() {
            VectorDistanceMetric::Cosine => ValidatedVectorCleanupAuthority::try_from_cleaning::<
                vector::distance::Cosine,
            >(scope, index, operation.operation_id()),
            VectorDistanceMetric::Euclidean => {
                ValidatedVectorCleanupAuthority::try_from_cleaning::<vector::distance::Euclidean>(
                    scope,
                    index,
                    operation.operation_id(),
                )
            }
            VectorDistanceMetric::Manhattan => {
                ValidatedVectorCleanupAuthority::try_from_cleaning::<vector::distance::Manhattan>(
                    scope,
                    index,
                    operation.operation_id(),
                )
            }
        };
        let Ok(authority) = authority else {
            tracing::error!(
                operation_id = %operation.operation_id().as_uuid(),
                "committed vector cleanup could not reconstruct its cache authority"
            );
            return;
        };
        if !self.cache_registry.forget_cleanup_generation(&authority) {
            tracing::error!(
                operation_id = %operation.operation_id().as_uuid(),
                "committed vector cleanup retained a non-closed cache generation"
            );
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "cleanup binds the exact canonical owner, cache fence, progress, and batch limits"
)]
async fn step_cleanup<D: Distance>(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    record: &IndexRecordV2,
    definition: &ValidatedVectorIndexDefinition,
    progress: &VectorCleanupProgress,
    aborting: bool,
    limits: SearchIndexBatchLimits,
    cache_registry: &vector::VectorCacheRegistry,
) -> Result<VectorStepResult> {
    let authority = ValidatedVectorCleanupAuthority::try_from_cleaning::<D>(
        scope,
        record,
        operation.operation_id(),
    )
    .map_err(|error| corruption(error.to_string()))?;
    if matches!(
        progress,
        VectorCleanupProgress::DeletePhysical(_)
            | VectorCleanupProgress::DeleteDeltas(_)
            | VectorCleanupProgress::Finalize(_)
    ) {
        cache_registry.retire_cleanup_generation(&authority).await;
    }
    let next = match progress {
        VectorCleanupProgress::RetireCache(progress) => {
            cache_registry.retire_cleanup_generation(&authority).await;
            VectorCleanupProgress::DeletePhysical(PrefixScanProgress {
                cursor: None,
                counters: progress.counters,
            })
        }
        VectorCleanupProgress::DeletePhysical(progress) => match authority.layout() {
            VectorPhysicalLayout::Unpartitioned { physical_index_id } => {
                if progress.cursor.is_some() {
                    return Err(corruption(
                        "unpartitioned vector cleanup retained a mapping cursor",
                    ));
                }
                if aborting
                    && let Some(reservation) =
                        super::super::repository::load_legacy_vector_physical_reservation(
                            transaction,
                            physical_index_id,
                        )
                        .await?
                {
                    if !matches!(
                        reservation,
                        LegacyVectorPhysicalReservation::AdoptionBuilding { .. }
                    ) {
                        return Err(corruption(
                            "vector abort found a non-building legacy reservation",
                        ));
                    }
                    let handle = authority
                        .physical_generation::<D>(physical_index_id)
                        .map_err(|error| corruption(error.to_string()))?;
                    let legacy = VectorIndex::<D>::from_generation(&handle);
                    let (cleanup, measured) = delete_simhash_directory(
                        transaction,
                        &legacy,
                        definition.element_kind(),
                        progress.counters,
                        limits,
                    )
                    .await?;
                    let PhysicalCleanupOutcome::Progress {
                        counters,
                        namespace_empty,
                        mapping_deleted: false,
                    } = cleanup
                    else {
                        return match cleanup {
                            PhysicalCleanupOutcome::Blocked(blocker) => {
                                Ok(VectorStepResult::ordinary(
                                    IndexOperationStepResult::Blocked(blocker),
                                ))
                            }
                            PhysicalCleanupOutcome::Progress {
                                mapping_deleted: true,
                                ..
                            } => Err(corruption(
                                "legacy directory cleanup deleted a partition mapping",
                            )),
                            PhysicalCleanupOutcome::Progress {
                                mapping_deleted: false,
                                ..
                            } => unreachable!(),
                        };
                    };
                    if !namespace_empty {
                        return Ok(VectorStepResult::vector_writes(
                            progressed_cleanup(
                                true,
                                VectorCleanupProgress::DeletePhysical(PrefixScanProgress {
                                    cursor: None,
                                    counters,
                                }),
                            ),
                            measured,
                        ));
                    }
                    let Some(source_reservation) = reservation.abort(
                        operation.index_id(),
                        operation.generation(),
                        operation.operation_id(),
                    ) else {
                        return Err(corruption(
                            "vector abort found a reservation owned by another generation",
                        ));
                    };
                    transaction.put(
                        IndexKey::Global {
                            kind: GlobalKey::LegacyVectorPhysicalReservation(physical_index_id),
                        }
                        .to_bytes(),
                        encode_metadata_value(
                            &IndexV2MetadataValue::LegacyVectorPhysicalReservation(
                                source_reservation,
                            ),
                        ),
                    )?;
                    return Ok(VectorStepResult::vector_writes(
                        progressed_cleanup(
                            true,
                            VectorCleanupProgress::DeleteDeltas(PrefixScanProgress {
                                cursor: None,
                                counters,
                            }),
                        ),
                        measured,
                    ));
                }
                let handle = authority
                    .physical_generation::<D>(physical_index_id)
                    .map_err(|error| corruption(error.to_string()))?;
                match delete_physical_namespace::<D>(
                    transaction,
                    &handle,
                    None,
                    definition.element_kind(),
                    progress.counters,
                    limits,
                )
                .await?
                {
                    PhysicalCleanupOutcome::Blocked(blocker) => {
                        return Ok(VectorStepResult::ordinary(
                            IndexOperationStepResult::Blocked(blocker),
                        ));
                    }
                    PhysicalCleanupOutcome::Progress {
                        counters,
                        namespace_empty,
                        mapping_deleted: false,
                    } if namespace_empty => {
                        VectorCleanupProgress::DeleteDeltas(PrefixScanProgress {
                            cursor: None,
                            counters,
                        })
                    }
                    PhysicalCleanupOutcome::Progress {
                        counters,
                        mapping_deleted: false,
                        ..
                    } => VectorCleanupProgress::DeletePhysical(PrefixScanProgress {
                        cursor: None,
                        counters,
                    }),
                    PhysicalCleanupOutcome::Progress {
                        mapping_deleted: true,
                        ..
                    } => {
                        return Err(corruption(
                            "unpartitioned vector cleanup deleted a partition mapping",
                        ));
                    }
                }
            }
            VectorPhysicalLayout::Partitioned => {
                let mapping = current_or_next_mapping(
                    transaction,
                    scope,
                    operation,
                    progress.cursor.as_ref(),
                )
                .await?;
                let Some(mapping) = mapping else {
                    return Ok(VectorStepResult::ordinary(progressed_cleanup(
                        aborting,
                        VectorCleanupProgress::DeleteDeltas(PrefixScanProgress {
                            cursor: None,
                            counters: progress.counters,
                        }),
                    )));
                };
                let handle = authority
                    .physical_generation::<D>(mapping.value.physical_index_id)
                    .map_err(|error| corruption(error.to_string()))?;
                match delete_physical_namespace::<D>(
                    transaction,
                    &handle,
                    Some(&mapping),
                    definition.element_kind(),
                    progress.counters,
                    limits,
                )
                .await?
                {
                    PhysicalCleanupOutcome::Blocked(blocker) => {
                        return Ok(VectorStepResult::ordinary(
                            IndexOperationStepResult::Blocked(blocker),
                        ));
                    }
                    PhysicalCleanupOutcome::Progress { counters, .. } => {
                        VectorCleanupProgress::DeletePhysical(PrefixScanProgress {
                            cursor: Some(mapping.cursor),
                            counters,
                        })
                    }
                }
            }
        },
        VectorCleanupProgress::DeleteDeltas(progress) => {
            if progress.cursor.is_some() {
                return Err(corruption(
                    "vector delta cleanup uses delete-from-prefix rather than a stale cursor",
                ));
            }
            match delete_delta_and_applied_rows(
                transaction,
                scope,
                operation,
                progress.counters,
                limits,
            )
            .await?
            {
                CleanupWorkOutcome::Blocked(blocker) => {
                    return Ok(VectorStepResult::ordinary(
                        IndexOperationStepResult::Blocked(blocker),
                    ));
                }
                CleanupWorkOutcome::Progress {
                    counters,
                    exhausted: false,
                } => VectorCleanupProgress::DeleteDeltas(PrefixScanProgress {
                    cursor: None,
                    counters,
                }),
                CleanupWorkOutcome::Progress {
                    counters,
                    exhausted: true,
                } => VectorCleanupProgress::Finalize(NoCursorProgress { counters }),
            }
        }
        VectorCleanupProgress::Finalize(_) => {
            if !aborting
                && let VectorPhysicalLayout::Unpartitioned { physical_index_id } =
                    authority.layout()
                && let Some(reservation) =
                    super::super::repository::load_legacy_vector_physical_reservation(
                        transaction,
                        physical_index_id,
                    )
                    .await?
            {
                if !reservation.is_owned_by(operation.index_id(), operation.generation()) {
                    return Err(corruption(
                        "vector drop found a reservation owned by another generation",
                    ));
                }
                transaction.delete(
                    IndexKey::Global {
                        kind: GlobalKey::LegacyVectorPhysicalReservation(physical_index_id),
                    }
                    .to_bytes(),
                )?;
            }
            return Ok(VectorStepResult::ordinary(
                IndexOperationStepResult::Completed(if aborting {
                    IndexOperationOutcome::Build(BuildOperationOutcome::Aborted)
                } else {
                    IndexOperationOutcome::DropSucceeded
                }),
            ));
        }
    };
    Ok(VectorStepResult::ordinary(progressed_cleanup(
        aborting, next,
    )))
}

/// One partition mapping retained until its physical namespace is empty.
struct MappingCleanupRow {
    key: Bytes,
    cursor: IndexCursor,
    input_bytes: u64,
    value: crate::index_lifecycle::work::VectorPartitionMappingValue,
}

async fn current_or_next_mapping(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    cursor: Option<&IndexCursor>,
) -> Result<Option<MappingCleanupRow>> {
    let prefix = generation_prefix(
        scope,
        RecordKind::VectorPartitionMapping,
        operation.index_id(),
        operation.generation(),
    );
    if let Some(cursor) = cursor {
        cursor_suffix(&prefix, Some(cursor))?;
        if let Some(value) = transaction.get(cursor.as_bytes()).await? {
            let key = Bytes::copy_from_slice(cursor.as_bytes());
            let decoded = decode_mapping(scope, &key, &value, operation)?;
            return Ok(Some(MappingCleanupRow {
                input_bytes: key.len().saturating_add(value.len()) as u64,
                key,
                cursor: cursor.clone(),
                value: decoded,
            }));
        }
    }
    let start = cursor_suffix(&prefix, cursor)?.map_or(Bound::Unbounded, Bound::Excluded);
    let mut rows = transaction
        .scan_prefix(&prefix, (start, Bound::<Bytes>::Unbounded))
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let value = decode_mapping(scope, &row.key, &row.value, operation)?;
    Ok(Some(MappingCleanupRow {
        input_bytes: row.key.len().saturating_add(row.value.len()) as u64,
        cursor: IndexCursor::try_new(row.key.clone()).map_err(operation_error)?,
        key: row.key,
        value,
    }))
}

enum PhysicalCleanupOutcome {
    Progress {
        counters: OperationCounters,
        namespace_empty: bool,
        mapping_deleted: bool,
    },
    Blocked(IndexOperationBlocker),
}

async fn delete_simhash_directory<D: Distance>(
    transaction: &DbTransaction,
    index: &VectorIndex<D>,
    entity_kind: IndexElementKind,
    counters: OperationCounters,
    limits: SearchIndexBatchLimits,
) -> Result<(PhysicalCleanupOutcome, VectorWriteMeasurement)> {
    let mut scan = index.simhash_directory_cleanup_scan(transaction).await?;
    let mut rows = Vec::<VectorCleanupRow>::new();
    let mut input_bytes = 0_u64;
    let mut predicted_output_bytes = 0_u64;
    let mut namespace_empty = true;
    loop {
        if rows.len() >= limits.max_entities().get() {
            namespace_empty = false;
            break;
        }
        let Some(row) = scan.next().await? else {
            break;
        };
        let next_input = input_bytes.saturating_add(row.input_bytes());
        let next_operations = rows.len().saturating_add(1) as u64;
        let next_output_bytes = predicted_output_bytes.saturating_add(row.output_bytes());
        if next_input > limits.max_input_bytes().get()
            || next_operations > limits.max_output_operations().get()
            || next_output_bytes > limits.max_output_bytes().get()
        {
            if rows.is_empty() {
                return Ok((
                    PhysicalCleanupOutcome::Blocked(IndexOperationBlocker::OversizedEntity {
                        entity_kind,
                        entity_id: IndexEntityId::initial(),
                        observed: next_input.max(next_output_bytes),
                        limit: limits
                            .max_input_bytes()
                            .get()
                            .min(limits.max_output_bytes().get()),
                    }),
                    VectorWriteMeasurement::zero(),
                ));
            }
            namespace_empty = false;
            break;
        }
        input_bytes = next_input;
        predicted_output_bytes = next_output_bytes;
        rows.push(row);
    }
    let recorder = VectorWriteRecorder::new();
    let write = recorder.bind(transaction);
    for row in &rows {
        index.stage_cleanup_row(&write, row)?;
    }
    let measured = write.measurement().map_err(measurement_error)?;
    if measured.operations() != rows.len() as u64
        || measured.encoded_bytes() != predicted_output_bytes
    {
        return Err(corruption(
            "SimHash directory cleanup measurement disagrees with staged deletes",
        ));
    }
    let counters = OperationCounters {
        entities: checked_add(
            counters.entities,
            rows.len() as u64,
            "directory cleanup entities",
        )?,
        input_bytes: checked_add(
            counters.input_bytes,
            input_bytes,
            "directory cleanup input bytes",
        )?,
        output_operations: checked_add(
            counters.output_operations,
            measured.operations(),
            "directory cleanup output operations",
        )?,
        output_bytes: checked_add(
            counters.output_bytes,
            measured.encoded_bytes(),
            "directory cleanup output bytes",
        )?,
    };
    Ok((
        PhysicalCleanupOutcome::Progress {
            counters,
            namespace_empty,
            mapping_deleted: false,
        },
        measured,
    ))
}

async fn delete_physical_namespace<D: Distance>(
    transaction: &DbTransaction,
    handle: &crate::search::vector::ValidatedVectorGenerationHandle,
    mapping: Option<&MappingCleanupRow>,
    entity_kind: IndexElementKind,
    counters: OperationCounters,
    limits: SearchIndexBatchLimits,
) -> Result<PhysicalCleanupOutcome> {
    let mapping_input_bytes = mapping.map_or(0, |mapping| mapping.input_bytes);
    if mapping_input_bytes > limits.max_input_bytes().get() {
        return Ok(PhysicalCleanupOutcome::Blocked(
            IndexOperationBlocker::OversizedEntity {
                entity_kind,
                entity_id: IndexEntityId::initial(),
                observed: mapping_input_bytes,
                limit: limits.max_input_bytes().get(),
            },
        ));
    }
    let index = VectorIndex::<D>::from_generation(handle);
    let mut scan = index.cleanup_scan(transaction).await?;
    let mut rows = Vec::<VectorCleanupRow>::new();
    let mut input_bytes = mapping_input_bytes;
    let mut predicted_output_bytes = 0_u64;
    let mut namespace_empty = true;
    // Physical storage rows are not decoded source entities. Cleanup retains
    // only typed delete tokens and is bounded by the transaction's input,
    // output-operation, and output-byte ceilings below.
    loop {
        let Some(row) = scan.next().await? else {
            break;
        };
        let next_input = input_bytes.saturating_add(row.input_bytes());
        let next_operations = rows.len().saturating_add(1) as u64;
        let next_output_bytes = predicted_output_bytes.saturating_add(row.output_bytes());
        if next_input > limits.max_input_bytes().get()
            || next_operations > limits.max_output_operations().get()
            || next_output_bytes > limits.max_output_bytes().get()
        {
            if rows.is_empty() {
                return Ok(PhysicalCleanupOutcome::Blocked(
                    IndexOperationBlocker::OversizedEntity {
                        entity_kind,
                        entity_id: IndexEntityId::initial(),
                        observed: next_input.max(next_output_bytes),
                        limit: limits
                            .max_input_bytes()
                            .get()
                            .min(limits.max_output_bytes().get()),
                    },
                ));
            }
            namespace_empty = false;
            break;
        }
        input_bytes = next_input;
        predicted_output_bytes = next_output_bytes;
        rows.push(row);
    }

    let mapping_delete_bytes = mapping.map_or(0, |mapping| mapping.key.len() as u64);
    let can_delete_mapping = namespace_empty
        && mapping.is_some()
        && (rows.len() as u64).saturating_add(1) <= limits.max_output_operations().get()
        && predicted_output_bytes.saturating_add(mapping_delete_bytes)
            <= limits.max_output_bytes().get();
    if namespace_empty && mapping.is_some() && !can_delete_mapping && rows.is_empty() {
        return Ok(PhysicalCleanupOutcome::Blocked(
            IndexOperationBlocker::OversizedEntity {
                entity_kind,
                entity_id: IndexEntityId::initial(),
                observed: mapping_input_bytes.max(mapping_delete_bytes),
                limit: limits
                    .max_input_bytes()
                    .get()
                    .min(limits.max_output_bytes().get()),
            },
        ));
    }

    let recorder = VectorWriteRecorder::new();
    let write = recorder.bind(transaction);
    for row in &rows {
        index.stage_cleanup_row(&write, row)?;
    }
    let measured = write.measurement().map_err(measurement_error)?;
    if measured.operations() != rows.len() as u64
        || measured.encoded_bytes() != predicted_output_bytes
    {
        return Err(corruption(
            "vector cleanup token measurement disagrees with staged deletes",
        ));
    }
    if can_delete_mapping {
        let Some(mapping) = mapping else {
            return Err(corruption(
                "vector cleanup admitted a mapping delete without a mapping",
            ));
        };
        transaction.delete(&mapping.key)?;
    }
    let entities = rows.len() as u64 + u64::from(can_delete_mapping && rows.is_empty());
    let counters = OperationCounters {
        entities: checked_add(counters.entities, entities, "cumulative entities")?,
        input_bytes: checked_add(counters.input_bytes, input_bytes, "cumulative input bytes")?,
        output_operations: checked_add(
            counters.output_operations,
            measured.operations() + u64::from(can_delete_mapping),
            "cumulative output operations",
        )?,
        output_bytes: checked_add(
            counters.output_bytes,
            measured.encoded_bytes()
                + if can_delete_mapping {
                    mapping_delete_bytes
                } else {
                    0
                },
            "cumulative output bytes",
        )?,
    };
    Ok(PhysicalCleanupOutcome::Progress {
        counters,
        namespace_empty,
        mapping_deleted: can_delete_mapping,
    })
}

enum CleanupWorkOutcome {
    Progress {
        counters: OperationCounters,
        exhausted: bool,
    },
    Blocked(IndexOperationBlocker),
}

async fn delete_delta_and_applied_rows(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    counters: OperationCounters,
    limits: SearchIndexBatchLimits,
) -> Result<CleanupWorkOutcome> {
    let mut accounting = VectorBatchAccounting::new(counters, limits);
    let mut exhausted = true;
    for kind in [RecordKind::BuildDelta, RecordKind::AppliedState] {
        let prefix = generation_prefix(scope, kind, operation.index_id(), operation.generation());
        let mut rows = transaction.scan_prefix(&prefix, ..).await?;
        while accounting.can_read_another() {
            let Some(row) = rows.next().await? else {
                break;
            };
            let input_bytes = row.key.len().saturating_add(row.value.len()) as u64;
            let output_bytes = row.key.len() as u64;
            if !accounting.can_admit_input(input_bytes)
                || !accounting.can_admit_output(VectorWriteMeasurement::zero(), 1, output_bytes)
            {
                if accounting.is_empty() {
                    let entity = if kind == RecordKind::BuildDelta {
                        decode_delta(scope, &row.key, &row.value)?.0
                    } else {
                        decode_applied(scope, &row.key, &row.value)?.0
                    };
                    return Ok(CleanupWorkOutcome::Blocked(
                        IndexOperationBlocker::OversizedEntity {
                            entity_kind: entity.kind,
                            entity_id: entity.id,
                            observed: input_bytes.max(output_bytes),
                            limit: limits
                                .max_input_bytes()
                                .get()
                                .min(limits.max_output_bytes().get()),
                        },
                    ));
                }
                exhausted = false;
                break;
            }
            transaction.delete(&row.key)?;
            accounting.admit(
                input_bytes,
                VectorWriteMeasurement::zero(),
                0,
                1,
                output_bytes,
            )?;
        }
        if !accounting.can_read_another() {
            exhausted = false;
            break;
        }
        if !exhausted {
            break;
        }
    }
    Ok(CleanupWorkOutcome::Progress {
        counters: accounting.finish()?,
        exhausted,
    })
}

fn progressed_cleanup(aborting: bool, progress: VectorCleanupProgress) -> IndexOperationStepResult {
    IndexOperationStepResult::Progressed(if aborting {
        IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(progress))
    } else {
        IndexOperationProgress::VectorCleanup(progress)
    })
}

struct VectorStepResult {
    result: IndexOperationStepResult,
    single_vector_output_bytes: u64,
    physical_operations: u64,
    output_bytes: u64,
    vector_planning: VectorPlanningUsage,
    retained: Option<OfferedVectorBuild>,
}

impl VectorStepResult {
    fn ordinary(result: IndexOperationStepResult) -> Self {
        Self {
            result,
            single_vector_output_bytes: 0,
            physical_operations: 0,
            output_bytes: 0,
            vector_planning: VectorPlanningUsage::default(),
            retained: None,
        }
    }

    /// Offers `session` for reuse once this step commits its progress.
    ///
    /// Only a step that progressed to another Scan step, the only stage that
    /// checks a session out, and whose session holds no unflushed rows can
    /// hand committed state to a later step; every other outcome drops the
    /// session and its lease here.
    fn retaining<D: Distance>(
        mut self,
        operation: &IndexOperationRecord,
        record: &IndexRecordV2,
        session: CheckedOutSession<D>,
    ) -> Self {
        let IndexOperationStepResult::Progressed(
            next @ IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                VectorBuildStage::Scan(_),
            )),
        ) = &self.result
        else {
            return self;
        };
        self.retained = session.into_offer(VectorPlanningCheckpoint::Build(
            VectorBuildCheckpoint::new(operation, record, next.clone()),
        ));
        self
    }

    fn metadata_transcode(
        result: IndexOperationStepResult,
        measurement: VectorWriteMeasurement,
    ) -> Self {
        Self {
            result,
            single_vector_output_bytes: 0,
            physical_operations: measurement.operations(),
            output_bytes: measurement.encoded_bytes(),
            vector_planning: VectorPlanningUsage::default(),
            retained: None,
        }
    }

    fn vector_writes(
        result: IndexOperationStepResult,
        measurement: VectorWriteMeasurement,
    ) -> Self {
        Self {
            result,
            single_vector_output_bytes: 0,
            physical_operations: measurement.operations(),
            output_bytes: measurement.encoded_bytes(),
            vector_planning: VectorPlanningUsage::default(),
            retained: None,
        }
    }

    fn with_vector_planning(mut self, vector_planning: VectorPlanningUsage) -> Self {
        self.vector_planning = vector_planning;
        self
    }

    fn into_execution(self) -> IndexOperationStepExecution {
        IndexOperationStepExecution::new(self.result)
            .with_resources(StepResourceUsage {
                physical_operations: self.physical_operations,
                output_bytes: self.output_bytes,
                single_vector_output_bytes: self.single_vector_output_bytes,
                vector_planning: self.vector_planning,
                ..StepResourceUsage::default()
            })
            .with_committed_state(
                self.retained
                    .map(|retained| CommittedStepState::VectorBuild(Box::new(retained))),
            )
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "one outbox step binds the exact durable operation, descriptor, limits, and runtime projection owner"
)]
async fn step_build<D: Distance>(
    db: &Db,
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    record: &IndexRecordV2,
    definition: &ValidatedVectorIndexDefinition,
    stage: &VectorBuildStage,
    limits: SearchIndexBatchLimits,
    scan_tuning: IndexLifecycleScanTuning,
    simhasher_registry: Arc<vector::SimHasherRegistry>,
    batch_reads: crate::batch_reads::BatchReads,
    build_cache: &VectorBuildCache,
) -> Result<VectorStepResult> {
    match stage {
        VectorBuildStage::AdoptLegacy(progress) => {
            adopt_legacy::<D>(
                transaction,
                scope,
                operation,
                record,
                definition,
                progress,
                limits,
            )
            .await
        }
        VectorBuildStage::ValidateAdoptedDirectory(progress) => {
            validate_adopted_directory::<D>(
                transaction,
                scope,
                operation,
                record,
                definition,
                progress,
                limits,
            )
            .await
        }
        VectorBuildStage::Scan(progress) => {
            let mut session = build_cache
                .checkout::<D>(&VectorBuildCheckpoint::new(
                    operation,
                    record,
                    operation.progress().clone(),
                ))
                .await;
            let step = scan_source::<D>(
                db,
                transaction,
                scope,
                operation,
                record,
                definition,
                progress,
                limits,
                scan_tuning,
                simhasher_registry,
                batch_reads,
                &mut session,
            )
            .await?;
            Ok(step.retaining(operation, record, session))
        }
        VectorBuildStage::CatchUp(progress) => {
            // Only builds started before operations were queued persist this
            // stage; queued builds go from Scan to ValidateDescriptor.
            if has_pre_queue_deltas(transaction, scope, operation).await? {
                return Ok(VectorStepResult::ordinary(
                    IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation),
                ));
            }
            Ok(VectorStepResult::ordinary(progressed_build(
                VectorBuildStage::ValidateDescriptor(PrefixScanProgress {
                    cursor: None,
                    counters: progress.counters,
                }),
            )))
        }
        VectorBuildStage::ValidateDescriptor(progress) => Ok(VectorStepResult::ordinary(
            validate_descriptor::<D>(
                db,
                transaction,
                scope,
                operation,
                record,
                definition,
                progress,
                limits,
                simhasher_registry,
            )
            .await?,
        )),
        VectorBuildStage::Activate(progress) => {
            let Some(PhysicalGeneration::Vector { layout, .. }) = record.state().physical() else {
                return Err(corruption(
                    "vector activation is not bound to a physical generation",
                ));
            };
            let physical_index_id = match layout {
                VectorPhysicalLayout::Unpartitioned { physical_index_id } => {
                    Some(*physical_index_id)
                }
                VectorPhysicalLayout::Partitioned => None,
            };
            if let Some((physical_index_id, reservation)) = match physical_index_id {
                Some(physical_index_id) => {
                    super::super::repository::load_legacy_vector_physical_reservation(
                        transaction,
                        physical_index_id,
                    )
                    .await?
                    .map(|reservation| (physical_index_id, reservation))
                }
                None => None,
            } {
                let Some(active_reservation) = reservation.activate(
                    operation.index_id(),
                    operation.generation(),
                    operation.operation_id(),
                ) else {
                    return Err(corruption(
                        "vector activation found a non-adoptable physical reservation",
                    ));
                };
                if generation_has_rows(
                    transaction,
                    scope,
                    RecordKind::BuildDelta,
                    operation.index_id(),
                    operation.generation(),
                )
                .await?
                    || generation_has_rows(
                        transaction,
                        scope,
                        RecordKind::AppliedState,
                        operation.index_id(),
                        operation.generation(),
                    )
                    .await?
                {
                    return Err(corruption(
                        "legacy vector adoption unexpectedly produced graph build rows",
                    ));
                }
                let source = crate::migrations::legacy_vector_adoption_source(
                    transaction,
                    scope,
                    definition,
                )
                .await?;
                if crate::search::vector::index_id_from_name(source.physical_name())
                    != physical_index_id.get()
                {
                    return Err(corruption(
                        "legacy vector activation source differs from its reserved namespace",
                    ));
                }
                let handle = ValidatedVectorBuildGenerationHandle::try_from_building::<D>(
                    scope,
                    record,
                    operation.operation_id(),
                    physical_index_id,
                )
                .map_err(|error| corruption(error.to_string()))?;
                let legacy = VectorIndex::<D>::for_legacy_migration(source.physical_name(), scope);
                #[cfg(feature = "production-coverage")]
                crate::migrations::trip_migration_failpoint(
                    crate::migrations::MigrationFailpoint::LegacyVectorMetadataPublicationBefore,
                )?;
                let measurement = legacy
                    .transcode_legacy_metadata(
                        transaction,
                        definition,
                        handle.generation().physical_name(),
                    )
                    .await?;
                if measurement.operations() != 1 {
                    return Err(corruption(
                        "legacy vector activation did not transcode exactly one metadata row",
                    ));
                }
                #[cfg(feature = "production-coverage")]
                crate::migrations::trip_migration_failpoint(
                    crate::migrations::MigrationFailpoint::LegacyVectorMetadataPublicationAfter,
                )?;
                #[cfg(feature = "production-coverage")]
                crate::migrations::trip_migration_failpoint(
                    crate::migrations::MigrationFailpoint::LegacyVectorReservationTransitionBefore,
                )?;
                transaction.put(
                    IndexKey::Global {
                        kind: GlobalKey::LegacyVectorPhysicalReservation(physical_index_id),
                    }
                    .to_bytes(),
                    encode_metadata_value(&IndexV2MetadataValue::LegacyVectorPhysicalReservation(
                        active_reservation,
                    )),
                )?;
                #[cfg(feature = "production-coverage")]
                crate::migrations::trip_migration_failpoint(
                    crate::migrations::MigrationFailpoint::LegacyVectorReservationTransitionAfter,
                )?;
                #[cfg(feature = "production-coverage")]
                crate::migrations::trip_migration_failpoint(
                    crate::migrations::MigrationFailpoint::LegacyDefinitionRetirementBefore,
                )?;
                transaction.delete(source.storage_key())?;
                #[cfg(feature = "production-coverage")]
                crate::migrations::trip_migration_failpoint(
                    crate::migrations::MigrationFailpoint::LegacyDefinitionRetirementAfter,
                )?;
                tracing::info!(
                    operation_id = %operation.operation_id().as_uuid(),
                    physical_index_id = physical_index_id.get(),
                    metadata_output_bytes = measurement.encoded_bytes(),
                    "adopted legacy vector namespace without rebuilding HNSW rows"
                );
                return Ok(VectorStepResult::metadata_transcode(
                    IndexOperationStepResult::Completed(IndexOperationOutcome::Build(
                        BuildOperationOutcome::Succeeded,
                    )),
                    measurement,
                ));
            }
            if has_pre_queue_deltas(transaction, scope, operation).await? {
                return Ok(VectorStepResult::ordinary(
                    IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation),
                ));
            }
            if generation_has_rows(
                transaction,
                scope,
                RecordKind::AppliedState,
                operation.index_id(),
                operation.generation(),
            )
            .await?
            {
                return Ok(VectorStepResult::ordinary(progressed_build(
                    VectorBuildStage::ValidateDescriptor(PrefixScanProgress {
                        cursor: None,
                        counters: progress.counters,
                    }),
                )));
            }
            Ok(VectorStepResult::ordinary(
                IndexOperationStepResult::Completed(IndexOperationOutcome::Build(
                    BuildOperationOutcome::Succeeded,
                )),
            ))
        }
    }
}

/// Detects build deltas written before vector operations were queued.
///
/// Queued builds never write `BuildDelta` rows: writes during a build enqueue
/// complete operations that the publisher applies after activation. Rows left
/// by an in-flight pre-queue build are not replayed, so that build blocks
/// until it is aborted and the index is created again.
async fn has_pre_queue_deltas(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
) -> Result<bool> {
    let found = generation_has_rows(
        transaction,
        scope,
        RecordKind::BuildDelta,
        operation.index_id(),
        operation.generation(),
    )
    .await?;
    if found {
        tracing::error!(
            operation_id = %operation.operation_id().as_uuid(),
            "vector build holds pre-queue build deltas; abort it and create the index again"
        );
    }
    Ok(found)
}

#[allow(
    clippy::too_many_arguments,
    reason = "legacy validation binds exact catalog, operation, namespace, and batch authorities"
)]
async fn adopt_legacy<D: Distance>(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    record: &IndexRecordV2,
    definition: &ValidatedVectorIndexDefinition,
    progress: &LegacyVectorValidationProgress,
    limits: SearchIndexBatchLimits,
) -> Result<VectorStepResult> {
    let Some(PhysicalGeneration::Vector {
        layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
        descriptor,
        ..
    }) = record.state().physical()
    else {
        return Err(corruption(
            "legacy vector adoption is not bound to one unpartitioned physical namespace",
        ));
    };
    let Some(reservation) = super::super::repository::load_legacy_vector_physical_reservation(
        transaction,
        *physical_index_id,
    )
    .await?
    else {
        return Err(corruption(
            "legacy vector adoption lost its physical reservation",
        ));
    };
    if reservation
        != (LegacyVectorPhysicalReservation::AdoptionBuilding {
            index_id: operation.index_id(),
            generation: operation.generation(),
            operation_id: operation.operation_id(),
        })
    {
        return Err(corruption(
            "legacy vector adoption reservation belongs to another generation",
        ));
    }
    let runtime = definition.to_runtime();
    let legacy_name = crate::search::vector_index_name(
        runtime.element_type(),
        runtime.label(),
        runtime.property(),
    );
    if crate::search::vector::index_id_from_name(&legacy_name) != physical_index_id.get() {
        return Err(corruption(
            "legacy vector adoption physical ID differs from its deterministic name",
        ));
    }
    let lane = match progress.lane {
        LegacyVectorValidationLane::Core => VectorStorageLane::Core,
        LegacyVectorValidationLane::Hot => VectorStorageLane::Hot,
        LegacyVectorValidationLane::Layer0 => VectorStorageLane::Layer0,
    };
    let started = std::time::Instant::now();
    let legacy = VectorIndex::<D>::for_legacy_migration(legacy_name, scope);
    #[cfg(feature = "production-coverage")]
    crate::migrations::trip_migration_failpoint(
        crate::migrations::MigrationFailpoint::LegacyVectorValidationCheckpointBefore,
    )?;
    let outcome = legacy
        .validate_legacy_physical(
            transaction,
            vector::LegacyVectorValidationPass::new(
                lane,
                match descriptor.routing_layout() {
                    VectorRoutingLayoutV2::LegacyHnsw => {
                        vector::LegacyVectorValidationMode::ReadOnly
                    }
                    VectorRoutingLayoutV2::SimHashDirectoryV1 => {
                        vector::LegacyVectorValidationMode::BackfillSimHashDirectory {
                            max_output_operations: limits.max_output_operations(),
                            max_output_bytes: limits.max_output_bytes(),
                        }
                    }
                },
            ),
            progress
                .cursor
                .as_ref()
                .map(|cursor| cursor.as_bytes().as_ref()),
            definition,
            limits.max_entities().get(),
            limits.max_input_bytes().get(),
        )
        .await?;
    let vector::LegacyVectorValidationOutcome::Valid {
        last_key,
        rows,
        input_bytes,
        exhausted,
        directory_entries,
        predicted_directory_writes,
    } = outcome
    else {
        match outcome {
            vector::LegacyVectorValidationOutcome::Oversized { observed, limit } => {
                return Ok(VectorStepResult::ordinary(
                    IndexOperationStepResult::Blocked(IndexOperationBlocker::OversizedEntity {
                        entity_kind: definition.element_kind(),
                        entity_id: IndexEntityId::initial(),
                        observed,
                        limit,
                    }),
                ));
            }
            vector::LegacyVectorValidationOutcome::Invalid { reason } => {
                tracing::error!(
                    operation_id = %operation.operation_id().as_uuid(),
                    lane = ?progress.lane,
                    reason,
                    "legacy vector physical validation failed"
                );
                return Ok(VectorStepResult::ordinary(
                    IndexOperationStepResult::Blocked(IndexOperationBlocker::InvalidLegacyPhysical),
                ));
            }
            vector::LegacyVectorValidationOutcome::Valid { .. } => unreachable!(),
        }
    };
    #[cfg(feature = "production-coverage")]
    crate::migrations::trip_migration_failpoint(
        crate::migrations::MigrationFailpoint::LegacyVectorValidationCheckpointAfter,
    )?;
    let recorder = VectorWriteRecorder::new();
    let measured_transaction = recorder.bind(transaction);
    for entry in &directory_entries {
        legacy.stage_simhash_directory_entry(&measured_transaction, entry)?;
    }
    let actual_directory_writes = measured_transaction
        .measurement()
        .map_err(|error| corruption(format!("directory write measurement failed: {error}")))?;
    if actual_directory_writes != predicted_directory_writes {
        return Err(corruption(
            "typed legacy directory writes differ from their admitted prediction",
        ));
    }
    let counters = OperationCounters {
        entities: checked_add(
            progress.counters.entities,
            rows,
            "legacy validation entities",
        )?,
        input_bytes: checked_add(
            progress.counters.input_bytes,
            input_bytes,
            "legacy validation input bytes",
        )?,
        output_operations: checked_add(
            progress.counters.output_operations,
            actual_directory_writes.operations(),
            "legacy directory output operations",
        )?,
        output_bytes: checked_add(
            progress.counters.output_bytes,
            actual_directory_writes.encoded_bytes(),
            "legacy directory output bytes",
        )?,
    };
    tracing::info!(
        operation_id = %operation.operation_id().as_uuid(),
        lane = ?progress.lane,
        rows,
        marker_count = actual_directory_writes.operations(),
        input_bytes,
        output_bytes = actual_directory_writes.encoded_bytes(),
        cursor = ?last_key,
        exhausted,
        elapsed_millis = started.elapsed().as_millis(),
        "validated legacy vector physical checkpoint"
    );
    let next = if exhausted {
        match progress.lane.next() {
            Some(lane) => VectorBuildStage::AdoptLegacy(LegacyVectorValidationProgress {
                lane,
                cursor: None,
                counters,
            }),
            None => match descriptor.routing_layout() {
                VectorRoutingLayoutV2::LegacyHnsw => {
                    VectorBuildStage::Activate(NoCursorProgress { counters })
                }
                VectorRoutingLayoutV2::SimHashDirectoryV1 => {
                    VectorBuildStage::ValidateAdoptedDirectory(
                        LegacyVectorDirectoryValidationProgress::initial(
                            counters.output_operations,
                            counters,
                        ),
                    )
                }
            },
        }
    } else {
        let Some(last_key) = last_key else {
            return Err(corruption(
                "non-exhausted legacy validation batch has no completed cursor",
            ));
        };
        VectorBuildStage::AdoptLegacy(LegacyVectorValidationProgress {
            lane: progress.lane,
            cursor: Some(IndexCursor::try_new(last_key).map_err(operation_error)?),
            counters,
        })
    };
    Ok(VectorStepResult::vector_writes(
        progressed_build(next),
        actual_directory_writes,
    ))
}

#[allow(
    clippy::too_many_arguments,
    reason = "directory validation binds exact catalog, operation, namespace, and batch authorities"
)]
async fn validate_adopted_directory<D: Distance>(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    record: &IndexRecordV2,
    definition: &ValidatedVectorIndexDefinition,
    progress: &LegacyVectorDirectoryValidationProgress,
    limits: SearchIndexBatchLimits,
) -> Result<VectorStepResult> {
    let Some(PhysicalGeneration::Vector {
        layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
        descriptor,
        ..
    }) = record.state().physical()
    else {
        return Err(corruption(
            "legacy directory validation is not bound to one unpartitioned namespace",
        ));
    };
    if descriptor.routing_layout() != VectorRoutingLayoutV2::SimHashDirectoryV1 {
        return Err(corruption(
            "legacy directory validation is bound to a non-directory descriptor",
        ));
    }
    if progress.expected_markers != progress.counters.output_operations
        || progress.verified_markers > progress.expected_markers
    {
        return Err(corruption(
            "legacy directory validation counters disagree with marker writes",
        ));
    }
    let Some(reservation) = super::super::repository::load_legacy_vector_physical_reservation(
        transaction,
        *physical_index_id,
    )
    .await?
    else {
        return Err(corruption(
            "legacy directory validation lost its physical reservation",
        ));
    };
    if reservation
        != (LegacyVectorPhysicalReservation::AdoptionBuilding {
            index_id: operation.index_id(),
            generation: operation.generation(),
            operation_id: operation.operation_id(),
        })
    {
        return Err(corruption(
            "legacy directory validation reservation belongs to another generation",
        ));
    }
    let runtime = definition.to_runtime();
    let legacy_name = crate::search::vector_index_name(
        runtime.element_type(),
        runtime.label(),
        runtime.property(),
    );
    if crate::search::vector::index_id_from_name(&legacy_name) != physical_index_id.get() {
        return Err(corruption(
            "legacy directory validation physical ID differs from its deterministic name",
        ));
    }
    let started = std::time::Instant::now();
    let legacy = VectorIndex::<D>::for_legacy_migration(legacy_name, scope);
    let outcome = legacy
        .validate_simhash_directory(
            transaction,
            progress
                .cursor
                .as_ref()
                .map(|cursor| cursor.as_bytes().as_ref()),
            definition,
            vector::SimHashDirectoryValidationMode::FinalLegacyWithEntryPoint,
            limits.max_entities().get(),
            limits.max_input_bytes().get(),
        )
        .await?;
    let vector::SimHashDirectoryValidationOutcome::Valid {
        last_key,
        markers,
        input_bytes,
        exhausted,
    } = outcome
    else {
        match outcome {
            vector::SimHashDirectoryValidationOutcome::Oversized { observed, limit } => {
                return Ok(VectorStepResult::ordinary(
                    IndexOperationStepResult::Blocked(IndexOperationBlocker::OversizedEntity {
                        entity_kind: definition.element_kind(),
                        entity_id: IndexEntityId::initial(),
                        observed,
                        limit,
                    }),
                ));
            }
            vector::SimHashDirectoryValidationOutcome::Invalid { reason } => {
                tracing::error!(
                    operation_id = %operation.operation_id().as_uuid(),
                    reason,
                    "legacy vector directory validation failed"
                );
                return Ok(VectorStepResult::ordinary(
                    IndexOperationStepResult::Blocked(IndexOperationBlocker::InvalidLegacyPhysical),
                ));
            }
            vector::SimHashDirectoryValidationOutcome::Valid { .. } => unreachable!(),
        }
    };
    let verified_markers = checked_add(
        progress.verified_markers,
        markers,
        "verified legacy directory markers",
    )?;
    if verified_markers > progress.expected_markers {
        tracing::error!(
            operation_id = %operation.operation_id().as_uuid(),
            expected_markers = progress.expected_markers,
            verified_markers,
            "legacy vector directory contains extra markers"
        );
        return Ok(VectorStepResult::ordinary(
            IndexOperationStepResult::Blocked(IndexOperationBlocker::InvalidLegacyPhysical),
        ));
    }
    let counters = OperationCounters {
        entities: checked_add(
            progress.counters.entities,
            markers,
            "legacy directory validation entities",
        )?,
        input_bytes: checked_add(
            progress.counters.input_bytes,
            input_bytes,
            "legacy directory validation input bytes",
        )?,
        output_operations: progress.counters.output_operations,
        output_bytes: progress.counters.output_bytes,
    };
    tracing::info!(
        operation_id = %operation.operation_id().as_uuid(),
        stage = "validate_adopted_directory",
        markers,
        input_bytes,
        cursor = ?last_key,
        exhausted,
        verified_markers,
        expected_markers = progress.expected_markers,
        elapsed_millis = started.elapsed().as_millis(),
        "validated legacy vector directory checkpoint"
    );
    let next = if exhausted {
        if verified_markers != progress.expected_markers {
            tracing::error!(
                operation_id = %operation.operation_id().as_uuid(),
                expected_markers = progress.expected_markers,
                verified_markers,
                "legacy vector directory marker count is incomplete"
            );
            return Ok(VectorStepResult::ordinary(
                IndexOperationStepResult::Blocked(IndexOperationBlocker::InvalidLegacyPhysical),
            ));
        }
        VectorBuildStage::Activate(NoCursorProgress { counters })
    } else {
        let Some(last_key) = last_key else {
            return Err(corruption(
                "non-exhausted legacy directory batch has no completed cursor",
            ));
        };
        VectorBuildStage::ValidateAdoptedDirectory(LegacyVectorDirectoryValidationProgress {
            cursor: Some(IndexCursor::try_new(last_key).map_err(operation_error)?),
            expected_markers: progress.expected_markers,
            verified_markers,
            counters,
        })
    };
    Ok(VectorStepResult::ordinary(progressed_build(next)))
}

#[allow(
    clippy::too_many_arguments,
    reason = "source scanning retains exact operation and physical planning authority"
)]
async fn scan_source<D: Distance>(
    db: &Db,
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    record: &IndexRecordV2,
    definition: &ValidatedVectorIndexDefinition,
    progress: &SourceScanProgress,
    limits: SearchIndexBatchLimits,
    scan_tuning: IndexLifecycleScanTuning,
    simhasher_registry: Arc<vector::SimHasherRegistry>,
    batch_reads: crate::batch_reads::BatchReads,
    build_session: &mut CheckedOutSession<D>,
) -> Result<VectorStepResult> {
    let source_prefix = source_prefix(scope, definition.element_kind());
    let start = cursor_suffix(&source_prefix, progress.cursor.as_ref())?;
    let upper = cursor_suffix(&source_prefix, Some(&progress.inclusive_upper_bound))?
        .ok_or_else(|| corruption("vector source upper bound is absent"))?;
    match start.as_ref().map(|start| start.cmp(&upper)) {
        Some(std::cmp::Ordering::Greater) => {
            return Err(corruption(
                "vector source cursor exceeds its inclusive upper bound",
            ));
        }
        Some(std::cmp::Ordering::Equal) => {
            return Ok(VectorStepResult::ordinary(progressed_build(
                VectorBuildStage::ValidateDescriptor(PrefixScanProgress {
                    cursor: None,
                    counters: progress.counters,
                }),
            )));
        }
        Some(std::cmp::Ordering::Less) | None => {}
    }
    let start = start.map_or(Bound::Unbounded, Bound::Excluded);
    let scan_options = scan_tuning.scan_options();
    let mut rows = transaction
        .scan_prefix_with_options(
            &source_prefix,
            (start, Bound::Included(upper)),
            &scan_options,
        )
        .await?;
    let target = VectorPlanTarget::build(scope, operation, record)?;
    let planning = db.begin(IsolationLevel::Snapshot).await?;
    let planning_recorder = VectorWriteRecorder::new();
    let mut accounting = VectorBatchAccounting::new(progress.counters, limits);
    let mut cursor = progress.cursor.clone();
    let mut exhausted = true;
    while accounting.can_read_another() {
        let Some(row) = rows.next().await? else {
            break;
        };
        let input_bytes = row.key.len().saturating_add(row.value.len()) as u64;
        if !accounting.can_admit_input(input_bytes) {
            if accounting.is_empty() {
                let entity_id = source_entity(scope, definition.element_kind(), &row.key)?
                    .unwrap_or(IndexEntityId::initial());
                return Ok(VectorStepResult::ordinary(
                    IndexOperationStepResult::Blocked(IndexOperationBlocker::OversizedEntity {
                        entity_kind: definition.element_kind(),
                        entity_id,
                        observed: input_bytes,
                        limit: limits.max_input_bytes().get(),
                    }),
                ));
            }
            exhausted = false;
            break;
        }
        let complete_cursor = IndexCursor::try_new(row.key.clone()).map_err(operation_error)?;
        let Some(entity_id) = source_entity(scope, definition.element_kind(), &row.key)? else {
            accounting.admit(input_bytes, VectorWriteMeasurement::zero(), 0, 0, 0)?;
            cursor = Some(complete_cursor);
            continue;
        };
        let properties = match decode_properties(&row.value) {
            Ok(properties) => properties,
            Err(_) => {
                return Ok(VectorStepResult::ordinary(invalid_source(
                    definition.element_kind(),
                    entity_id,
                )));
            }
        };
        let document = match vector_document(definition, &properties) {
            Ok(document) => document,
            Err(_) => {
                return Ok(VectorStepResult::ordinary(invalid_source(
                    definition.element_kind(),
                    entity_id,
                )));
            }
        };
        if load_applied(
            transaction,
            scope,
            operation.index_id(),
            operation.generation(),
            definition.element_kind(),
            entity_id,
        )
        .await?
        .is_some()
        {
            return Err(corruption(
                "vector source cursor has not advanced past existing applied state",
            ));
        }
        let outcome = plan_and_apply::<D>(
            &planning,
            &planning_recorder,
            transaction,
            &target,
            definition,
            Arc::clone(&simhasher_registry),
            batch_reads,
            entity_id,
            &[],
            document.as_ref(),
            &accounting,
            build_session,
        )
        .await?;
        accounting.record_planning();
        let EntityPlanOutcome::Admitted {
            vector_writes,
            single_vector_output_bytes,
            lifecycle_operations,
            lifecycle_bytes,
            next_partition,
        } = outcome
        else {
            build_session.discard_entity();
            return finish_or_block_scan(
                outcome,
                accounting,
                definition.element_kind(),
                entity_id,
                progress,
                cursor,
                build_session.stats(),
            );
        };
        if next_partition.is_some() {
            stage_applied(
                transaction,
                scope,
                operation,
                definition.element_kind(),
                entity_id,
                next_partition,
            )?;
        }
        accounting.admit(
            input_bytes,
            vector_writes,
            single_vector_output_bytes,
            lifecycle_operations,
            lifecycle_bytes,
        )?;
        cursor = Some(complete_cursor);
    }
    if !accounting.can_read_another() {
        exhausted = false;
    }
    let vector_planning = accounting.planning_usage(build_session.stats());
    let (counters, single_vector_output_bytes) = accounting.finish_with_max()?;
    let next = if exhausted {
        VectorBuildStage::ValidateDescriptor(PrefixScanProgress {
            cursor: None,
            counters,
        })
    } else {
        VectorBuildStage::Scan(SourceScanProgress {
            inclusive_upper_bound: progress.inclusive_upper_bound.clone(),
            cursor,
            counters,
        })
    };
    Ok(VectorStepResult {
        result: progressed_build(next),
        single_vector_output_bytes,
        physical_operations: 0,
        output_bytes: 0,
        vector_planning,
        retained: None,
    })
}

/// Generation one planner writes, and the owner its physical rows answer to.
///
/// Either owner is the only writer of the generation's physical rows while it
/// plans, so planning may read committed rows outside the transaction that
/// commits the plan: [`RetainedVectorBuild`] states the build's exclusivity and
/// [`VectorPublicationCheckpoint`] the publisher's.
pub(super) struct VectorPlanTarget<'a> {
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
    layout: VectorPhysicalLayout,
    owner: VectorPlanOwner<'a>,
}

/// Lifecycle owner of a planned generation.
#[derive(Clone, Copy)]
enum VectorPlanOwner<'a> {
    /// The build of a hidden generation. It plans each source entity once,
    /// before any other state of it, so its insertions are proven fresh.
    Build {
        operation: &'a IndexOperationRecord,
        record: &'a IndexRecordV2,
    },
    /// Queue publication into an Active generation. An entity may already be
    /// indexed, a tenant partition its removal empties is reclaimed, and the
    /// admitted writes to resident-cache rows are fenced in `cache_writes`.
    /// Publication writes no applied-state rows.
    Publication {
        active: &'a ActiveIndexHandle,
        cache_writes: &'a vector::VectorCacheWriteSet,
    },
}

impl<'a> VectorPlanTarget<'a> {
    /// Targets the hidden generation `operation` builds.
    fn build(
        scope: DataScope,
        operation: &'a IndexOperationRecord,
        record: &'a IndexRecordV2,
    ) -> Result<Self> {
        let IndexStateVectorPhysical { layout } = IndexStateVectorPhysical::from_record(record)?;
        Ok(Self {
            scope,
            index_id: operation.index_id(),
            generation: operation.generation(),
            layout,
            owner: VectorPlanOwner::Build { operation, record },
        })
    }

    /// Targets the Active generation `active` for queue publication.
    pub(super) fn publication(
        active: &'a ActiveIndexHandle,
        cache_writes: &'a vector::VectorCacheWriteSet,
    ) -> Result<Self> {
        let ActiveIndexHandle::Vector {
            scope,
            index_id,
            generation,
            layout,
            ..
        } = active
        else {
            return Err(corruption(
                "vector publication received another family handle",
            ));
        };
        Ok(Self {
            scope: *scope,
            index_id: *index_id,
            generation: *generation,
            layout: *layout,
            owner: VectorPlanOwner::Publication {
                active,
                cache_writes,
            },
        })
    }

    /// Projects one physical namespace of the generation under its owner.
    fn physical<D: Distance>(
        &self,
        physical_index_id: VectorPhysicalIndexId,
    ) -> Result<ValidatedVectorGenerationHandle> {
        match self.owner {
            VectorPlanOwner::Build { operation, record } => {
                ValidatedVectorBuildGenerationHandle::try_from_building::<D>(
                    self.scope,
                    record,
                    operation.operation_id(),
                    physical_index_id,
                )
                .map(|handle| handle.generation().clone())
            }
            VectorPlanOwner::Publication { active, .. } => {
                ValidatedVectorGenerationHandle::try_from_active::<D>(active, physical_index_id)
            }
        }
        .map_err(|error| corruption(error.to_string()))
    }
}

/// Plans one entity's transition and applies it to `transaction` if it fits.
///
/// The entity is removed from every `previous` partition other than its next
/// one, then inserted at its deterministic layer. Planning reads and writes
/// only the disposable `planning` transaction; `transaction` receives new
/// tenant mappings, publication's reclamations, and the captured plan only
/// once `accounting` admits the entity.
#[allow(
    clippy::too_many_arguments,
    reason = "planning binds the exact target, descriptor, entity transition, and both transactions"
)]
pub(super) async fn plan_and_apply<D: Distance>(
    planning: &DbTransaction,
    planning_recorder: &VectorWriteRecorder,
    transaction: &DbTransaction,
    target: &VectorPlanTarget<'_>,
    definition: &ValidatedVectorIndexDefinition,
    simhasher_registry: Arc<vector::SimHasherRegistry>,
    batch_reads: crate::batch_reads::BatchReads,
    entity_id: IndexEntityId,
    previous: &[TextPartition],
    next_document: Option<&VectorIndexedDocument>,
    accounting: &VectorBatchAccounting,
    build_session: &mut CheckedOutSession<D>,
) -> Result<EntityPlanOutcome> {
    let next_partition = next_document.map(|document| document.partition().clone());
    let next = match next_document {
        Some(document) => {
            let resolution =
                resolve_build_physical(transaction, target, document.partition(), true).await?;
            let handle = target.physical::<D>(resolution.physical_index_id)?;
            if resolution.mapping_is_new {
                require_unallocated_namespace(planning, transaction, &handle).await?;
            }
            Some((resolution, handle))
        }
        None => None,
    };
    let mut removals = Vec::new();
    for partition in previous
        .iter()
        .filter(|partition| Some(*partition) != next_partition.as_ref())
    {
        let Some(physical_index_id) =
            resolve_existing_build_physical(transaction, target, partition).await?
        else {
            // The partition never materialized, so nothing is indexed there.
            continue;
        };
        removals.push((partition, target.physical::<D>(physical_index_id)?));
    }
    let layer = next_document.map(|document| {
        deterministic_layer(
            target.index_id,
            target.generation,
            definition,
            entity_id,
            document,
        )
    });
    let planning_write = planning_recorder.bind(planning);
    let checkpoint = planning_write.checkpoint();
    apply_planned_change::<D>(
        &planning_write,
        target,
        definition,
        Arc::clone(&simhasher_registry),
        batch_reads,
        entity_id,
        &removals,
        next.as_ref(),
        next_document,
        layer,
        build_session,
    )
    .await?;
    build_session.flush_all(&planning_write)?;
    build_session.rebind().await;
    build_session.enforce_limits(&planning_write)?;
    let reclaimed = match target.owner {
        VectorPlanOwner::Build { .. } => Vec::new(),
        VectorPlanOwner::Publication { .. } => {
            let mut reclaimed = Vec::new();
            for (partition, handle) in &removals {
                let TextPartition::TenantValue(_) = partition else {
                    continue;
                };
                if super::publication::stage_empty_tenant_reclamation::<D>(&planning_write, handle)
                    .await?
                {
                    let tenant = VectorTenantPartition::try_from_partition((*partition).clone())
                        .map_err(|error| corruption(error.to_string()))?;
                    reclaimed.push((tenant, handle.clone()));
                }
            }
            reclaimed
        }
    };
    let plan: PlannedVectorMutation = planning_write
        .plan_since(checkpoint)
        .map_err(measurement_error)?;
    let entity_vector = plan.measurement();
    let cumulative_vector = planning_write.measurement().map_err(measurement_error)?;
    let applied_transition = match (target.owner, next_partition.as_ref()) {
        (VectorPlanOwner::Publication { .. }, _) => AppliedStateTransition::Absent,
        (VectorPlanOwner::Build { .. }, Some(partition)) => AppliedStateTransition::Put(partition),
        (VectorPlanOwner::Build { .. }, None) if previous.is_empty() => {
            AppliedStateTransition::Absent
        }
        (VectorPlanOwner::Build { .. }, None) => AppliedStateTransition::Delete,
    };
    let new_mapping = next_partition.as_ref().zip(
        next.as_ref()
            .filter(|(resolution, _)| resolution.mapping_is_new)
            .map(|(resolution, _)| resolution.physical_index_id),
    );
    let (lifecycle_operations, lifecycle_bytes) = lifecycle_write_measurement(
        target,
        definition.element_kind(),
        entity_id,
        applied_transition,
        new_mapping,
        &reclaimed,
    )?;
    if entity_vector.encoded_bytes() > accounting.limits.max_single_vector_output_bytes().get() {
        return Ok(EntityPlanOutcome::Blocked(
            IndexOperationBlocker::OversizedEntity {
                entity_kind: definition.element_kind(),
                entity_id,
                observed: entity_vector.encoded_bytes(),
                limit: accounting.limits.max_single_vector_output_bytes().get(),
            },
        ));
    }
    if !accounting.can_admit_output(cumulative_vector, lifecycle_operations, lifecycle_bytes) {
        if accounting.is_empty() {
            return Ok(EntityPlanOutcome::Blocked(
                IndexOperationBlocker::OversizedEntity {
                    entity_kind: definition.element_kind(),
                    entity_id,
                    observed: cumulative_vector
                        .encoded_bytes()
                        .saturating_add(accounting.lifecycle_bytes)
                        .saturating_add(lifecycle_bytes),
                    limit: accounting.limits.max_output_bytes().get(),
                },
            ));
        }
        return Ok(EntityPlanOutcome::BatchFull);
    }
    if let Some((partition, physical_index_id)) = new_mapping {
        let partition = VectorTenantPartition::try_from_partition(partition.clone())
            .map_err(|error| corruption(error.to_string()))?;
        let allocated = crate::index_lifecycle::repository::stage_vector_partition_mapping(
            transaction,
            target.scope,
            target.index_id,
            target.generation,
            VectorPhysicalLayout::Partitioned,
            &partition,
        )
        .await?;
        if allocated != physical_index_id {
            return Err(corruption(
                "vector physical allocation changed after admitted planning",
            ));
        }
    }
    match target.owner {
        VectorPlanOwner::Build { .. } => {}
        VectorPlanOwner::Publication { cache_writes, .. } => {
            for (tenant, handle) in &reclaimed {
                crate::index_lifecycle::repository::stage_delete_vector_partition_mapping(
                    transaction,
                    target.scope,
                    target.index_id,
                    target.generation,
                    target.layout,
                    tenant,
                    handle.identity().physical_index_id(),
                )
                .await?;
            }
            for handle in removals
                .iter()
                .map(|(_, handle)| handle)
                .chain(next.iter().map(|(_, handle)| handle))
            {
                cache_writes.record_planned(handle, &plan)?;
            }
            // Retirement replaces the reclaimed namespaces' dirty rows.
            for (_, handle) in &reclaimed {
                cache_writes.retire_after_commit(handle);
            }
        }
    }
    plan.apply_to(transaction)?;
    build_session.admit_entity();
    Ok(EntityPlanOutcome::Admitted {
        vector_writes: cumulative_vector,
        single_vector_output_bytes: entity_vector.encoded_bytes(),
        lifecycle_operations,
        lifecycle_bytes,
        next_partition,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "one deterministic HNSW plan binds every partition endpoint and the exact owner"
)]
async fn apply_planned_change<D: Distance>(
    write: &MeasuredVectorTransaction<'_>,
    target: &VectorPlanTarget<'_>,
    definition: &ValidatedVectorIndexDefinition,
    simhasher_registry: Arc<vector::SimHasherRegistry>,
    batch_reads: crate::batch_reads::BatchReads,
    entity_id: IndexEntityId,
    removals: &[(&TextPartition, ValidatedVectorGenerationHandle)],
    next: Option<&(BuildPhysicalResolution, ValidatedVectorGenerationHandle)>,
    next_document: Option<&VectorIndexedDocument>,
    layer: Option<u16>,
    build_session: &mut VectorBuildSession<D>,
) -> Result<()> {
    // Every existing namespace is checked against the canonical definition
    // before planning reads its degree limits or entry point, so corrupt
    // metadata fails closed instead of being planned under.
    for (_, handle) in removals {
        let index = VectorIndex::<D>::from_generation(handle)
            .with_simhasher_registry(Arc::clone(&simhasher_registry))
            .with_batch_reads(batch_reads);
        let Some(metadata) = index.get_metadata(write).await? else {
            return Err(corruption("vector physical namespace has no metadata"));
        };
        validate_metadata_config(
            &metadata.config,
            &VectorIndexConfig::from_v2_definition(definition, handle.physical_name()),
        )?;
        index
            .stage_delete_with_build_session(write, entity_id.get(), build_session)
            .await?;
    }
    let (Some((resolution, handle)), Some(document), Some(layer)) = (next, next_document, layer)
    else {
        return Ok(());
    };
    let index = VectorIndex::<D>::from_generation(handle)
        .with_simhasher_registry(simhasher_registry)
        .with_batch_reads(batch_reads);
    // A newly allocated namespace was proven absent before planning. Only a
    // build creates an existing namespace, its unpartitioned one, with its
    // first scanned entity: an Active generation's metadata was written before
    // activation, and a tenant mapping commits with its namespace's metadata.
    let create = resolution.mapping_is_new
        || match (
            index.get_metadata(write).await?,
            target.owner,
            target.layout,
        ) {
            (Some(metadata), ..) => {
                validate_metadata_config(
                    &metadata.config,
                    &VectorIndexConfig::from_v2_definition(definition, handle.physical_name()),
                )?;
                false
            }
            (None, VectorPlanOwner::Build { .. }, VectorPhysicalLayout::Unpartitioned { .. }) => {
                true
            }
            (None, ..) => {
                return Err(corruption("vector physical namespace has no metadata"));
            }
        };
    if create {
        index
            .stage_create(
                write,
                VectorIndexConfig::from_v2_definition(definition, handle.physical_name()),
            )
            .await?;
    }
    match target.owner {
        VectorPlanOwner::Build { operation, record } => {
            let proof = ValidatedVectorBuildGenerationHandle::try_from_building::<D>(
                target.scope,
                record,
                operation.operation_id(),
                resolution.physical_index_id,
            )
            .map_err(|error| corruption(error.to_string()))?
            .fresh_insert_proof();
            index
                .stage_known_fresh_at_layer_with_session(
                    write,
                    entity_id.get(),
                    document.vector(),
                    layer,
                    proof,
                    build_session,
                )
                .await
        }
        VectorPlanOwner::Publication { .. } => {
            index
                .stage_upsert_at_layer_with_session(
                    write,
                    entity_id.get(),
                    document.vector(),
                    layer,
                    build_session,
                )
                .await
        }
    }
}

async fn resolve_build_physical(
    transaction: &DbTransaction,
    target: &VectorPlanTarget<'_>,
    partition: &TextPartition,
    create_missing: bool,
) -> Result<BuildPhysicalResolution> {
    match (target.layout, partition) {
        (
            VectorPhysicalLayout::Unpartitioned { physical_index_id },
            TextPartition::Unpartitioned,
        ) => Ok(BuildPhysicalResolution {
            physical_index_id,
            mapping_is_new: false,
        }),
        (VectorPhysicalLayout::Partitioned, TextPartition::TenantValue(_)) => {
            let tenant = VectorTenantPartition::try_from_partition(partition.clone())
                .map_err(|error| corruption(error.to_string()))?;
            if let Some(physical_index_id) =
                crate::index_lifecycle::repository::load_vector_partition_mapping(
                    transaction,
                    target.scope,
                    target.index_id,
                    target.generation,
                    target.layout,
                    &tenant,
                )
                .await?
            {
                return Ok(BuildPhysicalResolution {
                    physical_index_id,
                    mapping_is_new: false,
                });
            }
            if !create_missing {
                return Err(corruption(
                    "builder-applied vector partition has no physical mapping",
                ));
            }
            Ok(BuildPhysicalResolution {
                physical_index_id: crate::index_lifecycle::repository::peek_vector_physical_id(
                    transaction,
                )
                .await?,
                mapping_is_new: true,
            })
        }
        (VectorPhysicalLayout::Unpartitioned { .. }, TextPartition::TenantValue(_))
        | (VectorPhysicalLayout::Partitioned, TextPartition::Unpartitioned) => Err(corruption(
            "vector document partition disagrees with physical layout",
        )),
    }
}

async fn resolve_existing_build_physical(
    transaction: &DbTransaction,
    target: &VectorPlanTarget<'_>,
    partition: &TextPartition,
) -> Result<Option<VectorPhysicalIndexId>> {
    match (target.layout, partition) {
        (
            VectorPhysicalLayout::Unpartitioned { physical_index_id },
            TextPartition::Unpartitioned,
        ) => Ok(Some(physical_index_id)),
        (VectorPhysicalLayout::Partitioned, TextPartition::TenantValue(_)) => {
            let tenant = VectorTenantPartition::try_from_partition(partition.clone())
                .map_err(|error| corruption(error.to_string()))?;
            crate::index_lifecycle::repository::load_vector_partition_mapping(
                transaction,
                target.scope,
                target.index_id,
                target.generation,
                target.layout,
                &tenant,
            )
            .await
        }
        (VectorPhysicalLayout::Unpartitioned { .. }, TextPartition::TenantValue(_))
        | (VectorPhysicalLayout::Partitioned, TextPartition::Unpartitioned) => Err(corruption(
            "vector removal partition disagrees with physical layout",
        )),
    }
}

/// Proves the namespace `transaction` newly allocates has no metadata row
/// under any name in the `planning` snapshot.
///
/// `transaction` read the physical-ID watermark before `planning` opened, so
/// another index in the scope may have allocated the same ID and committed its
/// namespace in between. That commit postdates the transaction's snapshot and
/// the transaction read the watermark it advanced, so the transaction cannot
/// commit: the conflict is returned now, before planning reads the foreign
/// namespace, and either owner retries. A row the transaction sees too means
/// the watermark trails an existing namespace, which fails closed.
async fn require_unallocated_namespace(
    planning: &DbTransaction,
    transaction: &DbTransaction,
    handle: &ValidatedVectorGenerationHandle,
) -> Result<()> {
    let key = DataKey::Data {
        scope: handle.scope(),
        kind: DataKeyKind::Vector(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
            handle.physical_index_id(),
        ))),
    }
    .to_bytes();
    if planning.get(&key).await?.is_none() {
        return Ok(());
    }
    if transaction.get(&key).await?.is_some() {
        return Err(corruption(
            "vector physical-ID watermark trails an existing namespace",
        ));
    }
    Err(HelixDbError::TransactionConflict(format!(
        "vector physical index {} was allocated by a concurrent commit",
        handle.physical_index_id()
    )))
}

#[derive(Debug, Clone, Copy)]
struct BuildPhysicalResolution {
    physical_index_id: VectorPhysicalIndexId,
    mapping_is_new: bool,
}

struct IndexStateVectorPhysical {
    layout: VectorPhysicalLayout,
}

impl IndexStateVectorPhysical {
    fn from_record(record: &IndexRecordV2) -> Result<Self> {
        let Some(PhysicalGeneration::Vector { layout, .. }) = record.state().physical() else {
            return Err(corruption(
                "vector operation record has another physical family",
            ));
        };
        Ok(Self { layout: *layout })
    }
}

/// Selects the HNSW layer of one entity in one partition of a generation.
///
/// The layer depends only on the logical index, generation, entity, and
/// partition, so a build and queue publication place an entity alike and a
/// replanned entity keeps its layer.
fn deterministic_layer(
    index_id: IndexId,
    generation: IndexGenerationId,
    definition: &ValidatedVectorIndexDefinition,
    entity_id: IndexEntityId,
    document: &VectorIndexedDocument,
) -> u16 {
    let mut digest = Sha256::new();
    digest.update(index_id.get().to_be_bytes());
    digest.update(generation.get().to_be_bytes());
    digest.update(entity_id.get().to_be_bytes());
    digest.update(document.partition().canonical_bytes());
    let bytes: [u8; 32] = digest.finalize().into();
    let seed = u64::from_be_bytes(
        bytes[..core::mem::size_of::<u64>()]
            .try_into()
            .expect("SHA-256 prefix is eight bytes"),
    );
    let mut rng = StdRng::seed_from_u64(seed);
    vector::select_layer(definition.ml(), &mut rng)
}

#[allow(
    clippy::too_many_arguments,
    reason = "descriptor validation cross-checks the independent database, owner, record, policy, and cache identities"
)]
async fn validate_descriptor<D: Distance>(
    db: &Db,
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    record: &IndexRecordV2,
    definition: &ValidatedVectorIndexDefinition,
    progress: &PrefixScanProgress,
    limits: SearchIndexBatchLimits,
    simhasher_registry: Arc<vector::SimHasherRegistry>,
) -> Result<IndexOperationStepResult> {
    if has_pre_queue_deltas(transaction, scope, operation).await? {
        return Ok(IndexOperationStepResult::Blocked(
            IndexOperationBlocker::InvariantViolation,
        ));
    }
    let cursor_kind = progress
        .cursor
        .as_ref()
        .map(|cursor| IndexKey::parse_from_slice(scope, cursor.as_bytes()))
        .transpose()?
        .and_then(|key| match key {
            IndexKey::Data { kind: key, .. } => Some(key.record_kind()),
            IndexKey::Global { .. } => None,
        });
    if !matches!(cursor_kind, Some(RecordKind::VectorPartitionMapping)) {
        let prefix = generation_prefix(
            scope,
            RecordKind::AppliedState,
            operation.index_id(),
            operation.generation(),
        );
        let start = cursor_suffix(&prefix, progress.cursor.as_ref())?
            .map_or(Bound::Unbounded, Bound::Excluded);
        let mut rows = transaction
            .scan_prefix(&prefix, (start, Bound::<Bytes>::Unbounded))
            .await?;
        let mut accounting = VectorBatchAccounting::new(progress.counters, limits);
        let mut cursor = progress.cursor.clone();
        let mut exhausted = true;
        while accounting.can_read_another() {
            let Some(row) = rows.next().await? else {
                break;
            };
            let input_bytes = row.key.len().saturating_add(row.value.len()) as u64;
            let (entity, applied) = decode_applied(scope, &row.key, &row.value)?;
            let AppliedFamilyState::Vector(Some(partition)) = applied.state else {
                return Err(corruption(
                    "vector validation found non-vector or empty applied state",
                ));
            };
            if applied.index_id != operation.index_id()
                || applied.generation != operation.generation()
                || entity.kind != definition.element_kind()
            {
                return Err(corruption("vector applied-state ownership mismatch"));
            }
            validate_partition_metadata::<D>(
                transaction,
                scope,
                operation,
                record,
                definition,
                &partition,
                Arc::clone(&simhasher_registry),
            )
            .await?;
            let output_bytes = row.key.len() as u64;
            if !accounting.can_admit_input(input_bytes)
                || !accounting.can_admit_output(VectorWriteMeasurement::zero(), 1, output_bytes)
            {
                if accounting.is_empty() {
                    return Ok(IndexOperationStepResult::Blocked(
                        IndexOperationBlocker::OversizedEntity {
                            entity_kind: entity.kind,
                            entity_id: entity.id,
                            observed: input_bytes.max(output_bytes),
                            limit: limits
                                .max_input_bytes()
                                .get()
                                .min(limits.max_output_bytes().get()),
                        },
                    ));
                }
                exhausted = false;
                break;
            }
            transaction.delete(&row.key)?;
            accounting.admit(
                input_bytes,
                VectorWriteMeasurement::zero(),
                0,
                1,
                output_bytes,
            )?;
            cursor = Some(IndexCursor::try_new(row.key).map_err(operation_error)?);
        }
        if !accounting.can_read_another() {
            exhausted = false;
        }
        let counters = accounting.finish()?;
        if !exhausted {
            return Ok(progressed_build(VectorBuildStage::ValidateDescriptor(
                PrefixScanProgress { cursor, counters },
            )));
        }
        return validate_mappings_or_finish::<D>(
            db,
            transaction,
            scope,
            operation,
            record,
            definition,
            None,
            counters,
            limits,
            simhasher_registry,
        )
        .await;
    }
    validate_mappings_or_finish::<D>(
        db,
        transaction,
        scope,
        operation,
        record,
        definition,
        progress.cursor.as_ref(),
        progress.counters,
        limits,
        simhasher_registry,
    )
    .await
}

#[allow(
    clippy::too_many_arguments,
    reason = "descriptor validation binds exact canonical and physical identities"
)]
async fn validate_mappings_or_finish<D: Distance>(
    db: &Db,
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    record: &IndexRecordV2,
    definition: &ValidatedVectorIndexDefinition,
    cursor: Option<&IndexCursor>,
    counters: OperationCounters,
    limits: SearchIndexBatchLimits,
    simhasher_registry: Arc<vector::SimHasherRegistry>,
) -> Result<IndexOperationStepResult> {
    let IndexStateVectorPhysical { layout } = IndexStateVectorPhysical::from_record(record)?;
    if let VectorPhysicalLayout::Unpartitioned { physical_index_id } = layout {
        let handle = ValidatedVectorBuildGenerationHandle::try_from_building::<D>(
            scope,
            record,
            operation.operation_id(),
            physical_index_id,
        )
        .map_err(|error| corruption(error.to_string()))?;
        let index = VectorIndex::<D>::from_generation(handle.generation())
            .with_simhasher_registry(simhasher_registry);
        let expected =
            VectorIndexConfig::from_v2_definition(definition, handle.generation().physical_name());
        match index.get_metadata(transaction).await? {
            Some(metadata) => validate_metadata_config(&metadata.config, &expected)?,
            None => {
                let planning = db.begin(IsolationLevel::Snapshot).await?;
                let planning_write = MeasuredVectorTransaction::new(&planning);
                let checkpoint = planning_write.checkpoint();
                index.stage_create(&planning_write, expected).await?;
                let plan: PlannedVectorMutation = planning_write
                    .plan_since(checkpoint)
                    .map_err(measurement_error)?;
                let writes = plan.measurement();
                if writes.operations() > limits.max_output_operations().get()
                    || writes.encoded_bytes() > limits.max_output_bytes().get()
                {
                    return Ok(IndexOperationStepResult::Blocked(
                        IndexOperationBlocker::OversizedEntity {
                            entity_kind: definition.element_kind(),
                            entity_id: IndexEntityId::initial(),
                            observed: writes.encoded_bytes(),
                            limit: limits.max_output_bytes().get(),
                        },
                    ));
                }
                plan.apply_to(transaction)?;
                let counters = OperationCounters {
                    entities: counters.entities,
                    input_bytes: counters.input_bytes,
                    output_operations: checked_add(
                        counters.output_operations,
                        writes.operations(),
                        "cumulative output operations",
                    )?,
                    output_bytes: checked_add(
                        counters.output_bytes,
                        writes.encoded_bytes(),
                        "cumulative output bytes",
                    )?,
                };
                return Ok(progressed_build(VectorBuildStage::Activate(
                    NoCursorProgress { counters },
                )));
            }
        }
        return Ok(progressed_build(VectorBuildStage::Activate(
            NoCursorProgress { counters },
        )));
    }
    let prefix = generation_prefix(
        scope,
        RecordKind::VectorPartitionMapping,
        operation.index_id(),
        operation.generation(),
    );
    let start = cursor_suffix(&prefix, cursor)?.map_or(Bound::Unbounded, Bound::Excluded);
    let mut rows = transaction
        .scan_prefix(&prefix, (start, Bound::<Bytes>::Unbounded))
        .await?;
    let mut accounting = VectorBatchAccounting::new(counters, limits);
    let mut next_cursor = cursor.cloned();
    let mut exhausted = true;
    while accounting.can_read_another() {
        let Some(row) = rows.next().await? else {
            break;
        };
        let input_bytes = row.key.len().saturating_add(row.value.len()) as u64;
        if !accounting.can_admit_input(input_bytes) {
            if accounting.is_empty() {
                return Ok(IndexOperationStepResult::Blocked(
                    IndexOperationBlocker::OversizedEntity {
                        entity_kind: definition.element_kind(),
                        entity_id: IndexEntityId::initial(),
                        observed: input_bytes,
                        limit: limits.max_input_bytes().get(),
                    },
                ));
            }
            exhausted = false;
            break;
        }
        let mapping = decode_mapping(scope, &row.key, &row.value, operation)?;
        validate_partition_metadata::<D>(
            transaction,
            scope,
            operation,
            record,
            definition,
            mapping.partition.as_partition(),
            Arc::clone(&simhasher_registry),
        )
        .await?;
        accounting.admit(input_bytes, VectorWriteMeasurement::zero(), 0, 0, 0)?;
        next_cursor = Some(IndexCursor::try_new(row.key).map_err(operation_error)?);
    }
    if !accounting.can_read_another() {
        exhausted = false;
    }
    let counters = accounting.finish()?;
    Ok(progressed_build(if exhausted {
        VectorBuildStage::Activate(NoCursorProgress { counters })
    } else {
        VectorBuildStage::ValidateDescriptor(PrefixScanProgress {
            cursor: next_cursor,
            counters,
        })
    }))
}

#[allow(
    clippy::too_many_arguments,
    reason = "metadata validation binds every canonical ownership component"
)]
async fn validate_partition_metadata<D: Distance>(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    record: &IndexRecordV2,
    definition: &ValidatedVectorIndexDefinition,
    partition: &TextPartition,
    simhasher_registry: Arc<vector::SimHasherRegistry>,
) -> Result<()> {
    let resolution = resolve_build_physical(
        transaction,
        &VectorPlanTarget::build(scope, operation, record)?,
        partition,
        false,
    )
    .await?;
    let handle = ValidatedVectorBuildGenerationHandle::try_from_building::<D>(
        scope,
        record,
        operation.operation_id(),
        resolution.physical_index_id,
    )
    .map_err(|error| corruption(error.to_string()))?;
    let index = VectorIndex::<D>::from_generation(handle.generation())
        .with_simhasher_registry(simhasher_registry);
    let Some(metadata) = index.get_metadata(transaction).await? else {
        return Err(corruption("vector partition has no physical metadata"));
    };
    let expected =
        VectorIndexConfig::from_v2_definition(definition, handle.generation().physical_name());
    validate_metadata_config(&metadata.config, &expected)
}

fn validate_metadata_config(
    actual: &VectorIndexConfig,
    expected: &VectorIndexConfig,
) -> Result<()> {
    if !actual.has_same_physical_contract(expected) {
        return Err(corruption(
            "physical vector metadata disagrees with canonical descriptor",
        ));
    }
    Ok(())
}

/// Closed applied-state write selected by one authoritative entity transition.
///
/// Only builds keep applied state; publication always measures `Absent`.
#[derive(Debug, Clone, Copy)]
enum AppliedStateTransition<'a> {
    Absent,
    Delete,
    Put(&'a TextPartition),
}

/// Measures the lifecycle rows one admitted entity stages beside its vector
/// writes: its applied state, a new tenant mapping with the physical-ID
/// watermark, and the mapping deletion of every partition it reclaims.
fn lifecycle_write_measurement(
    target: &VectorPlanTarget<'_>,
    entity_kind: IndexElementKind,
    entity_id: IndexEntityId,
    applied_transition: AppliedStateTransition<'_>,
    new_mapping: Option<(&TextPartition, VectorPhysicalIndexId)>,
    reclaimed: &[(VectorTenantPartition, ValidatedVectorGenerationHandle)],
) -> Result<(u64, u64)> {
    let applied_key = applied_key(
        target.scope,
        target.index_id,
        target.generation,
        entity_kind,
        entity_id,
    );
    let (mut operations, mut bytes) = match applied_transition {
        AppliedStateTransition::Put(partition) => {
            let value = encode_applied_state(&AppliedEntityStateValue {
                index_id: target.index_id,
                generation: target.generation,
                entity_kind,
                entity_id,
                state: AppliedFamilyState::Vector(Some(partition.clone())),
            });
            (1_u64, applied_key.len().saturating_add(value.len()) as u64)
        }
        AppliedStateTransition::Delete => (1, applied_key.len() as u64),
        AppliedStateTransition::Absent => (0, 0),
    };
    let mapping_key = |tenant: &VectorTenantPartition| {
        scoped_index_key(
            target.scope,
            ScopedKey::VectorPartitionMapping(
                crate::encoding::v2::keys::VectorPartitionMappingKey {
                    index_id: target.index_id,
                    generation: target.generation,
                    partition: tenant.fingerprint(),
                },
            ),
        )
    };
    if let Some((partition, physical_index_id)) = new_mapping {
        let tenant = VectorTenantPartition::try_from_partition(partition.clone())
            .map_err(|error| corruption(error.to_string()))?;
        let mapping_key = mapping_key(&tenant);
        let mapping_value =
            encode_partition_mapping(&crate::index_lifecycle::work::VectorPartitionMappingValue {
                index_id: target.index_id,
                generation: target.generation,
                partition: tenant,
                physical_index_id,
            });
        let watermark_key = IndexKey::Global {
            kind: GlobalKey::VectorPhysicalIdWatermark,
        }
        .to_bytes();
        let watermark_value = encode_metadata_value(
            &IndexV2MetadataValue::VectorPhysicalIdWatermark(VectorPhysicalIdWatermark {
                next_id: physical_index_id.checked_next()?,
            }),
        );
        operations = operations.saturating_add(2);
        bytes = bytes
            .saturating_add(mapping_key.len().saturating_add(mapping_value.len()) as u64)
            .saturating_add(watermark_key.len().saturating_add(watermark_value.len()) as u64);
    }
    for (tenant, _) in reclaimed {
        operations = operations.saturating_add(1);
        bytes = bytes.saturating_add(mapping_key(tenant).len() as u64);
    }
    Ok((operations, bytes))
}

fn stage_applied(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    entity_kind: IndexElementKind,
    entity_id: IndexEntityId,
    next_partition: Option<TextPartition>,
) -> Result<()> {
    let key = applied_key(
        scope,
        operation.index_id(),
        operation.generation(),
        entity_kind,
        entity_id,
    );
    match next_partition {
        Some(partition) => transaction.put(
            key,
            encode_applied_state(&AppliedEntityStateValue {
                index_id: operation.index_id(),
                generation: operation.generation(),
                entity_kind,
                entity_id,
                state: AppliedFamilyState::Vector(Some(partition)),
            }),
        )?,
        None => transaction.delete(key)?,
    }
    Ok(())
}

async fn load_applied(
    transaction: &DbTransaction,
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
    entity_kind: IndexElementKind,
    entity_id: IndexEntityId,
) -> Result<Option<TextPartition>> {
    let key = applied_key(scope, index_id, generation, entity_kind, entity_id);
    let Some(value) = transaction.get(&key).await? else {
        return Ok(None);
    };
    let (_, applied) = decode_applied(scope, &key, &value)?;
    if applied.index_id != index_id
        || applied.generation != generation
        || applied.entity_kind != entity_kind
        || applied.entity_id != entity_id
    {
        return Err(corruption("vector applied-state key/value mismatch"));
    }
    let AppliedFamilyState::Vector(partition) = applied.state else {
        return Err(corruption(
            "vector generation contains another applied family",
        ));
    };
    Ok(partition)
}

fn applied_key(
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
    entity_kind: IndexElementKind,
    entity_id: IndexEntityId,
) -> Bytes {
    scoped_index_key(
        scope,
        ScopedKey::AppliedState(IndexEntityStateKey {
            index_id,
            generation,
            entity: IndexEntity {
                kind: entity_kind,
                id: entity_id,
            },
        }),
    )
}

fn decode_delta(
    scope: DataScope,
    key: &[u8],
    value: &[u8],
) -> Result<(IndexEntity, CoalescedBuildDeltaValue)> {
    let IndexKey::Data {
        kind: ScopedKey::BuildDelta(key),
        ..
    } = IndexKey::parse_from_slice(scope, key)?
    else {
        return Err(corruption("build-delta prefix yielded another key kind"));
    };
    let value = crate::index_lifecycle::expect_typed_value(
        decode_build_delta(value),
        "build-delta key contains another value kind",
    )?;
    if key.index_id != value.index_id
        || key.generation != value.generation
        || key.entity.kind != value.entity_kind
        || key.entity.id != value.entity_id
    {
        return Err(corruption("build-delta key/value mismatch"));
    }
    Ok((key.entity, value))
}

fn decode_applied(
    scope: DataScope,
    key: &[u8],
    value: &[u8],
) -> Result<(IndexEntity, AppliedEntityStateValue)> {
    let IndexKey::Data {
        kind: ScopedKey::AppliedState(key),
        ..
    } = IndexKey::parse_from_slice(scope, key)?
    else {
        return Err(corruption("applied-state prefix yielded another key kind"));
    };
    let value = crate::index_lifecycle::expect_typed_value(
        decode_applied_state(value),
        "applied-state key contains another value kind",
    )?;
    if key.index_id != value.index_id
        || key.generation != value.generation
        || key.entity.kind != value.entity_kind
        || key.entity.id != value.entity_id
    {
        return Err(corruption("applied-state key/value mismatch"));
    }
    Ok((key.entity, value))
}

fn decode_mapping(
    scope: DataScope,
    key: &[u8],
    value: &[u8],
    operation: &IndexOperationRecord,
) -> Result<crate::index_lifecycle::work::VectorPartitionMappingValue> {
    let IndexKey::Data {
        kind: ScopedKey::VectorPartitionMapping(key),
        ..
    } = IndexKey::parse_from_slice(scope, key)?
    else {
        return Err(corruption("vector mapping prefix yielded another key kind"));
    };
    let value = crate::index_lifecycle::expect_typed_value(
        decode_partition_mapping(value),
        "vector partition mapping key contains another value kind",
    )?;
    if key.index_id != operation.index_id()
        || key.generation != operation.generation()
        || value.index_id != operation.index_id()
        || value.generation != operation.generation()
        || key.partition != value.partition.fingerprint()
    {
        return Err(corruption("vector mapping key/value ownership mismatch"));
    }
    Ok(value)
}

async fn load_operation_index(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
) -> Result<IndexRecordV2> {
    let key = scoped_index_key(scope, ScopedKey::index_record(operation.identity().clone()));
    let Some(value) = transaction.get(key).await? else {
        return Err(corruption("vector operation has no canonical index"));
    };
    let record = decode_index_record(&value)?;
    if record.index_id() != operation.index_id()
        || record.identity() != operation.identity()
        || record.revision() != operation.index_record_revision()
        || record.state().generation() != operation.generation()
    {
        return Err(corruption("vector operation/canonical record mismatch"));
    }
    Ok(record)
}

async fn generation_has_rows(
    transaction: &DbTransaction,
    scope: DataScope,
    kind: RecordKind,
    index_id: IndexId,
    generation: IndexGenerationId,
) -> Result<bool> {
    let prefix = generation_prefix(scope, kind, index_id, generation);
    let mut rows = transaction.scan_prefix(prefix, ..).await?;
    Ok(rows.next().await?.is_some())
}

fn source_prefix(scope: DataScope, kind: IndexElementKind) -> Bytes {
    let prefix = match kind {
        IndexElementKind::Node => KeyPrefix::NodeProperty,
        IndexElementKind::Edge => KeyPrefix::EdgePropertyById,
    };
    DataKey::data_prefix(scope, Bytes::copy_from_slice(prefix.as_slice()))
}

fn source_entity(
    scope: DataScope,
    expected: IndexElementKind,
    key: &[u8],
) -> Result<Option<IndexEntityId>> {
    let parsed = DataKey::parse_from_slice(scope, key)?;
    Ok(match (expected, parsed) {
        (
            IndexElementKind::Node,
            DataKey::Data {
                kind: DataKeyKind::NodeProperty(key),
                ..
            },
        ) => Some(IndexEntityId::new(key.node_id())),
        (
            IndexElementKind::Edge,
            DataKey::Data {
                kind: DataKeyKind::EdgePropertyById(key),
                ..
            },
        ) => Some(IndexEntityId::new(key.edge_id())),
        (IndexElementKind::Edge, DataKey::Data { .. }) => None,
        (IndexElementKind::Node, DataKey::Data { .. }) | (_, DataKey::Global { .. }) => {
            return Err(corruption("vector source prefix yielded another key kind"));
        }
    })
}

fn generation_prefix(
    scope: DataScope,
    kind: RecordKind,
    index_id: IndexId,
    generation: IndexGenerationId,
) -> Bytes {
    IndexKey::data_prefix(
        scope,
        ScopedKey::generation_prefix(kind, index_id, generation),
    )
}

fn cursor_suffix(prefix: &Bytes, cursor: Option<&IndexCursor>) -> Result<Option<Bytes>> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let Some(suffix) = cursor.as_bytes().strip_prefix(prefix.as_ref()) else {
        return Err(corruption("vector cursor is outside its exact scan prefix"));
    };
    Ok(Some(Bytes::copy_from_slice(suffix)))
}

fn scoped_index_key(scope: DataScope, key: ScopedKey) -> Bytes {
    IndexKey::Data { scope, kind: key }.to_bytes()
}

fn progressed_build(stage: VectorBuildStage) -> IndexOperationStepResult {
    IndexOperationStepResult::Progressed(IndexOperationProgress::VectorBuild(
        VectorBuildProgress::Constructing(stage),
    ))
}

fn invalid_source(
    entity_kind: IndexElementKind,
    entity_id: IndexEntityId,
) -> IndexOperationStepResult {
    IndexOperationStepResult::Blocked(IndexOperationBlocker::InvalidSourceData {
        entity_kind,
        entity_id,
    })
}

fn finish_or_block_scan(
    outcome: EntityPlanOutcome,
    accounting: VectorBatchAccounting,
    entity_kind: IndexElementKind,
    entity_id: IndexEntityId,
    progress: &SourceScanProgress,
    cursor: Option<IndexCursor>,
    session_stats: VectorBuildSessionStats,
) -> Result<VectorStepResult> {
    let vector_planning = accounting.planning_usage(session_stats);
    match outcome {
        EntityPlanOutcome::Blocked(blocker) => Ok(VectorStepResult::ordinary(
            IndexOperationStepResult::Blocked(blocker),
        )
        .with_vector_planning(vector_planning)),
        EntityPlanOutcome::BatchFull => {
            let (counters, single_vector_output_bytes) = accounting.finish_with_max()?;
            Ok(VectorStepResult {
                result: progressed_build(VectorBuildStage::Scan(SourceScanProgress {
                    inclusive_upper_bound: progress.inclusive_upper_bound.clone(),
                    cursor,
                    counters,
                })),
                single_vector_output_bytes,
                physical_operations: 0,
                output_bytes: 0,
                vector_planning,
                retained: None,
            })
        }
        EntityPlanOutcome::Admitted { .. } => Err(corruption(format!(
            "admitted vector entity {entity_kind:?}/{} escaped application",
            entity_id.get()
        ))),
    }
}

/// Admission of one planned entity.
pub(super) enum EntityPlanOutcome {
    /// The plan was applied to the target transaction.
    Admitted {
        vector_writes: VectorWriteMeasurement,
        single_vector_output_bytes: u64,
        lifecycle_operations: u64,
        lifecycle_bytes: u64,
        next_partition: Option<TextPartition>,
    },
    /// The entity does not fit beside the admitted ones; nothing was applied.
    BatchFull,
    /// The entity cannot fit any batch; nothing was applied.
    Blocked(IndexOperationBlocker),
}

/// Input and output one batch has admitted against its limits.
pub(super) struct VectorBatchAccounting {
    counters: OperationCounters,
    limits: SearchIndexBatchLimits,
    entities: usize,
    input_bytes: u64,
    vector_writes: VectorWriteMeasurement,
    max_single_vector_output_bytes: u64,
    lifecycle_operations: u64,
    lifecycle_bytes: u64,
    planning_executions: u64,
}

impl VectorBatchAccounting {
    fn new(counters: OperationCounters, limits: SearchIndexBatchLimits) -> Self {
        Self {
            counters,
            limits,
            entities: 0,
            input_bytes: 0,
            vector_writes: VectorWriteMeasurement::zero(),
            max_single_vector_output_bytes: 0,
            lifecycle_operations: 0,
            lifecycle_bytes: 0,
            planning_executions: 0,
        }
    }

    /// Starts a batch whose output budget already carries writes staged beside
    /// its entities, such as a queue acknowledgement.
    pub(super) fn reserving(
        limits: SearchIndexBatchLimits,
        reserved_operations: u64,
        reserved_bytes: u64,
    ) -> Self {
        Self {
            lifecycle_operations: reserved_operations,
            lifecycle_bytes: reserved_bytes,
            ..Self::new(OperationCounters::default(), limits)
        }
    }

    fn is_empty(&self) -> bool {
        self.entities == 0
    }

    fn can_read_another(&self) -> bool {
        self.entities < self.limits.max_entities().get()
    }

    fn can_admit_input(&self, bytes: u64) -> bool {
        self.input_bytes.saturating_add(bytes) <= self.limits.max_input_bytes().get()
    }

    fn can_admit_output(
        &self,
        cumulative_vector: VectorWriteMeasurement,
        lifecycle_operations: u64,
        lifecycle_bytes: u64,
    ) -> bool {
        cumulative_vector
            .operations()
            .saturating_add(self.lifecycle_operations)
            .saturating_add(lifecycle_operations)
            <= self.limits.max_output_operations().get()
            && cumulative_vector
                .encoded_bytes()
                .saturating_add(self.lifecycle_bytes)
                .saturating_add(lifecycle_bytes)
                <= self.limits.max_output_bytes().get()
    }

    fn record_planning(&mut self) {
        self.planning_executions = self.planning_executions.saturating_add(1);
    }

    fn planning_usage(&self, stats: VectorBuildSessionStats) -> VectorPlanningUsage {
        VectorPlanningUsage {
            planning_executions: self.planning_executions,
            planned_writes: self.vector_writes.operations(),
            replay_executions: 0,
            item_hits: stats.item_hits(),
            item_misses: stats.item_misses(),
            neighbor_hits: stats.neighbor_hits(),
            neighbor_misses: stats.neighbor_misses(),
            simhash_hits: stats.simhash_hits(),
            simhash_misses: stats.simhash_misses(),
            item_evictions: stats.item_evictions(),
            neighbor_evictions: stats.neighbor_evictions(),
            simhash_evictions: stats.simhash_evictions(),
            dirty_neighbor_flushes: stats.dirty_neighbor_flushes(),
            retained_payload_bytes: stats.max_retained_payload_bytes(),
        }
    }

    pub(super) fn admit(
        &mut self,
        input_bytes: u64,
        cumulative_vector: VectorWriteMeasurement,
        single_vector_output_bytes: u64,
        lifecycle_operations: u64,
        lifecycle_bytes: u64,
    ) -> Result<()> {
        self.entities += 1;
        self.input_bytes = checked_add(self.input_bytes, input_bytes, "batch input bytes")?;
        self.vector_writes = cumulative_vector;
        self.max_single_vector_output_bytes = self
            .max_single_vector_output_bytes
            .max(single_vector_output_bytes);
        self.lifecycle_operations = checked_add(
            self.lifecycle_operations,
            lifecycle_operations,
            "batch lifecycle operations",
        )?;
        self.lifecycle_bytes = checked_add(
            self.lifecycle_bytes,
            lifecycle_bytes,
            "batch lifecycle bytes",
        )?;
        Ok(())
    }

    fn finish(self) -> Result<OperationCounters> {
        Ok(OperationCounters {
            entities: checked_add(
                self.counters.entities,
                self.entities as u64,
                "cumulative entities",
            )?,
            input_bytes: checked_add(
                self.counters.input_bytes,
                self.input_bytes,
                "cumulative input bytes",
            )?,
            output_operations: checked_add(
                self.counters.output_operations,
                self.vector_writes
                    .operations()
                    .saturating_add(self.lifecycle_operations),
                "cumulative output operations",
            )?,
            output_bytes: checked_add(
                self.counters.output_bytes,
                self.vector_writes
                    .encoded_bytes()
                    .saturating_add(self.lifecycle_bytes),
                "cumulative output bytes",
            )?,
        })
    }

    fn finish_with_max(self) -> Result<(OperationCounters, u64)> {
        let max_single_vector_output_bytes = self.max_single_vector_output_bytes;
        Ok((self.finish()?, max_single_vector_output_bytes))
    }
}

fn checked_add(left: u64, right: u64, name: &'static str) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| corruption(format!("vector {name} overflowed")))
}

fn measurement_error(error: impl std::fmt::Display) -> HelixDbError {
    corruption(format!("vector write measurement failed: {error}"))
}

fn corruption(message: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(message.into())
}

fn operation_error(error: crate::index_lifecycle::IndexOperationModelError) -> HelixDbError {
    HelixDbError::InvariantViolation(error.to_string())
}

#[cfg(all(feature = "production-coverage", not(test)))]
#[path = "../../../tests/production_support/vector_build_cache.rs"]
pub(super) mod build_cache_production_contracts;

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};

    use slatedb::object_store::memory::InMemory;

    use super::*;
    use crate::config::{SearchIndexBackfillLimits, VectorIndexDefinition};
    use crate::encoding::property::property_value::PropertyValue;
    use crate::encoding::property::Property;
    use crate::encoding::v2::keys::NodePropertyKey;
    use crate::encoding::v2::values::property::encode_properties;
    use crate::index_lifecycle::lifecycle::{
        create_index_operation, create_legacy_vector_adoption_operation, drop_index_operation,
        InitialBuildProgress,
    };
    use crate::index_lifecycle::outbox::{
        claim_operation, execute_claimed_step, observe_operation_pointer, ClaimPermission,
        CommittedOperationStep, OperationPointerObservation,
    };
    use crate::index_lifecycle::repository::peek_vector_physical_id;
    use crate::index_lifecycle::{
        ActiveIndexHandle, ClaimSequence, IndexDdlReceipt, IndexOperationId, IndexScopeGates,
        IndexStateV2, WriterEpoch,
    };
    use crate::migrations::startup::bootstrap_writer;
    use crate::search::vector::{
        DistanceScore, SearchParams, SimHashMode, SimHasherRegistry,
        ValidatedVectorGenerationHandle, VectorCacheRegistry,
    };

    const NOW_MILLIS: u64 = 1;

    async fn test_db(name: &str) -> Db {
        let db = Db::builder(name, Arc::new(InMemory::new()))
            .build()
            .await
            .expect("vector driver test database opens");
        bootstrap_writer(&db)
            .await
            .expect("vector driver test database bootstraps V2 metadata");
        db
    }

    fn driver() -> VectorIndexDriver {
        VectorIndexDriver::new(
            Arc::new(IndexScopeGates::default()),
            Arc::new(VectorCacheRegistry::default()),
            Arc::new(SimHasherRegistry::default()),
        )
    }

    #[test]
    fn batch_admission_uses_cumulative_last_write_wins_vector_measurement() {
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut accounting = VectorBatchAccounting::new(OperationCounters::default(), limits);
        accounting
            .admit(10, VectorWriteMeasurement::for_test(2, 20), 20, 1, 5)
            .unwrap();
        accounting
            .admit(10, VectorWriteMeasurement::for_test(2, 14), 8, 1, 5)
            .unwrap();

        let counters = accounting.finish().unwrap();
        assert_eq!(counters.entities, 2);
        assert_eq!(counters.output_operations, 4);
        assert_eq!(counters.output_bytes, 24);
    }

    /// Exercises diagnostic and typed-error adapters that sit below the
    /// lifecycle state machine but still belong to the production surface.
    #[tokio::test]
    async fn diagnostic_and_error_adapters_preserve_their_error_categories() {
        assert!(format!("{:?}", driver()).contains("VectorIndexDriver"));
        assert!(matches!(
            invalid_source(IndexElementKind::Node, IndexEntityId::initial()),
            IndexOperationStepResult::Blocked(IndexOperationBlocker::InvalidSourceData {
                entity_kind: IndexElementKind::Node,
                entity_id,
            }) if entity_id == IndexEntityId::initial()
        ));
        assert!(matches!(
            checked_add(u64::MAX, 1, "fixture"),
            Err(HelixDbError::IndexCatalogCorruption(reason)) if reason.contains("fixture")
        ));
        let db = test_db("vector-driver-error-adapters").await;
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let first = MeasuredVectorTransaction::new(&transaction);
        let foreign_checkpoint = first.checkpoint();
        let second = MeasuredVectorTransaction::new(&transaction);
        let measurement_failure = second
            .plan_since(foreign_checkpoint)
            .expect_err("checkpoint belongs to another recorder");
        assert!(matches!(
            measurement_error(measurement_failure),
            HelixDbError::IndexCatalogCorruption(reason) if reason.contains("measurement")
        ));
        assert!(matches!(
            corruption("fixture corruption"),
            HelixDbError::IndexCatalogCorruption(reason) if reason == "fixture corruption"
        ));
        assert!(matches!(
            operation_error(crate::index_lifecycle::IndexOperationModelError::OversizedCursor {
                actual: 2,
                maximum: 1,
            }),
            HelixDbError::InvariantViolation(reason) if reason.contains("cursor")
        ));
        drop(transaction);
        db.close().await.expect("vector test database closes");
    }

    fn definition(tenant_property: Option<&str>) -> ValidatedDynamicIndexDefinition {
        let runtime = VectorIndexDefinition::new_node(
            "Document",
            "embedding",
            3,
            VectorDistanceMetric::Euclidean,
        )
        .expect("vector definition validates");
        let runtime = match tenant_property {
            Some(tenant_property) => runtime
                .with_tenant_property(tenant_property)
                .expect("tenant property validates"),
            None => runtime,
        };
        ValidatedDynamicIndexDefinition::Vector(
            ValidatedVectorIndexDefinition::try_from_runtime(&runtime)
                .expect("V2 vector definition validates"),
        )
    }

    fn properties(vector: [f32; 3], tenant: Option<i64>) -> Vec<Property> {
        let mut properties = vec![
            Property::new("$label", PropertyValue::String("Document".to_string())),
            Property::new("embedding", PropertyValue::F32Array(vector.to_vec())),
        ];
        if let Some(tenant) = tenant {
            properties.push(Property::new("account_id", PropertyValue::I64(tenant)));
        }
        properties
    }

    fn source_key(scope: DataScope, entity_id: u64) -> Bytes {
        DataKey::Data {
            scope,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity_id)),
        }
        .to_bytes()
    }

    fn source_cursor(scope: DataScope, entity_id: u64) -> IndexCursor {
        IndexCursor::try_new(source_key(scope, entity_id)).expect("source key is a valid cursor")
    }

    async fn put_source(db: &Db, scope: DataScope, entity_id: u64, properties: &[Property]) {
        db.put(source_key(scope, entity_id), encode_properties(properties))
            .await
            .expect("vector source is written");
    }

    async fn create_build(
        db: &Db,
        scope: DataScope,
        definition: &ValidatedDynamicIndexDefinition,
        upper_entity_id: u64,
    ) -> (IndexOperationId, IndexId, IndexGenerationId) {
        let receipt = create_index_operation(
            db,
            scope,
            definition.clone(),
            helix_planner::ir::IndexCreateMode::ErrorIfExists,
            InitialBuildProgress::vector(source_cursor(scope, upper_entity_id)),
        )
        .await
        .expect("vector build is enqueued");
        let IndexDdlReceipt::Accepted {
            operation_id,
            index_id,
            generation,
        } = receipt
        else {
            panic!("new vector definition must enqueue a build");
        };
        (operation_id, index_id, generation)
    }

    async fn drive_one(
        db: &Db,
        driver: &VectorIndexDriver,
        operation_id: IndexOperationId,
        claim_sequence: &mut u64,
        limits: SearchIndexBatchLimits,
    ) -> CommittedOperationStep {
        let writer_epoch = WriterEpoch::from_bytes([0x6B; 16]).expect("writer epoch is non-nil");
        let observation = observe_operation_pointer(db, operation_id, writer_epoch, NOW_MILLIS)
            .await
            .expect("vector operation pointer is readable");
        let OperationPointerObservation::Eligible(eligible) = observation else {
            panic!("queued vector operation must be eligible: {observation:?}");
        };
        let sequence = ClaimSequence::new(*claim_sequence).expect("claim sequence is non-zero");
        *claim_sequence = claim_sequence
            .checked_add(1)
            .expect("claim sequence remains bounded");
        let claimed = claim_operation(
            db,
            &eligible,
            writer_epoch,
            sequence,
            NOW_MILLIS,
            ClaimPermission::Normal,
        )
        .await
        .expect("vector claim succeeds")
        .expect("vector revision is claimable");
        execute_claimed_step(db, &claimed, driver, limits, NOW_MILLIS)
            .await
            .expect("vector step commits")
    }

    async fn drive_to_terminal(
        db: &Db,
        driver: &VectorIndexDriver,
        operation_id: IndexOperationId,
        claim_sequence: &mut u64,
    ) -> CommittedOperationStep {
        for _ in 0..64 {
            let step = drive_one(
                db,
                driver,
                operation_id,
                claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await;
            if step != CommittedOperationStep::Progressed {
                return step;
            }
        }
        panic!("vector operation exceeded bounded test checkpoints")
    }

    async fn read_index(
        db: &Db,
        scope: DataScope,
        definition: &ValidatedDynamicIndexDefinition,
    ) -> IndexRecordV2 {
        let key = scoped_index_key(scope, ScopedKey::index_record(definition.identity()));
        let value = db
            .get(key)
            .await
            .expect("canonical vector index is readable")
            .expect("canonical vector index exists");
        decode_index_record(&value).expect("canonical vector index decodes")
    }

    async fn read_operation(
        db: &Db,
        scope: DataScope,
        operation_id: IndexOperationId,
    ) -> IndexOperationRecord {
        let value = db
            .get(crate::index_lifecycle::outbox::scoped_operation_key(
                scope,
                operation_id,
            ))
            .await
            .expect("vector operation is readable")
            .expect("vector operation exists");
        crate::encoding::v2::values::decode_operation_record(&value)
            .expect("vector operation decodes")
    }

    async fn mapping_values(
        db: &Db,
        scope: DataScope,
        index_id: IndexId,
        generation: IndexGenerationId,
    ) -> Vec<crate::index_lifecycle::work::VectorPartitionMappingValue> {
        let prefix = generation_prefix(
            scope,
            RecordKind::VectorPartitionMapping,
            index_id,
            generation,
        );
        let mut rows = db
            .scan_prefix(prefix, ..)
            .await
            .expect("vector mappings are readable");
        let mut values = Vec::new();
        while let Some(row) = rows.next().await.expect("vector mapping row is readable") {
            let value = decode_partition_mapping(&row.value).expect("vector mapping value decodes");
            values.push(value);
        }
        values
    }

    #[tokio::test]
    async fn unpartitioned_build_restarts_activates_and_drop_removes_physical_rows() {
        let db = test_db("vector-driver-unpartitioned-build-drop").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(None);
        put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], None)).await;
        put_source(&db, scope, 1, &properties([3.0, 2.0, 1.0], None)).await;
        let (build_id, _, _) = create_build(&db, scope, &definition, 1).await;
        let one_entity = SearchIndexBatchLimits::try_new(
            NonZeroUsize::MIN,
            NonZeroU64::new(1024 * 1024).expect("one MiB is positive"),
            NonZeroU64::new(1024).expect("operation limit is positive"),
            NonZeroU64::new(16 * 1024 * 1024).expect("output limit is positive"),
            NonZeroU64::new(16 * 1024 * 1024).expect("entity output is positive"),
        )
        .expect("restart limits validate");
        let mut claim_sequence = 1;
        assert_eq!(
            drive_one(&db, &driver(), build_id, &mut claim_sequence, one_entity,).await,
            CommittedOperationStep::Progressed
        );
        let restarted = driver();
        assert_eq!(
            drive_to_terminal(&db, &restarted, build_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        let active = read_index(&db, scope, &definition).await;
        let IndexStateV2::Active {
            physical:
                PhysicalGeneration::Vector {
                    layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
                    ..
                },
            ..
        } = active.state()
        else {
            panic!("completed vector build is active and unpartitioned");
        };
        let active_handle = ActiveIndexHandle::try_from_record(scope, &active)
            .expect("active vector record projects a handle");
        let generation = ValidatedVectorGenerationHandle::try_from_active::<
            vector::distance::Euclidean,
        >(&active_handle, *physical_index_id)
        .expect("active physical generation validates");
        let index = VectorIndex::<vector::distance::Euclidean>::from_generation(&generation);
        assert!(index.get_item(&db, 0).await.unwrap().is_some());
        assert!(index.get_item(&db, 1).await.unwrap().is_some());

        let one_cleanup_operation = SearchIndexBatchLimits::try_new(
            NonZeroUsize::new(1024).expect("source-entity limit is positive"),
            NonZeroU64::new(1024 * 1024).expect("one MiB is positive"),
            NonZeroU64::MIN,
            NonZeroU64::new(16 * 1024 * 1024).expect("output limit is positive"),
            NonZeroU64::new(16 * 1024 * 1024).expect("entity output is positive"),
        )
        .expect("single-operation cleanup limits validate");
        let limited_cleanup_transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("limited vector cleanup transaction opens");
        let PhysicalCleanupOutcome::Progress {
            counters,
            namespace_empty,
            mapping_deleted,
        } = delete_physical_namespace::<vector::distance::Euclidean>(
            &limited_cleanup_transaction,
            &generation,
            None,
            IndexElementKind::Node,
            OperationCounters::default(),
            one_cleanup_operation,
        )
        .await
        .expect("limited physical cleanup batch plans")
        else {
            panic!("one physical delete fits the cleanup transaction budgets");
        };
        assert_eq!(counters.entities, 1);
        assert_eq!(counters.output_operations, 1);
        assert!(!namespace_empty);
        assert!(!mapping_deleted);
        drop(limited_cleanup_transaction);

        let cleanup_transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("vector cleanup transaction opens");
        let PhysicalCleanupOutcome::Progress {
            counters,
            namespace_empty,
            mapping_deleted,
        } = delete_physical_namespace::<vector::distance::Euclidean>(
            &cleanup_transaction,
            &generation,
            None,
            IndexElementKind::Node,
            OperationCounters::default(),
            one_entity,
        )
        .await
        .expect("physical cleanup batch plans")
        else {
            panic!("complete physical namespace fits the cleanup transaction budgets");
        };
        assert!(namespace_empty);
        assert!(!mapping_deleted);
        assert!(
            counters.entities
                > u64::try_from(one_entity.max_entities().get())
                    .expect("source-entity limit fits u64"),
            "physical cleanup rows must not consume the decoded source-entity limit"
        );
        assert_eq!(counters.output_operations, counters.entities);
        drop(cleanup_transaction);

        let IndexDdlReceipt::Accepted {
            operation_id: drop_id,
            ..
        } = drop_index_operation(&db, scope, &definition)
            .await
            .expect("active vector drop is enqueued")
        else {
            panic!("active vector drop creates a new operation");
        };
        assert_eq!(
            drive_one(
                &db,
                &restarted,
                drop_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Progressed
        );
        assert_eq!(
            drive_one(
                &db,
                &restarted,
                drop_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Progressed
        );
        let cleanup_restart = driver();
        assert_eq!(
            drive_to_terminal(&db, &cleanup_restart, drop_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        assert!(matches!(
            read_index(&db, scope, &definition).await.state(),
            IndexStateV2::Dropped { .. }
        ));
        assert!(index.get_metadata(&db).await.unwrap().is_none());
        assert!(index
            .cleanup_scan(&db)
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .is_none());
        db.close().await.expect("vector test database closes");
    }

    /// Proves partition mappings remain the cleanup cursor until their entire
    /// physical namespace is gone, including one-row batch restarts.
    #[tokio::test]
    async fn partitioned_drop_resumes_each_mapping_and_removes_every_namespace() {
        let db = test_db("vector-driver-partitioned-drop").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(Some("account_id"));
        put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], Some(10))).await;
        put_source(&db, scope, 1, &properties([4.0, 5.0, 6.0], Some(20))).await;
        let (build_id, index_id, generation) = create_build(&db, scope, &definition, 1).await;
        let driver = driver();
        let mut claim_sequence = 1;
        assert_eq!(
            drive_to_terminal(&db, &driver, build_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        let active = read_index(&db, scope, &definition).await;
        let active_handle = ActiveIndexHandle::try_from_record(scope, &active)
            .expect("partitioned Active record projects a handle");
        let mappings = mapping_values(&db, scope, index_id, generation).await;
        assert_eq!(mappings.len(), 2);
        let indexes = mappings
            .iter()
            .map(|mapping| {
                let generation = ValidatedVectorGenerationHandle::try_from_active::<
                    vector::distance::Euclidean,
                >(&active_handle, mapping.physical_index_id)
                .expect("partition mapping validates against the Active handle");
                VectorIndex::<vector::distance::Euclidean>::from_generation(&generation)
            })
            .collect::<Vec<_>>();

        let IndexDdlReceipt::Accepted {
            operation_id: drop_id,
            ..
        } = drop_index_operation(&db, scope, &definition)
            .await
            .expect("partitioned drop enqueues")
        else {
            panic!("partitioned Active drop creates a cleanup operation");
        };
        let one_entity = SearchIndexBatchLimits::try_new(
            NonZeroUsize::MIN,
            NonZeroU64::new(1024 * 1024).unwrap(),
            NonZeroU64::new(1024).unwrap(),
            NonZeroU64::new(16 * 1024 * 1024).unwrap(),
            NonZeroU64::new(16 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        for _ in 0..64 {
            let step = drive_one(&db, &driver, drop_id, &mut claim_sequence, one_entity).await;
            if step == CommittedOperationStep::Completed {
                break;
            }
            assert_eq!(step, CommittedOperationStep::Progressed);
        }
        assert!(matches!(
            read_index(&db, scope, &definition).await.state(),
            IndexStateV2::Dropped { .. }
        ));
        assert!(mapping_values(&db, scope, index_id, generation)
            .await
            .is_empty());
        for index in indexes {
            assert!(index
                .cleanup_scan(&db)
                .await
                .unwrap()
                .next()
                .await
                .unwrap()
                .is_none());
        }
        db.close().await.expect("vector test database closes");
    }

    /// Exercises every cleanup checkpoint rejection before the outbox is
    /// allowed to commit a new durable progress value.
    #[tokio::test]
    async fn cleanup_checkpoint_rejections_and_limit_blockers_are_typed() {
        let db = test_db("vector-driver-cleanup-boundaries").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(None);
        put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], None)).await;
        let (build_id, _, _) = create_build(&db, scope, &definition, 0).await;
        let driver = driver();
        let mut claim_sequence = 1;
        assert_eq!(
            drive_to_terminal(&db, &driver, build_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        let IndexDdlReceipt::Accepted {
            operation_id: drop_id,
            ..
        } = drop_index_operation(&db, scope, &definition)
            .await
            .expect("drop operation enqueues")
        else {
            panic!("Active vector drop creates a cleanup operation");
        };
        let record = read_index(&db, scope, &definition).await;
        let operation = crate::index_lifecycle::outbox::read_operation(&db, scope, drop_id)
            .await
            .unwrap()
            .expect("drop operation exists");
        let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
            unreachable!("fixture definition is vector");
        };
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let limits = SearchIndexBackfillLimits::default().batch();

        let stale_cursor = IndexCursor::try_new(Bytes::from_static(b"stale-cursor")).unwrap();
        for progress in [
            VectorCleanupProgress::DeletePhysical(PrefixScanProgress {
                cursor: Some(stale_cursor.clone()),
                counters: OperationCounters::default(),
            }),
            VectorCleanupProgress::DeleteDeltas(PrefixScanProgress {
                cursor: Some(stale_cursor),
                counters: OperationCounters::default(),
            }),
        ] {
            assert!(matches!(
                step_cleanup::<vector::distance::Euclidean>(
                    &transaction,
                    scope,
                    &operation,
                    &record,
                    vector_definition,
                    &progress,
                    false,
                    limits,
                    driver.cache_registry.as_ref(),
                )
                .await,
                Err(HelixDbError::IndexCatalogCorruption(_))
            ));
        }

        let tiny = SearchIndexBatchLimits::try_new(
            NonZeroUsize::MIN,
            NonZeroU64::MIN,
            NonZeroU64::MIN,
            NonZeroU64::MIN,
            NonZeroU64::MIN,
        )
        .unwrap();
        assert!(matches!(
            step_cleanup::<vector::distance::Euclidean>(
                &transaction,
                scope,
                &operation,
                &record,
                vector_definition,
                &VectorCleanupProgress::DeletePhysical(PrefixScanProgress {
                    cursor: None,
                    counters: OperationCounters::default(),
                }),
                false,
                tiny,
                driver.cache_registry.as_ref(),
            )
            .await
            .unwrap()
            .result,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::OversizedEntity { .. })
        ));
        assert!(matches!(
            step_cleanup::<vector::distance::Euclidean>(
                &transaction,
                scope,
                &operation,
                &record,
                vector_definition,
                &VectorCleanupProgress::RetireCache(NoCursorProgress::default()),
                false,
                limits,
                driver.cache_registry.as_ref(),
            )
            .await
            .unwrap()
            .result,
            IndexOperationStepResult::Progressed(IndexOperationProgress::VectorCleanup(
                VectorCleanupProgress::DeletePhysical(_)
            ))
        ));
        assert!(matches!(
            step_cleanup::<vector::distance::Euclidean>(
                &transaction,
                scope,
                &operation,
                &record,
                vector_definition,
                &VectorCleanupProgress::Finalize(NoCursorProgress::default()),
                false,
                limits,
                driver.cache_registry.as_ref(),
            )
            .await
            .unwrap()
            .result,
            IndexOperationStepResult::Completed(IndexOperationOutcome::DropSucceeded)
        ));
        drop(transaction);
        db.close().await.expect("vector test database closes");
    }

    /// Covers source-bound ordering, indivisible input admission, malformed
    /// documents, and duplicate applied-state rejection before HNSW planning.
    #[tokio::test]
    async fn source_scan_rejects_every_preplanning_boundary() {
        let db = test_db("vector-driver-source-boundaries").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(None);
        put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], None)).await;
        let (operation_id, _, _) = create_build(&db, scope, &definition, 0).await;
        let operation = crate::index_lifecycle::outbox::read_operation(&db, scope, operation_id)
            .await
            .unwrap()
            .expect("build operation exists");
        let record = read_index(&db, scope, &definition).await;
        let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
            unreachable!("fixture definition is vector");
        };
        let IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
            VectorBuildStage::Scan(initial_progress),
        )) = operation.progress()
        else {
            panic!("new vector build begins at source scan");
        };
        let driver = driver();
        let limits = SearchIndexBackfillLimits::default().batch();

        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let equal = SourceScanProgress {
            inclusive_upper_bound: initial_progress.inclusive_upper_bound.clone(),
            cursor: Some(initial_progress.inclusive_upper_bound.clone()),
            counters: OperationCounters::default(),
        };
        assert!(matches!(
            scan_source::<vector::distance::Euclidean>(
                &db,
                &transaction,
                scope,
                &operation,
                &record,
                vector_definition,
                &equal,
                limits,
                IndexLifecycleScanTuning::default(),
                Arc::clone(&driver.simhasher_registry),
                driver.batch_reads,
                &mut driver.build_cache.checkout_fresh().await,
            )
            .await
            .unwrap()
            .result,
            IndexOperationStepResult::Progressed(IndexOperationProgress::VectorBuild(
                VectorBuildProgress::Constructing(VectorBuildStage::ValidateDescriptor(_))
            ))
        ));
        let greater = SourceScanProgress {
            inclusive_upper_bound: initial_progress.inclusive_upper_bound.clone(),
            cursor: Some(source_cursor(scope, 1)),
            counters: OperationCounters::default(),
        };
        assert!(matches!(
            scan_source::<vector::distance::Euclidean>(
                &db,
                &transaction,
                scope,
                &operation,
                &record,
                vector_definition,
                &greater,
                limits,
                IndexLifecycleScanTuning::default(),
                Arc::clone(&driver.simhasher_registry),
                driver.batch_reads,
                &mut driver.build_cache.checkout_fresh().await,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        drop(transaction);

        let tiny = SearchIndexBatchLimits::try_new(
            NonZeroUsize::MIN,
            NonZeroU64::MIN,
            NonZeroU64::new(1024).unwrap(),
            NonZeroU64::new(1024).unwrap(),
            NonZeroU64::new(1024).unwrap(),
        )
        .unwrap();
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(matches!(
            scan_source::<vector::distance::Euclidean>(
                &db,
                &transaction,
                scope,
                &operation,
                &record,
                vector_definition,
                initial_progress,
                tiny,
                IndexLifecycleScanTuning::default(),
                Arc::clone(&driver.simhasher_registry),
                driver.batch_reads,
                &mut driver.build_cache.checkout_fresh().await,
            )
            .await
            .unwrap()
            .result,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::OversizedEntity {
                entity_id,
                ..
            }) if entity_id == IndexEntityId::new(0)
        ));
        drop(transaction);

        db.put(source_key(scope, 0), Bytes::from_static(&[0xff]))
            .await
            .unwrap();
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(matches!(
            scan_source::<vector::distance::Euclidean>(
                &db,
                &transaction,
                scope,
                &operation,
                &record,
                vector_definition,
                initial_progress,
                limits,
                IndexLifecycleScanTuning::default(),
                Arc::clone(&driver.simhasher_registry),
                driver.batch_reads,
                &mut driver.build_cache.checkout_fresh().await,
            )
            .await
            .unwrap()
            .result,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::InvalidSourceData {
                entity_id,
                ..
            }) if entity_id == IndexEntityId::new(0)
        ));
        drop(transaction);

        let wrong_dimension = vec![
            Property::new("$label", PropertyValue::String("Document".to_string())),
            Property::new("embedding", PropertyValue::F32Array(vec![1.0, 2.0])),
        ];
        put_source(&db, scope, 0, &wrong_dimension).await;
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(matches!(
            scan_source::<vector::distance::Euclidean>(
                &db,
                &transaction,
                scope,
                &operation,
                &record,
                vector_definition,
                initial_progress,
                limits,
                IndexLifecycleScanTuning::default(),
                Arc::clone(&driver.simhasher_registry),
                driver.batch_reads,
                &mut driver.build_cache.checkout_fresh().await,
            )
            .await
            .unwrap()
            .result,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::InvalidSourceData { .. })
        ));
        drop(transaction);

        put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], None)).await;
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        stage_applied(
            &transaction,
            scope,
            &operation,
            IndexElementKind::Node,
            IndexEntityId::new(0),
            Some(TextPartition::Unpartitioned),
        )
        .unwrap();
        assert!(matches!(
            scan_source::<vector::distance::Euclidean>(
                &db,
                &transaction,
                scope,
                &operation,
                &record,
                vector_definition,
                initial_progress,
                limits,
                IndexLifecycleScanTuning::default(),
                Arc::clone(&driver.simhasher_registry),
                driver.batch_reads,
                &mut driver.build_cache.checkout_fresh().await,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("existing applied state")
        ));
        drop(transaction);
        db.close().await.expect("vector test database closes");
    }

    #[tokio::test]
    async fn source_scan_attributes_out_of_domain_finite_vector_to_exact_entity() {
        let db = test_db("vector-driver-magnitude-source-attribution").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(None);
        let limit = crate::search::vector::magnitude_oracle::inclusive_limit(
            VectorDistanceMetric::Euclidean,
            3,
        )
        .unwrap();
        let outside = crate::search::vector::magnitude_oracle::next_up(limit);
        let entity_id = 7;
        put_source(
            &db,
            scope,
            entity_id,
            &properties([outside, 0.0, 0.0], None),
        )
        .await;
        let (operation_id, _, _) = create_build(&db, scope, &definition, entity_id).await;
        let mut claim_sequence = 1;
        let outcome = drive_to_terminal(&db, &driver(), operation_id, &mut claim_sequence).await;
        let operation = read_operation(&db, scope, operation_id).await;
        db.close().await.expect("vector test database closes");

        assert_eq!(outcome, CommittedOperationStep::Blocked);
        assert!(matches!(
            operation.execution_state(),
            crate::index_lifecycle::IndexOperationExecutionState::Blocked(
                IndexOperationBlocker::InvalidSourceData {
                    entity_kind: IndexElementKind::Node,
                    entity_id: blocked_entity_id,
                }
            ) if *blocked_entity_id == IndexEntityId::new(entity_id)
        ));
    }

    /// Verifies descriptor-validation cursors fail on malformed bytes and
    /// dispatch typed non-V2 and mapping keys to their exact scan lanes.
    #[tokio::test]
    async fn descriptor_validation_cursor_dispatch_is_typed() {
        let db = test_db("vector-driver-validation-cursors").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(None);
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
        let operation = crate::index_lifecycle::outbox::read_operation(&db, scope, operation_id)
            .await
            .unwrap()
            .expect("build operation exists");
        let record = read_index(&db, scope, &definition).await;
        let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
            unreachable!("fixture definition is vector");
        };
        let limits = SearchIndexBackfillLimits::default().batch();
        let driver = driver();
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();

        let malformed = PrefixScanProgress {
            cursor: Some(
                IndexCursor::try_new(Bytes::from_static(b"malformed"))
                    .expect("malformed bytes still fit the cursor envelope"),
            ),
            counters: OperationCounters::default(),
        };
        assert!(validate_descriptor::<vector::distance::Euclidean>(
            &db,
            &transaction,
            scope,
            &operation,
            &record,
            vector_definition,
            &malformed,
            limits,
            Arc::clone(&driver.simhasher_registry),
        )
        .await
        .is_err());

        for cursor in [
            source_cursor(scope, 0),
            IndexCursor::try_new(GlobalKey::StorageVersion.to_bytes())
                .expect("storage-version key is a bounded cursor"),
        ] {
            let progress = PrefixScanProgress {
                cursor: Some(cursor),
                counters: OperationCounters::default(),
            };
            validate_descriptor::<vector::distance::Euclidean>(
                &db,
                &transaction,
                scope,
                &operation,
                &record,
                vector_definition,
                &progress,
                limits,
                Arc::clone(&driver.simhasher_registry),
            )
            .await
            .expect_err("typed non-V2 cursor cannot resume the applied-state lane");
        }
        let mapping = PrefixScanProgress {
            cursor: Some(
                IndexCursor::try_new(scoped_index_key(
                    scope,
                    ScopedKey::VectorPartitionMapping(
                        crate::encoding::v2::keys::VectorPartitionMappingKey {
                            index_id,
                            generation,
                            partition: TextPartition::Unpartitioned.fingerprint(),
                        },
                    ),
                ))
                .expect("mapping key is a bounded cursor"),
            ),
            counters: OperationCounters::default(),
        };
        validate_descriptor::<vector::distance::Euclidean>(
            &db,
            &transaction,
            scope,
            &operation,
            &record,
            vector_definition,
            &mapping,
            limits,
            Arc::clone(&driver.simhasher_registry),
        )
        .await
        .expect("typed mapping cursor resumes mapping validation");
        drop(transaction);
        db.close().await.expect("vector test database closes");
    }

    /// A build started before operations were queued may hold `BuildDelta`
    /// rows that nothing replays; every stage that could activate them
    /// blocks. Without a delta the same stages move on, so each block is the
    /// delta's.
    #[tokio::test]
    async fn pre_queue_build_deltas_block_catch_up_validation_and_activation() {
        let db = test_db("vector-driver-pre-queue-deltas").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(None);
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
        let operation = read_operation(&db, scope, operation_id).await;
        let record = read_index(&db, scope, &definition).await;
        let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
            unreachable!("fixture definition is vector");
        };
        let driver = driver();
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let counters = OperationCounters::default();
        let catch_up = VectorBuildStage::CatchUp(PrefixScanProgress {
            cursor: None,
            counters,
        });
        let validate = VectorBuildStage::ValidateDescriptor(PrefixScanProgress {
            cursor: None,
            counters,
        });
        let activate = VectorBuildStage::Activate(NoCursorProgress { counters });
        let step = |stage: VectorBuildStage| {
            let (db, transaction, operation, record, driver) =
                (&db, &transaction, &operation, &record, &driver);
            let registry = Arc::clone(&driver.simhasher_registry);
            async move {
                step_build::<vector::distance::Euclidean>(
                    db,
                    transaction,
                    scope,
                    operation,
                    record,
                    vector_definition,
                    &stage,
                    SearchIndexBackfillLimits::default().batch(),
                    IndexLifecycleScanTuning::default(),
                    registry,
                    driver.batch_reads,
                    &driver.build_cache,
                )
                .await
                .expect("vector build step runs")
                .result
            }
        };

        assert!(matches!(
            step(catch_up.clone()).await,
            IndexOperationStepResult::Progressed(IndexOperationProgress::VectorBuild(
                VectorBuildProgress::Constructing(VectorBuildStage::ValidateDescriptor(_))
            ))
        ));
        assert!(matches!(
            step(validate.clone()).await,
            IndexOperationStepResult::Progressed(IndexOperationProgress::VectorBuild(
                VectorBuildProgress::Constructing(VectorBuildStage::Activate(_))
            ))
        ));
        assert!(matches!(
            step(activate.clone()).await,
            IndexOperationStepResult::Completed(IndexOperationOutcome::Build(
                BuildOperationOutcome::Succeeded
            ))
        ));

        let entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(0),
        };
        transaction
            .put(
                scoped_index_key(
                    scope,
                    ScopedKey::BuildDelta(IndexEntityStateKey {
                        index_id,
                        generation,
                        entity,
                    }),
                ),
                encode_build_delta(&CoalescedBuildDeltaValue {
                    index_id,
                    generation,
                    entity_kind: entity.kind,
                    entity_id: entity.id,
                    state: crate::index_lifecycle::work::CoalescedBuildDeltaState::Marker,
                }),
            )
            .unwrap();
        for stage in [catch_up, validate, activate] {
            assert!(
                matches!(
                    step(stage.clone()).await,
                    IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation)
                ),
                "{stage:?} must block on a pre-queue build delta"
            );
        }
        drop(transaction);
        db.close().await.expect("vector test database closes");
    }

    /// Drives every typed vector work-row decoder through wrong-key,
    /// wrong-value, and key/value-ownership failures and covers helper states
    /// that normal lifecycle construction makes unreachable.
    #[tokio::test]
    async fn typed_row_decoders_and_batch_accounting_fail_closed() {
        let db = test_db("vector-driver-row-boundaries").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(None);
        put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], None)).await;
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
        let operation = crate::index_lifecycle::outbox::read_operation(&db, scope, operation_id)
            .await
            .unwrap()
            .expect("build operation exists");
        let entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(0),
        };
        let other_entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(1),
        };
        let delta_key = scoped_index_key(
            scope,
            ScopedKey::BuildDelta(IndexEntityStateKey {
                index_id,
                generation,
                entity,
            }),
        );
        let applied_key = applied_key(
            scope,
            index_id,
            generation,
            IndexElementKind::Node,
            IndexEntityId::new(0),
        );
        let delta_value = CoalescedBuildDeltaValue {
            index_id,
            generation,
            entity_kind: entity.kind,
            entity_id: entity.id,
            state: crate::index_lifecycle::work::CoalescedBuildDeltaState::Marker,
        };
        let applied_value = AppliedEntityStateValue {
            index_id,
            generation,
            entity_kind: entity.kind,
            entity_id: entity.id,
            state: AppliedFamilyState::Vector(Some(TextPartition::Unpartitioned)),
        };
        let encoded_delta = encode_build_delta(&delta_value);
        let encoded_applied = encode_applied_state(&applied_value.clone());

        assert!(matches!(
            decode_delta(scope, &applied_key, &encoded_delta),
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("another key kind")
        ));
        assert!(matches!(
            decode_delta(scope, &delta_key, &encoded_applied),
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("another value kind")
        ));
        let mismatched_delta = encode_build_delta(&CoalescedBuildDeltaValue {
            entity_id: other_entity.id,
            ..delta_value
        });
        assert!(matches!(
            decode_delta(scope, &delta_key, &mismatched_delta),
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("key/value mismatch")
        ));

        assert!(matches!(
            decode_applied(scope, &delta_key, &encoded_applied),
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("another key kind")
        ));
        assert!(matches!(
            decode_applied(scope, &applied_key, &encoded_delta),
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("another value kind")
        ));
        let mismatched_applied = encode_applied_state(&AppliedEntityStateValue {
            entity_id: other_entity.id,
            ..applied_value.clone()
        });
        assert!(matches!(
            decode_applied(scope, &applied_key, &mismatched_applied),
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("key/value mismatch")
        ));

        let tenant = VectorTenantPartition::try_new(Bytes::from_static(b"tenant")).unwrap();
        let mapping_key = scoped_index_key(
            scope,
            ScopedKey::VectorPartitionMapping(
                crate::encoding::v2::keys::VectorPartitionMappingKey {
                    index_id,
                    generation,
                    partition: tenant.fingerprint(),
                },
            ),
        );
        let mapping_value = crate::index_lifecycle::work::VectorPartitionMappingValue {
            index_id,
            generation,
            partition: tenant.clone(),
            physical_index_id: VectorPhysicalIndexId::initial(),
        };
        let encoded_mapping = encode_partition_mapping(&mapping_value.clone());
        assert!(matches!(
            decode_mapping(scope, &delta_key, &encoded_mapping, &operation),
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("another key kind")
        ));
        assert!(matches!(
            decode_mapping(scope, &mapping_key, &encoded_delta, &operation),
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("another value kind")
        ));
        let mismatched_mapping =
            encode_partition_mapping(&crate::index_lifecycle::work::VectorPartitionMappingValue {
                index_id: IndexId::new(index_id.get() + 1).unwrap(),
                ..mapping_value
            });
        assert!(matches!(
            decode_mapping(scope, &mapping_key, &mismatched_mapping, &operation),
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("ownership mismatch")
        ));

        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        transaction
            .put(applied_key.clone(), mismatched_applied)
            .unwrap();
        assert!(matches!(
            load_applied(
                &transaction,
                scope,
                index_id,
                generation,
                entity.kind,
                entity.id,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("key/value mismatch")
        ));
        transaction
            .put(
                applied_key.clone(),
                encode_applied_state(&AppliedEntityStateValue {
                    state: AppliedFamilyState::Secondary(None),
                    ..applied_value
                }),
            )
            .unwrap();
        assert!(matches!(
            load_applied(
                &transaction,
                scope,
                index_id,
                generation,
                entity.kind,
                entity.id,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("another applied family")
        ));
        stage_applied(
            &transaction,
            scope,
            &operation,
            entity.kind,
            entity.id,
            None,
        )
        .unwrap();
        assert!(load_applied(
            &transaction,
            scope,
            index_id,
            generation,
            entity.kind,
            entity.id,
        )
        .await
        .unwrap()
        .is_none());
        assert!(!generation_has_rows(
            &transaction,
            scope,
            RecordKind::VectorPartitionMapping,
            index_id,
            generation,
        )
        .await
        .unwrap());
        transaction
            .put(delta_key.clone(), encoded_delta.clone())
            .unwrap();
        assert!(generation_has_rows(
            &transaction,
            scope,
            RecordKind::BuildDelta,
            index_id,
            generation,
        )
        .await
        .unwrap());

        let edge = IndexEntity {
            kind: IndexElementKind::Edge,
            id: IndexEntityId::new(7),
        };
        let edge_key = DataKey::Data {
            scope,
            kind: DataKeyKind::EdgePropertyById(
                crate::encoding::v2::keys::EdgePropertyByIdKey::new(edge.id.get()),
            ),
        }
        .to_bytes();
        assert_eq!(
            source_entity(scope, IndexElementKind::Edge, &edge_key).unwrap(),
            Some(edge.id)
        );
        assert_eq!(
            source_entity(scope, IndexElementKind::Edge, &source_key(scope, 0)).unwrap(),
            None
        );
        assert!(source_entity(scope, IndexElementKind::Node, &edge_key).is_err());
        let global = IndexKey::Global {
            kind: GlobalKey::StorageVersion,
        }
        .to_bytes();
        assert!(source_entity(scope, IndexElementKind::Node, &global).is_err());

        let prefix = source_prefix(scope, IndexElementKind::Node);
        assert_eq!(cursor_suffix(&prefix, None).unwrap(), None);
        assert_eq!(
            cursor_suffix(&prefix, Some(&source_cursor(scope, 0))).unwrap(),
            Some(source_key(scope, 0).slice(prefix.len()..))
        );
        assert!(cursor_suffix(
            &prefix,
            Some(&IndexCursor::try_new(Bytes::from_static(b"outside")).unwrap()),
        )
        .is_err());

        assert!(load_operation_index(&transaction, scope, &operation)
            .await
            .is_ok());
        assert!(matches!(
            load_operation_index(
                &transaction,
                DataScope::Tenant(
                    crate::encoding::v2::keys::scope::TenantId::from_u128(1)
                ),
                &operation,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("no canonical index")
        ));

        let limits = SearchIndexBackfillLimits::default().batch();
        let progress = SourceScanProgress {
            inclusive_upper_bound: source_cursor(scope, 0),
            cursor: None,
            counters: OperationCounters::default(),
        };
        assert!(matches!(
            finish_or_block_scan(
                EntityPlanOutcome::Blocked(IndexOperationBlocker::InvariantViolation),
                VectorBatchAccounting::new(OperationCounters::default(), limits),
                entity.kind,
                entity.id,
                &progress,
                None,
                VectorBuildSessionStats::default(),
            )
            .unwrap()
            .result,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation)
        ));
        assert!(matches!(
            finish_or_block_scan(
                EntityPlanOutcome::BatchFull,
                VectorBatchAccounting::new(OperationCounters::default(), limits),
                entity.kind,
                entity.id,
                &progress,
                None,
                VectorBuildSessionStats::default(),
            )
            .unwrap()
            .result,
            IndexOperationStepResult::Progressed(IndexOperationProgress::VectorBuild(
                VectorBuildProgress::Constructing(VectorBuildStage::Scan(_))
            ))
        ));
        assert!(finish_or_block_scan(
            EntityPlanOutcome::Admitted {
                vector_writes: VectorWriteMeasurement::zero(),
                single_vector_output_bytes: 0,
                lifecycle_operations: 0,
                lifecycle_bytes: 0,
                next_partition: None,
            },
            VectorBatchAccounting::new(OperationCounters::default(), limits),
            entity.kind,
            entity.id,
            &progress,
            None,
            VectorBuildSessionStats::default(),
        )
        .is_err());

        let mut accounting = VectorBatchAccounting::new(
            OperationCounters {
                entities: u64::MAX,
                ..OperationCounters::default()
            },
            limits,
        );
        accounting
            .admit(1, VectorWriteMeasurement::zero(), 0, 1, 1)
            .unwrap();
        assert!(accounting.finish().is_err());

        let mut planning_accounting =
            VectorBatchAccounting::new(OperationCounters::default(), limits);
        planning_accounting.record_planning();
        planning_accounting
            .admit(1, VectorWriteMeasurement::for_test(3, 30), 30, 0, 0)
            .unwrap();
        let planning = planning_accounting.planning_usage(VectorBuildSessionStats::default());
        assert_eq!(planning.planning_executions, 1);
        assert_eq!(planning.planned_writes, 3);
        assert_eq!(planning.replay_executions, 0);

        let record = read_index(&db, scope, &definition).await;
        let target = VectorPlanTarget::build(scope, &operation, &record).unwrap();
        assert!(
            lifecycle_write_measurement(
                &target,
                entity.kind,
                entity.id,
                AppliedStateTransition::Put(&TextPartition::Unpartitioned),
                Some((
                    &TextPartition::Unpartitioned,
                    VectorPhysicalIndexId::initial()
                )),
                &[],
            )
            .is_err(),
            "only a tenant partition owns a mapping"
        );
        assert_eq!(
            lifecycle_write_measurement(
                &target,
                entity.kind,
                entity.id,
                AppliedStateTransition::Delete,
                None,
                &[],
            )
            .unwrap()
            .0,
            1
        );
        assert!(matches!(record.state(), IndexStateV2::Building { .. }));
        drop(transaction);
        db.close().await.expect("vector test database closes");
    }

    #[tokio::test]
    async fn active_unpartitioned_search_matches_deterministic_brute_force_oracle() {
        const VECTORS: [[f32; 3]; 8] = [
            [0.0, 0.0, 0.0],
            [1.0, 0.0, 0.0],
            [0.0, 2.0, 0.0],
            [0.0, 0.0, 3.0],
            [4.0, 0.0, 0.0],
            [0.0, 5.0, 0.0],
            [0.0, 0.0, 6.0],
            [7.0, 7.0, 7.0],
        ];
        const QUERY: [f32; 3] = [0.25, 0.5, 0.75];

        let db = test_db("vector-driver-brute-force-oracle").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(None);
        for (entity_id, vector) in VECTORS.iter().enumerate() {
            put_source(&db, scope, entity_id as u64, &properties(*vector, None)).await;
        }
        let (build_id, _, _) =
            create_build(&db, scope, &definition, VECTORS.len() as u64 - 1).await;
        let driver = driver();
        let mut claim_sequence = 1;
        assert_eq!(
            drive_to_terminal(&db, &driver, build_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        let active = read_index(&db, scope, &definition).await;
        let IndexStateV2::Active {
            physical:
                PhysicalGeneration::Vector {
                    layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
                    ..
                },
            ..
        } = active.state()
        else {
            panic!("completed vector build is active and unpartitioned");
        };
        let active_handle = ActiveIndexHandle::try_from_record(scope, &active)
            .expect("active vector record projects a handle");
        let generation = ValidatedVectorGenerationHandle::try_from_active::<
            vector::distance::Euclidean,
        >(&active_handle, *physical_index_id)
        .expect("active physical generation validates");
        let index = VectorIndex::<vector::distance::Euclidean>::from_generation(&generation);
        let params = SearchParams::new(VECTORS.len())
            .unwrap()
            .with_ef(VECTORS.len())
            .unwrap()
            .with_simhash_mode(SimHashMode::Off)
            .with_pre_simhash_sampling_ratio(1.0)
            .unwrap();
        let actual = index.search(&db, &QUERY, &params).await.unwrap();

        let mut expected = VECTORS
            .iter()
            .enumerate()
            .map(|(entity_id, vector)| {
                let score = vector
                    .iter()
                    .zip(QUERY)
                    .map(|(component, query)| {
                        let difference = *component - query;
                        difference * difference
                    })
                    .sum::<f32>();
                (
                    entity_id as u64,
                    DistanceScore::try_new(score).expect("oracle score is finite"),
                )
            })
            .collect::<Vec<_>>();
        expected.sort_by(|left, right| left.1.cmp(&right.1).then(left.0.cmp(&right.0)));
        assert_eq!(
            actual
                .into_iter()
                .map(|result| (result.entity_id(), result.score()))
                .collect::<Vec<_>>(),
            expected
        );
        db.close().await.expect("vector test database closes");
    }

    #[tokio::test]
    async fn oversized_partition_build_blocks_before_mapping_or_watermark_writes() {
        let db = test_db("vector-driver-block-before-physical").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(Some("account_id"));
        put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], Some(10))).await;
        let (build_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
        let before_watermark = peek_vector_physical_id(&db)
            .await
            .expect("vector watermark is readable");
        let tiny_output = SearchIndexBatchLimits::try_new(
            NonZeroUsize::MIN,
            NonZeroU64::new(1024 * 1024).expect("input limit is positive"),
            NonZeroU64::MIN,
            NonZeroU64::MIN,
            NonZeroU64::MIN,
        )
        .expect("tiny output policy validates");
        let mut claim_sequence = 1;
        assert_eq!(
            drive_one(&db, &driver(), build_id, &mut claim_sequence, tiny_output,).await,
            CommittedOperationStep::Blocked
        );
        assert!(mapping_values(&db, scope, index_id, generation)
            .await
            .is_empty());
        assert_eq!(
            peek_vector_physical_id(&db)
                .await
                .expect("vector watermark remains readable"),
            before_watermark
        );
        db.close().await.expect("vector test database closes");
    }

    #[tokio::test]
    async fn abort_removes_hidden_physical_rows_and_builder_work() {
        let db = test_db("vector-driver-abort-cleanup").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(None);
        put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], None)).await;
        let (build_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
        let driver = driver();
        let mut claim_sequence = 1;
        assert_eq!(
            drive_one(
                &db,
                &driver,
                build_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Progressed
        );
        let receipt = drop_index_operation(&db, scope, &definition)
            .await
            .expect("building vector converts to abort cleanup");
        assert!(matches!(
            receipt,
            IndexDdlReceipt::ExistingOperation { operation_id } if operation_id == build_id
        ));
        assert_eq!(
            drive_to_terminal(&db, &driver, build_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        assert!(matches!(
            read_index(&db, scope, &definition).await.state(),
            IndexStateV2::Dropped { .. }
        ));
        for kind in [
            RecordKind::BuildDelta,
            RecordKind::AppliedState,
            RecordKind::VectorPartitionMapping,
        ] {
            let prefix = generation_prefix(scope, kind, index_id, generation);
            let mut rows = db
                .scan_prefix(prefix, ..)
                .await
                .expect("cleanup generation prefix is readable");
            assert!(rows
                .next()
                .await
                .expect("cleanup generation row is readable")
                .is_none());
        }
        db.close().await.expect("vector test database closes");
    }

    #[tokio::test]
    async fn adoption_abort_restores_source_reservation_without_deleting_physical_rows() {
        let db = test_db("vector-driver-adoption-abort").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(None);
        let physical_index_id = VectorPhysicalIndexId::new(55).expect("fixture ID is nonzero");
        let physical_row_key = DataKey::Data {
            scope,
            kind: DataKeyKind::Vector(
                crate::encoding::v2::keys::indexes::vector::VectorKey::SimHash(
                    crate::encoding::v2::keys::indexes::vector::VectorSimHashKey::new(
                        physical_index_id.get(),
                        77,
                    ),
                ),
            ),
        }
        .to_bytes();
        let physical_row_value = Bytes::copy_from_slice(
            &crate::encoding::v2::values::indexes::vector::simhash::encode_simhash(17),
        );
        let directory_keys = [1_u64, 2_u64].map(|node_id| {
            DataKey::Data {
                scope,
                kind: DataKeyKind::Vector(
                    crate::encoding::v2::keys::indexes::vector::VectorKey::SimHashDirectory(
                        crate::encoding::v2::keys::indexes::vector::VectorSimHashDirectoryKey::new(
                            physical_index_id.get(),
                            node_id,
                            node_id,
                        ),
                    ),
                ),
            }
            .to_bytes()
        });
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("legacy source transaction opens");
        transaction
            .put(&physical_row_key, &physical_row_value)
            .expect("legacy physical row stages");
        for directory_key in &directory_keys {
            transaction
                .put(
                    directory_key,
                    crate::encoding::v2::values::indexes::vector::markers::encode_simhash_directory_marker_v1(
                    ),
                )
                .expect("partial directory marker stages");
        }
        transaction
            .put(
                IndexKey::Global {
                    kind: GlobalKey::LegacyVectorPhysicalReservation(physical_index_id),
                }
                .to_bytes(),
                encode_metadata_value(&IndexV2MetadataValue::LegacyVectorPhysicalReservation(
                    LegacyVectorPhysicalReservation::LegacySource,
                )),
            )
            .expect("legacy source reservation stages");
        transaction
            .commit()
            .await
            .expect("legacy source transaction commits");

        let receipt = create_legacy_vector_adoption_operation(
            &db,
            scope,
            definition.clone(),
            physical_index_id,
        )
        .await
        .expect("legacy adoption enqueues");
        let IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
            panic!("new legacy adoption must enqueue one build")
        };
        assert!(matches!(
            crate::index_lifecycle::repository::load_legacy_vector_physical_reservation(
                &db,
                physical_index_id,
            )
            .await
            .expect("building reservation reads"),
            Some(LegacyVectorPhysicalReservation::AdoptionBuilding {
                operation_id: owner_operation,
                ..
            }) if owner_operation == operation_id
        ));
        let receipt = drop_index_operation(&db, scope, &definition)
            .await
            .expect("adoption converts to abort cleanup");
        assert!(matches!(
            receipt,
            IndexDdlReceipt::ExistingOperation { operation_id: aborted } if aborted == operation_id
        ));
        let mut claim_sequence = 1;
        assert_eq!(
            drive_to_terminal(&db, &driver(), operation_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        assert!(matches!(
            read_index(&db, scope, &definition).await.state(),
            IndexStateV2::Dropped { .. }
        ));
        assert_eq!(
            crate::index_lifecycle::repository::load_legacy_vector_physical_reservation(
                &db,
                physical_index_id,
            )
            .await
            .expect("restored source reservation reads"),
            Some(LegacyVectorPhysicalReservation::LegacySource)
        );
        assert_eq!(
            db.get(physical_row_key)
                .await
                .expect("legacy physical row reads"),
            Some(physical_row_value),
            "adoption abort must not delete or rewrite legacy physical rows"
        );
        for directory_key in directory_keys {
            assert!(
                db.get(directory_key)
                    .await
                    .expect("partial directory marker reads")
                    .is_none(),
                "adoption abort must delete only its partial directory"
            );
        }
        db.close().await.expect("vector test database closes");
    }

    #[tokio::test]
    async fn adoption_blocks_out_of_domain_legacy_physical_as_invalid() {
        let db = test_db("vector-driver-magnitude-legacy-adoption").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(None);
        let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
            unreachable!("vector definition is vector")
        };
        let runtime = vector_definition.to_runtime();
        let physical_name = crate::search::vector_index_name(
            runtime.element_type(),
            runtime.label(),
            runtime.property(),
        );
        let physical_index_id =
            VectorPhysicalIndexId::new(crate::search::vector::index_id_from_name(&physical_name))
                .expect("legacy fixture physical ID is nonzero");
        let limit = crate::search::vector::magnitude_oracle::inclusive_limit(
            VectorDistanceMetric::Euclidean,
            3,
        )
        .unwrap();
        let outside = crate::search::vector::magnitude_oracle::next_up(limit);
        let mut metadata = crate::search::vector::VectorIndexMetadata::new(
            VectorIndexConfig::from_v2_definition(vector_definition, &physical_name),
        );
        metadata.entry_point = Some(1);
        metadata.count = 1;
        let (legacy_key, legacy_value) =
            crate::migrations::migration_parity_legacy_catalog_row(&definition, false)
                .expect("legacy catalog row encodes");
        let simhash_bits = 0_u64;
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("legacy magnitude seed transaction opens");
        transaction
            .put(legacy_key, legacy_value)
            .expect("legacy catalog row stages");
        transaction
            .put(
                DataKey::Data {
                    scope,
                    kind: DataKeyKind::Vector(
                        crate::encoding::v2::keys::indexes::vector::VectorKey::IndexMetadata(
                            crate::encoding::v2::keys::indexes::vector::VectorIndexMetadataKey::new(
                                physical_index_id.get(),
                            ),
                        ),
                    ),
                }
                .to_bytes(),
                Bytes::copy_from_slice(
                    &crate::encoding::v2::legacy::vector::metadata::encode_legacy_metadata_for_contract(
                        &metadata,
                    ),
                ),
            )
            .expect("legacy metadata stages");
        transaction
            .put(
                DataKey::Data {
                    scope,
                    kind: DataKeyKind::Vector(
                        crate::encoding::v2::keys::indexes::vector::VectorKey::SimHash(
                            crate::encoding::v2::keys::indexes::vector::VectorSimHashKey::new(
                                physical_index_id.get(),
                                1,
                            ),
                        ),
                    ),
                }
                .to_bytes(),
                Bytes::copy_from_slice(
                    &crate::encoding::v2::values::indexes::vector::simhash::encode_simhash(
                        simhash_bits,
                    ),
                ),
            )
            .expect("legacy SimHash stages");
        transaction
            .put(
                DataKey::Data {
                    scope,
                    kind: DataKeyKind::Vector(
                        crate::encoding::v2::keys::indexes::vector::VectorKey::Vector(
                            crate::encoding::v2::keys::indexes::vector::VectorItemKey::new(
                                physical_index_id.get(),
                                crate::search::vector::simhash::order_code_from_simhash_bits(
                                    simhash_bits,
                                ),
                                1,
                            ),
                        ),
                    ),
                }
                .to_bytes(),
                crate::search::vector::encode_item(&crate::search::vector::Item::<
                    crate::search::vector::distance::Euclidean,
                >::new(vec![
                    outside, 0.0, 0.0,
                ])),
            )
            .expect("legacy out-of-domain payload stages");
        transaction
            .commit()
            .await
            .expect("legacy magnitude fixture commits");
        crate::migrations::preflight_legacy_vector_reservations(&db)
            .await
            .expect("legacy namespace preflight succeeds");
        let receipt = create_legacy_vector_adoption_operation(
            &db,
            scope,
            definition.clone(),
            physical_index_id,
        )
        .await
        .expect("legacy adoption enqueues");
        let IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
            panic!("new legacy adoption must enqueue one build")
        };
        let mut claim_sequence = 1;
        let driver = driver();
        assert_eq!(
            drive_one(
                &db,
                &driver,
                operation_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Progressed
        );
        assert_eq!(
            drive_one(
                &db,
                &driver,
                operation_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Progressed
        );
        let outcome = drive_one(
            &db,
            &driver,
            operation_id,
            &mut claim_sequence,
            SearchIndexBackfillLimits::default().batch(),
        )
        .await;
        let operation = read_operation(&db, scope, operation_id).await;
        db.close().await.expect("vector test database closes");

        assert_eq!(outcome, CommittedOperationStep::Blocked);
        assert!(matches!(
            operation.execution_state(),
            crate::index_lifecycle::IndexOperationExecutionState::Blocked(
                IndexOperationBlocker::InvalidLegacyPhysical
            )
        ));
    }

    #[tokio::test]
    async fn adoption_reopens_after_each_validation_lane_and_activation_boundary() {
        let store = Arc::new(InMemory::new());
        let database = "vector-driver-adoption-lane-reopen";
        let db = Db::builder(database, store.clone())
            .build()
            .await
            .expect("vector adoption database opens");
        bootstrap_writer(&db)
            .await
            .expect("vector adoption database bootstraps");
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(None);
        let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
            unreachable!("vector definition is vector")
        };
        let runtime = vector_definition.to_runtime();
        let physical_name = crate::search::vector_index_name(
            runtime.element_type(),
            runtime.label(),
            runtime.property(),
        );
        let physical_index_id =
            VectorPhysicalIndexId::new(crate::search::vector::index_id_from_name(&physical_name))
                .expect("legacy fixture physical ID is nonzero");
        let metadata = crate::search::vector::VectorIndexMetadata::new(
            VectorIndexConfig::from_v2_definition(vector_definition, &physical_name),
        );
        let (legacy_key, legacy_value) =
            crate::migrations::migration_parity_legacy_catalog_row(&definition, false)
                .expect("legacy catalog row encodes");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("legacy seed transaction opens");
        transaction
            .put(legacy_key.clone(), legacy_value)
            .expect("legacy catalog row stages");
        transaction
            .put(
                DataKey::Data {
                    scope,
                    kind: DataKeyKind::Vector(
                        crate::encoding::v2::keys::indexes::vector::VectorKey::IndexMetadata(
                            crate::encoding::v2::keys::indexes::vector::VectorIndexMetadataKey::new(
                                physical_index_id.get(),
                            ),
                        ),
                    ),
                }
                .to_bytes(),
                Bytes::copy_from_slice(
                    &crate::encoding::v2::legacy::vector::metadata::encode_legacy_metadata_for_contract(
                        &metadata,
                    ),
                ),
            )
            .expect("legacy metadata stages");
        transaction
            .commit()
            .await
            .expect("legacy seed transaction commits");
        crate::migrations::preflight_legacy_vector_reservations(&db)
            .await
            .expect("legacy namespace preflight succeeds");
        let receipt = create_legacy_vector_adoption_operation(
            &db,
            scope,
            definition.clone(),
            physical_index_id,
        )
        .await
        .expect("legacy adoption enqueues");
        let IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
            panic!("new legacy adoption must enqueue one build")
        };
        let mut claim_sequence = 1;
        let driver = driver();
        assert_eq!(
            drive_one(
                &db,
                &driver,
                operation_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Progressed
        );
        assert!(matches!(
            read_operation(&db, scope, operation_id).await.progress(),
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                VectorBuildStage::AdoptLegacy(LegacyVectorValidationProgress {
                    lane: LegacyVectorValidationLane::Hot,
                    ..
                })
            ))
        ));
        db.close().await.expect("core checkpoint closes");

        let db = Db::builder(database, store.clone())
            .build()
            .await
            .expect("core checkpoint reopens");
        assert_eq!(
            drive_one(
                &db,
                &driver,
                operation_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Progressed
        );
        assert!(matches!(
            read_operation(&db, scope, operation_id).await.progress(),
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                VectorBuildStage::AdoptLegacy(LegacyVectorValidationProgress {
                    lane: LegacyVectorValidationLane::Layer0,
                    ..
                })
            ))
        ));
        db.close().await.expect("hot checkpoint closes");

        let db = Db::builder(database, store.clone())
            .build()
            .await
            .expect("hot checkpoint reopens");
        assert_eq!(
            drive_one(
                &db,
                &driver,
                operation_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Progressed
        );
        assert!(matches!(
            read_operation(&db, scope, operation_id).await.progress(),
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                VectorBuildStage::ValidateAdoptedDirectory(_)
            ))
        ));
        db.close().await.expect("layer-zero checkpoint closes");

        let db = Db::builder(database, store.clone())
            .build()
            .await
            .expect("directory checkpoint reopens");
        assert_eq!(
            drive_one(
                &db,
                &driver,
                operation_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Progressed
        );
        assert!(matches!(
            read_operation(&db, scope, operation_id).await.progress(),
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                VectorBuildStage::Activate(_)
            ))
        ));
        db.close().await.expect("directory checkpoint closes");

        let db = Db::builder(database, store)
            .build()
            .await
            .expect("activation checkpoint reopens");
        assert_eq!(
            drive_one(
                &db,
                &driver,
                operation_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Completed
        );
        assert!(matches!(
            read_index(&db, scope, &definition).await.state(),
            IndexStateV2::Active { .. }
        ));
        assert!(db
            .get(legacy_key)
            .await
            .expect("legacy catalog reads")
            .is_none());
        assert!(matches!(
            crate::index_lifecycle::repository::load_legacy_vector_physical_reservation(
                &db,
                physical_index_id,
            )
            .await
            .expect("active reservation reads"),
            Some(LegacyVectorPhysicalReservation::AdoptedActive { .. })
        ));
        db.close().await.expect("active adoption closes");
    }

    /// Plans `entity_id` into a fresh tenant partition of the build `operation`,
    /// reading the watermark through `transaction` and rows through a planning
    /// snapshot opened now.
    async fn plan_new_partition(
        db: &Db,
        transaction: &DbTransaction,
        operation: &IndexOperationRecord,
        record: &IndexRecordV2,
        definition: &ValidatedVectorIndexDefinition,
        tenant: i64,
    ) -> Result<EntityPlanOutcome> {
        let planning = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let document = vector_document(definition, &properties([1.0, 2.0, 3.0], Some(tenant)))
            .unwrap()
            .unwrap();
        plan_and_apply::<vector::distance::Euclidean>(
            &planning,
            &VectorWriteRecorder::new(),
            transaction,
            &VectorPlanTarget::build(DataScope::LegacyUnscoped, operation, record)?,
            definition,
            Arc::new(SimHasherRegistry::default()),
            crate::batch_reads::BatchReads::Single,
            IndexEntityId::new(1),
            &[],
            Some(&document),
            &VectorBatchAccounting::new(
                OperationCounters::default(),
                SearchIndexBackfillLimits::default().batch(),
            ),
            &mut driver().build_cache.checkout_fresh().await,
        )
        .await
    }

    /// A physical ID the step transaction sees as free but another index in
    /// the scope allocated, with its namespace, before planning opened is a
    /// retryable conflict; a namespace the transaction sees too means the
    /// watermark trails it, which fails closed.
    #[tokio::test]
    async fn a_physical_id_allocated_before_planning_conflicts_and_a_stale_watermark_fails_closed()
    {
        let db = test_db("vector-driver-allocation-race").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(Some("account_id"));
        let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
            unreachable!("fixture definition is vector");
        };
        put_source(&db, scope, 1, &properties([1.0, 2.0, 3.0], Some(7))).await;
        let (operation_id, _, _) = create_build(&db, scope, &definition, 1).await;
        let record = read_index(&db, scope, &definition).await;
        let operation = read_operation(&db, scope, operation_id).await;
        let other = ValidatedDynamicIndexDefinition::Vector(
            ValidatedVectorIndexDefinition::try_from_runtime(
                &VectorIndexDefinition::new_node(
                    "Picture",
                    "embedding",
                    3,
                    VectorDistanceMetric::Euclidean,
                )
                .unwrap()
                .with_tenant_property("account_id")
                .unwrap(),
            )
            .unwrap(),
        );
        let (other_id, _, _) = create_build(&db, scope, &other, 1).await;
        let other_record = read_index(&db, scope, &other).await;
        let other_namespace = |physical_index_id| {
            let handle = ValidatedVectorBuildGenerationHandle::try_from_building::<
                vector::distance::Euclidean,
            >(scope, &other_record, other_id, physical_index_id)
            .unwrap();
            let ValidatedDynamicIndexDefinition::Vector(other_vector) = &other else {
                unreachable!("fixture definition is vector");
            };
            (
                VectorIndex::<vector::distance::Euclidean>::from_generation(handle.generation()),
                VectorIndexConfig::from_v2_definition(
                    other_vector,
                    handle.generation().physical_name(),
                ),
            )
        };

        // The step transaction reads the watermark, then the other build
        // allocates the same ID and creates its namespace.
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let peeked = peek_vector_physical_id(&transaction).await.unwrap();
        let concurrent = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let allocated = crate::index_lifecycle::repository::stage_vector_partition_mapping(
            &concurrent,
            scope,
            other_record.index_id(),
            other_record.state().generation(),
            VectorPhysicalLayout::Partitioned,
            &VectorTenantPartition::try_from_partition(
                vector_document(vector_definition, &properties([0.0, 0.0, 1.0], Some(8)))
                    .unwrap()
                    .unwrap()
                    .partition()
                    .clone(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(allocated, peeked);
        let (index, config) = other_namespace(allocated);
        index.create(&concurrent, config).await.unwrap();
        concurrent.commit().await.unwrap();
        assert!(matches!(
            plan_new_partition(&db, &transaction, &operation, &record, vector_definition, 7).await,
            Err(error) if error.is_transaction_conflict()
        ));
        drop(transaction);

        // A later transaction reads the advanced watermark and plans.
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            plan_new_partition(&db, &transaction, &operation, &record, vector_definition, 7).await,
            Ok(EntityPlanOutcome::Admitted { .. })
        ));
        drop(transaction);

        // A namespace at the next ID that the watermark never covered.
        let stale = peek_vector_physical_id(&db).await.unwrap();
        let (index, config) = other_namespace(stale);
        let create = db.begin(IsolationLevel::Snapshot).await.unwrap();
        index.create(&create, config).await.unwrap();
        create.commit().await.unwrap();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            plan_new_partition(&db, &transaction, &operation, &record, vector_definition, 7).await,
            Err(HelixDbError::IndexCatalogCorruption(reason)) if reason.contains("watermark")
        ));
        drop(transaction);
        db.close().await.expect("vector test database closes");
    }

    mod build_cache;
}
