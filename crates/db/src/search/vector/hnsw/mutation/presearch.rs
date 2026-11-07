//! Parallel distance pre-search for queued vector publication.
//!
//! Inserts plan one at a time in queue order, because each sees the graph the
//! last one left. Most of an insert is scoring beam candidates, though, and a
//! distance depends only on its two vectors. So while the planner works through
//! a batch, workers run each upcoming insert's descent against the graph as the
//! batch began and keep every distance in a [`DistanceMemo`]. The planner then
//! walks the real graph and scores only what the memo lacks.
//!
//! # Soundness
//!
//! A memo entry is `distance(query, item(node))` for one entity, and the planner
//! uses it only where it would compute that same value:
//!
//! - It is keyed by entity, layer, and node, so a hit also proves the item
//!   resolved at that layer, exactly as the planner's own lookup would.
//! - Nodes the batch changes are never recorded and never served. Only a
//!   batch's own entities change items, because an upsert may reuse a node ID
//!   for a new vector; other nodes' rows change, but those are links.
//!
//! The rest of the insert, including which nodes the beam visits, comes from
//! the planner's own view, so a worker that read old links costs only hits. The
//! stored graph is identical with and without workers, and an unreached, failed
//! or panicked job only means the planner scores for itself.
//!
//! # Memory
//!
//! Workers read a [`SharedGraph`] that is immutable during an attempt, so they
//! take no locks and the planner's cache keeps its exact semantics. Each worker
//! holds what it reads beyond that in a private overlay, folded in between
//! attempts. The graph is charged to the session's byte budget and evicts by
//! whole generations, so reads never write recency.

use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::{Condvar, Mutex};
use slatedb::DbTransaction;
use tokio::sync::Notify;

use crate::encoding::NodeId;
use crate::error::HelixDbError;
use crate::search::vector::distance::{ActiveVectorSemantics, Distance};
use crate::search::vector::hnsw::index::VectorIndex;
use crate::search::vector::item::Item;
use crate::search::vector::unaligned_vector::UnalignedVector;
use crate::search::vector::{
    MeasuredVectorTransaction, ValidatedMetricVector, VectorDimension, VectorIndexMetadata,
    VectorIndexState,
};

use super::{
    MutationDegreeLimits, MutationOpCache, MutationScratch, NeighborRowId, NeighborRowValue,
    VectorBuildSession, VECTOR_BUILD_SESSION_ITEM_OVERHEAD_BYTES,
    VECTOR_BUILD_SESSION_NEIGHBOR_OVERHEAD_BYTES,
};

/// Workers that keep the planner supplied.
///
/// A worker scores about as fast as the planner links, so two leave it waiting
/// on nothing, and more were measured to add no throughput.
const PRESEARCH_MAX_WORKERS: usize = 2;
/// Jobs each worker may run ahead of the planner, bounding the memos alive and
/// the work lost when the batch budget stops the planner early.
const PRESEARCH_WINDOW_PER_WORKER: usize = 6;
/// Beam distances a memo holds without growing, for the default `ef`.
const MEMO_INITIAL_CAPACITY: usize = 4_096;
/// Eighths of a session's budget one [`SharedGraph`] generation may charge.
const GENERATION_BUDGET_EIGHTHS: usize = 3;
/// Eighths of a session's budget that all overlays of one attempt may hold.
const OVERLAY_BUDGET_EIGHTHS: usize = 1;

/// Returns how many workers pre-search on this machine, none on a single core.
#[must_use]
pub(crate) fn default_presearch_workers() -> Option<NonZeroUsize> {
    let cores = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
    NonZeroUsize::new((cores / 2).min(PRESEARCH_MAX_WORKERS))
}

/// One insert to pre-search.
#[derive(Debug, Clone)]
pub(crate) struct PresearchJob {
    /// The inserted entity.
    pub(crate) node_id: NodeId,
    /// Its vector.
    pub(crate) vector: Vec<f32>,
    /// Its deterministic layer.
    pub(crate) layer: u16,
}

/// Distances from one entity's vector, by layer and node.
#[derive(Debug)]
pub(crate) struct DistanceMemo {
    node_id: NodeId,
    distances: foldhash::HashMap<(u16, NodeId), f32>,
}

impl DistanceMemo {
    /// Returns the entity whose insert this memo serves.
    pub(super) const fn node_id(&self) -> NodeId {
        self.node_id
    }
}

/// Lookups served from a memo, proving a test's planner replayed some.
#[cfg(test)]
pub(crate) static MEMOIZED_LOOKUPS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// What one beam does with the distances it scores.
#[derive(Debug, Default)]
pub(super) enum BeamDistances {
    /// Scores every neighbor.
    #[default]
    Compute,
    /// Scores every neighbor, keeping each distance a worker may share.
    Capture(DistanceCapture),
    /// Scores only what a worker's memo lacks.
    Replay(DistanceMemo),
}

/// The state of one worker's beams for one job.
#[derive(Debug)]
pub(super) struct DistanceCapture {
    memo: DistanceMemo,
    /// Nodes the batch changes, whose distances may not be shared.
    excluded: Arc<foldhash::HashSet<NodeId>>,
    /// Set once the planner no longer needs this job.
    cancelled: Arc<AtomicBool>,
}

impl BeamDistances {
    /// Returns the memoized distance from the query to `node_id` at `layer`.
    #[inline]
    pub(super) fn memoized(&self, layer: u16, node_id: NodeId) -> Option<f32> {
        let Self::Replay(memo) = self else {
            return None;
        };
        let distance = memo.distances.get(&(layer, node_id)).copied();
        #[cfg(test)]
        if distance.is_some() {
            MEMOIZED_LOOKUPS.fetch_add(1, Ordering::Relaxed);
        }
        distance
    }

    /// Keeps a distance this beam scored, if it is capturing and may share it.
    #[inline]
    pub(super) fn record(&mut self, layer: u16, node_id: NodeId, distance: f32) {
        let Self::Capture(capture) = self else {
            return;
        };
        if !capture.excluded.contains(&node_id) {
            capture.memo.distances.insert((layer, node_id), distance);
        }
    }

    /// Returns whether a capturing beam should stop early.
    #[inline]
    pub(super) fn cancelled(&self) -> bool {
        match self {
            Self::Capture(capture) => capture.cancelled.load(Ordering::Relaxed),
            Self::Compute | Self::Replay(_) => false,
        }
    }
}

/// Whether a [`SharedGraph`] entry survives the next rotation untouched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GraphAge {
    /// In the generation entries enter.
    Young,
    /// In the generation the next rotation drops.
    Old,
}

/// One decoded item a [`SharedGraph`] retains.
#[derive(Debug)]
pub(super) struct SharedItem<D: Distance> {
    pub(super) value: Arc<Item<'static, D>>,
    pub(super) payload_bytes: usize,
}

/// One generation of retained graph state.
#[derive(Debug)]
struct GraphGeneration<D: Distance> {
    items: foldhash::HashMap<(u16, NodeId), SharedItem<D>>,
    neighbors: foldhash::HashMap<NeighborRowId, NeighborRowValue>,
    charged_bytes: usize,
}

impl<D: Distance> Default for GraphGeneration<D> {
    fn default() -> Self {
        Self {
            items: foldhash::HashMap::default(),
            neighbors: foldhash::HashMap::default(),
            charged_bytes: 0,
        }
    }
}

/// Bytes charged for one retained neighbor row.
fn neighbor_charge(value: &NeighborRowValue) -> usize {
    let links = match value {
        NeighborRowValue::KnownAbsent => 0,
        NeighborRowValue::Present(neighbors) => neighbors.as_slice().len(),
    };
    links * size_of::<NodeId>() + VECTOR_BUILD_SESSION_NEIGHBOR_OVERHEAD_BYTES
}

impl<D: Distance> GraphGeneration<D> {
    fn forget_item(&mut self, key: (u16, NodeId)) {
        let Some(item) = self.items.remove(&key) else {
            return;
        };
        self.charged_bytes -= item.payload_bytes + VECTOR_BUILD_SESSION_ITEM_OVERHEAD_BYTES;
    }

    fn forget_neighbors(&mut self, row: NeighborRowId) {
        let Some(value) = self.neighbors.remove(&row) else {
            return;
        };
        self.charged_bytes -= neighbor_charge(&value);
    }

    fn keep_item(&mut self, key: (u16, NodeId), item: SharedItem<D>) {
        self.charged_bytes += item.payload_bytes + VECTOR_BUILD_SESSION_ITEM_OVERHEAD_BYTES;
        if let Some(previous) = self.items.insert(key, item) {
            self.charged_bytes -= previous.payload_bytes + VECTOR_BUILD_SESSION_ITEM_OVERHEAD_BYTES;
        }
    }

    fn keep_neighbors(&mut self, row: NeighborRowId, value: NeighborRowValue) {
        self.charged_bytes += neighbor_charge(&value);
        if let Some(previous) = self.neighbors.insert(row, value) {
            self.charged_bytes -= neighbor_charge(&previous);
        }
    }
}

/// Decoded graph state workers read without locks.
///
/// Immutable while an attempt runs. Entries hold the state committed at the
/// session's checkpoint, so everything an attempt changes is forgotten as it
/// settles, and the graph lives and dies with its session's reuse.
#[derive(Debug)]
pub(in crate::search::vector) struct SharedGraph<D: Distance> {
    young: GraphGeneration<D>,
    old: GraphGeneration<D>,
    /// Highest layer retained, bounding what forgetting one node visits.
    max_layer: u16,
}

impl<D: Distance> Default for SharedGraph<D> {
    fn default() -> Self {
        Self {
            young: GraphGeneration::default(),
            old: GraphGeneration::default(),
            max_layer: 0,
        }
    }
}

impl<D: Distance> SharedGraph<D> {
    /// Returns one retained item and its generation.
    pub(super) fn item(&self, layer: u16, node_id: NodeId) -> Option<(&SharedItem<D>, GraphAge)> {
        let key = (layer, node_id);
        self.young
            .items
            .get(&key)
            .map(|item| (item, GraphAge::Young))
            .or_else(|| self.old.items.get(&key).map(|item| (item, GraphAge::Old)))
    }

    /// Returns one retained neighbor row and its generation.
    pub(super) fn neighbors(&self, row: NeighborRowId) -> Option<(&NeighborRowValue, GraphAge)> {
        self.young
            .neighbors
            .get(&row)
            .map(|value| (value, GraphAge::Young))
            .or_else(|| {
                self.old
                    .neighbors
                    .get(&row)
                    .map(|value| (value, GraphAge::Old))
            })
    }

    /// Returns the bytes charged to the session budget.
    pub(super) const fn charged_bytes(&self) -> usize {
        self.young.charged_bytes + self.old.charged_bytes
    }

    /// Drops every state of `nodes`, and `rows`, in both generations.
    pub(super) fn forget(
        &mut self,
        nodes: &foldhash::HashSet<NodeId>,
        rows: &BTreeSet<NeighborRowId>,
    ) {
        for generation in [&mut self.young, &mut self.old] {
            for &node_id in nodes {
                for layer in 0..=self.max_layer {
                    generation.forget_item((layer, node_id));
                    generation.forget_neighbors(MutationOpCache::<D>::node_row_id(layer, node_id));
                }
            }
            for &row in rows {
                generation.forget_neighbors(row);
            }
        }
    }

    /// Folds what `overlays` read into the young generation, except what the
    /// attempt changed.
    ///
    /// A generation that would pass `generation_budget` first ages, returning
    /// the oldest entries for the caller to drop off the executor.
    pub(super) fn absorb(
        &mut self,
        overlays: &[MutationOpCache<D>],
        changed: &foldhash::HashSet<NodeId>,
        rows: &BTreeSet<NeighborRowId>,
        generation_budget: usize,
    ) -> Option<impl Send + use<D>> {
        let incoming = overlays
            .iter()
            .map(|overlay| overlay.retained_payload_bytes)
            .sum::<usize>();
        let retired = (self.young.charged_bytes + incoming > generation_budget)
            .then(|| core::mem::replace(&mut self.old, core::mem::take(&mut self.young)));
        for overlay in overlays {
            for (&(layer, node_id), cached) in overlay.items.iter() {
                let Some(value) = cached.value.clone() else {
                    continue;
                };
                if changed.contains(&node_id) {
                    continue;
                }
                self.max_layer = self.max_layer.max(layer);
                self.young.keep_item(
                    (layer, node_id),
                    SharedItem {
                        value,
                        payload_bytes: cached.payload_bytes,
                    },
                );
            }
            for (&row, cached) in overlay.neighbor_rows.iter() {
                let (layer, entity) = row.storage_parts();
                if changed.contains(&entity) || rows.contains(&row) {
                    continue;
                }
                self.max_layer = self.max_layer.max(layer);
                self.young.keep_neighbors(row, cached.current().clone());
            }
        }
        retired
    }
}

/// Bytes one generation may charge under a session's budget.
pub(super) const fn generation_budget(max_payload_bytes: usize) -> usize {
    max_payload_bytes / 8 * GENERATION_BUDGET_EIGHTHS
}

/// Bytes a [`SharedGraph`] may charge at all under a session's budget.
pub(super) const fn shared_graph_budget(max_payload_bytes: usize) -> usize {
    generation_budget(max_payload_bytes) * 2
}

/// Bytes all overlays of one attempt may hold under a session's budget.
///
/// Transient and not charged: they exist only while the planner runs, and
/// settling folds them into the charged graph.
pub(super) const fn overlay_budget(max_payload_bytes: usize) -> usize {
    max_payload_bytes / 8 * OVERLAY_BUDGET_EIGHTHS
}

/// How a cache sees the attempt's [`SharedGraph`].
#[derive(Debug, Default)]
pub(super) enum SharedGraphAccess<D: Distance> {
    /// The cache stands alone.
    #[default]
    None,
    /// The planner, which reads items no batch entity holds. Its neighbor rows
    /// must be current, so it never reads the graph's.
    Items(Arc<SharedGraph<D>>),
    /// A worker, which reads the graph as the batch began.
    ItemsAndNeighbors(Arc<SharedGraph<D>>),
}

/// Where one job stands.
#[derive(Debug)]
enum JobSlot {
    /// Not yet claimed.
    Waiting,
    /// A worker is running it.
    Running,
    /// A worker finished it, with a memo unless the job failed.
    Finished(Option<DistanceMemo>),
    /// The planner reached it first, or took its result.
    Planned,
}

/// Progress of one attempt's jobs.
#[derive(Debug)]
struct RunProgress {
    slots: Vec<JobSlot>,
    /// Next job a worker claims.
    next: usize,
    /// Jobs the planner has passed.
    planned: usize,
}

/// State shared by the planner and one attempt's workers.
#[derive(Debug)]
struct RunState {
    progress: Mutex<RunProgress>,
    /// Wakes workers waiting at the window's edge.
    planner_advanced: Condvar,
    /// Wakes the planner waiting on a running job.
    job_finished: Notify,
    cancelled: Arc<AtomicBool>,
    window: usize,
}

impl RunState {
    /// Claims the next job, waiting while it is past the window. `None` once
    /// nothing is left or the attempt is over.
    fn claim(&self) -> Option<usize> {
        let mut progress = self.progress.lock();
        loop {
            if self.cancelled.load(Ordering::Relaxed) || progress.next >= progress.slots.len() {
                return None;
            }
            if progress.next >= progress.planned + self.window {
                self.planner_advanced.wait(&mut progress);
                continue;
            }
            let job = progress.next;
            progress.next += 1;
            if matches!(progress.slots[job], JobSlot::Waiting) {
                progress.slots[job] = JobSlot::Running;
                return Some(job);
            }
        }
    }

    fn finish(&self, job: usize, memo: Option<DistanceMemo>) {
        self.progress.lock().slots[job] = JobSlot::Finished(memo);
        self.job_finished.notify_waiters();
    }

    fn cancel(&self) {
        // Holding the lock orders this with a worker about to wait.
        let _progress = self.progress.lock();
        self.cancelled.store(true, Ordering::Relaxed);
        self.planner_advanced.notify_all();
    }
}

#[cfg(test)]
tokio::task_local! {
    /// Makes the planner wait for every job in scope, so each insert replays a
    /// complete memo, however a run happens to interleave.
    pub(crate) static AWAIT_WORKERS: ();
}

/// Whether the planner may score a job no worker has started.
#[cfg(not(test))]
const fn planner_may_overtake() -> bool {
    true
}

/// Whether the planner may score a job no worker has started: outside an
/// [`AWAIT_WORKERS`] scope.
#[cfg(test)]
fn planner_may_overtake() -> bool {
    AWAIT_WORKERS.try_with(|()| ()).is_err()
}

/// Reports a job as failed if its worker unwinds, so the planner never waits
/// on a job nobody is running.
struct JobGuard<'run> {
    run: &'run RunState,
    job: usize,
}

impl JobGuard<'_> {
    fn finish(self, memo: Option<DistanceMemo>) {
        self.run.finish(self.job, memo);
        core::mem::forget(self);
    }
}

impl Drop for JobGuard<'_> {
    fn drop(&mut self) {
        self.run.finish(self.job, None);
    }
}

/// Workers pre-searching one attempt's jobs beside the planner.
///
/// Dropping a run stops its workers, which own everything they use, so a
/// cancelled publication leaks nothing. [`Self::finish`] also folds their reads
/// into the session.
pub(crate) struct PresearchRun<D: Distance> {
    run: Arc<RunState>,
    workers: Vec<tokio::task::JoinHandle<MutationOpCache<D>>>,
    changed: Arc<foldhash::HashSet<NodeId>>,
}

impl<D: Distance> PresearchRun<D> {
    /// Returns the memo of the effect at `job`, waiting if a worker is running
    /// it.
    ///
    /// Jobs must be taken in increasing order. A job no worker has started is
    /// left for the planner to score, which beats waiting behind other jobs.
    pub(crate) async fn take(&self, job: usize) -> Option<DistanceMemo> {
        {
            let mut progress = self.run.progress.lock();
            progress.planned = job;
            self.run.planner_advanced.notify_all();
            if planner_may_overtake() && matches!(progress.slots[job], JobSlot::Waiting) {
                progress.slots[job] = JobSlot::Planned;
                return None;
            }
        }
        loop {
            let finished = self.run.job_finished.notified();
            tokio::pin!(finished);
            finished.as_mut().enable();
            {
                let mut progress = self.run.progress.lock();
                if !matches!(progress.slots[job], JobSlot::Waiting | JobSlot::Running) {
                    let JobSlot::Finished(memo) =
                        core::mem::replace(&mut progress.slots[job], JobSlot::Planned)
                    else {
                        return None;
                    };
                    return memo;
                }
            }
            finished.await;
        }
    }

    /// Stops the workers and settles the session's graph: everything the
    /// attempt changed is forgotten, and what workers read is kept.
    ///
    /// Call once the planner is done, before offering the session for
    /// retention. A panicked worker costs its overlay and the graph.
    pub(crate) async fn finish(mut self, session: &mut VectorBuildSession<D>) {
        self.run.cancel();
        let mut overlays = Vec::with_capacity(self.workers.len());
        let mut panicked = false;
        for worker in core::mem::take(&mut self.workers) {
            match worker.await {
                Ok(overlay) => overlays.push(overlay),
                Err(error) => {
                    panicked |= error.is_panic();
                    tracing::error!(%error, "vector pre-search worker stopped");
                }
            }
        }
        session.settle_presearch(&overlays, &self.changed, !panicked);
    }
}

impl<D: Distance> Drop for PresearchRun<D> {
    fn drop(&mut self) {
        self.run.cancel();
    }
}

impl<D: Distance + 'static> VectorIndex<D> {
    /// Starts workers pre-searching `jobs` against `snapshot`, or returns
    /// `None` when there is no populated graph to search: an empty graph, or
    /// one whose failure the planner reports where it can act on it.
    ///
    /// `changed` must name every node the planner may change while the run is
    /// alive, including jobs it never reaches. `snapshot` must see exactly the
    /// session's checkpoint, and must not be the transaction planning writes
    /// through.
    pub(crate) async fn start_presearch(
        self,
        snapshot: DbTransaction,
        jobs: Vec<Option<PresearchJob>>,
        changed: foldhash::HashSet<NodeId>,
        workers: NonZeroUsize,
        session: &mut VectorBuildSession<D>,
    ) -> Result<Option<PresearchRun<D>>, HelixDbError> {
        let metadata = {
            let txn = MeasuredVectorTransaction::new(&snapshot);
            self.get_metadata(&txn).await?
        };
        let Some(metadata) = metadata else {
            return Ok(None);
        };
        if !matches!(
            metadata.validated_state()?,
            VectorIndexState::Populated { .. }
        ) {
            return Ok(None);
        }
        let semantics = ActiveVectorSemantics::for_distance::<D>().ok_or_else(|| {
            HelixDbError::Config(format!(
                "vector distance '{}' has no stable durable semantic identity",
                D::name()
            ))
        })?;
        let degree_limits = MutationDegreeLimits::try_from_metadata(&metadata)?;
        let identity = self.build_session_identity()?.clone();
        let changed = Arc::new(changed);
        let (graph, overlay_budget) = session.begin_presearch(identity, &changed);
        let overlay_budget = overlay_budget / workers.get();
        let run = Arc::new(RunState {
            progress: Mutex::new(RunProgress {
                slots: jobs
                    .iter()
                    .map(|job| job.as_ref().map_or(JobSlot::Planned, |_| JobSlot::Waiting))
                    .collect(),
                next: 0,
                planned: 0,
            }),
            planner_advanced: Condvar::new(),
            job_finished: Notify::new(),
            cancelled: Arc::new(AtomicBool::new(false)),
            window: workers.get() * PRESEARCH_WINDOW_PER_WORKER,
        });
        let context = Arc::new(WorkerContext {
            index: self,
            snapshot,
            jobs,
            changed: Arc::clone(&changed),
            metadata,
            semantics,
            degree_limits,
            graph,
            overlay_budget,
            run: Arc::clone(&run),
        });
        let runtime = tokio::runtime::Handle::current();
        let workers = (0..workers.get())
            .map(|_| {
                let (context, runtime) = (Arc::clone(&context), runtime.clone());
                tokio::task::spawn_blocking(move || runtime.block_on(context.run_worker()))
            })
            .collect();
        Ok(Some(PresearchRun {
            run,
            workers,
            changed,
        }))
    }
}

/// Everything one attempt's workers share.
struct WorkerContext<D: Distance> {
    index: VectorIndex<D>,
    snapshot: DbTransaction,
    jobs: Vec<Option<PresearchJob>>,
    changed: Arc<foldhash::HashSet<NodeId>>,
    metadata: VectorIndexMetadata,
    semantics: ActiveVectorSemantics,
    degree_limits: MutationDegreeLimits,
    graph: Arc<SharedGraph<D>>,
    overlay_budget: usize,
    run: Arc<RunState>,
}

impl<D: Distance> WorkerContext<D> {
    /// Runs jobs until none is left, returning what this worker read.
    ///
    /// A failed job is left for the planner, whose own read reports the error
    /// on the path that can act on it.
    async fn run_worker(self: Arc<Self>) -> MutationOpCache<D> {
        let txn = MeasuredVectorTransaction::new(&self.snapshot);
        let mut overlay =
            MutationOpCache::<D>::shared_overlay(self.degree_limits, Arc::clone(&self.graph));
        let mut scratch = MutationScratch::default();
        while let Some(job) = self.run.claim() {
            let guard = JobGuard {
                run: &self.run,
                job,
            };
            let search = self.jobs[job]
                .as_ref()
                .expect("only a position with a job is ever claimed");
            let memo = self
                .presearch(&txn, search, &mut overlay, &mut scratch)
                .await
                .ok();
            guard.finish(memo);
            overlay.bound_payload(self.overlay_budget);
        }
        overlay.release_shared_graph();
        overlay
    }

    /// Runs the read-only descent of one insert, keeping its beam distances.
    async fn presearch(
        &self,
        txn: &MeasuredVectorTransaction<'_>,
        job: &PresearchJob,
        overlay: &mut MutationOpCache<D>,
        scratch: &mut MutationScratch<D>,
    ) -> Result<DistanceMemo, HelixDbError> {
        let dimension = VectorDimension::try_new(self.metadata.config.dimension)
            .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?;
        let vector = ValidatedMetricVector::try_new(
            UnalignedVector::<D::VectorCodec>::from_slice(&job.vector),
            self.semantics.distance_metric(),
            dimension,
        )
        .map_err(HelixDbError::from)?;
        let item = Item::<D> {
            header: D::new_header(vector.values()),
            vector: std::borrow::Cow::Borrowed(vector.values()),
        };
        let VectorIndexState::Populated {
            entry_point,
            max_layer,
        } = self.metadata.validated_state()?
        else {
            return Err(HelixDbError::InvariantViolation(
                "vector pre-search started on an unpopulated graph".to_string(),
            ));
        };
        scratch.beam_distances = BeamDistances::Capture(DistanceCapture {
            memo: DistanceMemo {
                node_id: job.node_id,
                distances: foldhash::HashMap::with_capacity_and_hasher(
                    MEMO_INITIAL_CAPACITY,
                    foldhash::fast::RandomState::default(),
                ),
            },
            excluded: Arc::clone(&self.changed),
            cancelled: Arc::clone(&self.run.cancelled),
        });
        let mut current = entry_point;
        for layer in (job.layer + 1..=max_layer).rev() {
            current = self
                .index
                .search_layer_greedy_for_mutation(
                    txn,
                    &item,
                    current,
                    layer,
                    job.node_id,
                    overlay,
                    scratch,
                )
                .await?;
        }
        for layer in (0..=max_layer.min(job.layer)).rev() {
            let ef = self
                .degree_limits
                .search_width(layer, self.metadata.config.ef_construction);
            let candidates = self
                .index
                .search_layer_beam(
                    txn,
                    &item,
                    current,
                    layer,
                    ef,
                    job.node_id,
                    overlay,
                    scratch,
                )
                .await?;
            if let Some(candidate) = candidates.first() {
                current = candidate.node_id;
            }
        }
        let BeamDistances::Capture(capture) = core::mem::take(&mut scratch.beam_distances) else {
            unreachable!("a pre-search beam keeps capturing");
        };
        Ok(capture.memo)
    }
}

#[cfg(test)]
#[path = "presearch_tests.rs"]
mod tests;
