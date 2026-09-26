//! Exact-membership vector search over graph-traversal result IDs.
//!
//! Small candidate sets use a locality-sorted exact scan. Larger sets combine
//! deterministic exact samples, the generation-complete SimHash directory, and
//! the existing upper-layer HNSW route before a bounded ACORN-style layer-zero
//! walk. SimHash affects seed priority only: the exact bitmap remains the sole
//! admission authority for vector fetches, scoring, and returned entities.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
#[cfg(any(test, feature = "production-coverage"))]
use std::future::Future;
#[cfg(feature = "production-scale")]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(any(test, feature = "production-coverage"))]
use std::sync::Arc;
use std::sync::OnceLock;

use futures::future::try_join_all;
#[cfg(any(test, feature = "production-coverage"))]
use parking_lot::Mutex;
use roaring::RoaringTreemap;
use slatedb::DbReadOps;

use crate::encoding::NodeId;
use crate::error::HelixDbError;

use super::index::VectorIndex;
use super::model::Candidate;
use crate::search::vector::configuration::VectorIndexState;
use crate::search::vector::distance::{ActiveVectorSemantics, Distance};
use crate::search::vector::item::Item;
use crate::search::vector::storage::{CanonicalVectorRowKey, SimHashDirectoryEntry, VectorRows};
use crate::search::vector::unaligned_vector::UnalignedVector;
use crate::search::vector::{
    decode_item_borrowed, ResultCount, SearchParams, SearchResult, ValidatedMetricVector,
    VectorDimension, VectorParameterError,
};

const MAX_RESTRICTED_CANDIDATES: u64 = 1_000_000;
const EXACT_CARDINALITY_THRESHOLD: u64 = 256;
const EXACT_VECTOR_BYTES_THRESHOLD: u64 = 4 * 1024 * 1024;
const FETCH_BATCH_SIZE: usize = 256;
const DIRECTORY_PREFIX_BITS: u32 = 16;
const DIRECTORY_MAX_PROBES: usize = 64;
const DIRECTORY_MAX_ROWS: usize = 65_536;
const DIRECTORY_MAX_DECODED_BYTES: usize = 4 * 1024 * 1024;
const DIRECTORY_MAX_CONCURRENT_SCANS: usize = 8;
const FRONTIER_BATCH_SIZE: usize = 16;
const BRIDGE_BATCH_SIZE: usize = 256;
const FILTERED_BEAM_PERCENT: usize = 150;
const FILTERED_BEAM_PERCENT_DENOMINATOR: usize = 100;
const FILTERED_SAMPLED_SEEDS: usize = 64;
const FILTERED_DIRECTORY_SEEDS: usize = 256;
const MAX_RESTRICTED_RESULT_COUNT: usize = 800;
const FILTERED_VECTOR_PAYLOAD_LIMIT: usize = MAX_RESTRICTED_RESULT_COUNT;

#[cfg(feature = "production-scale")]
static FILTERED_BEAM_PERCENT_OVERRIDE: AtomicUsize = AtomicUsize::new(0);

/// Closed benchmark-only choices for comparing the deployed restricted beam.
#[cfg(feature = "production-scale")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RestrictedBeamScale {
    Base,
    OneAndHalf,
    Double,
}

#[cfg(feature = "production-scale")]
impl RestrictedBeamScale {
    pub(crate) const fn percent(self) -> usize {
        match self {
            Self::Base => 100,
            Self::OneAndHalf => 150,
            Self::Double => 200,
        }
    }

    pub(crate) const fn width(self, base_ef: usize) -> usize {
        base_ef.saturating_mul(self.percent()) / FILTERED_BEAM_PERCENT_DENOMINATOR
    }
}

/// Exclusive process-local override used only by production-scale benchmarks.
#[cfg(feature = "production-scale")]
pub(crate) struct RestrictedBeamOverrideGuard {
    scale: RestrictedBeamScale,
}

#[cfg(feature = "production-scale")]
impl RestrictedBeamOverrideGuard {
    pub(crate) fn acquire(scale: RestrictedBeamScale) -> Self {
        FILTERED_BEAM_PERCENT_OVERRIDE
            .compare_exchange(0, scale.percent(), Ordering::AcqRel, Ordering::Acquire)
            .expect("only one restricted beam benchmark override may be active");
        Self { scale }
    }
}

#[cfg(feature = "production-scale")]
impl Drop for RestrictedBeamOverrideGuard {
    fn drop(&mut self) {
        FILTERED_BEAM_PERCENT_OVERRIDE
            .compare_exchange(self.scale.percent(), 0, Ordering::AcqRel, Ordering::Acquire)
            .expect("restricted beam benchmark guard owns the active override");
    }
}

fn effective_filtered_beam_percent() -> usize {
    #[cfg(feature = "production-scale")]
    {
        let percent = FILTERED_BEAM_PERCENT_OVERRIDE.load(Ordering::Acquire);
        if percent > 0 {
            return percent;
        }
    }
    FILTERED_BEAM_PERCENT
}

/// Restricted-search execution selected after exact cardinality/byte admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RestrictedSearchStrategy {
    /// Locality-sorted exact vector scan.
    Exact,
    /// Bounded directory/sample-seeded filtered graph traversal.
    FilteredGraph,
}

/// Why one bounded filtered graph traversal stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RestrictedSearchTermination {
    /// No scored candidate or global routing anchor remained to expand.
    Exhausted,
    /// The full allowed beam proved that the remaining frontier was worse.
    BeamComplete,
    /// The total layer-zero routing-row budget was exhausted.
    RoutingBudget,
    /// The rejected-neighbor bridge-row budget was exhausted.
    BridgeBudget,
    /// The allowed vector-payload budget was exhausted.
    VectorBudget,
}

/// Stable counters used by correctness, I/O-budget, and performance gates.
#[derive(Debug, Clone, Default)]
pub(crate) struct RestrictedSearchStats {
    pub(crate) strategy: Option<RestrictedSearchStrategy>,
    pub(crate) termination: Option<RestrictedSearchTermination>,
    pub(crate) ef_filtered: usize,
    pub(crate) directory_scan_calls: usize,
    pub(crate) directory_rows: usize,
    pub(crate) directory_decoded_bytes: usize,
    pub(crate) directory_hits: usize,
    pub(crate) simhash_row_requests: usize,
    pub(crate) simhash_multi_get_calls: usize,
    pub(crate) companion_row_requests: usize,
    pub(crate) routing_rows: usize,
    pub(crate) bridge_rows: usize,
    pub(crate) bridge_frontier_pushes: usize,
    pub(crate) neighbor_multi_get_calls: usize,
    pub(crate) vector_payload_requests: usize,
    pub(crate) vector_multi_get_calls: usize,
    pub(crate) vector_bytes: usize,
    pub(crate) distance_computations: usize,
}

#[cfg(any(test, feature = "production-coverage"))]
tokio::task_local! {
    static RESTRICTED_SEARCH_OBSERVER: Arc<Mutex<Option<RestrictedSearchStats>>>;
}

/// Runs one future with task-local capture of its restricted-search counters.
#[cfg(any(test, feature = "production-coverage"))]
pub(crate) async fn observe_restricted_search<F>(
    future: F,
) -> (F::Output, Option<RestrictedSearchStats>)
where
    F: Future,
{
    let observation = Arc::new(Mutex::new(None));
    let output = RESTRICTED_SEARCH_OBSERVER
        .scope(Arc::clone(&observation), future)
        .await;
    let stats = observation.lock().take();
    (output, stats)
}

#[cfg(any(test, feature = "production-coverage"))]
fn record_restricted_search(stats: &RestrictedSearchStats) {
    let _ = RESTRICTED_SEARCH_OBSERVER.try_with(|observation| {
        *observation.lock() = Some(stats.clone());
    });
}

/// A non-zero restricted result count covered by the hard vector-payload budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RestrictedResultCount(ResultCount);

impl RestrictedResultCount {
    fn try_new(requested: usize, candidate_count: u64) -> Result<Self, VectorParameterError> {
        let candidate_count =
            usize::try_from(candidate_count).expect("bounded candidate count fits usize");
        let count = ResultCount::try_new(requested.min(candidate_count))?;
        if count.get() > MAX_RESTRICTED_RESULT_COUNT {
            return Err(VectorParameterError::AboveMaximum {
                parameter: "restricted vector search result count",
                maximum: MAX_RESTRICTED_RESULT_COUNT,
                actual: count.get(),
            });
        }
        Ok(Self(count))
    }

    const fn get(self) -> usize {
        self.0.get()
    }
}

#[derive(Debug, Clone, Copy)]
struct FilteredGraphBudgets {
    ef_filtered: usize,
    routing_rows: usize,
    bridge_rows: usize,
    vector_payloads: usize,
    sampled_seeds: usize,
    directory_seeds: usize,
}

impl FilteredGraphBudgets {
    fn with_beam_percent(
        params: &SearchParams,
        k: RestrictedResultCount,
        candidate_count: u64,
        beam_percent: usize,
    ) -> Self {
        assert!(beam_percent > 0, "filtered beam percent is nonzero");
        let candidate_count =
            usize::try_from(candidate_count).expect("bounded candidate count fits usize");
        let k = k.get();
        let scaled_ef =
            params.ef().saturating_mul(beam_percent) / FILTERED_BEAM_PERCENT_DENOMINATOR;
        let ef_filtered = scaled_ef.max(k.saturating_mul(4)).min(candidate_count);
        let sampled_seeds = FILTERED_SAMPLED_SEEDS.min(candidate_count);
        let directory_seeds = FILTERED_DIRECTORY_SEEDS.min(candidate_count);
        let vector_payloads = FILTERED_VECTOR_PAYLOAD_LIMIT.min(candidate_count);
        assert!(
            vector_payloads >= k,
            "validated restricted result count fits the vector-payload budget"
        );
        Self {
            ef_filtered,
            routing_rows: ef_filtered.saturating_mul(16),
            bridge_rows: ef_filtered.saturating_mul(8),
            vector_payloads,
            sampled_seeds,
            directory_seeds,
        }
    }
}

enum RestrictedExecutionPlan<'a> {
    Exact {
        candidates: &'a NonEmptyCandidateSet,
        k: RestrictedResultCount,
    },
    FilteredGraph {
        candidates: &'a NonEmptyCandidateSet,
        k: RestrictedResultCount,
        budgets: FilteredGraphBudgets,
    },
}

struct RestrictedQuery<'item, 'vector, D: Distance> {
    vector: &'vector [f32],
    item: &'item Item<'vector, D>,
    dimension: VectorDimension,
}

struct FilteredGraphPlan<'a> {
    state: VectorIndexState,
    k: RestrictedResultCount,
    budgets: FilteredGraphBudgets,
    allowed: &'a NonEmptyCandidateSet,
}

struct RestrictedScoringState<'a> {
    frontier: &'a mut BinaryHeap<Reverse<Candidate>>,
    top: &'a mut BinaryHeap<Candidate>,
    scored: &'a mut HashSet<NodeId>,
    beam_width: usize,
    stats: &'a mut RestrictedSearchStats,
}

struct RestrictedBridgeState {
    simhash_cache: HashMap<NodeId, Option<crate::search::vector::SimHash>>,
    queued: HashSet<NodeId>,
    frontier: BinaryHeap<Reverse<(u32, NodeId)>>,
}

/// A non-empty, exact traversal-membership bitmap.
#[derive(Debug, Clone)]
pub(crate) struct NonEmptyCandidateSet {
    ids: RoaringTreemap,
}

impl NonEmptyCandidateSet {
    fn len(&self) -> u64 {
        self.ids.len()
    }

    fn contains(&self, node_id: NodeId) -> bool {
        self.ids.contains(node_id)
    }

    fn iter(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.ids.iter()
    }

    /// Selects evenly spaced IDs without copying the complete candidate set.
    fn deterministic_sample_ids(&self, limit: usize) -> Vec<NodeId> {
        let sample_count = limit.min(self.len() as usize);
        if sample_count == self.len() as usize {
            return self.iter().collect();
        }
        if sample_count == 1 {
            return vec![self.ids.select(0).expect("non-empty bitmap has rank zero")];
        }
        let last_rank = self.len() - 1;
        (0..sample_count)
            .map(|sample| {
                let rank = u64::try_from(
                    (sample as u128) * u128::from(last_rank)
                        / (sample_count.saturating_sub(1) as u128),
                )
                .expect("sample rank is bounded by exact candidate cardinality");
                self.ids
                    .select(rank)
                    .expect("sample rank is inside exact candidate cardinality")
            })
            .collect()
    }
}

/// Closed traversal-candidate state; an empty bitmap cannot reach search planning.
#[derive(Debug, Clone)]
pub(crate) enum RestrictedVectorCandidates {
    /// The upstream traversal produced no unique entity IDs.
    Empty,
    /// The upstream traversal produced a bounded non-empty exact bitmap.
    NonEmpty(NonEmptyCandidateSet),
}

impl RestrictedVectorCandidates {
    /// Canonicalizes duplicate traversal outputs into one exact bounded bitmap.
    pub(crate) fn from_ids(ids: impl IntoIterator<Item = u64>) -> Result<Self, HelixDbError> {
        let mut bitmap = RoaringTreemap::new();
        for id in ids {
            bitmap.insert(id);
            if bitmap.len() > MAX_RESTRICTED_CANDIDATES {
                return Err(HelixDbError::Query(format!(
                    "restricted vector search accepts at most {MAX_RESTRICTED_CANDIDATES} unique candidates"
                )));
            }
        }
        if bitmap.is_empty() {
            Ok(Self::Empty)
        } else {
            Ok(Self::NonEmpty(NonEmptyCandidateSet { ids: bitmap }))
        }
    }

    /// Tests authoritative membership.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) fn contains(&self, node_id: NodeId) -> bool {
        match self {
            Self::Empty => false,
            Self::NonEmpty(candidates) => candidates.contains(node_id),
        }
    }

    #[cfg(any(test, feature = "production-coverage"))]
    fn iter(&self) -> impl Iterator<Item = NodeId> + '_ {
        match self {
            Self::Empty => None.into_iter().flatten(),
            Self::NonEmpty(candidates) => Some(candidates.iter()).into_iter().flatten(),
        }
    }
}

#[cfg(any(test, feature = "production-coverage"))]
fn restricted_execution_plan<'a>(
    candidates: &'a NonEmptyCandidateSet,
    dimension: usize,
    params: &SearchParams,
) -> RestrictedExecutionPlan<'a> {
    let k = RestrictedResultCount::try_new(params.k(), candidates.len())
        .expect("test restricted result count is bounded");
    restricted_execution_plan_with_beam_percent(
        candidates,
        dimension,
        params,
        k,
        FILTERED_BEAM_PERCENT,
    )
}

#[cfg(any(test, feature = "production-coverage"))]
fn restricted_execution_plan_with_beam_multiplier<'a>(
    candidates: &'a NonEmptyCandidateSet,
    dimension: usize,
    params: &SearchParams,
    beam_multiplier: usize,
) -> RestrictedExecutionPlan<'a> {
    let k = RestrictedResultCount::try_new(params.k(), candidates.len())
        .expect("test restricted result count is bounded");
    restricted_execution_plan_with_beam_percent(
        candidates,
        dimension,
        params,
        k,
        beam_multiplier.saturating_mul(FILTERED_BEAM_PERCENT_DENOMINATOR),
    )
}

fn restricted_execution_plan_with_beam_percent<'a>(
    candidates: &'a NonEmptyCandidateSet,
    dimension: usize,
    params: &SearchParams,
    k: RestrictedResultCount,
    beam_percent: usize,
) -> RestrictedExecutionPlan<'a> {
    let estimated_vector_bytes = candidates
        .len()
        .saturating_mul(dimension as u64)
        .saturating_mul(core::mem::size_of::<f32>() as u64);
    if candidates.len() <= EXACT_CARDINALITY_THRESHOLD
        && estimated_vector_bytes <= EXACT_VECTOR_BYTES_THRESHOLD
    {
        RestrictedExecutionPlan::Exact { candidates, k }
    } else {
        RestrictedExecutionPlan::FilteredGraph {
            candidates,
            k,
            budgets: FilteredGraphBudgets::with_beam_percent(
                params,
                k,
                candidates.len(),
                beam_percent,
            ),
        }
    }
}

fn directory_prefix_offsets() -> &'static [u16] {
    static OFFSETS: OnceLock<Vec<u16>> = OnceLock::new();
    OFFSETS.get_or_init(|| {
        let mut offsets = (u16::MIN..=u16::MAX).collect::<Vec<_>>();
        offsets.sort_unstable_by_key(|offset| (offset.count_ones(), *offset));
        offsets
    })
}

impl<D: Distance> VectorIndex<D> {
    /// Ranks only `allowed` entities, selecting exact or filter-aware execution.
    pub(crate) async fn search_restricted(
        &self,
        read: &(impl DbReadOps + Send + Sync),
        query: &[f32],
        params: &SearchParams,
        allowed: &RestrictedVectorCandidates,
    ) -> Result<Vec<SearchResult>, HelixDbError> {
        let (results, _stats) = self
            .search_restricted_observed(read, query, params, allowed)
            .await?;
        #[cfg(any(test, feature = "production-coverage"))]
        record_restricted_search(&_stats);
        Ok(results)
    }

    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) async fn search_restricted_with_stats(
        &self,
        read: &(impl DbReadOps + Send + Sync),
        query: &[f32],
        params: &SearchParams,
        allowed: &RestrictedVectorCandidates,
    ) -> Result<(Vec<SearchResult>, RestrictedSearchStats), HelixDbError> {
        self.search_restricted_observed(read, query, params, allowed)
            .await
    }

    async fn search_restricted_observed(
        &self,
        read: &(impl DbReadOps + Send + Sync),
        query: &[f32],
        params: &SearchParams,
        allowed: &RestrictedVectorCandidates,
    ) -> Result<(Vec<SearchResult>, RestrictedSearchStats), HelixDbError> {
        self.search_restricted_observed_with_beam_percent(
            read,
            query,
            params,
            allowed,
            effective_filtered_beam_percent(),
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn search_restricted_with_beam_multiplier(
        &self,
        read: &(impl DbReadOps + Send + Sync),
        query: &[f32],
        params: &SearchParams,
        allowed: &RestrictedVectorCandidates,
        beam_multiplier: usize,
    ) -> Result<(Vec<SearchResult>, RestrictedSearchStats), HelixDbError> {
        self.search_restricted_observed_with_beam_percent(
            read,
            query,
            params,
            allowed,
            beam_multiplier.saturating_mul(FILTERED_BEAM_PERCENT_DENOMINATOR),
        )
        .await
    }

    async fn search_restricted_observed_with_beam_percent(
        &self,
        read: &(impl DbReadOps + Send + Sync),
        query: &[f32],
        params: &SearchParams,
        allowed: &RestrictedVectorCandidates,
        beam_percent: usize,
    ) -> Result<(Vec<SearchResult>, RestrictedSearchStats), HelixDbError> {
        assert!(beam_percent > 0, "filtered beam percent is nonzero");
        let mut stats = RestrictedSearchStats::default();
        let RestrictedVectorCandidates::NonEmpty(allowed) = allowed else {
            return Ok((Vec::new(), stats));
        };
        let k = RestrictedResultCount::try_new(params.k(), allowed.len())
            .map_err(|error| HelixDbError::Query(error.to_string()))?;
        let metadata = self
            .get_metadata(read)
            .await?
            .ok_or_else(|| HelixDbError::IndexNotFound(self.name().to_string()))?;
        let dimension = VectorDimension::try_new(metadata.config.dimension)
            .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?;
        let semantics = ActiveVectorSemantics::for_distance::<D>().ok_or_else(|| {
            HelixDbError::Config(format!(
                "vector distance '{}' has no stable durable semantic identity",
                D::name()
            ))
        })?;
        let query_vector = ValidatedMetricVector::try_new(
            UnalignedVector::<D::VectorCodec>::from_slice(query),
            semantics.distance_metric(),
            dimension,
        )
        .map_err(HelixDbError::from)?;
        let state = metadata.validated_state()?;
        if state == VectorIndexState::Empty {
            return Ok((Vec::new(), stats));
        }

        let query_item = Item::<D> {
            header: D::new_header(query_vector.values()),
            vector: std::borrow::Cow::Borrowed(query_vector.values()),
        };

        match restricted_execution_plan_with_beam_percent(
            allowed,
            metadata.config.dimension,
            params,
            k,
            beam_percent,
        ) {
            RestrictedExecutionPlan::Exact { candidates, k } => {
                stats.strategy = Some(RestrictedSearchStrategy::Exact);
                let results = self
                    .restricted_exact_scan(read, &query_item, dimension, k, candidates, &mut stats)
                    .await?;
                Ok((results, stats))
            }
            RestrictedExecutionPlan::FilteredGraph {
                candidates,
                k,
                budgets,
            } => {
                stats.strategy = Some(RestrictedSearchStrategy::FilteredGraph);
                stats.ef_filtered = budgets.ef_filtered;
                let results = self
                    .restricted_filter_aware_search(
                        read,
                        RestrictedQuery {
                            vector: query,
                            item: &query_item,
                            dimension,
                        },
                        FilteredGraphPlan {
                            state,
                            k,
                            budgets,
                            allowed: candidates,
                        },
                        &mut stats,
                    )
                    .await?;
                Ok((results, stats))
            }
        }
    }

    async fn restricted_candidate_keys(
        &self,
        read: &(impl DbReadOps + Send + Sync),
        ids: &[NodeId],
        simhash_cache: &mut HashMap<NodeId, Option<crate::search::vector::SimHash>>,
        context: &'static str,
        stats: &mut RestrictedSearchStats,
    ) -> Result<Vec<(NodeId, CanonicalVectorRowKey)>, HelixDbError> {
        let (keys, reads) = self
            .resolve_canonical_vector_keys_batch_counted::<false>(read, ids, simhash_cache, context)
            .await?;
        stats.simhash_row_requests = stats.simhash_row_requests.saturating_add(reads.reads);
        stats.simhash_multi_get_calls = stats
            .simhash_multi_get_calls
            .saturating_add(reads.multi_get_calls);
        let mut missing = Vec::new();
        let keyed = ids
            .iter()
            .copied()
            .zip(keys)
            .filter_map(|(node_id, key)| match key {
                Some(key) => Some((node_id, key)),
                None => {
                    missing.push(node_id);
                    None
                }
            })
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            let existing = VectorRows::new(read, self.row_keyspace())
                .layer0_rows_exist(&missing)
                .await?;
            stats.companion_row_requests =
                stats.companion_row_requests.saturating_add(missing.len());
            stats.neighbor_multi_get_calls = stats.neighbor_multi_get_calls.saturating_add(1);
            if let Some(node_id) = missing
                .into_iter()
                .zip(existing)
                .find_map(|(node_id, exists)| exists.then_some(node_id))
            {
                return Err(self.missing_simhash_error(node_id, context));
            }
        }
        Ok(keyed)
    }

    async fn restricted_score_keys(
        &self,
        read: &(impl DbReadOps + Send + Sync),
        query: &Item<'_, D>,
        dimension: VectorDimension,
        mut keyed: Vec<(NodeId, CanonicalVectorRowKey)>,
        scoring: RestrictedScoringState<'_>,
    ) -> Result<(), HelixDbError> {
        keyed.retain(|(node_id, _)| !scoring.scored.contains(node_id));
        keyed.sort_by(|left, right| left.1.physical_order(&right.1));
        let keys = keyed.iter().map(|(_, key)| key.clone()).collect::<Vec<_>>();
        if keys.is_empty() {
            return Ok(());
        }
        let rows = VectorRows::new(read, self.row_keyspace())
            .canonical_vector_rows(&keys)
            .await?;
        scoring.stats.vector_payload_requests = scoring
            .stats
            .vector_payload_requests
            .saturating_add(keys.len());
        scoring.stats.vector_multi_get_calls =
            scoring.stats.vector_multi_get_calls.saturating_add(1);
        for ((node_id, _), row) in keyed.into_iter().zip(rows) {
            let Some(row) = row else {
                return Err(HelixDbError::InvariantViolation(format!(
                    "missing canonical vector payload for node {node_id} in index {}",
                    self.id()
                )));
            };
            scoring.stats.vector_bytes = scoring.stats.vector_bytes.saturating_add(row.len());
            let item = decode_item_borrowed::<D>(&row, dimension)?;
            scoring.stats.distance_computations =
                scoring.stats.distance_computations.saturating_add(1);
            let candidate = Candidate::try_new(node_id, D::distance(query, &item))?;
            scoring.scored.insert(node_id);
            scoring.frontier.push(Reverse(candidate));
            scoring.top.push(candidate);
            if scoring.top.len() > scoring.beam_width {
                scoring.top.pop();
            }
        }
        Ok(())
    }

    /// Adds unseen rejected graph nodes to the compact query-guided bridge frontier.
    ///
    /// Bridge routing never reads or scores vector payloads. A graph neighbor or
    /// entry point without its mandatory SimHash companion is index corruption,
    /// not an absent candidate that can be skipped.
    async fn restricted_enqueue_bridges(
        &self,
        read: &(impl DbReadOps + Send + Sync),
        query_hash: crate::search::vector::SimHash,
        node_ids: impl IntoIterator<Item = NodeId>,
        bridge_state: &mut RestrictedBridgeState,
        stats: &mut RestrictedSearchStats,
    ) -> Result<(), HelixDbError> {
        let node_ids = node_ids
            .into_iter()
            .filter(|node_id| bridge_state.queued.insert(*node_id))
            .collect::<Vec<_>>();
        if node_ids.is_empty() {
            return Ok(());
        }
        let reads = self
            .fill_simhash_cache_for_nodes_counted::<false>(
                read,
                &node_ids,
                &mut bridge_state.simhash_cache,
                "ranking traversal-scoped rejected bridge nodes",
            )
            .await?;
        stats.simhash_row_requests = stats.simhash_row_requests.saturating_add(reads.reads);
        stats.simhash_multi_get_calls = stats
            .simhash_multi_get_calls
            .saturating_add(reads.multi_get_calls);
        for node_id in node_ids {
            let Some(Some(simhash)) = bridge_state.simhash_cache.get(&node_id).copied() else {
                return Err(self.missing_simhash_error(
                    node_id,
                    "ranking traversal-scoped rejected bridge nodes",
                ));
            };
            bridge_state
                .frontier
                .push(Reverse((simhash.hamming_distance(&query_hash), node_id)));
            stats.bridge_frontier_pushes = stats.bridge_frontier_pushes.saturating_add(1);
        }
        Ok(())
    }

    async fn restricted_exact_scan(
        &self,
        read: &(impl DbReadOps + Send + Sync),
        query: &Item<'_, D>,
        dimension: VectorDimension,
        k: RestrictedResultCount,
        allowed: &NonEmptyCandidateSet,
        stats: &mut RestrictedSearchStats,
    ) -> Result<Vec<SearchResult>, HelixDbError> {
        let k = k.get();
        let mut top = BinaryHeap::<Candidate>::with_capacity(k.saturating_add(1));
        let mut unused_frontier = BinaryHeap::new();
        let mut scored = HashSet::with_capacity(allowed.len() as usize);
        let mut simhash_cache = HashMap::new();
        let mut batch = Vec::with_capacity(FETCH_BATCH_SIZE);

        for node_id in allowed.iter() {
            batch.push(node_id);
            if batch.len() < FETCH_BATCH_SIZE {
                continue;
            }
            let keyed = self
                .restricted_candidate_keys(
                    read,
                    &batch,
                    &mut simhash_cache,
                    "resolving traversal-scoped exact vector rows",
                    stats,
                )
                .await?;
            self.restricted_score_keys(
                read,
                query,
                dimension,
                keyed,
                RestrictedScoringState {
                    frontier: &mut unused_frontier,
                    top: &mut top,
                    scored: &mut scored,
                    beam_width: k,
                    stats,
                },
            )
            .await?;
            batch.clear();
        }
        if !batch.is_empty() {
            let keyed = self
                .restricted_candidate_keys(
                    read,
                    &batch,
                    &mut simhash_cache,
                    "resolving traversal-scoped exact vector rows",
                    stats,
                )
                .await?;
            self.restricted_score_keys(
                read,
                query,
                dimension,
                keyed,
                RestrictedScoringState {
                    frontier: &mut unused_frontier,
                    top: &mut top,
                    scored: &mut scored,
                    beam_width: k,
                    stats,
                },
            )
            .await?;
        }

        let mut results = top
            .into_iter()
            .map(|candidate| SearchResult::new(candidate.node_id, candidate.distance()))
            .collect::<Vec<_>>();
        results.sort_by(|left, right| {
            left.score()
                .cmp(&right.score())
                .then_with(|| left.entity_id().cmp(&right.entity_id()))
        });
        Ok(results)
    }

    async fn restricted_filter_aware_search(
        &self,
        read: &(impl DbReadOps + Send + Sync),
        query: RestrictedQuery<'_, '_, D>,
        plan: FilteredGraphPlan<'_>,
        stats: &mut RestrictedSearchStats,
    ) -> Result<Vec<SearchResult>, HelixDbError> {
        let FilteredGraphPlan {
            state,
            k,
            budgets,
            allowed,
        } = plan;
        let k = k.get();
        let VectorIndexState::Populated {
            entry_point,
            max_layer: _,
        } = state
        else {
            return Ok(Vec::new());
        };
        let simhash_cache = self.simhash_cache(query.dimension.get())?;
        let query_hash = simhash_cache
            .simhasher()
            .hash_from_slice(query.vector)
            .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?;
        let query_order =
            crate::search::vector::simhash::order_code_from_simhash_bits(query_hash.bits());
        let rows = VectorRows::new(read, self.row_keyspace());
        let mut directory_seeds = Vec::<(u32, NodeId, SimHashDirectoryEntry)>::new();

        if self.simhash_directory_enabled() {
            let query_prefix = (query_order >> (64 - DIRECTORY_PREFIX_BITS)) as u16;
            let prefixes = directory_prefix_offsets()
                .iter()
                .take(DIRECTORY_MAX_PROBES)
                .map(|offset| query_prefix ^ *offset)
                .collect::<Vec<_>>();
            let max_rows_per_scan = DIRECTORY_MAX_ROWS / DIRECTORY_MAX_PROBES;
            let max_bytes_per_scan = DIRECTORY_MAX_DECODED_BYTES / DIRECTORY_MAX_PROBES;
            let mut seen = HashSet::new();

            'directory: for prefix_batch in prefixes.chunks(DIRECTORY_MAX_CONCURRENT_SCANS) {
                let windows = try_join_all(prefix_batch.iter().map(|prefix| {
                    let min_order = (*prefix as u64) << (64 - DIRECTORY_PREFIX_BITS);
                    let max_order = min_order | (u64::MAX >> DIRECTORY_PREFIX_BITS);
                    rows.simhash_directory_window_measured(
                        min_order,
                        max_order,
                        max_rows_per_scan,
                        max_bytes_per_scan,
                    )
                }))
                .await?;
                stats.directory_scan_calls =
                    stats.directory_scan_calls.saturating_add(windows.len());
                stats.directory_rows = stats.directory_rows.saturating_add(
                    windows
                        .iter()
                        .map(|window| window.entries().len())
                        .sum::<usize>(),
                );
                stats.directory_decoded_bytes = stats.directory_decoded_bytes.saturating_add(
                    windows
                        .iter()
                        .map(crate::search::vector::storage::SimHashDirectoryWindow::decoded_bytes)
                        .sum::<usize>(),
                );
                for window in windows {
                    for entry in window.into_entries() {
                        let node_id = entry.node_id();
                        if allowed.contains(node_id) && seen.insert(node_id) {
                            stats.directory_hits = stats.directory_hits.saturating_add(1);
                            directory_seeds.push((
                                (entry.order_code() ^ query_order).count_ones(),
                                node_id,
                                entry,
                            ));
                        }
                    }
                    if directory_seeds.len() >= budgets.directory_seeds {
                        break 'directory;
                    }
                }
            }
        }
        directory_seeds.sort_unstable_by_key(|(hamming, node_id, _)| (*hamming, *node_id));
        directory_seeds.truncate(budgets.directory_seeds);

        let mut bridge_state = RestrictedBridgeState {
            simhash_cache: HashMap::new(),
            queued: HashSet::new(),
            frontier: BinaryHeap::new(),
        };
        let sampled_ids = allowed.deterministic_sample_ids(budgets.sampled_seeds);
        let mut attempted = sampled_ids.iter().copied().collect::<HashSet<_>>();
        let mut initial_keys = self
            .restricted_candidate_keys(
                read,
                &sampled_ids,
                &mut bridge_state.simhash_cache,
                "resolving traversal-scoped deterministic seeds",
                stats,
            )
            .await?;
        for (_, node_id, entry) in directory_seeds {
            if attempted.insert(node_id) {
                initial_keys.push((node_id, rows.canonical_vector_key_from_directory(entry)));
            }
        }

        if allowed.contains(entry_point) && attempted.insert(entry_point) {
            initial_keys.extend(
                self.restricted_candidate_keys(
                    read,
                    &[entry_point],
                    &mut bridge_state.simhash_cache,
                    "resolving traversal-scoped entry point",
                    stats,
                )
                .await?,
            );
        }
        initial_keys.truncate(budgets.vector_payloads);

        let mut frontier = BinaryHeap::<Reverse<Candidate>>::new();
        let mut top = BinaryHeap::<Candidate>::with_capacity(budgets.ef_filtered.saturating_add(1));
        let mut scored = HashSet::new();
        self.restricted_score_keys(
            read,
            query.item,
            query.dimension,
            initial_keys,
            RestrictedScoringState {
                frontier: &mut frontier,
                top: &mut top,
                scored: &mut scored,
                beam_width: budgets.ef_filtered,
                stats,
            },
        )
        .await?;

        let mut expanded = HashSet::new();
        if !allowed.contains(entry_point) {
            self.restricted_enqueue_bridges(
                read,
                query_hash,
                [entry_point],
                &mut bridge_state,
                stats,
            )
            .await?;
        }

        loop {
            if stats.vector_payload_requests >= budgets.vector_payloads {
                stats.termination = Some(RestrictedSearchTermination::VectorBudget);
                break;
            }
            if bridge_state.frontier.is_empty()
                && top.len() >= budgets.ef_filtered
                && frontier
                    .peek()
                    .zip(top.peek())
                    .is_some_and(|(Reverse(next), worst)| next > worst)
            {
                stats.termination = Some(RestrictedSearchTermination::BeamComplete);
                break;
            }

            let mut routing_batch = Vec::with_capacity(FRONTIER_BATCH_SIZE + 1);
            while routing_batch.len() < FRONTIER_BATCH_SIZE {
                let Some(Reverse(candidate)) = frontier.pop() else {
                    break;
                };
                if expanded.insert(candidate.node_id) {
                    routing_batch.push(candidate.node_id);
                }
            }
            if routing_batch.is_empty() && bridge_state.frontier.is_empty() {
                stats.termination = Some(RestrictedSearchTermination::Exhausted);
                break;
            }
            let mut routing_remaining = budgets.routing_rows.saturating_sub(stats.routing_rows);
            if routing_remaining == 0 {
                stats.termination = Some(RestrictedSearchTermination::RoutingBudget);
                break;
            }
            routing_batch.truncate(routing_remaining);
            let mut eligible = Vec::new();
            let mut eligible_seen = HashSet::new();
            let mut rejected = Vec::new();
            if !routing_batch.is_empty() {
                let direct_rows = rows.layer0_neighbor_rows(&routing_batch).await?;
                stats.routing_rows = stats.routing_rows.saturating_add(routing_batch.len());
                stats.neighbor_multi_get_calls = stats.neighbor_multi_get_calls.saturating_add(1);
                routing_remaining = routing_remaining.saturating_sub(routing_batch.len());
                for row in direct_rows.into_iter().flatten() {
                    for node_id in row {
                        if allowed.contains(node_id) {
                            if !attempted.contains(&node_id) && eligible_seen.insert(node_id) {
                                eligible.push(node_id);
                            }
                        } else {
                            rejected.push(node_id);
                        }
                    }
                }
            }
            self.restricted_enqueue_bridges(
                read,
                query_hash,
                rejected.drain(..),
                &mut bridge_state,
                stats,
            )
            .await?;

            let bridge_remaining = budgets.bridge_rows.saturating_sub(stats.bridge_rows);
            let bridge_batch_len = bridge_remaining
                .min(routing_remaining)
                .min(BRIDGE_BATCH_SIZE)
                .min(bridge_state.frontier.len());
            let bridge_batch = (0..bridge_batch_len)
                .filter_map(|_| {
                    bridge_state
                        .frontier
                        .pop()
                        .map(|Reverse((_, node_id))| node_id)
                })
                .collect::<Vec<_>>();
            if !bridge_batch.is_empty() {
                let bridge_rows = rows.layer0_neighbor_rows(&bridge_batch).await?;
                stats.routing_rows = stats.routing_rows.saturating_add(bridge_batch.len());
                stats.bridge_rows = stats.bridge_rows.saturating_add(bridge_batch.len());
                stats.neighbor_multi_get_calls = stats.neighbor_multi_get_calls.saturating_add(1);
                for row in bridge_rows.into_iter().flatten() {
                    for node_id in row {
                        if allowed.contains(node_id) {
                            if !attempted.contains(&node_id) && eligible_seen.insert(node_id) {
                                eligible.push(node_id);
                            }
                        } else {
                            rejected.push(node_id);
                        }
                    }
                }
                self.restricted_enqueue_bridges(
                    read,
                    query_hash,
                    rejected.drain(..),
                    &mut bridge_state,
                    stats,
                )
                .await?;
            } else if routing_batch.is_empty() && !bridge_state.frontier.is_empty() {
                stats.termination = Some(RestrictedSearchTermination::BridgeBudget);
                break;
            }

            let vector_remaining = budgets
                .vector_payloads
                .saturating_sub(stats.vector_payload_requests);
            if vector_remaining == 0 {
                stats.termination = Some(RestrictedSearchTermination::VectorBudget);
                break;
            }
            eligible.truncate(vector_remaining.min(budgets.ef_filtered));
            if eligible.is_empty() {
                continue;
            }
            attempted.extend(eligible.iter().copied());
            let keyed = self
                .restricted_candidate_keys(
                    read,
                    &eligible,
                    &mut bridge_state.simhash_cache,
                    "resolving traversal-scoped ACORN candidates",
                    stats,
                )
                .await?;
            self.restricted_score_keys(
                read,
                query.item,
                query.dimension,
                keyed,
                RestrictedScoringState {
                    frontier: &mut frontier,
                    top: &mut top,
                    scored: &mut scored,
                    beam_width: budgets.ef_filtered,
                    stats,
                },
            )
            .await?;
        }

        let mut results = top
            .into_iter()
            .map(|candidate| SearchResult::new(candidate.node_id, candidate.distance()))
            .collect::<Vec<_>>();
        results.sort_by(|left, right| {
            left.score()
                .cmp(&right.score())
                .then_with(|| left.entity_id().cmp(&right.entity_id()))
        });
        results.truncate(k);
        debug_assert!(results
            .iter()
            .all(|result| allowed.contains(result.entity_id())));
        Ok(results)
    }
}

#[cfg(any(test, feature = "production-coverage"))]
#[path = "../../../../tests/production_support/vector/restricted.rs"]
mod contracts;

#[cfg(feature = "production-coverage")]
pub(crate) use contracts::run as run_production_contracts;
