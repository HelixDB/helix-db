//! Search-session observation contracts.
//!
//! Vector search has one algorithm regardless of whether a caller requests
//! diagnostics. `SearchSession` binds one stable read view to one index handle,
//! while `SearchObserver` makes diagnostics explicit without exposing a second
//! traversal path or global telemetry state. A session owns no physical-key
//! construction or mutation behavior; it reads through typed storage contracts,
//! validates item payloads against one bound dimension, and publishes only a
//! completed layer-0 snapshot.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
#[cfg(not(any(test, feature = "production-coverage")))]
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::time::Instant;

use slatedb::DbReadOps;

use crate::encoding::NodeId;
use crate::error::HelixDbError;

use super::distance::{ActiveVectorSemantics, Distance};
use super::index::VectorIndex;
use super::item::Item;
use super::model::Candidate;
use super::policy::{
    AdaptiveBypassObservation, AdaptiveBypassPolicy, AdaptiveBypassState, Layer0Policy,
    SamplingDecision, SimHashContext, SimHashDecision,
};
use super::storage::{EntryCandidateLayerRow, VectorRows};
use super::unaligned_vector::UnalignedVector;
use super::{
    decode_item_borrowed, CollisionThreshold, DistanceScore, FailureProbability, SearchParams,
    SearchResult, SearchStats, SimHash, UnitInterval,
};
use super::{VectorDimension, VectorIndexState};

const LAYER0_NEIGHBOR_PREFETCH_MAX_PER_STEP: usize = 2;
const LAYER0_NEIGHBOR_PREFETCH_MIN_TARGETS: usize = 2;
/// Maximum speculative layer-0 neighbor reads owned by one search session.
pub(super) const LAYER0_NEIGHBOR_PREFETCH_MAX_PER_QUERY: usize = 8;

/// Selects the nearest uncached layer-0 rows within the remaining read budget.
///
/// The deterministic distance/ID ordering keeps replay stable, while both
/// current and prefetched caches prevent duplicate I/O in the same search.
pub(super) fn select_layer0_neighbor_prefetch_targets(
    newly_admitted_neighbors: &[(NodeId, f32)],
    neighbor_cache: &HashMap<NodeId, Vec<NodeId>>,
    prefetched_neighbor_cache: &HashMap<NodeId, Vec<NodeId>>,
    remaining_prefetch_budget: usize,
) -> Vec<NodeId> {
    if newly_admitted_neighbors.len() < LAYER0_NEIGHBOR_PREFETCH_MIN_TARGETS
        || remaining_prefetch_budget == 0
    {
        return Vec::new();
    }

    let target_limit = LAYER0_NEIGHBOR_PREFETCH_MAX_PER_STEP.min(remaining_prefetch_budget);
    let mut ranked = newly_admitted_neighbors.to_vec();
    ranked.sort_by(|(left_id, left_dist), (right_id, right_dist)| {
        left_dist
            .partial_cmp(right_dist)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left_id.cmp(right_id))
    });

    let mut targets = Vec::with_capacity(target_limit);
    let mut seen = HashSet::with_capacity(ranked.len());
    for (node_id, _) in ranked {
        if !seen.insert(node_id)
            || neighbor_cache.contains_key(&node_id)
            || prefetched_neighbor_cache.contains_key(&node_id)
        {
            continue;
        }
        targets.push(node_id);
        if targets.len() >= target_limit {
            break;
        }
    }
    targets
}

/// Marks sampled nodes visited only when they are admitted for vector fetch.
///
/// Deferred samples remain eligible for rediscovery on another graph path.
pub(super) fn mark_sampled_neighbors_visited(
    visited: &mut HashSet<NodeId>,
    sampled_neighbors: Vec<(NodeId, u32)>,
) -> Vec<(NodeId, u32)> {
    sampled_neighbors
        .into_iter()
        .filter(|(neighbor_id, _)| visited.insert(*neighbor_id))
        .collect()
}

/// Query fingerprint state admitted to layer-zero traversal.
///
/// The unused variant is valid only for `SimHashMode::Off` with exhaustive
/// pre-filter sampling. Encoding that case separately prevents the hot path
/// from constructing a projection that no policy decision can observe.
pub(super) enum Layer0QuerySimHash {
    UnusedExhaustive,
    Computed(SimHash),
}

impl Layer0QuerySimHash {
    /// Returns the computed fingerprint required by filtering or sampling.
    fn computed(&self) -> Option<&SimHash> {
        match self {
            Self::UnusedExhaustive => None,
            Self::Computed(simhash) => Some(simhash),
        }
    }
}

impl<D: Distance> VectorIndex<D> {
    /// Reads one deployed layer-zero neighbor list through typed storage.
    pub(super) async fn load_neighbors_layer0(
        &self,
        read: &(impl DbReadOps + Send + Sync),
        node_id: NodeId,
    ) -> Result<Vec<NodeId>, HelixDbError> {
        VectorRows::new(read, self.row_keyspace())
            .layer0_neighbors(node_id)
            .await
    }

    /// Reads one layer-zero list and reports its single logical row read.
    pub(super) async fn load_neighbors_layer0_counted(
        &self,
        read: &(impl DbReadOps + Send + Sync),
        node_id: NodeId,
    ) -> Result<(Vec<NodeId>, usize), HelixDbError> {
        Ok((self.load_neighbors_layer0(read, node_id).await?, 1))
    }

    /// Prefetches caller-ordered layer-zero rows into a query-local cache.
    ///
    /// Empty input performs no I/O. Missing physical rows become the deployed
    /// empty-neighbor state, and the returned count is the exact logical row
    /// budget consumed by the batch.
    pub(super) async fn prefetch_layer0_neighbors_counted(
        &self,
        read: &(impl DbReadOps + Send + Sync),
        node_ids: &[NodeId],
        prefetched_neighbor_cache: &mut HashMap<NodeId, Vec<NodeId>>,
    ) -> Result<usize, HelixDbError> {
        if node_ids.is_empty() {
            return Ok(0);
        }
        let rows = VectorRows::new(read, self.row_keyspace())
            .layer0_neighbor_rows(node_ids)
            .await?;
        for (node_id, maybe_row) in node_ids.iter().copied().zip(rows) {
            prefetched_neighbor_cache.insert(node_id, maybe_row.unwrap_or_default());
        }
        Ok(node_ids.len())
    }

    /// Greedily descends one HNSW layer to choose the next entry point.
    ///
    /// The traversal reads only through the caller's stable `DbReadOps` view.
    /// If the starting item is stale or missing, it returns the supplied entry
    /// unchanged so a lower layer can recover without introducing a write-side
    /// repair into read-only search.
    pub(super) async fn search_layer_greedy(
        &self,
        read: &(impl DbReadOps + Send + Sync),
        query: &Item<'_, D>,
        entry_point: NodeId,
        layer: u16,
    ) -> Result<NodeId, HelixDbError> {
        let mut visited = HashSet::new();
        let mut current = entry_point;
        let mut current_dist = {
            let Some(current_item) = self.get_item_for_layer(read, layer, current).await? else {
                tracing::warn!(
                    index_name = %self.name(),
                    index_id = self.id(),
                    operation = "greedy_search",
                    traversal_layer = layer,
                    stale_entry_point = entry_point,
                    "missing HNSW greedy entry point item; reusing caller-provided entry point"
                );
                return Ok(entry_point);
            };
            Candidate::try_new(current, D::distance(query, &current_item))?.score()
        };

        visited.insert(current);
        loop {
            let neighbors = if layer == 0 {
                self.load_neighbors_layer0(read, current).await?
            } else {
                self.load_upper_neighbors(read, layer, current)
                    .await?
                    .unwrap_or_default()
            };
            let mut changed = false;
            for neighbor_id in neighbors {
                if !visited.insert(neighbor_id) {
                    continue;
                }
                let Some(neighbor_item) = self.get_item_for_layer(read, layer, neighbor_id).await?
                else {
                    continue;
                };
                let distance =
                    Candidate::try_new(neighbor_id, D::distance(query, &neighbor_item))?.score();
                if distance < current_dist {
                    current = neighbor_id;
                    current_dist = distance;
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        Ok(current)
    }
}

impl<D: Distance> VectorIndex<D> {
    /// Finds the highest live entry candidate without staging cleanup.
    ///
    /// Search skips corrupt, mismatched, and payload-less candidate rows while
    /// preserving its read-only transaction boundary. Writable pruning remains
    /// owned by mutation repair.
    pub(super) async fn find_live_entry_candidate_readonly(
        &self,
        read: &(impl DbReadOps + Send + Sync),
    ) -> Result<Option<NodeId>, HelixDbError> {
        let rows = VectorRows::new(read, self.row_keyspace());
        let mut candidates = rows.entry_candidates().await?;

        while let Some(candidate) = candidates.next().await? {
            let layer = candidate.layer();
            let node_id = candidate.node_id();
            let EntryCandidateLayerRow::Present(node_layer) =
                rows.entry_candidate_layer(node_id).await?
            else {
                continue;
            };
            if node_layer != layer {
                continue;
            }
            if self.get_item(read, node_id).await?.is_some() {
                return Ok(Some(node_id));
            }
        }
        Ok(None)
    }

    /// Searches layer zero with the deployed LSM-Vec SimHash policy.
    ///
    /// This is the single layer-zero traversal used by result-only and observed
    /// search sessions. It owns bounded prefetch, adaptive bypass, deterministic
    /// sampling, beam maintenance, and diagnostic accounting while all physical
    /// row lookup remains behind typed storage/index contracts. The caller
    /// supplies the dimension validated from the same metadata snapshot so the
    /// traversal cannot reopen or disagree with that schema binding.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn search_layer0_with_simhash<
        const COLLECT_DIAGNOSTICS: bool,
        const STRICT_EXHAUSTIVE: bool,
    >(
        &self,
        txn: &(impl DbReadOps + Send + Sync),
        query: &Item<'_, D>,
        query_simhash: &Layer0QuerySimHash,
        expected_dimension: VectorDimension,
        entry_point: NodeId,
        params: &SearchParams,
        simhash_threshold: usize,
        sampling_ratio: f32,
        adaptive_enabled: bool,
        adaptive_failure_prob: f32,
    ) -> Result<(Vec<SearchResult>, super::SearchStats), HelixDbError> {
        debug_assert_eq!(
            STRICT_EXHAUSTIVE,
            matches!(query_simhash, Layer0QuerySimHash::UnusedExhaustive),
            "strict exhaustive specialization must agree with query fingerprint state",
        );
        if matches!(query_simhash, Layer0QuerySimHash::UnusedExhaustive)
            && params.requires_query_simhash()
        {
            return Err(HelixDbError::InvariantViolation(
                "layer-zero filtering or sampling requires a query fingerprint".to_string(),
            ));
        }

        let mut visited = HashSet::new();
        let mut candidates = BinaryHeap::new(); // nearest candidate via Reverse
        let mut w = BinaryHeap::new(); // farthest retained result
        let mut neighbor_cache: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
        let mut prefetched_neighbor_cache: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
        let mut remaining_neighbor_prefetch_budget = LAYER0_NEIGHBOR_PREFETCH_MAX_PER_QUERY;
        let mut vector_cache: HashMap<NodeId, bytes::Bytes> = HashMap::new();
        let mut simhash_local_cache: HashMap<NodeId, Option<super::SimHash>> = HashMap::new();
        let k = params.k();
        let ef = params.ef();
        let base_sampling_ratio = params.simhash_sampling_ratio_override.unwrap_or(
            UnitInterval::try_new(sampling_ratio)
                .map_err(|error| HelixDbError::InvalidVectorConfig(error.into()))?,
        );
        let pre_simhash_sampling_ratio_override = params.pre_simhash_sampling_ratio_override;
        let bypass_window_expansions = params.simhash_bypass_window_expansions.get();
        let active_failure_prob = params.simhash_failure_prob_override.unwrap_or(
            FailureProbability::try_new(adaptive_failure_prob)
                .map_err(|error| HelixDbError::InvalidVectorConfig(error.into()))?,
        );
        let semantics = ActiveVectorSemantics::for_distance::<D>().ok_or_else(|| {
            HelixDbError::Config(format!(
                "distance '{}' has no stable SimHash policy capability",
                D::name()
            ))
        })?;
        let layer0_policy = Layer0Policy::from_deployed(
            semantics.metric(),
            params.simhash_mode,
            CollisionThreshold::try_new(
                simhash_threshold,
                NonZeroUsize::new(super::SIMHASH_BITS).expect("SimHash bit width is nonzero"),
            )
            .map_err(|error| HelixDbError::InvalidVectorConfig(error.into()))?,
            base_sampling_ratio,
            pre_simhash_sampling_ratio_override,
            adaptive_enabled,
            active_failure_prob,
        )
        .with_adaptive_bypass(AdaptiveBypassPolicy::from_deployed(
            params.simhash_mode,
            params.ef,
            params.simhash_bypass_min_frontier,
            params.simhash_bypass_window_expansions,
            params.simhash_bypass_min_filter_rate,
            params.simhash_read_budget_multiplier,
        ));
        let mut search_session = match query_simhash {
            Layer0QuerySimHash::UnusedExhaustive => super::randomness::SearchSession::seeded(0),
            Layer0QuerySimHash::Computed(query_simhash) => {
                self.start_search_randomness(query_simhash, entry_point, ef)
            }
        };
        let mut simhash_window_examined = 0usize;
        let mut simhash_window_filtered = 0usize;
        let mut simhash_window_expansions = 0usize;
        let mut adaptive_bypass_state = AdaptiveBypassState::Ready;

        // Diagnostics and read accounting.
        let mut _txn_get_total = 0usize;
        let mut _txn_get_neighbors = 0usize;
        let mut _txn_get_simhash_filter = 0usize;
        let mut _txn_get_simhash_key_derivation = 0usize;
        let mut _txn_get_vectors = 0usize;
        let mut _txn_multi_get_calls_total = 0usize;
        let mut _txn_multi_get_calls_simhash_filter = 0usize;
        let mut _txn_multi_get_calls_simhash_key_derivation = 0usize;
        let mut _txn_multi_get_calls_vectors = 0usize;
        let mut _neighbors_fetch_ns = 0u64;
        let mut _simhash_fetch_ns_filter = 0u64;
        let mut _simhash_fetch_ns_key_derivation = 0u64;
        let mut _vector_fetch_ns = 0u64;
        let mut _distance_compute_ns = 0u64;
        let mut _distance_computations = 0usize;
        let mut _simhash_bypass_expansions = 0usize;
        let mut _simhash_skipped_candidates = 0usize;
        let mut _pre_simhash_sample_kept = 0usize;
        let mut _pre_simhash_sample_dropped = 0usize;
        let mut _simhash_bypass_trigger_budget = 0usize;
        let mut _simhash_bypass_trigger_low_yield = 0usize;
        let mut _simhash_examined = 0usize;
        let mut _simhash_missing_hash = 0usize;
        let mut _simhash_passed_before_sampling = 0usize;
        let mut _simhash_passed_after_sampling = 0usize;
        let mut _active_simhash_threshold_sum = 0usize;
        let mut _active_simhash_threshold_samples = 0usize;
        let mut _active_sampling_ratio_sum = 0f64;
        let mut _active_sampling_ratio_samples = 0usize;
        let mut _effective_beam_len_sum = 0usize;
        let mut _effective_beam_len_samples = 0usize;

        // Initialize with entry point (with stale-entry recovery).
        let mut resolved_entry_point = entry_point;
        let entry_fetch_start = COLLECT_DIAGNOSTICS.then(Instant::now);
        let (mut entry_bytes_opt, entry_reads) = self
            .get_canonical_vector_bytes_counted::<COLLECT_DIAGNOSTICS>(txn, resolved_entry_point)
            .await?;
        if let Some(entry_fetch_start) = entry_fetch_start {
            _vector_fetch_ns =
                _vector_fetch_ns.saturating_add(
                    entry_fetch_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                );
        }
        if COLLECT_DIAGNOSTICS {
            _txn_get_vectors = _txn_get_vectors.saturating_add(entry_reads.vector_reads);
            _txn_get_simhash_key_derivation =
                _txn_get_simhash_key_derivation.saturating_add(entry_reads.simhash_reads);
            _txn_multi_get_calls_total =
                _txn_multi_get_calls_total.saturating_add(entry_reads.simhash_multi_get_calls);
            _txn_multi_get_calls_simhash_key_derivation =
                _txn_multi_get_calls_simhash_key_derivation
                    .saturating_add(entry_reads.simhash_multi_get_calls);
            _simhash_fetch_ns_key_derivation =
                _simhash_fetch_ns_key_derivation.saturating_add(entry_reads.simhash_fetch_ns);
            _txn_get_total = _txn_get_total.saturating_add(entry_reads.total_reads());
        }

        'fallback: {
            if entry_bytes_opt.is_some() {
                break 'fallback;
            }
            let Some(fallback_entry) = self.find_live_entry_candidate_readonly(txn).await? else {
                break 'fallback;
            };
            if fallback_entry != resolved_entry_point {
                tracing::warn!(
                    index_name = %self.name(),
                    index_id = self.id(),
                    operation = "search_layer0",
                    stale_entry_point = resolved_entry_point,
                    replacement_entry_point = fallback_entry,
                    "recovered stale vector search entry point"
                );
                resolved_entry_point = fallback_entry;
                let fallback_fetch_start = COLLECT_DIAGNOSTICS.then(Instant::now);
                let (fallback_bytes, fallback_reads) = self
                    .get_canonical_vector_bytes_counted::<COLLECT_DIAGNOSTICS>(
                        txn,
                        resolved_entry_point,
                    )
                    .await?;
                if let Some(fallback_fetch_start) = fallback_fetch_start {
                    _vector_fetch_ns = _vector_fetch_ns.saturating_add(
                        fallback_fetch_start
                            .elapsed()
                            .as_nanos()
                            .min(u64::MAX as u128) as u64,
                    );
                }
                if COLLECT_DIAGNOSTICS {
                    _txn_get_vectors = _txn_get_vectors.saturating_add(fallback_reads.vector_reads);
                    _txn_get_simhash_key_derivation = _txn_get_simhash_key_derivation
                        .saturating_add(fallback_reads.simhash_reads);
                    _txn_multi_get_calls_total = _txn_multi_get_calls_total
                        .saturating_add(fallback_reads.simhash_multi_get_calls);
                    _txn_multi_get_calls_simhash_key_derivation =
                        _txn_multi_get_calls_simhash_key_derivation
                            .saturating_add(fallback_reads.simhash_multi_get_calls);
                    _simhash_fetch_ns_key_derivation = _simhash_fetch_ns_key_derivation
                        .saturating_add(fallback_reads.simhash_fetch_ns);
                    _txn_get_total = _txn_get_total.saturating_add(fallback_reads.total_reads());
                }
                entry_bytes_opt = fallback_bytes;
            }
        }

        let Some(entry_bytes) = entry_bytes_opt else {
            tracing::warn!(
                index_name = %self.name(),
                index_id = self.id(),
                operation = "search_layer0",
                stale_entry_point = resolved_entry_point,
                "unable to recover stale vector search entry point; returning empty results"
            );
            let txn_get_simhash =
                _txn_get_simhash_filter.saturating_add(_txn_get_simhash_key_derivation);
            let txn_multi_get_calls_simhash = _txn_multi_get_calls_simhash_filter
                .saturating_add(_txn_multi_get_calls_simhash_key_derivation);
            let simhash_fetch_ns =
                _simhash_fetch_ns_filter.saturating_add(_simhash_fetch_ns_key_derivation);
            let stats = super::SearchStats {
                object_store_reads: _txn_get_total,
                txn_get_total: _txn_get_total,
                txn_get_neighbors: _txn_get_neighbors,
                txn_get_simhash,
                txn_get_simhash_filter: _txn_get_simhash_filter,
                txn_get_simhash_key_derivation: _txn_get_simhash_key_derivation,
                txn_get_vectors: _txn_get_vectors,
                txn_multi_get_calls_total: _txn_multi_get_calls_total,
                txn_multi_get_calls_simhash,
                txn_multi_get_calls_simhash_filter: _txn_multi_get_calls_simhash_filter,
                txn_multi_get_calls_simhash_key_derivation:
                    _txn_multi_get_calls_simhash_key_derivation,
                txn_multi_get_calls_vectors: _txn_multi_get_calls_vectors,
                neighbors_fetch_ns: _neighbors_fetch_ns,
                simhash_fetch_ns,
                simhash_fetch_ns_filter: _simhash_fetch_ns_filter,
                simhash_fetch_ns_key_derivation: _simhash_fetch_ns_key_derivation,
                vector_fetch_ns: _vector_fetch_ns,
                distance_compute_ns: _distance_compute_ns,
                distance_computations: _distance_computations,
                ..super::SearchStats::default()
            };
            return Ok((Vec::new(), stats));
        };
        let entry_item = decode_item_borrowed::<D>(&entry_bytes, expected_dimension)?;
        let entry_distance_start = COLLECT_DIAGNOSTICS.then(Instant::now);
        let entry_dist = D::distance(query, &entry_item);
        if let Some(entry_distance_start) = entry_distance_start {
            _distance_compute_ns = _distance_compute_ns.saturating_add(
                entry_distance_start
                    .elapsed()
                    .as_nanos()
                    .min(u64::MAX as u128) as u64,
            );
        }
        if COLLECT_DIAGNOSTICS {
            _distance_computations = _distance_computations.saturating_add(1);
        }
        vector_cache.insert(resolved_entry_point, entry_bytes);

        let entry_candidate = Candidate::try_new(resolved_entry_point, entry_dist)?;
        candidates.push(Reverse(entry_candidate));
        w.push(entry_candidate);
        let topk_target = k.max(1);
        let mut topk = (!STRICT_EXHAUSTIVE).then(|| {
            let mut topk = BinaryHeap::new();
            topk.push(entry_candidate);
            topk
        });
        visited.insert(resolved_entry_point);
        // Virtual beam-fill slots consumed by SimHash-filtered nodes while the
        // effective beam is still filling. These are accounting-only slots; w
        // always contains real distance-scored candidates.
        let mut simhash_fill_slots: usize = 0;

        // Statistics for monitoring
        let mut _neighbors_examined = 0;
        let mut _simhash_filtered = 0;
        let mut _vectors_loaded = 0;
        let mut _expansion_steps = 0usize;

        while !candidates.is_empty() {
            if COLLECT_DIAGNOSTICS {
                _expansion_steps += 1;
            }
            let Reverse(current) = candidates.pop().unwrap();
            let current_dist = current.score();

            let effective_len = w.len().saturating_add(simhash_fill_slots);
            if COLLECT_DIAGNOSTICS {
                _effective_beam_len_sum = _effective_beam_len_sum.saturating_add(effective_len);
                _effective_beam_len_samples = _effective_beam_len_samples.saturating_add(1);
            }
            if effective_len >= ef && current_dist > w.peek().unwrap().score() {
                break;
            }

            // Load neighbors from layer 0
            let neighbors = if let Some(cached) = neighbor_cache.get(&current.node_id) {
                cached.clone()
            } else if let Some(prefetched) = prefetched_neighbor_cache.remove(&current.node_id) {
                neighbor_cache.insert(current.node_id, prefetched.clone());
                prefetched
            } else {
                let neighbors_fetch_start = COLLECT_DIAGNOSTICS.then(Instant::now);
                let (loaded, reads) = self
                    .load_neighbors_layer0_counted(txn, current.node_id)
                    .await?;
                if let Some(neighbors_fetch_start) = neighbors_fetch_start {
                    _neighbors_fetch_ns = _neighbors_fetch_ns.saturating_add(
                        neighbors_fetch_start
                            .elapsed()
                            .as_nanos()
                            .min(u64::MAX as u128) as u64,
                    );
                }
                if COLLECT_DIAGNOSTICS {
                    _txn_get_neighbors = _txn_get_neighbors.saturating_add(reads);
                    _txn_get_total = _txn_get_total.saturating_add(reads);
                }
                neighbor_cache.insert(current.node_id, loaded.clone());
                loaded
            };
            if COLLECT_DIAGNOSTICS {
                _neighbors_examined += neighbors.len();
            }

            let mut frontier_neighbors = Vec::with_capacity(neighbors.len());
            for &neighbor_id in &neighbors {
                if visited.contains(&neighbor_id) {
                    continue;
                }
                frontier_neighbors.push(neighbor_id);
            }

            if frontier_neighbors.is_empty() {
                continue;
            }

            let decision = if STRICT_EXHAUSTIVE {
                SimHashDecision::exhaustive()
            } else {
                let Some(_) = query_simhash.computed() else {
                    return Err(HelixDbError::InvariantViolation(
                        "non-exhaustive layer-zero search requires a query fingerprint".to_string(),
                    ));
                };
                let topk = topk
                    .as_ref()
                    .expect("computed query fingerprints always initialize adaptive top-k state");
                let topk_ready = topk.len() >= topk_target;
                let delta = topk
                    .peek()
                    .map(|candidate| candidate.score())
                    .unwrap_or(current_dist);
                let decision = layer0_policy.decide(SimHashContext {
                    topk_ready,
                    ef,
                    search_frontier_len: w.len(),
                    candidate_frontier_len: frontier_neighbors.len(),
                    current: current.distance(),
                    delta: DistanceScore::try_new(delta)
                        .expect("top-k candidates contain validated distances"),
                    adaptive_bypass: AdaptiveBypassObservation {
                        state: adaptive_bypass_state,
                        simhash_filter_reads: _txn_get_simhash_filter,
                        window_examined: simhash_window_examined,
                        window_filtered: simhash_window_filtered,
                        window_expansions: simhash_window_expansions,
                    },
                });
                adaptive_bypass_state = decision.next_bypass_state;
                if COLLECT_DIAGNOSTICS && decision.bypass_trigger.includes_read_budget() {
                    _simhash_bypass_trigger_budget =
                        _simhash_bypass_trigger_budget.saturating_add(1);
                }
                if COLLECT_DIAGNOSTICS && decision.bypass_trigger.includes_low_yield() {
                    _simhash_bypass_trigger_low_yield =
                        _simhash_bypass_trigger_low_yield.saturating_add(1);
                }
                decision
            };
            let active_sampling_ratio = decision.sampling.probability();
            let active_simhash_threshold = decision.threshold.map_or(0, |value| value.get());
            if COLLECT_DIAGNOSTICS {
                _active_sampling_ratio_sum += decision.base_sampling_probability as f64;
                _active_sampling_ratio_samples = _active_sampling_ratio_samples.saturating_add(1);
                if decision.filter_cached {
                    _active_simhash_threshold_sum =
                        _active_simhash_threshold_sum.saturating_add(active_simhash_threshold);
                    _active_simhash_threshold_samples =
                        _active_simhash_threshold_samples.saturating_add(1);
                }
            }

            // Stage-0 pre-sampling: trim candidate fanout before SimHash fetch when
            // frontiers get large on warm-cache workloads.
            let mut simhash_frontier = frontier_neighbors;
            let pre_sample_enabled = !matches!(decision.pre_sampling, SamplingDecision::Exhaustive);
            if pre_sample_enabled {
                let pre_ratio = decision.pre_sampling.probability();
                let mut sampled = Vec::with_capacity(simhash_frontier.len());
                for &neighbor_id in &simhash_frontier {
                    if search_session.should_sample(pre_ratio) {
                        sampled.push(neighbor_id);
                    } else if COLLECT_DIAGNOSTICS {
                        _pre_simhash_sample_dropped = _pre_simhash_sample_dropped.saturating_add(1);
                    }
                }

                if sampled.is_empty() {
                    let Some(fallback_idx) = search_session.choose_index(simhash_frontier.len())
                    else {
                        continue;
                    };
                    sampled.push(simhash_frontier[fallback_idx]);
                }
                if COLLECT_DIAGNOSTICS {
                    _pre_simhash_sample_kept =
                        _pre_simhash_sample_kept.saturating_add(sampled.len());
                }
                simhash_frontier = sampled;
            }

            // Fetch SimHash values for frontier neighbors.
            if COLLECT_DIAGNOSTICS && decision.bypassed {
                _simhash_bypass_expansions = _simhash_bypass_expansions.saturating_add(1);
                _simhash_skipped_candidates =
                    _simhash_skipped_candidates.saturating_add(simhash_frontier.len());
            }

            if decision.fetch_missing {
                let simhash_stats = self
                    .fill_simhash_cache_for_nodes_counted::<COLLECT_DIAGNOSTICS>(
                        txn,
                        &simhash_frontier,
                        &mut simhash_local_cache,
                        "fetching simhash for filtering",
                    )
                    .await?;
                if COLLECT_DIAGNOSTICS {
                    _txn_multi_get_calls_total =
                        _txn_multi_get_calls_total.saturating_add(simhash_stats.multi_get_calls);
                    _txn_multi_get_calls_simhash_filter = _txn_multi_get_calls_simhash_filter
                        .saturating_add(simhash_stats.multi_get_calls);
                    _txn_get_total = _txn_get_total.saturating_add(simhash_stats.reads);
                    _simhash_fetch_ns_filter =
                        _simhash_fetch_ns_filter.saturating_add(simhash_stats.fetch_ns);
                }
                _txn_get_simhash_filter =
                    _txn_get_simhash_filter.saturating_add(simhash_stats.reads);
            }

            // Section-3.3: threshold-gated candidate screening + proximity-aware probabilistic expansion.
            let mut sampled_neighbors = Vec::with_capacity(simhash_frontier.len());
            let mut deferred_neighbors = Vec::new();
            let should_sample = !pre_sample_enabled
                && !matches!(decision.sampling, SamplingDecision::Exhaustive)
                && active_sampling_ratio > 0.0;
            let mut simhash_examined_this_round = 0usize;
            let mut simhash_filtered_this_round = 0usize;

            for &neighbor_id in &simhash_frontier {
                let neighbor_hash = if decision.filter_cached {
                    simhash_local_cache.get(&neighbor_id).copied().flatten()
                } else {
                    None
                };
                if let Some(hash) = neighbor_hash {
                    let Some(query_simhash) = query_simhash.computed() else {
                        return Err(HelixDbError::InvariantViolation(
                            "layer-zero policy requested SimHash filtering without a query fingerprint"
                                .to_string(),
                        ));
                    };
                    simhash_examined_this_round = simhash_examined_this_round.saturating_add(1);
                    if COLLECT_DIAGNOSTICS {
                        _simhash_examined = _simhash_examined.saturating_add(1);
                    }
                    if !hash.passes_threshold(query_simhash, active_simhash_threshold) {
                        if COLLECT_DIAGNOSTICS {
                            _simhash_filtered += 1;
                        }
                        simhash_filtered_this_round = simhash_filtered_this_round.saturating_add(1);

                        // Mark as visited so it's not re-discovered via another path.
                        if visited.insert(neighbor_id)
                            && w.len().saturating_add(simhash_fill_slots) < ef
                        {
                            // Account for this filtered node as a virtual beam-fill
                            // slot while the beam is still filling.
                            simhash_fill_slots = simhash_fill_slots.saturating_add(1);
                        }

                        continue;
                    }
                } else if COLLECT_DIAGNOSTICS && decision.fetch_missing {
                    _simhash_missing_hash = _simhash_missing_hash.saturating_add(1);
                }

                if COLLECT_DIAGNOSTICS {
                    _simhash_passed_before_sampling =
                        _simhash_passed_before_sampling.saturating_add(1);
                }

                let similarity_bits = match (neighbor_hash, query_simhash.computed()) {
                    (Some(hash), Some(query_simhash)) => 64 - hash.hamming_distance(query_simhash),
                    (None, _) => 32,
                    (Some(_), None) => {
                        return Err(HelixDbError::InvariantViolation(
                            "layer-zero policy requested similarity sampling without a query fingerprint"
                                .to_string(),
                        ));
                    }
                };

                if should_sample {
                    let sampling_probability = decision
                        .sampling
                        .candidate_probability(similarity_bits, decision.threshold);
                    if search_session.should_sample(sampling_probability) {
                        sampled_neighbors.push((neighbor_id, similarity_bits));
                    } else {
                        deferred_neighbors.push((neighbor_id, similarity_bits));
                    }
                } else if active_sampling_ratio <= 0.0 {
                    deferred_neighbors.push((neighbor_id, similarity_bits));
                } else {
                    sampled_neighbors.push((neighbor_id, similarity_bits));
                }
            }

            if decision.filter_cached && simhash_examined_this_round > 0 {
                simhash_window_examined =
                    simhash_window_examined.saturating_add(simhash_examined_this_round);
                simhash_window_filtered =
                    simhash_window_filtered.saturating_add(simhash_filtered_this_round);
                simhash_window_expansions = simhash_window_expansions.saturating_add(1);
                if simhash_window_expansions > bypass_window_expansions {
                    // Cheap exponential decay to keep adaptation responsive.
                    simhash_window_examined /= 2;
                    simhash_window_filtered /= 2;
                    simhash_window_expansions = bypass_window_expansions / 2;
                }
            }

            // Avoid getting stuck on sparse frontier expansions.
            if active_sampling_ratio > 0.0
                && sampled_neighbors.is_empty()
                && !deferred_neighbors.is_empty()
            {
                let best_similarity = deferred_neighbors
                    .iter()
                    .map(|(_, similarity)| *similarity)
                    .max()
                    .unwrap_or(32);
                let mut best_neighbors = deferred_neighbors
                    .iter()
                    .filter(|(_, similarity)| *similarity == best_similarity)
                    .map(|(neighbor_id, _)| *neighbor_id)
                    .collect::<Vec<_>>();

                let Some(fallback_idx) = search_session.choose_index(best_neighbors.len()) else {
                    continue;
                };
                let fallback = best_neighbors.swap_remove(fallback_idx);
                sampled_neighbors.push((fallback, best_similarity));
            }

            if COLLECT_DIAGNOSTICS {
                _simhash_passed_after_sampling =
                    _simhash_passed_after_sampling.saturating_add(sampled_neighbors.len());
            }

            let accepted_neighbors =
                mark_sampled_neighbors_visited(&mut visited, sampled_neighbors);

            let simhash_passed: Vec<NodeId> = accepted_neighbors
                .iter()
                .map(|(neighbor_id, _)| *neighbor_id)
                .collect();

            // Phase 2: Load vectors (ONLY for SimHash-passed candidates)
            let mut missing_vector_ids = Vec::new();
            for &neighbor_id in &simhash_passed {
                if !vector_cache.contains_key(&neighbor_id) {
                    missing_vector_ids.push(neighbor_id);
                }
            }

            let vector_fetch_start = COLLECT_DIAGNOSTICS.then(Instant::now);
            if !missing_vector_ids.is_empty() {
                let (resolved_keys, key_stats) = self
                    .resolve_required_canonical_vector_keys_batch_counted::<COLLECT_DIAGNOSTICS>(
                        txn,
                        &missing_vector_ids,
                        &mut simhash_local_cache,
                        "deriving canonical vector keys during layer-0 vector fetch",
                    )
                    .await?;
                if COLLECT_DIAGNOSTICS {
                    _txn_multi_get_calls_total =
                        _txn_multi_get_calls_total.saturating_add(key_stats.multi_get_calls);
                    _txn_multi_get_calls_simhash_key_derivation =
                        _txn_multi_get_calls_simhash_key_derivation
                            .saturating_add(key_stats.multi_get_calls);
                    _txn_get_simhash_key_derivation =
                        _txn_get_simhash_key_derivation.saturating_add(key_stats.reads);
                    _txn_get_total = _txn_get_total.saturating_add(key_stats.reads);
                    _simhash_fetch_ns_key_derivation =
                        _simhash_fetch_ns_key_derivation.saturating_add(key_stats.fetch_ns);
                }

                let mut keyed_ids = missing_vector_ids
                    .into_iter()
                    .zip(resolved_keys)
                    .collect::<Vec<_>>();

                keyed_ids.sort_by(|left, right| left.1.physical_order(&right.1));
                let vector_keys = keyed_ids
                    .iter()
                    .map(|(_, key)| key.clone())
                    .collect::<Vec<_>>();
                let vector_rows = VectorRows::new(txn, self.row_keyspace())
                    .canonical_vector_rows(&vector_keys)
                    .await?;
                if COLLECT_DIAGNOSTICS {
                    _txn_multi_get_calls_total = _txn_multi_get_calls_total.saturating_add(1);
                    _txn_multi_get_calls_vectors = _txn_multi_get_calls_vectors.saturating_add(1);

                    let reads = vector_keys.len();
                    _txn_get_vectors = _txn_get_vectors.saturating_add(reads);
                    _txn_get_total = _txn_get_total.saturating_add(reads);
                }

                for ((neighbor_id, _), maybe_vector) in keyed_ids.into_iter().zip(vector_rows) {
                    if let Some(vector_bytes) = maybe_vector {
                        if COLLECT_DIAGNOSTICS {
                            _vectors_loaded += 1;
                        }
                        vector_cache.insert(neighbor_id, vector_bytes);
                    }
                }
            }
            if let Some(vector_fetch_start) = vector_fetch_start {
                _vector_fetch_ns = _vector_fetch_ns.saturating_add(
                    vector_fetch_start
                        .elapsed()
                        .as_nanos()
                        .min(u64::MAX as u128) as u64,
                );
            }

            let mut newly_admitted_neighbors = Vec::new();
            for neighbor_id in simhash_passed {
                let Some(neighbor_bytes) = vector_cache.get(&neighbor_id) else {
                    continue;
                };
                let neighbor_item = decode_item_borrowed::<D>(neighbor_bytes, expected_dimension)?;
                let distance_start = COLLECT_DIAGNOSTICS.then(Instant::now);
                let candidate =
                    Candidate::try_new(neighbor_id, D::distance(query, &neighbor_item))?;
                let dist = candidate.score();
                if let Some(distance_start) = distance_start {
                    _distance_compute_ns = _distance_compute_ns.saturating_add(
                        distance_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                    );
                }
                if COLLECT_DIAGNOSTICS {
                    _distance_computations = _distance_computations.saturating_add(1);
                }
                let effective_len = w.len().saturating_add(simhash_fill_slots);
                if dist < w.peek().unwrap().score() || effective_len < ef {
                    candidates.push(Reverse(candidate));
                    w.push(candidate);
                    newly_admitted_neighbors.push((neighbor_id, dist));

                    if let Some(topk) = &mut topk {
                        topk.push(candidate);
                        if topk.len() > topk_target {
                            topk.pop();
                        }
                    }

                    while w.len().saturating_add(simhash_fill_slots) > ef {
                        if simhash_fill_slots > 0 {
                            // A real candidate has effectively replaced a virtual
                            // fill slot.
                            simhash_fill_slots -= 1;
                        } else if w.len() > ef {
                            // Standard HNSW overflow trim once no virtual slots remain.
                            w.pop();
                        } else {
                            break;
                        }
                    }
                }
            }

            if decision.filter_cached && remaining_neighbor_prefetch_budget > 0 {
                let prefetch_targets = select_layer0_neighbor_prefetch_targets(
                    &newly_admitted_neighbors,
                    &neighbor_cache,
                    &prefetched_neighbor_cache,
                    remaining_neighbor_prefetch_budget,
                );
                if !prefetch_targets.is_empty() {
                    let prefetch_start = COLLECT_DIAGNOSTICS.then(Instant::now);
                    let reads = self
                        .prefetch_layer0_neighbors_counted(
                            txn,
                            &prefetch_targets,
                            &mut prefetched_neighbor_cache,
                        )
                        .await?;
                    if let Some(prefetch_start) = prefetch_start {
                        _neighbors_fetch_ns = _neighbors_fetch_ns.saturating_add(
                            prefetch_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                        );
                    }

                    if reads > 0 {
                        if COLLECT_DIAGNOSTICS {
                            _txn_multi_get_calls_total =
                                _txn_multi_get_calls_total.saturating_add(1);
                            _txn_get_neighbors = _txn_get_neighbors.saturating_add(reads);
                            _txn_get_total = _txn_get_total.saturating_add(reads);
                        }
                        remaining_neighbor_prefetch_budget =
                            remaining_neighbor_prefetch_budget.saturating_sub(reads);
                    }
                }
            }

            debug_assert!(w.len() <= ef);
            debug_assert!(w.len().saturating_add(simhash_fill_slots) <= ef);
        }

        // Convert to results
        let mut results: Vec<SearchResult> = w
            .into_iter()
            .map(|candidate| SearchResult::new(candidate.node_id, candidate.distance()))
            .collect();

        results.sort_by(|a, b| {
            a.score()
                .cmp(&b.score())
                .then_with(|| a.entity_id().cmp(&b.entity_id()))
        });

        let avg_active_simhash_threshold = if _active_simhash_threshold_samples > 0 {
            _active_simhash_threshold_sum as f32 / _active_simhash_threshold_samples as f32
        } else {
            0.0
        };
        let avg_active_sampling_ratio = if _active_sampling_ratio_samples > 0 {
            (_active_sampling_ratio_sum / _active_sampling_ratio_samples as f64) as f32
        } else {
            0.0
        };
        let avg_effective_beam_len = if _effective_beam_len_samples > 0 {
            _effective_beam_len_sum as f32 / _effective_beam_len_samples as f32
        } else {
            0.0
        };
        let txn_get_simhash =
            _txn_get_simhash_filter.saturating_add(_txn_get_simhash_key_derivation);
        let txn_multi_get_calls_simhash = _txn_multi_get_calls_simhash_filter
            .saturating_add(_txn_multi_get_calls_simhash_key_derivation);
        let simhash_fetch_ns =
            _simhash_fetch_ns_filter.saturating_add(_simhash_fetch_ns_key_derivation);

        let stats = super::SearchStats {
            expansion_steps: _expansion_steps,
            neighbors_examined: _neighbors_examined,
            simhash_filtered: _simhash_filtered,
            simhash_examined: _simhash_examined,
            simhash_missing_hash: _simhash_missing_hash,
            simhash_passed_before_sampling: _simhash_passed_before_sampling,
            simhash_passed_after_sampling: _simhash_passed_after_sampling,
            vectors_loaded: _vectors_loaded,
            object_store_reads: _txn_get_total,
            txn_get_total: _txn_get_total,
            txn_get_neighbors: _txn_get_neighbors,
            txn_get_simhash,
            txn_get_simhash_filter: _txn_get_simhash_filter,
            txn_get_simhash_key_derivation: _txn_get_simhash_key_derivation,
            txn_get_vectors: _txn_get_vectors,
            txn_multi_get_calls_total: _txn_multi_get_calls_total,
            txn_multi_get_calls_simhash,
            txn_multi_get_calls_simhash_filter: _txn_multi_get_calls_simhash_filter,
            txn_multi_get_calls_simhash_key_derivation: _txn_multi_get_calls_simhash_key_derivation,
            txn_multi_get_calls_vectors: _txn_multi_get_calls_vectors,
            neighbors_fetch_ns: _neighbors_fetch_ns,
            simhash_fetch_ns,
            simhash_fetch_ns_filter: _simhash_fetch_ns_filter,
            simhash_fetch_ns_key_derivation: _simhash_fetch_ns_key_derivation,
            vector_fetch_ns: _vector_fetch_ns,
            distance_compute_ns: _distance_compute_ns,
            distance_computations: _distance_computations,
            simhash_bypass_expansions: _simhash_bypass_expansions,
            simhash_skipped_candidates: _simhash_skipped_candidates,
            pre_simhash_sample_kept: _pre_simhash_sample_kept,
            pre_simhash_sample_dropped: _pre_simhash_sample_dropped,
            simhash_bypass_trigger_budget: _simhash_bypass_trigger_budget,
            simhash_bypass_trigger_low_yield: _simhash_bypass_trigger_low_yield,
            avg_active_simhash_threshold,
            avg_active_sampling_ratio,
            avg_effective_beam_len,
        };

        Ok((results, stats))
    }
}

/// One vector-search invocation bound to a stable read view and observer.
///
/// Public result-only and diagnostic APIs both construct this contract, so
/// validation, upper traversal, layer-0 policy, and error behavior cannot select
/// separate implementations. The read backend remains the narrow `DbReadOps`
/// boundary used by snapshots, transactions, and counting test doubles.
pub(crate) struct SearchSession<'index, 'observer, R, D: Distance> {
    index: &'index VectorIndex<D>,
    read: &'index R,
    observer: SearchObserver<'observer>,
}

impl<'index, 'observer, R, D> SearchSession<'index, 'observer, R, D>
where
    R: DbReadOps + Send + Sync,
    D: Distance,
{
    /// Binds one index, stable read view, and optional diagnostic destination.
    pub(crate) const fn new(
        index: &'index VectorIndex<D>,
        read: &'index R,
        observer: SearchObserver<'observer>,
    ) -> Self {
        Self {
            index,
            read,
            observer,
        }
    }

    /// Runs the single search implementation for this bound invocation.
    pub(crate) async fn run(
        &mut self,
        query: &[f32],
        params: &SearchParams,
    ) -> Result<Vec<SearchResult>, HelixDbError> {
        let metadata = self
            .index
            .get_metadata(self.read)
            .await?
            .ok_or_else(|| HelixDbError::IndexNotFound(self.index.name().to_string()))?;

        if query.len() != metadata.config.dimension {
            return Err(HelixDbError::InvalidDimension {
                expected: metadata.config.dimension,
                got: query.len(),
            });
        }
        if let Some(index) = query.iter().position(|component| !component.is_finite()) {
            return Err(HelixDbError::InvalidVectorComponent { index });
        }
        if matches!(
            ActiveVectorSemantics::for_distance::<D>().map(ActiveVectorSemantics::metric),
            Some(crate::encoding::v1::values::vector_generation::ActiveMetricKind::Cosine)
        ) && query.iter().all(|component| *component == 0.0)
        {
            return Err(HelixDbError::ZeroNormCosineVector);
        }

        let (entry_point, max_layer) = match metadata.validated_state()? {
            VectorIndexState::Empty => return Ok(Vec::new()),
            VectorIndexState::Populated {
                entry_point,
                max_layer,
            } => (entry_point, max_layer),
        };

        let vector = UnalignedVector::from_slice(query);
        let item: Item<'_, D> = Item {
            header: D::new_header(&vector),
            vector,
        };
        let query_simhash = if params.requires_query_simhash() {
            let simhash_cache = self.index.simhash_cache(metadata.config.dimension)?;
            Layer0QuerySimHash::Computed(
                simhash_cache
                    .simhasher()
                    .hash_from_slice(query)
                    .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?,
            )
        } else {
            Layer0QuerySimHash::UnusedExhaustive
        };
        let expected_dimension = VectorDimension::try_new(metadata.config.dimension)
            .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?;

        let mut entry = entry_point;
        for layer in (1..=max_layer).rev() {
            entry = self
                .index
                .search_layer_greedy(self.read, &item, entry, layer)
                .await?;
        }

        let (results, stats) = match (
            self.observer.is_collecting(),
            matches!(query_simhash, Layer0QuerySimHash::UnusedExhaustive),
        ) {
            (true, true) => {
                self.index
                    .search_layer0_with_simhash::<true, true>(
                        self.read,
                        &item,
                        &query_simhash,
                        expected_dimension,
                        entry,
                        params,
                        metadata.config.simhash_threshold,
                        metadata.config.sampling_ratio,
                        metadata.config.adaptive_enabled,
                        metadata.config.adaptive_failure_prob,
                    )
                    .await?
            }
            (true, false) => {
                self.index
                    .search_layer0_with_simhash::<true, false>(
                        self.read,
                        &item,
                        &query_simhash,
                        expected_dimension,
                        entry,
                        params,
                        metadata.config.simhash_threshold,
                        metadata.config.sampling_ratio,
                        metadata.config.adaptive_enabled,
                        metadata.config.adaptive_failure_prob,
                    )
                    .await?
            }
            (false, true) => {
                self.index
                    .search_layer0_with_simhash::<false, true>(
                        self.read,
                        &item,
                        &query_simhash,
                        expected_dimension,
                        entry,
                        params,
                        metadata.config.simhash_threshold,
                        metadata.config.sampling_ratio,
                        metadata.config.adaptive_enabled,
                        metadata.config.adaptive_failure_prob,
                    )
                    .await?
            }
            (false, false) => {
                self.index
                    .search_layer0_with_simhash::<false, false>(
                        self.read,
                        &item,
                        &query_simhash,
                        expected_dimension,
                        entry,
                        params,
                        metadata.config.simhash_threshold,
                        metadata.config.sampling_ratio,
                        metadata.config.adaptive_enabled,
                        metadata.config.adaptive_failure_prob,
                    )
                    .await?
            }
        };

        self.observer.publish(stats);
        Ok(results.into_iter().take(params.k()).collect())
    }
}

/// Optional diagnostics destination for one vector-search invocation.
pub(crate) struct SearchObserver<'a> {
    #[cfg(any(test, feature = "production-coverage"))]
    destination: Option<&'a mut SearchStats>,
    #[cfg(not(any(test, feature = "production-coverage")))]
    _destination: PhantomData<&'a mut SearchStats>,
}

impl SearchObserver<'_> {
    /// Creates an observer that discards the completed diagnostic snapshot.
    pub(crate) const fn disabled() -> Self {
        Self {
            #[cfg(any(test, feature = "production-coverage"))]
            destination: None,
            #[cfg(not(any(test, feature = "production-coverage")))]
            _destination: PhantomData,
        }
    }

    /// Reports whether this invocation requested the diagnostic snapshot.
    const fn is_collecting(&self) -> bool {
        #[cfg(any(test, feature = "production-coverage"))]
        {
            self.destination.is_some()
        }
        #[cfg(not(any(test, feature = "production-coverage")))]
        {
            false
        }
    }
}

impl<'a> SearchObserver<'a> {
    /// Borrows the caller-owned destination used by `search_with_stats`.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) fn collecting(destination: &'a mut SearchStats) -> Self {
        Self {
            destination: Some(destination),
        }
    }

    /// Publishes one completed layer-0 diagnostic snapshot when enabled.
    ///
    /// Validation failures and empty searches never call this method, leaving
    /// the caller's default snapshot unchanged exactly as the public contract
    /// requires.
    pub(crate) fn publish(&mut self, completed: SearchStats) {
        #[cfg(any(test, feature = "production-coverage"))]
        {
            let Some(destination) = &mut self.destination else {
                return;
            };
            **destination = completed;
        }
        #[cfg(not(any(test, feature = "production-coverage")))]
        {
            let _ = completed;
        }
    }
}

#[cfg(feature = "production-coverage")]
#[path = "../../../tests/production_support/vector/search.rs"]
pub(crate) mod production_contracts;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collecting_observer_publishes_while_disabled_observer_discards() {
        let mut destination = SearchStats::default();
        let completed = SearchStats {
            expansion_steps: 7,
            ..SearchStats::default()
        };

        SearchObserver::disabled().publish(completed.clone());
        assert_eq!(destination.expansion_steps, 0);

        SearchObserver::collecting(&mut destination).publish(completed);
        assert_eq!(destination.expansion_steps, 7);
    }
}
