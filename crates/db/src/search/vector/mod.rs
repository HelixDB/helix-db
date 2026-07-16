//! HNSW (Hierarchical Navigable Small World) vector index
//!
//! This module provides ANN (Approximate Nearest Neighbor) search capabilities
//! for f32 vector properties on graph nodes.
//!
//! # Architecture
//!
//! - Vector indexes are created on specific node properties (e.g., "embedding")
//! - Each index is independent with its own configuration (dimensions, metric, parameters)
//! - Multiple vector indexes can coexist on different properties
//! - Nodes can have multiple vector properties with different dimensions
//!
//! # Storage Layout
//!
//! Vector indexes use the following key-value layout:
//!
//! ## Index Metadata (default keyspace / persisted)
//! Key: `[0x03][0x03][index_id:8][kind:meta]`
//! Value: Serialized `VectorIndexMetadata`
//!
//! ## Layer 0 Neighbors (vector-hot keyspace / memory-hot)
//! Key: `[0xF0][index_id:8][kind:l0_vec_ks][node_id:8]`
//! Value: Compact packed neighbor snapshot (`count + NodeId list`, outgoing only)
//!
//! ## Vector Storage (vector-l0 keyspace / persisted)
//! Key: `[0xF1][index_id:8][kind:vec][order_code:8][node_id:8]`
//! Value: Serialized vector payload
//!
//! ## Entry Candidate Index (vector-l0 keyspace / persisted)
//! Sorted key: `[0xF1][index_id:8][kind:cand_sorted][inv_layer:2][node_id:8]`
//! Value: empty
//!
//! Node lookup key: `[0xF1][index_id:8][kind:cand_node][node_id:8]`
//! Value: `[layer:2]`
//!
//! ## Upper Layers (vector-hot keyspace / memory-hot)
//! Key: `[0xF0][index_id:8][kind:upper][layer:2][node_id:8]`
//! Value: Serialized upper-layer neighbor list
//!
//! ## Upper-Layer Vector Hot Cache (vector-hot keyspace / memory-hot)
//! Key: `[0xF0][index_id:8][kind:upper_vec][node_id:8]`
//! Value: Serialized vector payload (`Item`)
//!
//! ## SimHash Cache (vector-hot keyspace / memory-hot)
//! Key: `[0xF0][index_id:8][kind:simhash][node_id:8]`
//! Value: 8-byte SimHash
//!
//! ## Metadata Consistency Notes
//! - `entry_point` and `max_layer` in metadata are authoritative for online search behavior.
//! - `count` is advisory and may drift after incremental insert/delete operations.
//! - If an exact count is required, compute it from stored vectors rather than relying on
//!   metadata count.
//! - Monitoring or capacity logic that requires exact cardinality should not read `count`
//!   directly on hot indexes.
//!
//! # Validated public configuration
//!
//! ```
//! use db::config::VectorIndexDefinition;
//! use db::search::vector::{SearchParams, VectorDistanceMetric};
//!
//! let definition = VectorIndexDefinition::new_node(
//!     "Document",
//!     "embedding",
//!     128,
//!     VectorDistanceMetric::Cosine,
//! )?;
//! let params = SearchParams::new(10)?.with_ef(100)?;
//! assert_eq!(definition.dimension(), 128);
//! assert_eq!(params.k(), 10);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! Physical HNSW handles, deployed metadata DTOs, raw physical results, and
//! cache stores are crate-owned implementation details. These compile-fail
//! contracts prevent those lifecycle-bypassing surfaces from becoming public
//! again:
//!
//! ```compile_fail
//! use db::search::vector::VectorIndexConfig;
//! ```
//!
//! ```compile_fail
//! use db::search::vector::SearchResult;
//! ```
//!
//! ```compile_fail
//! use db::search::vector::VectorMemoryStore;
//! ```
//!
//! ```compile_fail
//! use db::search::vector::VectorIndex;
//! ```

mod configuration;
pub mod dimension;
pub mod distance;
#[cfg(feature = "production-coverage")]
#[path = "../../../tests/production_support/vector/distance_neighbors.rs"]
mod distance_neighbor_production_contracts;
mod generation;
mod hydration;
mod index;
pub mod item;
mod memory_registry;
mod memory_store;
mod model;
mod mutation;
mod neighbor_set;
pub mod parameters;
mod policy;
#[cfg(feature = "production-coverage")]
#[path = "../../../tests/production_support/vector/primitives.rs"]
mod primitive_production_contracts;
mod randomness;
#[cfg(feature = "production-coverage")]
#[path = "../../../tests/production_support/vector/read_fault.rs"]
pub(crate) mod read_fault_production_support;
mod read_index;
mod read_view;
mod result;
mod search;
pub mod simhash;
mod simhash_registry;
pub mod spaces;
mod storage;
pub mod unaligned_vector;
mod write_cache;
mod write_index;
mod write_transaction;

use std::collections::HashSet;
use std::num::NonZeroUsize;

use bytes::Bytes;
use rand::RngExt;
use serde::{Deserialize, Serialize};

use crate::error::HelixDbError;

#[cfg(test)]
use crate::encoding::v1::keys::vectors::{
    VectorEntryCandidateKey, VectorEntryCandidateNodeKey, VectorIndexMetadataKey,
    VectorIndexPrefixKey, VectorItemKey, VectorLayer0NeighborsKey, VectorTxnGuardKey,
    VectorUpperVectorKey,
};
use crate::encoding::v1::keys::vectors::{VectorKey, VectorMetadataScanPrefix};
#[cfg(test)]
use crate::encoding::v1::keys::vectors::{VectorReverseEdgeKey, VectorReverseEdgePrefixKey};
use crate::encoding::v1::values::vectors as vector_values;
use crate::encoding::NodeId;

pub use configuration::VectorConfigError;
pub(crate) use configuration::VectorIndexState;
pub use dimension::{SameDimensionPair, VectorDimension, VectorDimensionError, VectorRef};
pub use distance::Distance;
#[cfg(feature = "production-coverage")]
pub(crate) use distance_neighbor_production_contracts::run as run_distance_neighbor_contracts;
pub(crate) use generation::ValidatedVectorBuildGenerationHandle;
pub(crate) use generation::ValidatedVectorCleanupAuthority;
#[cfg(any(test, feature = "production-coverage"))]
pub(crate) use generation::VectorGenerationIdentity;
pub(crate) use generation::{ValidatedVectorGenerationHandle, VectorGenerationValidationError};
pub(crate) use hydration::{hydrate_active_generations, VectorCacheHydrationBudget};
#[cfg(feature = "production-coverage")]
pub(crate) use index::production_contracts::run as run_index_contracts;
pub(crate) use index::VectorIndex;
pub use item::Item;
#[cfg(feature = "production-coverage")]
pub(crate) use memory_registry::production_contracts::run as run_memory_registry_contracts;
pub(crate) use memory_registry::VectorCacheRegistry;
#[cfg(feature = "production-coverage")]
pub(crate) use memory_store::production_contracts::run as run_memory_store_contracts;
#[cfg(any(test, feature = "production-coverage"))]
pub(crate) use memory_store::VectorMemoryStore;
pub(crate) use model::Candidate;
#[cfg(feature = "production-coverage")]
pub(crate) use mutation::production_contracts::run as run_mutation_contracts;
pub use parameters::{
    CollisionThreshold, Connections, ConstructionBeamWidth, DistanceScore, FailureProbability,
    Layer0Connections, LayerMultiplier, ResultCount, SearchBeamWidth, UnitInterval,
    VectorParameterError,
};
#[cfg(feature = "production-coverage")]
pub(crate) use policy::production_contracts::run as run_policy_contracts;
#[cfg(feature = "production-coverage")]
pub(crate) use primitive_production_contracts::run as run_primitive_contracts;
#[cfg(feature = "production-coverage")]
pub(crate) use read_index::production_contracts::run as run_read_boundary_contracts;
pub(crate) use read_index::{ValidatedVectorReadIndex, VectorReadVisibility};
pub(crate) use read_view::VectorReadView;
pub use result::{
    DistanceOutputUnit, DistanceOutputVersion, MaterializedVectorDistance, VectorEntityId,
};
pub(crate) use result::{SearchResult, TypedVectorSearchResult};
#[cfg(feature = "production-coverage")]
pub(crate) use search::production_contracts::run as run_search_contracts;
#[cfg(feature = "production-coverage")]
pub(crate) use simhash::production_contracts::run as run_simhash_contracts;
pub use simhash::{
    SimHash, SimHashCache, SimHasher, DEFAULT_SIMHASH_COLLISION_THRESHOLD, SIMHASH_BITS,
};
#[cfg(feature = "production-coverage")]
pub(crate) use simhash_registry::production_contracts::run as run_simhash_registry_contracts;
pub(crate) use simhash_registry::{SimHashIdentity, SimHasherRegistry, SimHasherRegistryLimits};
#[cfg(feature = "production-coverage")]
pub(crate) use storage::production_contracts::run as run_storage_contracts;
pub(crate) use storage::VectorCleanupRow;
pub(crate) use vector_values::metadata::{VectorIndexConfig, VectorIndexMetadata};
pub(crate) use write_cache::VectorCacheWriteSet;
pub(crate) use write_index::managed_vector_write_index;
#[cfg(feature = "production-coverage")]
pub(crate) use write_transaction::production_contracts::run as run_write_transaction_contracts;
pub(crate) use write_transaction::{
    MeasuredVectorTransaction, VectorWriteMeasurement, VectorWriteRecorder,
};

/// Supported vector distance metrics
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum VectorDistanceMetric {
    /// Cosine similarity
    Cosine,
    /// Euclidean (L2) distance
    Euclidean,
    /// Manhattan (L1) distance
    Manhattan,
}

impl VectorDistanceMetric {
    /// Human-readable metric name
    pub fn as_str(self) -> &'static str {
        match self {
            VectorDistanceMetric::Cosine => "cosine",
            VectorDistanceMetric::Euclidean => "euclidean",
            VectorDistanceMetric::Manhattan => "manhattan",
        }
    }
}

#[cfg(any(test, feature = "production-coverage"))]
impl VectorIndexConfig {
    /// Create a new vector index configuration
    ///
    /// The distance type is determined by the generic parameter D when creating the index.
    pub(crate) fn new(
        index_name: impl Into<String>,
        property_name: impl Into<String>,
        dimension: usize,
    ) -> Self {
        let simhash_threshold = simhash::DEFAULT_SIMHASH_COLLISION_THRESHOLD;

        let m = DEFAULT_HNSW_M;
        Self {
            index_name: index_name.into(),
            property_name: property_name.into(),
            dimension,
            m,
            m0: m * 2,
            ef_construction: 200,
            ml: default_ml_for_m(m),
            simhash_threshold,
            // Paper uses ρ=0.8 for NVMe. Lower default (0.3) reduces S3 GETs;
            // adaptive_sampling_ratio() dynamically boosts when frontier is promising.
            sampling_ratio: 0.8,
            adaptive_enabled: true,
            adaptive_failure_prob: 0.1,
        }
    }

    /// Set HNSW M parameter
    pub(crate) fn with_m(mut self, m: usize) -> Self {
        self.m = m;
        self.ml = default_ml_for_m(m);
        self
    }

    /// Set HNSW level multiplier `ml`.
    ///
    /// The deployed DTO stores the exact supplied value. The crate-private
    /// validation boundary rejects non-finite and non-positive values before
    /// physical work can use it.
    #[cfg(test)]
    pub(crate) fn with_ml(mut self, ml: f32) -> Self {
        self.ml = ml;
        self
    }

    /// Set HNSW M0 parameter (layer 0 max connections)
    pub(crate) fn with_m0(mut self, m0: usize) -> Self {
        self.m0 = m0;
        self
    }

    /// Set HNSW ef_construction parameter
    pub(crate) fn with_ef_construction(mut self, ef_construction: usize) -> Self {
        self.ef_construction = ef_construction;
        self
    }

    /// Set SimHash threshold (number of matching bits required)
    #[cfg(test)]
    pub(crate) fn with_simhash_threshold(mut self, threshold: usize) -> Self {
        self.simhash_threshold = threshold;
        self
    }

    /// Set sampling ratio for layer 0 neighbors
    #[cfg(test)]
    pub(crate) fn with_sampling_ratio(mut self, ratio: f32) -> Self {
        self.sampling_ratio = ratio;
        self
    }

    /// Set failure probability for Hoeffding-style adaptive thresholding.
    ///
    /// The deployed DTO stores the exact supplied value. The crate-private
    /// validation boundary rejects non-finite values and values outside
    /// `(0, 1)` before physical work can use it.
    #[cfg(test)]
    pub(crate) fn with_adaptive_failure_prob(mut self, failure_prob: f32) -> Self {
        self.adaptive_failure_prob = failure_prob;
        self
    }
}

impl VectorIndexMetadata {
    /// Create new metadata from config
    pub(crate) fn new(config: VectorIndexConfig) -> Self {
        Self {
            config,
            entry_point: None,
            max_layer: 0,
            count: 0,
        }
    }
}

/// Search parameters for k-NN queries.
///
/// Layer-0 SimHash traversal currently runs in this order:
/// 1) optional stage-0 pre-SimHash frontier sampling on large frontiers,
/// 2) SimHash threshold gating (unless mode is `Off` or adaptive bypass activates),
/// 3) post-threshold probabilistic sampling before vector-distance fetch.
///
/// This is intentionally more performance-oriented than the paper/reference
/// "threshold first, then sampling" pipeline.
#[derive(Debug, Clone)]
pub struct SearchParams {
    /// Number of nearest neighbors to return
    k: ResultCount,

    /// Size of dynamic candidate list during search
    /// Typical values: k to 500. Higher = better recall, slower search
    /// Must be >= k
    ef: SearchBeamWidth,

    /// SimHash execution mode for layer-0 search.
    simhash_mode: SimHashMode,

    /// Optional override for stage-0 pre-SimHash sampling ratio.
    ///
    /// When set, this validated ratio is used directly before
    /// SimHash fetches on large frontiers.
    ///
    /// Important: this applies regardless of `simhash_mode`, including `Off`.
    /// For strict no-SimHash/no-sampling baselines, set this to `1.0`.
    pre_simhash_sampling_ratio_override: Option<UnitInterval>,

    /// Minimum frontier size required before adaptive SimHash bypass can activate.
    simhash_bypass_min_frontier: NonZeroUsize,

    /// Rolling expansion window used by adaptive SimHash bypass heuristics.
    simhash_bypass_window_expansions: NonZeroUsize,

    /// Minimum observed SimHash filter rate required to keep SimHash enabled.
    simhash_bypass_min_filter_rate: UnitInterval,

    /// SimHash read budget multiplier (`simhash_read_budget = ef * multiplier`).
    simhash_read_budget_multiplier: NonZeroUsize,

    /// Optional override for layer-0 SimHash sampling ratio.
    ///
    /// When set, this overrides index-level `sampling_ratio` for query-time
    /// probabilistic expansion decisions.
    simhash_sampling_ratio_override: Option<UnitInterval>,

    /// Optional override for SimHash Hoeffding failure probability (`epsilon`).
    ///
    /// When set, this value overrides index-level `adaptive_failure_prob` during
    /// query-time threshold calculation.
    simhash_failure_prob_override: Option<FailureProbability>,
}

/// SimHash execution mode for layer-0 search.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimHashMode {
    /// Always execute SimHash fetch + threshold filtering.
    ///
    /// Adaptive bypass is disabled in this mode.
    /// Stage-0 pre-SimHash sampling may still run if configured.
    Always,
    /// Execute SimHash by default, but adaptively bypass in short windows
    /// when read budget is exhausted or observed filter yield is low.
    ///
    /// Stage-0 pre-SimHash sampling may still run if configured.
    Adaptive,
    /// Skip SimHash fetch + threshold stage completely.
    ///
    /// Stage-0 pre-SimHash sampling can still run when
    /// `pre_simhash_sampling_ratio_override < 1.0`.
    ///
    /// Canonical vector keys are SimHash-derived, so searches in this mode may
    /// still read SimHash rows for key derivation after candidate selection.
    Off,
}

impl SearchParams {
    /// Create search parameters for a non-zero number of neighbors.
    pub fn new(k: usize) -> Result<Self, VectorParameterError> {
        let k = ResultCount::try_new(k)?;
        Ok(Self {
            k,
            ef: SearchBeamWidth::try_new(k.get().max(100), k)?,
            simhash_mode: SimHashMode::Adaptive,
            pre_simhash_sampling_ratio_override: None,
            simhash_bypass_min_frontier: NonZeroUsize::new(24)
                .expect("default SimHash bypass frontier is nonzero"),
            simhash_bypass_window_expansions: NonZeroUsize::new(4)
                .expect("default SimHash bypass window is nonzero"),
            simhash_bypass_min_filter_rate: UnitInterval::try_new(0.12)
                .expect("default SimHash filter rate is in the unit interval"),
            simhash_read_budget_multiplier: NonZeroUsize::new(3)
                .expect("default SimHash read budget multiplier is nonzero"),
            simhash_sampling_ratio_override: None,
            simhash_failure_prob_override: None,
        })
    }

    /// Returns the requested result count.
    pub const fn k(&self) -> usize {
        self.k.get()
    }

    /// Returns the effective search beam width, which is always at least `k`.
    pub const fn ef(&self) -> usize {
        self.ef.get()
    }

    /// Set the ef parameter
    pub fn with_ef(mut self, ef: usize) -> Result<Self, VectorParameterError> {
        self.ef = SearchBeamWidth::try_new(ef, self.k)?;
        Ok(self)
    }

    /// Set SimHash execution mode (`Off`, `Always`, `Adaptive`).
    ///
    /// If you want a strict baseline with no SimHash-path pruning, pair
    /// `SimHashMode::Off` with `.with_pre_simhash_sampling_ratio(1.0)`.
    pub fn with_simhash_mode(mut self, mode: SimHashMode) -> Self {
        self.simhash_mode = mode;
        self
    }

    /// Override stage-0 pre-SimHash sampling ratio.
    ///
    /// This runs before SimHash thresholding and applies in all modes.
    pub fn with_pre_simhash_sampling_ratio(
        mut self,
        ratio: f32,
    ) -> Result<Self, VectorParameterError> {
        self.pre_simhash_sampling_ratio_override = Some(UnitInterval::try_new(ratio)?);
        Ok(self)
    }

    /// Clear pre-SimHash sampling ratio override.
    pub fn clear_pre_simhash_sampling_ratio_override(mut self) -> Self {
        self.pre_simhash_sampling_ratio_override = None;
        self
    }

    /// Reports whether filtering or sampling can consume the query SimHash.
    ///
    /// `Off` is exhaustive unless its explicit pre-filter sampling override is
    /// below one. In that single closed state, layer zero never observes the
    /// query fingerprint, so search may omit its projection work entirely.
    pub(crate) fn requires_query_simhash(&self) -> bool {
        !matches!(self.simhash_mode, SimHashMode::Off)
            || self
                .pre_simhash_sampling_ratio_override
                .is_some_and(|ratio| ratio.get() < 1.0)
    }

    /// Configure adaptive SimHash bypass behavior.
    pub fn with_simhash_bypass_tuning(
        mut self,
        min_frontier: usize,
        window_expansions: usize,
        min_filter_rate: f32,
        read_budget_multiplier: usize,
    ) -> Result<Self, VectorParameterError> {
        let Some(min_frontier) = NonZeroUsize::new(min_frontier) else {
            return Err(VectorParameterError::Zero {
                parameter: "SimHash bypass minimum frontier",
            });
        };
        let Some(window_expansions) = NonZeroUsize::new(window_expansions) else {
            return Err(VectorParameterError::Zero {
                parameter: "SimHash bypass window expansions",
            });
        };
        let Some(read_budget_multiplier) = NonZeroUsize::new(read_budget_multiplier) else {
            return Err(VectorParameterError::Zero {
                parameter: "SimHash read budget multiplier",
            });
        };
        self.simhash_bypass_min_frontier = min_frontier;
        self.simhash_bypass_window_expansions = window_expansions;
        self.simhash_bypass_min_filter_rate = UnitInterval::try_new(min_filter_rate)?;
        self.simhash_read_budget_multiplier = read_budget_multiplier;
        Ok(self)
    }

    /// Override SimHash sampling ratio for this query.
    pub fn with_simhash_sampling_ratio(mut self, ratio: f32) -> Result<Self, VectorParameterError> {
        self.simhash_sampling_ratio_override = Some(UnitInterval::try_new(ratio)?);
        Ok(self)
    }

    /// Clear per-query SimHash sampling ratio override.
    pub fn clear_simhash_sampling_ratio_override(mut self) -> Self {
        self.simhash_sampling_ratio_override = None;
        self
    }

    /// Override SimHash failure probability (`epsilon`) for this query.
    pub fn with_simhash_failure_prob(
        mut self,
        failure_prob: f32,
    ) -> Result<Self, VectorParameterError> {
        self.simhash_failure_prob_override = Some(FailureProbability::try_new(failure_prob)?);
        Ok(self)
    }

    /// Clear per-query SimHash failure probability override.
    pub fn clear_simhash_failure_prob_override(mut self) -> Self {
        self.simhash_failure_prob_override = None;
        self
    }

    /// Throughput-oriented preset for workloads with recall floor ~= 0.92.
    pub fn throughput_profile_floor_92(k: usize) -> Result<Self, VectorParameterError> {
        let params = Self::new(k)?.with_ef(k.max(48))?;
        let params = params
            .with_simhash_mode(SimHashMode::Adaptive)
            .with_pre_simhash_sampling_ratio(0.20)?;
        params.with_simhash_bypass_tuning(24, 4, 0.12, 3)
    }
}

/// Diagnostics from a layer-0 search pass.
#[derive(Debug, Default, Clone)]
#[allow(
    dead_code,
    reason = "search updates a closed diagnostic snapshot; production-coverage contracts, rather than every build mode, observe every counter"
)]
pub(crate) struct SearchStats {
    /// Number of candidate-pop expansion iterations in layer-0 traversal.
    pub expansion_steps: usize,
    /// Number of layer-0 neighbor IDs examined before filtering.
    pub neighbors_examined: usize,
    /// Number of neighbors rejected by SimHash thresholding.
    pub simhash_filtered: usize,
    /// Number of neighbors evaluated against SimHash thresholding.
    pub simhash_examined: usize,
    /// Number of neighbors missing SimHash value when SimHash was expected.
    pub simhash_missing_hash: usize,
    /// Number of neighbors that passed SimHash threshold before sampling.
    pub simhash_passed_before_sampling: usize,
    /// Number of neighbors retained after sampling.
    pub simhash_passed_after_sampling: usize,
    /// Number of vectors materialized for distance scoring.
    pub vectors_loaded: usize,
    /// Backwards-compatible logical read count (mirrors `txn_get_total`).
    pub object_store_reads: usize,
    /// Total transactional point reads (`txn.get`) performed.
    pub txn_get_total: usize,
    /// Transactional reads initiated by neighbor-list fetch phase.
    pub txn_get_neighbors: usize,
    /// Transactional reads initiated by SimHash fetch phase.
    pub txn_get_simhash: usize,
    /// Transactional reads initiated by SimHash filtering fetches.
    pub txn_get_simhash_filter: usize,
    /// Transactional reads initiated by canonical-key derivation from SimHash.
    pub txn_get_simhash_key_derivation: usize,
    /// Transactional reads initiated by vector fetch phase.
    pub txn_get_vectors: usize,
    /// Number of `txn.multi_get` API calls issued during this query.
    pub txn_multi_get_calls_total: usize,
    /// Number of `txn.multi_get` API calls issued during SimHash fetch.
    pub txn_multi_get_calls_simhash: usize,
    /// Number of `txn.multi_get` API calls issued for SimHash filtering fetches.
    pub txn_multi_get_calls_simhash_filter: usize,
    /// Number of `txn.multi_get` API calls issued for SimHash key-derivation fetches.
    pub txn_multi_get_calls_simhash_key_derivation: usize,
    /// Number of `txn.multi_get` API calls issued during vector fetch.
    pub txn_multi_get_calls_vectors: usize,
    /// Time spent in neighbor-list fetch phase (nanoseconds).
    pub neighbors_fetch_ns: u64,
    /// Time spent in SimHash fetch phase (nanoseconds).
    pub simhash_fetch_ns: u64,
    /// Time spent fetching SimHash for filtering (nanoseconds).
    pub simhash_fetch_ns_filter: u64,
    /// Time spent fetching SimHash for canonical-key derivation (nanoseconds).
    pub simhash_fetch_ns_key_derivation: u64,
    /// Time spent in vector fetch phase (nanoseconds).
    pub vector_fetch_ns: u64,
    /// Time spent in distance computation phase (nanoseconds).
    pub distance_compute_ns: u64,
    /// Number of distance computations performed.
    pub distance_computations: usize,
    /// Number of expansions where SimHash stage was bypassed.
    pub simhash_bypass_expansions: usize,
    /// Number of candidates that skipped SimHash reads due to bypass.
    pub simhash_skipped_candidates: usize,
    /// Number of candidates kept after pre-SimHash sampling.
    pub pre_simhash_sample_kept: usize,
    /// Number of candidates dropped by pre-SimHash sampling.
    pub pre_simhash_sample_dropped: usize,
    /// Number of adaptive bypass activations caused by read-budget exhaustion.
    pub simhash_bypass_trigger_budget: usize,
    /// Number of adaptive bypass activations caused by low filter-yield.
    pub simhash_bypass_trigger_low_yield: usize,
    /// Average active SimHash threshold (bits) used during query traversal.
    pub avg_active_simhash_threshold: f32,
    /// Average active sampling ratio used during query traversal.
    pub avg_active_sampling_ratio: f32,
    /// Average effective beam length (`w.len + virtual_fill_slots`) during traversal.
    pub avg_effective_beam_len: f32,
}

const DEFAULT_HNSW_M: usize = 16;

#[inline]
pub(crate) fn default_ml_for_m(m: usize) -> f32 {
    let effective_m = m.max(2) as f32;
    1.0 / effective_m.ln()
}

/// Compute a stable 64-bit index identifier from index name.
#[inline]
pub fn index_id_from_name(name: &str) -> u64 {
    xxhash_rust::xxh3::xxh3_64(name.as_bytes())
}

// =============================================================================
// Key Space Layout for Vector Indexes
// =============================================================================

/// Create the current core-vector prefix used to discover metadata rows.
///
/// The deployed key layout places the row kind after the hashed index ID, so
/// this prefix also covers non-metadata core rows. Every result must be parsed
/// and filtered; bounded callers must count those unrelated rows as scan work.
pub fn make_vector_index_metadata_scan_prefix() -> Bytes {
    VectorMetadataScanPrefix::new().to_bytes()
}

pub fn is_vector_index_metadata_key(key: &[u8]) -> bool {
    matches!(
        VectorKey::parse_from_slice(key),
        Ok(VectorKey::IndexMetadata(_))
    )
}

/// Encode entry-candidate node layer as `[layer:2]`.
pub fn encode_entry_candidate_layer(layer: u16) -> Bytes {
    vector_values::entry::encode_entry_candidate_layer(layer)
}

/// Decode entry-candidate node layer from `[layer:2]`.
pub fn decode_entry_candidate_layer(data: &[u8]) -> Result<u16, String> {
    vector_values::entry::decode_entry_candidate_layer(data).map_err(|_| {
        format!(
            "Invalid entry-candidate layer length: expected 2 bytes, got {}",
            data.len()
        )
    })
}

// =============================================================================
// Layer Selection
// =============================================================================

/// Select a random layer for a new node using exponential decay
///
/// The probability of being at layer l is: P(l) = exp(-l / ml)
/// This creates a hierarchical structure where higher layers have fewer nodes.
///
/// # Arguments
/// * `ml` - Maximum layer multiplier (typically 1/ln(m); with m=16, ≈0.36)
/// * `rng` - Random number generator
///
/// # Returns
/// Layer number (0 = bottom layer, higher numbers = upper layers).
///
/// The sampled value is clamped away from 0 to avoid `ln(0)` pathologies,
/// and the final layer is capped to keep insertion/build loops bounded.
pub fn select_layer(ml: f32, rng: &mut impl rand::Rng) -> u16 {
    let uniform: f32 = rng.random();
    select_layer_from_uniform(ml, uniform)
}

const MAX_SELECTED_LAYER: u16 = 63;

#[inline]
fn select_layer_from_uniform(ml: f32, uniform: f32) -> u16 {
    let ml = if ml.is_finite() && ml > 0.0 {
        ml
    } else {
        default_ml_for_m(DEFAULT_HNSW_M)
    };

    let uniform = if uniform.is_finite() {
        uniform.clamp(f32::MIN_POSITIVE, 1.0 - f32::EPSILON)
    } else {
        0.5
    };

    let sampled = (-uniform.ln() * ml).floor();
    if !sampled.is_finite() || sampled <= 0.0 {
        return 0;
    }

    sampled.min(MAX_SELECTED_LAYER as f32) as u16
}

// =============================================================================
// Diversity-aware neighbor selection (HNSW Algorithm 4)
// =============================================================================

/// HNSW Algorithm 4: select up to `m` diverse neighbors.
///
/// `candidates` must be sorted by distance to `ref_item` (closest first).
/// `get_item` resolves a NodeId to its Item for inter-candidate distance checks.
/// Falls back to the closest remaining resolvable items if diversity filtering
/// yields fewer than `m` results. Missing item rows are never persisted as new
/// graph neighbors.
pub(crate) fn select_diverse<'a, D: Distance>(
    ref_item: &Item<'_, D>,
    candidates: &[Candidate],
    get_item: &dyn Fn(NodeId) -> Option<&'a Item<'static, D>>,
    m: usize,
) -> Result<Vec<NodeId>, HelixDbError> {
    let _ = ref_item; // ref_item context is captured in candidate distances
    let mut selected: Vec<NodeId> = Vec::with_capacity(m);
    let mut selected_ids: HashSet<NodeId> = HashSet::with_capacity(m);
    let mut selected_items: Vec<&'a Item<'static, D>> = Vec::with_capacity(m);

    for c in candidates {
        if selected.len() >= m {
            break;
        }
        let Some(c_item) = get_item(c.node_id) else {
            continue;
        };

        let mut is_diverse = true;
        for selected_item in &selected_items {
            let pair_distance =
                Candidate::try_new(c.node_id, D::distance(c_item, selected_item))?.score();
            if pair_distance < c.score() {
                is_diverse = false;
                break;
            }
        }
        if is_diverse {
            selected.push(c.node_id);
            selected_ids.insert(c.node_id);
            selected_items.push(c_item);
        }
    }

    // Backfill with closest remaining if diversity was too aggressive
    if selected.len() < m {
        for c in candidates {
            if selected.len() >= m {
                break;
            }
            if get_item(c.node_id).is_some() && selected_ids.insert(c.node_id) {
                selected.push(c.node_id);
            }
        }
    }
    Ok(selected)
}

// =============================================================================
// Serialization helpers
// =============================================================================

/// Serialize a vector index metadata
pub(crate) fn encode_metadata(metadata: &VectorIndexMetadata) -> rkyv::util::AlignedVec<16> {
    vector_values::metadata::encode_metadata(metadata)
}

/// Deserialize vector index metadata
pub(crate) fn decode_metadata(data: &[u8]) -> Result<VectorIndexMetadata, String> {
    vector_values::metadata::decode_metadata(data).map_err(|error| error.to_string())
}

/// Serialize an Item<D> (header + vector)
pub fn encode_item<D: Distance>(item: &Item<D>) -> Bytes {
    use bytemuck::bytes_of;

    vector_values::item::encode_item_parts(bytes_of(&item.header), item.vector.as_bytes())
}

/// Decodes a borrowed current-format item under an authoritative dimension.
///
/// Callers must obtain `expected_dimension` from validated index metadata (and,
/// once generation publication is active, from the validated generation
/// handle) before reading any vector row. The decoder keeps the existing row
/// format unchanged while rejecting rows whose payload length, finite-value
/// invariant, or metric-specific header does not match that binding.
///
/// Keeping this function crate-private prevents unbound bytes from becoming a
/// usable [`Item`]. Use [`VectorIndex::get_item`] through normal index code.
pub(crate) fn decode_item_borrowed<'a, D: Distance>(
    data: &'a [u8],
    expected_dimension: VectorDimension,
) -> Result<Item<'a, D>, VectorItemDecodeError> {
    use bytemuck::pod_read_unaligned;

    let header_size = std::mem::size_of::<D::Header>();

    let parts = vector_values::item::split_item_parts(data, header_size).map_err(|_| {
        VectorItemDecodeError::HeaderTooShort {
            expected: header_size,
            actual: data.len(),
        }
    })?;

    // Deserialize header
    let header = pod_read_unaligned::<D::Header>(parts.header());

    let vector = unaligned_vector::UnalignedVector::<D::VectorCodec>::from_bytes(parts.payload())
        .map_err(|error| VectorItemDecodeError::InvalidPayload(error.to_string()))?;

    let word_size = <D::VectorCodec as unaligned_vector::UnalignedVectorCodec>::word_size();
    let Some(word_adjustment) = word_size.checked_sub(1) else {
        return Err(VectorItemDecodeError::ZeroCodecWordSize);
    };
    let Some(expected_words) = expected_dimension.get().checked_add(word_adjustment) else {
        return Err(VectorItemDecodeError::DimensionArithmeticOverflow);
    };
    let Some(expected_encoded_dimension) = (expected_words / word_size).checked_mul(word_size)
    else {
        return Err(VectorItemDecodeError::DimensionArithmeticOverflow);
    };
    if vector.len() != expected_encoded_dimension {
        return Err(VectorItemDecodeError::DimensionMismatch {
            expected: expected_encoded_dimension,
            actual: vector.len(),
        });
    }
    for (index, value) in vector.iter().take(expected_dimension.get()).enumerate() {
        if !value.is_finite() {
            return Err(VectorItemDecodeError::NonFiniteComponent { index });
        }
    }

    let expected_header = D::new_header(&vector);
    if bytemuck::bytes_of(&header) != bytemuck::bytes_of(&expected_header) {
        return Err(VectorItemDecodeError::HeaderMismatch);
    }

    Ok(Item { header, vector })
}

/// Decodes an owned current-format item under an authoritative dimension.
///
/// This is the owned counterpart to [`decode_item_borrowed`]. It performs the
/// same fail-closed checks before copying the vector into owned storage.
pub(crate) fn decode_item<D: Distance>(
    data: &[u8],
    expected_dimension: VectorDimension,
) -> Result<Item<'static, D>, VectorItemDecodeError> {
    let item = decode_item_borrowed::<D>(data, expected_dimension)?;
    Ok(Item {
        header: item.header,
        vector: std::borrow::Cow::Owned(item.vector.into_owned()),
    })
}

/// Why current-format vector item bytes could not be bound to an index.
///
/// These errors describe corruption or a semantic mismatch between stored row
/// bytes and validated index metadata. They are separate from the rkyv metadata
/// codec because vector item rows use their existing header-plus-payload layout.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum VectorItemDecodeError {
    /// The row ended before its metric-specific header was complete.
    #[error("vector item header too short: expected {expected} bytes, got {actual}")]
    HeaderTooShort {
        /// Header size required by the bound metric.
        expected: usize,
        /// Bytes available in the row.
        actual: usize,
    },
    /// The codec could not interpret the payload bytes.
    #[error("invalid vector item payload: {0}")]
    InvalidPayload(String),
    /// Computing the codec-padded dimension overflowed `usize`.
    #[error("vector item dimension arithmetic overflow")]
    DimensionArithmeticOverflow,
    /// The selected codec violated its non-zero word-size contract.
    #[error("vector item codec word size must be non-zero")]
    ZeroCodecWordSize,
    /// The encoded payload length did not match the bound logical dimension.
    #[error("vector item dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch {
        /// Encoded component count required by the logical dimension and codec.
        expected: usize,
        /// Encoded component count present in the row.
        actual: usize,
    },
    /// A logical vector component was NaN or infinite.
    #[error("vector item component {index} is not finite")]
    NonFiniteComponent {
        /// Zero-based logical component offset.
        index: usize,
    },
    /// The stored header was not the canonical header for the payload and metric.
    #[error("vector item header does not match its payload and bound metric semantic")]
    HeaderMismatch,
}

/// Serialize neighbor list (Vec<NodeId>)
pub fn encode_neighbors(neighbors: &[NodeId]) -> Bytes {
    vector_values::neighbors::encode_flat_neighbors(neighbors)
}

/// Deserialize neighbor list
pub fn decode_neighbors(data: &[u8]) -> Result<Vec<NodeId>, String> {
    vector_values::neighbors::decode_flat_neighbors(data)
        .map_err(|_| "Invalid neighbor data length".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytemuck::{Pod, Zeroable};
    use rand::SeedableRng;
    use std::borrow::Cow;
    use std::collections::HashMap;

    #[derive(Debug, Clone)]
    enum BinaryTestDistance {}

    #[repr(C)]
    #[derive(Clone, Copy, Debug, Pod, Zeroable)]
    struct BinaryTestHeader {
        marker: u32,
    }

    impl Distance for BinaryTestDistance {
        type Header = BinaryTestHeader;
        type VectorCodec = unaligned_vector::Binary;

        fn name() -> &'static str {
            "binary-test"
        }

        fn new_header(
            _vector: &unaligned_vector::UnalignedVector<Self::VectorCodec>,
        ) -> Self::Header {
            BinaryTestHeader { marker: 7 }
        }

        fn distance(_p: &Item<Self>, _q: &Item<Self>) -> f32 {
            0.0
        }

        fn norm_no_header(_v: &unaligned_vector::UnalignedVector<Self::VectorCodec>) -> f32 {
            0.0
        }
    }

    impl distance::sealed::Sealed for BinaryTestDistance {}

    #[test]
    fn test_config_builder() {
        let config = VectorIndexConfig::new("test_idx", "embedding", 128)
            .with_m(32)
            .with_ef_construction(400);

        assert_eq!(config.index_name, "test_idx");
        assert_eq!(config.property_name, "embedding");
        assert_eq!(config.dimension, 128);
        assert_eq!(config.m, 32);
        assert!((config.ml - default_ml_for_m(32)).abs() < 1e-6);
        assert_eq!(config.ef_construction, 400);
        assert_eq!(
            config.simhash_threshold,
            DEFAULT_SIMHASH_COLLISION_THRESHOLD
        );
        assert!(config.adaptive_enabled);
        assert!((config.adaptive_failure_prob - 0.1).abs() < f32::EPSILON);
    }

    #[test]
    fn test_metric_names_and_raw_config_does_not_repair_invalid_m0() {
        assert_eq!(VectorDistanceMetric::Cosine.as_str(), "cosine");
        assert_eq!(VectorDistanceMetric::Euclidean.as_str(), "euclidean");
        assert_eq!(VectorDistanceMetric::Manhattan.as_str(), "manhattan");

        let mut config = VectorIndexConfig::new("idx", "embedding", 8);
        config.m0 = 0;
        let invalid = config.with_m(12);
        assert_eq!(invalid.m0, 0);
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn test_config_builder_with_ml_override() {
        let config = VectorIndexConfig::new("idx", "embedding", 8)
            .with_m(24)
            .with_ml(0.25);
        assert!((config.ml - 0.25).abs() < f32::EPSILON);

        let invalid = VectorIndexConfig::new("idx", "embedding", 8)
            .with_m(24)
            .with_ml(f32::NAN);
        assert!(invalid.ml.is_nan());
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn test_raw_config_preserves_invalid_probabilities_for_rejection() {
        let invalid_low =
            VectorIndexConfig::new("idx", "embedding", 8).with_adaptive_failure_prob(0.0);
        let invalid_high =
            VectorIndexConfig::new("idx", "embedding", 8).with_adaptive_failure_prob(1.0);
        let non_finite =
            VectorIndexConfig::new("idx", "embedding", 8).with_adaptive_failure_prob(f32::NAN);

        assert_eq!(invalid_low.adaptive_failure_prob, 0.0);
        assert_eq!(invalid_high.adaptive_failure_prob, 1.0);
        assert!(non_finite.adaptive_failure_prob.is_nan());
        assert!(invalid_low.validate().is_err());
        assert!(invalid_high.validate().is_err());
        assert!(non_finite.validate().is_err());
    }

    #[test]
    fn test_raw_config_preserves_invalid_sampling_ratio_for_rejection() {
        let out_of_range = VectorIndexConfig::new("idx", "embedding", 8).with_sampling_ratio(2.0);
        let non_finite =
            VectorIndexConfig::new("idx", "embedding", 8).with_sampling_ratio(f32::NAN);

        assert_eq!(out_of_range.sampling_ratio, 2.0);
        assert!(non_finite.sampling_ratio.is_nan());
        assert!(out_of_range.validate().is_err());
        assert!(non_finite.validate().is_err());
    }

    #[test]
    fn test_search_params() {
        let params = SearchParams::new(10).unwrap().with_ef(100).unwrap();
        assert_eq!(params.k(), 10);
        assert_eq!(params.ef(), 100);

        assert!(SearchParams::new(0).is_err());
        assert!(SearchParams::new(100).unwrap().with_ef(50).is_err());
        assert!(SearchParams::new(10)
            .unwrap()
            .with_pre_simhash_sampling_ratio(2.0)
            .is_err());
        assert!(SearchParams::new(10)
            .unwrap()
            .with_simhash_bypass_tuning(0, 4, 0.12, 3)
            .is_err());
        assert!(SearchParams::new(10)
            .unwrap()
            .with_simhash_bypass_tuning(24, 0, 0.12, 3)
            .is_err());
        assert!(SearchParams::new(10)
            .unwrap()
            .with_simhash_bypass_tuning(24, 4, 0.12, 0)
            .is_err());
        assert!(SearchParams::new(10)
            .unwrap()
            .with_simhash_sampling_ratio(f32::NAN)
            .is_err());
        assert!(SearchParams::new(10)
            .unwrap()
            .with_simhash_failure_prob(1.0)
            .is_err());

        let tuned = SearchParams::new(10)
            .unwrap()
            .with_simhash_mode(SimHashMode::Off)
            .with_pre_simhash_sampling_ratio(0.75)
            .unwrap()
            .with_simhash_bypass_tuning(2, 3, 0.25, 4)
            .unwrap()
            .with_simhash_sampling_ratio(0.5)
            .unwrap()
            .with_simhash_failure_prob(0.2)
            .unwrap();
        assert_eq!(tuned.simhash_mode, SimHashMode::Off);
        assert_eq!(
            tuned
                .pre_simhash_sampling_ratio_override
                .map(UnitInterval::get),
            Some(0.75)
        );
        assert_eq!(tuned.simhash_bypass_min_frontier.get(), 2);
        assert_eq!(tuned.simhash_bypass_window_expansions.get(), 3);
        assert_eq!(tuned.simhash_bypass_min_filter_rate.get(), 0.25);
        assert_eq!(tuned.simhash_read_budget_multiplier.get(), 4);
        assert_eq!(
            tuned.simhash_sampling_ratio_override.map(UnitInterval::get),
            Some(0.5)
        );
        assert_eq!(
            tuned
                .simhash_failure_prob_override
                .map(FailureProbability::get),
            Some(0.2)
        );

        let cleared = tuned
            .clear_pre_simhash_sampling_ratio_override()
            .clear_simhash_sampling_ratio_override()
            .clear_simhash_failure_prob_override();
        assert_eq!(cleared.pre_simhash_sampling_ratio_override, None);
        assert_eq!(cleared.simhash_sampling_ratio_override, None);
        assert_eq!(cleared.simhash_failure_prob_override, None);

        let profile = SearchParams::throughput_profile_floor_92(10).unwrap();
        assert_eq!(profile.ef(), 48);
        assert_eq!(profile.simhash_mode, SimHashMode::Adaptive);
        assert_eq!(
            profile
                .pre_simhash_sampling_ratio_override
                .map(UnitInterval::get),
            Some(0.2)
        );
    }

    #[test]
    fn test_decoders_reject_invalid_inputs() {
        assert!(decode_entry_candidate_layer(&[0]).is_err());
        assert_eq!(decode_metadata(&[]).unwrap_err(), "Empty metadata data");
        assert!(matches!(
            decode_item::<BinaryTestDistance>(&[0; 3], VectorDimension::try_new(8).unwrap()),
            Err(VectorItemDecodeError::HeaderTooShort { .. })
        ));
        assert_eq!(
            decode_neighbors(&[0]).unwrap_err(),
            "Invalid neighbor data length"
        );
    }

    #[test]
    fn test_neighbors_encoding() {
        let neighbors = vec![1, 42, 1000, u64::MAX - 1];
        let encoded = encode_neighbors(&neighbors);
        let decoded = decode_neighbors(&encoded).unwrap();

        assert_eq!(neighbors, decoded);
    }

    #[test]
    fn test_metadata_encoding() {
        let config = VectorIndexConfig::new("test", "emb", 64);
        let metadata = VectorIndexMetadata::new(config);

        let encoded = encode_metadata(&metadata);
        let decoded = decode_metadata(&encoded).unwrap();

        assert_eq!(metadata.config.index_name, decoded.config.index_name);
        assert_eq!(metadata.config.dimension, decoded.config.dimension);
    }

    #[test]
    fn test_item_encoding_roundtrip_binary_codec() {
        let vector = unaligned_vector::UnalignedVector::<unaligned_vector::Binary>::from_slice(&[
            1.0, -1.0, 3.0, 0.0, 2.0, -5.0, 0.0, 1.0,
        ]);
        assert_eq!(BinaryTestDistance::name(), "binary-test");
        assert_eq!(BinaryTestDistance::new_header(&vector).marker, 7);
        assert_eq!(BinaryTestDistance::norm_no_header(&vector), 0.0);
        let item = Item::<BinaryTestDistance> {
            header: BinaryTestDistance::new_header(&vector),
            vector: std::borrow::Cow::Owned(vector.into_owned()),
        };
        assert_eq!(BinaryTestDistance::distance(&item, &item), 0.0);

        let encoded = encode_item(&item);
        let decoded =
            decode_item::<BinaryTestDistance>(&encoded, VectorDimension::try_new(8).unwrap())
                .unwrap();

        assert_eq!(decoded.header.marker, 7);
        assert_eq!(decoded.vector.as_bytes(), item.vector.as_bytes());
    }

    #[test]
    fn test_item_encoding_roundtrip_f32_codec() {
        let vector = unaligned_vector::UnalignedVector::<f32>::from_slice(&[1.0, -2.0, 0.5, 3.25]);
        let item = Item::<distance::Cosine> {
            header: distance::Cosine::new_header(&vector),
            vector: std::borrow::Cow::Owned(vector.into_owned()),
        };

        let encoded = encode_item(&item);
        let decoded =
            decode_item::<distance::Cosine>(&encoded, VectorDimension::try_new(4).unwrap())
                .unwrap();

        assert_eq!(
            bytemuck::bytes_of(&decoded.header),
            bytemuck::bytes_of(&item.header)
        );
        assert_eq!(decoded.vector.as_bytes(), item.vector.as_bytes());
    }

    #[test]
    fn item_decoder_rejects_dimension_finiteness_header_and_trailing_payload_corruption() {
        let vector = unaligned_vector::UnalignedVector::<f32>::from_slice(&[1.0, 2.0, 3.0]);
        let item = Item::<distance::Cosine> {
            header: distance::Cosine::new_header(&vector),
            vector,
        };
        let encoded = encode_item(&item);
        let dimension = VectorDimension::try_new(3).unwrap();

        assert!(matches!(
            decode_item::<distance::Cosine>(&encoded, VectorDimension::try_new(2).unwrap()),
            Err(VectorItemDecodeError::DimensionMismatch {
                expected: 2,
                actual: 3
            })
        ));

        let mut trailing = encoded.to_vec();
        trailing.extend_from_slice(&4.0_f32.to_ne_bytes());
        assert!(matches!(
            decode_item::<distance::Cosine>(&trailing, dimension),
            Err(VectorItemDecodeError::DimensionMismatch {
                expected: 3,
                actual: 4
            })
        ));

        let mut non_finite = encoded.to_vec();
        let header_size = core::mem::size_of::<<distance::Cosine as Distance>::Header>();
        non_finite[header_size..header_size + core::mem::size_of::<f32>()]
            .copy_from_slice(&f32::NAN.to_ne_bytes());
        assert!(matches!(
            decode_item::<distance::Cosine>(&non_finite, dimension),
            Err(VectorItemDecodeError::NonFiniteComponent { index: 0 })
        ));

        let mut wrong_header = encoded.to_vec();
        wrong_header[0] ^= 1;
        assert!(matches!(
            decode_item::<distance::Cosine>(&wrong_header, dimension),
            Err(VectorItemDecodeError::HeaderMismatch)
        ));
    }

    #[test]
    fn test_entry_candidate_layer_encoding() {
        let layer = 321u16;
        let encoded = encode_entry_candidate_layer(layer);
        let decoded = decode_entry_candidate_layer(&encoded).unwrap();
        assert_eq!(decoded, layer);
    }

    #[test]
    fn test_entry_candidate_key_roundtrip() {
        let index_id = index_id_from_name("idx");
        let key = VectorKey::EntryCandidateSorted(VectorEntryCandidateKey::new(index_id, 12, 99))
            .to_bytes();
        let VectorKey::EntryCandidateSorted(parsed) = VectorKey::parse_from_slice(&key).unwrap()
        else {
            panic!("entry candidate key should parse as EntryCandidateSorted");
        };
        assert_eq!(parsed.layer(), 12);
        assert_eq!(parsed.node_id(), 99);
    }

    #[test]
    fn test_entry_candidate_keys_live_in_vector_l0_keyspace() {
        let index_id = index_id_from_name("idx");
        let sorted_key =
            VectorKey::EntryCandidateSorted(VectorEntryCandidateKey::new(index_id, 3, 7))
                .to_bytes();
        let node_key =
            VectorKey::EntryCandidateNode(VectorEntryCandidateNodeKey::new(index_id, 7)).to_bytes();

        assert!(matches!(
            VectorKey::parse_from_slice(&sorted_key).unwrap(),
            VectorKey::EntryCandidateSorted(_)
        ));
        assert!(matches!(
            VectorKey::parse_from_slice(&node_key).unwrap(),
            VectorKey::EntryCandidateNode(_)
        ));
    }

    #[test]
    fn test_index_id_deterministic() {
        let id_a = index_id_from_name("idx");
        let id_b = index_id_from_name("idx");
        let id_c = index_id_from_name("other_idx");

        assert_eq!(id_a, id_b);
        assert_ne!(id_a, id_c);
    }

    #[test]
    fn test_v2_key_lengths() {
        let index_id = index_id_from_name("idx");
        let meta_key = VectorKey::IndexMetadata(VectorIndexMetadataKey::new(index_id)).to_bytes();
        let vec_key = VectorKey::Vector(VectorItemKey::new(index_id, 0, 7)).to_bytes();
        let l0_key =
            VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(index_id, 7)).to_bytes();
        let upper_vec_key =
            VectorKey::UpperVector(VectorUpperVectorKey::new(index_id, 7)).to_bytes();
        let cand_sorted_key =
            VectorKey::EntryCandidateSorted(VectorEntryCandidateKey::new(index_id, 3, 7))
                .to_bytes();
        let cand_node_key =
            VectorKey::EntryCandidateNode(VectorEntryCandidateNodeKey::new(index_id, 7)).to_bytes();
        let txn_guard_key = VectorKey::TxnGuard(VectorTxnGuardKey::new(index_id)).to_bytes();
        let idx_prefix = VectorKey::IndexPrefix(VectorIndexPrefixKey::new(index_id)).to_bytes();
        let reverse_edge_prefix =
            VectorKey::ReverseEdgePrefix(VectorReverseEdgePrefixKey::new(index_id, 7)).to_bytes();
        let reverse_edge_key =
            VectorKey::ReverseEdge(VectorReverseEdgeKey::new(index_id, 7, 3, 9)).to_bytes();

        assert_eq!(meta_key.len(), 11);
        assert_eq!(vec_key.len(), 26);
        assert_eq!(l0_key.len(), 18);
        assert_eq!(upper_vec_key.len(), 18);
        assert_eq!(cand_sorted_key.len(), 20);
        assert_eq!(cand_node_key.len(), 18);
        assert_eq!(txn_guard_key.len(), 11);
        assert_eq!(idx_prefix.len(), 10);
        assert_eq!(reverse_edge_prefix.len(), 18);
        assert_eq!(reverse_edge_key.len(), 28);
    }

    #[test]
    fn test_reverse_edge_key_roundtrip() {
        let key = VectorKey::ReverseEdge(VectorReverseEdgeKey::new(
            index_id_from_name("idx"),
            7,
            4,
            9,
        ))
        .to_bytes();
        let VectorKey::ReverseEdge(parsed) = VectorKey::parse_from_slice(&key).unwrap() else {
            panic!("reverse edge key should parse as ReverseEdge");
        };
        assert_eq!(parsed.target_node_id(), 7);
        assert_eq!(parsed.layer(), 4);
        assert_eq!(parsed.source_node_id(), 9);
        assert!(VectorKey::parse_from_slice(&key[..27]).is_err());
    }

    #[test]
    fn test_layer_selection() {
        use rand::rngs::StdRng;

        let ml = default_ml_for_m(16);
        let mut rng = StdRng::seed_from_u64(42);

        // Generate 1000 layers and check distribution
        let mut layers = vec![];
        for _ in 0..1000 {
            let layer = select_layer(ml, &mut rng);
            layers.push(layer);
        }

        // Most nodes should be at layer 0
        let layer_0_count = layers.iter().filter(|&&l| l == 0).count();
        assert!(
            layer_0_count > 850,
            "Expected >850 nodes at layer 0 for m=16-derived ml, got {}",
            layer_0_count
        );

        // Some nodes should be at higher layers
        let higher_layers = layers.iter().filter(|&&l| l > 0).count();
        assert!(higher_layers > 0, "Expected some nodes at higher layers");
        assert!(
            higher_layers < 200,
            "Expected <200 nodes above layer 0 for m=16-derived ml, got {}",
            higher_layers
        );

        // Very few nodes should be at very high layers
        let very_high = layers.iter().filter(|&&l| l > 5).count();
        assert!(
            very_high < 50,
            "Expected <50 nodes at layer >5, got {}",
            very_high
        );

        assert!(layers.iter().all(|&l| l <= MAX_SELECTED_LAYER));
    }

    fn euclidean_item(value: f32) -> Item<'static, distance::Euclidean> {
        let vector = unaligned_vector::UnalignedVector::<f32>::from_vec(vec![value]);
        Item::<distance::Euclidean> {
            header: distance::Euclidean::new_header(&vector),
            vector: Cow::Owned(vector.into_owned()),
        }
    }

    #[test]
    fn test_select_diverse_prefers_separated_candidates() {
        let query = euclidean_item(0.0);

        let mut item_map: HashMap<NodeId, Item<'static, distance::Euclidean>> = HashMap::new();
        item_map.insert(1, euclidean_item(1.0));
        item_map.insert(2, euclidean_item(1.1));
        item_map.insert(3, euclidean_item(-10.0));
        item_map.insert(4, euclidean_item(20.0));

        let mut candidates = vec![
            Candidate::try_new(
                1,
                distance::Euclidean::distance(&query, item_map.get(&1).unwrap()),
            )
            .unwrap(),
            Candidate::try_new(
                2,
                distance::Euclidean::distance(&query, item_map.get(&2).unwrap()),
            )
            .unwrap(),
            Candidate::try_new(
                3,
                distance::Euclidean::distance(&query, item_map.get(&3).unwrap()),
            )
            .unwrap(),
            Candidate::try_new(
                4,
                distance::Euclidean::distance(&query, item_map.get(&4).unwrap()),
            )
            .unwrap(),
        ];
        candidates.sort();

        let selected = select_diverse(&query, &candidates, &|id| item_map.get(&id), 2).unwrap();
        assert_eq!(selected, vec![1, 3]);
    }

    #[test]
    fn test_select_diverse_skips_missing_items_and_backfills() {
        let query = euclidean_item(0.0);
        let mut item_map: HashMap<NodeId, Item<'static, distance::Euclidean>> = HashMap::new();
        item_map.insert(1, euclidean_item(1.0));
        item_map.insert(2, euclidean_item(1.1));
        let candidates = [
            Candidate::try_new(99, 0.5).unwrap(),
            Candidate::try_new(1, 1.0).unwrap(),
            Candidate::try_new(2, 1.1).unwrap(),
        ];

        let selected = select_diverse(&query, &candidates, &|id| item_map.get(&id), 2).unwrap();
        assert_eq!(selected, vec![1, 2]);
    }

    #[test]
    fn test_layer_selection_handles_zero_uniform() {
        let ml = default_ml_for_m(16);
        let layer = select_layer_from_uniform(ml, 0.0);
        assert!(layer <= MAX_SELECTED_LAYER);
        assert_ne!(layer, u16::MAX);

        assert_eq!(select_layer_from_uniform(f32::NAN, 1.0), 0);
        assert!(select_layer_from_uniform(ml, f32::NAN) <= MAX_SELECTED_LAYER);
    }

    #[test]
    fn test_layer_selection_caps_extreme_values() {
        let layer = select_layer_from_uniform(1_000.0, f32::MIN_POSITIVE);
        assert_eq!(layer, MAX_SELECTED_LAYER);
    }
}
