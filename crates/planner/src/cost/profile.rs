//! Tunable storage-backed planner cost profile.
//!
//! The profile facade owns the public field contract. Defaults, JSON
//! experiment overrides, cost formulas, and parallel peak-memory helpers live in
//! sibling modules so every part of the cost model can be tested and evolved
//! independently.

mod defaults;
mod formulas;
mod overrides;
mod parallel;

use serde::{Deserialize, Serialize};

use crate::properties::PositiveUsize;

use super::{
    units::{ByteEstimate, EstimatedRows, LatencyEstimate, UniqueEqualityRows},
    vector::CostVector,
};

pub use overrides::StorageCostProfileOverrides;

/// Tunable object-storage-backed LSM cost profile.
///
/// All default values are intentionally centralized in `profile::defaults` so
/// experiments can tune costs without changing optimizer rules.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageCostProfile {
    /// Latency for a cold logical object/KV read.
    pub object_get_latency: LatencyEstimate,
    /// Per-key SSTable/filter probe work.
    pub sstable_filter_probe: LatencyEstimate,
    /// Fixed setup cost for a `multi_get`.
    pub multi_get_setup: LatencyEstimate,
    /// Per-key work inside a `multi_get`.
    pub multi_get_per_key: LatencyEstimate,
    /// Fixed range seek cost.
    pub range_seek: LatencyEstimate,
    /// Per-row range scan step cost.
    pub range_next: LatencyEstimate,
    /// Per-row residual predicate CPU cost.
    pub cpu_predicate_eval: LatencyEstimate,
    /// Per-row generic stream operator CPU cost.
    pub stream_operator_eval: LatencyEstimate,
    /// Per-ID CPU cost for decoding a secondary equality bitmap.
    pub bitmap_decode_per_id: LatencyEstimate,
    /// Per-ID CPU cost for a bitmap union or intersection.
    pub secondary_set_per_id: LatencyEstimate,
    /// Per-ID CPU cost for constructing the final execution row.
    pub secondary_row_materialization_per_id: LatencyEstimate,
    /// Per-candidate cost for an authoritative graph-property verification.
    pub authoritative_verify_per_id: LatencyEstimate,
    /// Fixed setup cost for an explicit sort.
    pub sort_setup: LatencyEstimate,
    /// Per-row explicit sort CPU/materialization cost.
    pub sort_per_row: LatencyEstimate,
    /// Fixed side-effect/materialization barrier cost.
    pub barrier_overhead: LatencyEstimate,
    /// Fixed source-variable injection cost.
    pub source_inject_overhead: LatencyEstimate,
    /// Fixed per-entry `ForEach` wrapper scheduling/materialization cost.
    pub foreach_overhead: LatencyEstimate,
    /// Async task scheduling overhead.
    pub task_overhead: LatencyEstimate,
    /// Default bytes charged per key read when no row-size stats exist.
    pub default_key_read_bytes: ByteEstimate,
    /// Default bytes charged per row materialized by blocking operators.
    pub default_materialized_row_bytes: ByteEstimate,
    /// Default compressed bytes charged per decoded bitmap ID.
    pub default_bitmap_id_bytes: ByteEstimate,
    /// Default row count used when no catalog or runtime row stats exist.
    pub default_unknown_scan_rows: EstimatedRows,
    /// Default row count for non-unique equality index lookups with no stats.
    pub default_equality_index_rows: EstimatedRows,
    /// Default row count for range-index lookups with no stats.
    pub default_range_index_rows: EstimatedRows,
    /// Default row count for unique equality index lookups with no stats.
    pub default_unique_equality_rows: UniqueEqualityRows,
    /// Maximum parallel KV reads the planner should schedule by default.
    pub max_parallel_kv_reads: PositiveUsize,
    /// Maximum close-key `multi_get` batch size.
    pub close_key_multi_get_batch: PositiveUsize,
    /// Maximum sparse-key `multi_get` batch size.
    pub sparse_key_multi_get_batch: PositiveUsize,
}
