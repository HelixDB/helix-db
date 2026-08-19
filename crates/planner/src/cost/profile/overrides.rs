//! Typed partial storage-cost overrides.

use serde::{Deserialize, Serialize};

use crate::properties::PositiveUsize;

use super::{ByteEstimate, EstimatedRows, LatencyEstimate, StorageCostProfile, UniqueEqualityRows};

/// Partial storage-cost overrides for planner experiments.
///
/// Unknown fields are rejected and each supplied value is deserialized through
/// the same wrapper types as [`StorageCostProfile`], so invalid values such as
/// zero concurrency limits stay unrepresentable at the configuration boundary.
///
/// ```
/// use helix_planner::cost::StorageCostProfile;
///
/// let profile = StorageCostProfile::default()
///     .with_json_overrides(r#"{"range_next":3,"default_equality_index_rows":42}"#)
///     .unwrap();
///
/// assert_eq!(profile.range_next.as_micros(), 3);
/// assert_eq!(profile.default_equality_index_rows.as_rows(), 42);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageCostProfileOverrides {
    object_get_latency: Option<LatencyEstimate>,
    sstable_filter_probe: Option<LatencyEstimate>,
    multi_get_setup: Option<LatencyEstimate>,
    multi_get_per_key: Option<LatencyEstimate>,
    range_seek: Option<LatencyEstimate>,
    range_next: Option<LatencyEstimate>,
    cpu_predicate_eval: Option<LatencyEstimate>,
    stream_operator_eval: Option<LatencyEstimate>,
    bitmap_decode_per_id: Option<LatencyEstimate>,
    secondary_set_per_id: Option<LatencyEstimate>,
    secondary_row_materialization_per_id: Option<LatencyEstimate>,
    authoritative_verify_per_id: Option<LatencyEstimate>,
    sort_setup: Option<LatencyEstimate>,
    sort_per_row: Option<LatencyEstimate>,
    barrier_overhead: Option<LatencyEstimate>,
    source_inject_overhead: Option<LatencyEstimate>,
    foreach_overhead: Option<LatencyEstimate>,
    task_overhead: Option<LatencyEstimate>,
    default_key_read_bytes: Option<ByteEstimate>,
    default_materialized_row_bytes: Option<ByteEstimate>,
    default_bitmap_id_bytes: Option<ByteEstimate>,
    default_unknown_scan_rows: Option<EstimatedRows>,
    default_equality_index_rows: Option<EstimatedRows>,
    default_range_index_rows: Option<EstimatedRows>,
    default_unique_equality_rows: Option<UniqueEqualityRows>,
    max_parallel_kv_reads: Option<PositiveUsize>,
    close_key_multi_get_batch: Option<PositiveUsize>,
    sparse_key_multi_get_batch: Option<PositiveUsize>,
}

impl StorageCostProfileOverrides {
    /// Parse partial storage-cost overrides from JSON.
    pub fn from_json_str(input: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(input)
    }

    /// Apply these overrides to a base profile.
    pub fn apply_to(self, mut profile: StorageCostProfile) -> StorageCostProfile {
        if let Some(value) = self.object_get_latency {
            profile.object_get_latency = value;
        }
        if let Some(value) = self.sstable_filter_probe {
            profile.sstable_filter_probe = value;
        }
        if let Some(value) = self.multi_get_setup {
            profile.multi_get_setup = value;
        }
        if let Some(value) = self.multi_get_per_key {
            profile.multi_get_per_key = value;
        }
        if let Some(value) = self.range_seek {
            profile.range_seek = value;
        }
        if let Some(value) = self.range_next {
            profile.range_next = value;
        }
        if let Some(value) = self.cpu_predicate_eval {
            profile.cpu_predicate_eval = value;
        }
        if let Some(value) = self.stream_operator_eval {
            profile.stream_operator_eval = value;
        }
        if let Some(value) = self.bitmap_decode_per_id {
            profile.bitmap_decode_per_id = value;
        }
        if let Some(value) = self.secondary_set_per_id {
            profile.secondary_set_per_id = value;
        }
        if let Some(value) = self.secondary_row_materialization_per_id {
            profile.secondary_row_materialization_per_id = value;
        }
        if let Some(value) = self.authoritative_verify_per_id {
            profile.authoritative_verify_per_id = value;
        }
        if let Some(value) = self.sort_setup {
            profile.sort_setup = value;
        }
        if let Some(value) = self.sort_per_row {
            profile.sort_per_row = value;
        }
        if let Some(value) = self.barrier_overhead {
            profile.barrier_overhead = value;
        }
        if let Some(value) = self.source_inject_overhead {
            profile.source_inject_overhead = value;
        }
        if let Some(value) = self.foreach_overhead {
            profile.foreach_overhead = value;
        }
        if let Some(value) = self.task_overhead {
            profile.task_overhead = value;
        }
        if let Some(value) = self.default_key_read_bytes {
            profile.default_key_read_bytes = value;
        }
        if let Some(value) = self.default_materialized_row_bytes {
            profile.default_materialized_row_bytes = value;
        }
        if let Some(value) = self.default_bitmap_id_bytes {
            profile.default_bitmap_id_bytes = value;
        }
        if let Some(value) = self.default_unknown_scan_rows {
            profile.default_unknown_scan_rows = value;
        }
        if let Some(value) = self.default_equality_index_rows {
            profile.default_equality_index_rows = value;
        }
        if let Some(value) = self.default_range_index_rows {
            profile.default_range_index_rows = value;
        }
        if let Some(value) = self.default_unique_equality_rows {
            profile.default_unique_equality_rows = value;
        }
        if let Some(value) = self.max_parallel_kv_reads {
            profile.max_parallel_kv_reads = value;
        }
        if let Some(value) = self.close_key_multi_get_batch {
            profile.close_key_multi_get_batch = value;
        }
        if let Some(value) = self.sparse_key_multi_get_batch {
            profile.sparse_key_multi_get_batch = value;
        }
        profile
    }
}

impl StorageCostProfile {
    /// Apply typed partial overrides to this profile.
    pub fn with_overrides(self, overrides: StorageCostProfileOverrides) -> Self {
        overrides.apply_to(self)
    }

    /// Apply JSON partial overrides to this profile.
    pub fn with_json_overrides(self, input: &str) -> Result<Self, serde_json::Error> {
        Ok(self.with_overrides(StorageCostProfileOverrides::from_json_str(input)?))
    }
}
