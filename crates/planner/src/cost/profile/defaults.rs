//! Default storage cost assumptions.

use crate::properties::PositiveUsize;

use super::{ByteEstimate, EstimatedRows, LatencyEstimate, StorageCostProfile, UniqueEqualityRows};

impl Default for StorageCostProfile {
    fn default() -> Self {
        Self {
            object_get_latency: LatencyEstimate::micros(5_000),
            sstable_filter_probe: LatencyEstimate::micros(50),
            multi_get_setup: LatencyEstimate::micros(750),
            multi_get_per_key: LatencyEstimate::micros(75),
            range_seek: LatencyEstimate::micros(2_000),
            range_next: LatencyEstimate::micros(10),
            cpu_predicate_eval: LatencyEstimate::micros(1),
            stream_operator_eval: LatencyEstimate::micros(1),
            bitmap_decode_per_id: LatencyEstimate::micros(1),
            secondary_set_per_id: LatencyEstimate::micros(1),
            secondary_row_materialization_per_id: LatencyEstimate::micros(1),
            authoritative_verify_per_id: LatencyEstimate::micros(10),
            sort_setup: LatencyEstimate::micros(100),
            sort_per_row: LatencyEstimate::micros(2),
            barrier_overhead: LatencyEstimate::micros(50),
            source_inject_overhead: LatencyEstimate::micros(5),
            foreach_overhead: LatencyEstimate::micros(25),
            task_overhead: LatencyEstimate::micros(25),
            default_key_read_bytes: ByteEstimate::bytes(256),
            default_materialized_row_bytes: ByteEstimate::bytes(256),
            default_bitmap_id_bytes: ByteEstimate::bytes(8),
            default_unknown_scan_rows: EstimatedRows::rows(1_000),
            default_equality_index_rows: EstimatedRows::rows(10),
            default_range_index_rows: EstimatedRows::rows(200),
            default_unique_equality_rows: UniqueEqualityRows::at_most(1),
            max_parallel_kv_reads: PositiveUsize::at_least_one(16),
            close_key_multi_get_batch: PositiveUsize::at_least_one(256),
            sparse_key_multi_get_batch: PositiveUsize::at_least_one(16),
        }
    }
}
