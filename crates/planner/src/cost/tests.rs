use super::*;
use crate::properties::{self, PositiveUsize};

#[test]
fn selectivity_rejects_invalid_ratios_and_applies_with_rounding() {
    assert!(Selectivity::from_ratio(2, 1).is_none());
    assert!(Selectivity::from_ratio(1, 0).is_none());
    assert_eq!(Selectivity::from_ratio(1, 1), Some(Selectivity::ONE));
    assert_eq!(Selectivity::from_ratio(0, 10), Some(Selectivity::ZERO));
    assert_eq!(
        Selectivity::from_ratio(1, 3)
            .unwrap()
            .apply_to(EstimatedRows::rows(10)),
        EstimatedRows::rows(4)
    );
}

#[test]
fn storage_cost_profile_keeps_all_costs_tunable() {
    let profile = StorageCostProfile {
        multi_get_setup: LatencyEstimate::micros(1),
        multi_get_per_key: LatencyEstimate::micros(2),
        sort_setup: LatencyEstimate::micros(3),
        sort_per_row: LatencyEstimate::micros(4),
        barrier_overhead: LatencyEstimate::micros(5),
        foreach_overhead: LatencyEstimate::micros(6),
        default_materialized_row_bytes: ByteEstimate::bytes(8),
        default_unknown_scan_rows: EstimatedRows::rows(42),
        default_equality_index_rows: EstimatedRows::rows(7),
        default_unique_equality_rows: UniqueEqualityRows::ZERO,
        ..StorageCostProfile::default()
    };

    let cost = profile.multi_get(
        PositiveUsize::new(3).unwrap(),
        properties::KeyLocality::Close,
    );
    assert_eq!(cost.latency, LatencyEstimate::micros(7));
    assert_eq!(cost.multi_get_calls, 1);
    assert_eq!(cost.object_reads, 3);
    assert_eq!(profile.default_unknown_scan_rows, EstimatedRows::rows(42));
    assert_eq!(profile.equality_index_rows(None), EstimatedRows::rows(7));
    assert_eq!(
        profile.explicit_sort(EstimatedRows::rows(2)).latency,
        LatencyEstimate::micros(11)
    );
    assert_eq!(
        profile.explicit_sort(EstimatedRows::rows(2)).peak_memory,
        ByteEstimate::bytes(16)
    );
    assert_eq!(profile.barrier().latency, LatencyEstimate::micros(5));
    assert_eq!(
        profile.foreach_wrapper().latency,
        LatencyEstimate::micros(6)
    );
    assert_eq!(profile.unique_equality_rows(None), EstimatedRows::ZERO);
    assert_eq!(
        profile.unique_equality_rows(Some(3)),
        EstimatedRows::rows(1)
    );
}

#[test]
fn storage_cost_profile_overrides_load_partial_typed_experiment_profiles() {
    let profile = StorageCostProfile::default()
        .with_json_overrides(
            r#"{
                "range_next": 3,
                "default_unknown_scan_rows": 42,
                "default_equality_index_rows": 11,
                "default_unique_equality_rows": 0,
                "max_parallel_kv_reads": 2
            }"#,
        )
        .unwrap();

    assert_eq!(profile.range_next, LatencyEstimate::micros(3));
    assert_eq!(profile.default_unknown_scan_rows, EstimatedRows::rows(42));
    assert_eq!(profile.default_equality_index_rows, EstimatedRows::rows(11));
    assert_eq!(
        profile.default_unique_equality_rows,
        UniqueEqualityRows::ZERO
    );
    assert_eq!(
        profile.max_parallel_kv_reads,
        PositiveUsize::new(2).unwrap()
    );
    assert_eq!(
        profile.object_get_latency,
        StorageCostProfile::default().object_get_latency
    );
    assert!(StorageCostProfileOverrides::from_json_str(r#"{"not_a_cost": 1}"#).is_err());
    assert!(StorageCostProfileOverrides::from_json_str(r#"{"max_parallel_kv_reads": 0}"#).is_err());
    assert!(
        StorageCostProfileOverrides::from_json_str(r#"{"default_unique_equality_rows": 2}"#)
            .is_err()
    );
}

#[test]
fn storage_cost_profile_overrides_cover_every_tunable_field() {
    let profile = StorageCostProfile::default()
        .with_json_overrides(
            r#"{
                "object_get_latency": 1,
                "sstable_filter_probe": 2,
                "multi_get_setup": 3,
                "multi_get_per_key": 4,
                "range_seek": 5,
                "range_next": 6,
                "cpu_predicate_eval": 7,
                "stream_operator_eval": 8,
                "sort_setup": 9,
                "sort_per_row": 10,
                "barrier_overhead": 11,
                "source_inject_overhead": 12,
                "foreach_overhead": 13,
                "task_overhead": 14,
                "default_key_read_bytes": 15,
                "default_materialized_row_bytes": 16,
                "default_unknown_scan_rows": 17,
                "default_equality_index_rows": 18,
                "default_unique_equality_rows": 1,
                "max_parallel_kv_reads": 20,
                "close_key_multi_get_batch": 21,
                "sparse_key_multi_get_batch": 22
            }"#,
        )
        .unwrap();

    assert_eq!(profile.object_get_latency, LatencyEstimate::micros(1));
    assert_eq!(profile.sstable_filter_probe, LatencyEstimate::micros(2));
    assert_eq!(profile.multi_get_setup, LatencyEstimate::micros(3));
    assert_eq!(profile.multi_get_per_key, LatencyEstimate::micros(4));
    assert_eq!(profile.range_seek, LatencyEstimate::micros(5));
    assert_eq!(profile.range_next, LatencyEstimate::micros(6));
    assert_eq!(profile.cpu_predicate_eval, LatencyEstimate::micros(7));
    assert_eq!(profile.stream_operator_eval, LatencyEstimate::micros(8));
    assert_eq!(profile.sort_setup, LatencyEstimate::micros(9));
    assert_eq!(profile.sort_per_row, LatencyEstimate::micros(10));
    assert_eq!(profile.barrier_overhead, LatencyEstimate::micros(11));
    assert_eq!(profile.source_inject_overhead, LatencyEstimate::micros(12));
    assert_eq!(profile.foreach_overhead, LatencyEstimate::micros(13));
    assert_eq!(profile.task_overhead, LatencyEstimate::micros(14));
    assert_eq!(profile.default_key_read_bytes, ByteEstimate::bytes(15));
    assert_eq!(
        profile.default_materialized_row_bytes,
        ByteEstimate::bytes(16)
    );
    assert_eq!(profile.default_unknown_scan_rows, EstimatedRows::rows(17));
    assert_eq!(profile.default_equality_index_rows, EstimatedRows::rows(18));
    assert_eq!(
        profile.default_unique_equality_rows,
        UniqueEqualityRows::rows(1).unwrap()
    );
    assert_eq!(
        profile.max_parallel_kv_reads,
        PositiveUsize::new(20).unwrap()
    );
    assert_eq!(
        profile.close_key_multi_get_batch,
        PositiveUsize::new(21).unwrap()
    );
    assert_eq!(
        profile.sparse_key_multi_get_batch,
        PositiveUsize::new(22).unwrap()
    );
}

#[test]
fn bounded_row_estimates_reject_values_above_their_contract() {
    type SingletonRows = EstimatedRowsAtMost<1>;

    assert_eq!(
        SingletonRows::new(EstimatedRows::rows(1))
            .unwrap()
            .as_rows(),
        1
    );
    assert!(SingletonRows::new(EstimatedRows::rows(2)).is_none());
    assert_eq!(
        SingletonRows::clamp(EstimatedRows::rows(5)).estimated_rows(),
        EstimatedRows::rows(1)
    );
    assert_eq!(
        SingletonRows::at_most(5).estimated_rows(),
        EstimatedRows::rows(1)
    );
}

#[test]
fn cost_vectors_scale_without_repeat_loops() {
    let cost = CostVector {
        latency: LatencyEstimate::micros(3),
        object_reads: 2,
        multi_get_calls: 1,
        range_seeks: 1,
        range_nexts: 5,
        cpu_units: 7,
        bytes: ByteEstimate::bytes(11),
        peak_memory: ByteEstimate::bytes(13),
        parallel_width: 4,
    };

    let scaled = cost.saturating_mul(3);

    assert_eq!(scaled.latency, LatencyEstimate::micros(9));
    assert_eq!(scaled.object_reads, 6);
    assert_eq!(scaled.multi_get_calls, 3);
    assert_eq!(scaled.range_nexts, 15);
    assert_eq!(scaled.cpu_units, 21);
    assert_eq!(scaled.bytes, ByteEstimate::bytes(33));
    assert_eq!(scaled.peak_memory, ByteEstimate::bytes(13));
    assert_eq!(scaled.parallel_width, 4);
}

#[test]
fn sparse_multi_get_costs_more_than_close_multi_get() {
    let profile = StorageCostProfile::default();
    let keys = PositiveUsize::new(4).unwrap();

    assert!(
        profile
            .multi_get(keys, properties::KeyLocality::Sparse)
            .latency
            > profile
                .multi_get(keys, properties::KeyLocality::Close)
                .latency
    );
}

#[test]
fn parallel_cost_uses_critical_path_and_sums_work() {
    let profile = StorageCostProfile::default();
    let children = [
        profile.point_gets(PositiveUsize::new(1).unwrap()),
        profile.range_scan(EstimatedRows::rows(3)),
        profile.explicit_sort(EstimatedRows::rows(2)),
        profile.explicit_sort(EstimatedRows::rows(5)),
    ];

    let cost = profile.parallel(&children, PositiveUsize::new(2).unwrap());
    assert_eq!(cost.object_reads, children[0].object_reads);
    assert_eq!(cost.range_nexts, children[1].range_nexts);
    assert_eq!(cost.parallel_width, 2);
    assert_eq!(
        cost.peak_memory,
        children[2]
            .peak_memory
            .saturating_add(children[3].peak_memory)
    );
    assert!(cost.latency >= children[0].latency.max(children[1].latency));
}

#[test]
fn parallel_task_overhead_is_tunable_by_width() {
    let profile = StorageCostProfile {
        task_overhead: LatencyEstimate::micros(7),
        ..StorageCostProfile::default()
    };

    let cost = profile.parallel_task_overhead(PositiveUsize::new(3).unwrap());

    assert_eq!(cost.latency, LatencyEstimate::micros(21));
    assert_eq!(cost.parallel_width, 3);
}

#[test]
fn parallel_peak_memory_uses_largest_concurrent_child_peaks() {
    let profile = StorageCostProfile::default();
    let children = [
        CostVector {
            peak_memory: ByteEstimate::bytes(5),
            ..CostVector::ZERO
        },
        CostVector {
            peak_memory: ByteEstimate::bytes(100),
            ..CostVector::ZERO
        },
        CostVector {
            peak_memory: ByteEstimate::bytes(1),
            ..CostVector::ZERO
        },
        CostVector {
            peak_memory: ByteEstimate::bytes(20),
            ..CostVector::ZERO
        },
    ];

    assert_eq!(
        profile
            .parallel(&children, PositiveUsize::new(2).unwrap())
            .peak_memory,
        ByteEstimate::bytes(120)
    );
    assert_eq!(
        profile
            .parallel(&children, PositiveUsize::new(8).unwrap())
            .peak_memory,
        ByteEstimate::bytes(126)
    );
}
