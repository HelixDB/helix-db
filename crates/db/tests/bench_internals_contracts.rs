//! Contracts for the benchmark fixtures that `bench-internals` exposes.
//!
//! The benches only time these fixtures, so this target checks that they
//! reject invalid cases, run every metric, workload, layout and strategy, and,
//! for full-text prefiltering, match an exact oracle.

use db::bench_internals::{
    FtsPrefilterBenchmarkCase, FtsPrefilterBenchmarkFixture, FtsPrefilterBenchmarkLayout,
    FtsPrefilterBenchmarkStrategy, VectorBatchBenchmarkCacheLimits, VectorBatchBenchmarkCase,
    VectorBatchBenchmarkFixture, VectorBatchBenchmarkMetric, VectorBatchBenchmarkWorkload,
};

/// Exercises the vector batch harness across every metric and both fresh and
/// replacement transaction shapes.
#[tokio::test]
async fn vector_batch_benchmark_exercises_all_metrics_and_workloads() {
    for (batch_size, initial_count, dimension) in [
        (0, 0, 4),
        (1, 0, 0),
        (usize::MAX, 1, 4),
        (2, usize::MAX, 128),
    ] {
        assert!(
            VectorBatchBenchmarkCase::try_new_with_initial_count(
                batch_size,
                initial_count,
                dimension,
                VectorBatchBenchmarkMetric::Cosine,
                VectorBatchBenchmarkWorkload::Fresh,
            )
            .is_err(),
            "batch {batch_size}, initial {initial_count}, dimension {dimension}"
        );
    }
    assert!(VectorBatchBenchmarkCacheLimits::try_new(0, 1, 1, 1).is_err());

    for metric in [
        VectorBatchBenchmarkMetric::Cosine,
        VectorBatchBenchmarkMetric::Euclidean,
        VectorBatchBenchmarkMetric::Manhattan,
    ] {
        for workload in [
            VectorBatchBenchmarkWorkload::Fresh,
            VectorBatchBenchmarkWorkload::Replacement,
        ] {
            // The benchmark's scripted selector assigns ordinal 11 to layer 1.
            // Keep one case large enough to make upper-neighbor staging deterministic.
            let batch_size = match (metric, workload) {
                (VectorBatchBenchmarkMetric::Cosine, VectorBatchBenchmarkWorkload::Fresh) => 12,
                _ => 2,
            };
            let initial_count =
                usize::from(workload == VectorBatchBenchmarkWorkload::Replacement) * 2;
            let case = VectorBatchBenchmarkCase::try_new_with_initial_count(
                batch_size,
                initial_count,
                4,
                metric,
                workload,
            )
            .expect("small vector benchmark case validates");
            let fixture = if workload == VectorBatchBenchmarkWorkload::Replacement {
                VectorBatchBenchmarkFixture::prepare_with_cache_limits(
                    case,
                    VectorBatchBenchmarkCacheLimits::try_new(256, 1, 1, 1)
                        .expect("pressure benchmark limits validate"),
                )
                .await
            } else {
                VectorBatchBenchmarkFixture::prepare(case).await
            }
            .expect("small vector benchmark fixture prepares");
            let sample = fixture
                .run_sample()
                .await
                .expect("small vector benchmark sample completes")
                .with_allocations(3, 128);
            assert_eq!(sample.allocated_calls, 3);
            assert_eq!(sample.allocated_bytes, 128);
            assert!(sample.unique_final_rows > 0);
            assert!(sample.unique_final_bytes > 0);
            assert!(sample.telemetry.staged_write_bytes > 0);
            assert!((0.0..=1.0).contains(&sample.recall));
            fixture
                .close()
                .await
                .expect("small vector benchmark fixture closes");
        }
    }
}

/// Exercises both production FTS layouts and both candidate-selection
/// strategies against the fixture's exact oracle.
#[tokio::test]
async fn text_prefilter_benchmark_covers_exact_layout_and_strategy_matrix() {
    assert!(FtsPrefilterBenchmarkFixture::try_new(0, 1).await.is_err());
    assert!(FtsPrefilterBenchmarkFixture::try_new(4, 0).await.is_err());
    assert!(FtsPrefilterBenchmarkFixture::try_new(3, 4).await.is_err());

    let fixture = FtsPrefilterBenchmarkFixture::try_new(24, 3)
        .await
        .expect("small production FTS fixture prepares");
    assert_eq!(fixture.document_count(), 24);
    for (candidates, query, k) in [
        (0, "rareterm", 3),
        (fixture.document_count() + 1, "rareterm", 3),
        (8, "unsupported", 3),
        (8, "rareterm", 0),
    ] {
        assert!(
            FtsPrefilterBenchmarkCase::try_new(
                FtsPrefilterBenchmarkLayout::MultiSplit,
                FtsPrefilterBenchmarkStrategy::Collector,
                candidates,
                query,
                k,
                fixture.document_count(),
            )
            .is_err(),
            "{candidates} candidates, query {query}, k {k}"
        );
    }

    for layout in [
        FtsPrefilterBenchmarkLayout::MultiSplit,
        FtsPrefilterBenchmarkLayout::Compacted,
    ] {
        for strategy in [
            FtsPrefilterBenchmarkStrategy::Collector,
            FtsPrefilterBenchmarkStrategy::Unrestricted,
        ] {
            assert!(!layout.as_str().is_empty());
            assert!(!strategy.as_str().is_empty());
            for query in ["rareterm", "mediumterm", "commonterm"] {
                let case = FtsPrefilterBenchmarkCase::try_new(
                    layout,
                    strategy,
                    8,
                    query,
                    5,
                    fixture.document_count(),
                )
                .expect("benchmark matrix case validates");
                let sample = fixture
                    .run_case(case)
                    .await
                    .expect("benchmark matrix case matches its exact oracle");
                assert_eq!(sample.candidate_count, 8);
                assert_eq!(
                    sample.split_count,
                    if layout.as_str() == "multi_split" {
                        3
                    } else {
                        1
                    }
                );
                assert!(sample.result_count <= 5);
                assert_eq!(sample.result_digest.len(), 64);
            }
        }
    }
}
