//! Deterministic one-transaction vector batch benchmark.
//!
//! Run with:
//! `cargo bench -p db --features bench-internals --bench vector_batch_insert`

mod benchmark {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::process::Command;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    use db::bench_internals::{
        VectorBatchBenchmarkCacheLimits, VectorBatchBenchmarkCase, VectorBatchBenchmarkFixture,
        VectorBatchBenchmarkMetric, VectorBatchBenchmarkSample, VectorBatchBenchmarkWorkload,
    };
    use serde::Serialize;

    struct CountingAllocator;

    static TRACK_ALLOCATIONS: AtomicBool = AtomicBool::new(false);
    static ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
    static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

    // SAFETY: every operation is forwarded unchanged to the system allocator.
    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            if TRACK_ALLOCATIONS.load(Ordering::Relaxed) {
                ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
                ALLOCATED_BYTES.fetch_add(
                    u64::try_from(layout.size()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
            }
            // SAFETY: the caller supplied `layout` under `GlobalAlloc::alloc`.
            unsafe { System.alloc(layout) }
        }

        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            // SAFETY: the caller supplied the allocation and layout pair.
            unsafe { System.dealloc(pointer, layout) }
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            if TRACK_ALLOCATIONS.load(Ordering::Relaxed) {
                ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
                ALLOCATED_BYTES.fetch_add(
                    u64::try_from(layout.size()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
            }
            // SAFETY: the caller supplied `layout` under `GlobalAlloc::alloc_zeroed`.
            unsafe { System.alloc_zeroed(layout) }
        }

        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            if TRACK_ALLOCATIONS.load(Ordering::Relaxed) {
                ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
                ALLOCATED_BYTES.fetch_add(
                    u64::try_from(new_size).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
            }
            // SAFETY: the caller supplied the allocation, layout, and new size.
            unsafe { System.realloc(pointer, layout, new_size) }
        }
    }

    #[global_allocator]
    static ALLOCATOR: CountingAllocator = CountingAllocator;

    #[derive(Serialize)]
    struct SampleRecord<'sample> {
        record: &'static str,
        commit: &'sample str,
        sample_index: usize,
        sample: &'sample VectorBatchBenchmarkSample,
    }

    #[derive(Serialize)]
    struct SummaryRecord<'summary> {
        record: &'static str,
        commit: &'summary str,
        case: VectorBatchBenchmarkCase,
        cache_limits: VectorBatchBenchmarkCacheLimits,
        samples: usize,
        staging_p50_ns: u64,
        staging_p95_ns: u64,
        commit_p50_ns: u64,
        commit_p95_ns: u64,
        total_p50_ns: u64,
        total_p95_ns: u64,
        vectors_per_second_p50: f64,
        point_get_calls_p50: u64,
        multi_get_calls_p50: u64,
        multi_get_keys_p50: u64,
        put_calls_p50: u64,
        delete_calls_p50: u64,
        staged_write_bytes_p50: u64,
        item_hits_p50: u64,
        item_misses_p50: u64,
        neighbor_hits_p50: u64,
        neighbor_misses_p50: u64,
        simhash_hits_p50: u64,
        simhash_misses_p50: u64,
        cache_evictions_p50: u64,
        dirty_neighbor_flushes_p50: u64,
        peak_retained_payload_bytes: u64,
        allocated_calls_p50: u64,
        allocated_bytes_p50: u64,
        unique_final_rows: u64,
        unique_final_bytes: u64,
        graph_digest: &'summary str,
        recall: f64,
    }

    pub fn main() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("vector batch benchmark runtime starts");
        runtime.block_on(run());
    }

    async fn run() {
        let commit = git_commit();
        let smoke = cfg!(debug_assertions);
        let warmups = env_usize("HELIX_VECTOR_BATCH_BENCH_WARMUPS", usize::from(!smoke) * 3);
        let samples = env_usize(
            "HELIX_VECTOR_BATCH_BENCH_SAMPLES",
            if smoke { 1 } else { 20 },
        );
        let allow_short = smoke
            || std::env::var("HELIX_VECTOR_BATCH_BENCH_ALLOW_SHORT")
                .is_ok_and(|value| value == "1");
        assert!(
            samples >= 20 || allow_short,
            "measured benchmark runs require at least 20 samples"
        );
        let batches = env_usize_list(
            "HELIX_VECTOR_BATCH_BENCH_BATCHES",
            if smoke {
                &[1]
            } else {
                &[1, 8, 32, 128, 512, 1_024, 2_048, 4_096]
            },
        );
        let initial_counts = env_usize_list("HELIX_VECTOR_BATCH_BENCH_INITIAL_COUNTS", &[0]);
        let dimensions = env_usize_list(
            "HELIX_VECTOR_BATCH_BENCH_DIMENSIONS",
            if smoke { &[2] } else { &[128, 1_536] },
        );
        let metrics = env_metrics();
        let workloads = env_workloads();
        let cache_limits = VectorBatchBenchmarkCacheLimits::try_new(
            u64::try_from(env_usize(
                "HELIX_VECTOR_BATCH_BENCH_MAX_PAYLOAD_BYTES",
                8 * 1024 * 1024,
            ))
            .expect("benchmark payload limit fits u64"),
            env_usize("HELIX_VECTOR_BATCH_BENCH_MAX_ITEMS", 4_096),
            env_usize("HELIX_VECTOR_BATCH_BENCH_MAX_NEIGHBORS", 2_048),
            env_usize("HELIX_VECTOR_BATCH_BENCH_MAX_SIMHASHES", 4_096),
        )
        .expect("benchmark cache limits validate");

        for metric in metrics {
            for workload in workloads.iter().copied() {
                for dimension in dimensions.iter().copied() {
                    for initial_count in initial_counts.iter().copied() {
                        for batch_size in batches.iter().copied() {
                            let case = VectorBatchBenchmarkCase::try_new_with_initial_count(
                                batch_size,
                                initial_count,
                                dimension,
                                metric,
                                workload,
                            )
                            .expect("benchmark case validates");
                            for _ in 0..warmups {
                                let fixture =
                                    VectorBatchBenchmarkFixture::prepare_with_cache_limits(
                                        case,
                                        cache_limits,
                                    )
                                    .await
                                    .expect("warmup fixture prepares");
                                let _ = fixture.run_sample().await.expect("warmup sample succeeds");
                                fixture.close().await.expect("warmup fixture closes");
                            }

                            let mut measured = Vec::with_capacity(samples);
                            for sample_index in 0..samples {
                                let fixture =
                                    VectorBatchBenchmarkFixture::prepare_with_cache_limits(
                                        case,
                                        cache_limits,
                                    )
                                    .await
                                    .expect("measured fixture prepares");
                                ALLOCATION_CALLS.store(0, Ordering::Relaxed);
                                ALLOCATED_BYTES.store(0, Ordering::Relaxed);
                                TRACK_ALLOCATIONS.store(true, Ordering::Release);
                                let sample = fixture
                                    .run_sample()
                                    .await
                                    .expect("measured sample succeeds");
                                TRACK_ALLOCATIONS.store(false, Ordering::Release);
                                let sample = sample.with_allocations(
                                    ALLOCATION_CALLS.load(Ordering::Relaxed),
                                    ALLOCATED_BYTES.load(Ordering::Relaxed),
                                );
                                println!(
                                    "{}",
                                    serde_json::to_string(&SampleRecord {
                                        record: "sample",
                                        commit: &commit,
                                        sample_index,
                                        sample: &sample,
                                    })
                                    .expect("sample serializes")
                                );
                                fixture.close().await.expect("measured fixture closes");
                                measured.push(sample);
                            }
                            let summary = summarize(&commit, case, &measured);
                            println!(
                                "{}",
                                serde_json::to_string(&summary).expect("summary serializes")
                            );
                        }
                    }
                }
            }
        }
    }

    fn summarize<'sample>(
        commit: &'sample str,
        case: VectorBatchBenchmarkCase,
        samples: &'sample [VectorBatchBenchmarkSample],
    ) -> SummaryRecord<'sample> {
        let first = samples.first().expect("benchmark has measured samples");
        assert!(
            samples.iter().all(|sample| sample.case == case),
            "summary contains one benchmark case"
        );
        assert!(
            samples
                .iter()
                .all(|sample| sample.cache_limits == first.cache_limits),
            "summary contains one cache-limit configuration"
        );
        assert!(
            samples
                .iter()
                .all(|sample| sample.graph_digest == first.graph_digest),
            "scripted benchmark graph digest is deterministic"
        );
        assert!(
            samples
                .iter()
                .all(|sample| sample.unique_final_rows == first.unique_final_rows
                    && sample.unique_final_bytes == first.unique_final_bytes),
            "scripted benchmark final row shape is deterministic"
        );
        assert!(
            samples.iter().all(|sample| sample.recall == first.recall),
            "scripted benchmark recall is deterministic"
        );
        let cache_evictions = samples
            .iter()
            .map(|sample| {
                sample
                    .telemetry
                    .item_evictions
                    .saturating_add(sample.telemetry.neighbor_evictions)
                    .saturating_add(sample.telemetry.simhash_evictions)
            })
            .collect::<Vec<_>>();
        SummaryRecord {
            record: "summary",
            commit,
            case,
            cache_limits: first.cache_limits,
            samples: samples.len(),
            staging_p50_ns: percentile(samples, |sample| sample.staging_ns, 50),
            staging_p95_ns: percentile(samples, |sample| sample.staging_ns, 95),
            commit_p50_ns: percentile(samples, |sample| sample.commit_ns, 50),
            commit_p95_ns: percentile(samples, |sample| sample.commit_ns, 95),
            total_p50_ns: percentile(samples, |sample| sample.total_ns, 50),
            total_p95_ns: percentile(samples, |sample| sample.total_ns, 95),
            vectors_per_second_p50: case.batch_size as f64
                / (percentile(samples, |sample| sample.total_ns, 50) as f64 / 1_000_000_000.0),
            point_get_calls_p50: percentile(samples, |sample| sample.telemetry.point_get_calls, 50),
            multi_get_calls_p50: percentile(samples, |sample| sample.telemetry.multi_get_calls, 50),
            multi_get_keys_p50: percentile(samples, |sample| sample.telemetry.multi_get_keys, 50),
            put_calls_p50: percentile(samples, |sample| sample.telemetry.put_calls, 50),
            delete_calls_p50: percentile(samples, |sample| sample.telemetry.delete_calls, 50),
            staged_write_bytes_p50: percentile(
                samples,
                |sample| sample.telemetry.staged_write_bytes,
                50,
            ),
            item_hits_p50: percentile(samples, |sample| sample.telemetry.item_hits, 50),
            item_misses_p50: percentile(samples, |sample| sample.telemetry.item_misses, 50),
            neighbor_hits_p50: percentile(samples, |sample| sample.telemetry.neighbor_hits, 50),
            neighbor_misses_p50: percentile(samples, |sample| sample.telemetry.neighbor_misses, 50),
            simhash_hits_p50: percentile(samples, |sample| sample.telemetry.simhash_hits, 50),
            simhash_misses_p50: percentile(samples, |sample| sample.telemetry.simhash_misses, 50),
            cache_evictions_p50: percentile_values(cache_evictions, 50),
            dirty_neighbor_flushes_p50: percentile(
                samples,
                |sample| sample.telemetry.dirty_neighbor_flushes,
                50,
            ),
            peak_retained_payload_bytes: samples
                .iter()
                .map(|sample| sample.telemetry.peak_retained_payload_bytes)
                .max()
                .unwrap_or(0),
            allocated_calls_p50: percentile(samples, |sample| sample.allocated_calls, 50),
            allocated_bytes_p50: percentile(samples, |sample| sample.allocated_bytes, 50),
            unique_final_rows: first.unique_final_rows,
            unique_final_bytes: first.unique_final_bytes,
            graph_digest: &first.graph_digest,
            recall: first.recall,
        }
    }

    fn percentile(
        samples: &[VectorBatchBenchmarkSample],
        project: impl Fn(&VectorBatchBenchmarkSample) -> u64,
        percentile: usize,
    ) -> u64 {
        percentile_values(samples.iter().map(project).collect(), percentile)
    }

    fn percentile_values(mut values: Vec<u64>, percentile: usize) -> u64 {
        values.sort_unstable();
        let rank = values
            .len()
            .saturating_mul(percentile)
            .div_ceil(100)
            .saturating_sub(1);
        values[rank]
    }

    fn env_usize(name: &str, default: usize) -> usize {
        std::env::var(name).map_or(default, |value| {
            value
                .parse::<usize>()
                .unwrap_or_else(|error| panic!("{name} must be usize: {error}"))
        })
    }

    fn env_usize_list(name: &str, default: &[usize]) -> Vec<usize> {
        std::env::var(name).map_or_else(
            |_| default.to_vec(),
            |value| {
                value
                    .split(',')
                    .map(|entry| {
                        entry
                            .parse::<usize>()
                            .unwrap_or_else(|error| panic!("{name} entry must be usize: {error}"))
                    })
                    .collect()
            },
        )
    }

    fn env_metrics() -> Vec<VectorBatchBenchmarkMetric> {
        std::env::var("HELIX_VECTOR_BATCH_BENCH_METRICS").map_or_else(
            |_| vec![VectorBatchBenchmarkMetric::Cosine],
            |value| {
                value
                    .split(',')
                    .map(|entry| match entry {
                        "cosine" => VectorBatchBenchmarkMetric::Cosine,
                        "euclidean" => VectorBatchBenchmarkMetric::Euclidean,
                        "manhattan" => VectorBatchBenchmarkMetric::Manhattan,
                        other => panic!("unsupported benchmark metric: {other}"),
                    })
                    .collect()
            },
        )
    }

    fn env_workloads() -> Vec<VectorBatchBenchmarkWorkload> {
        std::env::var("HELIX_VECTOR_BATCH_BENCH_WORKLOADS").map_or_else(
            |_| vec![VectorBatchBenchmarkWorkload::Fresh],
            |value| {
                value
                    .split(',')
                    .map(|entry| match entry {
                        "fresh" => VectorBatchBenchmarkWorkload::Fresh,
                        "replacement" => VectorBatchBenchmarkWorkload::Replacement,
                        other => panic!("unsupported benchmark workload: {other}"),
                    })
                    .collect()
            },
        )
    }

    fn git_commit() -> String {
        let output = Command::new("git")
            .args(["rev-parse", "HEAD"])
            .output()
            .expect("git resolves the benchmark commit");
        assert!(output.status.success(), "git rev-parse HEAD succeeds");
        String::from_utf8(output.stdout)
            .expect("git commit is UTF-8")
            .trim()
            .to_string()
    }
}

fn main() {
    benchmark::main();
}
