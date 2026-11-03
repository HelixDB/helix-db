//! Exact collector-only FTS prefilter benchmark.
//!
//! Release protocol: three independent 100k-document fixtures, three warmups,
//! and 25 measured samples for every layout, density, term frequency, k, and
//! execution mode. Set `HELIX_FTS_PREFILTER_MILLION_SMOKE=1` for the separate
//! cap run.

use std::alloc::{GlobalAlloc, Layout, System};
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::Instant;

use db::bench_internals::{
    FtsPrefilterBenchmarkCase, FtsPrefilterBenchmarkFixture, FtsPrefilterBenchmarkLayout,
    FtsPrefilterBenchmarkSample, FtsPrefilterBenchmarkStrategy,
};
use serde::Serialize;

const RELEASE_DOCUMENTS: usize = 100_000;
const RELEASE_SPLITS: usize = 10;
const RELEASE_CANDIDATES: &[usize] = &[1, 10, 25, 50, 100, 1_000, 10_000, 50_000, 100_000];
const QUERIES: &[&str] = &["rareterm", "mediumterm", "commonterm"];
const K_VALUES: &[usize] = &[10, 100];
const LAYOUTS: &[FtsPrefilterBenchmarkLayout] = &[
    FtsPrefilterBenchmarkLayout::MultiSplit,
    FtsPrefilterBenchmarkLayout::Compacted,
];
const STRATEGIES: &[FtsPrefilterBenchmarkStrategy] = &[
    FtsPrefilterBenchmarkStrategy::Collector,
    FtsPrefilterBenchmarkStrategy::Unrestricted,
];
const MILLION_SMOKE_PEAK_ALLOCATION_LIMIT_BYTES: u64 = 64 * 1024 * 1024;

struct CountingAllocator;

static TRACK_ALLOCATIONS: AtomicBool = AtomicBool::new(false);
static ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static LIVE_ALLOCATED_BYTES: AtomicI64 = AtomicI64::new(0);
static PEAK_ALLOCATED_BYTES: AtomicI64 = AtomicI64::new(0);

fn adjust_live_allocations(delta: i64) {
    let live = LIVE_ALLOCATED_BYTES.fetch_add(delta, Ordering::Relaxed) + delta;
    let mut peak = PEAK_ALLOCATED_BYTES.load(Ordering::Relaxed);
    while live > peak {
        match PEAK_ALLOCATED_BYTES.compare_exchange_weak(
            peak,
            live,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(observed) => peak = observed,
        }
    }
}

// SAFETY: every operation is forwarded unchanged to the system allocator.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            adjust_live_allocations(i64::try_from(layout.size()).unwrap_or(i64::MAX));
        }
        if TRACK_ALLOCATIONS.load(Ordering::Relaxed) && !pointer.is_null() {
            ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(
                u64::try_from(layout.size()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        adjust_live_allocations(-i64::try_from(layout.size()).unwrap_or(i64::MAX));
        // SAFETY: the allocation and layout are forwarded unchanged.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            adjust_live_allocations(i64::try_from(layout.size()).unwrap_or(i64::MAX));
        }
        if TRACK_ALLOCATIONS.load(Ordering::Relaxed) && !pointer.is_null() {
            ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(
                u64::try_from(layout.size()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        }
        pointer
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let resized = unsafe { System.realloc(pointer, layout, new_size) };
        if !resized.is_null() {
            let old_size = i64::try_from(layout.size()).unwrap_or(i64::MAX);
            let new_size_i64 = i64::try_from(new_size).unwrap_or(i64::MAX);
            adjust_live_allocations(new_size_i64.saturating_sub(old_size));
        }
        if TRACK_ALLOCATIONS.load(Ordering::Relaxed) && !resized.is_null() {
            ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(
                u64::try_from(new_size).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        }
        resized
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[derive(Serialize)]
struct EnvironmentRecord<'record> {
    record: &'static str,
    commit: &'record str,
    rustc: String,
    machine: String,
    document_count: usize,
    initial_split_count: usize,
    independent_runs: usize,
    warmups_per_run: usize,
    samples_per_run: usize,
    million_document_smoke: bool,
}

#[derive(Serialize)]
struct SampleRecord<'sample> {
    record: &'static str,
    commit: &'sample str,
    independent_run: usize,
    sample_index: usize,
    layout: &'static str,
    candidate_count: usize,
    query: &'static str,
    k: usize,
    strategy: &'static str,
    latency_ns: u64,
    allocation_calls: u64,
    allocated_bytes: u64,
    peak_allocated_bytes: u64,
    object_store_reads: u64,
    object_store_bytes: u64,
    split_count: usize,
    result_count: usize,
    result_digest: &'sample str,
}

#[derive(Debug, Clone)]
struct MeasuredSample {
    latency_ns: u64,
    allocation_calls: u64,
    allocated_bytes: u64,
    peak_allocated_bytes: u64,
    sample: FtsPrefilterBenchmarkSample,
}

#[derive(Debug, Clone)]
struct RunSummary {
    independent_run: usize,
    case: FtsPrefilterBenchmarkCase,
    latency_p50_ns: u64,
    latency_p95_ns: u64,
    allocation_calls_p50: u64,
    allocation_calls_p95: u64,
    allocated_bytes_p50: u64,
    allocated_bytes_p95: u64,
    peak_allocated_bytes_p50: u64,
    peak_allocated_bytes_p95: u64,
    object_store_reads_p50: u64,
    object_store_reads_p95: u64,
    object_store_bytes_p50: u64,
    object_store_bytes_p95: u64,
    split_count: usize,
    result_digest: String,
}

#[derive(Serialize)]
struct SummaryRecord<'summary> {
    record: &'static str,
    commit: &'summary str,
    independent_run: usize,
    layout: &'static str,
    candidate_count: usize,
    query: &'static str,
    k: usize,
    strategy: &'static str,
    samples: usize,
    latency_p50_ns: u64,
    latency_p95_ns: u64,
    allocation_calls_p50: u64,
    allocation_calls_p95: u64,
    allocated_bytes_p50: u64,
    allocated_bytes_p95: u64,
    peak_allocated_bytes_p50: u64,
    peak_allocated_bytes_p95: u64,
    object_store_reads_p50: u64,
    object_store_reads_p95: u64,
    object_store_bytes_p50: u64,
    object_store_bytes_p95: u64,
    split_count: usize,
    result_digest: &'summary str,
}

#[derive(Serialize)]
struct EvidenceReport {
    environment: serde_json::Value,
    summaries: Vec<serde_json::Value>,
}

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("FTS prefilter benchmark runtime starts");
    runtime.block_on(run());
}

async fn run() {
    let commit = command_output("git", &["rev-parse", "HEAD"]);
    let million_smoke =
        std::env::var("HELIX_FTS_PREFILTER_MILLION_SMOKE").is_ok_and(|value| value == "1");
    let debug_smoke = cfg!(debug_assertions);
    let document_count = if million_smoke {
        1_000_000
    } else if debug_smoke {
        env_usize("HELIX_FTS_PREFILTER_DOCUMENTS", 1_000)
    } else {
        env_usize("HELIX_FTS_PREFILTER_DOCUMENTS", RELEASE_DOCUMENTS)
    };
    let initial_split_count = env_usize(
        "HELIX_FTS_PREFILTER_SPLITS",
        if debug_smoke { 4 } else { RELEASE_SPLITS },
    );
    let independent_runs = env_usize(
        "HELIX_FTS_PREFILTER_RUNS",
        if debug_smoke || million_smoke { 1 } else { 3 },
    );
    let warmups = env_usize(
        "HELIX_FTS_PREFILTER_WARMUPS",
        if debug_smoke || million_smoke { 0 } else { 3 },
    );
    let samples = env_usize(
        "HELIX_FTS_PREFILTER_SAMPLES",
        if debug_smoke || million_smoke { 1 } else { 25 },
    );
    let allow_short = debug_smoke
        || million_smoke
        || std::env::var("HELIX_FTS_PREFILTER_ALLOW_SHORT").is_ok_and(|value| value == "1");
    assert!(
        (document_count == RELEASE_DOCUMENTS
            && independent_runs >= 3
            && warmups >= 3
            && samples >= 25)
            || allow_short,
        "release measurements require 100k documents, 3 independent runs, 3 warmups, and 25 samples"
    );
    let default_candidate_counts = if million_smoke {
        vec![1_000_000]
    } else if debug_smoke {
        vec![100.min(document_count), document_count]
    } else {
        RELEASE_CANDIDATES
            .iter()
            .copied()
            .filter(|count| *count <= document_count)
            .collect()
    };
    let candidate_counts =
        env_usize_list("HELIX_FTS_PREFILTER_CANDIDATES", default_candidate_counts);
    let default_queries = if million_smoke {
        vec![QUERIES[2]]
    } else {
        QUERIES.to_vec()
    };
    let queries = env_string_list("HELIX_FTS_PREFILTER_QUERIES")
        .map(|values| {
            values
                .into_iter()
                .map(|query| match query.as_str() {
                    "rareterm" => "rareterm",
                    "mediumterm" => "mediumterm",
                    "commonterm" => "commonterm",
                    _ => panic!("HELIX_FTS_PREFILTER_QUERIES contains an unsupported query"),
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or(default_queries);
    let default_k_values = if million_smoke {
        vec![K_VALUES[1]]
    } else {
        K_VALUES.to_vec()
    };
    let k_values = env_usize_list("HELIX_FTS_PREFILTER_K", default_k_values);
    let layouts = env_string_list("HELIX_FTS_PREFILTER_LAYOUTS")
        .map(|values| {
            values
                .into_iter()
                .map(|layout| match layout.as_str() {
                    "multi_split" => FtsPrefilterBenchmarkLayout::MultiSplit,
                    "compacted" => FtsPrefilterBenchmarkLayout::Compacted,
                    _ => panic!("HELIX_FTS_PREFILTER_LAYOUTS contains an unsupported layout"),
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| LAYOUTS.to_vec());
    let strategies = env_string_list("HELIX_FTS_PREFILTER_STRATEGIES")
        .map(|values| {
            values
                .into_iter()
                .map(|strategy| match strategy.as_str() {
                    "collector" => FtsPrefilterBenchmarkStrategy::Collector,
                    "unrestricted" => FtsPrefilterBenchmarkStrategy::Unrestricted,
                    _ => panic!("HELIX_FTS_PREFILTER_STRATEGIES contains an unsupported strategy"),
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| STRATEGIES.to_vec());

    let environment = EnvironmentRecord {
        record: "environment",
        commit: &commit,
        rustc: command_output("rustc", &["-Vv"]),
        machine: command_output("uname", &["-a"]),
        document_count,
        initial_split_count,
        independent_runs,
        warmups_per_run: warmups,
        samples_per_run: samples,
        million_document_smoke: million_smoke,
    };
    let environment_json =
        serde_json::to_value(&environment).expect("environment evidence serializes");
    println!(
        "{}",
        serde_json::to_string(&environment).expect("environment serializes")
    );

    let mut summaries = Vec::new();
    let mut evidence_summaries = Vec::new();
    for independent_run in 0..independent_runs {
        let fixture = FtsPrefilterBenchmarkFixture::try_new(document_count, initial_split_count)
            .await
            .expect("FTS prefilter benchmark fixture builds");
        assert_eq!(fixture.document_count(), document_count);
        for layout in layouts.iter().copied() {
            for candidate_count in candidate_counts.iter().copied() {
                for query in queries.iter().copied() {
                    for k in k_values.iter().copied() {
                        for strategy in strategies.iter().copied() {
                            let case = FtsPrefilterBenchmarkCase::try_new(
                                layout,
                                strategy,
                                candidate_count,
                                query,
                                k,
                                document_count,
                            )
                            .expect("benchmark case validates");
                            for _ in 0..warmups {
                                fixture
                                    .run_case(case)
                                    .await
                                    .expect("benchmark warmup matches the exact oracle");
                            }
                            let mut measured = Vec::with_capacity(samples);
                            for sample_index in 0..samples {
                                let measurement = measure(&fixture, case).await;
                                println!(
                                    "{}",
                                    serde_json::to_string(&SampleRecord {
                                        record: "sample",
                                        commit: &commit,
                                        independent_run,
                                        sample_index,
                                        layout: layout.as_str(),
                                        candidate_count,
                                        query,
                                        k,
                                        strategy: strategy.as_str(),
                                        latency_ns: measurement.latency_ns,
                                        allocation_calls: measurement.allocation_calls,
                                        allocated_bytes: measurement.allocated_bytes,
                                        peak_allocated_bytes: measurement.peak_allocated_bytes,
                                        object_store_reads: measurement.sample.object_store_reads,
                                        object_store_bytes: measurement.sample.object_store_bytes,
                                        split_count: measurement.sample.split_count,
                                        result_count: measurement.sample.result_count,
                                        result_digest: &measurement.sample.result_digest,
                                    })
                                    .expect("sample serializes")
                                );
                                measured.push(measurement);
                            }
                            let summary = summarize(independent_run, case, &measured);
                            let summary_record = SummaryRecord {
                                record: "summary",
                                commit: &commit,
                                independent_run,
                                layout: layout.as_str(),
                                candidate_count,
                                query,
                                k,
                                strategy: strategy.as_str(),
                                samples,
                                latency_p50_ns: summary.latency_p50_ns,
                                latency_p95_ns: summary.latency_p95_ns,
                                allocation_calls_p50: summary.allocation_calls_p50,
                                allocation_calls_p95: summary.allocation_calls_p95,
                                allocated_bytes_p50: summary.allocated_bytes_p50,
                                allocated_bytes_p95: summary.allocated_bytes_p95,
                                peak_allocated_bytes_p50: summary.peak_allocated_bytes_p50,
                                peak_allocated_bytes_p95: summary.peak_allocated_bytes_p95,
                                object_store_reads_p50: summary.object_store_reads_p50,
                                object_store_reads_p95: summary.object_store_reads_p95,
                                object_store_bytes_p50: summary.object_store_bytes_p50,
                                object_store_bytes_p95: summary.object_store_bytes_p95,
                                split_count: summary.split_count,
                                result_digest: &summary.result_digest,
                            };
                            println!(
                                "{}",
                                serde_json::to_string(&summary_record).expect("summary serializes")
                            );
                            evidence_summaries.push(
                                serde_json::to_value(&summary_record)
                                    .expect("summary evidence serializes"),
                            );
                            summaries.push(summary);
                        }
                    }
                }
            }
        }
    }

    if let Ok(path) = std::env::var("HELIX_FTS_PREFILTER_REPORT_PATH") {
        let path = PathBuf::from(path);
        let path = if path.is_absolute() {
            path
        } else {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .join(path)
        };
        let report = EvidenceReport {
            environment: environment_json,
            summaries: evidence_summaries,
        };
        fs::write(
            path,
            serde_json::to_vec_pretty(&report).expect("benchmark evidence serializes"),
        )
        .expect("benchmark evidence report writes");
    }

    if million_smoke {
        assert_million_smoke_bounded(&summaries, document_count);
    } else {
        assert_dense_allocations_bounded(&summaries, document_count);
    }
}

async fn measure(
    fixture: &FtsPrefilterBenchmarkFixture,
    case: FtsPrefilterBenchmarkCase,
) -> MeasuredSample {
    ALLOCATION_CALLS.store(0, Ordering::Relaxed);
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    let allocation_baseline = LIVE_ALLOCATED_BYTES.load(Ordering::Relaxed);
    PEAK_ALLOCATED_BYTES.store(allocation_baseline, Ordering::Relaxed);
    TRACK_ALLOCATIONS.store(true, Ordering::SeqCst);
    let started = Instant::now();
    let sample = fixture
        .run_case(case)
        .await
        .expect("measured result matches the exact oracle");
    let latency_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    TRACK_ALLOCATIONS.store(false, Ordering::SeqCst);
    let peak_allocated_bytes = PEAK_ALLOCATED_BYTES
        .load(Ordering::Relaxed)
        .saturating_sub(allocation_baseline);
    MeasuredSample {
        latency_ns,
        allocation_calls: ALLOCATION_CALLS.load(Ordering::Relaxed),
        allocated_bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
        peak_allocated_bytes: u64::try_from(peak_allocated_bytes).unwrap_or(0),
        sample,
    }
}

fn summarize(
    independent_run: usize,
    case: FtsPrefilterBenchmarkCase,
    samples: &[MeasuredSample],
) -> RunSummary {
    let digests = samples
        .iter()
        .map(|sample| sample.sample.result_digest.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(digests.len(), 1, "result digest must be deterministic");
    RunSummary {
        independent_run,
        case,
        latency_p50_ns: percentile(samples.iter().map(|sample| sample.latency_ns), 50),
        latency_p95_ns: percentile(samples.iter().map(|sample| sample.latency_ns), 95),
        allocation_calls_p50: percentile(samples.iter().map(|sample| sample.allocation_calls), 50),
        allocation_calls_p95: percentile(samples.iter().map(|sample| sample.allocation_calls), 95),
        allocated_bytes_p50: percentile(samples.iter().map(|sample| sample.allocated_bytes), 50),
        allocated_bytes_p95: percentile(samples.iter().map(|sample| sample.allocated_bytes), 95),
        peak_allocated_bytes_p50: percentile(
            samples.iter().map(|sample| sample.peak_allocated_bytes),
            50,
        ),
        peak_allocated_bytes_p95: percentile(
            samples.iter().map(|sample| sample.peak_allocated_bytes),
            95,
        ),
        object_store_reads_p50: percentile(
            samples
                .iter()
                .map(|sample| sample.sample.object_store_reads),
            50,
        ),
        object_store_reads_p95: percentile(
            samples
                .iter()
                .map(|sample| sample.sample.object_store_reads),
            95,
        ),
        object_store_bytes_p50: percentile(
            samples
                .iter()
                .map(|sample| sample.sample.object_store_bytes),
            50,
        ),
        object_store_bytes_p95: percentile(
            samples
                .iter()
                .map(|sample| sample.sample.object_store_bytes),
            95,
        ),
        split_count: samples[0].sample.split_count,
        result_digest: samples[0].sample.result_digest.clone(),
    }
}

fn assert_dense_allocations_bounded(summaries: &[RunSummary], document_count: usize) {
    let comparisons = summaries
        .iter()
        .filter(|summary| {
            summary.case.candidate_count == document_count
                && summary.case.strategy == FtsPrefilterBenchmarkStrategy::Collector
        })
        .filter_map(|collector| {
            matching_summary(
                summaries,
                collector,
                FtsPrefilterBenchmarkStrategy::Unrestricted,
            )
            .map(|unrestricted| (collector, unrestricted))
        })
        .collect::<Vec<_>>();
    if comparisons.is_empty() {
        return;
    }
    assert!(
        comparisons.iter().all(|(collector, unrestricted)| {
            collector.result_digest == unrestricted.result_digest
                && collector.peak_allocated_bytes_p95
                    <= unrestricted.peak_allocated_bytes_p95.saturating_mul(2)
        }),
        "collector results must match unrestricted FTS at full density and allocate no more than 2x"
    );
}

fn assert_million_smoke_bounded(summaries: &[RunSummary], document_count: usize) {
    let collector_summaries = summaries
        .iter()
        .filter(|summary| {
            summary.case.candidate_count == document_count
                && summary.case.strategy == FtsPrefilterBenchmarkStrategy::Collector
        })
        .collect::<Vec<_>>();
    assert!(
        !collector_summaries.is_empty(),
        "million-candidate smoke must measure the collector"
    );
    assert!(
        collector_summaries.iter().all(|collector| {
            let Some(unrestricted) = matching_summary(
                summaries,
                collector,
                FtsPrefilterBenchmarkStrategy::Unrestricted,
            ) else {
                return false;
            };
            collector.result_digest == unrestricted.result_digest
                && collector.peak_allocated_bytes_p95
                    <= MILLION_SMOKE_PEAK_ALLOCATION_LIMIT_BYTES
        }),
        "million-candidate collector results must match unrestricted FTS and remain below the absolute peak-allocation limit"
    );
}

fn matching_summary<'summary>(
    summaries: &'summary [RunSummary],
    source: &RunSummary,
    strategy: FtsPrefilterBenchmarkStrategy,
) -> Option<&'summary RunSummary> {
    summaries.iter().find(|candidate| {
        candidate.independent_run == source.independent_run
            && candidate.case.layout == source.case.layout
            && candidate.case.candidate_count == source.case.candidate_count
            && candidate.case.query == source.case.query
            && candidate.case.k == source.case.k
            && candidate.case.strategy == strategy
    })
}

fn percentile(values: impl IntoIterator<Item = u64>, percentile: usize) -> u64 {
    let mut values = values.into_iter().collect::<Vec<_>>();
    values.sort_unstable();
    let index = (values.len() - 1) * percentile / 100;
    values[index]
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .parse()
                .unwrap_or_else(|_| panic!("{name} must be usize"))
        })
        .unwrap_or(default)
}

fn env_usize_list(name: &str, default: Vec<usize>) -> Vec<usize> {
    env_string_list(name)
        .map(|values| {
            values
                .into_iter()
                .map(|value| {
                    value
                        .parse()
                        .unwrap_or_else(|_| panic!("{name} entries must be usize"))
                })
                .collect()
        })
        .unwrap_or(default)
}

fn env_string_list(name: &str) -> Option<Vec<String>> {
    std::env::var(name).ok().map(|value| {
        value
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(str::to_owned)
            .collect()
    })
}

fn command_output(program: &str, arguments: &[&str]) -> String {
    String::from_utf8(
        Command::new(program)
            .args(arguments)
            .output()
            .unwrap_or_else(|error| panic!("{program} failed to start: {error}"))
            .stdout,
    )
    .expect("command output is UTF-8")
    .trim()
    .to_string()
}
