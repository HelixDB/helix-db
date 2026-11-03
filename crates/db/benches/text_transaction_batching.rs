//! Transaction-batched Active V3 text-delta benchmark.
//!
//! Release protocol: three independent runs, each with three warmups and 25
//! measured samples, across 1/10/100/500 inserts and local/1 ms FTS upload modes.
//! Baseline instrumentation worktrees set `HELIX_TEXT_TRANSACTION_BENCH_SOURCE_COMMIT`
//! to record the untouched source revision separately from the harness commit.

use std::process::Command;

use db::bench_internals::{
    run_text_transaction_batch_benchmark_sample, TextTransactionBatchBenchmarkCase,
    TextTransactionBatchBenchmarkSample,
};
use serde::Serialize;

const DEFAULT_BATCHES: &[usize] = &[1, 10, 100, 500];
const DEFAULT_UPLOAD_LATENCIES_MILLIS: &[u64] = &[0, 1];

#[derive(Serialize)]
struct EnvironmentRecord<'record> {
    record: &'static str,
    commit: &'record str,
    harness_commit: &'record str,
    rustc: String,
    machine: String,
    independent_runs: usize,
    warmups_per_run: usize,
    samples_per_run: usize,
}

#[derive(Serialize)]
struct SampleRecord<'sample> {
    record: &'static str,
    commit: &'sample str,
    independent_run: usize,
    sample_index: usize,
    batch_size: usize,
    upload_latency_millis: u64,
    transaction_latency_ns: u64,
    inserts_per_second: f64,
    upload_count: u64,
    upload_bytes: u64,
    manifest_split_growth: u64,
    immediate_search_latency_ns: u64,
    post_compaction_search_latency_ns: u64,
    post_compaction_split_count: u64,
    search_digest: &'sample str,
}

#[derive(Serialize)]
struct SummaryRecord<'summary> {
    record: &'static str,
    commit: &'summary str,
    independent_run: usize,
    batch_size: usize,
    upload_latency_millis: u64,
    samples: usize,
    transaction_latency_median_ns: u64,
    transaction_latency_p50_ns: u64,
    transaction_latency_p95_ns: u64,
    inserts_per_second_median: f64,
    inserts_per_second_p50: f64,
    inserts_per_second_p95: f64,
    upload_count_median: u64,
    upload_count_p50: u64,
    upload_count_p95: u64,
    upload_bytes_median: u64,
    upload_bytes_p50: u64,
    upload_bytes_p95: u64,
    manifest_split_growth_median: u64,
    manifest_split_growth_p50: u64,
    manifest_split_growth_p95: u64,
    immediate_search_latency_median_ns: u64,
    immediate_search_latency_p50_ns: u64,
    immediate_search_latency_p95_ns: u64,
    post_compaction_search_latency_median_ns: u64,
    post_compaction_search_latency_p50_ns: u64,
    post_compaction_search_latency_p95_ns: u64,
    post_compaction_split_count_median: u64,
    post_compaction_split_count_p50: u64,
    post_compaction_split_count_p95: u64,
    search_digest: &'summary str,
}

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("text transaction benchmark runtime starts");
    runtime.block_on(run());
}

async fn run() {
    let harness_commit = command_output("git", &["rev-parse", "HEAD"]);
    let commit = std::env::var("HELIX_TEXT_TRANSACTION_BENCH_SOURCE_COMMIT")
        .unwrap_or_else(|_| harness_commit.clone());
    let rustc = command_output("rustc", &["-Vv"]);
    let machine = format!(
        "{}; cpu={}; model={}; memory_bytes={}",
        command_output("uname", &["-a"]),
        command_output("sysctl", &["-n", "machdep.cpu.brand_string"]),
        command_output("sysctl", &["-n", "hw.model"]),
        command_output("sysctl", &["-n", "hw.memsize"]),
    );
    let smoke = cfg!(debug_assertions);
    let independent_runs = env_usize(
        "HELIX_TEXT_TRANSACTION_BENCH_RUNS",
        if smoke { 1 } else { 3 },
    );
    let warmups = env_usize(
        "HELIX_TEXT_TRANSACTION_BENCH_WARMUPS",
        if smoke { 0 } else { 3 },
    );
    let samples = env_usize(
        "HELIX_TEXT_TRANSACTION_BENCH_SAMPLES",
        if smoke { 1 } else { 25 },
    );
    let allow_short = smoke
        || std::env::var("HELIX_TEXT_TRANSACTION_BENCH_ALLOW_SHORT")
            .is_ok_and(|value| value == "1");
    assert!(
        (independent_runs >= 3 && warmups >= 3 && samples >= 25) || allow_short,
        "release measurements require 3 independent runs, 3 warmups, and 25 samples"
    );
    let batches = env_usize_list("HELIX_TEXT_TRANSACTION_BENCH_BATCHES", DEFAULT_BATCHES);
    let upload_latencies = env_u64_list(
        "HELIX_TEXT_TRANSACTION_BENCH_UPLOAD_LATENCIES_MILLIS",
        DEFAULT_UPLOAD_LATENCIES_MILLIS,
    );
    println!(
        "{}",
        serde_json::to_string(&EnvironmentRecord {
            record: "environment",
            commit: &commit,
            harness_commit: &harness_commit,
            rustc,
            machine,
            independent_runs,
            warmups_per_run: warmups,
            samples_per_run: samples,
        })
        .expect("environment serializes")
    );

    for upload_latency_millis in upload_latencies {
        for batch_size in batches.iter().copied() {
            let case =
                TextTransactionBatchBenchmarkCase::try_new(batch_size, upload_latency_millis)
                    .expect("benchmark case validates");
            for independent_run in 0..independent_runs {
                for _ in 0..warmups {
                    run_text_transaction_batch_benchmark_sample(case)
                        .await
                        .expect("warmup sample succeeds");
                }
                let mut measured = Vec::with_capacity(samples);
                for sample_index in 0..samples {
                    let sample = run_text_transaction_batch_benchmark_sample(case)
                        .await
                        .expect("measured sample succeeds");
                    println!(
                        "{}",
                        serde_json::to_string(&sample_record(
                            &commit,
                            independent_run,
                            sample_index,
                            &sample,
                        ))
                        .expect("sample serializes")
                    );
                    measured.push(sample);
                }
                println!(
                    "{}",
                    serde_json::to_string(&summarize(&commit, independent_run, case, &measured,))
                        .expect("summary serializes")
                );
            }
        }
    }
}

fn sample_record<'sample>(
    commit: &'sample str,
    independent_run: usize,
    sample_index: usize,
    sample: &'sample TextTransactionBatchBenchmarkSample,
) -> SampleRecord<'sample> {
    SampleRecord {
        record: "sample",
        commit,
        independent_run,
        sample_index,
        batch_size: sample.case.batch_size,
        upload_latency_millis: sample.case.upload_latency_millis,
        transaction_latency_ns: sample.transaction_latency_ns,
        inserts_per_second: throughput(sample.case.batch_size, sample.transaction_latency_ns),
        upload_count: sample.upload_count,
        upload_bytes: sample.upload_bytes,
        manifest_split_growth: sample.manifest_split_growth,
        immediate_search_latency_ns: sample.immediate_search_latency_ns,
        post_compaction_search_latency_ns: sample.post_compaction_search_latency_ns,
        post_compaction_split_count: sample.post_compaction_split_count,
        search_digest: &sample.search_digest,
    }
}

fn summarize<'sample>(
    commit: &'sample str,
    independent_run: usize,
    case: TextTransactionBatchBenchmarkCase,
    samples: &'sample [TextTransactionBatchBenchmarkSample],
) -> SummaryRecord<'sample> {
    let first = samples.first().expect("summary has samples");
    assert!(samples.iter().all(|sample| sample.case == case));
    assert!(
        samples
            .iter()
            .all(|sample| sample.search_digest == first.search_digest),
        "deterministic workload has one result digest"
    );
    let transaction = sorted(samples.iter().map(|sample| sample.transaction_latency_ns));
    let throughput_values = sorted_f64(
        samples
            .iter()
            .map(|sample| throughput(sample.case.batch_size, sample.transaction_latency_ns)),
    );
    let uploads = sorted(samples.iter().map(|sample| sample.upload_count));
    let upload_bytes = sorted(samples.iter().map(|sample| sample.upload_bytes));
    let split_growth = sorted(samples.iter().map(|sample| sample.manifest_split_growth));
    let immediate = sorted(
        samples
            .iter()
            .map(|sample| sample.immediate_search_latency_ns),
    );
    let post = sorted(
        samples
            .iter()
            .map(|sample| sample.post_compaction_search_latency_ns),
    );
    let post_splits = sorted(
        samples
            .iter()
            .map(|sample| sample.post_compaction_split_count),
    );
    SummaryRecord {
        record: "summary",
        commit,
        independent_run,
        batch_size: case.batch_size,
        upload_latency_millis: case.upload_latency_millis,
        samples: samples.len(),
        transaction_latency_median_ns: percentile(&transaction, 50),
        transaction_latency_p50_ns: percentile(&transaction, 50),
        transaction_latency_p95_ns: percentile(&transaction, 95),
        inserts_per_second_median: percentile_f64(&throughput_values, 50),
        inserts_per_second_p50: percentile_f64(&throughput_values, 50),
        inserts_per_second_p95: percentile_f64(&throughput_values, 95),
        upload_count_median: percentile(&uploads, 50),
        upload_count_p50: percentile(&uploads, 50),
        upload_count_p95: percentile(&uploads, 95),
        upload_bytes_median: percentile(&upload_bytes, 50),
        upload_bytes_p50: percentile(&upload_bytes, 50),
        upload_bytes_p95: percentile(&upload_bytes, 95),
        manifest_split_growth_median: percentile(&split_growth, 50),
        manifest_split_growth_p50: percentile(&split_growth, 50),
        manifest_split_growth_p95: percentile(&split_growth, 95),
        immediate_search_latency_median_ns: percentile(&immediate, 50),
        immediate_search_latency_p50_ns: percentile(&immediate, 50),
        immediate_search_latency_p95_ns: percentile(&immediate, 95),
        post_compaction_search_latency_median_ns: percentile(&post, 50),
        post_compaction_search_latency_p50_ns: percentile(&post, 50),
        post_compaction_search_latency_p95_ns: percentile(&post, 95),
        post_compaction_split_count_median: percentile(&post_splits, 50),
        post_compaction_split_count_p50: percentile(&post_splits, 50),
        post_compaction_split_count_p95: percentile(&post_splits, 95),
        search_digest: &first.search_digest,
    }
}

fn throughput(batch_size: usize, latency_ns: u64) -> f64 {
    batch_size as f64 * 1_000_000_000.0 / latency_ns.max(1) as f64
}

fn sorted(values: impl Iterator<Item = u64>) -> Vec<u64> {
    let mut values = values.collect::<Vec<_>>();
    values.sort_unstable();
    values
}

fn sorted_f64(values: impl Iterator<Item = f64>) -> Vec<f64> {
    let mut values = values.collect::<Vec<_>>();
    values.sort_by(f64::total_cmp);
    values
}

fn percentile(values: &[u64], percentile: usize) -> u64 {
    values[percentile_index(values.len(), percentile)]
}

fn percentile_f64(values: &[f64], percentile: usize) -> f64 {
    values[percentile_index(values.len(), percentile)]
}

fn percentile_index(len: usize, percentile: usize) -> usize {
    assert!(len > 0);
    assert!(percentile <= 100);
    (len.saturating_sub(1) * percentile).div_ceil(100)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .parse()
                .expect("benchmark usize environment validates")
        })
        .unwrap_or(default)
}

fn env_usize_list(name: &str, default: &[usize]) -> Vec<usize> {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(|part| part.parse().expect("benchmark usize list validates"))
                .collect()
        })
        .unwrap_or_else(|| default.to_vec())
}

fn env_u64_list(name: &str, default: &[u64]) -> Vec<u64> {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(|part| part.parse().expect("benchmark u64 list validates"))
                .collect()
        })
        .unwrap_or_else(|| default.to_vec())
}

fn command_output(program: &str, arguments: &[&str]) -> String {
    let output = Command::new(program)
        .args(arguments)
        .output()
        .expect("benchmark environment command starts");
    assert!(
        output.status.success(),
        "benchmark environment command succeeds"
    );
    String::from_utf8(output.stdout)
        .expect("benchmark environment output is UTF-8")
        .trim()
        .to_string()
}
