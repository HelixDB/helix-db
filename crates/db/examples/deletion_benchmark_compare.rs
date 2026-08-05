//! Hierarchical-bootstrap comparison for deletion benchmark JSONL files.
//!
//! Usage: `deletion_benchmark_compare baseline-a.jsonl,baseline-b.jsonl \
//! candidate-a.jsonl,candidate-b.jsonl`.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;

use db::production_coverage::{DeletionBenchmarkCase, DeletionBenchmarkSample};
use rand::{RngExt, SeedableRng};
use serde::{Deserialize, Serialize};

const BOOTSTRAP_RESAMPLES: usize = 10_000;
const BOOTSTRAP_SEED: u64 = 0x4845_4c49_5844_454c;

#[derive(Deserialize)]
struct SampleRecord {
    record: String,
    run_id: usize,
    sample: DeletionBenchmarkSample,
}

#[derive(Serialize)]
struct ComparisonRecord {
    record: &'static str,
    case: DeletionBenchmarkCase,
    baseline_runs: usize,
    candidate_runs: usize,
    samples_per_baseline_run: usize,
    samples_per_candidate_run: usize,
    baseline_p95_ns: u64,
    candidate_p95_ns: u64,
    improvement_fraction: f64,
    improvement_ci95_low: f64,
    improvement_ci95_high: f64,
}

fn main() {
    let mut arguments = std::env::args().skip(1);
    let baseline = arguments.next().expect("baseline JSONL paths are required");
    let candidate = arguments
        .next()
        .expect("candidate JSONL paths are required");
    assert!(
        arguments.next().is_none(),
        "only two path groups are accepted"
    );

    let baseline = load_group(&baseline);
    let candidate = load_group(&candidate);
    assert_eq!(
        baseline.keys().collect::<BTreeSet<_>>(),
        candidate.keys().collect::<BTreeSet<_>>(),
        "baseline and candidate cases must match"
    );
    for (case, baseline_runs) in baseline {
        let candidate_runs = candidate
            .get(&case)
            .expect("matching candidate case exists");
        let comparison = compare(case, &baseline_runs, candidate_runs);
        println!(
            "{}",
            serde_json::to_string(&comparison).expect("comparison serializes")
        );
    }
}

fn load_group(specification: &str) -> BTreeMap<DeletionBenchmarkCase, Vec<Vec<u64>>> {
    let mut cases = BTreeMap::<DeletionBenchmarkCase, BTreeMap<usize, Vec<u64>>>::new();
    for path in specification.split(',').map(str::trim) {
        let input = fs::read_to_string(path)
            .unwrap_or_else(|error| panic!("failed to read benchmark file `{path}`: {error}"));
        for line in input.lines().filter(|line| !line.trim().is_empty()) {
            let Ok(record) = serde_json::from_str::<SampleRecord>(line) else {
                continue;
            };
            if record.record != "sample" {
                continue;
            }
            cases
                .entry(record.sample.case)
                .or_default()
                .entry(record.run_id)
                .or_default()
                .push(record.sample.total_ns);
        }
    }
    cases
        .into_iter()
        .map(|(case, runs)| (case, runs.into_values().collect()))
        .collect()
}

fn compare(
    case: DeletionBenchmarkCase,
    baseline: &[Vec<u64>],
    candidate: &[Vec<u64>],
) -> ComparisonRecord {
    validate_runs("baseline", baseline);
    validate_runs("candidate", candidate);
    let baseline_p95 = percentile(&flatten(baseline), 95);
    let candidate_p95 = percentile(&flatten(candidate), 95);
    let observed_improvement = improvement(baseline_p95, candidate_p95);
    let mut rng = rand::rngs::StdRng::seed_from_u64(BOOTSTRAP_SEED);
    let mut resamples = Vec::with_capacity(BOOTSTRAP_RESAMPLES);
    for _ in 0..BOOTSTRAP_RESAMPLES {
        let baseline_sample = hierarchical_resample(baseline, &mut rng);
        let candidate_sample = hierarchical_resample(candidate, &mut rng);
        resamples.push(improvement(
            percentile(&baseline_sample, 95),
            percentile(&candidate_sample, 95),
        ));
    }
    resamples.sort_by(f64::total_cmp);
    ComparisonRecord {
        record: "comparison",
        case,
        baseline_runs: baseline.len(),
        candidate_runs: candidate.len(),
        samples_per_baseline_run: baseline[0].len(),
        samples_per_candidate_run: candidate[0].len(),
        baseline_p95_ns: baseline_p95,
        candidate_p95_ns: candidate_p95,
        improvement_fraction: observed_improvement,
        improvement_ci95_low: resamples[BOOTSTRAP_RESAMPLES * 25 / 1_000],
        improvement_ci95_high: resamples[BOOTSTRAP_RESAMPLES * 975 / 1_000],
    }
}

fn validate_runs(label: &str, runs: &[Vec<u64>]) {
    let allow_short =
        std::env::var("HELIX_DELETION_BENCH_ALLOW_SHORT").is_ok_and(|value| value == "1");
    assert!(!runs.is_empty(), "{label} contains no measured runs");
    assert!(
        allow_short || runs.len() >= 5,
        "{label} requires at least five independent runs"
    );
    let samples = runs[0].len();
    assert!(
        allow_short || samples >= 25,
        "{label} requires at least 25 samples per run"
    );
    assert!(
        runs.iter().all(|run| run.len() == samples),
        "{label} runs must have equal sample counts"
    );
}

fn hierarchical_resample(runs: &[Vec<u64>], rng: &mut rand::rngs::StdRng) -> Vec<u64> {
    let mut sample = Vec::with_capacity(runs.len() * runs[0].len());
    for _ in 0..runs.len() {
        let run = &runs[rng.random_range(0..runs.len())];
        for _ in 0..run.len() {
            sample.push(run[rng.random_range(0..run.len())]);
        }
    }
    sample
}

fn flatten(runs: &[Vec<u64>]) -> Vec<u64> {
    runs.iter().flatten().copied().collect()
}

fn percentile(values: &[u64], percentile: usize) -> u64 {
    let mut values = values.to_vec();
    values.sort_unstable();
    let rank = values.len().saturating_mul(percentile).div_ceil(100);
    values[rank.saturating_sub(1).min(values.len().saturating_sub(1))]
}

fn improvement(baseline: u64, candidate: u64) -> f64 {
    (baseline as f64 - candidate as f64) / baseline as f64
}
