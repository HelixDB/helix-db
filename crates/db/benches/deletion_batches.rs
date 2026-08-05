//! Serialized foreground deletion benchmark with raw JSON observations.
//!
//! Release runs execute five independent child processes by default. Override
//! the profile, runs, samples, workloads, sizes, or cache policies with the
//! documented `HELIX_DELETION_BENCH_*` environment variables.

use std::alloc::{GlobalAlloc, Layout, System};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use db::production_coverage::{
    DeletionBenchmarkCachePolicy, DeletionBenchmarkCase, DeletionBenchmarkFixture,
    DeletionBenchmarkSample, DeletionBenchmarkWorkload,
};
use serde::Serialize;

struct CountingAllocator;

static TRACK_ALLOCATIONS: AtomicBool = AtomicBool::new(false);
static ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

// SAFETY: every operation is forwarded unchanged to the system allocator.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation(layout.size());
        // SAFETY: the caller supplied `layout` under `GlobalAlloc::alloc`.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: the caller supplied the allocation and layout pair.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation(layout.size());
        // SAFETY: the caller supplied `layout` under `GlobalAlloc::alloc_zeroed`.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record_allocation(new_size);
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
    run_id: usize,
    sample_index: usize,
    sample: &'sample DeletionBenchmarkSample,
}

#[derive(Serialize)]
struct SummaryRecord<'summary> {
    record: &'static str,
    commit: &'summary str,
    run_id: usize,
    case: DeletionBenchmarkCase,
    samples: usize,
    staging_p50_ns: u64,
    staging_p95_ns: u64,
    preparation_p50_ns: u64,
    preparation_p95_ns: u64,
    commit_p50_ns: u64,
    commit_p95_ns: u64,
    post_commit_p50_ns: u64,
    post_commit_p95_ns: u64,
    total_p50_ns: u64,
    total_p95_ns: u64,
    entities_per_second_p50: f64,
    process_cpu_p50_ns: u64,
    allocation_calls_p50: u64,
    allocated_bytes_p50: u64,
    peak_rss_bytes: u64,
    physical_requests_p50: u64,
    physical_put_bytes_p50: u64,
}

fn main() {
    if should_orchestrate_children() {
        run_children();
        return;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("deletion benchmark runtime starts");
    runtime.block_on(run());
}

fn should_orchestrate_children() -> bool {
    !cfg!(debug_assertions)
        && std::env::var_os("HELIX_DELETION_BENCH_CHILD").is_none()
        && env_usize("HELIX_DELETION_BENCH_RUNS", 5) > 1
}

fn run_children() {
    let executable = std::env::current_exe().expect("deletion benchmark executable resolves");
    let runs = env_usize("HELIX_DELETION_BENCH_RUNS", 5);
    for run_id in 0..runs {
        let status = Command::new(&executable)
            .env("HELIX_DELETION_BENCH_CHILD", "1")
            .env("HELIX_DELETION_BENCH_RUN_ID", run_id.to_string())
            .status()
            .expect("independent deletion benchmark process starts");
        assert!(status.success(), "deletion benchmark run {run_id} failed");
    }
}

async fn run() {
    let commit = git_commit();
    let run_id = env_usize("HELIX_DELETION_BENCH_RUN_ID", 0);
    let profile = std::env::var("HELIX_DELETION_BENCH_PROFILE").unwrap_or_else(|_| {
        if cfg!(debug_assertions) {
            "smoke".to_string()
        } else {
            "standard".to_string()
        }
    });
    let samples = env_usize(
        "HELIX_DELETION_BENCH_SAMPLES",
        if profile == "smoke" { 1 } else { 25 },
    );
    let allow_short = profile == "smoke"
        || std::env::var("HELIX_DELETION_BENCH_ALLOW_SHORT").is_ok_and(|value| value == "1");
    assert!(
        samples >= 25 || allow_short,
        "merge-blocking deletion measurements require at least 25 samples"
    );

    let sizes = selected_sizes(&profile);
    let workloads = selected_workloads(&profile);
    let cache_policies = selected_cache_policies(&profile);
    for workload in workloads {
        for cache_policy in cache_policies.iter().copied() {
            for size in sizes.iter().copied() {
                let case = if profile == "vector-100k" {
                    DeletionBenchmarkCase::stress_100k(workload, cache_policy)
                } else {
                    DeletionBenchmarkCase::try_supported(workload, size, cache_policy)
                        .expect("selected deletion benchmark case validates")
                };
                let mut measured = Vec::with_capacity(samples);
                for sample_index in 0..samples {
                    let fixture = DeletionBenchmarkFixture::prepare(case)
                        .await
                        .expect("deletion benchmark fixture prepares");
                    ALLOCATION_CALLS.store(0, Ordering::Relaxed);
                    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
                    let (rss, baseline_rss) =
                        RssSampler::start().await.expect("RSS sampler starts");
                    let cpu_before = process_cpu_ns().expect("process CPU reads");
                    TRACK_ALLOCATIONS.store(true, Ordering::Release);
                    let sample = fixture
                        .run_sample()
                        .await
                        .expect("deletion benchmark sample succeeds");
                    TRACK_ALLOCATIONS.store(false, Ordering::Release);
                    let cpu_after = process_cpu_ns().expect("process CPU reads");
                    let peak_rss = rss.stop().await.expect("RSS sampler stops");
                    let sample = sample.with_process_measurements(
                        ALLOCATION_CALLS.load(Ordering::Relaxed),
                        ALLOCATED_BYTES.load(Ordering::Relaxed),
                        baseline_rss,
                        peak_rss,
                        cpu_after.saturating_sub(cpu_before),
                    );
                    println!(
                        "{}",
                        serde_json::to_string(&SampleRecord {
                            record: "sample",
                            commit: &commit,
                            run_id,
                            sample_index,
                            sample: &sample,
                        })
                        .expect("sample serializes")
                    );
                    fixture
                        .verify_and_close()
                        .await
                        .expect("deletion benchmark fixture verifies and closes");
                    measured.push(sample);
                }
                println!(
                    "{}",
                    serde_json::to_string(&summarize(&commit, run_id, case, &measured))
                        .expect("summary serializes")
                );
            }
        }
    }
}

fn summarize<'sample>(
    commit: &'sample str,
    run_id: usize,
    case: DeletionBenchmarkCase,
    samples: &'sample [DeletionBenchmarkSample],
) -> SummaryRecord<'sample> {
    let peak_rss_bytes = samples
        .iter()
        .map(|sample| sample.peak_rss_bytes)
        .max()
        .unwrap_or(0);
    SummaryRecord {
        record: "summary",
        commit,
        run_id,
        case,
        samples: samples.len(),
        staging_p50_ns: percentile(samples, |sample| sample.staging_ns, 50),
        staging_p95_ns: percentile(samples, |sample| sample.staging_ns, 95),
        preparation_p50_ns: percentile(
            samples,
            |sample| sample.telemetry.phases.preparation_ns,
            50,
        ),
        preparation_p95_ns: percentile(
            samples,
            |sample| sample.telemetry.phases.preparation_ns,
            95,
        ),
        commit_p50_ns: percentile(samples, |sample| sample.telemetry.phases.commit_ns, 50),
        commit_p95_ns: percentile(samples, |sample| sample.telemetry.phases.commit_ns, 95),
        post_commit_p50_ns: percentile(
            samples,
            |sample| sample.telemetry.phases.post_commit_ns,
            50,
        ),
        post_commit_p95_ns: percentile(
            samples,
            |sample| sample.telemetry.phases.post_commit_ns,
            95,
        ),
        total_p50_ns: percentile(samples, |sample| sample.total_ns, 50),
        total_p95_ns: percentile(samples, |sample| sample.total_ns, 95),
        entities_per_second_p50: case.batch_size.get() as f64
            / (percentile(samples, |sample| sample.total_ns, 50) as f64 / 1_000_000_000.0),
        process_cpu_p50_ns: percentile(samples, |sample| sample.process_cpu_ns, 50),
        allocation_calls_p50: percentile(samples, |sample| sample.allocation_calls, 50),
        allocated_bytes_p50: percentile(samples, |sample| sample.allocated_bytes, 50),
        peak_rss_bytes,
        physical_requests_p50: percentile(
            samples,
            |sample| {
                let operations = sample.physical_object_store_operations;
                operations
                    .puts
                    .saturating_add(operations.multipart_starts)
                    .saturating_add(operations.gets)
                    .saturating_add(operations.delete_streams)
                    .saturating_add(operations.lists)
                    .saturating_add(operations.delimiter_lists)
                    .saturating_add(operations.copies)
            },
            50,
        ),
        physical_put_bytes_p50: percentile(
            samples,
            |sample| sample.physical_object_store_operations.put_bytes,
            50,
        ),
    }
}

fn percentile(
    samples: &[DeletionBenchmarkSample],
    value: impl Fn(&DeletionBenchmarkSample) -> u64,
    percentile: usize,
) -> u64 {
    let mut values = samples.iter().map(value).collect::<Vec<_>>();
    values.sort_unstable();
    let rank = (values.len().saturating_mul(percentile).saturating_add(99)) / 100;
    values[rank.saturating_sub(1).min(values.len().saturating_sub(1))]
}

fn selected_sizes(profile: &str) -> Vec<usize> {
    if let Some(sizes) = env_list("HELIX_DELETION_BENCH_SIZES") {
        return sizes
            .into_iter()
            .map(|value| value.parse().expect("benchmark size is an integer"))
            .collect();
    }
    match profile {
        "smoke" => vec![1, 10],
        "standard" => vec![1, 10, 100, 500, 1_000],
        "scale" => vec![10_000],
        "vector-100k" => vec![100_000],
        other => panic!("unknown deletion benchmark profile `{other}`"),
    }
}

fn selected_workloads(profile: &str) -> Vec<DeletionBenchmarkWorkload> {
    let values = env_list("HELIX_DELETION_BENCH_WORKLOADS").unwrap_or_else(|| match profile {
        "smoke" => vec!["isolated_nodes".to_string()],
        "standard" => vec![
            "isolated_nodes".to_string(),
            "chain_nodes".to_string(),
            "parallel_edges_by_id".to_string(),
            "edge_pairs".to_string(),
        ],
        "scale" => vec![
            "isolated_nodes".to_string(),
            "high_degree_node".to_string(),
            "parallel_edges_by_id".to_string(),
        ],
        "vector-100k" => vec!["isolated_nodes".to_string()],
        other => panic!("unknown deletion benchmark profile `{other}`"),
    });
    values
        .into_iter()
        .map(|value| match value.as_str() {
            "isolated_nodes" => DeletionBenchmarkWorkload::IsolatedNodes,
            "chain_nodes" => DeletionBenchmarkWorkload::ChainNodes,
            "high_degree_node" => DeletionBenchmarkWorkload::HighDegreeNode,
            "parallel_edges_by_id" => DeletionBenchmarkWorkload::ParallelEdgesById,
            "edge_pairs" => DeletionBenchmarkWorkload::EdgePairs,
            other => panic!("unknown deletion benchmark workload `{other}`"),
        })
        .collect()
}

fn selected_cache_policies(profile: &str) -> Vec<DeletionBenchmarkCachePolicy> {
    let values = env_list("HELIX_DELETION_BENCH_CACHE_POLICIES").unwrap_or_else(|| {
        if profile == "scale" {
            vec!["warm".to_string(), "cold".to_string()]
        } else {
            vec!["warm".to_string()]
        }
    });
    values
        .into_iter()
        .map(|value| match value.as_str() {
            "warm" => DeletionBenchmarkCachePolicy::Warm,
            "cold" => DeletionBenchmarkCachePolicy::Cold,
            other => panic!("unknown deletion benchmark cache policy `{other}`"),
        })
        .collect()
}

fn env_list(name: &str) -> Option<Vec<String>> {
    let value = std::env::var(name).ok()?;
    Some(
        value
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .collect(),
    )
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .map(|value| value.parse().expect("benchmark option is an integer"))
        .unwrap_or(default)
}

fn git_commit() -> String {
    Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|commit| commit.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn record_allocation(bytes: usize) {
    if TRACK_ALLOCATIONS.load(Ordering::Relaxed) {
        ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
    }
}

struct RssSampler {
    stop: Arc<AtomicBool>,
    peak: Arc<AtomicU64>,
    task: tokio::task::JoinHandle<()>,
}

impl RssSampler {
    async fn start() -> Result<(Self, u64), String> {
        let baseline = current_rss_bytes()?;
        let stop = Arc::new(AtomicBool::new(false));
        let peak = Arc::new(AtomicU64::new(baseline));
        let task_stop = Arc::clone(&stop);
        let task_peak = Arc::clone(&peak);
        let task = tokio::spawn(async move {
            while !task_stop.load(Ordering::Acquire) {
                if let Ok(current) = current_rss_bytes() {
                    task_peak.fetch_max(current, Ordering::AcqRel);
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });
        Ok((Self { stop, peak, task }, baseline))
    }

    async fn stop(self) -> Result<u64, String> {
        self.stop.store(true, Ordering::Release);
        self.task
            .await
            .map_err(|error| format!("RSS sampler task failed: {error}"))?;
        Ok(self.peak.load(Ordering::Acquire).max(current_rss_bytes()?))
    }
}

fn process_cpu_ns() -> Result<u64, String> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: `getrusage` initializes the provided `rusage` on success.
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return Err(format!(
            "getrusage failed: {}",
            std::io::Error::last_os_error()
        ));
    }
    // SAFETY: the successful call initialized `usage`.
    let usage = unsafe { usage.assume_init() };
    Ok(timeval_ns(usage.ru_utime).saturating_add(timeval_ns(usage.ru_stime)))
}

fn timeval_ns(value: libc::timeval) -> u64 {
    let seconds = u64::try_from(value.tv_sec).unwrap_or(0);
    let micros = u64::try_from(value.tv_usec).unwrap_or(0);
    seconds
        .saturating_mul(1_000_000_000)
        .saturating_add(micros.saturating_mul(1_000))
}

#[cfg(target_os = "linux")]
fn current_rss_bytes() -> Result<u64, String> {
    let statm = std::fs::read_to_string("/proc/self/statm")
        .map_err(|error| format!("failed to read process RSS: {error}"))?;
    let resident_pages = statm
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| "process RSS has no resident pages".to_string())?
        .parse::<u64>()
        .map_err(|error| format!("invalid process RSS: {error}"))?;
    // SAFETY: `sysconf` is called with a supported constant and has no aliases.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return Err("sysconf(_SC_PAGESIZE) failed".to_string());
    }
    Ok(resident_pages.saturating_mul(page_size as u64))
}

#[cfg(target_os = "macos")]
#[allow(
    deprecated,
    reason = "libc exposes the process task port needed by task_info through this stable ABI"
)]
fn current_rss_bytes() -> Result<u64, String> {
    let mut info = std::mem::MaybeUninit::<libc::mach_task_basic_info>::zeroed();
    let mut count = libc::MACH_TASK_BASIC_INFO_COUNT;
    // SAFETY: the buffer and count match `MACH_TASK_BASIC_INFO`.
    let result = unsafe {
        libc::task_info(
            libc::mach_task_self(),
            libc::MACH_TASK_BASIC_INFO,
            info.as_mut_ptr().cast::<libc::integer_t>(),
            &mut count,
        )
    };
    if result != libc::KERN_SUCCESS {
        return Err(format!("mach task_info failed with {result}"));
    }
    // SAFETY: the successful call initialized `info`.
    Ok(unsafe { info.assume_init() }.resident_size)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn current_rss_bytes() -> Result<u64, String> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: `getrusage` initializes the provided `rusage` on success.
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return Err(format!(
            "getrusage failed: {}",
            std::io::Error::last_os_error()
        ));
    }
    // SAFETY: the successful call initialized `usage`.
    let rss = unsafe { usage.assume_init() }.ru_maxrss;
    Ok(u64::try_from(rss).unwrap_or(0).saturating_mul(1_024))
}
