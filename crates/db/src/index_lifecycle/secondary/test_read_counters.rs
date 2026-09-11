//! Unit-test measurements belong to one libtest thread and its current-thread
//! Tokio runtime, including tasks spawned on that runtime. Reject multi-thread
//! measurement runtimes instead of silently losing worker-thread reads.
//! Production-coverage binaries retain the process-wide atomic counters for
//! concurrent benchmarks; this module is compiled only for unit tests.

use std::thread;

use super::atomic;

thread_local! {
    pub(super) static BENCHMARK_POINT_READS: atomic::AtomicU64 = const { atomic::AtomicU64::new(0) };
    pub(super) static BENCHMARK_MULTI_GETS: atomic::AtomicU64 = const { atomic::AtomicU64::new(0) };
    pub(super) static BENCHMARK_SCANS: atomic::AtomicU64 = const { atomic::AtomicU64::new(0) };
    pub(super) static BENCHMARK_GRAPH_READS: atomic::AtomicU64 = const { atomic::AtomicU64::new(0) };
}

/// Keeps instrumentation call sites identical to the benchmark atomics while
/// isolating unit-test reads and resets on their owning test thread.
pub(super) trait ThreadLocalCounter {
    fn store(&'static self, value: u64, ordering: atomic::Ordering);
    fn load(&'static self, ordering: atomic::Ordering) -> u64;
    fn fetch_add(&'static self, value: u64, ordering: atomic::Ordering) -> u64;
}

impl ThreadLocalCounter for thread::LocalKey<atomic::AtomicU64> {
    fn store(&'static self, value: u64, ordering: atomic::Ordering) {
        assert!(
            tokio::runtime::Handle::try_current().map_or(true, |handle| matches!(
                handle.runtime_flavor(),
                tokio::runtime::RuntimeFlavor::CurrentThread
            )),
            "equality read metric unit tests require a current-thread runtime"
        );
        self.with(|counter| counter.store(value, ordering));
    }

    fn load(&'static self, ordering: atomic::Ordering) -> u64 {
        self.with(|counter| counter.load(ordering))
    }

    fn fetch_add(&'static self, value: u64, ordering: atomic::Ordering) -> u64 {
        self.with(|counter| counter.fetch_add(value, ordering))
    }
}

#[test]
fn equality_read_metrics_isolate_parallel_test_reads_and_resets() {
    let barrier = std::sync::Barrier::new(2);
    let measurements = std::thread::scope(|scope| {
        let workers = (1..=2)
            .map(|reads| {
                let barrier = &barrier;
                scope.spawn(move || {
                    super::reset_equality_read_metrics();
                    barrier.wait();
                    for _ in 0..reads {
                        super::record_equality_point_read();
                        super::record_equality_graph_read();
                        super::BENCHMARK_MULTI_GETS.fetch_add(1, super::AtomicOrdering::Relaxed);
                        super::BENCHMARK_SCANS.fetch_add(1, super::AtomicOrdering::Relaxed);
                    }
                    barrier.wait();
                    let measured = super::equality_read_metrics();
                    barrier.wait();
                    if reads == 1 {
                        super::reset_equality_read_metrics();
                    }
                    barrier.wait();
                    (reads, measured, super::equality_read_metrics())
                })
            })
            .collect::<Vec<_>>();
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>()
    });

    for (reads, measured, after_reset) in measurements {
        let expected = super::SecondaryEqualityReadMetrics {
            point_reads: reads,
            multi_get_calls: reads,
            scans: reads,
            graph_reads: reads,
        };
        assert_eq!(measured, expected);
        assert_eq!(
            after_reset,
            if reads == 1 {
                Default::default()
            } else {
                expected
            }
        );
    }
}

#[tokio::test]
async fn equality_read_metrics_include_spawned_current_thread_work() {
    super::reset_equality_read_metrics();
    tokio::spawn(async {
        super::record_equality_point_read();
        tokio::task::yield_now().await;
        super::record_equality_graph_read();
    })
    .await
    .unwrap();
    assert_eq!(
        super::equality_read_metrics(),
        super::SecondaryEqualityReadMetrics {
            point_reads: 1,
            graph_reads: 1,
            ..Default::default()
        }
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "equality read metric unit tests require a current-thread runtime")]
async fn equality_read_metrics_reject_multi_thread_unit_test_runtimes() {
    super::reset_equality_read_metrics();
}
