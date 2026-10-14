//! Explicit measurement scopes for tests and production-coverage benchmarks.
//! Unscoped work is ignored. A spawned worker must enter a clone of its parent's
//! observer; concurrent measurements and nested scopes cannot reset each other.
use std::{
    future::Future,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SecondaryEqualityReadMetrics {
    pub(crate) point_reads: u64,
    pub(crate) multi_get_calls: u64,
    pub(crate) scans: u64,
    pub(crate) graph_reads: u64,
}
#[derive(Default)]
struct Counters {
    point: AtomicU64,
    multi_get: AtomicU64,
    scan: AtomicU64,
    graph: AtomicU64,
}
#[derive(Clone, Default)]
pub(crate) struct EqualityReadObserver(Arc<Counters>);
tokio::task_local! {
    static OBSERVER: EqualityReadObserver;
}
impl EqualityReadObserver {
    pub(crate) fn scope<F: Future>(self, future: F) -> impl Future<Output = F::Output> {
        // Measurement futures can contain large native execution states. Pin
        // each once so concurrent/nested scopes carry only a small owner.
        OBSERVER.scope(self, Box::pin(future))
    }
    /// Capture only an explicitly entered scope for a spawned benchmark worker.
    pub(crate) fn current() -> Self {
        OBSERVER
            .try_with(Clone::clone)
            .expect("equality measurement requires an observer scope")
    }
    pub(crate) fn metrics(&self) -> SecondaryEqualityReadMetrics {
        SecondaryEqualityReadMetrics {
            point_reads: self.0.point.load(Ordering::Relaxed),
            multi_get_calls: self.0.multi_get.load(Ordering::Relaxed),
            scans: self.0.scan.load(Ordering::Relaxed),
            graph_reads: self.0.graph.load(Ordering::Relaxed),
        }
    }
}
pub(crate) fn reset_equality_read_metrics() {
    let observer = EqualityReadObserver::current();
    for counter in [
        &observer.0.point,
        &observer.0.multi_get,
        &observer.0.scan,
        &observer.0.graph,
    ] {
        counter.store(0, Ordering::Relaxed);
    }
}
pub(crate) fn equality_read_metrics() -> SecondaryEqualityReadMetrics {
    EqualityReadObserver::current().metrics()
}
pub(super) enum ReadKind {
    Point,
    MultiGet,
    Scan,
    Graph,
}
pub(super) fn record(kind: ReadKind) {
    let _ = OBSERVER.try_with(|observer| {
        let counter = match kind {
            ReadKind::Point => &observer.0.point,
            ReadKind::MultiGet => &observer.0.multi_get,
            ReadKind::Scan => &observer.0.scan,
            ReadKind::Graph => &observer.0.graph,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn observers_isolate_concurrent_nested_and_unscoped_work() {
        let first = EqualityReadObserver::default();
        let second = EqualityReadObserver::default();
        let mut workers = tokio::task::JoinSet::new();
        for (observer, count) in [(first.clone(), 37), (second.clone(), 53)] {
            for _ in 0..2 {
                workers.spawn(observer.clone().scope(async move {
                    for _ in 0..count {
                        record(ReadKind::Point);
                        record(ReadKind::MultiGet);
                        record(ReadKind::Scan);
                        record(ReadKind::Graph);
                        tokio::task::yield_now().await;
                    }
                }));
            }
        }
        // These reads have no observer and cannot contaminate either task.
        for _ in 0..100 {
            record(ReadKind::Point);
        }
        while let Some(result) = workers.join_next().await {
            result.unwrap();
        }
        for (observer, expected) in [(first.clone(), 74), (second.clone(), 106)] {
            assert_eq!(
                observer.metrics(),
                SecondaryEqualityReadMetrics {
                    point_reads: expected,
                    multi_get_calls: expected,
                    scans: expected,
                    graph_reads: expected
                }
            );
        }
        first
            .clone()
            .scope(async {
                reset_equality_read_metrics();
                record(ReadKind::Point);
                second
                    .clone()
                    .scope(async {
                        reset_equality_read_metrics();
                        record(ReadKind::Graph);
                        tokio::task::yield_now().await;
                        assert_eq!(equality_read_metrics().graph_reads, 1);
                    })
                    .await;
                assert_eq!(
                    equality_read_metrics(),
                    SecondaryEqualityReadMetrics {
                        point_reads: 1,
                        ..Default::default()
                    }
                );
                let shared = EqualityReadObserver::current();
                tokio::spawn(shared.scope(async {
                    record(ReadKind::Point);
                }))
                .await
                .unwrap();
                assert_eq!(equality_read_metrics().point_reads, 2);
            })
            .await;
        assert_eq!(
            second.metrics(),
            SecondaryEqualityReadMetrics {
                graph_reads: 1,
                ..Default::default()
            }
        );
        assert!(OBSERVER.try_with(|_| ()).is_err());
        // Dropping a pending scoped future restores task-local state and releases
        // the observer reference, without losing already observed work.
        use futures::FutureExt;
        let dropped = EqualityReadObserver::default();
        let future = dropped.clone().scope(async {
            record(ReadKind::Scan);
            futures::future::pending::<()>().await
        });
        assert!(future.now_or_never().is_none());
        assert_eq!(Arc::strong_count(&dropped.0), 1);
        assert_eq!(dropped.metrics().scans, 1);
        assert!(OBSERVER.try_with(|_| ()).is_err());
    }
    #[test]
    #[should_panic(expected = "equality measurement requires an observer scope")]
    fn reset_cannot_mutate_an_unrelated_unscoped_measurement() {
        reset_equality_read_metrics();
    }
}
