//! Upstream isolation regressions adapted to explicit observer ownership.
//! Worker runtimes may use multiple threads; measurement always needs a scope.
#[test]
fn equality_read_metrics_isolate_parallel_test_reads_and_resets() {
    let barrier = std::sync::Barrier::new(2);
    let measurements = std::thread::scope(|scope| {
        let workers = (1..=2)
            .map(|reads| {
                let barrier = &barrier;
                scope.spawn(move || {
                    let observer = super::EqualityReadObserver::default();
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap()
                        .block_on(observer.scope(async {
                            super::reset_equality_read_metrics();
                            barrier.wait();
                            for _ in 0..reads {
                                super::record_equality_point_read();
                                super::record_equality_graph_read();
                                super::record(super::ReadKind::MultiGet);
                                super::record(super::ReadKind::Scan);
                            }
                            barrier.wait();
                            let measured = super::equality_read_metrics();
                            barrier.wait();
                            if reads == 1 {
                                super::reset_equality_read_metrics();
                            }
                            barrier.wait();
                            (reads, measured, super::equality_read_metrics())
                        }))
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
    let observer = super::EqualityReadObserver::default();
    let work = observer.clone();
    observer
        .clone()
        .scope(async {
            super::reset_equality_read_metrics();
            tokio::spawn(work.scope(async {
                super::record_equality_point_read();
                tokio::task::yield_now().await;
                super::record_equality_graph_read();
            }))
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
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "equality measurement requires an observer scope")]
async fn equality_read_metrics_reject_unscoped_measurements() {
    super::reset_equality_read_metrics();
}
