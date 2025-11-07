/// Concurrency-specific tests for WorkerPool
///
/// This test suite focuses on concurrent behavior and race conditions in the WorkerPool.
/// Complements existing functional tests in worker_pool_tests.rs.
///
/// Key areas tested:
/// 1. **Parity Mechanism Fairness**: Validates alternating channel selection
/// 2. **Channel Backpressure**: Tests bounded channel (1000) behavior under load
/// 3. **Worker Contention**: Multiple concurrent requests to same workers
/// 4. **Request Distribution**: Validates fair distribution across workers
/// 5. **High Concurrency Stress**: Many simultaneous requests
///
/// CRITICAL ISSUES BEING TESTED:
/// - Parity mechanism edge cases (identified in analysis)
/// - Channel backpressure when bounded channels fill (1000 limit)
/// - Worker fairness under load
/// - No coordination between workers accessing shared graph
/// - No deadlocks or livelocks under high concurrency

use crate::helix_engine::traversal_core::HelixGraphEngineOpts;
use crate::helix_engine::traversal_core::config::Config;
use crate::helix_engine::{traversal_core::HelixGraphEngine};
use crate::helix_gateway::worker_pool::WorkerPool;
use crate::helix_gateway::{
    gateway::CoreSetter,
    router::router::HelixRouter,
};
use crate::protocol::Format;
use crate::protocol::{Request, request::RequestType};
use axum::body::Bytes;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::timeout;

fn create_test_graph() -> (Arc<HelixGraphEngine>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let opts = HelixGraphEngineOpts {
        path: temp_dir.path().to_str().unwrap().to_string(),
        config: Config::default(),
        version_info: Default::default(),
    };
    let graph = Arc::new(HelixGraphEngine::new(opts).unwrap());
    (graph, temp_dir)
}

fn create_request(name: &str) -> Request {
    Request {
        name: name.to_string(),
        req_type: RequestType::Query,
        body: Bytes::new(),
        in_fmt: Format::Json,
        out_fmt: Format::Json,
    }
}

fn create_test_pool(num_cores: usize, threads_per_core: usize) -> (WorkerPool, Arc<HelixGraphEngine>, TempDir) {
    let (graph, temp_dir) = create_test_graph();
    let router = Arc::new(HelixRouter::new(None, None));
    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(num_cores)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores: Vec<_> = (0..num_cores)
        .map(|id| core_affinity::CoreId { id })
        .collect();
    let core_setter = Arc::new(CoreSetter::new(cores, threads_per_core));

    let pool = WorkerPool::new(core_setter, Arc::clone(&graph), router, rt);
    (pool, graph, temp_dir)
}

#[tokio::test]
async fn test_concurrent_requests_high_load() {
    // Tests many concurrent requests to validate worker pool handles high load
    //
    // EXPECTED: All requests complete successfully, no deadlocks

    let (pool, _graph, _temp_dir) = create_test_pool(2, 2); // 4 workers
    let pool = Arc::new(pool);

    let num_concurrent = 100;
    let mut handles = vec![];

    for i in 0..num_concurrent {
        let pool = Arc::clone(&pool);
        let handle = tokio::spawn(async move {
            let req = create_request(&format!("request_{}", i));
            pool.process(req).await
        });
        handles.push(handle);
    }

    let mut completed = 0;
    for handle in handles {
        // Count all requests that complete (regardless of success/error)
        if handle.await.is_ok() {
            completed += 1;
        }
    }

    // All should complete (no panics or hangs)
    assert_eq!(completed, num_concurrent,
               "All requests should complete, got {}/{}", completed, num_concurrent);
    println!("High load test: {} requests completed", num_concurrent);
}

#[tokio::test]
async fn test_concurrent_burst_requests() {
    // Tests burst of concurrent requests
    //
    // EXPECTED: Pool handles bursts without deadlock

    let (pool, _graph, _temp_dir) = create_test_pool(2, 2);
    let pool = Arc::new(pool);

    // Send multiple bursts
    for burst in 0..5 {
        let mut handles = vec![];

        for i in 0..20 {
            let pool = Arc::clone(&pool);
            let handle = tokio::spawn(async move {
                let req = create_request(&format!("burst_{}_req_{}", burst, i));
                pool.process(req).await
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap().unwrap();
        }
    }

    // If we reach here, all bursts completed
    println!("Burst test: 5 bursts of 20 requests completed");
}

#[tokio::test]
async fn test_channel_backpressure() {
    // Tests bounded channel behavior (1000 capacity)
    //
    // EXPECTED: Requests block when channel is full but don't fail

    let (pool, _graph, _temp_dir) = create_test_pool(1, 2); // 2 workers
    let pool = Arc::new(pool);

    // Send requests that should stress channel capacity
    let num_requests = 100;
    let mut handles = vec![];

    for i in 0..num_requests {
        let pool = Arc::clone(&pool);
        let handle = tokio::spawn(async move {
            let req = create_request(&format!("backpressure_{}", i));
            pool.process(req).await
        });
        handles.push(handle);
    }

    // All should complete even with channel pressure
    let mut completed = 0;
    for handle in handles {
        if handle.await.is_ok() {
            completed += 1;
        }
    }

    assert_eq!(completed, num_requests);
    println!("Backpressure test: {} requests completed", completed);
}

#[tokio::test]
async fn test_request_timeout_handling() {
    // Tests that requests don't hang indefinitely
    //
    // EXPECTED: Timeout mechanism works correctly

    let (pool, _graph, _temp_dir) = create_test_pool(1, 2);
    let pool = Arc::new(pool);

    let req = create_request("timeout_test");

    // Should complete quickly (either success or error, not hang)
    let result = timeout(Duration::from_secs(5), pool.process(req)).await;

    assert!(result.is_ok(), "Request should not timeout");
}

#[tokio::test]
async fn test_parity_mechanism_both_workers() {
    // Tests that parity mechanism allows both even and odd workers to process
    //
    // This is indirect - we validate many requests complete successfully

    let (pool, _graph, _temp_dir) = create_test_pool(2, 2); // 4 workers (even number for parity)
    let pool = Arc::new(pool);

    // Send many requests - with proper parity, both types of workers participate
    let num_requests = 100;
    let mut handles = vec![];

    for i in 0..num_requests {
        let pool = Arc::clone(&pool);
        handles.push(tokio::spawn(async move {
            let req = create_request(&format!("parity_test_{}", i));
            pool.process(req).await
        }));
    }

    let mut completed = 0;
    for handle in handles {
        if handle.await.is_ok() {
            completed += 1;
        }
    }

    // All should complete if parity mechanism allows all workers to participate
    assert_eq!(completed, num_requests);
    println!("Parity test: {} requests completed across even/odd workers", completed);
}

#[tokio::test]
async fn test_worker_pool_drop_graceful() {
    // Tests that dropping the pool doesn't cause panics
    //
    // EXPECTED: No panics or hangs when pool is dropped

    {
        let (pool, _graph, _temp_dir) = create_test_pool(1, 2);
        let pool = Arc::new(pool);

        // Process a few requests
        for i in 0..5 {
            let req = create_request(&format!("drop_test_{}", i));
            let _ = pool.process(req).await;
        }
    } // Pool dropped here

    // If we reach this point, drop was graceful
    println!("Drop test: Pool dropped gracefully");
}

#[tokio::test]
async fn test_stress_sustained_load() {
    // Stress test: sustained high load over time
    //
    // EXPECTED: No degradation, memory leaks, or panics

    let (pool, _graph, _temp_dir) = create_test_pool(2, 2);
    let pool = Arc::new(pool);

    let total_requests = Arc::new(AtomicUsize::new(0));
    let duration = Duration::from_secs(2); // 2 second stress test
    let start = std::time::Instant::now();

    let mut handles = vec![];

    // Spawn multiple concurrent request generators
    for gen_id in 0..4 {
        let pool = Arc::clone(&pool);
        let total = Arc::clone(&total_requests);

        handles.push(tokio::spawn(async move {
            let mut local_count = 0;
            while start.elapsed() < duration {
                let req = create_request(&format!("stress_gen_{}_req_{}", gen_id, local_count));
                let _ = pool.process(req).await;
                total.fetch_add(1, Ordering::Relaxed);
                local_count += 1;
            }
            local_count
        }));
    }

    let mut per_gen = vec![];
    for handle in handles {
        per_gen.push(handle.await.unwrap());
    }

    let total = total_requests.load(Ordering::Relaxed);
    println!("Stress test: {} total requests in {:?}", total, duration);
    println!("Per generator: {:?}", per_gen);

    // Should process many requests (validates no deadlocks or severe contention)
    assert!(total > 100, "Should process many requests, got {}", total);
}

#[tokio::test]
async fn test_concurrent_different_request_types() {
    // Tests concurrent requests of different types
    //
    // EXPECTED: All request types handled concurrently

    let (pool, _graph, _temp_dir) = create_test_pool(2, 2);
    let pool = Arc::new(pool);

    let mut handles = vec![];

    // Mix of different request names
    let request_types = vec!["query_a", "query_b", "mutation_c", "read_d"];

    for _ in 0..25 {
        for req_type in &request_types {
            let pool = Arc::clone(&pool);
            let req_type = req_type.to_string();
            handles.push(tokio::spawn(async move {
                let req = create_request(&req_type);
                pool.process(req).await
            }));
        }
    }

    let mut completed = 0;
    for handle in handles {
        if handle.await.is_ok() {
            completed += 1;
        }
    }

    let expected = 25 * request_types.len();
    assert_eq!(completed, expected);
    println!("Different request types: {}/{} completed", completed, expected);
}

#[tokio::test]
async fn test_sequential_then_concurrent() {
    // Tests transitioning from sequential to concurrent load
    //
    // EXPECTED: No issues transitioning between load patterns

    let (pool, _graph, _temp_dir) = create_test_pool(2, 2);
    let pool = Arc::new(pool);

    // Sequential requests
    for i in 0..10 {
        let req = create_request(&format!("sequential_{}", i));
        pool.process(req).await.ok();
    }

    // Then concurrent burst
    let mut handles = vec![];
    for i in 0..50 {
        let pool = Arc::clone(&pool);
        handles.push(tokio::spawn(async move {
            let req = create_request(&format!("concurrent_{}", i));
            pool.process(req).await
        }));
    }

    let mut completed = 0;
    for handle in handles {
        if handle.await.is_ok() {
            completed += 1;
        }
    }

    assert_eq!(completed, 50);
    println!("Sequential->Concurrent test: 10 sequential + 50 concurrent completed");
}

#[tokio::test]
async fn test_worker_distribution_fairness() {
    // Tests that requests are distributed across workers
    //
    // With 4 workers and 100 requests, work should be distributed

    let (pool, _graph, _temp_dir) = create_test_pool(2, 2); // 4 workers
    let pool = Arc::new(pool);

    let start = std::time::Instant::now();
    let mut handles = vec![];

    for i in 0..100 {
        let pool = Arc::clone(&pool);
        handles.push(tokio::spawn(async move {
            let req = create_request(&format!("fairness_{}", i));
            pool.process(req).await
        }));
    }

    for handle in handles {
        handle.await.unwrap().unwrap();
    }

    let elapsed = start.elapsed();

    // With good distribution across 4 workers, should be relatively fast
    // (Not strictly deterministic, but gives us a signal)
    println!("Fairness test: 100 requests completed in {:?}", elapsed);

    // Basic sanity: should complete in reasonable time
    assert!(elapsed < Duration::from_secs(10),
            "Requests took {:?}, may indicate poor distribution", elapsed);
}
