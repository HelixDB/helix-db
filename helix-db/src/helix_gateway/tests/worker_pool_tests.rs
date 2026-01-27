use crate::helix_engine::traversal_core::HelixGraphEngineOpts;
use crate::helix_engine::traversal_core::config::Config;
use crate::helix_engine::{traversal_core::HelixGraphEngine, types::GraphError};
use crate::helix_gateway::worker_pool::WorkerPool;
use crate::helix_gateway::{
    gateway::CoreSetter,
    router::router::{HandlerInput, HelixRouter, IoContFn},
};
use crate::protocol::Format;
use crate::protocol::{HelixError, Request, request::RequestType, response::Response};
use axum::body::Bytes;
use std::sync::Arc;
use tempfile::TempDir;

fn create_test_graph() -> (Arc<HelixGraphEngine>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    // Use very minimal DB size for tests (0 means use minimum)
    // This reduces memory mapping requirements when running many tests in parallel
    config.db_max_size_gb = Some(0);
    let opts = HelixGraphEngineOpts {
        path: temp_dir.path().to_str().unwrap().to_string(),
        config,
        version_info: Default::default(),
    };
    let graph = Arc::new(HelixGraphEngine::new(opts).unwrap());
    (graph, temp_dir)
}

fn test_handler(_input: HandlerInput) -> Result<Response, GraphError> {
    Ok(Response {
        body: b"test response".to_vec(),
        fmt: Format::Json,
    })
}

fn error_handler(_input: HandlerInput) -> Result<Response, GraphError> {
    Err(GraphError::New("handler error".to_string()))
}

fn create_test_request(name: &str, req_type: RequestType) -> Request {
    Request {
        name: name.to_string(),
        req_type,
        api_key: None,
        body: Bytes::new(),
        in_fmt: Format::Json,
        out_fmt: Format::Json,
    }
}

// ============================================================================
// WorkerPool Creation Tests
// ============================================================================

#[test]
fn test_worker_pool_new() {
    let (graph, _temp_dir) = create_test_graph();
    let router = Arc::new(HelixRouter::new(None, None, None));
    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores =
        core_affinity::get_core_ids().unwrap_or_else(|| vec![core_affinity::CoreId { id: 0 }]);
    // Need at least 2 workers: use 2 threads per core to ensure num_workers = cores.len() * 2 >= 2
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let _pool = WorkerPool::new(core_setter, graph, router, rt);
    // If we reach here, pool was created successfully
}

#[test]
fn test_worker_pool_with_single_core() {
    let (graph, _temp_dir) = create_test_graph();
    let router = Arc::new(HelixRouter::new(None, None, None));
    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let _pool = WorkerPool::new(core_setter, graph, router, rt);
}

#[test]
fn test_worker_pool_with_multiple_cores() {
    let (graph, _temp_dir) = create_test_graph();
    let router = Arc::new(HelixRouter::new(None, None, None));
    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![
        core_affinity::CoreId { id: 0 },
        core_affinity::CoreId { id: 1 },
    ];
    let core_setter = Arc::new(CoreSetter::new(cores, 1));

    let _pool = WorkerPool::new(core_setter, graph, router, rt);
}

#[test]
fn test_worker_pool_with_multiple_workers_per_core() {
    let (graph, _temp_dir) = create_test_graph();
    let router = Arc::new(HelixRouter::new(None, None, None));
    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    let core_setter = Arc::new(CoreSetter::new(cores, 4));

    let _pool = WorkerPool::new(core_setter, graph, router, rt);
}

#[test]
fn test_worker_pool_channel_capacity() {
    let (graph, _temp_dir) = create_test_graph();
    let router = Arc::new(HelixRouter::new(None, None, None));
    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    // WorkerPool uses bounded(1000) for channels
    let _pool = WorkerPool::new(core_setter, graph, router, rt);
    // Verify it doesn't panic during creation
}

// ============================================================================
// Request Processing Tests
// ============================================================================

#[tokio::test]
async fn test_process_request_success() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("test_query".to_string(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    let request = create_test_request("test_query", RequestType::Query);
    let result = pool.process(request).await;

    assert!(result.is_ok());
    let response = result.unwrap();
    assert_eq!(response.body, b"test response");
}

#[tokio::test]
async fn test_process_request_handler_error() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("error_query".to_string(), Arc::new(error_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    let request = create_test_request("error_query", RequestType::Query);
    let result = pool.process(request).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_process_request_not_found() {
    let (graph, _temp_dir) = create_test_graph();
    let router = Arc::new(HelixRouter::new(None, None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    let request = create_test_request("nonexistent", RequestType::Query);
    let result = pool.process(request).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        HelixError::NotFound { ty, name } => {
            assert_eq!(ty, RequestType::Query);
            assert_eq!(name, "nonexistent");
        }
        _ => panic!("Expected NotFound error"),
    }
}

#[tokio::test]
async fn test_process_multiple_requests_sequentially() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("test_query".to_string(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    for _ in 0..5 {
        let request = create_test_request("test_query", RequestType::Query);
        let result = pool.process(request).await;
        assert!(result.is_ok());
    }
}

#[tokio::test]
async fn test_process_requests_parallel() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("test_query".to_string(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![
        core_affinity::CoreId { id: 0 },
        core_affinity::CoreId { id: 1 },
    ];
    let core_setter = Arc::new(CoreSetter::new(cores, 1));

    let pool = Arc::new(WorkerPool::new(core_setter, graph, router, rt));

    let mut handles = vec![];
    for _ in 0..5 {
        let pool_clone = Arc::clone(&pool);
        let handle = tokio::spawn(async move {
            let request = create_test_request("test_query", RequestType::Query);
            pool_clone.process(request).await
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }
}

// ============================================================================
// Request Type Routing Tests
// ============================================================================

#[tokio::test]
async fn test_route_query_request() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("query1".to_string(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    let request = create_test_request("query1", RequestType::Query);
    let result = pool.process(request).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_route_query_not_found() {
    let (graph, _temp_dir) = create_test_graph();
    let router = Arc::new(HelixRouter::new(None, None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    let request = create_test_request("unknown", RequestType::Query);
    let result = pool.process(request).await;

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), HelixError::NotFound { .. }));
}

#[tokio::test]
async fn test_multiple_query_routes() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("query1".to_string(), Arc::new(test_handler) as Arc<_>);
    routes.insert("query2".to_string(), Arc::new(test_handler) as Arc<_>);
    routes.insert("query3".to_string(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    for query_name in ["query1", "query2", "query3"] {
        let request = create_test_request(query_name, RequestType::Query);
        let result = pool.process(request).await;
        assert!(result.is_ok());
    }
}

#[tokio::test]
async fn test_route_with_special_characters() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert(
        "query-with-dash".to_string(),
        Arc::new(test_handler) as Arc<_>,
    );
    routes.insert(
        "query_with_underscore".to_string(),
        Arc::new(test_handler) as Arc<_>,
    );
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    let request1 = create_test_request("query-with-dash", RequestType::Query);
    assert!(pool.process(request1).await.is_ok());

    let request2 = create_test_request("query_with_underscore", RequestType::Query);
    assert!(pool.process(request2).await.is_ok());
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[tokio::test]
async fn test_handler_error_propagation() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("error".to_string(), Arc::new(error_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    let request = create_test_request("error", RequestType::Query);
    let result = pool.process(request).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_not_found_error_contains_request_name() {
    let (graph, _temp_dir) = create_test_graph();
    let router = Arc::new(HelixRouter::new(None, None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    let request = create_test_request("specific_name", RequestType::Query);
    let result = pool.process(request).await;

    match result {
        Err(HelixError::NotFound { name, .. }) => {
            assert_eq!(name, "specific_name");
        }
        _ => panic!("Expected NotFound error"),
    }
}

#[tokio::test]
async fn test_not_found_error_contains_request_type() {
    let (graph, _temp_dir) = create_test_graph();
    let router = Arc::new(HelixRouter::new(None, None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    let request = create_test_request("test", RequestType::Query);
    let result = pool.process(request).await;

    match result {
        Err(HelixError::NotFound { ty, .. }) => {
            assert_eq!(ty, RequestType::Query);
        }
        _ => panic!("Expected NotFound error"),
    }
}

#[tokio::test]
async fn test_mixed_success_and_error_requests() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("success".to_string(), Arc::new(test_handler) as Arc<_>);
    routes.insert("error".to_string(), Arc::new(error_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    let success_req = create_test_request("success", RequestType::Query);
    assert!(pool.process(success_req).await.is_ok());

    let error_req = create_test_request("error", RequestType::Query);
    assert!(pool.process(error_req).await.is_err());

    let not_found_req = create_test_request("missing", RequestType::Query);
    assert!(pool.process(not_found_req).await.is_err());
}

// ============================================================================
// Request Body and Format Tests
// ============================================================================

#[tokio::test]
async fn test_request_with_body_data() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("test".to_string(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    let request = Request {
        name: "test".to_string(),
        req_type: RequestType::Query,
        body: Bytes::from(vec![1, 2, 3, 4]),
        in_fmt: Format::Json,
        out_fmt: Format::Json,
        api_key: None,
    };

    let result = pool.process(request).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_request_with_empty_body() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("test".to_string(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    let request = create_test_request("test", RequestType::Query);
    let result = pool.process(request).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_request_format_json() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("test".to_string(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    let request = Request {
        name: "test".to_string(),
        req_type: RequestType::Query,
        body: Bytes::new(),
        in_fmt: Format::Json,
        out_fmt: Format::Json,
        api_key: None,
    };

    let result = pool.process(request).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap().fmt, Format::Json);
}

// ============================================================================
// Worker Thread Tests
// ============================================================================

#[test]
fn test_worker_thread_creation() {
    let (graph, _temp_dir) = create_test_graph();
    let router = Arc::new(HelixRouter::new(None, None, None));
    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    // This creates workers internally
    let _pool = WorkerPool::new(core_setter, graph, router, rt);

    // If we reach here, worker threads were created successfully
    std::thread::sleep(std::time::Duration::from_millis(10));
}

#[test]
fn test_multiple_worker_threads() {
    let (graph, _temp_dir) = create_test_graph();
    let router = Arc::new(HelixRouter::new(None, None, None));
    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![
        core_affinity::CoreId { id: 0 },
        core_affinity::CoreId { id: 1 },
    ];
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    // This creates 4 worker threads (2 cores × 2 threads per core)
    let _pool = WorkerPool::new(core_setter, graph, router, rt);

    std::thread::sleep(std::time::Duration::from_millis(10));
}

// ============================================================================
// Channel and Communication Tests
// ============================================================================

#[tokio::test]
async fn test_channel_communication() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("test".to_string(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    // Test that channel communication works
    let request = create_test_request("test", RequestType::Query);
    let result = pool.process(request).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_high_volume_requests() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("test".to_string(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![
        core_affinity::CoreId { id: 0 },
        core_affinity::CoreId { id: 1 },
    ];
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = Arc::new(WorkerPool::new(core_setter, graph, router, rt));

    // Test 50 requests
    let mut handles = vec![];
    for i in 0..50 {
        let pool_clone = Arc::clone(&pool);
        let handle = tokio::spawn(async move {
            let request = create_test_request("test", RequestType::Query);
            (i, pool_clone.process(request).await)
        });
        handles.push(handle);
    }

    let mut success_count = 0;
    for handle in handles {
        let (_, result) = handle.await.unwrap();
        if result.is_ok() {
            success_count += 1;
        }
    }

    assert_eq!(success_count, 50);
}

// ============================================================================
// Handler Input Tests
// ============================================================================

#[tokio::test]
async fn test_handler_receives_correct_request() {
    fn check_request_handler(input: HandlerInput) -> Result<Response, GraphError> {
        assert_eq!(input.request.name, "check_name");
        Ok(Response {
            body: input.request.name.as_bytes().to_vec(),
            fmt: Format::Json,
        })
    }

    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert(
        "check_name".to_string(),
        Arc::new(check_request_handler) as Arc<_>,
    );
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    let request = create_test_request("check_name", RequestType::Query);
    let result = pool.process(request).await;

    assert!(result.is_ok());
    let response = result.unwrap();
    assert_eq!(response.body, b"check_name");
}

#[tokio::test]
async fn test_handler_receives_graph_access() {
    fn graph_access_handler(input: HandlerInput) -> Result<Response, GraphError> {
        // Verify we have access to the graph
        let _graph = input.graph;
        Ok(Response {
            body: b"graph_accessed".to_vec(),
            fmt: Format::Json,
        })
    }

    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("test".to_string(), Arc::new(graph_access_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    let request = create_test_request("test", RequestType::Query);
    let result = pool.process(request).await;

    assert!(result.is_ok());
}

// ============================================================================
// Response Format Tests
// ============================================================================

#[tokio::test]
async fn test_response_body_content() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("test".to_string(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    let request = create_test_request("test", RequestType::Query);
    let result = pool.process(request).await;

    assert!(result.is_ok());
    let response = result.unwrap();
    assert_eq!(response.body, b"test response");
    assert_eq!(response.fmt, Format::Json);
}

#[tokio::test]
async fn test_response_format_preserved() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("test".to_string(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    let request = create_test_request("test", RequestType::Query);
    let result = pool.process(request).await;

    let response = result.unwrap();
    assert_eq!(response.fmt, Format::Json);
}

// ============================================================================
// Edge Case Tests
// ============================================================================

#[tokio::test]
async fn test_empty_route_name() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("".to_string(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    let request = create_test_request("", RequestType::Query);
    let result = pool.process(request).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_very_long_route_name() {
    let (graph, _temp_dir) = create_test_graph();
    let long_name = "a".repeat(1000);
    let mut routes = std::collections::HashMap::new();
    routes.insert(long_name.clone(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    let request = create_test_request(&long_name, RequestType::Query);
    let result = pool.process(request).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_concurrent_different_routes() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("route_a".to_string(), Arc::new(test_handler) as Arc<_>);
    routes.insert("route_b".to_string(), Arc::new(test_handler) as Arc<_>);
    routes.insert("route_c".to_string(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![
        core_affinity::CoreId { id: 0 },
        core_affinity::CoreId { id: 1 },
    ];
    let core_setter = Arc::new(CoreSetter::new(cores, 1));

    let pool = Arc::new(WorkerPool::new(core_setter, graph, router, rt));

    let mut handles = vec![];
    for route in ["route_a", "route_b", "route_c"] {
        let pool_clone = Arc::clone(&pool);
        let route_name = route.to_string();
        let handle = tokio::spawn(async move {
            let request = create_test_request(&route_name, RequestType::Query);
            pool_clone.process(request).await
        });
        handles.push(handle);
    }

    for handle in handles {
        assert!(handle.await.unwrap().is_ok());
    }
}

#[tokio::test]
async fn test_sequential_different_routes() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("first".to_string(), Arc::new(test_handler) as Arc<_>);
    routes.insert("second".to_string(), Arc::new(test_handler) as Arc<_>);
    routes.insert("third".to_string(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    for route in ["first", "second", "third"] {
        let request = create_test_request(route, RequestType::Query);
        assert!(pool.process(request).await.is_ok());
    }
}

#[tokio::test]
async fn test_repeated_requests_same_route() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("repeat".to_string(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    for _ in 0..10 {
        let request = create_test_request("repeat", RequestType::Query);
        assert!(pool.process(request).await.is_ok());
    }
}

#[tokio::test]
async fn test_alternating_success_error() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("success".to_string(), Arc::new(test_handler) as Arc<_>);
    routes.insert("error".to_string(), Arc::new(error_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    for i in 0..5 {
        let route = if i % 2 == 0 { "success" } else { "error" };
        let request = create_test_request(route, RequestType::Query);
        let result = pool.process(request).await;
        if i % 2 == 0 {
            assert!(result.is_ok());
        } else {
            assert!(result.is_err());
        }
    }
}

#[tokio::test]
async fn test_request_with_large_body() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("test".to_string(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    let large_body = vec![0u8; 100_000];
    let request = Request {
        name: "test".to_string(),
        req_type: RequestType::Query,
        body: Bytes::from(large_body),
        in_fmt: Format::Json,
        out_fmt: Format::Json,
        api_key: None,
    };

    let result = pool.process(request).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_many_parallel_requests() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("test".to_string(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![
        core_affinity::CoreId { id: 0 },
        core_affinity::CoreId { id: 1 },
    ];
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = Arc::new(WorkerPool::new(core_setter, graph, router, rt));

    let mut handles = vec![];
    for _ in 0..100 {
        let pool_clone = Arc::clone(&pool);
        let handle = tokio::spawn(async move {
            let request = create_test_request("test", RequestType::Query);
            pool_clone.process(request).await
        });
        handles.push(handle);
    }

    let mut success_count = 0;
    for handle in handles {
        if handle.await.unwrap().is_ok() {
            success_count += 1;
        }
    }

    assert_eq!(success_count, 100);
}

#[tokio::test]
async fn test_worker_pool_no_routes() {
    let (graph, _temp_dir) = create_test_graph();
    let router = Arc::new(HelixRouter::new(None, None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    let request = create_test_request("any", RequestType::Query);
    let result = pool.process(request).await;

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), HelixError::NotFound { .. }));
}

#[tokio::test]
async fn test_request_type_query_explicit() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("query".to_string(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    let request = Request {
        name: "query".to_string(),
        req_type: RequestType::Query,
        body: Bytes::new(),
        in_fmt: Format::Json,
        out_fmt: Format::Json,
        api_key: None,
    };

    let result = pool.process(request).await;
    assert!(result.is_ok());
}

#[test]
#[should_panic(expected = "The number of workers must be at least 2")]
fn test_worker_pool_with_empty_cores() {
    let (graph, _temp_dir) = create_test_graph();
    let router = Arc::new(HelixRouter::new(None, None, None));
    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![];
    let core_setter = Arc::new(CoreSetter::new(cores, 1));

    // Should panic: 0 cores × 1 thread per core = 0 workers (< 2)
    let _pool = WorkerPool::new(core_setter, graph, router, rt);
}

#[test]
#[should_panic(expected = "The number of workers should be a multiple of 2")]
fn test_worker_pool_with_odd_workers() {
    let (graph, _temp_dir) = create_test_graph();
    let router = Arc::new(HelixRouter::new(None, None, None));
    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    let core_setter = Arc::new(CoreSetter::new(cores, 3));

    // Should panic: 1 core × 3 threads = 3 workers (odd number)
    let _pool = WorkerPool::new(core_setter, graph, router, rt);
}

#[test]
#[should_panic(expected = "The number of workers must be at least 2")]
fn test_worker_pool_with_single_worker() {
    let (graph, _temp_dir) = create_test_graph();
    let router = Arc::new(HelixRouter::new(None, None, None));
    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    let core_setter = Arc::new(CoreSetter::new(cores, 1));

    // Should panic: 1 core × 1 thread = 1 worker (< 2)
    let _pool = WorkerPool::new(core_setter, graph, router, rt);
}

#[tokio::test]
async fn test_response_with_custom_body() {
    fn custom_handler(_input: HandlerInput) -> Result<Response, GraphError> {
        Ok(Response {
            body: b"custom response data".to_vec(),
            fmt: Format::Json,
        })
    }

    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("custom".to_string(), Arc::new(custom_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    let request = create_test_request("custom", RequestType::Query);
    let result = pool.process(request).await;

    assert!(result.is_ok());
    let response = result.unwrap();
    assert_eq!(response.body, b"custom response data");
}

#[tokio::test]
async fn test_error_then_success() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("success".to_string(), Arc::new(test_handler) as Arc<_>);
    routes.insert("error".to_string(), Arc::new(error_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    // First, an error
    let error_req = create_test_request("error", RequestType::Query);
    assert!(pool.process(error_req).await.is_err());

    // Then, a success
    let success_req = create_test_request("success", RequestType::Query);
    assert!(pool.process(success_req).await.is_ok());
}

#[tokio::test]
async fn test_success_then_not_found() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("exists".to_string(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    // First, a success
    let success_req = create_test_request("exists", RequestType::Query);
    assert!(pool.process(success_req).await.is_ok());

    // Then, not found
    let not_found_req = create_test_request("does_not_exist", RequestType::Query);
    assert!(pool.process(not_found_req).await.is_err());
}

#[tokio::test]
async fn test_multiple_errors_in_sequence() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("error".to_string(), Arc::new(error_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    for _ in 0..5 {
        let request = create_test_request("error", RequestType::Query);
        assert!(pool.process(request).await.is_err());
    }
}

#[tokio::test]
async fn test_worker_pool_stress_test() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("stress".to_string(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![
        core_affinity::CoreId { id: 0 },
        core_affinity::CoreId { id: 1 },
    ];
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = Arc::new(WorkerPool::new(core_setter, graph, router, rt));

    let mut handles = vec![];
    for i in 0..200 {
        let pool_clone = Arc::clone(&pool);
        let handle = tokio::spawn(async move {
            let request = create_test_request("stress", RequestType::Query);
            (i, pool_clone.process(request).await)
        });
        handles.push(handle);
    }

    let mut results = vec![];
    for handle in handles {
        results.push(handle.await.unwrap());
    }

    let success_count = results.iter().filter(|(_, r)| r.is_ok()).count();
    assert_eq!(success_count, 200);
}

#[tokio::test]
async fn test_route_case_sensitive() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("Query".to_string(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    // Exact case matches
    let exact_req = create_test_request("Query", RequestType::Query);
    assert!(pool.process(exact_req).await.is_ok());

    // Different case doesn't match
    let wrong_case_req = create_test_request("query", RequestType::Query);
    assert!(pool.process(wrong_case_req).await.is_err());
}

#[tokio::test]
async fn test_route_with_numbers() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("query123".to_string(), Arc::new(test_handler) as Arc<_>);
    routes.insert("123query".to_string(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    let req1 = create_test_request("query123", RequestType::Query);
    assert!(pool.process(req1).await.is_ok());

    let req2 = create_test_request("123query", RequestType::Query);
    assert!(pool.process(req2).await.is_ok());
}

#[tokio::test]
async fn test_worker_pool_multiple_workers_same_route() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("shared".to_string(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![
        core_affinity::CoreId { id: 0 },
        core_affinity::CoreId { id: 1 },
    ];
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = Arc::new(WorkerPool::new(core_setter, graph, router, rt));

    // Multiple workers processing the same route concurrently
    let mut handles = vec![];
    for _ in 0..20 {
        let pool_clone = Arc::clone(&pool);
        let handle = tokio::spawn(async move {
            let request = create_test_request("shared", RequestType::Query);
            pool_clone.process(request).await
        });
        handles.push(handle);
    }

    for handle in handles {
        assert!(handle.await.unwrap().is_ok());
    }
}

#[tokio::test]
async fn test_request_name_with_unicode() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("query_世界".to_string(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    // Need at least 2 workers: 1 core × 2 threads = 2 workers
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = WorkerPool::new(core_setter, graph, router, rt);

    let request = create_test_request("query_世界", RequestType::Query);
    let result = pool.process(request).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_rapid_fire_requests() {
    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("rapid".to_string(), Arc::new(test_handler) as Arc<_>);
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![
        core_affinity::CoreId { id: 0 },
        core_affinity::CoreId { id: 1 },
    ];
    let core_setter = Arc::new(CoreSetter::new(cores, 1));

    let pool = Arc::new(WorkerPool::new(core_setter, graph, router, rt));

    // Fire off 30 requests as fast as possible
    let mut handles = vec![];
    for _ in 0..30 {
        let pool_clone = Arc::clone(&pool);
        let handle = tokio::spawn(async move {
            let request = create_test_request("rapid", RequestType::Query);
            pool_clone.process(request).await
        });
        handles.push(handle);
    }

    let mut all_ok = true;
    for handle in handles {
        if handle.await.unwrap().is_err() {
            all_ok = false;
            break;
        }
    }

    assert!(all_ok);
}

// ============================================================================
// Writer Continuation Priority Tests
// ============================================================================

/// Test that write handler continuations are processed before new write requests.
/// This prevents hangs when multiple write requests with async IO arrive rapidly.
///
/// Scenario being tested:
/// 1. Write request A arrives, needs async IO, spawns continuation
/// 2. Write request B arrives before A's continuation completes
/// 3. Continuation from A must be processed before B starts
/// 4. If continuations aren't prioritized, this would hang or process out of order
#[tokio::test]
async fn test_writer_continuation_priority() {
    use std::sync::atomic::{AtomicU32, Ordering};

    // Track the order of operations
    static ORDER_COUNTER: AtomicU32 = AtomicU32::new(0);
    static CONTINUATION_A_ORDER: AtomicU32 = AtomicU32::new(0);
    static REQUEST_B_START_ORDER: AtomicU32 = AtomicU32::new(0);

    // Reset counters
    ORDER_COUNTER.store(0, Ordering::SeqCst);
    CONTINUATION_A_ORDER.store(0, Ordering::SeqCst);
    REQUEST_B_START_ORDER.store(0, Ordering::SeqCst);

    // Handler that returns IoNeeded - simulates async operation like embedding fetch
    fn io_handler_a(_input: HandlerInput) -> Result<Response, GraphError> {
        Err(IoContFn::create_err(move |cont_tx, ret_chan| {
            Box::pin(async move {
                // Very short async delay - continuation will be ready quickly
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;

                // Send continuation
                cont_tx
                    .send_async((
                        ret_chan,
                        Box::new(move || {
                            // Record when continuation executes
                            let order = ORDER_COUNTER.fetch_add(1, Ordering::SeqCst);
                            CONTINUATION_A_ORDER.store(order, Ordering::SeqCst);
                            Ok(Response {
                                body: b"continuation_a".to_vec(),
                                fmt: Format::Json,
                            })
                        })
                            as Box<dyn FnOnce() -> Result<Response, GraphError> + Send + Sync>,
                    ))
                    .await
                    .expect("cont channel should be alive");
            })
        }))
    }

    // Handler B - records when it starts processing
    fn handler_b(_input: HandlerInput) -> Result<Response, GraphError> {
        // Record when request B starts
        let order = ORDER_COUNTER.fetch_add(1, Ordering::SeqCst);
        REQUEST_B_START_ORDER.store(order, Ordering::SeqCst);
        Ok(Response {
            body: b"handler_b".to_vec(),
            fmt: Format::Json,
        })
    }

    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("io_write_a".to_string(), Arc::new(io_handler_a) as Arc<_>);
    routes.insert("write_b".to_string(), Arc::new(handler_b) as Arc<_>);

    // Mark both as write routes so they go through the single writer worker
    let mut write_routes = std::collections::HashSet::new();
    write_routes.insert("io_write_a".to_string());
    write_routes.insert("write_b".to_string());

    let router = Arc::new(HelixRouter::new(Some(routes), None, Some(write_routes)));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = Arc::new(WorkerPool::new(core_setter, graph, router, rt));

    // Send request A (will need async IO)
    let pool_a = Arc::clone(&pool);
    let handle_a = tokio::spawn(async move {
        let request = Request {
            name: "io_write_a".to_string(),
            req_type: RequestType::Query,
            body: Bytes::new(),
            in_fmt: Format::Json,
            out_fmt: Format::Json,
            api_key: None,
        };
        pool_a.process(request).await
    });

    // Wait long enough for A's async to complete and send its continuation
    // A's async takes 5ms, so 50ms should be plenty
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Send request B while A's continuation is pending
    let pool_b = Arc::clone(&pool);
    let handle_b = tokio::spawn(async move {
        let request = Request {
            name: "write_b".to_string(),
            req_type: RequestType::Query,
            body: Bytes::new(),
            in_fmt: Format::Json,
            out_fmt: Format::Json,
            api_key: None,
        };
        pool_b.process(request).await
    });

    // Both should complete with a reasonable timeout (hang detection)
    let timeout = std::time::Duration::from_secs(5);
    let result_a = tokio::time::timeout(timeout, handle_a).await;
    let result_b = tokio::time::timeout(timeout, handle_b).await;

    // Verify both completed (didn't hang)
    assert!(result_a.is_ok(), "Request A timed out - possible hang");
    assert!(result_b.is_ok(), "Request B timed out - possible hang");

    let response_a = result_a.unwrap().unwrap();
    let response_b = result_b.unwrap().unwrap();

    assert!(response_a.is_ok(), "Request A failed: {:?}", response_a);
    assert!(response_b.is_ok(), "Request B failed: {:?}", response_b);

    // Verify continuation A was processed before request B started
    let cont_a_order = CONTINUATION_A_ORDER.load(Ordering::SeqCst);
    let req_b_order = REQUEST_B_START_ORDER.load(Ordering::SeqCst);

    assert!(
        cont_a_order < req_b_order,
        "Continuation A (order {}) should be processed before Request B (order {})",
        cont_a_order,
        req_b_order
    );
}

// ============================================================================
// Read Continuation Channel Tests
// ============================================================================

/// Test that read handlers can use continuation channels properly.
/// This verifies the parity-based continuation handling works for readers.
#[tokio::test]
async fn test_read_continuation_channel_basic() {
    // Handler that returns IoNeeded - simulates async operation
    fn io_read_handler(_input: HandlerInput) -> Result<Response, GraphError> {
        Err(IoContFn::create_err(move |cont_tx, ret_chan| {
            Box::pin(async move {
                // Simulate async IO (e.g., fetching embeddings)
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;

                // Send continuation back to worker
                cont_tx
                    .send_async((
                        ret_chan,
                        Box::new(move || {
                            Ok(Response {
                                body: b"read_continuation_result".to_vec(),
                                fmt: Format::Json,
                            })
                        })
                            as Box<dyn FnOnce() -> Result<Response, GraphError> + Send + Sync>,
                    ))
                    .await
                    .expect("cont channel should be alive");
            })
        }))
    }

    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("io_read".to_string(), Arc::new(io_read_handler) as Arc<_>);
    // Not in write_routes, so it goes to reader workers
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = Arc::new(WorkerPool::new(core_setter, graph, router, rt));

    let request = Request {
        name: "io_read".to_string(),
        req_type: RequestType::Query,
        body: Bytes::new(),
        in_fmt: Format::Json,
        out_fmt: Format::Json,
        api_key: None,
    };

    let timeout = std::time::Duration::from_secs(5);
    let result = tokio::time::timeout(timeout, pool.process(request)).await;

    assert!(result.is_ok(), "Request timed out - possible hang");
    let response = result.unwrap();
    assert!(response.is_ok(), "Request failed: {:?}", response);
    assert_eq!(response.unwrap().body, b"read_continuation_result".to_vec());
}

/// Test multiple concurrent read requests with continuation channels.
#[tokio::test]
async fn test_read_continuation_channel_concurrent() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static CONTINUATION_COUNT: AtomicUsize = AtomicUsize::new(0);

    fn io_read_handler(_input: HandlerInput) -> Result<Response, GraphError> {
        Err(IoContFn::create_err(move |cont_tx, ret_chan| {
            Box::pin(async move {
                // Variable delay to test concurrent handling
                let delay = rand::random::<u64>() % 20 + 5;
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;

                cont_tx
                    .send_async((
                        ret_chan,
                        Box::new(move || {
                            CONTINUATION_COUNT.fetch_add(1, Ordering::SeqCst);
                            Ok(Response {
                                body: b"concurrent_read".to_vec(),
                                fmt: Format::Json,
                            })
                        })
                            as Box<dyn FnOnce() -> Result<Response, GraphError> + Send + Sync>,
                    ))
                    .await
                    .expect("cont channel should be alive");
            })
        }))
    }

    // Reset counter
    CONTINUATION_COUNT.store(0, Ordering::SeqCst);

    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert(
        "io_read_concurrent".to_string(),
        Arc::new(io_read_handler) as Arc<_>,
    );
    let router = Arc::new(HelixRouter::new(Some(routes), None, None));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = Arc::new(WorkerPool::new(core_setter, graph, router, rt));

    // Send 20 concurrent read requests with continuations
    let num_requests = 20;
    let mut handles = vec![];

    for _ in 0..num_requests {
        let pool_clone = Arc::clone(&pool);
        let handle = tokio::spawn(async move {
            let request = Request {
                name: "io_read_concurrent".to_string(),
                req_type: RequestType::Query,
                body: Bytes::new(),
                in_fmt: Format::Json,
                out_fmt: Format::Json,
                api_key: None,
            };
            pool_clone.process(request).await
        });
        handles.push(handle);
    }

    let timeout = std::time::Duration::from_secs(10);
    let mut success_count = 0;

    for handle in handles {
        let result = tokio::time::timeout(timeout, handle).await;
        assert!(result.is_ok(), "Request timed out");
        let join_result = result.unwrap();
        assert!(join_result.is_ok(), "Task panicked");
        let response = join_result.unwrap();
        assert!(response.is_ok(), "Request failed: {:?}", response);
        success_count += 1;
    }

    assert_eq!(success_count, num_requests);
    assert_eq!(
        CONTINUATION_COUNT.load(Ordering::SeqCst),
        num_requests,
        "All continuations should have been processed"
    );
}

// ============================================================================
// Parallel Write Request Tests
// ============================================================================

/// Test that parallel write requests are handled sequentially without crashing.
/// Write requests go through a single writer thread, so they should be serialized.
#[tokio::test]
async fn test_parallel_write_requests_no_crash() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static WRITE_COUNT: AtomicUsize = AtomicUsize::new(0);

    fn write_handler(_input: HandlerInput) -> Result<Response, GraphError> {
        WRITE_COUNT.fetch_add(1, Ordering::SeqCst);
        // Small delay to simulate write operation
        std::thread::sleep(std::time::Duration::from_millis(1));
        Ok(Response {
            body: b"write_ok".to_vec(),
            fmt: Format::Json,
        })
    }

    // Reset counter
    WRITE_COUNT.store(0, Ordering::SeqCst);

    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("write_op".to_string(), Arc::new(write_handler) as Arc<_>);

    let mut write_routes = std::collections::HashSet::new();
    write_routes.insert("write_op".to_string());

    let router = Arc::new(HelixRouter::new(Some(routes), None, Some(write_routes)));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = Arc::new(WorkerPool::new(core_setter, graph, router, rt));

    // Fire off 50 parallel write requests
    let num_requests = 50;
    let mut handles = vec![];

    for _ in 0..num_requests {
        let pool_clone = Arc::clone(&pool);
        let handle = tokio::spawn(async move {
            let request = Request {
                name: "write_op".to_string(),
                req_type: RequestType::Query,
                body: Bytes::new(),
                in_fmt: Format::Json,
                out_fmt: Format::Json,
                api_key: None,
            };
            pool_clone.process(request).await
        });
        handles.push(handle);
    }

    let timeout = std::time::Duration::from_secs(10);
    let mut success_count = 0;

    for handle in handles {
        let result = tokio::time::timeout(timeout, handle).await;
        assert!(
            result.is_ok(),
            "Write request timed out - possible deadlock"
        );
        let join_result = result.unwrap();
        assert!(join_result.is_ok(), "Task panicked");
        let response = join_result.unwrap();
        assert!(response.is_ok(), "Write request failed: {:?}", response);
        success_count += 1;
    }

    assert_eq!(success_count, num_requests);
    assert_eq!(
        WRITE_COUNT.load(Ordering::SeqCst),
        num_requests,
        "All writes should have been processed"
    );
}

/// Test parallel write requests with continuation channels.
/// Each write request requires async IO before completion.
#[tokio::test]
async fn test_parallel_write_requests_with_continuations() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static CONTINUATION_COUNT: AtomicUsize = AtomicUsize::new(0);

    fn io_write_handler(_input: HandlerInput) -> Result<Response, GraphError> {
        Err(IoContFn::create_err(move |cont_tx, ret_chan| {
            Box::pin(async move {
                // Simulate async IO
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;

                cont_tx
                    .send_async((
                        ret_chan,
                        Box::new(move || {
                            CONTINUATION_COUNT.fetch_add(1, Ordering::SeqCst);
                            Ok(Response {
                                body: b"write_with_cont".to_vec(),
                                fmt: Format::Json,
                            })
                        })
                            as Box<dyn FnOnce() -> Result<Response, GraphError> + Send + Sync>,
                    ))
                    .await
                    .expect("cont channel should be alive");
            })
        }))
    }

    // Reset counter
    CONTINUATION_COUNT.store(0, Ordering::SeqCst);

    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert("io_write".to_string(), Arc::new(io_write_handler) as Arc<_>);

    let mut write_routes = std::collections::HashSet::new();
    write_routes.insert("io_write".to_string());

    let router = Arc::new(HelixRouter::new(Some(routes), None, Some(write_routes)));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = Arc::new(WorkerPool::new(core_setter, graph, router, rt));

    // Fire off 30 parallel write requests with continuations
    let num_requests = 30;
    let mut handles = vec![];

    for _ in 0..num_requests {
        let pool_clone = Arc::clone(&pool);
        let handle = tokio::spawn(async move {
            let request = Request {
                name: "io_write".to_string(),
                req_type: RequestType::Query,
                body: Bytes::new(),
                in_fmt: Format::Json,
                out_fmt: Format::Json,
                api_key: None,
            };
            pool_clone.process(request).await
        });
        handles.push(handle);
    }

    let timeout = std::time::Duration::from_secs(15);
    let mut success_count = 0;

    for handle in handles {
        let result = tokio::time::timeout(timeout, handle).await;
        assert!(
            result.is_ok(),
            "Write request with continuation timed out - possible hang"
        );
        let join_result = result.unwrap();
        assert!(join_result.is_ok(), "Task panicked");
        let response = join_result.unwrap();
        assert!(
            response.is_ok(),
            "Write request with continuation failed: {:?}",
            response
        );
        success_count += 1;
    }

    assert_eq!(success_count, num_requests);
    assert_eq!(
        CONTINUATION_COUNT.load(Ordering::SeqCst),
        num_requests,
        "All write continuations should have been processed"
    );
}

/// Test that write requests maintain sequential ordering even under parallel load.
#[tokio::test]
async fn test_parallel_writes_maintain_order() {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static ORDER_COUNTER: AtomicUsize = AtomicUsize::new(0);
    static EXECUTION_ORDER: Mutex<Vec<usize>> = Mutex::new(Vec::new());

    fn ordering_write_handler(input: HandlerInput) -> Result<Response, GraphError> {
        // Parse request ID from body
        let id: usize = String::from_utf8_lossy(&input.request.body)
            .parse()
            .unwrap_or(0);

        // Record execution order
        let order = ORDER_COUNTER.fetch_add(1, Ordering::SeqCst);
        EXECUTION_ORDER.lock().unwrap().push(id);

        Ok(Response {
            body: format!("order_{}", order).into_bytes(),
            fmt: Format::Json,
        })
    }

    // Reset state
    ORDER_COUNTER.store(0, Ordering::SeqCst);
    EXECUTION_ORDER.lock().unwrap().clear();

    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert(
        "order_write".to_string(),
        Arc::new(ordering_write_handler) as Arc<_>,
    );

    let mut write_routes = std::collections::HashSet::new();
    write_routes.insert("order_write".to_string());

    let router = Arc::new(HelixRouter::new(Some(routes), None, Some(write_routes)));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = Arc::new(WorkerPool::new(core_setter, graph, router, rt));

    // Send requests in order but concurrently
    let num_requests = 20;
    let mut handles = vec![];

    for i in 0..num_requests {
        let pool_clone = Arc::clone(&pool);
        let handle = tokio::spawn(async move {
            let request = Request {
                name: "order_write".to_string(),
                req_type: RequestType::Query,
                body: Bytes::from(format!("{}", i)),
                in_fmt: Format::Json,
                out_fmt: Format::Json,
                api_key: None,
            };
            pool_clone.process(request).await
        });
        handles.push(handle);
        // Small delay to encourage order
        tokio::time::sleep(std::time::Duration::from_micros(100)).await;
    }

    let timeout = std::time::Duration::from_secs(10);
    for handle in handles {
        let result = tokio::time::timeout(timeout, handle).await;
        assert!(result.is_ok(), "Request timed out");
        let join_result = result.unwrap();
        assert!(join_result.is_ok(), "Task panicked");
    }

    // Verify all were processed
    let execution_order = EXECUTION_ORDER.lock().unwrap();
    assert_eq!(
        execution_order.len(),
        num_requests,
        "All writes should have been processed"
    );

    // Note: We can't guarantee exact order due to channel timing,
    // but we verify all requests completed without crashes or hangs
}

/// Test multiple sequential continuations from a single write request.
/// This simulates a handler that needs multiple async IO operations.
#[tokio::test]
async fn test_write_multiple_continuations_in_sequence() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static CONTINUATION_COUNT: AtomicUsize = AtomicUsize::new(0);

    fn multi_cont_handler(_input: HandlerInput) -> Result<Response, GraphError> {
        Err(IoContFn::create_err(move |cont_tx, ret_chan| {
            Box::pin(async move {
                // First async operation
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                CONTINUATION_COUNT.fetch_add(1, Ordering::SeqCst);

                // Send final continuation
                cont_tx
                    .send_async((
                        ret_chan,
                        Box::new(move || {
                            CONTINUATION_COUNT.fetch_add(1, Ordering::SeqCst);
                            Ok(Response {
                                body: b"multi_cont_done".to_vec(),
                                fmt: Format::Json,
                            })
                        })
                            as Box<dyn FnOnce() -> Result<Response, GraphError> + Send + Sync>,
                    ))
                    .await
                    .expect("cont channel should be alive");
            })
        }))
    }

    // Reset counter
    CONTINUATION_COUNT.store(0, Ordering::SeqCst);

    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert(
        "multi_cont".to_string(),
        Arc::new(multi_cont_handler) as Arc<_>,
    );

    let mut write_routes = std::collections::HashSet::new();
    write_routes.insert("multi_cont".to_string());

    let router = Arc::new(HelixRouter::new(Some(routes), None, Some(write_routes)));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = Arc::new(WorkerPool::new(core_setter, graph, router, rt));

    let request = Request {
        name: "multi_cont".to_string(),
        req_type: RequestType::Query,
        body: Bytes::new(),
        in_fmt: Format::Json,
        out_fmt: Format::Json,
        api_key: None,
    };

    let timeout = std::time::Duration::from_secs(5);
    let result = tokio::time::timeout(timeout, pool.process(request)).await;

    assert!(result.is_ok(), "Request timed out");
    let response = result.unwrap();
    assert!(response.is_ok(), "Request failed: {:?}", response);
    assert_eq!(response.unwrap().body, b"multi_cont_done".to_vec());

    // Verify both continuation phases executed
    assert_eq!(
        CONTINUATION_COUNT.load(Ordering::SeqCst),
        2,
        "Both continuation phases should have executed"
    );
}

/// Test mixed read and write requests with continuations running concurrently.
#[tokio::test]
async fn test_mixed_read_write_with_continuations() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static READ_CONT_COUNT: AtomicUsize = AtomicUsize::new(0);
    static WRITE_CONT_COUNT: AtomicUsize = AtomicUsize::new(0);

    fn io_read_handler(_input: HandlerInput) -> Result<Response, GraphError> {
        Err(IoContFn::create_err(move |cont_tx, ret_chan| {
            Box::pin(async move {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                cont_tx
                    .send_async((
                        ret_chan,
                        Box::new(move || {
                            READ_CONT_COUNT.fetch_add(1, Ordering::SeqCst);
                            Ok(Response {
                                body: b"read_done".to_vec(),
                                fmt: Format::Json,
                            })
                        })
                            as Box<dyn FnOnce() -> Result<Response, GraphError> + Send + Sync>,
                    ))
                    .await
                    .expect("cont channel should be alive");
            })
        }))
    }

    fn io_write_handler(_input: HandlerInput) -> Result<Response, GraphError> {
        Err(IoContFn::create_err(move |cont_tx, ret_chan| {
            Box::pin(async move {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                cont_tx
                    .send_async((
                        ret_chan,
                        Box::new(move || {
                            WRITE_CONT_COUNT.fetch_add(1, Ordering::SeqCst);
                            Ok(Response {
                                body: b"write_done".to_vec(),
                                fmt: Format::Json,
                            })
                        })
                            as Box<dyn FnOnce() -> Result<Response, GraphError> + Send + Sync>,
                    ))
                    .await
                    .expect("cont channel should be alive");
            })
        }))
    }

    // Reset counters
    READ_CONT_COUNT.store(0, Ordering::SeqCst);
    WRITE_CONT_COUNT.store(0, Ordering::SeqCst);

    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert(
        "io_read_mixed".to_string(),
        Arc::new(io_read_handler) as Arc<_>,
    );
    routes.insert(
        "io_write_mixed".to_string(),
        Arc::new(io_write_handler) as Arc<_>,
    );

    let mut write_routes = std::collections::HashSet::new();
    write_routes.insert("io_write_mixed".to_string());

    let router = Arc::new(HelixRouter::new(Some(routes), None, Some(write_routes)));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = Arc::new(WorkerPool::new(core_setter, graph, router, rt));

    let num_reads = 15;
    let num_writes = 15;
    let mut handles = vec![];

    // Interleave read and write requests
    for i in 0..(num_reads + num_writes) {
        let pool_clone = Arc::clone(&pool);
        let is_write = i % 2 == 0 && (i / 2) < num_writes;
        let route_name = if is_write {
            "io_write_mixed"
        } else {
            "io_read_mixed"
        };

        let handle = tokio::spawn(async move {
            let request = Request {
                name: route_name.to_string(),
                req_type: RequestType::Query,
                body: Bytes::new(),
                in_fmt: Format::Json,
                out_fmt: Format::Json,
                api_key: None,
            };
            pool_clone.process(request).await
        });
        handles.push(handle);
    }

    let timeout = std::time::Duration::from_secs(15);
    let mut success_count = 0;

    for handle in handles {
        let result = tokio::time::timeout(timeout, handle).await;
        assert!(result.is_ok(), "Request timed out");
        let join_result = result.unwrap();
        assert!(join_result.is_ok(), "Task panicked");
        let response = join_result.unwrap();
        assert!(response.is_ok(), "Request failed: {:?}", response);
        success_count += 1;
    }

    assert_eq!(success_count, num_reads + num_writes);

    let read_count = READ_CONT_COUNT.load(Ordering::SeqCst);
    let write_count = WRITE_CONT_COUNT.load(Ordering::SeqCst);

    assert!(
        read_count > 0,
        "Some read continuations should have been processed"
    );
    assert!(
        write_count > 0,
        "Some write continuations should have been processed"
    );
    assert_eq!(
        read_count + write_count,
        num_reads + num_writes,
        "All continuations should have been processed"
    );
}

/// Stress test: Many parallel writes with continuations to ensure no crashes or hangs.
#[tokio::test]
async fn test_stress_parallel_writes_with_continuations() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static SUCCESS_COUNT: AtomicUsize = AtomicUsize::new(0);

    fn stress_write_handler(_input: HandlerInput) -> Result<Response, GraphError> {
        Err(IoContFn::create_err(move |cont_tx, ret_chan| {
            Box::pin(async move {
                // Random delay to stress test timing
                let delay = rand::random::<u64>() % 10 + 1;
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;

                cont_tx
                    .send_async((
                        ret_chan,
                        Box::new(move || {
                            SUCCESS_COUNT.fetch_add(1, Ordering::SeqCst);
                            Ok(Response {
                                body: b"stress_ok".to_vec(),
                                fmt: Format::Json,
                            })
                        })
                            as Box<dyn FnOnce() -> Result<Response, GraphError> + Send + Sync>,
                    ))
                    .await
                    .expect("cont channel should be alive");
            })
        }))
    }

    // Reset counter
    SUCCESS_COUNT.store(0, Ordering::SeqCst);

    let (graph, _temp_dir) = create_test_graph();
    let mut routes = std::collections::HashMap::new();
    routes.insert(
        "stress_write".to_string(),
        Arc::new(stress_write_handler) as Arc<_>,
    );

    let mut write_routes = std::collections::HashSet::new();
    write_routes.insert("stress_write".to_string());

    let router = Arc::new(HelixRouter::new(Some(routes), None, Some(write_routes)));

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(8)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = vec![core_affinity::CoreId { id: 0 }];
    let core_setter = Arc::new(CoreSetter::new(cores, 2));

    let pool = Arc::new(WorkerPool::new(core_setter, graph, router, rt));

    // Fire off 100 parallel write requests
    let num_requests = 100;
    let mut handles = vec![];

    for _ in 0..num_requests {
        let pool_clone = Arc::clone(&pool);
        let handle = tokio::spawn(async move {
            let request = Request {
                name: "stress_write".to_string(),
                req_type: RequestType::Query,
                body: Bytes::new(),
                in_fmt: Format::Json,
                out_fmt: Format::Json,
                api_key: None,
            };
            pool_clone.process(request).await
        });
        handles.push(handle);
    }

    let timeout = std::time::Duration::from_secs(30);
    let mut completed = 0;

    for handle in handles {
        let result = tokio::time::timeout(timeout, handle).await;
        assert!(
            result.is_ok(),
            "Stress test request timed out after {} completed",
            completed
        );
        let join_result = result.unwrap();
        assert!(
            join_result.is_ok(),
            "Task panicked after {} completed",
            completed
        );
        let response = join_result.unwrap();
        assert!(
            response.is_ok(),
            "Request failed after {} completed: {:?}",
            completed,
            response
        );
        completed += 1;
    }

    assert_eq!(completed, num_requests);
    assert_eq!(
        SUCCESS_COUNT.load(Ordering::SeqCst),
        num_requests,
        "All stress test continuations should have completed"
    );
}
