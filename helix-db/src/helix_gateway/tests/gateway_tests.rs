use crate::helix_engine::traversal_core::{HelixGraphEngine, HelixGraphEngineOpts};
use crate::helix_gateway::gateway::{AppState, CoreSetter, GatewayOpts, HelixGateway};
use crate::helix_gateway::router::router::HelixRouter;
use crate::helix_gateway::worker_pool::WorkerPool;
use core_affinity::CoreId;
use std::sync::atomic;
use std::{collections::HashMap, sync::Arc};

use crate::helix_engine::traversal_core::config::Config;
use tempfile::TempDir;

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

// ============================================================================
// HelixGateway Tests
// ============================================================================

#[test]
fn test_gateway_new_basic() {
    let (graph, _temp_dir) = create_test_graph();
    let gateway = HelixGateway::new("127.0.0.1:8080", graph, 8, None, None, None, None);

    assert_eq!(gateway.address, "127.0.0.1:8080");
    assert_eq!(gateway.workers_per_core, 8);
    assert!(gateway.opts.is_none());
}

#[test]
fn test_gateway_new_with_routes() {
    let (graph, _temp_dir) = create_test_graph();
    let routes = HashMap::new();
    let gateway = HelixGateway::new("127.0.0.1:8080", graph, 8, Some(routes), None, None, None);

    assert_eq!(gateway.address, "127.0.0.1:8080");
    assert!(gateway.router.routes.is_empty());
}

#[test]
fn test_gateway_new_with_mcp_routes() {
    let (graph, _temp_dir) = create_test_graph();
    let mcp_routes = HashMap::new();
    let gateway = HelixGateway::new(
        "127.0.0.1:8080",
        graph,
        8,
        None,
        Some(mcp_routes),
        None,
        None,
    );

    assert_eq!(gateway.address, "127.0.0.1:8080");
    assert!(gateway.router.mcp_routes.is_empty());
}

#[test]
fn test_gateway_new_with_opts() {
    let (graph, temp_dir) = create_test_graph();
    let opts = HelixGraphEngineOpts {
        path: temp_dir.path().to_str().unwrap().to_string(),
        config: Config::default(),
        version_info: Default::default(),
    };
    let gateway = HelixGateway::new("127.0.0.1:8080", graph, 8, None, None, None, Some(opts));

    assert!(gateway.opts.is_some());
}

#[test]
fn test_gateway_new_with_cluster_id() {
    unsafe {
        std::env::set_var("CLUSTER_ID", "test-cluster-123");
    }
    let (graph, _temp_dir) = create_test_graph();
    let gateway = HelixGateway::new("127.0.0.1:8080", graph, 8, None, None, None, None);

    assert!(gateway.cluster_id.is_some());
    assert_eq!(gateway.cluster_id.unwrap(), "test-cluster-123");
    unsafe {
        std::env::remove_var("CLUSTER_ID");
    }
}

#[test]
fn test_gateway_fields() {
    let (graph, _temp_dir) = create_test_graph();
    let gateway = HelixGateway::new("0.0.0.0:3000", graph, 10, None, None, None, None);

    assert_eq!(gateway.address, "0.0.0.0:3000");
    assert_eq!(gateway.workers_per_core, 10);
}

#[test]
fn test_gateway_address_format() {
    let (graph, _temp_dir) = create_test_graph();
    let gateway = HelixGateway::new("localhost:8080", graph.clone(), 1, None, None, None, None);
    assert_eq!(gateway.address, "localhost:8080");

    let gateway2 = HelixGateway::new("0.0.0.0:80", graph, 1, None, None, None, None);
    assert_eq!(gateway2.address, "0.0.0.0:80");
}

#[test]
fn test_gateway_workers_per_core() {
    let (graph, _temp_dir) = create_test_graph();

    let gateway1 = HelixGateway::new("127.0.0.1:8080", graph.clone(), 1, None, None, None, None);
    assert_eq!(gateway1.workers_per_core, 1);

    let gateway2 = HelixGateway::new("127.0.0.1:8080", graph.clone(), 10, None, None, None, None);
    assert_eq!(gateway2.workers_per_core, 10);

    let gateway3 = HelixGateway::new(
        "127.0.0.1:8080",
        graph,
        GatewayOpts::DEFAULT_WORKERS_PER_CORE,
        None,
        None,
        None,
        None,
    );
    assert_eq!(gateway3.workers_per_core, 8);
}

// ============================================================================
// AppState Tests
// ============================================================================

#[test]
fn test_app_state_creation() {
    let (graph, _temp_dir) = create_test_graph();
    let router = Arc::new(HelixRouter::new(None, None, None));
    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = core_affinity::get_core_ids().unwrap_or_default();
    let core_setter = Arc::new(CoreSetter::new(cores, 2));
    let worker_pool = WorkerPool::new(core_setter, graph, router, rt);

    let state = AppState {
        worker_pool,
        schema_json: None,
        cluster_id: None,
    };

    assert!(state.schema_json.is_none());
    assert!(state.cluster_id.is_none());
}

#[test]
fn test_app_state_with_schema() {
    let (graph, _temp_dir) = create_test_graph();
    let router = Arc::new(HelixRouter::new(None, None, None));
    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = core_affinity::get_core_ids().unwrap_or_default();
    let core_setter = Arc::new(CoreSetter::new(cores, 2));
    let worker_pool = WorkerPool::new(core_setter, graph, router, rt);

    let state = AppState {
        worker_pool,
        schema_json: Some("{\"schema\": \"test\"}".to_string()),
        cluster_id: None,
    };

    assert!(state.schema_json.is_some());
    assert_eq!(state.schema_json.unwrap(), "{\"schema\": \"test\"}");
}

#[test]
fn test_app_state_with_cluster_id() {
    let (graph, _temp_dir) = create_test_graph();
    let router = Arc::new(HelixRouter::new(None, None, None));
    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );

    let cores = core_affinity::get_core_ids().unwrap_or_default();
    let core_setter = Arc::new(CoreSetter::new(cores, 2));
    let worker_pool = WorkerPool::new(core_setter, graph, router, rt);

    let state = AppState {
        worker_pool,
        schema_json: None,
        cluster_id: Some("cluster-456".to_string()),
    };

    assert!(state.cluster_id.is_some());
    assert_eq!(state.cluster_id.unwrap(), "cluster-456");
}

// ============================================================================
// CoreSetter Tests
// ============================================================================

#[test]
fn test_core_setter_new() {
    let cores = vec![CoreId { id: 0 }, CoreId { id: 1 }];
    let setter = CoreSetter::new(cores.clone(), 8);

    assert_eq!(setter.cores.len(), 2);
    assert_eq!(setter.threads_per_core, 8);
}

#[test]
fn test_core_setter_num_threads_single_core() {
    let cores = vec![CoreId { id: 0 }];
    let setter = CoreSetter::new(cores, 1);

    assert_eq!(setter.num_threads(), 1);
}

#[test]
fn test_core_setter_num_threads_multiple_cores() {
    let cores = vec![CoreId { id: 0 }, CoreId { id: 1 }, CoreId { id: 2 }];
    let setter = CoreSetter::new(cores, 1);

    assert_eq!(setter.num_threads(), 3);
}

#[test]
fn test_core_setter_num_threads_multiple_threads_per_core() {
    let cores = vec![CoreId { id: 0 }, CoreId { id: 1 }];
    let setter = CoreSetter::new(cores, 8);

    assert_eq!(setter.num_threads(), 16);
}

#[test]
fn test_core_setter_num_threads_edge_cases() {
    // Zero cores
    let setter1 = CoreSetter::new(vec![], 8);
    assert_eq!(setter1.num_threads(), 0);

    // Zero threads per core
    let cores = vec![CoreId { id: 0 }, CoreId { id: 1 }];
    let setter2 = CoreSetter::new(cores, 0);
    assert_eq!(setter2.num_threads(), 0);
}

#[test]
fn test_core_setter_calculation() {
    let cores = vec![
        CoreId { id: 0 },
        CoreId { id: 1 },
        CoreId { id: 2 },
        CoreId { id: 3 },
    ];
    let setter = CoreSetter::new(cores, 8);

    assert_eq!(setter.num_threads(), 32);
}

#[test]
fn test_core_setter_empty_cores() {
    let setter = CoreSetter::new(vec![], 10);

    assert_eq!(setter.cores.len(), 0);
    assert_eq!(setter.num_threads(), 0);
}

#[test]
fn test_core_setter_single_thread() {
    let cores = vec![CoreId { id: 0 }, CoreId { id: 1 }];
    let setter = CoreSetter::new(cores, 1);

    assert_eq!(setter.threads_per_core, 1);
    assert_eq!(setter.num_threads(), 2);
}

#[test]
fn test_core_setter_many_threads() {
    let cores = vec![CoreId { id: 0 }];
    let setter = CoreSetter::new(cores, 100);

    assert_eq!(setter.num_threads(), 100);
}

#[test]
fn test_core_setter_num_threads_consistency() {
    let cores = vec![CoreId { id: 0 }, CoreId { id: 1 }];
    let setter = CoreSetter::new(cores, 8);

    assert_eq!(setter.num_threads(), 16);
}

#[test]
fn test_core_setter_threads_per_core_zero() {
    let cores = vec![CoreId { id: 0 }];
    let setter = CoreSetter::new(cores, 0);

    assert_eq!(setter.threads_per_core, 0);
    assert_eq!(setter.num_threads(), 0);
}

#[test]
fn test_core_setter_with_default_workers() {
    let cores = vec![CoreId { id: 0 }, CoreId { id: 1 }];
    let setter = CoreSetter::new(cores, GatewayOpts::DEFAULT_WORKERS_PER_CORE);

    assert_eq!(setter.threads_per_core, 8);
    assert_eq!(setter.num_threads(), 16);
}

#[test]
fn test_core_setter_index_initial_value() {
    let cores = vec![CoreId { id: 0 }];
    let setter = CoreSetter::new(cores, 1);

    assert_eq!(setter.incrementing_index.load(atomic::Ordering::SeqCst), 0);
}

#[test]
fn test_gateway_opts_default_workers_per_core() {
    assert_eq!(GatewayOpts::DEFAULT_WORKERS_PER_CORE, 8);
}

// ============================================================================
// API Key Verification Integration Tests
// ============================================================================

#[cfg(feature = "api-key")]
mod api_key_tests {

    use crate::helix_gateway::key_verification::verify_key;
    use crate::protocol::Format;
    use crate::protocol::{HelixError, request::Request};
    use axum::body::Bytes;

    #[test]
    fn test_verify_key_wrong_key() {
        let wrong_key = "wrong-api-key";
        let result = verify_key(wrong_key);

        assert!(result.is_err());
        if let Err(e) = result {
            assert!(matches!(e, HelixError::InvalidApiKey));
        }
    }

    #[test]
    fn test_verify_key_empty_key() {
        let empty_key = "";
        let result = verify_key(empty_key);

        assert!(result.is_err());
    }

    #[test]
    fn test_request_with_api_key() {
        let api_key = "test-api-key".to_string();

        let request = Request {
            name: "test_query".to_string(),
            req_type: crate::protocol::request::RequestType::Query,
            api_key: Some(api_key.clone()),
            body: Bytes::from("{}"),
            in_fmt: Format::Json,
            out_fmt: Format::Json,
        };

        assert!(request.api_key.is_some());
        assert_eq!(request.api_key.unwrap(), api_key);
    }

    #[test]
    fn test_request_without_api_key() {
        let request = Request {
            name: "test_query".to_string(),
            req_type: crate::protocol::request::RequestType::Query,
            api_key: None,
            body: Bytes::from("{}"),
            in_fmt: Format::Json,
            out_fmt: Format::Json,
        };

        assert!(request.api_key.is_none());
    }

    #[test]
    fn test_api_key_hash_consistency() {
        // Test that SHA-256 produces consistent hashes for the same input
        use sha2::{Digest, Sha256};

        let test_key = "test-api-key";
        let hash1 = Sha256::digest(test_key.as_bytes());
        let hash2 = Sha256::digest(test_key.as_bytes());

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_sha256_verification_works() {
        use sha2::{Digest, Sha256};
        use subtle::ConstantTimeEq;

        // Test that SHA-256 verification works correctly
        let test_key = "test-api-key-12345";
        let hash = Sha256::digest(test_key.as_bytes());

        let correct_hash = Sha256::digest(test_key.as_bytes());
        let wrong_hash = Sha256::digest("wrong-key".as_bytes());

        assert!(bool::from(hash.ct_eq(&correct_hash)));
        assert!(!bool::from(hash.ct_eq(&wrong_hash)));
    }

    #[test]
    fn test_verify_key_error_type() {
        let wrong_key = "definitely-wrong-key";
        let result = verify_key(wrong_key);

        assert!(result.is_err());
        match result {
            Err(HelixError::InvalidApiKey) => {
                // Expected error type
            }
            _ => panic!("Expected InvalidApiKey error"),
        }
    }

    #[test]
    fn test_verify_key_error_message() {
        let wrong_key = "wrong-key";
        let result = verify_key(wrong_key);

        if let Err(e) = result {
            assert_eq!(e.to_string(), "Invalid API key");
        }
    }

    #[test]
    fn test_verify_key_error_http_status() {
        use axum::response::IntoResponse;

        let wrong_key = "wrong-key";
        let result = verify_key(wrong_key);

        if let Err(e) = result {
            let response = e.into_response();
            assert_eq!(response.status(), 403);
        }
    }
}
