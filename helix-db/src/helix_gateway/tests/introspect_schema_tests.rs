use std::sync::Arc;

use crate::helix_gateway::{
    gateway::CoreSetter, router::router::HelixRouter, worker_pool::WorkerPool,
};
use crate::{
    helix_engine::{
        storage_core::version_info::VersionInfo,
        traversal_core::{HelixGraphEngine, HelixGraphEngineOpts, config::Config},
    },
    helix_gateway::{gateway::AppState, introspect_schema::introspect_schema_handler},
};
use axum::extract::State;
use reqwest::StatusCode;
use tempfile::TempDir;

fn create_test_app_state(schema_json: Option<String>) -> Arc<AppState> {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let opts = HelixGraphEngineOpts {
        path: db_path.to_string(),
        config: Config::default(),
        version_info: VersionInfo::default(),
    };
    let graph = Arc::new(HelixGraphEngine::new(opts).unwrap());
    let router = Arc::new(HelixRouter::new(None, None));
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

    Arc::new(AppState {
        worker_pool,
        schema_json,
        cluster_id: None,
    })
}

#[tokio::test]
async fn test_introspect_schema_with_valid_schema() {
    let schema_json = r#"{"version":"1.0","tables":[]}"#.to_string();
    let state = create_test_app_state(Some(schema_json.clone()));

    let response = introspect_schema_handler(State(state)).await;

    assert_eq!(response.status(), StatusCode::OK);

    let content_type = response.headers().get("Content-Type");
    assert!(content_type.is_some());
    assert_eq!(content_type.unwrap(), "application/json");

    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
    assert_eq!(body_str, schema_json);
}

#[tokio::test]
async fn test_introspect_schema_without_schema() {
    let state = create_test_app_state(None);

    let response = introspect_schema_handler(State(state)).await;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
    assert_eq!(body_str, "Could not find schema");
}

#[tokio::test]
async fn test_introspect_schema_with_empty_schema() {
    let schema_json = "".to_string();
    let state = create_test_app_state(Some(schema_json.clone()));

    let response = introspect_schema_handler(State(state)).await;

    assert_eq!(response.status(), StatusCode::OK);

    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
    assert_eq!(body_str, "");
}

#[tokio::test]
async fn test_introspect_schema_with_complex_schema() {
    let schema_json = r#"{"version":"2.0","tables":[{"name":"users","fields":["id","name","email"]},{"name":"posts","fields":["id","title","content"]}]}"#.to_string();
    let state = create_test_app_state(Some(schema_json.clone()));

    let response = introspect_schema_handler(State(state)).await;

    assert_eq!(response.status(), StatusCode::OK);

    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
    assert_eq!(body_str, schema_json);
}

#[tokio::test]
async fn test_introspect_schema_response_format() {
    let schema_json = r#"{"test":"data"}"#.to_string();
    let state = create_test_app_state(Some(schema_json));

    let response = introspect_schema_handler(State(state)).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("Content-Type").unwrap(),
        "application/json"
    );

    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(!body_bytes.is_empty());
}
