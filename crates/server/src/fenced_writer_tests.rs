//! A writer a newer writer fenced stops taking traffic but stays live, over
//! both transports, and its probes say why; a reader is unaffected.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::Router;
use db::{HelixDB, HelixDbMode, IndexRuntimeReadiness, StorageState};
use grpc::pb::helix_db_server_server::HelixDbServer;
use helix_ast::{batch, query::QueryRequest, traversal};
use serde_json::json;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use tower::ServiceExt;

use crate::{grpc, http, state};

/// Longer than SlateDB's default one-second manifest poll, by which an idle
/// fenced writer learns of the fence.
const DETECTION_BOUND: Duration = Duration::from_secs(10);

fn serve(db: &Arc<HelixDB>) -> (Router, grpc::GrpcService) {
    let state = state::ServerState::new(Arc::clone(db), None, crate::StorageConfig::Memory);
    (http::router(state.clone()), grpc::GrpcService::new(state))
}

async fn get(router: &Router, path: &str) -> (StatusCode, serde_json::Value) {
    let response = router
        .clone()
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 4096).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

async fn post(router: &Router, path: &str, body: Vec<u8>) -> (StatusCode, serde_json::Value) {
    let response = router
        .clone()
        .oneshot(
            Request::post(path)
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 65536).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

fn native_read() -> Vec<u8> {
    simd_json::to_vec(&QueryRequest::read(
        batch::read_batch()
            .var_as("count", traversal::g().n_with_label("Doc").count())
            .returning(["count"]),
    ))
    .unwrap()
}

fn native_write() -> Vec<u8> {
    simd_json::to_vec(&QueryRequest::write(
        batch::write_batch()
            .var_as(
                "created",
                traversal::g().add_n("Doc", Vec::<(&str, i64)>::new()),
            )
            .returning(["created"]),
    ))
    .unwrap()
}

/// One request of every kind each transport takes: (HTTP path, body).
fn every_request() -> [(&'static str, Vec<u8>); 8] {
    let sql = |query: &str| {
        json!({"query": query, "mappings": {"tables": {"docs": "Doc"}}})
            .to_string()
            .into_bytes()
    };
    let cypher = |query: &str| json!({ "query": query }).to_string().into_bytes();
    [
        ("/v2/query", native_read()),
        ("/v2/query", native_write()),
        ("/v2/cypher", cypher("MATCH (d:Doc) RETURN count(d)")),
        ("/v2/cypher", cypher("CREATE (:Doc)")),
        ("/v2/cypher/explain", cypher("MATCH (d:Doc) RETURN d")),
        ("/v2/sql", sql("SELECT * FROM docs")),
        ("/v2/sql", sql("INSERT INTO docs (name) VALUES ('x')")),
        ("/v2/sql/explain", sql("SELECT * FROM docs")),
    ]
}

/// The gRPC call matching an HTTP path, as its error code or success.
async fn grpc_call(service: &grpc::GrpcService, path: &str, body: Vec<u8>) -> Option<String> {
    let request = tonic::Request::new(grpc::pb::QueryJsonRequest {
        body: body.into(),
        ..Default::default()
    });
    let result = match path {
        "/v2/query" => service.execute_query(request).await,
        "/v2/cypher" => service.execute_cypher(request).await,
        "/v2/cypher/explain" => service.explain_cypher(request).await,
        "/v2/sql" => service.execute_sql(request).await,
        "/v2/sql/explain" => service.explain_sql(request).await,
        other => unreachable!("no gRPC call for {other}"),
    };
    let Err(status) = result else {
        return None;
    };
    Some(
        status
            .metadata()
            .get("helix-error-code")
            .expect("every failure carries its code")
            .to_str()
            .unwrap()
            .to_owned(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fenced_writer_is_unready_but_live_and_fails_every_request_with_the_fenced_code() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let old = Arc::new(
        HelixDB::open_with_object_store("transport-fenced-writer", Arc::clone(&store))
            .await
            .unwrap(),
    );
    let (router, service) = serve(&old);
    for path in ["/healthz", "/readyz"] {
        let (status, body) = get(&router, path).await;
        assert_eq!(status, StatusCode::OK, "{path}");
        assert_eq!(body["ready"], true, "{path}");
        assert_eq!(body["storage"], "open", "{path}");
    }
    let health = service
        .health(tonic::Request::new(grpc::pb::HealthRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert!(health.ready);
    assert_eq!(health.storage, "open");
    for (path, body) in every_request() {
        let (status, value) = post(&router, path, body.clone()).await;
        assert_eq!(status, StatusCode::OK, "{path}: {value}");
        assert_eq!(grpc_call(&service, path, body).await, None, "{path}");
    }

    // The fenced writer gets no request until its readiness turns.
    let new = HelixDB::open_with_object_store("transport-fenced-writer", Arc::clone(&store))
        .await
        .unwrap();
    let fenced = Instant::now();
    loop {
        // Each answer comes from one look: its code and body agree even
        // while the fence lands.
        let (status, body) = get(&router, "/readyz").await;
        assert_eq!(status == StatusCode::OK, body["ready"] == true, "{body}");
        assert_eq!(
            status == StatusCode::OK,
            body["storage"] == "open",
            "{body}"
        );
        if status != StatusCode::OK {
            break;
        }
        assert!(
            fenced.elapsed() < DETECTION_BOUND,
            "an idle fenced writer turns unready"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(old.storage_state(), StorageState::Fenced);

    let (status, body) = get(&router, "/readyz").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["ready"], false);
    assert_eq!(body["storage"], "fenced");
    assert_eq!(body["mode"], "writer");
    let (status, body) = get(&router, "/healthz").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a restart would fence the newer writer, so liveness holds"
    );
    assert_eq!(body["ready"], false);
    assert_eq!(body["storage"], "fenced");
    let health = service
        .health(tonic::Request::new(grpc::pb::HealthRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert!(!health.ready);
    assert_eq!(health.storage, "fenced");

    for (path, body) in every_request() {
        let (status, value) = post(&router, path, body.clone()).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{path}: {value}");
        assert_eq!(
            value["error"], "writer_fenced_commit_outcome_unknown",
            "{path}: {value}"
        );
        assert_eq!(value["retryable"], false, "{path}: {value}");
        assert_eq!(
            grpc_call(&service, path, body).await.as_deref(),
            Some("writer_fenced_commit_outcome_unknown"),
            "{path}"
        );
    }

    // The newer writer has every write the old one acknowledged.
    let (new_router, _) = serve(&Arc::new(new));
    let (status, value) = post(&new_router, "/v2/query", native_read()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["count"], 6, "three writes over each transport");
    assert_eq!(get(&new_router, "/readyz").await.0, StatusCode::OK);

    old.close().await.unwrap();
    // A closed fenced writer still says why it closed.
    assert_eq!(get(&router, "/healthz").await.0, StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reader_server_stays_ready_when_its_writer_is_fenced() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let old = HelixDB::open_with_object_store("transport-fenced-reader", Arc::clone(&store))
        .await
        .unwrap();
    old.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "created",
                traversal::g().add_n("Doc", Vec::<(&str, i64)>::new()),
            )
            .returning(["created"]),
    ))
    .await
    .unwrap();
    old.flush_writer().await.unwrap();
    let reader = Arc::new(
        HelixDB::open_reader_with_object_store("transport-fenced-reader", Arc::clone(&store))
            .await
            .unwrap(),
    );
    let (router, service) = serve(&reader);

    let new = HelixDB::open_with_object_store("transport-fenced-reader", Arc::clone(&store))
        .await
        .unwrap();
    let fenced = Instant::now();
    while old.storage_state() != StorageState::Fenced {
        assert!(
            fenced.elapsed() < DETECTION_BOUND,
            "the old writer is fenced"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    for path in ["/healthz", "/readyz"] {
        let (status, body) = get(&router, path).await;
        assert_eq!(status, StatusCode::OK, "{path}");
        assert_eq!(body["ready"], true, "{path}");
        assert_eq!(body["storage"], "open", "{path}");
        assert_eq!(body["mode"], "reader", "{path}");
    }
    let health = service
        .health(tonic::Request::new(grpc::pb::HealthRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert!(health.ready);
    assert_eq!(health.storage, "open");
    let (status, value) = post(&router, "/v2/query", native_read()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["count"], 1);

    reader.close().await.unwrap();
    old.close().await.unwrap();
    new.close().await.unwrap();
}

/// A server whose storage closed, here by its own shutdown, is neither
/// ready nor live: a restart is what it needs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_server_whose_storage_closed_is_neither_ready_nor_live() {
    let db = Arc::new(
        HelixDB::open(db::HelixDbSource::InMemory {
            database: "transport-closed-storage".into(),
        })
        .await
        .unwrap(),
    );
    let (router, service) = serve(&db);
    db.close().await.unwrap();
    for path in ["/healthz", "/readyz"] {
        let (status, body) = get(&router, path).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{path}");
        assert_eq!(body["ready"], false, "{path}");
        assert_eq!(body["storage"], "closed", "{path}");
    }
    let health = service
        .health(tonic::Request::new(grpc::pb::HealthRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert!(!health.ready);
    assert_eq!(health.storage, "closed");
}

async fn answer(response: axum::response::Response) -> (StatusCode, serde_json::Value) {
    let status = response.status();
    let body = to_bytes(response.into_body(), 4096).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

/// Each storage state answers liveness and readiness over both transports
/// from the one look each probe takes. Only open storage is ready; failed
/// and closed storage is not live either, as a restart is what it needs,
/// while a fenced writer stays live so that no restart fences the newer one.
#[tokio::test]
async fn every_storage_state_answers_both_probes_from_one_look() {
    for (storage, live, ready) in [
        (StorageState::Open, StatusCode::OK, StatusCode::OK),
        (
            StorageState::Fenced,
            StatusCode::OK,
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        (
            StorageState::Failed,
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        (
            StorageState::Closed,
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::SERVICE_UNAVAILABLE,
        ),
    ] {
        let health = state::Health {
            storage,
            mode: HelixDbMode::Writer,
            index_readiness: IndexRuntimeReadiness::Ready,
            blocked_index_entity_count: 3,
        };
        let expected = json!({
            "ready": ready == StatusCode::OK,
            "mode": "writer",
            "storage": storage.as_str(),
            "index_runtime": "ready",
            "blocked_index_entity_count": 3,
        });
        assert_eq!(
            answer(http::liveness(&health)).await,
            (live, expected.clone()),
            "{storage:?}"
        );
        assert_eq!(
            answer(http::readiness(&health)).await,
            (ready, expected),
            "{storage:?}"
        );
        assert_eq!(
            grpc::health_response(&health),
            grpc::pb::HealthResponse {
                ready: ready == StatusCode::OK,
                mode: "writer".to_string(),
                storage: storage.as_str().to_string(),
                index_runtime: "ready".to_string(),
                blocked_index_entity_count: 3,
            },
            "{storage:?}"
        );
    }
}
