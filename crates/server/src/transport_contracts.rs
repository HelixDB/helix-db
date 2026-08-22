use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::http::{Request as HttpRequest, StatusCode};
use axum::routing::post;
use db::encoding::keys::tenant::{DataScope, TenantId};
use db::encoding::property::Property as DbProperty;
use db::execution::interpreter::{
    ElementRef, ExecutionResult, ExecutionRow, ExecutionValue, ReturnedValue, RowPath, RowSack,
    RowVirtualProperties,
};
use db::query_service::{
    execute_query_on_scoped, HelixQueryService, QueryMode, QueryResponse, QueryServiceError,
};
use db::{HelixDB, HelixDbSource, ProcessLocalDatabaseToken};
use helix_ast::batch;
use helix_ast::index::IndexSpec;
use helix_ast::query::{QueryRequest, QueryRequestType};
use helix_ast::traversal;
use helix_db_testkit::fixtures::QueryCorpusAdapter;
use helix_db_testkit::transport_corpus::{
    execute_transport_corpus, expected_transport_observations,
};
use helix_db_testkit::{Result as TestkitResult, TestkitError};
use helix_planner::ir::NonEmptyString;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};
use tower::ServiceExt;

use crate::grpc;
use crate::grpc::pb::helix_db_server_client::HelixDbServerClient;
use crate::grpc::pb::{HealthRequest, QueryJsonRequest, QueryJsonResponse};
use crate::state::ServerState;
use crate::{http, MAX_QUERY_BODY_BYTES};

struct EmbeddedAdapter {
    db: Arc<HelixDB>,
}

#[async_trait]
impl QueryCorpusAdapter for EmbeddedAdapter {
    async fn execute_query(&mut self, request: QueryRequest) -> TestkitResult<serde_json::Value> {
        self.db
            .query(request)
            .await
            .map_err(|error| TestkitError::Adapter(error.to_string()))
    }

    async fn close(&mut self) -> TestkitResult<()> {
        self.db
            .close()
            .await
            .map_err(|error| TestkitError::Adapter(error.to_string()))
    }
}

struct ServiceAdapter {
    db: Arc<HelixDB>,
    service: HelixQueryService,
}

#[async_trait]
impl QueryCorpusAdapter for ServiceAdapter {
    async fn execute_query(&mut self, request: QueryRequest) -> TestkitResult<serde_json::Value> {
        let response = self
            .service
            .execute_query(request)
            .await
            .map_err(|error| TestkitError::Adapter(error.to_string()))?;
        serde_json::from_slice(
            &response
                .to_json_bytes()
                .map_err(|error| TestkitError::Adapter(error.to_string()))?,
        )
        .map_err(Into::into)
    }

    async fn close(&mut self) -> TestkitResult<()> {
        self.db
            .close()
            .await
            .map_err(|error| TestkitError::Adapter(error.to_string()))
    }
}

struct HttpAdapter {
    db: Arc<HelixDB>,
    router: axum::Router,
}

#[async_trait]
impl QueryCorpusAdapter for HttpAdapter {
    async fn execute_query(&mut self, request: QueryRequest) -> TestkitResult<serde_json::Value> {
        let is_write = request.request_type() == QueryRequestType::Write;
        let body =
            sonic_rs::to_vec(&request).map_err(|error| TestkitError::Adapter(error.to_string()))?;
        let request = HttpRequest::post("/v2/query")
            .header("content-type", "application/json")
            .header("x-helix-await-durable", is_write.to_string())
            .body(Body::from(body))
            .map_err(|error| TestkitError::Adapter(error.to_string()))?;
        let response = self
            .router
            .clone()
            .oneshot(request)
            .await
            .map_err(|error| TestkitError::Adapter(error.to_string()))?;
        if response.status() != StatusCode::OK {
            return Err(TestkitError::Adapter(format!(
                "HTTP adapter returned {}",
                response.status()
            )));
        }
        let body = to_bytes(response.into_body(), MAX_QUERY_BODY_BYTES)
            .await
            .map_err(|error| TestkitError::Adapter(error.to_string()))?;
        serde_json::from_slice(&body).map_err(Into::into)
    }

    async fn close(&mut self) -> TestkitResult<()> {
        self.db
            .close()
            .await
            .map_err(|error| TestkitError::Adapter(error.to_string()))
    }
}

struct GrpcAdapter {
    db: Arc<HelixDB>,
    address: std::net::SocketAddr,
    client: HelixDbServerClient<Channel>,
    shutdown: watch::Sender<bool>,
    server: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl GrpcAdapter {
    async fn start(db: Arc<HelixDB>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = ServerState::new(Arc::clone(&db), None);
        let (shutdown, mut shutdown_rx) = watch::channel(false);
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(grpc::server_service(state))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    while shutdown_rx.changed().await.is_ok() {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                })
                .await
        });
        let client = HelixDbServerClient::connect(format!("http://{address}"))
            .await
            .unwrap();
        Self {
            db,
            address,
            client,
            shutdown,
            server: Some(server),
        }
    }

    async fn raw_query(
        &mut self,
        request: QueryJsonRequest,
    ) -> Result<QueryJsonResponse, tonic::Status> {
        self.client
            .execute_query(request)
            .await
            .map(tonic::Response::into_inner)
    }
}

#[async_trait]
impl QueryCorpusAdapter for GrpcAdapter {
    async fn execute_query(&mut self, request: QueryRequest) -> TestkitResult<serde_json::Value> {
        let is_write = request.request_type() == QueryRequestType::Write;
        let body =
            sonic_rs::to_vec(&request).map_err(|error| TestkitError::Adapter(error.to_string()))?;
        let response = self
            .raw_query(QueryJsonRequest {
                body: body.into(),
                warm_only: false,
                require_writer: false,
                await_durable: is_write,
            })
            .await
            .map_err(|error| TestkitError::Adapter(error.to_string()))?;
        serde_json::from_slice(&response.body).map_err(Into::into)
    }

    async fn close(&mut self) -> TestkitResult<()> {
        let _ = self.shutdown.send(true);
        let server = self
            .server
            .take()
            .expect("gRPC adapter server is joined exactly once");
        server
            .await
            .map_err(|error| TestkitError::Adapter(error.to_string()))?
            .map_err(|error| TestkitError::Adapter(error.to_string()))?;
        self.db
            .close()
            .await
            .map_err(|error| TestkitError::Adapter(error.to_string()))
    }
}

async fn fresh_database(name: &str) -> Arc<HelixDB> {
    Arc::new(
        HelixDB::open(HelixDbSource::InMemory {
            database: name.to_string(),
        })
        .await
        .unwrap(),
    )
}

#[test]
fn transport_response_uses_empty_default_planner_diagnostics() {
    let response = QueryResponse::from_execution_result(ExecutionResult {
        last: None,
        variables: BTreeMap::new(),
        returns: BTreeMap::new(),
    })
    .expect("empty execution result converts");

    assert!(response.returns().is_empty());
    assert!(response.diagnostics().insights.is_empty());
}

#[test]
fn transport_response_preserves_declared_empty_return_shapes() {
    let response = QueryResponse::from_execution_result(ExecutionResult {
        last: None,
        variables: BTreeMap::new(),
        returns: BTreeMap::from([
            (
                NonEmptyString::new("list").expect("return name is non-empty"),
                ReturnedValue::EmptyList,
            ),
            (
                NonEmptyString::new("object").expect("return name is non-empty"),
                ReturnedValue::EmptyObject,
            ),
        ]),
    })
    .expect("shaped empty execution result converts");

    assert_eq!(
        response.returns(),
        &BTreeMap::from([
            ("list".to_string(), serde_json::json!([])),
            ("object".to_string(), serde_json::Value::Null),
        ])
    );
}

#[test]
fn transport_response_preserves_ranked_public_element_metadata() {
    let row = |current: ElementRef, property: &str, value: f64| ExecutionRow {
        current: Some(current.clone()),
        virtual_properties: RowVirtualProperties::from_one(
            NonEmptyString::new(property).expect("rank property is non-empty"),
            DbProperty::f64("rank", value).value,
        ),
        bindings: BTreeMap::new(),
        binding_virtual_properties: BTreeMap::new(),
        path: RowPath::from_current(current),
        path_visible: false,
        sack: RowSack::empty(),
    };
    let response = QueryResponse::from_execution_result(ExecutionResult {
        last: None,
        variables: BTreeMap::new(),
        returns: BTreeMap::from([
            (
                NonEmptyString::new("distance").expect("return name is non-empty"),
                ReturnedValue::Present(ExecutionValue::Stream(vec![row(
                    ElementRef::Node(7),
                    "$distance",
                    0.25,
                )])),
            ),
            (
                NonEmptyString::new("score").expect("return name is non-empty"),
                ReturnedValue::Present(ExecutionValue::Stream(vec![row(
                    ElementRef::Edge(9),
                    "$score",
                    1.5,
                )])),
            ),
        ]),
    })
    .expect("ranked execution result converts");

    assert_eq!(
        response.returns(),
        &BTreeMap::from([
            (
                "distance".to_string(),
                serde_json::json!([{ "$id": 7, "$distance": 0.25 }]),
            ),
            (
                "score".to_string(),
                serde_json::json!([{ "$id": 9, "$score": 1.5 }]),
            ),
        ])
    );
}

#[test]
fn vector_input_errors_are_client_failures_but_physical_errors_are_internal() {
    let invalid_inputs = [
        db::error::HelixDbError::InvalidDimension {
            expected: 3,
            got: 2,
        },
        db::error::HelixDbError::InvalidVectorComponent { index: 1 },
        db::error::HelixDbError::VectorComponentMagnitudeExceeded {
            metric: db::search::vector::VectorDistanceMetric::Euclidean,
            dimension: 3,
            component_index: 1,
            observed_magnitude: 4.0,
            inclusive_maximum: 3.0,
        },
        db::error::HelixDbError::ZeroNormCosineVector,
    ];
    for error in invalid_inputs {
        let error = QueryServiceError::Db(error);
        assert_eq!(
            http::service_error_response(error).status(),
            StatusCode::BAD_REQUEST
        );
    }

    let invalid_inputs = [
        db::error::HelixDbError::InvalidDimension {
            expected: 3,
            got: 2,
        },
        db::error::HelixDbError::InvalidVectorComponent { index: 1 },
        db::error::HelixDbError::VectorComponentMagnitudeExceeded {
            metric: db::search::vector::VectorDistanceMetric::Manhattan,
            dimension: 3,
            component_index: 1,
            observed_magnitude: 4.0,
            inclusive_maximum: 3.0,
        },
        db::error::HelixDbError::ZeroNormCosineVector,
    ];
    for error in invalid_inputs {
        assert_eq!(
            grpc::status_from_service_error(QueryServiceError::Db(error)).code(),
            tonic::Code::InvalidArgument
        );
    }

    for error in [
        db::error::HelixDbError::InvalidVectorItem(
            db::search::vector::VectorItemDecodeError::HeaderMismatch,
        ),
        db::error::HelixDbError::InvariantViolation("stored vector is corrupt".to_string()),
    ] {
        assert_eq!(
            http::service_error_response(QueryServiceError::Db(error)).status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
    for error in [
        db::error::HelixDbError::InvalidVectorItem(
            db::search::vector::VectorItemDecodeError::HeaderMismatch,
        ),
        db::error::HelixDbError::InvariantViolation("stored vector is corrupt".to_string()),
    ] {
        assert_eq!(
            grpc::status_from_service_error(QueryServiceError::Db(error)).code(),
            tonic::Code::Internal
        );
    }
}

#[tokio::test]
async fn fenced_commit_outcome_crosses_http_once_and_is_terminal_to_the_rust_sdk() {
    let request_count = Arc::new(AtomicUsize::new(0));
    let handler_count = Arc::clone(&request_count);
    let router = axum::Router::new().route(
        "/v2/query",
        post(move || {
            let handler_count = Arc::clone(&handler_count);
            async move {
                handler_count.fetch_add(1, Ordering::SeqCst);
                http::service_error_response(QueryServiceError::Db(
                    db::error::HelixDbError::WriterFencedCommitOutcomeUnknown,
                ))
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let client = helix_db::Client::new(Some(&format!("http://{address}"))).unwrap();
    let error = client
        .query::<serde_json::Value>(QueryRequest::write(batch::write_batch()))
        .send()
        .await
        .expect_err("fenced commit outcome must be terminal");

    assert_eq!(request_count.load(Ordering::SeqCst), 1);
    assert_eq!(error.status_code(), Some(503));
    assert_eq!(
        error.remote_code(),
        Some("writer_fenced_commit_outcome_unknown")
    );
    assert_eq!(error.retryable(), Some(false));
    assert!(!error.is_conflict());
    assert!(!error.is_retryable());

    shutdown_tx.send(()).unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn embedded_service_http_and_grpc_match_the_shared_model_corpus() {
    let expected = expected_transport_observations();

    let db = fresh_database("transport-corpus-embedded").await;
    let mut embedded = EmbeddedAdapter { db };
    assert_eq!(
        execute_transport_corpus(&mut embedded).await.unwrap(),
        expected
    );
    embedded.close().await.unwrap();

    let db = fresh_database("transport-corpus-service").await;
    let mut service = ServiceAdapter {
        service: HelixQueryService::new(Arc::clone(&db)),
        db,
    };
    assert_eq!(
        execute_transport_corpus(&mut service).await.unwrap(),
        expected
    );
    service.close().await.unwrap();

    let db = fresh_database("transport-corpus-http").await;
    let mut http = HttpAdapter {
        router: http::router(ServerState::new(Arc::clone(&db), None)),
        db,
    };
    assert_eq!(execute_transport_corpus(&mut http).await.unwrap(), expected);
    http.close().await.unwrap();

    let db = fresh_database("transport-corpus-grpc").await;
    let mut grpc = GrpcAdapter::start(db).await;
    assert_eq!(execute_transport_corpus(&mut grpc).await.unwrap(), expected);
    grpc.close().await.unwrap();
}

#[tokio::test]
async fn query_service_scoped_entry_points_execute_the_same_tenant_read() {
    let db = fresh_database("transport-query-service-tenant-scope").await;
    let service = HelixQueryService::new(Arc::clone(&db));
    let scope = DataScope::Tenant(
        TenantId::from_ulid_str("0000000000000000000000000A").expect("valid tenant"),
    );
    let request = QueryRequest::read(
        batch::read_batch()
            .var_as(
                "count",
                traversal::g().n(helix_ast::graph::NodeRef::all()).count(),
            )
            .returning(["count"]),
    );

    let service_response = service
        .execute_query_scoped(request.clone(), scope)
        .await
        .expect("service tenant-scoped read succeeds");
    let direct_response = execute_query_on_scoped(db.as_ref(), request, QueryMode::Execute, scope)
        .await
        .expect("direct tenant-scoped read succeeds");

    assert_eq!(service_response.returns(), direct_response.returns());
    assert_eq!(service_response.returns().get("count"), Some(&0.into()));
    db.close().await.unwrap();
}

#[tokio::test]
async fn missing_text_index_preserves_the_public_error_code() {
    let db = fresh_database("transport-query-service-missing-text-index").await;
    let service = HelixQueryService::new(Arc::clone(&db));
    let request = QueryRequest::read(
        batch::read_batch()
            .var_as(
                "matches",
                traversal::g().text_search_nodes("Document", "body", "needle", 5, None),
            )
            .returning(["matches"]),
    );

    let error = service.execute_query(request).await.unwrap_err();
    assert_eq!(error.index_error_code(), Some("index_not_found"));
    let db_error: db::error::HelixDbError = error.into();
    assert!(matches!(
        db_error,
        db::error::HelixDbError::IndexNotFound(_)
    ));
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grpc_rejects_malformed_and_oversized_queries_then_shuts_down_cleanly() {
    let db = fresh_database("transport-errors-grpc").await;
    let mut grpc = GrpcAdapter::start(db).await;

    let malformed = grpc
        .raw_query(QueryJsonRequest {
            body: b"{".to_vec().into(),
            warm_only: false,
            require_writer: false,
            await_durable: false,
        })
        .await
        .unwrap_err();
    assert_eq!(malformed.code(), tonic::Code::InvalidArgument);
    assert!(malformed.message().starts_with("invalid query JSON:"));
    assert_eq!(
        malformed
            .metadata()
            .get(grpc::HELIX_ERROR_CODE_METADATA)
            .expect("malformed-query status includes an error code")
            .to_str()
            .expect("error codes are ASCII"),
        "invalid_query_json"
    );

    let oversized = grpc
        .raw_query(QueryJsonRequest {
            body: vec![b' '; MAX_QUERY_BODY_BYTES + 1].into(),
            warm_only: false,
            require_writer: false,
            await_durable: false,
        })
        .await
        .unwrap_err();
    assert_eq!(oversized.code(), tonic::Code::ResourceExhausted);
    assert_eq!(
        oversized
            .metadata()
            .get(grpc::HELIX_ERROR_CODE_METADATA)
            .expect("oversized-query status includes an error code")
            .to_str()
            .expect("error codes are ASCII"),
        "invalid_request_body"
    );

    grpc.close().await.unwrap();
}

#[tokio::test]
async fn http_rejects_malformed_oversized_and_incompatible_options() {
    let db = fresh_database("transport-errors-http").await;
    let router = http::router(ServerState::new(Arc::clone(&db), None));

    let malformed = router
        .clone()
        .oneshot(
            HttpRequest::post("/v2/query")
                .body(Body::from("{"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
    let malformed_body = to_bytes(malformed.into_body(), 4_096).await.unwrap();
    let malformed_json: serde_json::Value =
        serde_json::from_slice(&malformed_body).expect("malformed-query error is JSON");
    assert_eq!(malformed_json["error"], "invalid_query_json");
    assert!(malformed_json["msg"]
        .as_str()
        .expect("error message is a string")
        .starts_with("invalid query JSON:"));
    assert_eq!(malformed_json.get("code"), None);

    let oversized = router
        .clone()
        .oneshot(
            HttpRequest::post("/v2/query")
                .body(Body::from(vec![b' '; MAX_QUERY_BODY_BYTES + 1]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(oversized.status(), StatusCode::BAD_REQUEST);
    let oversized_body = to_bytes(oversized.into_body(), 4_096).await.unwrap();
    let oversized_json: serde_json::Value =
        serde_json::from_slice(&oversized_body).expect("oversized-query error is JSON");
    assert_eq!(oversized_json["error"], "invalid_request_body");
    assert!(oversized_json["msg"]
        .as_str()
        .expect("error message is a string")
        .starts_with("failed to read request body:"));
    assert_eq!(oversized_json.get("code"), None);

    let write = QueryRequest::write(batch::write_batch());
    let incompatible = router
        .oneshot(
            HttpRequest::post("/v2/query")
                .header("x-helix-warm", "true")
                .body(Body::from(sonic_rs::to_vec(&write).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(incompatible.status(), StatusCode::BAD_REQUEST);
    let incompatible_body = to_bytes(incompatible.into_body(), 4_096).await.unwrap();
    let incompatible_json: serde_json::Value =
        serde_json::from_slice(&incompatible_body).expect("option error is JSON");
    assert_eq!(incompatible_json["error"], "invalid_request_option");
    assert_eq!(
        incompatible_json["msg"],
        "x-helix-warm is only valid for read requests"
    );
    assert_eq!(incompatible_json.get("code"), None);

    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transport_readiness_reports_direct_text_storage_as_ready() {
    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(
        HelixDB::open_with_object_store("transport-readiness-missing", object_store)
            .await
            .unwrap(),
    );
    let router = http::router(ServerState::new(Arc::clone(&db), None));

    let liveness = router
        .clone()
        .oneshot(HttpRequest::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(liveness.status(), StatusCode::OK);
    let liveness_body = to_bytes(liveness.into_body(), 4_096).await.unwrap();
    let liveness_json: serde_json::Value = serde_json::from_slice(&liveness_body).unwrap();
    assert_eq!(liveness_json["ready"], true);
    assert_eq!(liveness_json["index_runtime"], "ready");
    assert_eq!(liveness_json.get("text_index_runtime"), None);

    let readiness = router
        .clone()
        .oneshot(HttpRequest::get("/readyz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(readiness.status(), StatusCode::OK);

    let create = QueryRequest::write(
        batch::write_batch()
            .var_as(
                "operation",
                traversal::g()
                    .create_index_if_not_exists(IndexSpec::node_equality("Document", "rank")),
            )
            .returning(["operation"]),
    );
    let response = router
        .oneshot(
            HttpRequest::post("/v2/query")
                .header("content-type", "application/json")
                .body(Body::from(sonic_rs::to_vec(&create).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grpc_enforces_writer_routing_deadlines_connection_churn_and_restart() {
    let token = ProcessLocalDatabaseToken::new("transport-grpc-restart").unwrap();
    let writer = Arc::new(
        HelixDB::open(HelixDbSource::InMemoryToken {
            token: token.clone(),
        })
        .await
        .unwrap(),
    );
    let mut grpc = GrpcAdapter::start(Arc::clone(&writer)).await;

    for _ in 0..4 {
        let mut client = HelixDbServerClient::connect(format!("http://{}", grpc.address))
            .await
            .unwrap();
        let health = client.health(HealthRequest {}).await.unwrap().into_inner();
        assert!(health.ready);
        assert_eq!(health.index_runtime, "ready");
    }

    let mut expired = tonic::Request::new(QueryJsonRequest {
        body: vec![b' '; MAX_QUERY_BODY_BYTES / 2].into(),
        warm_only: false,
        require_writer: false,
        await_durable: false,
    });
    expired.set_timeout(Duration::from_millis(1));
    let deadline = grpc.client.execute_query(expired).await.unwrap_err();
    assert!(matches!(
        deadline.code(),
        tonic::Code::Cancelled | tonic::Code::DeadlineExceeded
    ));

    let insert = helix_db_testkit::transport_corpus::transport_query_corpus()
        .remove(0)
        .request();
    assert_eq!(
        grpc.execute_query(insert).await.unwrap(),
        expected_transport_observations()[0]
    );
    grpc.close().await.unwrap();

    let reopened = Arc::new(
        HelixDB::open(HelixDbSource::InMemoryToken {
            token: token.clone(),
        })
        .await
        .unwrap(),
    );
    let mut restarted = GrpcAdapter::start(reopened).await;
    let read = helix_db_testkit::transport_corpus::transport_query_corpus()
        .remove(1)
        .request();
    assert_eq!(
        restarted.execute_query(read).await.unwrap(),
        expected_transport_observations()[1]
    );
    restarted.close().await.unwrap();

    let writer = Arc::new(
        HelixDB::open(HelixDbSource::InMemoryToken {
            token: token.clone(),
        })
        .await
        .unwrap(),
    );
    writer.flush_writer().await.unwrap();
    let reader = Arc::new(
        HelixDB::open_reader(HelixDbSource::InMemoryToken { token })
            .await
            .unwrap(),
    );
    let mut read_transport = GrpcAdapter::start(reader).await;
    let count = QueryRequest::read(
        batch::read_batch()
            .var_as(
                "count",
                traversal::g().n(helix_ast::graph::NodeRef::all()).count(),
            )
            .returning(["count"]),
    );
    let body = sonic_rs::to_vec(&count).unwrap();
    let require_writer = read_transport
        .raw_query(QueryJsonRequest {
            body: body.into(),
            warm_only: false,
            require_writer: true,
            await_durable: false,
        })
        .await
        .unwrap_err();
    assert_eq!(require_writer.code(), tonic::Code::Unavailable);
    assert_eq!(
        require_writer
            .metadata()
            .get(grpc::HELIX_ERROR_CODE_METADATA)
            .expect("writer routing status includes an error code")
            .to_str()
            .expect("error codes are ASCII"),
        "invalid_request_option"
    );

    let write_on_reader = read_transport
        .raw_query(QueryJsonRequest {
            body: sonic_rs::to_vec(&QueryRequest::write(batch::write_batch()))
                .unwrap()
                .into(),
            warm_only: false,
            require_writer: false,
            await_durable: false,
        })
        .await
        .unwrap_err();
    assert_eq!(write_on_reader.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        write_on_reader
            .metadata()
            .get(grpc::HELIX_ERROR_CODE_METADATA)
            .expect("reader-mode write status includes an error code")
            .to_str()
            .expect("error codes are ASCII"),
        "writer_mode_required"
    );

    read_transport.close().await.unwrap();
    writer.close().await.unwrap();
}
