//! Both transports report where the server time of an executed request went.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use grpc::pb::helix_db_server_server::HelixDbServer;
use helix_ast::{batch, traversal};
use serde_json::json;
use tower::ServiceExt;

use crate::{grpc, http, server_timing, state};

/// Whether a request succeeded, and the metrics it reported in milliseconds.
type Reported = (bool, BTreeMap<String, f64>);

fn metrics(value: Option<&str>) -> BTreeMap<String, f64> {
    value
        .into_iter()
        .flat_map(|value| value.split(", "))
        .map(|metric| {
            let (name, duration) = metric.split_once(";dur=").expect("a duration metric");
            (name.to_string(), duration.parse().expect("milliseconds"))
        })
        .collect()
}

/// Sends one request over HTTP, or over gRPC when given a service, setting
/// `await_durable` only when `flag` is present.
async fn send(
    router: axum::Router,
    grpc: Option<&grpc::GrpcService>,
    path: &str,
    body: Vec<u8>,
    flag: Option<bool>,
) -> Reported {
    let Some(grpc) = grpc else {
        let mut request = Request::post(path).header("content-type", "application/json");
        if let Some(flag) = flag {
            request = request.header("x-helix-await-durable", flag.to_string());
        }
        let response = router
            .oneshot(request.body(Body::from(body)).unwrap())
            .await
            .unwrap();
        let value = response
            .headers()
            .get(server_timing::NAME)
            .map(|value| value.to_str().unwrap());
        return (response.status() == StatusCode::OK, metrics(value));
    };
    let request = tonic::Request::new(grpc::pb::QueryJsonRequest {
        body: body.into(),
        await_durable: flag,
        ..Default::default()
    });
    let result = match path {
        "/v2/query" => grpc.execute_query(request).await,
        "/v2/cypher" => grpc.execute_cypher(request).await,
        "/v2/sql" => grpc.execute_sql(request).await,
        "/v2/cypher/explain" => grpc.explain_cypher(request).await,
        _ => grpc.explain_sql(request).await,
    };
    let metadata = match &result {
        Ok(response) => response.metadata(),
        Err(status) => status.metadata(),
    };
    let value = metadata
        .get(server_timing::NAME)
        .map(|value| value.to_str().unwrap());
    (result.is_ok(), metrics(value))
}

fn names(reported: &Reported) -> Vec<&str> {
    reported.1.keys().map(String::as_str).collect()
}

struct Server {
    store: Arc<helix_db_testkit::gated_wal::GatedWalStore>,
    db: Arc<db::HelixDB>,
    router: axum::Router,
    grpc: grpc::GrpcService,
}

impl Server {
    async fn open(database: &str) -> Self {
        let store = Arc::new(helix_db_testkit::gated_wal::GatedWalStore::default());
        let db = Arc::new(
            db::HelixDB::open_with_object_store(database, Arc::clone(&store) as _)
                .await
                .unwrap(),
        );
        let state = state::ServerState::new(Arc::clone(&db), None, crate::StorageConfig::Memory);
        Self {
            store,
            db,
            router: http::router(state.clone()),
            grpc: grpc::GrpcService::new(state),
        }
    }

    async fn close(self) {
        let Self {
            db, router, grpc, ..
        } = self;
        drop(grpc);
        drop(router);
        db.close().await.unwrap();
    }
}

fn native_write() -> Vec<u8> {
    serde_json::to_vec(&helix_ast::query::QueryRequest::write(
        batch::write_batch().var_as(
            "created",
            traversal::g().add_n(
                "Timed",
                vec![("source", helix_ast::value::PropertyInput::from("native"))],
            ),
        ),
    ))
    .unwrap()
}

fn sql(query: &str, parameters: serde_json::Value) -> Vec<u8> {
    json!({
        "query": query,
        "parameters": parameters,
        "mappings": {"tables": {"timed": "Timed"}},
    })
    .to_string()
    .into_bytes()
}

/// Past SlateDB's 100 ms flush interval, so a pending write is held.
const HELD: Duration = Duration::from_millis(400);
const BOUNDED: Duration = Duration::from_secs(30);

/// A write that waits reports the held WAL upload as `durable` and keeps it
/// out of `exec`; a write told not to wait reports no `durable` at all.
#[tokio::test(flavor = "multi_thread")]
async fn every_write_route_separates_the_durable_wait_from_execution() {
    let server = Server::open("server-timing-writes").await;
    // A writer's first insert persists its ID lease durably in any mode.
    assert!(
        send(
            server.router.clone(),
            None,
            "/v2/query",
            native_write(),
            None
        )
        .await
        .0
    );

    for (path, body) in [
        ("/v2/query", native_write()),
        (
            "/v2/cypher",
            json!({"query": "CREATE (:Timed)"}).to_string().into_bytes(),
        ),
        (
            "/v2/sql",
            sql("INSERT INTO timed (name) VALUES (?)", json!(["Ada"])),
        ),
    ] {
        for grpc in [None, Some(&server.grpc)] {
            server.store.hold();
            let applied = tokio::time::timeout(
                BOUNDED,
                send(server.router.clone(), grpc, path, body.clone(), Some(false)),
            )
            .await
            .unwrap();
            assert!(applied.0, "{path}");
            assert_eq!(names(&applied), ["commit", "exec"], "{path}");

            for flag in [None, Some(true)] {
                let mut write =
                    Box::pin(send(server.router.clone(), grpc, path, body.clone(), flag));
                assert!(
                    tokio::time::timeout(HELD, &mut write).await.is_err(),
                    "{path} {flag:?} must wait for its upload"
                );
                server.store.release();
                let durable = tokio::time::timeout(BOUNDED, write).await.unwrap();
                server.store.hold();
                assert!(durable.0, "{path} {flag:?}");
                assert_eq!(
                    names(&durable),
                    ["commit", "durable", "exec"],
                    "{path} {flag:?}"
                );
                // The wait began within the hold, after execution and commit.
                let held = HELD.as_secs_f64() * 1000.0;
                let waited = durable.1["durable"];
                let before = durable.1["exec"] + durable.1["commit"];
                assert!(
                    waited + before >= held && waited > before,
                    "{path} {flag:?}: {:?}",
                    durable.1
                );
            }
            server.store.release();
        }
    }
    server.close().await;
}

/// A read never commits, whatever the route or transport.
#[tokio::test(flavor = "multi_thread")]
async fn every_read_route_reports_only_execution() {
    let server = Server::open("server-timing-reads").await;
    assert!(
        send(
            server.router.clone(),
            None,
            "/v2/query",
            native_write(),
            None
        )
        .await
        .0
    );
    // A read must not wait for anything, even with uploads held.
    server.store.hold();
    for (path, body) in [
        (
            "/v2/query",
            serde_json::to_vec(&helix_ast::query::QueryRequest::read(
                batch::read_batch()
                    .var_as("count", traversal::g().n_with_label("Timed").count())
                    .returning(["count"]),
            ))
            .unwrap(),
        ),
        (
            "/v2/cypher",
            json!({"query": "MATCH (n:Timed) RETURN count(n) AS count"})
                .to_string()
                .into_bytes(),
        ),
        ("/v2/sql", sql("SELECT count(*) FROM timed", json!([]))),
    ] {
        for grpc in [None, Some(&server.grpc)] {
            let read = tokio::time::timeout(
                BOUNDED,
                send(server.router.clone(), grpc, path, body.clone(), None),
            )
            .await
            .unwrap();
            assert!(read.0, "{path}");
            assert_eq!(names(&read), ["exec"], "{path}");
        }
    }
    server.store.release();
    server.close().await;
}

/// Index DDL always waits for durability, in its own phase of the request,
/// so it reports the wait without a graph commit.
#[tokio::test(flavor = "multi_thread")]
async fn index_ddl_reports_its_durable_wait() {
    let server = Server::open("server-timing-ddl").await;
    let create = serde_json::to_vec(&helix_ast::query::QueryRequest::write(
        batch::write_batch()
            .var_as(
                "operation",
                traversal::g().create_index_if_not_exists(
                    helix_ast::index::IndexSpec::node_equality("Timed", "rank"),
                ),
            )
            .returning(["operation"]),
    ))
    .unwrap();
    for grpc in [None, Some(&server.grpc)] {
        // Even told not to wait, DDL stays durable.
        let ddl = tokio::time::timeout(
            BOUNDED,
            send(
                server.router.clone(),
                grpc,
                "/v2/query",
                create.clone(),
                Some(false),
            ),
        )
        .await
        .unwrap();
        assert!(ddl.0);
        assert_eq!(names(&ddl), ["durable", "exec"]);
    }
    server.close().await;
}

/// A request that fails after its body arrived still reports how long the
/// server spent on it, as execution alone.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_request_reports_only_execution() {
    let server = Server::open("server-timing-errors").await;
    for (path, body) in [
        ("/v2/query", b"{".to_vec()),
        ("/v2/cypher", b"{".to_vec()),
        ("/v2/sql", b"{".to_vec()),
        (
            "/v2/cypher",
            json!({"query": "MATCH (n RETURN n"})
                .to_string()
                .into_bytes(),
        ),
        ("/v2/sql", sql("SELEC 1", json!([]))),
    ] {
        for grpc in [None, Some(&server.grpc)] {
            let failed = send(server.router.clone(), grpc, path, body.clone(), None).await;
            assert!(!failed.0, "{path}");
            assert_eq!(names(&failed), ["exec"], "{path}");
        }
    }
    server.close().await;
}

/// Planning commits nothing, and health routes do no request work to time.
#[tokio::test(flavor = "multi_thread")]
async fn explain_reports_only_execution_and_health_reports_nothing() {
    let server = Server::open("server-timing-other-routes").await;
    for (path, body) in [
        (
            "/v2/cypher/explain",
            json!({"query": "MATCH (n:Timed) RETURN n"})
                .to_string()
                .into_bytes(),
        ),
        ("/v2/sql/explain", sql("SELECT name FROM timed", json!([]))),
    ] {
        for grpc in [None, Some(&server.grpc)] {
            let explained = send(server.router.clone(), grpc, path, body.clone(), None).await;
            assert!(explained.0, "{path}");
            assert_eq!(names(&explained), ["exec"], "{path}");
            let failed = send(server.router.clone(), grpc, path, b"{".to_vec(), None).await;
            assert!(!failed.0, "{path}");
            assert_eq!(names(&failed), ["exec"], "{path}");
        }
    }
    let health = server
        .router
        .clone()
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert!(health.headers().get(server_timing::NAME).is_none());
    server.close().await;
}
