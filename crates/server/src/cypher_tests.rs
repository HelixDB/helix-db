//! Production transport boundaries share one Cypher service and database.
use crate::{grpc, http, state};
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use grpc::pb::helix_db_server_server::HelixDbServer;
use serde_json::json;
use std::sync::Arc;
use tower::ServiceExt;

#[tokio::test]
async fn cypher_http_and_grpc_preserve_values_errors_and_atomic_writes() {
    let db = Arc::new(
        db::HelixDB::open(db::HelixDbSource::InMemory {
            database: "cypher-transport".into(),
        })
        .await
        .unwrap(),
    );
    let state = state::ServerState::new(Arc::clone(&db), None);
    let router = http::router(state.clone());
    let grpc = grpc::GrpcService::new(state);
    let response = router.clone().oneshot(Request::post("/v2/cypher").header("content-type","application/json")
        .body(Body::from(json!({"query":"CREATE (n:Transport {name:$name}) RETURN n.name AS name, $large AS large", "parameters":{"name":"Ada","large":{"$type":"integer","value":"9223372036854775807"}}}).to_string())).unwrap()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let value: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
    assert_eq!(
        value,
        json!({"columns":["name","large"],"rows":[["Ada",{"$type":"integer","value":"9223372036854775807"}]]})
    );

    let result = grpc
        .execute_cypher(tonic::Request::new(grpc::pb::QueryJsonRequest {
            body: json!({"query":"MATCH (n:Transport) RETURN n.name AS name"})
                .to_string()
                .into_bytes()
                .into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&result.body).unwrap(),
        json!({"columns":["name"],"rows":[["Ada"]]})
    );

    let response = router.clone().oneshot(Request::post("/v2/cypher").body(Body::from(json!({"query":"CREATE (:Transport {name:'rollback'}) WITH 1 AS x RETURN 1 / (x - x)"}).to_string())).unwrap()).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let error: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
    assert_eq!(error["details"]["phase"], "runtime");
    assert_eq!(error["details"]["detail"], "DivisionByZero");

    let error = grpc
        .execute_cypher(tonic::Request::new(grpc::pb::QueryJsonRequest {
            body: json!({"query":"RETURN missing"})
                .to_string()
                .into_bytes()
                .into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    let details: helix_planner::relational::QueryError =
        serde_json::from_slice(error.details()).unwrap();
    assert_eq!(details.detail, "UndefinedVariable");
    assert_eq!(
        details.phase,
        helix_planner::relational::ErrorPhase::Compile
    );

    let response = router
        .clone()
        .oneshot(
            Request::post("/v2/cypher")
                .header("x-helix-warm", "true")
                .body(Body::from(
                    json!({"query":"CREATE (:Transport {name:'warm'})"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let result = db
        .cypher(db::cypher::Request::new(
            "MATCH (n:Transport) RETURN count(n)",
        ))
        .await
        .unwrap();
    assert_eq!(result.rows, vec![vec![json!(1)]]);
    drop(router);
    drop(grpc);
    db.close().await.unwrap();
}

#[tokio::test]
async fn cypher_explain_has_a_separate_read_only_http_contract() {
    let source = db::HelixDbSource::InMemory {
        database: "cypher-explain-transport".into(),
    };
    let config = source
        .embedded_default_config()
        .with_query_telemetry(db::config::QueryTelemetry::Disabled);
    let db = Arc::new(db::HelixDB::open_with_config(source, config).await.unwrap());
    let router = http::router(state::ServerState::new(Arc::clone(&db), None));
    for (body, status) in [
        (
            json!({"query":"CREATE (:N {key:7})"}).to_string(),
            StatusCode::OK,
        ),
        (json!({"query":"RETURN 1/0"}).to_string(), StatusCode::OK),
        (
            json!({"query":"RETURN $missing"}).to_string(),
            StatusCode::BAD_REQUEST,
        ),
        ("invalid json".to_owned(), StatusCode::BAD_REQUEST),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::post("/v2/cypher/explain")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        let value: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap();
        if status == StatusCode::OK {
            assert!(value.get("operators").is_some());
            assert!(value.get("columns").is_none());
            assert!(value.get("rows").is_none());
        }
    }
    assert_eq!(
        db.cypher(db::cypher::Request::new("MATCH (n:N) RETURN count(*)"))
            .await
            .unwrap()
            .rows,
        vec![vec![json!(0)]]
    );
    drop(router);
    db.close().await.unwrap();
}
