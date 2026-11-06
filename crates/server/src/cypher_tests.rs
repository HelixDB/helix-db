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
    let state = state::ServerState::new(Arc::clone(&db), None, crate::StorageConfig::Memory);
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
    assert_eq!(error["error"], "arithmetic_error");
    assert_eq!(error["details"]["phase"], "runtime");
    assert_eq!(error["details"]["detail"], "division_by_zero");

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
    let details: serde_json::Value = serde_json::from_slice(error.details()).unwrap();
    assert_eq!(details["details"]["detail"], "undefined_variable");
    assert_eq!(details["details"]["phase"], "compile");

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
    let router = http::router(state::ServerState::new(
        Arc::clone(&db),
        None,
        crate::StorageConfig::Memory,
    ));
    let create = json!({"query":"CREATE (:N {key:7})"}).to_string();
    for (body, durable, status) in [
        (create.clone(), false, StatusCode::OK),
        // Explaining never commits, so it cannot await durability.
        (create, true, StatusCode::BAD_REQUEST),
        (
            json!({"query":"RETURN 1/0"}).to_string(),
            false,
            StatusCode::OK,
        ),
        (
            json!({"query":"RETURN $missing"}).to_string(),
            false,
            StatusCode::BAD_REQUEST,
        ),
        ("invalid json".to_owned(), false, StatusCode::BAD_REQUEST),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::post("/v2/cypher/explain")
                    .header("x-helix-await-durable", durable.to_string())
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

#[tokio::test]
async fn cypher_routing_checks_effects_before_parameter_validation() {
    let source = db::HelixDbSource::InMemory {
        database: "cypher-routing-precedence".into(),
    };
    let config = source
        .embedded_default_config()
        .with_query_telemetry(db::config::QueryTelemetry::Disabled);
    let db = Arc::new(db::HelixDB::open_with_config(source, config).await.unwrap());
    let state = state::ServerState::new(Arc::clone(&db), None, crate::StorageConfig::Memory);
    let router = http::router(state.clone());
    let grpc = grpc::GrpcService::new(state);
    for (text, warm, durable, detail) in [
        ("CREATE (:N {key:$missing})", true, false, None),
        ("RETURN $missing", false, true, None),
        ("RETURN $missing", true, false, Some("missing_parameter")),
        ("RETURN missing", false, true, Some("undefined_variable")),
        ("RETURN sqrt(4) AS x", false, true, Some("function")),
    ] {
        let body = json!({"query":text}).to_string();
        let response = router
            .clone()
            .oneshot(
                Request::post("/v2/cypher")
                    .header("x-helix-warm", warm.to_string())
                    .header("x-helix-await-durable", durable.to_string())
                    .body(Body::from(body.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let value: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
        let error = grpc
            .execute_cypher(tonic::Request::new(grpc::pb::QueryJsonRequest {
                body: body.into_bytes().into(),
                warm_only: warm,
                await_durable: durable.then_some(true),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        match detail {
            Some(detail) => {
                assert_eq!(value["details"]["detail"], detail);
                assert_eq!(value["details"]["phase"], "compile");
                let grpc_body: serde_json::Value = serde_json::from_slice(error.details()).unwrap();
                assert_eq!(grpc_body, value, "gRPC details carry the HTTP body");
            }
            None => {
                assert_eq!(value["error"], "invalid_request_option");
                assert_eq!(
                    error
                        .metadata()
                        .get(grpc::HELIX_ERROR_CODE_METADATA)
                        .unwrap()
                        .to_str()
                        .unwrap(),
                    "invalid_request_option"
                );
            }
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
    drop(grpc);
    db.close().await.unwrap();
}

#[tokio::test]
async fn native_and_cypher_share_one_state() {
    let db = Arc::new(
        db::HelixDB::open(db::HelixDbSource::InMemory {
            database: "cypher-beside-native".into(),
        })
        .await
        .unwrap(),
    );
    let state = state::ServerState::new(Arc::clone(&db), None, crate::StorageConfig::Memory);
    let router = http::router(state.clone());
    let grpc = grpc::GrpcService::new(state);
    let post = |path: &'static str, body: Vec<u8>| {
        let router = router.clone();
        async move {
            let response = router
                .oneshot(Request::post(path).body(Body::from(body)).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{path}");
            serde_json::from_slice::<serde_json::Value>(
                &to_bytes(response.into_body(), 65_536).await.unwrap(),
            )
            .unwrap()
        }
    };
    let cypher = |query: &str| json!({ "query": query }).to_string().into_bytes();
    let grpc_body = |query: &str| grpc::pb::QueryJsonRequest {
        body: cypher(query).into(),
        ..Default::default()
    };
    let native_count = serde_json::to_vec(&helix_ast::query::QueryRequest::read(
        helix_ast::batch::read_batch()
            .var_as(
                "count",
                helix_ast::traversal::g().n_with_label("Shared").count(),
            )
            .returning(["count"]),
    ))
    .unwrap();

    let native_write = helix_ast::query::QueryRequest::write(
        helix_ast::batch::write_batch()
            .var_as(
                "created",
                helix_ast::traversal::g().add_n(
                    "Shared",
                    vec![("source", helix_ast::value::PropertyInput::from("native"))],
                ),
            )
            .returning(["created"]),
    );
    post("/v2/query", serde_json::to_vec(&native_write).unwrap()).await;
    let durable = router
        .clone()
        .oneshot(
            Request::post(helix_cypher::api::HTTP_PATH)
                .header("x-helix-await-durable", "true")
                .body(Body::from(cypher(
                    "CREATE (:Shared {source:'cypher-http'})",
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(durable.status(), StatusCode::OK);
    grpc.execute_cypher(tonic::Request::new(grpc::pb::QueryJsonRequest {
        await_durable: Some(true),
        ..grpc_body("CREATE (:Shared {source:'cypher-grpc'})")
    }))
    .await
    .unwrap();

    assert_eq!(post("/v2/query", native_count.clone()).await["count"], 3);
    let sources = grpc
        .execute_query(tonic::Request::new(grpc::pb::QueryJsonRequest {
            body: native_count.clone().into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&sources.body).unwrap()["count"],
        3
    );
    assert_eq!(
        post(
            helix_cypher::api::HTTP_PATH,
            cypher("MATCH (n:Shared) RETURN n.source AS source ORDER BY source"),
        )
        .await["rows"],
        json!([["cypher-grpc"], ["cypher-http"], ["native"]])
    );

    // Explaining plans without executing, and both transports return one body.
    let explain = "CREATE (:Shared {source:'explained'})";
    let without_timing = |mut value: serde_json::Value| {
        let Some(_) = value["planner"]
            .as_object_mut()
            .and_then(|planner| planner.remove("optimization_micros"))
        else {
            panic!("explanation omitted its optimization duration: {value:#}");
        };
        value
    };
    let http_plan = post(helix_cypher::api::HTTP_EXPLAIN_PATH, cypher(explain)).await;
    let grpc_plan = grpc
        .explain_cypher(tonic::Request::new(grpc_body(explain)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        without_timing(http_plan),
        without_timing(serde_json::from_slice(&grpc_plan.body).unwrap())
    );
    assert_eq!(post("/v2/query", native_count).await["count"], 3);
    drop(router);
    drop(grpc);
    db.close().await.unwrap();
}

#[tokio::test]
async fn grpc_cypher_rejections_use_native_codes_and_read_explain_options() {
    let token = db::ProcessLocalDatabaseToken::new("cypher-grpc-rejections").unwrap();
    let writer = Arc::new(
        db::HelixDB::open(db::HelixDbSource::InMemoryToken {
            token: token.clone(),
        })
        .await
        .unwrap(),
    );
    let grpc = grpc::GrpcService::new(state::ServerState::new(
        Arc::clone(&writer),
        None,
        crate::StorageConfig::Memory,
    ));
    let request = |body: Vec<u8>, require_writer: bool, await_durable: bool| {
        tonic::Request::new(grpc::pb::QueryJsonRequest {
            body: body.into(),
            warm_only: false,
            require_writer,
            await_durable: await_durable.then_some(true),
        })
    };
    let error_code = |status: &tonic::Status| {
        status
            .metadata()
            .get(grpc::HELIX_ERROR_CODE_METADATA)
            .expect("status includes an error code")
            .to_str()
            .expect("error codes are ASCII")
            .to_owned()
    };
    let create = json!({"query":"CREATE (:Rejected)"})
        .to_string()
        .into_bytes();
    for (body, code, metadata) in [
        (
            b"{".to_vec(),
            tonic::Code::InvalidArgument,
            "invalid_query_json",
        ),
        (
            vec![b' '; crate::MAX_QUERY_BODY_BYTES + 1],
            tonic::Code::InvalidArgument,
            "invalid_request_body",
        ),
    ] {
        for status in [
            grpc.execute_cypher(request(body.clone(), false, false))
                .await
                .unwrap_err(),
            grpc.explain_cypher(request(body.clone(), false, false))
                .await
                .unwrap_err(),
        ] {
            assert_eq!(status.code(), code);
            assert_eq!(error_code(&status), metadata);
        }
    }
    let durable_explain = grpc
        .explain_cypher(request(create.clone(), false, true))
        .await
        .unwrap_err();
    assert_eq!(durable_explain.code(), tonic::Code::InvalidArgument);
    assert_eq!(error_code(&durable_explain), "invalid_request_option");
    let missing = grpc
        .explain_cypher(request(
            json!({"query":"RETURN $missing"}).to_string().into_bytes(),
            false,
            false,
        ))
        .await
        .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::InvalidArgument);
    let details: serde_json::Value = serde_json::from_slice(missing.details()).unwrap();
    assert_eq!(details["details"]["detail"], "missing_parameter");
    writer.flush_writer().await.unwrap();

    let reader = Arc::new(
        db::HelixDB::open_reader(db::HelixDbSource::InMemoryToken { token })
            .await
            .unwrap(),
    );
    let read_grpc = grpc::GrpcService::new(state::ServerState::new(
        Arc::clone(&reader),
        None,
        crate::StorageConfig::Memory,
    ));
    let require_writer = read_grpc
        .explain_cypher(request(create.clone(), true, false))
        .await
        .unwrap_err();
    assert_eq!(require_writer.code(), tonic::Code::FailedPrecondition);
    assert_eq!(error_code(&require_writer), "writer_mode_required");
    // A reader still plans a modifying statement, and refuses to run it.
    read_grpc
        .explain_cypher(request(create.clone(), false, false))
        .await
        .unwrap();
    let write_on_reader = read_grpc
        .execute_cypher(request(create, false, false))
        .await
        .unwrap_err();
    assert_eq!(write_on_reader.code(), tonic::Code::FailedPrecondition);
    let details: serde_json::Value = serde_json::from_slice(write_on_reader.details()).unwrap();
    assert_eq!(details["error"], "access_mode_error");
    assert_eq!(details["details"]["detail"], "writer_required");
    drop(read_grpc);
    drop(grpc);
    reader.close().await.unwrap();
    writer.close().await.unwrap();
}

#[tokio::test]
async fn cypher_rejects_malformed_bodies_and_options_before_executing() {
    let db = Arc::new(
        db::HelixDB::open(db::HelixDbSource::InMemory {
            database: "cypher-rejections".into(),
        })
        .await
        .unwrap(),
    );
    let state = state::ServerState::new(Arc::clone(&db), None, crate::StorageConfig::Memory);
    let router = http::router(state.clone());
    let malformed = router
        .clone()
        .oneshot(Request::post("/v2/cypher").body(Body::from("{")).unwrap())
        .await
        .unwrap();
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
    // A warm read option cannot apply to a modifying statement.
    let warm_write = router
        .clone()
        .oneshot(
            Request::post("/v2/cypher")
                .header("x-helix-warm", "true")
                .body(Body::from(
                    json!({"query":"CREATE (:Rejected)"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(warm_write.status(), StatusCode::BAD_REQUEST);
    let error = grpc::GrpcService::new(state)
        .execute_cypher(tonic::Request::new(grpc::pb::QueryJsonRequest {
            body: b"{".to_vec().into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    let count = router
        .oneshot(
            Request::post("/v2/cypher")
                .body(Body::from(
                    json!({"query":"MATCH (n:Rejected) RETURN count(n) AS n"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let value: serde_json::Value =
        serde_json::from_slice(&to_bytes(count.into_body(), 4096).await.unwrap()).unwrap();
    assert_eq!(value["rows"], json!([[0]]));
    db.close().await.unwrap();
}

#[tokio::test]
async fn cypher_executions_emit_anonymous_query_telemetry() {
    let db = Arc::new(
        db::HelixDB::open(db::HelixDbSource::InMemory {
            database: "cypher-telemetry".into(),
        })
        .await
        .unwrap(),
    );
    let identity =
        helix_metrics::query::OssIdentity::new(helix_metrics::query::InstallationId::now(), None);
    // An unroutable endpoint: events are counted when queued, never delivered.
    let started = helix_metrics::query::transport::start(
        helix_metrics::telemetry::Source::Server,
        &identity,
        "http://127.0.0.1:9",
    )
    .unwrap();
    let state = state::ServerState::new(
        Arc::clone(&db),
        Some(started.recorder.clone()),
        crate::StorageConfig::Memory,
    );
    let router = http::router(state.clone());
    let grpc = grpc::GrpcService::new(state);

    let response = router
        .clone()
        .oneshot(
            Request::post("/v2/cypher")
                .header(crate::TENANT_ID_HEADER_NAME, "tenant-1")
                .body(Body::from(
                    json!({"query":"CREATE (:Logged {secret:'value'})","query_name":"log"})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut failing = tonic::Request::new(grpc::pb::QueryJsonRequest {
        body: json!({"query":"WITH 1 AS x RETURN 1 / (x - x)"})
            .to_string()
            .into_bytes()
            .into(),
        ..Default::default()
    });
    failing.metadata_mut().insert(
        crate::TENANT_ID_HEADER_NAME,
        "tenant-1".parse().expect("valid metadata"),
    );
    let error = grpc.execute_cypher(failing).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    // A statement that fails to compile never reaches execution or telemetry.
    grpc.execute_cypher(tonic::Request::new(grpc::pb::QueryJsonRequest {
        body: json!({"query":"RETURN ("}).to_string().into_bytes().into(),
        ..Default::default()
    }))
    .await
    .unwrap_err();

    assert_eq!(started.recorder.counters().emitted_events, 2);
    drop(router);
    drop(grpc);
    started.runtime.shutdown().await;
    db.close().await.unwrap();
}

/// `POST /v2/sql` translates, routes and reports exactly like `/v2/cypher`.
#[tokio::test]
async fn sql_http_translates_routes_and_reports_like_cypher() {
    let db = Arc::new(
        db::HelixDB::open(db::HelixDbSource::InMemory {
            database: "sql-transport".into(),
        })
        .await
        .unwrap(),
    );
    let identity =
        helix_metrics::query::OssIdentity::new(helix_metrics::query::InstallationId::now(), None);
    let started = helix_metrics::query::transport::start(
        helix_metrics::telemetry::Source::Server,
        &identity,
        "http://127.0.0.1:9",
    )
    .unwrap();
    let state = state::ServerState::new(
        Arc::clone(&db),
        Some(started.recorder.clone()),
        crate::StorageConfig::Memory,
    );
    let router = http::router(state);
    let post = |body: String, headers: &[(&str, &str)]| {
        let mut request = Request::post(helix_cypher::api::HTTP_SQL_PATH)
            .header("content-type", "application/json");
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        let router = router.clone();
        async move {
            let response = router
                .oneshot(request.body(Body::from(body)).unwrap())
                .await
                .unwrap();
            let status = response.status();
            let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
            (
                status,
                serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
            )
        }
    };

    let (status, value) = post(
        json!({
            "query": "INSERT INTO people (name, born) VALUES (?, ?), ('Grace', 1906)",
            "parameters": ["Ada", {"$type": "integer", "value": "1815"}],
            "mappings": {"tables": {"people": "Person"}},
            "query_name": "seed",
        })
        .to_string(),
        &[("x-helix-await-durable", "true")],
    )
    .await;
    assert_eq!(
        (status, value),
        (StatusCode::OK, json!({"columns": [], "rows": []}))
    );
    let (status, value) = post(
        json!({"query": "SELECT name, born FROM Person p WHERE born < :year ORDER BY name",
               "parameters": {"year": 2000}})
        .to_string(),
        &[("x-helix-warm", "true")],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        value,
        json!({"columns": ["name", "born"], "rows": [["Ada", 1815], ["Grace", 1906]]})
    );
    // The same graph serves Cypher.
    assert_eq!(
        db.cypher(db::cypher::Request::new("MATCH (p:Person) RETURN count(p)"))
            .await
            .unwrap()
            .rows,
        vec![vec![json!(2)]]
    );

    for (body, headers, expected_status, expected_error, expected_detail) in [
        (
            json!({"query": "INSERT INTO Person (name) VALUES ('warm')"}).to_string(),
            vec![("x-helix-warm", "true")],
            StatusCode::BAD_REQUEST,
            "invalid_request_option",
            None,
        ),
        (
            "{".to_owned(),
            vec![],
            StatusCode::BAD_REQUEST,
            "invalid_query_json",
            None,
        ),
        (
            json!({"query": "SELECT 1", "unknown": true}).to_string(),
            vec![],
            StatusCode::BAD_REQUEST,
            "invalid_query_json",
            None,
        ),
        (
            json!({"query": "SELECT 1 UNION SELECT 2"}).to_string(),
            vec![],
            StatusCode::BAD_REQUEST,
            "unsupported_feature",
            Some("union"),
        ),
        (
            json!({"query": "SELECT sqrt(p.born) FROM Person p"}).to_string(),
            vec![],
            StatusCode::BAD_REQUEST,
            "unsupported_feature",
            Some("function"),
        ),
        (
            json!({"query": format!("SELECT {}1", "1,".repeat(600_000))}).to_string(),
            vec![],
            StatusCode::TOO_MANY_REQUESTS,
            "resource_limit",
            Some("too_many_tokens"),
        ),
        (
            json!({"query": "DELETE FROM Person WHERE name = ?"}).to_string(),
            vec![],
            StatusCode::BAD_REQUEST,
            "parameter_missing",
            Some("missing_parameter"),
        ),
    ] {
        let (status, value) = post(body.clone(), &headers).await;
        assert_eq!(status, expected_status, "{value}");
        assert_eq!(value["error"], expected_error, "{value}");
        let Some(detail) = expected_detail else {
            continue;
        };
        assert_eq!(value["details"]["detail"], detail, "{value}");
        assert_eq!(value["details"]["phase"], "compile", "{value}");
    }
    let (_, value) = post(
        json!({"query": "SELECT sqrt(p.born) FROM Person p"}).to_string(),
        &[],
    )
    .await;
    assert_eq!(value["details"]["span"], json!({"start": 7, "end": 18}));
    // Rejected requests changed nothing.
    assert_eq!(
        db.cypher(db::cypher::Request::new("MATCH (p:Person) RETURN count(p)"))
            .await
            .unwrap()
            .rows,
        vec![vec![json!(2)]]
    );
    // Executed SQL is recorded like Cypher, including the DELETE that failed
    // for its missing parameter; requests rejected before execution are not.
    assert_eq!(started.recorder.counters().emitted_events, 3);
    drop(router);
    started.runtime.shutdown().await;
    db.close().await.unwrap();
}

#[tokio::test]
async fn sql_explain_and_grpc_share_the_cypher_paths() {
    let db = Arc::new(
        db::HelixDB::open(db::HelixDbSource::InMemory {
            database: "sql-explain-transport".into(),
        })
        .await
        .unwrap(),
    );
    let state = state::ServerState::new(Arc::clone(&db), None, crate::StorageConfig::Memory);
    let router = http::router(state.clone());
    let grpc = grpc::GrpcService::new(state);
    let explain = |body: serde_json::Value, headers: &[(&str, &str)]| {
        let mut request = Request::post(helix_cypher::api::HTTP_SQL_EXPLAIN_PATH)
            .header("content-type", "application/json");
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        let router = router.clone();
        async move {
            let response = router
                .oneshot(request.body(Body::from(body.to_string())).unwrap())
                .await
                .unwrap();
            let status = response.status();
            let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
            (
                status,
                serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
            )
        }
    };
    let request = |body: serde_json::Value, warm_only, await_durable: bool| {
        tonic::Request::new(grpc::pb::QueryJsonRequest {
            body: body.to_string().into_bytes().into(),
            warm_only,
            require_writer: false,
            await_durable: await_durable.then_some(true),
        })
    };
    let insert = json!({
        "query": "INSERT INTO people (name) VALUES (?)",
        "parameters": ["Ada"],
        "mappings": {"tables": {"people": "Person"}},
    });

    // Explaining plans a write as a read request and writes nothing, over
    // HTTP and gRPC alike; both report the plan of the translated Cypher.
    // Plans compare without the planner's own timing.
    let untimed = |mut plan: serde_json::Value| {
        plan["planner"]
            .as_object_mut()
            .expect("plans report planner metrics")
            .remove("optimization_micros")
            .expect("plans time optimization");
        plan
    };
    let (status, http_plan) = explain(insert.clone(), &[("x-helix-warm", "true")]).await;
    assert_eq!(status, StatusCode::OK, "{http_plan}");
    assert_eq!(http_plan["effect"], "Write");
    let http_plan = untimed(http_plan);
    let grpc_plan = grpc
        .explain_sql(request(insert.clone(), true, false))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        untimed(serde_json::from_slice(&grpc_plan.body).unwrap()),
        http_plan
    );
    let cypher_plan = db
        .explain_cypher(
            serde_json::from_value(json!({
                "query": "CREATE (people:Person {name: $1})",
                "parameters": {"1": "Ada"},
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        untimed(serde_json::to_value(&cypher_plan).unwrap()),
        http_plan
    );
    let (status, error) = explain(insert.clone(), &[("x-helix-await-durable", "true")]).await;
    assert_eq!(
        (status, &error["error"]),
        (StatusCode::BAD_REQUEST, &json!("invalid_request_option"))
    );
    let (status, error) = explain(json!({"query": "SELECT ?"}), &[]).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error["details"]["detail"], "missing_parameter");
    let (status, error) = explain(json!({"query": "SELECT 1 UNION SELECT 2"}), &[]).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error["details"]["detail"], "union");
    assert_eq!(
        db.cypher(db::cypher::Request::new("MATCH (p:Person) RETURN count(p)"))
            .await
            .unwrap()
            .rows,
        vec![vec![json!(0)]]
    );

    // gRPC executes SQL with the request options of ExecuteCypher.
    let inserted = grpc
        .execute_sql(request(insert.clone(), false, true))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&inserted.body).unwrap(),
        json!({"columns": [], "rows": []})
    );
    let read = grpc
        .execute_sql(request(
            json!({"query": "SELECT p.name FROM Person p", "parameters": {}}),
            true,
            false,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&read.body).unwrap(),
        json!({"columns": ["p.name"], "rows": [["Ada"]]})
    );
    let warm_write = grpc
        .execute_sql(request(insert.clone(), true, false))
        .await
        .unwrap_err();
    assert_eq!(warm_write.code(), tonic::Code::InvalidArgument);
    for (body, code, metadata) in [
        (
            json!({"query": "SELECT 1", "mapping": {}}),
            tonic::Code::InvalidArgument,
            Some("invalid_query_json"),
        ),
        (
            json!({"query": "SELECT (SELECT 1)"}),
            tonic::Code::InvalidArgument,
            None,
        ),
    ] {
        for status in [
            grpc.execute_sql(request(body.clone(), false, false))
                .await
                .unwrap_err(),
            grpc.explain_sql(request(body.clone(), false, false))
                .await
                .unwrap_err(),
        ] {
            assert_eq!(status.code(), code);
            match metadata {
                Some(metadata) => assert_eq!(
                    status
                        .metadata()
                        .get(grpc::HELIX_ERROR_CODE_METADATA)
                        .unwrap(),
                    metadata
                ),
                None => {
                    let details: serde_json::Value =
                        serde_json::from_slice(status.details()).unwrap();
                    assert_eq!(details["details"]["detail"], "procedures_and_subqueries");
                }
            }
        }
    }
    assert_eq!(
        db.cypher(db::cypher::Request::new("MATCH (p:Person) RETURN count(p)"))
            .await
            .unwrap()
            .rows,
        vec![vec![json!(1)]]
    );
    drop(grpc);
    drop(router);
    db.close().await.unwrap();
}

/// Sends one write over HTTP, or over gRPC when given a service, setting
/// `await_durable` only when `flag` is present.
async fn write_route(
    router: axum::Router,
    grpc: Option<&grpc::GrpcService>,
    path: &str,
    body: Vec<u8>,
    flag: Option<bool>,
) {
    let Some(grpc) = grpc else {
        let mut request = Request::post(path).header("content-type", "application/json");
        if let Some(flag) = flag {
            request = request.header("x-helix-await-durable", flag.to_string());
        }
        let response = router
            .oneshot(request.body(Body::from(body)).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        return;
    };
    let request = tonic::Request::new(grpc::pb::QueryJsonRequest {
        body: body.into(),
        await_durable: flag,
        ..Default::default()
    });
    match path {
        "/v2/query" => grpc.execute_query(request).await,
        helix_cypher::api::HTTP_PATH => grpc.execute_cypher(request).await,
        _ => grpc.execute_sql(request).await,
    }
    .unwrap();
}

/// Every write route on both transports waits for the WAL upload unless the
/// client explicitly passes `false`. A client that never heard of the option,
/// including a gRPC client whose field stays unset, keeps a durable write.
#[tokio::test(flavor = "multi_thread")]
async fn only_an_explicit_false_skips_the_durable_wait() {
    let store = Arc::new(helix_db_testkit::gated_wal::GatedWalStore::default());
    let db = Arc::new(
        db::HelixDB::open_with_object_store("await-durable-routes", Arc::clone(&store) as _)
            .await
            .unwrap(),
    );
    let state = state::ServerState::new(Arc::clone(&db), None, crate::StorageConfig::Memory);
    let router = http::router(state.clone());
    let service = grpc::GrpcService::new(state);
    let native = serde_json::to_vec(&helix_ast::query::QueryRequest::write(
        helix_ast::batch::write_batch().var_as(
            "created",
            helix_ast::traversal::g().add_n(
                "Durable",
                vec![("source", helix_ast::value::PropertyInput::from("native"))],
            ),
        ),
    ))
    .unwrap();
    let cypher = json!({"query": "CREATE (:Durable)"})
        .to_string()
        .into_bytes();
    let sql = json!({
        "query": "INSERT INTO durable (name) VALUES (?)",
        "parameters": ["Ada"],
        "mappings": {"tables": {"durable": "Durable"}},
    })
    .to_string()
    .into_bytes();
    let bounded = std::time::Duration::from_secs(30);
    // Past SlateDB's 100 ms flush interval, so a pending write is held.
    let pending = std::time::Duration::from_millis(400);

    // A writer's first insert persists its ID lease durably in any mode.
    write_route(router.clone(), None, "/v2/query", native.clone(), None).await;

    for (path, body) in [
        ("/v2/query", native),
        (helix_cypher::api::HTTP_PATH, cypher),
        ("/v2/sql", sql),
    ] {
        for grpc in [None, Some(&service)] {
            store.hold();
            tokio::time::timeout(
                bounded,
                write_route(router.clone(), grpc, path, body.clone(), Some(false)),
            )
            .await
            .unwrap_or_else(|_| panic!("{path} must not wait when told not to"));
            for flag in [None, Some(true)] {
                let mut write =
                    Box::pin(write_route(router.clone(), grpc, path, body.clone(), flag));
                assert!(
                    tokio::time::timeout(pending, &mut write).await.is_err(),
                    "{path} {flag:?} must wait for its upload"
                );
                store.release();
                tokio::time::timeout(bounded, write).await.unwrap();
                store.hold();
            }
            store.release();
        }
    }
    drop(service);
    drop(router);
    db.close().await.unwrap();
}
