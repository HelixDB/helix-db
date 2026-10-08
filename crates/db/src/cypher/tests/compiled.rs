use super::*;
use crate::encoding::v2::keys::scope;
use crate::execution_control::ExecutionControl;
use crate::query_service::{HelixQueryService, QueryMode};
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn compiled_execution_preserves_parameters_limits_cancellation_and_rollback() {
    let source = crate::HelixDbSource::InMemory {
        database: "compiled-request-boundaries".into(),
    };
    let config = source
        .embedded_default_config()
        .with_query_telemetry(crate::config::QueryTelemetry::Disabled);
    let db = Arc::new(
        crate::HelixDB::open_with_config(source, config)
            .await
            .unwrap(),
    );
    let service = HelixQueryService::new(Arc::clone(&db));
    let request: Request = serde_json::from_value(json!({
        "query":"RETURN $large AS large, $map AS map, null AS empty",
        "parameters":{
            "large":{"$type":"integer","value":"9223372036854775807"},
            "map":{"$type":"map","value":{"$type":"user","value":[1,true]}}
        }
    }))
    .unwrap();
    let expected = db.cypher(request.clone()).await.unwrap();
    let result = service
        .execute_compiled_cypher_json_scoped_controlled(
            request.compile().unwrap(),
            QueryMode::Execute,
            scope::DataScope::LegacyUnscoped,
            ExecutionControl::unlimited(),
            Limits::default(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(result.body()).unwrap(),
        serde_json::to_value(expected).unwrap()
    );

    for (text, mode, control, limits, expected) in [
        (
            "RETURN $missing",
            QueryMode::Execute,
            ExecutionControl::unlimited(),
            Limits::default(),
            "MissingParameter",
        ),
        (
            "CREATE (:N) RETURN 1/0",
            QueryMode::Execute,
            ExecutionControl::unlimited(),
            Limits::default(),
            "DivisionByZero",
        ),
        (
            "CREATE (:N)",
            QueryMode::Warm,
            ExecutionControl::unlimited(),
            Limits::default(),
            "WriterRequired",
        ),
        (
            "CREATE (:N)",
            QueryMode::Execute,
            ExecutionControl::from_timeout(std::time::Duration::ZERO),
            Limits::default(),
            "deadline",
        ),
        (
            "RETURN 1",
            QueryMode::Execute,
            ExecutionControl::unlimited(),
            Limits {
                batch_rows: 0,
                ..Default::default()
            },
            "InvalidLimits",
        ),
        (
            "RETURN 1",
            QueryMode::Execute,
            ExecutionControl::unlimited(),
            Limits {
                memory_bytes: 0,
                ..Default::default()
            },
            "InvalidLimits",
        ),
        (
            "RETURN 1",
            QueryMode::Execute,
            ExecutionControl::unlimited(),
            Limits {
                result_bytes: 0,
                ..Default::default()
            },
            "InvalidLimits",
        ),
        (
            "RETURN 1",
            QueryMode::Execute,
            ExecutionControl::unlimited(),
            Limits {
                collection_items: 0,
                ..Default::default()
            },
            "InvalidLimits",
        ),
    ] {
        let error = service
            .execute_compiled_cypher_json_scoped_controlled(
                Request::new(text).compile().unwrap(),
                mode,
                scope::DataScope::LegacyUnscoped,
                control,
                limits,
                None,
            )
            .await
            .unwrap_err();
        if expected == "deadline" {
            assert!(matches!(
                error,
                Error::Storage(crate::HelixDbError::QueryDeadlineExceeded)
            ));
        } else {
            let Error::Query(error) = error else {
                panic!("query error: {error:?}")
            };
            assert_eq!(error.detail, expected);
        }
    }
    assert_eq!(
        db.cypher(Request::new("MATCH (n:N) RETURN count(*)"))
            .await
            .unwrap()
            .rows,
        vec![vec![json!(0)]]
    );
    drop(service);
    db.close().await.unwrap();
}

#[tokio::test]
async fn compiled_requests_obtain_the_execution_catalog_and_tenant_scope() {
    let source = crate::HelixDbSource::InMemory {
        database: "compiled-request-authority".into(),
    };
    let config = source
        .embedded_default_config()
        .with_query_telemetry(crate::config::QueryTelemetry::Disabled);
    let db = Arc::new(
        crate::HelixDB::open_with_config(source, config)
            .await
            .unwrap(),
    );
    let service = HelixQueryService::new(Arc::clone(&db));
    let read = Request::new("MATCH (n:N {key:7}) RETURN n.key")
        .compile()
        .unwrap();
    db.install_index_for_tests(
        crate::config::SecondaryIndexDefinition::node_equality("N", "key")
            .unwrap()
            .try_into()
            .unwrap(),
    )
    .await
    .unwrap();
    db.cypher(Request::new("CREATE (:N {key:7})"))
        .await
        .unwrap();
    let result = service
        .execute_compiled_cypher_json_scoped_controlled(
            read,
            QueryMode::Execute,
            scope::DataScope::LegacyUnscoped,
            ExecutionControl::unlimited(),
            Limits::default(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(result.body()).unwrap()["rows"],
        json!([[7]])
    );
    assert_eq!(result.resources.reads.scans, 0);
    for (tenant, count) in [(1, 2), (2, 3)] {
        let scope = scope::DataScope::Tenant(scope::TenantId::from_u128(tenant));
        let create = Request::new(format!(
            "UNWIND range(1,{count}) AS i CREATE (:Scoped {{key:i}})"
        ))
        .compile()
        .unwrap();
        let read = Request::new("MATCH (n:Scoped) RETURN count(*)")
            .compile()
            .unwrap();
        service
            .execute_compiled_cypher_json_scoped_controlled(
                create,
                QueryMode::Execute,
                scope,
                ExecutionControl::unlimited(),
                Limits::default(),
                None,
            )
            .await
            .unwrap();
        let result = service
            .execute_compiled_cypher_json_scoped_controlled(
                read,
                QueryMode::Execute,
                scope,
                ExecutionControl::unlimited(),
                Limits::default(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(result.body()).unwrap()["rows"],
            json!([[count]])
        );
    }
    assert_eq!(
        db.cypher(Request::new("MATCH (n:Scoped) RETURN count(*)"))
            .await
            .unwrap()
            .rows,
        vec![vec![json!(0)]]
    );
    drop(service);
    db.close().await.unwrap();
}
