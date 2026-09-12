use super::*;
use crate::execution::interpreter::Interpreter;

#[tokio::test]
async fn planner_to_execution_transfers_both_parameter_encodings_without_allocating() {
    let source = crate::HelixDbSource::InMemory {
        database: "cypher-parameter-handoff".into(),
    };
    let config = source
        .embedded_default_config()
        .with_query_telemetry(crate::config::QueryTelemetry::Disabled);
    let db = HelixDB::open_with_config(source, config).await.unwrap();
    db.install_index_for_tests(
        crate::config::SecondaryIndexDefinition::node_equality("N", "key")
            .unwrap()
            .try_into()
            .unwrap(),
    )
    .await
    .unwrap();
    db.cypher(Request::new("CREATE (:N {key:'marker'})"))
        .await
        .unwrap();
    let payload = "p".repeat(16 * 1024);
    let request: Request = serde_json::from_value(serde_json::json!({
        "query":"MATCH (n:N {key:$key}) RETURN n.key AS key,$payload AS payload",
        "parameters":{"key":"marker","payload":payload}
    }))
    .unwrap();
    let PreparedRequest {
        query,
        params,
        values,
        ..
    } = prepare_request(request, Limits::default()).unwrap();
    let key = ir::NonEmptyString::new("payload").unwrap();
    let helix_ast::value::PropertyValue::String(native) = &params.values[&key] else {
        panic!("native string")
    };
    let helix_ast::query::QueryValue::String(query_value) = &params.query_values[&key] else {
        panic!("query string")
    };
    let addresses = (native.as_ptr().addr(), query_value.as_ptr().addr());
    let prepared = db
        .planner_context_scoped_prepared(params, DataScope::LegacyUnscoped)
        .await
        .unwrap();
    let plan = r::plan(query, prepared.context()).unwrap();
    let ((params, proof), allocation) =
        crate::allocation_testing::observe(|| prepared.into_execution_inputs());
    assert_eq!(allocation.allocations, 0);
    let helix_ast::value::PropertyValue::String(native) = &params.values[&key] else {
        panic!("native string")
    };
    let helix_ast::query::QueryValue::String(query_value) = &params.query_values[&key] else {
        panic!("query string")
    };
    assert_eq!(
        (native.as_ptr().addr(), query_value.as_ptr().addr()),
        addresses
    );
    let result = Interpreter::new_scoped_controlled_prepared(
        &db,
        params,
        DataScope::LegacyUnscoped,
        ExecutionControl::unlimited(),
        proof,
    )
    .execute_rows(&plan, &values, Limits::default())
    .await
    .unwrap();
    assert_eq!(result.columns, ["key", "payload"]);
    assert_eq!(
        result.rows,
        vec![vec![
            serde_json::json!("marker"),
            serde_json::json!(payload)
        ]]
    );
    assert_eq!(result.resources.reads.scans, 0);
    db.close().await.unwrap();
}
