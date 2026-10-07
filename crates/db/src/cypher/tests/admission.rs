use super::*;
use crate::allocation_testing;
use query::QueryValue as Q;

#[test]
fn parameter_preflight_is_allocation_free_and_covers_construction() {
    let mut spare = Vec::with_capacity(4096);
    spare.push(Q::Null);
    let mut text = String::with_capacity(8192);
    text.push('p');
    let mut pruned: BTreeMap<_, _> = (0..256).map(|i| (format!("key{i}"), Q::I64(i))).collect();
    pruned.retain(|key, _| key == "key17");
    let mut nested = Q::String("p".repeat(4096));
    for _ in 0..20 {
        nested = Q::Object(BTreeMap::from([("child".into(), Q::Array(vec![nested]))]));
    }
    let cases = vec![
        Q::Null,
        Q::Bool(true),
        Q::I64(i64::MIN),
        Q::F64(f64::MAX),
        Q::F32(f32::NEG_INFINITY),
        Q::String(text),
        Q::Array(spare),
        Q::Array(vec![]),
        Q::Object(BTreeMap::new()),
        Q::Object(pruned),
        Q::Object((0..1000).map(|i| (format!("key{i}"), Q::I64(i))).collect()),
        nested,
    ];
    for value in cases {
        let mut name = String::with_capacity(4096);
        name.push('x');
        let input = BTreeMap::from([(name, value)]);
        let (footprint, observation) = allocation_testing::observe(|| preflight(&input).unwrap());
        assert_eq!(observation.allocations, 0);
        assert!(
            footprint.retained() >= 4096,
            "retained name capacity is part of the bound"
        );
        let (prepared, observation) =
            allocation_testing::observe(|| prepare(input, footprint.construction()).unwrap());
        assert!(
            observation.bytes <= footprint.construction(),
            "observed {} allocation bytes exceed {}",
            observation.bytes,
            footprint.construction()
        );
        assert_eq!(prepared.footprint.retained(), footprint.retained());
        assert_eq!(prepared.bindings.values.len(), 1);
        assert_eq!(prepared.bindings.query_values.len(), 1);
        assert_eq!(prepared.values.len(), 1);
        assert!(prepared.values.contains_key("x"));
    }
    let input = (0..1000).map(|i| (format!("key{i}"), Q::I64(i))).collect();
    let footprint = preflight(&input).unwrap();
    let (prepared, observation) =
        allocation_testing::observe(|| prepare(input, footprint.construction()).unwrap());
    assert!(observation.bytes <= footprint.construction());
    assert_eq!(prepared.values.len(), 1000);
    for i in 0..1000 {
        assert_eq!(prepared.values[&format!("key{i}")], r::Value::Integer(i));
    }
}

#[test]
fn admission_rejects_before_target_allocation_and_preserves_empty_and_numeric_values() {
    let input = BTreeMap::from([("x".into(), Q::String("p".repeat(16 * 1024)))]);
    let footprint = preflight(&input).unwrap();
    let (result, observation) =
        allocation_testing::observe(|| prepare(input, footprint.construction() - 1));
    assert!(
        matches!(result,Err(crate::cypher::Error::Query(error)) if error.detail=="MemoryLimit" && error.phase==r::ErrorPhase::Runtime)
    );
    assert!(
        observation.bytes < 512,
        "only the diagnostic may allocate after rejection"
    );
    let mut empty: BTreeMap<_, _> = (0..64).map(|i| (i.to_string(), Q::Null)).collect();
    while empty.pop_first().is_some() {}
    let (prepared, observation) = allocation_testing::observe(|| prepare(empty, 0).unwrap());
    assert_eq!(observation.allocations, 0);
    assert_eq!(prepared.footprint.retained(), 0);
    assert_eq!(prepared.footprint.construction(), 0);
    let input = BTreeMap::from([
        ("nan".into(), Q::F64(f64::NAN)),
        ("float".into(), Q::F32(1.5)),
        ("min".into(), Q::I64(i64::MIN)),
    ]);
    let prepared = prepare(input, 1024 * 1024).unwrap();
    assert!(matches!(prepared.values["nan"],r::Value::Float(value) if value.is_nan()));
    assert!(
        matches!(prepared.bindings.values["nan"],value::PropertyValue::F64(value) if value.is_nan())
    );
    assert!(matches!(prepared.bindings.query_values["nan"],Q::F64(value) if value.is_nan()));
    assert_eq!(prepared.values["float"], r::Value::Float(1.5));
    assert_eq!(prepared.values["min"], r::Value::Integer(i64::MIN));
}

#[test]
fn preflight_preserves_name_depth_and_node_limit_errors_without_a_wide_worklist() {
    let input = BTreeMap::from([(String::new(), Q::Null)]);
    let error = preflight(&input).unwrap_err();
    assert_eq!(
        (error.category.as_str(), error.detail.as_str(), error.phase),
        ("SyntaxError", "InvalidParameter", r::ErrorPhase::Compile)
    );
    let mut nested = Q::Null;
    for _ in 1..r::MAX_EXPRESSION_DEPTH {
        nested = Q::Array(vec![nested]);
    }
    let input = BTreeMap::from([("x".into(), nested)]);
    preflight(&input).unwrap();
    let input = BTreeMap::from([("x".into(), Q::Object(input))]);
    assert_eq!(preflight(&input).unwrap_err().detail, "ValueDepth");
    let input = BTreeMap::from([("x".into(), Q::Array(vec![Q::Null; 199_999]))]);
    let (result, observation) = allocation_testing::observe(|| preflight(&input));
    result.unwrap();
    assert_eq!(observation.allocations, 0);
    let input = BTreeMap::from([("x".into(), Q::Array(vec![Q::Null; 200_000]))]);
    let (result, observation) = allocation_testing::observe(|| preflight(&input));
    let error = result.unwrap_err();
    assert_eq!(
        (error.detail.as_str(), error.phase),
        ("ValueDepth", r::ErrorPhase::Compile)
    );
    assert!(
        observation.bytes < 512,
        "structural rejection must not allocate a wide worklist"
    );
}

#[tokio::test]
async fn service_reports_parameter_memory_and_rejects_before_opening_writes() {
    use crate::{cypher, HelixDB, HelixDbSource};
    let source = HelixDbSource::InMemory {
        database: "cypher-parameter-admission".into(),
    };
    let config = source
        .embedded_default_config()
        .with_query_telemetry(crate::config::QueryTelemetry::Disabled);
    let db = HelixDB::open_with_config(source, config).await.unwrap();
    let query = "RETURN 1 AS value";
    let baseline = db.cypher(cypher::Request::new(query)).await.unwrap();
    let mut request = cypher::Request::new(query);
    request.parameters = (0..256).map(|i| (format!("key{i}"), Q::I64(i))).collect();
    let footprint = preflight(&request.parameters).unwrap();
    let result = db.cypher(request).await.unwrap();
    assert_eq!(result.rows, baseline.rows);
    assert_eq!(
        result.resources.peak_memory_bytes,
        footprint
            .construction()
            .max(footprint.retained() + baseline.resources.peak_memory_bytes)
    );
    let mut request = cypher::Request::new("CREATE (:Rejected) RETURN 1");
    request.parameters = BTreeMap::from([("x".into(), Q::I64(1))]);
    let result = cypher::execute(
        &db,
        request,
        crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
        crate::query_service::QueryMode::Execute,
        crate::execution_control::ExecutionControl::unlimited(),
        cypher::Limits {
            memory_bytes: 512,
            ..Default::default()
        },
    )
    .await;
    assert!(matches!(result,Err(cypher::Error::Query(error)) if error.detail=="MemoryLimit"));
    assert_eq!(
        db.cypher(cypher::Request::new("MATCH (n:Rejected) RETURN count(*)"))
            .await
            .unwrap()
            .rows,
        vec![vec![serde_json::json!(0)]]
    );
    db.close().await.unwrap();
}
