use super::super::*;
use crate::encoding::v2::values::property::property_value::PropertyValue as P;
use r::GraphValues;
use serde_json::json;

#[test]
fn response_admission_accounts_for_repeated_graph_values_in_paths() {
    let payload = "x".repeat(2048);
    let mut graph = GraphBatch::default();
    graph.entities.insert(
        r::Entity::Node(1),
        graph::EntityData {
            kind: graph::EntityKind::Node {
                label: Some("N".into()),
            },
            properties: BTreeMap::from([("payload".into(), Ok(r::Value::String(payload)))]),
        },
    );
    graph.entities.insert(
        r::Entity::Relationship(2),
        graph::EntityData {
            kind: graph::EntityKind::Relationship {
                label: "R".into(),
                endpoints: (1, 1),
            },
            properties: BTreeMap::new(),
        },
    );
    let path = r::Value::Path(r::Path::new(vec![1; 100], vec![2; 99]).unwrap());
    assert!(path.allocated_bytes() < 4096);
    assert!(graph.wire_memory(&path).unwrap() > 2048 * 100);
    assert!(memory::Budget::new(128 * 1024)
        .reserve(graph.wire_memory(&path).unwrap())
        .is_err());
    let nested = r::Value::Map(BTreeMap::from([
        ("$type".into(), r::Value::String("user map".into())),
        (
            "values".into(),
            r::Value::List(vec![
                r::Value::Integer(i64::MAX),
                r::Value::Float(f64::NAN),
                r::Value::String("text".into()),
                r::Value::Boolean(true),
                r::Value::Null,
            ]),
        ),
    ]));
    for value in [path, nested, r::Value::Float(1.5), r::Value::Integer(7)] {
        let estimate = graph.wire_memory(&value).unwrap();
        let wire = graph.wire(&value).unwrap();
        assert!(estimate >= json_bytes(&wire));
    }
    assert!(graph
        .wire_memory(&r::Value::Entity(r::Entity::Node(3)))
        .is_err());
}

#[tokio::test]
async fn raw_read_memory_failure_after_create_is_classified_and_rolls_back() {
    let db = crate::execution::interpreter::test_support::open_db("read-limit-rollback").await;
    db.cypher(crate::cypher::Request {
        parameters: BTreeMap::from([(
            "payload".into(),
            helix_ast::query::QueryValue::String("x".repeat(32 * 1024)),
        )]),
        ..crate::cypher::Request::new("CREATE (:Payload {data:$payload})")
    })
    .await
    .unwrap();
    let error = crate::cypher::execute(
        &db,
        crate::cypher::Request::new("CREATE (:Transient) WITH 1 AS x MATCH (n:Payload) RETURN n"),
        crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
        crate::query_service::QueryMode::Execute,
        crate::execution_control::ExecutionControl::unlimited(),
        Limits {
            memory_bytes: 16 * 1024,
            batch_rows: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, Error::Query(error) if error.category == "ResourceLimit" && error.detail == "MemoryLimit" && error.phase == r::ErrorPhase::Runtime)
    );
    let after = db
        .cypher(crate::cypher::Request::new(
            "MATCH (n:Transient) RETURN count(*)",
        ))
        .await
        .unwrap();
    assert_eq!(after.rows, vec![vec![json!(0)]]);
    let retained = db
        .cypher(crate::cypher::Request::new(
            "MATCH (n:Payload) RETURN size(n.data)",
        ))
        .await
        .unwrap();
    assert_eq!(retained.rows, vec![vec![json!(32 * 1024)]]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn native_storage_reads_retain_and_release_query_memory_on_every_result_path() {
    use crate::encoding::v2::keys;
    let db = crate::execution::interpreter::test_support::open_db("raw-read-admission").await;
    let response = db
        .cypher(crate::cypher::Request {
            parameters: BTreeMap::from([(
                "payload".into(),
                helix_ast::query::QueryValue::String("x".repeat(2048)),
            )]),
            ..crate::cypher::Request::new(
                "CREATE (a:N {payload:$payload}),(b:N {payload:$payload}) RETURN a,b",
            )
        })
        .await
        .unwrap();
    let ids = response.rows[0]
        .iter()
        .map(|node| node["id"].as_str().unwrap().parse::<u64>().unwrap())
        .collect::<Vec<_>>();
    let mut context = ExecutionContext::new(&db, helix_planner::context::ParamBindings::default());
    context.enable_request_read_view().await.unwrap();
    context.row_memory = Some(memory::Budget::new(4096));
    let keys = ids
        .iter()
        .map(|id| {
            context.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
                *id,
            )))
        })
        .collect::<Vec<_>>();
    let bytes = context.get_raw(&keys[0]).await.unwrap().unwrap();
    let available = context.row_budget().available();
    assert!(available < 4096 - 2048);
    let cloned = bytes.clone();
    let error = context.get_raw(&keys[0]).await.unwrap_err();
    assert!(matches!(
        error,
        crate::HelixDbError::QueryMemoryLimitExceeded
    ));
    assert_eq!(context.row_budget().available(), available);
    drop(bytes);
    assert_eq!(context.row_budget().available(), available);
    drop(cloned);
    assert_eq!(context.row_budget().available(), 4096);

    // A partially admitted multi-get must release its earlier successful value
    // when a later value cannot be retained.
    assert!(matches!(
        context.multi_get_raw(&keys).await.unwrap_err(),
        crate::HelixDbError::QueryMemoryLimitExceeded
    ));
    assert_eq!(context.row_budget().available(), 4096);
    let missing = context.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
        u64::MAX,
    )));
    assert!(context.get_raw(&missing).await.unwrap().is_none());
    let values = context
        .multi_get_raw(&[keys[0].clone(), missing])
        .await
        .unwrap();
    assert!(values[0].is_some() && values[1].is_none());
    assert!(context.row_budget().available() < 4096 - 2048);
    drop(values);
    assert_eq!(context.row_budget().available(), 4096);

    let prefix = bytes::Bytes::from(vec![keys::KeyPrefix::NodeProperty.as_u8()]);
    assert!(matches!(
        context.scan_raw_prefix(prefix.clone()).await.unwrap_err(),
        crate::HelixDbError::QueryMemoryLimitExceeded
    ));
    assert_eq!(context.row_budget().available(), 4096);
    let values = context
        .scan_raw_prefix_limited(prefix, Some(1))
        .await
        .unwrap();
    assert_eq!(values.len(), 1);
    assert!(context.row_budget().available() < 4096 - 2048);
    drop(values);
    assert_eq!(context.row_budget().available(), 4096);

    context.row_memory = Some(memory::Budget::new(1));
    assert!(matches!(
        context.get_raw(&keys[0]).await.unwrap_err(),
        crate::HelixDbError::QueryMemoryLimitExceeded
    ));
    assert!(matches!(
        context.multi_get_raw(&keys).await.unwrap_err(),
        crate::HelixDbError::QueryMemoryLimitExceeded
    ));
    assert_eq!(context.row_budget().available(), 1);
    assert_eq!(
        context.row_budget().reads(),
        crate::cypher::StorageReadUsage::default()
    );
    context.close_request_read_view().unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn graph_demand_admission_precedes_storage_reads_and_deduplicates_references() {
    let db = crate::execution::interpreter::test_support::open_db("graph-demand-admission").await;
    let mut context = ExecutionContext::new(&db, helix_planner::context::ParamBindings::default());
    context.enable_request_read_view().await.unwrap();
    // One sparse tree node is larger than the old per-entry estimate. This
    // admits one distinct reference but still rejects 100 before any I/O.
    context.row_memory = Some(memory::Budget::new(4096));
    let demand = BTreeMap::from([(r::Slot(0), r::PropertyDemand::All)]);
    let rows = vec![vec![r::Value::List(
        (0..100)
            .map(|id| r::Value::Entity(r::Entity::Node(id)))
            .collect(),
    )]];
    let error = context
        .graph_batch_required(&rows, &demand)
        .await
        .err()
        .unwrap();
    assert!(matches!(error, Error::Query(error) if error.detail=="MemoryLimit"));
    assert_eq!(
        context.row_budget().reads(),
        crate::cypher::StorageReadUsage::default()
    );
    // Repeated references consume one demand entry, including nested lists/maps.
    let value = r::Value::Map(BTreeMap::from([(
        "nodes".into(),
        r::Value::List(
            (0..100)
                .map(|_| r::Value::Entity(r::Entity::Node(7)))
                .collect(),
        ),
    )]));
    assert!(context
        .graph_batch_required(&[vec![value]], &demand)
        .await
        .unwrap()
        .entities
        .is_empty());
    assert_eq!(context.row_budget().reads().multi_get_keys, 1);
    assert_eq!(context.row_budget().available(), 4096);
    context.close_request_read_view().unwrap();
    db.close().await.unwrap();
}

#[test]
fn stored_value_conversion_preserves_widths_and_rejects_unstorable_composites() {
    for length in [1, 7, 8, 9, 4095, 4096, 4097] {
        let input = (0..length)
            .map(|i| r::Value::Boolean(i % 3 == 0))
            .collect::<Vec<_>>();
        let bound = input.capacity() * size_of::<r::Value>();
        let P::Array(values) = graph::to_property(r::Value::List(input)).unwrap() else {
            panic!("boolean array storage form")
        };
        assert!(values.capacity() * size_of::<P>() <= bound);
        assert!(values
            .iter()
            .enumerate()
            .all(|(i, value)| *value == P::Bool(i % 3 == 0)));
    }
    for (stored, expected) in [
        (P::Null, r::Value::Null),
        (P::F32(1.5), r::Value::Float(1.5)),
        (
            P::Object(BTreeMap::from([("x".into(), P::I64(i64::MAX))])),
            r::Value::Map(BTreeMap::from([("x".into(), r::Value::Integer(i64::MAX))])),
        ),
        (
            P::Array(vec![P::Bool(true)]),
            r::Value::List(vec![r::Value::Boolean(true)]),
        ),
        (
            P::I64Array(vec![i64::MIN, i64::MAX]),
            r::Value::List(vec![
                r::Value::Integer(i64::MIN),
                r::Value::Integer(i64::MAX),
            ]),
        ),
        (
            P::F64Array(vec![1.5]),
            r::Value::List(vec![r::Value::Float(1.5)]),
        ),
        (
            P::F32Array(vec![1.5]),
            r::Value::List(vec![r::Value::Float(1.5)]),
        ),
        (
            P::StringArray(vec!["x".into()]),
            r::Value::List(vec![r::Value::String("x".into())]),
        ),
    ] {
        assert_eq!(
            property_conversion::Conversion::new(stored)
                .finish()
                .unwrap(),
            expected
        );
    }
    for value in [
        r::Value::Null,
        r::Value::Boolean(true),
        r::Value::Integer(i64::MAX),
        r::Value::Float(1.5),
        r::Value::String("x".into()),
        r::Value::List(vec![r::Value::Boolean(true), r::Value::Boolean(false)]),
        r::Value::List(vec![]),
        r::Value::List(vec![
            r::Value::Integer(i64::MIN),
            r::Value::Integer(i64::MAX),
        ]),
        r::Value::List(vec![r::Value::Float(1.5), r::Value::Float(f64::INFINITY)]),
        r::Value::List(vec![
            r::Value::String("a".into()),
            r::Value::String("b".into()),
        ]),
    ] {
        assert_eq!(
            property_conversion::Conversion::new(graph::to_property(value.clone()).unwrap())
                .finish()
                .unwrap(),
            value
        );
    }
    for value in [
        r::Value::List(vec![r::Value::Null]),
        r::Value::List(vec![r::Value::Integer(1), r::Value::Float(1.0)]),
        r::Value::List(vec![r::Value::List(vec![])]),
        r::Value::Map(BTreeMap::new()),
        r::Value::Entity(r::Entity::Node(1)),
        r::Value::Path(r::Path::new(vec![1], vec![]).unwrap()),
    ] {
        assert!(
            matches!(graph::to_property(value),Err(Error::Query(error)) if error.detail=="InvalidPropertyType")
        );
    }
    assert!(graph::properties(BTreeMap::from([(
        "$label".into(),
        r::Value::String("wrong".into())
    )]))
    .is_err());
    for name in ["", "$label", "$custom"] {
        assert!(graph::properties(BTreeMap::from([(name.into(), r::Value::Null)])).is_err());
    }
    let graph = GraphBatch::default();
    for (value, expected) in [
        (f64::INFINITY, "Infinity"),
        (f64::NEG_INFINITY, "-Infinity"),
        (f64::NAN, "NaN"),
    ] {
        assert_eq!(
            graph.wire(&r::Value::Float(value)).unwrap(),
            json!({"$type":"float","value":expected})
        );
    }
    assert!(graph.properties(r::Entity::Node(1)).is_err());
    assert!(graph.property(r::Entity::Node(1), "x").is_err());
    assert!(graph.keys(r::Entity::Node(1)).is_err());
    assert!(graph.label(r::Entity::Node(1)).is_err());
    assert!(graph.wire(&r::Value::Entity(r::Entity::Node(1))).is_err());
    assert!(graph
        .wire(&r::Value::List(vec![r::Value::Entity(r::Entity::Node(1))]))
        .is_err());
    assert!(graph
        .wire(&r::Value::Map(BTreeMap::from([(
            "x".into(),
            r::Value::Entity(r::Entity::Node(1))
        )])))
        .is_err());
}
