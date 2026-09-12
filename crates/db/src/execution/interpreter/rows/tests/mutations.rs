use super::super::*;
use helix_planner::context;
use serde_json::json;

#[tokio::test]
async fn created_path_payload_is_admitted_before_allocation_and_retained_with_rows() {
    let db = crate::execution::interpreter::test_support::open_db("create-path-admission").await;
    let mut ctx = ExecutionContext::new(&db, helix_planner::context::ParamBindings::default());
    let pattern = r::Pattern {
        nodes: vec![],
        relationships: vec![],
        paths: vec![r::PathPattern {
            slot: r::Slot(2),
            nodes: vec![r::Slot(0); 9],
            relationships: vec![r::Slot(1); 8],
        }],
    };
    let params = BTreeMap::new();
    // Supply already bound entities to isolate path construction from storage
    // allocations. Nine IDs also catch geometric Vec over-allocation.
    for (node, relationship, error) in [
        (
            r::Value::Entity(r::Entity::Node(5)),
            r::Value::Entity(r::Entity::Relationship(7)),
            None,
        ),
        (
            r::Value::Null,
            r::Value::Entity(r::Entity::Relationship(7)),
            Some("ExpectedNode"),
        ),
        (
            r::Value::Entity(r::Entity::Node(5)),
            r::Value::Null,
            Some("ExpectedRelationship"),
        ),
    ] {
        for short in [false, true] {
            let data = vec![vec![node.clone(), relationship.clone(), r::Value::Null]; 3];
            let bytes = rows_bytes(&data) + 3 * (9 + 8) * size_of::<u64>() - usize::from(short);
            ctx.row_memory = Some(memory::Budget::new(bytes));
            let rows = memory::Rows::new(data, ctx.row_budget()).unwrap();
            let result = ctx
                .create_rows(rows, &pattern, &params, Limits::default())
                .await;
            let expected = if short { Some("MemoryLimit") } else { error };
            match expected {
                Some(detail) => {
                    assert!(matches!(result, Err(Error::Query(error)) if error.detail == detail))
                }
                None => {
                    let mut rows = result.unwrap();
                    assert_eq!(ctx.row_budget().available(), 0);
                    rows.refresh().unwrap();
                    assert_eq!(ctx.row_budget().available(), 0);
                    for row in &rows {
                        let r::Value::Path(path) = &row[2] else {
                            panic!("expected constructed path")
                        };
                        assert_eq!(path.nodes(), &[5; 9]);
                        assert_eq!(path.relationships(), &[7; 8]);
                    }
                }
            }
            assert_eq!(ctx.row_budget().available(), bytes);
            assert_eq!(
                ctx.row_budget().reads(),
                crate::query_resources::StorageReadUsage::default()
            );
        }
    }
    ctx.row_memory = Some(memory::Budget::new(0));
    let empty = memory::Rows::new(vec![], ctx.row_budget()).unwrap();
    assert!(ctx
        .create_rows(empty, &pattern, &params, Limits::default())
        .await
        .unwrap()
        .is_empty());
    drop(ctx);
    db.close().await.unwrap();
}

#[tokio::test]
async fn delete_target_memory_failure_rolls_back_prior_writes_and_keeps_indexes() {
    let db = crate::execution::interpreter::test_support::open_db("delete-target-rollback").await;
    db.cypher(crate::cypher::Request::new(
        "UNWIND range(0,127) AS i CREATE (:Kept {key:i})",
    ))
    .await
    .unwrap();
    let mut limits = Limits {
        batch_rows: 1,
        ..Default::default()
    };
    // Calibrate from a complete no-op deletion, then prove that the identical
    // prefix succeeds under that limit. This controls for scan and hydration
    // memory without depending on the target set's internal allocation bound.
    for _ in 0..2 {
        let selected = crate::cypher::execute(
            &db,
            crate::cypher::Request::new(
                "CREATE (:Control) WITH 1 AS marker MATCH (n:Kept) DELETE null",
            ),
            crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
            crate::query_service::QueryMode::Execute,
            crate::execution_control::ExecutionControl::unlimited(),
            limits,
        )
        .await
        .unwrap();
        limits.memory_bytes = selected.resources.peak_memory_bytes + 1024;
        db.cypher(crate::cypher::Request::new("MATCH (n:Control) DELETE n"))
            .await
            .unwrap();
    }
    let error = crate::cypher::execute(
        &db,
        crate::cypher::Request::new("CREATE (:Transient) WITH 1 AS marker MATCH (n:Kept) DELETE n"),
        crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
        crate::query_service::QueryMode::Execute,
        crate::execution_control::ExecutionControl::unlimited(),
        limits,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, Error::Query(error) if error.category == "ResourceLimit" && error.detail == "MemoryLimit")
    );
    for (query, expected) in [
        ("MATCH (n:Transient) RETURN count(*)", 0),
        ("MATCH (n:Kept) RETURN count(*)", 128),
        ("MATCH (n:Kept {key:63}) RETURN count(*)", 1),
    ] {
        assert_eq!(
            db.cypher(crate::cypher::Request::new(query))
                .await
                .unwrap()
                .rows,
            vec![vec![json!(expected)]]
        );
    }
    // The same statement succeeds when admission is available, including its
    // explicit relationships-before-nodes deletion barrier and index updates.
    db.cypher(crate::cypher::Request::new("MATCH (n:Kept) DELETE n"))
        .await
        .unwrap();
    assert_eq!(
        db.cypher(crate::cypher::Request::new(
            "MATCH (n:Kept {key:63}) RETURN count(*)"
        ))
        .await
        .unwrap()
        .rows,
        vec![vec![json!(0)]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn cumulative_property_limits_roll_back_creates_and_updates() {
    let db = crate::execution::interpreter::test_support::open_db("property-map-rollback").await;
    db.cypher(crate::cypher::Request::new("CREATE (:Kept {key:1})"))
        .await
        .unwrap();
    for query in [
        "CREATE (:Marker) WITH 1 AS x CREATE (:Bad {a:$payload,b:$payload})",
        "CREATE (:Marker) WITH 1 AS x CREATE (:Bad)-[:R {a:$payload,b:$payload}]->(:Bad)",
        "MATCH (n:Kept) SET n.key=9 SET n += {a:$payload,b:$payload}",
        "MATCH (n:Kept) SET n.key=9 SET n = {a:$payload,b:$payload}",
    ] {
        let mut request = crate::cypher::Request::new(query);
        request.parameters.insert(
            "payload".into(),
            helix_ast::query::QueryValue::String("x".repeat(12 * 1024)),
        );
        let error = crate::cypher::execute(
            &db,
            request,
            crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
            crate::query_service::QueryMode::Execute,
            crate::execution_control::ExecutionControl::unlimited(),
            Limits {
                memory_bytes: 48 * 1024,
                batch_rows: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert!(
            matches!(error, Error::Query(error) if error.category == "ResourceLimit" && error.detail == "MemoryLimit"),
            "{query}"
        );
        assert_eq!(
            db.cypher(crate::cypher::Request::new(
                "MATCH (n) RETURN labels(n),properties(n)"
            ))
            .await
            .unwrap()
            .rows,
            vec![vec![json!(["Kept"]), json!({"key":1})]],
            "{query}"
        );
        assert_eq!(
            db.cypher(crate::cypher::Request::new(
                "MATCH (n:Kept {key:1}) RETURN count(*)"
            ))
            .await
            .unwrap()
            .rows,
            vec![vec![json!(1)]]
        );
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn named_create_paths_relationship_updates_and_delete_paths_preserve_atomicity() {
    let db =
        crate::execution::interpreter::test_support::open_db("cypher-path-crud-boundaries").await;
    let result = db
        .cypher(crate::cypher::Request::new(
            "CREATE p=(a:N {key:1})<-[r:R {old:1,keep:2}]-(b:N {key:2}) RETURN p",
        ))
        .await
        .unwrap();
    let path = &result.rows[0][0];
    assert_eq!(path["nodes"][0]["properties"]["key"], 1);
    assert_eq!(path["nodes"][1]["properties"]["key"], 2);
    assert_eq!(path["relationships"][0]["start"], path["nodes"][1]["id"]);
    assert_eq!(path["relationships"][0]["end"], path["nodes"][0]["id"]);
    let result=db.cypher(crate::cypher::Request::new(
        "MATCH ()-[r:R]->(a) SET r={x:3,keep:4} SET r += {keep:null,y:5} REMOVE r.x RETURN properties(r)",
    )).await.unwrap();
    assert_eq!(result.rows, vec![vec![json!({"y":5})]]);
    let result = db
        .cypher(crate::cypher::Request::new(
            "MATCH (a:N {key:1}),(b:N {key:2}) SET a=b RETURN properties(a)",
        ))
        .await
        .unwrap();
    assert_eq!(result.rows, vec![vec![json!({"key":2})]]);
    let result = db
        .cypher(crate::cypher::Request::new(
            "MATCH ()-[r:R]->() SET r=null RETURN properties(r)",
        ))
        .await
        .unwrap();
    assert_eq!(result.rows, vec![vec![json!({})]]);
    db.cypher(crate::cypher::Request::new("MATCH p=()-[:R]->() DELETE p"))
        .await
        .unwrap();
    let result = db
        .cypher(crate::cypher::Request::new("MATCH (n) RETURN count(*)"))
        .await
        .unwrap();
    assert_eq!(result.rows, vec![vec![json!(0)]]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn dynamic_mutation_types_and_reserved_keys_roll_back_prior_updates() {
    let db = crate::execution::interpreter::test_support::open_db("cypher-dynamic-mutations").await;
    db.cypher(crate::cypher::Request::new("CREATE (:N {key:1})"))
        .await
        .unwrap();
    for (query, parameter, detail) in [
        (
            "MATCH (n:N) SET n.key=9 SET n=$value",
            json!(false),
            "ExpectedMap",
        ),
        (
            "MATCH (n:N) SET n.key=9 SET n += $value",
            json!({"$label":"Bad"}),
            "ReservedPropertyName",
        ),
        (
            "MATCH (n:N) SET n.key=9 SET n += $value",
            json!({"":7}),
            "EmptyPropertyName",
        ),
        (
            "MATCH (n:N) SET n.key=9 SET n.x=$value",
            json!([1, "two"]),
            "InvalidPropertyType",
        ),
        (
            "MATCH (n:N) SET n.key=9 SET n.x=$value",
            json!([null]),
            "InvalidPropertyType",
        ),
        (
            "MATCH (n:N) SET n.key=9 SET n.x=$value",
            json!({"nested":1}),
            "InvalidPropertyType",
        ),
        (
            "MATCH (n:N) SET n.key=9 WITH $value AS x DELETE x",
            json!(1),
            "InvalidArgumentType",
        ),
        (
            "MATCH (n:N) SET n.key=9 WITH $value AS x SET x.y=1",
            json!(1),
            "ExpectedEntity",
        ),
        (
            "MATCH (n:N) SET n.key=9 WITH $value AS x CREATE (x)-[:R]->(:N)",
            json!(null),
            "ExpectedNode",
        ),
        (
            "MATCH (n:N) SET n.key=9 WITH $value AS x CREATE (:N)-[:R]->(x)",
            json!(null),
            "ExpectedNode",
        ),
    ] {
        let mut request = crate::cypher::Request::new(query);
        request
            .parameters
            .insert("value".into(), serde_json::from_value(parameter).unwrap());
        let error = db.cypher(request).await.unwrap_err();
        assert!(
            matches!(&error,Error::Query(error) if error.detail==detail),
            "{query}: {error:?}"
        );
        let response = db
            .cypher(crate::cypher::Request::new(
                "MATCH (n:N) RETURN properties(n)",
            ))
            .await
            .unwrap();
        assert_eq!(response.rows, vec![vec![json!({"key":1})]], "{query}");
    }
    let response = db
        .cypher(crate::cypher::Request::new(
            "OPTIONAL MATCH (n:Absent) SET n += {x:1} REMOVE n.x DELETE n RETURN n",
        ))
        .await
        .unwrap();
    assert_eq!(response.rows, vec![vec![json!(null)]]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn late_bound_match_type_errors_roll_back_instead_of_silently_dropping_rows() {
    let db =
        crate::execution::interpreter::test_support::open_db("cypher-bound-pattern-types").await;
    for (pattern, detail) in [
        ("(x)", "ExpectedNode"),
        ("()-[x]->()", "ExpectedRelationship"),
    ] {
        let mut request = crate::cypher::Request::new(format!(
            "CREATE (:Marker) WITH $value AS x MATCH {pattern} RETURN x"
        ));
        request
            .parameters
            .insert("value".into(), helix_ast::query::QueryValue::I64(7));
        let error = db.cypher(request).await.unwrap_err();
        assert!(
            matches!(error,Error::Query(ref error) if error.detail==detail),
            "{error:?}"
        );
        assert_eq!(
            db.cypher(crate::cypher::Request::new(
                "MATCH (n:Marker) RETURN count(*)"
            ))
            .await
            .unwrap()
            .rows,
            vec![vec![json!(0)]]
        );
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn match_label_access_errors_propagate_and_roll_back_deletion() {
    let db = crate::execution::interpreter::test_support::open_db_with_config(
        crate::execution::interpreter::test_support::in_memory_config("deleted-label-constraint")
            .with_equality_index("N", "key"),
    )
    .await;
    db.cypher(crate::cypher::Request::new("CREATE (:N {key:7})"))
        .await
        .unwrap();
    for text in [
        "MATCH (n:N) DELETE n WITH n MATCH (n:N) RETURN count(*)",
        "MATCH (n:N) DELETE n WITH n OPTIONAL MATCH (n:N) RETURN count(*)",
        "MATCH (n:N) DELETE n WITH n UNWIND [n,n] AS m OPTIONAL MATCH (m:N) RETURN count(*)",
    ] {
        let plan = r::plan(
            helix_cypher::compile(text).unwrap(),
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        for strategy in [r::RowExecution::Batched, r::RowExecution::Materialized] {
            let error = Interpreter::new(&db, context::ParamBindings::default())
                .execute_rows(
                    &plan.clone().with_execution(strategy),
                    &BTreeMap::new(),
                    Limits {
                        batch_rows: 2,
                        ..Default::default()
                    },
                )
                .await
                .unwrap_err();
            assert!(
                matches!(error,Error::Query(error) if error.category=="EntityNotFound" && error.detail=="DeletedEntityAccess"),
                "{text}"
            );
            assert_eq!(
                db.cypher(crate::cypher::Request::new(
                    "MATCH (n:N {key:7}) RETURN count(*)"
                ))
                .await
                .unwrap()
                .rows,
                vec![vec![json!(1)]]
            );
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn deleted_relationship_types_survive_but_properties_fail_and_roll_back() {
    let db =
        crate::execution::interpreter::test_support::open_db("deleted-relationship-values").await;
    for deletion in [
        "DELETE r",
        "DELETE r DELETE r",
        "DETACH DELETE a",
        "DELETE p",
    ] {
        let result = db.cypher(crate::cypher::Request::new(format!(
            "CREATE p=(a:N)-[r:R {{key:1}}]->(b:N) {deletion} RETURN type(r),type(head(relationships(p)))",
        ))).await.unwrap();
        assert_eq!(
            result.rows,
            vec![vec![json!("R"), json!("R")]],
            "{deletion}"
        );
        db.cypher(crate::cypher::Request::new("MATCH (n) DETACH DELETE n"))
            .await
            .unwrap();
    }
    db.cypher(crate::cypher::Request::new(
        "CREATE (:N)-[:R {key:1}]->(:N)",
    ))
    .await
    .unwrap();
    for expression in ["r.key", "properties(r)", "keys(r)"] {
        let error = db
            .cypher(crate::cypher::Request::new(format!(
                "MATCH (a)-[r:R]->(b) DELETE r RETURN type(r),{expression}",
            )))
            .await
            .unwrap_err();
        assert!(
            matches!(error,Error::Query(error) if error.category=="EntityNotFound" && error.detail=="DeletedEntityAccess")
        );
        assert_eq!(
            db.cypher(crate::cypher::Request::new(
                "MATCH ()-[r:R]->() RETURN count(*)"
            ))
            .await
            .unwrap()
            .rows,
            vec![vec![json!(1)]]
        );
    }
    let result = db
        .cypher(crate::cypher::Request::new(
            "MATCH ()-[r:R]->() DELETE r RETURN CASE WHEN type(r)='R' THEN 1 ELSE r.key END",
        ))
        .await
        .unwrap();
    assert_eq!(result.rows, vec![vec![json!(1)]]);
    db.close().await.unwrap();
}
