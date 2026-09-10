use super::super::*;
use helix_planner::context;
use serde_json::json;

#[tokio::test]
async fn downstream_limits_stop_total_prefixes_without_hiding_source_errors() {
    let db = crate::execution::interpreter::test_support::open_db("cypher-downstream-demand").await;
    for (query, expected) in [
        (
            "UNWIND range(1,1000000000) AS x WITH x AS a RETURN a LIMIT 1",
            json!([[1]]),
        ),
        (
            "UNWIND range(1,1000000000) AS x WITH x SKIP 5 WITH x SKIP 6 RETURN x SKIP 7 LIMIT 2",
            json!([[19], [20]]),
        ),
        (
            "UNWIND range(1,1000000000) AS x WITH x SKIP 999999999 RETURN x LIMIT 0",
            json!([]),
        ),
        ("MATCH (n:Absent) WITH n AS a RETURN a LIMIT 0", json!([])),
        (
            "OPTIONAL MATCH (n:Absent) WITH n AS a RETURN a LIMIT 1",
            json!([[null]]),
        ),
        (
            "UNWIND [1,0] AS x WITH x LIMIT 1 WITH 1/x AS y RETURN y",
            json!([[1]]),
        ),
    ] {
        let plan = r::plan(
            helix_cypher::compile(query).unwrap(),
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        assert!(plan.input_window(0).is_some(), "{query}");
        let interpreter = Interpreter::new(&db, context::ParamBindings::default());
        // The largest prefix consumes twenty source rows through three stages.
        // This bound permits their validation work and rejects draining the range.
        interpreter.ctx.fail_deadline_after(500);
        let response = interpreter
            .execute_rows(
                &plan,
                &BTreeMap::new(),
                Limits {
                    batch_rows: 2,
                    memory_bytes: 128 * 1024,
                    ..Default::default()
                },
            )
            .await
            .unwrap_or_else(|error| panic!("{query}: {error}"));
        assert_eq!(
            serde_json::to_value(response.rows).unwrap(),
            expected,
            "{query}"
        );
    }
    for query in [
        "UNWIND [1/0] AS x WITH x AS a RETURN a LIMIT 0",
        "UNWIND [1,2,0] AS x WITH 1/x AS a WITH a AS b RETURN b LIMIT 1",
        "UNWIND [1,0] AS x WITH x SKIP 1 LIMIT 1 WITH 1/x AS y RETURN y",
    ] {
        let plan = r::plan(
            helix_cypher::compile(query).unwrap(),
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        for strategy in [r::RowExecution::Batched, r::RowExecution::Materialized] {
            let error = Interpreter::new(&db, context::ParamBindings::default())
                .execute_rows(
                    &plan.clone().with_execution(strategy),
                    &BTreeMap::new(),
                    Limits {
                        batch_rows: 1,
                        ..Default::default()
                    },
                )
                .await
                .unwrap_err();
            assert!(
                matches!(error,Error::Query(ref error) if error.detail=="DivisionByZero"),
                "{query}: {error}"
            );
        }
    }
    for query in [
        "UNWIND range(1,31) AS x WITH x SKIP 2 WITH x SKIP 3 RETURN x SKIP 4 LIMIT 5",
        "UNWIND [3,1,2] AS x WITH x ORDER BY x RETURN x LIMIT 1",
        "UNWIND [3,1,2] AS x WITH x AS y WHERE y<3 RETURN y LIMIT 1",
        "UNWIND [1,1,2] AS x WITH x RETURN DISTINCT x LIMIT 2",
    ] {
        let plan = r::plan(
            helix_cypher::compile(query).unwrap(),
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        let reference = Interpreter::new(&db, context::ParamBindings::default())
            .execute_rows(
                &plan.clone().with_execution(r::RowExecution::Materialized),
                &BTreeMap::new(),
                Limits::default(),
            )
            .await
            .unwrap();
        for batch_rows in [1, 2, 7, 32] {
            let result = Interpreter::new(&db, context::ParamBindings::default())
                .execute_rows(
                    &plan,
                    &BTreeMap::new(),
                    Limits {
                        batch_rows,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            assert_eq!(result.rows, reference.rows, "{query}");
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn projection_chain_windows_validate_parameters_on_empty_and_nonempty_input() {
    let db = crate::execution::interpreter::test_support::open_db("cypher-chain-window-parameters")
        .await;
    for source in ["[]", "[1,2,3]"] {
        for window in ["SKIP", "LIMIT"] {
            let query =
                format!("UNWIND {source} AS x WITH x WITH x {window} $window RETURN count(*)");
            let plan = r::plan(
                helix_cypher::compile(&query).unwrap(),
                &db.planner_context(context::ParamBindings::default()),
            )
            .unwrap();
            assert!(matches!(
                plan.batch_consumer(0),
                Some(r::BatchConsumer::Pipeline { .. })
            ));
            for (value, detail) in [
                (r::Value::Integer(-1), "NegativeIntegerArgument"),
                (r::Value::Float(1.0), "InvalidArgumentType"),
                (r::Value::Null, "InvalidArgumentType"),
                (r::Value::Boolean(false), "InvalidArgumentType"),
                (r::Value::List(vec![]), "InvalidArgumentType"),
            ] {
                let parameters = BTreeMap::from([("window".into(), value)]);
                for strategy in [r::RowExecution::Batched, r::RowExecution::Materialized] {
                    let error = Interpreter::new(&db, context::ParamBindings::default())
                        .execute_rows(
                            &plan.clone().with_execution(strategy),
                            &parameters,
                            Limits {
                                batch_rows: 1,
                                ..Default::default()
                            },
                        )
                        .await
                        .unwrap_err();
                    assert!(
                        matches!(error,Error::Query(ref error) if error.detail==detail && error.phase==r::ErrorPhase::Runtime),
                        "{query}: {error}"
                    );
                }
            }
            let parameters = BTreeMap::from([("window".into(), r::Value::Integer(1))]);
            let expected = if source == "[]" {
                0
            } else if window == "SKIP" {
                2
            } else {
                1
            };
            for batch_rows in [1, 2, 8] {
                let response = Interpreter::new(&db, context::ParamBindings::default())
                    .execute_rows(
                        &plan,
                        &parameters,
                        Limits {
                            batch_rows,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(response.rows, vec![vec![json!(expected)]], "{query}");
            }
        }
    }
    let mut request = crate::cypher::Request::new("CREATE (:Marker) WITH 1 AS seed UNWIND [1,2,3] AS x WITH x WITH x LIMIT $window RETURN count(*)");
    request
        .parameters
        .insert("window".into(), helix_ast::query::QueryValue::I64(-1));
    let error = db.cypher(request).await.unwrap_err();
    assert!(matches!(error,Error::Query(ref error) if error.detail=="NegativeIntegerArgument"));
    assert_eq!(
        db.cypher(crate::cypher::Request::new(
            "MATCH (n:Marker) RETURN count(*)"
        ))
        .await
        .unwrap()
        .rows,
        vec![vec![json!(0)]]
    );
    db.close().await.unwrap();
}
