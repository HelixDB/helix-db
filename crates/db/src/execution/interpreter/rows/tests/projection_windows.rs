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
        (
            "UNWIND range(1,1000000000) AS x WITH x LIMIT 1000000 WITH x SKIP 5 LIMIT 200 RETURN x SKIP 6 LIMIT 2",
            json!([[12], [13]]),
        ),
        (
            "UNWIND range(1,1000000000) AS x WITH x LIMIT 1 RETURN x SKIP 2 LIMIT 2",
            json!([]),
        ),
        (
            "UNWIND range(1,1000000000) AS x WITH x LIMIT 0 RETURN x LIMIT 1",
            json!([]),
        ),
        (
            "UNWIND range(1,1000000000) AS x WITH x LIMIT 1000000 WITH x LIMIT 2 UNWIND [x,x] AS y RETURN sum(y)",
            json!([[6]]),
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
        "UNWIND [1/0] AS x WITH x LIMIT 1 RETURN x LIMIT 0",
        "UNWIND [1,2,0] AS x WITH x LIMIT 3 WITH 1/x AS y RETURN y LIMIT 1",
        "UNWIND [1,2,0] AS x WITH x LIMIT 3 WITH x AS y WHERE 1/y>0 RETURN y LIMIT 1",
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

#[tokio::test]
async fn immutable_match_constraints_bound_work_after_the_first_complete_match() {
    let db = crate::execution::interpreter::test_support::open_db("cypher-parameter-window").await;
    db.cypher(crate::cypher::Request::new(
        "UNWIND range(0,11) AS k CREATE (:N {k:k})",
    ))
    .await
    .unwrap();
    db.cypher(crate::cypher::Request::new(
        "MATCH(a:N),(b:N) CREATE(a)-[:R {k:1}]->(b)",
    ))
    .await
    .unwrap();
    db.install_index_for_tests(
        crate::config::SecondaryIndexDefinition::node_equality("N", "k")
            .unwrap()
            .try_into()
            .unwrap(),
    )
    .await
    .unwrap();
    for constraint in ["0", "$key"] {
        for limit in [0, 1, 2] {
            let query=format!("MATCH (a:N {{k:{constraint}}})-[:R]->(b)-[:R]->(c)-[:R]->(d) RETURN d LIMIT {limit}");
            let mut request = crate::cypher::Request::new(&query);
            request
                .parameters
                .insert("key".into(), helix_ast::query::QueryValue::I64(0));
            let explanation = db.explain_cypher(request.clone()).await.unwrap();
            assert_eq!(
                explanation.plan().input_window(0).unwrap().termination(),
                r::Termination::AfterFirstBatch
            );
            let result = db.cypher(request).await.unwrap();
            assert_eq!(result.rows.len(), limit);
            assert!(
                result.resources.reads.point_gets < 100,
                "{query}: {:?}",
                result.resources.reads
            );
            assert!(
                result.resources.reads.multi_get_keys < 256,
                "{query}: {:?}",
                result.resources.reads
            );
        }
    }
    for query in [
        "MATCH(n:N {k:$missing}) RETURN n LIMIT 0",
        "MATCH(n:N)-[:R {k:$missing}]->(m) RETURN m LIMIT 0",
        "MATCH(n:N {k:1/0}) RETURN n LIMIT 0",
        "MATCH(n:N)-[:R {k:1/0}]->(m) RETURN m LIMIT 1",
    ] {
        let plan = r::plan(
            helix_cypher::compile(query).unwrap(),
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        let expected = if query.contains("$missing") {
            "MissingParameter"
        } else {
            "DivisionByZero"
        };
        for strategy in [r::RowExecution::Batched, r::RowExecution::Materialized] {
            let error = Interpreter::new(&db, context::ParamBindings::default())
                .execute_rows(
                    &plan.clone().with_execution(strategy),
                    &BTreeMap::new(),
                    Limits {
                        batch_rows: 1,
                        ..Limits::default()
                    },
                )
                .await
                .unwrap_err();
            assert!(
                matches!(error,Error::Query(ref error) if error.detail==expected),
                "{query}: {error}"
            );
        }
    }
    for query in [
        "MATCH(n:Absent {k:$missing}) RETURN n LIMIT 0",
        "OPTIONAL MATCH(n:Absent {k:$missing}) RETURN n LIMIT 0",
        "OPTIONAL MATCH(n:Absent {k:$missing}) RETURN n LIMIT 1",
    ] {
        let plan = r::plan(
            helix_cypher::compile(query).unwrap(),
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        let expected = Interpreter::new(&db, context::ParamBindings::default())
            .execute_rows(
                &plan.clone().with_execution(r::RowExecution::Materialized),
                &BTreeMap::new(),
                Limits::default(),
            )
            .await
            .unwrap();
        let result = Interpreter::new(&db, context::ParamBindings::default())
            .execute_rows(
                &plan,
                &BTreeMap::new(),
                Limits {
                    batch_rows: 1,
                    ..Limits::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(result.rows, expected.rows, "{query}");
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn composed_windows_match_sequence_model_across_batch_boundaries() {
    let db = crate::execution::interpreter::test_support::open_db("cypher-composed-windows").await;
    let input = [
        json!(3),
        json!(1),
        json!(3),
        json!(2),
        json!(0),
        json!(null),
    ];
    for skip in [0, 2, 8] {
        for limit in [0, 1, 20] {
            for next_skip in [0, 3, 8] {
                for next_limit in [0, 2, 20] {
                    let query = format!("UNWIND [3,1,3,2,0,null] AS x WITH x SKIP {skip} LIMIT {limit} RETURN x SKIP {next_skip} LIMIT {next_limit}");
                    let plan = r::plan(
                        helix_cypher::compile(&query).unwrap(),
                        &db.planner_context(context::ParamBindings::default()),
                    )
                    .unwrap();
                    let expected: Vec<_> = input
                        .iter()
                        .skip(skip)
                        .take(limit)
                        .skip(next_skip)
                        .take(next_limit)
                        .map(|value| vec![value.clone()])
                        .collect();
                    for (execution, batch_rows) in [
                        (r::RowExecution::Materialized, 7),
                        (r::RowExecution::Batched, 1),
                        (r::RowExecution::Batched, 2),
                        (r::RowExecution::Batched, 7),
                    ] {
                        let result = Interpreter::new(&db, context::ParamBindings::default())
                            .execute_rows(
                                &plan.clone().with_execution(execution),
                                &BTreeMap::new(),
                                Limits {
                                    batch_rows,
                                    ..Limits::default()
                                },
                            )
                            .await
                            .unwrap();
                        assert_eq!(
                            result.rows, expected,
                            "{query}: {execution:?}, batch {batch_rows}"
                        );
                    }
                }
            }
        }
    }
    // Parameterized later windows stop through their consumer counters; they
    // must not need a literal source bound or drain the billion-row source.
    // The middle window exhausts before the final window: checking only the
    // final counter would keep pulling the million-row first window.
    for first_limit in [0, 1, 1_000_000] {
        for (skip, limit) in [(0, 0), (0, 2), (5, 2), (5, 0)] {
            let query = format!("UNWIND range(1,1000000000) AS x WITH x LIMIT {first_limit} WITH x SKIP $skip LIMIT $limit RETURN x SKIP 1 LIMIT 2");
            let plan = r::plan(
                helix_cypher::compile(&query).unwrap(),
                &db.planner_context(context::ParamBindings::default()),
            )
            .unwrap();
            let parameters = BTreeMap::from([
                ("skip".into(), r::Value::Integer(skip)),
                ("limit".into(), r::Value::Integer(limit)),
            ]);
            let interpreter = Interpreter::new(&db, context::ParamBindings::default());
            interpreter.ctx.fail_deadline_after(500);
            let result = interpreter
                .execute_rows(
                    &plan,
                    &parameters,
                    Limits {
                        batch_rows: 2,
                        memory_bytes: 128 * 1024,
                        ..Limits::default()
                    },
                )
                .await
                .unwrap();
            let expected: Vec<_> = (1..100)
                .take(first_limit)
                .skip(skip as usize)
                .take(limit as usize)
                .skip(1)
                .take(2)
                .map(|x| vec![json!(x)])
                .collect();
            assert_eq!(result.rows, expected, "{query}, {parameters:?}");
        }
    }
    for query in [
        "UNWIND [1,2,3] AS x WITH x LIMIT 0 RETURN x LIMIT -1",
        "UNWIND [] AS x WITH x LIMIT 1 RETURN x SKIP -1 LIMIT 0",
    ] {
        let error = db
            .cypher(crate::cypher::Request::new(query))
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::Query(ref error) if error.detail=="NegativeIntegerArgument"),
            "{query}: {error}"
        );
    }
    let error = db
        .cypher(crate::cypher::Request::new(
            "MATCH(n:Absent) WITH n LIMIT 10 RETURN $missing LIMIT 0",
        ))
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::Query(ref error) if error.detail=="MissingParameter"),
        "{error}"
    );
    db.cypher(crate::cypher::Request::new(
        "UNWIND [1,2,3] AS x WITH x LIMIT 3 CREATE (:WindowMarker {x:x}) RETURN x LIMIT 1",
    ))
    .await
    .unwrap();
    assert_eq!(
        db.cypher(crate::cypher::Request::new(
            "MATCH(n:WindowMarker) RETURN count(*)"
        ))
        .await
        .unwrap()
        .rows,
        vec![vec![json!(3)]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn composed_windows_bound_storage_reads_as_the_graph_grows() {
    let db =
        crate::execution::interpreter::test_support::open_db("cypher-composed-window-reads").await;
    let mut previous_size = 0;
    for size in [64, 4096] {
        db.cypher(crate::cypher::Request::new(format!(
            "UNWIND range({previous_size},{}) AS x CREATE (:WindowInput)",
            size - 1
        )))
        .await
        .unwrap();
        previous_size = size;
        let query = "MATCH(n:WindowInput) WITH n LIMIT 1000000 WITH n SKIP 2 LIMIT 10000 RETURN 1 AS found LIMIT 3";
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
        let optimized = Interpreter::new(&db, context::ParamBindings::default())
            .execute_rows(&plan, &BTreeMap::new(), Limits::default())
            .await
            .unwrap();
        assert_eq!(optimized.rows, vec![vec![json!(1)]; 3]);
        assert_eq!(optimized.rows, reference.rows);
        assert!(reference.resources.reads.multi_get_keys >= size);
        assert!(
            optimized.resources.reads.multi_get_keys <= 16,
            "{:?}",
            optimized.resources.reads
        );
        assert!(optimized.resources.peak_memory_bytes <= 256 * 1024);
        eprintln!(
            "composed window at {size} nodes: optimized={:?}, materialized={:?}",
            optimized.resources.reads, reference.resources.reads
        );
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn empty_windows_ignore_skips_without_skipping_validation() {
    let db =
        crate::execution::interpreter::test_support::open_db("cypher-empty-window-reads").await;
    db.cypher(crate::cypher::Request::new(
        "UNWIND range(1,1024) AS x CREATE (:EmptyWindowInput {k:1})",
    ))
    .await
    .unwrap();
    let parameters = BTreeMap::from([("key".into(), r::Value::Integer(1))]);
    for source in [
        "MATCH(n:EmptyWindowInput)",
        "MATCH(n:EmptyWindowInput {k:$key})",
    ] {
        for tail in [
            "RETURN n SKIP 1000000 LIMIT 0",
            "WITH n SKIP 1000000 RETURN n SKIP 1000000 LIMIT 0",
            "WITH n SKIP 1000000 LIMIT 1000000 RETURN n SKIP 1000000 LIMIT 0",
            "WITH n SKIP 1000000 LIMIT 0 RETURN n SKIP 1000000 LIMIT 1000000",
        ] {
            let query = format!("{source} {tail}");
            let plan = r::plan(
                helix_cypher::compile(&query).unwrap(),
                &db.planner_context(context::ParamBindings::default()),
            )
            .unwrap();
            let reference = Interpreter::new(&db, context::ParamBindings::default())
                .execute_rows(
                    &plan.clone().with_execution(r::RowExecution::Materialized),
                    &parameters,
                    Limits::default(),
                )
                .await
                .unwrap();
            assert!(reference.rows.is_empty());
            for batch_rows in [1, 512] {
                let result = Interpreter::new(&db, context::ParamBindings::default())
                    .execute_rows(
                        &plan,
                        &parameters,
                        Limits {
                            batch_rows,
                            ..Limits::default()
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(result.rows, reference.rows, "{query}");
                // The constrained source validates one candidate; the plain
                // scan need not hydrate any. Neither depends on batch width.
                assert!(
                    result.resources.reads.multi_get_keys
                        <= if source.contains("$key") { 2 } else { 0 },
                    "{query}, batch {batch_rows}: {:?}",
                    result.resources.reads
                );
            }
        }
    }
    for (query, detail) in [
        ("UNWIND [1/0] AS n RETURN n SKIP 1000000 LIMIT 0", "DivisionByZero"),
        ("MATCH(n:EmptyWindowInput {k:$missing}) WITH n SKIP 1000000 LIMIT 1000000 RETURN n SKIP 1000000 LIMIT 0", "MissingParameter"),
        ("MATCH(n:EmptyWindowInput) WITH n SKIP $negative LIMIT 1000000 RETURN n SKIP 1000000 LIMIT 0", "NegativeIntegerArgument"),
        ("MATCH(n:EmptyWindowInput) WITH n SKIP 1000000 LIMIT 0 RETURN n SKIP $negative LIMIT 0", "NegativeIntegerArgument"),
        ("MATCH(n:EmptyWindowInput) WITH n SKIP 1000000 LIMIT 0 RETURN n LIMIT $fraction", "InvalidArgumentType"),
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
                    &BTreeMap::from([
                        ("negative".into(), r::Value::Integer(-1)),
                        ("fraction".into(), r::Value::Float(1.0)),
                    ]),
                    Limits::default(),
                )
                .await
                .unwrap_err();
            assert!(
                matches!(error, Error::Query(ref error) if error.detail == detail && error.phase == r::ErrorPhase::Runtime),
                "{query}: {error}"
            );
        }
    }
    db.close().await.unwrap();
}
