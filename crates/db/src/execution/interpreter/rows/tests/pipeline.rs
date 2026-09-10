use super::super::*;
use helix_planner::context;
use serde_json::json;

#[tokio::test]
async fn bound_pattern_constraints_and_path_predicates_match_independent_expectations() {
    let db =
        crate::execution::interpreter::test_support::open_db("bound-pattern-constraints").await;
    db.cypher(crate::cypher::Request::new(
        "CREATE (a:N {key:1}),(b:N {key:2}),(c:Other {key:3}), (a)-[:R {weight:5}]->(b),(a)-[:S {weight:6}]->(c)",
    )).await.unwrap();
    for (text, expected) in [
        ("MATCH p=(a:N)-[r:R]->(b:N) WHERE head(nodes(p)).key=1 AND head(relationships(p)).weight=5 RETURN last(nodes(p)).key", vec![vec![json!(2)]]),
        ("MATCH (a:N {key:1}) MATCH (a:Other) RETURN count(*)", vec![vec![json!(0)]]),
        ("MATCH (a:N {key:1}) MATCH (a:N {key:2}) RETURN count(*)", vec![vec![json!(0)]]),
        ("MATCH (a)-[r:R]->(b) MATCH (a)-[r:S]->(b) RETURN count(*)", vec![vec![json!(0)]]),
        ("MATCH (a)-[r:R]->(b) MATCH (a)-[r:R {weight:0}]->(b) RETURN count(*)", vec![vec![json!(0)]]),
        ("MATCH (a)-[r:R]->(b) MATCH (a)-[s:R]->(b) RETURN r=s", vec![vec![json!(true)]]),
        ("MATCH (a)-[r:R]->(b),(a)-[s:R]->(b) RETURN count(*)", vec![vec![json!(0)]]),
        ("MATCH (a)-[r:R]->(b) OPTIONAL MATCH (a)-[r:S]->(c) RETURN a.key,b.key,c.key", vec![vec![json!(1),json!(2),json!(null)]]),
    ] {
        let selected = r::plan(helix_cypher::compile(text).unwrap(), &db.planner_context(context::ParamBindings::default())).unwrap();
        for execution in [r::RowExecution::Batched, r::RowExecution::Materialized] {
            let result = Interpreter::new(&db, context::ParamBindings::default())
                .execute_rows(&selected.clone().with_execution(execution), &BTreeMap::new(), Limits { batch_rows: 1, ..Default::default() })
                .await.unwrap_or_else(|error| panic!("{text}: {error}"));
            assert_eq!(result.rows, expected, "{text}: {execution:?}");
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn batch_and_materialized_pipelines_agree_on_rows_errors_and_mutations() {
    let db = crate::execution::interpreter::test_support::open_db("row-strategy-oracle").await;
    db.cypher(crate::cypher::Request::new(
        "UNWIND range(0,99) AS i CREATE (:N {key:i,group:i%7})",
    ))
    .await
    .unwrap();
    let queries = [
        "MATCH (n:N) WHERE n.key%2=0 RETURN n.group,count(*),sum(n.key) ORDER BY n.group",
        "MATCH (n:N) WHERE n.key%2=0 RETURN n.key AS key ORDER BY key DESC SKIP 3 LIMIT 4",
        "MATCH (n:N) RETURN 1+count(*) AS total ORDER BY total",
        "MATCH (n:N) WITH n.key AS k WHERE k%2=0 RETURN count(*),collect(k)",
        "UNWIND [null,2,2,1,3,null] AS x RETURN x,count(*),count(x),collect(DISTINCT x) ORDER BY x",
        "UNWIND [] AS x RETURN count(*),sum(x),avg(x),collect(x)",
        "UNWIND range(0,100) AS x RETURN x%7 AS g ORDER BY g SKIP 5 LIMIT 9",
        "UNWIND range(0,100) AS x RETURN x%7 AS g ORDER BY g LIMIT 0",
        "MATCH (n:Absent) RETURN count(*),sum(n.key)",
        "OPTIONAL MATCH (n:Absent) RETURN count(*),count(n),collect(n)",
        "OPTIONAL MATCH (n:N) WHERE n.key<0 RETURN count(*),count(n),collect(n)",
        "OPTIONAL MATCH (n:N) WHERE n.key/0>1 RETURN count(*)",
        "OPTIONAL MATCH (n:Absent) RETURN n ORDER BY n LIMIT 1",
        "OPTIONAL MATCH (n:N) RETURN n.key AS key ORDER BY key DESC LIMIT 3",
        "MATCH (n:N) WITH n ORDER BY n.key SKIP 5 LIMIT 8 RETURN n.key ORDER BY n.key DESC",
        "MATCH (n:N) WHERE n.key/0>1 RETURN count(*)",
        "UNWIND [1,2,0] AS x RETURN sum(10/x)",
        "UNWIND [1,2,0] AS x RETURN 10/x AS k ORDER BY k LIMIT 1",
        "MATCH (n:N) RETURN n.key/0 AS k ORDER BY k LIMIT 0",
        "UNWIND range(0,100) AS x RETURN 1/(100-x) LIMIT 1",
        "UNWIND range(0,100) AS x WITH x AS y SKIP 5 LIMIT 4 WHERE y%2=0 RETURN y",
        "MATCH (n:N) RETURN n.key SKIP 5 LIMIT 4",
    ];
    for text in queries {
        let query = helix_cypher::compile(text).unwrap();
        let selected = r::plan(
            query,
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        let reference = selected
            .clone()
            .with_execution(r::RowExecution::Materialized);
        let limits = Limits {
            batch_rows: 3,
            ..Default::default()
        };
        let mut results = Vec::new();
        for plan in [&selected, &reference] {
            let result = Interpreter::new(&db, context::ParamBindings::default())
                .execute_rows(plan, &BTreeMap::new(), limits)
                .await;
            results.push(match result {
                Ok(response) => Ok((response.columns, response.rows)),
                Err(Error::Query(error)) => Err((error.category, error.detail, error.phase)),
                Err(error) => panic!("{text}: {error:?}"),
            });
        }
        assert_eq!(results[0], results[1], "{text}");
    }
    // Independent arithmetic model checks grouping instead of relying on two
    // implementations agreeing on the same incorrect result.
    let response = db
        .cypher(crate::cypher::Request::new(queries[0]))
        .await
        .unwrap();
    let expected = (0..7)
        .map(|group| {
            let values = (0..100)
                .filter(|i| i % 2 == 0 && i % 7 == group)
                .collect::<Vec<_>>();
            vec![
                json!(group),
                json!(values.len()),
                json!(values.iter().sum::<i64>()),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(response.rows, expected);
    let optional = db
        .cypher(crate::cypher::Request::new(
            "OPTIONAL MATCH (n:N) WHERE n.key<0 RETURN count(*),count(n),collect(n)",
        ))
        .await
        .unwrap();
    assert_eq!(optional.rows, vec![vec![json!(1), json!(0), json!([])]]);

    db.close().await.unwrap();

    for strategy in [r::RowExecution::Batched, r::RowExecution::Materialized] {
        let db = crate::execution::interpreter::test_support::open_db("row-strategy-writes").await;
        db.cypher(crate::cypher::Request::new(
            "UNWIND range(0,9) AS x CREATE (:N {key:x})",
        ))
        .await
        .unwrap();
        for (text, succeeds) in [
            (
                "MATCH (n:N) WITH n ORDER BY n.key SKIP 2 LIMIT 2 SET n.selected=true RETURN n.key",
                true,
            ),
            (
                "MATCH (n:N) SET n.key=n.key+10 WITH n RETURN n.key/0",
                false,
            ),
        ] {
            let query = helix_cypher::compile(text).unwrap();
            let plan = r::plan(
                query,
                &db.planner_context(context::ParamBindings::default()),
            )
            .unwrap()
            .with_execution(strategy);
            let result = Interpreter::new(&db, context::ParamBindings::default())
                .execute_rows(&plan, &BTreeMap::new(), Limits::default())
                .await;
            assert_eq!(result.is_ok(), succeeds, "{text}");
        }
        let rows = db
            .cypher(crate::cypher::Request::new(
                "MATCH (n:N) WHERE n.selected RETURN n.key ORDER BY n.key",
            ))
            .await
            .unwrap()
            .rows;
        assert_eq!(rows, vec![vec![json!(2)], vec![json!(3)]]);
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn index_extraction_preserves_errors_in_nullable_boolean_conjunctions() {
    use crate::execution::interpreter::test_support;
    let plain = test_support::open_db("cypher-error-order-scan").await;
    let indexed = test_support::open_db_with_config(
        test_support::in_memory_config("cypher-error-order-index").with_equality_index("N", "key"),
    )
    .await;
    for db in [&plain, &indexed] {
        db.cypher(crate::cypher::Request::new("CREATE (:N {key:1}),(:N)"))
            .await
            .unwrap();
    }
    for query in [
        "MATCH (n:N) WHERE 1/0>0 AND n.key=7 RETURN count(*)",
        "MATCH (n:N) WHERE n.key=7 AND 1/0>0 RETURN count(*)",
    ] {
        let mut errors = Vec::new();
        for db in [&plain, &indexed] {
            let Error::Query(error) = db
                .cypher(crate::cypher::Request::new(query))
                .await
                .unwrap_err()
            else {
                panic!("expected expression error")
            };
            errors.push((error.category, error.detail, error.phase));
        }
        assert_eq!(errors[0], errors[1], "{query}");
    }
    let result = indexed
        .cypher(crate::cypher::Request::new(
            "MATCH (n:N) WHERE n.key=1 AND n.key=1 RETURN count(*)",
        ))
        .await
        .unwrap();
    assert_eq!(result.rows, vec![vec![json!(1)]]);
    // Explored scan alternatives must not appear as selected range-scan work.
    assert_eq!(result.diagnostics.selected_cost.range_nexts, 0);
    plain.close().await.unwrap();
    indexed.close().await.unwrap();
}

#[tokio::test]
async fn connected_patterns_stream_paths_cycles_and_write_selection_with_bounded_memory() {
    let db = crate::execution::interpreter::test_support::open_db("streamed-graph-patterns").await;
    db.cypher(crate::cypher::Request::new(
        "UNWIND range(0,511) AS i CREATE (:Root {key:i})-[:R]->(:Middle {key:i})-[:S]->(:Leaf {key:i})",
    )).await.unwrap();
    db.cypher(crate::cypher::Request::new(
        "CREATE (a:Loop {key:1})-[:R]->(b:Loop {key:2})-[:R]->(c:Loop {key:3})-[:R]->(a), (a)-[:R]->(a), (a)-[:R]->(b)",
    )).await.unwrap();
    let queries = [
        "MATCH p=(a:Root)-[:R]->(b:Middle)-[:S]->(c:Leaf) WHERE a.key%2=0 RETURN count(*),sum(c.key),min(length(p))",
        "MATCH p=(a:Root)-[:R]->(b:Middle)-[:S]->(c:Leaf) RETURN p,a.key AS k ORDER BY k DESC SKIP 3 LIMIT 5",
        "MATCH (a:Loop)-[r:R]-(b)-[s:R]-(c) RETURN count(*),count(DISTINCT r),count(DISTINCT s)",
        "MATCH (a:Loop)-[r:R]->(b)-[s:R]->(a) RETURN count(*)",
        "MATCH (a:Loop)-[r:R|S]->(b) RETURN a.key,b.key,count(*) ORDER BY a.key,b.key",
        "MATCH p=(a:Loop)-[:Absent]->(b) RETURN count(*),min(length(p))",
        "OPTIONAL MATCH p=(a:Loop)-[:Absent]->(b) RETURN count(*),min(length(p))",
        "OPTIONAL MATCH p=(a:Loop)-[:Absent]->(b) RETURN p,a,b",
        "OPTIONAL MATCH (a:Root)-[:R]->(b:Middle)-[:S]->(c:Leaf) RETURN count(*),sum(c.key)",
        "MATCH (a:Root)-[:R]->(b:Middle)-[:S]->(c:Leaf) WHERE a.key/0>1 RETURN count(*)",
        "MATCH (a:Root)-[:R]->(b:Middle)-[:S]->(c:Leaf) RETURN c.key SKIP 3 LIMIT 4",
        "MATCH (a:Root)-[:R]->(b:Middle)-[:S]->(c:Leaf) RETURN c.key/0 LIMIT 0",
    ];
    for text in queries {
        let plan = r::plan(
            helix_cypher::compile(text).unwrap(),
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        assert!(plan.batch_consumer(0).is_some(), "{text}");
        let mut results = Vec::new();
        for strategy in [r::RowExecution::Batched, r::RowExecution::Materialized] {
            let result = Interpreter::new(&db, context::ParamBindings::default())
                .execute_rows(
                    &plan.clone().with_execution(strategy),
                    &BTreeMap::new(),
                    Limits {
                        batch_rows: 7,
                        ..Default::default()
                    },
                )
                .await;
            results.push(match result {
                Ok(response) => Ok((response.columns, response.rows)),
                Err(Error::Query(error)) => Err((error.category, error.detail, error.phase)),
                Err(error) => panic!("{text}: {error}"),
            });
        }
        assert_eq!(results[0], results[1], "{text}");
    }
    let text = queries[0];
    let plan = r::plan(
        helix_cypher::compile(text).unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    let limits = Limits {
        batch_rows: 16,
        memory_bytes: 128 * 1024,
        ..Default::default()
    };
    let result = Interpreter::new(&db, context::ParamBindings::default())
        .execute_rows(&plan, &BTreeMap::new(), limits)
        .await
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            json!(256),
            json!((0..512).filter(|i| i % 2 == 0).sum::<i64>()),
            json!(2)
        ]]
    );
    assert!(result.resources.peak_memory_bytes <= limits.memory_bytes);
    let error = Interpreter::new(&db, context::ParamBindings::default())
        .execute_rows(
            &plan.with_execution(r::RowExecution::Materialized),
            &BTreeMap::new(),
            limits,
        )
        .await
        .unwrap_err();
    assert!(matches!(error,Error::Query(error) if error.detail=="MemoryLimit"));
    // The complete input selection is consumed before any write changes a
    // matching label/property, so the mutation cannot alter its own cursor.
    let result = db.cypher(crate::cypher::Request::new(
        "MATCH (a:Root)-[:R]->(b:Middle)-[:S]->(c:Leaf) WITH a ORDER BY a.key DESC LIMIT 2 SET a.key=-1 RETURN count(*)",
    )).await.unwrap();
    assert_eq!(result.rows, vec![vec![json!(2)]]);
    assert_eq!(
        db.cypher(crate::cypher::Request::new(
            "MATCH (a:Root) WHERE a.key=-1 RETURN count(*)"
        ))
        .await
        .unwrap()
        .rows,
        vec![vec![json!(2)]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn high_fan_out_keeps_compact_ids_and_bounded_endpoint_batches() {
    let db = crate::execution::interpreter::test_support::open_db_with_config(
        crate::execution::interpreter::test_support::in_memory_config("compact-fan-out")
            .with_equality_index("Hub", "key"),
    )
    .await;
    db.cypher(crate::cypher::Request::new(
        "CREATE (h:Hub {key:0}) WITH h UNWIND range(0,4095) AS i CREATE (h)-[:R]->(:Leaf {key:i})",
    ))
    .await
    .unwrap();
    for (query, expected) in [
        (
            "MATCH (h:Hub {key:0})-[r:R]->(n:Leaf) RETURN count(*),sum(n.key)",
            json!([[4096, 8386560]]),
        ),
        (
            "MATCH (h:Hub {key:0})-[r:R]->(n:Leaf) RETURN n.key ORDER BY n.key DESC LIMIT 3",
            json!([[4095], [4094], [4093]]),
        ),
    ] {
        let plan = r::plan(
            helix_cypher::compile(query).unwrap(),
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        let limits = Limits {
            memory_bytes: 192 * 1024,
            batch_rows: 16,
            ..Default::default()
        };
        let result = Interpreter::new(&db, context::ParamBindings::default())
            .execute_rows(&plan, &BTreeMap::new(), limits)
            .await
            .unwrap();
        assert_eq!(json!(result.rows), expected);
        assert!(result.resources.peak_memory_bytes <= limits.memory_bytes);
        let error = Interpreter::new(&db, context::ParamBindings::default())
            .execute_rows(
                &plan.with_execution(r::RowExecution::Materialized),
                &BTreeMap::new(),
                limits,
            )
            .await
            .unwrap_err();
        assert!(matches!(error,Error::Query(error) if error.detail=="MemoryLimit"));
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn correlated_index_probes_preserve_scan_semantics_and_pending_writes() {
    use crate::execution::interpreter::test_support;
    let plain = test_support::open_db("correlated-scan").await;
    let indexed = test_support::open_db_with_config(
        test_support::in_memory_config("correlated-index").with_equality_index("N", "key"),
    )
    .await;
    for db in [&plain, &indexed] {
        db.cypher(crate::cypher::Request::new(
            "UNWIND range(0,999) AS i CREATE (:N {key:i})",
        ))
        .await
        .unwrap();
        db.cypher(crate::cypher::Request::new(
            "CREATE (:N {key:2}),(:N {key:[1,2]}),(:N {key:[1.0,2.0]}),(:N {key:[]}),(:N {key:['a','b']}),(:N),(:N {key:true}),(:N {key:'text'})",
        ))
        .await
        .unwrap();
    }
    for text in [
        "UNWIND [2,2,999,1001,null] AS k MATCH (n:N {key:k}) RETURN k,n.key ORDER BY k,n.key",
        "UNWIND [2,2,999,1001,null] AS k OPTIONAL MATCH (n:N {key:k}) RETURN k,n.key ORDER BY k,n.key",
        "UNWIND [2,2.0,null,[1,2],{},true,'text'] AS k MATCH (n:N) WHERE k=n.key RETURN k,n.key ORDER BY k,n.key",
        "UNWIND [[1,2],[1.0,2.0],[1,2.0],[],['a','b'],{}] AS k OPTIONAL MATCH (n:N) WHERE n.key=k RETURN k,n.key ORDER BY k,n.key",
        "WITH null AS k MATCH (n:N {key:k}) RETURN count(*)",
        "CREATE (:N {key:1007}) WITH 1007 AS k MATCH (n:N {key:k}) RETURN n.key",
        "MATCH (a:N {key:999}) SET a.key=1008 WITH 1008 AS k MATCH (n:N {key:k}) RETURN n.key",
    ] {
        let plan = r::plan(helix_cypher::compile(text).unwrap(),
            &indexed.planner_context(context::ParamBindings::default())).unwrap();
        assert!(plan.matches().values().any(|plan| plan.steps.iter().any(|step|
            matches!(step, r::MatchStep::IndexLookup(_)))), "{text}");
        let mut results = Vec::new();
        for db in [&plain, &indexed] {
            results.push(db.cypher(crate::cypher::Request::new(text)).await.unwrap().rows);
        }
        assert_eq!(results[0],results[1],"{text}");
    }
    let result = indexed.cypher(crate::cypher::Request::new(
        "UNWIND [2,2,null,7777] AS k OPTIONAL MATCH (n:N {key:k}) RETURN k,n.key ORDER BY k,n.key",
    )).await.unwrap();
    assert_eq!(
        result.rows,
        vec![vec![json!(2), json!(2)]; 4]
            .into_iter()
            .chain([
                vec![json!(7777), json!(null)],
                vec![json!(null), json!(null)]
            ])
            .collect::<Vec<_>>()
    );
    assert_eq!(result.resources.reads.scans, 0);
    assert!(
        result.resources.reads.multi_get_keys < 64,
        "{:?}",
        result.resources
    );
    assert!(result.resources.peak_memory_bytes < 512 * 1024);
    let nonfinite: crate::cypher::Request = serde_json::from_value(json!({
        "query":"UNWIND $values AS k OPTIONAL MATCH (n:N {key:k}) RETURN count(n)",
        "parameters":{"values":[{"$type":"float","value":"NaN"},{"$type":"float","value":"Infinity"}]}
    })).unwrap();
    assert_eq!(
        indexed.cypher(nonfinite).await.unwrap().rows,
        vec![vec![json!(0)]]
    );
    for text in [
        "WITH 7 AS k MATCH (n:N) WHERE n.key=k AND 1/0>1 RETURN count(*)",
        "WITH 7 AS k MATCH (n:N {key:k, other:1/0}) RETURN count(*)",
    ] {
        let plan = r::plan(
            helix_cypher::compile(text).unwrap(),
            &indexed.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        assert!(
            plan.matches().values().all(|plan| plan
                .steps
                .iter()
                .all(|step| !matches!(step, r::MatchStep::IndexLookup(_)))),
            "{text}"
        );
        for db in [&plain, &indexed] {
            assert!(
                db.cypher(crate::cypher::Request::new(text)).await.is_err(),
                "{text}"
            );
        }
    }
    assert!(indexed
        .cypher(crate::cypher::Request::new(
            "CREATE (:N {key:2009}),(:N {key:[true,false]})"
        ))
        .await
        .is_err());
    assert_eq!(
        indexed
            .cypher(crate::cypher::Request::new(
                "MATCH (n:N {key:2009}) RETURN count(*)"
            ))
            .await
            .unwrap()
            .rows,
        vec![vec![json!(0)]]
    );
    plain.close().await.unwrap();
    indexed.close().await.unwrap();
}

#[tokio::test]
async fn explicit_filter_contracts_and_empty_or_limited_matches_preserve_rows() {
    let db =
        crate::execution::interpreter::test_support::open_db("common-row-filter-contract").await;
    db.cypher(crate::cypher::Request::new(
        "CREATE (:N {key:0}),(:N {key:1}),(:N {key:2})",
    ))
    .await
    .unwrap();
    let query =
        helix_cypher::compile("MATCH (n:N) WHERE n.key>0 RETURN n.key ORDER BY n.key").unwrap();
    let mut operators = query.operators().to_vec();
    let r::Operator::Match { predicate, .. } = &mut operators[0] else {
        panic!("match")
    };
    let predicate = predicate.take().unwrap();
    operators.insert(1, r::Operator::Filter(predicate));
    let query = r::Query::new(
        query.bindings().to_vec(),
        operators,
        query.returns().to_vec(),
    )
    .unwrap();
    let plan = r::plan(
        query,
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    assert_eq!(
        Interpreter::new(&db, context::ParamBindings::default())
            .execute_rows(
                &plan,
                &BTreeMap::new(),
                Limits {
                    batch_rows: 1,
                    ..Default::default()
                }
            )
            .await
            .unwrap()
            .rows,
        vec![vec![json!(1)], vec![json!(2)]]
    );
    for (text, expected) in [
        ("UNWIND [] AS key MATCH (n:N) RETURN count(*)", json!([[0]])),
        (
            "OPTIONAL MATCH (n:Absent) MATCH (n)-[:R]->(m) RETURN count(*)",
            json!([[0]]),
        ),
        (
            "OPTIONAL MATCH (n:Absent) OPTIONAL MATCH (n)-[:R]->(m) RETURN count(*)",
            json!([[1]]),
        ),
        (
            "UNWIND range(0,99) AS key MATCH (n:N) RETURN key LIMIT 1",
            json!([[0]]),
        ),
    ] {
        let response = db.cypher(crate::cypher::Request::new(text)).await.unwrap();
        assert_eq!(
            serde_json::to_value(response.rows).unwrap(),
            expected,
            "{text}"
        );
        if text.starts_with("UNWIND []") {
            assert_eq!(
                response.resources.reads,
                crate::cypher::StorageReadUsage::default()
            );
        }
    }
    for (values, expected) in [
        (
            vec![
                r::Value::Boolean(true),
                r::Value::Null,
                r::Value::Boolean(false),
            ],
            true,
        ),
        (vec![r::Value::Integer(1)], false),
    ] {
        let query = r::Query::new(
            vec![r::Binding {
                name: "value".into(),
                kind: r::BindingType::Scalar,
                nullable: true,
                value_type: r::ValueType::Any,
            }],
            vec![
                r::Operator::Unwind {
                    expression: r::Expression::Literal(r::Value::List(values)),
                    slot: r::Slot(0),
                },
                r::Operator::Filter(r::Expression::Slot(r::Slot(0))),
            ],
            vec![("value".into(), r::Slot(0))],
        )
        .unwrap();
        let plan = r::plan(query, &context::PlannerContext::default()).unwrap();
        let result = Interpreter::new(&db, context::ParamBindings::default())
            .execute_rows(&plan, &BTreeMap::new(), Limits::default())
            .await;
        if expected {
            assert_eq!(result.unwrap().rows, vec![vec![json!(true)]]);
        } else {
            assert!(matches!(result,Err(Error::Query(error)) if error.category=="TypeError"));
        }
    }
    let text = format!(
        "RETURN {}",
        (0..100)
            .map(|i| format!("{i} AS c{i}"))
            .collect::<Vec<_>>()
            .join(",")
    );
    let error = crate::cypher::execute(
        &db,
        crate::cypher::Request::new(text),
        crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
        crate::query_service::QueryMode::Execute,
        crate::execution_control::ExecutionControl::unlimited(),
        Limits {
            memory_bytes: 1024,
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(error,Error::Query(error) if error.detail=="MemoryLimit"));
    db.close().await.unwrap();
}
