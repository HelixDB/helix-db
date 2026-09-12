use super::super::*;
use helix_planner::context;
use serde_json::json;

#[tokio::test]
async fn mixed_pipelines_stream_expansion_filters_aggregation_and_top_k() {
    let db = crate::execution::interpreter::test_support::open_db("cypher-mixed-pipeline").await;
    let prefix = "UNWIND range(1,2000) AS x WITH x+1 AS seed UNWIND [seed,seed+1] AS y WITH y WHERE y%2=0 UNWIND range(1,3) AS z ";
    // Independent tuple model establishes multiplicity, not just a checksum of
    // execution through another physical strategy.
    let expected: Vec<_> = (1_i64..=2000)
        .flat_map(|x| [x + 1, x + 2])
        .filter(|y| y % 2 == 0)
        .flat_map(|y| (1..=3).map(move |z| (y, z)))
        .collect();
    let mut greatest: Vec<_> = expected.iter().map(|(y, z)| y * 100 + z).collect();
    greatest.sort_unstable_by(|a, b| b.cmp(a));
    greatest.truncate(5);
    for (tail, expected) in [
        (
            "RETURN count(*),sum(y*z)",
            vec![vec![
                json!(expected.len()),
                json!(expected.iter().map(|(y, z)| y * z).sum::<i64>()),
            ]],
        ),
        (
            "RETURN y*100+z AS value ORDER BY value DESC LIMIT 5",
            greatest.into_iter().map(|x| vec![json!(x)]).collect(),
        ),
    ] {
        let query = helix_cypher::compile(&format!("{prefix}{tail}")).unwrap();
        let selected = r::plan(
            query,
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        assert!(matches!(
            selected.batch_consumer(0),
            Some(r::BatchConsumer::Pipeline { .. })
        ));
        assert!(selected
            .explain()
            .operators
            .iter()
            .skip(1)
            .all(|op| !op.blocking.contains(&r::BlockingWork::MaterializedRelation)));
        let limits = Limits {
            batch_rows: 8,
            memory_bytes: 128 * 1024,
            ..Default::default()
        };
        let actual = Interpreter::new(&db, context::ParamBindings::default())
            .execute_rows(&selected, &BTreeMap::new(), limits)
            .await
            .unwrap();
        assert_eq!(actual.rows, expected);
        assert!(actual.resources.peak_memory_bytes <= limits.memory_bytes);
        let reference = selected.with_execution(r::RowExecution::Materialized);
        assert_eq!(
            Interpreter::new(&db, context::ParamBindings::default())
                .execute_rows(&reference, &BTreeMap::new(), Limits::default())
                .await
                .unwrap()
                .rows,
            expected
        );
        assert!(
            matches!(Interpreter::new(&db, context::ParamBindings::default()).execute_rows(&reference, &BTreeMap::new(), limits).await, Err(Error::Query(error)) if error.detail == "MemoryLimit")
        );
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn mixed_pipeline_windows_preserve_pending_expansions_and_errors() {
    let db = crate::execution::interpreter::test_support::open_db("cypher-mixed-windows").await;
    for (text, expected) in [
        ("UNWIND [1,2,3] AS x WITH x LIMIT 1 UNWIND [x,x+10,x+20] AS y RETURN y", vec![vec![json!(1)],vec![json!(11)],vec![json!(21)]]),
        ("UNWIND [] AS x UNWIND [x] AS y WITH y RETURN count(*)", vec![vec![json!(0)]]),
        ("UNWIND [1,2,3] AS x UNWIND [x,x] AS y WITH y SKIP 1 LIMIT 3 UNWIND [y,y+10] AS z RETURN z SKIP 1 LIMIT 4", vec![vec![json!(11)],vec![json!(2)],vec![json!(12)],vec![json!(2)]]),
        ("OPTIONAL MATCH (n:Absent) UNWIND [n,1] AS x RETURN x", vec![vec![json!(null)],vec![json!(1)]]),
    ] {
        let plan = r::plan(helix_cypher::compile(text).unwrap(), &db.planner_context(context::ParamBindings::default())).unwrap();
        for batch_rows in [1,2,7,32] {
            for strategy in [r::RowExecution::Batched, r::RowExecution::Materialized] {
                let plan = plan.clone().with_execution(strategy);
                let response = Interpreter::new(&db, context::ParamBindings::default()).execute_rows(&plan, &BTreeMap::new(), Limits { batch_rows, ..Default::default() }).await.unwrap();
                assert_eq!(response.rows, expected, "{text} batch={batch_rows} strategy={strategy:?}");
            }
        }
    }
    for text in [
        "UNWIND [1,0] AS x UNWIND [1/x] AS y WITH y RETURN y LIMIT 1",
        "UNWIND [1,2] AS x UNWIND [x] AS y WITH 1/(2-y) AS z UNWIND [z] AS result RETURN result LIMIT 0",
        "CREATE (:Rollback) WITH 1 AS seed UNWIND [1,0] AS x UNWIND [1/x] AS y RETURN y LIMIT 1",
    ] {
        let plan = r::plan(helix_cypher::compile(text).unwrap(), &db.planner_context(context::ParamBindings::default())).unwrap();
        for strategy in [r::RowExecution::Batched, r::RowExecution::Materialized] {
            let result = Interpreter::new(&db, context::ParamBindings::default()).execute_rows(&plan.clone().with_execution(strategy), &BTreeMap::new(), Limits { batch_rows: 1, ..Default::default() }).await;
            assert!(matches!(result, Err(Error::Query(error)) if error.detail == "DivisionByZero"), "{text}");
        }
    }
    assert_eq!(
        db.cypher(crate::cypher::Request::new(
            "MATCH (n:Rollback) RETURN count(*)"
        ))
        .await
        .unwrap()
        .rows,
        vec![vec![json!(0)]]
    );
    // Downstream writes must not change the graph producer's own input scan.
    db.cypher(crate::cypher::Request::new(
        "CREATE (:Seed {key:1}),(:Seed {key:2})",
    ))
    .await
    .unwrap();
    assert_eq!(db.cypher(crate::cypher::Request::new("MATCH (n:Seed) UNWIND [n.key,n.key] AS key WITH key CREATE (:Seed {key:key+10}) RETURN count(*)")).await.unwrap().rows, vec![vec![json!(4)]]);
    assert_eq!(
        db.cypher(crate::cypher::Request::new(
            "MATCH (n:Seed) RETURN count(*)"
        ))
        .await
        .unwrap()
        .rows,
        vec![vec![json!(6)]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn standalone_common_filters_stream_and_reject_non_boolean_values() {
    let db = crate::execution::interpreter::test_support::open_db("cypher-pipeline-filter").await;
    for predicate in [
        r::Value::Boolean(true),
        r::Value::Boolean(false),
        r::Value::Null,
        r::Value::Integer(1),
    ] {
        let query =
            helix_cypher::compile("UNWIND [1,2] AS x UNWIND [x] AS y RETURN count(*)").unwrap();
        let mut operators = query.operators().to_vec();
        operators.insert(
            1,
            r::Operator::Filter(
                r::SelectionProgram::new(r::Expression::Literal(predicate.clone())).unwrap(),
            ),
        );
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
        for strategy in [r::RowExecution::Batched, r::RowExecution::Materialized] {
            let result = Interpreter::new(&db, context::ParamBindings::default())
                .execute_rows(
                    &plan.clone().with_execution(strategy),
                    &BTreeMap::new(),
                    Limits {
                        batch_rows: 1,
                        ..Default::default()
                    },
                )
                .await;
            if matches!(predicate, r::Value::Integer(_)) {
                assert!(
                    matches!(result, Err(Error::Query(error)) if error.detail == "InvalidArgumentType")
                );
            } else {
                assert_eq!(
                    result.unwrap().rows,
                    vec![vec![json!(if predicate == r::Value::Boolean(true) {
                        2
                    } else {
                        0
                    })]]
                );
            }
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn long_mixed_pipelines_use_bounded_execution_stack_and_planning_work() {
    let db =
        crate::execution::interpreter::test_support::open_db("cypher-long-mixed-pipeline").await;
    let text = format!(
        "UNWIND [1] AS x {} RETURN count(*)",
        "WITH x AS x UNWIND [x] AS y WITH y AS x ".repeat(200)
    );
    let query = helix_cypher::compile(&text).unwrap();
    let plan = r::plan(
        query,
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    assert!(plan.metrics.memo_groups <= 4);
    assert!(plan.metrics.rule_fires <= 8);
    let response = Interpreter::new(&db, context::ParamBindings::default())
        .execute_rows(
            &plan,
            &BTreeMap::new(),
            Limits {
                batch_rows: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(response.rows, vec![vec![json!(1)]]);
    db.close().await.unwrap();
}
