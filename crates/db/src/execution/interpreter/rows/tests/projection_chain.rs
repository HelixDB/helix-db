use super::super::*;
use helix_planner::context;
use serde_json::json;

#[tokio::test]
async fn projection_chains_keep_global_windows_and_bounded_intermediate_memory() {
    let db = crate::execution::interpreter::test_support::open_db("cypher-projection-chain").await;
    for (text, expected) in [
        ("UNWIND range(1,20000) AS x WITH x AS a WHERE a%2=0 WITH a+1 AS b RETURN count(*),sum(b)", vec![vec![json!(10000), json!(100020000)]]),
        ("UNWIND range(1,20000) AS x WITH x*2 AS a WITH a+1 AS b RETURN b ORDER BY b DESC LIMIT 3", vec![vec![json!(40001)],vec![json!(39999)],vec![json!(39997)]]),
        ("UNWIND range(1,20000) AS x WITH x AS a SKIP 3 LIMIT 7 WHERE a%2=0 WITH a+1 AS b RETURN b SKIP 2 LIMIT 3", vec![vec![json!(13)],vec![json!(15)],vec![json!(17)]]),
        ("UNWIND [1,2,3] AS x WITH x AS a WHERE a<0 WITH a AS b RETURN count(*),sum(b),collect(b)", vec![vec![json!(0),json!(0),json!([])]]),
    ] {
        let selected = r::plan(helix_cypher::compile(text).unwrap(), &db.planner_context(context::ParamBindings::default())).unwrap();
        assert!(matches!(selected.batch_consumer(0), Some(r::BatchConsumer::Pipeline { .. })));
        let limits = Limits { batch_rows: 8, memory_bytes: 128 * 1024, ..Default::default() };
        let result = Interpreter::new(&db, context::ParamBindings::default()).execute_rows(&selected, &BTreeMap::new(), limits).await.unwrap_or_else(|e| panic!("{text}: {e}"));
        assert_eq!(result.rows, expected, "{text}");
        assert!(result.resources.peak_memory_bytes <= limits.memory_bytes);
        let reference = selected.with_execution(r::RowExecution::Materialized);
        let result = Interpreter::new(&db, context::ParamBindings::default()).execute_rows(&reference, &BTreeMap::new(), Limits::default()).await.unwrap();
        assert_eq!(result.rows, expected, "materialized: {text}");
        if text.contains("20000") {
            let failure = Interpreter::new(&db, context::ParamBindings::default()).execute_rows(&reference, &BTreeMap::new(), limits).await.unwrap_err();
            assert!(matches!(failure, Error::Query(error) if error.detail == "MemoryLimit"));
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn projection_chain_limits_keep_later_errors_and_mutation_barriers() {
    let db =
        crate::execution::interpreter::test_support::open_db("cypher-projection-chain-barriers")
            .await;
    for text in [
        "UNWIND [1,2,0] AS x WITH 1/x AS a LIMIT 1 WITH a AS b RETURN b LIMIT 1",
        "UNWIND [1,2,0] AS x WITH x AS a WITH 1/a AS b RETURN b ORDER BY b LIMIT 1",
        "UNWIND [1,2,0] AS x WITH x AS a WITH 1/a AS b RETURN count(b)",
        "UNWIND [1/0] AS x RETURN x LIMIT 0",
        "UNWIND [1/0] AS x WITH x LIMIT 0 WITH x AS y RETURN y",
        "UNWIND [1,0] AS x UNWIND [1/x] AS y RETURN y LIMIT 1",
    ] {
        let plan = r::plan(
            helix_cypher::compile(text).unwrap(),
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        for execution in [r::RowExecution::Batched, r::RowExecution::Materialized] {
            let error = Interpreter::new(&db, context::ParamBindings::default())
                .execute_rows(
                    &plan.clone().with_execution(execution),
                    &BTreeMap::new(),
                    Limits {
                        batch_rows: 1,
                        ..Default::default()
                    },
                )
                .await
                .unwrap_err();
            assert!(
                matches!(error, Error::Query(error) if error.detail == "DivisionByZero"),
                "{text}"
            );
        }
    }
    db.cypher(crate::cypher::Request::new(
        "UNWIND range(0,2) AS x CREATE (:N {key:x})",
    ))
    .await
    .unwrap();
    let response = db
        .cypher(crate::cypher::Request::new(
            "MATCH (n:N) WITH n AS a WITH a AS b SET b.key=b.key+1 RETURN b.key ORDER BY b.key",
        ))
        .await
        .unwrap();
    assert_eq!(
        response.rows,
        vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]]
    );
    assert!(db.cypher(crate::cypher::Request::new("CREATE (:Before) WITH 1 AS seed UNWIND range(1,30) AS x WITH x AS a WITH 1/(30-a) AS b RETURN b LIMIT 1")).await.is_err());
    assert_eq!(
        db.cypher(crate::cypher::Request::new(
            "MATCH (n:Before) RETURN count(*)"
        ))
        .await
        .unwrap()
        .rows,
        vec![vec![json!(0)]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn total_first_projection_windows_stop_large_valid_sources() {
    let db = crate::execution::interpreter::test_support::open_db("cypher-chain-termination").await;
    for (text, expected) in [
        (
            "UNWIND range(1,1000000000) AS x WITH x SKIP 5 LIMIT 1 WITH x AS y RETURN y",
            vec![vec![json!(6)]],
        ),
        (
            "UNWIND range(1,1000000000) AS x WITH x LIMIT 0 WITH x AS y RETURN count(y)",
            vec![vec![json!(0)]],
        ),
        (
            "MATCH (n:Absent) WITH n LIMIT 0 WITH n AS x RETURN x",
            vec![],
        ),
    ] {
        let plan = r::plan(
            helix_cypher::compile(text).unwrap(),
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        let interpreter = Interpreter::new(&db, context::ParamBindings::default());
        interpreter.ctx.fail_deadline_after(100);
        let result = interpreter
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
            .unwrap();
        assert_eq!(result.rows, expected);
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn long_projection_chains_use_an_iterative_execution_stack() {
    let db =
        crate::execution::interpreter::test_support::open_db("cypher-long-projection-chain").await;
    let text = format!("UNWIND [1] AS x {} RETURN x", "WITH x AS x ".repeat(1000));
    let plan = r::plan(
        helix_cypher::compile(&text).unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    assert!(matches!(
        plan.batch_consumer(0),
        Some(r::BatchConsumer::Pipeline { end: 1001, .. })
    ));
    let result = Interpreter::new(&db, context::ParamBindings::default())
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
    assert_eq!(result.rows, vec![vec![json!(1)]]);
    db.close().await.unwrap();
}
