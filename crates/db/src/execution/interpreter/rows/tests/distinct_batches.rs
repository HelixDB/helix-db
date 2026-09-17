use super::super::*;
use crate::execution::interpreter::test_support;
use helix_planner::context;
use serde_json::json;

#[tokio::test]
async fn batched_distinct_matches_materialized_values_scopes_and_windows() {
    let db = test_support::open_db("distinct-batch-semantics").await;
    db.cypher(crate::cypher::Request::new(
        "CREATE (a:N {k:0})-[:R]->(b:N {k:1}), (a)-[:R]->(b)",
    ))
    .await
    .unwrap();
    for query in [
        "UNWIND [null,2,1,1.0,null,2,3] AS x RETURN DISTINCT x",
        "UNWIND [3,1,2,3,1] AS x RETURN DISTINCT x SKIP 1 LIMIT 1",
        "UNWIND [] AS x RETURN DISTINCT x",
        "UNWIND [3,1,2,3,1] AS x WITH DISTINCT x UNWIND [x,x] AS y RETURN DISTINCT y",
        "UNWIND [null,3,1,2,3] AS x WITH x AS y WHERE y IS NOT NULL RETURN DISTINCT y LIMIT 2",
        "UNWIND [1,2,1] AS x WITH x AS a,3-x AS b RETURN DISTINCT b,a SKIP 1",
        "UNWIND [{a:1},{a:1.0},{a:null},{}] AS x RETURN DISTINCT x",
        "UNWIND [[1,null],[1.0,null],[2],[]] AS x RETURN DISTINCT x",
        "OPTIONAL MATCH (n:Missing) RETURN DISTINCT n",
        "MATCH p=(a:N)-[:R]->(b:N) RETURN DISTINCT p",
        "MATCH p=(a:N)-[:R]->(b:N) RETURN DISTINCT nodes(p),length(p)",
        "UNWIND [1,2,3] AS x WITH DISTINCT x AS y WHERE y<>2 RETURN y",
        "UNWIND [1,2,3] AS x RETURN DISTINCT x LIMIT 0",
    ] {
        let plan = r::plan(
            helix_cypher::compile(query).unwrap(),
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        assert!(
            plan.query()
                .operators()
                .iter()
                .enumerate()
                .any(|(index, _)| matches!(
                    plan.batch_consumer(index),
                    Some(r::BatchConsumer::Distinct | r::BatchConsumer::Pipeline { .. })
                )),
            "{query}"
        );
        let expected = Interpreter::new(&db, context::ParamBindings::default())
            .execute_rows(
                &plan.clone().with_execution(r::RowExecution::Materialized),
                &BTreeMap::new(),
                Limits::default(),
            )
            .await
            .unwrap();
        for batch_rows in [1, 2, 17, 512] {
            let result = Interpreter::new(&db, context::ParamBindings::default())
                .execute_rows(
                    &plan,
                    &BTreeMap::new(),
                    Limits {
                        batch_rows,
                        ..Limits::default()
                    },
                )
                .await
                .unwrap_or_else(|error| panic!("{query}: {error}"));
            assert_eq!(result.columns, expected.columns, "{query}");
            assert_eq!(result.rows, expected.rows, "{query}, batch {batch_rows}");
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn distinct_retention_scales_with_unique_rows_and_drains_late_errors() {
    let db = test_support::open_db("distinct-batch-bounds").await;
    let query = "UNWIND range(1,100000) AS x RETURN DISTINCT x%7 AS key";
    let plan = r::plan(
        helix_cypher::compile(query).unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    let result = Interpreter::new(&db, context::ParamBindings::default())
        .execute_rows(
            &plan,
            &BTreeMap::new(),
            Limits {
                memory_bytes: 2 * 1024 * 1024,
                batch_rows: 64,
                ..Limits::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        result.rows,
        (0..7).map(|key| vec![json!(key)]).collect::<Vec<_>>()
    );
    assert!(result.resources.peak_memory_bytes < 2 * 1024 * 1024);
    for query in [
        "UNWIND [1,2,0] AS x RETURN DISTINCT 1/x LIMIT 1",
        "UNWIND [1,2,0] AS x WITH x AS y RETURN DISTINCT 1/y LIMIT 0",
        "UNWIND [1,2] AS x RETURN DISTINCT x LIMIT 1/0",
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
                        ..Limits::default()
                    },
                )
                .await
                .unwrap_err();
            assert!(
                matches!(error, Error::Query(ref error) if error.detail=="DivisionByZero"),
                "{query}: {error}"
            );
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn distinct_memory_exhaustion_and_cancellation_rollback_prior_writes() {
    let db = test_support::open_db("distinct-batch-rollback").await;
    let query = "CREATE (:Transient) WITH 1 AS marker UNWIND range(1,10000) AS x RETURN DISTINCT x";
    let error = crate::cypher::execute(
        &db,
        crate::cypher::Request::new(query),
        crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
        crate::query_service::QueryMode::Execute,
        crate::execution_control::ExecutionControl::unlimited(),
        Limits {
            memory_bytes: 64 * 1024,
            batch_rows: 1,
            ..Limits::default()
        },
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error,Error::Query(ref error) if error.detail=="MemoryLimit"),
        "{error}"
    );
    assert_eq!(
        db.cypher(crate::cypher::Request::new(
            "MATCH(n:Transient) RETURN count(n)"
        ))
        .await
        .unwrap()
        .rows,
        vec![vec![json!(0)]]
    );
    let plan = r::plan(
        helix_cypher::compile(query).unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    let interpreter = Interpreter::new(&db, context::ParamBindings::default());
    interpreter.ctx.fail_deadline_after(200);
    let error = interpreter
        .execute_rows(
            &plan,
            &BTreeMap::new(),
            Limits {
                batch_rows: 1,
                ..Limits::default()
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            Error::Storage(crate::HelixDbError::QueryDeadlineExceeded)
        ),
        "{error}"
    );
    assert_eq!(
        db.cypher(crate::cypher::Request::new(
            "MATCH(n:Transient) RETURN count(n)"
        ))
        .await
        .unwrap()
        .rows,
        vec![vec![json!(0)]]
    );
    db.close().await.unwrap();
}
