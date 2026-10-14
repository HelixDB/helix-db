//! A downstream window may stop a source across an OPTIONAL MATCH that cannot
//! fail. Aggregates keep results deterministic while LIMIT selects any rows.
use super::super::*;
use helix_planner::context;
use serde_json::json;

pub(super) async fn execute(
    db: &crate::HelixDB,
    plan: &r::RowPlan,
    strategy: r::RowExecution,
    batch_rows: usize,
) -> Result<crate::cypher::Response> {
    Interpreter::new(db, context::ParamBindings::default())
        .execute_rows(
            &plan.clone().with_execution(strategy),
            &BTreeMap::new(),
            Limits {
                batch_rows,
                ..Default::default()
            },
        )
        .await
}

pub(super) fn plan(db: &crate::HelixDB, text: &str) -> r::RowPlan {
    r::plan(
        helix_cypher::compile(text).unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap()
}

#[tokio::test]
async fn proven_optional_matches_stop_unbounded_sources() {
    let db =
        crate::execution::interpreter::test_support::open_db("cypher-optional-window-stop").await;
    for (text, expected) in [
        (
            "UNWIND range(1,1000000000) AS x OPTIONAL MATCH (n:Absent) RETURN x, n LIMIT 2",
            json!([[1, null], [2, null]]),
        ),
        (
            "UNWIND range(1,1000000000) AS x OPTIONAL MATCH (n:Absent) WITH x, n SKIP 3 RETURN x LIMIT 1",
            json!([[4]]),
        ),
    ] {
        let plan = plan(&db, text);
        assert!(plan.input_window(0).is_some(), "{text}");
        let interpreter = Interpreter::new(&db, context::ParamBindings::default());
        // Draining the range would exceed this execution-check budget.
        interpreter.ctx.fail_deadline_after(500);
        let response = interpreter
            .execute_rows(
                &plan,
                &BTreeMap::new(),
                Limits {
                    batch_rows: 2,
                    ..Default::default()
                },
            )
            .await
            .unwrap_or_else(|error| panic!("{text}: {error}"));
        assert_eq!(serde_json::to_value(response.rows).unwrap(), expected, "{text}");
    }
}

#[tokio::test]
async fn proven_optional_matches_expand_only_demanded_parents() {
    let db =
        crate::execution::interpreter::test_support::open_db("cypher-optional-window-reads").await;
    for statement in [
        "UNWIND range(0,255) AS k CREATE (:OptLeft {k:k})",
        "MATCH (a:OptLeft) WHERE a.k % 4 = 0 CREATE (a)-[:R]->(:OptRight {k:a.k})",
    ] {
        db.cypher(crate::cypher::Request::new(statement))
            .await
            .unwrap();
    }
    for (text, expected) in [
        (
            "MATCH (a:OptLeft) OPTIONAL MATCH (a)-[:R]->(b) WITH a, b LIMIT 3 RETURN count(*), count(a)",
            json!([[3, 3]]),
        ),
        (
            "MATCH (a:OptLeft) OPTIONAL MATCH (a)-[:MISSING]->(b) WITH a, b SKIP 2 LIMIT 2 RETURN count(*), count(b)",
            json!([[2, 0]]),
        ),
        (
            "MATCH (a:OptLeft) WITH a SKIP 5 OPTIONAL MATCH p=(a)-[:R]->(:OptRight) WITH p LIMIT 4 RETURN count(*)",
            json!([[4]]),
        ),
        (
            "MATCH (a:OptLeft) OPTIONAL MATCH (a)-[:R]->(b) WITH a, b LIMIT 0 RETURN count(*)",
            json!([[0]]),
        ),
    ] {
        let plan = plan(&db, text);
        assert!(plan.input_window(0).is_some(), "{text}");
        assert!(plan.batch_consumer(0).is_some(), "{text}");
        let reference = execute(&db, &plan, r::RowExecution::Materialized, 512)
            .await
            .unwrap();
        assert_eq!(serde_json::to_value(&reference.rows).unwrap(), expected, "{text}");
        // The reference expands every unskipped parent with one serial
        // adjacency read.
        assert!(reference.resources.reads.point_gets >= 250, "{text}");
        for batch_rows in [1, 2, 7, 512] {
            let response = execute(&db, &plan, r::RowExecution::Batched, batch_rows)
                .await
                .unwrap();
            assert_eq!(response.rows, reference.rows, "{text} at {batch_rows}");
            let reads = response.resources.reads;
            assert!(
                reads.scans == 0 && reads.point_gets <= 16 && reads.multi_get_keys <= 64,
                "{text} at {batch_rows}: {reads:?}"
            );
        }
    }
}

#[tokio::test]
async fn proven_optional_matches_stop_hub_parents_and_feed_later_windows() {
    let db =
        crate::execution::interpreter::test_support::open_db("cypher-optional-window-hub").await;
    for statement in [
        "CREATE (:Hub {k:0})",
        "MATCH (h:Hub {k:0}) UNWIND range(1,400) AS i CREATE (h)-[:R]->(:Leaf {i:i})",
        "UNWIND range(1,3) AS k CREATE (:Hub {k:k})",
    ] {
        db.cypher(crate::cypher::Request::new(statement))
            .await
            .unwrap();
    }
    for (text, expected, keys) in [
        (
            "MATCH (a:Hub) OPTIONAL MATCH (a)-[:R]->(b) WITH a, b LIMIT 1 RETURN count(*)",
            json!([[1]]),
            // Between the stopped hub (~1022 keys) and a drained one (>2400).
            Some(1536),
        ),
        (
            "MATCH (a:Hub) WITH a LIMIT 2 OPTIONAL MATCH (a)-[:R]->(b) WITH a, b LIMIT 50 RETURN count(*)",
            json!([[50]]),
            None,
        ),
        (
            "MATCH (a:Hub) OPTIONAL MATCH (a)-[:R]->(b) WITH a, b SKIP 170 LIMIT 5 RETURN count(*), count(b)",
            json!([[5, 5]]),
            None,
        ),
        (
            "MATCH (a:Hub) OPTIONAL MATCH (a)-[:R]->(b) RETURN count(*), count(b)",
            json!([[403, 400]]),
            None,
        ),
    ] {
        let plan = plan(&db, text);
        let reference = execute(&db, &plan, r::RowExecution::Materialized, 512)
            .await
            .unwrap();
        assert_eq!(serde_json::to_value(&reference.rows).unwrap(), expected, "{text}");
        for batch_rows in [1, 2, 7, 512] {
            let response = execute(&db, &plan, r::RowExecution::Batched, batch_rows)
                .await
                .unwrap();
            assert_eq!(response.rows, reference.rows, "{text} at {batch_rows}");
            if let Some(keys) = keys {
                assert!(
                    response.resources.reads.multi_get_keys <= keys,
                    "{text} at {batch_rows}: {:?}",
                    response.resources.reads
                );
            }
        }
    }
}

#[tokio::test]
async fn proven_optional_matches_keep_late_errors_and_rollback() {
    let db =
        crate::execution::interpreter::test_support::open_db("cypher-optional-window-errors").await;
    db.cypher(crate::cypher::Request::new(
        "UNWIND range(0,31) AS k CREATE (:OptLeft {k:k})-[:R]->(:OptRight {k:k})",
    ))
    .await
    .unwrap();
    for text in [
        "UNWIND [null, 1] AS a OPTIONAL MATCH (a)-[:R]->(b) RETURN a, b LIMIT 1",
        "UNWIND [null, 1] AS r OPTIONAL MATCH ()-[r]->() RETURN r LIMIT 1",
        "MATCH (a:OptLeft) OPTIONAL MATCH (a)-[:R]->(b) WHERE 1 / (a.k - 31) > 0 RETURN a, b LIMIT 1",
        "MATCH (a:OptLeft) OPTIONAL MATCH (a)-[:R]->(b {k: 1 / (a.k - 31)}) RETURN a, b LIMIT 1",
    ] {
        let plan = plan(&db, text);
        assert!(plan.input_window(0).is_none(), "{text}");
        for (strategy, batch_rows) in [
            (r::RowExecution::Materialized, 512),
            (r::RowExecution::Batched, 1),
            (r::RowExecution::Batched, 7),
        ] {
            let error = execute(&db, &plan, strategy, batch_rows).await.unwrap_err();
            assert!(
                matches!(&error, Error::Query(error) if error.category == "type_error" || error.detail == "division_by_zero"),
                "{text}: {error}"
            );
        }
    }
    let error = db
        .cypher(crate::cypher::Request::new(
            "MATCH (a:OptLeft) OPTIONAL MATCH (a)-[:R]->(b) WITH a, b LIMIT 1 CREATE (:OptMarker) WITH 1 AS x RETURN 1 / 0",
        ))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("division_by_zero"), "{error}");
    for (statement, expected) in [
        ("MATCH (m:OptMarker) RETURN count(m)", json!([[0]])),
        (
            "MATCH (a:OptLeft) OPTIONAL MATCH (a)-[:R]->(b) WITH a, b LIMIT 2 CREATE (:OptMarker) RETURN count(*)",
            json!([[2]]),
        ),
        ("MATCH (m:OptMarker) RETURN count(m)", json!([[2]])),
    ] {
        let response = db.cypher(crate::cypher::Request::new(statement)).await.unwrap();
        assert_eq!(serde_json::to_value(response.rows).unwrap(), expected, "{statement}");
    }
}
