use super::super::*;
use crate::execution::interpreter::test_support;
use helix_planner::context;
use serde_json::json;

#[tokio::test]
async fn correlated_disconnected_patterns_preserve_duplicates_and_optional_scope() {
    let db = test_support::open_db("correlated-source-model").await;
    db.cypher(crate::cypher::Request::new(
        "CREATE (:A {k:0}),(:A {k:1}),(:A {k:1}),(:B {k:1}),(:B {k:2})",
    ))
    .await
    .unwrap();
    let mut expected = Vec::new();
    for key in [1, 1, 2] {
        let before = expected.len();
        for a in [0, 1, 1] {
            for b in [1, 2] {
                if a == key && b == a {
                    expected.push(vec![json!(key), json!(a), json!(b)]);
                }
            }
        }
        if before == expected.len() {
            expected.push(vec![json!(key), json!(null), json!(null)]);
        }
    }
    for (text, mut expected) in [
        ("UNWIND [1,1,2] AS key OPTIONAL MATCH (a:A),(b:B) WHERE a.k=key AND b.k=a.k RETURN key,a.k,b.k",expected),
        ("UNWIND [1,1] AS key MATCH (a:A),(b:B) RETURN count(*),sum(key)",vec![vec![json!(12),json!(12)]]),
        ("UNWIND [1,null] AS key OPTIONAL MATCH (a:A),(b:B) WHERE a.k=key RETURN count(*),count(a),count(b)",vec![vec![json!(5),json!(4),json!(4)]]),
        ("UNWIND [] AS key OPTIONAL MATCH (a:A),(b:B) RETURN count(*)",vec![vec![json!(0)]]),
        ("UNWIND [1,2] AS key OPTIONAL MATCH (a:A),(b:Absent) RETURN key,a,b",vec![vec![json!(1),json!(null),json!(null)],vec![json!(2),json!(null),json!(null)]]),
        ("UNWIND [1,2] AS key MATCH (a:A),(b:B) RETURN key,a.k,b.k ORDER BY key DESC,a.k DESC,b.k DESC LIMIT 2",vec![vec![json!(2),json!(1),json!(2)],vec![json!(2),json!(1),json!(2)]]),
    ] {
        let query=helix_cypher::compile(text).unwrap();
        let plan=r::plan(query.clone(),&db.planner_context(context::ParamBindings::default())).unwrap();
        assert!(matches!(plan.batch_consumer(0),Some(r::BatchConsumer::Pipeline{..})),"{text}");
        assert!(plan.matches()[&1].steps.iter().all(|step|!matches!(step,r::MatchStep::IndexLookup(_))));
        if !text.contains("ORDER BY") { expected.sort_by_key(|row|serde_json::to_string(row).unwrap()); }
        for batch_rows in [1,2,7] {
            for strategy in [plan.clone(),plan.clone().with_execution(r::RowExecution::Materialized),r::RowPlan::reference(query.clone()).unwrap()] {
                let mut actual=Interpreter::new(&db,context::ParamBindings::default()).execute_rows(&strategy,&BTreeMap::new(),Limits{batch_rows,..Default::default()}).await.unwrap().rows;
                if !text.contains("ORDER BY") { actual.sort_by_key(|row|serde_json::to_string(row).unwrap()); }
                assert_eq!(actual,expected,"{text}; batch={batch_rows}");
            }
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn correlated_sources_build_once_across_outer_batches_and_bound_cardinality() {
    let db = test_support::open_db("correlated-source-budget").await;
    db.cypher(crate::cypher::Request::new(
        "UNWIND range(0,31) AS k CREATE (:A {k:k}) WITH k WHERE k<16 CREATE (:B {k:k})",
    ))
    .await
    .unwrap();
    for (predicate, inner) in [("", 32 * 16), ("WHERE a.k=b.k", 16)] {
        let text = format!(
            "UNWIND range(1,128) AS outer MATCH (a:A),(b:B) {predicate} RETURN count(*),sum(outer)"
        );
        let plan = r::plan(
            helix_cypher::compile(&text).unwrap(),
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        assert!(matches!(
            plan.batch_consumer(0),
            Some(r::BatchConsumer::Pipeline { .. })
        ));
        assert_eq!(
            plan.matches()[&1]
                .steps
                .iter()
                .any(|step| matches!(step, r::MatchStep::HashJoin { .. })),
            !predicate.is_empty()
        );
        for batch_rows in [8, 17] {
            let limits = Limits {
                memory_bytes: 192 * 1024,
                batch_rows,
                ..Default::default()
            };
            let response = Interpreter::new(&db, context::ParamBindings::default())
                .execute_rows(&plan, &BTreeMap::new(), limits)
                .await
                .unwrap();
            assert_eq!(
                response.rows,
                vec![vec![json!(128 * inner), json!(inner * 128 * 129 / 2)]]
            );
            assert!(response.resources.peak_memory_bytes <= limits.memory_bytes);
            assert_eq!(
                response.resources.reads.point_gets, 2,
                "source caches span all 128 outer rows"
            );
            assert_eq!(response.resources.reads.scans, 0);
        }
        if predicate.is_empty() {
            let error = Interpreter::new(&db, context::ParamBindings::default())
                .execute_rows(
                    &plan.with_execution(r::RowExecution::Materialized),
                    &BTreeMap::new(),
                    Limits {
                        memory_bytes: 192 * 1024,
                        batch_rows: 8,
                        ..Default::default()
                    },
                )
                .await
                .unwrap_err();
            assert!(
                matches!(&error,Error::Query(error) if error.detail=="MemoryLimit")
                    || matches!(
                        &error,
                        Error::Storage(crate::HelixDbError::QueryMemoryLimitExceeded)
                    )
            );
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn correlated_source_boundaries_keep_write_visibility_errors_and_rollback() {
    let db = test_support::open_db("correlated-source-writes").await;
    assert_eq!(db.cypher(crate::cypher::Request::new("CREATE (:A {k:1}),(:B {k:1}) WITH 1 AS marker UNWIND [1,2] AS key MATCH (a:A),(b:B) RETURN count(*)")).await.unwrap().rows,vec![vec![json!(2)]]);
    db.cypher(crate::cypher::Request::new(
        "MATCH (a:A),(b:B) CREATE (a)-[:R]->(b)",
    ))
    .await
    .unwrap();
    assert_eq!(db.cypher(crate::cypher::Request::new("MATCH (n:A) UNWIND [n,null] AS start OPTIONAL MATCH (start)-[:R]->(b:B),(c:A) RETURN count(*),count(b),count(c)")).await.unwrap().rows,vec![vec![json!(2),json!(1),json!(1)]]);
    for (text, detail) in [
        ("UNWIND [null,1] AS start OPTIONAL MATCH (start),(a:A),(b:B) RETURN a LIMIT 0","ExpectedNode"),
        ("UNWIND [null] AS left UNWIND [1] AS right OPTIONAL MATCH (left),(right),(a:A) RETURN a LIMIT 0","ExpectedNode"),
        ("UNWIND [1,2] AS key MATCH (a:A),(b:B) WHERE 1/(2-key)>0 RETURN a LIMIT 1","DivisionByZero"),
        ("CREATE (:Rollback) WITH 1 AS marker UNWIND [1,2] AS key MATCH (a:A),(b:B) WHERE 1/(2-key)>0 RETURN a LIMIT 1","DivisionByZero"),
    ] {
        let plan=r::plan(helix_cypher::compile(text).unwrap(),&db.planner_context(context::ParamBindings::default())).unwrap();
        for batch_rows in [1,2,7] {
            for strategy in [r::RowExecution::Batched,r::RowExecution::Materialized] {
                let error=Interpreter::new(&db,context::ParamBindings::default()).execute_rows(&plan.clone().with_execution(strategy),&BTreeMap::new(),Limits{batch_rows,..Default::default()}).await.unwrap_err();
                assert!(matches!(error,Error::Query(error) if error.detail==detail),"{text}");
            }
        }
    }
    assert_eq!(
        db.cypher(crate::cypher::Request::new(
            "MATCH (:Rollback) RETURN count(*)"
        ))
        .await
        .unwrap()
        .rows,
        vec![vec![json!(0)]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn correlated_source_owners_survive_cursor_changes_and_release_on_failure() {
    use super::super::bound_match::{BoundCursor, BoundMatch};
    let db = test_support::open_db("correlated-source-ownership").await;
    db.cypher(crate::cypher::Request::new(
        "UNWIND [1,2] AS key CREATE (:A {key:key}),(:B {key:key})",
    ))
    .await
    .unwrap();
    let plan = r::plan(
        helix_cypher::compile(
            "UNWIND [1] AS marker OPTIONAL MATCH (a:A),(b:B) WHERE a.key=b.key RETURN marker,a,b",
        )
        .unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    let r::Operator::Match {
        pattern,
        optional,
        predicate,
    } = &plan.query().operators()[1]
    else {
        unreachable!()
    };
    let operation = matches::Match {
        pattern,
        optional: *optional,
        predicate: predicate.as_deref(),
        demand: usize::MAX,
    };
    for failure in ["drop", "cancel", "memory"] {
        let limits = Limits {
            memory_bytes: 192 * 1024,
            batch_rows: 1,
            ..Default::default()
        };
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.row_memory = Some(memory::Budget::new(limits.memory_bytes));
        ctx.enable_request_read_view().await.unwrap();
        let mut stage = BoundMatch::new(operation, &plan.matches()[&1]);
        for marker in [1, 99] {
            let mut rows = RowBuffer::new(ctx.row_budget()).unwrap();
            for _ in 0..3 {
                let mut row = vec![r::Value::Null; plan.query().bindings().len()];
                row[0] = r::Value::Integer(marker);
                push_row(&mut rows, row, limits).unwrap();
            }
            let mut cursor = BoundCursor::new(rows.finish(), ctx.row_budget()).unwrap();
            let first = stage
                .next_batch(&mut cursor, &ctx, &BTreeMap::new(), limits)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(first[0][0], r::Value::Integer(marker));
            drop(first);
            assert_eq!(
                ctx.row_budget().reads().point_gets,
                2,
                "both cursors share their source builds"
            );
            let held = if marker == 99 && failure == "memory" {
                Some(
                    ctx.row_budget()
                        .reserve(ctx.row_budget().available())
                        .unwrap(),
                )
            } else {
                None
            };
            if marker == 99 && failure != "drop" {
                if failure == "cancel" {
                    ctx.fail_deadline_after(0);
                }
                let error = stage
                    .next_batch(&mut cursor, &ctx, &BTreeMap::new(), limits)
                    .await
                    .err()
                    .expect("failure must interrupt the resumed cursor");
                assert!(
                    matches!(
                        &error,
                        Error::Storage(
                            crate::HelixDbError::QueryDeadlineExceeded
                                | crate::HelixDbError::QueryMemoryLimitExceeded
                        )
                    ) || matches!(&error,Error::Query(error) if error.detail=="MemoryLimit")
                );
            }
            drop(cursor);
            drop(held);
            assert!(
                ctx.row_budget().available() < limits.memory_bytes,
                "stage owns reusable sources after cursor drop"
            );
        }
        drop(stage);
        assert_eq!(ctx.row_budget().available(), limits.memory_bytes);
    }
    db.close().await.unwrap();
}
