use super::super::*;
use crate::execution::interpreter::test_support;
use helix_planner::context;
use serde_json::json;

#[tokio::test]
async fn correlated_pipelines_bound_outer_rows_and_preserve_optional_multiplicity() {
    let db = test_support::open_db_with_config(
        test_support::in_memory_config("correlated-bounded").with_equality_index("N", "key"),
    )
    .await;
    db.cypher(crate::cypher::Request::new(
        "UNWIND range(0,30) AS k CREATE (:N {key:k})",
    ))
    .await
    .unwrap();
    let text = "UNWIND range(0,4095) AS i WITH i%32 AS k OPTIONAL MATCH (n:N {key:k}) UNWIND [n.key,null] AS value RETURN count(*),count(value),sum(value)";
    let plan = r::plan(
        helix_cypher::compile(text).unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    assert!(matches!(
        plan.batch_consumer(0),
        Some(r::BatchConsumer::Pipeline { .. })
    ));
    assert!(plan
        .matches()
        .values()
        .any(|plan| matches!(plan.steps.as_slice(), [r::MatchStep::IndexLookup(_)])));
    let values: Vec<_> = (0_i64..4096)
        .flat_map(|i| [((i % 32) < 31).then_some(i % 32), None])
        .collect();
    let expected = vec![vec![
        json!(values.len()),
        json!(values.iter().flatten().count()),
        json!(values.into_iter().flatten().sum::<i64>()),
    ]];
    for batch_rows in [1, 8, 17] {
        let limits = Limits {
            batch_rows,
            memory_bytes: 96 * 1024,
            ..Default::default()
        };
        let response = Interpreter::new(&db, context::ParamBindings::default())
            .execute_rows(&plan, &BTreeMap::new(), limits)
            .await
            .unwrap();
        assert_eq!(response.rows, expected);
        assert!(response.resources.peak_memory_bytes <= limits.memory_bytes);
        assert_eq!(response.resources.reads.scans, 0);
        assert!(response.resources.reads.point_gets <= 2 * 4096);
        assert!(
            response.resources.reads.multi_get_batches
                <= 4 * 4096_usize.div_ceil((batch_rows / 2).max(1)) + 16,
            "candidate properties must hydrate across outer rows: {:?}",
            response.resources.reads,
        );
    }
    let reference = plan.with_execution(r::RowExecution::Materialized);
    assert_eq!(
        Interpreter::new(&db, context::ParamBindings::default())
            .execute_rows(&reference, &BTreeMap::new(), Limits::default())
            .await
            .unwrap()
            .rows,
        expected
    );
    assert!(
        matches!(Interpreter::new(&db, context::ParamBindings::default()).execute_rows(&reference, &BTreeMap::new(), Limits { batch_rows:8, memory_bytes:96*1024, ..Default::default() }).await, Err(Error::Query(error)) if error.detail=="MemoryLimit")
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn correlated_scan_cache_is_reused_across_outer_batches() {
    let db = test_support::open_db("correlated-cache").await;
    db.cypher(crate::cypher::Request::new(
        "UNWIND range(0,64) AS k CREATE (:N {key:k})",
    ))
    .await
    .unwrap();
    let text =
        "UNWIND range(0,256) AS i MATCH (n:N) WHERE (n.key+i)%31=0 RETURN count(*),sum(n.key+i)";
    let plan = r::plan(
        helix_cypher::compile(text).unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    let expected: Vec<_> = (0_i64..257)
        .flat_map(|i| (0..65).map(move |k| k + i))
        .filter(|value| value % 31 == 0)
        .collect();
    for batch_rows in [3, 16] {
        let response = Interpreter::new(&db, context::ParamBindings::default())
            .execute_rows(
                &plan,
                &BTreeMap::new(),
                Limits {
                    batch_rows,
                    memory_bytes: 96 * 1024,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            response.rows,
            vec![vec![
                json!(expected.len()),
                json!(expected.iter().sum::<i64>())
            ]]
        );
        assert_eq!(
            response.resources.reads.point_gets, 1,
            "source cache is opened only once"
        );
        assert_eq!(response.resources.reads.scans, 0);
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn correlated_pipeline_scopes_paths_nulls_and_fallback_probes_match_reference() {
    let db = test_support::open_db_with_config(
        test_support::in_memory_config("correlated-values").with_equality_index("N", "key"),
    )
    .await;
    db.cypher(crate::cypher::Request::new("CREATE (:N {key:1}),(:N {key:1}),(:N {key:2}),(:N {key:[1,2]}),(:N {key:'text'}),(:N {key:true})")).await.unwrap();
    for (text, expected) in [
        ("UNWIND [1,1,2,3,null] AS k OPTIONAL MATCH (n:N {key:k}) RETURN k,n.key", vec![vec![json!(1),json!(1)];4].into_iter().chain([vec![json!(2),json!(2)],vec![json!(3),json!(null)],vec![json!(null),json!(null)]]).collect()),
        ("UNWIND [1,2,3] AS k OPTIONAL MATCH p=(n:N {key:k}) WHERE n.key=2 RETURN k,length(p)", vec![vec![json!(1),json!(null)],vec![json!(2),json!(0)],vec![json!(3),json!(null)]]),
        ("UNWIND [1,2,3] AS k OPTIONAL MATCH (n:N {key:k}) OPTIONAL MATCH (n:N {key:2}) RETURN k,n.key",vec![vec![json!(1),json!(1)],vec![json!(1),json!(1)],vec![json!(2),json!(2)],vec![json!(3),json!(null)]]),
        ("UNWIND [[1,2],[1.0,2.0],{},null,true,'text'] AS k OPTIONAL MATCH (n:N {key:k}) RETURN count(*),count(n)",vec![vec![json!(6),json!(4)]]),
        ("UNWIND [] AS k OPTIONAL MATCH (n:N {key:k}) RETURN count(*)",vec![vec![json!(0)]]),
        ("UNWIND [1,2,3] AS k OPTIONAL MATCH (n:Absent) RETURN count(*),count(n)",vec![vec![json!(3),json!(0)]]),
        ("UNWIND [1,2,3] AS k MATCH (n:N {key:k}),(n:N) RETURN count(*)",vec![vec![json!(3)]]),
        ("UNWIND [1,2,3] AS k MATCH (n:N {key:k}) WITH n.key AS k UNWIND [k,k] AS j OPTIONAL MATCH (m:N {key:j}) RETURN count(*)",vec![vec![json!(10)]]),
        ("UNWIND [1,2] AS k MATCH (n:N {key:k}) RETURN n.key AS key ORDER BY key DESC LIMIT 2",vec![vec![json!(2)],vec![json!(1)]]),
        ("UNWIND [1,2,3] AS k OPTIONAL MATCH (n:N {key:k}) WITH k,n SKIP 1 LIMIT 2 UNWIND [k,k+10] AS j RETURN j",vec![vec![json!(1)],vec![json!(11)],vec![json!(2)],vec![json!(12)]]),
    ] {
        let query = helix_cypher::compile(text).unwrap();
        let plan = r::plan(query.clone(), &db.planner_context(context::ParamBindings::default())).unwrap();
        assert!(matches!(plan.batch_consumer(0), Some(r::BatchConsumer::Pipeline { .. })), "{text}");
        for batch_rows in [1,2,7] {
            for strategy in [plan.clone(),plan.clone().with_execution(r::RowExecution::Materialized),r::RowPlan::reference(query.clone()).unwrap()] {
                let actual = Interpreter::new(&db, context::ParamBindings::default()).execute_rows(&strategy, &BTreeMap::new(), Limits {batch_rows, ..Default::default()}).await.unwrap();
                assert_eq!(actual.rows, expected, "{text}; batch={batch_rows}");
            }
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn correlated_pipelines_flush_pending_indexes_and_drain_late_errors() {
    let db = test_support::open_db_with_config(
        test_support::in_memory_config("correlated-visibility").with_equality_index("N", "key"),
    )
    .await;
    assert_eq!(db.cypher(crate::cypher::Request::new("CREATE (:N {key:91}) WITH 1 AS seed UNWIND [91,92] AS k OPTIONAL MATCH (n:N {key:k}) RETURN count(*),count(n)")).await.unwrap().rows, vec![vec![json!(2),json!(1)]]);
    assert_eq!(db.cypher(crate::cypher::Request::new("MATCH (n:N {key:91}) SET n.key=92 WITH 1 AS seed UNWIND [91,92] AS k MATCH (m:N {key:k}) RETURN m.key")).await.unwrap().rows, vec![vec![json!(92)]]);
    for (text, detail) in [
        (
            "UNWIND [null,1] AS n OPTIONAL MATCH (n) RETURN n LIMIT 1",
            "ExpectedNode",
        ),
        (
            "UNWIND [1,0] AS k OPTIONAL MATCH (n:N) WHERE 1/k>0 RETURN k LIMIT 0",
            "DivisionByZero",
        ),
        (
            "CREATE (:Rollback) WITH 1 AS x UNWIND [1,0] AS k MATCH (n:N) RETURN 1/k LIMIT 1",
            "DivisionByZero",
        ),
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
                        batch_rows: 1,
                        ..Default::default()
                    },
                )
                .await
                .unwrap_err();
            assert!(
                matches!(error,Error::Query(error) if error.detail==detail),
                "{text}"
            );
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
    db.close().await.unwrap();
}

#[tokio::test]
async fn correlated_continuations_keep_deep_clause_stacks_bounded() {
    let db = test_support::open_db("correlated-stack").await;
    db.cypher(crate::cypher::Request::new("CREATE (:N)"))
        .await
        .unwrap();
    let mut text = String::from("UNWIND [1] AS x MATCH (n:N) ");
    for _ in 0..600 {
        text.push_str("OPTIONAL MATCH (n) ");
    }
    text.push_str("RETURN count(*)");
    assert_eq!(
        db.cypher(crate::cypher::Request::new(text))
            .await
            .unwrap()
            .rows,
        vec![vec![json!(1)]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn correlated_cancellation_and_early_drop_release_cache_and_active_lookup() {
    use super::super::correlated::{MatchCursor, NodeMatch};
    let db = test_support::open_db_with_config(
        test_support::in_memory_config("correlated-release").with_equality_index("N", "key"),
    )
    .await;
    db.cypher(crate::cypher::Request::new(
        "CREATE (:N {key:1}),(:N {key:1}),(:N {key:2})",
    ))
    .await
    .unwrap();
    for fallback in [false, true] {
        let text = "UNWIND [1,2] AS key OPTIONAL MATCH (n:N {key:key}) RETURN n.key";
        let plan = r::plan(
            helix_cypher::compile(text).unwrap(),
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
        let r::Operator::Unwind { slot: probe, .. } = &plan.query().operators()[0] else {
            unreachable!()
        };
        for cancel_after in [None, Some(0), Some(1), Some(4), Some(12)] {
            let limits = Limits {
                batch_rows: 1,
                memory_bytes: 64 * 1024,
                ..Default::default()
            };
            let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
            ctx.row_memory = Some(memory::Budget::new(limits.memory_bytes));
            ctx.enable_request_read_view().await.unwrap();
            let mut input = RowBuffer::new(ctx.row_budget()).unwrap();
            for value in [1, 2].into_iter().cycle().take(32) {
                let mut row = vec![r::Value::Null; plan.query().bindings().len()];
                // Exercise both the physical index cursor and the non-indexable
                // value fallback; plain scans now use the shared graph stack.
                row[probe.0 as usize] = if fallback {
                    r::Value::List(vec![r::Value::Integer(value)])
                } else {
                    r::Value::Integer(value)
                };
                push_row(&mut input, row, limits).unwrap();
            }
            let mut cursor = MatchCursor::new(input.finish(), ctx.row_budget()).unwrap();
            let mut stage = NodeMatch::new(
                matches::Match {
                    pattern,
                    optional: *optional,
                    predicate: predicate.as_deref(),
                    demand: usize::MAX,
                },
                &plan.matches()[&1],
            );
            let parameters = BTreeMap::new();
            let first = stage
                .next_batch(&mut cursor, &ctx, &parameters, limits)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(first.len(), 1);
            drop(first);
            assert!(ctx.row_budget().available() < limits.memory_bytes);
            if let Some(checkpoints) = cancel_after {
                ctx.fail_deadline_after(checkpoints);
                loop {
                    match stage
                        .next_batch(&mut cursor, &ctx, &parameters, limits)
                        .await
                    {
                        Ok(Some(_)) => {}
                        Err(Error::Storage(crate::HelixDbError::QueryDeadlineExceeded)) => break,
                        Ok(None) => panic!("cancellation did not interrupt the input"),
                        Err(error) => panic!("unexpected cancellation error: {error:?}"),
                    }
                }
            }
            drop(cursor);
            drop(stage);
            assert_eq!(ctx.row_budget().available(), limits.memory_bytes);
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn correlated_batch_errors_preserve_candidate_order_and_drain_after_limits() {
    let db = test_support::open_db("correlated-error-order").await;
    db.cypher(crate::cypher::Request::new("CREATE (:N {key:1})"))
        .await
        .unwrap();
    for (values, detail) in [
        ("[n,1]", "DivisionByZero"),
        ("[1,n]", "ExpectedNode"),
        ("[n,null,1]", "DivisionByZero"),
    ] {
        for limit in [0, 1] {
            let text=format!("MATCH (n:N) WITH {values} AS values UNWIND values AS candidate MATCH (candidate) WHERE 1/0>0 RETURN candidate LIMIT {limit}");
            let plan = r::plan(
                helix_cypher::compile(&text).unwrap(),
                &db.planner_context(context::ParamBindings::default()),
            )
            .unwrap();
            assert!(matches!(
                plan.batch_consumer(0),
                Some(r::BatchConsumer::Pipeline { .. })
            ));
            for batch_rows in [1, 2, 3, 8] {
                for strategy in [r::RowExecution::Batched, r::RowExecution::Materialized] {
                    let error = Interpreter::new(&db, context::ParamBindings::default())
                        .execute_rows(
                            &plan.clone().with_execution(strategy),
                            &BTreeMap::new(),
                            Limits {
                                batch_rows,
                                ..Default::default()
                            },
                        )
                        .await
                        .unwrap_err();
                    assert!(
                        matches!(error,Error::Query(error) if error.detail==detail),
                        "{text}, batch={batch_rows}, strategy={strategy:?}"
                    );
                }
            }
        }
    }
    // The source error must still be observed after a successful prefix filled
    // RETURN's limit. No mutation preceding the pipeline may survive that error.
    let text="CREATE (:Rollback) WITH 1 AS marker MATCH (n:N) WITH [n,1] AS values UNWIND values AS candidate OPTIONAL MATCH (candidate) RETURN candidate.key LIMIT 1";
    let error = db
        .cypher(crate::cypher::Request::new(text))
        .await
        .unwrap_err();
    assert!(matches!(error,Error::Query(error) if error.detail=="ExpectedNode"));
    assert_eq!(
        db.cypher(crate::cypher::Request::new(
            "MATCH (n:Rollback) RETURN count(*)"
        ))
        .await
        .unwrap()
        .rows,
        vec![vec![json!(0)]]
    );
    db.close().await.unwrap();
}
