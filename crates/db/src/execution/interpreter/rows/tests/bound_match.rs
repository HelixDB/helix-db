use super::super::*;
use crate::execution::interpreter::test_support;
use helix_planner::context;
use serde_json::json;

#[tokio::test]
async fn bound_expansions_match_independent_paths_and_optional_outer_rows() {
    let db = test_support::open_db("bound-pattern-model").await;
    db.cypher(crate::cypher::Request::new("CREATE (a:N {k:0}),(b:N {k:1}),(c:N {k:2}),(d:N {k:3}),(a)-[:R]->(b),(a)-[:R]->(b),(b)-[:R]->(c),(c)-[:R]->(a),(a)-[:R]->(a),(a)-[:S]->(c)"))
        .await.unwrap();
    let edges = [(0, 1), (0, 1), (1, 2), (2, 0), (0, 0)];
    for (arrows, undirected) in [("-[r:R]->(m)-[s:R]->", false), ("-[r:R]-(m)-[s:R]-", true)] {
        let text = format!("MATCH (n:N) UNWIND [n,n,null] AS start OPTIONAL MATCH p=(start){arrows}(end) WHERE end.k<>1 RETURN start.k,end.k,length(p)");
        let query = helix_cypher::compile(&text).unwrap();
        let plan = r::plan(
            query.clone(),
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        assert!(matches!(
            plan.batch_consumer(0),
            Some(r::BatchConsumer::Pipeline { end: 3 })
        ));
        assert!(plan.matches()[&2]
            .steps
            .iter()
            .all(|step| matches!(step, r::MatchStep::Expand { .. })));
        // Enumerate edge identities independently. Parallel edges remain
        // distinct; an undirected self-loop contributes only one orientation.
        let mut expected = Vec::new();
        for start in 0..4 {
            for parent in [Some(start), Some(start), None] {
                let mut found = Vec::new();
                if let Some(parent) = parent {
                    for (first, &(a, b)) in edges.iter().enumerate() {
                        let middle = if a == parent {
                            Some(b)
                        } else if undirected && b == parent {
                            Some(a)
                        } else {
                            None
                        };
                        let Some(middle) = middle else {
                            continue;
                        };
                        for (second, &(c, d)) in edges.iter().enumerate() {
                            if first == second {
                                continue;
                            }
                            let end = if c == middle {
                                Some(d)
                            } else if undirected && d == middle {
                                Some(c)
                            } else {
                                None
                            };
                            let Some(end) = end.filter(|end| *end != 1) else {
                                continue;
                            };
                            found.push(vec![json!(parent), json!(end), json!(2)]);
                        }
                    }
                }
                if found.is_empty() {
                    found.push(vec![json!(parent), json!(null), json!(null)]);
                }
                expected.extend(found);
            }
        }
        expected.sort_by_key(|row| serde_json::to_string(row).unwrap());
        for batch_rows in [1, 2, 7] {
            for strategy in [
                plan.clone(),
                plan.clone().with_execution(r::RowExecution::Materialized),
                r::RowPlan::reference(query.clone()).unwrap(),
            ] {
                let mut actual = Interpreter::new(&db, context::ParamBindings::default())
                    .execute_rows(
                        &strategy,
                        &BTreeMap::new(),
                        Limits {
                            batch_rows,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap()
                    .rows;
                actual.sort_by_key(|row| serde_json::to_string(row).unwrap());
                assert_eq!(actual, expected, "{text}; batch={batch_rows}");
            }
        }
    }
    for (text, expected) in [
        ("MATCH (n:N) UNWIND [n,n,null] AS a OPTIONAL MATCH (a),(n) WHERE a.k=999 RETURN count(*),count(a)",vec![vec![json!(12),json!(8)]]),
        ("MATCH (n:N) UNWIND [n,null] AS a MATCH (a)-[:R]->(b) RETURN count(*)",vec![vec![json!(5)]]),
        ("MATCH (n:N) OPTIONAL MATCH (n)-[:Absent]->(b) RETURN count(*),count(b)",vec![vec![json!(4),json!(0)]]),
        ("MATCH (n:N) OPTIONAL MATCH (n)-[:R]->(b) RETURN b.k ORDER BY b.k DESC LIMIT 2",vec![vec![json!(null)],vec![json!(2)]]),
    ] {
        let query=helix_cypher::compile(text).unwrap();
        let plan=r::plan(query.clone(),&db.planner_context(context::ParamBindings::default())).unwrap();
        assert!(matches!(plan.batch_consumer(0),Some(r::BatchConsumer::Pipeline {..})),"{text}");
        for strategy in [plan.clone(),plan.with_execution(r::RowExecution::Materialized),r::RowPlan::reference(query).unwrap()] {
            assert_eq!(Interpreter::new(&db,context::ParamBindings::default()).execute_rows(&strategy,&BTreeMap::new(),Limits{batch_rows:2,..Default::default()}).await.unwrap().rows,expected,"{text}");
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn bound_expansions_keep_large_correlations_within_a_small_budget() {
    let db = test_support::open_db("bound-pattern-budget").await;
    db.cypher(crate::cypher::Request::new(
        "CREATE (a:Root) WITH a UNWIND range(1,32) AS k CREATE (a)-[:R]->(:Leaf {k:k})",
    ))
    .await
    .unwrap();
    let text = "MATCH (a:Root) UNWIND range(1,2048) AS i OPTIONAL MATCH p=(a)-[:R]->(b) WHERE b.k%2=0 RETURN count(*),sum(b.k),sum(length(p))";
    let plan = r::plan(
        helix_cypher::compile(text).unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    assert!(matches!(
        plan.batch_consumer(0),
        Some(r::BatchConsumer::Pipeline { .. })
    ));
    let expected = vec![vec![
        json!(2048 * 16),
        json!(2048 * (2..=32).step_by(2).sum::<i64>()),
        json!(2048 * 16),
    ]];
    for batch_rows in [1, 8, 17] {
        let limits = Limits {
            memory_bytes: 192 * 1024,
            batch_rows,
            ..Default::default()
        };
        let result = Interpreter::new(&db, context::ParamBindings::default())
            .execute_rows(&plan, &BTreeMap::new(), limits)
            .await
            .unwrap();
        assert_eq!(result.rows, expected);
        assert!(result.resources.peak_memory_bytes <= limits.memory_bytes);
    }
    let materialized = plan.with_execution(r::RowExecution::Materialized);
    let error = Interpreter::new(&db, context::ParamBindings::default())
        .execute_rows(
            &materialized,
            &BTreeMap::new(),
            Limits {
                memory_bytes: 192 * 1024,
                batch_rows: 8,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(error,Error::Query(error) if error.detail=="MemoryLimit"));
    db.close().await.unwrap();
}

#[tokio::test]
async fn bound_pattern_levels_do_not_materialize_a_fanout_product() {
    let db = test_support::open_db("bound-pattern-fanout").await;
    db.cypher(crate::cypher::Request::new("CREATE (a:Root),(z:Leaf) WITH a,z UNWIND range(1,32) AS k CREATE (a)-[:R]->(b:Middle {k:k}) WITH b,z UNWIND range(1,32) AS j CREATE (b)-[:R]->(z)"))
        .await.unwrap();
    let text="MATCH (a:Root) WITH a OPTIONAL MATCH p=(a)-[:R]->(b)-[:R]->(z) RETURN count(*),sum(b.k),sum(length(p))";
    let plan = r::plan(
        helix_cypher::compile(text).unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    assert!(matches!(
        plan.batch_consumer(0),
        Some(r::BatchConsumer::Pipeline { .. })
    ));
    // There is only one outer row. Each first-hop edge has 32 distinct second-
    // hop edges, so retaining an intermediate expansion level exceeds 192 KiB.
    let expected = vec![vec![
        json!(32 * 32),
        json!(32 * (1..=32).sum::<i64>()),
        json!(2 * 32 * 32),
    ]];
    for batch_rows in [1, 8, 17] {
        let result = Interpreter::new(&db, context::ParamBindings::default())
            .execute_rows(
                &plan,
                &BTreeMap::new(),
                Limits {
                    memory_bytes: 192 * 1024,
                    batch_rows,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(result.rows, expected);
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
    let error = Interpreter::new(&db, context::ParamBindings::default())
        .execute_rows(
            &reference,
            &BTreeMap::new(),
            Limits {
                memory_bytes: 192 * 1024,
                batch_rows: 8,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(error,Error::Query(error) if error.detail=="MemoryLimit"));
    db.close().await.unwrap();
}

#[tokio::test]
async fn bound_expansions_flush_topology_and_preserve_errors_and_rollback() {
    let db = test_support::open_db("bound-pattern-writes").await;
    assert_eq!(db.cypher(crate::cypher::Request::new("CREATE (a:N {k:0})-[:R]->(b:N {k:1}) WITH a UNWIND [a,a] AS n OPTIONAL MATCH (n)-[:R]->(m) RETURN count(*),sum(m.k)")).await.unwrap().rows,vec![vec![json!(2),json!(2)]]);
    for (text,detail) in [
        ("MATCH (a:N {k:0}) UNWIND [a,1] AS n OPTIONAL MATCH (n)-[:R]->(b) RETURN b LIMIT 1","ExpectedNode"),
        ("MATCH (a:N {k:0}) UNWIND [a,1] AS n OPTIONAL MATCH (n)-[:R]->(b) WHERE 1/0>0 RETURN b LIMIT 0","DivisionByZero"),
        ("MATCH (a:N {k:0}) UNWIND [1,a] AS n OPTIONAL MATCH (n)-[:R]->(b) WHERE 1/0>0 RETURN b LIMIT 0","ExpectedNode"),
        ("MATCH (a:N)-[found:R]->(b) UNWIND [found,1] AS r OPTIONAL MATCH (a)-[r:R]->(b) RETURN r LIMIT 0","ExpectedRelationship"),
        ("CREATE (:Rollback) WITH 1 AS marker MATCH (a:N {k:0}) UNWIND [a,1] AS n OPTIONAL MATCH (n)-[:R]->(b) RETURN b LIMIT 1","ExpectedNode"),
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
async fn bound_expansion_continuations_release_every_level_on_failure_or_drop() {
    use super::super::bound_match::{BoundCursor, BoundMatch};
    let db = test_support::open_db("bound-pattern-release").await;
    let created = db
        .cypher(crate::cypher::Request::new(
            "CREATE (a:N)-[:R]->(b:N)-[:R]->(c:N),(a)-[:R]->(c) RETURN id(a)",
        ))
        .await
        .unwrap();
    let id = created.rows[0][0].as_u64().unwrap();
    let plan = r::plan(
        helix_cypher::compile("MATCH (a:N) OPTIONAL MATCH (a)-[:R]->(b)-[:R]->(c) RETURN c")
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
    let incoming = plan.matches()[&1].incoming.iter().next().copied().unwrap();
    for cancel_after in [None, Some(0), Some(1), Some(4), Some(12)] {
        let limits = Limits {
            memory_bytes: 192 * 1024,
            batch_rows: 1,
            ..Default::default()
        };
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.row_memory = Some(memory::Budget::new(limits.memory_bytes));
        ctx.enable_request_read_view().await.unwrap();
        let mut rows = RowBuffer::new(ctx.row_budget()).unwrap();
        for _ in 0..32 {
            let mut row = vec![r::Value::Null; plan.query().bindings().len()];
            row[incoming.0 as usize] = r::Value::Entity(r::Entity::Node(id));
            push_row(&mut rows, row, limits).unwrap();
        }
        let mut cursor = BoundCursor::new(rows.finish(), ctx.row_budget()).unwrap();
        let mut stage = BoundMatch::new(
            matches::Match {
                pattern,
                optional: *optional,
                predicate: predicate.as_deref(),
                demand: usize::MAX,
            },
            &plan.matches()[&1],
        );
        let first = stage
            .next_batch(&mut cursor, &ctx, &BTreeMap::new(), limits)
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
                    .next_batch(&mut cursor, &ctx, &BTreeMap::new(), limits)
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
        assert_eq!(ctx.row_budget().available(), limits.memory_bytes);
    }
    // Exhaust the remaining allowance after a successful batch. The resumed
    // stack must fail cleanly and release every retained parent/edge buffer.
    let limits = Limits {
        memory_bytes: 192 * 1024,
        batch_rows: 1,
        ..Default::default()
    };
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(limits.memory_bytes));
    ctx.enable_request_read_view().await.unwrap();
    let mut rows = RowBuffer::new(ctx.row_budget()).unwrap();
    let mut row = vec![r::Value::Null; plan.query().bindings().len()];
    row[incoming.0 as usize] = r::Value::Entity(r::Entity::Node(id));
    for _ in 0..3 {
        push_row(&mut rows, row.clone(), limits).unwrap();
    }
    let mut cursor = BoundCursor::new(rows.finish(), ctx.row_budget()).unwrap();
    let mut stage = BoundMatch::new(
        matches::Match {
            pattern,
            optional: *optional,
            predicate: predicate.as_deref(),
            demand: usize::MAX,
        },
        &plan.matches()[&1],
    );
    drop(
        stage
            .next_batch(&mut cursor, &ctx, &BTreeMap::new(), limits)
            .await
            .unwrap()
            .unwrap(),
    );
    let held = ctx
        .row_budget()
        .reserve(ctx.row_budget().available())
        .unwrap();
    let error = match stage
        .next_batch(&mut cursor, &ctx, &BTreeMap::new(), limits)
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("resumed pattern exceeded its remaining allowance"),
    };
    assert!(
        matches!(&error,Error::Query(error) if error.detail=="MemoryLimit")
            || matches!(
                &error,
                Error::Storage(crate::HelixDbError::QueryMemoryLimitExceeded)
            )
    );
    drop(cursor);
    drop(held);
    assert_eq!(ctx.row_budget().available(), limits.memory_bytes);
    drop(ctx);
    db.close().await.unwrap();
}

#[tokio::test]
async fn repeated_bound_expansion_clauses_use_the_normal_stack() {
    let db = test_support::open_db("bound-pattern-depth").await;
    db.cypher(crate::cypher::Request::new("CREATE (n:N)-[:R]->(n)"))
        .await
        .unwrap();
    let mut text = String::from("MATCH (n:N) ");
    for _ in 0..300 {
        text.push_str("OPTIONAL MATCH (n)-[:R]->(n) ");
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
async fn fallback_patterns_limit_complete_matches_and_keep_supported_prefixes() {
    let db = test_support::open_db("bound-pattern-fallback-window").await;
    db.cypher(crate::cypher::Request::new(
        "CREATE (:Orphan),(a:A)-[:T]->(b:B)",
    ))
    .await
    .unwrap();
    for (text,expected,streamed) in [
        ("MATCH ()-[r]->() WITH r LIMIT 1 OPTIONAL MATCH (a2)-[r]->(b2) RETURN labels(a2),type(r),labels(b2)",vec![vec![json!(["A"]),json!("T"),json!(["B"])]],true),
        ("MATCH (a1)-[r]->() WITH r,a1 LIMIT 1 OPTIONAL MATCH (a2)<-[r]-(b2) WHERE a1=a2 RETURN labels(a1),type(r),b2,a2",vec![vec![json!(["A"]),json!("T"),json!(null),json!(null)]],true),
        ("MATCH ()-[r]->() WITH r WITH r LIMIT 1 OPTIONAL MATCH (a2)-[r]->(b2) RETURN labels(a2),labels(b2)",vec![vec![json!(["A"]),json!(["B"])]],true),
        ("MATCH (x),(a)-[:T]->(b) WITH b LIMIT 1 RETURN labels(b)",vec![vec![json!(["B"])]],true),
    ] {
        let query=helix_cypher::compile(text).unwrap();
        let plan=r::plan(query.clone(),&db.planner_context(context::ParamBindings::default())).unwrap();
        assert_eq!(plan.batch_consumer(0).is_some(),streamed,"{text}");
        for batch_rows in [1,2,7] {
            for strategy in [plan.clone(),plan.clone().with_execution(r::RowExecution::Materialized),r::RowPlan::reference(query.clone()).unwrap()] {
                assert_eq!(Interpreter::new(&db,context::ParamBindings::default()).execute_rows(&strategy,&BTreeMap::new(),Limits{batch_rows,..Default::default()}).await.unwrap().rows,expected,"{text}");
            }
        }
    }
    db.close().await.unwrap();
}
