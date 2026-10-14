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
    assert!(matches!(error,Error::Query(error) if error.detail=="memory_limit"));
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
    assert!(matches!(error,Error::Query(error) if error.detail=="memory_limit"));
    db.close().await.unwrap();
}

#[tokio::test]
async fn bound_expansions_flush_topology_and_preserve_errors_and_rollback() {
    let db = test_support::open_db("bound-pattern-writes").await;
    assert_eq!(db.cypher(crate::cypher::Request::new("CREATE (a:N {k:0})-[:R]->(b:N {k:1}) WITH a UNWIND [a,a] AS n OPTIONAL MATCH (n)-[:R]->(m) RETURN count(*),sum(m.k)")).await.unwrap().rows,vec![vec![json!(2),json!(2)]]);
    for (text,detail) in [
        ("MATCH (a:N {k:0}) UNWIND [a,1] AS n OPTIONAL MATCH (n)-[:R]->(b) RETURN b LIMIT 1","expected_node"),
        ("MATCH (a:N {k:0}) UNWIND [a,1] AS n OPTIONAL MATCH (n)-[:R]->(b) WHERE 1/0>0 RETURN b LIMIT 0","division_by_zero"),
        ("MATCH (a:N {k:0}) UNWIND [1,a] AS n OPTIONAL MATCH (n)-[:R]->(b) WHERE 1/0>0 RETURN b LIMIT 0","expected_node"),
        ("MATCH (a:N)-[found:R]->(b) UNWIND [found,1] AS r OPTIONAL MATCH (a)-[r:R]->(b) RETURN r LIMIT 0","expected_relationship"),
        ("CREATE (:Rollback) WITH 1 AS marker MATCH (a:N {k:0}) UNWIND [a,1] AS n OPTIONAL MATCH (n)-[:R]->(b) RETURN b LIMIT 1","expected_node"),
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
        matches!(&error,Error::Query(error) if error.detail=="memory_limit")
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

/// A bound pattern admits its source cache, seed row, stack, continuation and
/// every candidate copy before allocating them. Whatever allowance remains
/// when a parent starts or resumes, the stage either produces the parent's
/// next match or fails with MemoryLimit, and dropping the failed stage
/// releases everything. A wide outer row makes each row copy the largest owner.
#[tokio::test]
async fn bound_pattern_admission_failures_release_every_owner() {
    use super::super::bound_match::{BoundCursor, BoundMatch};
    let db = test_support::open_db("bound-pattern-admission-sweep").await;
    let created = db
        .cypher(crate::cypher::Request::new(
            "CREATE (a:N)-[:R]->(:M),(a)-[:R]->(:M),(a)-[:R]->(:M) RETURN id(a)",
        ))
        .await
        .unwrap();
    let id = created.rows[0][0].as_u64().unwrap();
    let limits = Limits {
        memory_bytes: 1024 * 1024,
        batch_rows: 1,
        ..Default::default()
    };
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(limits.memory_bytes));
    ctx.enable_request_read_view().await.unwrap();
    let parameters = BTreeMap::new();
    for text in [
        "WITH '' AS pad UNWIND [1] AS i MATCH (a:N)-[:R]->(b) RETURN pad, b",
        "MATCH (a:N) WITH a, '' AS pad OPTIONAL MATCH (a)-[:R]->(b) RETURN pad, b",
    ] {
        let plan = r::plan(
            helix_cypher::compile(text).unwrap(),
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        let (index, pattern, optional, predicate) = plan
            .query()
            .operators()
            .iter()
            .enumerate()
            .rev()
            .find_map(|(index, operator)| {
                let r::Operator::Match {
                    pattern,
                    optional,
                    predicate,
                } = operator
                else {
                    return None;
                };
                Some((index, pattern, *optional, predicate.as_deref()))
            })
            .unwrap();
        let physical = &plan.matches()[&index];
        let pad = plan
            .query()
            .bindings()
            .iter()
            .position(|binding| binding.name == "pad")
            .unwrap();
        for resumed in [false, true] {
            let (mut failures, mut successes) = (0, 0);
            for available in (0..16 * 1024)
                .step_by(8)
                .chain((16 * 1024..=256 * 1024).step_by(512))
            {
                let mut row = vec![r::Value::Null; plan.query().bindings().len()];
                row[pad] = r::Value::String("p".repeat(32 * 1024));
                for slot in &physical.incoming {
                    row[slot.0 as usize] = r::Value::Entity(r::Entity::Node(id));
                }
                let mut input = RowBuffer::new(ctx.row_budget()).unwrap();
                push_row(&mut input, row, limits).unwrap();
                let mut cursor = BoundCursor::new(input.finish(), ctx.row_budget()).unwrap();
                let mut stage = BoundMatch::new(
                    matches::Match {
                        pattern,
                        optional,
                        predicate,
                        demand: usize::MAX,
                    },
                    physical,
                );
                if resumed {
                    let first = stage
                        .next_batch(&mut cursor, &ctx, &parameters, limits)
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(first.len(), 1);
                }
                let held = ctx
                    .row_budget()
                    .reserve(ctx.row_budget().available().saturating_sub(available))
                    .unwrap();
                match stage
                    .next_batch(&mut cursor, &ctx, &parameters, limits)
                    .await
                {
                    Ok(Some(rows)) => {
                        assert_eq!(rows.len(), 1);
                        successes += 1;
                    }
                    Err(Error::Query(error)) if error.detail == "memory_limit" => failures += 1,
                    Err(Error::Storage(crate::HelixDbError::QueryMemoryLimitExceeded)) => {
                        failures += 1
                    }
                    Ok(None) => panic!("the parent has another match"),
                    Err(error) => panic!("{available}: {error:?}"),
                }
                drop(held);
                drop(cursor);
                drop(stage);
                assert_eq!(
                    ctx.row_budget().available(),
                    limits.memory_bytes,
                    "{text}, resumed={resumed}, available={available}"
                );
            }
            assert!(failures > 0 && successes > 0, "{text}, resumed={resumed}");
        }
    }
    ctx.close_request_read_view().unwrap();
    drop(ctx);
    db.close().await.unwrap();
}

/// A stack level admits its frame and that frame's first poll before
/// allocating either. When another owner takes the budget just as the level
/// below yields, the stack fails with MemoryLimit, whatever allowance was left,
/// and releases everything it holds, whether a hash level builds its table
/// or reuses the one the stage already built.
#[tokio::test]
async fn stack_levels_admit_frames_and_polls_after_their_input_yields() {
    use super::super::expansion_stack::{ExpansionStack, SourceCache};
    let db = test_support::open_db("stack-level-admission").await;
    let created = db
        .cypher(crate::cypher::Request::new(
            "CREATE (a:A {key:1})-[:R]->(:B {key:1}) RETURN id(a)",
        ))
        .await
        .unwrap();
    let id = created.rows[0][0].as_u64().unwrap();
    let limits = Limits {
        memory_bytes: 256 * 1024,
        batch_rows: 4,
        ..Default::default()
    };
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(limits.memory_bytes));
    ctx.enable_request_read_view().await.unwrap();
    for (text, completed, reuse) in [
        ("MATCH (a:A) WITH a MATCH (a)-[:R]->(b) RETURN b", 0, false),
        ("MATCH (a:A),(b:B) WHERE a.key=b.key RETURN b", 1, false),
        ("MATCH (a:A),(b:B) WHERE a.key=b.key RETURN b", 1, true),
    ] {
        let plan = r::plan(
            helix_cypher::compile(text).unwrap(),
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        let (index, pattern) = plan
            .query()
            .operators()
            .iter()
            .enumerate()
            .rev()
            .find_map(|(index, operator)| {
                let r::Operator::Match { pattern, .. } = operator else {
                    return None;
                };
                Some((index, pattern))
            })
            .unwrap();
        let physical = &plan.matches()[&index];
        // The input binds the stage's incoming nodes and every completed scan.
        let mut row = vec![r::Value::Null; plan.query().bindings().len()];
        for slot in physical
            .incoming
            .iter()
            .chain(physical.steps[..completed].iter().map(|step| {
                let r::MatchStep::Scan(slot) = step else {
                    unreachable!("completed steps are scans")
                };
                slot
            }))
        {
            row[slot.0 as usize] = r::Value::Entity(r::Entity::Node(id));
        }
        let mut reused =
            SourceCache::new(physical, completed, limits.batch_rows, ctx.row_budget()).unwrap();
        if reuse {
            let seed = Rows::new(vec![row.clone()], ctx.row_budget()).unwrap();
            let mut warm = ExpansionStack::new(
                pattern,
                physical,
                completed,
                futures::stream::once(async move { Ok::<_, Error>(seed) }),
                ctx.row_budget(),
            )
            .unwrap();
            while warm
                .next_batch(&ctx, limits, &mut reused)
                .await
                .unwrap()
                .is_some()
            {}
        }
        let retained = ctx.row_budget().available();
        let (mut failures, mut successes) = (0, 0);
        for available in (0..=32 * 1024).step_by(8) {
            let mut fresh =
                SourceCache::new(physical, completed, limits.batch_rows, ctx.row_budget()).unwrap();
            let cache = if reuse { &mut reused } else { &mut fresh };
            // The source outlives this iteration's borrows: the stack shares
            // the cache's lifetime. It hands the taken budget back through
            // this shared slot.
            let held = std::sync::Arc::new(std::sync::Mutex::new(None));
            let seed = Rows::new(vec![row.clone()], ctx.row_budget()).unwrap();
            let budget = ctx.row_budget().clone();
            let held_by_source = std::sync::Arc::clone(&held);
            let source = futures::stream::once(async move {
                *held_by_source.lock().unwrap() = Some(
                    budget
                        .reserve(budget.available().saturating_sub(available))
                        .unwrap(),
                );
                Ok::<_, Error>(seed)
            });
            let mut stack =
                ExpansionStack::new(pattern, physical, completed, source, ctx.row_budget())
                    .unwrap();
            match stack.next_batch(&ctx, limits, cache).await {
                Ok(Some(rows)) => {
                    assert_eq!(rows.len(), 1);
                    successes += 1;
                }
                Err(Error::Query(error)) if error.detail == "memory_limit" => failures += 1,
                Err(Error::Storage(crate::HelixDbError::QueryMemoryLimitExceeded)) => failures += 1,
                Ok(None) => panic!("the input has a match"),
                Err(error) => panic!("{available}: {error:?}"),
            }
            drop(stack);
            drop(held);
            drop(fresh);
            assert_eq!(
                ctx.row_budget().available(),
                retained,
                "{text}, reuse={reuse}, available={available}"
            );
        }
        assert!(failures > 0 && successes > 0, "{text}, reuse={reuse}");
        drop(reused);
        assert_eq!(ctx.row_budget().available(), limits.memory_bytes);
    }
    ctx.close_request_read_view().unwrap();
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
