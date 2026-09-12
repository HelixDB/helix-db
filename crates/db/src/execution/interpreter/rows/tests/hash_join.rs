use super::super::*;
use crate::execution::interpreter::test_support;
use helix_planner::context;
use serde_json::json;

#[tokio::test]
async fn streamed_hash_joins_match_independent_duplicate_and_numeric_relations() {
    let db = test_support::open_db("hash-probe-model").await;
    db.cypher(crate::cypher::Request::new(
        "CREATE (:L {k:1,tag:0}),(:L {k:1.0,tag:1}),(:L {k:2,tag:2}),(:L {tag:3}),(:L {k:9007199254740993,tag:4}),(:R {k:1.0,tag:10}),(:R {k:2,tag:11}),(:R {k:1,tag:12}),(:R {tag:13}),(:R {k:9007199254740992.0,tag:14})",
    )).await.unwrap();
    let pairs = [(0, 10), (0, 12), (1, 10), (1, 12), (2, 11)];
    for (text, expected) in [
        (
            "MATCH (a:L),(b:R) WHERE a.k=b.k RETURN a.tag,b.tag",
            pairs
                .iter()
                .map(|&(a, b)| vec![json!(a), json!(b)])
                .collect::<Vec<_>>(),
        ),
        (
            "MATCH (a:L),(b:R) WHERE a.k=b.k WITH a,b UNWIND [a.tag,a.tag] AS x RETURN x,b.tag",
            pairs
                .iter()
                .flat_map(|&(a, b)| [vec![json!(a), json!(b)], vec![json!(a), json!(b)]])
                .collect(),
        ),
        (
            "MATCH (a:L),(b:R) WHERE a.k=b.k RETURN count(*),sum(a.tag),sum(b.tag)",
            vec![vec![
                json!(pairs.len()),
                json!(pairs.iter().map(|&(a, _)| a).sum::<i64>()),
                json!(pairs.iter().map(|&(_, b)| b).sum::<i64>()),
            ]],
        ),
        (
            "OPTIONAL MATCH (a:Missing),(b:R) WHERE a.k=b.k RETURN a,b",
            vec![vec![json!(null), json!(null)]],
        ),
    ] {
        let query = helix_cypher::compile(text).unwrap();
        let plan = r::plan(
            query.clone(),
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        assert!(plan.batch_consumer(0).is_some(), "{text}");
        assert!(
            plan.matches()[&0]
                .steps
                .iter()
                .any(|step| matches!(step, r::MatchStep::HashJoin { .. })),
            "{text}"
        );
        let mut expected = expected;
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
    db.close().await.unwrap();
}

#[tokio::test]
async fn duplicate_hash_buckets_stream_beyond_the_query_memory_budget() {
    let db = test_support::open_db("hash-probe-budget").await;
    db.cypher(crate::cypher::Request::new(
        "UNWIND range(1,256) AS n CREATE (:L {k:0,n:n}),(:R {k:0,n:n})",
    ))
    .await
    .unwrap();
    let query =
        helix_cypher::compile("MATCH (a:L),(b:R) WHERE a.k=b.k RETURN count(*),sum(a.n),sum(b.n)")
            .unwrap();
    let plan = r::plan(
        query.clone(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    assert_eq!(plan.batch_consumer(0), Some(r::BatchConsumer::Aggregate));
    let expected = vec![vec![
        json!(256 * 256),
        json!(256 * (1..=256).sum::<i64>()),
        json!(256 * (1..=256).sum::<i64>()),
    ]];
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
        assert_eq!(response.rows, expected);
        assert!(response.resources.peak_memory_bytes <= limits.memory_bytes);
        let error = Interpreter::new(&db, context::ParamBindings::default())
            .execute_rows(
                &plan.clone().with_execution(r::RowExecution::Materialized),
                &BTreeMap::new(),
                limits,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(&error,Error::Query(error) if error.detail=="MemoryLimit")
                || matches!(
                    &error,
                    Error::Storage(crate::HelixDbError::QueryMemoryLimitExceeded)
                ),
            "{error:?}"
        );
    }
    let reference = Interpreter::new(&db, context::ParamBindings::default())
        .execute_rows(
            &r::RowPlan::reference(query).unwrap(),
            &BTreeMap::new(),
            Limits::default(),
        )
        .await
        .unwrap();
    assert_eq!(reference.rows, expected);
    db.close().await.unwrap();
}

#[tokio::test]
async fn hash_probe_continuations_preserve_error_order_and_release_owned_state() {
    use super::super::{hash_probe::ProbeCursor, joins::HashJoinTable};
    use crate::query_resources::bitmap;
    use helix_ast::value::PropertyValue as P;
    let db = test_support::open_db("hash-probe-release").await;
    let mut ids = Vec::new();
    for value in [P::I64(1), P::F64(1.0), P::Null, P::F64(f64::NAN)] {
        ids.push(test_support::add_node_with_properties(&db, "N", vec![("k", value)]).await);
    }
    let absent = test_support::add_node_with_properties(&db, "N", vec![("k", P::I64(42))]).await;
    let limits = Limits {
        memory_bytes: 192 * 1024,
        batch_rows: 3,
        ..Default::default()
    };
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(limits.memory_bytes));
    ctx.enable_request_read_view().await.unwrap();
    let mut source = bitmap::Builder::new(Some(ctx.row_budget())).unwrap();
    for &id in &ids {
        source.insert(id).unwrap();
    }
    let source = source.finish();
    let table = HashJoinTable::build(&ctx, &source, 2, r::Slot(1), "k", limits)
        .await
        .unwrap();
    drop(source);
    let mut rows = RowBuffer::new(ctx.row_budget()).unwrap();
    for value in [
        r::Value::Null,
        r::Value::Integer(7),
        r::Value::Entity(r::Entity::Node(ids[2])),
        r::Value::Entity(r::Entity::Node(ids[3])),
        r::Value::Entity(r::Entity::Node(absent)),
        r::Value::Entity(r::Entity::Node(ids[0])),
        r::Value::Entity(r::Entity::Node(u64::MAX)),
    ] {
        push_row(&mut rows, vec![value, r::Value::Null], limits).unwrap();
    }
    let mut cursor = ProbeCursor::new(rows.finish());
    let first = cursor
        .next_batch(&table, &ctx, r::Slot(1), r::Slot(0), "k", limits)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.len(), 2);
    assert_eq!(
        first.iter().map(|row| row[1].clone()).collect::<Vec<_>>(),
        ids[..2]
            .iter()
            .map(|&id| r::Value::Entity(r::Entity::Node(id)))
            .collect::<Vec<_>>()
    );
    drop(first);
    let error = match cursor
        .next_batch(&table, &ctx, r::Slot(1), r::Slot(0), "k", limits)
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("a later graph access failure was lost"),
    };
    assert!(
        matches!(&error,Error::Query(error) if error.detail=="DeletedEntityAccess"),
        "{error:?}"
    );
    drop(cursor);
    drop(table);
    assert_eq!(ctx.row_budget().available(), limits.memory_bytes);
    drop(ctx);

    enum Stop {
        Drop,
        Cancel(usize),
        ExhaustMemory,
    }
    for stop in [
        Stop::Drop,
        Stop::Cancel(0),
        Stop::Cancel(1),
        Stop::Cancel(4),
        Stop::Cancel(12),
        Stop::ExhaustMemory,
    ] {
        let limits = Limits {
            batch_rows: 1,
            ..limits
        };
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.row_memory = Some(memory::Budget::new(limits.memory_bytes));
        ctx.enable_request_read_view().await.unwrap();
        let source = bitmap::Bitmap::singleton(ids[0], Some(ctx.row_budget())).unwrap();
        let table = HashJoinTable::build(&ctx, &source, 2, r::Slot(1), "k", limits)
            .await
            .unwrap();
        drop(source);
        let mut rows = RowBuffer::new(ctx.row_budget()).unwrap();
        for _ in 0..32 {
            push_row(
                &mut rows,
                vec![r::Value::Entity(r::Entity::Node(ids[0])), r::Value::Null],
                limits,
            )
            .unwrap();
        }
        let mut cursor = ProbeCursor::new(rows.finish());
        drop(
            cursor
                .next_batch(&table, &ctx, r::Slot(1), r::Slot(0), "k", limits)
                .await
                .unwrap()
                .unwrap(),
        );
        match stop {
            Stop::Drop => {}
            Stop::Cancel(checkpoints) => {
                ctx.fail_deadline_after(checkpoints);
                loop {
                    match cursor
                        .next_batch(&table, &ctx, r::Slot(1), r::Slot(0), "k", limits)
                        .await
                    {
                        Ok(Some(_)) => {}
                        Err(Error::Storage(crate::HelixDbError::QueryDeadlineExceeded)) => break,
                        Ok(None) => panic!("cancellation failed to interrupt a retained probe"),
                        Err(error) => panic!("unexpected failure: {error:?}"),
                    }
                }
            }
            Stop::ExhaustMemory => {
                let held = ctx
                    .row_budget()
                    .reserve(ctx.row_budget().available())
                    .unwrap();
                let error = match cursor
                    .next_batch(&table, &ctx, r::Slot(1), r::Slot(0), "k", limits)
                    .await
                {
                    Err(error) => error,
                    Ok(_) => panic!("a resumed probe exceeded its remaining allowance"),
                };
                assert!(
                    matches!(&error,Error::Query(error) if error.detail=="MemoryLimit")
                        || matches!(
                            &error,
                            Error::Storage(crate::HelixDbError::QueryMemoryLimitExceeded)
                        ),
                    "{error:?}"
                );
                drop(held);
            }
        }
        drop(cursor);
        drop(table);
        assert_eq!(ctx.row_budget().available(), limits.memory_bytes);
    }
    db.close().await.unwrap();
}
