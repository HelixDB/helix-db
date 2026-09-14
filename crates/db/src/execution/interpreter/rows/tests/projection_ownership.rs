use super::super::*;
use crate::execution::interpreter::test_support;
use helix_planner::context;

#[tokio::test]
async fn dropping_a_pending_top_k_releases_retained_rows_and_admission() {
    use futures::StreamExt;
    let db = test_support::open_db("projection-top-k-cancellation").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let memory_bytes = 64 * 1024;
    ctx.row_memory = Some(memory::Budget::new(memory_bytes));
    let rows = Rows::new(
        vec![vec![r::Value::String("retained".repeat(128))]],
        ctx.row_budget(),
    )
    .unwrap();
    let batches =
        futures::stream::iter([Ok(memory::Batch::from(rows))]).chain(futures::stream::pending());
    let items = r::ProjectionProgram::new(vec![r::Projection {
        slot: r::Slot(0),
        expression: r::Expression::Slot(r::Slot(0)),
    }])
    .unwrap();
    let ordering = [r::Ordering {
        expression: r::Expression::Slot(r::Slot(0)),
        descending: false,
    }];
    let limit = r::Expression::Literal(r::Value::Integer(1));
    let parameters = BTreeMap::new();
    let projection = projection::Projection {
        items: &items,
        distinct: false,
        ordering: &ordering,
        predicate: None,
        skip: None,
        limit: Some(&limit),
    };
    let mut future = Box::pin(ctx.top_k_batches(
        batches,
        1,
        projection,
        &parameters,
        Limits {
            memory_bytes,
            ..Default::default()
        },
    ));
    assert!(futures::poll!(&mut future).is_pending());
    assert!(ctx.row_budget().available() < memory_bytes);
    drop(future);
    assert_eq!(ctx.row_budget().available(), memory_bytes);
    db.close().await.unwrap();
}

#[tokio::test]
async fn compound_group_keys_share_admission_before_copying_input_rows() {
    use futures::FutureExt;
    let db = test_support::open_db("projection-group-key-admission").await;
    const PAYLOAD_BYTES: usize = 256 * 1024;
    let parameters = BTreeMap::from([("key".into(), r::Value::String("k".repeat(PAYLOAD_BYTES)))]);
    let memory_bytes = PAYLOAD_BYTES + 64 * 1024;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(memory_bytes));
    let rows = Rows::new(vec![vec![r::Value::Null; 3]], ctx.row_budget()).unwrap();
    let items = r::ProjectionProgram::new(vec![
        r::Projection {
            slot: r::Slot(0),
            expression: r::Expression::Parameter("key".into()),
        },
        r::Projection {
            slot: r::Slot(1),
            expression: r::Expression::Parameter("key".into()),
        },
        r::Projection {
            slot: r::Slot(2),
            expression: r::Expression::Binary(
                r::Binary::Add,
                Box::new(r::Expression::Aggregate {
                    function: r::Aggregate::Count,
                    argument: None,
                    distinct: false,
                }),
                Box::new(r::Expression::Literal(r::Value::Integer(1))),
            ),
        },
    ])
    .unwrap();
    let projection = projection::Projection {
        items: &items,
        distinct: false,
        ordering: &[],
        predicate: None,
        skip: None,
        limit: None,
    };
    let (result, allocated) = crate::allocation_testing::observe(|| {
        ctx.project_rows(
            rows,
            3,
            projection,
            &parameters,
            Limits {
                memory_bytes,
                ..Default::default()
            },
        )
        .now_or_never()
    });
    let result = result.expect("scalar grouping keys need no pending I/O");
    assert!(
        allocated.bytes <= memory_bytes,
        "group keys copied before admission: {allocated:?}"
    );
    assert!(matches!(result, Err(Error::Query(error)) if error.detail=="MemoryLimit"));
    assert_eq!(ctx.row_budget().available(), memory_bytes);
    db.close().await.unwrap();
}

#[test]
fn admitted_batches_borrow_or_move_without_copying_or_releasing_payload() {
    let data = vec![vec![r::Value::String("payload".repeat(1024))]; 3];
    let bytes = rows_bytes(&data);
    let budget = memory::Budget::new(bytes);
    let rows = Rows::new(data, &budget).unwrap();
    let pointer = rows.data.as_ptr();
    let (_, allocated) = crate::allocation_testing::observe(|| {
        let mut batches = rows.batches(2);
        let first = batches.next().unwrap();
        assert_eq!(first.as_ref().as_ptr(), pointer);
        assert_eq!(first.as_ref().len(), 2);
        assert_eq!(batches.next().unwrap().as_ref().len(), 1);
        assert!(batches.next().is_none());
        assert_eq!(budget.available(), 0);
    });
    assert_eq!(allocated.allocations, 0);
    let (batch, allocated) = crate::allocation_testing::observe(|| memory::Batch::from(rows));
    assert_eq!(allocated.allocations, 0);
    assert_eq!(batch.as_ref().as_ptr(), pointer);
    assert_eq!(budget.available(), 0);
    drop(batch);
    assert_eq!(budget.available(), bytes);
}

#[test]
fn completed_rows_transfer_a_full_budget_and_reject_an_insufficient_bound() {
    let mut data = Vec::with_capacity(4);
    data.push(vec![r::Value::String("owned".repeat(1024))]);
    let retained = rows_bytes(&data) + 3 * size_of::<r::Row>();
    let budget = memory::Budget::new(retained + 100);
    let reservation = budget.reserve(retained + 100).unwrap();
    let pointer = data.as_ptr();
    let (rows, allocated) =
        crate::allocation_testing::observe(|| Rows::from_admitted(data, reservation));
    assert_eq!(allocated.allocations, 0);
    assert_eq!(rows.data.as_ptr(), pointer);
    assert_eq!(budget.available(), 100);
    drop(rows);
    assert_eq!(budget.available(), retained + 100);

    let reservation = budget.reserve(0).unwrap();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        Rows::from_admitted(vec![vec![r::Value::Integer(1)]], reservation)
    }))
    .is_err());
    assert_eq!(budget.available(), retained + 100);
    drop(Rows::from_admitted(Vec::new(), budget.reserve(0).unwrap()));
    assert_eq!(budget.available(), retained + 100);
}

#[tokio::test]
async fn grouping_and_windows_preserve_rows_when_transferring_admission() {
    use serde_json::json;
    let db = test_support::open_db("projection-group-ownership").await;
    db.cypher(crate::cypher::Request::new(
        "CREATE (:A {key:1}),(:B {key:2})",
    ))
    .await
    .unwrap();
    for (query, expected) in [
        (
            "MATCH (n) RETURN n.key AS key ORDER BY n:A",
            vec![vec![json!(2)], vec![json!(1)]],
        ),
        (
            "MATCH (n) RETURN n.key AS key ORDER BY n:A LIMIT 1",
            vec![vec![json!(2)]],
        ),
        (
            "UNWIND [2,1,2,3,1] AS x RETURN x,count(*)+1 AS n ORDER BY x",
            vec![
                vec![json!(1), json!(3)],
                vec![json!(2), json!(3)],
                vec![json!(3), json!(2)],
            ],
        ),
        (
            "UNWIND [] AS x RETURN count(*)+1 AS n",
            vec![vec![json!(1)]],
        ),
        ("UNWIND [] AS x RETURN x,count(*)+1 AS n", vec![]),
        (
            "UNWIND [2,1,2,3,1] AS x RETURN DISTINCT x ORDER BY x SKIP 1 LIMIT 1",
            vec![vec![json!(2)]],
        ),
        (
            "UNWIND [2,1] AS x RETURN x ORDER BY x SKIP 10 LIMIT 1",
            vec![],
        ),
        ("UNWIND [2,1] AS x RETURN x ORDER BY x LIMIT 0", vec![]),
        (
            "UNWIND [2,1] AS x RETURN x+1 AS y ORDER BY x",
            vec![vec![json!(2)], vec![json!(3)]],
        ),
        (
            "UNWIND [2,1] AS x RETURN x+1 AS y ORDER BY x LIMIT 1",
            vec![vec![json!(2)]],
        ),
    ] {
        let query_plan = r::plan(
            helix_cypher::compile(query).unwrap(),
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        let materialized = query_plan
            .clone()
            .with_execution(r::RowExecution::Materialized);
        for plan in [&query_plan, &materialized] {
            let result = Interpreter::new(&db, context::ParamBindings::default())
                .execute_rows(plan, &BTreeMap::new(), Limits::default())
                .await
                .unwrap_or_else(|error| panic!("{query}: {error}"));
            assert_eq!(result.rows, expected, "{query}");
        }
    }
    db.close().await.unwrap();
}
