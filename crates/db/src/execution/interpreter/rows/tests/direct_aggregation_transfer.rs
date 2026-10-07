use super::super::*;
use crate::execution::interpreter::test_support;
use helix_planner::context;

#[test]
fn admitted_output_rows_transfer_at_a_full_budget_and_release_on_growth_failure() {
    for spare in [0, 3 * size_of::<r::Row>()] {
        let text = "owned".repeat(4096);
        let pointer = text.as_ptr();
        let row = vec![r::Value::String(text)];
        let bytes = row_bytes(&row);
        let budget = memory::Budget::new(bytes + spare);
        let owned = budget.reserve(bytes).unwrap();
        let mut output = RowBuffer::new(&budget).unwrap();
        let (result, allocated) =
            crate::allocation_testing::observe(|| output.push_admitted(row, owned));
        assert!(
            allocated.bytes < 1024,
            "row transfer copied payload: {allocated:?}"
        );
        if spare == 0 {
            assert!(matches!(result,Err(Error::Query(error)) if error.detail=="MemoryLimit"));
            assert_eq!(output.len(), 0);
            drop(output);
        } else {
            result.unwrap();
            assert_eq!(budget.available(), 0);
            let rows = output.finish();
            let r::Value::String(value) = &rows[0][0] else {
                panic!("transferred string");
            };
            assert_eq!(value.as_ptr(), pointer);
            drop(rows);
        }
        assert_eq!(budget.available(), bytes + spare);
    }
}

#[tokio::test]
async fn dropping_a_pending_direct_aggregate_releases_group_state() {
    use futures::StreamExt;
    let db = test_support::open_db("direct-aggregate-cancellation").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let memory_bytes = 64 * 1024;
    ctx.row_memory = Some(memory::Budget::new(memory_bytes));
    let rows = Rows::new(
        vec![vec![r::Value::Integer(1), r::Value::Null]],
        ctx.row_budget(),
    )
    .unwrap();
    let batches =
        futures::stream::iter([Ok(memory::Batch::from(rows))]).chain(futures::stream::pending());
    let items = r::ProjectionProgram::new(vec![r::Projection {
        slot: r::Slot(1),
        expression: r::Expression::Aggregate {
            function: r::Aggregate::Collect,
            argument: Some(Box::new(r::Expression::Slot(r::Slot(0)))),
            distinct: true,
        },
    }])
    .unwrap();
    let projection = projection::Projection {
        items: &items,
        distinct: false,
        ordering: &[],
        predicate: None,
        skip: None,
        limit: None,
    };
    let parameters = BTreeMap::new();
    let mut future = Box::pin(ctx.aggregate_batches(
        batches,
        2,
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
async fn aggregate_collection_failure_after_create_rolls_back_and_allows_retry() {
    use serde_json::json;
    let db = test_support::open_db("direct-aggregate-rollback").await;
    let query="CREATE (:AggregateRollback) WITH 1 AS seed UNWIND range(1,20) AS value RETURN collect(value)";
    let plan = r::plan(
        helix_cypher::compile(query).unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    let interpreter = Interpreter::new(&db, context::ParamBindings::default());
    let error = interpreter
        .execute_rows(
            &plan,
            &BTreeMap::new(),
            Limits {
                collection_items: 8,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(error,Error::Query(error) if error.detail=="CollectionLimit"));
    let after = db
        .cypher(crate::cypher::Request::new(
            "MATCH (n:AggregateRollback) RETURN count(*)",
        ))
        .await
        .unwrap();
    assert_eq!(after.rows, vec![vec![json!(0)]]);
    let success = db.cypher(crate::cypher::Request::new(query)).await.unwrap();
    assert_eq!(
        success.rows,
        vec![vec![json!((1..=20).collect::<Vec<_>>())]]
    );
    let after = db
        .cypher(crate::cypher::Request::new(
            "MATCH (n:AggregateRollback) RETURN count(*)",
        ))
        .await
        .unwrap();
    assert_eq!(after.rows, vec![vec![json!(1)]]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn direct_groups_retain_incoming_order_dependencies_in_shared_plans() {
    use serde_json::json;
    let db = test_support::open_db("direct-group-incoming-order").await;
    let original = helix_cypher::compile(
        "UNWIND [[0,2],[1,1]] AS pair WITH pair[0] AS g,pair[1] AS key RETURN g,count(*) AS n",
    )
    .unwrap();
    let key = r::Slot(
        original
            .bindings()
            .iter()
            .position(|binding| binding.name == "key")
            .unwrap() as u32,
    );
    let mut operators = original.operators().to_vec();
    let r::Operator::Project { ordering, .. } = operators.last_mut().unwrap() else {
        panic!("terminal projection");
    };
    ordering.push(r::Ordering {
        expression: r::Expression::Slot(key),
        descending: false,
    });
    // The shared validated row contract permits this incoming reference even
    // though the Cypher frontend requires projected aggregate ordering values.
    let query = r::Query::new(
        original.bindings().to_vec(),
        operators,
        original.returns().to_vec(),
    )
    .unwrap();
    let planned = r::plan(
        query.clone(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    let materialized = planned
        .clone()
        .with_execution(r::RowExecution::Materialized);
    let reference = r::RowPlan::reference(query).unwrap();
    for plan in [&planned, &materialized, &reference] {
        let result = Interpreter::new(&db, context::ParamBindings::default())
            .execute_rows(plan, &BTreeMap::new(), Limits::default())
            .await
            .unwrap();
        assert_eq!(
            result.rows,
            vec![vec![json!(1), json!(1)], vec![json!(0), json!(1)]]
        );
    }
    db.close().await.unwrap();
}
