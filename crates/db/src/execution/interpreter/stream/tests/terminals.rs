use super::super::super::{ExecutionContext, FoldedStream, ReturnedValue};
use super::support::*;

#[tokio::test]
async fn stream_rows_rejects_folded_streams_with_unfold_guidance() {
    let db = test_support::open_db("stream-rows-folded-rejection").await;
    let ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    let error = ctx
        .stream_rows(
            ExecutionValue::FoldedStream(FoldedStream::new(rows(&[1]))),
            "test",
        )
        .expect_err("folded streams require an explicit unfold");

    assert_eq!(
        error.to_string(),
        "Query error: test expected stream input, got folded stream; use unfold first"
    );
}

#[tokio::test]
async fn terminal_chain_counts_scalar_terminal_items() {
    let db = test_support::open_db("stream-terminal-chain-count").await;
    let ids = [
        test_support::add_user(&db, "ada").await,
        test_support::add_user(&db, "grace").await,
    ];
    let ids_param = name("ids");
    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    let first_count_id = exec::ExecStepId::new(2).expect("positive step id");
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            node_access_step(1, ids_param.clone()),
            test_support::step(
                2,
                vec![access_id],
                exec::ExecOp::Count {
                    plan: Box::new(exec::ExecCountPlan::InputRows {
                        window: exec::ExecCountWindowPlan::identity(),
                    }),
                },
            ),
            test_support::step(
                3,
                vec![first_count_id],
                exec::ExecOp::Count {
                    plan: Box::new(exec::ExecCountPlan::InputScalars {
                        window: exec::ExecCountWindowPlan::identity(),
                    }),
                },
            ),
        ],
        3,
    );

    let result = db
        .execute(
            &plan,
            context::ParamBindings::default().with_value(ids_param, ids_value(&ids)),
        )
        .await
        .expect("terminal chain executes")
        .last
        .expect("project step returns a value");

    assert_eq!(result, ExecutionValue::Count(1));
}

#[tokio::test]
async fn planner_emitted_terminal_chain_executes_through_db_facade() {
    let db = test_support::open_db("stream-planner-terminal-chain").await;
    test_support::add_user(&db, "ada").await;
    test_support::add_user(&db, "grace").await;
    let output = name("nested");
    let batch = ReadBatch::try_from_parts(
        vec![BatchEntry::Query(Box::new(NamedQuery {
            name: Some(output.as_ref().to_string()),
            root: AstNode::Count {
                input: Box::new(AstNode::Count {
                    input: Box::new(AstNode::Nodes {
                        reference: NodeRef::All,
                    }),
                }),
            },
            condition: None,
        }))],
        vec![output.as_ref().to_string()],
    )
    .expect("read fixture should contain no mutation");
    let plan = planning::plan_read_batch(&batch, &context::PlannerContext::default())
        .expect("manual terminal-chain AST plans");

    let result = db
        .execute(&plan, context::ParamBindings::default())
        .await
        .expect("planner-emitted terminal chain executes");

    assert_eq!(
        result.returns.get(&output),
        Some(&ReturnedValue::Present(ExecutionValue::Count(1)))
    );
}

#[tokio::test]
async fn scalar_terminal_windows_and_distinct_execute_as_scalar_items() {
    let db = test_support::open_db("stream-scalar-terminal-pipeline").await;
    let ids = [
        test_support::add_user(&db, "ada").await,
        test_support::add_user(&db, "grace").await,
    ];
    let ids_param = name("ids");
    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    let ids_project_id = exec::ExecStepId::new(2).expect("positive step id");
    let limited_id = exec::ExecStepId::new(3).expect("positive step id");
    let limited_scalar_count = test_support::executable(
        ir::PlanKind::Read,
        vec![
            node_access_step(1, ids_param.clone()),
            test_support::step(
                2,
                vec![access_id],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::Id,
                },
            ),
            test_support::step(
                3,
                vec![ids_project_id],
                exec::ExecOp::Limit {
                    count: ir::StreamBoundPlan::Literal(1),
                },
            ),
            test_support::step(
                4,
                vec![limited_id],
                exec::ExecOp::Count {
                    plan: Box::new(exec::ExecCountPlan::InputScalars {
                        window: exec::ExecCountWindowPlan::identity(),
                    }),
                },
            ),
        ],
        4,
    );
    let limited_result = db
        .execute(
            &limited_scalar_count,
            context::ParamBindings::default().with_value(ids_param.clone(), ids_value(&ids)),
        )
        .await
        .expect("scalar limit executes")
        .last
        .expect("project step returns a value");

    assert_eq!(limited_result, ExecutionValue::Count(1));

    let first_count_id = exec::ExecStepId::new(2).expect("positive step id");
    let distinct_count = test_support::executable(
        ir::PlanKind::Read,
        vec![
            node_access_step(1, ids_param.clone()),
            test_support::step(
                2,
                vec![access_id],
                exec::ExecOp::Count {
                    plan: Box::new(exec::ExecCountPlan::InputRows {
                        window: exec::ExecCountWindowPlan::identity(),
                    }),
                },
            ),
            test_support::step(3, vec![first_count_id], exec::ExecOp::Distinct),
        ],
        3,
    );
    let distinct_result = db
        .execute(
            &distinct_count,
            context::ParamBindings::default().with_value(ids_param, ids_value(&ids)),
        )
        .await
        .expect("scalar distinct executes")
        .last
        .expect("distinct step returns a value");

    assert_eq!(
        distinct_result,
        ExecutionValue::Scalars(vec![ExecutionScalar::Value(DbPropertyValue::I64(2))])
    );
}
