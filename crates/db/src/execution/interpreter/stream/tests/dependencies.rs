use super::support::*;

#[tokio::test]
async fn multi_dependency_input_rejects_mixed_stream_and_scalar_outputs() {
    let db = test_support::open_db("stream-mixed-dependency").await;
    let id = test_support::add_user(&db, "ada").await;
    let ids_param = name("ids");
    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    let count_id = exec::ExecStepId::new(2).expect("positive step id");
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
            test_support::step(3, vec![access_id, count_id], exec::ExecOp::Noop),
        ],
        3,
    );

    let err = db
        .execute(
            &plan,
            context::ParamBindings::default().with_value(ids_param, PropertyValue::I64(id as i64)),
        )
        .await
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("cannot concatenate mixed stream and scalar dependency outputs"));
}
