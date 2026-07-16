use super::support::*;

#[tokio::test]
async fn foreach_executes_body_once_per_parameter_item() {
    let db = test_support::open_db("control-foreach-side-effects").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let carol = test_support::add_user(&db, "carol").await;
    let from = test_support::name("from");
    let items = test_support::name("items");
    let to = test_support::name("to");
    let foreach = exec::ExecStepId::new(1).expect("positive step id");
    let access = exec::ExecStepId::new(2).expect("positive step id");
    let expand = exec::ExecStepId::new(3).expect("positive step id");
    let plan = test_support::executable(
        ir::PlanKind::Write,
        vec![
            test_support::step(
                1,
                Vec::new(),
                exec::ExecOp::ForEach {
                    param: items.clone(),
                    body: Box::new(add_edge_to_param_subplan(from.clone(), to.clone(), "KNOWS")),
                },
            ),
            test_support::step(2, vec![foreach], access_param_op(from.clone())),
            test_support::step(
                3,
                vec![access],
                exec::ExecOp::Expand {
                    plan: ir::ExpandPlan {
                        direction: ir::ExpandDirection::Out,
                        label: ir::ExpandLabelPlan::Label(test_support::name("KNOWS")),
                        output: ir::ExpandOutput::Edges,
                    },
                },
            ),
            test_support::step(
                4,
                vec![expand],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::Id,
                },
            ),
        ],
        4,
    );

    let result = db
        .execute(
            &plan,
            context::ParamBindings::default()
                .with_value(from, PropertyValue::I64(alice as i64))
                .with_value(
                    items,
                    PropertyValue::array([
                        PropertyValue::object([("to", PropertyValue::I64(bob as i64))]),
                        PropertyValue::object([("to", PropertyValue::I64(carol as i64))]),
                    ]),
                ),
        )
        .await
        .expect("foreach write executes");

    let edge_ids = scalars(result);
    assert_eq!(edge_ids.len(), 2);
    assert!(edge_ids
        .iter()
        .all(|value| matches!(value, ExecutionScalar::EdgeId(_))));
}

#[tokio::test]
async fn foreach_restores_parameter_after_body_execution() {
    let db = test_support::open_db("control-foreach-restore").await;
    let bob = test_support::add_user(&db, "bob").await;
    let carol = test_support::add_user(&db, "carol").await;
    let items = test_support::name("items");
    let target = test_support::name("target");
    let foreach = exec::ExecStepId::new(1).expect("positive step id");
    let access = exec::ExecStepId::new(2).expect("positive step id");
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            test_support::step(
                1,
                Vec::new(),
                exec::ExecOp::ForEach {
                    param: items.clone(),
                    body: Box::new(access_param_subplan(target.clone())),
                },
            ),
            test_support::step(2, vec![foreach], access_param_op(target.clone())),
            test_support::step(
                3,
                vec![access],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::Id,
                },
            ),
        ],
        3,
    );

    let result = db
        .execute(
            &plan,
            context::ParamBindings::default()
                .with_value(
                    target,
                    PropertyValue::I64Array(vec![bob as i64, carol as i64]),
                )
                .with_value(
                    items,
                    PropertyValue::array([
                        PropertyValue::object([("target", PropertyValue::I64(carol as i64))]),
                        PropertyValue::object([("target", PropertyValue::I64(bob as i64))]),
                    ]),
                ),
        )
        .await
        .expect("foreach read executes");

    assert_eq!(
        scalars(result),
        vec![ExecutionScalar::NodeId(bob), ExecutionScalar::NodeId(carol)]
    );
}
