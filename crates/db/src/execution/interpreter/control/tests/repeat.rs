use super::support::*;

#[tokio::test]
async fn repeat_emits_each_iteration_from_context_subplan() {
    let db = test_support::open_db("control-repeat").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let carol = test_support::add_user(&db, "carol").await;
    test_support::add_edge(&db, alice, bob, "KNOWS").await;
    test_support::add_edge(&db, bob, carol, "KNOWS").await;
    let start = test_support::name("start");
    let access = exec::ExecStepId::new(1).expect("positive step id");
    let repeat = exec::ExecStepId::new(2).expect("positive step id");
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            test_support::step(1, Vec::new(), access_param_op(start.clone())),
            test_support::step(
                2,
                vec![access],
                exec::ExecOp::Repeat {
                    plan: exec::ExecRepeatPlan {
                        body: Box::new(source_context_expand_nodes_subplan("KNOWS")),
                        stop: ir::RepeatStopPlan::MaxDepthOnly,
                        emit: ir::RepeatEmitPlan::After,
                        max_depth: NonZeroUsize::new(2).expect("positive depth"),
                    },
                },
            ),
            test_support::step(
                3,
                vec![repeat],
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
            context::ParamBindings::default().with_value(start, PropertyValue::I64(alice as i64)),
        )
        .await
        .expect("repeat executes");

    assert_eq!(
        scalars(result),
        vec![ExecutionScalar::NodeId(bob), ExecutionScalar::NodeId(carol)]
    );
}

#[tokio::test]
async fn repeat_stop_variants_bound_iterations() {
    let db = test_support::open_db("control-repeat-stops").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let carol = test_support::add_user(&db, "carol").await;
    test_support::add_edge(&db, alice, bob, "KNOWS").await;
    test_support::add_edge(&db, bob, carol, "KNOWS").await;
    let start = test_support::name("start");

    for (index, stop, expected) in [
        (
            0,
            ir::RepeatStopPlan::Times {
                count: NonZeroUsize::new(1).expect("positive count"),
            },
            vec![ExecutionScalar::NodeId(bob)],
        ),
        (
            1,
            ir::RepeatStopPlan::Until {
                predicate: name_eq("bob"),
            },
            vec![ExecutionScalar::NodeId(bob)],
        ),
        (
            2,
            ir::RepeatStopPlan::TimesOrUntil {
                count: NonZeroUsize::new(2).expect("positive count"),
                predicate: name_eq("carol"),
            },
            vec![ExecutionScalar::NodeId(bob), ExecutionScalar::NodeId(carol)],
        ),
    ] {
        let access = exec::ExecStepId::new(1).expect("positive step id");
        let repeat = exec::ExecStepId::new(2).expect("positive step id");
        let plan = test_support::executable(
            ir::PlanKind::Read,
            vec![
                test_support::step(1, Vec::new(), access_param_op(start.clone())),
                test_support::step(
                    2,
                    vec![access],
                    exec::ExecOp::Repeat {
                        plan: exec::ExecRepeatPlan {
                            body: Box::new(source_context_expand_nodes_subplan("KNOWS")),
                            stop,
                            emit: ir::RepeatEmitPlan::After,
                            max_depth: NonZeroUsize::new(3).expect("positive depth"),
                        },
                    },
                ),
                test_support::step(
                    3,
                    vec![repeat],
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
                    .with_value(start.clone(), PropertyValue::I64(alice as i64)),
            )
            .await
            .unwrap_or_else(|err| panic!("repeat stop case {index} executes: {err}"));

        assert_eq!(scalars(result), expected, "repeat stop case {index}");
    }
}

#[tokio::test]
async fn repeat_emit_variants_preserve_before_after_and_filtered_contracts() {
    let db = test_support::open_db("control-repeat-emits").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    test_support::add_edge(&db, alice, bob, "KNOWS").await;
    let start = test_support::name("start");

    for (index, emit, expected) in [
        (
            0,
            ir::RepeatEmitPlan::Before,
            vec![ExecutionScalar::NodeId(alice)],
        ),
        (
            1,
            ir::RepeatEmitPlan::All,
            vec![ExecutionScalar::NodeId(alice), ExecutionScalar::NodeId(bob)],
        ),
        (
            2,
            ir::RepeatEmitPlan::AfterIf {
                predicate: name_eq("bob"),
            },
            vec![ExecutionScalar::NodeId(bob)],
        ),
        (
            3,
            ir::RepeatEmitPlan::None,
            vec![ExecutionScalar::NodeId(bob)],
        ),
    ] {
        let access = exec::ExecStepId::new(1).expect("positive step id");
        let repeat = exec::ExecStepId::new(2).expect("positive step id");
        let plan = test_support::executable(
            ir::PlanKind::Read,
            vec![
                test_support::step(1, Vec::new(), access_param_op(start.clone())),
                test_support::step(
                    2,
                    vec![access],
                    exec::ExecOp::Repeat {
                        plan: exec::ExecRepeatPlan {
                            body: Box::new(source_context_expand_nodes_subplan("KNOWS")),
                            stop: ir::RepeatStopPlan::MaxDepthOnly,
                            emit,
                            max_depth: NonZeroUsize::new(1).expect("positive depth"),
                        },
                    },
                ),
                test_support::step(
                    3,
                    vec![repeat],
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
                    .with_value(start.clone(), PropertyValue::I64(alice as i64)),
            )
            .await
            .unwrap_or_else(|err| panic!("repeat emit case {index} executes: {err}"));

        assert_eq!(scalars(result), expected, "repeat emit case {index}");
    }
}
