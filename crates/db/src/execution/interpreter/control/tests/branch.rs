use super::support::*;

#[tokio::test]
async fn optional_branch_returns_input_when_body_is_empty() {
    let db = test_support::open_db("control-optional-empty").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let nodes = test_support::name("nodes");
    let access = exec::ExecStepId::new(1).expect("positive step id");
    let branch = exec::ExecStepId::new(2).expect("positive step id");
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            test_support::step(1, Vec::new(), access_param_op(nodes.clone())),
            test_support::step(
                2,
                vec![access],
                exec::ExecOp::Branch {
                    plan: exec::ExecBranchPlan::Optional(Box::new(source_context_limit_subplan(0))),
                },
            ),
            test_support::step(
                3,
                vec![branch],
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
            context::ParamBindings::default().with_value(
                nodes,
                PropertyValue::I64Array(vec![alice as i64, bob as i64]),
            ),
        )
        .await
        .expect("optional branch executes");

    assert_eq!(
        scalars(result),
        vec![ExecutionScalar::NodeId(alice), ExecutionScalar::NodeId(bob)]
    );
}

#[tokio::test]
async fn optional_branch_preserves_fallback_per_input_row() {
    let db = test_support::open_db("control-optional-per-row").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let carol = test_support::add_user(&db, "carol").await;
    test_support::add_edge(&db, alice, carol, "KNOWS").await;
    let nodes = test_support::name("nodes");
    let access = exec::ExecStepId::new(1).expect("positive step id");
    let branch = exec::ExecStepId::new(2).expect("positive step id");
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            test_support::step(1, Vec::new(), access_param_op(nodes.clone())),
            test_support::step(
                2,
                vec![access],
                exec::ExecOp::Branch {
                    plan: exec::ExecBranchPlan::Optional(Box::new(
                        source_context_expand_nodes_subplan("KNOWS"),
                    )),
                },
            ),
            test_support::step(
                3,
                vec![branch],
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
            context::ParamBindings::default().with_value(
                nodes,
                PropertyValue::I64Array(vec![alice as i64, bob as i64]),
            ),
        )
        .await
        .expect("optional branch executes per input row");

    assert_eq!(
        scalars(result),
        vec![ExecutionScalar::NodeId(carol), ExecutionScalar::NodeId(bob)]
    );
}

#[tokio::test]
async fn choose_else_branch_splits_context_by_predicate() {
    let db = test_support::open_db("control-choose-else").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let nodes = test_support::name("nodes");
    let access = exec::ExecStepId::new(1).expect("positive step id");
    let branch = exec::ExecStepId::new(2).expect("positive step id");
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            test_support::step(1, Vec::new(), access_param_op(nodes.clone())),
            test_support::step(
                2,
                vec![access],
                exec::ExecOp::Branch {
                    plan: exec::ExecBranchPlan::ChooseElse {
                        condition: name_eq("alice"),
                        then_plan: Box::new(source_context_subplan()),
                        else_plan: Box::new(source_context_limit_subplan(0)),
                    },
                },
            ),
            test_support::step(
                3,
                vec![branch],
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
            context::ParamBindings::default().with_value(
                nodes,
                PropertyValue::I64Array(vec![alice as i64, bob as i64]),
            ),
        )
        .await
        .expect("choose/else branch executes");

    assert_eq!(scalars(result), vec![ExecutionScalar::NodeId(alice)]);
}

#[tokio::test]
async fn union_branch_preserves_branch_order_per_input_row() {
    let db = test_support::open_db("control-union").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let nodes = test_support::name("nodes");
    let access = exec::ExecStepId::new(1).expect("positive step id");
    let branch = exec::ExecStepId::new(2).expect("positive step id");
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            test_support::step(1, Vec::new(), access_param_op(nodes.clone())),
            test_support::step(
                2,
                vec![access],
                exec::ExecOp::Branch {
                    plan: exec::ExecBranchPlan::Union(ir::AtLeast::<_, 2>::from_pair(
                        source_context_subplan(),
                        source_context_subplan(),
                    )),
                },
            ),
            test_support::step(
                3,
                vec![branch],
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
            context::ParamBindings::default().with_value(
                nodes,
                PropertyValue::I64Array(vec![alice as i64, bob as i64]),
            ),
        )
        .await
        .expect("union branch executes");

    assert_eq!(
        scalars(result),
        vec![
            ExecutionScalar::NodeId(alice),
            ExecutionScalar::NodeId(alice),
            ExecutionScalar::NodeId(bob),
            ExecutionScalar::NodeId(bob)
        ]
    );
}

#[tokio::test]
async fn choose_branch_runs_only_passing_context_rows() {
    let db = test_support::open_db("control-choose").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let nodes = test_support::name("nodes");
    let access = exec::ExecStepId::new(1).expect("positive step id");
    let branch = exec::ExecStepId::new(2).expect("positive step id");
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            test_support::step(1, Vec::new(), access_param_op(nodes.clone())),
            test_support::step(
                2,
                vec![access],
                exec::ExecOp::Branch {
                    plan: exec::ExecBranchPlan::Choose {
                        condition: name_eq("alice"),
                        then_plan: Box::new(source_context_subplan()),
                    },
                },
            ),
            test_support::step(
                3,
                vec![branch],
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
            context::ParamBindings::default().with_value(
                nodes,
                PropertyValue::I64Array(vec![alice as i64, bob as i64]),
            ),
        )
        .await
        .expect("choose branch executes");

    assert_eq!(scalars(result), vec![ExecutionScalar::NodeId(alice)]);
}

#[tokio::test]
async fn choose_branch_returns_empty_when_no_rows_pass() {
    let db = test_support::open_db("control-choose-none").await;
    let bob = test_support::add_user(&db, "bob").await;
    let nodes = test_support::name("nodes");
    let access = exec::ExecStepId::new(1).expect("positive step id");
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            test_support::step(1, Vec::new(), access_param_op(nodes.clone())),
            test_support::step(
                2,
                vec![access],
                exec::ExecOp::Branch {
                    plan: exec::ExecBranchPlan::Choose {
                        condition: name_eq("alice"),
                        then_plan: Box::new(source_context_subplan()),
                    },
                },
            ),
        ],
        2,
    );

    let result = db
        .execute(
            &plan,
            context::ParamBindings::default().with_value(nodes, PropertyValue::I64(bob as i64)),
        )
        .await
        .expect("choose branch executes");

    assert_eq!(result.last, Some(ExecutionValue::Stream(Vec::new())));
}

#[tokio::test]
async fn choose_else_shortcuts_all_passing_and_all_failing_inputs() {
    let db = test_support::open_db("control-choose-else-shortcuts").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let nodes = test_support::name("nodes");
    let access = exec::ExecStepId::new(1).expect("positive step id");
    let branch = exec::ExecStepId::new(2).expect("positive step id");
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            test_support::step(1, Vec::new(), access_param_op(nodes.clone())),
            test_support::step(
                2,
                vec![access],
                exec::ExecOp::Branch {
                    plan: exec::ExecBranchPlan::ChooseElse {
                        condition: name_eq("alice"),
                        then_plan: Box::new(source_context_subplan()),
                        else_plan: Box::new(source_context_limit_subplan(0)),
                    },
                },
            ),
            test_support::step(
                3,
                vec![branch],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::Id,
                },
            ),
        ],
        3,
    );

    let passing = db
        .execute(
            &plan,
            context::ParamBindings::default()
                .with_value(nodes.clone(), PropertyValue::I64(alice as i64)),
        )
        .await
        .expect("all-passing choose/else executes");
    let failing = db
        .execute(
            &plan,
            context::ParamBindings::default().with_value(nodes, PropertyValue::I64(bob as i64)),
        )
        .await
        .expect("all-failing choose/else executes");

    assert_eq!(scalars(passing), vec![ExecutionScalar::NodeId(alice)]);
    assert_eq!(scalars(failing), Vec::new());
}

#[tokio::test]
async fn coalesce_branch_returns_first_non_empty_branch() {
    let db = test_support::open_db("control-coalesce").await;
    let alice = test_support::add_user(&db, "alice").await;
    let nodes = test_support::name("nodes");
    let access = exec::ExecStepId::new(1).expect("positive step id");
    let branch = exec::ExecStepId::new(2).expect("positive step id");
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            test_support::step(1, Vec::new(), access_param_op(nodes.clone())),
            test_support::step(
                2,
                vec![access],
                exec::ExecOp::Branch {
                    plan: exec::ExecBranchPlan::Coalesce(
                        ir::AtLeast::<_, 1>::try_from_vec(vec![
                            source_context_limit_subplan(0),
                            source_context_subplan(),
                        ])
                        .expect("non-empty coalesce"),
                    ),
                },
            ),
            test_support::step(
                3,
                vec![branch],
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
            context::ParamBindings::default().with_value(nodes, PropertyValue::I64(alice as i64)),
        )
        .await
        .expect("coalesce branch executes");

    assert_eq!(scalars(result), vec![ExecutionScalar::NodeId(alice)]);
}

#[tokio::test]
async fn coalesce_branch_selects_first_non_empty_branch_per_input_row() {
    let db = test_support::open_db("control-coalesce-per-row").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let carol = test_support::add_user(&db, "carol").await;
    test_support::add_edge(&db, alice, carol, "KNOWS").await;
    let nodes = test_support::name("nodes");
    let access = exec::ExecStepId::new(1).expect("positive step id");
    let branch = exec::ExecStepId::new(2).expect("positive step id");
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            test_support::step(1, Vec::new(), access_param_op(nodes.clone())),
            test_support::step(
                2,
                vec![access],
                exec::ExecOp::Branch {
                    plan: exec::ExecBranchPlan::Coalesce(
                        ir::AtLeast::<_, 1>::try_from_vec(vec![
                            source_context_expand_nodes_subplan("KNOWS"),
                            source_context_subplan(),
                        ])
                        .expect("non-empty coalesce"),
                    ),
                },
            ),
            test_support::step(
                3,
                vec![branch],
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
            context::ParamBindings::default().with_value(
                nodes,
                PropertyValue::I64Array(vec![alice as i64, bob as i64]),
            ),
        )
        .await
        .expect("coalesce branch executes per input row");

    assert_eq!(
        scalars(result),
        vec![ExecutionScalar::NodeId(carol), ExecutionScalar::NodeId(bob)]
    );
}
