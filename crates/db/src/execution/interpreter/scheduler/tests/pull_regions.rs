//! Mixed pull/whole-value stages must retain order, ownership and demand.
use super::*;

fn mixed_plan(right_limit: usize) -> exec::ExecutablePlan {
    let mut steps = Vec::new();
    for (start, parameter, take) in [(1, "left", 1), (4, "right", right_limit)] {
        steps.extend([
            test_support::step(
                start,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::FromParam {
                            param: named(parameter),
                        },
                    )),
                },
            ),
            test_support::step(
                start + 1,
                vec![id(start)],
                exec::ExecOp::Filter {
                    predicate: ir::PredicatePlan::new(helix_ast::expr::Predicate::eq(
                        "name", "match",
                    ))
                    .unwrap(),
                },
            ),
            test_support::step(
                start + 2,
                vec![id(start + 1)],
                exec::ExecOp::Limit {
                    count: ir::StreamBoundPlan::Literal(take),
                },
            ),
        ]);
    }
    // A third chain has no demand consumer, so these stages combine pull
    // regions with ordinary whole-value steps instead of testing one mode only.
    steps.extend([
        test_support::step(7, Vec::new(), exec::ExecOp::Noop),
        test_support::step(8, vec![id(7)], exec::ExecOp::Noop),
        test_support::step(9, vec![id(8)], exec::ExecOp::Noop),
        test_support::step(
            10,
            vec![id(3), id(6), id(9)],
            exec::ExecOp::Variable {
                op: exec::ExecVariableOp::Stream(ir::StreamVariableOp::Store(named("seen"))),
            },
        ),
    ]);
    test_support::executable(ir::PlanKind::Read, steps, 10)
}

#[tokio::test]
async fn mixed_pull_stages_preserve_order_demand_and_atomic_variable_publication() {
    use crate::encoding::v2::keys;
    let config = test_support::in_memory_config("mixed-pull-scheduler");
    let writer = test_support::open_db_with_config(config.clone()).await;
    let mut ids = Vec::new();
    for _ in 0..4 {
        ids.push(test_support::add_user(&writer, "match").await);
    }
    writer
        .inner_db()
        .put(
            keys::DataKey::Data {
                scope: keys::scope::DataScope::LegacyUnscoped,
                kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(ids[3])),
            }
            .to_bytes(),
            bytes::Bytes::from_static(b"deliberately invalid property record"),
        )
        .await
        .unwrap();
    writer.flush_writer().await.unwrap();
    let reader = test_support::open_reader_with_config(config).await;
    let plan = mixed_plan(2);
    assert_eq!(
        plan.execution_program()
            .regions()
            .map(|(terminal, region)| (terminal.get(), region.steps().len()))
            .collect::<Vec<_>>(),
        vec![(3, 3), (6, 3)]
    );
    assert!(plan.execution_order().stages().iter().any(|stage| {
        matches!(stage, exec::ExecExecutionStage::Parallel(_))
            && stage.iter().collect::<Vec<_>>() == vec![id(3), id(6), id(9)]
    }));
    let params = context::ParamBindings::default()
        .with_value(
            named("left"),
            helix_ast::value::PropertyValue::I64Array(vec![ids[0] as i64, ids[3] as i64]),
        )
        .with_value(
            named("right"),
            helix_ast::value::PropertyValue::I64Array(vec![
                ids[1] as i64,
                ids[2] as i64,
                ids[3] as i64,
            ]),
        );
    let expected = ExecutionValue::Stream(
        ids.iter()
            .take(3)
            .map(|value| ExecutionRow::current(ElementRef::Node(*value)))
            .collect(),
    );
    // Reader snapshots permit parallel stages; writer snapshots require serial
    // execution. The same validated program must behave identically in both.
    for db in [&reader, &writer] {
        let mut ctx = ExecutionContext::new(db, params.clone());
        ctx.enable_request_read_view().await.unwrap();
        ctx.execute_steps(
            plan.steps(),
            plan.execution_order(),
            plan.root(),
            plan.execution_program(),
        )
        .await
        .unwrap();
        assert_eq!(ctx.pull_work.snapshot().source_visits, 3);
        assert_eq!(ctx.variables.get(&named("seen")), Some(&expected));
        assert_eq!(
            ctx.finish(plan.root(), &exec::ExecutableReturns::None)
                .unwrap()
                .last,
            Some(expected.clone())
        );
        assert!(ctx.step_output_uses.is_empty());
        ctx.close_request_read_view().unwrap();

        // Demanding the corrupt tail must fail before the later variable store.
        let failed = mixed_plan(3);
        let mut ctx = ExecutionContext::new(db, params.clone());
        ctx.enable_request_read_view().await.unwrap();
        assert!(ctx
            .execute_steps(
                failed.steps(),
                failed.execution_order(),
                failed.root(),
                failed.execution_program(),
            )
            .await
            .is_err());
        assert!(ctx.variables.get(&named("seen")).is_none());
        ctx.close_request_read_view().unwrap();
    }
    reader.close().await.unwrap();
    writer.close().await.unwrap();
}

#[tokio::test]
async fn invalid_consumer_ownership_fails_before_transferring_any_value() {
    let db = test_support::open_db("scheduler-invalid-ownership").await;
    let step = test_support::step(2, vec![id(1)], exec::ExecOp::Noop);
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.step_outputs.insert(id(1), ExecutionValue::Bool(true));
    assert!(matches!(
        ctx.parallel_step_context(&step),
        Err(HelixDbError::InvariantViolation(message)) if message.contains("unplanned dependency")
    ));
    ctx.step_output_uses.insert(id(1), NonZeroUsize::MIN);
    let mut repeated = step.clone();
    repeated.dependencies.push(id(1));
    assert!(matches!(
        ctx.parallel_step_context(&repeated),
        Err(HelixDbError::InvariantViolation(message)) if message.contains("over-consumes")
    ));
    assert!(ctx.step_outputs.contains_key(&id(1)));
    assert_eq!(ctx.step_output_uses.get(&id(1)), Some(&NonZeroUsize::MIN));
    assert!(matches!(
        ctx.initialize_step_output_uses(std::slice::from_ref(&step), id(2)),
        Err(HelixDbError::InvariantViolation(message)) if message.contains("not isolated")
    ));

    let mut uses = runtime_context::StepOutputUsePlan::default();
    uses.insert(id(1), NonZeroUsize::new(usize::MAX).unwrap());
    assert!(matches!(
        increment_output_use(&mut uses, id(1)),
        Err(HelixDbError::InvariantViolation(message)) if message.contains("more consumers")
    ));
    assert_eq!(uses.get(&id(1)).unwrap().get(), usize::MAX);
    db.close().await.unwrap();
}
