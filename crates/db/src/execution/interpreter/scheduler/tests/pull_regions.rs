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

/// A region whose condition gates it off returns an empty stream and releases
/// every reference without opening its source.
#[tokio::test]
async fn gated_region_releases_references_without_opening_its_source() {
    let db = test_support::open_db("scheduler-gated-region").await;
    let gate = exec::ExecCondition::PreviousStepNotEmpty { dependency: id(1) };
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            test_support::step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::Empty)),
                },
            ),
            exec::ExecStep {
                condition: gate.clone(),
                ..test_support::step(
                    2,
                    vec![id(1)],
                    exec::ExecOp::Access {
                        plan: Box::new(exec::ExecAccessPlan::Node(
                            exec::ExecNodeAccessPlan::FromParam {
                                param: named("unbound"),
                            },
                        )),
                    },
                )
            },
            exec::ExecStep {
                condition: gate,
                ..test_support::step(
                    3,
                    vec![id(2)],
                    exec::ExecOp::Limit {
                        count: ir::StreamBoundPlan::Literal(1),
                    },
                )
            },
        ],
        3,
    );
    assert_eq!(
        plan.execution_program()
            .regions()
            .map(|(terminal, region)| (terminal.get(), region.steps().len()))
            .collect::<Vec<_>>(),
        vec![(3, 2)]
    );
    // The parameter is unbound, so opening the source would fail the plan.
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.execute_steps(
        plan.steps(),
        plan.execution_order(),
        plan.root(),
        plan.execution_program(),
    )
    .await
    .unwrap();

    assert_eq!(ctx.pull_work.snapshot().source_visits, 0);
    assert_eq!(
        ctx.finish(plan.root(), &exec::ExecutableReturns::None)
            .unwrap()
            .last,
        Some(ExecutionValue::Stream(Vec::new()))
    );
    assert!(ctx.step_output_uses.is_empty());
    db.close().await.unwrap();
}

/// Stages two writes that conflict on a unique email, then runs a pull region
/// counting the rows of `source`, which fails on its unbound parameter if the
/// region opens it. Returns the region's result and its source visits.
async fn count_region_over_staged_unique_conflict(
    name: &str,
    source: exec::ExecNodeAccessPlan,
) -> (Result<()>, usize) {
    let db = test_support::open_db_with_config(
        test_support::in_memory_config(name).with_unique_equality_index("User", "email"),
    )
    .await;
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            test_support::step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(source)),
                },
            ),
            test_support::step(
                2,
                vec![id(1)],
                exec::ExecOp::Count {
                    plan: Box::new(exec::ExecCountPlan::InputRows {
                        window: exec::ExecCountWindowPlan::identity(),
                    }),
                },
            ),
        ],
        2,
    );
    assert!(plan.execution_program().region(id(2)).is_some());
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.enable_request_write_scope().await.unwrap();
    let add = exec::ExecOp::Mutation {
        plan: exec::ExecMutationPlan::AddNodeSource {
            label: named("User"),
            properties: test_support::assignments(vec![(
                "email",
                helix_ast::value::PropertyValue::from("taken@example.com"),
            )]),
        },
    };
    for _ in 0..2 {
        ctx.execute_op(&add, ExecutionValue::Stream(Vec::new()))
            .await
            .expect("conflicting writes stage without flushing");
    }
    let result = ctx
        .execute_steps(
            plan.steps(),
            plan.execution_order(),
            plan.root(),
            plan.execution_program(),
        )
        .await;
    let visits = ctx.pull_work.snapshot().source_visits;
    ctx.abort_request_write_scope();
    db.close().await.unwrap();
    (result, visits)
}

/// A region flushes the deferred index maintenance its operators observe
/// before opening any source, so a pending unique conflict fails the region.
#[tokio::test]
async fn region_flush_surfaces_a_pending_unique_conflict_before_reading() {
    let (result, visits) = count_region_over_staged_unique_conflict(
        "scheduler-region-unique-flush",
        exec::ExecNodeAccessPlan::DynamicEquality {
            index: helix_planner::catalog::NodeEqualityIndexMeta::try_new("user-email").unwrap(),
            key: helix_planner::catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
            param: named("unbound"),
        },
    )
    .await;
    assert!(matches!(
        result,
        Err(HelixDbError::UniqueConstraintViolation { .. })
    ));
    assert_eq!(visits, 0);
}

/// A region that only counts rows it reads itself observes no deferred index
/// state, so it opens its source without flushing; the pending conflict stays
/// staged for the commit to reject.
#[tokio::test]
async fn region_counting_its_own_rows_skips_the_deferred_flush() {
    let (result, _) = count_region_over_staged_unique_conflict(
        "scheduler-region-held-rows",
        exec::ExecNodeAccessPlan::FromParam {
            param: named("unbound"),
        },
    )
    .await;
    let Err(HelixDbError::Query(message)) = result else {
        panic!("the unbound source parameter fails before any flush: {result:?}");
    };
    assert!(message.contains("`unbound` is not bound"), "{message}");
}
