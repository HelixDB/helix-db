use super::*;

#[test]
fn executable_plan_validates_root_dependencies_and_cycles() {
    let steps = ir::AtLeast::<_, 1>::from_one(step(1, vec![id(2)], ExecSchedule::Pipeline));
    assert!(matches!(
        executable(steps, id(1)),
        Err(ExecPlanError::MissingDependency { .. })
    ));

    let cyclic = ir::AtLeast::<_, 1>::from_one_and_rest(
        step(1, vec![id(2)], ExecSchedule::Pipeline),
        vec![step(2, vec![id(1)], ExecSchedule::Pipeline)],
    );
    assert!(matches!(
        executable(cyclic, id(1)),
        Err(ExecPlanError::DependencyCycle { .. })
    ));
}

#[test]
fn executable_plan_accepts_valid_parallel_dag() {
    let steps = ir::AtLeast::<_, 1>::from_one_and_rest(
        step(1, Vec::new(), ExecSchedule::Pipeline),
        vec![
            step(2, Vec::new(), ExecSchedule::Pipeline),
            step(
                3,
                vec![id(1), id(2)],
                ExecSchedule::Parallel {
                    max_concurrency: properties::PositiveUsize::new(2).unwrap(),
                    preserve_order: false,
                },
            ),
        ],
    );

    assert!(executable(steps, id(3)).is_ok());
}

#[test]
fn executable_plan_validates_previous_result_conditions() {
    let steps = ir::AtLeast::<_, 1>::from_one_and_rest(
        run_step(1, Vec::new(), ExecCondition::Always),
        vec![run_step(
            2,
            Vec::new(),
            ExecCondition::PreviousStepNotEmpty { dependency: id(1) },
        )],
    );

    assert!(matches!(
        executable(steps, id(2)),
        Err(ExecPlanError::PreviousConditionMissingDependency { .. })
    ));

    let valid = executable(
        ir::AtLeast::<_, 1>::from_one_and_rest(
            run_step(1, Vec::new(), ExecCondition::Always),
            vec![run_step(
                2,
                vec![id(1)],
                ExecCondition::PreviousStepNotEmpty { dependency: id(1) },
            )],
        ),
        id(2),
    )
    .unwrap();
    let mut serialized = serde_json::to_value(valid).unwrap();
    serialized["steps"][1]["dependencies"] = serde_json::json!([]);
    assert!(serde_json::from_value::<ExecutablePlan>(serialized).is_err());

    let native_missing_dependency = executable(
        ir::AtLeast::<_, 1>::from_one(ExecStep {
            id: id(1),
            dependencies: Vec::new(),
            output: ir::BatchOutputPlan::Discard,
            condition: ExecCondition::PreviousStepNotEmpty { dependency: id(2) },
            op: ExecOp::KvRead(KvReadPlan::Get {
                key: ElementKeyspace::NodeProperty.point_key(1),
            }),
            schedule: ExecSchedule::Pipeline,
            delivered: properties::DeliveredProperties::default(),
            cost: cost::CostVector::ZERO,
        }),
        id(1),
    );
    assert!(matches!(
        native_missing_dependency,
        Err(ExecPlanError::PreviousConditionMissingDependency { .. })
    ));
}

#[test]
fn executable_plan_deserialization_revalidates_count_input_arity() {
    let source = step(1, Vec::new(), ExecSchedule::Pipeline);
    let mut count = step(2, vec![id(1)], ExecSchedule::Barrier);
    count.op = ExecOp::Count {
        plan: Box::new(ExecCountPlan::InputRows {
            window: ExecCountWindowPlan::identity(),
        }),
    };
    let valid = executable(
        ir::AtLeast::<_, 1>::from_one_and_rest(source, vec![count]),
        id(2),
    )
    .unwrap();

    let mut serialized = serde_json::to_value(valid).unwrap();
    serialized["steps"][1]["dependencies"] = serde_json::json!([]);
    let error = serde_json::from_value::<ExecutablePlan>(serialized).unwrap_err();

    assert!(error.to_string().contains("Rows input"));
}

#[test]
fn executable_step_ids_are_typed_one_based_cursors() {
    assert!(ExecStepId::new(0).is_none());
    assert_eq!(ExecStepId::first().get(), 1);
    assert_eq!(ExecStepId::first().next().unwrap().get(), 2);

    let profile = cost::StorageCostProfile::default();
    let mut lowering = ExecutableDagBuilder::new(&profile);
    let first = lowering
        .push_step(StepDraft {
            dependencies: Vec::new(),
            output: ir::BatchOutputPlan::Discard,
            condition: ExecCondition::Always,
            op: ExecOp::Noop,
            schedule: ExecSchedule::Pipeline,
            delivered: properties::DeliveredProperties::default(),
            cost: cost::CostVector::ZERO,
        })
        .unwrap();
    let second = lowering
        .push_step(StepDraft {
            dependencies: vec![first],
            output: ir::BatchOutputPlan::Discard,
            condition: ExecCondition::Always,
            op: ExecOp::Noop,
            schedule: ExecSchedule::Pipeline,
            delivered: properties::DeliveredProperties::default(),
            cost: cost::CostVector::ZERO,
        })
        .unwrap();

    assert_eq!(first.get(), 1);
    assert_eq!(second.get(), 2);

    let mut exhausted = ExecutableDagBuilder::with_next_id(&profile, None);
    assert_eq!(
        exhausted.push_step(StepDraft {
            dependencies: Vec::new(),
            output: ir::BatchOutputPlan::Discard,
            condition: ExecCondition::Always,
            op: ExecOp::Noop,
            schedule: ExecSchedule::Pipeline,
            delivered: properties::DeliveredProperties::default(),
            cost: cost::CostVector::ZERO,
        }),
        Err(ExecPlanError::StepIdSpaceExhausted)
    );
    assert_eq!(
        ExecPlanError::StepIdSpaceExhausted.to_string(),
        "executable DAG step ID space exhausted"
    );
}

#[test]
fn executable_plan_rejects_steps_unreachable_from_root() {
    let steps = ir::AtLeast::<_, 1>::from_one_and_rest(
        step(1, Vec::new(), ExecSchedule::Pipeline),
        vec![step(2, Vec::new(), ExecSchedule::Pipeline)],
    );

    assert!(matches!(
        executable(steps, id(1)),
        Err(ExecPlanError::UnreachableStep {
            step,
            root,
        }) if step == id(2) && root == id(1)
    ));
}

#[test]
fn executable_execution_order_covers_serial_parallel_conditions_and_barriers() {
    let serial = executable(
        ir::AtLeast::<_, 1>::from_one_and_rest(
            step(1, Vec::new(), ExecSchedule::Pipeline),
            vec![
                step(2, vec![id(1)], ExecSchedule::Pipeline),
                step(3, vec![id(2)], ExecSchedule::Pipeline),
            ],
        ),
        id(3),
    )
    .unwrap();
    assert_eq!(
        serial.execution_order().step_ids().collect::<Vec<_>>(),
        vec![id(1), id(2), id(3)]
    );
    assert_eq!(serial.execution_order().stages().len(), 3);

    let parallel = executable(
        ir::AtLeast::<_, 1>::from_one_and_rest(
            step(1, Vec::new(), ExecSchedule::Pipeline),
            vec![
                step(2, Vec::new(), ExecSchedule::Pipeline),
                step(
                    3,
                    vec![id(1), id(2)],
                    ExecSchedule::Parallel {
                        max_concurrency: properties::PositiveUsize::new(1).unwrap(),
                        preserve_order: true,
                    },
                ),
            ],
        ),
        id(3),
    )
    .unwrap();
    let parallel_order = parallel.execution_order();
    let [ExecExecutionStage::Parallel(stage), ExecExecutionStage::Single(root)] =
        parallel_order.stages()
    else {
        panic!("parallel dependency group should execute before its merge root");
    };
    assert_eq!(stage.ids(), &[id(1), id(2)]);
    assert_eq!(stage.max_concurrency().get(), 1);
    assert!(stage.preserve_order());
    assert_eq!(*root, id(3));

    let prefixed_parallel = executable(
        ir::AtLeast::<_, 1>::from_one_and_rest(
            step(1, Vec::new(), ExecSchedule::Pipeline),
            vec![
                step(2, Vec::new(), ExecSchedule::Pipeline),
                step(3, Vec::new(), ExecSchedule::Pipeline),
                step(
                    4,
                    vec![id(2), id(3)],
                    ExecSchedule::Parallel {
                        max_concurrency: properties::PositiveUsize::new(1).unwrap(),
                        preserve_order: false,
                    },
                ),
                step(5, vec![id(1), id(4)], ExecSchedule::Pipeline),
            ],
        ),
        id(5),
    )
    .unwrap();
    let prefixed_order = prefixed_parallel.execution_order();
    let [ExecExecutionStage::Single(prefix), ExecExecutionStage::Parallel(prefixed_stage), ExecExecutionStage::Single(prefixed_merge), ExecExecutionStage::Single(prefixed_root)] =
        prefixed_order.stages()
    else {
        panic!("unrelated ready prefix should stay outside the parallel dependency group");
    };
    assert_eq!(*prefix, id(1));
    assert_eq!(prefixed_stage.ids(), &[id(2), id(3)]);
    assert_eq!(prefixed_stage.max_concurrency().get(), 1);
    assert!(!prefixed_stage.preserve_order());
    assert_eq!(*prefixed_merge, id(4));
    assert_eq!(*prefixed_root, id(5));

    let conditional = executable(
        ir::AtLeast::<_, 1>::from_one_and_rest(
            run_step(1, Vec::new(), ExecCondition::Always),
            vec![run_step(
                2,
                vec![id(1)],
                ExecCondition::PreviousStepNotEmpty { dependency: id(1) },
            )],
        ),
        id(2),
    )
    .unwrap();
    assert_eq!(
        conditional.execution_order().step_ids().collect::<Vec<_>>(),
        vec![id(1), id(2)]
    );

    let barrier_heavy = executable(
        ir::AtLeast::<_, 1>::from_one_and_rest(
            step(1, Vec::new(), ExecSchedule::Barrier),
            vec![
                step(2, vec![id(1)], ExecSchedule::Barrier),
                step(3, vec![id(2)], ExecSchedule::Barrier),
            ],
        ),
        id(3),
    )
    .unwrap();
    assert!(barrier_heavy
        .execution_order()
        .stages()
        .iter()
        .all(|stage| matches!(stage, ExecExecutionStage::Single(_))));

    let barrier_boundary = executable(
        ir::AtLeast::<_, 1>::from_one_and_rest(
            step(1, Vec::new(), ExecSchedule::Pipeline),
            vec![
                step(2, Vec::new(), ExecSchedule::Barrier),
                step(3, Vec::new(), ExecSchedule::Pipeline),
                step(4, vec![id(1), id(2), id(3)], ExecSchedule::Pipeline),
            ],
        ),
        id(4),
    )
    .unwrap();
    assert_eq!(
        barrier_boundary.execution_order().stages(),
        &[
            ExecExecutionStage::Single(id(1)),
            ExecExecutionStage::Single(id(2)),
            ExecExecutionStage::Single(id(3)),
            ExecExecutionStage::Single(id(4)),
        ]
    );
}

#[test]
fn executable_subplan_exposes_interpreter_execution_order() {
    let subplan = ExecutableSubplan::new(
        ir::AtLeast::<_, 1>::from_one_and_rest(
            step(1, Vec::new(), ExecSchedule::Pipeline),
            vec![step(2, vec![id(1)], ExecSchedule::Barrier)],
        ),
        id(2),
    )
    .unwrap();

    assert_eq!(
        subplan.execution_order().step_ids().collect::<Vec<_>>(),
        vec![id(1), id(2)]
    );
}

#[test]
fn executable_plan_accessors_serde_and_errors_cover_validation_contract() {
    let valid = executable(
        ir::AtLeast::<_, 1>::from_one(run_step(1, Vec::new(), ExecCondition::Always)),
        id(1),
    )
    .unwrap();
    assert!(valid.trace().events.is_empty());
    assert_eq!(valid.metrics(), &PlannerMetrics::default());

    let encoded = serde_json::to_value(&valid).unwrap();
    assert!(encoded.get("execution_order").is_none());
    let decoded: ExecutablePlan = serde_json::from_value(encoded).unwrap();
    assert_eq!(decoded.root(), id(1));
    assert_eq!(
        decoded.execution_order().step_ids().collect::<Vec<_>>(),
        vec![id(1)]
    );

    let subplan = ExecutableSubplan::new(
        ir::AtLeast::<_, 1>::from_one(run_step(1, Vec::new(), ExecCondition::Always)),
        id(1),
    )
    .unwrap();
    let encoded_subplan = serde_json::to_value(&subplan).unwrap();
    assert!(encoded_subplan.get("execution_order").is_none());
    let decoded_subplan: ExecutableSubplan = serde_json::from_value(encoded_subplan).unwrap();
    assert_eq!(decoded_subplan.root(), id(1));
    assert_eq!(
        decoded_subplan
            .execution_order()
            .step_ids()
            .collect::<Vec<_>>(),
        vec![id(1)]
    );

    assert!(matches!(
        executable(
            ir::AtLeast::<_, 1>::from_one_and_rest(
                step(1, Vec::new(), ExecSchedule::Pipeline),
                vec![step(1, Vec::new(), ExecSchedule::Pipeline)]
            ),
            id(1)
        ),
        Err(ExecPlanError::DuplicateStepId { .. })
    ));
    assert!(matches!(
        executable(
            ir::AtLeast::<_, 1>::from_one(step(1, Vec::new(), ExecSchedule::Pipeline)),
            id(2)
        ),
        Err(ExecPlanError::MissingRoot { .. })
    ));
    assert!(matches!(
        executable(
            ir::AtLeast::<_, 1>::from_one(step(1, vec![id(1)], ExecSchedule::Pipeline)),
            id(1)
        ),
        Err(ExecPlanError::SelfDependency { .. })
    ));
    assert!(matches!(
        executable(
            ir::AtLeast::<_, 1>::from_one(step(
                1,
                vec![id(2)],
                ExecSchedule::Parallel {
                    max_concurrency: properties::PositiveUsize::new(2).unwrap(),
                    preserve_order: false,
                }
            )),
            id(1)
        ),
        Err(ExecPlanError::InvalidParallelDependencyCount { .. })
    ));

    assert_eq!(
        coalesce_multi_get_batches(
            Vec::new(),
            properties::KeyLocality::Close,
            &cost::StorageCostProfile::default()
        )
        .unwrap(),
        Vec::<KvMultiGetPlan>::new()
    );
    assert!(matches!(
        KvMultiGetPlan::new(
            Vec::new(),
            properties::KeyLocality::Close,
            properties::PositiveUsize::new(4).unwrap()
        ),
        Err(ExecPlanError::EmptyMultiGet)
    ));

    let errors = [
        ExecPlanError::EmptyMultiGet,
        ExecPlanError::MixedMultiGetKeyspace {
            expected: ElementKeyspace::NodeProperty,
            actual: ElementKeyspace::EdgeEndpoints,
        },
        ExecPlanError::MultiGetBatchTooLarge {
            max: properties::PositiveUsize::new(1).unwrap(),
            actual: 2,
        },
        ExecPlanError::UnsupportedSimpleAccessLeaf {
            element: properties::ElementKind::Node,
        },
        ExecPlanError::DuplicateStepId { id: id(1) },
        ExecPlanError::MissingRoot { root: id(2) },
        ExecPlanError::MissingDependency {
            step: id(1),
            dependency: id(2),
        },
        ExecPlanError::SelfDependency { step: id(1) },
        ExecPlanError::DependencyCycle { step: id(1) },
        ExecPlanError::UnreachableStep {
            step: id(2),
            root: id(1),
        },
        ExecPlanError::InvalidParallelDependencyCount {
            step: id(1),
            actual: 1,
        },
        ExecPlanError::PreviousConditionMissingDependency {
            step: id(2),
            dependency: id(1),
        },
        ExecPlanError::InvalidExecutionStage { actual: 0 },
        ExecPlanError::IncompleteExecutionOrder {
            emitted: 1,
            total: 2,
        },
        ExecPlanError::UnsupportedSelectedExecutableAlternative {
            reason: name("incompatible"),
        },
    ];
    assert!(errors.iter().all(|error| !error.to_string().is_empty()));
}
