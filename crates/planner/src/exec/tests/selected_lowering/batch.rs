use super::super::*;

#[test]
fn selected_executable_alternative_lowers_to_native_executable_subplan() {
    let source = node_access_expr(ir::NodeAccessPlan::AllScan);
    let alternative = selected_kv_node_scan();
    let output = ir::BatchOutputPlan::Bind(name("users"));

    let subplan = ExecutableSubplan::from_selected_executable_alternative_with_io(
        &source,
        &alternative,
        &cost::StorageCostProfile::default(),
        output.clone(),
        ExecCondition::Always,
    )
    .unwrap();

    assert_eq!(subplan.steps().len(), 1);
    assert_eq!(subplan.steps()[0].output, output);
    assert_eq!(subplan.steps()[0].cost, alternative.cost);
    assert_eq!(subplan.steps()[0].delivered, alternative.delivered);
    assert!(matches!(
        &subplan.steps()[0].op,
        ExecOp::KvRead(KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == ElementKeyspace::NodeProperty
    ));

    let executable = ExecutablePlan::from_selected_executable_alternative_with_io(
        SelectedExecutablePlanRequest {
            kind: ir::PlanKind::Read,
            returns: ir::ReturnPlan::None,
            trace: trace::PlanningTrace::default(),
            metrics: PlannerMetrics::default(),
            source_expr: &source,
            alternative: &alternative,
            profile: &cost::StorageCostProfile::default(),
            output: ir::BatchOutputPlan::Discard,
            condition: ExecCondition::Always,
        },
    )
    .unwrap();
    assert_eq!(executable.steps().len(), 1);
}

#[test]
fn selected_executable_alternative_preserves_upstream_dependencies_inside_dag() {
    let profile = cost::StorageCostProfile::default();
    let source = node_access_expr(ir::NodeAccessPlan::AllScan);
    let alternative = selected_kv_node_scan();
    let mut lowering = ExecutableDagBuilder::new(&profile);
    let prior = lowering
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

    let root = lowering
        .push_selected_executable_alternative(
            &source,
            &SelectedPhysicalPlan::from(&alternative),
            vec![prior],
            ir::BatchOutputPlan::Bind(name("users")),
            ExecCondition::Always,
        )
        .unwrap();
    lowering
        .override_step_contract(root, alternative.delivered.clone(), alternative.cost)
        .unwrap();
    let subplan = lowering
        .finish_with_root(
            root,
            SelectedExecutableRejectionReason::SelectedAlternativeEmptyDag,
        )
        .unwrap();

    assert_eq!(subplan.steps().len(), 2);
    assert_eq!(subplan.steps()[1].dependencies, vec![prior]);
    assert!(matches!(
        &subplan.steps()[1].op,
        ExecOp::KvRead(KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == ElementKeyspace::NodeProperty
    ));
}

#[test]
fn selected_executable_lowering_finish_rejects_empty_step_sets() {
    let profile = cost::StorageCostProfile::default();
    let root = ExecStepId::new(1).unwrap();

    let result = ExecutableDagBuilder::new(&profile).finish_with_root(
        root,
        SelectedExecutableRejectionReason::SelectedAlternativeEmptyDag,
    );

    assert!(matches!(
        result,
        Err(ExecPlanError::UnsupportedSelectedExecutableAlternative { .. })
    ));
}

#[test]
fn selected_executable_lowering_finish_requires_previous_root() {
    let profile = cost::StorageCostProfile::default();

    let result = ExecutableDagBuilder::new(&profile).finish_with_previous(
        SelectedExecutableRejectionReason::SelectedBatchEntriesMissingRoot,
        SelectedExecutableRejectionReason::SelectedBatchEntriesEmptyDag,
    );

    assert!(matches!(
        result,
        Err(ExecPlanError::UnsupportedSelectedExecutableAlternative { .. })
    ));
}

#[test]
fn selected_executable_lowering_override_rejects_unknown_step_ids() {
    let profile = cost::StorageCostProfile::default();
    let unknown_step = ExecStepId::new(1).unwrap();

    let result = ExecutableDagBuilder::new(&profile).override_step_contract(
        unknown_step,
        properties::DeliveredProperties::default(),
        cost::CostVector::ZERO,
    );

    assert!(matches!(
        result,
        Err(ExecPlanError::UnsupportedSelectedExecutableAlternative { .. })
    ));
}

#[test]
fn selected_executable_batch_lowers_followup_conditions_inside_one_dag() {
    let profile = cost::StorageCostProfile::default();
    let source = node_access_expr(ir::NodeAccessPlan::AllScan);
    let alternative = selected_kv_node_scan();
    let plan = ExecutablePlan::from_selected_executable_batch(SelectedExecutableBatchPlanRequest {
        kind: ir::PlanKind::Read,
        returns: ir::ReturnPlan::None,
        trace: trace::PlanningTrace::default(),
        metrics: PlannerMetrics::default(),
        entries: SelectedExecutableBatchEntries::WithFollowups {
            first: SelectedInitialExecutableBatchEntry::Run(Box::new(SelectedExecutableRunEntry {
                root: SelectedExecutableRunRoot::alternative(source.clone(), alternative.clone()),
                output: ir::BatchOutputPlan::Bind(name("seed")),
                return_shape: ReturnShape::List,
                condition: ir::RunConditionPlan::Always,
            })),
            rest: ir::AtLeast::<_, 1>::from_one(SelectedFollowupExecutableBatchEntry::Run(
                Box::new(SelectedExecutableRunEntry {
                    root: SelectedExecutableRunRoot::alternative(source, alternative),
                    output: ir::BatchOutputPlan::Bind(name("users")),
                    return_shape: ReturnShape::List,
                    condition: ir::RunConditionPlan::If(ir::BatchConditionPlan::PrevNotEmpty),
                }),
            )),
        },
        profile: &profile,
    })
    .unwrap();

    assert_eq!(plan.steps().len(), 2);
    assert_eq!(plan.steps()[1].dependencies, vec![plan.steps()[0].id]);
    assert!(matches!(
        plan.steps()[1].condition,
        ExecCondition::PreviousStepNotEmpty { dependency }
            if dependency == plan.steps()[0].id
    ));
    assert!(matches!(
        &plan.steps()[0].op,
        ExecOp::KvRead(KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == ElementKeyspace::NodeProperty
    ));
    assert!(matches!(
        &plan.steps()[1].output,
        ir::BatchOutputPlan::Bind(name) if name.as_ref() == "users"
    ));
}

#[test]
fn selected_executable_batch_lowers_foreach_body_as_selected_subplan() {
    let profile = cost::StorageCostProfile {
        foreach_overhead: cost::LatencyEstimate::micros(123),
        ..cost::StorageCostProfile::default()
    };
    let source = node_access_expr(ir::NodeAccessPlan::AllScan);
    let alternative = selected_kv_node_scan();
    let body_entries = SelectedExecutableBatchEntries::Single(
        SelectedInitialExecutableBatchEntry::Run(Box::new(SelectedExecutableRunEntry {
            root: SelectedExecutableRunRoot::alternative(source.clone(), alternative.clone()),
            output: ir::BatchOutputPlan::Discard,
            return_shape: ReturnShape::List,
            condition: ir::RunConditionPlan::Always,
        })),
    );

    let plan = ExecutablePlan::from_selected_executable_batch(SelectedExecutableBatchPlanRequest {
        kind: ir::PlanKind::Write,
        returns: ir::ReturnPlan::None,
        trace: trace::PlanningTrace::default(),
        metrics: PlannerMetrics::default(),
        entries: SelectedExecutableBatchEntries::WithFollowups {
            first: SelectedInitialExecutableBatchEntry::Run(Box::new(SelectedExecutableRunEntry {
                root: SelectedExecutableRunRoot::alternative(source, alternative),
                output: ir::BatchOutputPlan::Bind(name("seed")),
                return_shape: ReturnShape::List,
                condition: ir::RunConditionPlan::Always,
            })),
            rest: ir::AtLeast::<_, 1>::from_one(SelectedFollowupExecutableBatchEntry::ForEach(
                SelectedForEachBatch::new(name("items"), body_entries),
            )),
        },
        profile: &profile,
    })
    .unwrap();

    assert_eq!(plan.steps().len(), 2);
    assert!(matches!(&plan.steps()[0].op, ExecOp::KvRead(_)));
    assert_eq!(plan.steps()[1].dependencies, vec![plan.steps()[0].id]);
    assert_eq!(plan.steps()[1].schedule, ExecSchedule::Barrier);
    let ExecOp::ForEach { param, body } = &plan.steps()[1].op else {
        panic!("expected foreach step");
    };
    assert_eq!(param.as_ref(), "items");
    assert_eq!(body.steps().len(), 1);
    assert!(matches!(&body.steps()[0].op, ExecOp::KvRead(_)));
    assert_eq!(
        plan.steps()[1].cost,
        profile.foreach_wrapper().serial(body.steps()[0].cost)
    );
}

#[test]
fn selected_foreach_batch_exposes_validated_parts() {
    let source = node_access_expr(ir::NodeAccessPlan::AllScan);
    let alternative = selected_kv_node_scan();
    let body = SelectedExecutableBatchEntries::Single(SelectedInitialExecutableBatchEntry::Run(
        Box::new(SelectedExecutableRunEntry {
            root: SelectedExecutableRunRoot::alternative(source, alternative),
            output: ir::BatchOutputPlan::Discard,
            return_shape: ReturnShape::List,
            condition: ir::RunConditionPlan::Always,
        }),
    ));
    let foreach = SelectedForEachBatch::new(name("items"), body.clone());

    assert_eq!(foreach.param().as_ref(), "items");
    assert_eq!(foreach.body(), &body);
    assert_eq!(foreach.into_parts(), (name("items"), body));
}

#[test]
fn selected_executable_batch_lowers_mutation_input_as_selected_run_root() {
    let profile = cost::StorageCostProfile::default();
    let input_source = node_access_expr(ir::NodeAccessPlan::AllScan);
    let input_alternative = selected_kv_node_scan();
    let mutation_alternative = selected_mutation_alternative(&profile);

    let plan = ExecutablePlan::from_selected_executable_batch(SelectedExecutableBatchPlanRequest {
        kind: ir::PlanKind::Write,
        returns: ir::ReturnPlan::None,
        trace: trace::PlanningTrace::default(),
        metrics: PlannerMetrics::default(),
        entries: SelectedExecutableBatchEntries::Single(SelectedInitialExecutableBatchEntry::Run(
            Box::new(SelectedExecutableRunEntry {
                root: SelectedExecutableRunRoot::Mutation(Box::new(selected_root_mutation(
                    mutation_alternative,
                    SelectedMutationPlan::SetProperty {
                        input: Box::new(SelectedExecutableRunRoot::alternative(
                            input_source,
                            input_alternative,
                        )),
                        name: name("active"),
                        value: ir::PropertyInputPlan::new(PropertyInput::from(true)).unwrap(),
                    },
                ))),
                output: ir::BatchOutputPlan::Bind(name("updated")),
                return_shape: ReturnShape::List,
                condition: ir::RunConditionPlan::Always,
            }),
        )),
        profile: &profile,
    })
    .unwrap();

    assert_eq!(plan.steps().len(), 2);
    assert!(matches!(&plan.steps()[0].op, ExecOp::KvRead(_)));
    assert_eq!(plan.steps()[1].dependencies, vec![plan.steps()[0].id]);
    assert_eq!(plan.steps()[1].schedule, ExecSchedule::Barrier);
    assert!(matches!(
        &plan.steps()[1].op,
        ExecOp::Mutation {
            plan: ExecMutationPlan::SetProperty { name, .. }
        } if name.as_ref() == "active"
    ));
    assert!(matches!(
        &plan.steps()[1].output,
        ir::BatchOutputPlan::Bind(name) if name.as_ref() == "updated"
    ));
}

#[test]
fn selected_executable_batch_lowers_control_payloads_as_selected_subplans() {
    let profile = cost::StorageCostProfile::default();
    let branch_alternative = selected_branch_alternative(&profile);

    let branch =
        ExecutablePlan::from_selected_executable_batch(SelectedExecutableBatchPlanRequest {
            kind: ir::PlanKind::Read,
            returns: ir::ReturnPlan::None,
            trace: trace::PlanningTrace::default(),
            metrics: PlannerMetrics::default(),
            entries: SelectedExecutableBatchEntries::Single(
                SelectedInitialExecutableBatchEntry::Run(Box::new(SelectedExecutableRunEntry {
                    root: SelectedExecutableRunRoot::Branch(Box::new(selected_root_branch(
                        branch_alternative,
                        selected_kv_node_scan_root(),
                        SelectedBranchPlan::Optional(Box::new(selected_kv_node_scan_root())),
                    ))),
                    output: ir::BatchOutputPlan::Bind(name("branched")),
                    return_shape: ReturnShape::List,
                    condition: ir::RunConditionPlan::Always,
                })),
            ),
            profile: &profile,
        })
        .unwrap();

    assert_eq!(branch.steps().len(), 2);
    assert!(matches!(&branch.steps()[0].op, ExecOp::KvRead(_)));
    assert_eq!(branch.steps()[1].dependencies, vec![branch.steps()[0].id]);
    let ExecOp::Branch {
        plan: ExecBranchPlan::Optional(body),
    } = &branch.steps()[1].op
    else {
        panic!("expected selected branch step");
    };
    assert_eq!(body.steps().len(), 1);
    assert!(matches!(&body.steps()[0].op, ExecOp::KvRead(_)));
    assert_eq!(body.root(), body.steps()[0].id);
    assert!(matches!(
        &branch.steps()[1].output,
        ir::BatchOutputPlan::Bind(name) if name.as_ref() == "branched"
    ));

    let repeat_alternative = selected_repeat_alternative(&profile);

    let repeat =
        ExecutablePlan::from_selected_executable_batch(SelectedExecutableBatchPlanRequest {
            kind: ir::PlanKind::Read,
            returns: ir::ReturnPlan::None,
            trace: trace::PlanningTrace::default(),
            metrics: PlannerMetrics::default(),
            entries: SelectedExecutableBatchEntries::Single(
                SelectedInitialExecutableBatchEntry::Run(Box::new(SelectedExecutableRunEntry {
                    root: SelectedExecutableRunRoot::Repeat(Box::new(selected_root_repeat(
                        repeat_alternative,
                        selected_kv_node_scan_root(),
                        SelectedRepeatPlan {
                            body: Box::new(selected_kv_node_scan_root()),
                            stop: ir::RepeatStopPlan::MaxDepthOnly,
                            emit: ir::RepeatEmitPlan::None,
                            max_depth: NonZeroUsize::new(3).unwrap(),
                        },
                    ))),
                    output: ir::BatchOutputPlan::Bind(name("repeated")),
                    return_shape: ReturnShape::List,
                    condition: ir::RunConditionPlan::Always,
                })),
            ),
            profile: &profile,
        })
        .unwrap();

    assert_eq!(repeat.steps().len(), 2);
    assert!(matches!(&repeat.steps()[0].op, ExecOp::KvRead(_)));
    assert_eq!(repeat.steps()[1].dependencies, vec![repeat.steps()[0].id]);
    let ExecOp::Repeat { plan } = &repeat.steps()[1].op else {
        panic!("expected selected repeat step");
    };
    assert_eq!(plan.max_depth.get(), 3);
    assert_eq!(plan.body.steps().len(), 1);
    assert!(matches!(&plan.body.steps()[0].op, ExecOp::KvRead(_)));
    assert_eq!(plan.body.root(), plan.body.steps()[0].id);
    assert!(matches!(
        &repeat.steps()[1].output,
        ir::BatchOutputPlan::Bind(name) if name.as_ref() == "repeated"
    ));
}
