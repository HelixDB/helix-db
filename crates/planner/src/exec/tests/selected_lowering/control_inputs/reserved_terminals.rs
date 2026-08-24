use super::*;

#[test]
fn selected_executable_batch_lowers_reserved_root_terminal_input() {
    let profile = cost::StorageCostProfile::default();
    let branch_alternative = selected_branch_alternative(&profile);
    let branch_input = SelectedRootStreamInput::Branch(Box::new(selected_root_branch(
        branch_alternative.clone(),
        selected_kv_node_scan_root(),
        SelectedBranchPlan::Optional(Box::new(selected_kv_node_scan_root())),
    )));
    let reserved_delivered = selected::lowering::selected_stream_reserved_delivered_properties(
        branch_alternative.delivered.clone(),
        &ir::ReservedOp::Fold,
    );
    let reserved_cost = profile.stream_operator(profile.default_unknown_scan_rows);
    let reserved_alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one(physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Reserved,
            )),
        )),
        reserved_delivered,
        reserved_cost,
    );
    let terminal_delivered = project_delivered_properties(
        reserved_alternative.delivered.clone(),
        &ir::ProjectionPlan::Exists,
    );
    let terminal_cost = profile.stream_operator(cost::EstimatedRows::rows(1));
    let terminal_alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one(physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Project,
            )),
        )),
        terminal_delivered.clone(),
        terminal_cost,
    );

    let plan = ExecutablePlan::from_selected_executable_batch(SelectedExecutableBatchPlanRequest {
        kind: ir::PlanKind::Read,
        returns: ir::ReturnPlan::None,
        trace: trace::PlanningTrace::default(),
        metrics: PlannerMetrics::default(),
        entries: SelectedExecutableBatchEntries::Single(SelectedInitialExecutableBatchEntry::Run(
            Box::new(SelectedExecutableRunEntry {
                root: SelectedExecutableRunRoot::Terminal(Box::new(selected_root_terminal_plan(
                    terminal_alternative,
                    SelectedRootTerminal::Project {
                        input: SelectedRootStreamInput::Terminal(Box::new(
                            selected_root_terminal_plan(
                                reserved_alternative,
                                SelectedRootTerminal::Reserved {
                                    input: branch_input,
                                    op: ir::ReservedOp::Fold,
                                },
                            ),
                        )),
                        projection: ir::ProjectionPlan::Exists,
                    },
                ))),
                output: ir::BatchOutputPlan::Bind(name("count")),
                return_shape: ReturnShape::Scalar,
                condition: ir::RunConditionPlan::Always,
            }),
        )),
        profile: &profile,
    })
    .unwrap();

    assert_eq!(plan.steps().len(), 4);
    assert!(matches!(&plan.steps()[0].op, ExecOp::KvRead(_)));
    assert!(matches!(
        &plan.steps()[1].op,
        ExecOp::Branch {
            plan: ExecBranchPlan::Optional(body)
        } if body.steps().len() == 1 && matches!(&body.steps()[0].op, ExecOp::KvRead(_))
    ));
    assert_eq!(plan.steps()[1].dependencies, vec![plan.steps()[0].id]);
    assert!(matches!(
        &plan.steps()[2].op,
        ExecOp::Reserved {
            op: ir::ReservedOp::Fold,
        }
    ));
    assert_eq!(plan.steps()[2].dependencies, vec![plan.steps()[1].id]);
    assert!(matches!(
        &plan.steps()[3].op,
        ExecOp::Project {
            projection: ir::ProjectionPlan::Exists,
        }
    ));
    assert_eq!(plan.steps()[3].dependencies, vec![plan.steps()[2].id]);
    assert_eq!(plan.steps()[3].delivered, terminal_delivered);
    assert!(matches!(
        &plan.steps()[3].output,
        ir::BatchOutputPlan::Bind(name) if name.as_ref() == "count"
    ));
}

#[test]
fn selected_executable_batch_lowers_reserved_root_aggregate_terminal_input() {
    let profile = cost::StorageCostProfile::default();
    let branch_alternative = selected_branch_alternative(&profile);
    let branch_input = SelectedRootStreamInput::Branch(Box::new(selected_root_branch(
        branch_alternative.clone(),
        selected_kv_node_scan_root(),
        SelectedBranchPlan::Optional(Box::new(selected_kv_node_scan_root())),
    )));
    let reserved_delivered = selected::lowering::selected_stream_reserved_delivered_properties(
        branch_alternative.delivered.clone(),
        &ir::ReservedOp::Fold,
    );
    let reserved_cost = profile.stream_operator(profile.default_unknown_scan_rows);
    let reserved_alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one(physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Reserved,
            )),
        )),
        reserved_delivered,
        reserved_cost,
    );
    let aggregate = ir::AggregatePlan::Group(name("kind"));
    let terminal_delivered =
        aggregate_delivered_properties(properties::DeliveredProperties::default(), &aggregate);
    let terminal_cost = profile.explicit_sort(cost::EstimatedRows::rows(1));
    let terminal_alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one(physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Aggregate,
            )),
        )),
        terminal_delivered.clone(),
        terminal_cost,
    );

    let plan = ExecutablePlan::from_selected_executable_batch(SelectedExecutableBatchPlanRequest {
        kind: ir::PlanKind::Read,
        returns: ir::ReturnPlan::None,
        trace: trace::PlanningTrace::default(),
        metrics: PlannerMetrics::default(),
        entries: SelectedExecutableBatchEntries::Single(SelectedInitialExecutableBatchEntry::Run(
            Box::new(SelectedExecutableRunEntry {
                root: SelectedExecutableRunRoot::Terminal(Box::new(selected_root_terminal_plan(
                    terminal_alternative,
                    SelectedRootTerminal::Aggregate {
                        input: SelectedRootStreamInput::Terminal(Box::new(
                            selected_root_terminal_plan(
                                reserved_alternative,
                                SelectedRootTerminal::Reserved {
                                    input: branch_input,
                                    op: ir::ReservedOp::Fold,
                                },
                            ),
                        )),
                        aggregate,
                    },
                ))),
                output: ir::BatchOutputPlan::Bind(name("groups")),
                return_shape: ReturnShape::List,
                condition: ir::RunConditionPlan::Always,
            }),
        )),
        profile: &profile,
    })
    .unwrap();

    assert_eq!(plan.steps().len(), 4);
    assert!(matches!(&plan.steps()[0].op, ExecOp::KvRead(_)));
    assert!(matches!(
        &plan.steps()[1].op,
        ExecOp::Branch {
            plan: ExecBranchPlan::Optional(body)
        } if body.steps().len() == 1 && matches!(&body.steps()[0].op, ExecOp::KvRead(_))
    ));
    assert_eq!(plan.steps()[1].dependencies, vec![plan.steps()[0].id]);
    assert!(matches!(
        &plan.steps()[2].op,
        ExecOp::Reserved {
            op: ir::ReservedOp::Fold,
        }
    ));
    assert_eq!(plan.steps()[2].dependencies, vec![plan.steps()[1].id]);
    assert!(matches!(
        &plan.steps()[3].op,
        ExecOp::Aggregate {
            aggregate: ir::AggregatePlan::Group(property),
        } if property.as_ref() == "kind"
    ));
    assert_eq!(plan.steps()[3].schedule, ExecSchedule::Barrier);
    assert_eq!(plan.steps()[3].dependencies, vec![plan.steps()[2].id]);
    assert_eq!(plan.steps()[3].delivered, terminal_delivered);
    assert!(matches!(
        &plan.steps()[3].output,
        ir::BatchOutputPlan::Bind(name) if name.as_ref() == "groups"
    ));
}

#[test]
fn selected_executable_batch_lowers_reserved_root_variable_write_terminal_input() {
    let profile = cost::StorageCostProfile::default();
    let branch_alternative = selected_branch_alternative(&profile);
    let branch_input = SelectedRootStreamInput::Branch(Box::new(selected_root_branch(
        branch_alternative.clone(),
        selected_kv_node_scan_root(),
        SelectedBranchPlan::Optional(Box::new(selected_kv_node_scan_root())),
    )));
    let reserved_delivered = selected::lowering::selected_stream_reserved_delivered_properties(
        branch_alternative.delivered.clone(),
        &ir::ReservedOp::Fold,
    );
    let reserved_cost = profile.stream_operator(profile.default_unknown_scan_rows);
    let reserved_alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one(physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Reserved,
            )),
        )),
        reserved_delivered,
        reserved_cost,
    );
    let write_op = logical::StreamVariableWriteOp::Store(name("cached"));
    let terminal_delivered =
        selected::lowering::selected_stream_variable_write_delivered_properties(
            reserved_alternative.delivered.clone(),
            &write_op,
        );
    let terminal_cost = profile.stream_operator(cost::EstimatedRows::rows(1));
    let terminal_alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one(physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Variable,
            )),
        )),
        terminal_delivered.clone(),
        terminal_cost,
    );

    let plan = ExecutablePlan::from_selected_executable_batch(SelectedExecutableBatchPlanRequest {
        kind: ir::PlanKind::Read,
        returns: ir::ReturnPlan::None,
        trace: trace::PlanningTrace::default(),
        metrics: PlannerMetrics::default(),
        entries: SelectedExecutableBatchEntries::Single(SelectedInitialExecutableBatchEntry::Run(
            Box::new(SelectedExecutableRunEntry {
                root: SelectedExecutableRunRoot::Terminal(Box::new(selected_root_terminal_plan(
                    terminal_alternative,
                    SelectedRootTerminal::VariableWrite {
                        input: SelectedRootStreamInput::Terminal(Box::new(
                            selected_root_terminal_plan(
                                reserved_alternative,
                                SelectedRootTerminal::Reserved {
                                    input: branch_input,
                                    op: ir::ReservedOp::Fold,
                                },
                            ),
                        )),
                        op: write_op,
                    },
                ))),
                output: ir::BatchOutputPlan::Bind(name("paths")),
                return_shape: ReturnShape::List,
                condition: ir::RunConditionPlan::Always,
            }),
        )),
        profile: &profile,
    })
    .unwrap();

    assert_eq!(plan.steps().len(), 4);
    assert!(matches!(&plan.steps()[0].op, ExecOp::KvRead(_)));
    assert!(matches!(
        &plan.steps()[1].op,
        ExecOp::Branch {
            plan: ExecBranchPlan::Optional(body)
        } if body.steps().len() == 1 && matches!(&body.steps()[0].op, ExecOp::KvRead(_))
    ));
    assert_eq!(plan.steps()[1].dependencies, vec![plan.steps()[0].id]);
    assert!(matches!(
        &plan.steps()[2].op,
        ExecOp::Reserved {
            op: ir::ReservedOp::Fold,
        }
    ));
    assert_eq!(plan.steps()[2].dependencies, vec![plan.steps()[1].id]);
    assert!(matches!(
        &plan.steps()[3].op,
        ExecOp::Variable {
            op: ExecVariableOp::Stream(ir::StreamVariableOp::Store(variable))
        } if variable.as_ref() == "cached"
    ));
    assert_eq!(plan.steps()[3].schedule, ExecSchedule::Barrier);
    assert_eq!(plan.steps()[3].dependencies, vec![plan.steps()[2].id]);
    assert_eq!(plan.steps()[3].delivered, terminal_delivered);
    assert!(matches!(
        &plan.steps()[3].output,
        ir::BatchOutputPlan::Bind(name) if name.as_ref() == "paths"
    ));
}
