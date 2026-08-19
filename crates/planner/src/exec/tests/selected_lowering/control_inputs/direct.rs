use super::*;

#[test]
fn selected_executable_batch_lowers_control_root_pipeline_with_selected_input() {
    let profile = cost::StorageCostProfile::default();
    let branch_alternative = selected_branch_alternative(&profile);
    let expand = ir::ExpandPlan {
        direction: ir::ExpandDirection::Out,
        output: ir::ExpandOutput::Edges,
        label: ir::ExpandLabelPlan::Label(name("LIKES")),
    };
    let pipeline_alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one(physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Expand,
            )),
        )),
        expand_delivered_properties(&expand),
        profile.stream_operator(profile.default_unknown_scan_rows),
    );

    let plan = ExecutablePlan::from_selected_executable_batch(SelectedExecutableBatchPlanRequest {
        kind: ir::PlanKind::Read,
        returns: ir::ReturnPlan::None,
        trace: trace::PlanningTrace::default(),
        metrics: PlannerMetrics::default(),
        entries: SelectedExecutableBatchEntries::Single(SelectedInitialExecutableBatchEntry::Run(
            Box::new(SelectedExecutableRunEntry {
                root: SelectedExecutableRunRoot::Pipeline(Box::new(selected_root_pipeline(
                    pipeline_alternative,
                    SelectedRootStreamInput::Branch(Box::new(selected_root_branch(
                        branch_alternative,
                        selected_kv_node_scan_root(),
                        SelectedBranchPlan::Optional(Box::new(selected_kv_node_scan_root())),
                    ))),
                    ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Expand {
                        plan: expand.clone(),
                    }),
                ))),
                output: ir::BatchOutputPlan::Bind(name("expanded")),
                condition: ir::RunConditionPlan::Always,
            }),
        )),
        profile: &profile,
    })
    .unwrap();

    assert_eq!(plan.steps().len(), 3);
    assert!(matches!(&plan.steps()[0].op, ExecOp::KvRead(_)));
    assert_eq!(plan.steps()[1].dependencies, vec![plan.steps()[0].id]);
    assert!(matches!(
        &plan.steps()[1].op,
        ExecOp::Branch {
            plan: ExecBranchPlan::Optional(body)
        } if body.steps().len() == 1 && matches!(&body.steps()[0].op, ExecOp::KvRead(_))
    ));
    assert_eq!(plan.steps()[2].dependencies, vec![plan.steps()[1].id]);
    assert!(matches!(
        &plan.steps()[2].op,
        ExecOp::Expand { plan } if plan == &expand
    ));
    assert!(matches!(
        &plan.steps()[2].output,
        ir::BatchOutputPlan::Bind(name) if name.as_ref() == "expanded"
    ));
}

#[test]
fn selected_executable_batch_lowers_control_root_terminals_with_selected_input() {
    #[derive(Clone, Copy)]
    enum ExpectedTerminal {
        Project,
        Aggregate,
        Reserved,
        VariableWrite,
    }

    let profile = cost::StorageCostProfile::default();
    let branch_alternative = selected_branch_alternative(&profile);
    let branch_root = selected_root_branch(
        branch_alternative.clone(),
        selected_kv_node_scan_root(),
        SelectedBranchPlan::Optional(Box::new(selected_kv_node_scan_root())),
    );
    let branch_input = SelectedRootStreamInput::Branch(Box::new(branch_root));
    let cases = vec![
        (
            SelectedRootTerminal::Project {
                input: branch_input.clone(),
                projection: ir::ProjectionPlan::Exists,
            },
            physical::PhysicalStreamOp::Project,
            project_delivered_properties(
                branch_alternative.delivered.clone(),
                &ir::ProjectionPlan::Exists,
            ),
            profile.stream_operator(profile.default_unknown_scan_rows),
            ExpectedTerminal::Project,
        ),
        (
            SelectedRootTerminal::Aggregate {
                input: branch_input.clone(),
                aggregate: ir::AggregatePlan::Group(name("kind")),
            },
            physical::PhysicalStreamOp::Aggregate,
            aggregate_delivered_properties(
                properties::DeliveredProperties::default(),
                &ir::AggregatePlan::Group(name("kind")),
            ),
            profile.explicit_sort(profile.default_unknown_scan_rows),
            ExpectedTerminal::Aggregate,
        ),
        (
            SelectedRootTerminal::Reserved {
                input: branch_input.clone(),
                op: ir::ReservedOp::Fold,
            },
            physical::PhysicalStreamOp::Reserved,
            selected::lowering::selected_stream_reserved_delivered_properties(
                branch_alternative.delivered.clone(),
                &ir::ReservedOp::Fold,
            ),
            profile.stream_operator(profile.default_unknown_scan_rows),
            ExpectedTerminal::Reserved,
        ),
        (
            SelectedRootTerminal::VariableWrite {
                input: branch_input.clone(),
                op: logical::StreamVariableWriteOp::Store(name("cache")),
            },
            physical::PhysicalStreamOp::Variable,
            selected::lowering::selected_stream_variable_write_delivered_properties(
                branch_alternative.delivered.clone(),
                &logical::StreamVariableWriteOp::Store(name("cache")),
            ),
            profile.stream_operator(profile.default_unknown_scan_rows),
            ExpectedTerminal::VariableWrite,
        ),
    ];

    for (terminal, physical_suffix, delivered, cost, expected) in cases {
        let terminal_alternative = physical::PhysicalAlternative::new(
            physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
                ir::AtLeast::<_, 1>::from_one(physical::PhysicalPipelineOp::Stream(
                    physical_suffix,
                )),
            )),
            delivered.clone(),
            cost,
        );
        let plan =
            ExecutablePlan::from_selected_executable_batch(SelectedExecutableBatchPlanRequest {
                kind: ir::PlanKind::Read,
                returns: ir::ReturnPlan::None,
                trace: trace::PlanningTrace::default(),
                metrics: PlannerMetrics::default(),
                entries: SelectedExecutableBatchEntries::Single(
                    SelectedInitialExecutableBatchEntry::Run(Box::new(
                        SelectedExecutableRunEntry {
                            root: SelectedExecutableRunRoot::Terminal(Box::new(
                                selected_root_terminal_plan(terminal_alternative, terminal),
                            )),
                            output: ir::BatchOutputPlan::Bind(name("terminal")),
                            condition: ir::RunConditionPlan::Always,
                        },
                    )),
                ),
                profile: &profile,
            })
            .unwrap();

        assert_eq!(plan.steps().len(), 3);
        assert!(matches!(&plan.steps()[0].op, ExecOp::KvRead(_)));
        assert!(matches!(
            &plan.steps()[1].op,
            ExecOp::Branch {
                plan: ExecBranchPlan::Optional(body)
            } if body.steps().len() == 1 && matches!(&body.steps()[0].op, ExecOp::KvRead(_))
        ));
        assert_eq!(plan.steps()[1].dependencies, vec![plan.steps()[0].id]);
        assert_eq!(plan.steps()[2].dependencies, vec![plan.steps()[1].id]);
        assert_eq!(plan.steps()[2].delivered, delivered);
        assert_eq!(plan.steps()[2].cost, cost);
        assert!(matches!(
            &plan.steps()[2].output,
            ir::BatchOutputPlan::Bind(name) if name.as_ref() == "terminal"
        ));
        match expected {
            ExpectedTerminal::Project => assert!(matches!(
                &plan.steps()[2].op,
                ExecOp::Project {
                    projection: ir::ProjectionPlan::Exists,
                }
            )),
            ExpectedTerminal::Aggregate => assert!(matches!(
                &plan.steps()[2].op,
                ExecOp::Aggregate {
                    aggregate: ir::AggregatePlan::Group(property),
                } if property.as_ref() == "kind"
            )),
            ExpectedTerminal::Reserved => assert!(matches!(
                &plan.steps()[2].op,
                ExecOp::Reserved {
                    op: ir::ReservedOp::Fold,
                }
            )),
            ExpectedTerminal::VariableWrite => assert!(matches!(
                &plan.steps()[2].op,
                ExecOp::Variable {
                    op: ExecVariableOp::Stream(ir::StreamVariableOp::Store(variable))
                } if variable.as_ref() == "cache"
            )),
        }
    }
}
