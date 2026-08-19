use super::*;

#[test]
fn selected_executable_batch_lowers_control_root_pipeline_terminal_with_selected_input() {
    let profile = cost::StorageCostProfile::default();
    let branch_alternative = selected_branch_alternative(&profile);
    let expand = ir::ExpandPlan {
        direction: ir::ExpandDirection::Out,
        output: ir::ExpandOutput::Edges,
        label: ir::ExpandLabelPlan::Label(name("LIKES")),
    };
    let pipeline_cost = profile.stream_operator(profile.default_unknown_scan_rows);
    let pipeline_alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one(physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Expand,
            )),
        )),
        expand_delivered_properties(&expand),
        pipeline_cost,
    );
    let terminal_delivered = project_delivered_properties(
        pipeline_alternative.delivered.clone(),
        &ir::ProjectionPlan::Exists,
    );
    let terminal_cost = profile.stream_operator(profile.default_unknown_scan_rows);
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
                        input: SelectedRootStreamInput::Pipeline(Box::new(selected_root_pipeline(
                            pipeline_alternative,
                            SelectedRootStreamInput::Branch(Box::new(selected_root_branch(
                                branch_alternative,
                                selected_kv_node_scan_root(),
                                SelectedBranchPlan::Optional(
                                    Box::new(selected_kv_node_scan_root()),
                                ),
                            ))),
                            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Expand {
                                plan: expand.clone(),
                            }),
                        ))),
                        projection: ir::ProjectionPlan::Exists,
                    },
                ))),
                output: ir::BatchOutputPlan::Bind(name("count")),
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
        ExecOp::Expand { plan } if plan == &expand
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
    assert_eq!(plan.steps()[3].cost, terminal_cost);
    assert!(matches!(
        &plan.steps()[3].output,
        ir::BatchOutputPlan::Bind(name) if name.as_ref() == "count"
    ));
}
