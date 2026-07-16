use super::*;

#[test]
fn selected_executable_batch_lowers_reserved_root_pipeline_input() {
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
        reserved_delivered.clone(),
        reserved_cost,
    );
    let pipeline_delivered =
        materialized_delivered_properties(filtered_delivered_properties(reserved_delivered));
    let pipeline_cost = profile.explicit_sort(cost::EstimatedRows::rows(1));
    let pipeline_alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one(physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Distinct,
            )),
        )),
        pipeline_delivered.clone(),
        pipeline_cost,
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
                    SelectedRootStreamInput::Terminal(Box::new(selected_root_terminal_plan(
                        reserved_alternative,
                        SelectedRootTerminal::Reserved {
                            input: branch_input,
                            op: ir::ReservedOp::Fold,
                        },
                    ))),
                    ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Distinct),
                ))),
                output: ir::BatchOutputPlan::Bind(name("deduped")),
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
    assert!(matches!(&plan.steps()[3].op, ExecOp::Distinct));
    assert_eq!(plan.steps()[3].dependencies, vec![plan.steps()[2].id]);
    assert_eq!(plan.steps()[3].delivered, pipeline_delivered);
    assert!(matches!(
        &plan.steps()[3].output,
        ir::BatchOutputPlan::Bind(name) if name.as_ref() == "deduped"
    ));
}

#[test]
fn selected_executable_batch_lowers_reserved_root_variable_pipeline_input() {
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
        reserved_delivered.clone(),
        reserved_cost,
    );
    let variable_op = logical::PureStreamVariableOp::Select(name("cached"));
    let pipeline_delivered = selected::lowering::selected_stream_variable_delivered_properties(
        reserved_delivered,
        &variable_op,
    );
    let pipeline_cost = profile.stream_operator(cost::EstimatedRows::rows(1));
    let pipeline_alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one(physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Variable,
            )),
        )),
        pipeline_delivered.clone(),
        pipeline_cost,
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
                    SelectedRootStreamInput::Terminal(Box::new(selected_root_terminal_plan(
                        reserved_alternative,
                        SelectedRootTerminal::Reserved {
                            input: branch_input,
                            op: ir::ReservedOp::Fold,
                        },
                    ))),
                    ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Variable {
                        op: variable_op,
                    }),
                ))),
                output: ir::BatchOutputPlan::Bind(name("selected")),
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
            op: ExecVariableOp::Stream(ir::StreamVariableOp::Select(variable))
        } if variable.as_ref() == "cached"
    ));
    assert_eq!(plan.steps()[3].dependencies, vec![plan.steps()[2].id]);
    assert_eq!(plan.steps()[3].delivered, pipeline_delivered);
    assert!(matches!(
        &plan.steps()[3].output,
        ir::BatchOutputPlan::Bind(name) if name.as_ref() == "selected"
    ));
}
