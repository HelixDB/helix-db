use super::super::super::*;

#[test]
fn selected_reserved_root_stream_pipeline_lowers_to_native_dag() {
    let profile = cost::StorageCostProfile::default();
    let reserved_alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one_and_rest(
                selected_kv_node_access(),
                vec![physical::PhysicalPipelineOp::Stream(
                    physical::PhysicalStreamOp::Reserved,
                )],
            ),
        )),
        properties::DeliveredProperties {
            cardinality: properties::CardinalityBounds::exact(1),
            materialization: properties::Materialization::Materialized,
            ..properties::DeliveredProperties::default()
        },
        cost::CostVector::ZERO,
    );
    let alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one(physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Distinct,
            )),
        )),
        reserved_alternative.delivered.clone(),
        cost::CostVector::ZERO,
    );

    let plan = ExecutablePlan::from_selected_executable_batch(SelectedExecutableBatchPlanRequest {
        kind: ir::PlanKind::Read,
        returns: ir::ReturnPlan::None,
        trace: trace::PlanningTrace::default(),
        metrics: PlannerMetrics::default(),
        entries: SelectedExecutableBatchEntries::Single(SelectedInitialExecutableBatchEntry::Run(
            Box::new(SelectedExecutableRunEntry {
                root: SelectedExecutableRunRoot::Pipeline(Box::new(selected_root_pipeline(
                    alternative,
                    SelectedRootStreamInput::Terminal(Box::new(selected_root_terminal_plan(
                        reserved_alternative,
                        SelectedRootTerminal::Reserved {
                            input: SelectedRootStreamInput::Access(logical::AccessStream::Path(
                                node_access_path(ir::NodeAccessPlan::AllScan),
                            )),
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

    assert_eq!(plan.steps().len(), 3);
    assert!(matches!(
        &plan.steps()[0].op,
        ExecOp::KvRead(KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == ElementKeyspace::NodeProperty
    ));
    assert!(matches!(
        &plan.steps()[1].op,
        ExecOp::Reserved {
            op: ir::ReservedOp::Fold,
        }
    ));
    assert_eq!(plan.steps()[1].dependencies, vec![plan.steps()[0].id]);
    assert!(matches!(&plan.steps()[2].op, ExecOp::Distinct));
    assert_eq!(plan.steps()[2].dependencies, vec![plan.steps()[1].id]);
    assert!(matches!(
        &plan.steps()[2].output,
        ir::BatchOutputPlan::Bind(name) if name.as_ref() == "deduped"
    ));
}

#[test]
fn selected_reserved_root_stream_variable_pipeline_lowers_to_native_dag() {
    let profile = cost::StorageCostProfile::default();
    let reserved_alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one_and_rest(
                selected_kv_node_access(),
                vec![physical::PhysicalPipelineOp::Stream(
                    physical::PhysicalStreamOp::Reserved,
                )],
            ),
        )),
        properties::DeliveredProperties {
            cardinality: properties::CardinalityBounds::exact(1),
            ..properties::DeliveredProperties::default()
        },
        cost::CostVector::ZERO,
    );
    let variable_op = logical::PureStreamVariableOp::Select(name("cached"));
    let alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one(physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Variable,
            )),
        )),
        reserved_alternative.delivered.clone(),
        cost::CostVector::ZERO,
    );

    let plan = ExecutablePlan::from_selected_executable_batch(SelectedExecutableBatchPlanRequest {
        kind: ir::PlanKind::Read,
        returns: ir::ReturnPlan::None,
        trace: trace::PlanningTrace::default(),
        metrics: PlannerMetrics::default(),
        entries: SelectedExecutableBatchEntries::Single(SelectedInitialExecutableBatchEntry::Run(
            Box::new(SelectedExecutableRunEntry {
                root: SelectedExecutableRunRoot::Pipeline(Box::new(selected_root_pipeline(
                    alternative,
                    SelectedRootStreamInput::Terminal(Box::new(selected_root_terminal_plan(
                        reserved_alternative,
                        SelectedRootTerminal::Reserved {
                            input: SelectedRootStreamInput::Access(logical::AccessStream::Path(
                                node_access_path(ir::NodeAccessPlan::AllScan),
                            )),
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

    assert_eq!(plan.steps().len(), 3);
    assert!(matches!(
        &plan.steps()[0].op,
        ExecOp::KvRead(KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == ElementKeyspace::NodeProperty
    ));
    assert!(matches!(
        &plan.steps()[1].op,
        ExecOp::Reserved {
            op: ir::ReservedOp::Fold,
        }
    ));
    assert_eq!(plan.steps()[1].dependencies, vec![plan.steps()[0].id]);
    assert!(matches!(
        &plan.steps()[2].op,
        ExecOp::Variable {
            op: ExecVariableOp::Stream(ir::StreamVariableOp::Select(variable))
        } if variable.as_ref() == "cached"
    ));
    assert_eq!(plan.steps()[2].dependencies, vec![plan.steps()[1].id]);
    assert!(matches!(
        &plan.steps()[2].output,
        ir::BatchOutputPlan::Bind(name) if name.as_ref() == "selected"
    ));
}
