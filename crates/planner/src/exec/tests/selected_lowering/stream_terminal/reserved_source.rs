use super::super::super::*;

#[test]
fn selected_stream_reserved_lowers_terminal_to_native_dag() {
    let profile = cost::StorageCostProfile::default();
    let alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one_and_rest(
                selected_kv_node_access(),
                vec![physical::PhysicalPipelineOp::Stream(
                    physical::PhysicalStreamOp::Reserved,
                )],
            ),
        )),
        properties::DeliveredProperties {
            cardinality: properties::CardinalityBounds::zero_to(Some(1)),
            materialization: properties::Materialization::Materialized,
            ..properties::DeliveredProperties::default()
        },
        cost::CostVector::ZERO,
    );

    let plan = selected_terminal_plan(
        alternative,
        SelectedRootTerminal::Reserved {
            input: selected_access_stream_input(ir::NodeAccessPlan::AllScan),
            op: ir::ReservedOp::Fold,
        },
        ir::BatchOutputPlan::Bind(name("folded")),
        &profile,
    );

    assert_eq!(plan.steps().len(), 2);
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
    assert_eq!(plan.steps()[1].schedule, ExecSchedule::Barrier);
    assert_eq!(plan.steps()[1].delivered.cardinality.upper(), Some(1));
    assert_eq!(plan.steps()[1].dependencies, vec![plan.steps()[0].id]);
    assert!(matches!(
        &plan.steps()[1].output,
        ir::BatchOutputPlan::Bind(name) if name.as_ref() == "folded"
    ));
}
