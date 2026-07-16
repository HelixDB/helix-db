use super::super::super::*;

fn reserved_source_alternative() -> physical::PhysicalAlternative {
    physical::PhysicalAlternative::new(
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
    )
}

fn reserved_source_input() -> SelectedRootStreamInput {
    SelectedRootStreamInput::Terminal(Box::new(selected_root_terminal_plan(
        reserved_source_alternative(),
        SelectedRootTerminal::Reserved {
            input: selected_access_stream_input(ir::NodeAccessPlan::AllScan),
            op: ir::ReservedOp::Fold,
        },
    )))
}

#[test]
fn selected_reserved_root_stream_variable_write_lowers_to_native_dag() {
    let profile = cost::StorageCostProfile::default();
    let alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one(physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Variable,
            )),
        )),
        properties::DeliveredProperties {
            effect: properties::EffectKind::Barrier,
            ..properties::DeliveredProperties::default()
        },
        cost::CostVector::ZERO,
    );

    let plan = selected_terminal_plan(
        alternative,
        SelectedRootTerminal::VariableWrite {
            input: reserved_source_input(),
            op: logical::StreamVariableWriteOp::Store(name("cached")),
        },
        ir::BatchOutputPlan::Bind(name("paths")),
        &profile,
    );

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
            op: ExecVariableOp::Stream(ir::StreamVariableOp::Store(variable))
        } if variable.as_ref() == "cached"
    ));
    assert_eq!(plan.steps()[2].schedule, ExecSchedule::Barrier);
    assert_eq!(
        plan.steps()[2].delivered.effect,
        properties::EffectKind::Barrier
    );
    assert_eq!(plan.steps()[2].dependencies, vec![plan.steps()[1].id]);
    assert!(matches!(
        &plan.steps()[2].output,
        ir::BatchOutputPlan::Bind(name) if name.as_ref() == "paths"
    ));
}

#[test]
fn selected_reserved_root_stream_project_lowers_to_native_dag() {
    let profile = cost::StorageCostProfile::default();
    let alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one(physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Project,
            )),
        )),
        properties::DeliveredProperties {
            cardinality: properties::CardinalityBounds::exact(1),
            materialization: properties::Materialization::Materialized,
            ..properties::DeliveredProperties::default()
        },
        cost::CostVector::ZERO,
    );

    let plan = selected_terminal_plan(
        alternative,
        SelectedRootTerminal::Project {
            input: reserved_source_input(),
            projection: ir::ProjectionPlan::Count,
        },
        ir::BatchOutputPlan::Bind(name("count")),
        &profile,
    );

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
        ExecOp::Project {
            projection: ir::ProjectionPlan::Count,
        }
    ));
    assert_eq!(plan.steps()[2].dependencies, vec![plan.steps()[1].id]);
    assert!(matches!(
        &plan.steps()[2].output,
        ir::BatchOutputPlan::Bind(name) if name.as_ref() == "count"
    ));
}

#[test]
fn selected_reserved_root_stream_aggregate_lowers_to_native_dag() {
    let profile = cost::StorageCostProfile::default();
    let alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one(physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Aggregate,
            )),
        )),
        properties::DeliveredProperties {
            materialization: properties::Materialization::Materialized,
            ..properties::DeliveredProperties::default()
        },
        cost::CostVector::ZERO,
    );

    let plan = selected_terminal_plan(
        alternative,
        SelectedRootTerminal::Aggregate {
            input: reserved_source_input(),
            aggregate: ir::AggregatePlan::Group(name("kind")),
        },
        ir::BatchOutputPlan::Bind(name("groups")),
        &profile,
    );

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
        ExecOp::Aggregate {
            aggregate: ir::AggregatePlan::Group(property),
        } if property.as_ref() == "kind"
    ));
    assert_eq!(plan.steps()[2].schedule, ExecSchedule::Barrier);
    assert_eq!(plan.steps()[2].dependencies, vec![plan.steps()[1].id]);
    assert!(matches!(
        &plan.steps()[2].output,
        ir::BatchOutputPlan::Bind(name) if name.as_ref() == "groups"
    ));
}
