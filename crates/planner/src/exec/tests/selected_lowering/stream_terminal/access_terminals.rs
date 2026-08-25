use super::super::super::*;

#[test]
fn selected_stream_variable_write_lowers_terminal_to_native_dag() {
    let profile = cost::StorageCostProfile::default();
    let alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one_and_rest(
                selected_kv_node_access(),
                vec![physical::PhysicalPipelineOp::Stream(
                    physical::PhysicalStreamOp::Variable,
                )],
            ),
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
            input: selected_access_stream_input(ir::NodeAccessPlan::AllScan),
            op: logical::StreamVariableWriteOp::Store(name("cached")),
        },
        ir::BatchOutputPlan::Bind(name("users")),
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
        ExecOp::Variable {
            op: ExecVariableOp::Stream(ir::StreamVariableOp::Store(variable))
        } if variable.as_ref() == "cached"
    ));
    assert_eq!(plan.steps()[1].schedule, ExecSchedule::Barrier);
    assert_eq!(
        plan.steps()[1].delivered.effect,
        properties::EffectKind::Barrier
    );
    assert_eq!(plan.steps()[1].dependencies, vec![plan.steps()[0].id]);
    assert!(matches!(
        &plan.steps()[1].output,
        ir::BatchOutputPlan::Bind(name) if name.as_ref() == "users"
    ));
}

#[test]
fn selected_stream_terminal_rejects_non_pipeline_physical_shape() {
    let alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Access {
            element: properties::ElementKind::Node,
            access: physical::PhysicalAccess::Empty,
        },
        properties::DeliveredProperties::default(),
        cost::CostVector::ZERO,
    );

    let result = SelectedRootTerminalPlan::new(
        alternative.into(),
        selected_root_provenance(),
        SelectedRootTerminal::Project {
            input: selected_access_stream_input(ir::NodeAccessPlan::AllScan),
            projection: ir::ProjectionPlan::Exists,
        },
    );

    assert_eq!(
        result,
        Err(SelectedRootConstructionError::IncompatiblePhysicalShape)
    );
}

#[test]
fn selected_stream_terminal_rejects_incompatible_physical_suffix() {
    let alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one(selected_kv_node_access()),
        )),
        properties::DeliveredProperties::default(),
        cost::CostVector::ZERO,
    );

    let result = SelectedRootTerminalPlan::new(
        alternative.into(),
        selected_root_provenance(),
        SelectedRootTerminal::Project {
            input: selected_access_stream_input(ir::NodeAccessPlan::AllScan),
            projection: ir::ProjectionPlan::Exists,
        },
    );

    assert_eq!(
        result,
        Err(SelectedRootConstructionError::RootTerminalPhysicalSuffixMismatch)
    );
}

#[test]
fn selected_stream_project_and_aggregate_pipelines_lower_to_native_dag() {
    let profile = cost::StorageCostProfile::default();
    let predicate = predicate();
    let order_keys = ir::OrderKeys::from(ir::OrderKey {
        property: name("age"),
        order: Order::Asc,
    });
    let project = SelectedRootTerminal::Project {
        input: SelectedRootStreamInput::Access(logical::AccessStream::Filter(
            logical::AccessFilter::new(
                node_access_path(ir::NodeAccessPlan::AllScan),
                predicate.clone(),
            ),
        )),
        projection: ir::ProjectionPlan::Exists,
    };
    let aggregate = SelectedRootTerminal::Aggregate {
        input: SelectedRootStreamInput::Access(logical::AccessStream::Order(
            logical::AccessOrder::new(
                node_access_path(ir::NodeAccessPlan::AllScan),
                order_keys.clone(),
            ),
        )),
        aggregate: ir::AggregatePlan::Group(name("kind")),
    };
    let cases = [
        (
            project,
            vec![
                physical::PhysicalPipelineOp::ResidualFilter,
                physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Project),
            ],
            "project",
        ),
        (
            aggregate,
            vec![
                physical::PhysicalPipelineOp::Sort,
                physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Aggregate),
            ],
            "aggregate",
        ),
    ];

    for (terminal, suffix, expected) in cases {
        let alternative = physical::PhysicalAlternative::new(
            physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
                ir::AtLeast::<_, 1>::from_one_and_rest(selected_kv_node_access(), suffix),
            )),
            properties::DeliveredProperties::default(),
            cost::CostVector::ZERO,
        );

        let plan = selected_terminal_plan(
            alternative,
            terminal,
            ir::BatchOutputPlan::Discard,
            &profile,
        );

        assert!(matches!(
            &plan.steps()[0].op,
            ExecOp::KvRead(KvReadPlan::RangeScan { keyspace, .. })
                if *keyspace == ElementKeyspace::NodeProperty
        ));
        match expected {
            "project" => {
                assert_eq!(plan.steps().len(), 3);
                assert!(matches!(
                    &plan.steps()[1].op,
                    ExecOp::Filter { predicate: lowered } if lowered == &predicate
                ));
                assert!(matches!(
                    &plan.steps()[2].op,
                    ExecOp::Project {
                        projection: ir::ProjectionPlan::Exists
                    }
                ));
                assert_eq!(plan.steps()[2].dependencies, vec![plan.steps()[1].id]);
            }
            "aggregate" => {
                assert_eq!(plan.steps().len(), 3);
                assert!(matches!(
                    &plan.steps()[1].op,
                    ExecOp::Order {
                        plan: ir::OrderPlan::ExplicitSort(keys),
                    } if keys == &order_keys
                ));
                assert!(matches!(
                    &plan.steps()[2].op,
                    ExecOp::Aggregate {
                        aggregate: ir::AggregatePlan::Group(property),
                    } if property.as_ref() == "kind"
                ));
                assert_eq!(plan.steps()[2].dependencies, vec![plan.steps()[1].id]);
            }
            _ => unreachable!("test cases use known expected operators"),
        }
    }
}
