use super::super::*;

fn exact_access_alternative(
    element: properties::ElementKind,
    access: physical::PhysicalAccess,
) -> physical::PhysicalAlternative {
    physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Access { element, access },
        properties::DeliveredProperties {
            element: Some(element),
            ..properties::DeliveredProperties::default()
        },
        cost::CostVector::ZERO,
    )
}

#[test]
fn selected_exact_access_payloads_are_copied_without_reclassification() {
    let node_equality = |property: &str, value: &str| ir::NodeAccessPlan::EqualityIndex {
        index: catalog::NodeEqualityIndexMeta::try_new(format!("node_eq:User:{property}")).unwrap(),
        key: catalog::ScopedPropertyKey::try_new("User", property).unwrap(),
        value: index_value(value),
    };
    let node_plan = ir::NodeAccessPlan::Union(ir::AtLeast::from_pair(
        node_source(node_equality("status", "active")),
        node_source(node_equality("role", "admin")),
    ));
    let node_exact = ExecNodeAccessPlan::SecondarySet {
        set: node_secondary_set(&node_plan).unwrap(),
    };
    let node_subplan = ExecutableSubplan::from_selected_executable_alternative(
        &node_access_expr(node_plan),
        &exact_access_alternative(
            properties::ElementKind::Node,
            physical::PhysicalAccess::NodeExact(Box::new(node_exact.clone())),
        ),
        &cost::StorageCostProfile::default(),
    )
    .unwrap();
    assert!(matches!(
        &node_subplan.steps()[0].op,
        ExecOp::Access { plan }
            if plan.as_ref() == &ExecAccessPlan::Node(node_exact)
    ));

    let edge_plan = ir::EdgeAccessPlan::EqualityIndex {
        index: catalog::EdgeEqualityIndexMeta::try_new("edge_eq:FOLLOWS:status").unwrap(),
        key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
        value: index_value("active"),
    };
    let ir::EdgeAccessPlan::EqualityIndex { index, key, value } = &edge_plan else {
        unreachable!()
    };
    let edge_exact = ExecEdgeAccessPlan::exact_equality(index.clone(), key.clone(), value.clone());
    let edge_subplan = ExecutableSubplan::from_selected_executable_alternative(
        &edge_access_expr(edge_plan),
        &exact_access_alternative(
            properties::ElementKind::Edge,
            physical::PhysicalAccess::EdgeExact(Box::new(edge_exact.clone())),
        ),
        &cost::StorageCostProfile::default(),
    )
    .unwrap();
    assert!(matches!(
        &edge_subplan.steps()[0].op,
        ExecOp::Access { plan }
            if plan.as_ref() == &ExecAccessPlan::Edge(edge_exact)
    ));
}

#[test]
fn selected_exact_access_rejects_a_payload_for_another_logical_equality() {
    let index = catalog::NodeEqualityIndexMeta::try_new("node_eq:User:status").unwrap();
    let key = catalog::ScopedPropertyKey::try_new("User", "status").unwrap();
    let source = node_access_expr(ir::NodeAccessPlan::EqualityIndex {
        index: index.clone(),
        key: key.clone(),
        value: index_value("active"),
    });
    let mismatched = ExecNodeAccessPlan::exact_equality(index, key, index_value("paused"));

    assert!(matches!(
        ExecutableSubplan::from_selected_executable_alternative(
            &source,
            &exact_access_alternative(
                properties::ElementKind::Node,
                physical::PhysicalAccess::NodeExact(Box::new(mismatched)),
            ),
            &cost::StorageCostProfile::default(),
        ),
        Err(ExecPlanError::UnsupportedSelectedExecutableAlternative { .. })
    ));
}

#[test]
fn selected_executable_alternative_rejects_incompatible_source_contracts() {
    let source = node_access_expr(ir::NodeAccessPlan::AllScan);
    let edge_alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Access {
            element: properties::ElementKind::Edge,
            access: physical::PhysicalAccess::Empty,
        },
        properties::DeliveredProperties {
            element: Some(properties::ElementKind::Edge),
            cardinality: properties::CardinalityBounds::exact(0),
            ..properties::DeliveredProperties::default()
        },
        cost::CostVector::ZERO,
    );

    assert!(matches!(
        ExecutableSubplan::from_selected_executable_alternative(
            &source,
            &edge_alternative,
            &cost::StorageCostProfile::default()
        ),
        Err(ExecPlanError::UnsupportedSelectedExecutableAlternative { .. })
    ));
}

#[test]
fn selected_executable_alternative_family_contract_filters_non_executable_pairs() {
    let physical_pipeline = || {
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one(selected_kv_node_access()),
        ))
    };
    let order_keys = || {
        ir::OrderKeys::from(ir::OrderKey {
            property: name("age"),
            order: Order::Asc,
        })
    };
    let source = logical::LogicalExpr::Pure(logical::PureLogicalOp::Source {
        element: properties::ElementKind::Node,
    });
    let source_kv = physical::PhysicalExpr::Access {
        element: properties::ElementKind::Node,
        access: physical::PhysicalAccess::Kv(KvReadPlan::RangeScan {
            keyspace: ElementKeyspace::NodeProperty,
            start: KvKeyBound::Unbounded,
            end: KvKeyBound::Unbounded,
            limit: None,
        }),
    };
    let source_label = physical::PhysicalExpr::Access {
        element: properties::ElementKind::Node,
        access: physical::PhysicalAccess::LabelScan,
    };
    assert_eq!(
        selected_executable_alternative_family(&source, &source_kv),
        Ok(SelectedExecutableAlternativeFamily::KV_SOURCE)
    );
    assert_eq!(
        selected_executable_alternative_family(&source, &source_label),
        Err(SelectedAlternativeConstructionError::UnsupportedLogicalPhysicalPair)
    );

    let access_path = node_access_expr(ir::NodeAccessPlan::AllScan);
    assert_eq!(
        selected_executable_alternative_family(&access_path, &source_label),
        Ok(SelectedExecutableAlternativeFamily::NODE_ACCESS_PATH)
    );
    let edge_access = edge_access_expr(ir::EdgeAccessPlan::AllScan);
    let edge_physical = physical::PhysicalExpr::Access {
        element: properties::ElementKind::Edge,
        access: physical::PhysicalAccess::LabelScan,
    };
    assert_eq!(
        selected_executable_alternative_family(&edge_access, &edge_physical),
        Ok(SelectedExecutableAlternativeFamily::EDGE_ACCESS_PATH)
    );
    assert_eq!(
        selected_executable_alternative_family(
            &logical::LogicalExpr::Pure(logical::PureLogicalOp::NoOp),
            &physical::PhysicalExpr::NoOp,
        ),
        Ok(SelectedExecutableAlternativeFamily::NO_OP)
    );
    assert_eq!(
        selected_executable_alternative_family(
            &logical::LogicalExpr::VariableSource(logical::VariableSource::new(name("seed"))),
            &physical::PhysicalExpr::Stream(physical::PhysicalStreamOp::Variable),
        ),
        Ok(SelectedExecutableAlternativeFamily::VARIABLE_SOURCE)
    );
    assert_eq!(
        selected_executable_alternative_family(
            &node_access_filter_expr(ir::NodeAccessPlan::AllScan, predicate()),
            &physical_pipeline(),
        ),
        Ok(SelectedExecutableAlternativeFamily::ACCESS_FILTER_PIPELINE)
    );
    assert_eq!(
        selected_executable_alternative_family(
            &node_access_window_expr(
                ir::NodeAccessPlan::AllScan,
                logical::AccessWindowRange::new(1, Some(3)).unwrap(),
            ),
            &physical_pipeline(),
        ),
        Ok(SelectedExecutableAlternativeFamily::ACCESS_WINDOW_PIPELINE)
    );
    assert_eq!(
        selected_executable_alternative_family(
            &node_access_order_expr(ir::NodeAccessPlan::AllScan, order_keys()),
            &physical_pipeline(),
        ),
        Ok(SelectedExecutableAlternativeFamily::ACCESS_ORDER_PIPELINE)
    );
    assert_eq!(
        selected_executable_alternative_family(
            &node_access_distinct_expr(ir::NodeAccessPlan::AllScan),
            &physical_pipeline(),
        ),
        Ok(SelectedExecutableAlternativeFamily::ACCESS_DISTINCT_PIPELINE)
    );
    let access_pipeline = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            node_access_path(ir::NodeAccessPlan::AllScan),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Distinct),
        )
        .unwrap(),
    );
    assert_eq!(
        selected_executable_alternative_family(&access_pipeline, &physical_pipeline()),
        Ok(SelectedExecutableAlternativeFamily::ACCESS_PIPELINE)
    );
    assert_eq!(
        selected_executable_alternative_family(
            &logical::LogicalExpr::Pure(logical::PureLogicalOp::Empty),
            &physical::PhysicalExpr::Empty
        ),
        Err(SelectedAlternativeConstructionError::UnsupportedLogicalPhysicalPair)
    );
}

#[test]
fn selected_variable_source_lowers_to_native_source_inject_step() {
    let source = logical::LogicalExpr::VariableSource(logical::VariableSource::new(name("seed")));
    let alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Stream(physical::PhysicalStreamOp::Variable),
        properties::DeliveredProperties::default(),
        cost::StorageCostProfile::default().source_inject(),
    );

    let subplan = ExecutableSubplan::from_selected_executable_alternative_with_io(
        &source,
        &alternative,
        &cost::StorageCostProfile::default(),
        ir::BatchOutputPlan::Bind(name("out")),
        ExecCondition::Always,
    )
    .unwrap();

    assert_eq!(subplan.steps().len(), 1);
    assert!(matches!(
        &subplan.steps()[0].op,
        ExecOp::Variable {
            op: ExecVariableOp::SourceInject { variable }
        } if variable.as_ref() == "seed"
    ));
    assert!(matches!(
        &subplan.steps()[0].output,
        ir::BatchOutputPlan::Bind(variable) if variable.as_ref() == "out"
    ));
}

#[test]
fn selected_root_pipeline_lowers_variable_source_stream_to_native_dag() {
    let profile = cost::StorageCostProfile::default();
    let ops = ir::AtLeast::<_, 1>::from_one_and_rest(
        logical::StreamPipelineOp::Variable {
            op: logical::PureStreamVariableOp::Select(name("cached")),
        },
        vec![logical::StreamPipelineOp::Distinct],
    );
    let alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one_and_rest(
                physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Variable),
                vec![
                    physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Variable),
                    physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Distinct),
                ],
            ),
        )),
        properties::DeliveredProperties {
            materialization: properties::Materialization::Materialized,
            ..properties::DeliveredProperties::default()
        },
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
                    SelectedRootStreamInput::VariableSource(logical::VariableSource::new(name(
                        "seed",
                    ))),
                    ops,
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
        ExecOp::Variable {
            op: ExecVariableOp::SourceInject { variable }
        } if variable.as_ref() == "seed"
    ));
    assert!(matches!(
        &plan.steps()[1].op,
        ExecOp::Variable {
            op: ExecVariableOp::Stream(ir::StreamVariableOp::Select(variable))
        } if variable.as_ref() == "cached"
    ));
    assert!(matches!(&plan.steps()[2].op, ExecOp::Distinct));
    assert_eq!(plan.steps()[1].dependencies, vec![plan.steps()[0].id]);
    assert_eq!(plan.steps()[2].dependencies, vec![plan.steps()[1].id]);
    assert!(matches!(
        &plan.steps()[2].output,
        ir::BatchOutputPlan::Bind(name) if name.as_ref() == "selected"
    ));
}

#[test]
fn selected_root_payload_barriers_lower_to_native_dag() {
    let profile = cost::StorageCostProfile::default();
    let mutation_alternative = selected_mutation_alternative(&profile);

    let mutation = selected_run_root_plan(
        SelectedExecutableRunRoot::Mutation(Box::new(selected_root_mutation(
            mutation_alternative.clone(),
            SelectedMutationPlan::AddNode {
                input: SelectedMutationInput::Source,
                label: name("User"),
                properties: ir::PropertyAssignments::default(),
            },
        ))),
        ir::BatchOutputPlan::Bind(name("created")),
        &profile,
    );

    assert_eq!(mutation.steps().len(), 1);
    assert_eq!(mutation.steps()[0].schedule, ExecSchedule::Barrier);
    assert!(matches!(
        &mutation.steps()[0].op,
        ExecOp::Mutation {
            plan: ExecMutationPlan::AddNodeSource { label, .. }
        } if label.as_ref() == "User"
    ));
    assert!(matches!(
        &mutation.steps()[0].output,
        ir::BatchOutputPlan::Bind(name) if name.as_ref() == "created"
    ));

    let ddl_plan = ir::IndexDdlPlan::Drop {
        spec: ir::IndexDdlDropSpec::NodeEquality {
            key: crate::catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
            uniqueness: crate::catalog::IndexUniqueness::NonUnique,
        },
    };
    let ddl_alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Barrier,
        properties::DeliveredProperties {
            cardinality: properties::CardinalityBounds::exact(1),
            materialization: properties::Materialization::Materialized,
            effect: properties::EffectKind::Barrier,
            ..properties::DeliveredProperties::default()
        },
        profile.barrier(),
    );

    let ddl = selected_run_root_plan(
        SelectedExecutableRunRoot::IndexDdl(Box::new(selected_root_index_ddl(
            ddl_alternative.clone(),
            ddl_plan.clone(),
        ))),
        ir::BatchOutputPlan::Bind(name("ddl")),
        &profile,
    );

    assert_eq!(ddl.steps().len(), 1);
    assert_eq!(ddl.steps()[0].schedule, ExecSchedule::Barrier);
    assert!(matches!(
        &ddl.steps()[0].op,
        ExecOp::IndexDdl {
            plan: ir::IndexDdlPlan::Drop { .. }
        }
    ));
    assert_eq!(
        ddl.steps()[0].delivered.cardinality,
        properties::CardinalityBounds::exact(1)
    );

    let mutation_source =
        logical::LogicalExpr::RootMutation(logical::RootMutation::new(ir::MutationPlan::AddNode {
            input: ir::MutationInput::Source,
            label: name("User"),
            properties: ir::PropertyAssignments::default(),
        }));
    assert!(matches!(
        ExecutableSubplan::from_selected_executable_alternative_with_io(
            &mutation_source,
            &mutation_alternative,
            &profile,
            ir::BatchOutputPlan::Bind(name("created")),
            ExecCondition::Always,
        ),
        Err(ExecPlanError::UnsupportedSelectedExecutableAlternative { .. })
    ));

    let ddl_source =
        logical::LogicalExpr::RootIndexDdl(logical::RootIndexDdl::new(ddl_plan.clone()));
    assert!(matches!(
        ExecutableSubplan::from_selected_executable_alternative_with_io(
            &ddl_source,
            &ddl_alternative,
            &profile,
            ir::BatchOutputPlan::Bind(name("ddl")),
            ExecCondition::Always,
        ),
        Err(ExecPlanError::UnsupportedSelectedExecutableAlternative { .. })
    ));

    let incompatible = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::NoOp,
        properties::DeliveredProperties::default(),
        cost::CostVector::ZERO,
    );
    assert!(matches!(
        ExecutableSubplan::from_selected_executable_alternative(
            &mutation_source,
            &incompatible,
            &profile
        ),
        Err(ExecPlanError::UnsupportedSelectedExecutableAlternative { .. })
    ));
}

#[test]
fn selected_root_control_flow_direct_alternatives_require_selected_wrappers() {
    let profile = cost::StorageCostProfile::default();
    let pipeline_source = logical::LogicalExpr::RootPipeline(
        logical::RootPipeline::new(
            logical::RootStream::VariableSource(logical::VariableSource::new(name("seed"))),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Distinct),
        )
        .unwrap(),
    );
    let pipeline_alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one_and_rest(
                physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Variable),
                vec![physical::PhysicalPipelineOp::Stream(
                    physical::PhysicalStreamOp::Distinct,
                )],
            ),
        )),
        properties::DeliveredProperties::default(),
        cost::CostVector::ZERO,
    );

    assert!(matches!(
        ExecutableSubplan::from_selected_executable_alternative_with_io(
            &pipeline_source,
            &pipeline_alternative,
            &profile,
            ir::BatchOutputPlan::Bind(name("pipelined")),
            ExecCondition::Always,
        ),
        Err(ExecPlanError::UnsupportedSelectedExecutableAlternative { .. })
    ));

    let terminal_source = logical::LogicalExpr::StreamProject(logical::StreamProject::new(
        logical::RootStream::VariableSource(logical::VariableSource::new(name("seed"))),
        ir::ProjectionPlan::Exists,
    ));
    let terminal_alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one_and_rest(
                physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Variable),
                vec![physical::PhysicalPipelineOp::Stream(
                    physical::PhysicalStreamOp::Project,
                )],
            ),
        )),
        properties::DeliveredProperties::default(),
        cost::CostVector::ZERO,
    );

    assert!(matches!(
        ExecutableSubplan::from_selected_executable_alternative_with_io(
            &terminal_source,
            &terminal_alternative,
            &profile,
            ir::BatchOutputPlan::Bind(name("count")),
            ExecCondition::Always,
        ),
        Err(ExecPlanError::UnsupportedSelectedExecutableAlternative { .. })
    ));

    let branch_source = logical_optional_branch(
        node_access_expr(ir::NodeAccessPlan::PointIds {
            ids: element_ids(vec![1]),
        }),
        edge_access_expr(ir::EdgeAccessPlan::PointIds {
            ids: element_ids(vec![2]),
        }),
    );
    let branch_alternative = selected_branch_alternative(&profile);

    assert!(matches!(
        ExecutableSubplan::from_selected_executable_alternative_with_io(
            &branch_source,
            &branch_alternative,
            &profile,
            ir::BatchOutputPlan::Bind(name("branched")),
            ExecCondition::Always,
        ),
        Err(ExecPlanError::UnsupportedSelectedExecutableAlternative { .. })
    ));

    let repeat_source = logical_repeat(
        node_access_expr(ir::NodeAccessPlan::PointIds {
            ids: element_ids(vec![3]),
        }),
        edge_access_expr(ir::EdgeAccessPlan::PointIds {
            ids: element_ids(vec![4]),
        }),
        2,
    );
    let repeat_alternative = selected_repeat_alternative(&profile);

    assert!(matches!(
        ExecutableSubplan::from_selected_executable_alternative_with_io(
            &repeat_source,
            &repeat_alternative,
            &profile,
            ir::BatchOutputPlan::Bind(name("repeated")),
            ExecCondition::Always,
        ),
        Err(ExecPlanError::UnsupportedSelectedExecutableAlternative { .. })
    ));

    let wrong_control = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Control(physical::PhysicalControlOp::Repeat),
        selected_barrier_delivered_properties(),
        cost::CostVector::ZERO,
    );
    assert!(matches!(
        ExecutableSubplan::from_selected_executable_alternative(
            &branch_source,
            &wrong_control,
            &profile
        ),
        Err(ExecPlanError::UnsupportedSelectedExecutableAlternative { .. })
    ));
}

#[test]
fn selected_stream_project_lowers_variable_source_terminal_to_native_dag() {
    let profile = cost::StorageCostProfile::default();
    let alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one_and_rest(
                physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Variable),
                vec![physical::PhysicalPipelineOp::Stream(
                    physical::PhysicalStreamOp::Project,
                )],
            ),
        )),
        properties::DeliveredProperties {
            cardinality: properties::CardinalityBounds::exact(1),
            materialization: properties::Materialization::Materialized,
            ..properties::DeliveredProperties::default()
        },
        profile
            .source_inject()
            .serial(profile.stream_operator(profile.default_unknown_scan_rows)),
    );

    let plan = selected_terminal_plan(
        alternative,
        SelectedRootTerminal::Project {
            input: SelectedRootStreamInput::VariableSource(logical::VariableSource::new(name(
                "seed",
            ))),
            projection: ir::ProjectionPlan::Exists,
        },
        ir::BatchOutputPlan::Bind(name("count")),
        &profile,
    );

    assert_eq!(plan.steps().len(), 2);
    assert!(matches!(
        &plan.steps()[0].op,
        ExecOp::Variable {
            op: ExecVariableOp::SourceInject { variable }
        } if variable.as_ref() == "seed"
    ));
    assert!(matches!(
        &plan.steps()[1].op,
        ExecOp::Project {
            projection: ir::ProjectionPlan::Exists,
        }
    ));
    assert_eq!(plan.steps()[1].dependencies, vec![plan.steps()[0].id]);
    assert!(matches!(
        &plan.steps()[1].output,
        ir::BatchOutputPlan::Bind(name) if name.as_ref() == "count"
    ));
}
