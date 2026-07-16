use super::super::*;

#[test]
fn selected_access_filter_pipeline_lowers_to_native_access_then_filter() {
    let profile = cost::StorageCostProfile::default();
    let predicate = predicate();
    let source = node_access_filter_expr(ir::NodeAccessPlan::AllScan, predicate.clone());
    let access = physical::PhysicalAccess::Kv(KvReadPlan::RangeScan {
        keyspace: ElementKeyspace::NodeProperty,
        start: KvKeyBound::Unbounded,
        end: KvKeyBound::Unbounded,
        limit: None,
    });
    let selected_cost = profile
        .range_scan(profile.default_unknown_scan_rows)
        .serial(profile.predicate_eval(profile.default_unknown_scan_rows));
    let selected_delivered = properties::DeliveredProperties {
        element: Some(properties::ElementKind::Node),
        ..properties::DeliveredProperties::default()
    };
    let alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one_and_rest(
                physical::PhysicalPipelineOp::Access {
                    element: properties::ElementKind::Node,
                    access,
                },
                vec![physical::PhysicalPipelineOp::ResidualFilter],
            ),
        )),
        selected_delivered.clone(),
        selected_cost,
    );

    let subplan = ExecutableSubplan::from_selected_executable_alternative_with_io(
        &source,
        &alternative,
        &profile,
        ir::BatchOutputPlan::Bind(name("users")),
        ExecCondition::Always,
    )
    .unwrap();

    assert_eq!(subplan.steps().len(), 2);
    assert!(matches!(
        &subplan.steps()[0].op,
        ExecOp::KvRead(KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == ElementKeyspace::NodeProperty
    ));
    assert!(matches!(
        &subplan.steps()[1].op,
        ExecOp::Filter { predicate: lowered } if lowered == &predicate
    ));
    assert_eq!(subplan.steps()[1].dependencies, vec![subplan.steps()[0].id]);
    assert_eq!(subplan.steps()[1].cost, selected_cost);
    assert_eq!(subplan.steps()[1].delivered, selected_delivered);
}

#[test]
fn selected_access_window_order_and_distinct_pipelines_lower_to_native_dag() {
    let profile = cost::StorageCostProfile::default();
    let order_keys = ir::OrderKeys::from(ir::OrderKey {
        property: name("age"),
        order: Order::Asc,
    });
    let cases = [
        (
            node_access_window_expr(
                ir::NodeAccessPlan::AllScan,
                logical::AccessWindowRange::new(2, Some(5)).unwrap(),
            ),
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Range),
            "range",
        ),
        (
            node_access_order_expr(ir::NodeAccessPlan::AllScan, order_keys.clone()),
            physical::PhysicalPipelineOp::Sort,
            "order",
        ),
        (
            node_access_distinct_expr(ir::NodeAccessPlan::AllScan),
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Distinct),
            "distinct",
        ),
    ];

    for (source, suffix, expected) in cases {
        let alternative = physical::PhysicalAlternative::new(
            physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
                ir::AtLeast::<_, 1>::from_one_and_rest(selected_kv_node_access(), vec![suffix]),
            )),
            properties::DeliveredProperties {
                element: Some(properties::ElementKind::Node),
                ..properties::DeliveredProperties::default()
            },
            profile.range_scan(profile.default_unknown_scan_rows),
        );

        let subplan = ExecutableSubplan::from_selected_executable_alternative_with_io(
            &source,
            &alternative,
            &profile,
            ir::BatchOutputPlan::Discard,
            ExecCondition::Always,
        )
        .unwrap();

        assert_eq!(subplan.steps().len(), 2);
        assert!(matches!(
            &subplan.steps()[0].op,
            ExecOp::KvRead(KvReadPlan::RangeScan { keyspace, .. })
                if *keyspace == ElementKeyspace::NodeProperty
        ));
        assert_eq!(subplan.steps()[1].dependencies, vec![subplan.steps()[0].id]);
        match expected {
            "range" => assert!(matches!(
                &subplan.steps()[1].op,
                ExecOp::Range {
                    range: ir::StreamRangePlan::Literal(range),
                } if range.start() == 2 && range.end() == 5
            )),
            "order" => assert!(matches!(
                &subplan.steps()[1].op,
                ExecOp::Order {
                    plan: ir::OrderPlan::ExplicitSort(keys),
                } if keys == &order_keys
            )),
            "distinct" => assert!(matches!(&subplan.steps()[1].op, ExecOp::Distinct)),
            _ => unreachable!("test cases use known expected operators"),
        }
    }
}

#[test]
fn selected_access_window_pushes_static_end_into_kv_scan_limit() {
    let profile = cost::StorageCostProfile::default();
    let source = node_access_window_expr(
        ir::NodeAccessPlan::AllScan,
        logical::AccessWindowRange::new(2, Some(5)).unwrap(),
    );
    let alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one_and_rest(
                selected_kv_node_access(),
                vec![physical::PhysicalPipelineOp::Stream(
                    physical::PhysicalStreamOp::Range,
                )],
            ),
        )),
        properties::DeliveredProperties {
            element: Some(properties::ElementKind::Node),
            ..properties::DeliveredProperties::default()
        },
        profile.range_scan(profile.default_unknown_scan_rows),
    );

    let subplan = ExecutableSubplan::from_selected_executable_alternative_with_io(
        &source,
        &alternative,
        &profile,
        ir::BatchOutputPlan::Discard,
        ExecCondition::Always,
    )
    .unwrap();

    assert_eq!(subplan.steps().len(), 2);
    assert!(matches!(
        &subplan.steps()[0].op,
        ExecOp::KvRead(KvReadPlan::RangeScan {
            limit: Some(limit),
            ..
        }) if limit.get() == 5
    ));
    assert!(matches!(
        &subplan.steps()[1].op,
        ExecOp::Range {
            range: ir::StreamRangePlan::Literal(range),
        } if range.start() == 2 && range.end() == 5
    ));
}

#[test]
fn selected_access_window_elides_prefix_limit_after_pushing_read_cap() {
    let profile = cost::StorageCostProfile::default();
    let source = node_access_window_expr(
        ir::NodeAccessPlan::AllScan,
        logical::AccessWindowRange::new(0, Some(5)).unwrap(),
    );
    let alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one_and_rest(
                selected_kv_node_access(),
                vec![physical::PhysicalPipelineOp::Stream(
                    physical::PhysicalStreamOp::Range,
                )],
            ),
        )),
        properties::DeliveredProperties {
            element: Some(properties::ElementKind::Node),
            ..properties::DeliveredProperties::default()
        },
        profile.range_scan(profile.default_unknown_scan_rows),
    );

    let subplan = ExecutableSubplan::from_selected_executable_alternative_with_io(
        &source,
        &alternative,
        &profile,
        ir::BatchOutputPlan::Bind(name("users")),
        ExecCondition::Always,
    )
    .unwrap();

    assert_eq!(subplan.steps().len(), 1);
    assert!(matches!(
        &subplan.steps()[0].op,
        ExecOp::KvRead(KvReadPlan::RangeScan {
            limit: Some(limit),
            ..
        }) if limit.get() == 5
    ));
    assert!(matches!(
        &subplan.steps()[0].output,
        ir::BatchOutputPlan::Bind(name) if name.as_ref() == "users"
    ));
}

#[test]
fn selected_access_window_pushes_static_end_into_native_access_limit() {
    let profile = cost::StorageCostProfile::default();
    let source = node_access_window_expr(
        ir::NodeAccessPlan::FromParam { param: name("ids") },
        logical::AccessWindowRange::new(1, Some(3)).unwrap(),
    );
    let alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one_and_rest(
                physical::PhysicalPipelineOp::Access {
                    element: properties::ElementKind::Node,
                    access: physical::PhysicalAccess::RuntimeInput,
                },
                vec![physical::PhysicalPipelineOp::Stream(
                    physical::PhysicalStreamOp::Range,
                )],
            ),
        )),
        properties::DeliveredProperties {
            element: Some(properties::ElementKind::Node),
            ..properties::DeliveredProperties::default()
        },
        profile.stream_operator(profile.default_unknown_scan_rows),
    );

    let subplan = ExecutableSubplan::from_selected_executable_alternative_with_io(
        &source,
        &alternative,
        &profile,
        ir::BatchOutputPlan::Discard,
        ExecCondition::Always,
    )
    .unwrap();

    assert_eq!(subplan.steps().len(), 2);
    assert!(matches!(
        &subplan.steps()[0].op,
        ExecOp::Access { plan }
            if matches!(
                plan.as_ref(),
                ExecAccessPlan::Limited(limited)
                    if limited.limit().get() == 3
                        && matches!(
                            limited.source(),
                            ExecAccessPlan::Node(ExecNodeAccessPlan::FromParam { param })
                                if param.as_ref() == "ids"
                        )
            )
    ));
    assert!(matches!(
        &subplan.steps()[1].op,
        ExecOp::Range {
            range: ir::StreamRangePlan::Literal(range),
        } if range.start() == 1 && range.end() == 3
    ));
}

#[test]
fn selected_access_pipeline_elides_leading_prefix_limit_and_keeps_later_ops() {
    let profile = cost::StorageCostProfile::default();
    let predicate = predicate();
    let source = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            node_access_path(ir::NodeAccessPlan::AllScan),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOp::Window {
                    window: logical::AccessWindowRange::new(0, Some(4)).unwrap(),
                },
                vec![logical::StreamPipelineOp::Filter {
                    predicate: predicate.clone(),
                }],
            ),
        )
        .unwrap(),
    );
    let alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one_and_rest(
                selected_kv_node_access(),
                vec![
                    physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Range),
                    physical::PhysicalPipelineOp::ResidualFilter,
                ],
            ),
        )),
        properties::DeliveredProperties::default(),
        cost::CostVector::ZERO,
    );

    let subplan = ExecutableSubplan::from_selected_executable_alternative_with_io(
        &source,
        &alternative,
        &profile,
        ir::BatchOutputPlan::Bind(name("active_users")),
        ExecCondition::Always,
    )
    .unwrap();

    assert_eq!(subplan.steps().len(), 2);
    assert!(matches!(
        &subplan.steps()[0].op,
        ExecOp::KvRead(KvReadPlan::RangeScan {
            limit: Some(limit),
            ..
        }) if limit.get() == 4
    ));
    assert!(matches!(
        &subplan.steps()[1].op,
        ExecOp::Filter { predicate: lowered } if lowered == &predicate
    ));
    assert_eq!(subplan.steps()[1].dependencies, vec![subplan.steps()[0].id]);
    assert!(matches!(
        &subplan.steps()[1].output,
        ir::BatchOutputPlan::Bind(name) if name.as_ref() == "active_users"
    ));
}

#[test]
fn selected_access_pipeline_pushes_only_leading_window_into_kv_scan_limit() {
    let profile = cost::StorageCostProfile::default();
    let predicate = predicate();
    let leading_native_window = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            node_access_path(ir::NodeAccessPlan::FromParam { param: name("ids") }),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOp::Window {
                    window: logical::AccessWindowRange::new(1, Some(4)).unwrap(),
                },
                vec![logical::StreamPipelineOp::Filter {
                    predicate: predicate.clone(),
                }],
            ),
        )
        .unwrap(),
    );
    let leading_window = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            node_access_path(ir::NodeAccessPlan::AllScan),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOp::Window {
                    window: logical::AccessWindowRange::new(1, Some(4)).unwrap(),
                },
                vec![logical::StreamPipelineOp::Filter {
                    predicate: predicate.clone(),
                }],
            ),
        )
        .unwrap(),
    );
    let after_filter = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            node_access_path(ir::NodeAccessPlan::AllScan),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOp::Filter {
                    predicate: predicate.clone(),
                },
                vec![logical::StreamPipelineOp::Window {
                    window: logical::AccessWindowRange::new(1, Some(4)).unwrap(),
                }],
            ),
        )
        .unwrap(),
    );
    let leading_alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one_and_rest(
                selected_kv_node_access(),
                vec![
                    physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Range),
                    physical::PhysicalPipelineOp::ResidualFilter,
                ],
            ),
        )),
        properties::DeliveredProperties::default(),
        cost::CostVector::ZERO,
    );
    let leading_native_alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one_and_rest(
                physical::PhysicalPipelineOp::Access {
                    element: properties::ElementKind::Node,
                    access: physical::PhysicalAccess::RuntimeInput,
                },
                vec![
                    physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Range),
                    physical::PhysicalPipelineOp::ResidualFilter,
                ],
            ),
        )),
        properties::DeliveredProperties::default(),
        cost::CostVector::ZERO,
    );
    let after_filter_alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one_and_rest(
                selected_kv_node_access(),
                vec![
                    physical::PhysicalPipelineOp::ResidualFilter,
                    physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Range),
                ],
            ),
        )),
        properties::DeliveredProperties::default(),
        cost::CostVector::ZERO,
    );

    let leading_subplan = ExecutableSubplan::from_selected_executable_alternative_with_io(
        &leading_window,
        &leading_alternative,
        &profile,
        ir::BatchOutputPlan::Discard,
        ExecCondition::Always,
    )
    .unwrap();
    let leading_native_subplan = ExecutableSubplan::from_selected_executable_alternative_with_io(
        &leading_native_window,
        &leading_native_alternative,
        &profile,
        ir::BatchOutputPlan::Discard,
        ExecCondition::Always,
    )
    .unwrap();
    let after_filter_subplan = ExecutableSubplan::from_selected_executable_alternative_with_io(
        &after_filter,
        &after_filter_alternative,
        &profile,
        ir::BatchOutputPlan::Discard,
        ExecCondition::Always,
    )
    .unwrap();

    assert!(matches!(
        &leading_subplan.steps()[0].op,
        ExecOp::KvRead(KvReadPlan::RangeScan {
            limit: Some(limit),
            ..
        }) if limit.get() == 4
    ));
    assert!(matches!(
        &leading_native_subplan.steps()[0].op,
        ExecOp::Access { plan }
            if matches!(
                plan.as_ref(),
                ExecAccessPlan::Limited(limited)
                    if limited.limit().get() == 4
                        && matches!(
                            limited.source(),
                            ExecAccessPlan::Node(ExecNodeAccessPlan::FromParam { param })
                                if param.as_ref() == "ids"
                        )
            )
    ));
    assert!(matches!(
        &after_filter_subplan.steps()[0].op,
        ExecOp::KvRead(KvReadPlan::RangeScan { limit: None, .. })
    ));
}

#[test]
fn selected_access_pipeline_lowers_composed_stream_to_native_dag() {
    let profile = cost::StorageCostProfile::default();
    let predicate = predicate();
    let source = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            node_access_path(ir::NodeAccessPlan::AllScan),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOp::Filter {
                    predicate: predicate.clone(),
                },
                vec![logical::StreamPipelineOp::Window {
                    window: logical::AccessWindowRange::new(1, Some(3)).unwrap(),
                }],
            ),
        )
        .unwrap(),
    );
    let alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one_and_rest(
                selected_kv_node_access(),
                vec![
                    physical::PhysicalPipelineOp::ResidualFilter,
                    physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Range),
                ],
            ),
        )),
        properties::DeliveredProperties::default(),
        cost::CostVector::ZERO,
    );

    let subplan = ExecutableSubplan::from_selected_executable_alternative_with_io(
        &source,
        &alternative,
        &profile,
        ir::BatchOutputPlan::Bind(name("users")),
        ExecCondition::Always,
    )
    .unwrap();

    assert_eq!(subplan.steps().len(), 3);
    assert!(matches!(
        &subplan.steps()[0].op,
        ExecOp::KvRead(KvReadPlan::RangeScan {
            keyspace,
            limit: None,
            ..
        })
            if *keyspace == ElementKeyspace::NodeProperty
    ));
    assert!(matches!(
        &subplan.steps()[1].op,
        ExecOp::Filter { predicate: lowered } if lowered == &predicate
    ));
    assert!(matches!(
        &subplan.steps()[2].op,
        ExecOp::Range {
            range: ir::StreamRangePlan::Literal(range),
        } if range.start() == 1 && range.end() == 3
    ));
    assert_eq!(subplan.steps()[1].dependencies, vec![subplan.steps()[0].id]);
    assert_eq!(subplan.steps()[2].dependencies, vec![subplan.steps()[1].id]);
    assert!(matches!(
        &subplan.steps()[2].output,
        ir::BatchOutputPlan::Bind(name) if name.as_ref() == "users"
    ));
}

#[test]
fn selected_access_pipeline_lowers_expansion_to_native_dag() {
    let profile = cost::StorageCostProfile::default();
    let expand = ir::ExpandPlan {
        direction: ir::ExpandDirection::Out,
        output: ir::ExpandOutput::Edges,
        label: ir::ExpandLabelPlan::Label(name("LIKES")),
    };
    let source = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            node_access_path(ir::NodeAccessPlan::AllScan),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Expand {
                plan: expand.clone(),
            }),
        )
        .unwrap(),
    );
    let alternative = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one_and_rest(
                selected_kv_node_access(),
                vec![physical::PhysicalPipelineOp::Stream(
                    physical::PhysicalStreamOp::Expand,
                )],
            ),
        )),
        properties::DeliveredProperties {
            effect: properties::EffectKind::Barrier,
            ..properties::DeliveredProperties::default()
        },
        cost::CostVector::ZERO,
    );

    let subplan = ExecutableSubplan::from_selected_executable_alternative_with_io(
        &source,
        &alternative,
        &profile,
        ir::BatchOutputPlan::Bind(name("likes")),
        ExecCondition::Always,
    )
    .unwrap();

    assert_eq!(subplan.steps().len(), 2);
    assert!(matches!(
        &subplan.steps()[0].op,
        ExecOp::KvRead(KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == ElementKeyspace::NodeProperty
    ));
    assert!(matches!(
        &subplan.steps()[1].op,
        ExecOp::Expand { plan } if plan == &expand
    ));
    assert_eq!(subplan.steps()[1].dependencies, vec![subplan.steps()[0].id]);
    assert!(matches!(
        &subplan.steps()[1].output,
        ir::BatchOutputPlan::Bind(name) if name.as_ref() == "likes"
    ));
}

#[test]
fn selected_access_pipeline_lowers_variable_to_native_dag() {
    let profile = cost::StorageCostProfile::default();
    let source = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            node_access_path(ir::NodeAccessPlan::AllScan),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Variable {
                op: logical::PureStreamVariableOp::Select(name("cached")),
            }),
        )
        .unwrap(),
    );
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

    let subplan = ExecutableSubplan::from_selected_executable_alternative_with_io(
        &source,
        &alternative,
        &profile,
        ir::BatchOutputPlan::Bind(name("users")),
        ExecCondition::Always,
    )
    .unwrap();

    assert_eq!(subplan.steps().len(), 2);
    assert!(matches!(
        &subplan.steps()[0].op,
        ExecOp::KvRead(KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == ElementKeyspace::NodeProperty
    ));
    assert!(matches!(
        &subplan.steps()[1].op,
        ExecOp::Variable {
            op: ExecVariableOp::Stream(ir::StreamVariableOp::Select(variable))
        } if variable.as_ref() == "cached"
    ));
    assert_eq!(subplan.steps()[1].dependencies, vec![subplan.steps()[0].id]);
    assert!(matches!(
        &subplan.steps()[1].output,
        ir::BatchOutputPlan::Bind(name) if name.as_ref() == "users"
    ));
}
