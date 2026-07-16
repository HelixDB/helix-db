use super::*;

#[test]
fn root_pipeline_implementation_rule_keeps_variable_source_streams_in_cascades() {
    let rule = RootPipelineImplementationRule::default();
    let storage = cost::StorageCostProfile {
        source_inject_overhead: cost::LatencyEstimate::micros(13),
        stream_operator_eval: cost::LatencyEstimate::micros(3),
        sort_setup: cost::LatencyEstimate::micros(17),
        sort_per_row: cost::LatencyEstimate::micros(5),
        default_unknown_scan_rows: cost::EstimatedRows::rows(20),
        ..cost::StorageCostProfile::default()
    };
    let expr = logical::LogicalExpr::RootPipeline(
        logical::RootPipeline::new(
            logical::RootStream::VariableSource(logical::VariableSource::new(name("users"))),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOp::Variable {
                    op: logical::PureStreamVariableOp::Select(name("cached")),
                },
                vec![logical::StreamPipelineOp::Distinct],
            ),
        )
        .unwrap(),
    );

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "seed_root_pipeline");
    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical root pipeline");
    };
    assert_eq!(
        pipeline.ops(),
        &[
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Variable),
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Variable),
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Distinct),
        ]
    );
    assert_eq!(
        alternative.delivered.materialization,
        properties::Materialization::Materialized
    );
    let rows = storage.default_unknown_scan_rows;
    assert_eq!(
        alternative.cost,
        storage
            .source_inject()
            .serial(storage.stream_operator(rows))
            .serial(storage.explicit_sort(rows))
    );
    assert_eq!(
        rule.apply(optimizer::RuleInput {
            expr: &source(properties::ElementKind::Node),
            storage: &storage,
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        }),
        optimizer::RuleResult::NotApplicable
    );
}

#[test]
fn root_pipeline_implementation_rule_lowers_dynamic_stream_bounds() {
    let rule = RootPipelineImplementationRule::default();
    let storage = cost::StorageCostProfile {
        source_inject_overhead: cost::LatencyEstimate::micros(13),
        stream_operator_eval: cost::LatencyEstimate::micros(3),
        default_unknown_scan_rows: cost::EstimatedRows::rows(20),
        ..cost::StorageCostProfile::default()
    };
    let expr = logical::LogicalExpr::RootPipeline(
        logical::RootPipeline::new(
            logical::RootStream::VariableSource(logical::VariableSource::new(name("users"))),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOp::Limit {
                    count: ir::StreamBoundPlan::Expr(
                        ir::StreamBoundExprPlan::new(helix_ast::expr::Expr::param("limit"))
                            .unwrap(),
                    ),
                },
                vec![logical::StreamPipelineOp::Range {
                    range: ir::StreamRangePlan::Dynamic(
                        ir::StreamDynamicRange::new(
                            ir::StreamBoundPlan::Expr(
                                ir::StreamBoundExprPlan::new(helix_ast::expr::Expr::param("start"))
                                    .unwrap(),
                            ),
                            ir::StreamBoundPlan::Expr(
                                ir::StreamBoundExprPlan::new(helix_ast::expr::Expr::param("end"))
                                    .unwrap(),
                            ),
                        )
                        .unwrap(),
                    ),
                }],
            ),
        )
        .unwrap(),
    );

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical root pipeline");
    };
    assert_eq!(
        pipeline.ops(),
        &[
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Variable),
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Limit),
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Range),
        ]
    );
    let rows = storage.default_unknown_scan_rows;
    assert_eq!(
        alternative.cost,
        storage
            .source_inject()
            .serial(storage.stream_operator(rows))
            .serial(storage.stream_operator(rows))
    );
}

#[test]
fn root_pipeline_implementation_rule_supports_control_flow_inputs() {
    let rule = RootPipelineImplementationRule::default();
    let storage = cost::StorageCostProfile::default();
    let expand = ir::ExpandPlan {
        direction: ir::ExpandDirection::Out,
        output: ir::ExpandOutput::Edges,
        label: ir::ExpandLabelPlan::Label(name("LIKES")),
    };
    let expr = logical::LogicalExpr::RootPipeline(
        logical::RootPipeline::new(
            logical::RootStream::Branch(Box::new(optional_branch(
                node_all_expr(),
                edge_all_expr(),
            ))),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Expand {
                plan: expand.clone(),
            }),
        )
        .unwrap(),
    );

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical root pipeline");
    };
    assert_eq!(
        pipeline.ops(),
        &[physical::PhysicalPipelineOp::Stream(
            physical::PhysicalStreamOp::Expand
        )]
    );
    assert_eq!(
        alternative.delivered.element,
        Some(properties::ElementKind::Edge)
    );
    assert_eq!(
        alternative.cost,
        storage.stream_operator(storage.default_unknown_scan_rows)
    );
}

#[test]
fn root_pipeline_implementation_rule_supports_control_flow_reserved_inputs() {
    let rule = RootPipelineImplementationRule::default();
    let storage = cost::StorageCostProfile {
        stream_operator_eval: cost::LatencyEstimate::micros(3),
        sort_setup: cost::LatencyEstimate::micros(17),
        sort_per_row: cost::LatencyEstimate::micros(5),
        default_unknown_scan_rows: cost::EstimatedRows::rows(20),
        ..cost::StorageCostProfile::default()
    };
    let expr = logical::LogicalExpr::RootPipeline(
        logical::RootPipeline::new(
            logical::RootStream::Reserved(Box::new(logical::StreamReserved::new(
                logical::RootStream::Branch(Box::new(optional_branch(
                    node_all_expr(),
                    edge_all_expr(),
                ))),
                ir::ReservedOp::Fold,
            ))),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Distinct),
        )
        .unwrap(),
    );

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical root pipeline");
    };
    assert_eq!(
        pipeline.ops(),
        &[physical::PhysicalPipelineOp::Stream(
            physical::PhysicalStreamOp::Distinct
        )]
    );
    assert_eq!(alternative.delivered.cardinality.upper(), Some(1));
    assert_eq!(
        alternative.delivered.materialization,
        properties::Materialization::Materialized
    );
    assert_eq!(
        alternative.cost,
        storage.explicit_sort(cost::EstimatedRows::rows(1))
    );
}

#[test]
fn root_pipeline_implementation_rule_supports_control_flow_reserved_variable_inputs() {
    let rule = RootPipelineImplementationRule::default();
    let storage = cost::StorageCostProfile {
        stream_operator_eval: cost::LatencyEstimate::micros(3),
        default_unknown_scan_rows: cost::EstimatedRows::rows(20),
        ..cost::StorageCostProfile::default()
    };
    let expr = logical::LogicalExpr::RootPipeline(
        logical::RootPipeline::new(
            logical::RootStream::Reserved(Box::new(logical::StreamReserved::new(
                logical::RootStream::Branch(Box::new(optional_branch(
                    node_all_expr(),
                    edge_all_expr(),
                ))),
                ir::ReservedOp::Fold,
            ))),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Variable {
                op: logical::PureStreamVariableOp::Select(name("cached")),
            }),
        )
        .unwrap(),
    );

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical root pipeline");
    };
    assert_eq!(
        pipeline.ops(),
        &[physical::PhysicalPipelineOp::Stream(
            physical::PhysicalStreamOp::Variable
        )]
    );
    assert_eq!(alternative.delivered.cardinality.upper(), None);
    assert_eq!(
        alternative.cost,
        storage.stream_operator(cost::EstimatedRows::rows(1))
    );
}
