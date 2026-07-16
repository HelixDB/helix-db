use super::*;

#[test]
fn access_pipeline_implementation_rule_keeps_composed_streams_in_cascades() {
    let rule = AccessPipelineImplementationRule::default();
    let storage = cost::StorageCostProfile {
        range_seek: cost::LatencyEstimate::micros(7),
        range_next: cost::LatencyEstimate::micros(11),
        cpu_predicate_eval: cost::LatencyEstimate::micros(5),
        stream_operator_eval: cost::LatencyEstimate::micros(3),
        default_unknown_scan_rows: cost::EstimatedRows::rows(20),
        ..cost::StorageCostProfile::default()
    };
    let predicate = ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap();
    let expr = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            node_access_path(ir::NodeAccessPlan::AllScan),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOp::Filter { predicate },
                vec![logical::StreamPipelineOp::Window {
                    window: logical::AccessWindowRange::new(1, Some(3)).unwrap(),
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

    assert_eq!(rule.metadata().id.as_ref(), "seed_access_pipeline");
    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical access pipeline");
    };
    assert!(matches!(
        pipeline.ops(),
        [
            physical::PhysicalPipelineOp::Access {
                element: properties::ElementKind::Node,
                access: physical::PhysicalAccess::Kv(exec::KvReadPlan::RangeScan { .. }),
            },
            physical::PhysicalPipelineOp::ResidualFilter,
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Range),
        ]
    ));
    assert_eq!(alternative.delivered.cardinality.upper(), Some(2));
    let rows = storage.default_unknown_scan_rows;
    assert_eq!(
        alternative.cost,
        storage
            .range_scan(rows)
            .serial(storage.predicate_eval(rows))
            .serial(storage.stream_operator(cost::EstimatedRows::rows(2)))
    );
}

#[test]
fn access_pipeline_implementation_rule_lowers_dynamic_stream_bounds() {
    let rule = AccessPipelineImplementationRule::default();
    let storage = cost::StorageCostProfile {
        range_seek: cost::LatencyEstimate::micros(7),
        range_next: cost::LatencyEstimate::micros(11),
        stream_operator_eval: cost::LatencyEstimate::micros(3),
        default_unknown_scan_rows: cost::EstimatedRows::rows(20),
        ..cost::StorageCostProfile::default()
    };
    let expr = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            node_access_path(ir::NodeAccessPlan::AllScan),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOp::Limit {
                    count: ir::StreamBoundPlan::Expr(
                        ir::StreamBoundExprPlan::new(helix_ast::expr::Expr::param("limit"))
                            .unwrap(),
                    ),
                },
                vec![
                    logical::StreamPipelineOp::Skip {
                        count: ir::StreamBoundPlan::Expr(
                            ir::StreamBoundExprPlan::new(helix_ast::expr::Expr::param("offset"))
                                .unwrap(),
                        ),
                    },
                    logical::StreamPipelineOp::Range {
                        range: ir::StreamRangePlan::Dynamic(
                            ir::StreamDynamicRange::new(
                                ir::StreamBoundPlan::Expr(
                                    ir::StreamBoundExprPlan::new(helix_ast::expr::Expr::param(
                                        "start",
                                    ))
                                    .unwrap(),
                                ),
                                ir::StreamBoundPlan::Expr(
                                    ir::StreamBoundExprPlan::new(helix_ast::expr::Expr::param(
                                        "end",
                                    ))
                                    .unwrap(),
                                ),
                            )
                            .unwrap(),
                        ),
                    },
                ],
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
        panic!("expected physical access pipeline");
    };
    assert!(matches!(
        pipeline.ops(),
        [
            physical::PhysicalPipelineOp::Access {
                element: properties::ElementKind::Node,
                access: physical::PhysicalAccess::Kv(exec::KvReadPlan::RangeScan { .. }),
            },
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Limit),
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Skip),
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Range),
        ]
    ));
    assert_eq!(alternative.delivered.cardinality.upper(), None);
    let rows = storage.default_unknown_scan_rows;
    assert_eq!(
        alternative.cost,
        storage
            .range_scan(rows)
            .serial(storage.stream_operator(rows))
            .serial(storage.stream_operator(rows))
            .serial(storage.stream_operator(rows))
    );
}
