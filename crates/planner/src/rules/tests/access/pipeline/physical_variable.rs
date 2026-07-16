use super::*;

#[test]
fn access_pipeline_implementation_rule_lowers_stateful_variable_writes() {
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
                logical::StreamPipelineOp::VariableWrite {
                    op: logical::StreamVariableWriteOp::Store(name("cached")),
                },
                vec![logical::StreamPipelineOp::Expand {
                    plan: ir::ExpandPlan {
                        direction: ir::ExpandDirection::Out,
                        output: ir::ExpandOutput::Edges,
                        label: ir::ExpandLabelPlan::Label(name("LIKES")),
                    },
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
        panic!("expected physical access pipeline");
    };
    assert!(matches!(
        pipeline.ops(),
        [
            physical::PhysicalPipelineOp::Access {
                element: properties::ElementKind::Node,
                access: physical::PhysicalAccess::Kv(exec::KvReadPlan::RangeScan { .. }),
            },
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Variable),
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Expand),
        ]
    ));
    assert_eq!(
        alternative.delivered.effect,
        properties::EffectKind::Barrier
    );
    assert_eq!(
        alternative.delivered.element,
        Some(properties::ElementKind::Edge)
    );
    let rows = storage.default_unknown_scan_rows;
    assert_eq!(
        alternative.cost,
        storage
            .range_scan(rows)
            .serial(storage.stream_operator(rows))
            .serial(storage.stream_operator(rows))
    );
}

#[test]
fn access_pipeline_implementation_rule_lowers_variable_payloads() {
    let rule = AccessPipelineImplementationRule::default();
    let storage = cost::StorageCostProfile {
        stream_operator_eval: cost::LatencyEstimate::micros(3),
        default_unknown_scan_rows: cost::EstimatedRows::rows(20),
        ..cost::StorageCostProfile::default()
    };
    let expr = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            node_access_path(ir::NodeAccessPlan::PointIds {
                ids: element_ids(vec![1]),
            }),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Variable {
                op: logical::PureStreamVariableOp::Within(name("allowed")),
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
        panic!("expected physical access pipeline");
    };
    assert!(matches!(
        pipeline.ops(),
        [
            physical::PhysicalPipelineOp::Access {
                element: properties::ElementKind::Node,
                ..
            },
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Variable),
        ]
    ));
    assert_eq!(alternative.delivered.cardinality.lower(), 0);
    assert_eq!(alternative.delivered.cardinality.upper(), Some(1));
    assert_eq!(
        alternative.cost,
        storage
            .point_gets(properties::PositiveUsize::new(1).unwrap())
            .serial(storage.stream_operator(cost::EstimatedRows::rows(1)))
    );
}
