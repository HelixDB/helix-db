use super::*;

#[test]
fn access_pipeline_implementation_rule_lowers_expansion_payloads() {
    let rule = AccessPipelineImplementationRule::default();
    let storage = cost::StorageCostProfile {
        range_seek: cost::LatencyEstimate::micros(7),
        range_next: cost::LatencyEstimate::micros(11),
        stream_operator_eval: cost::LatencyEstimate::micros(3),
        default_unknown_scan_rows: cost::EstimatedRows::rows(20),
        ..cost::StorageCostProfile::default()
    };
    let plan = ir::ExpandPlan {
        direction: ir::ExpandDirection::Out,
        output: ir::ExpandOutput::Edges,
        label: ir::ExpandLabelPlan::Label(name("LIKES")),
    };
    let expr = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            node_access_path(ir::NodeAccessPlan::AllScan),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Expand { plan: plan.clone() }),
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
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Expand),
        ]
    ));
    assert_eq!(
        alternative.delivered.element,
        Some(properties::ElementKind::Edge)
    );
    assert_eq!(
        alternative.delivered.key_locality,
        properties::KeyLocality::Close
    );
    let rows = storage.default_unknown_scan_rows;
    assert_eq!(
        alternative.cost,
        storage
            .range_scan(rows)
            .serial(storage.stream_operator(rows))
    );
}
