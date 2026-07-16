use super::*;

#[test]
fn access_window_implementation_rule_keeps_unfoldable_window_in_cascades() {
    let rule = AccessWindowImplementationRule::default();
    let storage = cost::StorageCostProfile {
        range_seek: cost::LatencyEstimate::micros(7),
        range_next: cost::LatencyEstimate::micros(11),
        stream_operator_eval: cost::LatencyEstimate::micros(3),
        default_unknown_scan_rows: cost::EstimatedRows::rows(20),
        ..cost::StorageCostProfile::default()
    };
    let window = logical::AccessWindowRange::new(3, Some(8)).unwrap();
    let expr = node_access_window_expr(ir::NodeAccessPlan::AllScan, window);

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "seed_access_window");
    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical access-window pipeline");
    };
    assert!(matches!(
        pipeline.ops(),
        [
            physical::PhysicalPipelineOp::Access {
                element: properties::ElementKind::Node,
                access: physical::PhysicalAccess::Kv(exec::KvReadPlan::RangeScan { .. }),
            },
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Range),
        ]
    ));
    assert_eq!(alternative.delivered.cardinality.upper(), Some(5));
    assert_eq!(
        alternative.cost,
        storage
            .range_scan(storage.default_unknown_scan_rows)
            .serial(storage.stream_operator(cost::EstimatedRows::rows(5)))
    );

    let foldable = node_access_window_expr(
        ir::NodeAccessPlan::PointIds {
            ids: element_ids(vec![10, 20, 30]),
        },
        logical::AccessWindowRange::new(1, Some(2)).unwrap(),
    );
    assert_eq!(
        rule.apply(optimizer::RuleInput {
            expr: &foldable,
            storage: &storage,
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        }),
        optimizer::RuleResult::NotApplicable
    );
}
