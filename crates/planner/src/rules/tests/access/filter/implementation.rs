use super::*;

#[test]
fn access_filter_implementation_rule_preserves_residual_filter_pipeline() {
    let rule = AccessFilterImplementationRule::default();
    let storage = cost::StorageCostProfile {
        cpu_predicate_eval: cost::LatencyEstimate::micros(5),
        range_seek: cost::LatencyEstimate::micros(7),
        range_next: cost::LatencyEstimate::micros(11),
        default_unknown_scan_rows: cost::EstimatedRows::rows(100),
        ..cost::StorageCostProfile::default()
    };
    let stats =
        crate::context::StatsSnapshot::default().with_node_label_cardinality(name("User"), 12);
    let predicate = ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap();
    let expr = node_access_filter_expr(
        ir::NodeAccessPlan::LabelScan {
            label: name("User"),
        },
        predicate,
    );

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: &stats,
    }));

    assert_eq!(rule.metadata().id.as_ref(), "seed_access_filter");
    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical access-filter pipeline");
    };
    assert!(matches!(
        pipeline.ops(),
        [
            physical::PhysicalPipelineOp::Access {
                element: properties::ElementKind::Node,
                access: physical::PhysicalAccess::LabelScan,
            },
            physical::PhysicalPipelineOp::ResidualFilter,
        ]
    ));
    assert_eq!(
        alternative.delivered.element,
        Some(properties::ElementKind::Node)
    );
    assert_eq!(alternative.delivered.cardinality.upper(), None);
    let rows = cost::EstimatedRows::rows(12);
    assert_eq!(
        alternative.cost,
        storage
            .range_scan(rows)
            .serial(storage.predicate_eval(rows))
    );

    let empty_expr = node_access_filter_expr(
        ir::NodeAccessPlan::Empty,
        ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap(),
    );
    assert_eq!(
        rule.apply(optimizer::RuleInput {
            expr: &empty_expr,
            storage: &storage,
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: &stats,
        }),
        optimizer::RuleResult::NotApplicable
    );
}
