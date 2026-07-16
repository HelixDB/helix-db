use super::*;

#[test]
fn filter_and_barrier_rules_preserve_effect_boundaries() {
    let filter = FilterImplementationRule::default();
    let barrier = BarrierImplementationRule::default();
    let storage = cost::StorageCostProfile {
        cpu_predicate_eval: cost::LatencyEstimate::micros(4),
        barrier_overhead: cost::LatencyEstimate::micros(9),
        default_unknown_scan_rows: cost::EstimatedRows::rows(3),
        ..cost::StorageCostProfile::default()
    };
    let predicate = ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap();
    let filter_expr = logical::LogicalExpr::Pure(logical::PureLogicalOp::Filter { predicate });
    let ddl_expr = logical::LogicalExpr::Barrier(logical::BarrierLogicalOp::IndexDdl);

    let filter = physical_alternative(filter.apply(optimizer::RuleInput {
        expr: &filter_expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let barrier = physical_alternative(barrier.apply(optimizer::RuleInput {
        expr: &ddl_expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert!(matches!(
        filter.expr,
        physical::PhysicalExpr::ResidualFilter
    ));
    assert_eq!(filter.cost.latency, cost::LatencyEstimate::micros(12));
    assert_eq!(barrier.delivered.effect, properties::EffectKind::Barrier);
    assert_eq!(
        barrier.delivered.cardinality,
        properties::CardinalityBounds::exact(1)
    );
    assert_eq!(barrier.cost.latency, cost::LatencyEstimate::micros(9));
}

#[test]
fn barrier_rule_covers_all_barrier_variants_and_rejects_pure_exprs() {
    let rule = BarrierImplementationRule::default();
    let storage = cost::StorageCostProfile::default();

    for op in [
        logical::BarrierLogicalOp::Mutation,
        logical::BarrierLogicalOp::StateWrite,
        logical::BarrierLogicalOp::TraversalControl,
    ] {
        let expr = logical::LogicalExpr::Barrier(op);
        let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
            expr: &expr,
            storage: &storage,
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        }));
        assert_eq!(
            alternative.delivered.cardinality,
            properties::CardinalityBounds::unknown()
        );
        assert_eq!(
            alternative.delivered.effect,
            properties::EffectKind::Barrier
        );
    }

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
