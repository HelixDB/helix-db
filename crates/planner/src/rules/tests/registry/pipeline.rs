use super::*;

#[test]
fn seed_rule_set_implements_explored_filter_pushdown_pipeline() {
    let rules = SeedRuleSet::default();
    let optimizer = rules.optimizer();
    let config = optimizer::OptimizerConfig {
        params: Default::default(),
        late_bound_params: Default::default(),
        limits: crate::context::OptimizerLimits::default(),
        planner_limits: crate::context::PlannerLimits::default(),
        stats: crate::context::StatsSnapshot::default(),
        storage: cost::StorageCostProfile::default(),
        indexes: catalog::IndexCatalogSnapshot::default(),
    };
    let predicate = ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap();

    let result = optimize(
        &optimizer,
        filter_pushdown_expr(logical::FilterPushdownOp::Distinct, predicate),
        &config,
    );
    let best = result.best_alternative(result.root()).unwrap();

    assert!(result.memo().group_count() >= 1);
    assert_eq!(result.memo().expression_count(), 2);
    assert_eq!(result.metrics().alternatives_considered, 1);
    assert!(matches!(
        &best.expr,
        physical::PhysicalExpr::Pipeline(pipeline)
            if matches!(
                pipeline.ops(),
                [
                    physical::PhysicalPipelineOp::ResidualFilter,
                    physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Distinct),
                ]
        )
    ));
}

#[test]
fn seed_rule_set_simplifies_pure_pipeline_before_implementation() {
    let rules = SeedRuleSet::default();
    let optimizer = rules.optimizer();
    let config = optimizer::OptimizerConfig {
        params: Default::default(),
        late_bound_params: Default::default(),
        limits: crate::context::OptimizerLimits::default(),
        planner_limits: crate::context::PlannerLimits::default(),
        stats: crate::context::StatsSnapshot::default(),
        storage: cost::StorageCostProfile::default(),
        indexes: catalog::IndexCatalogSnapshot::default(),
    };
    let expr = pipeline_expr(vec![
        logical::PureLogicalOp::NoOp,
        logical::PureLogicalOp::Source {
            element: properties::ElementKind::Node,
        },
        skip(0),
        logical::PureLogicalOp::Distinct,
        logical::PureLogicalOp::Distinct,
    ]);

    let result = optimize(&optimizer, expr, &config);
    let best = result.best_alternative(result.root()).unwrap();

    assert!(result.memo().group_count() >= 1);
    assert!(result.memo().expression_count() >= 2);
    assert!(result.metrics().alternatives_considered >= 2);
    assert!(matches!(
        &best.expr,
        physical::PhysicalExpr::Pipeline(pipeline)
            if matches!(
                pipeline.ops(),
                [
                    physical::PhysicalPipelineOp::Access { .. },
                    physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Distinct),
                ]
        )
    ));
}

#[test]
fn seed_rule_set_composes_static_stream_windows_before_implementation() {
    let rules = SeedRuleSet::default();
    let optimizer = rules.optimizer();
    let config = optimizer::OptimizerConfig {
        params: Default::default(),
        late_bound_params: Default::default(),
        limits: crate::context::OptimizerLimits::default(),
        planner_limits: crate::context::PlannerLimits::default(),
        stats: crate::context::StatsSnapshot::default(),
        storage: cost::StorageCostProfile::default(),
        indexes: catalog::IndexCatalogSnapshot::default(),
    };
    let expr = pipeline_expr(vec![
        logical::PureLogicalOp::Source {
            element: properties::ElementKind::Node,
        },
        skip(2),
        limit(3),
    ]);

    let result = optimize(&optimizer, expr, &config);
    let best = result.best_alternative(result.root()).unwrap();

    assert!(result.memo().expression_count() >= 2);
    assert!(matches!(
        &best.expr,
        physical::PhysicalExpr::Pipeline(pipeline)
            if matches!(
                pipeline.ops(),
                [
                    physical::PhysicalPipelineOp::Access { .. },
                    physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Range),
                ]
        )
    ));
}
