use super::*;

#[test]
fn seed_rule_set_simplifies_empty_root_control_flow_before_implementation() {
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
    let branch = optional_branch_expr(edge_access_expr(ir::EdgeAccessPlan::Empty), node_all_expr());
    let repeat = repeat_root_expr(
        node_access_expr(ir::NodeAccessPlan::Empty),
        edge_all_expr(),
        2,
    );

    for (expr, element) in [
        (branch, properties::ElementKind::Edge),
        (repeat, properties::ElementKind::Node),
    ] {
        let result = optimize(&optimizer, expr, &config);
        let best = result.best_alternative(result.root()).unwrap();

        assert!(result.memo().group_count() >= 1);
        assert!(result.memo().expression_count() >= 2);
        assert!(matches!(
            &best.expr,
            physical::PhysicalExpr::Access {
                element: delivered,
                access: physical::PhysicalAccess::Empty,
            } if delivered == &element
        ));
    }
}
