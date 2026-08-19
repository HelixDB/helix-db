use super::*;

#[test]
fn seed_rule_set_explores_access_filter_before_access_implementation() {
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
    let impossible = ir::PredicatePlan::new(helix_ast::expr::Predicate::compare(
        helix_ast::expr::Expr::val(1),
        helix_ast::expr::CompareOp::Eq,
        helix_ast::expr::Expr::val(2),
    ))
    .unwrap();
    let expr = node_access_filter_expr(
        ir::NodeAccessPlan::PointIds {
            ids: element_ids(vec![7]),
        },
        impossible,
    );
    let label_conflict = node_access_filter_expr(
        ir::NodeAccessPlan::LabelScan {
            label: name("User"),
        },
        ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("$label", "Admin")).unwrap(),
    );

    for expr in [expr, label_conflict] {
        let result = optimize(&optimizer, expr, &config);
        let best = result.best_alternative(result.root()).unwrap();

        assert!(result.memo().group_count() >= 1);
        assert!(result.memo().expression_count() >= 2);
        assert!(result.metrics().alternatives_considered >= 1);
        assert!(matches!(
            &best.expr,
            physical::PhysicalExpr::Access {
                access: physical::PhysicalAccess::Empty,
                ..
            }
        ));
    }
}

#[test]
fn seed_rule_set_explores_catalog_indexed_access_filters_before_implementation() {
    let rules = SeedRuleSet::default();
    let optimizer = rules.optimizer();
    let key = catalog::ScopedPropertyKey::try_new("User", "age").unwrap();
    let config = optimizer::OptimizerConfig {
        params: Default::default(),
        late_bound_params: Default::default(),
        limits: crate::context::OptimizerLimits::default(),
        planner_limits: crate::context::PlannerLimits::default(),
        stats: crate::context::StatsSnapshot::default(),
        storage: cost::StorageCostProfile::default(),
        indexes: catalog::IndexCatalogSnapshot::default().with_node_eq(key),
    };
    let expr = node_access_filter_expr(
        ir::NodeAccessPlan::LabelScan {
            label: name("User"),
        },
        ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("age", 42)).unwrap(),
    );

    let result = optimize(&optimizer, expr, &config);
    let best = result.best_alternative(result.root()).unwrap();

    assert!(result.memo().group_count() >= 1);
    assert!(result.memo().expression_count() >= 2);
    assert!(result.metrics().alternatives_considered >= 1);
    assert!(matches!(
        &best.expr,
        physical::PhysicalExpr::Access {
            access: physical::PhysicalAccess::NodeExact(exact),
            ..
        } if matches!(exact.as_ref(), exec::ExecNodeAccessPlan::Bitmap { .. })
    ));
}

#[test]
fn seed_rule_set_explores_catalog_indexed_access_filter_intersections() {
    let rules = SeedRuleSet::default();
    let optimizer = rules.optimizer();
    let age_key = range_key("User", "age", helix_ast::index::RangeIndexDirection::Asc);
    let score_key = catalog::ScopedPropertyKey::try_new("User", "score").unwrap();
    let config = optimizer::OptimizerConfig {
        params: Default::default(),
        late_bound_params: Default::default(),
        limits: crate::context::OptimizerLimits::default(),
        planner_limits: crate::context::PlannerLimits::default(),
        stats: crate::context::StatsSnapshot::default(),
        storage: cost::StorageCostProfile::default(),
        indexes: catalog::IndexCatalogSnapshot::default()
            .with_node_range(age_key)
            .with_node_eq(score_key),
    };
    let expr = node_access_filter_expr(
        ir::NodeAccessPlan::LabelScan {
            label: name("User"),
        },
        ir::PredicatePlan::new(helix_ast::expr::Predicate::and(vec![
            helix_ast::expr::Predicate::gte("age", 21),
            helix_ast::expr::Predicate::eq("score", 90),
        ]))
        .unwrap(),
    );

    let result = optimize(&optimizer, expr, &config);
    let best = result.best_alternative(result.root()).unwrap();

    assert!(result.memo().group_count() >= 1);
    assert!(result.memo().expression_count() >= 2);
    assert!(result.metrics().alternatives_considered >= 1);
    assert!(matches!(
        &best.expr,
        physical::PhysicalExpr::Access {
            access: physical::PhysicalAccess::NodeExact(exact),
            ..
        } if matches!(
            exact.as_ref(),
            exec::ExecNodeAccessPlan::SecondarySet {
                set: exec::ExecNodeSecondarySetPlan::OrderedIntersect { .. }
            }
        )
    ));
}

#[test]
fn seed_rule_set_explores_catalog_indexed_access_filter_unions() {
    let rules = SeedRuleSet::default();
    let optimizer = rules.optimizer();
    let age_key = catalog::ScopedPropertyKey::try_new("User", "age").unwrap();
    let config = optimizer::OptimizerConfig {
        params: Default::default(),
        late_bound_params: Default::default(),
        limits: crate::context::OptimizerLimits::default(),
        planner_limits: crate::context::PlannerLimits::default(),
        stats: crate::context::StatsSnapshot::default(),
        storage: cost::StorageCostProfile::default(),
        indexes: catalog::IndexCatalogSnapshot::default().with_node_eq(age_key),
    };
    let expr = node_access_filter_expr(
        ir::NodeAccessPlan::LabelScan {
            label: name("User"),
        },
        ir::PredicatePlan::new(helix_ast::expr::Predicate::or(vec![
            helix_ast::expr::Predicate::eq("age", 21),
            helix_ast::expr::Predicate::eq("age", 42),
        ]))
        .unwrap(),
    );

    let result = optimize(&optimizer, expr, &config);
    let best = result.best_alternative(result.root()).unwrap();

    assert!(result.memo().group_count() >= 1);
    assert!(result.memo().expression_count() >= 2);
    assert!(result.metrics().alternatives_considered >= 1);
    assert!(matches!(
        &best.expr,
        physical::PhysicalExpr::Access {
            access: physical::PhysicalAccess::NodeExact(exact),
            ..
        } if matches!(
            exact.as_ref(),
            exec::ExecNodeAccessPlan::SecondarySet {
                set: exec::ExecNodeSecondarySetPlan::Bitmap(
                    exec::ExecNodeBitmapExpr::BatchedUnionRead { .. }
                )
            }
        )
    ));
}
