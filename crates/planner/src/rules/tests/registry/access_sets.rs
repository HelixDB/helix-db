use super::*;

#[test]
fn seed_rule_set_explores_access_set_simplification_before_implementation() {
    let rules = SeedRuleSet::default();
    let optimizer = rules.optimizer();
    let config = optimizer::OptimizerConfig {
        limits: crate::context::OptimizerLimits::default(),
        planner_limits: crate::context::PlannerLimits::default(),
        stats: crate::context::StatsSnapshot::default(),
        storage: cost::StorageCostProfile::default(),
        indexes: catalog::IndexCatalogSnapshot::default(),
    };
    let point = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::PointIds {
        ids: element_ids(vec![7]),
    })
    .unwrap();
    let expr = node_access_expr(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Empty).unwrap(),
        point,
    )));

    let result = optimize(&optimizer, expr, &config);
    let best = result.best_alternative(result.root()).unwrap();

    assert!(result.memo().group_count() >= 1);
    assert_eq!(result.memo().expression_count(), 2);
    assert!(result.metrics().alternatives_considered >= 2);
    assert!(matches!(
        &best.expr,
        physical::PhysicalExpr::Access {
            access: physical::PhysicalAccess::Kv(exec::KvReadPlan::Get { .. }),
            ..
        }
    ));
}

#[test]
fn seed_rule_set_explores_range_intersection_before_access_implementation() {
    let rules = SeedRuleSet::default();
    let optimizer = rules.optimizer();
    let config = optimizer::OptimizerConfig {
        limits: crate::context::OptimizerLimits::default(),
        planner_limits: crate::context::PlannerLimits::default(),
        stats: crate::context::StatsSnapshot::default(),
        storage: cost::StorageCostProfile::default(),
        indexes: catalog::IndexCatalogSnapshot::default(),
    };
    let expr = node_access_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            node_range_source("User", "age", lower_range(18)),
            node_range_source("User", "age", upper_range(65)),
        ),
    ));

    let result = optimize(&optimizer, expr, &config);
    let best = result.best_alternative(result.root()).unwrap();

    assert!(result.memo().group_count() >= 1);
    assert_eq!(result.memo().expression_count(), 2);
    assert!(result.metrics().alternatives_considered >= 2);
    assert!(matches!(
        &best.expr,
        physical::PhysicalExpr::Access {
            access: physical::PhysicalAccess::RangeIndex,
            ..
        }
    ));
}

#[test]
fn seed_rule_set_explores_equality_range_intersection_before_access_implementation() {
    let rules = SeedRuleSet::default();
    let optimizer = rules.optimizer();
    let config = optimizer::OptimizerConfig {
        limits: crate::context::OptimizerLimits::default(),
        planner_limits: crate::context::PlannerLimits::default(),
        stats: crate::context::StatsSnapshot::default(),
        storage: cost::StorageCostProfile::default(),
        indexes: catalog::IndexCatalogSnapshot::default(),
    };
    let union =
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            node_eq_source("User", "age", equality_literal(10)),
            node_eq_source("User", "age", equality_literal(30)),
        )))
        .unwrap();
    let expr = node_access_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(union, node_range_source("User", "age", lower_range(21))),
    ));

    let result = optimize(&optimizer, expr, &config);
    let best = result.best_alternative(result.root()).unwrap();

    assert!(result.memo().group_count() >= 1);
    assert!(result.memo().expression_count() >= 2);
    assert!(result.metrics().alternatives_considered >= 2);
    assert!(matches!(
        &best.expr,
        physical::PhysicalExpr::Access {
            access: physical::PhysicalAccess::EqualityIndex,
            ..
        }
    ));
}

#[test]
fn seed_rule_set_explores_equality_range_union_before_access_implementation() {
    let rules = SeedRuleSet::default();
    let optimizer = rules.optimizer();
    let config = optimizer::OptimizerConfig {
        limits: crate::context::OptimizerLimits::default(),
        planner_limits: crate::context::PlannerLimits::default(),
        stats: crate::context::StatsSnapshot::default(),
        storage: cost::StorageCostProfile::default(),
        indexes: catalog::IndexCatalogSnapshot::default(),
    };
    let expr = node_access_expr(ir::NodeAccessPlan::Union(
        ir::AtLeast::<_, 2>::try_from_vec(vec![
            node_eq_source("User", "age", equality_literal(30)),
            node_eq_source("User", "age", equality_literal(40)),
            node_range_source("User", "age", lower_range(21)),
        ])
        .unwrap(),
    ));

    let result = optimize(&optimizer, expr, &config);
    let best = result.best_alternative(result.root()).unwrap();

    assert!(result.memo().group_count() >= 1);
    assert!(result.memo().expression_count() >= 2);
    assert!(result.metrics().alternatives_considered >= 2);
    assert!(matches!(
        &best.expr,
        physical::PhysicalExpr::Access {
            access: physical::PhysicalAccess::RangeIndex,
            ..
        }
    ));
}

#[test]
fn seed_rule_set_explores_access_contradiction_before_access_implementation() {
    let rules = SeedRuleSet::default();
    let optimizer = rules.optimizer();
    let config = optimizer::OptimizerConfig {
        limits: crate::context::OptimizerLimits::default(),
        planner_limits: crate::context::PlannerLimits::default(),
        stats: crate::context::StatsSnapshot::default(),
        storage: cost::StorageCostProfile::default(),
        indexes: catalog::IndexCatalogSnapshot::default(),
    };
    let expr = node_access_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            node_eq_source("User", "age", equality_literal(10)),
            node_range_source("User", "age", lower_range(21)),
        ),
    ));

    let result = optimize(&optimizer, expr, &config);
    let best = result.best_alternative(result.root()).unwrap();

    assert!(result.memo().group_count() >= 1);
    assert!(result.memo().expression_count() >= 2);
    assert!(result.metrics().alternatives_considered >= 2);
    assert!(matches!(
        &best.expr,
        physical::PhysicalExpr::Access {
            access: physical::PhysicalAccess::Empty,
            ..
        }
    ));
}

#[test]
fn seed_rule_set_explores_access_subsumption_before_access_implementation() {
    let rules = SeedRuleSet::default();
    let optimizer = rules.optimizer();
    let config = optimizer::OptimizerConfig {
        limits: crate::context::OptimizerLimits::default(),
        planner_limits: crate::context::PlannerLimits::default(),
        stats: crate::context::StatsSnapshot::default(),
        storage: cost::StorageCostProfile::default(),
        indexes: catalog::IndexCatalogSnapshot::default(),
    };
    let label = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::LabelScan {
        label: name("User"),
    })
    .unwrap();
    let expr = node_access_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(label, node_eq_source("User", "age", equality_literal(30))),
    ));

    let result = optimize(&optimizer, expr, &config);
    let best = result.best_alternative(result.root()).unwrap();

    assert!(result.memo().group_count() >= 1);
    assert!(result.memo().expression_count() >= 2);
    assert!(result.metrics().alternatives_considered >= 2);
    assert!(matches!(
        &best.expr,
        physical::PhysicalExpr::Access {
            access: physical::PhysicalAccess::EqualityIndex,
            ..
        }
    ));
}
