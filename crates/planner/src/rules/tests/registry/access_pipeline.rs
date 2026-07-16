use super::*;

#[test]
fn seed_rule_set_explores_access_windows_before_access_implementation() {
    let rules = SeedRuleSet::default();
    let optimizer = rules.optimizer();
    let config = optimizer::OptimizerConfig {
        limits: crate::context::OptimizerLimits::default(),
        planner_limits: crate::context::PlannerLimits::default(),
        stats: crate::context::StatsSnapshot::default(),
        storage: cost::StorageCostProfile::default(),
        indexes: catalog::IndexCatalogSnapshot::default(),
    };
    let expr = node_access_window_expr(
        ir::NodeAccessPlan::PointIds {
            ids: element_ids(vec![10, 20, 30]),
        },
        logical::AccessWindowRange::new(1, Some(2)).unwrap(),
    );

    let result = optimize(&optimizer, expr, &config);
    let best = result.best_alternative(result.root()).unwrap();

    assert!(result.memo().group_count() >= 1);
    assert!(result.memo().expression_count() >= 2);
    assert!(result.metrics().alternatives_considered >= 1);
    assert!(matches!(
        &best.expr,
        physical::PhysicalExpr::Access {
            access: physical::PhysicalAccess::Kv(exec::KvReadPlan::Get { .. }),
            ..
        }
    ));
}

#[test]
fn seed_rule_set_explores_access_order_before_access_implementation() {
    let rules = SeedRuleSet::default();
    let optimizer = rules.optimizer();
    let config = optimizer::OptimizerConfig {
        limits: crate::context::OptimizerLimits::default(),
        planner_limits: crate::context::PlannerLimits::default(),
        stats: crate::context::StatsSnapshot::default(),
        storage: cost::StorageCostProfile::default(),
        indexes: catalog::IndexCatalogSnapshot::default(),
    };
    let expr = node_access_order_expr(
        ir::NodeAccessPlan::from(node_range_source("User", "age", lower_range(18))),
        order_keys(),
    );

    let result = optimize(&optimizer, expr, &config);
    let best = result.best_alternative(result.root()).unwrap();

    assert!(result.memo().group_count() >= 1);
    assert!(result.memo().expression_count() >= 2);
    assert!(result.metrics().alternatives_considered >= 1);
    assert!(matches!(
        &best.expr,
        physical::PhysicalExpr::Access {
            access: physical::PhysicalAccess::RangeIndex,
            ..
        }
    ));
}

#[test]
fn seed_rule_set_rewrites_access_order_to_catalog_direction_before_implementation() {
    let rules = SeedRuleSet::default();
    let optimizer = rules.optimizer();
    let config = optimizer::OptimizerConfig {
        limits: crate::context::OptimizerLimits::default(),
        planner_limits: crate::context::PlannerLimits::default(),
        stats: crate::context::StatsSnapshot::default(),
        storage: cost::StorageCostProfile::default(),
        indexes: catalog::IndexCatalogSnapshot::default().with_node_range(range_key(
            "User",
            "age",
            helix_ast::index::RangeIndexDirection::Desc,
        )),
    };
    let expr = node_access_order_expr(
        ir::NodeAccessPlan::from(node_range_source("User", "age", lower_range(18))),
        desc_order_keys(),
    );

    let result = optimize(&optimizer, expr, &config);
    let best = result.best_alternative(result.root()).unwrap();

    assert!(result.memo().group_count() >= 1);
    assert!(result.memo().expression_count() >= 2);
    assert!(result.metrics().alternatives_considered >= 1);
    assert!(matches!(
        &best.expr,
        physical::PhysicalExpr::Access {
            access: physical::PhysicalAccess::RangeIndex,
            ..
        }
    ));
    assert_eq!(
        best.delivered.ordering,
        properties::DeliveredOrdering::ByKeys(desc_order_keys())
    );
}

#[test]
fn seed_rule_set_explores_access_distinct_before_access_implementation() {
    let rules = SeedRuleSet::default();
    let optimizer = rules.optimizer();
    let config = optimizer::OptimizerConfig {
        limits: crate::context::OptimizerLimits::default(),
        planner_limits: crate::context::PlannerLimits::default(),
        stats: crate::context::StatsSnapshot::default(),
        storage: cost::StorageCostProfile::default(),
        indexes: catalog::IndexCatalogSnapshot::default(),
    };
    let expr = node_access_distinct_expr(ir::NodeAccessPlan::PointIds {
        ids: element_ids(vec![7]),
    });

    let result = optimize(&optimizer, expr, &config);
    let best = result.best_alternative(result.root()).unwrap();

    assert!(result.memo().group_count() >= 1);
    assert!(result.memo().expression_count() >= 2);
    assert!(result.metrics().alternatives_considered >= 1);
    assert!(matches!(
        &best.expr,
        physical::PhysicalExpr::Access {
            access: physical::PhysicalAccess::Kv(exec::KvReadPlan::Get { .. }),
            ..
        }
    ));
}

#[test]
fn seed_rule_set_simplifies_empty_access_pipeline_before_implementation() {
    let rules = SeedRuleSet::default();
    let optimizer = rules.optimizer();
    let config = optimizer::OptimizerConfig {
        limits: crate::context::OptimizerLimits::default(),
        planner_limits: crate::context::PlannerLimits::default(),
        stats: crate::context::StatsSnapshot::default(),
        storage: cost::StorageCostProfile::default(),
        indexes: catalog::IndexCatalogSnapshot::default(),
    };
    let expr = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            edge_access_path(ir::EdgeAccessPlan::Empty),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Expand {
                plan: ir::ExpandPlan {
                    direction: ir::ExpandDirection::Out,
                    output: ir::ExpandOutput::Nodes,
                    label: ir::ExpandLabelPlan::Any,
                },
            }),
        )
        .unwrap(),
    );

    let result = optimize(&optimizer, expr, &config);
    let best = result.best_alternative(result.root()).unwrap();

    assert!(result.memo().group_count() >= 1);
    assert!(result.memo().expression_count() >= 2);
    assert!(matches!(
        &best.expr,
        physical::PhysicalExpr::Access {
            element: properties::ElementKind::Node,
            access: physical::PhysicalAccess::Empty,
        }
    ));
}
