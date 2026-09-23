use super::*;

#[test]
fn selective_equality_type_union_retains_full_cost_competition() {
    let rules = SeedRuleSet::default();
    let indexes = ["tenant", "type"].into_iter().fold(
        catalog::IndexCatalogSnapshot::default(),
        |indexes, property| {
            indexes.with_node_eq(catalog::ScopedPropertyKey::try_new("Resource", property).unwrap())
        },
    );
    let config = optimizer::OptimizerConfig::from_context(&crate::context::PlannerContext {
        indexes,
        ..Default::default()
    });
    let expr = node_access_filter_expr(
        ir::NodeAccessPlan::LabelScan {
            label: name("Resource"),
        },
        ir::PredicatePlan::new(helix_ast::expr::Predicate::and(vec![
            helix_ast::expr::Predicate::eq("tenant", "one"),
            helix_ast::expr::Predicate::or(vec![
                helix_ast::expr::Predicate::eq("type", "pod"),
                helix_ast::expr::Predicate::eq("type", "service"),
            ]),
        ]))
        .unwrap(),
    );
    let result = optimize(&rules.optimizer(), expr, &config);
    assert_eq!(result.guardrail(), None);
    let candidates = result
        .physical()
        .iter()
        .flat_map(|group| &group.alternatives)
        .collect::<Vec<_>>();
    assert!(candidates.len() <= 5);
    let indexed = candidates
        .iter()
        .find(|entry| {
            matches!(
                entry.alternative.expr,
                physical::PhysicalExpr::Access { .. }
            )
        })
        .unwrap();
    assert_eq!(indexed.alternative.cost.latency.as_micros(), 6_020);
    assert_eq!(indexed.alternative.cost.object_reads, 3);
    assert_eq!(indexed.alternative.cost.multi_get_calls, 1);
    let best = result.best_alternative(result.root()).unwrap();
    assert_eq!(best.cost.latency.as_micros(), 5_190);
    assert_eq!(best.cost.authoritative_graph_reads, 10);
    assert!(best.cost.latency < indexed.alternative.cost.latency);
}

#[test]
fn selective_equality_retains_full_cost_competition() {
    let rules = SeedRuleSet::default();
    let indexes = ["tenant", "type", "deleted"].into_iter().fold(
        catalog::IndexCatalogSnapshot::default(),
        |indexes, property| {
            indexes.with_node_eq(catalog::ScopedPropertyKey::try_new("Resource", property).unwrap())
        },
    );
    let config = optimizer::OptimizerConfig::from_context(&crate::context::PlannerContext {
        indexes,
        ..Default::default()
    });
    let expr = node_access_filter_expr(
        ir::NodeAccessPlan::LabelScan {
            label: name("Resource"),
        },
        ir::PredicatePlan::new(helix_ast::expr::Predicate::and(vec![
            helix_ast::expr::Predicate::eq("tenant", "one"),
            helix_ast::expr::Predicate::eq("type", "pod"),
            helix_ast::expr::Predicate::eq("deleted", true),
        ]))
        .unwrap(),
    );
    let result = optimize(&rules.optimizer(), expr, &config);
    assert_eq!(result.guardrail(), None);
    let candidates = result
        .physical()
        .iter()
        .flat_map(|group| &group.alternatives)
        .collect::<Vec<_>>();
    assert!(
        candidates.len() <= 5,
        "seed pruning must keep the candidate set bounded"
    );
    let scan = candidates
        .iter()
        .find(|entry| matches!(entry.alternative.expr, physical::PhysicalExpr::Pipeline(_)))
        .unwrap();
    let indexed = candidates
        .iter()
        .find(|entry| {
            matches!(
                entry.alternative.expr,
                physical::PhysicalExpr::Access { .. }
            )
        })
        .unwrap();
    assert_eq!(scan.alternative.cost.latency.as_micros(), 30_050);
    assert_eq!(scan.alternative.cost.authoritative_graph_reads, 2000);
    assert_eq!(indexed.alternative.cost.latency.as_micros(), 15_220);
    assert_eq!(indexed.alternative.cost.object_reads, 3);
    assert_eq!(indexed.alternative.cost.cpu_units, 70);
    assert_eq!(indexed.alternative.cost.parallel_width, 1);
    let best = result.best_alternative(result.root()).unwrap();
    assert_eq!(best.cost.latency.as_micros(), 5_190);
    assert_eq!(best.cost.object_reads, 11);
    assert_eq!(best.cost.authoritative_graph_reads, 10);
    assert_eq!(best.cost.cpu_units, 50);
    assert_eq!(best.cost.parallel_width, 1);
}

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
        storage: cost::StorageCostProfile {
            default_equality_index_rows: cost::EstimatedRows::rows(2_000),
            ..Default::default()
        },
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

#[test]
fn indexed_conjunction_retains_faithfully_costed_seed_scan_and_intersection() {
    let rules = SeedRuleSet::default();
    let indexes = ["kind", "name", "namespace", "group_id", "tenant_id"]
        .into_iter()
        .fold(
            catalog::IndexCatalogSnapshot::default(),
            |indexes, property| {
                indexes
                    .with_node_eq(catalog::ScopedPropertyKey::try_new("Fixture", property).unwrap())
            },
        );
    let config = optimizer::OptimizerConfig::from_context(&crate::context::PlannerContext {
        indexes,
        ..Default::default()
    });
    let expr = node_access_filter_expr(
        ir::NodeAccessPlan::LabelScan {
            label: name("Fixture"),
        },
        ir::PredicatePlan::new(helix_ast::expr::Predicate::and(vec![
            helix_ast::expr::Predicate::eq("kind", "fixture-value"),
            helix_ast::expr::Predicate::eq("name", "fixture-value"),
            helix_ast::expr::Predicate::eq("namespace", "fixture-value"),
            helix_ast::expr::Predicate::eq("group_id", "fixture-value"),
            helix_ast::expr::Predicate::eq("tenant_id", "fixture-value"),
        ]))
        .unwrap(),
    );
    let result = optimize(&rules.optimizer(), expr, &config);
    assert_eq!(result.guardrail(), None);
    let candidates = result
        .physical()
        .iter()
        .flat_map(|group| &group.alternatives)
        .collect::<Vec<_>>();
    assert!(
        candidates.len() <= 5,
        "seed pruning must keep the candidate set bounded"
    );
    let scan = candidates
        .iter()
        .find(|entry| matches!(entry.alternative.expr, physical::PhysicalExpr::Pipeline(_)))
        .unwrap();
    let indexed = candidates
        .iter()
        .find(|entry| {
            matches!(
                entry.alternative.expr,
                physical::PhysicalExpr::Access { .. }
            )
        })
        .unwrap();
    assert_eq!(scan.alternative.cost.latency.as_micros(), 32_050);
    assert_eq!(scan.alternative.cost.authoritative_graph_reads, 2000);
    assert_eq!(indexed.alternative.cost.latency.as_micros(), 25_360);
    assert_eq!(indexed.alternative.cost.object_reads, 5);
    assert_eq!(indexed.alternative.cost.cpu_units, 110);
    assert_eq!(indexed.alternative.cost.parallel_width, 1);
    let best = result.best_alternative(result.root()).unwrap();
    assert_eq!(best.cost.latency.as_micros(), 5_210);
    assert_eq!(best.cost.object_reads, 11);
    assert_eq!(best.cost.authoritative_graph_reads, 10);
    assert_eq!(best.cost.cpu_units, 70);
    assert_eq!(best.cost.parallel_width, 1);
}
