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
    assert_eq!(candidates.len(), 2);
    for entry in &candidates {
        eprintln!(
            "type union candidate: {:?} cost={:?}",
            entry.alternative.expr, entry.alternative.cost
        );
    }
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
    assert_eq!(
        result.best_alternative(result.root()).unwrap(),
        &indexed.alternative
    );
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
    for group in result.physical() {
        for entry in &group.alternatives {
            eprintln!(
                "candidate {:?}: {:?} cost={:?}",
                entry.id, entry.alternative.expr, entry.alternative.cost
            );
        }
    }
    let candidates = result
        .physical()
        .iter()
        .flat_map(|group| &group.alternatives)
        .collect::<Vec<_>>();
    assert_eq!(
        candidates.len(),
        2,
        "both scan and intersection must survive exploration"
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
    assert_eq!(scan.alternative.cost.latency.as_micros(), 18_050);
    assert_eq!(scan.alternative.cost.authoritative_graph_reads, 1000);
    assert_eq!(indexed.alternative.cost.latency.as_micros(), 15_220);
    assert_eq!(indexed.alternative.cost.object_reads, 3);
    assert_eq!(indexed.alternative.cost.cpu_units, 70);
    assert_eq!(indexed.alternative.cost.parallel_width, 1);
    assert_eq!(
        result.best_alternative(result.root()).unwrap(),
        &indexed.alternative
    );
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
