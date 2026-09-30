use super::*;

#[test]
fn selective_equality_type_union_is_one_index_intersection() {
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
    assert!(!candidates
        .iter()
        .any(|entry| is_label_scan_pipeline(&entry.alternative.expr)));
    let indexed = candidates
        .iter()
        .find(|entry| {
            matches!(
                entry.alternative.expr,
                physical::PhysicalExpr::Access { .. }
            )
        })
        .unwrap();
    // The tenant bitmap (5,000 get + 50 probe + 10 decode) and the batched
    // type union (750 setup + 2 x 75 keys + 20 decode) are read
    // concurrently: 5,060 critical path + 2 x 25 task overhead, then a
    // 30-row set operation and 10 materialized rows.
    assert_eq!(indexed.alternative.cost.latency.as_micros(), 5_150);
    assert_eq!(indexed.alternative.cost.parallel_width, 2);
    assert_eq!(indexed.alternative.cost.object_reads, 3);
    assert_eq!(indexed.alternative.cost.multi_get_calls, 1);
    // No single-index seed evaluates the other indexed conjuncts per row.
    let best = result.best_alternative(result.root()).unwrap();
    assert_eq!(best.cost, indexed.alternative.cost);
    assert_eq!(best.cost.authoritative_graph_reads, 0);
}

#[test]
fn selective_equality_offers_only_the_full_intersection() {
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
        "index exploration must keep the candidate set bounded"
    );
    // Every conjunct is index-served, so no group holds a label scan with a
    // per-row filter, whatever it would cost.
    assert!(!candidates
        .iter()
        .any(|entry| is_label_scan_pipeline(&entry.alternative.expr)));
    let indexed = candidates
        .iter()
        .find(|entry| {
            matches!(
                entry.alternative.expr,
                physical::PhysicalExpr::Access { .. }
            )
        })
        .unwrap();
    // Three 5,060 us bitmap reads run concurrently: 5,060 + 3 x 25 task
    // overhead, then a 30-row set operation and 10 materialized rows.
    assert_eq!(indexed.alternative.cost.latency.as_micros(), 5_175);
    assert_eq!(indexed.alternative.cost.object_reads, 3);
    assert_eq!(indexed.alternative.cost.cpu_units, 70);
    assert_eq!(indexed.alternative.cost.parallel_width, 3);
    let best = result.best_alternative(result.root()).unwrap();
    assert_eq!(best.cost, indexed.alternative.cost);
    assert_eq!(best.cost.authoritative_graph_reads, 0);
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
fn indexed_conjunction_offers_only_the_full_intersection() {
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
        "index exploration must keep the candidate set bounded"
    );
    assert!(!candidates
        .iter()
        .any(|entry| is_label_scan_pipeline(&entry.alternative.expr)));
    let indexed = candidates
        .iter()
        .find(|entry| {
            matches!(
                entry.alternative.expr,
                physical::PhysicalExpr::Access { .. }
            )
        })
        .unwrap();
    // Five 5,060 us bitmap reads run concurrently: 5,060 + 5 x 25 task
    // overhead, then a 50-row set operation and 10 materialized rows.
    assert_eq!(indexed.alternative.cost.latency.as_micros(), 5_245);
    assert_eq!(indexed.alternative.cost.object_reads, 5);
    assert_eq!(indexed.alternative.cost.cpu_units, 110);
    assert_eq!(indexed.alternative.cost.parallel_width, 5);
    let best = result.best_alternative(result.root()).unwrap();
    assert_eq!(best.cost, indexed.alternative.cost);
    assert_eq!(best.cost.authoritative_graph_reads, 0);
}

/// Whether `expr` scans a label and filters its rows one by one.
fn is_label_scan_pipeline(expr: &physical::PhysicalExpr) -> bool {
    let physical::PhysicalExpr::Pipeline(pipeline) = expr else {
        return false;
    };
    let scans = pipeline.ops().iter().any(|op| match op {
        physical::PhysicalPipelineOp::Access { access, .. } => match access {
            physical::PhysicalAccess::LabelScan => true,
            physical::PhysicalAccess::NodeExact(access) => {
                matches!(access.as_ref(), exec::ExecNodeAccessPlan::LabelScan { .. })
            }
            physical::PhysicalAccess::EdgeExact(access) => {
                matches!(access.as_ref(), exec::ExecEdgeAccessPlan::LabelScan { .. })
            }
            _ => false,
        },
        _ => false,
    });
    scans
        && pipeline
            .ops()
            .iter()
            .any(|op| matches!(op, physical::PhysicalPipelineOp::ResidualFilter))
}
