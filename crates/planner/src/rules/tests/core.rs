use super::*;

#[test]
fn static_predicate_rule_rewrites_tautologies_and_impossible_filters() {
    let rule = StaticPredicateSimplificationRule::default();
    let storage = cost::StorageCostProfile::default();
    let tautology = logical::LogicalExpr::Pure(logical::PureLogicalOp::Filter {
        predicate: ir::PredicatePlan::new(helix_ast::expr::Predicate::compare(
            helix_ast::expr::Expr::val(1),
            helix_ast::expr::CompareOp::Eq,
            helix_ast::expr::Expr::val(1),
        ))
        .unwrap(),
    });
    let impossible = logical::LogicalExpr::Pure(logical::PureLogicalOp::Filter {
        predicate: ir::PredicatePlan::new(helix_ast::expr::Predicate::compare(
            helix_ast::expr::Expr::val(1),
            helix_ast::expr::CompareOp::Eq,
            helix_ast::expr::Expr::val(2),
        ))
        .unwrap(),
    });

    assert_eq!(
        rule.metadata().id.as_ref(),
        "static_predicate_simplification"
    );
    assert_eq!(
        logical_pure_op(rule.apply(optimizer::RuleInput {
            expr: &tautology,
            storage: &storage,
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        })),
        logical::PureLogicalOp::NoOp
    );
    assert_eq!(
        logical_pure_op(rule.apply(optimizer::RuleInput {
            expr: &impossible,
            storage: &storage,
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        })),
        logical::PureLogicalOp::Empty
    );
}

#[test]
fn static_predicate_rule_declines_feasible_and_non_filter_inputs() {
    let rule = StaticPredicateSimplificationRule::default();
    let storage = cost::StorageCostProfile::default();
    let feasible = logical::LogicalExpr::Pure(logical::PureLogicalOp::Filter {
        predicate: ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap(),
    });

    for expr in [
        feasible,
        source(properties::ElementKind::Node),
        logical::LogicalExpr::Pure(logical::PureLogicalOp::NoOp),
        logical::LogicalExpr::Barrier(logical::BarrierLogicalOp::Mutation),
    ] {
        assert_eq!(
            rule.apply(optimizer::RuleInput {
                expr: &expr,
                storage: &storage,
                indexes: empty_indexes(),
                planner_limits: default_planner_limits(),
                stats: default_stats(),
            }),
            optimizer::RuleResult::NotApplicable
        );
    }
}

#[test]
fn simplified_predicate_rule_implements_zero_cost_noop_and_empty() {
    let rule = SimplifiedPredicateImplementationRule::default();
    let storage = cost::StorageCostProfile::default();
    let noop = logical::LogicalExpr::Pure(logical::PureLogicalOp::NoOp);
    let empty = logical::LogicalExpr::Pure(logical::PureLogicalOp::Empty);
    let feasible = logical::LogicalExpr::Pure(logical::PureLogicalOp::Filter {
        predicate: ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap(),
    });

    let noop = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &noop,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let empty = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &empty,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "seed_simplified_predicate");
    assert!(matches!(noop.expr, physical::PhysicalExpr::NoOp));
    assert_eq!(noop.cost, cost::CostVector::ZERO);
    assert!(matches!(empty.expr, physical::PhysicalExpr::Empty));
    assert_eq!(empty.cost, cost::CostVector::ZERO);
    assert_eq!(
        empty.delivered.cardinality,
        properties::CardinalityBounds::exact(0)
    );
    assert_eq!(
        rule.apply(optimizer::RuleInput {
            expr: &feasible,
            storage: &storage,
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        }),
        optimizer::RuleResult::NotApplicable
    );
}

#[test]
fn filter_merge_rule_combines_adjacent_filters_into_one_predicate() {
    let rule = FilterMergeRule::default();
    let storage = cost::StorageCostProfile::default();
    let active = ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap();
    let tenant = ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("tenant", "acme")).unwrap();
    let expr = filter_chain_expr(vec![active.clone(), tenant.clone()]);

    let merged = logical_filter(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "filter_merge");
    assert!(matches!(
        merged.as_ref(),
        helix_ast::expr::Predicate::And { predicates }
            if predicates == &vec![active.as_ref().clone(), tenant.as_ref().clone()]
    ));
}

#[test]
fn filter_merge_rule_applies_only_to_filter_chains() {
    let rule = FilterMergeRule::default();
    let storage = cost::StorageCostProfile::default();
    let predicate = ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap();

    for expr in [
        logical::LogicalExpr::Pure(logical::PureLogicalOp::Filter { predicate }),
        pipeline_expr(vec![limit(1), skip(1)]),
        logical::LogicalExpr::Barrier(logical::BarrierLogicalOp::Mutation),
    ] {
        assert_eq!(
            rule.apply(optimizer::RuleInput {
                expr: &expr,
                storage: &storage,
                indexes: empty_indexes(),
                planner_limits: default_planner_limits(),
                stats: default_stats(),
            }),
            optimizer::RuleResult::NotApplicable
        );
    }
}

#[test]
fn filter_pushdown_rule_transposes_safe_order_and_distinct_pairs() {
    let rule = FilterPushdownRule::default();
    let storage = cost::StorageCostProfile::default();
    let predicate = ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap();
    let order = filter_pushdown_expr(
        logical::FilterPushdownOp::Order {
            ordering: properties::RequiredOrdering::ByKeys(order_keys()),
        },
        predicate.clone(),
    );
    let distinct = filter_pushdown_expr(logical::FilterPushdownOp::Distinct, predicate.clone());

    let ordered_pipeline = logical_pipeline(rule.apply(optimizer::RuleInput {
        expr: &order,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let distinct_pipeline = logical_pipeline(rule.apply(optimizer::RuleInput {
        expr: &distinct,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "filter_pushdown");
    assert!(matches!(
        ordered_pipeline.ops(),
        [
            logical::PureLogicalOp::Filter { predicate: pushed },
            logical::PureLogicalOp::Order {
                ordering: properties::RequiredOrdering::ByKeys(_)
            },
        ] if pushed == &predicate
    ));
    assert!(matches!(
        distinct_pipeline.ops(),
        [
            logical::PureLogicalOp::Filter { predicate: pushed },
            logical::PureLogicalOp::Distinct,
        ] if pushed == &predicate
    ));
}

#[test]
fn filter_pushdown_rule_declines_non_candidates() {
    let rule = FilterPushdownRule::default();
    let storage = cost::StorageCostProfile::default();
    let predicate = ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap();

    for expr in [
        logical::LogicalExpr::Pure(logical::PureLogicalOp::Filter { predicate }),
        pipeline_expr(vec![logical::PureLogicalOp::Distinct, limit(1)]),
        logical::LogicalExpr::Barrier(logical::BarrierLogicalOp::Mutation),
    ] {
        assert_eq!(
            rule.apply(optimizer::RuleInput {
                expr: &expr,
                storage: &storage,
                indexes: empty_indexes(),
                planner_limits: default_planner_limits(),
                stats: default_stats(),
            }),
            optimizer::RuleResult::NotApplicable
        );
    }
}

#[test]
fn pure_pipeline_simplification_removes_noops_zero_skips_and_duplicate_distincts() {
    let rule = PurePipelineSimplificationRule::default();
    let storage = cost::StorageCostProfile::default();
    let expr = pipeline_expr(vec![
        logical::PureLogicalOp::NoOp,
        logical::PureLogicalOp::Source {
            element: properties::ElementKind::Node,
        },
        skip(0),
        logical::PureLogicalOp::Distinct,
        logical::PureLogicalOp::NoOp,
        logical::PureLogicalOp::Distinct,
        limit(3),
    ]);

    let pipeline = logical_pipeline(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "pure_pipeline_simplification");
    assert!(matches!(
        pipeline.ops(),
        [
            logical::PureLogicalOp::Source {
                element: properties::ElementKind::Node
            },
            logical::PureLogicalOp::Distinct,
            logical::PureLogicalOp::Limit {
                count: ir::StreamBoundPlan::Literal(3)
            },
        ]
    ));
}

#[test]
fn pure_pipeline_simplification_returns_noop_when_every_op_is_removed() {
    let rule = PurePipelineSimplificationRule::default();
    let storage = cost::StorageCostProfile::default();
    let expr = pipeline_expr(vec![logical::PureLogicalOp::NoOp, skip(0)]);

    let op = logical_pure_op(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(op, logical::PureLogicalOp::NoOp);
}

#[test]
fn pure_pipeline_simplification_collapses_any_empty_pipeline_to_pure_empty() {
    let rule = PurePipelineSimplificationRule::default();
    let storage = cost::StorageCostProfile::default();
    let expr = pipeline_expr(vec![
        logical::PureLogicalOp::Source {
            element: properties::ElementKind::Node,
        },
        logical::PureLogicalOp::Empty,
        logical::PureLogicalOp::Distinct,
        logical::PureLogicalOp::Project,
    ]);

    let op = logical_pure_op(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(op, logical::PureLogicalOp::Empty);
}

#[test]
fn pure_pipeline_simplification_declines_irreducible_and_non_pipeline_inputs() {
    let rule = PurePipelineSimplificationRule::default();
    let storage = cost::StorageCostProfile::default();
    let irreducible = pipeline_expr(vec![
        logical::PureLogicalOp::Source {
            element: properties::ElementKind::Node,
        },
        logical::PureLogicalOp::Distinct,
        limit(3),
    ]);

    for expr in [
        irreducible,
        source(properties::ElementKind::Node),
        logical::LogicalExpr::Barrier(logical::BarrierLogicalOp::Mutation),
    ] {
        assert_eq!(
            rule.apply(optimizer::RuleInput {
                expr: &expr,
                storage: &storage,
                indexes: empty_indexes(),
                planner_limits: default_planner_limits(),
                stats: default_stats(),
            }),
            optimizer::RuleResult::NotApplicable
        );
    }
}
