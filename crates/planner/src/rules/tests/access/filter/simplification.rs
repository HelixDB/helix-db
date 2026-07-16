use super::*;

#[test]
fn access_filter_rule_removes_tautologies_and_collapses_impossible_filters() {
    let rule = AccessFilterSimplificationRule::default();
    let storage = cost::StorageCostProfile::default();
    let tautology = ir::PredicatePlan::new(helix_ast::expr::Predicate::compare(
        helix_ast::expr::Expr::val(1),
        helix_ast::expr::CompareOp::Eq,
        helix_ast::expr::Expr::val(1),
    ))
    .unwrap();
    let impossible = ir::PredicatePlan::new(helix_ast::expr::Predicate::compare(
        helix_ast::expr::Expr::val(1),
        helix_ast::expr::CompareOp::Eq,
        helix_ast::expr::Expr::val(2),
    ))
    .unwrap();
    let node = node_access_filter_expr(ir::NodeAccessPlan::AllScan, tautology);
    let edge = edge_access_filter_expr(
        ir::EdgeAccessPlan::PointIds {
            ids: element_ids(vec![1]),
        },
        impossible,
    );
    let empty_node = node_access_filter_expr(
        ir::NodeAccessPlan::Empty,
        ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap(),
    );

    let node = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &node,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let edge = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &edge,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let empty_node = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &empty_node,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "access_filter_simplification");
    assert!(matches!(
        node,
        logical::AccessPath::Node(path)
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::AllScan)
    ));
    assert!(matches!(
        edge,
        logical::AccessPath::Edge(path)
            if matches!(path.source().as_ref(), ir::EdgeAccessPlan::Empty)
    ));
    assert!(matches!(
        empty_node,
        logical::AccessPath::Node(path)
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::Empty)
    ));
}

#[test]
fn access_filter_rule_uses_known_access_labels() {
    let rule = AccessFilterSimplificationRule::default();
    let storage = cost::StorageCostProfile::default();
    let matching_label =
        ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("$label", "User")).unwrap();
    let matching_or_residual = ir::PredicatePlan::new(helix_ast::expr::Predicate::or(vec![
        helix_ast::expr::Predicate::eq("$label", "User"),
        helix_ast::expr::Predicate::eq("active", true),
    ]))
    .unwrap();
    let edge_label =
        ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("$label", "LIKES")).unwrap();
    let conflicting_label = ir::PredicatePlan::new(helix_ast::expr::Predicate::and(vec![
        helix_ast::expr::Predicate::eq("$label", "Admin"),
        helix_ast::expr::Predicate::eq("active", true),
    ]))
    .unwrap();
    let matching = node_access_filter_expr(
        ir::NodeAccessPlan::LabelScan {
            label: name("User"),
        },
        matching_label,
    );
    let matching_or = node_access_filter_expr(
        ir::NodeAccessPlan::LabelScan {
            label: name("User"),
        },
        matching_or_residual,
    );
    let nested_edge = edge_access_filter_expr(
        ir::EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::LabelScan {
                label: name("LIKES"),
            })
            .unwrap(),
            ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::LabelScan {
                label: name("LIKES"),
            })
            .unwrap(),
        )),
        edge_label,
    );
    let conflicting = node_access_filter_expr(
        ir::NodeAccessPlan::LabelScan {
            label: name("User"),
        },
        conflicting_label,
    );

    let matching = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &matching,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let matching_or = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &matching_or,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let nested_edge = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &nested_edge,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let conflicting = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &conflicting,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert!(matches!(
        matching,
        logical::AccessPath::Node(path)
            if matches!(
                path.source().as_ref(),
                ir::NodeAccessPlan::LabelScan { label } if label.as_ref() == "User"
            )
    ));
    assert!(matches!(
        matching_or,
        logical::AccessPath::Node(path)
            if matches!(
                path.source().as_ref(),
                ir::NodeAccessPlan::LabelScan { label } if label.as_ref() == "User"
            )
    ));
    assert!(matches!(
        nested_edge,
        logical::AccessPath::Edge(path)
            if matches!(path.source().as_ref(), ir::EdgeAccessPlan::Union(_))
    ));
    assert!(matches!(
        conflicting,
        logical::AccessPath::Node(path)
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::Empty)
    ));
}

#[test]
fn access_filter_rule_preserves_residuals_and_unknown_label_scopes() {
    let rule = AccessFilterSimplificationRule::default();
    let storage = cost::StorageCostProfile::default();
    let label_and_residual = ir::PredicatePlan::new(helix_ast::expr::Predicate::and(vec![
        helix_ast::expr::Predicate::eq("$label", "User"),
        helix_ast::expr::Predicate::eq("active", true),
    ]))
    .unwrap();
    let mixed_label_union = ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::LabelScan {
            label: name("User"),
        })
        .unwrap(),
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::LabelScan {
            label: name("Admin"),
        })
        .unwrap(),
    ));

    for expr in [
        node_access_filter_expr(
            ir::NodeAccessPlan::LabelScan {
                label: name("User"),
            },
            label_and_residual.clone(),
        ),
        node_access_filter_expr(mixed_label_union, label_and_residual),
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
fn access_filter_rule_declines_invalid_label_scope_predicates() {
    let rule = AccessFilterSimplificationRule::default();
    let storage = cost::StorageCostProfile::default();
    let expr = node_access_filter_expr(
        ir::NodeAccessPlan::LabelScan {
            label: name("User"),
        },
        ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("$label", "")).unwrap(),
    );

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

#[test]
fn access_filter_rule_declines_feasible_residuals_and_non_candidates() {
    let rule = AccessFilterSimplificationRule::default();
    let storage = cost::StorageCostProfile::default();
    let feasible = node_access_filter_expr(
        ir::NodeAccessPlan::AllScan,
        ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap(),
    );

    for expr in [
        feasible,
        node_access_expr(ir::NodeAccessPlan::AllScan),
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
