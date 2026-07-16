use super::*;

#[test]
fn access_filter_index_rule_rewrites_covered_conjunctions_to_intersections() {
    let rule = AccessFilterIndexRule::default();
    let storage = cost::StorageCostProfile::default();
    let node_age_key = range_key("User", "age", helix_ast::index::RangeIndexDirection::Asc);
    let node_score_key = catalog::ScopedPropertyKey::try_new("User", "score").unwrap();
    let edge_since_key = range_key(
        "FOLLOWS",
        "since",
        helix_ast::index::RangeIndexDirection::Asc,
    );
    let edge_weight_key = catalog::ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap();
    let indexes = catalog::IndexCatalogSnapshot::default()
        .with_node_range(node_age_key.clone())
        .with_node_eq(node_score_key.clone())
        .with_edge_range(edge_since_key.clone())
        .with_edge_eq(edge_weight_key.clone());
    let node_expr = node_access_filter_expr(
        ir::NodeAccessPlan::LabelScan {
            label: name("User"),
        },
        ir::PredicatePlan::new(helix_ast::expr::Predicate::and(vec![
            helix_ast::expr::Predicate::gte("age", 21),
            helix_ast::expr::Predicate::eq("score", 90),
        ]))
        .unwrap(),
    );
    let edge_expr = edge_access_filter_expr(
        ir::EdgeAccessPlan::LabelScan {
            label: name("FOLLOWS"),
        },
        ir::PredicatePlan::new(helix_ast::expr::Predicate::and(vec![
            helix_ast::expr::Predicate::eq("weight", 7),
            helix_ast::expr::Predicate::gte("since", 2020),
        ]))
        .unwrap(),
    );

    let node = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &node_expr,
        storage: &storage,
        indexes: &indexes,
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let edge = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &edge_expr,
        storage: &storage,
        indexes: &indexes,
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    let logical::AccessPath::Node(node) = node else {
        panic!("expected node access");
    };
    let ir::NodeAccessPlan::Intersect(node_children) = node.source().as_ref() else {
        panic!("expected node intersection");
    };
    assert!(matches!(
        node_children.as_ref()[0].as_ref(),
        ir::NodeAccessPlan::RangeIndex { key, range, .. }
            if key == &node_age_key && *range == lower_range(21)
    ));
    assert!(matches!(
        node_children.as_ref()[1].as_ref(),
        ir::NodeAccessPlan::EqualityIndex { key, value, .. }
            if key == &node_score_key && *value == equality_literal(90)
    ));

    let logical::AccessPath::Edge(edge) = edge else {
        panic!("expected edge access");
    };
    let ir::EdgeAccessPlan::Intersect(edge_children) = edge.source().as_ref() else {
        panic!("expected edge intersection");
    };
    assert!(matches!(
        edge_children.as_ref()[0].as_ref(),
        ir::EdgeAccessPlan::EqualityIndex { key, value, .. }
            if key == &edge_weight_key && *value == equality_literal(7)
    ));
    assert!(matches!(
        edge_children.as_ref()[1].as_ref(),
        ir::EdgeAccessPlan::RangeIndex { key, range, .. }
            if key == &edge_since_key && *range == lower_range(2020)
    ));
}
