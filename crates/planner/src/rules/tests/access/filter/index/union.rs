use super::*;

#[test]
fn access_filter_index_rule_rewrites_or_and_literal_in_to_unions() {
    let rule = AccessFilterIndexRule::default();
    let storage = cost::StorageCostProfile::default();
    let node_age_key = catalog::ScopedPropertyKey::try_new("User", "age").unwrap();
    let edge_weight_key = catalog::ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap();
    let indexes = catalog::IndexCatalogSnapshot::default()
        .with_node_eq(node_age_key.clone())
        .with_edge_eq(edge_weight_key.clone());
    let node_expr = node_access_filter_expr(
        ir::NodeAccessPlan::LabelScan {
            label: name("User"),
        },
        ir::PredicatePlan::new(helix_ast::expr::Predicate::or(vec![
            helix_ast::expr::Predicate::eq("age", 21),
            helix_ast::expr::Predicate::eq("age", 42),
        ]))
        .unwrap(),
    );
    let edge_expr = edge_access_filter_expr(
        ir::EdgeAccessPlan::LabelScan {
            label: name("FOLLOWS"),
        },
        ir::PredicatePlan::new(helix_ast::expr::Predicate::is_in(
            "weight",
            helix_ast::value::PropertyValue::I64Array(vec![7, 9]),
        ))
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
    let ir::NodeAccessPlan::Union(node_children) = node.source().as_ref() else {
        panic!("expected node union");
    };
    assert!(matches!(
        node_children.as_ref()[0].as_ref(),
        ir::NodeAccessPlan::EqualityIndex { key, value, .. }
            if key == &node_age_key && *value == equality_literal(21)
    ));
    assert!(matches!(
        node_children.as_ref()[1].as_ref(),
        ir::NodeAccessPlan::EqualityIndex { key, value, .. }
            if key == &node_age_key && *value == equality_literal(42)
    ));

    let logical::AccessPath::Edge(edge) = edge else {
        panic!("expected edge access");
    };
    let ir::EdgeAccessPlan::Union(edge_children) = edge.source().as_ref() else {
        panic!("expected edge union");
    };
    assert!(matches!(
        edge_children.as_ref()[0].as_ref(),
        ir::EdgeAccessPlan::EqualityIndex { key, value, .. }
            if key == &edge_weight_key && *value == equality_literal(7)
    ));
    assert!(matches!(
        edge_children.as_ref()[1].as_ref(),
        ir::EdgeAccessPlan::EqualityIndex { key, value, .. }
            if key == &edge_weight_key && *value == equality_literal(9)
    ));
}
