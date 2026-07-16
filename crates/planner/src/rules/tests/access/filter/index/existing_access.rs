use super::*;

#[test]
fn access_filter_index_rule_intersects_existing_same_label_access() {
    let rule = AccessFilterIndexRule::default();
    let storage = cost::StorageCostProfile::default();
    let score_key = catalog::ScopedPropertyKey::try_new("User", "score").unwrap();
    let indexes = catalog::IndexCatalogSnapshot::default().with_node_eq(score_key.clone());
    let base = node_range_source("User", "age", lower_range(18));
    let expr = node_access_filter_expr(
        ir::NodeAccessPlan::from(base.clone()),
        ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("score", 90)).unwrap(),
    );

    let access = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: &indexes,
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    let logical::AccessPath::Node(path) = access else {
        panic!("expected node access path");
    };
    let ir::NodeAccessPlan::Intersect(children) = path.source().as_ref() else {
        panic!("expected intersection");
    };
    assert_eq!(children.as_ref()[0], base);
    assert!(matches!(
        children.as_ref()[1].as_ref(),
        ir::NodeAccessPlan::EqualityIndex { key, value, .. }
            if key == &score_key && *value == equality_literal(90)
    ));
}

#[test]
fn access_filter_index_rule_intersects_existing_same_label_edge_access() {
    let rule = AccessFilterIndexRule::default();
    let storage = cost::StorageCostProfile::default();
    let weight_key = catalog::ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap();
    let indexes = catalog::IndexCatalogSnapshot::default().with_edge_eq(weight_key.clone());
    let base = edge_range_source("FOLLOWS", "since", lower_range(2020));
    let expr = edge_access_filter_expr(
        ir::EdgeAccessPlan::from(base.clone()),
        ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("weight", 7)).unwrap(),
    );

    let access = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: &indexes,
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    let logical::AccessPath::Edge(path) = access else {
        panic!("expected edge access path");
    };
    let ir::EdgeAccessPlan::Intersect(children) = path.source().as_ref() else {
        panic!("expected intersection");
    };
    assert_eq!(children.as_ref()[0], base);
    assert!(matches!(
        children.as_ref()[1].as_ref(),
        ir::EdgeAccessPlan::EqualityIndex { key, value, .. }
            if key == &weight_key && *value == equality_literal(7)
    ));
}
