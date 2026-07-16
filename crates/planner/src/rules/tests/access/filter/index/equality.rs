use super::*;

#[test]
fn access_filter_index_rule_rewrites_catalog_backed_node_and_edge_equalities() {
    let rule = AccessFilterIndexRule::default();
    let storage = cost::StorageCostProfile::default();
    let node_key = catalog::ScopedPropertyKey::try_new("User", "active").unwrap();
    let edge_key = catalog::ScopedPropertyKey::try_new("LIKES", "weight").unwrap();
    let indexes = catalog::IndexCatalogSnapshot::default()
        .with_node_eq(node_key.clone())
        .with_edge_eq(edge_key.clone());
    let node_expr = node_access_filter_expr(
        ir::NodeAccessPlan::LabelScan {
            label: name("User"),
        },
        ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap(),
    );
    let edge_expr = edge_access_filter_expr(
        ir::EdgeAccessPlan::LabelScan {
            label: name("LIKES"),
        },
        ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("weight", 7)).unwrap(),
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

    assert_eq!(rule.metadata().id.as_ref(), "access_filter_index");
    assert!(matches!(
        node,
        logical::AccessPath::Node(path)
            if matches!(
                path.source().as_ref(),
                ir::NodeAccessPlan::EqualityIndex { key, value, .. }
                    if key == &node_key
                        && *value
                            == ir::IndexValue::Literal(
                                ir::SecondaryIndexLiteral::new(
                                    helix_ast::value::PropertyValue::from(true),
                                )
                                .unwrap(),
                            )
            )
    ));
    assert!(matches!(
        edge,
        logical::AccessPath::Edge(path)
            if matches!(
                path.source().as_ref(),
                ir::EdgeAccessPlan::EqualityIndex { key, value, .. }
                    if key == &edge_key && *value == equality_literal(7)
            )
    ));
}

#[test]
fn access_filter_index_rule_rewrites_all_scan_with_label_scoped_range() {
    let rule = AccessFilterIndexRule::default();
    let storage = cost::StorageCostProfile::default();
    let range_key = range_key("User", "age", helix_ast::index::RangeIndexDirection::Asc);
    let indexes = catalog::IndexCatalogSnapshot::default().with_node_range(range_key.clone());
    let predicate = ir::PredicatePlan::new(helix_ast::expr::Predicate::and(vec![
        helix_ast::expr::Predicate::eq("$label", "User"),
        helix_ast::expr::Predicate::gte("age", 21),
    ]))
    .unwrap();
    let expr = node_access_filter_expr(ir::NodeAccessPlan::AllScan, predicate);

    let access = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: &indexes,
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert!(matches!(
        access,
        logical::AccessPath::Node(path)
            if matches!(
                path.source().as_ref(),
                ir::NodeAccessPlan::RangeIndex { key, range, .. }
                    if key == &range_key && *range == lower_range(21)
            )
    ));
}
