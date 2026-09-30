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

/// Apply the access-filter index rule to `expr` under `indexes`.
fn apply_index_rule(
    expr: &logical::LogicalExpr,
    indexes: &catalog::IndexCatalogSnapshot,
) -> optimizer::RuleResult {
    AccessFilterIndexRule::default().apply(optimizer::RuleInput {
        expr,
        storage: &cost::StorageCostProfile::default(),
        indexes,
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    })
}

fn point_range(value: i64) -> ir::IndexRange {
    ir::IndexRange::Between(
        ir::IndexBetweenRange::new(
            ir::IndexBound::Inclusive(range_literal(value)),
            ir::IndexBound::Inclusive(range_literal(value)),
        )
        .unwrap(),
    )
}

#[test]
fn equality_on_range_only_property_uses_point_range() {
    // The node index is ascending and the edge index descending: either
    // direction answers a point range.
    let node_key = range_key("User", "age", helix_ast::index::RangeIndexDirection::Asc);
    let edge_key = range_key(
        "LIKES",
        "weight",
        helix_ast::index::RangeIndexDirection::Desc,
    );
    let indexes = catalog::IndexCatalogSnapshot::default()
        .with_node_range(node_key.clone())
        .with_edge_range(edge_key.clone());
    let one = |property: &str| {
        ir::PredicatePlan::new(helix_ast::expr::Predicate::eq(property, 7)).unwrap()
    };
    let many = |property: &str| {
        ir::PredicatePlan::new(helix_ast::expr::Predicate::is_in(
            property,
            helix_ast::value::PropertyValue::I64Array(vec![7, 9]),
        ))
        .unwrap()
    };

    let node_scan = || ir::NodeAccessPlan::LabelScan {
        label: name("User"),
    };
    let node = logical_access_path(apply_index_rule(
        &node_access_filter_expr(node_scan(), one("age")),
        &indexes,
    ));
    assert!(matches!(
        node,
        logical::AccessPath::Node(path) if matches!(
            path.source().as_ref(),
            ir::NodeAccessPlan::RangeIndex { key, range, .. }
                if key == &node_key && *range == point_range(7)
        )
    ));
    let node = logical_access_path(apply_index_rule(
        &node_access_filter_expr(node_scan(), many("age")),
        &indexes,
    ));
    let logical::AccessPath::Node(path) = node else {
        panic!("expected a node access path");
    };
    let ir::NodeAccessPlan::Union(children) = path.source().as_ref() else {
        panic!("expected a union of point ranges: {path:?}");
    };
    assert_eq!(
        children
            .iter()
            .map(|child| match child.as_ref() {
                ir::NodeAccessPlan::RangeIndex { key, range, .. } if key == &node_key => {
                    range.clone()
                }
                other => panic!("expected a point range: {other:?}"),
            })
            .collect::<Vec<_>>(),
        [point_range(7), point_range(9)]
    );

    let edge_scan = || ir::EdgeAccessPlan::LabelScan {
        label: name("LIKES"),
    };
    let edge = logical_access_path(apply_index_rule(
        &edge_access_filter_expr(edge_scan(), one("weight")),
        &indexes,
    ));
    assert!(matches!(
        edge,
        logical::AccessPath::Edge(path) if matches!(
            path.source().as_ref(),
            ir::EdgeAccessPlan::RangeIndex { key, range, .. }
                if key == &edge_key && *range == point_range(7)
        )
    ));
    let edge = logical_access_path(apply_index_rule(
        &edge_access_filter_expr(edge_scan(), many("weight")),
        &indexes,
    ));
    let logical::AccessPath::Edge(path) = edge else {
        panic!("expected an edge access path");
    };
    let ir::EdgeAccessPlan::Union(children) = path.source().as_ref() else {
        panic!("expected a union of point ranges: {path:?}");
    };
    assert_eq!(
        children
            .iter()
            .map(|child| match child.as_ref() {
                ir::EdgeAccessPlan::RangeIndex { key, range, .. } if key == &edge_key => {
                    range.clone()
                }
                other => panic!("expected a point range: {other:?}"),
            })
            .collect::<Vec<_>>(),
        [point_range(7), point_range(9)]
    );
}

#[test]
fn null_param_or_bool_equality_on_range_only_property_stays_residual() {
    // A range lane holds no null entries, so a null literal or a parameter
    // that may bind null cannot be answered by a point range; bool and bytes
    // are not range-orderable, and a disjunction is answered only when every
    // branch is.
    let indexes = catalog::IndexCatalogSnapshot::default()
        .with_node_range(range_key(
            "User",
            "age",
            helix_ast::index::RangeIndexDirection::Asc,
        ))
        .with_edge_range(range_key(
            "LIKES",
            "age",
            helix_ast::index::RangeIndexDirection::Asc,
        ));
    for predicate in [
        helix_ast::expr::Predicate::eq("age", helix_ast::value::PropertyValue::Null),
        helix_ast::expr::Predicate::eq_param("age", "age"),
        helix_ast::expr::Predicate::eq("age", true),
        helix_ast::expr::Predicate::eq("age", helix_ast::value::PropertyValue::Bytes(vec![7])),
        helix_ast::expr::Predicate::is_in_param("age", "ages"),
        helix_ast::expr::Predicate::or(vec![
            helix_ast::expr::Predicate::eq("age", 7),
            helix_ast::expr::Predicate::eq("age", helix_ast::value::PropertyValue::Null),
        ]),
    ] {
        let predicate = ir::PredicatePlan::new(predicate).unwrap();
        for expr in [
            node_access_filter_expr(
                ir::NodeAccessPlan::LabelScan {
                    label: name("User"),
                },
                predicate.clone(),
            ),
            edge_access_filter_expr(
                ir::EdgeAccessPlan::LabelScan {
                    label: name("LIKES"),
                },
                predicate.clone(),
            ),
        ] {
            assert_eq!(
                apply_index_rule(&expr, &indexes),
                optimizer::RuleResult::NotApplicable,
                "{predicate:?}"
            );
        }
    }
}
