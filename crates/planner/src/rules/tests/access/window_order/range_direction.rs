use super::*;

#[test]
fn access_order_range_direction_rule_rewrites_catalog_backed_node_and_edge_ranges() {
    let rule = AccessOrderRangeDirectionRule::default();
    let storage = cost::StorageCostProfile::default();
    let node_key = range_key("User", "age", helix_ast::index::RangeIndexDirection::Desc);
    let edge_key = range_key(
        "LIKES",
        "weight",
        helix_ast::index::RangeIndexDirection::Desc,
    );
    let indexes = catalog::IndexCatalogSnapshot::default()
        .with_node_range(node_key.clone())
        .with_edge_range(edge_key.clone());
    let node_range = lower_range(18);
    let edge_range = lower_range(3);
    let node_expr = node_access_order_expr(
        ir::NodeAccessPlan::from(node_range_source("User", "age", node_range.clone())),
        desc_order_keys(),
    );
    let edge_expr = edge_access_order_expr(
        ir::EdgeAccessPlan::from(edge_range_source("LIKES", "weight", edge_range.clone())),
        desc_weight_order_keys(),
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

    assert_eq!(rule.metadata().id.as_ref(), "access_order_range_direction");
    assert!(matches!(
        node,
        logical::AccessPath::Node(path)
            if matches!(
                path.source().as_ref(),
                ir::NodeAccessPlan::RangeIndex { key, index, range }
                    if key == &node_key
                        && index == &indexes.node_range[&node_key]
                        && range == &node_range
            )
    ));
    assert!(matches!(
        edge,
        logical::AccessPath::Edge(path)
            if matches!(
                path.source().as_ref(),
                ir::EdgeAccessPlan::RangeIndex { key, index, range }
                    if key == &edge_key
                        && index == &indexes.edge_range[&edge_key]
                        && range == &edge_range
            )
    ));
}

#[test]
fn access_order_range_direction_rule_declines_missing_or_invalid_candidates() {
    let rule = AccessOrderRangeDirectionRule::default();
    let storage = cost::StorageCostProfile::default();
    let indexes = catalog::IndexCatalogSnapshot::default().with_node_range(range_key(
        "User",
        "age",
        helix_ast::index::RangeIndexDirection::Desc,
    ));
    let missing_index = node_access_order_expr(
        ir::NodeAccessPlan::from(node_range_source("User", "age", lower_range(18))),
        desc_order_keys(),
    );
    let already_matching = node_access_order_expr(
        ir::NodeAccessPlan::from(node_range_source_with_direction(
            "User",
            "age",
            helix_ast::index::RangeIndexDirection::Desc,
            lower_range(18),
        )),
        desc_order_keys(),
    );
    let multi_key = node_access_order_expr(
        ir::NodeAccessPlan::from(node_range_source("User", "age", lower_range(18))),
        multi_order_keys(),
    );
    let mismatched_property = node_access_order_expr(
        ir::NodeAccessPlan::from(node_range_source("User", "score", lower_range(90))),
        desc_order_keys(),
    );
    let label_scan = node_access_order_expr(
        ir::NodeAccessPlan::LabelScan {
            label: name("User"),
        },
        desc_order_keys(),
    );

    assert_eq!(
        rule.apply(optimizer::RuleInput {
            expr: &missing_index,
            storage: &storage,
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        }),
        optimizer::RuleResult::NotApplicable
    );
    for expr in [
        already_matching,
        multi_key,
        mismatched_property,
        label_scan,
        source(properties::ElementKind::Node),
    ] {
        assert_eq!(
            rule.apply(optimizer::RuleInput {
                expr: &expr,
                storage: &storage,
                indexes: &indexes,
                planner_limits: default_planner_limits(),
                stats: default_stats(),
            }),
            optimizer::RuleResult::NotApplicable
        );
    }
}
