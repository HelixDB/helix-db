use super::*;

#[test]
fn access_window_rule_tightens_zero_based_literal_search_windows() {
    let rule = AccessWindowRule::default();
    let storage = cost::StorageCostProfile::default();
    let node = node_access_window_expr(
        node_vector_search(search_limit(10)),
        logical::AccessWindowRange::new(0, Some(3)).unwrap(),
    );
    let edge = edge_access_window_expr(
        edge_text_search(search_limit(8)),
        logical::AccessWindowRange::new(0, Some(2)).unwrap(),
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

    assert!(matches!(
        node,
        logical::AccessPath::Node(path)
            if matches!(
                path.source().as_ref(),
                ir::NodeAccessPlan::VectorSearch {
                    k: ir::SearchLimitPlan::Literal(k),
                    ..
                } if k.get() == 3
            )
    ));
    assert!(matches!(
        edge,
        logical::AccessPath::Edge(path)
            if matches!(
                path.source().as_ref(),
                ir::EdgeAccessPlan::TextSearch {
                    k: ir::SearchLimitPlan::Literal(k),
                    ..
                } if k.get() == 2
            )
    ));
}

#[test]
fn access_window_rule_tightens_edge_vector_search_windows() {
    let rule = AccessWindowRule::default();
    let storage = cost::StorageCostProfile::default();
    let folded = edge_access_window_expr(
        edge_vector_search(search_limit(9)),
        logical::AccessWindowRange::new(0, Some(4)).unwrap(),
    );
    let prefix = edge_access_window_expr(
        edge_vector_search(search_limit(9)),
        logical::AccessWindowRange::new(2, Some(4)).unwrap(),
    );

    let folded = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &folded,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    assert!(matches!(
        folded,
        logical::AccessPath::Edge(path)
            if matches!(
                path.source().as_ref(),
                ir::EdgeAccessPlan::VectorSearch {
                    k: ir::SearchLimitPlan::Literal(k),
                    ..
                } if k.get() == 4
            )
    ));

    let prefix = logical_access_window(rule.apply(optimizer::RuleInput {
        expr: &prefix,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    assert_eq!(
        prefix.window(),
        logical::AccessWindowRange::new(2, Some(4)).unwrap()
    );
    assert!(matches!(
        prefix.access(),
        logical::AccessPath::Edge(path)
            if matches!(
                path.source().as_ref(),
                ir::EdgeAccessPlan::VectorSearch {
                    k: ir::SearchLimitPlan::Literal(k),
                    ..
                } if k.get() == 4
            )
    ));
}

#[test]
fn access_window_rule_tightens_edge_text_prefix() {
    let rule = AccessWindowRule::default();
    let storage = cost::StorageCostProfile::default();
    let prefix = edge_access_window_expr(
        edge_text_search(search_limit(8)),
        logical::AccessWindowRange::new(1, Some(4)).unwrap(),
    );

    let prefix = logical_access_window(rule.apply(optimizer::RuleInput {
        expr: &prefix,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(
        prefix.window(),
        logical::AccessWindowRange::new(1, Some(4)).unwrap()
    );
    assert!(matches!(
        prefix.access(),
        logical::AccessPath::Edge(path)
            if matches!(
                path.source().as_ref(),
                ir::EdgeAccessPlan::TextSearch {
                    k: ir::SearchLimitPlan::Literal(k),
                    ..
                } if k.get() == 4
            )
    ));
}

#[test]
fn access_window_rule_tightens_search_prefixes_that_need_a_remaining_window() {
    let rule = AccessWindowRule::default();
    let storage = cost::StorageCostProfile::default();
    let dynamic_k = ir::SearchLimitPlan::Expr(
        ir::SearchLimitExprPlan::new(helix_ast::expr::Expr::param("k")).unwrap(),
    );
    let skipped = node_access_window_expr(
        node_vector_search(search_limit(10)),
        logical::AccessWindowRange::new(1, Some(3)).unwrap(),
    );
    let open_ended = node_access_window_expr(
        node_vector_search(search_limit(10)),
        logical::AccessWindowRange::new(0, None).unwrap(),
    );
    let noop = node_access_window_expr(
        node_vector_search(search_limit(3)),
        logical::AccessWindowRange::new(0, Some(5)).unwrap(),
    );
    let runtime = node_access_window_expr(
        node_vector_search(dynamic_k),
        logical::AccessWindowRange::new(0, Some(2)).unwrap(),
    );

    let skipped = logical_access_window(rule.apply(optimizer::RuleInput {
        expr: &skipped,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    assert_eq!(
        skipped.window(),
        logical::AccessWindowRange::new(1, Some(3)).unwrap()
    );
    assert!(matches!(
        skipped.access(),
        logical::AccessPath::Node(path)
            if matches!(
                path.source().as_ref(),
                ir::NodeAccessPlan::VectorSearch {
                    k: ir::SearchLimitPlan::Literal(k),
                    ..
                } if k.get() == 3
            )
    ));

    let open_ended = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &open_ended,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    assert!(matches!(
        open_ended,
        logical::AccessPath::Node(path)
            if matches!(
                path.source().as_ref(),
                ir::NodeAccessPlan::VectorSearch {
                    k: ir::SearchLimitPlan::Literal(k),
                    ..
                } if k.get() == 10
            )
    ));

    let noop = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &noop,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    assert!(matches!(
        noop,
        logical::AccessPath::Node(path)
            if matches!(
                path.source().as_ref(),
                ir::NodeAccessPlan::VectorSearch {
                    k: ir::SearchLimitPlan::Literal(k),
                    ..
                } if k.get() == 3
            )
    ));

    assert_eq!(
        rule.apply(optimizer::RuleInput {
            expr: &runtime,
            storage: &storage,
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        }),
        optimizer::RuleResult::NotApplicable
    );
}
