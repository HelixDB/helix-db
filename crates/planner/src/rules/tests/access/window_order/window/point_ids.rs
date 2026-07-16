use super::*;

#[test]
fn access_window_rule_slices_node_point_ids_preserving_order() {
    let rule = AccessWindowRule::default();
    let storage = cost::StorageCostProfile::default();
    let expr = node_access_window_expr(
        ir::NodeAccessPlan::PointIds {
            ids: element_ids(vec![10, 20, 30, 40]),
        },
        logical::AccessWindowRange::new(1, Some(3)).unwrap(),
    );

    let rewritten = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "access_window");
    assert!(matches!(
        rewritten,
        logical::AccessPath::Node(path)
            if matches!(
                path.source().as_ref(),
                ir::NodeAccessPlan::PointIds { ids } if ids.as_ref() == [20, 30]
            )
    ));
}

#[test]
fn access_window_rule_slices_edge_point_ids_and_collapses_out_of_range_points() {
    let rule = AccessWindowRule::default();
    let storage = cost::StorageCostProfile::default();
    let edge_slice = edge_access_window_expr(
        ir::EdgeAccessPlan::PointIds {
            ids: element_ids(vec![10, 20, 30]),
        },
        logical::AccessWindowRange::new(1, None).unwrap(),
    );
    let node_empty = node_access_window_expr(
        ir::NodeAccessPlan::PointIds {
            ids: element_ids(vec![7]),
        },
        logical::AccessWindowRange::new(3, Some(6)).unwrap(),
    );
    let edge_empty = edge_access_window_expr(
        ir::EdgeAccessPlan::PointIds {
            ids: element_ids(vec![8]),
        },
        logical::AccessWindowRange::new(2, Some(4)).unwrap(),
    );

    let edge_slice = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &edge_slice,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let node_empty = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &node_empty,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let edge_empty = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &edge_empty,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert!(matches!(
        edge_slice,
        logical::AccessPath::Edge(path)
            if matches!(
                path.source().as_ref(),
                ir::EdgeAccessPlan::PointIds { ids } if ids.as_ref() == [20, 30]
            )
    ));
    assert!(matches!(
        node_empty,
        logical::AccessPath::Node(path)
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::Empty)
    ));
    assert!(matches!(
        edge_empty,
        logical::AccessPath::Edge(path)
            if matches!(path.source().as_ref(), ir::EdgeAccessPlan::Empty)
    ));
}

#[test]
fn access_window_rule_preserves_edge_identity_window() {
    let rule = AccessWindowRule::default();
    let storage = cost::StorageCostProfile::default();
    let identity = edge_access_window_expr(
        ir::EdgeAccessPlan::PointIds {
            ids: element_ids(vec![30, 40]),
        },
        logical::AccessWindowRange::new(0, None).unwrap(),
    );

    let identity = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &identity,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert!(matches!(
        identity,
        logical::AccessPath::Edge(path)
            if matches!(
                path.source().as_ref(),
                ir::EdgeAccessPlan::PointIds { ids } if ids.as_ref() == [30, 40]
            )
    ));
}
