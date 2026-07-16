use super::*;

#[test]
fn access_window_rule_collapses_empty_windows_for_any_access_kind() {
    let rule = AccessWindowRule::default();
    let storage = cost::StorageCostProfile::default();
    let node = node_access_window_expr(
        ir::NodeAccessPlan::PointIds {
            ids: element_ids(vec![7]),
        },
        logical::AccessWindowRange::new(4, Some(4)).unwrap(),
    );
    let edge = edge_access_window_expr(
        ir::EdgeAccessPlan::LabelScan {
            label: name("LIKES"),
        },
        logical::AccessWindowRange::new(2, Some(2)).unwrap(),
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
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::Empty)
    ));
    assert!(matches!(
        edge,
        logical::AccessPath::Edge(path)
            if matches!(path.source().as_ref(), ir::EdgeAccessPlan::Empty)
    ));
}
