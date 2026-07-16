use super::*;

#[test]
fn access_window_rule_declines_noop_or_unhandled_windows_and_non_candidates() {
    let rule = AccessWindowRule::default();
    let storage = cost::StorageCostProfile::default();
    let noop = node_access_window_expr(
        ir::NodeAccessPlan::PointIds {
            ids: element_ids(vec![10, 20]),
        },
        logical::AccessWindowRange::new(0, None).unwrap(),
    );
    let label_window = node_access_window_expr(
        ir::NodeAccessPlan::LabelScan {
            label: name("User"),
        },
        logical::AccessWindowRange::new(1, Some(2)).unwrap(),
    );

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
                ir::NodeAccessPlan::PointIds { ids } if ids.as_ref() == [10, 20]
            )
    ));

    for expr in [
        source(properties::ElementKind::Node),
        node_access_expr(ir::NodeAccessPlan::AllScan),
        label_window,
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
