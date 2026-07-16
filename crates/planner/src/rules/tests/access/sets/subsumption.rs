use super::*;

#[test]
fn access_subsumption_rule_removes_wider_node_intersection_sources() {
    let rule = AccessSubsumptionRule::default();
    let storage = cost::StorageCostProfile::default();
    let label = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::LabelScan {
        label: name("User"),
    })
    .unwrap();
    let expr = node_access_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::try_from_vec(vec![
            label,
            node_range_source("User", "age", lower_range(21)),
            node_eq_source("User", "age", equality_literal(30)),
        ])
        .unwrap(),
    ));

    let rewritten = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "access_subsumption");
    assert!(matches!(
        rewritten,
        logical::AccessPath::Node(path)
            if matches!(
                path.source().as_ref(),
                ir::NodeAccessPlan::EqualityIndex { value, .. }
                    if value == &equality_literal(30)
            )
    ));
}

#[test]
fn access_subsumption_rule_removes_subset_node_union_sources() {
    let rule = AccessSubsumptionRule::default();
    let storage = cost::StorageCostProfile::default();
    let expr = node_access_expr(ir::NodeAccessPlan::Union(
        ir::AtLeast::<_, 2>::try_from_vec(vec![
            node_range_source("User", "age", lower_range(21)),
            node_eq_source("User", "age", equality_literal(30)),
            node_range_source("User", "age", lower_range(30)),
        ])
        .unwrap(),
    ));

    let rewritten = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert!(matches!(
        rewritten,
        logical::AccessPath::Node(path)
            if matches!(
                path.source().as_ref(),
                ir::NodeAccessPlan::RangeIndex { range, .. } if range == &lower_range(21)
            )
    ));
}

#[test]
fn access_subsumption_rule_removes_subset_edge_union_sources() {
    let rule = AccessSubsumptionRule::default();
    let storage = cost::StorageCostProfile::default();
    let expr = edge_access_expr(ir::EdgeAccessPlan::Union(
        ir::AtLeast::<_, 2>::try_from_vec(vec![
            edge_range_source("FOLLOWS", "weight", lower_range(21)),
            edge_eq_source("FOLLOWS", "weight", equality_literal(30)),
            edge_range_source("FOLLOWS", "weight", lower_range(30)),
        ])
        .unwrap(),
    ));

    let rewritten = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert!(matches!(
        rewritten,
        logical::AccessPath::Edge(path)
            if matches!(
                path.source().as_ref(),
                ir::EdgeAccessPlan::RangeIndex { range, .. } if range == &lower_range(21)
            )
    ));
}

#[test]
fn access_subsumption_rule_removes_wider_edge_intersection_sources() {
    let rule = AccessSubsumptionRule::default();
    let storage = cost::StorageCostProfile::default();
    let label = ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::LabelScan {
        label: name("FOLLOWS"),
    })
    .unwrap();
    let expr = edge_access_expr(ir::EdgeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::try_from_vec(vec![
            label,
            edge_range_source("FOLLOWS", "weight", lower_range(21)),
            edge_eq_source("FOLLOWS", "weight", equality_literal(30)),
        ])
        .unwrap(),
    ));

    let rewritten = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert!(matches!(
        rewritten,
        logical::AccessPath::Edge(path)
            if matches!(
                path.source().as_ref(),
                ir::EdgeAccessPlan::EqualityIndex { value, .. }
                    if value == &equality_literal(30)
            )
    ));
}

#[test]
fn access_subsumption_rule_covers_all_scan_nested_sets_and_equivalent_ties() {
    let rule = AccessSubsumptionRule::default();
    let storage = cost::StorageCostProfile::default();
    let all_scan = node_access_expr(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::AllScan).unwrap(),
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::LabelScan {
            label: name("User"),
        })
        .unwrap(),
    )));
    let duplicate_ranges =
        node_access_expr(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            node_range_source("User", "age", lower_range(21)),
            node_range_source("User", "age", lower_range(21)),
        )));
    let nested_union =
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            node_eq_source("User", "age", equality_literal(30)),
            node_eq_source("User", "age", equality_literal(40)),
        )))
        .unwrap();
    let range_over_nested_union =
        node_access_expr(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            node_range_source("User", "age", lower_range(21)),
            nested_union,
        )));
    let nested_intersection = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            node_range_source("User", "age", lower_range(21)),
            node_range_source("User", "age", upper_range(50)),
        ),
    ))
    .unwrap();
    let intersection_over_equality =
        node_access_expr(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            nested_intersection,
            node_eq_source("User", "age", equality_literal(30)),
        )));

    let all_scan = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &all_scan,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let duplicate_ranges = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &duplicate_ranges,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let range_over_nested_union = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &range_over_nested_union,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let intersection_over_equality = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &intersection_over_equality,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert!(matches!(
        all_scan,
        logical::AccessPath::Node(path)
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::AllScan)
    ));
    assert!(matches!(
        duplicate_ranges,
        logical::AccessPath::Node(path)
            if matches!(
                path.source().as_ref(),
                ir::NodeAccessPlan::RangeIndex { range, .. } if range == &lower_range(21)
            )
    ));
    assert!(matches!(
        range_over_nested_union,
        logical::AccessPath::Node(path)
            if matches!(
                path.source().as_ref(),
                ir::NodeAccessPlan::RangeIndex { range, .. } if range == &lower_range(21)
            )
    ));
    assert!(matches!(
        intersection_over_equality,
        logical::AccessPath::Node(path)
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::Intersect(plans) if plans.len() == 2)
    ));
}

#[test]
fn access_subsumption_rule_covers_edge_all_scan_nested_sets_and_equivalent_ties() {
    let rule = AccessSubsumptionRule::default();
    let storage = cost::StorageCostProfile::default();
    let all_scan = edge_access_expr(ir::EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
        ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::AllScan).unwrap(),
        ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::LabelScan {
            label: name("FOLLOWS"),
        })
        .unwrap(),
    )));
    let duplicate_ranges =
        edge_access_expr(ir::EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            edge_range_source("FOLLOWS", "weight", lower_range(21)),
            edge_range_source("FOLLOWS", "weight", lower_range(21)),
        )));
    let nested_union =
        ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            edge_eq_source("FOLLOWS", "weight", equality_literal(30)),
            edge_eq_source("FOLLOWS", "weight", equality_literal(40)),
        )))
        .unwrap();
    let range_over_nested_union =
        edge_access_expr(ir::EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            edge_range_source("FOLLOWS", "weight", lower_range(21)),
            nested_union,
        )));
    let nested_intersection = ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            edge_range_source("FOLLOWS", "weight", lower_range(21)),
            edge_range_source("FOLLOWS", "weight", upper_range(50)),
        ),
    ))
    .unwrap();
    let intersection_over_equality =
        edge_access_expr(ir::EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            nested_intersection,
            edge_eq_source("FOLLOWS", "weight", equality_literal(30)),
        )));

    let all_scan = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &all_scan,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let duplicate_ranges = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &duplicate_ranges,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let range_over_nested_union = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &range_over_nested_union,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let intersection_over_equality = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &intersection_over_equality,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert!(matches!(
        all_scan,
        logical::AccessPath::Edge(path)
            if matches!(path.source().as_ref(), ir::EdgeAccessPlan::AllScan)
    ));
    assert!(matches!(
        duplicate_ranges,
        logical::AccessPath::Edge(path)
            if matches!(
                path.source().as_ref(),
                ir::EdgeAccessPlan::RangeIndex { range, .. } if range == &lower_range(21)
            )
    ));
    assert!(matches!(
        range_over_nested_union,
        logical::AccessPath::Edge(path)
            if matches!(
                path.source().as_ref(),
                ir::EdgeAccessPlan::RangeIndex { range, .. } if range == &lower_range(21)
            )
    ));
    assert!(matches!(
        intersection_over_equality,
        logical::AccessPath::Edge(path)
            if matches!(path.source().as_ref(), ir::EdgeAccessPlan::Intersect(plans) if plans.len() == 2)
    ));
}

#[test]
fn access_subsumption_rule_declines_runtime_unrelated_and_non_set_sources() {
    let rule = AccessSubsumptionRule::default();
    let storage = cost::StorageCostProfile::default();
    let runtime_value =
        node_access_expr(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            node_range_source("User", "age", lower_range(21)),
            node_eq_source("User", "age", ir::IndexValue::Param(name("age"))),
        )));
    let unrelated_labels = node_access_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::LabelScan {
                label: name("User"),
            })
            .unwrap(),
            node_eq_source("Account", "age", equality_literal(30)),
        ),
    ));
    let unrelated_ranges =
        edge_access_expr(ir::EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            edge_range_source("FOLLOWS", "weight", lower_range(21)),
            edge_range_source("FOLLOWS", "score", lower_range(30)),
        )));

    for expr in [
        source(properties::ElementKind::Node),
        node_access_expr(ir::NodeAccessPlan::AllScan),
        runtime_value,
        unrelated_labels,
        unrelated_ranges,
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
