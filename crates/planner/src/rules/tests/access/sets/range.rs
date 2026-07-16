use super::*;

#[test]
fn access_range_rule_merges_same_key_node_ranges_and_preserves_order() {
    let rule = AccessRangeIntersectionRule::default();
    let storage = cost::StorageCostProfile::default();
    let lower = node_range_source("User", "age", lower_range(18));
    let label = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::LabelScan {
        label: name("User"),
    })
    .unwrap();
    let upper = node_range_source("User", "age", upper_range(65));
    let expected_range = lower_range(18).intersect(&upper_range(65)).unwrap();
    let expr = node_access_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::try_from_vec(vec![lower, label, upper]).unwrap(),
    ));

    let rewritten = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "access_range_intersection");
    let logical::AccessPath::Node(path) = rewritten else {
        panic!("expected node access path");
    };
    let ir::NodeAccessPlan::Intersect(plans) = path.source().as_ref() else {
        panic!("expected node intersection");
    };
    assert_eq!(plans.len(), 2);
    assert!(matches!(
        plans.as_ref(),
        [
            node_range,
            label_scan,
        ] if matches!(
            node_range.as_ref(),
            ir::NodeAccessPlan::RangeIndex { range, .. } if range == &expected_range
        ) && matches!(
            label_scan.as_ref(),
            ir::NodeAccessPlan::LabelScan { label } if label.as_ref() == "User"
        )
    ));
}

#[test]
fn access_range_rule_collapses_two_edge_ranges_to_single_range_source() {
    let rule = AccessRangeIntersectionRule::default();
    let storage = cost::StorageCostProfile::default();
    let expected_range = lower_range(3).intersect(&upper_range(9)).unwrap();
    let expr = edge_access_expr(ir::EdgeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            edge_range_source("LIKES", "weight", lower_range(3)),
            edge_range_source("LIKES", "weight", upper_range(9)),
        ),
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
                ir::EdgeAccessPlan::RangeIndex { range, .. } if range == &expected_range
            )
    ));
}

#[test]
fn access_range_rule_collapses_two_node_ranges_to_single_range_source() {
    let rule = AccessRangeIntersectionRule::default();
    let storage = cost::StorageCostProfile::default();
    let expected_range = lower_range(3).intersect(&upper_range(9)).unwrap();
    let expr = node_access_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            node_range_source("User", "age", lower_range(3)),
            node_range_source("User", "age", upper_range(9)),
        ),
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
                ir::NodeAccessPlan::RangeIndex { range, .. } if range == &expected_range
            )
    ));
}

#[test]
fn access_range_rule_merges_same_key_edge_ranges_and_preserves_order() {
    let rule = AccessRangeIntersectionRule::default();
    let storage = cost::StorageCostProfile::default();
    let lower = edge_range_source("LIKES", "weight", lower_range(18));
    let label = ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::LabelScan {
        label: name("LIKES"),
    })
    .unwrap();
    let upper = edge_range_source("LIKES", "weight", upper_range(65));
    let expected_range = lower_range(18).intersect(&upper_range(65)).unwrap();
    let expr = edge_access_expr(ir::EdgeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::try_from_vec(vec![lower, label, upper]).unwrap(),
    ));

    let rewritten = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    let logical::AccessPath::Edge(path) = rewritten else {
        panic!("expected edge access path");
    };
    let ir::EdgeAccessPlan::Intersect(plans) = path.source().as_ref() else {
        panic!("expected edge intersection");
    };
    assert_eq!(plans.len(), 2);
    assert!(matches!(
        plans.as_ref(),
        [
            edge_range,
            label_scan,
        ] if matches!(
            edge_range.as_ref(),
            ir::EdgeAccessPlan::RangeIndex { range, .. } if range == &expected_range
        ) && matches!(
            label_scan.as_ref(),
            ir::EdgeAccessPlan::LabelScan { label } if label.as_ref() == "LIKES"
        )
    ));
}

#[test]
fn access_range_rule_declines_non_intersections_and_unprovable_range_merges() {
    let rule = AccessRangeIntersectionRule::default();
    let storage = cost::StorageCostProfile::default();
    let different_keys = node_access_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            node_range_source("User", "age", lower_range(18)),
            node_range_source("User", "score", lower_range(90)),
        ),
    ));
    let dynamic = node_access_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            node_range_source(
                "User",
                "age",
                ir::IndexRange::Lower {
                    lower: ir::IndexBound::Inclusive(ir::RangeIndexValue::param("min").unwrap()),
                },
            ),
            node_range_source(
                "User",
                "age",
                ir::IndexRange::Lower {
                    lower: ir::IndexBound::Inclusive(
                        ir::RangeIndexValue::param("other_min").unwrap(),
                    ),
                },
            ),
        ),
    ));

    for expr in [
        source(properties::ElementKind::Node),
        node_access_expr(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            node_range_source("User", "age", lower_range(18)),
            node_range_source("User", "age", upper_range(65)),
        ))),
        different_keys,
        dynamic,
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
