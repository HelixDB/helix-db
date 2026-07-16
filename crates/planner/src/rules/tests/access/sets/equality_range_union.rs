use super::*;

#[test]
fn access_equality_range_union_rule_drops_covered_node_values_and_preserves_order() {
    let rule = AccessEqualityRangeUnionRule::default();
    let storage = cost::StorageCostProfile::default();
    let expr = node_access_expr(ir::NodeAccessPlan::Union(
        ir::AtLeast::<_, 2>::try_from_vec(vec![
            node_eq_source("User", "age", equality_literal(10)),
            node_eq_source("User", "age", equality_literal(30)),
            node_range_source("User", "age", lower_range(21)),
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

    assert_eq!(rule.metadata().id.as_ref(), "access_equality_range_union");
    let logical::AccessPath::Node(path) = rewritten else {
        panic!("expected node access path");
    };
    let ir::NodeAccessPlan::Union(plans) = path.source().as_ref() else {
        panic!("expected reduced node union");
    };
    assert_eq!(plans.len(), 2);
    assert!(matches!(
        plans.as_ref(),
        [
            equality,
            range,
        ] if matches!(
            equality.as_ref(),
            ir::NodeAccessPlan::EqualityIndex { value, .. }
                if value == &equality_literal(10)
        ) && matches!(
            range.as_ref(),
            ir::NodeAccessPlan::RangeIndex { range, .. }
                if range == &lower_range(21)
        )
    ));
}

#[test]
fn access_equality_range_union_rule_collapses_when_range_covers_all_edge_values() {
    let rule = AccessEqualityRangeUnionRule::default();
    let storage = cost::StorageCostProfile::default();
    let expr = edge_access_expr(ir::EdgeAccessPlan::Union(
        ir::AtLeast::<_, 2>::try_from_vec(vec![
            edge_eq_source("FOLLOWS", "weight", equality_literal(30)),
            edge_range_source("FOLLOWS", "weight", lower_range(21)),
            edge_eq_source("FOLLOWS", "weight", equality_literal(40)),
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
fn access_equality_range_union_rule_collapses_when_range_covers_all_node_values() {
    let rule = AccessEqualityRangeUnionRule::default();
    let storage = cost::StorageCostProfile::default();
    let expr = node_access_expr(ir::NodeAccessPlan::Union(
        ir::AtLeast::<_, 2>::try_from_vec(vec![
            node_eq_source("User", "age", equality_literal(30)),
            node_range_source("User", "age", lower_range(21)),
            node_eq_source("User", "age", equality_literal(40)),
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
fn access_equality_range_union_rule_drops_covered_edge_values_and_preserves_order() {
    let rule = AccessEqualityRangeUnionRule::default();
    let storage = cost::StorageCostProfile::default();
    let expr = edge_access_expr(ir::EdgeAccessPlan::Union(
        ir::AtLeast::<_, 2>::try_from_vec(vec![
            edge_eq_source("FOLLOWS", "weight", equality_literal(10)),
            edge_eq_source("FOLLOWS", "weight", equality_literal(30)),
            edge_range_source("FOLLOWS", "weight", lower_range(21)),
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

    let logical::AccessPath::Edge(path) = rewritten else {
        panic!("expected edge access path");
    };
    let ir::EdgeAccessPlan::Union(plans) = path.source().as_ref() else {
        panic!("expected reduced edge union");
    };
    assert_eq!(plans.len(), 2);
    assert!(matches!(
        plans.as_ref(),
        [
            equality,
            range,
        ] if matches!(
            equality.as_ref(),
            ir::EdgeAccessPlan::EqualityIndex { value, .. }
                if value == &equality_literal(10)
        ) && matches!(
            range.as_ref(),
            ir::EdgeAccessPlan::RangeIndex { range, .. }
                if range == &lower_range(21)
        )
    ));
}

#[test]
fn access_equality_range_union_rule_declines_uncovered_dynamic_and_non_union() {
    let rule = AccessEqualityRangeUnionRule::default();
    let storage = cost::StorageCostProfile::default();
    let uncovered = node_access_expr(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
        node_eq_source("User", "age", equality_literal(10)),
        node_range_source("User", "age", lower_range(21)),
    )));
    let dynamic_value =
        node_access_expr(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            node_eq_source("User", "age", ir::IndexValue::Param(name("age"))),
            node_range_source("User", "age", lower_range(21)),
        )));
    let dynamic_range =
        node_access_expr(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            node_eq_source("User", "age", equality_literal(30)),
            node_range_source(
                "User",
                "age",
                ir::IndexRange::Lower {
                    lower: ir::IndexBound::Inclusive(ir::RangeIndexValue::param("min").unwrap()),
                },
            ),
        )));
    let different_property =
        node_access_expr(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            node_eq_source("User", "score", equality_literal(30)),
            node_range_source("User", "age", lower_range(21)),
        )));

    for expr in [
        source(properties::ElementKind::Node),
        node_access_expr(ir::NodeAccessPlan::AllScan),
        uncovered,
        dynamic_value,
        dynamic_range,
        different_property,
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
