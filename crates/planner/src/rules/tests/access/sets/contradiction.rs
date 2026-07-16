use super::*;

#[test]
fn access_contradiction_rule_collapses_excluded_node_equality_range_intersection() {
    let rule = AccessContradictionRule::default();
    let storage = cost::StorageCostProfile::default();
    let expr = node_access_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            node_eq_source("User", "age", equality_literal(10)),
            node_range_source("User", "age", lower_range(21)),
        ),
    ));

    let rewritten = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "access_contradiction");
    assert!(matches!(
        rewritten,
        logical::AccessPath::Node(path)
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::Empty)
    ));
}

#[test]
fn access_contradiction_rule_collapses_conflicting_node_equalities() {
    let rule = AccessContradictionRule::default();
    let storage = cost::StorageCostProfile::default();
    let expr = node_access_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            node_eq_source("User", "age", equality_literal(10)),
            node_eq_source("User", "age", equality_literal(20)),
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
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::Empty)
    ));
}

#[test]
fn access_contradiction_rule_collapses_conflicting_edge_equalities() {
    let rule = AccessContradictionRule::default();
    let storage = cost::StorageCostProfile::default();
    let expr = edge_access_expr(ir::EdgeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            edge_eq_source("FOLLOWS", "weight", equality_literal(10)),
            edge_eq_source("FOLLOWS", "weight", equality_literal(20)),
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
            if matches!(path.source().as_ref(), ir::EdgeAccessPlan::Empty)
    ));
}

#[test]
fn access_contradiction_rule_collapses_nested_edge_intersection_inputs() {
    let rule = AccessContradictionRule::default();
    let storage = cost::StorageCostProfile::default();
    let nested = ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            edge_eq_source("FOLLOWS", "weight", equality_literal(10)),
            edge_range_source("FOLLOWS", "weight", lower_range(21)),
        ),
    ))
    .unwrap();
    let expr = edge_access_expr(ir::EdgeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            nested,
            ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::AllScan).unwrap(),
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
            if matches!(path.source().as_ref(), ir::EdgeAccessPlan::Empty)
    ));
}

#[test]
fn access_contradiction_rule_collapses_excluded_edge_equality_range_intersection() {
    let rule = AccessContradictionRule::default();
    let storage = cost::StorageCostProfile::default();
    let expr = edge_access_expr(ir::EdgeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            edge_eq_source("FOLLOWS", "weight", equality_literal(10)),
            edge_range_source("FOLLOWS", "weight", lower_range(21)),
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
            if matches!(path.source().as_ref(), ir::EdgeAccessPlan::Empty)
    ));
}

#[test]
fn access_contradiction_rule_collapses_nested_union_when_every_branch_conflicts() {
    let rule = AccessContradictionRule::default();
    let storage = cost::StorageCostProfile::default();
    let union =
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            node_eq_source("User", "age", equality_literal(10)),
            node_eq_source("User", "age", equality_literal(20)),
        )))
        .unwrap();
    let expr = node_access_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(union, node_range_source("User", "age", lower_range(21))),
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
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::Empty)
    ));
}

#[test]
fn access_contradiction_rule_collapses_nested_static_empty_inputs() {
    let rule = AccessContradictionRule::default();
    let storage = cost::StorageCostProfile::default();
    let nested_empty = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Intersect(ir::AtLeast::<
        _,
        2,
    >::from_pair(
        node_eq_source("User", "age", equality_literal(10)),
        node_range_source("User", "age", lower_range(21)),
    )))
    .unwrap();
    let expr = node_access_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            nested_empty,
            ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::AllScan).unwrap(),
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
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::Empty)
    ));
}

#[test]
fn access_contradiction_rule_collapses_all_empty_union_inputs() {
    let rule = AccessContradictionRule::default();
    let storage = cost::StorageCostProfile::default();
    let empty_union =
        ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::Empty).unwrap(),
            ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::Empty).unwrap(),
        )))
        .unwrap();
    let expr = edge_access_expr(ir::EdgeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            empty_union,
            ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::AllScan).unwrap(),
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
            if matches!(path.source().as_ref(), ir::EdgeAccessPlan::Empty)
    ));
}

#[test]
fn access_contradiction_rule_declines_dynamic_partial_and_unrelated_sources() {
    let rule = AccessContradictionRule::default();
    let storage = cost::StorageCostProfile::default();
    let dynamic_value = node_access_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            node_eq_source("User", "age", ir::IndexValue::Param(name("age"))),
            node_range_source("User", "age", lower_range(21)),
        ),
    ));
    let dynamic_range = node_access_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            node_eq_source("User", "age", equality_literal(10)),
            node_range_source(
                "User",
                "age",
                ir::IndexRange::Lower {
                    lower: ir::IndexBound::Inclusive(ir::RangeIndexValue::param("min").unwrap()),
                },
            ),
        ),
    ));
    let unrelated_equalities = node_access_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            node_eq_source("User", "age", equality_literal(10)),
            node_eq_source("User", "score", equality_literal(20)),
        ),
    ));
    let partial_union =
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            node_eq_source("User", "age", equality_literal(10)),
            node_eq_source("User", "age", equality_literal(30)),
        )))
        .unwrap();
    let partial_union = node_access_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            partial_union,
            node_range_source("User", "age", lower_range(21)),
        ),
    ));

    for expr in [
        source(properties::ElementKind::Node),
        node_access_expr(ir::NodeAccessPlan::AllScan),
        dynamic_value,
        dynamic_range,
        unrelated_equalities,
        partial_union,
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
