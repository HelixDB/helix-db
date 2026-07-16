use super::*;

#[test]
fn access_equality_range_rule_drops_excluded_node_union_values() {
    let rule = AccessEqualityRangeIntersectionRule::default();
    let storage = cost::StorageCostProfile::default();
    let union = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Union(
        ir::AtLeast::<_, 2>::try_from_vec(vec![
            node_eq_source("User", "age", equality_literal(10)),
            node_eq_source("User", "age", equality_literal(30)),
            node_eq_source("User", "age", equality_literal(40)),
        ])
        .unwrap(),
    ))
    .unwrap();
    let label = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::LabelScan {
        label: name("User"),
    })
    .unwrap();
    let expr = node_access_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::try_from_vec(vec![
            node_range_source("User", "age", lower_range(21)),
            label,
            union,
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

    assert_eq!(
        rule.metadata().id.as_ref(),
        "access_equality_range_intersection"
    );
    let logical::AccessPath::Node(path) = rewritten else {
        panic!("expected node access path");
    };
    let ir::NodeAccessPlan::Intersect(plans) = path.source().as_ref() else {
        panic!("expected node intersection");
    };
    assert_eq!(plans.len(), 2);
    let ir::NodeAccessPlan::Union(values) = plans.as_ref()[0].as_ref() else {
        panic!("expected restricted equality union");
    };
    let values = values
        .iter()
        .map(|source| match source.as_ref() {
            ir::NodeAccessPlan::EqualityIndex { value, .. } => value.clone(),
            source => panic!("expected equality source, found {source:?}"),
        })
        .collect::<Vec<_>>();

    assert_eq!(values, vec![equality_literal(30), equality_literal(40)]);
    assert!(matches!(
        plans.as_ref()[1].as_ref(),
        ir::NodeAccessPlan::LabelScan { label } if label.as_ref() == "User"
    ));
}

#[test]
fn access_equality_range_rule_drops_redundant_range_when_union_is_contained() {
    let rule = AccessEqualityRangeIntersectionRule::default();
    let storage = cost::StorageCostProfile::default();
    let union =
        ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            edge_eq_source("FOLLOWS", "weight", equality_literal(30)),
            edge_eq_source("FOLLOWS", "weight", equality_literal(40)),
        )))
        .unwrap();
    let expr = edge_access_expr(ir::EdgeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            union,
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
            if matches!(path.source().as_ref(), ir::EdgeAccessPlan::Union(values) if values.len() == 2)
    ));
}

#[test]
fn access_equality_range_rule_drops_redundant_node_range_when_union_is_contained() {
    let rule = AccessEqualityRangeIntersectionRule::default();
    let storage = cost::StorageCostProfile::default();
    let union =
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            node_eq_source("User", "age", equality_literal(30)),
            node_eq_source("User", "age", equality_literal(40)),
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
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::Union(values) if values.len() == 2)
    ));
}

#[test]
fn access_equality_range_rule_collapses_fully_excluded_union_to_empty() {
    let rule = AccessEqualityRangeIntersectionRule::default();
    let storage = cost::StorageCostProfile::default();
    let union =
        ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            edge_eq_source("FOLLOWS", "weight", equality_literal(10)),
            edge_eq_source("FOLLOWS", "weight", equality_literal(20)),
        )))
        .unwrap();
    let expr = edge_access_expr(ir::EdgeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            union,
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
fn access_equality_range_rule_collapses_fully_excluded_node_union_to_empty() {
    let rule = AccessEqualityRangeIntersectionRule::default();
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
fn access_equality_range_rule_declines_dynamic_or_mixed_unions() {
    let rule = AccessEqualityRangeIntersectionRule::default();
    let storage = cost::StorageCostProfile::default();
    let dynamic_value =
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            node_eq_source("User", "age", ir::IndexValue::Param(name("age"))),
            node_eq_source("User", "age", equality_literal(30)),
        )))
        .unwrap();
    let mixed_property =
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            node_eq_source("User", "age", equality_literal(10)),
            node_eq_source("User", "score", equality_literal(30)),
        )))
        .unwrap();
    let dynamic_range =
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            node_eq_source("User", "age", equality_literal(10)),
            node_eq_source("User", "age", equality_literal(30)),
        )))
        .unwrap();

    for expr in [
        source(properties::ElementKind::Node),
        node_access_expr(ir::NodeAccessPlan::Intersect(
            ir::AtLeast::<_, 2>::from_pair(
                dynamic_value,
                node_range_source("User", "age", lower_range(21)),
            ),
        )),
        node_access_expr(ir::NodeAccessPlan::Intersect(
            ir::AtLeast::<_, 2>::from_pair(
                mixed_property,
                node_range_source("User", "age", lower_range(21)),
            ),
        )),
        node_access_expr(ir::NodeAccessPlan::Intersect(
            ir::AtLeast::<_, 2>::from_pair(
                dynamic_range,
                node_range_source(
                    "User",
                    "age",
                    ir::IndexRange::Lower {
                        lower: ir::IndexBound::Inclusive(
                            ir::RangeIndexValue::param("min").unwrap(),
                        ),
                    },
                ),
            ),
        )),
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
