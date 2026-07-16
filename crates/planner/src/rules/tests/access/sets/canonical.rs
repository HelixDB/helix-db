use super::*;

fn apply_access_set_rule(expr: &logical::LogicalExpr) -> logical::AccessPath {
    let rule = AccessSetSimplificationRule::default();
    let storage = cost::StorageCostProfile::default();
    logical_access_path(rule.apply(optimizer::RuleInput {
        expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }))
}

#[test]
fn access_set_rule_flattens_dedupes_and_elides_empty_node_unions() {
    let rule = AccessSetSimplificationRule::default();
    let storage = cost::StorageCostProfile::default();
    let point = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::PointIds {
        ids: element_ids(vec![7]),
    })
    .unwrap();
    let nested = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Union(
        ir::AtLeast::<_, 2>::from_pair(point.clone(), point.clone()),
    ))
    .unwrap();
    let expr = node_access_expr(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Empty).unwrap(),
        nested,
    )));

    let rewritten = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "access_set_simplification");
    assert!(matches!(
        rewritten,
        logical::AccessPath::Node(path)
            if matches!(
                path.source().as_ref(),
                ir::NodeAccessPlan::PointIds { ids } if ids.as_ref() == [7]
            )
    ));
}

#[test]
fn access_set_rule_collapses_all_empty_unions_to_empty_for_nodes_and_edges() {
    let node_expr = node_access_expr(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Empty).unwrap(),
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Empty).unwrap(),
    )));
    let edge_expr = edge_access_expr(ir::EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
        ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::Empty).unwrap(),
        ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::Empty).unwrap(),
    )));

    assert!(matches!(
        apply_access_set_rule(&node_expr),
        logical::AccessPath::Node(path)
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::Empty)
    ));
    assert!(matches!(
        apply_access_set_rule(&edge_expr),
        logical::AccessPath::Edge(path)
            if matches!(path.source().as_ref(), ir::EdgeAccessPlan::Empty)
    ));
}

#[test]
fn access_set_rule_dedupes_intersections_to_singleton_sources() {
    let node_point = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::PointIds {
        ids: element_ids(vec![7]),
    })
    .unwrap();
    let node_expr = node_access_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(node_point.clone(), node_point),
    ));
    let edge_label = ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::LabelScan {
        label: name("LIKES"),
    })
    .unwrap();
    let edge_expr = edge_access_expr(ir::EdgeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(edge_label.clone(), edge_label),
    ));

    assert!(matches!(
        apply_access_set_rule(&node_expr),
        logical::AccessPath::Node(path)
            if matches!(
                path.source().as_ref(),
                ir::NodeAccessPlan::PointIds { ids } if ids.as_ref() == [7]
            )
    ));
    assert!(matches!(
        apply_access_set_rule(&edge_expr),
        logical::AccessPath::Edge(path)
            if matches!(
                path.source().as_ref(),
                ir::EdgeAccessPlan::LabelScan { label } if label.as_ref() == "LIKES"
            )
    ));
}

#[test]
fn access_set_rule_flattens_and_dedupes_edge_intersections_preserving_order() {
    let likes = ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::LabelScan {
        label: name("LIKES"),
    })
    .unwrap();
    let point = ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::PointIds {
        ids: element_ids(vec![11]),
    })
    .unwrap();
    let knows = ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::LabelScan {
        label: name("KNOWS"),
    })
    .unwrap();
    let nested = ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(point.clone(), knows.clone()),
    ))
    .unwrap();
    let expr = edge_access_expr(ir::EdgeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::try_from_vec(vec![likes.clone(), nested, point]).unwrap(),
    ));

    let logical::AccessPath::Edge(path) = apply_access_set_rule(&expr) else {
        panic!("expected edge path");
    };
    let ir::EdgeAccessPlan::Intersect(plans) = path.source().as_ref() else {
        panic!("expected flattened edge intersection");
    };
    let shapes = plans
        .iter()
        .map(|plan| match plan.as_ref() {
            ir::EdgeAccessPlan::LabelScan { label } => label.as_ref().to_owned(),
            ir::EdgeAccessPlan::PointIds { ids } => format!("point:{}", ids.as_ref()[0]),
            plan => panic!("unexpected edge intersection source {plan:?}"),
        })
        .collect::<Vec<_>>();

    assert_eq!(shapes, vec!["LIKES", "point:11", "KNOWS"]);
}

#[test]
fn access_set_rule_dedupes_wide_sets_by_stable_digest_preserving_order() {
    let rule = AccessSetSimplificationRule::default();
    let storage = cost::StorageCostProfile::default();
    let sources = (0_u64..64)
        .flat_map(|id| {
            let source = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::PointIds {
                ids: element_ids(vec![id]),
            })
            .unwrap();
            [source.clone(), source]
        })
        .collect::<Vec<_>>();
    let expr = node_access_expr(ir::NodeAccessPlan::Union(
        ir::AtLeast::<_, 2>::try_from_vec(sources).unwrap(),
    ));

    let rewritten = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    let logical::AccessPath::Node(path) = rewritten else {
        panic!("expected node access path");
    };
    let ir::NodeAccessPlan::Union(plans) = path.source().as_ref() else {
        panic!("expected deduped node union");
    };
    let ids = plans
        .iter()
        .map(|plan| match plan.as_ref() {
            ir::NodeAccessPlan::PointIds { ids } => ids.as_ref()[0],
            plan => panic!("expected point-id source, found {plan:?}"),
        })
        .collect::<Vec<_>>();

    assert_eq!(ids, (0_u64..64).collect::<Vec<_>>());
}

#[test]
fn access_set_rule_collapses_empty_intersections_and_flattens_edges() {
    let rule = AccessSetSimplificationRule::default();
    let storage = cost::StorageCostProfile::default();
    let node_point = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::PointIds {
        ids: element_ids(vec![7]),
    })
    .unwrap();
    let node_empty = node_access_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            node_point,
            ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Empty).unwrap(),
        ),
    ));
    let label = ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::LabelScan {
        label: name("LIKES"),
    })
    .unwrap();
    let point = ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::PointIds {
        ids: element_ids(vec![11]),
    })
    .unwrap();
    let nested = ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::Union(
        ir::AtLeast::<_, 2>::from_pair(label.clone(), point.clone()),
    ))
    .unwrap();
    let edge_union = edge_access_expr(ir::EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
        nested, label,
    )));

    let node = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &node_empty,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let edge = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &edge_union,
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
            if matches!(path.source().as_ref(), ir::EdgeAccessPlan::Union(plans) if plans.len() == 2)
    ));
}

#[test]
fn access_set_rule_declines_non_sets_and_already_canonical_sets() {
    let rule = AccessSetSimplificationRule::default();
    let storage = cost::StorageCostProfile::default();
    let left = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::PointIds {
        ids: element_ids(vec![1]),
    })
    .unwrap();
    let right = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::PointIds {
        ids: element_ids(vec![2]),
    })
    .unwrap();
    let canonical = node_access_expr(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
        left, right,
    )));
    let canonical_edge = edge_access_expr(ir::EdgeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::LabelScan {
                label: name("LIKES"),
            })
            .unwrap(),
            ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::LabelScan {
                label: name("KNOWS"),
            })
            .unwrap(),
        ),
    ));

    for expr in [
        source(properties::ElementKind::Node),
        node_access_expr(ir::NodeAccessPlan::AllScan),
        canonical,
        canonical_edge,
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
