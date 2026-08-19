use super::*;

#[test]
fn access_order_rule_elides_matching_range_index_order_and_singletons() {
    let rule = AccessOrderRule::default();
    let storage = cost::StorageCostProfile::default();
    let range = node_access_order_expr(
        ir::NodeAccessPlan::from(node_range_source("User", "age", lower_range(18))),
        order_keys(),
    );
    let singleton = edge_access_order_expr(
        ir::EdgeAccessPlan::PointIds {
            ids: element_ids(vec![42]),
        },
        desc_order_keys(),
    );

    let range = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &range,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let singleton = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &singleton,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "access_order");
    assert!(matches!(
        range,
        logical::AccessPath::Node(path)
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::RangeIndex { .. })
    ));
    assert!(matches!(
        singleton,
        logical::AccessPath::Edge(path)
            if matches!(path.source().as_ref(), ir::EdgeAccessPlan::PointIds { .. })
    ));
}

#[test]
fn access_order_rule_promotes_the_requested_direct_node_range_driver() {
    let rule = AccessOrderRule::default();
    let storage = cost::StorageCostProfile::default();
    let expr = node_access_order_expr(
        ir::NodeAccessPlan::Intersect(ir::AtLeast::from_pair(
            node_range_source("User", "age", lower_range(18)),
            node_range_source("User", "score", lower_range(0)),
        )),
        order_keys_for("score", helix_ast::traversal::Order::Asc),
    );

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
    let ir::NodeAccessPlan::Intersect(children) = path.source().as_ref() else {
        panic!("expected node intersection");
    };

    assert!(matches!(
        children[0].as_ref(),
        ir::NodeAccessPlan::RangeIndex { key, .. } if key.property == "score"
    ));
    assert!(matches!(
        children[1].as_ref(),
        ir::NodeAccessPlan::RangeIndex { key, .. } if key.property == "age"
    ));
}

#[test]
fn access_order_rule_preserves_non_range_filters_during_driver_promotion() {
    let rule = AccessOrderRule::default();
    let storage = cost::StorageCostProfile::default();
    let union = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Union(ir::AtLeast::from_pair(
        node_eq_source("User", "status", equality_literal(1)),
        node_eq_source("User", "status", equality_literal(2)),
    )))
    .unwrap();
    let expr = node_access_order_expr(
        ir::NodeAccessPlan::Intersect(
            ir::AtLeast::try_from_vec(vec![
                node_eq_source("User", "tenant", equality_literal(7)),
                node_range_source("User", "age", lower_range(18)),
                union,
                node_range_source("User", "score", lower_range(0)),
                node_eq_source("User", "active", equality_literal(1)),
            ])
            .unwrap(),
        ),
        order_keys_for("score", helix_ast::traversal::Order::Asc),
    );

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
    let ir::NodeAccessPlan::Intersect(children) = path.source().as_ref() else {
        panic!("expected node intersection");
    };

    assert!(matches!(
        children[0].as_ref(),
        ir::NodeAccessPlan::EqualityIndex { key, .. } if key.property == "tenant"
    ));
    assert!(matches!(
        children[1].as_ref(),
        ir::NodeAccessPlan::RangeIndex { key, .. } if key.property == "score"
    ));
    assert!(matches!(
        children[2].as_ref(),
        ir::NodeAccessPlan::RangeIndex { key, .. } if key.property == "age"
    ));
    assert!(matches!(children[3].as_ref(), ir::NodeAccessPlan::Union(_)));
    assert!(matches!(
        children[4].as_ref(),
        ir::NodeAccessPlan::EqualityIndex { key, .. } if key.property == "active"
    ));
}

#[test]
fn access_order_rule_keeps_the_first_matching_direct_range_driver() {
    let rule = AccessOrderRule::default();
    let storage = cost::StorageCostProfile::default();
    let source = ir::NodeAccessPlan::Intersect(
        ir::AtLeast::try_from_vec(vec![
            node_eq_source("User", "tenant", equality_literal(7)),
            node_range_source("User", "score", lower_range(0)),
            node_eq_source("User", "active", equality_literal(1)),
            node_range_source("User", "score", upper_range(100)),
        ])
        .unwrap(),
    );
    let expr = node_access_order_expr(
        source.clone(),
        order_keys_for("score", helix_ast::traversal::Order::Asc),
    );

    let rewritten = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert!(matches!(
        rewritten,
        logical::AccessPath::Node(path) if path.source().as_ref() == &source
    ));
}

#[test]
fn access_order_rule_promotes_the_requested_direct_edge_range_driver() {
    let rule = AccessOrderRule::default();
    let storage = cost::StorageCostProfile::default();
    let expr = edge_access_order_expr(
        ir::EdgeAccessPlan::Intersect(ir::AtLeast::from_pair(
            edge_range_source_with_direction(
                "FOLLOWS",
                "age",
                helix_ast::index::RangeIndexDirection::Desc,
                lower_range(18),
            ),
            edge_range_source_with_direction(
                "FOLLOWS",
                "score",
                helix_ast::index::RangeIndexDirection::Desc,
                lower_range(0),
            ),
        )),
        order_keys_for("score", helix_ast::traversal::Order::Desc),
    );

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
    let ir::EdgeAccessPlan::Intersect(children) = path.source().as_ref() else {
        panic!("expected edge intersection");
    };

    assert!(matches!(
        children[0].as_ref(),
        ir::EdgeAccessPlan::RangeIndex { key, .. } if key.property == "score"
    ));
    assert!(matches!(
        children[1].as_ref(),
        ir::EdgeAccessPlan::RangeIndex { key, .. } if key.property == "age"
    ));
}

#[test]
fn access_order_rule_rejects_nested_only_range_drivers() {
    let rule = AccessOrderRule::default();
    let storage = cost::StorageCostProfile::default();
    let node_nested = node_access_order_expr(
        ir::NodeAccessPlan::Intersect(ir::AtLeast::from_pair(
            ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Intersect(ir::AtLeast::from_pair(
                node_range_source("User", "age", lower_range(18)),
                node_eq_source("User", "status", equality_literal(1)),
            )))
            .unwrap(),
            node_eq_source("User", "tenant", equality_literal(2)),
        )),
        order_keys(),
    );
    let edge_nested = edge_access_order_expr(
        ir::EdgeAccessPlan::Intersect(ir::AtLeast::from_pair(
            ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::Intersect(ir::AtLeast::from_pair(
                edge_range_source_with_direction(
                    "FOLLOWS",
                    "weight",
                    helix_ast::index::RangeIndexDirection::Desc,
                    lower_range(1),
                ),
                edge_eq_source("FOLLOWS", "status", equality_literal(1)),
            )))
            .unwrap(),
            edge_eq_source("FOLLOWS", "tenant", equality_literal(2)),
        )),
        desc_weight_order_keys(),
    );

    for expr in [node_nested, edge_nested] {
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

#[test]
fn access_order_rule_rejects_unsafe_intersection_order_requests() {
    let rule = AccessOrderRule::default();
    let storage = cost::StorageCostProfile::default();
    let matching_range = node_range_source("User", "score", lower_range(0));
    let mismatched = node_access_order_expr(
        ir::NodeAccessPlan::Intersect(ir::AtLeast::from_pair(
            node_eq_source("User", "active", equality_literal(1)),
            matching_range.clone(),
        )),
        order_keys_for("score", helix_ast::traversal::Order::Desc),
    );
    let multikey = node_access_order_expr(
        ir::NodeAccessPlan::Intersect(ir::AtLeast::from_pair(
            node_eq_source("User", "active", equality_literal(1)),
            matching_range.clone(),
        )),
        multi_order_keys(),
    );
    let mixed = node_access_order_expr(
        ir::NodeAccessPlan::Intersect(ir::AtLeast::from_pair(
            ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::AllScan).unwrap(),
            matching_range,
        )),
        order_keys_for("score", helix_ast::traversal::Order::Asc),
    );

    for expr in [mismatched, multikey, mixed] {
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

#[test]
fn access_order_rule_declines_mismatch_multikey_unknown_and_non_candidates() {
    let rule = AccessOrderRule::default();
    let storage = cost::StorageCostProfile::default();
    let mismatched_direction = node_access_order_expr(
        ir::NodeAccessPlan::from(node_range_source("User", "age", lower_range(18))),
        desc_order_keys(),
    );
    let multikey = node_access_order_expr(
        ir::NodeAccessPlan::from(node_range_source("User", "age", lower_range(18))),
        multi_order_keys(),
    );
    let unknown_bound = node_access_order_expr(
        ir::NodeAccessPlan::LabelScan {
            label: name("User"),
        },
        order_keys(),
    );

    for expr in [
        source(properties::ElementKind::Node),
        mismatched_direction,
        multikey,
        unknown_bound,
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

#[test]
fn access_order_implementation_rule_keeps_explicit_sort_in_cascades() {
    let rule = AccessOrderImplementationRule::default();
    let storage = cost::StorageCostProfile {
        range_seek: cost::LatencyEstimate::micros(7),
        range_next: cost::LatencyEstimate::micros(11),
        sort_setup: cost::LatencyEstimate::micros(13),
        sort_per_row: cost::LatencyEstimate::micros(17),
        default_unknown_scan_rows: cost::EstimatedRows::rows(9),
        ..cost::StorageCostProfile::default()
    };
    let expr = node_access_order_expr(ir::NodeAccessPlan::AllScan, order_keys());

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "seed_access_order");
    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical access-order pipeline");
    };
    assert!(matches!(
        pipeline.ops(),
        [
            physical::PhysicalPipelineOp::Access {
                element: properties::ElementKind::Node,
                access: physical::PhysicalAccess::Kv(exec::KvReadPlan::RangeScan { .. }),
            },
            physical::PhysicalPipelineOp::Sort,
        ]
    ));
    assert!(matches!(
        alternative.delivered.ordering,
        properties::DeliveredOrdering::ByKeys(_)
    ));
    assert_eq!(
        alternative.delivered.materialization,
        properties::Materialization::Materialized
    );
    let rows = storage.default_unknown_scan_rows;
    assert_eq!(
        alternative.cost,
        storage.range_scan(rows).serial(storage.explicit_sort(rows))
    );

    let already_satisfied = node_access_order_expr(
        ir::NodeAccessPlan::PointIds {
            ids: element_ids(vec![7]),
        },
        order_keys(),
    );
    assert_eq!(
        rule.apply(optimizer::RuleInput {
            expr: &already_satisfied,
            storage: &storage,
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        }),
        optimizer::RuleResult::NotApplicable
    );
}
