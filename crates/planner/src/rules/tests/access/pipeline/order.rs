use super::*;

#[test]
fn access_pipeline_order_rule_rewrites_range_direction_then_elides_satisfied_order() {
    let rule = AccessPipelineOrderRule::default();
    let storage = cost::StorageCostProfile::default();
    let desc_key = range_key("User", "age", helix_ast::index::RangeIndexDirection::Desc);
    let indexes = catalog::IndexCatalogSnapshot::default().with_node_range(desc_key.clone());
    let expr = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            logical::AccessPath::Node(logical::NodeAccessPath::new(node_range_source(
                "User",
                "age",
                lower_range(18),
            ))),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Order {
                ordering: desc_order_keys(),
            }),
        )
        .unwrap(),
    );

    let access = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: &indexes,
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    assert_eq!(rule.metadata().id.as_ref(), "access_pipeline_order");
    assert!(matches!(
        access,
        logical::AccessPath::Node(path)
            if matches!(
                path.source().as_ref(),
                ir::NodeAccessPlan::RangeIndex { key, .. } if key == &desc_key
            )
    ));
}

#[test]
fn access_pipeline_order_rule_keeps_suffix_after_rewriting_range_direction() {
    let rule = AccessPipelineOrderRule::default();
    let storage = cost::StorageCostProfile::default();
    let desc_key = range_key("User", "age", helix_ast::index::RangeIndexDirection::Desc);
    let indexes = catalog::IndexCatalogSnapshot::default().with_node_range(desc_key.clone());
    let window = logical::AccessWindowRange::new(0, Some(2)).unwrap();
    let expr = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            logical::AccessPath::Node(logical::NodeAccessPath::new(node_range_source(
                "User",
                "age",
                lower_range(18),
            ))),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOp::Order {
                    ordering: desc_order_keys(),
                },
                vec![logical::StreamPipelineOp::Window { window }],
            ),
        )
        .unwrap(),
    );

    let rewritten = logical_access_pipeline(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: &indexes,
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert!(matches!(
        rewritten.access(),
        logical::AccessPath::Node(path)
            if matches!(
                path.source().as_ref(),
                ir::NodeAccessPlan::RangeIndex { key, .. } if key == &desc_key
            )
    ));
    assert!(matches!(
        rewritten.ops(),
        [logical::StreamPipelineOp::Window { window: actual }] if *actual == window
    ));
}

#[test]
fn access_pipeline_order_rule_elides_satisfied_order_and_keeps_suffix() {
    let rule = AccessPipelineOrderRule::default();
    let storage = cost::StorageCostProfile::default();
    let indexes = catalog::IndexCatalogSnapshot::default();
    let window = logical::AccessWindowRange::new(0, Some(2)).unwrap();
    let expr = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            logical::AccessPath::Node(logical::NodeAccessPath::new(
                node_range_source_with_direction(
                    "User",
                    "age",
                    helix_ast::index::RangeIndexDirection::Desc,
                    lower_range(18),
                ),
            )),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOp::Order {
                    ordering: desc_order_keys(),
                },
                vec![logical::StreamPipelineOp::Window { window }],
            ),
        )
        .unwrap(),
    );

    let rewritten = logical_access_pipeline(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: &indexes,
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert!(matches!(
        rewritten.access(),
        logical::AccessPath::Node(path)
            if matches!(
                path.source().as_ref(),
                ir::NodeAccessPlan::RangeIndex { key, .. }
                    if key.direction == helix_ast::index::RangeIndexDirection::Desc
            )
    ));
    assert!(matches!(
        rewritten.ops(),
        [logical::StreamPipelineOp::Window { window: actual }] if *actual == window
    ));
}

#[test]
fn access_pipeline_order_rule_elides_order_after_residual_filters() {
    let rule = AccessPipelineOrderRule::default();
    let storage = cost::StorageCostProfile::default();
    let indexes = catalog::IndexCatalogSnapshot::default();
    let filter = logical::StreamPipelineOp::Filter {
        predicate: ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap(),
    };
    let window = logical::AccessWindowRange::new(0, Some(2)).unwrap();
    let expr = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            logical::AccessPath::Node(logical::NodeAccessPath::new(
                node_range_source_with_direction(
                    "User",
                    "age",
                    helix_ast::index::RangeIndexDirection::Desc,
                    lower_range(18),
                ),
            )),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                filter.clone(),
                vec![
                    logical::StreamPipelineOp::Order {
                        ordering: desc_order_keys(),
                    },
                    logical::StreamPipelineOp::Window { window },
                ],
            ),
        )
        .unwrap(),
    );

    let rewritten = logical_access_pipeline(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: &indexes,
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert!(matches!(
        rewritten.access(),
        logical::AccessPath::Node(path)
            if matches!(
                path.source().as_ref(),
                ir::NodeAccessPlan::RangeIndex { key, .. }
                    if key.direction == helix_ast::index::RangeIndexDirection::Desc
            )
    ));
    assert_eq!(
        rewritten.ops(),
        &[filter, logical::StreamPipelineOp::Window { window }]
    );
}

#[test]
fn access_pipeline_order_rule_promotes_the_requested_intersection_driver() {
    let rule = AccessPipelineOrderRule::default();
    let storage = cost::StorageCostProfile::default();
    let filter = logical::StreamPipelineOp::Filter {
        predicate: ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap(),
    };
    let window = logical::AccessWindowRange::new(0, Some(2)).unwrap();
    let expr = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            logical::AccessPath::Node(logical::NodeAccessPath::new(
                ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Intersect(
                    ir::AtLeast::from_pair(
                        node_range_source("User", "age", lower_range(18)),
                        node_range_source("User", "score", lower_range(0)),
                    ),
                ))
                .unwrap(),
            )),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                filter.clone(),
                vec![
                    logical::StreamPipelineOp::Order {
                        ordering: order_keys_for("score", helix_ast::traversal::Order::Asc),
                    },
                    logical::StreamPipelineOp::Window { window },
                ],
            ),
        )
        .unwrap(),
    );

    let rewritten = logical_access_pipeline(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let logical::AccessPath::Node(path) = rewritten.access() else {
        panic!("expected node access path");
    };
    let ir::NodeAccessPlan::Intersect(children) = path.source().as_ref() else {
        panic!("expected node intersection");
    };

    assert!(matches!(
        children[0].as_ref(),
        ir::NodeAccessPlan::RangeIndex { key, .. } if key.property == "score"
    ));
    assert_eq!(
        rewritten.ops(),
        &[filter, logical::StreamPipelineOp::Window { window }]
    );
}

#[test]
fn access_pipeline_order_rule_rejects_non_access_pipeline_input() {
    let rule = AccessPipelineOrderRule::default();
    let storage = cost::StorageCostProfile::default();
    let expr = logical::LogicalExpr::AccessPath(logical::AccessPath::Node(
        logical::NodeAccessPath::new(node_range_source("User", "age", lower_range(18))),
    ));

    assert_eq!(
        rule.apply(optimizer::RuleInput {
            expr: &expr,
            storage: &storage,
            indexes: &catalog::IndexCatalogSnapshot::default(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        }),
        optimizer::RuleResult::NotApplicable
    );
}

#[test]
fn access_pipeline_order_rule_rejects_pipeline_not_headed_by_order() {
    let rule = AccessPipelineOrderRule::default();
    let storage = cost::StorageCostProfile::default();
    let expr = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            logical::AccessPath::Node(logical::NodeAccessPath::new(node_range_source(
                "User",
                "age",
                lower_range(18),
            ))),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Window {
                window: logical::AccessWindowRange::new(0, Some(2)).unwrap(),
            }),
        )
        .unwrap(),
    );

    assert_eq!(
        rule.apply(optimizer::RuleInput {
            expr: &expr,
            storage: &storage,
            indexes: &catalog::IndexCatalogSnapshot::default(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        }),
        optimizer::RuleResult::NotApplicable
    );
}
