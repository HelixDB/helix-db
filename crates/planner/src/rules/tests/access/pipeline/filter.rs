use super::*;

#[test]
fn access_pipeline_filter_rule_indexes_leading_filter_and_preserves_suffix() {
    let rule = AccessPipelineFilterRule::default();
    let storage = cost::StorageCostProfile::default();
    let range_key = range_key("User", "age", helix_ast::index::RangeIndexDirection::Asc);
    let indexes = catalog::IndexCatalogSnapshot::default().with_node_range(range_key.clone());
    let predicate = ir::PredicatePlan::new(helix_ast::expr::Predicate::and(vec![
        helix_ast::expr::Predicate::eq("$label", "User"),
        helix_ast::expr::Predicate::gte("age", 21),
    ]))
    .unwrap();
    let expr = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            node_access_path(ir::NodeAccessPlan::AllScan),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOp::Filter { predicate },
                vec![logical::StreamPipelineOp::Order {
                    ordering: desc_order_keys(),
                }],
            ),
        )
        .unwrap(),
    );

    let pipeline = logical_access_pipeline(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: &indexes,
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "access_pipeline_filter");
    assert!(matches!(
        pipeline.access(),
        logical::AccessPath::Node(path)
            if matches!(
                path.source().as_ref(),
                ir::NodeAccessPlan::RangeIndex { key, .. } if key == &range_key
            )
    ));
    assert!(matches!(
        pipeline.ops(),
        [logical::StreamPipelineOp::Order { ordering }] if ordering == &desc_order_keys()
    ));
}
