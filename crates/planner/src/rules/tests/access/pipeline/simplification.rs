use super::*;

#[test]
fn access_pipeline_simplification_rule_collapses_empty_pipelines_by_output_kind() {
    let rule = AccessPipelineSimplificationRule::default();
    let storage = cost::StorageCostProfile::default();
    let expr = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            node_access_path(ir::NodeAccessPlan::Empty),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOp::Filter {
                    predicate: ir::PredicatePlan::new(helix_ast::expr::Predicate::eq(
                        "active", true,
                    ))
                    .unwrap(),
                },
                vec![
                    logical::StreamPipelineOp::Expand {
                        plan: ir::ExpandPlan {
                            direction: ir::ExpandDirection::Out,
                            output: ir::ExpandOutput::Edges,
                            label: ir::ExpandLabelPlan::Any,
                        },
                    },
                    logical::StreamPipelineOp::Order {
                        ordering: desc_order_keys(),
                    },
                ],
            ),
        )
        .unwrap(),
    );

    let access = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(
        rule.metadata().id.as_ref(),
        "access_pipeline_simplification"
    );
    assert!(matches!(
        access,
        logical::AccessPath::Edge(path)
            if matches!(path.source().as_ref(), ir::EdgeAccessPlan::Empty)
    ));

    let expr = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            edge_access_path(ir::EdgeAccessPlan::Empty),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Expand {
                plan: ir::ExpandPlan {
                    direction: ir::ExpandDirection::Out,
                    output: ir::ExpandOutput::Nodes,
                    label: ir::ExpandLabelPlan::Any,
                },
            }),
        )
        .unwrap(),
    );
    let access = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    assert!(matches!(
        access,
        logical::AccessPath::Node(path)
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::Empty)
    ));
}

#[test]
fn access_pipeline_simplification_rule_keeps_data_producing_variable_pipelines() {
    let rule = AccessPipelineSimplificationRule::default();
    let storage = cost::StorageCostProfile::default();
    let filter_expr = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            node_access_path(ir::NodeAccessPlan::Empty),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Variable {
                op: logical::PureStreamVariableOp::Within(name("allowed")),
            }),
        )
        .unwrap(),
    );
    let data_producing_expr = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            node_access_path(ir::NodeAccessPlan::Empty),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Variable {
                op: logical::PureStreamVariableOp::Inject(name("users")),
            }),
        )
        .unwrap(),
    );

    let access = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &filter_expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    assert!(matches!(
        access,
        logical::AccessPath::Node(path)
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::Empty)
    ));

    assert_eq!(
        rule.apply(optimizer::RuleInput {
            expr: &data_producing_expr,
            storage: &storage,
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        }),
        optimizer::RuleResult::NotApplicable
    );
}

#[test]
fn access_pipeline_simplification_rule_removes_distinct_noops() {
    let rule = AccessPipelineSimplificationRule::default();
    let storage = cost::StorageCostProfile::default();
    let singleton_expr = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            node_access_path(ir::NodeAccessPlan::PointIds {
                ids: element_ids(vec![42]),
            }),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOp::Distinct,
                vec![logical::StreamPipelineOp::Order {
                    ordering: order_keys(),
                }],
            ),
        )
        .unwrap(),
    );
    let duplicate_distinct_expr = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            edge_access_path(ir::EdgeAccessPlan::AllScan),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOp::Distinct,
                vec![logical::StreamPipelineOp::Distinct],
            ),
        )
        .unwrap(),
    );

    let pipeline = logical_access_pipeline(rule.apply(optimizer::RuleInput {
        expr: &singleton_expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    assert!(matches!(
        pipeline.ops(),
        [logical::StreamPipelineOp::Order { ordering }] if ordering == &order_keys()
    ));

    let pipeline = logical_access_pipeline(rule.apply(optimizer::RuleInput {
        expr: &duplicate_distinct_expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    assert!(matches!(
        pipeline.ops(),
        [logical::StreamPipelineOp::Distinct]
    ));
}
