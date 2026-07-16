use super::*;

#[test]
fn root_stream_access_rewrite_rule_pushes_direct_filters_through_root_wrappers() {
    let rule = RootStreamAccessRewriteRule::default();
    let indexes = username_indexes();

    let cases = [
        logical::LogicalExpr::RootPipeline(
            logical::RootPipeline::new(
                filtered_user_root(),
                ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Limit {
                    count: ir::StreamBoundPlan::Literal(4),
                }),
            )
            .unwrap(),
        ),
        logical::LogicalExpr::StreamReserved(logical::StreamReserved::new(
            filtered_user_root(),
            ir::ReservedOp::Fold,
        )),
        logical::LogicalExpr::StreamProject(logical::StreamProject::new(
            filtered_user_root(),
            ir::ProjectionPlan::Count,
        )),
        logical::LogicalExpr::StreamAggregate(logical::StreamAggregate::new(
            filtered_user_root(),
            ir::AggregatePlan::GroupCount(name("username")),
        )),
        logical::LogicalExpr::StreamVariableWrite(logical::StreamVariableWrite::new(
            filtered_user_root(),
            logical::StreamVariableWriteOp::Store(name("cached")),
        )),
    ];

    for expr in cases {
        let rewritten = logical_expr(rule.apply(optimizer::RuleInput {
            expr: &expr,
            storage: &cost::StorageCostProfile::default(),
            indexes: &indexes,
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        }));

        assert_indexed_root_input(&rewritten);
    }
}

#[test]
fn root_stream_access_rewrite_rule_preserves_pipeline_suffix_after_index_rewrite() {
    let rule = RootStreamAccessRewriteRule::default();
    let indexes = username_indexes();
    let pipeline = logical::AccessPipeline::new(
        user_label_access(),
        ir::AtLeast::<_, 1>::from_one_and_rest(
            logical::StreamPipelineOp::Filter {
                predicate: username_predicate(),
            },
            vec![logical::StreamPipelineOp::Order {
                ordering: order_keys_for("username", helix_ast::traversal::Order::Asc),
            }],
        ),
    )
    .unwrap();
    let expr = logical::LogicalExpr::StreamProject(logical::StreamProject::new(
        logical::RootStream::Access(logical::AccessStream::Pipeline(pipeline)),
        ir::ProjectionPlan::Count,
    ));

    let rewritten = logical_expr(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &cost::StorageCostProfile::default(),
        indexes: &indexes,
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    let logical::LogicalExpr::StreamProject(project) = rewritten else {
        panic!("expected stream project rewrite");
    };
    let logical::RootStream::Access(logical::AccessStream::Pipeline(pipeline)) = project.input()
    else {
        panic!("expected rewritten access pipeline");
    };
    assert_indexed_access(pipeline.access());
    assert!(matches!(
        pipeline.ops(),
        [logical::StreamPipelineOp::Order { ordering }]
            if ordering.as_ref()[0].property.as_ref() == "username"
    ));
}

#[test]
fn root_stream_access_rewrite_rule_declines_non_access_inputs() {
    let rule = RootStreamAccessRewriteRule::default();
    let expr = logical::LogicalExpr::StreamProject(logical::StreamProject::new(
        logical::RootStream::VariableSource(logical::VariableSource::new(name("users"))),
        ir::ProjectionPlan::Count,
    ));

    assert_eq!(
        rule.apply(optimizer::RuleInput {
            expr: &expr,
            storage: &cost::StorageCostProfile::default(),
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        }),
        optimizer::RuleResult::NotApplicable
    );
}

fn logical_expr(result: optimizer::RuleResult) -> logical::LogicalExpr {
    let optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(expressions)) = result else {
        panic!("expected logical rewrite");
    };
    expressions.into_iter().next().unwrap()
}

fn filtered_user_root() -> logical::RootStream {
    logical::RootStream::Access(logical::AccessStream::Filter(logical::AccessFilter::new(
        user_label_access(),
        username_predicate(),
    )))
}

fn user_label_access() -> logical::AccessPath {
    node_access_path(ir::NodeAccessPlan::LabelScan {
        label: name("User"),
    })
}

fn username_predicate() -> ir::PredicatePlan {
    ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("username", "alice")).unwrap()
}

fn username_indexes() -> catalog::IndexCatalogSnapshot {
    catalog::IndexCatalogSnapshot::default()
        .with_node_eq(catalog::ScopedPropertyKey::try_new("User", "username").unwrap())
}

fn assert_indexed_root_input(expr: &logical::LogicalExpr) {
    match expr {
        logical::LogicalExpr::RootPipeline(pipeline) => {
            assert_indexed_root_stream(pipeline.input());
            assert!(matches!(
                pipeline.ops(),
                [logical::StreamPipelineOp::Limit { count }]
                    if matches!(count, ir::StreamBoundPlan::Literal(4))
            ));
        }
        logical::LogicalExpr::StreamReserved(reserved) => {
            assert_indexed_root_stream(reserved.input());
            assert_eq!(reserved.op(), &ir::ReservedOp::Fold);
        }
        logical::LogicalExpr::StreamProject(project) => {
            assert_indexed_root_stream(project.input());
            assert_eq!(project.projection(), &ir::ProjectionPlan::Count);
        }
        logical::LogicalExpr::StreamAggregate(aggregate) => {
            assert_indexed_root_stream(aggregate.input());
            assert_eq!(
                aggregate.aggregate(),
                &ir::AggregatePlan::GroupCount(name("username"))
            );
        }
        logical::LogicalExpr::StreamVariableWrite(write) => {
            assert_indexed_root_stream(write.input());
            assert_eq!(
                write.op(),
                &logical::StreamVariableWriteOp::Store(name("cached"))
            );
        }
        _ => panic!("unexpected rewrite expression: {expr:?}"),
    }
}

fn assert_indexed_root_stream(input: &logical::RootStream) {
    let logical::RootStream::Access(logical::AccessStream::Path(access)) = input else {
        panic!("expected rewritten access path: {input:?}");
    };
    assert_indexed_access(access);
}

fn assert_indexed_access(access: &logical::AccessPath) {
    assert!(matches!(
        access,
        logical::AccessPath::Node(path)
            if matches!(
                path.source().as_ref(),
                ir::NodeAccessPlan::EqualityIndex { key, .. }
                    if key.label == "User" && key.property == "username"
            )
    ));
}
