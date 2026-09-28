use super::*;

fn indexes() -> catalog::IndexCatalogSnapshot {
    catalog::IndexCatalogSnapshot::default()
        .with_node_eq(catalog::ScopedPropertyKey::try_new("Attribute", "kind").unwrap())
        .with_node_eq(catalog::ScopedPropertyKey::try_new("Attribute", "status").unwrap())
}

fn expand(output: ir::ExpandOutput) -> logical::StreamPipelineOp {
    logical::StreamPipelineOp::Expand {
        plan: ir::ExpandPlan {
            direction: ir::ExpandDirection::Out,
            output,
            label: ir::ExpandLabelPlan::Label(name("HAS_ATTRIBUTE")),
        },
    }
}

fn filter(predicate: helix_ast::expr::Predicate) -> logical::StreamPipelineOp {
    logical::StreamPipelineOp::Filter {
        predicate: ir::PredicatePlan::new(predicate).unwrap(),
    }
}

fn kind_b() -> helix_ast::expr::Predicate {
    helix_ast::expr::Predicate::eq("kind", "B")
}

fn ops(ops: Vec<logical::StreamPipelineOp>) -> ir::AtLeast<logical::StreamPipelineOp, 1> {
    ir::AtLeast::try_from_vec(ops).unwrap()
}

fn access_pipeline(ops_list: Vec<logical::StreamPipelineOp>) -> logical::AccessPipeline {
    logical::AccessPipeline::new(node_access_path(ir::NodeAccessPlan::AllScan), ops(ops_list))
        .unwrap()
}

fn apply(expr: &logical::LogicalExpr) -> optimizer::RuleResult {
    AccessPipelineMembershipFilterRule::default().apply(optimizer::RuleInput {
        expr,
        storage: &cost::StorageCostProfile::default(),
        indexes: &indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    })
}

fn rewritten(result: optimizer::RuleResult) -> logical::LogicalExpr {
    let optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(exprs)) = result else {
        panic!("expected a logical rewrite: {result:?}");
    };
    let [expr] = exprs.as_ref() else {
        panic!("expected exactly one rewrite");
    };
    expr.clone()
}

fn membership_predicate(op: &logical::StreamPipelineOp) -> &helix_ast::expr::Predicate {
    let logical::StreamPipelineOp::IndexMembership { plan } = op else {
        panic!("expected index membership, got {op:?}");
    };
    plan.predicate().as_ref()
}

#[test]
fn rewrites_the_first_node_filter_behind_the_source_and_keeps_the_suffix() {
    let suffix = logical::StreamPipelineOp::Limit {
        count: ir::StreamBoundPlan::Literal(3),
    };
    let expr = logical::LogicalExpr::AccessPipeline(access_pipeline(vec![
        expand(ir::ExpandOutput::Nodes),
        filter(helix_ast::expr::Predicate::and(vec![
            kind_b(),
            helix_ast::expr::Predicate::contains("title", "x"),
        ])),
        suffix.clone(),
    ]));

    let logical::LogicalExpr::AccessPipeline(pipeline) = rewritten(apply(&expr)) else {
        panic!("expected an access pipeline");
    };
    assert_eq!(
        AccessPipelineMembershipFilterRule::default()
            .metadata()
            .id
            .as_ref(),
        "access_pipeline_membership_filter"
    );
    let [expanded, membership, residual, limit] = pipeline.ops() else {
        panic!(
            "expected expand, membership, residual, limit: {:?}",
            pipeline.ops()
        );
    };
    assert_eq!(expanded, &expand(ir::ExpandOutput::Nodes));
    assert_eq!(membership_predicate(membership), &kind_b());
    assert_eq!(
        residual,
        &filter(helix_ast::expr::Predicate::contains("title", "x"))
    );
    assert_eq!(limit, &suffix);
}

#[test]
fn rewrites_later_filters_on_reapplication_and_stops_at_residuals() {
    let status = helix_ast::expr::Predicate::eq("status", "live");
    let title = helix_ast::expr::Predicate::contains("title", "x");
    let expr = logical::LogicalExpr::AccessPipeline(access_pipeline(vec![
        expand(ir::ExpandOutput::Nodes),
        filter(kind_b()),
        expand(ir::ExpandOutput::Nodes),
        filter(helix_ast::expr::Predicate::and(vec![
            status.clone(),
            title.clone(),
        ])),
    ]));

    let first = rewritten(apply(&expr));
    let second = rewritten(apply(&first));
    let logical::LogicalExpr::AccessPipeline(pipeline) = &second else {
        panic!("expected an access pipeline");
    };
    assert_eq!(membership_predicate(&pipeline.ops()[1]), &kind_b());
    assert_eq!(membership_predicate(&pipeline.ops()[3]), &status);
    assert_eq!(pipeline.ops()[4], filter(title));
    assert_eq!(apply(&second), optimizer::RuleResult::NotApplicable);
}

#[test]
fn declines_source_filters_edge_streams_unknown_streams_and_other_exprs() {
    let leading = logical::LogicalExpr::AccessPipeline(access_pipeline(vec![
        filter(kind_b()),
        expand(ir::ExpandOutput::Nodes),
    ]));
    let edges = logical::LogicalExpr::AccessPipeline(access_pipeline(vec![
        expand(ir::ExpandOutput::Edges),
        filter(kind_b()),
    ]));
    let unknown = [
        logical::PureStreamVariableOp::Select(name("rows")),
        logical::PureStreamVariableOp::Inject(name("rows")),
    ]
    .map(|op| {
        logical::LogicalExpr::AccessPipeline(access_pipeline(vec![
            expand(ir::ExpandOutput::Nodes),
            logical::StreamPipelineOp::Variable { op },
            filter(kind_b()),
        ]))
    });
    let unindexed = logical::LogicalExpr::AccessPipeline(access_pipeline(vec![
        expand(ir::ExpandOutput::Nodes),
        filter(helix_ast::expr::Predicate::eq("color", "red")),
    ]));
    let variable_root = logical::LogicalExpr::RootPipeline(
        logical::RootPipeline::new(
            logical::RootStream::VariableSource(logical::VariableSource::new(name("seed"))),
            ops(vec![filter(kind_b())]),
        )
        .unwrap(),
    );

    for expr in [leading, edges, unindexed, variable_root, node_all_expr()]
        .into_iter()
        .chain(unknown)
    {
        assert_eq!(
            apply(&expr),
            optimizer::RuleResult::NotApplicable,
            "{expr:?}"
        );
    }
}

#[test]
fn element_preserving_variable_ops_keep_node_streams_rewritable() {
    for op in [
        logical::PureStreamVariableOp::Bind(name("row")),
        logical::PureStreamVariableOp::Within(name("allowed")),
        logical::PureStreamVariableOp::Without(name("denied")),
    ] {
        let expr = logical::LogicalExpr::AccessPipeline(access_pipeline(vec![
            expand(ir::ExpandOutput::Nodes),
            logical::StreamPipelineOp::Variable { op },
            filter(kind_b()),
        ]));
        let logical::LogicalExpr::AccessPipeline(pipeline) = rewritten(apply(&expr)) else {
            panic!("expected an access pipeline");
        };
        assert_eq!(membership_predicate(&pipeline.ops()[2]), &kind_b());
    }
}

#[test]
fn rewrites_root_pipelines_and_root_wrapper_inputs() {
    let variable_root = logical::RootPipeline::new(
        logical::RootStream::VariableSource(logical::VariableSource::new(name("seed"))),
        ops(vec![expand(ir::ExpandOutput::Nodes), filter(kind_b())]),
    )
    .unwrap();
    let logical::LogicalExpr::RootPipeline(pipeline) = rewritten(apply(
        &logical::LogicalExpr::RootPipeline(variable_root.clone()),
    )) else {
        panic!("expected a root pipeline");
    };
    assert_eq!(membership_predicate(&pipeline.ops()[1]), &kind_b());

    // A root pipeline after an access stream starts at its first operator; a
    // non-rewritable suffix falls back to the nested input stream.
    let after_access = logical::RootPipeline::new(
        logical::RootStream::Access(logical::AccessStream::Pipeline(access_pipeline(vec![
            expand(ir::ExpandOutput::Nodes),
        ]))),
        ops(vec![filter(kind_b())]),
    )
    .unwrap();
    let logical::LogicalExpr::RootPipeline(pipeline) =
        rewritten(apply(&logical::LogicalExpr::RootPipeline(after_access)))
    else {
        panic!("expected a root pipeline");
    };
    assert_eq!(membership_predicate(&pipeline.ops()[0]), &kind_b());
    let nested = logical::RootPipeline::new(
        logical::RootStream::Pipeline(Box::new(variable_root)),
        ops(vec![logical::StreamPipelineOp::Distinct]),
    )
    .unwrap();
    let logical::LogicalExpr::RootPipeline(pipeline) =
        rewritten(apply(&logical::LogicalExpr::RootPipeline(nested)))
    else {
        panic!("expected a root pipeline");
    };
    let logical::RootStream::Pipeline(input) = pipeline.input() else {
        panic!("expected the nested pipeline input");
    };
    assert_eq!(membership_predicate(&input.ops()[1]), &kind_b());

    let input = || {
        logical::RootStream::Access(logical::AccessStream::Pipeline(access_pipeline(vec![
            expand(ir::ExpandOutput::Nodes),
            filter(kind_b()),
        ])))
    };
    let params = crate::context::ParamBindings::default()
        .with_value(name("kind"), helix_ast::value::PropertyValue::from("B"));
    let late_bound = std::collections::BTreeSet::from([name("scope")]);
    let wrappers = [
        logical::LogicalExpr::StreamReserved(logical::StreamReserved::new(
            input(),
            ir::ReservedOp::Path,
        )),
        logical::LogicalExpr::StreamCardinality(
            logical::StreamCardinality::new(input())
                .with_planning_bindings(params.clone(), late_bound.clone()),
        ),
        logical::LogicalExpr::StreamProject(logical::StreamProject::new(
            input(),
            ir::ProjectionPlan::Id,
        )),
        logical::LogicalExpr::StreamAggregate(logical::StreamAggregate::new(
            input(),
            ir::AggregatePlan::Group(name("kind")),
        )),
        logical::LogicalExpr::StreamVariableWrite(logical::StreamVariableWrite::new(
            input(),
            logical::StreamVariableWriteOp::Store(name("rows")),
        )),
    ];
    for wrapper in wrappers {
        let rewritten = rewritten(apply(&wrapper));
        assert_eq!(rewritten.kind(), wrapper.kind());
        let input = match &rewritten {
            logical::LogicalExpr::StreamReserved(reserved) => {
                assert_eq!(reserved.op(), &ir::ReservedOp::Path);
                reserved.input()
            }
            logical::LogicalExpr::StreamCardinality(cardinality) => {
                assert_eq!(cardinality.params(), &params);
                assert_eq!(cardinality.late_bound_params(), &late_bound);
                cardinality.input()
            }
            logical::LogicalExpr::StreamProject(project) => project.input(),
            logical::LogicalExpr::StreamAggregate(aggregate) => aggregate.input(),
            logical::LogicalExpr::StreamVariableWrite(write) => write.input(),
            other => panic!("unexpected rewrite {other:?}"),
        };
        let logical::RootStream::Access(logical::AccessStream::Pipeline(pipeline)) = input else {
            panic!("expected the inline access pipeline");
        };
        assert_eq!(membership_predicate(&pipeline.ops()[1]), &kind_b());
    }

    let non_pipeline = logical::LogicalExpr::StreamProject(logical::StreamProject::new(
        logical::RootStream::VariableSource(logical::VariableSource::new(name("seed"))),
        ir::ProjectionPlan::Id,
    ));
    assert_eq!(apply(&non_pipeline), optimizer::RuleResult::NotApplicable);
}
