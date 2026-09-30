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

fn pipeline_over(
    source: ir::NodeAccessPlan,
    ops_list: Vec<logical::StreamPipelineOp>,
) -> logical::AccessPipeline {
    logical::AccessPipeline::new(node_access_path(source), ops(ops_list)).unwrap()
}

fn access_pipeline(ops_list: Vec<logical::StreamPipelineOp>) -> logical::AccessPipeline {
    pipeline_over(ir::NodeAccessPlan::AllScan, ops_list)
}

fn attribute_scan() -> ir::NodeAccessPlan {
    ir::NodeAccessPlan::LabelScan {
        label: name("Attribute"),
    }
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

fn rewritten_access_pipeline(expr: &logical::LogicalExpr) -> logical::AccessPipeline {
    let logical::LogicalExpr::AccessPipeline(pipeline) = rewritten(apply(expr)) else {
        panic!("expected an access pipeline");
    };
    pipeline
}

fn membership(op: &logical::StreamPipelineOp) -> &ir::NodeIndexMembershipPlan {
    let logical::StreamPipelineOp::IndexMembership { plan } = op else {
        panic!("expected index membership, got {op:?}");
    };
    plan
}

fn membership_predicate(op: &logical::StreamPipelineOp) -> &helix_ast::expr::Predicate {
    membership(op).predicate().as_ref()
}

#[test]
fn rewrites_the_node_filter_behind_the_source_with_its_residual_fused() {
    let suffix = logical::StreamPipelineOp::Limit {
        count: ir::StreamBoundPlan::Literal(3),
    };
    let title = helix_ast::expr::Predicate::contains("title", "x");
    let whole = helix_ast::expr::Predicate::and(vec![kind_b(), title.clone()]);
    let expr = logical::LogicalExpr::AccessPipeline(access_pipeline(vec![
        expand(ir::ExpandOutput::Nodes),
        filter(whole.clone()),
        suffix.clone(),
    ]));

    let pipeline = rewritten_access_pipeline(&expr);
    assert_eq!(
        AccessPipelineMembershipFilterRule::default()
            .metadata()
            .id
            .as_ref(),
        "access_pipeline_membership_filter"
    );
    let [expanded, fused, limit] = pipeline.ops() else {
        panic!("expected expand, membership, limit: {:?}", pipeline.ops());
    };
    assert_eq!(expanded, &expand(ir::ExpandOutput::Nodes));
    assert_eq!(membership_predicate(fused), &whole);
    assert_eq!(
        membership(fused).residual().map(AsRef::as_ref),
        Some(&title)
    );
    assert_eq!(limit, &suffix);
}

#[test]
fn rewrites_every_eligible_filter_in_one_application() {
    let status = helix_ast::expr::Predicate::eq("status", "live");
    let title = helix_ast::expr::Predicate::contains("title", "x");
    let whole = helix_ast::expr::Predicate::and(vec![status, title.clone()]);
    let expr = logical::LogicalExpr::AccessPipeline(access_pipeline(vec![
        expand(ir::ExpandOutput::Nodes),
        filter(kind_b()),
        expand(ir::ExpandOutput::Nodes),
        filter(whole.clone()),
    ]));

    let rewritten = rewritten(apply(&expr));
    let logical::LogicalExpr::AccessPipeline(pipeline) = &rewritten else {
        panic!("expected an access pipeline");
    };
    let [_, first, _, second] = pipeline.ops() else {
        panic!("expected two memberships: {:?}", pipeline.ops());
    };
    assert_eq!(membership_predicate(first), &kind_b());
    assert_eq!(membership(first).residual(), None);
    assert_eq!(membership_predicate(second), &whole);
    assert_eq!(
        membership(second).residual().map(AsRef::as_ref),
        Some(&title)
    );
    // The output has no eligible filter left, so the rewrite is idempotent.
    assert_eq!(apply(&rewritten), optimizer::RuleResult::NotApplicable);
}

#[test]
fn declines_labeled_source_filters_edge_streams_unindexed_filters_and_other_exprs() {
    let scoped = helix_ast::expr::Predicate::and(vec![
        helix_ast::expr::Predicate::eq("$label", "Attribute"),
        kind_b(),
    ]);
    let declined = [
        // A labeled source's leading filter belongs to the source-index rule.
        logical::LogicalExpr::AccessPipeline(pipeline_over(
            attribute_scan(),
            vec![filter(kind_b()), expand(ir::ExpandOutput::Nodes)],
        )),
        node_access_filter_expr(attribute_scan(), ir::PredicatePlan::new(kind_b()).unwrap()),
        // A label-scoped predicate gives the source-index rule its label.
        node_access_filter_expr(
            ir::NodeAccessPlan::PointIds {
                ids: element_ids(vec![1, 2]),
            },
            ir::PredicatePlan::new(scoped.clone()).unwrap(),
        ),
        logical::LogicalExpr::AccessPipeline(pipeline_over(
            ir::NodeAccessPlan::PointIds {
                ids: element_ids(vec![1, 2]),
            },
            vec![filter(scoped), expand(ir::ExpandOutput::Nodes)],
        )),
        // An empty source collapses instead.
        node_access_filter_expr(
            ir::NodeAccessPlan::Empty,
            ir::PredicatePlan::new(kind_b()).unwrap(),
        ),
        logical::LogicalExpr::AccessPipeline(access_pipeline(vec![
            expand(ir::ExpandOutput::Edges),
            filter(kind_b()),
        ])),
        logical::LogicalExpr::AccessPipeline(access_pipeline(vec![
            expand(ir::ExpandOutput::Nodes),
            filter(helix_ast::expr::Predicate::eq("color", "red")),
        ])),
        edge_access_filter_expr(
            ir::EdgeAccessPlan::AllScan,
            ir::PredicatePlan::new(kind_b()).unwrap(),
        ),
        node_all_expr(),
    ];

    for expr in declined {
        assert_eq!(
            apply(&expr),
            optimizer::RuleResult::NotApplicable,
            "{expr:?}"
        );
    }
}

#[test]
fn rewrites_label_less_leading_filters_and_access_filters() {
    let status = helix_ast::expr::Predicate::eq("status", "live");
    for source in [
        ir::NodeAccessPlan::PointIds {
            ids: element_ids(vec![1, 2]),
        },
        ir::NodeAccessPlan::FromParam { param: name("ids") },
        ir::NodeAccessPlan::FromVar {
            variable: name("rows"),
        },
        ir::NodeAccessPlan::AllScan,
    ] {
        let filter_expr =
            node_access_filter_expr(source.clone(), ir::PredicatePlan::new(kind_b()).unwrap());
        let pipeline = rewritten_access_pipeline(&filter_expr);
        assert_eq!(pipeline.access(), &node_access_path(source.clone()));
        let [only] = pipeline.ops() else {
            panic!("expected one membership: {:?}", pipeline.ops());
        };
        assert_eq!(membership_predicate(only), &kind_b());
        assert_eq!(
            apply(&logical::LogicalExpr::AccessPipeline(pipeline.clone())),
            optimizer::RuleResult::NotApplicable
        );

        let pipeline =
            rewritten_access_pipeline(&logical::LogicalExpr::AccessPipeline(pipeline_over(
                source.clone(),
                vec![
                    filter(kind_b()),
                    expand(ir::ExpandOutput::Nodes),
                    filter(status.clone()),
                ],
            )));
        let [leading, _, later] = pipeline.ops() else {
            panic!("expected two memberships: {:?}", pipeline.ops());
        };
        assert_eq!(membership_predicate(leading), &kind_b());
        assert_eq!(membership_predicate(later), &status);

        // Root wrappers rewrite the lone access filter they inline.
        let wrapped = logical::LogicalExpr::StreamProject(logical::StreamProject::new(
            logical::RootStream::Access(logical::AccessStream::Filter(logical::AccessFilter::new(
                node_access_path(source),
                ir::PredicatePlan::new(kind_b()).unwrap(),
            ))),
            ir::ProjectionPlan::Id,
        ));
        let logical::LogicalExpr::StreamProject(project) = rewritten(apply(&wrapped)) else {
            panic!("expected a projection");
        };
        let logical::RootStream::Access(logical::AccessStream::Pipeline(pipeline)) =
            project.input()
        else {
            panic!("expected an inline membership pipeline");
        };
        assert_eq!(membership_predicate(&pipeline.ops()[0]), &kind_b());
    }
}

#[test]
fn rewrites_unknown_element_streams() {
    for op in [
        logical::PureStreamVariableOp::Select(name("rows")),
        logical::PureStreamVariableOp::Inject(name("rows")),
    ] {
        let pipeline = rewritten_access_pipeline(&logical::LogicalExpr::AccessPipeline(
            access_pipeline(vec![
                expand(ir::ExpandOutput::Edges),
                logical::StreamPipelineOp::Variable { op },
                filter(kind_b()),
            ]),
        ));
        assert_eq!(membership_predicate(&pipeline.ops()[2]), &kind_b());
    }

    let variable_root = logical::LogicalExpr::RootPipeline(
        logical::RootPipeline::new(
            logical::RootStream::VariableSource(logical::VariableSource::new(name("seed"))),
            ops(vec![filter(kind_b())]),
        )
        .unwrap(),
    );
    let logical::LogicalExpr::RootPipeline(pipeline) = rewritten(apply(&variable_root)) else {
        panic!("expected a root pipeline");
    };
    assert_eq!(membership_predicate(&pipeline.ops()[0]), &kind_b());
}

#[test]
fn element_preserving_variable_ops_keep_node_streams_rewritable() {
    for op in [
        logical::PureStreamVariableOp::Bind(name("row")),
        logical::PureStreamVariableOp::Within(name("allowed")),
        logical::PureStreamVariableOp::Without(name("denied")),
    ] {
        let pipeline = rewritten_access_pipeline(&logical::LogicalExpr::AccessPipeline(
            access_pipeline(vec![
                expand(ir::ExpandOutput::Nodes),
                logical::StreamPipelineOp::Variable { op },
                filter(kind_b()),
            ]),
        ));
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
    // non-rewritable suffix still rewrites the nested input stream.
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

    // Own operators and the input stream are rewritten in one application.
    let status = helix_ast::expr::Predicate::eq("status", "live");
    let both = logical::LogicalExpr::RootPipeline(
        logical::RootPipeline::new(
            input(),
            ops(vec![
                expand(ir::ExpandOutput::Nodes),
                filter(status.clone()),
            ]),
        )
        .unwrap(),
    );
    let rewritten_both = rewritten(apply(&both));
    let logical::LogicalExpr::RootPipeline(pipeline) = &rewritten_both else {
        panic!("expected a root pipeline");
    };
    assert_eq!(membership_predicate(&pipeline.ops()[1]), &status);
    let logical::RootStream::Access(logical::AccessStream::Pipeline(input_pipeline)) =
        pipeline.input()
    else {
        panic!("expected the inline access pipeline");
    };
    assert_eq!(membership_predicate(&input_pipeline.ops()[1]), &kind_b());
    assert_eq!(apply(&rewritten_both), optimizer::RuleResult::NotApplicable);

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
