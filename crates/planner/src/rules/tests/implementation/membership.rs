use super::*;

fn indexes() -> catalog::IndexCatalogSnapshot {
    catalog::IndexCatalogSnapshot::default()
        .with_node_eq(catalog::ScopedPropertyKey::try_new("Attribute", "kind").unwrap())
}

/// The implementation rules of every expression kind the membership rewrite
/// matches.
fn implementation_rules() -> [Box<dyn optimizer::OptimizerRule>; 8] {
    [
        Box::new(AccessFilterImplementationRule::default()),
        Box::new(AccessPipelineImplementationRule::default()),
        Box::new(RootPipelineImplementationRule::default()),
        Box::new(StreamReservedImplementationRule::default()),
        Box::new(StreamCardinalityImplementationRule::default()),
        Box::new(StreamProjectImplementationRule::default()),
        Box::new(StreamAggregateImplementationRule::default()),
        Box::new(StreamVariableWriteImplementationRule::default()),
    ]
}

/// Apply the one implementation rule for `expr`'s kind.
fn implement(expr: &logical::LogicalExpr) -> optimizer::RuleResult {
    let rules = implementation_rules();
    let [rule] = rules
        .iter()
        .filter(|rule| rule.metadata().applicability.matches(expr))
        .collect::<Vec<_>>()[..]
    else {
        panic!("expected one implementation rule for {:?}", expr.kind());
    };
    rule.apply(optimizer::RuleInput {
        expr,
        storage: &cost::StorageCostProfile::default(),
        indexes: &indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    })
}

/// One expression of each kind whose only stream filter is `predicate`.
fn exprs(predicate: helix_ast::expr::Predicate) -> [logical::LogicalExpr; 8] {
    let predicate = ir::PredicatePlan::new(predicate).unwrap();
    let points = || {
        node_access_path(ir::NodeAccessPlan::PointIds {
            ids: element_ids(vec![1, 2]),
        })
    };
    let pipeline = || {
        logical::AccessPipeline::new(
            points(),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOp::Expand {
                    plan: ir::ExpandPlan {
                        direction: ir::ExpandDirection::Out,
                        output: ir::ExpandOutput::Nodes,
                        label: ir::ExpandLabelPlan::Label(name("HAS_ATTRIBUTE")),
                    },
                },
                vec![logical::StreamPipelineOp::Filter {
                    predicate: predicate.clone(),
                }],
            ),
        )
        .unwrap()
    };
    let input = || logical::RootStream::Access(logical::AccessStream::Pipeline(pipeline()));
    [
        logical::LogicalExpr::AccessFilter(logical::AccessFilter::new(points(), predicate.clone())),
        logical::LogicalExpr::AccessPipeline(pipeline()),
        logical::LogicalExpr::RootPipeline(
            logical::RootPipeline::new(
                logical::RootStream::VariableSource(logical::VariableSource::new(name("seed"))),
                ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Filter {
                    predicate: predicate.clone(),
                }),
            )
            .unwrap(),
        ),
        logical::LogicalExpr::StreamReserved(logical::StreamReserved::new(
            input(),
            ir::ReservedOp::Path,
        )),
        logical::LogicalExpr::StreamCardinality(logical::StreamCardinality::new(input())),
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
    ]
}

fn is_physical(result: &optimizer::RuleResult) -> bool {
    matches!(
        result,
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(_))
    )
}

#[test]
fn implementation_rules_defer_eligible_filters_to_membership() {
    let expected = [
        logical::LogicalExprKind::AccessFilter,
        logical::LogicalExprKind::AccessPipeline,
        logical::LogicalExprKind::RootPipeline,
        logical::LogicalExprKind::StreamReserved,
        logical::LogicalExprKind::StreamCardinality,
        logical::LogicalExprKind::StreamProject,
        logical::LogicalExprKind::StreamAggregate,
        logical::LogicalExprKind::StreamVariableWrite,
    ];
    assert_eq!(STREAM_MEMBERSHIP_KINDS, expected);

    for expr in exprs(helix_ast::expr::Predicate::eq("kind", "B")) {
        assert_eq!(
            implement(&expr),
            optimizer::RuleResult::NotApplicable,
            "{expr:?}"
        );
        let rewritten = membership_rewrite(&expr, &indexes(), default_planner_limits())
            .unwrap_or_else(|| panic!("expected a membership rewrite of {expr:?}"));
        // The rewrite leaves nothing to rewrite, so its output is implemented.
        assert_eq!(
            membership_rewrite(&rewritten, &indexes(), default_planner_limits()),
            None
        );
        assert!(is_physical(&implement(&rewritten)), "{rewritten:?}");
    }

    // A filter membership cannot serve keeps its per-row implementation.
    for expr in exprs(helix_ast::expr::Predicate::gte("rank", 3)) {
        assert_eq!(
            membership_rewrite(&expr, &indexes(), default_planner_limits()),
            None
        );
        assert!(is_physical(&implement(&expr)), "{expr:?}");
    }
}

#[test]
fn stream_membership_candidates_are_required_rewrites_over_exactly_their_kinds() {
    let applicability = RuleApplicability::stream_membership_candidate();
    assert!(applicability.is_required_rewrite());
    assert_eq!(
        AccessPipelineMembershipFilterRule::default()
            .metadata()
            .applicability,
        applicability
    );
    for expr in exprs(helix_ast::expr::Predicate::eq("kind", "B")) {
        assert!(applicability.matches(&expr), "{:?}", expr.kind());
    }
    assert!(!applicability.matches(&node_all_expr()));
    assert!(!applicability.matches(&source(properties::ElementKind::Node)));
}
