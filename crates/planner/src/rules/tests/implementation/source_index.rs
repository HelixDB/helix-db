use super::*;

fn indexes() -> catalog::IndexCatalogSnapshot {
    catalog::IndexCatalogSnapshot::default()
        .with_node_eq(catalog::ScopedPropertyKey::try_new("Item", "p0").unwrap())
        .with_node_eq(catalog::ScopedPropertyKey::try_new("Item", "p1").unwrap())
}

/// A row-preserving operator after the leading filter.
fn expand() -> logical::StreamPipelineOp {
    logical::StreamPipelineOp::Expand {
        plan: ir::ExpandPlan {
            direction: ir::ExpandDirection::Out,
            output: ir::ExpandOutput::Nodes,
            label: ir::ExpandLabelPlan::Label(name("HAS_ATTRIBUTE")),
        },
    }
}

/// The implementation rules of every expression kind the source-index
/// rewrite matches.
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

/// Apply the one implementation rule for `expr`'s kind, including the
/// access-path rule that implements a filter the index answers completely.
fn implement(expr: &logical::LogicalExpr) -> optimizer::RuleResult {
    let access_path: Box<dyn optimizer::OptimizerRule> =
        Box::new(AccessPathImplementationRule::default());
    let rules = implementation_rules()
        .into_iter()
        .chain(std::iter::once(access_path))
        .collect::<Vec<_>>();
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

fn rewrite(expr: &logical::LogicalExpr) -> Option<logical::LogicalExpr> {
    source_index_rewrite(expr, &indexes(), default_planner_limits())
}

fn item_scan() -> logical::AccessPath {
    node_access_path(ir::NodeAccessPlan::LabelScan {
        label: name("Item"),
    })
}

/// One expression of each kind whose only stream filter is `predicate`,
/// leading a label scan of `Item`.
fn exprs(predicate: helix_ast::expr::Predicate) -> [logical::LogicalExpr; 8] {
    let predicate = ir::PredicatePlan::new(predicate).unwrap();
    let filter = || logical::AccessFilter::new(item_scan(), predicate.clone());
    let pipeline = || {
        logical::AccessPipeline::new(
            item_scan(),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOp::Filter {
                    predicate: predicate.clone(),
                },
                vec![expand()],
            ),
        )
        .unwrap()
    };
    let input = || logical::RootStream::Access(logical::AccessStream::Filter(filter()));
    [
        logical::LogicalExpr::AccessFilter(filter()),
        logical::LogicalExpr::AccessPipeline(pipeline()),
        logical::LogicalExpr::RootPipeline(
            logical::RootPipeline::new(
                logical::RootStream::Access(logical::AccessStream::Pipeline(pipeline())),
                ir::AtLeast::<_, 1>::from_one(expand()),
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

/// Every predicate of every stream filter or access filter inlined in `expr`.
fn stream_filter_predicates(expr: &logical::LogicalExpr) -> Vec<helix_ast::expr::Predicate> {
    fn access(stream: &logical::AccessStream, out: &mut Vec<helix_ast::expr::Predicate>) {
        match stream {
            logical::AccessStream::Filter(filter) => out.push(filter.predicate().as_ref().clone()),
            logical::AccessStream::Pipeline(pipeline) => ops(pipeline.ops(), out),
            logical::AccessStream::Path(_)
            | logical::AccessStream::Window(_)
            | logical::AccessStream::Order(_)
            | logical::AccessStream::Distinct(_) => {}
        }
    }
    fn ops(ops: &[logical::StreamPipelineOp], out: &mut Vec<helix_ast::expr::Predicate>) {
        out.extend(ops.iter().filter_map(|op| match op {
            logical::StreamPipelineOp::Filter { predicate } => Some(predicate.as_ref().clone()),
            _ => None,
        }));
    }
    fn root(stream: &logical::RootStream, out: &mut Vec<helix_ast::expr::Predicate>) {
        match stream {
            logical::RootStream::Access(stream) => access(stream, out),
            logical::RootStream::Pipeline(pipeline) => {
                root(pipeline.input(), out);
                ops(pipeline.ops(), out);
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    match expr {
        logical::LogicalExpr::AccessFilter(filter) => out.push(filter.predicate().as_ref().clone()),
        logical::LogicalExpr::AccessPipeline(pipeline) => ops(pipeline.ops(), &mut out),
        logical::LogicalExpr::RootPipeline(pipeline) => {
            root(pipeline.input(), &mut out);
            ops(pipeline.ops(), &mut out);
        }
        logical::LogicalExpr::StreamReserved(wrapper) => root(wrapper.input(), &mut out),
        logical::LogicalExpr::StreamCardinality(wrapper) => root(wrapper.input(), &mut out),
        logical::LogicalExpr::StreamProject(wrapper) => root(wrapper.input(), &mut out),
        logical::LogicalExpr::StreamAggregate(wrapper) => root(wrapper.input(), &mut out),
        logical::LogicalExpr::StreamVariableWrite(wrapper) => root(wrapper.input(), &mut out),
        _ => {}
    }
    out
}

#[test]
fn implementation_rules_defer_index_served_leading_filters() {
    for predicate in [
        helix_ast::expr::Predicate::eq("p0", 7),
        helix_ast::expr::Predicate::and(vec![
            helix_ast::expr::Predicate::eq("p0", 7),
            helix_ast::expr::Predicate::eq("p1", 2),
        ]),
        helix_ast::expr::Predicate::and(vec![
            helix_ast::expr::Predicate::eq("p0", 7),
            helix_ast::expr::Predicate::gte("rank", 3),
        ]),
    ] {
        for expr in exprs(predicate) {
            assert_eq!(
                implement(&expr),
                optimizer::RuleResult::NotApplicable,
                "{expr:?}"
            );
            let rewritten =
                rewrite(&expr).unwrap_or_else(|| panic!("expected a source rewrite of {expr:?}"));
            // The rewrite leaves nothing to rewrite, so its output is
            // implemented.
            assert_eq!(rewrite(&rewritten), None);
            assert!(is_physical(&implement(&rewritten)), "{rewritten:?}");
        }
    }

    // A leading filter no property index serves keeps its per-row form.
    for expr in exprs(helix_ast::expr::Predicate::gte("rank", 3)) {
        assert_eq!(rewrite(&expr), None);
        assert!(is_physical(&implement(&expr)), "{expr:?}");
    }
}

#[test]
fn stream_source_index_candidates_are_required_rewrites_over_exactly_their_kinds() {
    let applicability = RuleApplicability::stream_source_index_candidate();
    assert!(applicability.is_required_rewrite());
    assert_eq!(
        AccessSourceIndexFilterRule::default()
            .metadata()
            .applicability,
        applicability
    );
    assert_eq!(
        AccessSourceIndexFilterRule::default()
            .metadata()
            .id
            .as_ref(),
        "access_source_index_filter"
    );
    for expr in exprs(helix_ast::expr::Predicate::eq("p0", 7)) {
        assert!(applicability.matches(&expr), "{:?}", expr.kind());
        assert!(REQUIRED_STREAM_FILTER_KINDS.contains(&expr.kind()));
    }
    assert!(!applicability.matches(&node_all_expr()));
    assert!(!applicability.matches(&source(properties::ElementKind::Node)));
}

#[test]
fn source_index_rewrite_is_idempotent_and_leaves_only_unindexed_residuals() {
    let unindexed = helix_ast::expr::Predicate::gte("rank", 3);
    let text = helix_ast::expr::Predicate::contains("title", "x");
    for predicate in [
        helix_ast::expr::Predicate::and(vec![
            helix_ast::expr::Predicate::eq("p0", 7),
            unindexed.clone(),
        ]),
        helix_ast::expr::Predicate::and(vec![
            unindexed.clone(),
            helix_ast::expr::Predicate::eq("p1", 2),
            text.clone(),
            helix_ast::expr::Predicate::eq("p0", 7),
        ]),
        helix_ast::expr::Predicate::and(vec![
            helix_ast::expr::Predicate::eq("$label", "Item"),
            helix_ast::expr::Predicate::eq("p0", 7),
            text.clone(),
        ]),
    ] {
        for expr in exprs(predicate) {
            let rewritten = rewrite(&expr).unwrap();
            assert_eq!(rewrite(&rewritten), None, "{rewritten:?}");
            // Only conjuncts no index serves are left for per-row evaluation.
            for residual in stream_filter_predicates(&rewritten) {
                let conjuncts = match residual {
                    helix_ast::expr::Predicate::And { predicates } => predicates,
                    predicate => vec![predicate],
                };
                assert!(
                    conjuncts
                        .iter()
                        .all(|conjunct| conjunct == &unindexed || conjunct == &text),
                    "{rewritten:?}"
                );
            }
        }
    }
}

#[test]
fn redundant_filter_on_equal_index_source_is_dropped() {
    let indexes = indexes();
    let key = catalog::ScopedPropertyKey::try_new("Item", "p0").unwrap();
    let lookup = node_access_path(ir::NodeAccessPlan::EqualityIndex {
        index: indexes.node_eq[&key].clone(),
        key,
        value: equality_literal(7),
    });
    let filter = logical::AccessFilter::new(
        lookup.clone(),
        ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("p0", 7)).unwrap(),
    );

    // The source answers the filter exactly: the result is the source
    // itself, not a per-row filter.
    let expr = logical::LogicalExpr::AccessFilter(filter.clone());
    assert_eq!(
        rewrite(&expr),
        Some(logical::LogicalExpr::AccessPath(lookup.clone()))
    );
    assert_eq!(implement(&expr), optimizer::RuleResult::NotApplicable);

    // A leading filter the source answers disappears from its pipeline.
    let pipeline = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            lookup.clone(),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOp::Filter {
                    predicate: filter.predicate().clone(),
                },
                vec![expand()],
            ),
        )
        .unwrap(),
    );
    assert_eq!(
        rewrite(&pipeline),
        Some(logical::LogicalExpr::AccessPipeline(
            logical::AccessPipeline::new(lookup, ir::AtLeast::<_, 1>::from_one(expand()),).unwrap()
        ))
    );
}
