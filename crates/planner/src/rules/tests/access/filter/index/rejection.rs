use super::*;

#[test]
fn access_filter_index_rule_declines_missing_inputs_and_keeps_partial_residuals() {
    let rule = AccessFilterIndexRule::default();
    let storage = cost::StorageCostProfile::default();
    let active_key = catalog::ScopedPropertyKey::try_new("User", "active").unwrap();
    let indexes = catalog::IndexCatalogSnapshot::default().with_node_eq(active_key);
    let missing_label = node_access_filter_expr(
        ir::NodeAccessPlan::AllScan,
        ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap(),
    );
    let missing_index = node_access_filter_expr(
        ir::NodeAccessPlan::LabelScan {
            label: name("User"),
        },
        ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("missing", true)).unwrap(),
    );
    let uncovered_residual = node_access_filter_expr(
        ir::NodeAccessPlan::LabelScan {
            label: name("User"),
        },
        ir::PredicatePlan::new(helix_ast::expr::Predicate::and(vec![
            helix_ast::expr::Predicate::eq("active", true),
            helix_ast::expr::Predicate::eq("tier", "free"),
        ]))
        .unwrap(),
    );

    for expr in [
        missing_label,
        missing_index,
        source(properties::ElementKind::Node),
    ] {
        assert_eq!(
            rule.apply(optimizer::RuleInput {
                expr: &expr,
                storage: &storage,
                indexes: &indexes,
                planner_limits: default_planner_limits(),
                stats: default_stats(),
            }),
            optimizer::RuleResult::NotApplicable
        );
    }

    let partial = logical_access_pipeline(rule.apply(optimizer::RuleInput {
        expr: &uncovered_residual,
        storage: &storage,
        indexes: &indexes,
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    assert!(matches!(
        partial.access(),
        logical::AccessPath::Node(path)
            if matches!(
                path.source().as_ref(),
                ir::NodeAccessPlan::EqualityIndex { key, .. }
                    if key.label.as_ref() == "User" && key.property.as_ref() == "active"
            )
    ));
    assert!(matches!(
        partial.ops(),
        [logical::StreamPipelineOp::Filter { predicate }]
            if predicate == &ir::PredicatePlan::new(
                helix_ast::expr::Predicate::eq("tier", "free")
            ).unwrap()
    ));
}
