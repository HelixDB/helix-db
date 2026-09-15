use super::*;

#[test]
fn wide_literal_sets_preserve_index_branch_limits_and_duplicate_singletons() {
    let rule = AccessFilterIndexRule::default();
    let storage = cost::StorageCostProfile::default();
    let key = catalog::ScopedPropertyKey::try_new("User", "age").unwrap();
    let indexes = catalog::IndexCatalogSnapshot::default().with_node_eq(key.clone());
    let limits = crate::context::PlannerLimits {
        max_index_union_branches: crate::context::IndexUnionBranchLimit::Disabled,
    };
    for duplicate in [false, true] {
        let values = (0..4096)
            .map(|index| if duplicate { 42 } else { index })
            .collect();
        let expr = node_access_filter_expr(
            ir::NodeAccessPlan::LabelScan {
                label: name("User"),
            },
            ir::PredicatePlan::new(helix_ast::expr::Predicate::is_in(
                "age",
                helix_ast::value::PropertyValue::I64Array(values),
            ))
            .unwrap(),
        );
        let result = rule.apply(optimizer::RuleInput {
            expr: &expr,
            storage: &storage,
            indexes: &indexes,
            planner_limits: &limits,
            stats: default_stats(),
        });
        if !duplicate {
            assert_eq!(result, optimizer::RuleResult::NotApplicable);
            continue;
        }
        assert!(
            matches!(logical_access_path(result), logical::AccessPath::Node(path)
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::EqualityIndex { key: actual, value, .. }
                if actual == &key && *value == equality_literal(42)))
        );
    }
}

#[test]
fn access_filter_index_rule_applies_union_branch_limits_without_blocking_singletons() {
    let rule = AccessFilterIndexRule::default();
    let storage = cost::StorageCostProfile::default();
    let age_key = catalog::ScopedPropertyKey::try_new("User", "age").unwrap();
    let indexes = catalog::IndexCatalogSnapshot::default().with_node_eq(age_key.clone());
    let limited = crate::context::PlannerLimits {
        max_index_union_branches: crate::context::IndexUnionBranchLimit::limited(1).unwrap(),
    };
    let disabled = crate::context::PlannerLimits {
        max_index_union_branches: crate::context::IndexUnionBranchLimit::Disabled,
    };
    let union = node_access_filter_expr(
        ir::NodeAccessPlan::LabelScan {
            label: name("User"),
        },
        ir::PredicatePlan::new(helix_ast::expr::Predicate::or(vec![
            helix_ast::expr::Predicate::eq("age", 21),
            helix_ast::expr::Predicate::eq("age", 42),
        ]))
        .unwrap(),
    );
    let singleton_in = node_access_filter_expr(
        ir::NodeAccessPlan::LabelScan {
            label: name("User"),
        },
        ir::PredicatePlan::new(helix_ast::expr::Predicate::is_in(
            "age",
            helix_ast::value::PropertyValue::I64Array(vec![42]),
        ))
        .unwrap(),
    );

    for limits in [&limited, &disabled] {
        assert_eq!(
            rule.apply(optimizer::RuleInput {
                expr: &union,
                storage: &storage,
                indexes: &indexes,
                planner_limits: limits,
                stats: default_stats(),
            }),
            optimizer::RuleResult::NotApplicable
        );
    }
    let singleton = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &singleton_in,
        storage: &storage,
        indexes: &indexes,
        planner_limits: &disabled,
        stats: default_stats(),
    }));
    assert!(matches!(
        singleton,
        logical::AccessPath::Node(path)
            if matches!(
                path.source().as_ref(),
                ir::NodeAccessPlan::EqualityIndex { key, value, .. }
                    if key == &age_key && *value == equality_literal(42)
            )
    ));
}
