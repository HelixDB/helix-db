use super::*;

#[test]
fn access_window_rule_collapses_windows_exhausting_known_cardinality_bounds() {
    let rule = AccessWindowRule::default();
    let storage = cost::StorageCostProfile::default();
    let unique = node_access_window_expr(
        ir::NodeAccessPlan::EqualityIndex {
            index: catalog::NodeEqualityIndexMeta::try_new("user_email")
                .unwrap()
                .with_uniqueness(catalog::IndexUniqueness::Unique),
            key: catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
            value: equality_literal(1),
        },
        logical::AccessWindowRange::new(1, None).unwrap(),
    );
    let empty = node_access_window_expr(
        ir::NodeAccessPlan::Empty,
        logical::AccessWindowRange::new(0, None).unwrap(),
    );
    let union = node_access_window_expr(
        ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::PointIds {
                ids: element_ids(vec![1]),
            })
            .unwrap(),
            ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::PointIds {
                ids: element_ids(vec![2]),
            })
            .unwrap(),
        )),
        logical::AccessWindowRange::new(2, None).unwrap(),
    );
    let intersection = edge_access_window_expr(
        ir::EdgeAccessPlan::Intersect(ir::AtLeast::<_, 2>::from_pair(
            ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::PointIds {
                ids: element_ids(vec![10, 20]),
            })
            .unwrap(),
            ir::EdgeAccessSourcePlan::new(edge_text_search(search_limit(5))).unwrap(),
        )),
        logical::AccessWindowRange::new(2, Some(3)).unwrap(),
    );

    for expr in [unique, empty, union] {
        let rewritten = logical_access_path(rule.apply(optimizer::RuleInput {
            expr: &expr,
            storage: &storage,
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        }));
        assert!(matches!(
            rewritten,
            logical::AccessPath::Node(path)
                if matches!(path.source().as_ref(), ir::NodeAccessPlan::Empty)
        ));
    }

    let rewritten = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &intersection,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    assert!(matches!(
        rewritten,
        logical::AccessPath::Edge(path)
            if matches!(path.source().as_ref(), ir::EdgeAccessPlan::Empty)
    ));
}

#[test]
fn access_window_rule_elides_prefix_windows_covering_known_cardinality_bounds() {
    let rule = AccessWindowRule::default();
    let storage = cost::StorageCostProfile::default();
    let unique = node_access_window_expr(
        ir::NodeAccessPlan::EqualityIndex {
            index: catalog::NodeEqualityIndexMeta::try_new("user_email")
                .unwrap()
                .with_uniqueness(catalog::IndexUniqueness::Unique),
            key: catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
            value: equality_literal(1),
        },
        logical::AccessWindowRange::new(0, Some(1)).unwrap(),
    );
    let points = node_access_window_expr(
        ir::NodeAccessPlan::PointIds {
            ids: element_ids(vec![1, 2, 3]),
        },
        logical::AccessWindowRange::new(0, Some(3)).unwrap(),
    );
    let search = edge_access_window_expr(
        edge_text_search(search_limit(3)),
        logical::AccessWindowRange::new(0, Some(5)).unwrap(),
    );

    let unique = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &unique,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    assert!(matches!(
        unique,
        logical::AccessPath::Node(path)
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::EqualityIndex { .. })
    ));

    let points = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &points,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    assert!(matches!(
        points,
        logical::AccessPath::Node(path)
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::PointIds { ids } if ids.as_ref() == [1, 2, 3])
    ));

    let search = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &search,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    assert!(matches!(
        search,
        logical::AccessPath::Edge(path)
            if matches!(
                path.source().as_ref(),
                ir::EdgeAccessPlan::TextSearch {
                    k: ir::SearchLimitPlan::Literal(k),
                    ..
                } if k.get() == 3
            )
    ));
}

#[test]
fn access_window_rule_declines_exhaustion_without_a_known_upper_bound() {
    let rule = AccessWindowRule::default();
    let storage = cost::StorageCostProfile::default();
    let non_unique = node_access_window_expr(
        ir::NodeAccessPlan::EqualityIndex {
            index: catalog::NodeEqualityIndexMeta::try_new("user_email").unwrap(),
            key: catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
            value: equality_literal(1),
        },
        logical::AccessWindowRange::new(1, None).unwrap(),
    );
    let runtime_search = edge_access_window_expr(
        edge_text_search(ir::SearchLimitPlan::Expr(
            ir::SearchLimitExprPlan::new(helix_ast::expr::Expr::param("k")).unwrap(),
        )),
        logical::AccessWindowRange::new(1, None).unwrap(),
    );

    for expr in [non_unique, runtime_search] {
        assert_eq!(
            rule.apply(optimizer::RuleInput {
                expr: &expr,
                storage: &storage,
                indexes: empty_indexes(),
                planner_limits: default_planner_limits(),
                stats: default_stats(),
            }),
            optimizer::RuleResult::NotApplicable
        );
    }
}
