use super::*;

#[test]
fn access_filter_index_rule_rewrites_or_and_literal_in_to_unions() {
    let rule = AccessFilterIndexRule::default();
    let storage = cost::StorageCostProfile::default();
    let node_age_key = catalog::ScopedPropertyKey::try_new("User", "age").unwrap();
    let edge_weight_key = catalog::ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap();
    let indexes = catalog::IndexCatalogSnapshot::default()
        .with_node_eq(node_age_key.clone())
        .with_edge_eq(edge_weight_key.clone());
    let node_expr = node_access_filter_expr(
        ir::NodeAccessPlan::LabelScan {
            label: name("User"),
        },
        ir::PredicatePlan::new(helix_ast::expr::Predicate::or(vec![
            helix_ast::expr::Predicate::eq("age", 21),
            helix_ast::expr::Predicate::eq("age", 42),
        ]))
        .unwrap(),
    );
    let edge_expr = edge_access_filter_expr(
        ir::EdgeAccessPlan::LabelScan {
            label: name("FOLLOWS"),
        },
        ir::PredicatePlan::new(helix_ast::expr::Predicate::is_in(
            "weight",
            helix_ast::value::PropertyValue::I64Array(vec![7, 9]),
        ))
        .unwrap(),
    );

    let node = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &node_expr,
        storage: &storage,
        indexes: &indexes,
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let edge = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &edge_expr,
        storage: &storage,
        indexes: &indexes,
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    let logical::AccessPath::Node(node) = node else {
        panic!("expected node access");
    };
    let ir::NodeAccessPlan::Union(node_children) = node.source().as_ref() else {
        panic!("expected node union");
    };
    assert!(matches!(
        node_children.as_ref()[0].as_ref(),
        ir::NodeAccessPlan::EqualityIndex { key, value, .. }
            if key == &node_age_key && *value == equality_literal(21)
    ));
    assert!(matches!(
        node_children.as_ref()[1].as_ref(),
        ir::NodeAccessPlan::EqualityIndex { key, value, .. }
            if key == &node_age_key && *value == equality_literal(42)
    ));

    let logical::AccessPath::Edge(edge) = edge else {
        panic!("expected edge access");
    };
    let ir::EdgeAccessPlan::Union(edge_children) = edge.source().as_ref() else {
        panic!("expected edge union");
    };
    assert!(matches!(
        edge_children.as_ref()[0].as_ref(),
        ir::EdgeAccessPlan::EqualityIndex { key, value, .. }
            if key == &edge_weight_key && *value == equality_literal(7)
    ));
    assert!(matches!(
        edge_children.as_ref()[1].as_ref(),
        ir::EdgeAccessPlan::EqualityIndex { key, value, .. }
            if key == &edge_weight_key && *value == equality_literal(9)
    ));
}

/// Equality indexes on `Item.p0`, `Item.p1`, and `Item.p2`; `keep` and
/// `title` have none.
fn partial_or_indexes() -> catalog::IndexCatalogSnapshot {
    ["p0", "p1", "p2"].into_iter().fold(
        catalog::IndexCatalogSnapshot::default(),
        |indexes, property| {
            indexes.with_node_eq(catalog::ScopedPropertyKey::try_new("Item", property).unwrap())
        },
    )
}

/// The index rule's rewrite of `predicate` over a label scan of `Item`.
fn rewrite_item_filter(
    predicate: helix_ast::expr::Predicate,
    planner_limits: &crate::context::PlannerLimits,
) -> optimizer::RuleResult {
    AccessFilterIndexRule::default().apply(optimizer::RuleInput {
        expr: &node_access_filter_expr(
            ir::NodeAccessPlan::LabelScan {
                label: name("Item"),
            },
            ir::PredicatePlan::new(predicate).unwrap(),
        ),
        storage: &cost::StorageCostProfile::default(),
        indexes: &partial_or_indexes(),
        planner_limits,
        stats: default_stats(),
    })
}

/// Sorted equality-index properties read by `plan`, recursively through
/// intersections and unions.
fn indexed_properties(plan: &ir::NodeAccessPlan) -> Vec<String> {
    let mut properties = match plan {
        ir::NodeAccessPlan::EqualityIndex { key, .. } => vec![key.property.to_string()],
        ir::NodeAccessPlan::Intersect(children) | ir::NodeAccessPlan::Union(children) => children
            .iter()
            .flat_map(|child| indexed_properties(child.as_ref()))
            .collect(),
        other => panic!("expected an index-only set: {other:?}"),
    };
    properties.sort_unstable();
    properties
}

fn residual_branches(
    path: &logical::AccessPath,
) -> Vec<(Vec<String>, Option<helix_ast::expr::Predicate>)> {
    let logical::AccessPath::Node(path) = path else {
        panic!("expected a node access path");
    };
    let ir::NodeAccessPlan::BranchResidualUnion(branches) = path.source().as_ref() else {
        panic!("expected a branch-residual union: {path:?}");
    };
    branches
        .as_ref()
        .iter()
        .map(|branch| {
            (
                indexed_properties(branch.source().as_ref()),
                branch.residual().map(|residual| residual.as_ref().clone()),
            )
        })
        .collect()
}

#[test]
fn partial_or_uses_branch_residual_union_without_label_scan() {
    let keep = helix_ast::expr::Predicate::eq("keep", 1);
    let predicate = helix_ast::expr::Predicate::or(vec![
        helix_ast::expr::Predicate::and(vec![
            helix_ast::expr::Predicate::eq("p0", 0),
            keep.clone(),
        ]),
        helix_ast::expr::Predicate::eq("p1", 0),
    ]);
    let path = logical_access_path(rewrite_item_filter(predicate, default_planner_limits()));
    // Each branch reads its own index; only `keep` is evaluated, and only on
    // the `p0` branch's rows.
    assert_eq!(
        residual_branches(&path),
        [
            (vec!["p0".to_string()], Some(keep)),
            (vec!["p1".to_string()], None),
        ]
    );
}

#[test]
fn and_over_partial_or_distributes_the_shared_index() {
    let title = helix_ast::expr::Predicate::contains("title", "x");
    let predicate = helix_ast::expr::Predicate::and(vec![
        helix_ast::expr::Predicate::eq("p0", 1),
        helix_ast::expr::Predicate::or(vec![
            helix_ast::expr::Predicate::and(vec![
                helix_ast::expr::Predicate::eq("p1", 2),
                title.clone(),
            ]),
            helix_ast::expr::Predicate::eq("p2", 3),
        ]),
    ]);
    let path = logical_access_path(rewrite_item_filter(predicate, default_planner_limits()));
    assert_eq!(
        residual_branches(&path),
        [
            (vec!["p0".to_string(), "p1".to_string()], Some(title)),
            (vec!["p0".to_string(), "p2".to_string()], None),
        ]
    );
}

#[test]
fn fully_indexed_or_keeps_union_shape() {
    // `p0 == 1 AND (p1 == 2 OR p2 == 3)`: the shared set is read once and
    // intersected with one exact union, with nothing left per row.
    let predicate = helix_ast::expr::Predicate::and(vec![
        helix_ast::expr::Predicate::eq("p0", 1),
        helix_ast::expr::Predicate::or(vec![
            helix_ast::expr::Predicate::eq("p1", 2),
            helix_ast::expr::Predicate::eq("p2", 3),
        ]),
    ]);
    let logical::AccessPath::Node(path) =
        logical_access_path(rewrite_item_filter(predicate, default_planner_limits()))
    else {
        panic!("expected a node access path");
    };
    let ir::NodeAccessPlan::Intersect(children) = path.source().as_ref() else {
        panic!("expected an intersection: {path:?}");
    };
    assert!(matches!(
        children.as_ref(),
        [shared, union]
            if indexed_properties(shared.as_ref()) == ["p0"]
                && matches!(union.as_ref(), ir::NodeAccessPlan::Union(_))
                && indexed_properties(union.as_ref()) == ["p1", "p2"]
    ));
}

#[test]
fn or_with_unindexed_branch_scans() {
    // A branch no index narrows may match any row, so the filter stays per
    // row; that reads no record the scan would not read anyway.
    for predicate in [
        helix_ast::expr::Predicate::or(vec![
            helix_ast::expr::Predicate::eq("p0", 1),
            helix_ast::expr::Predicate::contains("title", "x"),
        ]),
        helix_ast::expr::Predicate::or(vec![
            helix_ast::expr::Predicate::and(vec![
                helix_ast::expr::Predicate::eq("p0", 1),
                helix_ast::expr::Predicate::eq("keep", 1),
            ]),
            helix_ast::expr::Predicate::eq("keep", 2),
        ]),
    ] {
        assert_eq!(
            rewrite_item_filter(predicate, default_planner_limits()),
            optimizer::RuleResult::NotApplicable
        );
    }
}

#[test]
fn distribution_over_cap_keeps_index_superset_and_residual() {
    // With a limit of one branch the conjunction cannot distribute, so the
    // union of the disjunction's sets still narrows the rows and the whole
    // disjunction is their residual.
    let disjunction = helix_ast::expr::Predicate::or(vec![
        helix_ast::expr::Predicate::and(vec![
            helix_ast::expr::Predicate::eq("p1", 2),
            helix_ast::expr::Predicate::contains("title", "x"),
        ]),
        helix_ast::expr::Predicate::eq("p2", 3),
    ]);
    let predicate = helix_ast::expr::Predicate::and(vec![
        helix_ast::expr::Predicate::eq("p0", 1),
        disjunction.clone(),
    ]);
    let limits = crate::context::PlannerLimits {
        max_index_union_branches: crate::context::IndexUnionBranchLimit::limited(1).unwrap(),
    };
    let pipeline = logical_access_pipeline(rewrite_item_filter(predicate, &limits));
    let logical::AccessPath::Node(path) = pipeline.access() else {
        panic!("expected a node access path");
    };
    assert_eq!(
        indexed_properties(path.source().as_ref()),
        ["p0", "p1", "p2"]
    );
    assert!(matches!(
        pipeline.ops(),
        [logical::StreamPipelineOp::Filter { predicate }] if predicate.as_ref() == &disjunction
    ));
}

#[test]
fn disabled_limit_keeps_or_residual() {
    let disabled = crate::context::PlannerLimits {
        max_index_union_branches: crate::context::IndexUnionBranchLimit::Disabled,
    };
    let disjunction = helix_ast::expr::Predicate::or(vec![
        helix_ast::expr::Predicate::eq("p1", 2),
        helix_ast::expr::Predicate::eq("p2", 3),
    ]);
    assert_eq!(
        rewrite_item_filter(disjunction.clone(), &disabled),
        optimizer::RuleResult::NotApplicable
    );
    // A conjunct outside the disjunction still narrows the source.
    let pipeline = logical_access_pipeline(rewrite_item_filter(
        helix_ast::expr::Predicate::and(vec![
            helix_ast::expr::Predicate::eq("p0", 1),
            disjunction.clone(),
        ]),
        &disabled,
    ));
    let logical::AccessPath::Node(path) = pipeline.access() else {
        panic!("expected a node access path");
    };
    assert_eq!(indexed_properties(path.source().as_ref()), ["p0"]);
    assert!(matches!(
        pipeline.ops(),
        [logical::StreamPipelineOp::Filter { predicate }] if predicate.as_ref() == &disjunction
    ));
}
