use crate::analysis::{self, FeasibleLabelScope, LabelScope, PrunedPredicate};
use crate::planning::tests::support::*;

#[test]
fn divergent_or_label_scopes_union_residual_free_branch_sources() {
    let node_predicate = Predicate::or(vec![
        Predicate::eq("$label", "User"),
        Predicate::eq("$label", "Account"),
    ]);
    let node_plan = plan_traversal(g().n_where(node_predicate), ctx(builtin_label_indexes()));
    let NodeAccessPlan::Union(node_branches) = node_access(&node_plan) else {
        panic!(
            "expected node label-scan union: {:?}",
            node_access(&node_plan)
        );
    };

    assert_eq!(node_branches.len(), 2);
    assert!(node_branches.iter().any(|source| matches!(
        source.as_ref(),
        NodeAccessPlan::LabelScan { label } if label == "User"
    )));
    assert!(node_branches.iter().any(|source| matches!(
        source.as_ref(),
        NodeAccessPlan::LabelScan { label } if label == "Account"
    )));
    assert_decision(&node_plan, TracePass::AccessPath, TraceDecision::NodeUnion);
    assert_no_decision(&node_plan, TraceDecision::NodeScanOr);

    let edge_predicate = Predicate::or(vec![
        Predicate::eq("$label", "FOLLOWS"),
        Predicate::eq("$label", "LIKES"),
    ]);
    let edge_plan = plan_traversal(g().e_where(edge_predicate), ctx(builtin_label_indexes()));
    let EdgeAccessPlan::Union(edge_branches) = edge_access(&edge_plan) else {
        panic!(
            "expected edge label-scan union: {:?}",
            edge_access(&edge_plan)
        );
    };

    assert_eq!(edge_branches.len(), 2);
    assert!(edge_branches.iter().any(|source| matches!(
        source.as_ref(),
        EdgeAccessPlan::LabelScan { label } if label == "FOLLOWS"
    )));
    assert!(edge_branches.iter().any(|source| matches!(
        source.as_ref(),
        EdgeAccessPlan::LabelScan { label } if label == "LIKES"
    )));
    assert_decision(&edge_plan, TracePass::AccessPath, TraceDecision::EdgeUnion);
    assert_no_decision(&edge_plan, TraceDecision::EdgeScanOr);
}

#[test]
fn divergent_or_label_scopes_with_residuals_union_branch_sources() {
    let node_predicate = Predicate::or(vec![
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::eq("username", "alice"),
            Predicate::starts_with("bio", "engineer"),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "Account"),
            Predicate::eq("email", "billing@example.com"),
            Predicate::contains("notes", "manual"),
        ]),
    ]);
    let node_plan = plan_traversal(
        g().n_where(node_predicate.clone()),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
            .with_node_eq(ScopedPropertyKey::try_new("Account", "email").unwrap())),
    );
    let NodeAccessPlan::ScanThenFilter { source, residual } = node_access(&node_plan) else {
        panic!(
            "expected node residual over union source: {:?}",
            node_access(&node_plan)
        );
    };
    let NodeAccessPlan::Union(node_branches) = source.as_ref() else {
        panic!("expected node union source: {:?}", source.as_ref());
    };

    assert_eq!(node_branches.len(), 2);
    assert_node_eq(
        node_branches,
        "User",
        "username",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("alice")).unwrap()),
    );
    assert_node_eq(
        node_branches,
        "Account",
        "email",
        IndexValue::Literal(
            SecondaryIndexLiteral::new(PropertyValue::from("billing@example.com")).unwrap(),
        ),
    );
    assert_eq!(residual.predicate(), &node_predicate);
    assert_decision(&node_plan, TracePass::AccessPath, TraceDecision::NodeUnion);
    assert_decision(&node_plan, TracePass::AccessPath, TraceDecision::NodeScanOr);

    let edge_predicate = Predicate::or(vec![
        Predicate::and(vec![
            Predicate::eq("$label", "FOLLOWS"),
            Predicate::eq("status", "active"),
            Predicate::starts_with("note", "friend"),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "LIKES"),
            Predicate::eq("tenant_id", "acme"),
            Predicate::contains("note", "team"),
        ]),
    ]);
    let edge_plan = plan_traversal(
        g().e_where(edge_predicate.clone()),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
            .with_edge_eq(ScopedPropertyKey::try_new("LIKES", "tenant_id").unwrap())),
    );
    let EdgeAccessPlan::ScanThenFilter { source, residual } = edge_access(&edge_plan) else {
        panic!(
            "expected edge residual over union source: {:?}",
            edge_access(&edge_plan)
        );
    };
    let EdgeAccessPlan::Union(edge_branches) = source.as_ref() else {
        panic!("expected edge union source: {:?}", source.as_ref());
    };

    assert_eq!(edge_branches.len(), 2);
    assert_edge_eq(
        edge_branches,
        "FOLLOWS",
        "status",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap()),
    );
    assert_edge_eq(
        edge_branches,
        "LIKES",
        "tenant_id",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("acme")).unwrap()),
    );
    assert_eq!(residual.predicate(), &edge_predicate);
    assert_decision(&edge_plan, TracePass::AccessPath, TraceDecision::EdgeUnion);
    assert_decision(&edge_plan, TracePass::AccessPath, TraceDecision::EdgeScanOr);
}

#[test]
fn mixed_or_label_scope_does_not_scope_unlabeled_branches() {
    let predicate = Predicate::or(vec![
        Predicate::eq("$label", "User"),
        Predicate::eq("status", "active"),
    ]);
    let plan = plan_traversal(
        g().n_where(predicate.clone()),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "status").unwrap())),
    );

    let NodeAccessPlan::ScanThenFilter { source, residual } = node_access(&plan) else {
        panic!("expected scan residual: {:?}", node_access(&plan));
    };
    assert!(matches!(source.as_ref(), NodeAccessPlan::AllScan));
    assert_eq!(residual, &predicate);
    assert_no_decision(&plan, TraceDecision::NodeEqualityIndex);
}

#[test]
fn scoped_branch_or_helpers_report_bailouts_and_sources() {
    let default_node_ctx = PlannerContext::default();
    let mut node_planner = crate::planning::Planner::new(&default_node_ctx);
    let empty_node = node_planner
        .node_scoped_branch_or_plan(&[], "test.node")
        .unwrap();
    assert!(empty_node.source.is_none());
    assert!(!empty_node.covered);

    let unscoped_node = node_planner
        .node_scoped_branch_or_plan(&[Predicate::eq("status", "active")], "test.node")
        .unwrap();
    assert!(unscoped_node.source.is_none());
    assert!(!unscoped_node.covered);

    let residual_node = node_planner
        .node_scoped_branch_or_plan(
            &[Predicate::and(vec![
                Predicate::eq("$label", "User"),
                Predicate::contains("bio", "manual"),
            ])],
            "test.node",
        )
        .unwrap();
    assert!(matches!(
        residual_node.source.as_ref().map(AsRef::as_ref),
        Some(NodeAccessPlan::LabelScan { label }) if label == "User"
    ));
    assert!(!residual_node.covered);

    let empty_node_source = node_planner
        .node_scoped_branch_or_plan(
            &[
                Predicate::and(vec![
                    Predicate::eq("$label", "User"),
                    Predicate::eq("$label", "Account"),
                ]),
                Predicate::and(vec![
                    Predicate::eq("$label", "Doc"),
                    Predicate::eq("$label", "Account"),
                ]),
            ],
            "test.node",
        )
        .unwrap();
    assert!(empty_node_source.covered);
    assert!(matches!(
        empty_node_source.source.as_ref().map(AsRef::as_ref),
        Some(NodeAccessPlan::Empty)
    ));

    let single_node = node_planner
        .node_scoped_branch_or_plan(&[Predicate::eq("$label", "User")], "test.node")
        .unwrap();
    assert!(single_node.covered);
    assert!(matches!(
        single_node.source.as_ref().map(AsRef::as_ref),
        Some(NodeAccessPlan::LabelScan { label }) if label == "User"
    ));

    let capped_ctx = PlannerContext {
        limits: PlannerLimits {
            max_index_union_branches: IndexUnionBranchLimit::limited(1).unwrap(),
        },
        ..Default::default()
    };
    let mut capped_node_planner = crate::planning::Planner::new(&capped_ctx);
    let capped_node = capped_node_planner
        .node_scoped_branch_or_plan(
            &[
                Predicate::eq("$label", "User"),
                Predicate::eq("$label", "Account"),
            ],
            "test.node",
        )
        .unwrap();
    assert!(capped_node.source.is_none());
    assert!(!capped_node.covered);

    let capped_partial_node = capped_node_planner
        .node_scoped_branch_or_plan(
            &[
                Predicate::and(vec![
                    Predicate::eq("$label", "User"),
                    Predicate::contains("bio", "manual"),
                ]),
                Predicate::and(vec![
                    Predicate::eq("$label", "Account"),
                    Predicate::contains("notes", "manual"),
                ]),
            ],
            "test.node",
        )
        .unwrap();
    assert!(capped_partial_node.source.is_none());
    assert!(!capped_partial_node.covered);

    let default_edge_ctx = PlannerContext::default();
    let mut edge_planner = crate::planning::Planner::new(&default_edge_ctx);
    let empty_edge = edge_planner
        .edge_scoped_branch_or_plan(&[], "test.edge")
        .unwrap();
    assert!(empty_edge.source.is_none());
    assert!(!empty_edge.covered);

    let unscoped_edge = edge_planner
        .edge_scoped_branch_or_plan(&[Predicate::eq("status", "active")], "test.edge")
        .unwrap();
    assert!(unscoped_edge.source.is_none());
    assert!(!unscoped_edge.covered);

    let residual_edge = edge_planner
        .edge_scoped_branch_or_plan(
            &[Predicate::and(vec![
                Predicate::eq("$label", "FOLLOWS"),
                Predicate::contains("note", "manual"),
            ])],
            "test.edge",
        )
        .unwrap();
    assert!(matches!(
        residual_edge.source.as_ref().map(AsRef::as_ref),
        Some(EdgeAccessPlan::LabelScan { label }) if label == "FOLLOWS"
    ));
    assert!(!residual_edge.covered);

    let empty_edge_source = edge_planner
        .edge_scoped_branch_or_plan(
            &[
                Predicate::and(vec![
                    Predicate::eq("$label", "FOLLOWS"),
                    Predicate::eq("$label", "LIKES"),
                ]),
                Predicate::and(vec![
                    Predicate::eq("$label", "MENTIONS"),
                    Predicate::eq("$label", "LIKES"),
                ]),
            ],
            "test.edge",
        )
        .unwrap();
    assert!(empty_edge_source.covered);
    assert!(matches!(
        empty_edge_source.source.as_ref().map(AsRef::as_ref),
        Some(EdgeAccessPlan::Empty)
    ));

    let single_edge = edge_planner
        .edge_scoped_branch_or_plan(&[Predicate::eq("$label", "FOLLOWS")], "test.edge")
        .unwrap();
    assert!(single_edge.covered);
    assert!(matches!(
        single_edge.source.as_ref().map(AsRef::as_ref),
        Some(EdgeAccessPlan::LabelScan { label }) if label == "FOLLOWS"
    ));

    let mut capped_edge_planner = crate::planning::Planner::new(&capped_ctx);
    let capped_edge = capped_edge_planner
        .edge_scoped_branch_or_plan(
            &[
                Predicate::eq("$label", "FOLLOWS"),
                Predicate::eq("$label", "LIKES"),
            ],
            "test.edge",
        )
        .unwrap();
    assert!(capped_edge.source.is_none());
    assert!(!capped_edge.covered);

    let capped_partial_edge = capped_edge_planner
        .edge_scoped_branch_or_plan(
            &[
                Predicate::and(vec![
                    Predicate::eq("$label", "FOLLOWS"),
                    Predicate::contains("note", "manual"),
                ]),
                Predicate::and(vec![
                    Predicate::eq("$label", "LIKES"),
                    Predicate::contains("note", "team"),
                ]),
            ],
            "test.edge",
        )
        .unwrap();
    assert!(capped_partial_edge.source.is_none());
    assert!(!capped_partial_edge.covered);
}

#[test]
fn scoped_branch_or_helpers_propagate_analysis_and_branch_errors() {
    let node_ctx = ctx(builtin_label_indexes().with_node_range(
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
    ));
    let edge_ctx = ctx(builtin_label_indexes().with_edge_range(
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
    ));
    let bad_node_range = || Predicate::compare(Expr::prop("age"), CompareOp::Gte, Expr::param(""));
    let bad_edge_range =
        || Predicate::compare(Expr::prop("weight"), CompareOp::Lte, Expr::param(""));
    let expected_label_error = PlannerError::InvalidEmptyName {
        field: NameField::Label,
    };
    let expected_param_error = PlannerError::InvalidEmptyName {
        field: NameField::Param,
    };

    let mut node_planner = crate::planning::Planner::new(&node_ctx);
    assert_eq!(
        node_planner
            .node_scoped_branch_or_plan(&[Predicate::eq("$label", "")], "test.node")
            .err()
            .expect("invalid node label branch should propagate an error"),
        expected_label_error
    );
    let node_error_branches = vec![
        Predicate::and(vec![Predicate::eq("$label", "User"), bad_node_range()]),
        Predicate::eq("$label", "Account"),
    ];
    assert_eq!(
        node_planner
            .node_scoped_branch_or_plan(&node_error_branches, "test.node")
            .err()
            .expect("invalid node branch access should propagate an error"),
        expected_param_error
    );
    assert_eq!(
        node_planner
            .node_access_for_predicate(
                &Predicate::or(vec![
                    Predicate::and(vec![
                        Predicate::eq("$label", "User"),
                        Predicate::compare(
                            Expr::prop("age"),
                            CompareOp::Gte,
                            Expr::val(20).add_expr(Expr::val(1)),
                        ),
                    ]),
                    Predicate::eq("$label", "Account"),
                ]),
                "test.node",
            )
            .expect_err("invalid node scoped-branch OR dispatch should propagate an error"),
        PlannerError::NonLiteralIndexExpression {
            expression: format!("{:?}", Expr::val(20).add_expr(Expr::val(1)))
        }
    );

    let mut edge_planner = crate::planning::Planner::new(&edge_ctx);
    assert_eq!(
        edge_planner
            .edge_scoped_branch_or_plan(&[Predicate::eq("$label", "")], "test.edge")
            .err()
            .expect("invalid edge label branch should propagate an error"),
        expected_label_error
    );
    let edge_error_branches = vec![
        Predicate::and(vec![Predicate::eq("$label", "FOLLOWS"), bad_edge_range()]),
        Predicate::eq("$label", "LIKES"),
    ];
    assert_eq!(
        edge_planner
            .edge_scoped_branch_or_plan(&edge_error_branches, "test.edge")
            .err()
            .expect("invalid edge branch access should propagate an error"),
        expected_param_error
    );
    assert_eq!(
        edge_planner
            .edge_access_for_predicate(
                &Predicate::or(vec![
                    Predicate::and(vec![
                        Predicate::eq("$label", "FOLLOWS"),
                        Predicate::compare(
                            Expr::prop("weight"),
                            CompareOp::Lte,
                            Expr::val(1).add_expr(Expr::val(2)),
                        ),
                    ]),
                    Predicate::eq("$label", "LIKES"),
                ]),
                "test.edge",
            )
            .expect_err("invalid edge scoped-branch OR dispatch should propagate an error"),
        PlannerError::NonLiteralIndexExpression {
            expression: format!("{:?}", Expr::val(1).add_expr(Expr::val(2)))
        }
    );
}

#[test]
fn contradictory_node_label_scope_plans_empty_access() {
    let predicate = Predicate::and(vec![
        Predicate::eq("$label", "User"),
        Predicate::eq("$label", "Account"),
    ]);
    let plan = plan_traversal(g().n_where(predicate), ctx(builtin_label_indexes()));

    assert!(matches!(node_access(&plan), NodeAccessPlan::Empty));
    assert_decision(
        &plan,
        TracePass::AccessPath,
        TraceDecision::NodeEmptyLabelScope,
    );
}

#[test]
fn duplicate_node_label_constraints_keep_single_label_scope() {
    let predicate = Predicate::and(vec![
        Predicate::eq("$label", "User"),
        Predicate::eq("$label", "User"),
    ]);
    let plan = plan_traversal(g().n_where(predicate), ctx(builtin_label_indexes()));

    assert!(matches!(
        node_access(&plan),
        NodeAccessPlan::LabelScan { label } if label == "User"
    ));
}

#[test]
fn contradictory_edge_label_scope_plans_empty_access() {
    let predicate = Predicate::and(vec![
        Predicate::eq("$label", "FOLLOWS"),
        Predicate::eq("$label", "MENTIONS"),
    ]);
    let plan = plan_traversal(g().e_where(predicate), ctx(builtin_label_indexes()));

    assert!(matches!(edge_access(&plan), EdgeAccessPlan::Empty));
    assert_decision(
        &plan,
        TracePass::AccessPath,
        TraceDecision::EdgeEmptyLabelScope,
    );
}

#[test]
fn nested_impossible_label_scope_stays_empty_through_outer_and() {
    let impossible = Predicate::and(vec![
        Predicate::eq("$label", "User"),
        Predicate::eq("$label", "Account"),
    ]);
    let predicate = Predicate::and(vec![Predicate::eq("active", true), impossible]);
    let plan = plan_traversal(g().n_where(predicate), ctx(builtin_label_indexes()));

    assert!(matches!(node_access(&plan), NodeAccessPlan::Empty));
    assert_decision(
        &plan,
        TracePass::AccessPath,
        TraceDecision::NodeEmptyLabelScope,
    );
}

#[test]
fn contradictory_node_scalar_constraints_plan_empty_access() {
    let predicate = Predicate::and(vec![
        Predicate::eq("$label", "User"),
        Predicate::eq("username", "alice"),
        Predicate::eq("username", "bob"),
    ]);
    let plan = plan_traversal(
        g().n_where(predicate),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())),
    );

    assert!(matches!(node_access(&plan), NodeAccessPlan::Empty));
    assert_decision(
        &plan,
        TracePass::AccessPath,
        TraceDecision::NodeEmptyPredicate,
    );
    assert_no_decision(&plan, TraceDecision::NodeEqualityIndex);
}

#[test]
fn contradictory_edge_scalar_constraints_plan_empty_access() {
    let predicate = Predicate::and(vec![
        Predicate::eq("$label", "FOLLOWS"),
        Predicate::eq("since", 2020),
        Predicate::gt("since", 2020),
    ]);
    let plan = plan_traversal(
        g().e_where(predicate),
        ctx(builtin_label_indexes().with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "since", RangeIndexDirection::Asc)
                .unwrap(),
        )),
    );

    assert!(matches!(edge_access(&plan), EdgeAccessPlan::Empty));
    assert_decision(
        &plan,
        TracePass::AccessPath,
        TraceDecision::EdgeEmptyPredicate,
    );
    assert_no_decision(&plan, TraceDecision::EdgeRangeIndex);
}

#[test]
fn contradictory_scalar_range_bounds_plan_empty_access() {
    let node = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![Predicate::gt("age", 64), Predicate::lte("age", 64)]),
        ),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );
    let edge = plan_traversal(
        g().e_where(Predicate::and(vec![
            Predicate::eq("$label", "FOLLOWS"),
            Predicate::gte("since", 2024),
            Predicate::lt("since", 2024),
        ])),
        ctx(builtin_label_indexes().with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "since", RangeIndexDirection::Asc)
                .unwrap(),
        )),
    );

    assert!(matches!(node_access(&node), NodeAccessPlan::Empty));
    assert!(matches!(edge_access(&edge), EdgeAccessPlan::Empty));
    assert_decision(
        &node,
        TracePass::AccessPath,
        TraceDecision::NodeEmptyPredicate,
    );
    assert_decision(
        &edge,
        TracePass::AccessPath,
        TraceDecision::EdgeEmptyPredicate,
    );
}

#[test]
fn scalar_equality_bound_contradictions_cover_upper_and_lower_sides() {
    let lower_excludes_value = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![Predicate::eq("age", 18), Predicate::gt("age", 21)]),
        ),
        ctx(builtin_label_indexes()),
    );
    let upper_excludes_value = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![Predicate::eq("age", 30), Predicate::lt("age", 21)]),
        ),
        ctx(builtin_label_indexes()),
    );
    let exclusive_lower_excludes_equal_value = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![Predicate::eq("age", 21), Predicate::gt("age", 21)]),
        ),
        ctx(builtin_label_indexes()),
    );
    let exclusive_upper_excludes_equal_value = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![Predicate::eq("age", 21), Predicate::lt("age", 21)]),
        ),
        ctx(builtin_label_indexes()),
    );
    let lower_allows_value = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![Predicate::eq("age", 22), Predicate::gt("age", 21)]),
        ),
        ctx(builtin_label_indexes()),
    );
    let upper_allows_value = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![Predicate::eq("age", 20), Predicate::lt("age", 21)]),
        ),
        ctx(builtin_label_indexes()),
    );

    assert!(matches!(
        node_access(&lower_excludes_value),
        NodeAccessPlan::Empty
    ));
    assert!(matches!(
        node_access(&upper_excludes_value),
        NodeAccessPlan::Empty
    ));
    assert!(matches!(
        node_access(&exclusive_lower_excludes_equal_value),
        NodeAccessPlan::Empty
    ));
    assert!(matches!(
        node_access(&exclusive_upper_excludes_equal_value),
        NodeAccessPlan::Empty
    ));
    assert!(!matches!(
        node_access(&lower_allows_value),
        NodeAccessPlan::Empty
    ));
    assert!(!matches!(
        node_access(&upper_allows_value),
        NodeAccessPlan::Empty
    ));
}

#[test]
fn scalar_contradictions_are_detected_regardless_of_predicate_order() {
    let inequality_before_equality = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![Predicate::neq("age", 18), Predicate::eq("age", 18)]),
        ),
        ctx(builtin_label_indexes()),
    );
    let lower_before_equality = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![Predicate::gt("age", 21), Predicate::eq("age", 18)]),
        ),
        ctx(builtin_label_indexes()),
    );
    let upper_before_equality = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![Predicate::lt("age", 21), Predicate::eq("age", 30)]),
        ),
        ctx(builtin_label_indexes()),
    );
    let upper_before_lower = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![Predicate::lte("age", 64), Predicate::gt("age", 64)]),
        ),
        ctx(builtin_label_indexes()),
    );

    assert!(matches!(
        node_access(&inequality_before_equality),
        NodeAccessPlan::Empty
    ));
    assert!(matches!(
        node_access(&lower_before_equality),
        NodeAccessPlan::Empty
    ));
    assert!(matches!(
        node_access(&upper_before_equality),
        NodeAccessPlan::Empty
    ));
    assert!(matches!(
        node_access(&upper_before_lower),
        NodeAccessPlan::Empty
    ));
}

#[test]
fn impossible_direct_residual_filters_plan_empty_access() {
    let node = plan_traversal(
        g().n(NodeRef::all()).where_(Predicate::and(vec![
            Predicate::eq("age", 18),
            Predicate::neq("age", 18),
        ])),
        PlannerContext::default(),
    );
    let edge = plan_traversal(
        g().e([1u64]).where_(Predicate::and(vec![
            Predicate::eq("since", 2024),
            Predicate::gt("since", 2024),
        ])),
        PlannerContext::default(),
    );

    assert!(matches!(node_access(&node), NodeAccessPlan::Empty));
    assert!(matches!(edge_access(&edge), EdgeAccessPlan::Empty));
    assert_decision(
        &node,
        TracePass::AccessPath,
        TraceDecision::NodeEmptyPredicate,
    );
    assert_decision(
        &edge,
        TracePass::AccessPath,
        TraceDecision::EdgeEmptyPredicate,
    );
    assert_no_decision(&node, TraceDecision::ResidualFilter);
    assert_no_decision(&edge, TraceDecision::ResidualFilter);
}

#[test]
fn direct_scan_filters_reuse_source_access_planning() {
    let all_scan_index = plan_traversal(
        g().n(NodeRef::all()).where_(Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::eq("username", "alice"),
        ])),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())),
    );
    let all_scan_label = plan_traversal(
        g().n(NodeRef::all()).has_label("User"),
        ctx(builtin_label_indexes()),
    );
    let all_scan_label_residual = plan_traversal(
        g().n(NodeRef::all()).where_(Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::eq("unindexed", "value"),
        ])),
        ctx(builtin_label_indexes()),
    );
    let edge_all_scan_index = plan_ast(
        AstNode::Where {
            input: Box::new(AstNode::EdgesWhere {
                predicate: Predicate::compare(Expr::val(10), CompareOp::Gt, Expr::val(1)),
            }),
            predicate: Predicate::and(vec![
                Predicate::eq("$label", "FOLLOWS"),
                Predicate::eq("status", "active"),
            ]),
        },
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())),
    );
    let edge_all_scan_label_residual = plan_ast(
        AstNode::Where {
            input: Box::new(AstNode::EdgesWhere {
                predicate: Predicate::compare(Expr::val(10), CompareOp::Gt, Expr::val(1)),
            }),
            predicate: Predicate::and(vec![
                Predicate::eq("$label", "FOLLOWS"),
                Predicate::eq("unindexed", "value"),
            ]),
        },
        ctx(builtin_label_indexes()),
    );

    assert!(matches!(
        node_access(&all_scan_index),
        NodeAccessPlan::EqualityIndex { key, .. }
            if key.label == "User" && key.property == "username"
    ));
    assert_node_label_scan(node_access(&all_scan_label), "User");
    let NodeAccessPlan::ScanThenFilter { source, residual } = node_access(&all_scan_label_residual)
    else {
        panic!(
            "expected node residual over label scan: {:?}",
            node_access(&all_scan_label_residual)
        );
    };
    assert!(matches!(
        source.as_ref(),
        NodeAccessPlan::LabelScan { label } if label == "User"
    ));
    assert_eq!(residual.predicate(), &Predicate::eq("unindexed", "value"));
    assert!(matches!(
        edge_access(&edge_all_scan_index),
        EdgeAccessPlan::EqualityIndex { key, .. }
            if key.label == "FOLLOWS" && key.property == "status"
    ));
    let EdgeAccessPlan::ScanThenFilter { source, residual } =
        edge_access(&edge_all_scan_label_residual)
    else {
        panic!(
            "expected edge residual over label scan: {:?}",
            edge_access(&edge_all_scan_label_residual)
        );
    };
    assert!(matches!(
        source.as_ref(),
        EdgeAccessPlan::LabelScan { label } if label == "FOLLOWS"
    ));
    assert_eq!(residual.predicate(), &Predicate::eq("unindexed", "value"));
    assert_no_decision(&all_scan_index, TraceDecision::ResidualFilter);
    assert_no_decision(&all_scan_label, TraceDecision::ResidualFilter);
    assert_no_decision(&all_scan_label_residual, TraceDecision::ResidualFilter);
    assert_no_decision(&edge_all_scan_index, TraceDecision::ResidualFilter);
    assert_no_decision(&edge_all_scan_label_residual, TraceDecision::ResidualFilter);
}

#[test]
fn adjacent_filter_chains_reuse_combined_access_planning() {
    let node_label = plan_traversal(
        g().n(NodeRef::all())
            .has_key("name")
            .has("active", true)
            .has_label("User"),
        ctx(builtin_label_indexes()),
    );
    let node_range = plan_traversal(
        g().n(NodeRef::all())
            .has("active", true)
            .skip(0usize)
            .has_label("User")
            .where_(Predicate::lt("score", 100))
            .where_(Predicate::gte("age", 21)),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );
    let edge_range = plan_traversal(
        g().e_where(Predicate::compare(
            Expr::val(10),
            CompareOp::Gt,
            Expr::val(1),
        ))
        .edge_has("active", true)
        .edge_has_label("FOLLOWS")
        .where_(Predicate::gte("since", 2020)),
        ctx(builtin_label_indexes().with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "since", RangeIndexDirection::Asc)
                .unwrap(),
        )),
    );

    let NodeAccessPlan::ScanThenFilter { source, residual } = node_access(&node_label) else {
        panic!(
            "expected filtered node label source: {:?}",
            node_access(&node_label)
        );
    };
    assert!(matches!(
        source.as_ref(),
        NodeAccessPlan::LabelScan { label } if label == "User"
    ));
    assert!(predicate_contains(
        residual.predicate(),
        &Predicate::has_key("name")
    ));
    assert!(predicate_contains(
        residual.predicate(),
        &Predicate::eq("active", true)
    ));
    assert!(!predicate_contains(
        residual.predicate(),
        &Predicate::eq("$label", "User")
    ));

    let NodeAccessPlan::ScanThenFilter { source, residual } = node_access(&node_range) else {
        panic!(
            "expected filtered node range source: {:?}",
            node_access(&node_range)
        );
    };
    assert!(matches!(
        source.as_ref(),
        NodeAccessPlan::RangeIndex { key, .. }
            if key.label == "User"
                && key.property == "age"
                && key.direction == RangeIndexDirection::Asc
    ));
    assert!(predicate_contains(
        residual.predicate(),
        &Predicate::eq("active", true)
    ));
    assert!(predicate_contains(
        residual.predicate(),
        &Predicate::lt("score", 100)
    ));
    assert!(!predicate_contains(
        residual.predicate(),
        &Predicate::eq("$label", "User")
    ));
    assert!(!predicate_contains(
        residual.predicate(),
        &Predicate::gte("age", 21)
    ));

    let EdgeAccessPlan::ScanThenFilter { source, residual } = edge_access(&edge_range) else {
        panic!(
            "expected filtered edge range source: {:?}",
            edge_access(&edge_range)
        );
    };
    assert!(matches!(
        source.as_ref(),
        EdgeAccessPlan::RangeIndex { key, .. }
            if key.label == "FOLLOWS"
                && key.property == "since"
                && key.direction == RangeIndexDirection::Asc
    ));
    assert_eq!(residual.predicate(), &Predicate::eq("active", true));
    assert_no_decision(&node_label, TraceDecision::ResidualFilter);
    assert_no_decision(&node_range, TraceDecision::ResidualFilter);
    assert_no_decision(&edge_range, TraceDecision::ResidualFilter);
}

#[test]
fn distinct_filters_reuse_inner_access_planning() {
    let node_label = plan_traversal(
        g().n(NodeRef::all()).dedup().has_label("User"),
        ctx(builtin_label_indexes()),
    );
    let node_range = plan_traversal(
        g().n(NodeRef::all()).dedup().where_(Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::gte("age", 21),
            Predicate::eq("active", true),
        ])),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );
    let edge_label = plan_traversal(
        g().e_where(Predicate::compare(
            Expr::val(10),
            CompareOp::Gt,
            Expr::val(1),
        ))
        .dedup()
        .edge_has_label("FOLLOWS"),
        ctx(builtin_label_indexes()),
    );
    let residual = plan_traversal(
        g().n(NodeRef::all()).dedup().has("active", true),
        PlannerContext::default(),
    );

    assert!(matches!(
        run_op(&node_label),
        PhysicalOp::NodeAccess(NodeAccessPlan::LabelScan { label }) if label == "User"
    ));

    let PhysicalOp::NodeAccess(NodeAccessPlan::ScanThenFilter { source, .. }) = run_op(&node_range)
    else {
        panic!(
            "expected duplicate-free node range plan: {:?}",
            run_op(&node_range)
        );
    };
    assert!(matches!(
        source.as_ref(),
        NodeAccessPlan::RangeIndex { key, .. }
            if key.label == "User" && key.property == "age"
    ));

    assert!(matches!(
        run_op(&edge_label),
        PhysicalOp::EdgeAccess(EdgeAccessPlan::LabelScan { label }) if label == "FOLLOWS"
    ));

    let PhysicalOp::Filter {
        input,
        plan: FilterPlan::Residual { predicate },
    } = run_op(&residual)
    else {
        panic!(
            "expected residual filter after elided distinct: {:?}",
            run_op(&residual)
        );
    };
    assert_eq!(predicate.as_ref(), &Predicate::eq("active", true));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));
    assert_no_decision(&node_label, TraceDecision::ResidualFilter);
    assert_no_decision(&node_range, TraceDecision::ResidualFilter);
    assert_no_decision(&edge_label, TraceDecision::ResidualFilter);
    assert_decision(
        &residual,
        TracePass::PredicateIndex,
        TraceDecision::ResidualFilter,
    );
}

#[test]
fn wrapper_filters_reuse_inner_residual_access_planning() {
    let node = plan_traversal(
        g().n_with_label("User")
            .where_(Predicate::eq("unindexed", "value"))
            .dedup()
            .has("username", "alice"),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())),
    );
    let edge = plan_traversal(
        g().e_with_label("FOLLOWS")
            .where_(Predicate::eq("unindexed", "value"))
            .order_by("weight", Order::Asc)
            .edge_has("status", "active"),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())),
    );

    let PhysicalOp::NodeAccess(NodeAccessPlan::ScanThenFilter { source, residual }) = run_op(&node)
    else {
        panic!(
            "expected indexed node residual after elided distinct: {:?}",
            run_op(&node)
        );
    };
    assert!(matches!(
        source.as_ref(),
        NodeAccessPlan::EqualityIndex { key, .. }
            if key.label == "User" && key.property == "username"
    ));
    assert_eq!(residual.predicate(), &Predicate::eq("unindexed", "value"));

    let PhysicalOp::Order {
        input,
        plan: OrderPlan::ExplicitSort(keys),
    } = run_op(&edge)
    else {
        panic!("expected explicit edge order: {:?}", run_op(&edge));
    };
    assert_eq!(keys.as_ref()[0].property.as_ref(), "weight");
    let PhysicalOp::EdgeAccess(EdgeAccessPlan::ScanThenFilter { source, residual }) =
        input.as_ref()
    else {
        panic!("expected indexed edge residual under order: {input:?}");
    };
    assert!(matches!(
        source.as_ref(),
        EdgeAccessPlan::EqualityIndex { key, .. }
            if key.label == "FOLLOWS" && key.property == "status"
    ));
    assert_eq!(residual.predicate(), &Predicate::eq("unindexed", "value"));
}

#[test]
fn variable_filter_wrappers_reuse_inner_access_planning() {
    let node_within = plan_traversal(
        g().n(NodeRef::all())
            .within("allowed")
            .where_(Predicate::and(vec![
                Predicate::eq("$label", "User"),
                Predicate::gte("age", 21),
                Predicate::eq("active", true),
            ])),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );
    let node_without = plan_traversal(
        g().n(NodeRef::all())
            .without("blocked")
            .has_label("Account"),
        ctx(builtin_label_indexes()),
    );
    let within_empty = plan_traversal(
        g().n_with_label("User")
            .within("allowed")
            .has_label("Account"),
        ctx(builtin_label_indexes()),
    );
    let without_empty = plan_traversal(
        g().n(NodeRef::all())
            .without("blocked")
            .where_(Predicate::and(vec![
                Predicate::eq("active", true),
                Predicate::neq("active", true),
            ])),
        PlannerContext::default(),
    );
    let stored = plan_traversal(
        g().n(NodeRef::all())
            .store("seen")
            .where_(Predicate::and(vec![
                Predicate::eq("$label", "User"),
                Predicate::gte("age", 21),
            ])),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );

    let PhysicalOp::Variable(VariablePlan::Stream { input, op }) = run_op(&node_within) else {
        panic!(
            "expected within variable wrapper: {:?}",
            run_op(&node_within)
        );
    };
    assert_eq!(
        op,
        &StreamVariableOp::Within(NonEmptyString::new("allowed").unwrap())
    );
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::ScanThenFilter { source, .. })
            if matches!(
                source.as_ref(),
                NodeAccessPlan::RangeIndex { key, .. }
                    if key.label == "User" && key.property == "age"
            )
    ));

    let PhysicalOp::Variable(VariablePlan::Stream { input, op }) = run_op(&node_without) else {
        panic!(
            "expected without variable wrapper: {:?}",
            run_op(&node_without)
        );
    };
    assert_eq!(
        op,
        &StreamVariableOp::Without(NonEmptyString::new("blocked").unwrap())
    );
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::LabelScan { label }) if label == "Account"
    ));
    assert!(matches!(node_access(&within_empty), NodeAccessPlan::Empty));
    assert!(matches!(node_access(&without_empty), NodeAccessPlan::Empty));

    let PhysicalOp::Filter {
        input,
        plan: FilterPlan::Residual { .. },
    } = run_op(&stored)
    else {
        panic!(
            "expected residual filter above store: {:?}",
            run_op(&stored)
        );
    };
    let PhysicalOp::Variable(VariablePlan::Stream { input, op }) = input.as_ref() else {
        panic!("expected store under residual filter: {input:?}");
    };
    assert_eq!(
        op,
        &StreamVariableOp::Store(NonEmptyString::new("seen").unwrap())
    );
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    assert_no_decision(&node_within, TraceDecision::ResidualFilter);
    assert_no_decision(&node_without, TraceDecision::ResidualFilter);
    assert_decision(
        &stored,
        TracePass::PredicateIndex,
        TraceDecision::ResidualFilter,
    );
}

#[test]
fn explicit_sort_filters_reuse_inner_access_planning() {
    let node_label = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .has_label("User"),
        ctx(builtin_label_indexes()),
    );
    let node_range = plan_traversal(
        g().n(NodeRef::all())
            .order_by("score", Order::Asc)
            .where_(Predicate::and(vec![
                Predicate::eq("$label", "User"),
                Predicate::gte("age", 21),
                Predicate::eq("active", true),
            ])),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );
    let node_range_ordered = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .where_(Predicate::and(vec![
                Predicate::eq("$label", "User"),
                Predicate::gte("age", 21),
                Predicate::eq("active", true),
            ])),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );
    let node_multi_order = plan_traversal(
        g().n(NodeRef::all())
            .order_by_multiple(vec![("age", Order::Asc), ("name", Order::Desc)])
            .has_label("User"),
        ctx(builtin_label_indexes()),
    );
    let mut unique_indexes = builtin_label_indexes();
    unique_indexes.node_eq.insert(
        ScopedPropertyKey::try_new("User", "id").unwrap(),
        NodeEqualityIndexMeta::try_new("node_eq:User:id")
            .unwrap()
            .with_uniqueness(IndexUniqueness::Unique),
    );
    let node_unique_order_elided = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .where_(Predicate::and(vec![
                Predicate::eq("$label", "User"),
                Predicate::eq("id", 7),
            ])),
        ctx(unique_indexes),
    );
    let edge_label = plan_traversal(
        g().e_where(Predicate::compare(
            Expr::val(10),
            CompareOp::Gt,
            Expr::val(1),
        ))
        .order_by("since", Order::Desc)
        .edge_has_label("FOLLOWS"),
        ctx(builtin_label_indexes()),
    );
    let residual = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .has("active", true),
        PlannerContext::default(),
    );

    let PhysicalOp::Order {
        input,
        plan: OrderPlan::ExplicitSort(keys),
    } = run_op(&node_label)
    else {
        panic!(
            "expected ordered node label plan: {:?}",
            run_op(&node_label)
        );
    };
    assert_eq!(keys.as_ref()[0].property.as_ref(), "age");
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::LabelScan { label }) if label == "User"
    ));

    let PhysicalOp::Order {
        input,
        plan: OrderPlan::ExplicitSort(keys),
    } = run_op(&node_range)
    else {
        panic!(
            "expected ordered node range plan: {:?}",
            run_op(&node_range)
        );
    };
    assert_eq!(keys.as_ref()[0].property.as_ref(), "score");
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::ScanThenFilter { source, .. })
            if matches!(
                source.as_ref(),
                NodeAccessPlan::RangeIndex { key, .. }
                    if key.label == "User" && key.property == "age"
            )
    ));

    let PhysicalOp::Order {
        input,
        plan: OrderPlan::RangeIndex { key, index_id },
    } = run_op(&node_range_ordered)
    else {
        panic!(
            "expected range-ordered node range plan: {:?}",
            run_op(&node_range_ordered)
        );
    };
    assert_eq!(key.property.as_ref(), "age");
    assert_eq!(key.order, Order::Asc);
    assert_eq!(index_id.as_ref(), "node_range:User:age:Asc");
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::ScanThenFilter { source, .. })
            if matches!(
                source.as_ref(),
                NodeAccessPlan::RangeIndex { key, .. }
                    if key.label == "User" && key.property == "age"
            )
    ));

    let PhysicalOp::Order {
        input,
        plan: OrderPlan::ExplicitSort(keys),
    } = run_op(&node_multi_order)
    else {
        panic!(
            "expected explicit multi-key node plan: {:?}",
            run_op(&node_multi_order)
        );
    };
    assert_eq!(
        keys.as_ref(),
        &[
            OrderKey {
                property: NonEmptyString::new("age").unwrap(),
                order: Order::Asc,
            },
            OrderKey {
                property: NonEmptyString::new("name").unwrap(),
                order: Order::Desc,
            },
        ]
    );
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::LabelScan { label }) if label == "User"
    ));

    assert!(matches!(
        run_op(&node_unique_order_elided),
        PhysicalOp::NodeAccess(NodeAccessPlan::EqualityIndex { key, index, .. })
            if key.label == "User"
                && key.property == "id"
                && matches!(index.uniqueness, IndexUniqueness::Unique)
    ));

    let PhysicalOp::Order {
        input,
        plan: OrderPlan::ExplicitSort(keys),
    } = run_op(&edge_label)
    else {
        panic!(
            "expected ordered edge label plan: {:?}",
            run_op(&edge_label)
        );
    };
    assert_eq!(keys.as_ref()[0].property.as_ref(), "since");
    assert_eq!(keys.as_ref()[0].order, Order::Desc);
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::EdgeAccess(EdgeAccessPlan::LabelScan { label }) if label == "FOLLOWS"
    ));

    let PhysicalOp::Order {
        input,
        plan: OrderPlan::ExplicitSort(keys),
    } = run_op(&residual)
    else {
        panic!(
            "expected explicit sort above residual: {:?}",
            run_op(&residual)
        );
    };
    assert_eq!(keys.as_ref()[0].property.as_ref(), "age");
    assert_eq!(keys.as_ref()[0].order, Order::Asc);
    let PhysicalOp::Filter {
        input,
        plan: FilterPlan::Residual { predicate },
    } = input.as_ref()
    else {
        panic!("expected residual filter below explicit sort: {input:?}");
    };
    assert_eq!(predicate.as_ref(), &Predicate::eq("active", true));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    for plan in [
        &node_label,
        &node_range,
        &node_range_ordered,
        &node_multi_order,
        &node_unique_order_elided,
        &edge_label,
        &residual,
    ] {
        assert_decision(plan, TracePass::OrderPushdown, TraceDecision::ExplicitSort);
    }
    assert_decision(
        &node_range_ordered,
        TracePass::OrderPushdown,
        TraceDecision::RangeIndexOrder,
    );
    assert_no_decision(&node_label, TraceDecision::ResidualFilter);
    assert_no_decision(&node_range, TraceDecision::ResidualFilter);
    assert_no_decision(&node_range_ordered, TraceDecision::ResidualFilter);
    assert_no_decision(&node_multi_order, TraceDecision::ResidualFilter);
    assert_no_decision(&node_unique_order_elided, TraceDecision::ResidualFilter);
    assert_no_decision(&edge_label, TraceDecision::ResidualFilter);
    assert_decision(
        &residual,
        TracePass::PredicateIndex,
        TraceDecision::ResidualFilter,
    );
}

#[test]
fn edge_all_scan_filters_keep_residual_when_access_path_is_not_better() {
    let tautology = Predicate::compare(Expr::val(10), CompareOp::Gt, Expr::val(1));
    let tautological_filter = plan_ast(
        AstNode::Where {
            input: Box::new(AstNode::EdgesWhere {
                predicate: tautology.clone(),
            }),
            predicate: tautology,
        },
        PlannerContext::default(),
    );
    let residual = plan_ast(
        AstNode::Where {
            input: Box::new(AstNode::EdgesWhere {
                predicate: Predicate::compare(
                    Expr::val("planner"),
                    CompareOp::Eq,
                    Expr::val("planner"),
                ),
            }),
            predicate: Predicate::eq("status", "active"),
        },
        PlannerContext::default(),
    );

    assert!(matches!(
        edge_access(&tautological_filter),
        EdgeAccessPlan::AllScan
    ));

    let PhysicalOp::Filter { input, .. } = run_op(&residual) else {
        panic!("expected edge residual filter: {:?}", run_op(&residual));
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::EdgeAccess(EdgeAccessPlan::AllScan)
    ));
    assert_no_decision(&tautological_filter, TraceDecision::ResidualFilter);
    assert_decision(
        &residual,
        TracePass::PredicateIndex,
        TraceDecision::ResidualFilter,
    );
}

#[test]
fn label_scan_filters_reuse_scoped_source_access_planning() {
    let node_index = plan_traversal(
        g().n_with_label("User")
            .where_(Predicate::eq("username", "alice")),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())),
    );
    let edge_index = plan_traversal(
        g().e_with_label("FOLLOWS")
            .where_(Predicate::eq("status", "active")),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())),
    );

    assert!(matches!(
        node_access(&node_index),
        NodeAccessPlan::EqualityIndex { key, .. }
            if key.label == "User" && key.property == "username"
    ));
    assert!(matches!(
        edge_access(&edge_index),
        EdgeAccessPlan::EqualityIndex { key, .. }
            if key.label == "FOLLOWS" && key.property == "status"
    ));
    assert_no_decision(&node_index, TraceDecision::ResidualFilter);
    assert_no_decision(&edge_index, TraceDecision::ResidualFilter);
}

#[test]
fn label_scan_filters_keep_residual_when_access_path_is_not_better() {
    let node = plan_traversal(
        g().n_with_label("User")
            .where_(Predicate::eq("unindexed", "value")),
        ctx(builtin_label_indexes()),
    );
    let edge = plan_traversal(
        g().e_with_label("FOLLOWS")
            .where_(Predicate::eq("unindexed", "value")),
        ctx(builtin_label_indexes()),
    );

    let PhysicalOp::Filter {
        input: node_input, ..
    } = run_op(&node)
    else {
        panic!("expected node residual filter: {:?}", run_op(&node));
    };
    let PhysicalOp::Filter {
        input: edge_input, ..
    } = run_op(&edge)
    else {
        panic!("expected edge residual filter: {:?}", run_op(&edge));
    };

    assert!(matches!(
        node_input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::LabelScan { label }) if label == "User"
    ));
    assert!(matches!(
        edge_input.as_ref(),
        PhysicalOp::EdgeAccess(EdgeAccessPlan::LabelScan { label }) if label == "FOLLOWS"
    ));
    assert_decision(
        &node,
        TracePass::PredicateIndex,
        TraceDecision::ResidualFilter,
    );
    assert_decision(
        &edge,
        TracePass::PredicateIndex,
        TraceDecision::ResidualFilter,
    );
}

#[test]
fn impossible_residual_filters_skip_side_effect_free_stream_wrappers() {
    let node = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .where_(Predicate::and(vec![
                Predicate::eq("active", true),
                Predicate::neq("active", true),
            ])),
        PlannerContext::default(),
    );
    let skipped_node = plan_traversal(
        g().n(NodeRef::all())
            .skip(3usize)
            .where_(Predicate::and(vec![
                Predicate::eq("active", true),
                Predicate::neq("active", true),
            ])),
        PlannerContext::default(),
    );
    let edge = plan_traversal(
        g().e([1u64, 2])
            .dedup()
            .order_by("since", Order::Desc)
            .where_(Predicate::and(vec![
                Predicate::eq("since", 2024),
                Predicate::gt("since", 2024),
            ])),
        PlannerContext::default(),
    );

    assert!(matches!(node_access(&node), NodeAccessPlan::Empty));
    assert!(matches!(node_access(&skipped_node), NodeAccessPlan::Empty));
    assert!(matches!(edge_access(&edge), EdgeAccessPlan::Empty));
    assert_decision(
        &node,
        TracePass::AccessPath,
        TraceDecision::NodeEmptyPredicate,
    );
    assert_decision(
        &skipped_node,
        TracePass::AccessPath,
        TraceDecision::NodeEmptyPredicate,
    );
    assert_decision(
        &edge,
        TracePass::AccessPath,
        TraceDecision::EdgeEmptyPredicate,
    );
    assert_no_decision(&node, TraceDecision::ResidualFilter);
    assert_no_decision(&skipped_node, TraceDecision::ResidualFilter);
    assert_no_decision(&edge, TraceDecision::ResidualFilter);
}

#[test]
fn adjacent_residual_filters_coalesce_into_one_predicate() {
    let plan = plan_traversal(
        g().n(NodeRef::all()).has("age", 18).has_key("name"),
        PlannerContext::default(),
    );

    let PhysicalOp::Filter {
        input,
        plan: FilterPlan::Residual { predicate },
    } = run_op(&plan)
    else {
        panic!("expected residual filter: {:?}", run_op(&plan));
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));
    assert_eq!(
        predicate.as_ref(),
        &Predicate::and(vec![Predicate::eq("age", 18), Predicate::has_key("name")])
    );
}

#[test]
fn residual_filters_coalesce_after_elided_noop_limit() {
    let plan = plan_traversal(
        g().n([7u64]).has("age", 18).limit(10usize).has_key("name"),
        PlannerContext::default(),
    );

    let PhysicalOp::Filter {
        input,
        plan: FilterPlan::Residual { predicate },
    } = run_op(&plan)
    else {
        panic!("expected residual filter: {:?}", run_op(&plan));
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::PointIds { ids }) if ids.as_ref() == [7]
    ));
    assert_eq!(
        predicate.as_ref(),
        &Predicate::and(vec![Predicate::eq("age", 18), Predicate::has_key("name")])
    );
}

#[test]
fn source_access_residual_filters_coalesce_into_access_residuals() {
    let node = plan_traversal(
        g().n_with_label_where("User", Predicate::eq("unindexed", "one"))
            .where_(Predicate::eq("other", "two")),
        ctx(builtin_label_indexes()),
    );
    let edge = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::eq("unindexed", "one"))
            .where_(Predicate::eq("other", "two")),
        ctx(builtin_label_indexes()),
    );

    let NodeAccessPlan::ScanThenFilter { source, residual } = node_access(&node) else {
        panic!(
            "expected coalesced node access residual: {:?}",
            node_access(&node)
        );
    };
    assert!(matches!(
        source.as_ref(),
        NodeAccessPlan::LabelScan { label } if label == "User"
    ));
    assert_eq!(
        residual.predicate(),
        &Predicate::and(vec![
            Predicate::eq("unindexed", "one"),
            Predicate::eq("other", "two")
        ])
    );

    let EdgeAccessPlan::ScanThenFilter { source, residual } = edge_access(&edge) else {
        panic!(
            "expected coalesced edge access residual: {:?}",
            edge_access(&edge)
        );
    };
    assert!(matches!(
        source.as_ref(),
        EdgeAccessPlan::LabelScan { label } if label == "FOLLOWS"
    ));
    assert_eq!(
        residual.predicate(),
        &Predicate::and(vec![
            Predicate::eq("unindexed", "one"),
            Predicate::eq("other", "two")
        ])
    );
    assert_no_decision(&node, TraceDecision::ResidualFilter);
    assert_no_decision(&edge, TraceDecision::ResidualFilter);
}

#[test]
fn adjacent_impossible_residual_filters_plan_empty_access() {
    let node = plan_traversal(
        g().n(NodeRef::all())
            .has("age", 18)
            .where_(Predicate::neq("age", 18)),
        PlannerContext::default(),
    );
    let edge = plan_traversal(
        g().e([1u64])
            .edge_has("since", 2024)
            .where_(Predicate::gt("since", 2024)),
        PlannerContext::default(),
    );

    assert!(matches!(node_access(&node), NodeAccessPlan::Empty));
    assert!(matches!(edge_access(&edge), EdgeAccessPlan::Empty));
    assert_decision(
        &node,
        TracePass::AccessPath,
        TraceDecision::NodeEmptyPredicate,
    );
    assert_decision(
        &edge,
        TracePass::AccessPath,
        TraceDecision::EdgeEmptyPredicate,
    );
}

#[test]
fn split_indexed_filters_intersect_with_existing_access_paths() {
    let node_ctx = ctx(builtin_label_indexes()
        .with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )
        .with_node_eq(ScopedPropertyKey::try_new("User", "tenant_id").unwrap()));
    let edge_ctx = ctx(builtin_label_indexes()
        .with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
        )
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap()));

    let node = plan_traversal(
        g().n_with_label_where("User", Predicate::gte("age", 21))
            .has("tenant_id", "acme"),
        node_ctx.clone(),
    );
    let node_residual = plan_traversal(
        g().n_with_label_where("User", Predicate::gte("age", 21))
            .where_(Predicate::and(vec![
                Predicate::eq("tenant_id", "acme"),
                Predicate::eq("active", true),
            ])),
        node_ctx,
    );
    let edge = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::gte("weight", 10))
            .edge_has("status", "active"),
        edge_ctx.clone(),
    );
    let edge_residual = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::gte("weight", 10))
            .where_(Predicate::and(vec![
                Predicate::eq("status", "active"),
                Predicate::eq("region", "emea"),
            ])),
        edge_ctx,
    );

    let node_sources = node_candidate_sources(node_access(&node));
    assert_eq!(node_sources.len(), 2);
    assert_node_range(
        &node_sources,
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
            ),
        },
    );
    assert_node_eq(
        &node_sources,
        "User",
        "tenant_id",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("acme")).unwrap()),
    );

    let NodeAccessPlan::ScanThenFilter { source, residual } = node_access(&node_residual) else {
        panic!(
            "expected split node residual over intersect: {:?}",
            node_access(&node_residual)
        );
    };
    let NodeAccessPlan::Intersect(node_residual_sources) = source.as_ref() else {
        panic!("expected node residual intersect source: {source:?}");
    };
    assert_node_eq(
        node_residual_sources.as_ref(),
        "User",
        "tenant_id",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("acme")).unwrap()),
    );
    assert!(predicate_contains(
        residual.predicate(),
        &Predicate::eq("active", true)
    ));

    let edge_sources = edge_candidate_sources(edge_access(&edge));
    assert_eq!(edge_sources.len(), 2);
    assert_edge_range(
        &edge_sources,
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(10)).unwrap(),
            ),
        },
    );
    assert_edge_eq(
        &edge_sources,
        "FOLLOWS",
        "status",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap()),
    );

    let EdgeAccessPlan::ScanThenFilter { source, residual } = edge_access(&edge_residual) else {
        panic!(
            "expected split edge residual over intersect: {:?}",
            edge_access(&edge_residual)
        );
    };
    assert!(matches!(source.as_ref(), EdgeAccessPlan::Intersect(_)));
    assert!(predicate_contains(
        residual.predicate(),
        &Predicate::eq("region", "emea")
    ));

    assert_decision(&node, TracePass::AccessPath, TraceDecision::NodeIntersect);
    assert_decision(&edge, TracePass::AccessPath, TraceDecision::EdgeIntersect);
    assert_no_decision(&node, TraceDecision::ResidualFilter);
    assert_no_decision(&edge, TraceDecision::ResidualFilter);
}

#[test]
fn split_indexed_filters_flatten_existing_intersections() {
    let node_ctx = ctx(builtin_label_indexes()
        .with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )
        .with_node_eq(ScopedPropertyKey::try_new("User", "tenant_id").unwrap())
        .with_node_eq(ScopedPropertyKey::try_new("User", "status").unwrap()));
    let edge_ctx = ctx(builtin_label_indexes()
        .with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
        )
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "tenant_id").unwrap()));

    let node = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::gte("age", 21),
                Predicate::eq("tenant_id", "acme"),
            ]),
        )
        .has("status", "active"),
        node_ctx,
    );
    let edge = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::gte("weight", 10),
                Predicate::eq("status", "active"),
            ]),
        )
        .edge_has("tenant_id", "acme"),
        edge_ctx,
    );

    let node_sources = node_candidate_sources(node_access(&node));
    assert_eq!(node_sources.len(), 3);
    assert!(
        node_sources
            .iter()
            .all(|source| !matches!(source, NodeAccessPlan::Intersect(_))),
        "expected flat node intersection: {node_sources:?}"
    );
    assert_node_eq(
        &node_sources,
        "User",
        "status",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap()),
    );

    let edge_sources = edge_candidate_sources(edge_access(&edge));
    assert_eq!(edge_sources.len(), 3);
    assert!(
        edge_sources
            .iter()
            .all(|source| !matches!(source, EdgeAccessPlan::Intersect(_))),
        "expected flat edge intersection: {edge_sources:?}"
    );
    assert_edge_eq(
        &edge_sources,
        "FOLLOWS",
        "tenant_id",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("acme")).unwrap()),
    );
}

#[test]
fn source_implied_or_filters_are_elided_before_residual_pushdown() {
    let node = plan_traversal(
        g().n_with_label_where("User", Predicate::eq("username", "alice"))
            .where_(Predicate::or(vec![
                Predicate::contains("bio", "systems"),
                Predicate::eq("username", "alice"),
            ])),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())),
    );
    let edge = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::eq("status", "active"))
            .where_(Predicate::or(vec![
                Predicate::contains("note", "manual"),
                Predicate::eq("status", "active"),
            ])),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())),
    );

    assert_node_eq(
        &node_candidate_sources(node_access(&node)),
        "User",
        "username",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("alice")).unwrap()),
    );
    assert_edge_eq(
        &edge_candidate_sources(edge_access(&edge)),
        "FOLLOWS",
        "status",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap()),
    );
    assert_no_decision(&node, TraceDecision::NodeScanOr);
    assert_no_decision(&edge, TraceDecision::EdgeScanOr);
    assert_no_decision(&node, TraceDecision::ResidualFilter);
    assert_no_decision(&edge, TraceDecision::ResidualFilter);
}

#[test]
fn source_implied_range_or_filters_are_elided_before_residual_pushdown() {
    assert!(
        crate::planning::literal_range_filter_atom(&Predicate::compare(
            Expr::prop("age"),
            CompareOp::Gte,
            Expr::val(20).add_expr(Expr::val(1)),
        ))
        .is_none()
    );

    let node_intersection = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::gte("age", 21),
                Predicate::eq("tenant_id", "acme"),
            ]),
        )
        .where_(Predicate::or(vec![
            Predicate::contains("bio", "systems"),
            Predicate::gte("age", 18),
        ])),
        ctx(builtin_label_indexes()
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
            )
            .with_node_eq(ScopedPropertyKey::try_new("User", "tenant_id").unwrap())),
    );
    let node_equality = plan_traversal(
        g().n_with_label_where("User", Predicate::eq("age", 30))
            .where_(Predicate::or(vec![
                Predicate::contains("bio", "systems"),
                Predicate::gte("age", 18),
            ])),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "age").unwrap())),
    );
    let edge_intersection = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::gte("weight", 10),
                Predicate::eq("status", "active"),
            ]),
        )
        .where_(Predicate::or(vec![
            Predicate::contains("note", "manual"),
            Predicate::gte("weight", 5),
        ])),
        ctx(builtin_label_indexes()
            .with_edge_range(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                    .unwrap(),
            )
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())),
    );
    let edge_equality = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::eq("weight", 40))
            .where_(Predicate::or(vec![
                Predicate::contains("note", "manual"),
                Predicate::lte("weight", 100),
            ])),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap())),
    );

    assert_node_range(
        &node_candidate_sources(node_access(&node_intersection)),
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
            ),
        },
    );
    assert_node_eq(
        &node_candidate_sources(node_access(&node_equality)),
        "User",
        "age",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(30)).unwrap()),
    );
    assert_edge_range(
        &edge_candidate_sources(edge_access(&edge_intersection)),
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(10)).unwrap(),
            ),
        },
    );
    assert_edge_eq(
        &edge_candidate_sources(edge_access(&edge_equality)),
        "FOLLOWS",
        "weight",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(40)).unwrap()),
    );

    for plan in [
        &node_intersection,
        &node_equality,
        &edge_intersection,
        &edge_equality,
    ] {
        assert_no_decision(plan, TraceDecision::NodeScanOr);
        assert_no_decision(plan, TraceDecision::EdgeScanOr);
        assert_no_decision(plan, TraceDecision::ResidualFilter);
    }
}

#[test]
fn source_implied_and_conjuncts_are_pruned_before_residual_pushdown() {
    let node = plan_traversal(
        g().n_with_label_where("User", Predicate::gte("age", 21))
            .where_(Predicate::and(vec![
                Predicate::gte("age", 18),
                Predicate::contains("bio", "systems"),
            ])),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );
    let edge = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::gte("weight", 10))
            .where_(Predicate::and(vec![
                Predicate::gte("weight", 5),
                Predicate::contains("note", "manual"),
                Predicate::has_key("reason"),
            ])),
        ctx(builtin_label_indexes().with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
        )),
    );

    let NodeAccessPlan::ScanThenFilter {
        source: node_source,
        residual: node_residual,
    } = node_access(&node)
    else {
        panic!(
            "expected node residual with source-implied conjunct pruned: {:?}",
            node_access(&node)
        );
    };
    assert!(matches!(
        node_source.as_ref(),
        NodeAccessPlan::RangeIndex { key, range, .. }
            if key.label == "User"
                && key.property == "age"
                && *range == IndexRange::Lower {
                    lower: IndexBound::Inclusive(
                        RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
                    ),
                }
    ));
    assert_eq!(
        node_residual.predicate(),
        &Predicate::contains("bio", "systems")
    );

    let EdgeAccessPlan::ScanThenFilter {
        source: edge_source,
        residual: edge_residual,
    } = edge_access(&edge)
    else {
        panic!(
            "expected edge residual with source-implied conjunct pruned: {:?}",
            edge_access(&edge)
        );
    };
    assert!(matches!(
        edge_source.as_ref(),
        EdgeAccessPlan::RangeIndex { key, range, .. }
            if key.label == "FOLLOWS"
                && key.property == "weight"
                && *range == IndexRange::Lower {
                    lower: IndexBound::Inclusive(
                        RangeIndexValue::literal(PropertyValue::from(10)).unwrap(),
                    ),
                }
    ));
    assert_eq!(
        edge_residual.predicate(),
        &Predicate::and(vec![
            Predicate::contains("note", "manual"),
            Predicate::has_key("reason"),
        ])
    );
}

#[test]
fn source_implied_nested_filters_are_elided_before_residual_pushdown() {
    let node_and = plan_traversal(
        g().n_with_label_where("User", Predicate::gte("age", 21))
            .where_(Predicate::and(vec![
                Predicate::gte("age", 18),
                Predicate::gte("age", 20),
            ])),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );
    let node_or_with_and = plan_traversal(
        g().n_with_label_where("User", Predicate::gte("age", 21))
            .where_(Predicate::or(vec![
                Predicate::and(vec![Predicate::gte("age", 18), Predicate::gte("age", 20)]),
                Predicate::contains("bio", "systems"),
            ])),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );
    let edge_and = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::gte("weight", 10))
            .where_(Predicate::and(vec![
                Predicate::gte("weight", 5),
                Predicate::gte("weight", 8),
            ])),
        ctx(builtin_label_indexes().with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
        )),
    );
    let edge_or_with_and = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::gte("weight", 10))
            .where_(Predicate::or(vec![
                Predicate::and(vec![
                    Predicate::gte("weight", 5),
                    Predicate::gte("weight", 8),
                ]),
                Predicate::contains("note", "manual"),
            ])),
        ctx(builtin_label_indexes().with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
        )),
    );

    for plan in [&node_and, &node_or_with_and] {
        assert!(matches!(
            node_access(plan),
            NodeAccessPlan::RangeIndex { key, range, .. }
                if key.label == "User"
                    && key.property == "age"
                    && *range == IndexRange::Lower {
                        lower: IndexBound::Inclusive(
                            RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
                        ),
                    }
        ));
        assert_no_decision(plan, TraceDecision::NodeScanOr);
        assert_no_decision(plan, TraceDecision::ResidualFilter);
    }

    for plan in [&edge_and, &edge_or_with_and] {
        assert!(matches!(
            edge_access(plan),
            EdgeAccessPlan::RangeIndex { key, range, .. }
                if key.label == "FOLLOWS"
                    && key.property == "weight"
                    && *range == IndexRange::Lower {
                        lower: IndexBound::Inclusive(
                            RangeIndexValue::literal(PropertyValue::from(10)).unwrap(),
                        ),
                    }
        ));
        assert_no_decision(plan, TraceDecision::EdgeScanOr);
        assert_no_decision(plan, TraceDecision::ResidualFilter);
    }
}

#[test]
fn direct_and_residuals_drop_indexed_source_conjuncts() {
    let node_ctx = ctx(builtin_label_indexes()
        .with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )
        .with_node_eq(ScopedPropertyKey::try_new("User", "tenant_id").unwrap()));
    let edge_ctx = ctx(builtin_label_indexes()
        .with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
        )
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap()));

    let node = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::gte("age", 21),
                Predicate::eq("tenant_id", "acme"),
                Predicate::contains("bio", "systems"),
            ]),
        ),
        node_ctx.clone(),
    );
    let edge = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::gte("weight", 10),
                Predicate::eq("status", "active"),
                Predicate::has_key("reason"),
            ]),
        ),
        edge_ctx.clone(),
    );
    let node_tautology = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::gte("age", 21),
                Predicate::or(vec![
                    Predicate::gte("age", 18),
                    Predicate::contains("bio", "systems"),
                ]),
            ]),
        ),
        node_ctx,
    );
    let edge_tautology = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::gte("weight", 10),
                Predicate::or(vec![
                    Predicate::gte("weight", 5),
                    Predicate::contains("note", "manual"),
                ]),
            ]),
        ),
        edge_ctx,
    );

    let NodeAccessPlan::ScanThenFilter { source, residual } = node_access(&node) else {
        panic!("expected direct node residual over indexed source: {node:?}");
    };
    assert!(matches!(source.as_ref(), NodeAccessPlan::Intersect(_)));
    assert_eq!(residual.predicate(), &Predicate::contains("bio", "systems"));

    let EdgeAccessPlan::ScanThenFilter { source, residual } = edge_access(&edge) else {
        panic!("expected direct edge residual over indexed source: {edge:?}");
    };
    assert!(matches!(source.as_ref(), EdgeAccessPlan::Intersect(_)));
    assert_eq!(residual.predicate(), &Predicate::has_key("reason"));

    assert!(matches!(
        node_access(&node_tautology),
        NodeAccessPlan::RangeIndex { key, range, .. }
            if key.label == "User"
                && key.property == "age"
                && *range == IndexRange::Lower {
                    lower: IndexBound::Inclusive(
                        RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
                    ),
                }
    ));
    assert!(matches!(
        edge_access(&edge_tautology),
        EdgeAccessPlan::RangeIndex { key, range, .. }
            if key.label == "FOLLOWS"
                && key.property == "weight"
                && *range == IndexRange::Lower {
                    lower: IndexBound::Inclusive(
                        RangeIndexValue::literal(PropertyValue::from(10)).unwrap(),
                    ),
                }
    ));
}

#[test]
fn split_indexed_range_filters_narrow_existing_access_paths() {
    let node_ctx = ctx(builtin_label_indexes().with_node_range(
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
    ));
    let edge_ctx = ctx(builtin_label_indexes().with_edge_range(
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
    ));

    let node = plan_traversal(
        g().n_with_label_where("User", Predicate::gte("age", 21))
            .where_(Predicate::lt("age", 65)),
        node_ctx,
    );
    let edge = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::gte("weight", 10))
            .where_(Predicate::lt("weight", 100)),
        edge_ctx,
    );

    assert_node_range(
        &node_candidate_sources(node_access(&node)),
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Between(
            IndexBetweenRange::new(
                IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(21)).unwrap()),
                IndexBound::Exclusive(RangeIndexValue::literal(PropertyValue::from(65)).unwrap()),
            )
            .unwrap(),
        ),
    );
    assert_edge_range(
        &edge_candidate_sources(edge_access(&edge)),
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Between(
            IndexBetweenRange::new(
                IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(10)).unwrap()),
                IndexBound::Exclusive(RangeIndexValue::literal(PropertyValue::from(100)).unwrap()),
            )
            .unwrap(),
        ),
    );
    assert_no_decision(&node, TraceDecision::NodeIntersect);
    assert_no_decision(&edge, TraceDecision::EdgeIntersect);
}

#[test]
fn split_indexed_equality_range_filters_elide_or_empty() {
    let node_ctx = ctx(builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("User", "age").unwrap())
        .with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        ));
    let edge_ctx = ctx(builtin_label_indexes()
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap())
        .with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
        ));

    let node_equality_then_containing_range = plan_traversal(
        g().n_with_label_where("User", Predicate::eq("age", 30))
            .where_(Predicate::gte("age", 21)),
        node_ctx.clone(),
    );
    let node_range_then_contained_equality = plan_traversal(
        g().n_with_label_where("User", Predicate::gte("age", 21))
            .has("age", 30),
        node_ctx.clone(),
    );
    let node_equality_then_range = plan_traversal(
        g().n_with_label_where("User", Predicate::eq("age", 10))
            .where_(Predicate::gte("age", 21)),
        node_ctx.clone(),
    );
    let node_range_then_equality = plan_traversal(
        g().n_with_label_where("User", Predicate::gte("age", 21))
            .has("age", 10),
        node_ctx,
    );
    let edge_equality_then_range = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::eq("weight", 30))
            .where_(Predicate::lt("weight", 10)),
        edge_ctx.clone(),
    );
    let edge_range_then_equality = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::lt("weight", 10))
            .edge_has("weight", 30),
        edge_ctx.clone(),
    );

    for plan in [
        &node_equality_then_containing_range,
        &node_range_then_contained_equality,
    ] {
        let sources = node_candidate_sources(node_access(plan));
        assert_eq!(sources.len(), 1);
        assert_node_eq(
            &sources,
            "User",
            "age",
            IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(30)).unwrap()),
        );
        assert_no_decision(plan, TraceDecision::ResidualFilter);
        assert_no_decision(plan, TraceDecision::NodeIntersect);
    }

    let edge_equality_then_containing_range = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::eq("weight", 5))
            .where_(Predicate::lt("weight", 10)),
        edge_ctx.clone(),
    );
    let edge_range_then_contained_equality = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::lt("weight", 10))
            .edge_has("weight", 5),
        edge_ctx.clone(),
    );

    for plan in [
        &edge_equality_then_containing_range,
        &edge_range_then_contained_equality,
    ] {
        let sources = edge_candidate_sources(edge_access(plan));
        assert_eq!(sources.len(), 1);
        assert_edge_eq(
            &sources,
            "FOLLOWS",
            "weight",
            IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(5)).unwrap()),
        );
        assert_no_decision(plan, TraceDecision::ResidualFilter);
        assert_no_decision(plan, TraceDecision::EdgeIntersect);
    }

    assert!(matches!(
        node_access(&node_equality_then_range),
        NodeAccessPlan::Empty
    ));
    assert!(matches!(
        node_access(&node_range_then_equality),
        NodeAccessPlan::Empty
    ));
    assert!(matches!(
        edge_access(&edge_equality_then_range),
        EdgeAccessPlan::Empty
    ));
    assert!(matches!(
        edge_access(&edge_range_then_equality),
        EdgeAccessPlan::Empty
    ));
    for plan in [
        &node_equality_then_range,
        &node_range_then_equality,
        &edge_equality_then_range,
        &edge_range_then_equality,
    ] {
        assert_no_decision(plan, TraceDecision::ResidualFilter);
        assert_no_decision(plan, TraceDecision::NodeIntersect);
        assert_no_decision(plan, TraceDecision::EdgeIntersect);
    }
}

#[test]
fn split_indexed_filters_classify_known_label_empty_and_noop_filters() {
    let node_ctx = ctx(builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap()));
    let edge_ctx = ctx(builtin_label_indexes()
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap()));

    let node_label_noop = plan_traversal(
        g().n_with_label_where("User", Predicate::eq("username", "alice"))
            .has_label("User"),
        node_ctx.clone(),
    );
    let edge_label_noop = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::eq("status", "active"))
            .edge_has_label("FOLLOWS"),
        edge_ctx.clone(),
    );
    let node_tautology = plan_traversal(
        g().n_with_label_where("User", Predicate::eq("username", "alice"))
            .where_(Predicate::compare(
                Expr::val(2),
                CompareOp::Gt,
                Expr::val(1),
            )),
        node_ctx.clone(),
    );
    let edge_tautology = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::eq("status", "active"))
            .where_(Predicate::compare(
                Expr::val(2),
                CompareOp::Gt,
                Expr::val(1),
            )),
        edge_ctx.clone(),
    );
    let node_wrong_label = plan_traversal(
        g().n_with_label_where("User", Predicate::eq("username", "alice"))
            .has_label("Account"),
        node_ctx.clone(),
    );
    let node_contradictory_label = plan_traversal(
        g().n_with_label_where("User", Predicate::eq("username", "alice"))
            .where_(Predicate::and(vec![
                Predicate::eq("$label", "User"),
                Predicate::eq("$label", "Account"),
            ])),
        node_ctx.clone(),
    );
    let edge_wrong_label = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::eq("status", "active"))
            .edge_has_label("LIKES"),
        edge_ctx.clone(),
    );
    let node_scalar_false = plan_traversal(
        g().n_with_label_where("User", Predicate::eq("username", "alice"))
            .where_(Predicate::compare(
                Expr::val(1),
                CompareOp::Gt,
                Expr::val(2),
            )),
        node_ctx,
    );
    let edge_scalar_false = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::eq("status", "active"))
            .where_(Predicate::compare(
                Expr::val(1),
                CompareOp::Gt,
                Expr::val(2),
            )),
        edge_ctx,
    );

    assert!(matches!(
        node_access(&node_label_noop),
        NodeAccessPlan::EqualityIndex { key, .. }
            if key.label == "User" && key.property == "username"
    ));
    assert!(matches!(
        edge_access(&edge_label_noop),
        EdgeAccessPlan::EqualityIndex { key, .. }
            if key.label == "FOLLOWS" && key.property == "status"
    ));
    assert_eq!(node_access(&node_tautology), node_access(&node_label_noop));
    assert_eq!(edge_access(&edge_tautology), edge_access(&edge_label_noop));

    assert!(matches!(
        node_access(&node_wrong_label),
        NodeAccessPlan::Empty
    ));
    assert!(matches!(
        node_access(&node_contradictory_label),
        NodeAccessPlan::Empty
    ));
    assert!(matches!(
        edge_access(&edge_wrong_label),
        EdgeAccessPlan::Empty
    ));
    assert!(matches!(
        node_access(&node_scalar_false),
        NodeAccessPlan::Empty
    ));
    assert!(matches!(
        edge_access(&edge_scalar_false),
        EdgeAccessPlan::Empty
    ));
    assert_decision(
        &node_wrong_label,
        TracePass::AccessPath,
        TraceDecision::NodeEmptyLabelScope,
    );
    assert_decision(
        &node_contradictory_label,
        TracePass::AccessPath,
        TraceDecision::NodeEmptyLabelScope,
    );
    assert_decision(
        &edge_wrong_label,
        TracePass::AccessPath,
        TraceDecision::EdgeEmptyLabelScope,
    );
    assert_decision(
        &node_scalar_false,
        TracePass::AccessPath,
        TraceDecision::NodeEmptyPredicate,
    );
    assert_decision(
        &edge_scalar_false,
        TracePass::AccessPath,
        TraceDecision::EdgeEmptyPredicate,
    );
}

#[test]
fn common_label_contract_rejects_mixed_label_set_sources() {
    let node = NodeAccessPlan::Union(AtLeast::<_, 2>::from_pair(
        NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::LabelScan {
            label: NonEmptyString::new("User").unwrap(),
        }),
        NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::LabelScan {
            label: NonEmptyString::new("Account").unwrap(),
        }),
    ));
    let edge = EdgeAccessPlan::Intersect(AtLeast::<_, 2>::from_pair(
        EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::LabelScan {
            label: NonEmptyString::new("FOLLOWS").unwrap(),
        }),
        EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::LabelScan {
            label: NonEmptyString::new("LIKES").unwrap(),
        }),
    ));
    let node_unscoped_first = NodeAccessPlan::Intersect(AtLeast::<_, 2>::from_pair(
        NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::AllScan),
        NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::LabelScan {
            label: NonEmptyString::new("User").unwrap(),
        }),
    ));
    let edge_unscoped_first = EdgeAccessPlan::Union(AtLeast::<_, 2>::from_pair(
        EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::AllScan),
        EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::LabelScan {
            label: NonEmptyString::new("FOLLOWS").unwrap(),
        }),
    ));
    let node_unscoped_later = NodeAccessPlan::Intersect(AtLeast::<_, 2>::from_pair(
        NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::LabelScan {
            label: NonEmptyString::new("User").unwrap(),
        }),
        NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::AllScan),
    ));
    let edge_unscoped_later = EdgeAccessPlan::Union(AtLeast::<_, 2>::from_pair(
        EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::LabelScan {
            label: NonEmptyString::new("FOLLOWS").unwrap(),
        }),
        EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::AllScan),
    ));

    assert_eq!(crate::planning::node_access_common_label(&node), None);
    assert_eq!(crate::planning::edge_access_common_label(&edge), None);
    assert_eq!(
        crate::planning::node_access_common_label(&node_unscoped_first),
        None
    );
    assert_eq!(
        crate::planning::edge_access_common_label(&edge_unscoped_first),
        None
    );
    assert_eq!(
        crate::planning::node_access_common_label(&node_unscoped_later),
        None
    );
    assert_eq!(
        crate::planning::edge_access_common_label(&edge_unscoped_later),
        None
    );
}

#[test]
fn split_indexed_filter_errors_propagate_from_known_label_pushdown() {
    let node_ctx = ctx(builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
        .with_node_eq(ScopedPropertyKey::try_new("User", "tenant_id").unwrap()));
    let edge_ctx = ctx(builtin_label_indexes()
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "tenant_id").unwrap()));

    let node_label_error = read_batch().var_as(
        "result",
        g().n_with_label_where("User", Predicate::eq("username", "alice"))
            .where_(Predicate::eq("$label", "")),
    );
    let edge_label_error = read_batch().var_as(
        "result",
        g().e_with_label_where("FOLLOWS", Predicate::eq("status", "active"))
            .where_(Predicate::eq("$label", "")),
    );
    let node_param_error = read_batch().var_as(
        "result",
        g().n_with_label_where("User", Predicate::eq("username", "alice"))
            .where_(Predicate::eq_param("tenant_id", "")),
    );
    let edge_param_error = read_batch().var_as(
        "result",
        g().e_with_label_where("FOLLOWS", Predicate::eq("status", "active"))
            .where_(Predicate::eq_param("tenant_id", "")),
    );

    assert_eq!(
        plan_read(&node_label_error, &node_ctx).unwrap_err(),
        PlannerError::InvalidEmptyName {
            field: NameField::Label
        }
    );
    assert_eq!(
        plan_read(&edge_label_error, &edge_ctx).unwrap_err(),
        PlannerError::InvalidEmptyName {
            field: NameField::Label
        }
    );
    assert_eq!(
        plan_read(&node_param_error, &node_ctx).unwrap_err(),
        PlannerError::InvalidEmptyName {
            field: NameField::Param
        }
    );
    assert_eq!(
        plan_read(&edge_param_error, &edge_ctx).unwrap_err(),
        PlannerError::InvalidEmptyName {
            field: NameField::Param
        }
    );
}

#[test]
fn range_candidate_errors_propagate_from_atoms_and_conjunctions() {
    let node_ctx = ctx(builtin_label_indexes().with_node_range(
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
    ));
    let edge_ctx = ctx(builtin_label_indexes().with_edge_range(
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
    ));
    let bad_node_range = || Predicate::compare(Expr::prop("age"), CompareOp::Gte, Expr::param(""));
    let bad_edge_range =
        || Predicate::compare(Expr::prop("weight"), CompareOp::Lte, Expr::param(""));
    let expected_error = PlannerError::InvalidEmptyName {
        field: NameField::Param,
    };

    assert_eq!(
        crate::planning::literal_range_constraints_with_extra(&[bad_node_range()], &[])
            .err()
            .expect("invalid range constraint should propagate an error"),
        expected_error
    );
    let node_label = NonEmptyString::new("User").unwrap();
    let edge_label = NonEmptyString::new("FOLLOWS").unwrap();
    let mut node_planner = crate::planning::Planner::new(&node_ctx);
    let mut edge_planner = crate::planning::Planner::new(&edge_ctx);
    assert_eq!(
        node_planner
            .node_index_plan_with_conjunction_ranges(
                &Predicate::and(vec![bad_node_range()]),
                &[],
                &node_label,
                "test.node",
            )
            .err()
            .expect("invalid node conjunction range should propagate an error"),
        expected_error
    );
    assert_eq!(
        node_planner
            .node_index_atom(&bad_node_range(), &node_label, "test.node")
            .expect_err("invalid node range atom should propagate an error"),
        expected_error
    );
    assert_eq!(
        edge_planner
            .edge_index_plan_with_conjunction_ranges(
                &Predicate::and(vec![bad_edge_range()]),
                &[],
                &edge_label,
                "test.edge",
            )
            .err()
            .expect("invalid edge conjunction range should propagate an error"),
        expected_error
    );
    assert_eq!(
        edge_planner
            .edge_index_atom(&bad_edge_range(), &edge_label, "test.edge")
            .expect_err("invalid edge range atom should propagate an error"),
        expected_error
    );

    let node_atom = read_batch().var_as("result", g().n_with_label_where("User", bad_node_range()));
    let node_conjunction = read_batch().var_as(
        "result",
        g().n_with_label_where(
            "User",
            Predicate::and(vec![Predicate::eq("username", "alice"), bad_node_range()]),
        ),
    );
    let edge_atom = read_batch().var_as(
        "result",
        g().e_with_label_where("FOLLOWS", bad_edge_range()),
    );
    let edge_conjunction = read_batch().var_as(
        "result",
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![Predicate::eq("status", "active"), bad_edge_range()]),
        ),
    );
    let node_or = read_batch().var_as(
        "result",
        g().n_where(Predicate::or(vec![
            Predicate::and(vec![Predicate::eq("$label", "User"), bad_node_range()]),
            Predicate::eq("$label", "Account"),
        ])),
    );
    let edge_or = read_batch().var_as(
        "result",
        g().e_where(Predicate::or(vec![
            Predicate::and(vec![Predicate::eq("$label", "FOLLOWS"), bad_edge_range()]),
            Predicate::eq("$label", "LIKES"),
        ])),
    );

    for (query, planner_ctx) in [
        (&node_atom, &node_ctx),
        (&node_conjunction, &node_ctx),
        (&edge_atom, &edge_ctx),
        (&edge_conjunction, &edge_ctx),
        (&node_or, &node_ctx),
        (&edge_or, &edge_ctx),
    ] {
        assert_eq!(plan_read(query, planner_ctx).unwrap_err(), expected_error);
    }
}

#[test]
fn range_index_order_filter_pushdown_propagates_errors() {
    let planner_ctx = ctx(builtin_label_indexes()
        .with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )
        .with_node_eq(ScopedPropertyKey::try_new("User", "tenant_id").unwrap()));
    let query = read_batch().var_as(
        "result",
        g().n_with_label_where("User", Predicate::gte("age", 21))
            .order_by("age", Order::Asc)
            .where_(Predicate::eq_param("tenant_id", "")),
    );

    assert_eq!(
        plan_read(&query, &planner_ctx).unwrap_err(),
        PlannerError::InvalidEmptyName {
            field: NameField::Param
        }
    );
}

#[test]
fn split_indexed_filters_combine_existing_and_new_residuals() {
    let node_ctx = ctx(builtin_label_indexes()
        .with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )
        .with_node_eq(ScopedPropertyKey::try_new("User", "tenant_id").unwrap()));
    let edge_ctx = ctx(builtin_label_indexes()
        .with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
        )
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap()));

    let node = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::gte("age", 21),
                Predicate::eq("active", true),
            ]),
        )
        .where_(Predicate::and(vec![
            Predicate::eq("tenant_id", "acme"),
            Predicate::eq("region", "emea"),
        ])),
        node_ctx,
    );
    let edge = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::gte("weight", 10),
                Predicate::eq("audited", true),
            ]),
        )
        .where_(Predicate::and(vec![
            Predicate::eq("status", "active"),
            Predicate::eq("region", "emea"),
        ])),
        edge_ctx,
    );

    let NodeAccessPlan::ScanThenFilter { source, residual } = node_access(&node) else {
        panic!("expected node residual over split intersection: {node:?}");
    };
    assert!(matches!(source.as_ref(), NodeAccessPlan::Intersect(_)));
    assert!(predicate_contains(
        residual.predicate(),
        &Predicate::eq("active", true)
    ));
    assert!(predicate_contains(
        residual.predicate(),
        &Predicate::eq("region", "emea")
    ));
    assert!(!predicate_contains(
        residual.predicate(),
        &Predicate::gte("age", 21)
    ));
    assert!(!predicate_contains(
        residual.predicate(),
        &Predicate::eq("tenant_id", "acme")
    ));

    let EdgeAccessPlan::ScanThenFilter { source, residual } = edge_access(&edge) else {
        panic!("expected edge residual over split intersection: {edge:?}");
    };
    assert!(matches!(source.as_ref(), EdgeAccessPlan::Intersect(_)));
    assert!(predicate_contains(
        residual.predicate(),
        &Predicate::eq("audited", true)
    ));
    assert!(predicate_contains(
        residual.predicate(),
        &Predicate::eq("region", "emea")
    ));
    assert!(!predicate_contains(
        residual.predicate(),
        &Predicate::gte("weight", 10)
    ));
    assert!(!predicate_contains(
        residual.predicate(),
        &Predicate::eq("status", "active")
    ));
}

#[test]
fn impossible_source_access_residual_filters_plan_empty_access() {
    let node = plan_traversal(
        g().n_with_label_where("User", Predicate::eq("age", 18))
            .where_(Predicate::neq("age", 18)),
        ctx(builtin_label_indexes()),
    );
    let edge = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::eq("since", 2024))
            .where_(Predicate::gt("since", 2024)),
        ctx(builtin_label_indexes()),
    );

    assert!(matches!(node_access(&node), NodeAccessPlan::Empty));
    assert!(matches!(edge_access(&edge), EdgeAccessPlan::Empty));
    assert_decision(
        &node,
        TracePass::AccessPath,
        TraceDecision::NodeEmptyPredicate,
    );
    assert_decision(
        &edge,
        TracePass::AccessPath,
        TraceDecision::EdgeEmptyPredicate,
    );
    assert_no_decision(&node, TraceDecision::ResidualFilter);
    assert_no_decision(&edge, TraceDecision::ResidualFilter);
}

#[test]
fn statically_tautological_filters_are_elided() {
    let node = plan_traversal(
        g().n(NodeRef::all()).where_(Predicate::compare(
            Expr::val(10),
            CompareOp::Gt,
            Expr::val(1),
        )),
        PlannerContext::default(),
    );
    let ordered_node = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .where_(Predicate::Contains {
                value: Expr::val("planner"),
                substring: Expr::val("plan"),
            }),
        PlannerContext::default(),
    );
    let edge = plan_traversal(
        g().e([1u64, 2]).where_(Predicate::IsIn {
            value: Expr::val(2),
            values: Expr::val(PropertyValue::array([1, 2, 3])),
        }),
        PlannerContext::default(),
    );
    let false_node = plan_traversal(
        g().n(NodeRef::all()).where_(Predicate::Between {
            value: Expr::val(1),
            min: Expr::val(3),
            max: Expr::val(8),
        }),
        PlannerContext::default(),
    );

    assert!(matches!(node_access(&node), NodeAccessPlan::AllScan));
    assert!(matches!(
        run_op(&ordered_node),
        PhysicalOp::Order {
            input,
            plan: OrderPlan::ExplicitSort(_),
        } if matches!(input.as_ref(), PhysicalOp::NodeAccess(NodeAccessPlan::AllScan))
    ));
    assert!(matches!(
        edge_access(&edge),
        EdgeAccessPlan::PointIds { .. }
    ));
    assert!(matches!(node_access(&false_node), NodeAccessPlan::Empty));

    assert_no_decision(&node, TraceDecision::ResidualFilter);
    assert_no_decision(&ordered_node, TraceDecision::ResidualFilter);
    assert_no_decision(&edge, TraceDecision::ResidualFilter);
    assert_decision(
        &false_node,
        TracePass::AccessPath,
        TraceDecision::NodeEmptyPredicate,
    );
}

#[test]
fn statically_tautological_source_predicates_are_pruned() {
    let label_scoped = plan_traversal(
        g().n_where(Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::compare(Expr::val(10), CompareOp::Gt, Expr::val(1)),
        ])),
        ctx(builtin_label_indexes()),
    );
    let whole_node_source = plan_traversal(
        g().n_where(Predicate::or(vec![
            Predicate::eq("$label", "User"),
            Predicate::compare(Expr::val(10), CompareOp::Gt, Expr::val(1)),
        ])),
        ctx(builtin_label_indexes()),
    );
    let whole_edge_source = plan_traversal(
        g().e_where(Predicate::or(vec![
            Predicate::eq("status", "active"),
            Predicate::compare(Expr::val("planner"), CompareOp::Eq, Expr::val("planner")),
        ])),
        ctx(builtin_label_indexes()),
    );

    assert_node_label_scan(node_access(&label_scoped), "User");
    assert!(matches!(
        node_access(&whole_node_source),
        NodeAccessPlan::AllScan
    ));
    assert!(matches!(
        edge_access(&whole_edge_source),
        EdgeAccessPlan::AllScan
    ));

    assert_no_decision(&label_scoped, TraceDecision::ResidualFilter);
    assert_no_decision(&whole_node_source, TraceDecision::ResidualFilter);
    assert_no_decision(&whole_edge_source, TraceDecision::ResidualFilter);
}

#[test]
fn nullability_contradictions_plan_empty_access() {
    let node_cases = [
        Predicate::and(vec![
            Predicate::is_null("deleted_at"),
            Predicate::is_not_null("deleted_at"),
        ]),
        Predicate::and(vec![
            Predicate::eq("name", "alice"),
            Predicate::is_null("name"),
        ]),
        Predicate::and(vec![
            Predicate::eq("nickname", PropertyValue::Null),
            Predicate::is_not_null("nickname"),
        ]),
        Predicate::and(vec![
            Predicate::is_null("email"),
            Predicate::eq("email", "alice@example.com"),
        ]),
        Predicate::and(vec![Predicate::gt("age", 18), Predicate::is_null("age")]),
    ];

    for predicate in node_cases {
        let plan = plan_traversal(
            g().n_with_label_where("User", predicate),
            ctx(builtin_label_indexes()),
        );

        assert!(matches!(node_access(&plan), NodeAccessPlan::Empty));
        assert_decision(
            &plan,
            TracePass::AccessPath,
            TraceDecision::NodeEmptyPredicate,
        );
    }

    let edge = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::is_null("deleted_at"),
                Predicate::is_not_null("deleted_at"),
            ]),
        ),
        ctx(builtin_label_indexes()),
    );
    let direct_residual = plan_traversal(
        g().n(NodeRef::all()).where_(Predicate::and(vec![
            Predicate::is_not_null("name"),
            Predicate::is_null("name"),
        ])),
        PlannerContext::default(),
    );

    assert!(matches!(edge_access(&edge), EdgeAccessPlan::Empty));
    assert!(matches!(
        node_access(&direct_residual),
        NodeAccessPlan::Empty
    ));
    assert_decision(
        &edge,
        TracePass::AccessPath,
        TraceDecision::EdgeEmptyPredicate,
    );
    assert_decision(
        &direct_residual,
        TracePass::AccessPath,
        TraceDecision::NodeEmptyPredicate,
    );
    assert_no_decision(&direct_residual, TraceDecision::ResidualFilter);
}

#[test]
fn empty_literal_in_predicates_plan_empty_access() {
    let empty_collections = [
        PropertyValue::I64Array(Vec::new()),
        PropertyValue::F64Array(Vec::new()),
        PropertyValue::F32Array(Vec::new()),
        PropertyValue::StringArray(Vec::new()),
        PropertyValue::Array(Vec::new()),
    ];

    for values in empty_collections {
        let node = plan_traversal(
            g().n_with_label_where("User", Predicate::is_in("id", values)),
            ctx(builtin_label_indexes()),
        );

        assert!(matches!(node_access(&node), NodeAccessPlan::Empty));
        assert_decision(
            &node,
            TracePass::AccessPath,
            TraceDecision::NodeEmptyPredicate,
        );
    }

    let edge = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::is_in("id", PropertyValue::Array(Vec::new())),
        ),
        ctx(builtin_label_indexes()),
    );
    let non_empty = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::is_in("id", PropertyValue::I64Array(vec![1])),
        ),
        ctx(builtin_label_indexes()),
    );
    let edge_non_empty = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::is_in("id", PropertyValue::I64Array(vec![1])),
        ),
        ctx(builtin_label_indexes()),
    );
    let dynamic = plan_traversal(
        g().n_with_label_where("User", Predicate::is_in_param("id", "ids")),
        ctx(builtin_label_indexes()),
    );
    let non_collection_literal = plan_traversal(
        g().n_with_label_where("User", Predicate::is_in("id", "not-a-collection")),
        ctx(builtin_label_indexes()),
    );

    assert!(matches!(edge_access(&edge), EdgeAccessPlan::Empty));
    assert_decision(
        &edge,
        TracePass::AccessPath,
        TraceDecision::EdgeEmptyPredicate,
    );
    assert_node_label_scan(node_access(&non_empty), "User");
    assert_edge_label_scan(edge_access(&edge_non_empty), "FOLLOWS");
    assert_node_label_scan(node_access(&dynamic), "User");
    assert_node_label_scan(node_access(&non_collection_literal), "User");
}

#[test]
fn finite_in_scalar_contradictions_plan_empty_access() {
    let equality_after_in = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::is_in("id", PropertyValue::I64Array(vec![1, 2])),
                Predicate::eq("id", 3),
            ]),
        ),
        ctx(builtin_label_indexes()),
    );
    let equality_before_in = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::eq("id", 3),
                Predicate::is_in("id", PropertyValue::I64Array(vec![1, 2])),
            ]),
        ),
        ctx(builtin_label_indexes()),
    );
    let exhausted_by_inequalities = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::is_in("status", PropertyValue::StringArray(vec!["open".into()])),
                Predicate::neq("status", "open"),
            ]),
        ),
        ctx(builtin_label_indexes()),
    );
    let disjoint_intersections = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::is_in("tier", PropertyValue::array(["free", "team"])),
                Predicate::is_in("tier", PropertyValue::array(["enterprise"])),
            ]),
        ),
        ctx(builtin_label_indexes()),
    );
    let exhausted_by_ranges = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::is_in("age", PropertyValue::I64Array(vec![18, 21])),
                Predicate::gt("age", 18),
                Predicate::lt("age", 21),
            ]),
        ),
        ctx(builtin_label_indexes()),
    );
    let typed_float_arrays = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::is_in("rating", PropertyValue::F64Array(vec![1.0, 2.0])),
                Predicate::eq("rating", 3.0f64),
            ]),
        ),
        ctx(builtin_label_indexes()),
    );
    let typed_float32_arrays = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::is_in("score", PropertyValue::F32Array(vec![1.0, 2.0])),
                Predicate::eq("score", 3.0f32),
            ]),
        ),
        ctx(builtin_label_indexes()),
    );
    let nested_stable_values = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::is_in(
                    "payload",
                    PropertyValue::array([
                        PropertyValue::F64(1.0),
                        PropertyValue::F32(2.0),
                        PropertyValue::array([PropertyValue::object([("nested", 1)])]),
                    ]),
                ),
                Predicate::eq("payload", "missing"),
            ]),
        ),
        ctx(builtin_label_indexes()),
    );
    let nullability_excludes_in_values = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::is_null("email"),
                Predicate::is_in(
                    "email",
                    PropertyValue::StringArray(vec!["a@example.com".into()]),
                ),
            ]),
        ),
        ctx(builtin_label_indexes()),
    );
    let direct_residual = plan_traversal(
        g().n(NodeRef::all()).where_(Predicate::and(vec![
            Predicate::is_in("age", PropertyValue::I64Array(vec![18, 21])),
            Predicate::gte("age", 30),
        ])),
        PlannerContext::default(),
    );

    assert!(matches!(
        node_access(&equality_after_in),
        NodeAccessPlan::Empty
    ));
    assert!(matches!(
        node_access(&equality_before_in),
        NodeAccessPlan::Empty
    ));
    assert!(matches!(
        edge_access(&exhausted_by_inequalities),
        EdgeAccessPlan::Empty
    ));
    assert!(matches!(
        node_access(&disjoint_intersections),
        NodeAccessPlan::Empty
    ));
    assert!(matches!(
        node_access(&exhausted_by_ranges),
        NodeAccessPlan::Empty
    ));
    assert!(matches!(
        node_access(&typed_float_arrays),
        NodeAccessPlan::Empty
    ));
    assert!(matches!(
        node_access(&typed_float32_arrays),
        NodeAccessPlan::Empty
    ));
    assert!(matches!(
        node_access(&nested_stable_values),
        NodeAccessPlan::Empty
    ));
    assert!(matches!(
        node_access(&nullability_excludes_in_values),
        NodeAccessPlan::Empty
    ));
    assert!(matches!(
        node_access(&direct_residual),
        NodeAccessPlan::Empty
    ));
    assert_no_decision(&direct_residual, TraceDecision::ResidualFilter);
}

#[test]
fn finite_in_scalar_constraints_keep_feasible_inputs() {
    let intersecting_sets = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::is_in("tier", PropertyValue::array(["free", "team"])),
                Predicate::is_in("tier", PropertyValue::array(["team", "enterprise"])),
            ]),
        ),
        ctx(builtin_label_indexes()),
    );
    let surviving_range_value = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::is_in("age", PropertyValue::I64Array(vec![18, 21])),
                Predicate::gt("age", 18),
            ]),
        ),
        ctx(builtin_label_indexes()),
    );
    let null_remains_feasible = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::is_null("email"),
                Predicate::is_in("email", PropertyValue::array([PropertyValue::Null])),
            ]),
        ),
        ctx(builtin_label_indexes()),
    );
    let non_reflexive_float_values_remain_residual = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::is_in("score", PropertyValue::F64Array(vec![f64::NAN])),
                Predicate::neq("score", PropertyValue::F64(f64::NAN)),
            ]),
        ),
        ctx(builtin_label_indexes()),
    );
    let non_reflexive_array_values_remain_residual = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::is_in(
                    "score",
                    PropertyValue::array([PropertyValue::F64(f64::NAN)]),
                ),
                Predicate::neq("score", PropertyValue::F64(f64::NAN)),
            ]),
        ),
        ctx(builtin_label_indexes()),
    );

    assert_node_label_scan(node_access(&intersecting_sets), "User");
    assert_node_label_scan(node_access(&surviving_range_value), "User");
    assert_node_label_scan(node_access(&null_remains_feasible), "User");
    assert_node_label_scan(
        node_access(&non_reflexive_float_values_remain_residual),
        "User",
    );
    assert_node_label_scan(
        node_access(&non_reflexive_array_values_remain_residual),
        "User",
    );
}

#[test]
fn scalar_range_contradictions_cover_ordered_literal_types() {
    let float64 = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::gt("rating", 4.5f64),
                Predicate::lt("rating", 4.0f64),
            ]),
        ),
        ctx(builtin_label_indexes()),
    );
    let float32 = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::gt("score", 4.5f32),
                Predicate::lt("score", 4.0f32),
            ]),
        ),
        ctx(builtin_label_indexes()),
    );
    let string = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::gt("name", "zoe"),
                Predicate::lt("name", "amy"),
            ]),
        ),
        ctx(builtin_label_indexes()),
    );
    let datetime = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::gt("created_at", PropertyValue::datetime_millis(2_000)),
                Predicate::lt("created_at", PropertyValue::datetime_millis(1_000)),
            ]),
        ),
        ctx(builtin_label_indexes()),
    );

    assert!(matches!(node_access(&float64), NodeAccessPlan::Empty));
    assert!(matches!(node_access(&float32), NodeAccessPlan::Empty));
    assert!(matches!(node_access(&string), NodeAccessPlan::Empty));
    assert!(matches!(node_access(&datetime), NodeAccessPlan::Empty));
}

#[test]
fn all_scalar_impossible_or_branches_plan_empty_access() {
    let predicate = Predicate::or(vec![
        Predicate::and(vec![Predicate::eq("age", 18), Predicate::neq("age", 18)]),
        Predicate::and(vec![Predicate::gt("age", 64), Predicate::lte("age", 64)]),
    ]);
    let plan = plan_traversal(
        g().n_with_label_where("User", predicate),
        ctx(builtin_label_indexes()),
    );

    assert!(matches!(node_access(&plan), NodeAccessPlan::Empty));
    assert_decision(
        &plan,
        TracePass::AccessPath,
        TraceDecision::NodeEmptyPredicate,
    );
}

#[test]
fn static_impossible_branch_pruning_contracts_cover_boolean_boundaries() {
    assert_eq!(
        analysis::label_scope(&Predicate::or(Vec::new())).unwrap(),
        LabelScope::Feasible(FeasibleLabelScope::Unscoped)
    );

    let impossible = Predicate::and(vec![Predicate::eq("age", 18), Predicate::neq("age", 18)]);
    assert!(matches!(
        analysis::prune_statically_impossible_branches(&Predicate::and(vec![
            impossible.clone(),
            Predicate::eq("name", "alice"),
        ]))
        .unwrap(),
        PrunedPredicate::Impossible
    ));

    assert!(matches!(
        analysis::prune_statically_impossible_branches(&Predicate::and(vec![Predicate::eq(
            "name", "alice"
        )]))
        .unwrap(),
        PrunedPredicate::Feasible {
            predicate,
            label: FeasibleLabelScope::Unscoped,
        } if predicate == Predicate::eq("name", "alice")
    ));

    assert_eq!(
        analysis::prune_statically_impossible_branches(&Predicate::and(vec![
            Predicate::compare(Expr::val(10), CompareOp::Gt, Expr::val(1)),
            Predicate::Contains {
                value: Expr::val("planner"),
                substring: Expr::val("plan"),
            },
        ]))
        .unwrap(),
        PrunedPredicate::Tautology
    );

    assert!(matches!(
        analysis::prune_statically_impossible_branches(&Predicate::or(vec![
            impossible.clone(),
            Predicate::eq("name", "alice"),
        ]))
        .unwrap(),
        PrunedPredicate::Feasible {
            predicate,
            label: FeasibleLabelScope::Unscoped,
        } if predicate == Predicate::eq("name", "alice")
    ));

    assert_eq!(
        analysis::prune_statically_impossible_branches(&Predicate::or(vec![
            Predicate::eq("name", "alice"),
            Predicate::compare(Expr::val(10), CompareOp::Gt, Expr::val(1)),
        ]))
        .unwrap(),
        PrunedPredicate::Tautology
    );

    let pruned_label_contradiction = Predicate::and(vec![
        Predicate::or(vec![Predicate::eq("$label", "User"), impossible.clone()]),
        Predicate::eq("$label", "Account"),
    ]);
    assert!(matches!(
        analysis::prune_statically_impossible_branches(&pruned_label_contradiction).unwrap(),
        PrunedPredicate::Impossible
    ));
    assert!(matches!(
        analysis::prune_statically_impossible_branches(&Predicate::and(vec![
            pruned_label_contradiction,
            Predicate::eq("name", "alice"),
        ]))
        .unwrap(),
        PrunedPredicate::Impossible
    ));
}

#[test]
fn impossible_or_label_branches_prune_to_direct_label_scan() {
    let impossible = Predicate::and(vec![
        Predicate::eq("$label", "User"),
        Predicate::eq("$label", "Account"),
    ]);
    let predicate = Predicate::or(vec![impossible, Predicate::eq("$label", "User")]);
    let plan = plan_traversal(g().n_where(predicate), ctx(builtin_label_indexes()));

    assert!(matches!(
        node_access(&plan),
        NodeAccessPlan::LabelScan { label } if label == "User"
    ));
}

#[test]
fn trailing_impossible_or_label_branch_prunes_to_direct_label_scan() {
    let impossible = Predicate::and(vec![
        Predicate::eq("$label", "User"),
        Predicate::eq("$label", "Account"),
    ]);
    let predicate = Predicate::or(vec![Predicate::eq("$label", "User"), impossible]);
    let plan = plan_traversal(g().n_where(predicate), ctx(builtin_label_indexes()));

    assert!(matches!(
        node_access(&plan),
        NodeAccessPlan::LabelScan { label } if label == "User"
    ));
}

#[test]
fn impossible_or_branches_are_pruned_before_access_planning() {
    let impossible = Predicate::and(vec![Predicate::eq("age", 18), Predicate::neq("age", 18)]);
    let node_feasible = Predicate::and(vec![
        Predicate::eq("$label", "User"),
        Predicate::eq("username", "alice"),
    ]);
    let node = plan_traversal(
        g().n_where(Predicate::or(vec![
            impossible.clone(),
            node_feasible.clone(),
        ])),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())),
    );
    let edge_feasible = Predicate::and(vec![
        Predicate::eq("$label", "FOLLOWS"),
        Predicate::eq("status", "active"),
    ]);
    let edge = plan_traversal(
        g().e_where(Predicate::or(vec![edge_feasible.clone(), impossible])),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())),
    );

    assert!(matches!(
        node_access(&node),
        NodeAccessPlan::EqualityIndex { key, .. }
            if key.label == "User" && key.property == "username"
    ));
    assert_no_decision(&node, TraceDecision::NodeScanOr);

    assert!(matches!(
        edge_access(&edge),
        EdgeAccessPlan::EqualityIndex { key, .. }
            if key.label == "FOLLOWS" && key.property == "status"
    ));
    assert_no_decision(&edge, TraceDecision::EdgeScanOr);
}

#[test]
fn mixed_impossible_or_branches_plan_empty_access() {
    let scalar_impossible =
        Predicate::and(vec![Predicate::eq("age", 18), Predicate::neq("age", 18)]);
    let node_label_impossible = Predicate::and(vec![
        Predicate::eq("$label", "User"),
        Predicate::eq("$label", "Account"),
    ]);
    let node = plan_traversal(
        g().n_where(Predicate::or(vec![
            scalar_impossible.clone(),
            node_label_impossible,
        ])),
        ctx(builtin_label_indexes()),
    );
    let edge_label_impossible = Predicate::and(vec![
        Predicate::eq("$label", "FOLLOWS"),
        Predicate::eq("$label", "LIKES"),
    ]);
    let edge = plan_traversal(
        g().e_where(Predicate::or(vec![
            edge_label_impossible,
            scalar_impossible,
        ])),
        ctx(builtin_label_indexes()),
    );

    assert!(matches!(node_access(&node), NodeAccessPlan::Empty));
    assert!(matches!(edge_access(&edge), EdgeAccessPlan::Empty));
    assert_decision(
        &node,
        TracePass::AccessPath,
        TraceDecision::NodeEmptyPredicate,
    );
    assert_decision(
        &edge,
        TracePass::AccessPath,
        TraceDecision::EdgeEmptyPredicate,
    );
}

#[test]
fn all_impossible_or_label_branches_plan_empty_access() {
    let predicate = Predicate::or(vec![
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::eq("$label", "Account"),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "Team"),
            Predicate::eq("$label", "Org"),
        ]),
    ]);
    let plan = plan_traversal(g().n_where(predicate), ctx(builtin_label_indexes()));

    assert!(matches!(node_access(&plan), NodeAccessPlan::Empty));
    assert_decision(
        &plan,
        TracePass::AccessPath,
        TraceDecision::NodeEmptyLabelScope,
    );
}

#[test]
fn empty_boolean_predicates_are_rejected_before_access_planning() {
    let cases = [
        (
            read_batch().var_as("result", g().n_where(Predicate::and(Vec::new()))),
            PlannerContext::default(),
            PredicateSetOp::And,
        ),
        (
            read_batch().var_as(
                "result",
                g().n(NodeRef::all())
                    .dedup()
                    .where_(Predicate::and(Vec::new())),
            ),
            PlannerContext::default(),
            PredicateSetOp::And,
        ),
        (
            read_batch().var_as(
                "result",
                g().n(NodeRef::all())
                    .order_by("age", Order::Asc)
                    .where_(Predicate::and(Vec::new())),
            ),
            PlannerContext::default(),
            PredicateSetOp::And,
        ),
        (
            read_batch().var_as(
                "result",
                g().n_where(Predicate::and(vec![
                    Predicate::eq("$label", "User"),
                    Predicate::and(Vec::new()),
                ])),
            ),
            ctx(builtin_label_indexes()),
            PredicateSetOp::And,
        ),
        (
            read_batch().var_as(
                "result",
                g().n_where(Predicate::and(vec![
                    Predicate::eq("$label", "User"),
                    Predicate::or(Vec::new()),
                ])),
            ),
            ctx(builtin_label_indexes()),
            PredicateSetOp::Or,
        ),
        (
            read_batch().var_as("result", g().n_where(Predicate::or(Vec::new()))),
            PlannerContext::default(),
            PredicateSetOp::Or,
        ),
        (
            read_batch().var_as("result", g().e_where(Predicate::and(Vec::new()))),
            PlannerContext::default(),
            PredicateSetOp::And,
        ),
        (
            read_batch().var_as(
                "result",
                g().e_where(Predicate::and(vec![
                    Predicate::eq("$label", "FOLLOWS"),
                    Predicate::and(Vec::new()),
                ])),
            ),
            ctx(builtin_label_indexes()),
            PredicateSetOp::And,
        ),
        (
            read_batch().var_as(
                "result",
                g().e_where(Predicate::and(vec![
                    Predicate::eq("$label", "FOLLOWS"),
                    Predicate::or(Vec::new()),
                ])),
            ),
            ctx(builtin_label_indexes()),
            PredicateSetOp::Or,
        ),
        (
            read_batch().var_as("result", g().e_where(Predicate::or(Vec::new()))),
            PlannerContext::default(),
            PredicateSetOp::Or,
        ),
    ];

    for (batch, context, op) in cases {
        assert_eq!(
            plan_read(&batch, &context).unwrap_err(),
            PlannerError::InvalidPredicateArity {
                op,
                min: 1,
                actual: 0,
            }
        );
    }
}

#[test]
fn unscoped_and_non_indexable_predicates_fall_back_to_full_scan_residuals() {
    let node_predicate = Predicate::and(vec![
        Predicate::contains("bio", "systems"),
        Predicate::has_key("name"),
    ]);
    let node = plan_traversal(
        g().n_where(node_predicate.clone()),
        PlannerContext::default(),
    );

    let NodeAccessPlan::ScanThenFilter { source, residual } = node_access(&node) else {
        panic!("expected node scan residual: {:?}", node_access(&node));
    };
    assert!(matches!(source.as_ref(), NodeAccessPlan::AllScan));
    assert_eq!(residual, &node_predicate);
    assert_decision(&node, TracePass::AccessPath, TraceDecision::NodeFullScan);

    let edge_predicate = Predicate::and(vec![
        Predicate::contains("body", "systems"),
        Predicate::has_key("tenant"),
    ]);
    let edge = plan_traversal(
        g().e_where(edge_predicate.clone()),
        PlannerContext::default(),
    );

    let EdgeAccessPlan::ScanThenFilter { source, residual } = edge_access(&edge) else {
        panic!("expected edge scan residual: {:?}", edge_access(&edge));
    };
    assert!(matches!(source.as_ref(), EdgeAccessPlan::AllScan));
    assert_eq!(residual, &edge_predicate);
    assert_decision(&edge, TracePass::AccessPath, TraceDecision::EdgeFullScan);

    let edge_or_predicate = Predicate::or(vec![
        Predicate::contains("body", "systems"),
        Predicate::has_key("tenant"),
    ]);
    let edge_or = plan_traversal(
        g().e_where(edge_or_predicate.clone()),
        PlannerContext::default(),
    );

    let EdgeAccessPlan::ScanThenFilter { source, residual } = edge_access(&edge_or) else {
        panic!(
            "expected edge OR scan residual: {:?}",
            edge_access(&edge_or)
        );
    };
    assert!(matches!(source.as_ref(), EdgeAccessPlan::AllScan));
    assert_eq!(residual, &edge_or_predicate);
    assert_decision(&edge_or, TracePass::AccessPath, TraceDecision::EdgeScanOr);
}

#[test]
fn empty_index_union_helpers_return_no_plan() {
    let ctx = PlannerContext::default();
    let node_label = NonEmptyString::new("User").unwrap();
    let edge_label = NonEmptyString::new("FOLLOWS").unwrap();
    let mut planner = crate::planning::Planner::new(&ctx);

    let node_union = planner
        .node_union_plan(&[], &node_label, "$.result")
        .unwrap();
    let edge_union = planner
        .edge_union_plan(&[], &edge_label, "$.result")
        .unwrap();

    assert!(!node_union.covered);
    assert!(node_union.source.is_none());
    assert!(!edge_union.covered);
    assert!(edge_union.source.is_none());
}

#[test]
fn non_string_label_predicate_does_not_create_label_scope() {
    let predicate = Predicate::eq("$label", 42);
    let plan = plan_traversal(g().n_where(predicate.clone()), ctx(builtin_label_indexes()));

    let NodeAccessPlan::ScanThenFilter { source, residual } = node_access(&plan) else {
        panic!("expected scan residual: {:?}", node_access(&plan));
    };
    assert!(matches!(source.as_ref(), NodeAccessPlan::AllScan));
    assert_eq!(residual, &predicate);
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::NodeFullScan);
}

#[test]
fn non_equality_label_comparison_does_not_create_label_scope() {
    let predicate = Predicate::compare(Expr::prop("$label"), CompareOp::Neq, Expr::val("User"));
    let plan = plan_traversal(g().n_where(predicate.clone()), ctx(builtin_label_indexes()));

    let NodeAccessPlan::ScanThenFilter { source, residual } = node_access(&plan) else {
        panic!("expected scan residual: {:?}", node_access(&plan));
    };
    assert!(matches!(source.as_ref(), NodeAccessPlan::AllScan));
    assert_eq!(residual, &predicate);
}

#[test]
fn non_indexable_equality_atoms_remain_residual_under_label_scope() {
    let predicate = Predicate::and(vec![
        Predicate::eq("$label", "User"),
        Predicate::compare(Expr::param("wanted"), CompareOp::Eq, Expr::val("bob")),
    ]);
    let plan = plan_traversal(
        g().n_where(predicate.clone()),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())),
    );

    let NodeAccessPlan::ScanThenFilter { source, residual } = node_access(&plan) else {
        panic!("expected scan residual: {:?}", node_access(&plan));
    };
    assert!(matches!(
        source.as_ref(),
        NodeAccessPlan::LabelScan { label } if label == "User"
    ));
    assert_eq!(
        residual,
        &Predicate::compare(Expr::param("wanted"), CompareOp::Eq, Expr::val("bob"))
    );
    assert_no_decision(&plan, TraceDecision::NodeEqualityIndex);
}

#[test]
fn nested_literal_node_equality_keeps_label_scan_residual() {
    let predicate = Predicate::eq("payload", PropertyValue::array([1]));
    let plan = plan_traversal(
        g().n_with_label_where("User", predicate.clone()),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "payload").unwrap())),
    );

    let NodeAccessPlan::ScanThenFilter { residual, .. } = node_access(&plan) else {
        panic!("expected scan residual: {:?}", node_access(&plan));
    };
    assert_node_label_scan(node_access(&plan), "User");
    assert_eq!(residual, &predicate);
    assert_no_decision(&plan, TraceDecision::NodeEqualityIndex);
}

#[test]
fn inexact_literal_node_equality_keeps_label_scan_residual() {
    let cases = [
        (
            "rating",
            Predicate::eq("rating", PropertyValue::from(4.5_f64)),
        ),
        (
            "tags",
            Predicate::eq("tags", PropertyValue::I64Array(vec![1])),
        ),
    ];

    for (property, predicate) in cases {
        let plan = plan_traversal(
            g().n_with_label_where("User", predicate.clone()),
            ctx(builtin_label_indexes()
                .with_node_eq(ScopedPropertyKey::try_new("User", property).unwrap())),
        );

        let NodeAccessPlan::ScanThenFilter { residual, .. } = node_access(&plan) else {
            panic!("expected scan residual: {:?}", node_access(&plan));
        };
        assert_node_label_scan(node_access(&plan), "User");
        assert_eq!(residual, &predicate);
        assert_no_decision(&plan, TraceDecision::NodeEqualityIndex);
    }
}

#[test]
fn nested_literal_edge_equality_keeps_label_scan_residual() {
    let predicate = Predicate::eq("payload", PropertyValue::object([("nested", 1)]));
    let plan = plan_traversal(
        g().e_with_label_where("FOLLOWS", predicate.clone()),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "payload").unwrap())),
    );

    let EdgeAccessPlan::ScanThenFilter { residual, .. } = edge_access(&plan) else {
        panic!("expected scan residual: {:?}", edge_access(&plan));
    };
    assert_edge_label_scan(edge_access(&plan), "FOLLOWS");
    assert_eq!(residual, &predicate);
    assert_no_decision(&plan, TraceDecision::EdgeEqualityIndex);
}

#[test]
fn literal_in_indexes_disabled_keep_label_scan_residuals() {
    let mut node_ctx =
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "status").unwrap()));
    node_ctx.limits = PlannerLimits {
        max_index_union_branches: IndexUnionBranchLimit::Disabled,
    };
    let node = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::is_in("status", PropertyValue::array(["active", "pending"])),
        ),
        node_ctx,
    );

    let mut edge_ctx = ctx(builtin_label_indexes()
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap()));
    edge_ctx.limits = PlannerLimits {
        max_index_union_branches: IndexUnionBranchLimit::Disabled,
    };
    let edge = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::is_in("status", PropertyValue::array(["active", "pending"])),
        ),
        edge_ctx,
    );

    assert_node_label_scan(node_access(&node), "User");
    assert_edge_label_scan(edge_access(&edge), "FOLLOWS");
    assert_no_decision(&node, TraceDecision::NodeUnion);
    assert_no_decision(&edge, TraceDecision::EdgeUnion);
}

#[test]
fn dotted_literal_in_atoms_remain_residual_under_label_scope() {
    let node = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::is_in(
                "metadata.status",
                PropertyValue::StringArray(vec!["active".into()]),
            ),
        ),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "metadata.status").unwrap())),
    );
    let edge = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::is_in(
                "metadata.status",
                PropertyValue::StringArray(vec!["active".into()]),
            ),
        ),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "metadata.status").unwrap())),
    );

    assert_node_label_scan(node_access(&node), "User");
    assert_edge_label_scan(edge_access(&edge), "FOLLOWS");
    assert_no_decision(&node, TraceDecision::NodeEqualityIndex);
    assert_no_decision(&edge, TraceDecision::EdgeEqualityIndex);
}

#[test]
fn nested_literal_in_values_keep_label_scan_residuals() {
    let node_predicate =
        Predicate::is_in("payload", PropertyValue::array([PropertyValue::array([1])]));
    let node = plan_traversal(
        g().n_with_label_where("User", node_predicate.clone()),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "payload").unwrap())),
    );
    let edge_predicate = Predicate::is_in(
        "payload",
        PropertyValue::array([PropertyValue::object([("nested", 1)])]),
    );
    let edge = plan_traversal(
        g().e_with_label_where("FOLLOWS", edge_predicate.clone()),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "payload").unwrap())),
    );

    let NodeAccessPlan::ScanThenFilter { residual, .. } = node_access(&node) else {
        panic!("expected node scan residual: {:?}", node_access(&node));
    };
    assert_node_label_scan(node_access(&node), "User");
    assert_eq!(residual, &node_predicate);
    assert_no_decision(&node, TraceDecision::NodeEqualityIndex);

    let EdgeAccessPlan::ScanThenFilter { residual, .. } = edge_access(&edge) else {
        panic!("expected edge scan residual: {:?}", edge_access(&edge));
    };
    assert_edge_label_scan(edge_access(&edge), "FOLLOWS");
    assert_eq!(residual, &edge_predicate);
    assert_no_decision(&edge, TraceDecision::EdgeEqualityIndex);
}

#[test]
fn missing_node_indexes_keep_label_scan_residuals() {
    let missing_equality = plan_traversal(
        g().n_with_label_where("User", Predicate::eq("unindexed", "value")),
        ctx(builtin_label_indexes()),
    );
    let missing_range = plan_traversal(
        g().n_with_label_where("User", Predicate::gt("unindexed_score", 90)),
        ctx(builtin_label_indexes()),
    );

    assert_node_label_scan(node_access(&missing_equality), "User");
    assert_node_label_scan(node_access(&missing_range), "User");
    assert_no_decision(&missing_equality, TraceDecision::NodeEqualityIndex);
    assert_no_decision(&missing_range, TraceDecision::NodeRangeIndex);
}

#[test]
fn missing_edge_indexes_keep_label_scan_residuals() {
    let missing_equality = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::eq("unindexed", "value")),
        ctx(builtin_label_indexes()),
    );
    let missing_range = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::gt("unindexed_score", 90)),
        ctx(builtin_label_indexes()),
    );

    assert_edge_label_scan(edge_access(&missing_equality), "FOLLOWS");
    assert_edge_label_scan(edge_access(&missing_range), "FOLLOWS");
    assert_no_decision(&missing_equality, TraceDecision::EdgeEqualityIndex);
    assert_no_decision(&missing_range, TraceDecision::EdgeRangeIndex);
}

#[test]
fn single_branch_node_or_collapses_to_index_plan() {
    let predicate = Predicate::or(vec![Predicate::and(vec![
        Predicate::eq("$label", "User"),
        Predicate::eq("username", "alice"),
    ])]);
    let plan = plan_traversal(
        g().n_where(predicate),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())),
    );

    assert!(matches!(
        node_access(&plan),
        NodeAccessPlan::EqualityIndex { key, .. }
            if key.label == "User" && key.property == "username"
    ));
    assert_no_decision(&plan, TraceDecision::NodeUnion);
}

#[test]
fn single_branch_edge_or_collapses_to_index_plan() {
    let predicate = Predicate::or(vec![Predicate::and(vec![
        Predicate::eq("$label", "FOLLOWS"),
        Predicate::eq("status", "active"),
    ])]);
    let plan = plan_traversal(
        g().e_where(predicate),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())),
    );

    assert!(matches!(
        edge_access(&plan),
        EdgeAccessPlan::EqualityIndex { key, .. }
            if key.label == "FOLLOWS" && key.property == "status"
    ));
    assert_no_decision(&plan, TraceDecision::EdgeUnion);
}

#[test]
fn edge_or_branch_limit_falls_back_to_label_scan() {
    let predicate = Predicate::or(vec![
        Predicate::and(vec![
            Predicate::eq("$label", "FOLLOWS"),
            Predicate::eq("status", "active"),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "FOLLOWS"),
            Predicate::eq("tenant_id", "acme"),
        ]),
    ]);
    let mut planner_ctx = ctx(builtin_label_indexes()
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "tenant_id").unwrap()));
    planner_ctx.limits = PlannerLimits {
        max_index_union_branches: IndexUnionBranchLimit::limited(1).unwrap(),
    };
    let plan = plan_traversal(g().e_where(predicate), planner_ctx);

    assert_edge_label_scan(edge_access(&plan), "FOLLOWS");
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::EdgeScanOr);
}

#[test]
fn literal_equality_index_subsumes_same_property_node_range_candidates() {
    let predicate = Predicate::and(vec![
        Predicate::eq("age", 30),
        Predicate::gte("age", 21),
        Predicate::lt("age", 40),
    ]);
    let plan = plan_traversal(
        g().n_with_label_where("User", predicate),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "age").unwrap())
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let plans = node_candidate_sources(node_access(&plan));

    assert_eq!(plans.len(), 1);
    assert_node_eq(
        &plans,
        "User",
        "age",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(30)).unwrap()),
    );
    assert_no_decision(&plan, TraceDecision::NodeRangeIndex);
    assert_no_decision(&plan, TraceDecision::NodeIntersect);
}

#[test]
fn literal_equality_index_subsumes_same_property_edge_between_candidate() {
    let predicate = Predicate::and(vec![
        Predicate::eq("since", 2024),
        Predicate::between("since", 2020, 2025),
    ]);
    let plan = plan_traversal(
        g().e_with_label_where("FOLLOWS", predicate),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "since").unwrap())
            .with_edge_range(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "since", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let plans = edge_candidate_sources(edge_access(&plan));

    assert_eq!(plans.len(), 1);
    assert_edge_eq(
        &plans,
        "FOLLOWS",
        "since",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(2024)).unwrap()),
    );
    assert_no_decision(&plan, TraceDecision::EdgeRangeIndex);
    assert_no_decision(&plan, TraceDecision::EdgeIntersect);
}

#[test]
fn literal_equality_range_implication_requires_ordered_literal_bounds() {
    assert_eq!(
        analysis::literal_equality_property_implying_range(
            &Predicate::eq("age", 21),
            &Predicate::gte("age", 21),
        )
        .as_deref(),
        Some("age"),
    );
    assert_eq!(
        analysis::literal_equality_property_implying_range(
            &Predicate::eq("age", 21),
            &Predicate::gt("age", 21),
        )
        .as_deref(),
        None,
    );
    assert_eq!(
        analysis::literal_equality_property_implying_range(
            &Predicate::eq("age", 21),
            &Predicate::lte("age", 21),
        )
        .as_deref(),
        Some("age"),
    );
    assert_eq!(
        analysis::literal_equality_property_implying_range(
            &Predicate::eq("age", 21),
            &Predicate::lt("age", 21),
        )
        .as_deref(),
        None,
    );
    assert_eq!(
        analysis::literal_equality_property_implying_range(
            &Predicate::eq("age", "unknown"),
            &Predicate::gte("age", 21),
        )
        .as_deref(),
        None,
    );
    assert_eq!(
        analysis::literal_equality_property_implying_range(
            &Predicate::eq("age", "unknown"),
            &Predicate::lte("age", 21),
        )
        .as_deref(),
        None,
    );
}

#[test]
fn top_level_literal_equality_index_subsumes_same_property_range_candidates() {
    let node_plan = plan_traversal(
        g().n_where(Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::eq("age", 30),
            Predicate::gte("age", 21),
        ])),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "age").unwrap())
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let node_sources = node_candidate_sources(node_access(&node_plan));
    assert_eq!(node_sources.len(), 1);
    assert_node_eq(
        &node_sources,
        "User",
        "age",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(30)).unwrap()),
    );
    assert_no_decision(&node_plan, TraceDecision::NodeRangeIndex);

    let edge_plan = plan_traversal(
        g().e_where(Predicate::and(vec![
            Predicate::eq("$label", "FOLLOWS"),
            Predicate::eq("since", 2024),
            Predicate::lte("since", 2024),
        ])),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "since").unwrap())
            .with_edge_range(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "since", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let edge_sources = edge_candidate_sources(edge_access(&edge_plan));
    assert_eq!(edge_sources.len(), 1);
    assert_edge_eq(
        &edge_sources,
        "FOLLOWS",
        "since",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(2024)).unwrap()),
    );
    assert_no_decision(&edge_plan, TraceDecision::EdgeRangeIndex);
}

#[test]
fn dotted_property_range_candidates_are_not_subsumed_by_equality_indexes() {
    let node_plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::eq("metadata.age", 30),
                Predicate::gte("metadata.age", 21),
            ]),
        ),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "metadata.age").unwrap())
            .with_node_range(
                ScopedPropertyDirectionKey::try_new(
                    "User",
                    "metadata.age",
                    RangeIndexDirection::Asc,
                )
                .unwrap(),
            )),
    );
    assert_node_label_scan(node_access(&node_plan), "User");
    assert_no_decision(&node_plan, TraceDecision::NodeEqualityIndex);
    assert_no_decision(&node_plan, TraceDecision::NodeRangeIndex);

    let edge_plan = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::eq("metadata.since", 2024),
                Predicate::lte("metadata.since", 2024),
            ]),
        ),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "metadata.since").unwrap())
            .with_edge_range(
                ScopedPropertyDirectionKey::try_new(
                    "FOLLOWS",
                    "metadata.since",
                    RangeIndexDirection::Asc,
                )
                .unwrap(),
            )),
    );
    assert_edge_label_scan(edge_access(&edge_plan), "FOLLOWS");
    assert_no_decision(&edge_plan, TraceDecision::EdgeEqualityIndex);
    assert_no_decision(&edge_plan, TraceDecision::EdgeRangeIndex);
}

#[test]
fn same_property_range_candidates_remain_when_equality_is_not_proven_indexed() {
    let missing_equality_index = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![Predicate::eq("age", 30), Predicate::gte("age", 21)]),
        ),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );
    let parameter_equality = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::eq_param("age", "wanted_age"),
                Predicate::gte("age", 21),
            ]),
        ),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "age").unwrap())
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );

    let range_only = node_candidate_sources(node_access(&missing_equality_index));
    assert_eq!(range_only.len(), 1);
    assert_node_range(
        &range_only,
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
            ),
        },
    );
    assert_no_decision(&missing_equality_index, TraceDecision::NodeEqualityIndex);

    let parameter_sources = node_candidate_sources(node_access(&parameter_equality));
    assert_eq!(parameter_sources.len(), 2);
    assert_node_eq(
        &parameter_sources,
        "User",
        "age",
        IndexValue::Param(NonEmptyString::new("wanted_age").unwrap()),
    );
    assert_node_range(
        &parameter_sources,
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
            ),
        },
    );
    assert_decision(
        &parameter_equality,
        TracePass::AccessPath,
        TraceDecision::NodeIntersect,
    );
}

#[test]
fn less_than_and_lte_ranges_build_upper_bounds() {
    let predicate = Predicate::and(vec![
        Predicate::eq("$label", "User"),
        Predicate::lt("age", 30),
        Predicate::lte("rank", 5),
    ]);
    let plan = plan_traversal(
        g().n_where(predicate),
        ctx(builtin_label_indexes()
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
            )
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "rank", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let plans = node_candidate_sources(node_access(&plan));

    assert_node_range(
        &plans,
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Upper {
            upper: IndexBound::Exclusive(
                RangeIndexValue::literal(PropertyValue::from(30)).unwrap(),
            ),
        },
    );
    assert_node_range(
        &plans,
        ScopedPropertyDirectionKey::try_new("User", "rank", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Upper {
            upper: IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(5)).unwrap()),
        },
    );
    assert_no_node_label_scan_source(&plans);
}

#[test]
fn greater_than_range_builds_lower_bound() {
    let plan = plan_traversal(
        g().n_with_label_where("User", Predicate::gt("score", 900)),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "score", RangeIndexDirection::Asc).unwrap(),
        )),
    );

    assert_node_range(
        &node_candidate_sources(node_access(&plan)),
        ScopedPropertyDirectionKey::try_new("User", "score", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Exclusive(
                RangeIndexValue::literal(PropertyValue::from(900)).unwrap(),
            ),
        },
    );
}

#[test]
fn descending_node_range_index_is_used_when_ascending_index_is_absent() {
    let key =
        ScopedPropertyDirectionKey::try_new("User", "score", RangeIndexDirection::Desc).unwrap();
    let plan = plan_traversal(
        g().n_with_label_where("User", Predicate::gt("score", 900)),
        ctx(builtin_label_indexes().with_node_range(key.clone())),
    );

    assert_node_range(
        &node_candidate_sources(node_access(&plan)),
        key,
        IndexRange::Lower {
            lower: IndexBound::Exclusive(
                RangeIndexValue::literal(PropertyValue::from(900)).unwrap(),
            ),
        },
    );
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::NodeRangeIndex);
}

#[test]
fn descending_edge_range_index_is_used_when_ascending_index_is_absent() {
    let key =
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "since", RangeIndexDirection::Desc).unwrap();
    let plan = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::lte("since", 2024)),
        ctx(builtin_label_indexes().with_edge_range(key.clone())),
    );

    assert_edge_range(
        &edge_candidate_sources(edge_access(&plan)),
        key,
        IndexRange::Upper {
            upper: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(2024)).unwrap(),
            ),
        },
    );
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::EdgeRangeIndex);
}

#[test]
fn compare_lt_range_builds_upper_bound() {
    let predicate = Predicate::and(vec![
        Predicate::eq("$label", "User"),
        Predicate::compare(Expr::prop("age"), CompareOp::Lt, Expr::val(30)),
    ]);
    let plan = plan_traversal(
        g().n_where(predicate),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );

    assert_node_range(
        &node_candidate_sources(node_access(&plan)),
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Upper {
            upper: IndexBound::Exclusive(
                RangeIndexValue::literal(PropertyValue::from(30)).unwrap(),
            ),
        },
    );
}

#[test]
fn reversed_lte_compare_builds_lower_bound() {
    let predicate = Predicate::and(vec![
        Predicate::eq("$label", "User"),
        Predicate::compare(Expr::val(18), CompareOp::Lte, Expr::prop("age")),
    ]);
    let plan = plan_traversal(
        g().n_where(predicate),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );

    assert_node_range(
        &node_candidate_sources(node_access(&plan)),
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(18)).unwrap(),
            ),
        },
    );
}

#[test]
fn range_params_build_range_bounds() {
    let predicate = Predicate::and(vec![
        Predicate::eq("$label", "User"),
        Predicate::gte_param("age", "min_age"),
    ]);
    let plan = plan_traversal(
        g().n_where(predicate),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );

    assert_node_range(
        &node_candidate_sources(node_access(&plan)),
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(RangeIndexValue::Param(
                NonEmptyString::new("min_age").unwrap(),
            )),
        },
    );
}

#[test]
fn constant_comparisons_do_not_form_range_atoms() {
    let predicate = Predicate::and(vec![
        Predicate::eq("$label", "User"),
        Predicate::compare(Expr::val(10), CompareOp::Gt, Expr::val(1)),
    ]);
    let plan = plan_traversal(
        g().n_where(predicate.clone()),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );

    assert_node_label_scan(node_access(&plan), "User");
    assert_no_decision(&plan, TraceDecision::NodeRangeIndex);
    assert_no_decision(&plan, TraceDecision::ResidualFilter);
}

#[test]
fn non_orderable_range_literals_remain_residual_under_label_scope() {
    let cases = [
        Predicate::gt("active", true),
        Predicate::compare(
            Expr::val(PropertyValue::from(vec![1_u8, 2])),
            CompareOp::Lt,
            Expr::prop("age"),
        ),
        Predicate::between("age", true, 64),
        Predicate::between("age", 18, PropertyValue::Null),
    ];

    for non_orderable in cases {
        let predicate =
            Predicate::and(vec![Predicate::eq("$label", "User"), non_orderable.clone()]);
        let plan = plan_traversal(
            g().n_where(predicate.clone()),
            ctx(builtin_label_indexes()
                .with_node_range(
                    ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                        .unwrap(),
                )
                .with_node_range(
                    ScopedPropertyDirectionKey::try_new("User", "active", RangeIndexDirection::Asc)
                        .unwrap(),
                )),
        );

        let NodeAccessPlan::ScanThenFilter { source, residual } = node_access(&plan) else {
            panic!("expected scan residual: {:?}", node_access(&plan));
        };
        assert!(matches!(
            source.as_ref(),
            NodeAccessPlan::LabelScan { label } if label == "User"
        ));
        assert_eq!(residual, &non_orderable);
        assert_no_decision(&plan, TraceDecision::NodeRangeIndex);
    }
}

#[test]
fn inverted_between_range_literals_plan_empty_access() {
    let predicate = Predicate::and(vec![
        Predicate::eq("$label", "User"),
        Predicate::between("age", 64, 18),
    ]);
    let plan = plan_traversal(
        g().n_where(predicate.clone()),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );

    assert!(matches!(node_access(&plan), NodeAccessPlan::Empty));
    assert_decision(
        &plan,
        TracePass::AccessPath,
        TraceDecision::NodeEmptyPredicate,
    );
    assert_no_decision(&plan, TraceDecision::NodeRangeIndex);
}

#[test]
fn mixed_kind_between_range_literals_remain_residual_under_label_scope() {
    let residual_predicate = Predicate::between("age", 18, "bob");
    let predicate = Predicate::and(vec![
        Predicate::eq("$label", "User"),
        residual_predicate.clone(),
    ]);
    let plan = plan_traversal(
        g().n_where(predicate.clone()),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );

    let NodeAccessPlan::ScanThenFilter { source, residual } = node_access(&plan) else {
        panic!("expected scan residual: {:?}", node_access(&plan));
    };
    assert!(matches!(
        source.as_ref(),
        NodeAccessPlan::LabelScan { label } if label == "User"
    ));
    assert_eq!(residual, &residual_predicate);
    assert_no_decision(&plan, TraceDecision::NodeRangeIndex);
}

#[test]
fn between_with_non_property_value_remains_label_residual() {
    let residual_predicate = Predicate::Between {
        value: Expr::param("value"),
        min: Expr::val(1),
        max: Expr::val(10),
    };
    let predicate = Predicate::and(vec![
        Predicate::eq("$label", "User"),
        residual_predicate.clone(),
    ]);
    let plan = plan_traversal(
        g().n_where(predicate.clone()),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );

    let NodeAccessPlan::ScanThenFilter { source, residual } = node_access(&plan) else {
        panic!("expected scan residual: {:?}", node_access(&plan));
    };
    assert!(matches!(
        source.as_ref(),
        NodeAccessPlan::LabelScan { label } if label == "User"
    ));
    assert_eq!(residual, &residual_predicate);
    assert_no_decision(&plan, TraceDecision::NodeRangeIndex);
}

#[test]
fn dotted_range_properties_do_not_use_range_indexes() {
    let plan = plan_traversal(
        g().n_with_label_where("User", Predicate::gte("metadata.age", 18)),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "metadata.age", RangeIndexDirection::Asc)
                .unwrap(),
        )),
    );

    assert_node_label_scan(node_access(&plan), "User");
    assert_no_decision(&plan, TraceDecision::NodeRangeIndex);
}

#[test]
fn dotted_edge_range_properties_do_not_use_range_indexes() {
    let plan = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::gte("metadata.weight", 10)),
        ctx(builtin_label_indexes().with_edge_range(
            ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "metadata.weight",
                RangeIndexDirection::Asc,
            )
            .unwrap(),
        )),
    );

    assert_edge_label_scan(edge_access(&plan), "FOLLOWS");
    assert_no_decision(&plan, TraceDecision::EdgeRangeIndex);
}

fn predicate_contains(haystack: &Predicate, needle: &Predicate) -> bool {
    haystack == needle
        || match haystack {
            Predicate::And { predicates } | Predicate::Or { predicates } => predicates
                .iter()
                .any(|predicate| predicate_contains(predicate, needle)),
            Predicate::Not { predicate } => predicate_contains(predicate, needle),
            _ => false,
        }
}
