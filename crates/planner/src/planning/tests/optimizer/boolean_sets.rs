use crate::planning::tests::support::*;

#[test]
fn node_and_combination_uses_scoped_indexes_and_residual_without_label_scan() {
    let predicate = Predicate::and(vec![
        Predicate::eq("username", "alice"),
        Predicate::gte("age", 21),
        Predicate::starts_with("bio", "engineer"),
    ]);
    let plan = plan_traversal(
        g().n_with_label_where("User", predicate),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let plans = node_candidate_sources(node_access(&plan));

    assert_eq!(plans.len(), 2);
    assert_node_eq(
        &plans,
        "User",
        "username",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("alice")).unwrap()),
    );
    assert_node_range(
        &plans,
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
            ),
        },
    );
    assert_no_node_label_scan_source(&plans);
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::NodeIntersect);
}

#[test]
fn scoped_node_and_partial_index_coverage_keeps_residual_filter() {
    let predicate = Predicate::and(vec![
        Predicate::eq("$label", "User"),
        Predicate::eq("username", "alice"),
        Predicate::gte("age", 21),
        Predicate::starts_with("bio", "engineer"),
    ]);
    let plan = plan_traversal(
        g().n_where(predicate.clone()),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let NodeAccessPlan::ScanThenFilter { source, residual } = node_access(&plan) else {
        panic!("expected residual filter over node index intersection: {plan:?}");
    };
    let NodeAccessPlan::Intersect(plans) = source.as_ref() else {
        panic!("expected node index intersection source: {source:?}");
    };

    assert_eq!(
        residual.predicate(),
        &Predicate::starts_with("bio", "engineer")
    );
    assert_eq!(plans.len(), 2);
    assert_node_eq(
        plans,
        "User",
        "username",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("alice")).unwrap()),
    );
    assert_node_range(
        plans,
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
            ),
        },
    );
    assert_no_node_label_scan_source(plans);
}

#[test]
fn nested_node_and_partial_index_coverage_keeps_residual_filter() {
    let predicate = Predicate::and(vec![
        Predicate::and(vec![
            Predicate::eq("username", "alice"),
            Predicate::starts_with("bio", "engineer"),
        ]),
        Predicate::gte("age", 21),
    ]);
    let plan = plan_traversal(
        g().n_with_label_where("User", predicate.clone()),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let NodeAccessPlan::ScanThenFilter { source, residual } = node_access(&plan) else {
        panic!("expected residual filter over nested node index intersection: {plan:?}");
    };
    let NodeAccessPlan::Intersect(plans) = source.as_ref() else {
        panic!("expected nested node index intersection source: {source:?}");
    };

    assert_eq!(
        residual.predicate(),
        &Predicate::starts_with("bio", "engineer")
    );
    assert_eq!(plans.len(), 2);
    assert_node_eq(
        plans,
        "User",
        "username",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("alice")).unwrap()),
    );
    assert_node_range(
        plans,
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
            ),
        },
    );
}

#[test]
fn duplicate_and_index_candidates_are_planned_once() {
    let node_plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::eq("username", "alice"),
                Predicate::eq("username", "alice"),
                Predicate::gte("age", 21),
            ]),
        ),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let node_sources = node_candidate_sources(node_access(&node_plan));
    assert_eq!(node_sources.len(), 2);
    assert_eq!(
        node_sources
            .iter()
            .filter(|source| matches!(
                *source,
                NodeAccessPlan::EqualityIndex { key, .. } if key.property == "username"
            ))
            .count(),
        1
    );
    assert_node_range(
        &node_sources,
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
            ),
        },
    );

    let edge_plan = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::eq("status", "active"),
                Predicate::eq("status", "active"),
                Predicate::lt("weight", 50),
            ]),
        ),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
            .with_edge_range(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let edge_sources = edge_candidate_sources(edge_access(&edge_plan));
    assert_eq!(edge_sources.len(), 2);
    assert_eq!(
        edge_sources
            .iter()
            .filter(|source| matches!(
                *source,
                EdgeAccessPlan::EqualityIndex { key, .. } if key.property == "status"
            ))
            .count(),
        1
    );
    assert_edge_range(
        &edge_sources,
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Upper {
            upper: IndexBound::Exclusive(
                RangeIndexValue::literal(PropertyValue::from(50)).unwrap(),
            ),
        },
    );
}

#[test]
fn scoped_edge_and_partial_index_coverage_keeps_residual_filter() {
    let predicate = Predicate::and(vec![
        Predicate::eq("$label", "FOLLOWS"),
        Predicate::eq("status", "active"),
        Predicate::lt("weight", 50),
        Predicate::starts_with("note", "friend"),
    ]);
    let plan = plan_traversal(
        g().e_where(predicate.clone()),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
            .with_edge_range(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let EdgeAccessPlan::ScanThenFilter { source, residual } = edge_access(&plan) else {
        panic!("expected residual filter over edge index intersection: {plan:?}");
    };
    let EdgeAccessPlan::Intersect(plans) = source.as_ref() else {
        panic!("expected edge index intersection source: {source:?}");
    };

    assert_eq!(
        residual.predicate(),
        &Predicate::starts_with("note", "friend")
    );
    assert_eq!(plans.len(), 2);
    assert_edge_eq(
        plans,
        "FOLLOWS",
        "status",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap()),
    );
    assert_edge_range(
        plans,
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Upper {
            upper: IndexBound::Exclusive(
                RangeIndexValue::literal(PropertyValue::from(50)).unwrap(),
            ),
        },
    );
    assert_no_edge_label_scan_source(plans);
}

#[test]
fn nested_edge_and_partial_index_coverage_keeps_residual_filter() {
    let predicate = Predicate::and(vec![
        Predicate::and(vec![
            Predicate::eq("status", "active"),
            Predicate::starts_with("note", "friend"),
        ]),
        Predicate::lt("weight", 50),
    ]);
    let plan = plan_traversal(
        g().e_with_label_where("FOLLOWS", predicate.clone()),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
            .with_edge_range(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let EdgeAccessPlan::ScanThenFilter { source, residual } = edge_access(&plan) else {
        panic!("expected residual filter over nested edge index intersection: {plan:?}");
    };
    let EdgeAccessPlan::Intersect(plans) = source.as_ref() else {
        panic!("expected nested edge index intersection source: {source:?}");
    };

    assert_eq!(
        residual.predicate(),
        &Predicate::starts_with("note", "friend")
    );
    assert_eq!(plans.len(), 2);
    assert_edge_eq(
        plans,
        "FOLLOWS",
        "status",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap()),
    );
    assert_edge_range(
        plans,
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Upper {
            upper: IndexBound::Exclusive(
                RangeIndexValue::literal(PropertyValue::from(50)).unwrap(),
            ),
        },
    );
}

#[test]
fn node_and_range_sources_keep_only_narrowest_static_range() {
    for predicate in [
        Predicate::and(vec![Predicate::gte("age", 21), Predicate::gt("age", 30)]),
        Predicate::and(vec![Predicate::gt("age", 30), Predicate::gte("age", 21)]),
    ] {
        let plan = plan_traversal(
            g().n_with_label_where("User", predicate),
            ctx(builtin_label_indexes().with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
        );
        let sources = node_candidate_sources(node_access(&plan));

        assert_eq!(sources.len(), 1);
        assert_node_range(
            &sources,
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
            IndexRange::Lower {
                lower: IndexBound::Exclusive(
                    RangeIndexValue::literal(PropertyValue::from(30)).unwrap(),
                ),
            },
        );
        assert_no_decision(&plan, TraceDecision::NodeIntersect);
    }
}

#[test]
fn edge_and_range_sources_keep_only_narrowest_static_range() {
    for predicate in [
        Predicate::and(vec![
            Predicate::lte("weight", 50),
            Predicate::lt("weight", 45),
        ]),
        Predicate::and(vec![
            Predicate::lt("weight", 45),
            Predicate::lte("weight", 50),
        ]),
    ] {
        let plan = plan_traversal(
            g().e_with_label_where("FOLLOWS", predicate),
            ctx(builtin_label_indexes().with_edge_range(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
        );
        let sources = edge_candidate_sources(edge_access(&plan));

        assert_eq!(sources.len(), 1);
        assert_edge_range(
            &sources,
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
            IndexRange::Upper {
                upper: IndexBound::Exclusive(
                    RangeIndexValue::literal(PropertyValue::from(45)).unwrap(),
                ),
            },
        );
        assert_no_decision(&plan, TraceDecision::EdgeIntersect);
    }
}

#[test]
fn node_and_complementary_range_sources_merge_into_single_between_range() {
    for predicate in [
        Predicate::and(vec![
            Predicate::gte("age", 21),
            Predicate::lt("age", 65),
            Predicate::gt("age", 30),
        ]),
        Predicate::and(vec![
            Predicate::lt("age", 65),
            Predicate::gt("age", 30),
            Predicate::gte("age", 21),
        ]),
    ] {
        let plan = plan_traversal(
            g().n_with_label_where("User", predicate),
            ctx(builtin_label_indexes().with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
        );
        let sources = node_candidate_sources(node_access(&plan));

        assert_eq!(sources.len(), 1);
        assert_node_range(
            &sources,
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
            IndexBetweenRange::new(
                IndexBound::Exclusive(RangeIndexValue::literal(PropertyValue::from(30)).unwrap()),
                IndexBound::Exclusive(RangeIndexValue::literal(PropertyValue::from(65)).unwrap()),
            )
            .map(IndexRange::Between)
            .unwrap(),
        );
        assert_no_decision(&plan, TraceDecision::NodeIntersect);
    }
}

#[test]
fn edge_and_complementary_range_sources_merge_into_single_between_range() {
    for predicate in [
        Predicate::and(vec![
            Predicate::lte("weight", 50),
            Predicate::gt("weight", 10),
            Predicate::lt("weight", 45),
        ]),
        Predicate::and(vec![
            Predicate::gt("weight", 10),
            Predicate::lt("weight", 45),
            Predicate::lte("weight", 50),
        ]),
    ] {
        let plan = plan_traversal(
            g().e_with_label_where("FOLLOWS", predicate),
            ctx(builtin_label_indexes().with_edge_range(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
        );
        let sources = edge_candidate_sources(edge_access(&plan));

        assert_eq!(sources.len(), 1);
        assert_edge_range(
            &sources,
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
            IndexBetweenRange::new(
                IndexBound::Exclusive(RangeIndexValue::literal(PropertyValue::from(10)).unwrap()),
                IndexBound::Exclusive(RangeIndexValue::literal(PropertyValue::from(45)).unwrap()),
            )
            .map(IndexRange::Between)
            .unwrap(),
        );
        assert_no_decision(&plan, TraceDecision::EdgeIntersect);
    }
}

#[test]
fn node_labeled_or_combination_uses_scoped_index_union_without_label_scan() {
    let plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::eq("username", "alice"),
                Predicate::eq("email", "alice@example.com"),
            ]),
        ),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
            .with_node_eq(ScopedPropertyKey::try_new("User", "email").unwrap())),
    );
    let plans = node_candidate_sources(node_access(&plan));
    let [source] = plans.as_slice() else {
        panic!("expected single node union source: {plans:?}");
    };
    let NodeAccessPlan::Union(union) = source else {
        panic!("expected node union source: {plans:?}");
    };

    assert_node_eq(
        union,
        "User",
        "username",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("alice")).unwrap()),
    );
    assert_node_eq(
        union,
        "User",
        "email",
        IndexValue::Literal(
            SecondaryIndexLiteral::new(PropertyValue::from("alice@example.com")).unwrap(),
        ),
    );
    assert_no_node_label_scan_source(&plans);
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::NodeUnion);
    assert_no_decision(&plan, TraceDecision::NodeIntersect);
}

#[test]
fn node_literal_in_values_use_equality_index_union_without_label_scan() {
    let plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::is_in(
                "status",
                PropertyValue::StringArray(vec!["active".into(), "pending".into()]),
            ),
        ),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "status").unwrap())),
    );
    let plans = node_candidate_sources(node_access(&plan));
    let [source] = plans.as_slice() else {
        panic!("expected single node union source: {plans:?}");
    };
    let NodeAccessPlan::Union(union) = source else {
        panic!("expected node union source: {plans:?}");
    };

    assert_node_eq(
        union,
        "User",
        "status",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap()),
    );
    assert_node_eq(
        union,
        "User",
        "status",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("pending")).unwrap()),
    );
    assert_no_node_label_scan_source(&plans);
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::NodeUnion);
}

#[test]
fn node_literal_in_duplicate_values_dedupe_to_single_equality_index() {
    let mut planner_ctx =
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "status").unwrap()));
    planner_ctx.limits = PlannerLimits {
        max_index_union_branches: IndexUnionBranchLimit::limited(1).unwrap(),
    };
    let plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::is_in("status", PropertyValue::array(["active", "active"])),
        ),
        planner_ctx,
    );
    let plans = node_candidate_sources(node_access(&plan));

    assert_eq!(plans.len(), 1);
    assert_node_eq(
        &plans,
        "User",
        "status",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap()),
    );
    assert_no_node_label_scan_source(&plans);
    assert_no_decision(&plan, TraceDecision::NodeUnion);
}

#[test]
fn node_top_level_or_with_same_label_uses_union_without_extra_scan() {
    let predicate = Predicate::or(vec![
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::eq("username", "alice"),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::gte("age", 21),
        ]),
    ]);
    let plan = plan_traversal(
        g().n_where(predicate),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let NodeAccessPlan::Union(branches) = node_access(&plan) else {
        panic!("expected top-level node union: {:?}", node_access(&plan));
    };

    assert_node_eq(
        branches,
        "User",
        "username",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("alice")).unwrap()),
    );
    assert_node_range(
        branches,
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
            ),
        },
    );
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::NodeUnion);
}

#[test]
fn node_or_sources_drop_equality_branches_subsumed_by_wider_range_source() {
    let predicate = Predicate::or(vec![
        Predicate::eq("age", 30),
        Predicate::and(vec![
            Predicate::eq("age", 40),
            Predicate::eq("username", "alice"),
        ]),
        Predicate::gte("age", 21),
    ]);
    let plan = plan_traversal(
        g().n_with_label_where("User", predicate),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "age").unwrap())
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let sources = node_candidate_sources(node_access(&plan));

    assert_eq!(sources.len(), 1);
    assert_node_range(
        &sources,
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
            ),
        },
    );
    assert_no_decision(&plan, TraceDecision::NodeUnion);
}

#[test]
fn node_or_sources_drop_range_branches_subsumed_by_wider_range_source() {
    let predicate = Predicate::or(vec![Predicate::gt("age", 30), Predicate::gte("age", 21)]);
    let plan = plan_traversal(
        g().n_with_label_where("User", predicate),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );
    let sources = node_candidate_sources(node_access(&plan));

    assert_eq!(sources.len(), 1);
    assert_node_range(
        &sources,
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
            ),
        },
    );
    assert_no_decision(&plan, TraceDecision::NodeUnion);
}

#[test]
fn node_or_sources_drop_intersections_subsumed_by_wider_intersection() {
    let predicate = Predicate::or(vec![
        Predicate::and(vec![
            Predicate::eq("username", "alice"),
            Predicate::gte("age", 21),
        ]),
        Predicate::and(vec![
            Predicate::eq("username", "alice"),
            Predicate::gte("age", 21),
            Predicate::eq("status", "active"),
        ]),
    ]);
    let plan = plan_traversal(
        g().n_with_label_where("User", predicate),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
            .with_node_eq(ScopedPropertyKey::try_new("User", "status").unwrap())
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let sources = node_candidate_sources(node_access(&plan));

    assert_eq!(sources.len(), 2);
    assert_node_eq(
        &sources,
        "User",
        "username",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("alice")).unwrap()),
    );
    assert_node_range(
        &sources,
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
            ),
        },
    );
    assert!(
        sources.iter().all(|source| !matches!(
            *source,
            NodeAccessPlan::EqualityIndex { key, .. } if key.property == "status"
        )),
        "unexpected subsumed status branch: {sources:?}"
    );
    assert_no_decision(&plan, TraceDecision::NodeUnion);
}

#[test]
fn node_or_intersection_branches_factor_common_sources() {
    let predicate = Predicate::or(vec![
        Predicate::and(vec![
            Predicate::eq("username", "alice"),
            Predicate::gte("age", 21),
        ]),
        Predicate::and(vec![
            Predicate::eq("username", "alice"),
            Predicate::gt("score", 900),
        ]),
    ]);
    let plan = plan_traversal(
        g().n_with_label_where("User", predicate),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
            )
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "score", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let sources = node_candidate_sources(node_access(&plan));
    let range_union = sources
        .iter()
        .find_map(|source| match source {
            NodeAccessPlan::Union(branches) => Some(branches),
            _ => None,
        })
        .unwrap_or_else(|| panic!("expected factored node range union: {sources:?}"));

    assert_eq!(sources.len(), 2);
    assert_node_eq(
        &sources,
        "User",
        "username",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("alice")).unwrap()),
    );
    assert_eq!(range_union.len(), 2);
    assert_node_range(
        range_union,
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
            ),
        },
    );
    assert_node_range(
        range_union,
        ScopedPropertyDirectionKey::try_new("User", "score", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Exclusive(
                RangeIndexValue::literal(PropertyValue::from(900)).unwrap(),
            ),
        },
    );
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::NodeUnion);
}

#[test]
fn node_range_union_intersection_narrows_overlapping_branches() {
    let plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::between("age", 0, 10),
                Predicate::between("age", 20, 30),
            ]),
        )
        .where_(Predicate::between("age", 5, 25)),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );
    let sources = node_candidate_sources(node_access(&plan));
    let [source] = sources.as_slice() else {
        panic!("expected one narrowed node range union source: {sources:?}");
    };
    let NodeAccessPlan::Union(branches) = source else {
        panic!("expected node range union source: {source:?}");
    };

    assert_eq!(branches.len(), 2);
    assert_node_range(
        branches,
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::between(
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(5)).unwrap()),
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(10)).unwrap()),
        ),
    );
    assert_node_range(
        branches,
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::between(
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(20)).unwrap()),
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(25)).unwrap()),
        ),
    );
    assert_no_decision(&plan, TraceDecision::NodeIntersect);
}

#[test]
fn node_range_union_intersection_drops_disjoint_branches() {
    let plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::between("age", 0, 10),
                Predicate::between("age", 20, 30),
            ]),
        )
        .where_(Predicate::between("age", 5, 15)),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );
    let sources = node_candidate_sources(node_access(&plan));
    let [_source] = sources.as_slice() else {
        panic!("expected one surviving node range source: {sources:?}");
    };

    assert_node_range(
        &sources,
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::between(
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(5)).unwrap()),
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(10)).unwrap()),
        ),
    );
    assert_no_decision(&plan, TraceDecision::NodeIntersect);
}

#[test]
fn node_range_union_intersection_collapses_all_disjoint_branches_to_empty() {
    let plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::between("age", 0, 10),
                Predicate::between("age", 20, 30),
            ]),
        )
        .where_(Predicate::between("age", 40, 50)),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );

    assert!(matches!(node_access(&plan), NodeAccessPlan::Empty));
    assert_no_decision(&plan, TraceDecision::NodeIntersect);
}

#[test]
fn node_range_unions_intersect_before_runtime_scan() {
    let planner_ctx = ctx(builtin_label_indexes().with_node_range(
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
    ));
    let left = Predicate::or(vec![
        Predicate::between("age", 0, 10),
        Predicate::between("age", 20, 30),
    ]);
    let right = Predicate::or(vec![
        Predicate::between("age", 5, 15),
        Predicate::between("age", 25, 35),
    ]);

    for plan in [
        plan_traversal(
            g().n_with_label_where("User", left.clone())
                .where_(right.clone()),
            planner_ctx.clone(),
        ),
        plan_traversal(
            g().n_with_label_where("User", right).where_(left),
            planner_ctx,
        ),
    ] {
        let sources = node_candidate_sources(node_access(&plan));
        let [source] = sources.as_slice() else {
            panic!("expected one narrowed node range union source: {sources:?}");
        };
        let NodeAccessPlan::Union(branches) = source else {
            panic!("expected narrowed node range union: {source:?}");
        };

        assert_eq!(branches.len(), 2);
        assert_node_range(
            branches,
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
            IndexRange::between(
                IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(5)).unwrap()),
                IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(10)).unwrap()),
            ),
        );
        assert_node_range(
            branches,
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
            IndexRange::between(
                IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(25)).unwrap()),
                IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(30)).unwrap()),
            ),
        );
        assert_no_decision(&plan, TraceDecision::NodeIntersect);
    }
}

#[test]
fn node_range_unions_intersection_collapses_empty() {
    let plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::between("age", 0, 10),
                Predicate::between("age", 20, 30),
            ]),
        )
        .where_(Predicate::or(vec![
            Predicate::between("age", 40, 50),
            Predicate::between("age", 60, 70),
        ])),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );

    assert!(matches!(node_access(&plan), NodeAccessPlan::Empty));
    assert_no_decision(&plan, TraceDecision::NodeIntersect);
}

#[test]
fn node_range_union_intersection_keeps_mismatched_keys_and_fanout_conservative() {
    let mismatched_keys = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::between("age", 0, 10),
                Predicate::between("age", 20, 30),
            ]),
        )
        .where_(Predicate::or(vec![
            Predicate::between("score", 0, 10),
            Predicate::between("score", 20, 30),
        ])),
        ctx(builtin_label_indexes()
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
            )
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "score", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let fanout = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::between("age", 0, 10),
                Predicate::between("age", 20, 30),
                Predicate::between("age", 40, 50),
            ]),
        )
        .where_(Predicate::or(vec![
            Predicate::between("age", 5, 25),
            Predicate::between("age", 28, 45),
            Predicate::between("age", 48, 60),
        ])),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );

    for plan in [&mismatched_keys, &fanout] {
        assert!(
            matches!(node_access(plan), NodeAccessPlan::Intersect(_)),
            "expected conservative node intersection: {:?}",
            node_access(plan)
        );
        assert_decision(plan, TracePass::AccessPath, TraceDecision::NodeIntersect);
    }
}

#[test]
fn node_equality_union_intersects_range_union_before_runtime_scan() {
    let planner_ctx = ctx(builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("User", "age").unwrap())
        .with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        ));
    let literal_in = Predicate::is_in("age", PropertyValue::I64Array(vec![10, 20, 30]));
    let range_union = Predicate::or(vec![
        Predicate::between("age", 5, 15),
        Predicate::between("age", 25, 35),
    ]);

    for plan in [
        plan_traversal(
            g().n_with_label_where("User", literal_in.clone())
                .where_(range_union.clone()),
            planner_ctx.clone(),
        ),
        plan_traversal(
            g().n_with_label_where("User", range_union)
                .where_(literal_in),
            planner_ctx,
        ),
    ] {
        let sources = node_candidate_sources(node_access(&plan));
        let [source] = sources.as_slice() else {
            panic!("expected one retained node equality union source: {sources:?}");
        };
        let NodeAccessPlan::Union(branches) = source else {
            panic!("expected retained node equality union: {source:?}");
        };

        assert_eq!(branches.len(), 2);
        assert_node_eq(
            branches,
            "User",
            "age",
            IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(10)).unwrap()),
        );
        assert_node_eq(
            branches,
            "User",
            "age",
            IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(30)).unwrap()),
        );
        assert_no_decision(&plan, TraceDecision::NodeIntersect);
    }
}

#[test]
fn node_equality_union_range_union_intersection_collapses_empty() {
    let plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::is_in("age", PropertyValue::I64Array(vec![10, 20])),
        )
        .where_(Predicate::or(vec![
            Predicate::between("age", 30, 40),
            Predicate::between("age", 50, 60),
        ])),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "age").unwrap())
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );

    assert!(matches!(node_access(&plan), NodeAccessPlan::Empty));
    assert_no_decision(&plan, TraceDecision::NodeIntersect);
}

#[test]
fn node_or_literal_in_union_drops_values_subsumed_by_range_source() {
    let literal_in = Predicate::is_in("age", PropertyValue::I64Array(vec![10, 30]));
    let range = Predicate::gte("age", 21);
    for predicate in [
        Predicate::or(vec![literal_in.clone(), range.clone()]),
        Predicate::or(vec![range, literal_in]),
    ] {
        let plan = plan_traversal(
            g().n_with_label_where("User", predicate),
            ctx(builtin_label_indexes()
                .with_node_eq(ScopedPropertyKey::try_new("User", "age").unwrap())
                .with_node_range(
                    ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                        .unwrap(),
                )),
        );
        let NodeAccessPlan::Union(branches) = node_scan_source(node_access(&plan)) else {
            panic!("expected node union source: {:?}", node_access(&plan));
        };

        assert_eq!(branches.len(), 2);
        assert_node_eq(
            branches,
            "User",
            "age",
            IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(10)).unwrap()),
        );
        assert_node_range(
            branches,
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
            IndexRange::Lower {
                lower: IndexBound::Inclusive(
                    RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
                ),
            },
        );
        assert!(branches
            .iter()
            .all(|branch| !matches!(branch.as_ref(), NodeAccessPlan::Union(_))));
        assert!(!branches.iter().any(|branch| matches!(
            branch.as_ref(),
            NodeAccessPlan::EqualityIndex { value, .. }
                if value == &IndexValue::Literal(
                    SecondaryIndexLiteral::new(PropertyValue::from(30)).unwrap()
                )
        )));
    }
}

#[test]
fn node_or_literal_in_union_disappears_when_range_subsumes_all_values() {
    let literal_in = Predicate::is_in("age", PropertyValue::I64Array(vec![30, 40]));
    let range = Predicate::gte("age", 21);
    for predicate in [
        Predicate::or(vec![literal_in.clone(), range.clone()]),
        Predicate::or(vec![range, literal_in]),
    ] {
        let plan = plan_traversal(
            g().n_with_label_where("User", predicate),
            ctx(builtin_label_indexes()
                .with_node_eq(ScopedPropertyKey::try_new("User", "age").unwrap())
                .with_node_range(
                    ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                        .unwrap(),
                )),
        );
        let sources = node_candidate_sources(node_access(&plan));

        assert_eq!(sources.len(), 1);
        assert_node_range(
            &sources,
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
            IndexRange::Lower {
                lower: IndexBound::Inclusive(
                    RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
                ),
            },
        );
        assert!(!matches!(
            node_scan_source(node_access(&plan)),
            NodeAccessPlan::Union(_)
        ));
    }
}

#[test]
fn node_or_literal_in_union_flattens_dynamic_and_mixed_union_sources() {
    let dynamic_range = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::is_in("age", PropertyValue::I64Array(vec![10, 30])),
                Predicate::gte_param("age", "min_age"),
            ]),
        ),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "age").unwrap())
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let NodeAccessPlan::Union(dynamic_branches) = node_scan_source(node_access(&dynamic_range))
    else {
        panic!(
            "expected node union source: {:?}",
            node_access(&dynamic_range)
        );
    };
    assert_eq!(dynamic_branches.len(), 3);
    assert!(dynamic_branches
        .iter()
        .all(|branch| !matches!(branch.as_ref(), NodeAccessPlan::Union(_))));
    assert_node_eq(
        dynamic_branches,
        "User",
        "age",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(10)).unwrap()),
    );
    assert_node_eq(
        dynamic_branches,
        "User",
        "age",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(30)).unwrap()),
    );
    assert_node_range(
        dynamic_branches,
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(RangeIndexValue::param("min_age").unwrap()),
        },
    );

    let mixed_union = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::or(vec![
                    Predicate::is_in("age", PropertyValue::I64Array(vec![10])),
                    Predicate::eq("username", "alice"),
                ]),
                Predicate::gte("age", 21),
            ]),
        ),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "age").unwrap())
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let NodeAccessPlan::Union(mixed_branches) = node_scan_source(node_access(&mixed_union)) else {
        panic!(
            "expected node union source: {:?}",
            node_access(&mixed_union)
        );
    };
    assert_eq!(mixed_branches.len(), 3);
    assert!(mixed_branches
        .iter()
        .all(|branch| !matches!(branch.as_ref(), NodeAccessPlan::Union(_))));
    assert_node_eq(
        mixed_branches,
        "User",
        "age",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(10)).unwrap()),
    );
    assert_node_eq(
        mixed_branches,
        "User",
        "username",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("alice")).unwrap()),
    );
    assert_node_range(
        mixed_branches,
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
            ),
        },
    );

    let mixed_same_property_range = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::or(vec![
                    Predicate::is_in("age", PropertyValue::I64Array(vec![10])),
                    Predicate::gte("age", 31),
                ]),
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
    let NodeAccessPlan::Union(mixed_same_property_branches) =
        node_scan_source(node_access(&mixed_same_property_range))
    else {
        panic!(
            "expected node union source: {:?}",
            node_access(&mixed_same_property_range)
        );
    };
    assert_eq!(mixed_same_property_branches.len(), 2);
    assert!(mixed_same_property_branches
        .iter()
        .all(|branch| !matches!(branch.as_ref(), NodeAccessPlan::Union(_))));
    assert_node_eq(
        mixed_same_property_branches,
        "User",
        "age",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(10)).unwrap()),
    );
    assert_node_range(
        mixed_same_property_branches,
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
            ),
        },
    );
}

#[test]
fn node_or_overlapping_literal_in_unions_flatten_and_dedupe_before_branch_limit() {
    let mut planner_ctx =
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "status").unwrap()));
    planner_ctx.limits = PlannerLimits {
        max_index_union_branches: IndexUnionBranchLimit::limited(3).unwrap(),
    };
    let plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::is_in("status", PropertyValue::array(["active", "pending"])),
                Predicate::is_in("status", PropertyValue::array(["pending", "archived"])),
            ]),
        ),
        planner_ctx,
    );
    let NodeAccessPlan::Union(branches) = node_scan_source(node_access(&plan)) else {
        panic!(
            "expected flattened node union source: {:?}",
            node_access(&plan)
        );
    };

    assert_eq!(branches.len(), 3);
    assert!(branches
        .iter()
        .all(|branch| !matches!(branch.as_ref(), NodeAccessPlan::Union(_))));
    for value in ["active", "pending", "archived"] {
        assert_node_eq(
            branches,
            "User",
            "status",
            IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(value)).unwrap()),
        );
    }
    assert_no_node_label_scan_source(&node_candidate_sources(node_access(&plan)));
}

#[test]
fn nested_node_or_sources_participate_in_union_subsumption() {
    let range_then_nested = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::gte("age", 21),
                Predicate::or(vec![Predicate::eq("age", 30), Predicate::eq("age", 40)]),
            ]),
        ),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "age").unwrap())
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let range_sources = node_candidate_sources(node_access(&range_then_nested));
    assert_eq!(range_sources.len(), 1);
    assert_node_range(
        &range_sources,
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
            ),
        },
    );

    let nested_then_equality = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::or(vec![
                    Predicate::gte("age", 21),
                    Predicate::eq("username", "alice"),
                ]),
                Predicate::eq("age", 30),
            ]),
        ),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "age").unwrap())
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let NodeAccessPlan::Union(branches) = node_scan_source(node_access(&nested_then_equality))
    else {
        panic!(
            "expected nested node union source: {:?}",
            node_access(&nested_then_equality)
        );
    };

    assert_eq!(branches.len(), 2);
    assert_node_range(
        branches,
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
            ),
        },
    );
    assert_node_eq(
        branches,
        "User",
        "username",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("alice")).unwrap()),
    );
    assert!(
        branches.iter().all(|branch| !matches!(
            branch.as_ref(),
            NodeAccessPlan::EqualityIndex { key, .. } if key.property == "age"
        )),
        "subsumed age equality branch remained in {branches:?}"
    );
}

#[test]
fn node_or_with_unindexed_branch_falls_back_to_label_scan() {
    let plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::eq("username", "alice"),
                Predicate::contains("bio", "systems"),
            ]),
        ),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())),
    );

    assert_node_label_scan(node_access(&plan), "User");
    assert_no_decision(&plan, TraceDecision::NodeUnion);
}

#[test]
fn node_or_covered_branch_subsumes_residual_branch_without_runtime_filter() {
    let plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::eq("username", "alice"),
                Predicate::and(vec![
                    Predicate::eq("username", "alice"),
                    Predicate::contains("bio", "systems"),
                ]),
            ]),
        ),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())),
    );

    assert!(matches!(
        node_access(&plan),
        NodeAccessPlan::EqualityIndex { key, value, .. }
            if key.label == "User"
                && key.property == "username"
                && value == &IndexValue::Literal(
                    SecondaryIndexLiteral::new(PropertyValue::from("alice")).unwrap()
                )
    ));
    assert_no_decision(&plan, TraceDecision::NodeScanOr);
    assert_no_decision(&plan, TraceDecision::ResidualFilter);
}

#[test]
fn node_or_with_partially_indexed_branches_uses_union_source_residual() {
    let predicate = Predicate::or(vec![
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::eq("username", "alice"),
            Predicate::starts_with("bio", "engineer"),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::eq("email", "alice@example.com"),
            Predicate::contains("bio", "database"),
        ]),
    ]);
    let plan = plan_traversal(
        g().n_where(predicate.clone()),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
            .with_node_eq(ScopedPropertyKey::try_new("User", "email").unwrap())),
    );
    let NodeAccessPlan::ScanThenFilter { source, residual } = node_access(&plan) else {
        panic!("expected node OR residual fallback: {plan:?}");
    };
    let NodeAccessPlan::Union(branches) = source.as_ref() else {
        panic!("expected node OR residual over union source: {plan:?}");
    };

    assert_eq!(branches.len(), 2);
    assert_node_eq(
        branches,
        "User",
        "username",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("alice")).unwrap()),
    );
    assert_node_eq(
        branches,
        "User",
        "email",
        IndexValue::Literal(
            SecondaryIndexLiteral::new(PropertyValue::from("alice@example.com")).unwrap(),
        ),
    );
    assert_eq!(residual.predicate(), &predicate);
    assert_no_node_label_scan_source(branches);
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::NodeScanOr);
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::NodeUnion);
}

#[test]
fn node_or_with_shared_partial_index_uses_common_source_residual() {
    let predicate = Predicate::or(vec![
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::eq("username", "alice"),
            Predicate::eq("tenant_id", "acme"),
            Predicate::starts_with("bio", "engineer"),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::eq("username", "alice"),
            Predicate::eq("tenant_id", "acme"),
            Predicate::contains("bio", "database"),
        ]),
    ]);
    let plan = plan_traversal(
        g().n_where(predicate.clone()),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
            .with_node_eq(ScopedPropertyKey::try_new("User", "tenant_id").unwrap())),
    );
    let NodeAccessPlan::ScanThenFilter { source, residual } = node_access(&plan) else {
        panic!("expected node OR residual over shared index source: {plan:?}");
    };
    let sources = node_candidate_sources(source.as_ref());

    assert_eq!(sources.len(), 2);
    assert_node_eq(
        &sources,
        "User",
        "username",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("alice")).unwrap()),
    );
    assert_node_eq(
        &sources,
        "User",
        "tenant_id",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("acme")).unwrap()),
    );
    assert_eq!(residual.predicate(), &predicate);
    assert_no_node_label_scan_source(&sources);
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::NodeScanOr);
    assert_no_decision(&plan, TraceDecision::NodeUnion);
}

#[test]
fn node_or_with_subsuming_partial_range_uses_wider_source_residual() {
    let predicate = Predicate::or(vec![
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::gt("age", 30),
            Predicate::starts_with("bio", "engineer"),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::gte("age", 21),
            Predicate::contains("bio", "database"),
        ]),
    ]);
    let plan = plan_traversal(
        g().n_where(predicate.clone()),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );
    let NodeAccessPlan::ScanThenFilter { source, residual } = node_access(&plan) else {
        panic!("expected node OR residual over subsuming range source: {plan:?}");
    };
    let sources = node_candidate_sources(source.as_ref());

    assert_eq!(sources.len(), 1);
    assert_node_range(
        &sources,
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
            ),
        },
    );
    assert_eq!(residual.predicate(), &predicate);
    assert_no_node_label_scan_source(&sources);
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::NodeScanOr);
    assert_no_decision(&plan, TraceDecision::NodeUnion);
}

#[test]
fn node_or_label_only_branches_plan_direct_label_scan() {
    let predicate = Predicate::or(vec![
        Predicate::eq("$label", "User"),
        Predicate::compare(Expr::val("User"), CompareOp::Eq, Expr::prop("$label")),
    ]);
    let plan = plan_traversal(g().n_where(predicate), ctx(builtin_label_indexes()));

    assert!(matches!(
        node_access(&plan),
        NodeAccessPlan::LabelScan { label } if label == "User"
    ));
    assert_no_decision(&plan, TraceDecision::NodeScanOr);
    assert_no_decision(&plan, TraceDecision::NodeUnion);
}

#[test]
fn node_or_branch_limit_falls_back_to_label_scan() {
    let predicate = Predicate::or(vec![
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::eq("username", "alice"),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::eq("email", "alice@example.com"),
        ]),
    ]);
    let mut planner_ctx = ctx(builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
        .with_node_eq(ScopedPropertyKey::try_new("User", "email").unwrap()));
    planner_ctx.limits = PlannerLimits {
        max_index_union_branches: IndexUnionBranchLimit::limited(1).unwrap(),
    };
    let plan = plan_traversal(g().n_where(predicate), planner_ctx);

    assert_node_label_scan(node_access(&plan), "User");
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::NodeScanOr);
}

#[test]
fn node_partial_or_union_branch_limit_falls_back_to_label_scan() {
    let predicate = Predicate::or(vec![
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::eq("username", "alice"),
            Predicate::starts_with("bio", "engineer"),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::eq("email", "alice@example.com"),
            Predicate::contains("bio", "database"),
        ]),
    ]);
    let mut planner_ctx = ctx(builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
        .with_node_eq(ScopedPropertyKey::try_new("User", "email").unwrap()));
    planner_ctx.limits = PlannerLimits {
        max_index_union_branches: IndexUnionBranchLimit::limited(1).unwrap(),
    };
    let plan = plan_traversal(g().n_where(predicate), planner_ctx);

    assert_node_label_scan(node_access(&plan), "User");
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::NodeScanOr);
    assert_no_decision(&plan, TraceDecision::NodeUnion);
}

#[test]
fn duplicate_node_or_branches_collapse_before_branch_limit() {
    let branch = Predicate::and(vec![
        Predicate::eq("$label", "User"),
        Predicate::eq("username", "alice"),
    ]);
    let predicate = Predicate::or(vec![branch.clone(), branch.clone(), branch]);
    let mut planner_ctx = ctx(builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap()));
    planner_ctx.limits = PlannerLimits {
        max_index_union_branches: IndexUnionBranchLimit::limited(1).unwrap(),
    };
    let plan = plan_traversal(g().n_where(predicate), planner_ctx);

    assert!(matches!(
        node_access(&plan),
        NodeAccessPlan::EqualityIndex { key, .. }
            if key.label == "User" && key.property == "username"
    ));
    assert_no_decision(&plan, TraceDecision::NodeUnion);
    assert_no_decision(&plan, TraceDecision::NodeScanOr);
}

#[test]
fn node_or_later_subsuming_branch_collapses_before_branch_limit() {
    let predicate = Predicate::or(vec![
        Predicate::eq("age", 30),
        Predicate::eq("age", 40),
        Predicate::gte("age", 21),
    ]);
    let mut planner_ctx = ctx(builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("User", "age").unwrap())
        .with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        ));
    planner_ctx.limits = PlannerLimits {
        max_index_union_branches: IndexUnionBranchLimit::limited(1).unwrap(),
    };
    let plan = plan_traversal(g().n_with_label_where("User", predicate), planner_ctx);

    let sources = node_candidate_sources(node_access(&plan));
    assert_eq!(sources.len(), 1);
    assert_node_range(
        &sources,
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
            ),
        },
    );
    assert_no_decision(&plan, TraceDecision::NodeUnion);
    assert_no_decision(&plan, TraceDecision::NodeScanOr);
}

#[test]
fn node_or_disabled_branch_limit_falls_back_to_label_scan() {
    let predicate = Predicate::or(vec![
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::eq("username", "alice"),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::eq("email", "alice@example.com"),
        ]),
    ]);
    let mut planner_ctx = ctx(builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
        .with_node_eq(ScopedPropertyKey::try_new("User", "email").unwrap()));
    planner_ctx.limits = PlannerLimits {
        max_index_union_branches: IndexUnionBranchLimit::Disabled,
    };
    let plan = plan_traversal(g().n_where(predicate), planner_ctx);

    assert_node_label_scan(node_access(&plan), "User");
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::NodeScanOr);
}

#[test]
fn node_default_or_branch_limit_accepts_boundary_and_rejects_overflow() {
    let max_branches = default_index_union_branch_limit();
    let (boundary_indexes, boundary_predicate) =
        node_or_indexes_and_predicate(max_branches, "User");
    let boundary_plan = plan_traversal(
        g().n_where(boundary_predicate),
        PlannerContext {
            indexes: boundary_indexes,
            ..PlannerContext::default()
        },
    );
    let NodeAccessPlan::Union(branches) = node_access(&boundary_plan) else {
        panic!(
            "expected boundary node union: {:?}",
            node_access(&boundary_plan)
        );
    };

    assert_eq!(branches.len(), max_branches);
    assert_decision(
        &boundary_plan,
        TracePass::AccessPath,
        TraceDecision::NodeUnion,
    );

    let (overflow_indexes, overflow_predicate) =
        node_or_indexes_and_predicate(max_branches + 1, "User");
    let overflow_plan = plan_traversal(
        g().n_where(overflow_predicate),
        PlannerContext {
            indexes: overflow_indexes,
            ..PlannerContext::default()
        },
    );

    assert_node_label_scan(node_access(&overflow_plan), "User");
    assert_decision(
        &overflow_plan,
        TracePass::AccessPath,
        TraceDecision::NodeScanOr,
    );
    assert_no_decision(&overflow_plan, TraceDecision::NodeUnion);
}

#[test]
fn node_literal_in_values_respect_union_branch_limit_after_dedup() {
    let mut boundary_ctx =
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "status").unwrap()));
    boundary_ctx.limits = PlannerLimits {
        max_index_union_branches: IndexUnionBranchLimit::limited(2).unwrap(),
    };
    let boundary_plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::is_in(
                "status",
                PropertyValue::array(["active", "active", "pending"]),
            ),
        ),
        boundary_ctx,
    );
    let boundary_sources = node_candidate_sources(node_access(&boundary_plan));
    let [source] = boundary_sources.as_slice() else {
        panic!("expected one node union source: {boundary_sources:?}");
    };
    let NodeAccessPlan::Union(branches) = source else {
        panic!("expected node union source: {source:?}");
    };

    assert_eq!(branches.len(), 2);
    assert_decision(
        &boundary_plan,
        TracePass::AccessPath,
        TraceDecision::NodeUnion,
    );

    let mut overflow_ctx =
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "status").unwrap()));
    overflow_ctx.limits = PlannerLimits {
        max_index_union_branches: IndexUnionBranchLimit::limited(1).unwrap(),
    };
    let overflow_plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::is_in("status", PropertyValue::array(["active", "pending"])),
        ),
        overflow_ctx,
    );

    assert_node_label_scan(node_access(&overflow_plan), "User");
    assert_no_decision(&overflow_plan, TraceDecision::NodeUnion);
}

#[test]
fn node_literal_in_range_prunes_values_before_union_branch_limit() {
    let mut planner_ctx = ctx(builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("User", "age").unwrap())
        .with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        ));
    planner_ctx.limits = PlannerLimits {
        max_index_union_branches: IndexUnionBranchLimit::limited(2).unwrap(),
    };
    let literal_in = Predicate::is_in("age", PropertyValue::I64Array(vec![10, 20, 30, 40]));
    let range = Predicate::gte("age", 30);

    for predicate in [
        Predicate::and(vec![literal_in.clone(), range.clone()]),
        Predicate::and(vec![range, literal_in]),
    ] {
        let plan = plan_traversal(
            g().n_with_label_where("User", predicate),
            planner_ctx.clone(),
        );
        let sources = node_candidate_sources(node_access(&plan));
        let [source] = sources.as_slice() else {
            panic!("expected one pruned node union source: {sources:?}");
        };
        let NodeAccessPlan::Union(branches) = source else {
            panic!("expected node union source: {source:?}");
        };

        assert_eq!(branches.len(), 2);
        assert_node_eq(
            branches,
            "User",
            "age",
            IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(30)).unwrap()),
        );
        assert_node_eq(
            branches,
            "User",
            "age",
            IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(40)).unwrap()),
        );
        assert_no_node_label_scan_source(&sources);
        assert_decision(&plan, TracePass::AccessPath, TraceDecision::NodeUnion);
        assert_no_decision(&plan, TraceDecision::NodeScanOr);
    }
}

#[test]
fn split_node_literal_in_filter_uses_existing_range_before_branch_limit() {
    let mut planner_ctx = ctx(builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("User", "age").unwrap())
        .with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        ));
    planner_ctx.limits = PlannerLimits {
        max_index_union_branches: IndexUnionBranchLimit::limited(2).unwrap(),
    };

    let plan = plan_traversal(
        g().n_with_label_where("User", Predicate::gte("age", 30))
            .where_(Predicate::is_in(
                "age",
                PropertyValue::I64Array(vec![10, 20, 30, 40]),
            )),
        planner_ctx,
    );
    let sources = node_candidate_sources(node_access(&plan));
    let [source] = sources.as_slice() else {
        panic!("expected one split-filter pruned node union source: {sources:?}");
    };
    let NodeAccessPlan::Union(branches) = source else {
        panic!("expected node union source: {source:?}");
    };

    assert_eq!(branches.len(), 2);
    assert_node_eq(
        branches,
        "User",
        "age",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(30)).unwrap()),
    );
    assert_node_eq(
        branches,
        "User",
        "age",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(40)).unwrap()),
    );
    assert_no_node_label_scan_source(&sources);
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::NodeUnion);
}

#[test]
fn split_node_literal_in_filters_intersect_before_runtime_scan() {
    let planner_ctx =
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "age").unwrap()));

    let overlap = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::is_in("age", PropertyValue::I64Array(vec![10, 20])),
        )
        .where_(Predicate::is_in(
            "age",
            PropertyValue::I64Array(vec![20, 30]),
        )),
        planner_ctx.clone(),
    );
    let overlap_sources = node_candidate_sources(node_access(&overlap));

    assert_eq!(overlap_sources.len(), 1);
    assert_node_eq(
        &overlap_sources,
        "User",
        "age",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(20)).unwrap()),
    );
    assert_no_decision(&overlap, TraceDecision::NodeIntersect);

    let disjoint = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::is_in("age", PropertyValue::I64Array(vec![10, 20])),
        )
        .where_(Predicate::is_in(
            "age",
            PropertyValue::I64Array(vec![30, 40]),
        )),
        planner_ctx.clone(),
    );

    assert!(matches!(node_access(&disjoint), NodeAccessPlan::Empty));
    assert_no_decision(&disjoint, TraceDecision::NodeIntersect);

    let equality_overlap = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::is_in("age", PropertyValue::I64Array(vec![10, 20])),
        )
        .has("age", 20),
        planner_ctx.clone(),
    );
    let equality_overlap_sources = node_candidate_sources(node_access(&equality_overlap));

    assert_eq!(equality_overlap_sources.len(), 1);
    assert_node_eq(
        &equality_overlap_sources,
        "User",
        "age",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(20)).unwrap()),
    );
    assert_no_decision(&equality_overlap, TraceDecision::NodeIntersect);

    let equality_disjoint = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::is_in("age", PropertyValue::I64Array(vec![10, 20])),
        )
        .has("age", 30),
        planner_ctx.clone(),
    );

    assert!(matches!(
        node_access(&equality_disjoint),
        NodeAccessPlan::Empty
    ));
    assert_no_decision(&equality_disjoint, TraceDecision::NodeIntersect);

    let conflicting_equalities = plan_traversal(
        g().n_with_label_where("User", Predicate::eq("age", 10))
            .has("age", 20),
        planner_ctx,
    );

    assert!(matches!(
        node_access(&conflicting_equalities),
        NodeAccessPlan::Empty
    ));
    assert_no_decision(&conflicting_equalities, TraceDecision::NodeIntersect);
}

#[test]
fn node_or_branches_pruned_empty_by_literal_ranges_collapse_without_scan() {
    let plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::and(vec![
                    Predicate::is_in("age", PropertyValue::I64Array(vec![10])),
                    Predicate::gte("age", 21),
                ]),
                Predicate::and(vec![
                    Predicate::is_in("age", PropertyValue::I64Array(vec![20])),
                    Predicate::gte("age", 21),
                ]),
            ]),
        ),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "age").unwrap())),
    );

    assert!(matches!(node_access(&plan), NodeAccessPlan::Empty));
    assert_no_decision(&plan, TraceDecision::NodeScanOr);
}

#[test]
fn node_literal_in_range_intersection_drops_excluded_values_to_single_equality() {
    let literal_in = Predicate::is_in("age", PropertyValue::I64Array(vec![10, 30]));
    let range = Predicate::gte("age", 21);
    for predicate in [
        Predicate::and(vec![literal_in.clone(), range.clone()]),
        Predicate::and(vec![range, literal_in]),
    ] {
        let plan = plan_traversal(
            g().n_with_label_where("User", predicate),
            ctx(builtin_label_indexes()
                .with_node_eq(ScopedPropertyKey::try_new("User", "age").unwrap())
                .with_node_range(
                    ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                        .unwrap(),
                )),
        );
        let sources = node_candidate_sources(node_access(&plan));

        assert_eq!(sources.len(), 1);
        assert_node_eq(
            &sources,
            "User",
            "age",
            IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(30)).unwrap()),
        );
        assert_no_node_label_scan_source(&sources);
        assert_no_decision(&plan, TraceDecision::NodeIntersect);
    }
}

#[test]
fn node_literal_in_range_intersection_drops_excluded_values_to_smaller_union() {
    let plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::is_in("age", PropertyValue::I64Array(vec![10, 30, 40])),
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
    let sources = node_candidate_sources(node_access(&plan));
    let [source] = sources.as_slice() else {
        panic!("expected one node union source: {sources:?}");
    };
    let NodeAccessPlan::Union(branches) = source else {
        panic!("expected node union source: {source:?}");
    };

    assert_eq!(branches.len(), 2);
    assert_node_eq(
        branches,
        "User",
        "age",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(30)).unwrap()),
    );
    assert_node_eq(
        branches,
        "User",
        "age",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(40)).unwrap()),
    );
    assert_no_node_label_scan_source(&sources);
}

#[test]
fn node_literal_in_range_intersection_can_collapse_distributed_or_to_empty() {
    let plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::or(vec![
                    Predicate::is_in("age", PropertyValue::I64Array(vec![10])),
                    Predicate::is_in("age", PropertyValue::I64Array(vec![20])),
                ]),
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

    assert!(matches!(node_access(&plan), NodeAccessPlan::Empty));
}

#[test]
fn node_literal_in_dynamic_range_intersection_keeps_union_and_range_sources() {
    let plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::is_in("age", PropertyValue::I64Array(vec![10, 30])),
                Predicate::gte_param("age", "min_age"),
            ]),
        ),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "age").unwrap())
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let sources = node_candidate_sources(node_access(&plan));

    assert_eq!(sources.len(), 2);
    let union = sources
        .iter()
        .find_map(|source| match source {
            NodeAccessPlan::Union(branches) => Some(branches),
            _ => None,
        })
        .expect("expected node IN union to survive dynamic range");
    assert_eq!(union.len(), 2);
    assert_node_range(
        &sources,
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(RangeIndexValue::param("min_age").unwrap()),
        },
    );
}

#[test]
fn node_literal_in_range_intersection_drops_impossible_mixed_or_branch_sources() {
    let indexes = builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("User", "age").unwrap())
        .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
        .with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )
        .with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "score", RangeIndexDirection::Asc).unwrap(),
        );
    let age_range = IndexRange::Lower {
        lower: IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(21)).unwrap()),
    };

    let username_plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::or(vec![
                    Predicate::is_in("age", PropertyValue::I64Array(vec![10])),
                    Predicate::eq("username", "alice"),
                ]),
                Predicate::gte("age", 21),
            ]),
        ),
        ctx(indexes.clone()),
    );
    let username_sources = node_candidate_sources(node_access(&username_plan));
    assert_eq!(username_sources.len(), 2);
    assert!(username_sources
        .iter()
        .all(|source| !matches!(source, NodeAccessPlan::Union(_))));
    assert_node_eq(
        &username_sources,
        "User",
        "username",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("alice")).unwrap()),
    );
    assert_node_range(
        &username_sources,
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        age_range.clone(),
    );

    let score_plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::or(vec![
                    Predicate::is_in("age", PropertyValue::I64Array(vec![10])),
                    Predicate::gt("score", 900),
                ]),
                Predicate::gte("age", 21),
            ]),
        ),
        ctx(indexes),
    );
    let score_sources = node_candidate_sources(node_access(&score_plan));
    assert_eq!(score_sources.len(), 2);
    assert!(score_sources
        .iter()
        .all(|source| !matches!(source, NodeAccessPlan::Union(_))));
    assert_node_range(
        &score_sources,
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        age_range,
    );
    assert_node_range(
        &score_sources,
        ScopedPropertyDirectionKey::try_new("User", "score", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Exclusive(
                RangeIndexValue::literal(PropertyValue::from(900)).unwrap(),
            ),
        },
    );
}

#[test]
fn empty_node_access_is_subsumed_by_every_intersection_source() {
    assert!(crate::planning::node_access_subsumes(
        &NodeAccessPlan::LabelScan {
            label: NonEmptyString::new("User").unwrap(),
        },
        &NodeAccessPlan::Empty,
    ));
}

#[test]
fn access_subsumption_contract_covers_scan_label_and_filtered_sources() {
    let node_user_label = NonEmptyString::new("User").unwrap();
    let node_account_label = NonEmptyString::new("Account").unwrap();
    let node_eq = NodeAccessPlan::EqualityIndex {
        index: NodeEqualityIndexMeta::new("node_username"),
        key: ScopedPropertyKey::new(
            node_user_label.clone(),
            NonEmptyString::new("username").unwrap(),
        ),
        value: IndexValue::Literal(
            SecondaryIndexLiteral::new(PropertyValue::from("alice")).unwrap(),
        ),
    };
    let node_account_eq = NodeAccessPlan::EqualityIndex {
        index: NodeEqualityIndexMeta::new("node_account_username"),
        key: ScopedPropertyKey::new(node_account_label, NonEmptyString::new("username").unwrap()),
        value: IndexValue::Literal(
            SecondaryIndexLiteral::new(PropertyValue::from("alice")).unwrap(),
        ),
    };
    let node_label_scan = NodeAccessPlan::LabelScan {
        label: node_user_label.clone(),
    };
    let node_range = NodeAccessPlan::RangeIndex {
        index: NodeRangeIndexMeta::new("node_age"),
        key: ScopedPropertyDirectionKey::new(
            node_user_label,
            NonEmptyString::new("age").unwrap(),
            RangeIndexDirection::Asc,
        ),
        range: IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
            ),
        },
    };
    let node_filtered_range = NodeAccessPlan::ScanThenFilter {
        source: NodeAccessSourcePlan::from_unfiltered(node_range.clone()),
        residual: PredicatePlan::new(Predicate::eq("status", "active")).unwrap(),
    };

    assert!(crate::planning::node_access_subsumes(
        &NodeAccessPlan::AllScan,
        &node_eq,
    ));
    assert!(crate::planning::node_access_subsumes(
        &node_label_scan,
        &node_eq,
    ));
    assert!(!crate::planning::node_access_subsumes(
        &node_label_scan,
        &node_account_eq,
    ));
    assert!(crate::planning::node_access_subsumes(
        &node_range,
        &node_filtered_range,
    ));

    let edge_follows_label = NonEmptyString::new("FOLLOWS").unwrap();
    let edge_likes_label = NonEmptyString::new("LIKES").unwrap();
    let edge_eq = EdgeAccessPlan::EqualityIndex {
        index: EdgeEqualityIndexMeta::new("edge_status"),
        key: ScopedPropertyKey::new(
            edge_follows_label.clone(),
            NonEmptyString::new("status").unwrap(),
        ),
        value: IndexValue::Literal(
            SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap(),
        ),
    };
    let edge_likes_eq = EdgeAccessPlan::EqualityIndex {
        index: EdgeEqualityIndexMeta::new("edge_likes_status"),
        key: ScopedPropertyKey::new(edge_likes_label, NonEmptyString::new("status").unwrap()),
        value: IndexValue::Literal(
            SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap(),
        ),
    };
    let edge_label_scan = EdgeAccessPlan::LabelScan {
        label: edge_follows_label.clone(),
    };
    let edge_range = EdgeAccessPlan::RangeIndex {
        index: EdgeRangeIndexMeta::new("edge_weight"),
        key: ScopedPropertyDirectionKey::new(
            edge_follows_label,
            NonEmptyString::new("weight").unwrap(),
            RangeIndexDirection::Asc,
        ),
        range: IndexRange::Upper {
            upper: IndexBound::Exclusive(
                RangeIndexValue::literal(PropertyValue::from(50)).unwrap(),
            ),
        },
    };
    let edge_filtered_range = EdgeAccessPlan::ScanThenFilter {
        source: EdgeAccessSourcePlan::from_unfiltered(edge_range.clone()),
        residual: PredicatePlan::new(Predicate::eq("tenant_id", "acme")).unwrap(),
    };

    assert!(crate::planning::edge_access_subsumes(
        &EdgeAccessPlan::AllScan,
        &edge_eq,
    ));
    assert!(crate::planning::edge_access_subsumes(
        &edge_label_scan,
        &edge_eq,
    ));
    assert!(!crate::planning::edge_access_subsumes(
        &edge_label_scan,
        &edge_likes_eq,
    ));
    assert!(crate::planning::edge_access_subsumes(
        &edge_range,
        &edge_filtered_range,
    ));
}

#[test]
fn literal_range_pruning_contract_keeps_inexact_values_and_reports_empty() {
    let ranges = [crate::planning::LiteralRangeConstraint {
        property: "score".to_string(),
        range: IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(10)).unwrap(),
            ),
        },
    }];

    assert_eq!(
        crate::planning::prune_literal_in_values_by_ranges(
            "score",
            vec![PropertyValue::F64(1.5), PropertyValue::from(3)],
            &ranges,
        ),
        Some(vec![PropertyValue::F64(1.5)])
    );
    assert_eq!(
        crate::planning::prune_literal_in_values_by_ranges(
            "score",
            vec![PropertyValue::from(3)],
            &ranges,
        ),
        Some(Vec::new())
    );
    assert_eq!(
        crate::planning::prune_literal_in_values_by_ranges(
            "other",
            vec![PropertyValue::from(3)],
            &ranges,
        ),
        None
    );
}

#[test]
fn source_candidate_contract_rebuilds_singletons_and_unions() {
    let user = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::LabelScan {
        label: NonEmptyString::new("User").unwrap(),
    });
    let account = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::LabelScan {
        label: NonEmptyString::new("Account").unwrap(),
    });
    let follows = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::LabelScan {
        label: NonEmptyString::new("FOLLOWS").unwrap(),
    });
    let mentions = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::LabelScan {
        label: NonEmptyString::new("MENTIONS").unwrap(),
    });

    assert!(matches!(
        crate::planning::node_source_from_candidates(vec![user.clone()]).as_ref(),
        NodeAccessPlan::LabelScan { label } if label.as_ref() == "User"
    ));
    assert!(matches!(
        crate::planning::node_source_from_candidates(vec![user, account]).as_ref(),
        NodeAccessPlan::Union(branches) if branches.len() == 2
    ));
    assert!(matches!(
        crate::planning::edge_source_from_candidates(vec![follows.clone()]).as_ref(),
        EdgeAccessPlan::LabelScan { label } if label.as_ref() == "FOLLOWS"
    ));
    assert!(matches!(
        crate::planning::edge_source_from_candidates(vec![follows, mentions]).as_ref(),
        EdgeAccessPlan::Union(branches) if branches.len() == 2
    ));
}

#[test]
fn common_node_intersection_factoring_keeps_empty_remainder_conservative() {
    let user = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::LabelScan {
        label: NonEmptyString::new("User").unwrap(),
    });
    let account = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::LabelScan {
        label: NonEmptyString::new("Account").unwrap(),
    });
    let intersection = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::Intersect(
        AtLeast::from_pair(user.clone(), account),
    ));
    let union = AtLeast::from_pair(user, intersection);

    assert!(crate::planning::factor_node_common_intersection_union(
        &PlannerContext::default(),
        &union,
    )
    .is_none());
}

#[test]
fn common_edge_intersection_factoring_keeps_empty_remainder_conservative() {
    let follows = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::LabelScan {
        label: NonEmptyString::new("FOLLOWS").unwrap(),
    });
    let mentions = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::LabelScan {
        label: NonEmptyString::new("MENTIONS").unwrap(),
    });
    let intersection = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::Intersect(
        AtLeast::from_pair(follows.clone(), mentions),
    ));
    let union = AtLeast::from_pair(follows, intersection);

    assert!(crate::planning::factor_edge_common_intersection_union(
        &PlannerContext::default(),
        &union,
    )
    .is_none());
}

#[test]
fn equality_union_range_contract_rejects_mixed_sources_and_collapses_empty() {
    let node_range_key =
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap();
    let node_range = IndexRange::Lower {
        lower: IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(21)).unwrap()),
    };
    let node_age_key = ScopedPropertyKey::try_new("User", "age").unwrap();
    let node_username_key = ScopedPropertyKey::try_new("User", "username").unwrap();
    let node_index = NodeEqualityIndexMeta::new("node_eq");
    let node_age_10 = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::EqualityIndex {
        index: node_index.clone(),
        key: node_age_key.clone(),
        value: IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(10)).unwrap()),
    });
    let node_age_20 = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::EqualityIndex {
        index: node_index.clone(),
        key: node_age_key,
        value: IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(20)).unwrap()),
    });
    let node_username = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::EqualityIndex {
        index: node_index,
        key: node_username_key,
        value: IndexValue::Literal(
            SecondaryIndexLiteral::new(PropertyValue::from("alice")).unwrap(),
        ),
    });
    let node_label = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::LabelScan {
        label: NonEmptyString::new("User").unwrap(),
    });

    let empty_node_source = crate::planning::restrict_node_equality_union_by_range(
        &AtLeast::from_pair(node_age_10.clone(), node_age_20),
        &node_range_key,
        &node_range,
    )
    .unwrap();
    assert!(matches!(empty_node_source.as_ref(), NodeAccessPlan::Empty));
    assert!(crate::planning::restrict_node_equality_union_by_range(
        &AtLeast::from_pair(node_age_10.clone(), node_username),
        &node_range_key,
        &node_range,
    )
    .is_none());
    assert!(crate::planning::restrict_node_equality_union_by_range(
        &AtLeast::from_pair(node_age_10, node_label),
        &node_range_key,
        &node_range,
    )
    .is_none());

    let edge_range_key =
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap();
    let edge_range = IndexRange::Lower {
        lower: IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(21)).unwrap()),
    };
    let edge_weight_key = ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap();
    let edge_status_key = ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap();
    let edge_index = EdgeEqualityIndexMeta::new("edge_eq");
    let edge_weight_10 = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::EqualityIndex {
        index: edge_index.clone(),
        key: edge_weight_key.clone(),
        value: IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(10)).unwrap()),
    });
    let edge_weight_20 = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::EqualityIndex {
        index: edge_index.clone(),
        key: edge_weight_key,
        value: IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(20)).unwrap()),
    });
    let edge_status = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::EqualityIndex {
        index: edge_index,
        key: edge_status_key,
        value: IndexValue::Literal(
            SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap(),
        ),
    });
    let edge_label = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::LabelScan {
        label: NonEmptyString::new("FOLLOWS").unwrap(),
    });

    let empty_edge_source = crate::planning::restrict_edge_equality_union_by_range(
        &AtLeast::from_pair(edge_weight_10.clone(), edge_weight_20),
        &edge_range_key,
        &edge_range,
    )
    .unwrap();
    assert!(matches!(empty_edge_source.as_ref(), EdgeAccessPlan::Empty));
    assert!(crate::planning::restrict_edge_equality_union_by_range(
        &AtLeast::from_pair(edge_weight_10.clone(), edge_status),
        &edge_range_key,
        &edge_range,
    )
    .is_none());
    assert!(crate::planning::restrict_edge_equality_union_by_range(
        &AtLeast::from_pair(edge_weight_10, edge_label),
        &edge_range_key,
        &edge_range,
    )
    .is_none());
}

#[test]
fn equality_union_intersection_contract_keeps_mixed_sources_conservative() {
    let node_index = NodeEqualityIndexMeta::new("node_eq");
    let node_age_key = ScopedPropertyKey::try_new("User", "age").unwrap();
    let node_username_key = ScopedPropertyKey::try_new("User", "username").unwrap();
    let node_age_10 = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::EqualityIndex {
        index: node_index.clone(),
        key: node_age_key.clone(),
        value: IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(10)).unwrap()),
    });
    let node_age_20 = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::EqualityIndex {
        index: node_index.clone(),
        key: node_age_key.clone(),
        value: IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(20)).unwrap()),
    });
    let node_age_30 = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::EqualityIndex {
        index: node_index.clone(),
        key: node_age_key,
        value: IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(30)).unwrap()),
    });
    let node_username_alice =
        NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::EqualityIndex {
            index: node_index.clone(),
            key: node_username_key.clone(),
            value: IndexValue::Literal(
                SecondaryIndexLiteral::new(PropertyValue::from("alice")).unwrap(),
            ),
        });
    let node_username_bob = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::EqualityIndex {
        index: node_index,
        key: node_username_key,
        value: IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("bob")).unwrap()),
    });
    let node_label = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::LabelScan {
        label: NonEmptyString::new("User").unwrap(),
    });
    let node_mixed_source_union = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::Union(
        AtLeast::from_pair(node_label.clone(), node_age_10.clone()),
    ));
    let node_mixed_key_union = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::Union(
        AtLeast::from_pair(node_age_10.clone(), node_username_alice.clone()),
    ));
    let node_username_union = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::Union(
        AtLeast::from_pair(node_username_alice, node_username_bob),
    ));

    assert!(crate::planning::merge_node_equality_intersection(
        &node_mixed_source_union,
        &node_age_30,
    )
    .is_none());
    assert!(crate::planning::merge_node_equality_intersection(
        &node_mixed_key_union,
        &NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::Union(AtLeast::from_pair(
            node_age_20.clone(),
            node_age_30.clone(),
        ))),
    )
    .is_none());
    assert!(crate::planning::merge_node_equality_intersection(
        &NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::Union(AtLeast::from_pair(
            node_age_10,
            node_age_20,
        ))),
        &node_username_union,
    )
    .is_none());
    assert!(crate::planning::intersect_node_equality_unions(
        &AtLeast::from_pair(node_age_30.clone(), node_label),
        &AtLeast::from_pair(node_age_30.clone(), node_age_30),
    )
    .is_none());

    let edge_index = EdgeEqualityIndexMeta::new("edge_eq");
    let edge_weight_key = ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap();
    let edge_status_key = ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap();
    let edge_weight_10 = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::EqualityIndex {
        index: edge_index.clone(),
        key: edge_weight_key.clone(),
        value: IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(10)).unwrap()),
    });
    let edge_weight_20 = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::EqualityIndex {
        index: edge_index.clone(),
        key: edge_weight_key.clone(),
        value: IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(20)).unwrap()),
    });
    let edge_weight_30 = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::EqualityIndex {
        index: edge_index.clone(),
        key: edge_weight_key,
        value: IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(30)).unwrap()),
    });
    let edge_status_active = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::EqualityIndex {
        index: edge_index.clone(),
        key: edge_status_key.clone(),
        value: IndexValue::Literal(
            SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap(),
        ),
    });
    let edge_status_pending =
        EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::EqualityIndex {
            index: edge_index,
            key: edge_status_key,
            value: IndexValue::Literal(
                SecondaryIndexLiteral::new(PropertyValue::from("pending")).unwrap(),
            ),
        });
    let edge_mixed_key_union = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::Union(
        AtLeast::from_pair(edge_weight_10.clone(), edge_status_active.clone()),
    ));
    let edge_status_union = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::Union(
        AtLeast::from_pair(edge_status_active, edge_status_pending),
    ));
    let edge_label = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::LabelScan {
        label: NonEmptyString::new("FOLLOWS").unwrap(),
    });

    assert!(crate::planning::merge_edge_equality_intersection(
        &edge_mixed_key_union,
        &EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::Union(AtLeast::from_pair(
            edge_weight_20.clone(),
            edge_weight_30.clone(),
        ))),
    )
    .is_none());
    assert!(crate::planning::merge_edge_equality_intersection(
        &EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::Union(AtLeast::from_pair(
            edge_weight_10,
            edge_weight_20,
        ))),
        &edge_status_union,
    )
    .is_none());
    assert!(crate::planning::intersect_edge_equality_unions(
        &AtLeast::from_pair(edge_weight_30.clone(), edge_label),
        &AtLeast::from_pair(edge_weight_30.clone(), edge_weight_30),
    )
    .is_none());
}

#[test]
fn range_union_intersection_contract_keeps_unknown_and_mismatched_sources_conservative() {
    let node_index = NodeRangeIndexMeta::new("node_range");
    let node_age_key =
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap();
    let node_score_key =
        ScopedPropertyDirectionKey::try_new("User", "score", RangeIndexDirection::Asc).unwrap();
    let node_age_range = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::RangeIndex {
        index: node_index.clone(),
        key: node_age_key.clone(),
        range: IndexRange::between(
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(0)).unwrap()),
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(10)).unwrap()),
        ),
    });
    let node_score_range = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::RangeIndex {
        index: node_index.clone(),
        key: node_score_key,
        range: IndexRange::between(
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(0)).unwrap()),
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(10)).unwrap()),
        ),
    });
    let node_dynamic_age_range =
        NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::RangeIndex {
            index: node_index.clone(),
            key: node_age_key.clone(),
            range: IndexRange::Lower {
                lower: IndexBound::Inclusive(RangeIndexValue::param("min_age").unwrap()),
            },
        });
    let node_age_lower = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::RangeIndex {
        index: node_index,
        key: node_age_key.clone(),
        range: IndexRange::Lower {
            lower: IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(5)).unwrap()),
        },
    });
    let node_label = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::LabelScan {
        label: NonEmptyString::new("User").unwrap(),
    });
    let node_mismatched_key_union = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::Union(
        AtLeast::from_pair(node_age_range.clone(), node_score_range),
    ));

    assert!(crate::planning::merge_node_range_union_intersection(
        &node_mismatched_key_union,
        &node_age_lower,
    )
    .is_none());
    assert!(crate::planning::merge_node_range_union_intersection(
        &node_age_lower,
        &node_mismatched_key_union,
    )
    .is_none());
    assert!(crate::planning::merge_node_range_union_intersection(
        &NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::Union(AtLeast::from_pair(
            node_age_range.clone(),
            node_label.clone(),
        ))),
        &node_age_lower,
    )
    .is_none());
    assert!(crate::planning::merge_node_range_union_intersection(
        &NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::Union(AtLeast::from_pair(
            node_dynamic_age_range.clone(),
            node_age_range.clone(),
        ))),
        &node_age_lower,
    )
    .is_none());
    assert!(!crate::planning::ranges_are_proven_disjoint(
        &IndexRange::Upper {
            upper: IndexBound::Inclusive(RangeIndexValue::param("max_age").unwrap()),
        },
        &IndexRange::Lower {
            lower: IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(5)).unwrap(),),
        },
    ));
    let node_range_union = AtLeast::from_pair(node_age_range.clone(), node_age_lower.clone());
    let node_mixed_source_union = AtLeast::from_pair(node_label, node_age_range.clone());
    assert!(crate::planning::intersect_node_range_unions(
        &node_mixed_source_union,
        &node_range_union,
    )
    .is_none());
    assert!(crate::planning::intersect_node_range_unions(
        &node_range_union,
        &node_mixed_source_union,
    )
    .is_none());
    assert!(crate::planning::intersect_node_range_unions(
        &AtLeast::from_pair(node_dynamic_age_range.clone(), node_age_range.clone()),
        &AtLeast::from_pair(node_age_lower.clone(), node_age_range),
    )
    .is_none());

    let edge_index = EdgeRangeIndexMeta::new("edge_range");
    let edge_weight_key =
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap();
    let edge_rank_key =
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "rank", RangeIndexDirection::Asc).unwrap();
    let edge_weight_range = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::RangeIndex {
        index: edge_index.clone(),
        key: edge_weight_key.clone(),
        range: IndexRange::between(
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(0)).unwrap()),
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(10)).unwrap()),
        ),
    });
    let edge_rank_range = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::RangeIndex {
        index: edge_index.clone(),
        key: edge_rank_key,
        range: IndexRange::between(
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(0)).unwrap()),
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(10)).unwrap()),
        ),
    });
    let edge_dynamic_weight_range =
        EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::RangeIndex {
            index: edge_index.clone(),
            key: edge_weight_key.clone(),
            range: IndexRange::Lower {
                lower: IndexBound::Inclusive(RangeIndexValue::param("min_weight").unwrap()),
            },
        });
    let edge_weight_lower = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::RangeIndex {
        index: edge_index,
        key: edge_weight_key,
        range: IndexRange::Lower {
            lower: IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(5)).unwrap()),
        },
    });
    let edge_label = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::LabelScan {
        label: NonEmptyString::new("FOLLOWS").unwrap(),
    });
    let edge_mismatched_key_union = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::Union(
        AtLeast::from_pair(edge_weight_range.clone(), edge_rank_range),
    ));

    assert!(crate::planning::merge_edge_range_union_intersection(
        &edge_mismatched_key_union,
        &edge_weight_lower,
    )
    .is_none());
    assert!(crate::planning::merge_edge_range_union_intersection(
        &edge_weight_lower,
        &edge_mismatched_key_union,
    )
    .is_none());
    assert!(crate::planning::merge_edge_range_union_intersection(
        &EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::Union(AtLeast::from_pair(
            edge_weight_range.clone(),
            edge_label.clone(),
        ))),
        &edge_weight_lower,
    )
    .is_none());
    assert!(crate::planning::merge_edge_range_union_intersection(
        &EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::Union(AtLeast::from_pair(
            edge_dynamic_weight_range.clone(),
            edge_weight_range.clone(),
        ))),
        &edge_weight_lower,
    )
    .is_none());
    let edge_range_union = AtLeast::from_pair(edge_weight_range.clone(), edge_weight_lower.clone());
    let edge_mixed_source_union = AtLeast::from_pair(edge_label, edge_weight_range.clone());
    assert!(crate::planning::intersect_edge_range_unions(
        &edge_mixed_source_union,
        &edge_range_union,
    )
    .is_none());
    assert!(crate::planning::intersect_edge_range_unions(
        &edge_range_union,
        &edge_mixed_source_union,
    )
    .is_none());
    assert!(crate::planning::intersect_edge_range_unions(
        &AtLeast::from_pair(edge_dynamic_weight_range, edge_weight_range.clone()),
        &AtLeast::from_pair(edge_weight_lower, edge_weight_range),
    )
    .is_none());
}

#[test]
fn equality_range_union_intersection_contract_keeps_unknown_and_mixed_ranges_conservative() {
    let node_eq_index = NodeEqualityIndexMeta::new("node_eq");
    let node_age_key = ScopedPropertyKey::try_new("User", "age").unwrap();
    let node_age_10 = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::EqualityIndex {
        index: node_eq_index.clone(),
        key: node_age_key.clone(),
        value: IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(10)).unwrap()),
    });
    let node_age_20 = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::EqualityIndex {
        index: node_eq_index,
        key: node_age_key,
        value: IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(20)).unwrap()),
    });
    let node_equality_union = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::Union(
        AtLeast::from_pair(node_age_10, node_age_20),
    ));
    let node_range_index = NodeRangeIndexMeta::new("node_range");
    let node_age_range_key =
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap();
    let node_score_range_key =
        ScopedPropertyDirectionKey::try_new("User", "score", RangeIndexDirection::Asc).unwrap();
    let node_age_range = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::RangeIndex {
        index: node_range_index.clone(),
        key: node_age_range_key.clone(),
        range: IndexRange::between(
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(0)).unwrap()),
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(15)).unwrap()),
        ),
    });
    let node_age_later_range = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::RangeIndex {
        index: node_range_index.clone(),
        key: node_age_range_key,
        range: IndexRange::between(
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(30)).unwrap()),
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(40)).unwrap()),
        ),
    });
    let node_dynamic_range = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::RangeIndex {
        index: node_range_index.clone(),
        key: ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        range: IndexRange::Lower {
            lower: IndexBound::Inclusive(RangeIndexValue::param("min_age").unwrap()),
        },
    });
    let node_score_range = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::RangeIndex {
        index: node_range_index.clone(),
        key: node_score_range_key.clone(),
        range: IndexRange::between(
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(0)).unwrap()),
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(15)).unwrap()),
        ),
    });
    let node_score_later_range =
        NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::RangeIndex {
            index: node_range_index,
            key: node_score_range_key,
            range: IndexRange::between(
                IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(30)).unwrap()),
                IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(40)).unwrap()),
            ),
        });
    let node_label = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::LabelScan {
        label: NonEmptyString::new("User").unwrap(),
    });

    for range_union in [
        NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::Union(AtLeast::from_pair(
            node_score_range.clone(),
            node_score_later_range,
        ))),
        NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::Union(AtLeast::from_pair(
            node_age_range.clone(),
            node_score_range,
        ))),
        NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::Union(AtLeast::from_pair(
            node_dynamic_range,
            node_age_later_range,
        ))),
        NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::Union(AtLeast::from_pair(
            node_age_range,
            node_label,
        ))),
    ] {
        assert!(
            crate::planning::merge_node_equality_range_unions_intersection(
                &node_equality_union,
                &range_union,
            )
            .is_none()
        );
    }

    let edge_eq_index = EdgeEqualityIndexMeta::new("edge_eq");
    let edge_weight_key = ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap();
    let edge_weight_10 = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::EqualityIndex {
        index: edge_eq_index.clone(),
        key: edge_weight_key.clone(),
        value: IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(10)).unwrap()),
    });
    let edge_weight_20 = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::EqualityIndex {
        index: edge_eq_index,
        key: edge_weight_key,
        value: IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(20)).unwrap()),
    });
    let edge_equality_union = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::Union(
        AtLeast::from_pair(edge_weight_10, edge_weight_20),
    ));
    let edge_range_index = EdgeRangeIndexMeta::new("edge_range");
    let edge_weight_range_key =
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap();
    let edge_rank_range_key =
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "rank", RangeIndexDirection::Asc).unwrap();
    let edge_weight_range = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::RangeIndex {
        index: edge_range_index.clone(),
        key: edge_weight_range_key.clone(),
        range: IndexRange::between(
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(0)).unwrap()),
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(15)).unwrap()),
        ),
    });
    let edge_weight_later_range =
        EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::RangeIndex {
            index: edge_range_index.clone(),
            key: edge_weight_range_key,
            range: IndexRange::between(
                IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(30)).unwrap()),
                IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(40)).unwrap()),
            ),
        });
    let edge_dynamic_range = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::RangeIndex {
        index: edge_range_index.clone(),
        key: ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
            .unwrap(),
        range: IndexRange::Lower {
            lower: IndexBound::Inclusive(RangeIndexValue::param("min_weight").unwrap()),
        },
    });
    let edge_rank_range = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::RangeIndex {
        index: edge_range_index.clone(),
        key: edge_rank_range_key.clone(),
        range: IndexRange::between(
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(0)).unwrap()),
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(15)).unwrap()),
        ),
    });
    let edge_rank_later_range = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::RangeIndex {
        index: edge_range_index,
        key: edge_rank_range_key,
        range: IndexRange::between(
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(30)).unwrap()),
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(40)).unwrap()),
        ),
    });
    let edge_label = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::LabelScan {
        label: NonEmptyString::new("FOLLOWS").unwrap(),
    });

    for range_union in [
        EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::Union(AtLeast::from_pair(
            edge_rank_range.clone(),
            edge_rank_later_range,
        ))),
        EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::Union(AtLeast::from_pair(
            edge_weight_range.clone(),
            edge_rank_range,
        ))),
        EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::Union(AtLeast::from_pair(
            edge_dynamic_range,
            edge_weight_later_range,
        ))),
        EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::Union(AtLeast::from_pair(
            edge_weight_range,
            edge_label,
        ))),
    ] {
        assert!(
            crate::planning::merge_edge_equality_range_unions_intersection(
                &edge_equality_union,
                &range_union,
            )
            .is_none()
        );
    }
}

#[test]
fn equality_range_intersection_contract_keeps_dynamic_ranges_conservative() {
    let node_key = ScopedPropertyKey::try_new("User", "age").unwrap();
    let node_range_key =
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap();
    let node_equality = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::EqualityIndex {
        index: NodeEqualityIndexMeta::new("node_eq"),
        key: node_key,
        value: IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(30)).unwrap()),
    });
    let node_dynamic_range = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::RangeIndex {
        index: NodeRangeIndexMeta::new("node_range"),
        key: node_range_key,
        range: IndexRange::Lower {
            lower: IndexBound::Inclusive(RangeIndexValue::param("min_age").unwrap()),
        },
    });

    assert!(crate::planning::merge_node_equality_range_intersection(
        &node_equality,
        &node_dynamic_range,
    )
    .is_none());

    let edge_key = ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap();
    let edge_range_key =
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap();
    let edge_equality = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::EqualityIndex {
        index: EdgeEqualityIndexMeta::new("edge_eq"),
        key: edge_key,
        value: IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(5)).unwrap()),
    });
    let edge_dynamic_range = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::RangeIndex {
        index: EdgeRangeIndexMeta::new("edge_range"),
        key: edge_range_key,
        range: IndexRange::Upper {
            upper: IndexBound::Exclusive(RangeIndexValue::param("max_weight").unwrap()),
        },
    });

    assert!(crate::planning::merge_edge_equality_range_intersection(
        &edge_equality,
        &edge_dynamic_range,
    )
    .is_none());
}

#[test]
fn literal_range_pruned_empty_sources_are_covered_contracts() {
    let node_label = NonEmptyString::new("User").unwrap();
    let node_ranges = [crate::planning::LiteralRangeConstraint {
        property: "age".to_string(),
        range: IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
            ),
        },
    }];
    let node_ctx =
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "age").unwrap()));
    let mut node_planner = crate::planning::Planner::new(&node_ctx);
    let node_indexed = node_planner
        .node_index_plan_with_conjunction_ranges(
            &Predicate::is_in("age", PropertyValue::I64Array(vec![10])),
            &node_ranges,
            &node_label,
            "test.node",
        )
        .unwrap();

    assert!(node_indexed.covered);
    assert!(matches!(
        node_indexed.source.unwrap().as_ref(),
        NodeAccessPlan::Empty
    ));

    let edge_label = NonEmptyString::new("FOLLOWS").unwrap();
    let edge_ranges = [crate::planning::LiteralRangeConstraint {
        property: "weight".to_string(),
        range: IndexRange::Upper {
            upper: IndexBound::Exclusive(
                RangeIndexValue::literal(PropertyValue::from(10)).unwrap(),
            ),
        },
    }];
    let edge_ctx = ctx(builtin_label_indexes()
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap()));
    let mut edge_planner = crate::planning::Planner::new(&edge_ctx);
    let edge_indexed = edge_planner
        .edge_index_plan_with_conjunction_ranges(
            &Predicate::is_in("weight", PropertyValue::I64Array(vec![30])),
            &edge_ranges,
            &edge_label,
            "test.edge",
        )
        .unwrap();

    assert!(edge_indexed.covered);
    assert!(matches!(
        edge_indexed.source.unwrap().as_ref(),
        EdgeAccessPlan::Empty
    ));
}

#[test]
fn pruned_edge_literal_in_falls_back_when_retained_values_have_no_index() {
    let label = NonEmptyString::new("FOLLOWS").unwrap();
    let ranges = [crate::planning::LiteralRangeConstraint {
        property: "weight".to_string(),
        range: IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(20)).unwrap(),
            ),
        },
    }];
    let planner_ctx = ctx(builtin_label_indexes());
    let mut planner = crate::planning::Planner::new(&planner_ctx);
    let indexed = planner
        .edge_index_plan_with_conjunction_ranges(
            &Predicate::is_in("weight", PropertyValue::I64Array(vec![10, 30])),
            &ranges,
            &label,
            "test.edge_missing_index",
        )
        .unwrap();

    assert!(!indexed.covered);
    assert!(indexed.source.is_none());
}

#[test]
fn union_plans_collapse_when_every_pruned_branch_is_empty() {
    let node_label = NonEmptyString::new("User").unwrap();
    let node_ctx =
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "age").unwrap()));
    let mut node_planner = crate::planning::Planner::new(&node_ctx);
    let node_source = node_planner
        .node_union_plan(
            &[
                Predicate::and(vec![
                    Predicate::is_in("age", PropertyValue::I64Array(vec![10])),
                    Predicate::gte("age", 21),
                ]),
                Predicate::and(vec![
                    Predicate::is_in("age", PropertyValue::I64Array(vec![20])),
                    Predicate::gte("age", 21),
                ]),
            ],
            &node_label,
            "test.node_or",
        )
        .unwrap();

    assert!(node_source.covered);
    let node_source = node_source.source.unwrap();
    assert!(matches!(node_source.as_ref(), NodeAccessPlan::Empty));

    let edge_label = NonEmptyString::new("FOLLOWS").unwrap();
    let edge_ctx = ctx(builtin_label_indexes()
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap()));
    let mut edge_planner = crate::planning::Planner::new(&edge_ctx);
    let edge_source = edge_planner
        .edge_union_plan(
            &[
                Predicate::and(vec![
                    Predicate::is_in("weight", PropertyValue::I64Array(vec![30])),
                    Predicate::lt("weight", 10),
                ]),
                Predicate::and(vec![
                    Predicate::is_in("weight", PropertyValue::I64Array(vec![40])),
                    Predicate::lt("weight", 10),
                ]),
            ],
            &edge_label,
            "test.edge_or",
        )
        .unwrap();

    assert!(edge_source.covered);
    let edge_source = edge_source.source.unwrap();
    assert!(matches!(edge_source.as_ref(), EdgeAccessPlan::Empty));
}

#[test]
fn partial_or_union_helpers_report_empty_and_single_sources() {
    let planner_ctx = PlannerContext::default();
    let mut planner = crate::planning::Planner::new(&planner_ctx);

    assert!(planner
        .node_partial_or_union_source(&[], "test.node_partial_or_union")
        .is_none());
    let node_single = planner
        .node_partial_or_union_source(
            &[NodeAccessSourcePlan::from_unfiltered(
                NodeAccessPlan::LabelScan {
                    label: NonEmptyString::new("User").unwrap(),
                },
            )],
            "test.node_partial_or_union",
        )
        .expect("single node partial OR source should be preserved");
    assert!(matches!(
        node_single.as_ref(),
        NodeAccessPlan::LabelScan { label } if label == "User"
    ));

    assert!(planner
        .edge_partial_or_union_source(&[], "test.edge_partial_or_union")
        .is_none());
    let edge_single = planner
        .edge_partial_or_union_source(
            &[EdgeAccessSourcePlan::from_unfiltered(
                EdgeAccessPlan::LabelScan {
                    label: NonEmptyString::new("FOLLOWS").unwrap(),
                },
            )],
            "test.edge_partial_or_union",
        )
        .expect("single edge partial OR source should be preserved");
    assert!(matches!(
        edge_single.as_ref(),
        EdgeAccessPlan::LabelScan { label } if label == "FOLLOWS"
    ));
}

#[test]
fn union_candidate_helpers_skip_empty_sources() {
    let mut node_sources = vec![NodeAccessSourcePlan::from_unfiltered(
        NodeAccessPlan::AllScan,
    )];
    assert!(!crate::planning::push_node_union_candidate(
        &mut node_sources,
        NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::Empty),
    ));
    assert_eq!(node_sources.len(), 1);
    assert!(matches!(node_sources[0].as_ref(), NodeAccessPlan::AllScan));

    let mut edge_sources = vec![EdgeAccessSourcePlan::from_unfiltered(
        EdgeAccessPlan::AllScan,
    )];
    assert!(!crate::planning::push_edge_union_candidate(
        &mut edge_sources,
        EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::Empty),
    ));
    assert_eq!(edge_sources.len(), 1);
    assert!(matches!(edge_sources[0].as_ref(), EdgeAccessPlan::AllScan));
}

#[test]
fn edge_labeled_or_combination_uses_scoped_index_union_without_label_scan() {
    let plan = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::or(vec![
                Predicate::eq("status", "active"),
                Predicate::gte("since", 2020),
            ]),
        ),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
            .with_edge_range(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "since", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let plans = edge_candidate_sources(edge_access(&plan));
    let [source] = plans.as_slice() else {
        panic!("expected single edge union source: {plans:?}");
    };
    let EdgeAccessPlan::Union(union) = source else {
        panic!("expected edge union source: {plans:?}");
    };

    assert_edge_eq(
        union,
        "FOLLOWS",
        "status",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap()),
    );
    assert_edge_range(
        union,
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "since", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(2020)).unwrap(),
            ),
        },
    );
    assert_no_edge_label_scan_source(&plans);
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::EdgeUnion);
    assert_no_decision(&plan, TraceDecision::EdgeIntersect);
}

#[test]
fn edge_literal_in_values_use_equality_index_union_without_label_scan() {
    let plan = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::is_in(
                "status",
                PropertyValue::StringArray(vec!["active".into(), "pending".into()]),
            ),
        ),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())),
    );
    let plans = edge_candidate_sources(edge_access(&plan));
    let [source] = plans.as_slice() else {
        panic!("expected single edge union source: {plans:?}");
    };
    let EdgeAccessPlan::Union(union) = source else {
        panic!("expected edge union source: {plans:?}");
    };

    assert_edge_eq(
        union,
        "FOLLOWS",
        "status",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap()),
    );
    assert_edge_eq(
        union,
        "FOLLOWS",
        "status",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("pending")).unwrap()),
    );
    assert_no_edge_label_scan_source(&plans);
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::EdgeUnion);
}

#[test]
fn edge_literal_in_duplicate_values_dedupe_to_single_equality_index() {
    let mut planner_ctx = ctx(builtin_label_indexes()
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap()));
    planner_ctx.limits = PlannerLimits {
        max_index_union_branches: IndexUnionBranchLimit::limited(1).unwrap(),
    };
    let plan = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::is_in("status", PropertyValue::array(["active", "active"])),
        ),
        planner_ctx,
    );
    let plans = edge_candidate_sources(edge_access(&plan));

    assert_eq!(plans.len(), 1);
    assert_edge_eq(
        &plans,
        "FOLLOWS",
        "status",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap()),
    );
    assert_no_edge_label_scan_source(&plans);
    assert_no_decision(&plan, TraceDecision::EdgeUnion);
}

#[test]
fn edge_or_with_unindexed_branch_falls_back_to_label_scan() {
    let plan = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::or(vec![
                Predicate::eq("status", "active"),
                Predicate::contains("note", "manual"),
            ]),
        ),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())),
    );

    assert_edge_label_scan(edge_access(&plan), "FOLLOWS");
    assert_no_decision(&plan, TraceDecision::EdgeUnion);
}

#[test]
fn edge_or_covered_branch_subsumes_residual_branch_without_runtime_filter() {
    let plan = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::or(vec![
                Predicate::eq("status", "active"),
                Predicate::and(vec![
                    Predicate::eq("status", "active"),
                    Predicate::contains("note", "manual"),
                ]),
            ]),
        ),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())),
    );

    assert!(matches!(
        edge_access(&plan),
        EdgeAccessPlan::EqualityIndex { key, value, .. }
            if key.label == "FOLLOWS"
                && key.property == "status"
                && value == &IndexValue::Literal(
                    SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap()
                )
    ));
    assert_no_decision(&plan, TraceDecision::EdgeScanOr);
    assert_no_decision(&plan, TraceDecision::ResidualFilter);
}

#[test]
fn edge_or_with_partially_indexed_branches_uses_union_source_residual() {
    let predicate = Predicate::or(vec![
        Predicate::and(vec![
            Predicate::eq("$label", "FOLLOWS"),
            Predicate::eq("status", "active"),
            Predicate::starts_with("note", "friend"),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "FOLLOWS"),
            Predicate::eq("tenant_id", "acme"),
            Predicate::contains("note", "team"),
        ]),
    ]);
    let plan = plan_traversal(
        g().e_where(predicate.clone()),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "tenant_id").unwrap())),
    );
    let EdgeAccessPlan::ScanThenFilter { source, residual } = edge_access(&plan) else {
        panic!("expected edge OR residual fallback: {plan:?}");
    };
    let EdgeAccessPlan::Union(branches) = source.as_ref() else {
        panic!("expected edge OR residual over union source: {plan:?}");
    };

    assert_eq!(branches.len(), 2);
    assert_edge_eq(
        branches,
        "FOLLOWS",
        "status",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap()),
    );
    assert_edge_eq(
        branches,
        "FOLLOWS",
        "tenant_id",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("acme")).unwrap()),
    );
    assert_eq!(residual.predicate(), &predicate);
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::EdgeScanOr);
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::EdgeUnion);
}

#[test]
fn edge_or_with_shared_partial_index_uses_common_source_residual() {
    let predicate = Predicate::or(vec![
        Predicate::and(vec![
            Predicate::eq("$label", "FOLLOWS"),
            Predicate::eq("status", "active"),
            Predicate::eq("tenant_id", "acme"),
            Predicate::starts_with("note", "friend"),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "FOLLOWS"),
            Predicate::eq("status", "active"),
            Predicate::eq("tenant_id", "acme"),
            Predicate::contains("note", "team"),
        ]),
    ]);
    let plan = plan_traversal(
        g().e_where(predicate.clone()),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "tenant_id").unwrap())),
    );
    let EdgeAccessPlan::ScanThenFilter { source, residual } = edge_access(&plan) else {
        panic!("expected edge OR residual over shared index source: {plan:?}");
    };
    let sources = edge_candidate_sources(source.as_ref());

    assert_eq!(sources.len(), 2);
    assert_edge_eq(
        &sources,
        "FOLLOWS",
        "status",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap()),
    );
    assert_edge_eq(
        &sources,
        "FOLLOWS",
        "tenant_id",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("acme")).unwrap()),
    );
    assert_eq!(residual.predicate(), &predicate);
    assert_no_edge_label_scan_source(&sources);
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::EdgeScanOr);
    assert_no_decision(&plan, TraceDecision::EdgeUnion);
}

#[test]
fn edge_or_with_subsuming_partial_range_uses_wider_source_residual() {
    let predicate = Predicate::or(vec![
        Predicate::and(vec![
            Predicate::eq("$label", "FOLLOWS"),
            Predicate::lt("weight", 45),
            Predicate::starts_with("note", "friend"),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "FOLLOWS"),
            Predicate::lte("weight", 50),
            Predicate::contains("note", "team"),
        ]),
    ]);
    let plan = plan_traversal(
        g().e_where(predicate.clone()),
        ctx(builtin_label_indexes().with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
        )),
    );
    let EdgeAccessPlan::ScanThenFilter { source, residual } = edge_access(&plan) else {
        panic!("expected edge OR residual over subsuming range source: {plan:?}");
    };
    let sources = edge_candidate_sources(source.as_ref());

    assert_eq!(sources.len(), 1);
    assert_edge_range(
        &sources,
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Upper {
            upper: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(50)).unwrap(),
            ),
        },
    );
    assert_eq!(residual.predicate(), &predicate);
    assert_no_edge_label_scan_source(&sources);
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::EdgeScanOr);
    assert_no_decision(&plan, TraceDecision::EdgeUnion);
}

#[test]
fn edge_or_label_only_branches_plan_direct_label_scan() {
    let predicate = Predicate::or(vec![
        Predicate::eq("$label", "FOLLOWS"),
        Predicate::compare(Expr::val("FOLLOWS"), CompareOp::Eq, Expr::prop("$label")),
    ]);
    let plan = plan_traversal(g().e_where(predicate), ctx(builtin_label_indexes()));

    assert!(matches!(
        edge_access(&plan),
        EdgeAccessPlan::LabelScan { label } if label == "FOLLOWS"
    ));
    assert_no_decision(&plan, TraceDecision::EdgeScanOr);
    assert_no_decision(&plan, TraceDecision::EdgeUnion);
}

#[test]
fn label_tautologies_inside_indexed_conjunctions_do_not_add_sources_or_residuals() {
    let node_label_tautology = Predicate::or(vec![
        Predicate::eq("$label", "User"),
        Predicate::compare(Expr::val("User"), CompareOp::Eq, Expr::prop("$label")),
    ]);
    let node = plan_traversal(
        g().n_where(Predicate::and(vec![
            Predicate::eq("$label", "User"),
            node_label_tautology,
            Predicate::eq("username", "alice"),
        ])),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())),
    );
    let node_sources = node_candidate_sources(node_access(&node));

    assert!(matches!(
        node_access(&node),
        NodeAccessPlan::EqualityIndex { key, .. }
            if key.label == "User" && key.property == "username"
    ));
    assert_eq!(node_sources.len(), 1);
    assert_no_node_label_scan_source(&node_sources);

    let edge_label_tautology = Predicate::or(vec![
        Predicate::eq("$label", "FOLLOWS"),
        Predicate::compare(Expr::val("FOLLOWS"), CompareOp::Eq, Expr::prop("$label")),
    ]);
    let edge = plan_traversal(
        g().e_where(Predicate::and(vec![
            Predicate::eq("$label", "FOLLOWS"),
            edge_label_tautology,
            Predicate::eq("status", "active"),
        ])),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())),
    );
    let edge_sources = edge_candidate_sources(edge_access(&edge));

    assert!(matches!(
        edge_access(&edge),
        EdgeAccessPlan::EqualityIndex { key, .. }
            if key.label == "FOLLOWS" && key.property == "status"
    ));
    assert_eq!(edge_sources.len(), 1);
    assert_no_edge_label_scan_source(&edge_sources);
}

#[test]
fn union_helpers_report_label_tautology_branches_as_covered_without_source() {
    let node_label = NonEmptyString::new("User").unwrap();
    let node_ctx = ctx(builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap()));
    let mut node_planner = crate::planning::Planner::new(&node_ctx);
    let node_plan = node_planner
        .node_union_plan(
            &[
                Predicate::eq("$label", "User"),
                Predicate::eq("username", "alice"),
            ],
            &node_label,
            "test.node_label_or",
        )
        .unwrap();

    assert!(node_plan.covered);
    assert!(node_plan.source.is_none());

    let edge_label = NonEmptyString::new("FOLLOWS").unwrap();
    let edge_ctx = ctx(builtin_label_indexes()
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap()));
    let mut edge_planner = crate::planning::Planner::new(&edge_ctx);
    let edge_plan = edge_planner
        .edge_union_plan(
            &[
                Predicate::eq("$label", "FOLLOWS"),
                Predicate::eq("status", "active"),
            ],
            &edge_label,
            "test.edge_label_or",
        )
        .unwrap();

    assert!(edge_plan.covered);
    assert!(edge_plan.source.is_none());
}

#[test]
fn edge_and_sources_drop_union_subsumed_by_required_branch() {
    let status = Predicate::eq("status", "active");
    let tenant = Predicate::eq("tenant_id", "acme");
    for predicate in [
        Predicate::and(vec![
            status.clone(),
            Predicate::or(vec![status.clone(), tenant.clone()]),
        ]),
        Predicate::and(vec![
            Predicate::or(vec![status.clone(), tenant.clone()]),
            status.clone(),
        ]),
    ] {
        let plan = plan_traversal(
            g().e_with_label_where("FOLLOWS", predicate),
            ctx(builtin_label_indexes()
                .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
                .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "tenant_id").unwrap())),
        );
        let sources = edge_candidate_sources(edge_access(&plan));

        assert_eq!(sources.len(), 1);
        assert_edge_eq(
            &sources,
            "FOLLOWS",
            "status",
            IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap()),
        );
        assert_no_decision(&plan, TraceDecision::EdgeIntersect);
    }
}

#[test]
fn edge_or_sources_drop_intersections_subsumed_by_single_source() {
    let predicate = Predicate::or(vec![
        Predicate::and(vec![
            Predicate::eq("status", "active"),
            Predicate::eq("tenant_id", "acme"),
        ]),
        Predicate::eq("status", "active"),
    ]);
    let plan = plan_traversal(
        g().e_with_label_where("FOLLOWS", predicate),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "tenant_id").unwrap())),
    );
    let sources = edge_candidate_sources(edge_access(&plan));

    assert_eq!(sources.len(), 1);
    assert_edge_eq(
        &sources,
        "FOLLOWS",
        "status",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap()),
    );
    assert_no_decision(&plan, TraceDecision::EdgeUnion);
}

#[test]
fn edge_or_sources_drop_intersections_subsumed_by_wider_intersection() {
    let predicate = Predicate::or(vec![
        Predicate::and(vec![
            Predicate::eq("status", "active"),
            Predicate::lt("weight", 50),
        ]),
        Predicate::and(vec![
            Predicate::eq("status", "active"),
            Predicate::lt("weight", 50),
            Predicate::eq("tenant_id", "acme"),
        ]),
    ]);
    let plan = plan_traversal(
        g().e_with_label_where("FOLLOWS", predicate),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "tenant_id").unwrap())
            .with_edge_range(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let sources = edge_candidate_sources(edge_access(&plan));

    assert_eq!(sources.len(), 2);
    assert_edge_eq(
        &sources,
        "FOLLOWS",
        "status",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap()),
    );
    assert_edge_range(
        &sources,
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Upper {
            upper: IndexBound::Exclusive(
                RangeIndexValue::literal(PropertyValue::from(50)).unwrap(),
            ),
        },
    );
    assert!(
        sources.iter().all(|source| !matches!(
            *source,
            EdgeAccessPlan::EqualityIndex { key, .. } if key.property == "tenant_id"
        )),
        "unexpected subsumed tenant branch: {sources:?}"
    );
    assert_no_decision(&plan, TraceDecision::EdgeUnion);
}

#[test]
fn edge_or_intersection_branches_factor_common_sources() {
    let predicate = Predicate::or(vec![
        Predicate::and(vec![
            Predicate::eq("status", "active"),
            Predicate::lt("weight", 50),
        ]),
        Predicate::and(vec![
            Predicate::eq("status", "active"),
            Predicate::gte("since", 2024),
        ]),
    ]);
    let plan = plan_traversal(
        g().e_with_label_where("FOLLOWS", predicate),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
            .with_edge_range(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                    .unwrap(),
            )
            .with_edge_range(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "since", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let sources = edge_candidate_sources(edge_access(&plan));
    let range_union = sources
        .iter()
        .find_map(|source| match source {
            EdgeAccessPlan::Union(branches) => Some(branches),
            _ => None,
        })
        .unwrap_or_else(|| panic!("expected factored edge range union: {sources:?}"));

    assert_eq!(sources.len(), 2);
    assert_edge_eq(
        &sources,
        "FOLLOWS",
        "status",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap()),
    );
    assert_eq!(range_union.len(), 2);
    assert_edge_range(
        range_union,
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Upper {
            upper: IndexBound::Exclusive(
                RangeIndexValue::literal(PropertyValue::from(50)).unwrap(),
            ),
        },
    );
    assert_edge_range(
        range_union,
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "since", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(2024)).unwrap(),
            ),
        },
    );
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::EdgeUnion);
}

#[test]
fn edge_or_sources_drop_range_branches_subsumed_by_wider_range_source() {
    let predicate = Predicate::or(vec![
        Predicate::lt("weight", 45),
        Predicate::lte("weight", 50),
    ]);
    let plan = plan_traversal(
        g().e_with_label_where("FOLLOWS", predicate),
        ctx(builtin_label_indexes().with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
        )),
    );
    let sources = edge_candidate_sources(edge_access(&plan));

    assert_eq!(sources.len(), 1);
    assert_edge_range(
        &sources,
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Upper {
            upper: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(50)).unwrap(),
            ),
        },
    );
    assert_no_decision(&plan, TraceDecision::EdgeUnion);
}

#[test]
fn edge_range_union_intersection_narrows_overlapping_branches() {
    let plan = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::or(vec![
                Predicate::between("weight", 0, 10),
                Predicate::between("weight", 20, 30),
            ]),
        )
        .where_(Predicate::between("weight", 5, 25)),
        ctx(builtin_label_indexes().with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
        )),
    );
    let sources = edge_candidate_sources(edge_access(&plan));
    let [source] = sources.as_slice() else {
        panic!("expected one narrowed edge range union source: {sources:?}");
    };
    let EdgeAccessPlan::Union(branches) = source else {
        panic!("expected edge range union source: {source:?}");
    };

    assert_eq!(branches.len(), 2);
    assert_edge_range(
        branches,
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
        IndexRange::between(
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(5)).unwrap()),
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(10)).unwrap()),
        ),
    );
    assert_edge_range(
        branches,
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
        IndexRange::between(
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(20)).unwrap()),
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(25)).unwrap()),
        ),
    );
    assert_no_decision(&plan, TraceDecision::EdgeIntersect);
}

#[test]
fn edge_range_union_intersection_drops_exclusive_adjacent_branches() {
    let plan = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::or(vec![
                Predicate::lt("weight", 5),
                Predicate::between("weight", 10, 20),
            ]),
        )
        .where_(Predicate::gte("weight", 5)),
        ctx(builtin_label_indexes().with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
        )),
    );
    let sources = edge_candidate_sources(edge_access(&plan));
    let [_source] = sources.as_slice() else {
        panic!("expected one surviving edge range source: {sources:?}");
    };

    assert_edge_range(
        &sources,
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
        IndexRange::between(
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(10)).unwrap()),
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(20)).unwrap()),
        ),
    );
    assert_no_decision(&plan, TraceDecision::EdgeIntersect);
}

#[test]
fn edge_range_unions_intersect_before_runtime_scan() {
    let planner_ctx = ctx(builtin_label_indexes().with_edge_range(
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
    ));
    let left = Predicate::or(vec![
        Predicate::between("weight", 0, 10),
        Predicate::between("weight", 20, 30),
    ]);
    let right = Predicate::or(vec![
        Predicate::between("weight", 5, 15),
        Predicate::between("weight", 25, 35),
    ]);

    for plan in [
        plan_traversal(
            g().e_with_label_where("FOLLOWS", left.clone())
                .where_(right.clone()),
            planner_ctx.clone(),
        ),
        plan_traversal(
            g().e_with_label_where("FOLLOWS", right).where_(left),
            planner_ctx,
        ),
    ] {
        let sources = edge_candidate_sources(edge_access(&plan));
        let [source] = sources.as_slice() else {
            panic!("expected one narrowed edge range union source: {sources:?}");
        };
        let EdgeAccessPlan::Union(branches) = source else {
            panic!("expected narrowed edge range union: {source:?}");
        };

        assert_eq!(branches.len(), 2);
        assert_edge_range(
            branches,
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
            IndexRange::between(
                IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(5)).unwrap()),
                IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(10)).unwrap()),
            ),
        );
        assert_edge_range(
            branches,
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
            IndexRange::between(
                IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(25)).unwrap()),
                IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(30)).unwrap()),
            ),
        );
        assert_no_decision(&plan, TraceDecision::EdgeIntersect);
    }
}

#[test]
fn edge_range_unions_intersection_collapses_empty() {
    let plan = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::or(vec![
                Predicate::between("weight", 0, 10),
                Predicate::between("weight", 20, 30),
            ]),
        )
        .where_(Predicate::or(vec![
            Predicate::between("weight", 40, 50),
            Predicate::between("weight", 60, 70),
        ])),
        ctx(builtin_label_indexes().with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
        )),
    );

    assert!(matches!(edge_access(&plan), EdgeAccessPlan::Empty));
    assert_no_decision(&plan, TraceDecision::EdgeIntersect);
}

#[test]
fn edge_range_union_intersection_keeps_mismatched_keys_and_fanout_conservative() {
    let mismatched_keys = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::or(vec![
                Predicate::between("weight", 0, 10),
                Predicate::between("weight", 20, 30),
            ]),
        )
        .where_(Predicate::or(vec![
            Predicate::between("rank", 0, 10),
            Predicate::between("rank", 20, 30),
        ])),
        ctx(builtin_label_indexes()
            .with_edge_range(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                    .unwrap(),
            )
            .with_edge_range(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "rank", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let fanout = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::or(vec![
                Predicate::between("weight", 0, 10),
                Predicate::between("weight", 20, 30),
                Predicate::between("weight", 40, 50),
            ]),
        )
        .where_(Predicate::or(vec![
            Predicate::between("weight", 5, 25),
            Predicate::between("weight", 28, 45),
            Predicate::between("weight", 48, 60),
        ])),
        ctx(builtin_label_indexes().with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
        )),
    );

    for plan in [&mismatched_keys, &fanout] {
        assert!(
            matches!(edge_access(plan), EdgeAccessPlan::Intersect(_)),
            "expected conservative edge intersection: {:?}",
            edge_access(plan)
        );
        assert_decision(plan, TracePass::AccessPath, TraceDecision::EdgeIntersect);
    }
}

#[test]
fn edge_equality_union_intersects_range_union_before_runtime_scan() {
    let planner_ctx = ctx(builtin_label_indexes()
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap())
        .with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
        ));
    let literal_in = Predicate::is_in("weight", PropertyValue::I64Array(vec![10, 20, 30]));
    let range_union = Predicate::or(vec![
        Predicate::between("weight", 5, 15),
        Predicate::between("weight", 25, 35),
    ]);

    for plan in [
        plan_traversal(
            g().e_with_label_where("FOLLOWS", literal_in.clone())
                .where_(range_union.clone()),
            planner_ctx.clone(),
        ),
        plan_traversal(
            g().e_with_label_where("FOLLOWS", range_union)
                .where_(literal_in),
            planner_ctx,
        ),
    ] {
        let sources = edge_candidate_sources(edge_access(&plan));
        let [source] = sources.as_slice() else {
            panic!("expected one retained edge equality union source: {sources:?}");
        };
        let EdgeAccessPlan::Union(branches) = source else {
            panic!("expected retained edge equality union: {source:?}");
        };

        assert_eq!(branches.len(), 2);
        assert_edge_eq(
            branches,
            "FOLLOWS",
            "weight",
            IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(10)).unwrap()),
        );
        assert_edge_eq(
            branches,
            "FOLLOWS",
            "weight",
            IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(30)).unwrap()),
        );
        assert_no_decision(&plan, TraceDecision::EdgeIntersect);
    }
}

#[test]
fn edge_equality_union_range_union_intersection_collapses_empty() {
    let plan = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::is_in("weight", PropertyValue::I64Array(vec![10, 20])),
        )
        .where_(Predicate::or(vec![
            Predicate::between("weight", 30, 40),
            Predicate::between("weight", 50, 60),
        ])),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap())
            .with_edge_range(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );

    assert!(matches!(edge_access(&plan), EdgeAccessPlan::Empty));
    assert_no_decision(&plan, TraceDecision::EdgeIntersect);
}

#[test]
fn edge_or_literal_in_union_drops_values_subsumed_by_range_source() {
    let literal_in = Predicate::is_in("weight", PropertyValue::I64Array(vec![10, 30]));
    let range = Predicate::gte("weight", 21);
    for predicate in [
        Predicate::or(vec![literal_in.clone(), range.clone()]),
        Predicate::or(vec![range, literal_in]),
    ] {
        let plan = plan_traversal(
            g().e_with_label_where("FOLLOWS", predicate),
            ctx(builtin_label_indexes()
                .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap())
                .with_edge_range(
                    ScopedPropertyDirectionKey::try_new(
                        "FOLLOWS",
                        "weight",
                        RangeIndexDirection::Asc,
                    )
                    .unwrap(),
                )),
        );
        let EdgeAccessPlan::Union(branches) = edge_scan_source(edge_access(&plan)) else {
            panic!("expected edge union source: {:?}", edge_access(&plan));
        };

        assert_eq!(branches.len(), 2);
        assert_edge_eq(
            branches,
            "FOLLOWS",
            "weight",
            IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(10)).unwrap()),
        );
        assert_edge_range(
            branches,
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
            IndexRange::Lower {
                lower: IndexBound::Inclusive(
                    RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
                ),
            },
        );
        assert!(branches
            .iter()
            .all(|branch| !matches!(branch.as_ref(), EdgeAccessPlan::Union(_))));
        assert!(!branches.iter().any(|branch| matches!(
            branch.as_ref(),
            EdgeAccessPlan::EqualityIndex { value, .. }
                if value == &IndexValue::Literal(
                    SecondaryIndexLiteral::new(PropertyValue::from(30)).unwrap()
                )
        )));
    }
}

#[test]
fn edge_or_literal_in_union_disappears_when_range_subsumes_all_values() {
    let literal_in = Predicate::is_in("weight", PropertyValue::I64Array(vec![30, 40]));
    let range = Predicate::gte("weight", 21);
    for predicate in [
        Predicate::or(vec![literal_in.clone(), range.clone()]),
        Predicate::or(vec![range, literal_in]),
    ] {
        let plan = plan_traversal(
            g().e_with_label_where("FOLLOWS", predicate),
            ctx(builtin_label_indexes()
                .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap())
                .with_edge_range(
                    ScopedPropertyDirectionKey::try_new(
                        "FOLLOWS",
                        "weight",
                        RangeIndexDirection::Asc,
                    )
                    .unwrap(),
                )),
        );
        let sources = edge_candidate_sources(edge_access(&plan));

        assert_eq!(sources.len(), 1);
        assert_edge_range(
            &sources,
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
            IndexRange::Lower {
                lower: IndexBound::Inclusive(
                    RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
                ),
            },
        );
        assert!(!matches!(
            edge_scan_source(edge_access(&plan)),
            EdgeAccessPlan::Union(_)
        ));
    }
}

#[test]
fn edge_or_literal_in_union_flattens_dynamic_and_mixed_union_sources() {
    let dynamic_range = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::or(vec![
                Predicate::is_in("weight", PropertyValue::I64Array(vec![10, 30])),
                Predicate::gte_param("weight", "min_weight"),
            ]),
        ),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap())
            .with_edge_range(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let EdgeAccessPlan::Union(dynamic_branches) = edge_scan_source(edge_access(&dynamic_range))
    else {
        panic!(
            "expected edge union source: {:?}",
            edge_access(&dynamic_range)
        );
    };
    assert_eq!(dynamic_branches.len(), 3);
    assert!(dynamic_branches
        .iter()
        .all(|branch| !matches!(branch.as_ref(), EdgeAccessPlan::Union(_))));
    assert_edge_eq(
        dynamic_branches,
        "FOLLOWS",
        "weight",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(10)).unwrap()),
    );
    assert_edge_eq(
        dynamic_branches,
        "FOLLOWS",
        "weight",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(30)).unwrap()),
    );
    assert_edge_range(
        dynamic_branches,
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(RangeIndexValue::param("min_weight").unwrap()),
        },
    );

    let mixed_union = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::or(vec![
                Predicate::or(vec![
                    Predicate::is_in("weight", PropertyValue::I64Array(vec![10])),
                    Predicate::eq("status", "active"),
                ]),
                Predicate::gte("weight", 21),
            ]),
        ),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap())
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
            .with_edge_range(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let EdgeAccessPlan::Union(mixed_branches) = edge_scan_source(edge_access(&mixed_union)) else {
        panic!(
            "expected edge union source: {:?}",
            edge_access(&mixed_union)
        );
    };
    assert_eq!(mixed_branches.len(), 3);
    assert!(mixed_branches
        .iter()
        .all(|branch| !matches!(branch.as_ref(), EdgeAccessPlan::Union(_))));
    assert_edge_eq(
        mixed_branches,
        "FOLLOWS",
        "weight",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(10)).unwrap()),
    );
    assert_edge_eq(
        mixed_branches,
        "FOLLOWS",
        "status",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap()),
    );
    assert_edge_range(
        mixed_branches,
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
            ),
        },
    );

    let mixed_same_property_range = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::or(vec![
                Predicate::or(vec![
                    Predicate::is_in("weight", PropertyValue::I64Array(vec![10])),
                    Predicate::gte("weight", 31),
                ]),
                Predicate::gte("weight", 21),
            ]),
        ),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap())
            .with_edge_range(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let EdgeAccessPlan::Union(mixed_same_property_branches) =
        edge_scan_source(edge_access(&mixed_same_property_range))
    else {
        panic!(
            "expected edge union source: {:?}",
            edge_access(&mixed_same_property_range)
        );
    };
    assert_eq!(mixed_same_property_branches.len(), 2);
    assert!(mixed_same_property_branches
        .iter()
        .all(|branch| !matches!(branch.as_ref(), EdgeAccessPlan::Union(_))));
    assert_edge_eq(
        mixed_same_property_branches,
        "FOLLOWS",
        "weight",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(10)).unwrap()),
    );
    assert_edge_range(
        mixed_same_property_branches,
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
            ),
        },
    );
}

#[test]
fn edge_or_overlapping_literal_in_unions_flatten_and_dedupe_before_branch_limit() {
    let mut planner_ctx = ctx(builtin_label_indexes()
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap()));
    planner_ctx.limits = PlannerLimits {
        max_index_union_branches: IndexUnionBranchLimit::limited(3).unwrap(),
    };
    let plan = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::or(vec![
                Predicate::is_in("status", PropertyValue::array(["active", "pending"])),
                Predicate::is_in("status", PropertyValue::array(["pending", "archived"])),
            ]),
        ),
        planner_ctx,
    );
    let EdgeAccessPlan::Union(branches) = edge_scan_source(edge_access(&plan)) else {
        panic!(
            "expected flattened edge union source: {:?}",
            edge_access(&plan)
        );
    };

    assert_eq!(branches.len(), 3);
    assert!(branches
        .iter()
        .all(|branch| !matches!(branch.as_ref(), EdgeAccessPlan::Union(_))));
    for value in ["active", "pending", "archived"] {
        assert_edge_eq(
            branches,
            "FOLLOWS",
            "status",
            IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(value)).unwrap()),
        );
    }
    assert_no_edge_label_scan_source(&edge_candidate_sources(edge_access(&plan)));
}

#[test]
fn nested_edge_or_sources_participate_in_union_subsumption() {
    let range_then_nested = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::or(vec![
                Predicate::lte("weight", 50),
                Predicate::or(vec![
                    Predicate::eq("weight", 40),
                    Predicate::eq("weight", 45),
                ]),
            ]),
        ),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap())
            .with_edge_range(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let range_sources = edge_candidate_sources(edge_access(&range_then_nested));
    assert_eq!(range_sources.len(), 1);
    assert_edge_range(
        &range_sources,
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Upper {
            upper: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(50)).unwrap(),
            ),
        },
    );

    let nested_then_equality = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::or(vec![
                Predicate::or(vec![
                    Predicate::lte("weight", 50),
                    Predicate::eq("status", "active"),
                ]),
                Predicate::eq("weight", 40),
            ]),
        ),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap())
            .with_edge_range(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let EdgeAccessPlan::Union(branches) = edge_scan_source(edge_access(&nested_then_equality))
    else {
        panic!(
            "expected nested edge union source: {:?}",
            edge_access(&nested_then_equality)
        );
    };

    assert_eq!(branches.len(), 2);
    assert_edge_range(
        branches,
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Upper {
            upper: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(50)).unwrap(),
            ),
        },
    );
    assert_edge_eq(
        branches,
        "FOLLOWS",
        "status",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap()),
    );
    assert!(
        branches.iter().all(|branch| !matches!(
            branch.as_ref(),
            EdgeAccessPlan::EqualityIndex { key, .. } if key.property == "weight"
        )),
        "subsumed weight equality branch remained in {branches:?}"
    );
}

#[test]
fn duplicate_edge_or_branches_are_deduped_before_branch_limit() {
    let status = Predicate::and(vec![
        Predicate::eq("$label", "FOLLOWS"),
        Predicate::eq("status", "active"),
    ]);
    let tenant = Predicate::and(vec![
        Predicate::eq("$label", "FOLLOWS"),
        Predicate::eq("tenant_id", "acme"),
    ]);
    let predicate = Predicate::or(vec![status.clone(), tenant, status]);
    let mut planner_ctx = ctx(builtin_label_indexes()
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "tenant_id").unwrap()));
    planner_ctx.limits = PlannerLimits {
        max_index_union_branches: IndexUnionBranchLimit::limited(2).unwrap(),
    };
    let plan = plan_traversal(g().e_where(predicate), planner_ctx);
    let EdgeAccessPlan::Union(branches) = edge_access(&plan) else {
        panic!("expected edge union: {:?}", edge_access(&plan));
    };

    assert_eq!(branches.len(), 2);
    assert_edge_eq(
        branches,
        "FOLLOWS",
        "status",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap()),
    );
    assert_edge_eq(
        branches,
        "FOLLOWS",
        "tenant_id",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("acme")).unwrap()),
    );
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::EdgeUnion);
    assert_no_decision(&plan, TraceDecision::EdgeScanOr);
}

#[test]
fn edge_or_later_subsuming_branch_collapses_before_branch_limit() {
    let predicate = Predicate::or(vec![
        Predicate::eq("weight", 40),
        Predicate::eq("weight", 45),
        Predicate::lte("weight", 50),
    ]);
    let mut planner_ctx = ctx(builtin_label_indexes()
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap())
        .with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
        ));
    planner_ctx.limits = PlannerLimits {
        max_index_union_branches: IndexUnionBranchLimit::limited(1).unwrap(),
    };
    let plan = plan_traversal(g().e_with_label_where("FOLLOWS", predicate), planner_ctx);

    let sources = edge_candidate_sources(edge_access(&plan));
    assert_eq!(sources.len(), 1);
    assert_edge_range(
        &sources,
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Upper {
            upper: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(50)).unwrap(),
            ),
        },
    );
    assert_no_decision(&plan, TraceDecision::EdgeUnion);
    assert_no_decision(&plan, TraceDecision::EdgeScanOr);
}

#[test]
fn edge_or_disabled_branch_limit_falls_back_to_label_scan() {
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
        max_index_union_branches: IndexUnionBranchLimit::Disabled,
    };
    let plan = plan_traversal(g().e_where(predicate), planner_ctx);

    assert_edge_label_scan(edge_access(&plan), "FOLLOWS");
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::EdgeScanOr);
}

#[test]
fn edge_partial_or_union_branch_limit_falls_back_to_label_scan() {
    let predicate = Predicate::or(vec![
        Predicate::and(vec![
            Predicate::eq("$label", "FOLLOWS"),
            Predicate::eq("status", "active"),
            Predicate::starts_with("note", "friend"),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "FOLLOWS"),
            Predicate::eq("tenant_id", "acme"),
            Predicate::contains("note", "team"),
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
    assert_no_decision(&plan, TraceDecision::EdgeUnion);
}

#[test]
fn edge_default_or_branch_limit_accepts_boundary_and_rejects_overflow() {
    let max_branches = default_index_union_branch_limit();
    let (boundary_indexes, boundary_predicate) =
        edge_or_indexes_and_predicate(max_branches, "FOLLOWS");
    let boundary_plan = plan_traversal(
        g().e_where(boundary_predicate),
        PlannerContext {
            indexes: boundary_indexes,
            ..PlannerContext::default()
        },
    );
    let EdgeAccessPlan::Union(branches) = edge_access(&boundary_plan) else {
        panic!(
            "expected boundary edge union: {:?}",
            edge_access(&boundary_plan)
        );
    };

    assert_eq!(branches.len(), max_branches);
    assert_decision(
        &boundary_plan,
        TracePass::AccessPath,
        TraceDecision::EdgeUnion,
    );

    let (overflow_indexes, overflow_predicate) =
        edge_or_indexes_and_predicate(max_branches + 1, "FOLLOWS");
    let overflow_plan = plan_traversal(
        g().e_where(overflow_predicate),
        PlannerContext {
            indexes: overflow_indexes,
            ..PlannerContext::default()
        },
    );

    assert_edge_label_scan(edge_access(&overflow_plan), "FOLLOWS");
    assert_decision(
        &overflow_plan,
        TracePass::AccessPath,
        TraceDecision::EdgeScanOr,
    );
    assert_no_decision(&overflow_plan, TraceDecision::EdgeUnion);
}

#[test]
fn edge_literal_in_values_respect_union_branch_limit_after_dedup() {
    let mut boundary_ctx = ctx(builtin_label_indexes()
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap()));
    boundary_ctx.limits = PlannerLimits {
        max_index_union_branches: IndexUnionBranchLimit::limited(2).unwrap(),
    };
    let boundary_plan = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::is_in(
                "status",
                PropertyValue::array(["active", "active", "pending"]),
            ),
        ),
        boundary_ctx,
    );
    let boundary_sources = edge_candidate_sources(edge_access(&boundary_plan));
    let [source] = boundary_sources.as_slice() else {
        panic!("expected one edge union source: {boundary_sources:?}");
    };
    let EdgeAccessPlan::Union(branches) = source else {
        panic!("expected edge union source: {source:?}");
    };

    assert_eq!(branches.len(), 2);
    assert_decision(
        &boundary_plan,
        TracePass::AccessPath,
        TraceDecision::EdgeUnion,
    );

    let mut overflow_ctx = ctx(builtin_label_indexes()
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap()));
    overflow_ctx.limits = PlannerLimits {
        max_index_union_branches: IndexUnionBranchLimit::limited(1).unwrap(),
    };
    let overflow_plan = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::is_in("status", PropertyValue::array(["active", "pending"])),
        ),
        overflow_ctx,
    );

    assert_edge_label_scan(edge_access(&overflow_plan), "FOLLOWS");
    assert_no_decision(&overflow_plan, TraceDecision::EdgeUnion);
}

#[test]
fn edge_literal_in_range_prunes_values_before_union_branch_limit() {
    let mut planner_ctx = ctx(builtin_label_indexes()
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap())
        .with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
        ));
    planner_ctx.limits = PlannerLimits {
        max_index_union_branches: IndexUnionBranchLimit::limited(2).unwrap(),
    };
    let literal_in = Predicate::is_in("weight", PropertyValue::I64Array(vec![10, 20, 30, 40]));
    let range = Predicate::lte("weight", 20);

    for predicate in [
        Predicate::and(vec![literal_in.clone(), range.clone()]),
        Predicate::and(vec![range, literal_in]),
    ] {
        let plan = plan_traversal(
            g().e_with_label_where("FOLLOWS", predicate),
            planner_ctx.clone(),
        );
        let sources = edge_candidate_sources(edge_access(&plan));
        let [source] = sources.as_slice() else {
            panic!("expected one pruned edge union source: {sources:?}");
        };
        let EdgeAccessPlan::Union(branches) = source else {
            panic!("expected edge union source: {source:?}");
        };

        assert_eq!(branches.len(), 2);
        assert_edge_eq(
            branches,
            "FOLLOWS",
            "weight",
            IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(10)).unwrap()),
        );
        assert_edge_eq(
            branches,
            "FOLLOWS",
            "weight",
            IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(20)).unwrap()),
        );
        assert_no_edge_label_scan_source(&sources);
        assert_decision(&plan, TracePass::AccessPath, TraceDecision::EdgeUnion);
        assert_no_decision(&plan, TraceDecision::EdgeScanOr);
    }
}

#[test]
fn split_edge_literal_in_filter_uses_existing_range_before_branch_limit() {
    let mut planner_ctx = ctx(builtin_label_indexes()
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap())
        .with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
        ));
    planner_ctx.limits = PlannerLimits {
        max_index_union_branches: IndexUnionBranchLimit::limited(2).unwrap(),
    };

    let plan = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::lte("weight", 20))
            .where_(Predicate::is_in(
                "weight",
                PropertyValue::I64Array(vec![10, 20, 30, 40]),
            )),
        planner_ctx,
    );
    let sources = edge_candidate_sources(edge_access(&plan));
    let [source] = sources.as_slice() else {
        panic!("expected one split-filter pruned edge union source: {sources:?}");
    };
    let EdgeAccessPlan::Union(branches) = source else {
        panic!("expected edge union source: {source:?}");
    };

    assert_eq!(branches.len(), 2);
    assert_edge_eq(
        branches,
        "FOLLOWS",
        "weight",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(10)).unwrap()),
    );
    assert_edge_eq(
        branches,
        "FOLLOWS",
        "weight",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(20)).unwrap()),
    );
    assert_no_edge_label_scan_source(&sources);
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::EdgeUnion);
}

#[test]
fn split_edge_literal_in_filters_intersect_before_runtime_scan() {
    let planner_ctx = ctx(builtin_label_indexes()
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap()));

    let overlap = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::is_in("weight", PropertyValue::I64Array(vec![10, 20])),
        )
        .where_(Predicate::is_in(
            "weight",
            PropertyValue::I64Array(vec![20, 30]),
        )),
        planner_ctx.clone(),
    );
    let overlap_sources = edge_candidate_sources(edge_access(&overlap));

    assert_eq!(overlap_sources.len(), 1);
    assert_edge_eq(
        &overlap_sources,
        "FOLLOWS",
        "weight",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(20)).unwrap()),
    );
    assert_no_decision(&overlap, TraceDecision::EdgeIntersect);

    let disjoint = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::is_in("weight", PropertyValue::I64Array(vec![10, 20])),
        )
        .where_(Predicate::is_in(
            "weight",
            PropertyValue::I64Array(vec![30, 40]),
        )),
        planner_ctx.clone(),
    );

    assert!(matches!(edge_access(&disjoint), EdgeAccessPlan::Empty));
    assert_no_decision(&disjoint, TraceDecision::EdgeIntersect);

    let equality_overlap = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::is_in("weight", PropertyValue::I64Array(vec![10, 20])),
        )
        .edge_has("weight", 20),
        planner_ctx.clone(),
    );
    let equality_overlap_sources = edge_candidate_sources(edge_access(&equality_overlap));

    assert_eq!(equality_overlap_sources.len(), 1);
    assert_edge_eq(
        &equality_overlap_sources,
        "FOLLOWS",
        "weight",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(20)).unwrap()),
    );
    assert_no_decision(&equality_overlap, TraceDecision::EdgeIntersect);

    let equality_disjoint = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::is_in("weight", PropertyValue::I64Array(vec![10, 20])),
        )
        .edge_has("weight", 30),
        planner_ctx.clone(),
    );

    assert!(matches!(
        edge_access(&equality_disjoint),
        EdgeAccessPlan::Empty
    ));
    assert_no_decision(&equality_disjoint, TraceDecision::EdgeIntersect);

    let conflicting_equalities = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::eq("weight", 10))
            .edge_has("weight", 20),
        planner_ctx,
    );

    assert!(matches!(
        edge_access(&conflicting_equalities),
        EdgeAccessPlan::Empty
    ));
    assert_no_decision(&conflicting_equalities, TraceDecision::EdgeIntersect);
}

#[test]
fn edge_or_branches_pruned_empty_by_literal_ranges_collapse_without_scan() {
    let plan = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::or(vec![
                Predicate::and(vec![
                    Predicate::is_in("weight", PropertyValue::I64Array(vec![30])),
                    Predicate::lt("weight", 10),
                ]),
                Predicate::and(vec![
                    Predicate::is_in("weight", PropertyValue::I64Array(vec![40])),
                    Predicate::lt("weight", 10),
                ]),
            ]),
        ),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap())),
    );

    assert!(matches!(edge_access(&plan), EdgeAccessPlan::Empty));
    assert_no_decision(&plan, TraceDecision::EdgeScanOr);
}

#[test]
fn edge_literal_in_range_intersection_drops_excluded_values_to_single_equality() {
    let literal_in = Predicate::is_in("weight", PropertyValue::I64Array(vec![10, 30]));
    let range = Predicate::gte("weight", 21);
    for predicate in [
        Predicate::and(vec![literal_in.clone(), range.clone()]),
        Predicate::and(vec![range, literal_in]),
    ] {
        let plan = plan_traversal(
            g().e_with_label_where("FOLLOWS", predicate),
            ctx(builtin_label_indexes()
                .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap())
                .with_edge_range(
                    ScopedPropertyDirectionKey::try_new(
                        "FOLLOWS",
                        "weight",
                        RangeIndexDirection::Asc,
                    )
                    .unwrap(),
                )),
        );
        let sources = edge_candidate_sources(edge_access(&plan));

        assert_eq!(sources.len(), 1);
        assert_edge_eq(
            &sources,
            "FOLLOWS",
            "weight",
            IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(30)).unwrap()),
        );
        assert_no_edge_label_scan_source(&sources);
        assert_no_decision(&plan, TraceDecision::EdgeIntersect);
    }
}

#[test]
fn edge_literal_in_range_intersection_drops_excluded_values_to_smaller_union() {
    let plan = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::is_in("weight", PropertyValue::I64Array(vec![10, 30, 40])),
                Predicate::lte("weight", 35),
            ]),
        ),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap())
            .with_edge_range(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let sources = edge_candidate_sources(edge_access(&plan));
    let [source] = sources.as_slice() else {
        panic!("expected one edge union source: {sources:?}");
    };
    let EdgeAccessPlan::Union(branches) = source else {
        panic!("expected edge union source: {source:?}");
    };

    assert_eq!(branches.len(), 2);
    assert_edge_eq(
        branches,
        "FOLLOWS",
        "weight",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(10)).unwrap()),
    );
    assert_edge_eq(
        branches,
        "FOLLOWS",
        "weight",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(30)).unwrap()),
    );
    assert_no_edge_label_scan_source(&sources);
}

#[test]
fn edge_literal_in_range_intersection_can_collapse_distributed_or_to_empty() {
    let plan = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::or(vec![
                    Predicate::is_in("weight", PropertyValue::I64Array(vec![10])),
                    Predicate::is_in("weight", PropertyValue::I64Array(vec![20])),
                ]),
                Predicate::gte("weight", 21),
            ]),
        ),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap())
            .with_edge_range(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );

    assert!(matches!(edge_access(&plan), EdgeAccessPlan::Empty));
}

#[test]
fn edge_literal_in_dynamic_range_intersection_keeps_union_and_range_sources() {
    let plan = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::is_in("weight", PropertyValue::I64Array(vec![10, 30])),
                Predicate::gte_param("weight", "min_weight"),
            ]),
        ),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap())
            .with_edge_range(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );
    let sources = edge_candidate_sources(edge_access(&plan));

    assert_eq!(sources.len(), 2);
    let union = sources
        .iter()
        .find_map(|source| match source {
            EdgeAccessPlan::Union(branches) => Some(branches),
            _ => None,
        })
        .expect("expected edge IN union to survive dynamic range");
    assert_eq!(union.len(), 2);
    assert_edge_range(
        &sources,
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Inclusive(RangeIndexValue::param("min_weight").unwrap()),
        },
    );
}

#[test]
fn edge_literal_in_range_intersection_drops_impossible_mixed_or_branch_sources() {
    let indexes = builtin_label_indexes()
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "weight").unwrap())
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
        .with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
        )
        .with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "since", RangeIndexDirection::Asc)
                .unwrap(),
        );
    let weight_range = IndexRange::Lower {
        lower: IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(21)).unwrap()),
    };

    let status_plan = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::or(vec![
                    Predicate::is_in("weight", PropertyValue::I64Array(vec![10])),
                    Predicate::eq("status", "active"),
                ]),
                Predicate::gte("weight", 21),
            ]),
        ),
        ctx(indexes.clone()),
    );
    let status_sources = edge_candidate_sources(edge_access(&status_plan));
    assert_eq!(status_sources.len(), 2);
    assert!(status_sources
        .iter()
        .all(|source| !matches!(source, EdgeAccessPlan::Union(_))));
    assert_edge_eq(
        &status_sources,
        "FOLLOWS",
        "status",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap()),
    );
    assert_edge_range(
        &status_sources,
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
        weight_range.clone(),
    );

    let since_plan = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::or(vec![
                    Predicate::is_in("weight", PropertyValue::I64Array(vec![10])),
                    Predicate::gt("since", 2024),
                ]),
                Predicate::gte("weight", 21),
            ]),
        ),
        ctx(indexes),
    );
    let since_sources = edge_candidate_sources(edge_access(&since_plan));
    assert_eq!(since_sources.len(), 2);
    assert!(since_sources
        .iter()
        .all(|source| !matches!(source, EdgeAccessPlan::Union(_))));
    assert_edge_range(
        &since_sources,
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
        weight_range,
    );
    assert_edge_range(
        &since_sources,
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "since", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Lower {
            lower: IndexBound::Exclusive(
                RangeIndexValue::literal(PropertyValue::from(2024)).unwrap(),
            ),
        },
    );
}

#[test]
fn empty_edge_access_is_subsumed_by_every_intersection_source() {
    assert!(crate::planning::edge_access_subsumes(
        &EdgeAccessPlan::LabelScan {
            label: NonEmptyString::new("FOLLOWS").unwrap(),
        },
        &EdgeAccessPlan::Empty,
    ));
}

#[test]
fn edge_top_level_or_with_unindexed_branch_falls_back_to_label_scan() {
    let or_fallback = plan_traversal(
        g().e_where(Predicate::or(vec![
            Predicate::and(vec![
                Predicate::eq("$label", "FOLLOWS"),
                Predicate::eq("status", "active"),
            ]),
            Predicate::and(vec![
                Predicate::eq("$label", "FOLLOWS"),
                Predicate::contains("note", "manual"),
            ]),
        ])),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())),
    );

    assert_edge_label_scan(edge_access(&or_fallback), "FOLLOWS");
    assert_decision(
        &or_fallback,
        TracePass::AccessPath,
        TraceDecision::EdgeScanOr,
    );
}

fn default_index_union_branch_limit() -> usize {
    match PlannerLimits::default().max_index_union_branches {
        IndexUnionBranchLimit::Limited(limit) => limit.get(),
        IndexUnionBranchLimit::Disabled => {
            panic!("default index-union branch limit must not be disabled")
        }
    }
}

fn node_or_indexes_and_predicate(
    branches: usize,
    label: &str,
) -> (IndexCatalogSnapshot, Predicate) {
    let mut indexes = builtin_label_indexes();
    let mut predicates = Vec::new();

    for index in 0..branches {
        let property = format!("or_field_{index:02}");
        indexes =
            indexes.with_node_eq(ScopedPropertyKey::try_new(label, property.clone()).unwrap());
        predicates.push(Predicate::and(vec![
            Predicate::eq("$label", label),
            Predicate::eq(property, format!("value_{index:02}")),
        ]));
    }

    (indexes, Predicate::or(predicates))
}

fn edge_or_indexes_and_predicate(
    branches: usize,
    label: &str,
) -> (IndexCatalogSnapshot, Predicate) {
    let mut indexes = builtin_label_indexes();
    let mut predicates = Vec::new();

    for index in 0..branches {
        let property = format!("or_field_{index:02}");
        indexes =
            indexes.with_edge_eq(ScopedPropertyKey::try_new(label, property.clone()).unwrap());
        predicates.push(Predicate::and(vec![
            Predicate::eq("$label", label),
            Predicate::eq(property, format!("value_{index:02}")),
        ]));
    }

    (indexes, Predicate::or(predicates))
}
