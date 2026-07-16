use crate::planning::tests::support::*;

#[test]
fn node_intersections_scan_lowest_cardinality_index_first() {
    let plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::gte("age", 21),
                Predicate::eq("username", "alice"),
                Predicate::eq("tenant_id", "acme"),
                Predicate::eq("unestimated", "kept-last"),
            ]),
        ),
        PlannerContext {
            indexes: builtin_label_indexes()
                .with_node_range(
                    ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                        .unwrap(),
                )
                .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
                .with_node_eq(ScopedPropertyKey::try_new("User", "tenant_id").unwrap())
                .with_node_eq(ScopedPropertyKey::try_new("User", "unestimated").unwrap()),
            stats: StatsSnapshot::default()
                .with_node_label_cardinality(NonEmptyString::new("User").unwrap(), 50_000)
                .with_node_range_cardinality(
                    ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                        .unwrap(),
                    900,
                )
                .with_node_eq_cardinality(
                    ScopedPropertyKey::try_new("User", "username").unwrap(),
                    12,
                )
                .with_node_eq_cardinality(
                    ScopedPropertyKey::try_new("User", "tenant_id").unwrap(),
                    3,
                ),
            ..PlannerContext::default()
        },
    );
    let plans = node_intersect(node_access(&plan));

    assert!(matches!(
        plans[0].as_ref(),
        NodeAccessPlan::EqualityIndex { key, .. } if key.property == "tenant_id"
    ));
    assert!(matches!(
        plans[1].as_ref(),
        NodeAccessPlan::EqualityIndex { key, .. } if key.property == "username"
    ));
    assert!(matches!(
        plans[2].as_ref(),
        NodeAccessPlan::RangeIndex { key, .. } if key.property == "age"
    ));
    assert!(matches!(
        plans[3].as_ref(),
        NodeAccessPlan::EqualityIndex { key, .. } if key.property == "unestimated"
    ));
    assert_no_node_label_scan_source(plans);
    assert_decision(
        &plan,
        TracePass::CardinalityOrder,
        TraceDecision::NodeIntersect,
    );
}

#[test]
fn edge_intersections_scan_lowest_cardinality_index_first() {
    let plan = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::lt("weight", 50),
                Predicate::eq("status", "active"),
                Predicate::eq("tenant_id", "acme"),
            ]),
        ),
        PlannerContext {
            indexes: builtin_label_indexes()
                .with_edge_range(
                    ScopedPropertyDirectionKey::try_new(
                        "FOLLOWS",
                        "weight",
                        RangeIndexDirection::Asc,
                    )
                    .unwrap(),
                )
                .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
                .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "tenant_id").unwrap()),
            stats: StatsSnapshot::default()
                .with_edge_label_cardinality(NonEmptyString::new("FOLLOWS").unwrap(), 1_000_000)
                .with_edge_range_cardinality(
                    ScopedPropertyDirectionKey::try_new(
                        "FOLLOWS",
                        "weight",
                        RangeIndexDirection::Asc,
                    )
                    .unwrap(),
                    4_000,
                )
                .with_edge_eq_cardinality(
                    ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
                    80_000,
                )
                .with_edge_eq_cardinality(
                    ScopedPropertyKey::try_new("FOLLOWS", "tenant_id").unwrap(),
                    40,
                ),
            ..PlannerContext::default()
        },
    );
    let plans = edge_intersect(edge_access(&plan));

    assert!(matches!(
        plans[0].as_ref(),
        EdgeAccessPlan::EqualityIndex { key, .. } if key.property == "tenant_id"
    ));
    assert!(matches!(
        plans[1].as_ref(),
        EdgeAccessPlan::RangeIndex { key, .. } if key.property == "weight"
    ));
    assert!(matches!(
        plans[2].as_ref(),
        EdgeAccessPlan::EqualityIndex { key, .. } if key.property == "status"
    ));
    assert_no_edge_label_scan_source(plans);
    assert_decision(
        &plan,
        TracePass::CardinalityOrder,
        TraceDecision::EdgeIntersect,
    );
}

#[test]
fn nested_node_and_intersections_are_flattened_before_cardinality_ordering() {
    let plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::and(vec![
                    Predicate::eq("username", "alice"),
                    Predicate::eq("tenant_id", "acme"),
                ]),
                Predicate::gte("age", 21),
            ]),
        ),
        PlannerContext {
            indexes: builtin_label_indexes()
                .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
                .with_node_eq(ScopedPropertyKey::try_new("User", "tenant_id").unwrap())
                .with_node_range(
                    ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                        .unwrap(),
                ),
            stats: StatsSnapshot::default()
                .with_node_label_cardinality(NonEmptyString::new("User").unwrap(), 50_000)
                .with_node_eq_cardinality(
                    ScopedPropertyKey::try_new("User", "username").unwrap(),
                    12,
                )
                .with_node_eq_cardinality(
                    ScopedPropertyKey::try_new("User", "tenant_id").unwrap(),
                    3,
                )
                .with_node_range_cardinality(
                    ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                        .unwrap(),
                    900,
                ),
            ..PlannerContext::default()
        },
    );
    let plans = node_intersect(node_access(&plan));

    assert_eq!(plans.len(), 3, "expected flattened intersection: {plans:?}");
    assert!(
        plans
            .iter()
            .all(|plan| !matches!(plan.as_ref(), NodeAccessPlan::Intersect(_))),
        "nested node intersection was not flattened: {plans:?}"
    );
    assert!(matches!(
        plans[0].as_ref(),
        NodeAccessPlan::EqualityIndex { key, .. } if key.property == "tenant_id"
    ));
    assert!(matches!(
        plans[1].as_ref(),
        NodeAccessPlan::EqualityIndex { key, .. } if key.property == "username"
    ));
    assert!(matches!(
        plans[2].as_ref(),
        NodeAccessPlan::RangeIndex { key, .. } if key.property == "age"
    ));
    assert_no_node_label_scan_source(plans);
    assert!(plan.trace.events.iter().any(|event| {
        event.pass == TracePass::AccessPath
            && event.decision == TraceDecision::NodeIntersect
            && event.reason == TraceReason::NestedAndIndexedAtoms
    }));
}

#[test]
fn nested_edge_and_intersections_are_flattened_before_cardinality_ordering() {
    let plan = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::and(vec![
                    Predicate::eq("status", "active"),
                    Predicate::eq("tenant_id", "acme"),
                ]),
                Predicate::lt("weight", 50),
            ]),
        ),
        PlannerContext {
            indexes: builtin_label_indexes()
                .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
                .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "tenant_id").unwrap())
                .with_edge_range(
                    ScopedPropertyDirectionKey::try_new(
                        "FOLLOWS",
                        "weight",
                        RangeIndexDirection::Asc,
                    )
                    .unwrap(),
                ),
            stats: StatsSnapshot::default()
                .with_edge_label_cardinality(NonEmptyString::new("FOLLOWS").unwrap(), 1_000_000)
                .with_edge_eq_cardinality(
                    ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
                    80_000,
                )
                .with_edge_eq_cardinality(
                    ScopedPropertyKey::try_new("FOLLOWS", "tenant_id").unwrap(),
                    40,
                )
                .with_edge_range_cardinality(
                    ScopedPropertyDirectionKey::try_new(
                        "FOLLOWS",
                        "weight",
                        RangeIndexDirection::Asc,
                    )
                    .unwrap(),
                    4_000,
                ),
            ..PlannerContext::default()
        },
    );
    let plans = edge_intersect(edge_access(&plan));

    assert_eq!(plans.len(), 3, "expected flattened intersection: {plans:?}");
    assert!(
        plans
            .iter()
            .all(|plan| !matches!(plan.as_ref(), EdgeAccessPlan::Intersect(_))),
        "nested edge intersection was not flattened: {plans:?}"
    );
    assert!(matches!(
        plans[0].as_ref(),
        EdgeAccessPlan::EqualityIndex { key, .. } if key.property == "tenant_id"
    ));
    assert!(matches!(
        plans[1].as_ref(),
        EdgeAccessPlan::RangeIndex { key, .. } if key.property == "weight"
    ));
    assert!(matches!(
        plans[2].as_ref(),
        EdgeAccessPlan::EqualityIndex { key, .. } if key.property == "status"
    ));
    assert_no_edge_label_scan_source(plans);
    assert!(plan.trace.events.iter().any(|event| {
        event.pass == TracePass::AccessPath
            && event.decision == TraceDecision::EdgeIntersect
            && event.reason == TraceReason::NestedAndIndexedAtoms
    }));
}

#[test]
fn node_intersections_order_nested_union_by_combined_cardinality() {
    let plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::or(vec![
                    Predicate::eq("username", "alice"),
                    Predicate::eq("email", "alice@example.com"),
                ]),
                Predicate::eq("tenant_id", "acme"),
            ]),
        ),
        PlannerContext {
            indexes: builtin_label_indexes()
                .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
                .with_node_eq(ScopedPropertyKey::try_new("User", "email").unwrap())
                .with_node_eq(ScopedPropertyKey::try_new("User", "tenant_id").unwrap()),
            stats: StatsSnapshot::default()
                .with_node_label_cardinality(NonEmptyString::new("User").unwrap(), 50_000)
                .with_node_eq_cardinality(
                    ScopedPropertyKey::try_new("User", "username").unwrap(),
                    12,
                )
                .with_node_eq_cardinality(ScopedPropertyKey::try_new("User", "email").unwrap(), 1)
                .with_node_eq_cardinality(
                    ScopedPropertyKey::try_new("User", "tenant_id").unwrap(),
                    3,
                ),
            ..PlannerContext::default()
        },
    );
    let plans = node_intersect(node_access(&plan));

    assert_eq!(plans.len(), 2);
    assert!(matches!(
        plans[0].as_ref(),
        NodeAccessPlan::EqualityIndex { key, .. } if key.property == "tenant_id"
    ));
    let NodeAccessPlan::Union(union) = plans[1].as_ref() else {
        panic!("expected node union second: {plans:?}");
    };
    assert_eq!(node_branch_properties(&union[0]), ["email"]);
    assert_eq!(node_branch_properties(&union[1]), ["username"]);
    assert_no_node_label_scan_source(plans);
    assert_decision(
        &plan,
        TracePass::CardinalityOrder,
        TraceDecision::NodeIntersect,
    );
    assert_decision(&plan, TracePass::CardinalityOrder, TraceDecision::NodeUnion);
}

#[test]
fn edge_intersections_order_nested_union_by_combined_cardinality() {
    let plan = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::or(vec![
                    Predicate::eq("status", "active"),
                    Predicate::gte("since", 2020),
                ]),
                Predicate::eq("tenant_id", "acme"),
            ]),
        ),
        PlannerContext {
            indexes: builtin_label_indexes()
                .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
                .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "tenant_id").unwrap())
                .with_edge_range(
                    ScopedPropertyDirectionKey::try_new(
                        "FOLLOWS",
                        "since",
                        RangeIndexDirection::Asc,
                    )
                    .unwrap(),
                ),
            stats: StatsSnapshot::default()
                .with_edge_label_cardinality(NonEmptyString::new("FOLLOWS").unwrap(), 1_000_000)
                .with_edge_eq_cardinality(
                    ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
                    900,
                )
                .with_edge_eq_cardinality(
                    ScopedPropertyKey::try_new("FOLLOWS", "tenant_id").unwrap(),
                    30,
                )
                .with_edge_range_cardinality(
                    ScopedPropertyDirectionKey::try_new(
                        "FOLLOWS",
                        "since",
                        RangeIndexDirection::Asc,
                    )
                    .unwrap(),
                    5_000,
                ),
            ..PlannerContext::default()
        },
    );
    let plans = edge_intersect(edge_access(&plan));

    assert_eq!(plans.len(), 2);
    assert!(matches!(
        plans[0].as_ref(),
        EdgeAccessPlan::EqualityIndex { key, .. } if key.property == "tenant_id"
    ));
    let EdgeAccessPlan::Union(union) = plans[1].as_ref() else {
        panic!("expected edge union second: {plans:?}");
    };
    assert_eq!(edge_branch_properties(&union[0]), ["status"]);
    assert_eq!(edge_branch_properties(&union[1]), ["since"]);
    assert_no_edge_label_scan_source(plans);
    assert_decision(
        &plan,
        TracePass::CardinalityOrder,
        TraceDecision::EdgeIntersect,
    );
    assert_decision(&plan, TracePass::CardinalityOrder, TraceDecision::EdgeUnion);
}

#[test]
fn wide_node_intersection_stress_orders_all_indexed_candidates() {
    let mut indexes = builtin_label_indexes();
    let mut stats = StatsSnapshot::default()
        .with_node_label_cardinality(NonEmptyString::new("User").unwrap(), 50_000);
    let mut predicates = Vec::new();

    for index in 0..32 {
        let property = format!("field_{index:02}");
        let key = ScopedPropertyKey::try_new("User", property.clone()).unwrap();
        indexes = indexes.with_node_eq(key.clone());
        stats = stats.with_node_eq_cardinality(key, (32 - index) as u64);
        predicates.push(Predicate::eq(property, format!("value_{index:02}")));
    }

    let plan = plan_traversal(
        g().n_with_label_where("User", Predicate::and(predicates)),
        PlannerContext {
            indexes,
            stats,
            ..PlannerContext::default()
        },
    );
    let plans = node_intersect(node_access(&plan));
    let ordered_properties = plans
        .iter()
        .filter_map(|plan| match plan.as_ref() {
            NodeAccessPlan::EqualityIndex { key, .. } => Some(key.property.as_ref()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let expected = (0..32)
        .rev()
        .map(|index| format!("field_{index:02}"))
        .collect::<Vec<_>>();

    assert_eq!(plans.len(), 32, "expected all index candidates");
    assert_eq!(
        ordered_properties,
        expected.iter().map(String::as_str).collect::<Vec<_>>()
    );
    assert_no_node_label_scan_source(plans);
    assert_decision(
        &plan,
        TracePass::CardinalityOrder,
        TraceDecision::NodeIntersect,
    );
}

#[test]
fn wide_edge_union_stress_orders_all_indexed_branches() {
    let mut indexes = builtin_label_indexes();
    let mut stats = StatsSnapshot::default();
    let mut predicates = Vec::new();

    for index in 0..32 {
        let property = format!("field_{index:02}");
        let key = ScopedPropertyKey::try_new("FOLLOWS", property.clone()).unwrap();
        indexes = indexes.with_edge_eq(key.clone());
        stats = stats.with_edge_eq_cardinality(key, (32 - index) as u64);
        predicates.push(Predicate::and(vec![
            Predicate::eq("$label", "FOLLOWS"),
            Predicate::eq(property, format!("value_{index:02}")),
        ]));
    }

    let plan = plan_traversal(
        g().e_where(Predicate::or(predicates)),
        PlannerContext {
            indexes,
            stats,
            ..PlannerContext::default()
        },
    );
    let EdgeAccessPlan::Union(branches) = edge_access(&plan) else {
        panic!("expected edge union: {:?}", edge_access(&plan));
    };
    let ordered_properties = branches
        .as_ref()
        .iter()
        .filter_map(|plan| match plan.as_ref() {
            EdgeAccessPlan::EqualityIndex { key, .. } => Some(key.property.as_ref()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let expected = (0..32)
        .rev()
        .map(|index| format!("field_{index:02}"))
        .collect::<Vec<_>>();

    assert_eq!(branches.as_ref().len(), 32);
    assert_eq!(
        ordered_properties,
        expected.iter().map(String::as_str).collect::<Vec<_>>()
    );
    assert_decision(&plan, TracePass::CardinalityOrder, TraceDecision::EdgeUnion);
}

#[test]
fn node_intersection_cardinality_order_is_independent_of_predicate_order() {
    let candidates = ["slow", "fast", "medium", "unique", "unestimated"];
    let expected = ["unique", "fast", "medium", "slow", "unestimated"];
    let mut indexes = builtin_label_indexes();
    let mut stats = StatsSnapshot::default()
        .with_node_label_cardinality(NonEmptyString::new("User").unwrap(), 50_000);

    for (property, cardinality) in [
        ("slow", Some(300)),
        ("fast", Some(10)),
        ("medium", Some(50)),
        ("unique", Some(1)),
        ("unestimated", None),
    ] {
        let key = ScopedPropertyKey::try_new("User", property).unwrap();
        indexes = indexes.with_node_eq(key.clone());
        if let Some(cardinality) = cardinality {
            stats = stats.with_node_eq_cardinality(key, cardinality);
        }
    }

    for_each_permutation(&candidates, |permutation| {
        let plan = plan_traversal(
            g().n_with_label_where(
                "User",
                Predicate::and(
                    permutation
                        .iter()
                        .map(|property| Predicate::eq(*property, format!("value:{property}")))
                        .collect(),
                ),
            ),
            PlannerContext {
                indexes: indexes.clone(),
                stats: stats.clone(),
                ..PlannerContext::default()
            },
        );
        let plans = node_intersect(node_access(&plan));
        let ordered_properties = plans
            .iter()
            .map(|plan| match plan.as_ref() {
                NodeAccessPlan::EqualityIndex { key, .. } => key.property.as_ref(),
                other => panic!("expected node equality index, got {other:?}"),
            })
            .collect::<Vec<_>>();

        assert_eq!(ordered_properties, expected, "permutation: {permutation:?}");
        assert_no_node_label_scan_source(plans);
        assert_decision(
            &plan,
            TracePass::CardinalityOrder,
            TraceDecision::NodeIntersect,
        );
    });
}

#[test]
fn edge_union_cardinality_order_is_independent_of_branch_order() {
    let candidates = ["slow", "fast", "medium", "unique", "unestimated"];
    let expected = ["unique", "fast", "medium", "slow", "unestimated"];
    let mut indexes = builtin_label_indexes();
    let mut stats = StatsSnapshot::default();

    for (property, cardinality) in [
        ("slow", Some(300)),
        ("fast", Some(10)),
        ("medium", Some(50)),
        ("unique", Some(1)),
        ("unestimated", None),
    ] {
        let key = ScopedPropertyKey::try_new("FOLLOWS", property).unwrap();
        indexes = indexes.with_edge_eq(key.clone());
        if let Some(cardinality) = cardinality {
            stats = stats.with_edge_eq_cardinality(key, cardinality);
        }
    }

    for_each_permutation(&candidates, |permutation| {
        let plan = plan_traversal(
            g().e_where(Predicate::or(
                permutation
                    .iter()
                    .map(|property| {
                        Predicate::and(vec![
                            Predicate::eq("$label", "FOLLOWS"),
                            Predicate::eq(*property, format!("value:{property}")),
                        ])
                    })
                    .collect(),
            )),
            PlannerContext {
                indexes: indexes.clone(),
                stats: stats.clone(),
                ..PlannerContext::default()
            },
        );
        let EdgeAccessPlan::Union(branches) = edge_access(&plan) else {
            panic!("expected edge union: {:?}", edge_access(&plan));
        };
        let ordered_properties = branches
            .iter()
            .map(|plan| match plan.as_ref() {
                EdgeAccessPlan::EqualityIndex { key, .. } => key.property.as_ref(),
                other => panic!("expected edge equality index, got {other:?}"),
            })
            .collect::<Vec<_>>();

        assert_eq!(ordered_properties, expected, "permutation: {permutation:?}");
        assert_decision(&plan, TracePass::CardinalityOrder, TraceDecision::EdgeUnion);
    });
}

#[test]
fn node_unions_order_index_branches_by_cardinality() {
    let predicate = Predicate::or(vec![
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::gte("age", 21),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::eq("username", "alice"),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::eq("email", "alice@example.com"),
        ]),
    ]);
    let plan = plan_traversal(
        g().n_where(predicate),
        PlannerContext {
            indexes: builtin_label_indexes()
                .with_node_range(
                    ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                        .unwrap(),
                )
                .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
                .with_node_eq(ScopedPropertyKey::try_new("User", "email").unwrap()),
            stats: StatsSnapshot::default()
                .with_node_range_cardinality(
                    ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                        .unwrap(),
                    600,
                )
                .with_node_eq_cardinality(
                    ScopedPropertyKey::try_new("User", "username").unwrap(),
                    4,
                )
                .with_node_eq_cardinality(ScopedPropertyKey::try_new("User", "email").unwrap(), 1),
            ..PlannerContext::default()
        },
    );
    let NodeAccessPlan::Union(branches) = node_access(&plan) else {
        panic!("expected node union: {:?}", node_access(&plan));
    };

    assert!(matches!(
        branches[0].as_ref(),
        NodeAccessPlan::EqualityIndex { key, .. } if key.property == "email"
    ));
    assert!(matches!(
        branches[1].as_ref(),
        NodeAccessPlan::EqualityIndex { key, .. } if key.property == "username"
    ));
    assert!(matches!(
        branches[2].as_ref(),
        NodeAccessPlan::RangeIndex { key, .. } if key.property == "age"
    ));
    assert_decision(&plan, TracePass::CardinalityOrder, TraceDecision::NodeUnion);
}

#[test]
fn edge_unions_order_index_branches_by_cardinality() {
    let predicate = Predicate::or(vec![
        Predicate::and(vec![
            Predicate::eq("$label", "FOLLOWS"),
            Predicate::gte("since", 2020),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "FOLLOWS"),
            Predicate::eq("status", "active"),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "FOLLOWS"),
            Predicate::eq("tenant_id", "acme"),
        ]),
    ]);
    let plan = plan_traversal(
        g().e_where(predicate),
        PlannerContext {
            indexes: builtin_label_indexes()
                .with_edge_range(
                    ScopedPropertyDirectionKey::try_new(
                        "FOLLOWS",
                        "since",
                        RangeIndexDirection::Asc,
                    )
                    .unwrap(),
                )
                .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
                .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "tenant_id").unwrap()),
            stats: StatsSnapshot::default()
                .with_edge_range_cardinality(
                    ScopedPropertyDirectionKey::try_new(
                        "FOLLOWS",
                        "since",
                        RangeIndexDirection::Asc,
                    )
                    .unwrap(),
                    5_000,
                )
                .with_edge_eq_cardinality(
                    ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
                    900,
                )
                .with_edge_eq_cardinality(
                    ScopedPropertyKey::try_new("FOLLOWS", "tenant_id").unwrap(),
                    30,
                ),
            ..PlannerContext::default()
        },
    );
    let EdgeAccessPlan::Union(branches) = edge_access(&plan) else {
        panic!("expected edge union: {:?}", edge_access(&plan));
    };

    assert!(matches!(
        branches[0].as_ref(),
        EdgeAccessPlan::EqualityIndex { key, .. } if key.property == "tenant_id"
    ));
    assert!(matches!(
        branches[1].as_ref(),
        EdgeAccessPlan::EqualityIndex { key, .. } if key.property == "status"
    ));
    assert!(matches!(
        branches[2].as_ref(),
        EdgeAccessPlan::RangeIndex { key, .. } if key.property == "since"
    ));
    assert_decision(&plan, TracePass::CardinalityOrder, TraceDecision::EdgeUnion);
}

#[test]
fn node_unions_order_composite_branches_by_effective_cardinality() {
    let email = ScopedPropertyKey::try_new("User", "email").unwrap();
    let tenant_id = ScopedPropertyKey::try_new("User", "tenant_id").unwrap();
    let username = ScopedPropertyKey::try_new("User", "username").unwrap();
    let unestimated = ScopedPropertyKey::try_new("User", "unestimated").unwrap();
    let age = ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap();
    let score =
        ScopedPropertyDirectionKey::try_new("User", "score", RangeIndexDirection::Asc).unwrap();
    let mut indexes = builtin_label_indexes()
        .with_node_eq(username.clone())
        .with_node_eq(tenant_id.clone())
        .with_node_eq(unestimated)
        .with_node_range(age.clone())
        .with_node_range(score.clone());
    indexes.node_eq.insert(
        email.clone(),
        NodeEqualityIndexMeta::new(NonEmptyString::new("node_eq:User:email").unwrap())
            .with_uniqueness(IndexUniqueness::Unique),
    );
    let predicate = Predicate::or(vec![
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::eq("username", "alice"),
            Predicate::gte("age", 21),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::eq("email", "alice@example.com"),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::eq("tenant_id", "acme"),
            Predicate::gte("score", 900),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::eq("unestimated", "kept-last"),
        ]),
    ]);
    let plan = plan_traversal(
        g().n_where(predicate),
        PlannerContext {
            indexes,
            stats: StatsSnapshot::default()
                .with_node_eq_cardinality(username, 90)
                .with_node_eq_cardinality(tenant_id, 3)
                .with_node_range_cardinality(age, 50)
                .with_node_range_cardinality(score, 20),
            ..PlannerContext::default()
        },
    );
    let NodeAccessPlan::Union(branches) = node_access(&plan) else {
        panic!("expected node union: {:?}", node_access(&plan));
    };

    assert_eq!(branches.len(), 4);
    assert_eq!(node_branch_properties(&branches[0]), ["email"]);
    assert_eq!(node_branch_properties(&branches[1]), ["tenant_id", "score"]);
    assert_eq!(node_branch_properties(&branches[2]), ["age", "username"]);
    assert_eq!(node_branch_properties(&branches[3]), ["unestimated"]);
    assert_decision(&plan, TracePass::CardinalityOrder, TraceDecision::NodeUnion);
    assert_decision(
        &plan,
        TracePass::CardinalityOrder,
        TraceDecision::NodeIntersect,
    );
}

#[test]
fn edge_unions_order_composite_branches_by_effective_cardinality() {
    let status = ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap();
    let tenant_id = ScopedPropertyKey::try_new("FOLLOWS", "tenant_id").unwrap();
    let unestimated = ScopedPropertyKey::try_new("FOLLOWS", "unestimated").unwrap();
    let since =
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "since", RangeIndexDirection::Asc).unwrap();
    let weight =
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap();
    let predicate = Predicate::or(vec![
        Predicate::and(vec![
            Predicate::eq("$label", "FOLLOWS"),
            Predicate::eq("status", "active"),
            Predicate::gte("since", 2020),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "FOLLOWS"),
            Predicate::eq("tenant_id", "acme"),
            Predicate::lt("weight", 50),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "FOLLOWS"),
            Predicate::eq("unestimated", "kept-last"),
        ]),
    ]);
    let plan = plan_traversal(
        g().e_where(predicate),
        PlannerContext {
            indexes: builtin_label_indexes()
                .with_edge_eq(status.clone())
                .with_edge_eq(tenant_id.clone())
                .with_edge_eq(unestimated)
                .with_edge_range(since.clone())
                .with_edge_range(weight.clone()),
            stats: StatsSnapshot::default()
                .with_edge_eq_cardinality(status, 900)
                .with_edge_eq_cardinality(tenant_id, 30)
                .with_edge_range_cardinality(since, 5_000)
                .with_edge_range_cardinality(weight, 4_000),
            ..PlannerContext::default()
        },
    );
    let EdgeAccessPlan::Union(branches) = edge_access(&plan) else {
        panic!("expected edge union: {:?}", edge_access(&plan));
    };

    assert_eq!(branches.len(), 3);
    assert_eq!(
        edge_branch_properties(&branches[0]),
        ["tenant_id", "weight"]
    );
    assert_eq!(edge_branch_properties(&branches[1]), ["status", "since"]);
    assert_eq!(edge_branch_properties(&branches[2]), ["unestimated"]);
    assert_decision(&plan, TracePass::CardinalityOrder, TraceDecision::EdgeUnion);
    assert_decision(
        &plan,
        TracePass::CardinalityOrder,
        TraceDecision::EdgeIntersect,
    );
}

fn node_branch_properties(branch: &NodeAccessSourcePlan) -> Vec<&str> {
    match branch.as_ref() {
        NodeAccessPlan::EqualityIndex { key, .. } => vec![key.property.as_ref()],
        NodeAccessPlan::RangeIndex { key, .. } => vec![key.property.as_ref()],
        NodeAccessPlan::Intersect(plans) => plans
            .iter()
            .flat_map(|plan| node_branch_properties(plan))
            .collect(),
        other => panic!("expected node index branch, got {other:?}"),
    }
}

fn edge_branch_properties(branch: &EdgeAccessSourcePlan) -> Vec<&str> {
    match branch.as_ref() {
        EdgeAccessPlan::EqualityIndex { key, .. } => vec![key.property.as_ref()],
        EdgeAccessPlan::RangeIndex { key, .. } => vec![key.property.as_ref()],
        EdgeAccessPlan::Intersect(plans) => plans
            .iter()
            .flat_map(|plan| edge_branch_properties(plan))
            .collect(),
        other => panic!("expected edge index branch, got {other:?}"),
    }
}

fn for_each_permutation<T: Clone>(items: &[T], mut visit: impl FnMut(&[T])) {
    let mut items = items.to_vec();
    permute(&mut items, 0, &mut visit);
}

fn permute<T>(items: &mut [T], start: usize, visit: &mut impl FnMut(&[T])) {
    if start == items.len() {
        visit(items);
        return;
    }
    for index in start..items.len() {
        items.swap(start, index);
        permute(items, start + 1, visit);
        items.swap(start, index);
    }
}
