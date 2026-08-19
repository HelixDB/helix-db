use super::support::{
    diagnostics_for_ops, executable_plan, missing_index_insights, missing_indexes, name, plan,
    plan_batch, search_context, step,
};
use crate::{catalog, context, diagnostics, exec, ir, properties};
use helix_ast::batch::read_batch;
use helix_ast::expr::{CompareOp, Expr, Predicate};
use helix_ast::index::RangeIndexDirection;
use helix_ast::traversal::{g, Order};
use helix_ast::value::PropertyValue;

#[test]
fn restricted_vector_ranking_preserves_input_scope_for_diagnostics() {
    let output = plan(
        g().n_with_label("Doc")
            .vector_search("Doc", "embedding", vec![1.0, 0.0], 10, None)
            .where_(Predicate::eq("category", "science")),
        &search_context(),
    );

    let actual = missing_indexes(&output);
    assert_eq!(actual.len(), 1);
    assert_eq!(actual[0].element, catalog::ElementKind::Node);
    assert_eq!(actual[0].label.as_ref(), "Doc");
    assert_eq!(actual[0].property.as_ref(), "category");
    assert_eq!(
        actual[0].index_kind,
        diagnostics::SecondaryIndexKind::Equality
    );
    assert_eq!(output.diagnostics().statistics.total_operators, 3);
}

#[test]
fn equality_and_every_range_shape_are_typed_for_nodes_and_edges() {
    let ctx = context::PlannerContext {
        params: context::ParamBindings::default().with_value(name("needle"), 5),
        ..context::PlannerContext::default()
    };
    let cases = [
        (
            Predicate::eq("value", 5),
            diagnostics::SecondaryIndexKind::Equality,
        ),
        (
            Predicate::eq_param("value", "needle"),
            diagnostics::SecondaryIndexKind::Equality,
        ),
        (
            Predicate::gt("value", 5),
            diagnostics::SecondaryIndexKind::Range,
        ),
        (
            Predicate::gte("value", 5),
            diagnostics::SecondaryIndexKind::Range,
        ),
        (
            Predicate::lt("value", 5),
            diagnostics::SecondaryIndexKind::Range,
        ),
        (
            Predicate::lte("value", 5),
            diagnostics::SecondaryIndexKind::Range,
        ),
        (
            Predicate::between("value", 1, 9),
            diagnostics::SecondaryIndexKind::Range,
        ),
    ];

    for (predicate, expected_kind) in cases {
        let node = plan(g().n_with_label_where("User", predicate.clone()), &ctx);
        let edge = plan(g().e_with_label_where("FOLLOWS", predicate), &ctx);

        for (actual, expected_element, expected_label) in [
            (missing_indexes(&node), catalog::ElementKind::Node, "User"),
            (
                missing_indexes(&edge),
                catalog::ElementKind::Edge,
                "FOLLOWS",
            ),
        ] {
            assert_eq!(actual.len(), 1);
            assert_eq!(actual[0].element, expected_element);
            assert_eq!(actual[0].label.as_ref(), expected_label);
            assert_eq!(actual[0].property.as_ref(), "value");
            assert_eq!(actual[0].index_kind, expected_kind);
            assert_eq!(actual[0].occurrences, 1);
        }
    }
}

#[test]
fn reversed_comparisons_and_predicate_derived_labels_remain_actionable() {
    let reversed_equality =
        Predicate::compare(Expr::val("alice"), CompareOp::Eq, Expr::prop("username"));
    let reversed_range = Predicate::compare(Expr::val(21), CompareOp::Lt, Expr::prop("age"));
    let equality = plan(
        g().n_with_label_where("User", reversed_equality),
        &context::PlannerContext::default(),
    );
    let range = plan(
        g().n_with_label_where("User", reversed_range),
        &context::PlannerContext::default(),
    );
    assert_eq!(
        missing_indexes(&equality)[0].index_kind,
        diagnostics::SecondaryIndexKind::Equality
    );
    assert_eq!(
        missing_indexes(&range)[0].index_kind,
        diagnostics::SecondaryIndexKind::Range
    );

    let label_index = catalog::ScopedPropertyKey::try_new("User", "$label").unwrap();
    let ctx = context::PlannerContext {
        indexes: catalog::IndexCatalogSnapshot::default().with_node_eq(label_index),
        ..context::PlannerContext::default()
    };
    let predicate_label = plan(
        g().n_where(Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::eq("username", "alice"),
        ])),
        &ctx,
    );
    let actual = missing_indexes(&predicate_label);
    assert_eq!(actual.len(), 1);
    assert_eq!(actual[0].label.as_ref(), "User");
    assert_eq!(actual[0].property.as_ref(), "username");
}

#[test]
fn matching_catalog_indexes_suppress_recommendations_in_both_range_directions() {
    let equality_key = catalog::ScopedPropertyKey::try_new("User", "username").unwrap();
    let equality_ctx = context::PlannerContext {
        indexes: catalog::IndexCatalogSnapshot::default().with_node_eq(equality_key),
        ..context::PlannerContext::default()
    };
    assert!(missing_indexes(&plan(
        g().n_with_label_where("User", Predicate::eq("username", "alice")),
        &equality_ctx,
    ))
    .is_empty());

    for direction in [RangeIndexDirection::Asc, RangeIndexDirection::Desc] {
        let range_key =
            catalog::ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", direction).unwrap();
        let range_ctx = context::PlannerContext {
            indexes: catalog::IndexCatalogSnapshot::default().with_edge_range(range_key),
            ..context::PlannerContext::default()
        };
        assert!(missing_indexes(&plan(
            g().e_with_label_where("FOLLOWS", Predicate::gte("weight", 10)),
            &range_ctx,
        ))
        .is_empty());
    }
}

#[test]
fn wrong_kind_element_label_or_property_does_not_suppress_a_recommendation() {
    let indexes = catalog::IndexCatalogSnapshot::default()
        .with_node_range(
            catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "username",
                RangeIndexDirection::Asc,
            )
            .unwrap(),
        )
        .with_edge_eq(catalog::ScopedPropertyKey::try_new("User", "username").unwrap())
        .with_node_eq(catalog::ScopedPropertyKey::try_new("Account", "username").unwrap())
        .with_node_eq(catalog::ScopedPropertyKey::try_new("User", "email").unwrap());
    let ctx = context::PlannerContext {
        indexes,
        ..context::PlannerContext::default()
    };
    let output = plan(
        g().n_with_label_where("User", Predicate::eq("username", "alice")),
        &ctx,
    );

    assert_eq!(missing_indexes(&output).len(), 1);
    assert_eq!(
        missing_indexes(&output)[0].index_kind,
        diagnostics::SecondaryIndexKind::Equality
    );
}

#[test]
fn and_and_or_recommend_only_indexes_that_can_change_the_selected_scan() {
    let tenant = catalog::ScopedPropertyKey::try_new("User", "tenant").unwrap();
    let ctx = context::PlannerContext {
        indexes: catalog::IndexCatalogSnapshot::default().with_node_eq(tenant),
        ..context::PlannerContext::default()
    };
    let partial_and = plan(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::eq("tenant", "acme"),
                Predicate::gte("age", 21),
                Predicate::contains("bio", "rust"),
            ]),
        ),
        &ctx,
    );
    assert_eq!(
        missing_indexes(&partial_and)
            .iter()
            .map(|insight| (insight.property.as_ref(), insight.index_kind))
            .collect::<Vec<_>>(),
        vec![("age", diagnostics::SecondaryIndexKind::Range)]
    );

    let complete_or = plan(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::eq("username", "alice"),
                Predicate::eq("email", "alice@example.com"),
            ]),
        ),
        &context::PlannerContext::default(),
    );
    assert_eq!(
        missing_indexes(&complete_or)
            .iter()
            .map(|insight| insight.property.as_ref())
            .collect::<Vec<_>>(),
        vec!["email", "username"]
    );
    let one_existing_ctx = context::PlannerContext {
        indexes: catalog::IndexCatalogSnapshot::default()
            .with_node_eq(catalog::ScopedPropertyKey::try_new("User", "username").unwrap()),
        ..context::PlannerContext::default()
    };
    let one_existing = plan(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::eq("username", "alice"),
                Predicate::eq("email", "alice@example.com"),
            ]),
        ),
        &one_existing_ctx,
    );
    assert_eq!(
        missing_indexes(&one_existing)
            .iter()
            .map(|insight| insight.property.as_ref())
            .collect::<Vec<_>>(),
        vec!["email"]
    );

    let incomplete_or = plan(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::eq("age", 42),
                Predicate::contains("bio", "rust"),
            ]),
        ),
        &context::PlannerContext::default(),
    );
    assert!(missing_indexes(&incomplete_or).is_empty());
}

#[test]
fn disabled_or_branch_limits_suppress_non_actionable_union_recommendations() {
    let ctx = context::PlannerContext {
        limits: context::PlannerLimits {
            max_index_union_branches: context::IndexUnionBranchLimit::Disabled,
        },
        ..context::PlannerContext::default()
    };
    let output = plan(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::eq("username", "alice"),
                Predicate::eq("email", "alice@example.com"),
            ]),
        ),
        &ctx,
    );

    assert!(missing_indexes(&output).is_empty());
}

#[test]
fn unsupported_nested_null_property_comparison_and_impossible_labels_are_suppressed() {
    let predicates = [
        Predicate::eq("profile.age", 42),
        Predicate::contains("bio", "rust"),
        Predicate::eq("deleted_at", PropertyValue::Null),
        Predicate::compare(Expr::prop("left"), CompareOp::Eq, Expr::prop("right")),
        Predicate::compare(Expr::val(1), CompareOp::Eq, Expr::val(1)),
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::eq("$label", "Account"),
            Predicate::eq("username", "alice"),
        ]),
    ];

    for predicate in predicates {
        assert!(missing_indexes(&plan(
            g().n_with_label_where("User", predicate),
            &context::PlannerContext::default(),
        ))
        .is_empty());
    }
    assert!(missing_indexes(&plan(
        g().n_where(Predicate::eq("username", "alice")),
        &context::PlannerContext::default(),
    ))
    .is_empty());
    assert!(missing_indexes(&plan(
        g().n_with_label("User")
            .out(Some("FOLLOWS"))
            .where_(Predicate::eq("username", "alice")),
        &context::PlannerContext::default(),
    ))
    .is_empty());
}

#[test]
fn recommendations_are_deduplicated_counted_sorted_and_bounded() {
    let repeated = read_batch()
        .var_as(
            "one",
            g().n_with_label_where(
                "User",
                Predicate::and(vec![
                    Predicate::eq("username", "alice"),
                    Predicate::eq("username", "alice"),
                ]),
            ),
        )
        .var_as(
            "two",
            g().n_with_label_where("User", Predicate::eq("username", "bob")),
        );
    let repeated = plan_batch(&repeated, &context::PlannerContext::default());
    assert_eq!(missing_indexes(&repeated).len(), 1);
    assert_eq!(missing_indexes(&repeated)[0].occurrences, 2);

    let ordered = read_batch()
        .var_as("edge", g().e_with_label_where("ZED", Predicate::eq("z", 1)))
        .var_as(
            "node_range",
            g().n_with_label_where("Alpha", Predicate::gte("same", 1)),
        )
        .var_as(
            "node_eq",
            g().n_with_label_where("Alpha", Predicate::eq("same", 1)),
        );
    let ordered = plan_batch(&ordered, &context::PlannerContext::default());
    assert_eq!(
        missing_indexes(&ordered)
            .iter()
            .map(|insight| {
                (
                    insight.element,
                    insight.label.as_ref(),
                    insight.property.as_ref(),
                    insight.index_kind,
                )
            })
            .collect::<Vec<_>>(),
        vec![
            (
                catalog::ElementKind::Node,
                "Alpha",
                "same",
                diagnostics::SecondaryIndexKind::Equality,
            ),
            (
                catalog::ElementKind::Node,
                "Alpha",
                "same",
                diagnostics::SecondaryIndexKind::Range,
            ),
            (
                catalog::ElementKind::Edge,
                "ZED",
                "z",
                diagnostics::SecondaryIndexKind::Equality,
            ),
        ]
    );

    let bounded = diagnostics_for_ops((0..20).flat_map(|index| {
        [
            exec::ExecOp::Access {
                plan: Box::new(
                    exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::LabelScan {
                        label: name("User"),
                    })
                    .limited(properties::PositiveUsize::new(1).unwrap()),
                ),
            },
            exec::ExecOp::Filter {
                predicate: ir::PredicatePlan::new(Predicate::eq(
                    format!("property_{index:02}"),
                    i64::from(index),
                ))
                .unwrap(),
            },
        ]
    }));
    assert_eq!(bounded.insights.len(), diagnostics::MAX_PLANNER_INSIGHTS);
    assert_eq!(
        missing_index_insights(&bounded).len(),
        diagnostics::MAX_PLANNER_INSIGHTS
    );
    assert_eq!(
        missing_index_insights(&bounded)[0].property.as_ref(),
        "property_00"
    );
    assert_eq!(
        missing_index_insights(&bounded)[diagnostics::MAX_PLANNER_INSIGHTS - 1]
            .property
            .as_ref(),
        "property_15"
    );
}

#[test]
fn predicate_permutation_does_not_change_missing_index_order() {
    let predicates = [
        vec![Predicate::eq("zeta", 1), Predicate::gte("alpha", 1)],
        vec![Predicate::gte("alpha", 1), Predicate::eq("zeta", 1)],
    ];
    let outputs = predicates.map(|predicates| {
        plan(
            g().n_with_label_where("User", Predicate::and(predicates)),
            &context::PlannerContext::default(),
        )
    });
    let keys = outputs.map(|output| {
        missing_indexes(&output)
            .iter()
            .map(|insight| (insight.property.clone(), insight.index_kind))
            .collect::<Vec<_>>()
    });

    assert_eq!(keys[0], keys[1]);
}

#[test]
fn batch_permutation_does_not_change_missing_index_order_or_counts() {
    let first = read_batch()
        .var_as(
            "zeta",
            g().e_with_label_where("FOLLOWS", Predicate::gte("weight", 1)),
        )
        .var_as(
            "alpha",
            g().n_with_label_where("User", Predicate::eq("username", "alice")),
        );
    let second = read_batch()
        .var_as(
            "alpha",
            g().n_with_label_where("User", Predicate::eq("username", "alice")),
        )
        .var_as(
            "zeta",
            g().e_with_label_where("FOLLOWS", Predicate::gte("weight", 1)),
        );
    let outputs = [first, second].map(|batch| {
        plan_batch(&batch, &context::PlannerContext::default())
            .diagnostics()
            .insights
            .clone()
    });

    assert_eq!(outputs[0], outputs[1]);
}

#[test]
fn scope_resolution_preserves_actionability_through_unary_passthrough_steps() {
    let access = exec::ExecOp::Access {
        plan: Box::new(exec::ExecAccessPlan::Node(
            exec::ExecNodeAccessPlan::LabelScan {
                label: name("User"),
            },
        )),
    };
    let order = ir::OrderPlan::ExplicitSort(
        ir::OrderKey {
            property: name("name"),
            order: Order::Asc,
        }
        .into(),
    );
    let ops = [
        access,
        exec::ExecOp::Filter {
            predicate: ir::PredicatePlan::new(Predicate::contains("bio", "rust")).unwrap(),
        },
        exec::ExecOp::Limit {
            count: ir::StreamBoundPlan::Literal(10),
        },
        exec::ExecOp::Skip {
            count: ir::StreamBoundPlan::Literal(1),
        },
        exec::ExecOp::Range {
            range: ir::StreamRangePlan::Literal(ir::StreamLiteralRange::new(1, 8).unwrap()),
        },
        exec::ExecOp::Distinct,
        exec::ExecOp::Order { plan: order },
        exec::ExecOp::Reserved {
            op: ir::ReservedOp::Fold,
        },
        exec::ExecOp::Barrier {
            name: name("barrier"),
        },
        exec::ExecOp::Noop,
        exec::ExecOp::Filter {
            predicate: ir::PredicatePlan::new(Predicate::eq("username", "alice")).unwrap(),
        },
    ];
    let diagnostics = super::support::diagnostics_for_ops(ops);

    let indexes = missing_index_insights(&diagnostics);
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].label.as_ref(), "User");
    assert_eq!(indexes[0].property.as_ref(), "username");
    assert_eq!(diagnostics.statistics.residual_filters, 2);
}

#[test]
fn direct_access_scope_matrix_covers_all_limited_unsupported_and_mismatched_sources() {
    let label_indexes = catalog::IndexCatalogSnapshot::default()
        .with_node_eq(catalog::ScopedPropertyKey::try_new("User", "$label").unwrap())
        .with_edge_eq(catalog::ScopedPropertyKey::try_new("FOLLOWS", "$label").unwrap());
    let ctx = context::PlannerContext {
        indexes: label_indexes,
        ..context::PlannerContext::default()
    };
    let scoped = |label, property| {
        ir::PredicatePlan::new(Predicate::and(vec![
            Predicate::eq("$label", label),
            Predicate::eq(property, "value"),
        ]))
        .unwrap()
    };
    let equality = |property| ir::PredicatePlan::new(Predicate::eq(property, "value")).unwrap();
    let cases = [
        (
            exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::AllScan),
            scoped("User", "username"),
            Some((catalog::ElementKind::Node, "User", "username")),
        ),
        (
            exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::AllScan),
            scoped("FOLLOWS", "weight"),
            Some((catalog::ElementKind::Edge, "FOLLOWS", "weight")),
        ),
        (
            exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::LabelScan {
                label: name("User"),
            })
            .limited(crate::properties::PositiveUsize::at_least_one(5)),
            equality("username"),
            Some((catalog::ElementKind::Node, "User", "username")),
        ),
        (
            exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::Empty),
            equality("username"),
            None,
        ),
        (
            exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::LabelScan {
                label: name("User"),
            }),
            scoped("Account", "username"),
            None,
        ),
    ];

    for (access, predicate, expected) in cases {
        let diagnostics = super::support::diagnostics_for_ops_with(
            [
                exec::ExecOp::Access {
                    plan: Box::new(access),
                },
                exec::ExecOp::Filter { predicate },
            ],
            exec::PlannerMetrics::default(),
            &ctx,
        );
        let actual = missing_index_insights(&diagnostics);

        match expected {
            Some((element, label, property)) => {
                assert_eq!(actual.len(), 1);
                assert_eq!(actual[0].element, element);
                assert_eq!(actual[0].label.as_ref(), label);
                assert_eq!(actual[0].property.as_ref(), property);
            }
            None => assert!(actual.is_empty()),
        }
    }
}

#[test]
fn merge_scope_resolution_requires_matching_element_and_label_scopes() {
    let diagnostics_for =
        |left_element, left_label: &str, right_element, right_label: &str, mode| {
            let access = |element, label: &str| match element {
                catalog::ElementKind::Node => exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::LabelScan { label: name(label) },
                    )),
                },
                catalog::ElementKind::Edge => exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Edge(
                        exec::ExecEdgeAccessPlan::LabelScan { label: name(label) },
                    )),
                },
            };
            let one = exec::ExecStepId::new(1).unwrap();
            let two = exec::ExecStepId::new(2).unwrap();
            let three = exec::ExecStepId::new(3).unwrap();
            let steps = vec![
                step(1, Vec::new(), access(left_element, left_label)),
                step(2, Vec::new(), access(right_element, right_label)),
                step(3, vec![one, two], exec::ExecOp::Merge { mode }),
                step(
                    4,
                    vec![three],
                    exec::ExecOp::Filter {
                        predicate: ir::PredicatePlan::new(Predicate::eq("username", "alice"))
                            .unwrap(),
                    },
                ),
            ];
            let plan = executable_plan(steps, exec::PlannerMetrics::default());
            crate::diagnostics::analyze(&plan, &context::PlannerContext::default())
        };

    for mode in [exec::ExecMergeMode::Union, exec::ExecMergeMode::Intersect] {
        let matching = diagnostics_for(
            catalog::ElementKind::Node,
            "User",
            catalog::ElementKind::Node,
            "User",
            mode,
        );
        assert_eq!(missing_index_insights(&matching).len(), 1);

        let wrong_label = diagnostics_for(
            catalog::ElementKind::Node,
            "User",
            catalog::ElementKind::Node,
            "Account",
            mode,
        );
        assert!(missing_index_insights(&wrong_label).is_empty());

        let wrong_element = diagnostics_for(
            catalog::ElementKind::Node,
            "User",
            catalog::ElementKind::Edge,
            "User",
            mode,
        );
        assert!(missing_index_insights(&wrong_element).is_empty());
    }
}
