use super::support::{
    diagnostics_for_ops, diagnostics_for_ops_with, executable_plan, expand_op, linear_steps, name,
    plan, plan_batch, search_context, step, subplan, unbounded_scans,
};
use crate::{catalog, context, cost, diagnostics, exec, ir};
use helix_ast::batch::{read_batch, write_batch};
use helix_ast::expr::Predicate;
use helix_ast::graph::{EdgeRef, NodeRef};
use helix_ast::index::RangeIndexDirection;
use helix_ast::traversal::{g, Order};

#[test]
fn every_exported_planner_work_field_is_copied_exactly() {
    let metrics = exec::PlannerMetrics {
        memo_groups: 11,
        memo_exprs: 12,
        rule_fires: 13,
        rejected_alternatives: 14,
        alternatives_considered: 15,
        selected_cost: cost::CostVector {
            object_reads: 999,
            ..cost::CostVector::ZERO
        },
        optimization_micros: 16,
        guardrail_hit: true,
    };
    let diagnostics = diagnostics_for_ops_with(
        [exec::ExecOp::Noop],
        metrics,
        &context::PlannerContext::default(),
    );

    assert_eq!(
        diagnostics.statistics,
        diagnostics::PlannerStatistics {
            memo_groups: 11,
            memo_expressions: 12,
            rules_fired: 13,
            rejected_alternatives: 14,
            alternatives_considered: 15,
            optimization_micros: 16,
            guardrail_hit: true,
            total_operators: 1,
            maximum_operator_depth: 1,
            ..diagnostics::PlannerStatistics::default()
        }
    );
    let encoded = serde_json::to_string(&diagnostics).unwrap();
    assert!(!encoded.contains("selected_cost"));
    assert!(!encoded.contains("999"));
}

#[test]
fn selected_access_statistics_cover_every_node_and_edge_access_family() {
    let mut ctx = search_context();
    ctx.indexes = ctx
        .indexes
        .with_node_eq(catalog::ScopedPropertyKey::try_new("User", "username").unwrap())
        .with_node_range(
            catalog::ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                .unwrap(),
        )
        .with_edge_eq(catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
        .with_edge_range(
            catalog::ScopedPropertyDirectionKey::try_new(
                "FOLLOWS",
                "weight",
                RangeIndexDirection::Desc,
            )
            .unwrap(),
        );
    let batch = read_batch()
        .var_as("node_all", g().n(NodeRef::all()))
        .var_as("node_label", g().n_with_label("User"))
        .var_as("node_get", g().n(NodeRef::ids(vec![1])))
        .var_as("node_multi_get", g().n(NodeRef::ids(vec![2, 3])))
        .var_as(
            "node_equality",
            g().n_with_label_where("User", Predicate::eq("username", "alice")),
        )
        .var_as(
            "node_range",
            g().n_with_label_where("User", Predicate::gte("age", 21)),
        )
        .var_as(
            "node_vector",
            g().vector_search_nodes("Doc", "embedding", vec![0.1f32, 0.2], 3, None),
        )
        .var_as(
            "node_text",
            g().text_search_nodes("Doc", "body", "needle", 3, None),
        )
        .var_as("edge_all", g().e(EdgeRef::all()))
        .var_as("edge_label", g().e_with_label("FOLLOWS"))
        .var_as("edge_get", g().e(EdgeRef::ids(vec![4])))
        .var_as("edge_multi_get", g().e(EdgeRef::ids(vec![5, 6])))
        .var_as(
            "edge_equality",
            g().e_with_label_where("FOLLOWS", Predicate::eq("status", "active")),
        )
        .var_as(
            "edge_range",
            g().e_with_label_where("FOLLOWS", Predicate::gte("weight", 10)),
        )
        .var_as(
            "edge_vector",
            g().vector_search_edges("MENTIONS", "embedding", vec![0.3f32, 0.4], 3, None),
        )
        .var_as(
            "edge_text",
            g().text_search_edges("MENTIONS", "body", "needle", 3, None),
        );
    let output = plan_batch(&batch, &ctx);

    let expected = diagnostics::AccessStatistics {
        all_scans: 1,
        label_scans: 1,
        point_lookups: 2,
        equality_index_lookups: 1,
        range_index_scans: 1,
        reverse_range_index_scans: 0,
        dynamic_bounded_accesses: 0,
        vector_searches: 1,
        text_searches: 1,
        bounded_accesses: 0,
    };
    assert_eq!(output.diagnostics().statistics.node_accesses, expected);
    assert_eq!(output.diagnostics().statistics.edge_accesses, expected);
}

#[test]
fn empty_parameter_and_variable_native_accesses_do_not_invent_access_counts() {
    let diagnostics = diagnostics_for_ops([
        exec::ExecOp::Access {
            plan: Box::new(exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::Empty)),
        },
        exec::ExecOp::Access {
            plan: Box::new(exec::ExecAccessPlan::Node(
                exec::ExecNodeAccessPlan::FromParam {
                    param: name("node_ids"),
                },
            )),
        },
        exec::ExecOp::Access {
            plan: Box::new(exec::ExecAccessPlan::Node(
                exec::ExecNodeAccessPlan::FromVar {
                    variable: name("nodes"),
                },
            )),
        },
        exec::ExecOp::Access {
            plan: Box::new(exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::Empty)),
        },
        exec::ExecOp::Access {
            plan: Box::new(exec::ExecAccessPlan::Edge(
                exec::ExecEdgeAccessPlan::FromParam {
                    param: name("edge_ids"),
                },
            )),
        },
        exec::ExecOp::Access {
            plan: Box::new(exec::ExecAccessPlan::Edge(
                exec::ExecEdgeAccessPlan::FromVar {
                    variable: name("edges"),
                },
            )),
        },
    ]);

    assert_eq!(
        diagnostics.statistics.node_accesses,
        diagnostics::AccessStatistics::default()
    );
    assert_eq!(
        diagnostics.statistics.edge_accesses,
        diagnostics::AccessStatistics::default()
    );
    assert!(diagnostics.insights.is_empty());
    assert_eq!(diagnostics.statistics.total_operators, 6);
}

#[test]
fn native_all_scans_and_nested_serialized_limits_keep_exact_bounds() {
    let diagnostics = diagnostics_for_ops([
        exec::ExecOp::Access {
            plan: Box::new(exec::ExecAccessPlan::Node(
                exec::ExecNodeAccessPlan::AllScan,
            )),
        },
        exec::ExecOp::Access {
            plan: Box::new(exec::ExecAccessPlan::Edge(
                exec::ExecEdgeAccessPlan::AllScan,
            )),
        },
    ]);
    assert_eq!(diagnostics.statistics.node_accesses.all_scans, 1);
    assert_eq!(diagnostics.statistics.edge_accesses.all_scans, 1);
    assert_eq!(unbounded_scans(&diagnostics).len(), 2);

    // Construction flattens nested limits, but the public serde shape can still
    // contain one. Exercise the analyzer's defensive recursive arm explicitly.
    let nested = serde_json::from_value(serde_json::json!({
        "limited": {
            "source": {
                "limited": {
                    "source": { "node": "all_scan" },
                    "limit": { "static": 2 }
                }
            },
            "limit": { "static": 3 }
        }
    }))
    .unwrap();
    let diagnostics = diagnostics_for_ops([exec::ExecOp::Access {
        plan: Box::new(nested),
    }]);
    assert_eq!(diagnostics.statistics.node_accesses.all_scans, 1);
    assert_eq!(diagnostics.statistics.node_accesses.bounded_accesses, 1);
    assert!(unbounded_scans(&diagnostics).is_empty());
}

#[test]
fn a_filter_without_a_selected_input_is_counted_but_not_diagnosed() {
    let diagnostics = diagnostics_for_ops([exec::ExecOp::Filter {
        predicate: ir::PredicatePlan::new(Predicate::eq("username", "alice")).unwrap(),
    }]);

    assert_eq!(diagnostics.statistics.residual_filters, 1);
    assert!(diagnostics.insights.is_empty());
}

#[test]
fn bounded_and_unbounded_kv_scans_are_counted_per_element() {
    let diagnostics = diagnostics_for_ops([
        exec::ExecOp::KvRead(exec::KvReadPlan::RangeScan {
            keyspace: exec::ElementKeyspace::NodeProperty,
            start: exec::KvKeyBound::Unbounded,
            end: exec::KvKeyBound::Unbounded,
            limit: None,
        }),
        exec::ExecOp::KvRead(exec::KvReadPlan::RangeScan {
            keyspace: exec::ElementKeyspace::EdgeEndpoints,
            start: exec::KvKeyBound::Unbounded,
            end: exec::KvKeyBound::Unbounded,
            limit: Some(crate::properties::PositiveUsize::new(5).unwrap()),
        }),
    ]);

    assert_eq!(diagnostics.statistics.node_accesses.all_scans, 1);
    assert_eq!(diagnostics.statistics.node_accesses.bounded_accesses, 0);
    assert_eq!(diagnostics.statistics.edge_accesses.all_scans, 1);
    assert_eq!(diagnostics.statistics.edge_accesses.bounded_accesses, 1);
    let scans = unbounded_scans(&diagnostics);
    assert_eq!(scans.len(), 1);
    assert_eq!(scans[0].element, catalog::ElementKind::Node);
}

#[test]
fn residual_filter_and_index_set_plans_count_selected_operators_exactly() {
    let residual = plan(
        g().n_with_label_where("User", Predicate::contains("bio", "rust")),
        &context::PlannerContext::default(),
    );
    assert_eq!(residual.diagnostics().statistics.residual_filters, 1);

    let union_ctx = context::PlannerContext {
        indexes: catalog::IndexCatalogSnapshot::default()
            .with_node_eq(catalog::ScopedPropertyKey::try_new("User", "username").unwrap())
            .with_node_eq(catalog::ScopedPropertyKey::try_new("User", "email").unwrap()),
        ..context::PlannerContext::default()
    };
    let union = plan(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::eq("username", "alice"),
                Predicate::eq("email", "alice@example.com"),
            ]),
        ),
        &union_ctx,
    );
    assert_eq!(union.diagnostics().statistics.unions, 1);
    assert_eq!(
        union
            .diagnostics()
            .statistics
            .node_accesses
            .equality_index_lookups,
        2
    );
    assert_eq!(union.diagnostics().statistics.residual_filters, 0);

    let intersect_ctx = context::PlannerContext {
        indexes: union_ctx.indexes.clone().with_node_range(
            catalog::ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                .unwrap(),
        ),
        ..context::PlannerContext::default()
    };
    let intersection = plan(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::eq("username", "alice"),
                Predicate::gte("age", 21),
            ]),
        ),
        &intersect_ctx,
    );
    assert_eq!(intersection.diagnostics().statistics.intersections, 1);
    assert_eq!(
        intersection
            .diagnostics()
            .statistics
            .node_accesses
            .equality_index_lookups,
        1
    );
    assert_eq!(
        intersection
            .diagnostics()
            .statistics
            .node_accesses
            .range_index_scans,
        1
    );
}

#[test]
fn stream_operator_counters_and_depth_are_exact_for_a_validated_linear_dag() {
    let predicate = ir::PredicatePlan::new(Predicate::eq("active", true)).unwrap();
    let order = ir::OrderPlan::ExplicitSort(
        ir::OrderKey {
            property: name("name"),
            order: Order::Asc,
        }
        .into(),
    );
    let diagnostics = diagnostics_for_ops([
        exec::ExecOp::Noop,
        exec::ExecOp::Filter { predicate },
        exec::ExecOp::Limit {
            count: ir::StreamBoundPlan::Literal(10),
        },
        exec::ExecOp::Skip {
            count: ir::StreamBoundPlan::Literal(2),
        },
        exec::ExecOp::Range {
            range: ir::StreamRangePlan::Literal(ir::StreamLiteralRange::new(2, 8).unwrap()),
        },
        exec::ExecOp::Order { plan: order },
        exec::ExecOp::Distinct,
        exec::ExecOp::Merge {
            mode: exec::ExecMergeMode::Concat,
        },
        exec::ExecOp::Barrier {
            name: name("materialize"),
        },
    ]);

    assert_eq!(diagnostics.statistics.residual_filters, 1);
    assert_eq!(diagnostics.statistics.limits, 1);
    assert_eq!(diagnostics.statistics.skips, 1);
    assert_eq!(diagnostics.statistics.ranges, 1);
    assert_eq!(diagnostics.statistics.explicit_sorts, 1);
    assert_eq!(diagnostics.statistics.unions, 0);
    assert_eq!(diagnostics.statistics.intersections, 0);
    assert_eq!(diagnostics.statistics.total_operators, 9);
    assert_eq!(diagnostics.statistics.maximum_operator_depth, 9);
}

#[test]
fn union_intersection_and_concat_merge_modes_have_independent_counters() {
    for (mode, expected_unions, expected_intersections) in [
        (exec::ExecMergeMode::Union, 1, 0),
        (exec::ExecMergeMode::Intersect, 0, 1),
        (exec::ExecMergeMode::Concat, 0, 0),
    ] {
        let first = exec::ExecStepId::new(1).unwrap();
        let second = exec::ExecStepId::new(2).unwrap();
        let steps = vec![
            step(1, Vec::new(), exec::ExecOp::Noop),
            step(2, Vec::new(), exec::ExecOp::Noop),
            step(3, vec![first, second], exec::ExecOp::Merge { mode }),
        ];
        let plan = executable_plan(steps, exec::PlannerMetrics::default());
        let diagnostics = crate::diagnostics::analyze(&plan, &context::PlannerContext::default());

        assert_eq!(diagnostics.statistics.unions, expected_unions);
        assert_eq!(diagnostics.statistics.intersections, expected_intersections);
        assert_eq!(diagnostics.statistics.total_operators, 3);
        assert_eq!(diagnostics.statistics.maximum_operator_depth, 2);
    }
}

#[test]
fn every_branch_form_counts_nested_selected_operators() {
    let condition = ir::PredicatePlan::new(Predicate::eq("active", true)).unwrap();
    let cases = [
        (
            exec::ExecBranchPlan::Union(ir::AtLeast::<_, 2>::from_pair(
                subplan([expand_op("A")]),
                subplan([expand_op("B")]),
            )),
            2,
        ),
        (
            exec::ExecBranchPlan::Choose {
                condition: condition.clone(),
                then_plan: Box::new(subplan([expand_op("A")])),
            },
            1,
        ),
        (
            exec::ExecBranchPlan::ChooseElse {
                condition,
                then_plan: Box::new(subplan([expand_op("A")])),
                else_plan: Box::new(subplan([expand_op("B")])),
            },
            2,
        ),
        (
            exec::ExecBranchPlan::Coalesce(ir::AtLeast::<_, 1>::from_one_and_rest(
                subplan([expand_op("A")]),
                vec![subplan([expand_op("B")])],
            )),
            2,
        ),
        (
            exec::ExecBranchPlan::Optional(Box::new(subplan([expand_op("A")]))),
            1,
        ),
    ];

    for (branch, expected_expansions) in cases {
        let diagnostics =
            diagnostics_for_ops([exec::ExecOp::Noop, exec::ExecOp::Branch { plan: branch }]);
        assert_eq!(diagnostics.statistics.branches, 1);
        assert_eq!(diagnostics.statistics.expansions, expected_expansions);
        assert_eq!(
            diagnostics.statistics.total_operators,
            2 + expected_expansions
        );
        assert_eq!(diagnostics.statistics.maximum_operator_depth, 3);
    }
}

#[test]
fn foreach_counts_its_body_without_inheriting_outer_traversal_depth() {
    let diagnostics = diagnostics_for_ops([
        expand_op("OUTER"),
        exec::ExecOp::ForEach {
            param: name("items"),
            body: Box::new(subplan([exec::ExecOp::Noop, expand_op("INNER")])),
        },
    ]);

    assert_eq!(diagnostics.statistics.for_each, 1);
    assert_eq!(diagnostics.statistics.expansions, 2);
    assert_eq!(diagnostics.statistics.total_operators, 4);
    assert_eq!(diagnostics.statistics.maximum_operator_depth, 4);
}

#[test]
fn real_write_foreach_and_mutation_plans_remain_diagnostic_safe() {
    let body = write_batch().var_as("created", g().add_n("Audit", vec![("kind", "event")]));
    let batch = write_batch().for_each_param("events", body);
    let output = crate::planning::plan_write_batch_with_diagnostics(
        &batch,
        &context::PlannerContext::default(),
    )
    .unwrap();

    assert_eq!(output.diagnostics().statistics.for_each, 1);
    assert!(output.diagnostics().statistics.total_operators >= 2);
    assert!(output.diagnostics().insights.is_empty());
}

#[test]
fn selected_sort_expand_and_repeat_statistics_match_real_lowering() {
    let expanded = plan(
        g().n_with_label("User")
            .order_by("name", Order::Asc)
            .out_e(Some("FOLLOWS"))
            .out_n()
            .out_e(Some("LIKES")),
        &context::PlannerContext::default(),
    );

    assert_eq!(expanded.diagnostics().statistics.expansions, 3);
    assert_eq!(expanded.diagnostics().statistics.explicit_sorts, 1);
    assert_eq!(
        expanded.diagnostics().statistics.node_accesses.label_scans,
        1
    );
}

#[test]
fn direct_linear_fixture_keeps_every_step_reachable() {
    let steps = linear_steps([exec::ExecOp::Noop, exec::ExecOp::Noop, exec::ExecOp::Noop]);
    let plan = executable_plan(steps, exec::PlannerMetrics::default());
    let diagnostics = crate::diagnostics::analyze(&plan, &context::PlannerContext::default());

    assert_eq!(diagnostics.statistics.total_operators, 3);
    assert_eq!(diagnostics.statistics.maximum_operator_depth, 3);
}
