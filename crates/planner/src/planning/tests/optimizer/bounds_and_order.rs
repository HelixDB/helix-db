use crate::planning::tests::support::*;
use crate::planning::{
    apply_aggregate_input, apply_count_input, apply_distinct, apply_exists_input, apply_filter,
    apply_limit, apply_required_prefix, empty_access_for_side_effect_free_shape,
    hard_cardinality_upper_bound, matching_range_direction_key, order_plan_satisfies,
    physical_input_is_already_distinct, physical_order_satisfies, physical_residual_filter_count,
    range_index_order_plan, rewrite_edge_range_index_order, rewrite_node_range_index_order,
    Planner,
};

fn test_order_key(property: &str, order: Order) -> OrderKey {
    OrderKey {
        property: NonEmptyString::new(property).unwrap(),
        order,
    }
}

fn residual_filter_op() -> PhysicalOp {
    PhysicalOp::Filter {
        input: Box::new(PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)),
        plan: FilterPlan::Residual {
            predicate: PredicatePlan::new(Predicate::eq("active", true)).unwrap(),
        },
    }
}

fn explicit_sort_op() -> PhysicalOp {
    PhysicalOp::Order {
        input: Box::new(PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)),
        plan: OrderPlan::ExplicitSort(OrderKeys::from(test_order_key("age", Order::Asc))),
    }
}

fn node_from_param_op() -> PhysicalOp {
    PhysicalOp::NodeAccess(NodeAccessPlan::FromParam {
        param: NonEmptyString::new("ids").unwrap(),
    })
}

fn edge_from_param_op() -> PhysicalOp {
    PhysicalOp::EdgeAccess(EdgeAccessPlan::FromParam {
        param: NonEmptyString::new("edge_ids").unwrap(),
    })
}

#[test]
fn residual_filter_count_walks_wrapper_inputs() {
    let range = StreamRangePlan::new(StreamBound::Literal(0), StreamBound::Literal(1)).unwrap();
    let branch_body = || PhysicalOp::NodeAccess(NodeAccessPlan::AllScan);

    let wrapped = [
        PhysicalOp::Limit {
            input: Box::new(residual_filter_op()),
            count: StreamBoundPlan::Literal(1),
        },
        PhysicalOp::Range {
            input: Box::new(residual_filter_op()),
            range,
        },
        PhysicalOp::Distinct {
            input: Box::new(residual_filter_op()),
        },
        PhysicalOp::TopN {
            input: Box::new(residual_filter_op()),
            keys: OrderKeys::from(test_order_key("age", Order::Asc)),
            count: NonZeroUsize::new(2).unwrap(),
        },
        PhysicalOp::Project {
            input: Box::new(residual_filter_op()),
            projection: ProjectionPlan::Id,
        },
        PhysicalOp::Aggregate {
            input: Box::new(residual_filter_op()),
            aggregate: AggregatePlan::Group(NonEmptyString::new("status").unwrap()),
        },
        PhysicalOp::Branch {
            input: Box::new(residual_filter_op()),
            plan: BranchPlan::Optional(Box::new(branch_body())),
        },
        PhysicalOp::Repeat {
            input: Box::new(residual_filter_op()),
            plan: RepeatPlan {
                body: Box::new(branch_body()),
                stop: RepeatStopPlan::MaxDepthOnly,
                emit: RepeatEmitPlan::None,
                max_depth: NonZeroUsize::new(1).unwrap(),
            },
        },
        PhysicalOp::Reserved {
            input: Box::new(residual_filter_op()),
            op: ReservedOp::Fold,
        },
    ];

    for op in wrapped {
        assert_eq!(physical_residual_filter_count(&op), 1, "{op:?}");
    }
}

#[test]
fn distinct_helper_contracts_cover_runtime_and_access_sources() {
    let runtime_nodes = node_from_param_op();
    let runtime_edges = edge_from_param_op();
    let residual = PredicatePlan::new(Predicate::eq("active", true)).unwrap();

    let projected = apply_distinct(PhysicalOp::Project {
        input: Box::new(runtime_nodes.clone()),
        projection: ProjectionPlan::Id,
    });
    assert!(matches!(
        projected,
        PhysicalOp::Project {
            input,
            projection: ProjectionPlan::Id,
        } if matches!(input.as_ref(), PhysicalOp::Distinct { .. })
    ));

    let sorted = apply_distinct(PhysicalOp::Order {
        input: Box::new(runtime_nodes.clone()),
        plan: OrderPlan::ExplicitSort(OrderKeys::from(test_order_key("age", Order::Asc))),
    });
    assert!(matches!(
        sorted,
        PhysicalOp::Order {
            input,
            plan: OrderPlan::ExplicitSort(_),
        } if matches!(input.as_ref(), PhysicalOp::Distinct { .. })
    ));

    let filtered = apply_filter(
        PhysicalOp::Distinct {
            input: Box::new(runtime_nodes.clone()),
        },
        FilterPlan::Residual {
            predicate: residual.clone(),
        },
    );
    assert!(matches!(
        filtered,
        PhysicalOp::Distinct { input }
            if matches!(input.as_ref(), PhysicalOp::Filter { .. })
    ));

    assert!(physical_input_is_already_distinct(&PhysicalOp::Distinct {
        input: Box::new(runtime_nodes.clone()),
    }));
    assert!(!physical_input_is_already_distinct(&runtime_nodes));
    assert!(!physical_input_is_already_distinct(&runtime_edges));

    let node_points = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::PointIds {
        ids: ElementIds::new(AtLeast::<_, 1>::from_one(1)).unwrap(),
    });
    let edge_points = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::PointIds {
        ids: ElementIds::new(AtLeast::<_, 1>::from_one(2)).unwrap(),
    });
    let node_param = NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::FromParam {
        param: NonEmptyString::new("ids").unwrap(),
    });
    let edge_param = EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::FromParam {
        param: NonEmptyString::new("edge_ids").unwrap(),
    });

    assert!(physical_input_is_already_distinct(&PhysicalOp::NodeAccess(
        NodeAccessPlan::Intersect(AtLeast::<_, 2>::from_pair(node_points, node_param)),
    )));
    assert!(physical_input_is_already_distinct(&PhysicalOp::NodeAccess(
        NodeAccessPlan::ScanThenFilter {
            source: NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::LabelScan {
                label: NonEmptyString::new("User").unwrap(),
            }),
            residual: residual.clone(),
        },
    )));
    assert!(physical_input_is_already_distinct(&PhysicalOp::EdgeAccess(
        EdgeAccessPlan::Intersect(AtLeast::<_, 2>::from_pair(edge_points.clone(), edge_param,)),
    )));
    assert!(physical_input_is_already_distinct(&PhysicalOp::EdgeAccess(
        EdgeAccessPlan::ScanThenFilter {
            source: EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::LabelScan {
                label: NonEmptyString::new("FOLLOWS").unwrap(),
            }),
            residual,
        },
    )));
    assert!(!physical_input_is_already_distinct(
        &PhysicalOp::EdgeAccess(EdgeAccessPlan::Union(AtLeast::<_, 2>::from_pair(
            edge_points.clone(),
            edge_points
        )),)
    ));

    let count_input = apply_count_input(PhysicalOp::Distinct {
        input: Box::new(runtime_nodes.clone()),
    });
    assert!(matches!(count_input, PhysicalOp::Distinct { .. }));

    let aggregate_input = apply_aggregate_input(PhysicalOp::Distinct {
        input: Box::new(runtime_nodes.clone()),
    });
    assert!(matches!(aggregate_input, PhysicalOp::Distinct { .. }));

    let exists_input = apply_exists_input(PhysicalOp::Distinct {
        input: Box::new(runtime_nodes.clone()),
    });
    assert!(matches!(
        exists_input,
        PhysicalOp::Limit {
            input,
            count: StreamBoundPlan::Literal(1),
        } if matches!(input.as_ref(), PhysicalOp::NodeAccess(NodeAccessPlan::FromParam { .. }))
    ));

    assert!(matches!(
        empty_access_for_side_effect_free_shape(&PhysicalOp::Distinct {
            input: Box::new(PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)),
        }),
        Some(PhysicalOp::NodeAccess(NodeAccessPlan::Empty))
    ));
    assert_eq!(
        hard_cardinality_upper_bound(&PhysicalOp::Distinct {
            input: Box::new(PhysicalOp::Limit {
                input: Box::new(runtime_nodes),
                count: StreamBoundPlan::Literal(2),
            }),
        }),
        Some(2)
    );
}

#[test]
fn distinct_helpers_preserve_index_pushdown_and_order_proofs() {
    let indexes = builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
        .with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        );
    let planner_ctx = ctx(indexes.clone());
    let mut planner = Planner::new(&planner_ctx);
    let pushed = planner
        .filter_access_path_pushdown(
            &PhysicalOp::Distinct {
                input: Box::new(PhysicalOp::NodeAccess(NodeAccessPlan::LabelScan {
                    label: NonEmptyString::new("User").unwrap(),
                })),
            },
            &Predicate::eq("username", "alice"),
            "test.distinct",
        )
        .unwrap()
        .expect("distinct access pushdown should improve the source");
    assert!(matches!(
        pushed,
        PhysicalOp::NodeAccess(NodeAccessPlan::EqualityIndex { key, .. })
            if key.property == "username"
    ));
    assert_eq!(
        planner
            .filter_access_path_pushdown(
                &PhysicalOp::Distinct {
                    input: Box::new(PhysicalOp::NodeAccess(NodeAccessPlan::LabelScan {
                        label: NonEmptyString::new("User").unwrap(),
                    })),
                },
                &Predicate::eq(String::new(), "alice"),
                "test.distinct",
            )
            .expect_err("distinct wrapper should propagate inner pushdown errors"),
        PlannerError::InvalidEmptyName {
            field: NameField::Property
        }
    );

    let range_key =
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap();
    let mut input = PhysicalOp::Distinct {
        input: Box::new(PhysicalOp::NodeAccess(NodeAccessPlan::RangeIndex {
            index: NodeRangeIndexMeta::new("node_range:User:age:Asc"),
            key: range_key,
            range: IndexRange::lower(IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
            )),
        })),
    };
    let (plan, index_id) =
        range_index_order_plan(&mut input, &test_order_key("age", Order::Asc), &indexes)
            .expect("distinct wrapper should preserve range-index order proof");
    assert!(matches!(plan, OrderPlan::RangeIndex { .. }));
    assert_eq!(index_id.as_ref(), "node_range:User:age:Asc");
}

#[test]
fn direct_node_range_index_order_uses_range_index_order_plan() {
    let plan = plan_traversal(
        g().n_where(Predicate::or(vec![Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::gte("age", 21),
        ])]))
        .order_by("age", Order::Asc),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&plan)
    else {
        panic!("expected order: {:?}", run_op(&plan));
    };
    let OrderPlan::RangeIndex { key, index_id } = order_plan else {
        panic!("expected range index order plan: {order_plan:?}");
    };
    assert_eq!(key.property.as_ref(), "age");
    assert_eq!(key.order, Order::Asc);
    assert_eq!(index_id.as_ref(), "node_range:User:age:Asc");
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::RangeIndex { key, .. })
            if key.property == "age" && key.direction == RangeIndexDirection::Asc
    ));
    assert_decision(
        &plan,
        TracePass::OrderPushdown,
        TraceDecision::RangeIndexOrder,
    );
    assert_no_decision(&plan, TraceDecision::ExplicitSort);
}

#[test]
fn direct_node_range_index_order_by_multiple_uses_range_index_order_plan() {
    let plan = plan_traversal(
        g().n_where(Predicate::or(vec![Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::gte("age", 21),
        ])]))
        .order_by_multiple(vec![("age", Order::Asc)]),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&plan)
    else {
        panic!("expected order: {:?}", run_op(&plan));
    };
    let OrderPlan::RangeIndex { key, index_id } = order_plan else {
        panic!("expected range index order plan: {order_plan:?}");
    };
    assert_eq!(key.property.as_ref(), "age");
    assert_eq!(key.order, Order::Asc);
    assert_eq!(index_id.as_ref(), "node_range:User:age:Asc");
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::RangeIndex { key, .. })
            if key.property == "age" && key.direction == RangeIndexDirection::Asc
    ));
    assert_decision(
        &plan,
        TracePass::OrderPushdown,
        TraceDecision::RangeIndexOrder,
    );
    assert_no_decision(&plan, TraceDecision::ExplicitSort);
}

#[test]
fn direct_edge_range_index_order_uses_range_index_order_plan() {
    let plan = plan_traversal(
        g().e_where(Predicate::or(vec![Predicate::and(vec![
            Predicate::eq("$label", "FOLLOWS"),
            Predicate::lte("since", 2024),
        ])]))
        .order_by("since", Order::Desc),
        ctx(builtin_label_indexes().with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "since", RangeIndexDirection::Desc)
                .unwrap(),
        )),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&plan)
    else {
        panic!("expected order: {:?}", run_op(&plan));
    };
    let OrderPlan::RangeIndex { key, index_id } = order_plan else {
        panic!("expected range index order plan: {order_plan:?}");
    };
    assert_eq!(key.property.as_ref(), "since");
    assert_eq!(key.order, Order::Desc);
    assert_eq!(index_id.as_ref(), "edge_range:FOLLOWS:since:Desc");
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::EdgeAccess(EdgeAccessPlan::RangeIndex { key, .. })
            if key.property == "since" && key.direction == RangeIndexDirection::Desc
    ));
    assert_decision(
        &plan,
        TracePass::OrderPushdown,
        TraceDecision::RangeIndexOrder,
    );
    assert_no_decision(&plan, TraceDecision::ExplicitSort);
}

#[test]
fn node_residual_range_index_order_uses_filtered_source_order() {
    let plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::gte("age", 21),
                Predicate::eq("active", true),
            ]),
        )
        .order_by("age", Order::Asc),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&plan)
    else {
        panic!("expected order: {:?}", run_op(&plan));
    };
    let OrderPlan::RangeIndex { key, index_id } = order_plan else {
        panic!("expected range index order plan: {order_plan:?}");
    };
    assert_eq!(key.property.as_ref(), "age");
    assert_eq!(key.order, Order::Asc);
    assert_eq!(index_id.as_ref(), "node_range:User:age:Asc");
    let PhysicalOp::NodeAccess(NodeAccessPlan::ScanThenFilter { source, residual }) =
        input.as_ref()
    else {
        panic!("expected filtered node range source: {input:?}");
    };
    assert!(matches!(
        source.as_ref(),
        NodeAccessPlan::RangeIndex { key, index, .. }
            if key.property == "age"
                && key.direction == RangeIndexDirection::Asc
                && index.index_id.as_ref() == "node_range:User:age:Asc"
    ));
    assert_eq!(residual.as_ref(), &Predicate::eq("active", true));
    assert_decision(
        &plan,
        TracePass::OrderPushdown,
        TraceDecision::RangeIndexOrder,
    );
    assert_no_decision(&plan, TraceDecision::ExplicitSort);
}

#[test]
fn node_residual_range_index_order_rewrites_filtered_source_direction() {
    let asc = ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap();
    let desc =
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Desc).unwrap();
    let plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::gte("age", 21),
                Predicate::eq("active", true),
            ]),
        )
        .order_by("age", Order::Desc),
        ctx(builtin_label_indexes()
            .with_node_range(asc)
            .with_node_range(desc)),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&plan)
    else {
        panic!("expected order: {:?}", run_op(&plan));
    };
    let OrderPlan::RangeIndex { key, index_id } = order_plan else {
        panic!("expected range index order plan: {order_plan:?}");
    };
    assert_eq!(key.property.as_ref(), "age");
    assert_eq!(key.order, Order::Desc);
    assert_eq!(index_id.as_ref(), "node_range:User:age:Desc");
    let PhysicalOp::NodeAccess(NodeAccessPlan::ScanThenFilter { source, .. }) = input.as_ref()
    else {
        panic!("expected filtered node range source: {input:?}");
    };
    assert!(matches!(
        source.as_ref(),
        NodeAccessPlan::RangeIndex { key, index, .. }
            if key.property == "age"
                && key.direction == RangeIndexDirection::Desc
                && index.index_id.as_ref() == "node_range:User:age:Desc"
    ));
    assert_decision(
        &plan,
        TracePass::OrderPushdown,
        TraceDecision::RangeIndexOrder,
    );
    assert_no_decision(&plan, TraceDecision::ExplicitSort);
}

#[test]
fn edge_residual_range_index_order_uses_filtered_source_order() {
    let plan = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::gte("since", 2020),
                Predicate::eq("active", true),
            ]),
        )
        .order_by("since", Order::Asc),
        ctx(builtin_label_indexes().with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "since", RangeIndexDirection::Asc)
                .unwrap(),
        )),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&plan)
    else {
        panic!("expected order: {:?}", run_op(&plan));
    };
    let OrderPlan::RangeIndex { key, index_id } = order_plan else {
        panic!("expected range index order plan: {order_plan:?}");
    };
    assert_eq!(key.property.as_ref(), "since");
    assert_eq!(key.order, Order::Asc);
    assert_eq!(index_id.as_ref(), "edge_range:FOLLOWS:since:Asc");
    let PhysicalOp::EdgeAccess(EdgeAccessPlan::ScanThenFilter { source, residual }) =
        input.as_ref()
    else {
        panic!("expected filtered edge range source: {input:?}");
    };
    assert!(matches!(
        source.as_ref(),
        EdgeAccessPlan::RangeIndex { key, index, .. }
            if key.property == "since"
                && key.direction == RangeIndexDirection::Asc
                && index.index_id.as_ref() == "edge_range:FOLLOWS:since:Asc"
    ));
    assert_eq!(residual.as_ref(), &Predicate::eq("active", true));
    assert_decision(
        &plan,
        TracePass::OrderPushdown,
        TraceDecision::RangeIndexOrder,
    );
    assert_no_decision(&plan, TraceDecision::ExplicitSort);
}

#[test]
fn edge_residual_range_index_order_rewrites_filtered_source_direction() {
    let asc =
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "since", RangeIndexDirection::Asc).unwrap();
    let desc =
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "since", RangeIndexDirection::Desc).unwrap();
    let plan = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::gte("since", 2020),
                Predicate::eq("active", true),
            ]),
        )
        .order_by("since", Order::Desc),
        ctx(builtin_label_indexes()
            .with_edge_range(asc)
            .with_edge_range(desc)),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&plan)
    else {
        panic!("expected order: {:?}", run_op(&plan));
    };
    let OrderPlan::RangeIndex { key, index_id } = order_plan else {
        panic!("expected range index order plan: {order_plan:?}");
    };
    assert_eq!(key.property.as_ref(), "since");
    assert_eq!(key.order, Order::Desc);
    assert_eq!(index_id.as_ref(), "edge_range:FOLLOWS:since:Desc");
    let PhysicalOp::EdgeAccess(EdgeAccessPlan::ScanThenFilter { source, .. }) = input.as_ref()
    else {
        panic!("expected filtered edge range source: {input:?}");
    };
    assert!(matches!(
        source.as_ref(),
        EdgeAccessPlan::RangeIndex { key, index, .. }
            if key.property == "since"
                && key.direction == RangeIndexDirection::Desc
                && index.index_id.as_ref() == "edge_range:FOLLOWS:since:Desc"
    ));
    assert_decision(
        &plan,
        TracePass::OrderPushdown,
        TraceDecision::RangeIndexOrder,
    );
    assert_no_decision(&plan, TraceDecision::ExplicitSort);
}

#[test]
fn filtered_unordered_access_sources_keep_explicit_sort() {
    let node_plan = plan_traversal(
        g().n_with_label_where("User", Predicate::eq("active", true))
            .order_by("age", Order::Asc),
        ctx(builtin_label_indexes()),
    );
    let edge_plan = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::eq("active", true))
            .order_by("since", Order::Asc),
        ctx(builtin_label_indexes()),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&node_plan)
    else {
        panic!("expected node order: {:?}", run_op(&node_plan));
    };
    let OrderPlan::ExplicitSort(keys) = order_plan else {
        panic!("expected explicit sort for unordered node source: {order_plan:?}");
    };
    assert_eq!(keys.as_ref(), &[test_order_key("age", Order::Asc)]);
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::ScanThenFilter { source, .. })
            if matches!(
                source.as_ref(),
                NodeAccessPlan::LabelScan { label } if label.as_ref() == "User"
            )
    ));
    assert_decision(
        &node_plan,
        TracePass::OrderPushdown,
        TraceDecision::ExplicitSort,
    );
    assert_no_decision(&node_plan, TraceDecision::RangeIndexOrder);

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&edge_plan)
    else {
        panic!("expected edge order: {:?}", run_op(&edge_plan));
    };
    let OrderPlan::ExplicitSort(keys) = order_plan else {
        panic!("expected explicit sort for unordered edge source: {order_plan:?}");
    };
    assert_eq!(keys.as_ref(), &[test_order_key("since", Order::Asc)]);
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::EdgeAccess(EdgeAccessPlan::ScanThenFilter { source, .. })
            if matches!(
                source.as_ref(),
                EdgeAccessPlan::LabelScan { label } if label.as_ref() == "FOLLOWS"
            )
    ));
    assert_decision(
        &edge_plan,
        TracePass::OrderPushdown,
        TraceDecision::ExplicitSort,
    );
    assert_no_decision(&edge_plan, TraceDecision::RangeIndexOrder);
}

#[test]
fn bounded_filtered_range_source_does_not_rewrite_direction_before_order() {
    let asc = ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap();
    let desc =
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Desc).unwrap();
    let plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::gte("age", 21),
                Predicate::eq("active", true),
            ]),
        )
        .limit(10usize)
        .order_by("age", Order::Desc),
        ctx(builtin_label_indexes()
            .with_node_range(asc)
            .with_node_range(desc)),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&plan)
    else {
        panic!("expected order: {:?}", run_op(&plan));
    };
    let OrderPlan::ExplicitSort(keys) = order_plan else {
        panic!("expected explicit sort over bounded filtered source: {order_plan:?}");
    };
    assert_eq!(keys.as_ref(), &[test_order_key("age", Order::Desc)]);
    let PhysicalOp::Limit { input, .. } = input.as_ref() else {
        panic!("expected limit under order: {input:?}");
    };
    let PhysicalOp::NodeAccess(NodeAccessPlan::ScanThenFilter { source, .. }) = input.as_ref()
    else {
        panic!("expected filtered node range source: {input:?}");
    };
    assert!(matches!(
        source.as_ref(),
        NodeAccessPlan::RangeIndex { key, index, .. }
            if key.property == "age"
                && key.direction == RangeIndexDirection::Asc
                && index.index_id.as_ref() == "node_range:User:age:Asc"
    ));
    assert_decision(&plan, TracePass::OrderPushdown, TraceDecision::ExplicitSort);
    assert_no_decision(&plan, TraceDecision::RangeIndexOrder);
}

#[test]
fn node_intersection_range_index_order_keeps_explicit_sort_without_order_contract() {
    let plan = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::eq("username", "alice"),
                Predicate::gte("age", 21),
            ]),
        )
        .order_by("age", Order::Asc),
        ctx(builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
            )),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&plan)
    else {
        panic!("expected order: {:?}", run_op(&plan));
    };
    let OrderPlan::ExplicitSort(keys) = order_plan else {
        panic!("expected explicit sort over unordered intersection: {order_plan:?}");
    };
    assert_eq!(keys.as_ref()[0].property.as_ref(), "age");
    assert_eq!(keys.as_ref()[0].order, Order::Asc);
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::Intersect(_))
    ));
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::NodeIntersect);
    assert_decision(&plan, TracePass::OrderPushdown, TraceDecision::ExplicitSort);
    assert_no_decision(&plan, TraceDecision::RangeIndexOrder);
}

#[test]
fn edge_intersection_range_index_order_keeps_explicit_sort_without_order_contract() {
    let plan = plan_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::eq("status", "active"),
                Predicate::lte("since", 2024),
            ]),
        )
        .order_by("since", Order::Desc),
        ctx(builtin_label_indexes()
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
            .with_edge_range(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "since", RangeIndexDirection::Desc)
                    .unwrap(),
            )),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&plan)
    else {
        panic!("expected order: {:?}", run_op(&plan));
    };
    let OrderPlan::ExplicitSort(keys) = order_plan else {
        panic!("expected explicit sort over unordered intersection: {order_plan:?}");
    };
    assert_eq!(keys.as_ref()[0].property.as_ref(), "since");
    assert_eq!(keys.as_ref()[0].order, Order::Desc);
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::EdgeAccess(EdgeAccessPlan::Intersect(_))
    ));
    assert_decision(&plan, TracePass::AccessPath, TraceDecision::EdgeIntersect);
    assert_decision(&plan, TracePass::OrderPushdown, TraceDecision::ExplicitSort);
    assert_no_decision(&plan, TraceDecision::RangeIndexOrder);
}

#[test]
fn singleton_sources_skip_order_plans() {
    let node_point = plan_traversal(
        g().n([7u64]).order_by("age", Order::Asc),
        ctx(IndexCatalogSnapshot::default()),
    );
    let edge_point = plan_traversal(
        g().e([11u64]).order_by("since", Order::Desc),
        ctx(IndexCatalogSnapshot::default()),
    );
    let node_multi_order = plan_traversal(
        g().n([13u64])
            .order_by_multiple(vec![("age", Order::Asc), ("name", Order::Desc)]),
        ctx(IndexCatalogSnapshot::default()),
    );

    let unique = ScopedPropertyKey::try_new("User", "id").unwrap();
    let mut indexes = builtin_label_indexes().with_node_range(
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
    );
    indexes.node_eq.insert(
        unique,
        NodeEqualityIndexMeta::try_new("node_eq:User:id")
            .unwrap()
            .with_uniqueness(IndexUniqueness::Unique),
    );
    let unique_index = plan_traversal(
        g().n_with_label_where("User", Predicate::eq("id", 7))
            .order_by("age", Order::Asc),
        ctx(indexes.clone()),
    );
    let search = plan_traversal(
        g().text_search_nodes("Doc", "body", "planner", 1, None)
            .order_by("age", Order::Asc),
        ctx(indexes.with_text(
            SearchIndexKey::try_new(ElementKind::Node, "Doc", "body").unwrap(),
            SearchIndexScope::Unscoped,
        )),
    );

    assert!(matches!(
        run_op(&node_point),
        PhysicalOp::NodeAccess(NodeAccessPlan::PointIds { .. })
    ));
    assert!(matches!(
        run_op(&edge_point),
        PhysicalOp::EdgeAccess(EdgeAccessPlan::PointIds { .. })
    ));
    assert!(matches!(
        run_op(&node_multi_order),
        PhysicalOp::NodeAccess(NodeAccessPlan::PointIds { .. })
    ));
    assert!(matches!(
        run_op(&unique_index),
        PhysicalOp::NodeAccess(NodeAccessPlan::EqualityIndex { .. })
    ));
    assert!(matches!(
        run_op(&search),
        PhysicalOp::NodeAccess(NodeAccessPlan::TextSearch { .. })
    ));

    for plan in [
        &node_point,
        &edge_point,
        &node_multi_order,
        &unique_index,
        &search,
    ] {
        assert_no_decision(plan, TraceDecision::ExplicitSort);
        assert_no_decision(plan, TraceDecision::RangeIndexOrder);
    }
}

#[test]
fn singleton_sources_skip_distinct_plans() {
    let node_point = plan_traversal(g().n([7u64]).dedup(), ctx(IndexCatalogSnapshot::default()));
    let edge_point = plan_traversal(g().e([11u64]).dedup(), ctx(IndexCatalogSnapshot::default()));
    let stored_singleton = plan_traversal(
        g().n([13u64]).store("seed").dedup(),
        ctx(IndexCatalogSnapshot::default()),
    );

    let unique = ScopedPropertyKey::try_new("User", "id").unwrap();
    let mut indexes = builtin_label_indexes();
    indexes.node_eq.insert(
        unique,
        NodeEqualityIndexMeta::try_new("node_eq:User:id")
            .unwrap()
            .with_uniqueness(IndexUniqueness::Unique),
    );
    let unique_index = plan_traversal(
        g().n_with_label_where("User", Predicate::eq("id", 7))
            .dedup(),
        ctx(indexes.clone()),
    );
    let unique_null = plan_traversal(
        g().n_with_label_where("User", Predicate::eq("id", PropertyValue::Null))
            .dedup(),
        ctx(indexes.clone()),
    );
    let search = plan_traversal(
        g().text_search_nodes("Doc", "body", "planner", 1, None)
            .dedup(),
        ctx(indexes.with_text(
            SearchIndexKey::try_new(ElementKind::Node, "Doc", "body").unwrap(),
            SearchIndexScope::Unscoped,
        )),
    );

    assert!(matches!(
        run_op(&node_point),
        PhysicalOp::NodeAccess(NodeAccessPlan::PointIds { .. })
    ));
    assert!(matches!(
        run_op(&edge_point),
        PhysicalOp::EdgeAccess(EdgeAccessPlan::PointIds { .. })
    ));
    assert!(matches!(
        run_op(&stored_singleton),
        PhysicalOp::Variable(VariablePlan::Stream {
            op: StreamVariableOp::Store(_),
            ..
        })
    ));
    assert!(matches!(
        run_op(&unique_index),
        PhysicalOp::NodeAccess(NodeAccessPlan::EqualityIndex { .. })
    ));
    assert!(matches!(
        run_op(&unique_null),
        PhysicalOp::NodeAccess(NodeAccessPlan::EqualityIndex { .. })
    ));
    assert!(matches!(
        run_op(&search),
        PhysicalOp::NodeAccess(NodeAccessPlan::TextSearch { .. })
    ));

    for plan in [
        &node_point,
        &edge_point,
        &stored_singleton,
        &unique_index,
        &unique_null,
        &search,
    ] {
        assert!(
            !matches!(run_op(plan), PhysicalOp::Distinct { .. }),
            "singleton input should not need a distinct wrapper: {:?}",
            run_op(plan)
        );
    }
}

#[test]
fn vector_search_sources_skip_distinct_plans_but_text_search_stays_conservative() {
    let indexes = builtin_label_indexes()
        .with_vector(
            SearchIndexKey::try_new(ElementKind::Node, "Doc", "embedding").unwrap(),
            SearchIndexScope::Unscoped,
        )
        .with_vector(
            SearchIndexKey::try_new(ElementKind::Edge, "MENTIONS", "embedding").unwrap(),
            SearchIndexScope::Unscoped,
        )
        .with_text(
            SearchIndexKey::try_new(ElementKind::Node, "Doc", "body").unwrap(),
            SearchIndexScope::Unscoped,
        );
    let node_vector = plan_traversal(
        g().vector_search_nodes("Doc", "embedding", vec![0.1f32, 0.2], 3, None)
            .dedup(),
        ctx(indexes.clone()),
    );
    let edge_vector = plan_traversal(
        g().vector_search_edges("MENTIONS", "embedding", vec![0.3f32, 0.4], 3, None)
            .dedup(),
        ctx(indexes.clone()),
    );
    let node_text = plan_traversal(
        g().text_search_nodes("Doc", "body", "planner", 3, None)
            .dedup(),
        ctx(indexes),
    );

    assert!(matches!(
        run_op(&node_vector),
        PhysicalOp::NodeAccess(NodeAccessPlan::VectorSearch { .. })
    ));
    assert!(matches!(
        run_op(&edge_vector),
        PhysicalOp::EdgeAccess(EdgeAccessPlan::VectorSearch { .. })
    ));
    let PhysicalOp::Distinct { input } = run_op(&node_text) else {
        panic!(
            "expected text search to keep distinct boundary: {:?}",
            run_op(&node_text)
        );
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::TextSearch { .. })
    ));
}

#[test]
fn distinct_point_id_sources_skip_distinct_plans() {
    let node_point = plan_traversal(
        g().n([7u64, 9]).dedup(),
        ctx(IndexCatalogSnapshot::default()),
    );
    let edge_point = plan_traversal(
        g().e([11u64, 13]).dedup(),
        ctx(IndexCatalogSnapshot::default()),
    );
    let filtered_node_point = plan_traversal(
        g().n([1u64, 2]).has("active", true).dedup(),
        ctx(IndexCatalogSnapshot::default()),
    );
    let ordered_node_point = plan_traversal(
        g().n([3u64, 5]).order_by("age", Order::Asc).dedup(),
        ctx(IndexCatalogSnapshot::default()),
    );
    let limited_node_point = plan_traversal(
        g().n([17u64, 19])
            .limit(StreamBound::expr(Expr::param("limit")))
            .dedup(),
        ctx(IndexCatalogSnapshot::default()),
    );
    let skipped_node_point = plan_traversal(
        g().n([23u64, 29])
            .skip(StreamBound::expr(Expr::param("offset")))
            .dedup(),
        ctx(IndexCatalogSnapshot::default()),
    );
    let ranged_node_point = plan_traversal(
        g().n([31u64, 37, 41])
            .range(
                StreamBound::expr(Expr::param("start")),
                StreamBound::expr(Expr::param("end")),
            )
            .dedup(),
        ctx(IndexCatalogSnapshot::default()),
    );
    let stored_node_point = plan_traversal(
        g().n([43u64, 47]).store("seed").dedup(),
        ctx(IndexCatalogSnapshot::default()),
    );
    let variable_chain_node_point = plan_traversal(
        g().n([53u64, 59])
            .as_("seen")
            .store("seed")
            .bind("row")
            .within("allowed")
            .without("blocked")
            .dedup(),
        ctx(IndexCatalogSnapshot::default()),
    );
    let injected_node_point = plan_traversal(
        g().n([61u64, 67]).inject("extra").dedup(),
        ctx(IndexCatalogSnapshot::default()),
    );

    assert!(matches!(
        run_op(&node_point),
        PhysicalOp::NodeAccess(NodeAccessPlan::PointIds { .. })
    ));
    assert!(matches!(
        run_op(&edge_point),
        PhysicalOp::EdgeAccess(EdgeAccessPlan::PointIds { .. })
    ));
    assert!(matches!(
        run_op(&filtered_node_point),
        PhysicalOp::Filter { .. }
    ));
    assert!(matches!(
        run_op(&ordered_node_point),
        PhysicalOp::Order { .. }
    ));
    assert!(matches!(
        run_op(&limited_node_point),
        PhysicalOp::Limit { .. }
    ));
    assert!(matches!(
        run_op(&skipped_node_point),
        PhysicalOp::Skip { .. }
    ));
    assert!(matches!(
        run_op(&ranged_node_point),
        PhysicalOp::Range { .. }
    ));
    assert!(matches!(
        run_op(&stored_node_point),
        PhysicalOp::Variable(VariablePlan::Stream {
            op: StreamVariableOp::Store(_),
            ..
        })
    ));
    assert!(matches!(
        run_op(&variable_chain_node_point),
        PhysicalOp::Variable(VariablePlan::Stream {
            op: StreamVariableOp::Without(_),
            ..
        })
    ));
    let PhysicalOp::Distinct { input } = run_op(&injected_node_point) else {
        panic!(
            "inject can change stream identity and must retain distinct: {:?}",
            run_op(&injected_node_point)
        );
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::Variable(VariablePlan::Stream {
            op: StreamVariableOp::Inject(_),
            ..
        })
    ));

    for plan in [
        &node_point,
        &edge_point,
        &filtered_node_point,
        &ordered_node_point,
        &limited_node_point,
        &skipped_node_point,
        &ranged_node_point,
        &stored_node_point,
        &variable_chain_node_point,
    ] {
        assert!(
            !matches!(run_op(plan), PhysicalOp::Distinct { .. }),
            "unique point-id input should not need a distinct wrapper: {:?}",
            run_op(plan)
        );
    }
}

#[test]
fn top_n_preserves_inner_distinctness_contract() {
    let distinct_input = PhysicalOp::TopN {
        input: Box::new(PhysicalOp::NodeAccess(NodeAccessPlan::PointIds {
            ids: ElementIds::new(AtLeast::<_, 1>::from_one_and_rest(1, vec![2])).unwrap(),
        })),
        keys: OrderKeys::from(test_order_key("age", Order::Asc)),
        count: NonZeroUsize::new(3).unwrap(),
    };

    let PhysicalOp::TopN { input, keys, count } = apply_distinct(distinct_input) else {
        panic!("expected distinct over already distinct top-n to be elided");
    };
    assert_eq!(keys.iter().next().unwrap().property.as_ref(), "age");
    assert_eq!(count.get(), 3);
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::PointIds { .. })
    ));

    let non_distinct_input = PhysicalOp::TopN {
        input: Box::new(PhysicalOp::NodeAccess(NodeAccessPlan::FromParam {
            param: NonEmptyString::new("ids").unwrap(),
        })),
        keys: OrderKeys::from(test_order_key("age", Order::Asc)),
        count: NonZeroUsize::new(3).unwrap(),
    };

    assert!(matches!(
        apply_distinct(non_distinct_input),
        PhysicalOp::Distinct { .. }
    ));
}

#[test]
fn unique_node_equality_unions_skip_distinct_only_when_duplicate_free() {
    let email = ScopedPropertyKey::try_new("User", "email").unwrap();
    let username = ScopedPropertyKey::try_new("User", "username").unwrap();
    let mut unique_indexes = builtin_label_indexes();
    unique_indexes.node_eq.insert(
        email.clone(),
        NodeEqualityIndexMeta::try_new("node_eq:User:email")
            .unwrap()
            .with_uniqueness(IndexUniqueness::Unique),
    );
    unique_indexes.node_eq.insert(
        username,
        NodeEqualityIndexMeta::try_new("node_eq:User:username")
            .unwrap()
            .with_uniqueness(IndexUniqueness::Unique),
    );

    let duplicate_free = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::is_in(
                "email",
                PropertyValue::StringArray(vec![
                    "alice@example.com".into(),
                    "bob@example.com".into(),
                ]),
            ),
        )
        .dedup(),
        ctx(unique_indexes.clone()),
    );
    let non_unique = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::is_in(
                "email",
                PropertyValue::StringArray(vec![
                    "alice@example.com".into(),
                    "bob@example.com".into(),
                ]),
            ),
        )
        .dedup(),
        ctx(builtin_label_indexes().with_node_eq(email)),
    );
    let nullable = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::eq("email", PropertyValue::Null),
                Predicate::eq("email", "alice@example.com"),
            ]),
        )
        .dedup(),
        ctx(unique_indexes.clone()),
    );
    let mixed_properties = plan_traversal(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::eq("email", "alice@example.com"),
                Predicate::eq("username", "alice"),
            ]),
        )
        .dedup(),
        ctx(unique_indexes),
    );

    assert!(
        matches!(
            run_op(&duplicate_free),
            PhysicalOp::NodeAccess(NodeAccessPlan::Union(_))
        ),
        "same-property unique equality union should not need distinct: {:?}",
        run_op(&duplicate_free)
    );
    assert_distinct_over_node_union(&non_unique);
    assert_distinct_over_node_union(&nullable);
    assert_distinct_over_node_union(&mixed_properties);
}

#[test]
fn unique_node_equality_union_distinct_contract_rejects_invalid_sources() {
    let key = ScopedPropertyKey::try_new("User", "email").unwrap();
    let index = NodeEqualityIndexMeta::try_new("node_eq:User:email")
        .unwrap()
        .with_uniqueness(IndexUniqueness::Unique);
    let equality = |value| {
        NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::EqualityIndex {
            index: index.clone(),
            key: key.clone(),
            value: IndexValue::Literal(
                SecondaryIndexLiteral::new(PropertyValue::from(value)).unwrap(),
            ),
        })
    };

    let duplicate_values = AtLeast::<_, 2>::from_pair(equality("alice"), equality("alice"));
    let mixed_sources = AtLeast::<_, 2>::from_pair(
        equality("alice"),
        NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::LabelScan {
            label: NonEmptyString::new("User").unwrap(),
        }),
    );

    assert!(!crate::planning::node_unique_equality_union_is_duplicate_free(&duplicate_values));
    assert!(!crate::planning::node_unique_equality_union_is_duplicate_free(&mixed_sources));
}

#[test]
fn id_projection_over_distinct_input_skips_distinct_but_label_projection_does_not() {
    let id_projection = plan_ast(
        AstNode::Dedup {
            input: Box::new(AstNode::Id {
                input: Box::new(AstNode::Nodes {
                    reference: NodeRef::ids([7u64, 9]),
                }),
            }),
        },
        PlannerContext::default(),
    );
    let label_projection = plan_ast(
        AstNode::Dedup {
            input: Box::new(AstNode::Label {
                input: Box::new(AstNode::Nodes {
                    reference: NodeRef::ids([11u64, 13]),
                }),
            }),
        },
        PlannerContext::default(),
    );

    assert!(
        matches!(
            run_op(&id_projection),
            PhysicalOp::Project {
                projection: ProjectionPlan::Id,
                ..
            }
        ),
        "expected id projection without distinct: {:?}",
        run_op(&id_projection)
    );

    let PhysicalOp::Distinct { input } = run_op(&label_projection) else {
        panic!(
            "expected label projection to keep distinct: {:?}",
            run_op(&label_projection)
        );
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::Project {
            projection: ProjectionPlan::Label,
            ..
        }
    ));
}

#[test]
fn distinct_pushes_below_id_projection_but_not_label_projection() {
    let id_projection = plan_ast(
        AstNode::Dedup {
            input: Box::new(AstNode::Id {
                input: Box::new(AstNode::Nodes {
                    reference: NodeRef::all(),
                }),
            }),
        },
        PlannerContext::default(),
    );
    let label_projection = plan_ast(
        AstNode::Dedup {
            input: Box::new(AstNode::Label {
                input: Box::new(AstNode::Nodes {
                    reference: NodeRef::all(),
                }),
            }),
        },
        PlannerContext::default(),
    );

    let PhysicalOp::Project {
        input,
        projection: ProjectionPlan::Id,
    } = run_op(&id_projection)
    else {
        panic!(
            "expected id projection above inner distinct: {:?}",
            run_op(&id_projection)
        );
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::Distinct { input } = run_op(&label_projection) else {
        panic!(
            "expected label projection to keep outer distinct: {:?}",
            run_op(&label_projection)
        );
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::Project {
            projection: ProjectionPlan::Label,
            ..
        }
    ));
}

#[test]
fn distinct_binding_projection_skips_outer_distinct_but_all_mode_does_not() {
    let binding_projection = |distinct| AstNode::ProjectBindings {
        input: Box::new(AstNode::Nodes {
            reference: NodeRef::all(),
        }),
        projections: vec![BindingProjection::current("name", "name")],
        distinct,
    };
    let already_distinct = plan_ast(
        AstNode::Dedup {
            input: Box::new(binding_projection(true)),
        },
        PlannerContext::default(),
    );
    let all_mode = plan_ast(
        AstNode::Dedup {
            input: Box::new(binding_projection(false)),
        },
        PlannerContext::default(),
    );

    assert!(
        matches!(
            run_op(&already_distinct),
            PhysicalOp::Project {
                projection: ProjectionPlan::ProjectBindings {
                    dedup: ProjectionDedupMode::Distinct,
                    ..
                },
                ..
            }
        ),
        "expected distinct binding projection without outer distinct: {:?}",
        run_op(&already_distinct)
    );

    let PhysicalOp::Distinct { input } = run_op(&all_mode) else {
        panic!(
            "expected all-mode binding projection to keep outer distinct: {:?}",
            run_op(&all_mode)
        );
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::Project {
            projection: ProjectionPlan::ProjectBindings {
                dedup: ProjectionDedupMode::All,
                ..
            },
            ..
        }
    ));
}

#[test]
fn duplicate_free_sources_skip_adjacent_distinct_wrappers() {
    let nodes = plan_traversal(
        g().n(NodeRef::all()).dedup().dedup(),
        PlannerContext::default(),
    );
    let edges = plan_traversal(
        g().e_with_label("FOLLOWS").dedup().dedup(),
        ctx(builtin_label_indexes()),
    );

    assert!(matches!(
        run_op(&nodes),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::EdgeAccess(access) = run_op(&edges) else {
        panic!("expected duplicate-free edge access: {:?}", run_op(&edges));
    };
    assert_edge_label_scan(access, "FOLLOWS");
}

#[test]
fn duplicate_free_input_elides_distinct_after_order() {
    let explicit = plan_traversal(
        g().n(NodeRef::all()).order_by("age", Order::Asc).dedup(),
        PlannerContext::default(),
    );
    let range_index = plan_traversal(
        g().n_where(Predicate::or(vec![Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::gte("age", 21),
        ])]))
        .order_by("age", Order::Asc)
        .dedup(),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );

    let PhysicalOp::Order {
        input,
        plan: OrderPlan::ExplicitSort(keys),
    } = run_op(&explicit)
    else {
        panic!(
            "expected explicit sort above distinct: {:?}",
            run_op(&explicit)
        );
    };
    assert_eq!(keys.as_ref()[0].property.as_ref(), "age");
    assert_eq!(keys.as_ref()[0].order, Order::Asc);
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::Order {
        input,
        plan: OrderPlan::RangeIndex { .. },
    } = run_op(&range_index)
    else {
        panic!(
            "expected range-index order after elided distinct: {:?}",
            run_op(&range_index)
        );
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::RangeIndex { .. })
    ));
}

#[test]
fn exists_terminal_limits_input_and_strips_order_distinct() {
    let ordered = plan_traversal(
        g().n(NodeRef::all()).order_by("age", Order::Asc).exists(),
        PlannerContext::default(),
    );
    let distinct_point = plan_traversal(
        g().n([1u64, 2, 3]).dedup().exists(),
        PlannerContext::default(),
    );
    let distinct_all = plan_traversal(
        g().n(NodeRef::all()).dedup().exists(),
        PlannerContext::default(),
    );
    let filtered = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .where_(Predicate::has_key("name"))
            .exists(),
        PlannerContext::default(),
    );
    let limited = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .limit(10usize)
            .exists(),
        PlannerContext::default(),
    );
    let skipped = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .skip(3usize)
            .exists(),
        PlannerContext::default(),
    );
    let ranged = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .range(2usize, 7usize)
            .exists(),
        PlannerContext::default(),
    );

    let PhysicalOp::Project {
        input,
        projection: ProjectionPlan::Exists,
    } = run_op(&ordered)
    else {
        panic!("expected exists projection: {:?}", run_op(&ordered));
    };
    let PhysicalOp::Limit { input, count } = input.as_ref() else {
        panic!("expected exists input limit: {input:?}");
    };
    assert_eq!(count, &StreamBoundPlan::Literal(1));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::Project {
        input,
        projection: ProjectionPlan::Exists,
    } = run_op(&filtered)
    else {
        panic!("expected exists projection: {:?}", run_op(&filtered));
    };
    let PhysicalOp::Limit { input, count } = input.as_ref() else {
        panic!("expected exists input limit: {input:?}");
    };
    assert_eq!(count, &StreamBoundPlan::Literal(1));
    let PhysicalOp::Filter { input, .. } = input.as_ref() else {
        panic!("expected filter to remain under exists: {input:?}");
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::Project {
        input,
        projection: ProjectionPlan::Exists,
    } = run_op(&limited)
    else {
        panic!("expected exists projection: {:?}", run_op(&limited));
    };
    let PhysicalOp::Limit { input, count } = input.as_ref() else {
        panic!("expected coalesced limit under exists: {input:?}");
    };
    assert_eq!(count, &StreamBoundPlan::Literal(1));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::Project {
        input,
        projection: ProjectionPlan::Exists,
    } = run_op(&skipped)
    else {
        panic!("expected exists projection: {:?}", run_op(&skipped));
    };
    let PhysicalOp::Range { input, range } = input.as_ref() else {
        panic!("expected skip plus exists to become range: {input:?}");
    };
    assert_eq!(
        range,
        &StreamRangePlan::new(StreamBound::Literal(3), StreamBound::Literal(4)).unwrap()
    );
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::Project {
        input,
        projection: ProjectionPlan::Exists,
    } = run_op(&ranged)
    else {
        panic!("expected exists projection: {:?}", run_op(&ranged));
    };
    let PhysicalOp::Range { input, range } = input.as_ref() else {
        panic!("expected range plus exists to stay range: {input:?}");
    };
    assert_eq!(
        range,
        &StreamRangePlan::new(StreamBound::Literal(2), StreamBound::Literal(3)).unwrap()
    );
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::Project {
        input,
        projection: ProjectionPlan::Exists,
    } = run_op(&distinct_all)
    else {
        panic!("expected exists projection: {:?}", run_op(&distinct_all));
    };
    let PhysicalOp::Limit { input, count } = input.as_ref() else {
        panic!("expected exists input limit: {input:?}");
    };
    assert_eq!(count, &StreamBoundPlan::Literal(1));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::Project {
        input,
        projection: ProjectionPlan::Exists,
    } = run_op(&distinct_point)
    else {
        panic!("expected exists projection: {:?}", run_op(&distinct_point));
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::PointIds { ids }) if ids.as_ref() == [1]
    ));
}

#[test]
fn count_terminal_strips_count_irrelevant_order_plans() {
    let explicit = plan_traversal(
        g().n(NodeRef::all()).order_by("age", Order::Asc).count(),
        PlannerContext::default(),
    );
    let distinct = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .dedup()
            .count(),
        PlannerContext::default(),
    );
    let filtered = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .where_(Predicate::has_key("name"))
            .count(),
        PlannerContext::default(),
    );
    let skipped = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .skip(3usize)
            .count(),
        PlannerContext::default(),
    );
    let ranged = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .range(2usize, 7usize)
            .count(),
        PlannerContext::default(),
    );
    let range_ordered = plan_traversal(
        g().n_with_label_where("User", Predicate::gte("age", 21))
            .order_by("age", Order::Asc)
            .count(),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );

    let PhysicalOp::Project {
        input,
        projection: ProjectionPlan::Exists,
    } = run_op(&explicit)
    else {
        panic!("expected count projection: {:?}", run_op(&explicit));
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::Project {
        input,
        projection: ProjectionPlan::Exists,
    } = run_op(&filtered)
    else {
        panic!("expected count projection: {:?}", run_op(&filtered));
    };
    let PhysicalOp::Filter { input, .. } = input.as_ref() else {
        panic!("expected filter to remain under count: {input:?}");
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::Project {
        input,
        projection: ProjectionPlan::Exists,
    } = run_op(&skipped)
    else {
        panic!("expected count projection: {:?}", run_op(&skipped));
    };
    let PhysicalOp::Skip { input, count } = input.as_ref() else {
        panic!("expected skip to remain under count: {input:?}");
    };
    assert_eq!(count, &StreamBoundPlan::Literal(3));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::Project {
        input,
        projection: ProjectionPlan::Exists,
    } = run_op(&ranged)
    else {
        panic!("expected count projection: {:?}", run_op(&ranged));
    };
    let PhysicalOp::Range { input, range } = input.as_ref() else {
        panic!("expected range to remain under count: {input:?}");
    };
    assert_eq!(
        range,
        &StreamRangePlan::new(StreamBound::Literal(2), StreamBound::Literal(7)).unwrap()
    );
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::Project {
        input,
        projection: ProjectionPlan::Exists,
    } = run_op(&distinct)
    else {
        panic!("expected count projection: {:?}", run_op(&distinct));
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::Project {
        input,
        projection: ProjectionPlan::Exists,
    } = run_op(&range_ordered)
    else {
        panic!("expected count projection: {:?}", run_op(&range_ordered));
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::RangeIndex { key, .. })
            if key.property == "age" && key.direction == RangeIndexDirection::Asc
    ));
}

#[test]
fn count_terminal_strips_row_count_preserving_projections() {
    let all_nodes = || AstNode::Nodes {
        reference: NodeRef::all(),
    };
    let value_map_count = plan_ast(
        AstNode::Count {
            input: Box::new(AstNode::ValueMap {
                input: Box::new(all_nodes()),
                properties: Some(vec!["name".to_string()]),
            }),
        },
        PlannerContext::default(),
    );
    let all_binding_count = plan_ast(
        AstNode::Count {
            input: Box::new(AstNode::ProjectBindings {
                input: Box::new(all_nodes()),
                projections: vec![BindingProjection::current("name", "name")],
                distinct: false,
            }),
        },
        PlannerContext::default(),
    );
    let values_count = plan_ast(
        AstNode::Count {
            input: Box::new(AstNode::Values {
                input: Box::new(all_nodes()),
                properties: vec!["name".to_string()],
            }),
        },
        PlannerContext::default(),
    );
    let distinct_binding_count = plan_ast(
        AstNode::Count {
            input: Box::new(AstNode::ProjectBindings {
                input: Box::new(all_nodes()),
                projections: vec![BindingProjection::current("name", "name")],
                distinct: true,
            }),
        },
        PlannerContext::default(),
    );

    for plan in [value_map_count, all_binding_count] {
        let PhysicalOp::Project {
            input,
            projection: ProjectionPlan::Exists,
        } = run_op(&plan)
        else {
            panic!("expected count projection: {:?}", run_op(&plan));
        };
        assert!(matches!(
            input.as_ref(),
            PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
        ));
    }

    let PhysicalOp::Project {
        input,
        projection: ProjectionPlan::Exists,
    } = run_op(&values_count)
    else {
        panic!("expected count projection: {:?}", run_op(&values_count));
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::Project {
            projection: ProjectionPlan::Values(_),
            ..
        }
    ));

    let PhysicalOp::Project {
        input,
        projection: ProjectionPlan::Exists,
    } = run_op(&distinct_binding_count)
    else {
        panic!(
            "expected count projection: {:?}",
            run_op(&distinct_binding_count)
        );
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::Project {
            projection: ProjectionPlan::ProjectBindings {
                dedup: ProjectionDedupMode::Distinct,
                ..
            },
            ..
        }
    ));
}

#[test]
fn exists_terminal_strips_emptiness_preserving_projections() {
    let all_nodes = || AstNode::Nodes {
        reference: NodeRef::all(),
    };
    let project_exists = plan_ast(
        AstNode::Exists {
            input: Box::new(AstNode::Project {
                input: Box::new(all_nodes()),
                projections: vec![Projection::property("name", "name")],
            }),
        },
        PlannerContext::default(),
    );
    let distinct_binding_exists = plan_ast(
        AstNode::Exists {
            input: Box::new(AstNode::ProjectBindings {
                input: Box::new(all_nodes()),
                projections: vec![BindingProjection::current("name", "name")],
                distinct: true,
            }),
        },
        PlannerContext::default(),
    );
    let values_exists = plan_ast(
        AstNode::Exists {
            input: Box::new(AstNode::Values {
                input: Box::new(all_nodes()),
                properties: vec!["name".to_string()],
            }),
        },
        PlannerContext::default(),
    );

    for plan in [project_exists, distinct_binding_exists] {
        let PhysicalOp::Project {
            input,
            projection: ProjectionPlan::Exists,
        } = run_op(&plan)
        else {
            panic!("expected exists projection: {:?}", run_op(&plan));
        };
        assert!(matches!(
            input.as_ref(),
            PhysicalOp::Limit {
                input,
                count: StreamBoundPlan::Literal(1),
            } if matches!(input.as_ref(), PhysicalOp::NodeAccess(NodeAccessPlan::AllScan))
        ));
    }

    let PhysicalOp::Project {
        input,
        projection: ProjectionPlan::Exists,
    } = run_op(&values_exists)
    else {
        panic!("expected exists projection: {:?}", run_op(&values_exists));
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::Limit {
            input,
            count: StreamBoundPlan::Literal(1),
        } if matches!(
            input.as_ref(),
            PhysicalOp::Project {
                projection: ProjectionPlan::Values(_),
                ..
            }
        )
    ));
}

#[test]
fn aggregate_terminals_strip_order_without_crossing_bounds() {
    let grouped = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .group("tenant_id"),
        PlannerContext::default(),
    );
    let group_counted = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .dedup()
            .group_count("status"),
        PlannerContext::default(),
    );
    let aggregate_filtered = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .where_(Predicate::has_key("score"))
            .aggregate_by(AggregateFunction::Mean, "score"),
        PlannerContext::default(),
    );
    let bounded = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .limit(3usize)
            .aggregate_by(AggregateFunction::Sum, "score"),
        PlannerContext::default(),
    );

    let PhysicalOp::Aggregate {
        input,
        aggregate: AggregatePlan::Group(property),
    } = run_op(&grouped)
    else {
        panic!("expected group aggregate: {:?}", run_op(&grouped));
    };
    assert_eq!(property.as_ref(), "tenant_id");
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::Aggregate {
        input,
        aggregate: AggregatePlan::GroupCount(property),
    } = run_op(&group_counted)
    else {
        panic!(
            "expected group-count aggregate: {:?}",
            run_op(&group_counted)
        );
    };
    assert_eq!(property.as_ref(), "status");
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::Aggregate {
        input,
        aggregate:
            AggregatePlan::AggregateBy {
                function: AggregateFunction::Mean,
                property,
            },
    } = run_op(&aggregate_filtered)
    else {
        panic!(
            "expected filtered aggregate-by: {:?}",
            run_op(&aggregate_filtered)
        );
    };
    assert_eq!(property.as_ref(), "score");
    let PhysicalOp::Filter { input, .. } = input.as_ref() else {
        panic!("expected filter to remain under aggregate: {input:?}");
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::Aggregate {
        input,
        aggregate:
            AggregatePlan::AggregateBy {
                function: AggregateFunction::Sum,
                property,
            },
    } = run_op(&bounded)
    else {
        panic!("expected bounded aggregate-by: {:?}", run_op(&bounded));
    };
    assert_eq!(property.as_ref(), "score");
    let PhysicalOp::TopN { input, keys, count } = input.as_ref() else {
        panic!("expected top-n barrier under aggregate: {input:?}");
    };
    assert_eq!(count.get(), 3);
    assert_eq!(keys.as_ref()[0].property.as_ref(), "age");
    assert_eq!(keys.as_ref()[0].order, Order::Asc);
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));
}

#[test]
fn aggregate_terminals_strip_noop_stream_wrappers() {
    let grouped = || AstNode::Group {
        input: Box::new(AstNode::Nodes {
            reference: NodeRef::all(),
        }),
        property: "tenant_id".to_string(),
    };
    let group_counted = || AstNode::GroupCount {
        input: Box::new(AstNode::Nodes {
            reference: NodeRef::all(),
        }),
        property: "status".to_string(),
    };
    let aggregate_by = || AstNode::AggregateBy {
        input: Box::new(AstNode::Nodes {
            reference: NodeRef::all(),
        }),
        function: AggregateFunction::Mean,
        property: "score".to_string(),
    };
    let group_limit = plan_ast(
        AstNode::Limit {
            input: Box::new(grouped()),
            count: StreamBound::Literal(1),
        },
        PlannerContext::default(),
    );
    let group_count_range = plan_ast(
        AstNode::Range {
            input: Box::new(group_counted()),
            start: StreamBound::Literal(0),
            end: StreamBound::Literal(1),
        },
        PlannerContext::default(),
    );
    let aggregate_order = plan_ast(
        AstNode::OrderBy {
            input: Box::new(aggregate_by()),
            property: "score".to_string(),
            order: Order::Desc,
        },
        PlannerContext::default(),
    );
    let aggregate_dedup = plan_ast(
        AstNode::Dedup {
            input: Box::new(aggregate_by()),
        },
        PlannerContext::default(),
    );
    let zero_group_limit = plan_ast(
        AstNode::Limit {
            input: Box::new(grouped()),
            count: StreamBound::Literal(0),
        },
        PlannerContext::default(),
    );

    assert!(
        matches!(
            run_op(&group_limit),
            PhysicalOp::Aggregate {
                aggregate: AggregatePlan::Group(_),
                ..
            }
        ),
        "expected group aggregate without limit: {:?}",
        run_op(&group_limit)
    );
    assert!(
        matches!(
            run_op(&group_count_range),
            PhysicalOp::Aggregate {
                aggregate: AggregatePlan::GroupCount(_),
                ..
            }
        ),
        "expected group-count aggregate without range: {:?}",
        run_op(&group_count_range)
    );
    for plan in [aggregate_order, aggregate_dedup] {
        assert!(
            matches!(
                run_op(&plan),
                PhysicalOp::Aggregate {
                    aggregate: AggregatePlan::AggregateBy { .. },
                    ..
                }
            ),
            "expected aggregate-by without wrapper: {:?}",
            run_op(&plan)
        );
    }

    let PhysicalOp::Limit { input, count } = run_op(&zero_group_limit) else {
        panic!(
            "expected zero limit above aggregate: {:?}",
            run_op(&zero_group_limit)
        );
    };
    assert_eq!(count, &StreamBoundPlan::Literal(0));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::Aggregate {
            aggregate: AggregatePlan::Group(_),
            ..
        }
    ));
}

#[test]
fn order_plan_satisfaction_contract_covers_prefixes_and_range_indexes() {
    let age = test_order_key("age", Order::Asc);
    let name = test_order_key("name", Order::Desc);
    let explicit_multi = OrderPlan::ExplicitSort(
        OrderKeys::new(AtLeast::<_, 1>::from_one_and_rest(
            age.clone(),
            vec![name.clone()],
        ))
        .unwrap(),
    );
    let explicit_age = OrderPlan::ExplicitSort(OrderKeys::from(age.clone()));
    let explicit_name = OrderPlan::ExplicitSort(OrderKeys::from(name));
    let range_age = OrderPlan::RangeIndex {
        key: age,
        index_id: NonEmptyString::new("node_range:User:age:Asc").unwrap(),
    };

    assert!(order_plan_satisfies(&explicit_multi, &explicit_age));
    assert!(order_plan_satisfies(&explicit_multi, &range_age));
    assert!(order_plan_satisfies(&range_age, &explicit_age));
    assert!(!order_plan_satisfies(&explicit_age, &explicit_multi));
    assert!(!order_plan_satisfies(&range_age, &explicit_name));
}

#[test]
fn redundant_order_over_order_preserving_streams_is_elided() {
    let direct = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .order_by("age", Order::Asc),
        PlannerContext::default(),
    );
    let limited = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .limit(10usize)
            .order_by("age", Order::Asc),
        PlannerContext::default(),
    );
    let prefix = plan_traversal(
        g().n(NodeRef::all())
            .order_by_multiple(vec![("age", Order::Asc), ("name", Order::Desc)])
            .order_by("age", Order::Asc),
        PlannerContext::default(),
    );
    let range_limited = plan_traversal(
        g().n_with_label_where("User", Predicate::gte("age", 21))
            .order_by("age", Order::Asc)
            .limit(10usize)
            .order_by("age", Order::Asc),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );
    let mismatched = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .order_by("age", Order::Desc),
        PlannerContext::default(),
    );

    let PhysicalOp::Order {
        input,
        plan: OrderPlan::ExplicitSort(keys),
    } = run_op(&direct)
    else {
        panic!("expected one direct order: {:?}", run_op(&direct));
    };
    assert_eq!(keys.as_ref(), &[test_order_key("age", Order::Asc)]);
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::TopN { input, keys, count } = run_op(&limited) else {
        panic!("expected top-n with no outer order: {:?}", run_op(&limited));
    };
    assert_eq!(count.get(), 10);
    assert_eq!(keys.as_ref(), &[test_order_key("age", Order::Asc)]);
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::Order {
        plan: OrderPlan::ExplicitSort(keys),
        ..
    } = run_op(&prefix)
    else {
        panic!("expected prefix-providing order: {:?}", run_op(&prefix));
    };
    assert_eq!(
        keys.as_ref(),
        &[
            test_order_key("age", Order::Asc),
            test_order_key("name", Order::Desc)
        ]
    );

    let PhysicalOp::Order {
        input,
        plan: OrderPlan::RangeIndex { key, .. },
    } = run_op(&range_limited)
    else {
        panic!(
            "expected one range-backed order with no outer order: {:?}",
            run_op(&range_limited)
        );
    };
    assert_eq!(key, &test_order_key("age", Order::Asc));
    assert!(matches!(input.as_ref(), PhysicalOp::Limit { .. }));

    let PhysicalOp::Order {
        input,
        plan: OrderPlan::ExplicitSort(keys),
    } = run_op(&mismatched)
    else {
        panic!("expected outer order for mismatched direction");
    };
    assert_eq!(keys.as_ref(), &[test_order_key("age", Order::Desc)]);
    assert!(matches!(input.as_ref(), PhysicalOp::Order { .. }));
}

#[test]
fn direct_node_order_rewrites_range_index_to_matching_direction() {
    let asc = ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap();
    let desc =
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Desc).unwrap();
    let plan = plan_traversal(
        g().n_where(Predicate::or(vec![Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::gte("age", 21),
        ])]))
        .order_by("age", Order::Desc),
        ctx(builtin_label_indexes()
            .with_node_range(asc)
            .with_node_range(desc)),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&plan)
    else {
        panic!("expected order: {:?}", run_op(&plan));
    };
    let OrderPlan::RangeIndex { key, index_id } = order_plan else {
        panic!("expected range index order plan: {order_plan:?}");
    };
    assert_eq!(key.property.as_ref(), "age");
    assert_eq!(key.order, Order::Desc);
    assert_eq!(index_id.as_ref(), "node_range:User:age:Desc");
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::RangeIndex { key, index, .. })
            if key.property == "age"
                && key.direction == RangeIndexDirection::Desc
                && index.index_id.as_ref() == "node_range:User:age:Desc"
    ));
    assert_decision(
        &plan,
        TracePass::OrderPushdown,
        TraceDecision::RangeIndexOrder,
    );
    assert_no_decision(&plan, TraceDecision::ExplicitSort);
}

#[test]
fn filter_order_rewrites_edge_range_index_to_matching_direction() {
    let asc =
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "since", RangeIndexDirection::Asc).unwrap();
    let desc =
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "since", RangeIndexDirection::Desc).unwrap();
    let plan = plan_traversal(
        g().e_where(Predicate::or(vec![Predicate::and(vec![
            Predicate::eq("$label", "FOLLOWS"),
            Predicate::lte("since", 2024),
        ])]))
        .edge_has("active", true)
        .order_by("since", Order::Desc),
        ctx(builtin_label_indexes()
            .with_edge_range(asc)
            .with_edge_range(desc)),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&plan)
    else {
        panic!("expected order: {:?}", run_op(&plan));
    };
    let OrderPlan::RangeIndex { key, index_id } = order_plan else {
        panic!("expected range index order plan: {order_plan:?}");
    };
    assert_eq!(key.property.as_ref(), "since");
    assert_eq!(key.order, Order::Desc);
    assert_eq!(index_id.as_ref(), "edge_range:FOLLOWS:since:Desc");

    let PhysicalOp::Filter { input, .. } = input.as_ref() else {
        panic!("expected filter above edge range access: {input:?}");
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::EdgeAccess(EdgeAccessPlan::RangeIndex { key, index, .. })
            if key.property == "since"
                && key.direction == RangeIndexDirection::Desc
                && index.index_id.as_ref() == "edge_range:FOLLOWS:since:Desc"
    ));
    assert_decision(
        &plan,
        TracePass::OrderPushdown,
        TraceDecision::RangeIndexOrder,
    );
    assert_no_decision(&plan, TraceDecision::ExplicitSort);
}

#[test]
fn order_does_not_rewrite_range_index_direction_through_bounds_or_variables() {
    let asc = ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap();
    let desc =
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Desc).unwrap();
    let indexes = builtin_label_indexes()
        .with_node_range(asc)
        .with_node_range(desc);

    let bounded = plan_traversal(
        g().n_where(Predicate::or(vec![Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::gte("age", 21),
        ])]))
        .limit(10usize)
        .order_by("age", Order::Desc),
        ctx(indexes.clone()),
    );
    let variable = plan_traversal(
        g().n_where(Predicate::or(vec![Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::gte("age", 21),
        ])]))
        .store("users")
        .order_by("age", Order::Desc),
        ctx(indexes),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&bounded)
    else {
        panic!("expected bounded order: {:?}", run_op(&bounded));
    };
    let OrderPlan::ExplicitSort(keys) = order_plan else {
        panic!("expected explicit sort for bounded input: {order_plan:?}");
    };
    assert_eq!(keys.as_ref()[0].property.as_ref(), "age");
    assert_eq!(keys.as_ref()[0].order, Order::Desc);
    let PhysicalOp::Limit { input, .. } = input.as_ref() else {
        panic!("expected limit under bounded order: {input:?}");
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::RangeIndex { key, index, .. })
            if key.property == "age"
                && key.direction == RangeIndexDirection::Asc
                && index.index_id.as_ref() == "node_range:User:age:Asc"
    ));
    assert_decision(
        &bounded,
        TracePass::OrderPushdown,
        TraceDecision::ExplicitSort,
    );
    assert_no_decision(&bounded, TraceDecision::RangeIndexOrder);

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&variable)
    else {
        panic!("expected variable order: {:?}", run_op(&variable));
    };
    let OrderPlan::ExplicitSort(keys) = order_plan else {
        panic!("expected explicit sort for variable input: {order_plan:?}");
    };
    assert_eq!(keys.as_ref()[0].property.as_ref(), "age");
    assert_eq!(keys.as_ref()[0].order, Order::Desc);
    let input = assert_stream_variable(
        input.as_ref(),
        &StreamVariableOp::Store(NonEmptyString::new("users").unwrap()),
    );
    assert!(matches!(
        input,
        PhysicalOp::NodeAccess(NodeAccessPlan::RangeIndex { key, index, .. })
            if key.property == "age"
                && key.direction == RangeIndexDirection::Asc
                && index.index_id.as_ref() == "node_range:User:age:Asc"
    ));
    assert_decision(
        &variable,
        TracePass::OrderPushdown,
        TraceDecision::ExplicitSort,
    );
    assert_no_decision(&variable, TraceDecision::RangeIndexOrder);
}

#[test]
fn range_direction_matching_contract_rewrites_ascending_and_rejects_other_properties() {
    let range_key =
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Desc).unwrap();
    let matching_order = OrderKey {
        property: NonEmptyString::new("age").unwrap(),
        order: Order::Asc,
    };
    let mismatched_order = OrderKey {
        property: NonEmptyString::new("score").unwrap(),
        order: Order::Asc,
    };

    let rewritten = matching_range_direction_key(&range_key, &matching_order)
        .expect("expected matching property to rewrite direction");
    assert_eq!(rewritten.label.as_ref(), "User");
    assert_eq!(rewritten.property.as_ref(), "age");
    assert_eq!(rewritten.direction, RangeIndexDirection::Asc);
    assert!(matching_range_direction_key(&range_key, &mismatched_order).is_none());
}

#[test]
fn range_index_rewrite_contract_keeps_already_matching_directions() {
    let order_key = test_order_key("age", Order::Asc);
    let node_key =
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap();
    let mut node_range_key = node_key.clone();
    let mut node_index = NodeRangeIndexMeta::try_new("node_range:User:age:Asc").unwrap();
    let node_indexes = builtin_label_indexes().with_node_range(node_key.clone());

    assert_eq!(
        rewrite_node_range_index_order(
            &mut node_range_key,
            &mut node_index,
            &order_key,
            &node_indexes,
        ),
        None
    );
    assert_eq!(node_range_key, node_key);
    assert_eq!(node_index.index_id.as_ref(), "node_range:User:age:Asc");

    let edge_key =
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "since", RangeIndexDirection::Desc).unwrap();
    let mut edge_range_key = edge_key.clone();
    let mut edge_index = EdgeRangeIndexMeta::try_new("edge_range:FOLLOWS:since:Desc").unwrap();
    let edge_indexes = builtin_label_indexes().with_edge_range(edge_key.clone());
    let edge_order_key = test_order_key("since", Order::Desc);

    assert_eq!(
        rewrite_edge_range_index_order(
            &mut edge_range_key,
            &mut edge_index,
            &edge_order_key,
            &edge_indexes,
        ),
        None
    );
    assert_eq!(edge_range_key, edge_key);
    assert_eq!(
        edge_index.index_id.as_ref(),
        "edge_range:FOLLOWS:since:Desc"
    );
}

#[test]
fn range_index_order_survives_order_preserving_stream_wrappers() {
    let plan = plan_traversal(
        g().n_where(Predicate::or(vec![Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::gte("age", 21),
        ])]))
        .has("active", true)
        .skip(1usize)
        .limit(StreamBound::expr(Expr::param("limit")))
        .range(0usize, 5usize)
        .order_by("age", Order::Asc),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&plan)
    else {
        panic!("expected order: {:?}", run_op(&plan));
    };
    let OrderPlan::RangeIndex { key, index_id } = order_plan else {
        panic!("expected range index order plan: {order_plan:?}");
    };
    assert_eq!(key.property.as_ref(), "age");
    assert_eq!(key.order, Order::Asc);
    assert_eq!(index_id.as_ref(), "node_range:User:age:Asc");

    let PhysicalOp::Range { input, .. } = input.as_ref() else {
        panic!("expected range wrapper: {input:?}");
    };
    let PhysicalOp::Limit { input, .. } = input.as_ref() else {
        panic!("expected limit wrapper: {input:?}");
    };
    let PhysicalOp::Skip { input, .. } = input.as_ref() else {
        panic!("expected skip wrapper: {input:?}");
    };
    let PhysicalOp::Filter { input, .. } = input.as_ref() else {
        panic!("expected filter wrapper: {input:?}");
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::RangeIndex { key, .. })
            if key.property == "age" && key.direction == RangeIndexDirection::Asc
    ));
    assert_decision(
        &plan,
        TracePass::OrderPushdown,
        TraceDecision::RangeIndexOrder,
    );
    assert_no_decision(&plan, TraceDecision::ExplicitSort);
}

#[test]
fn range_index_order_survives_order_preserving_variable_wrappers() {
    let plan = plan_traversal(
        g().n_where(Predicate::or(vec![Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::gte("age", 21),
        ])]))
        .as_("seen")
        .store("users")
        .bind("row")
        .within("allowed")
        .without("blocked")
        .order_by("age", Order::Asc),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&plan)
    else {
        panic!("expected order: {:?}", run_op(&plan));
    };
    let OrderPlan::RangeIndex { key, index_id } = order_plan else {
        panic!("expected range index order plan: {order_plan:?}");
    };
    assert_eq!(key.property.as_ref(), "age");
    assert_eq!(key.order, Order::Asc);
    assert_eq!(index_id.as_ref(), "node_range:User:age:Asc");

    let input = assert_stream_variable(
        input.as_ref(),
        &StreamVariableOp::Without(NonEmptyString::new("blocked").unwrap()),
    );
    let input = assert_stream_variable(
        input,
        &StreamVariableOp::Within(NonEmptyString::new("allowed").unwrap()),
    );
    let input = assert_stream_variable(
        input,
        &StreamVariableOp::Bind(NonEmptyString::new("row").unwrap()),
    );
    let input = assert_stream_variable(
        input,
        &StreamVariableOp::Store(NonEmptyString::new("users").unwrap()),
    );
    let input = assert_stream_variable(
        input,
        &StreamVariableOp::As(NonEmptyString::new("seen").unwrap()),
    );
    assert!(matches!(
        input,
        PhysicalOp::NodeAccess(NodeAccessPlan::RangeIndex { key, .. })
            if key.property == "age" && key.direction == RangeIndexDirection::Asc
    ));
    assert_decision(
        &plan,
        TracePass::OrderPushdown,
        TraceDecision::RangeIndexOrder,
    );
    assert_no_decision(&plan, TraceDecision::ExplicitSort);
}

#[test]
fn inject_wrapper_does_not_claim_range_index_order() {
    let plan = plan_traversal(
        g().n_where(Predicate::or(vec![Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::gte("age", 21),
        ])]))
        .inject("extra_users")
        .order_by("age", Order::Asc),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&plan)
    else {
        panic!("expected order: {:?}", run_op(&plan));
    };
    let OrderPlan::ExplicitSort(keys) = order_plan else {
        panic!("expected explicit sort order plan: {order_plan:?}");
    };
    assert_eq!(keys.as_ref()[0].property.as_ref(), "age");
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::Variable(VariablePlan::Stream {
            op: StreamVariableOp::Inject(variable),
            ..
        }) if variable.as_ref() == "extra_users"
    ));
    assert_decision(&plan, TracePass::OrderPushdown, TraceDecision::ExplicitSort);
    assert_no_decision(&plan, TraceDecision::RangeIndexOrder);
}

#[test]
fn duplicate_free_distinct_input_uses_range_index_order() {
    let plan = plan_traversal(
        g().n_where(Predicate::or(vec![Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::gte("age", 21),
        ])]))
        .dedup()
        .order_by("age", Order::Asc),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&plan)
    else {
        panic!("expected order: {:?}", run_op(&plan));
    };
    let OrderPlan::RangeIndex { key, index_id } = order_plan else {
        panic!("expected range index order plan: {order_plan:?}");
    };
    assert_eq!(key.property.as_ref(), "age");
    assert_eq!(key.order, Order::Asc);
    assert_eq!(index_id.as_ref(), "node_range:User:age:Asc");
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::RangeIndex { .. })
    ));
    assert_decision(
        &plan,
        TracePass::OrderPushdown,
        TraceDecision::RangeIndexOrder,
    );
    assert_no_decision(&plan, TraceDecision::ExplicitSort);
}

#[test]
fn direct_range_index_with_mismatched_direction_keeps_explicit_sort() {
    let plan = plan_traversal(
        g().n_where(Predicate::or(vec![Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::gte("age", 21),
        ])]))
        .order_by("age", Order::Desc),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&plan)
    else {
        panic!("expected order: {:?}", run_op(&plan));
    };
    let OrderPlan::ExplicitSort(keys) = order_plan else {
        panic!("expected explicit sort order plan: {order_plan:?}");
    };
    assert_eq!(keys.as_ref()[0].property.as_ref(), "age");
    assert_eq!(keys.as_ref()[0].order, Order::Desc);
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::RangeIndex { key, .. })
            if key.property == "age" && key.direction == RangeIndexDirection::Asc
    ));
    assert_decision(&plan, TracePass::OrderPushdown, TraceDecision::ExplicitSort);
    assert_no_decision(&plan, TraceDecision::RangeIndexOrder);
}

#[test]
fn direct_edge_range_index_with_mismatched_direction_keeps_explicit_sort() {
    let plan = plan_traversal(
        g().e_where(Predicate::or(vec![Predicate::and(vec![
            Predicate::eq("$label", "FOLLOWS"),
            Predicate::lte("since", 2024),
        ])]))
        .order_by("since", Order::Desc),
        ctx(builtin_label_indexes().with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "since", RangeIndexDirection::Asc)
                .unwrap(),
        )),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&plan)
    else {
        panic!("expected order: {:?}", run_op(&plan));
    };
    let OrderPlan::ExplicitSort(keys) = order_plan else {
        panic!("expected explicit sort order plan: {order_plan:?}");
    };
    assert_eq!(keys.as_ref()[0].property.as_ref(), "since");
    assert_eq!(keys.as_ref()[0].order, Order::Desc);
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::EdgeAccess(EdgeAccessPlan::RangeIndex { key, .. })
            if key.property == "since" && key.direction == RangeIndexDirection::Asc
    ));
    assert_decision(&plan, TracePass::OrderPushdown, TraceDecision::ExplicitSort);
    assert_no_decision(&plan, TraceDecision::RangeIndexOrder);
}

#[test]
fn direct_range_index_with_mismatched_order_by_multiple_keeps_explicit_sort() {
    let plan = plan_traversal(
        g().n_where(Predicate::or(vec![Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::gte("age", 21),
        ])]))
        .order_by_multiple(vec![("age", Order::Desc)]),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&plan)
    else {
        panic!("expected order: {:?}", run_op(&plan));
    };
    let OrderPlan::ExplicitSort(keys) = order_plan else {
        panic!("expected explicit sort order plan: {order_plan:?}");
    };
    assert_eq!(keys.as_ref()[0].property.as_ref(), "age");
    assert_eq!(keys.as_ref()[0].order, Order::Desc);
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::RangeIndex { key, .. })
            if key.property == "age" && key.direction == RangeIndexDirection::Asc
    ));
    assert_decision(&plan, TracePass::OrderPushdown, TraceDecision::ExplicitSort);
    assert_no_decision(&plan, TraceDecision::RangeIndexOrder);
}

#[test]
fn explicit_sort_literal_limit_uses_top_n() {
    let plan = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .limit(3usize),
        PlannerContext::default(),
    );

    let PhysicalOp::TopN { input, keys, count } = run_op(&plan) else {
        panic!("expected top-n: {:?}", run_op(&plan));
    };
    assert_eq!(count.get(), 3);
    assert_eq!(keys.as_ref()[0].property.as_ref(), "age");
    assert_eq!(keys.as_ref()[0].order, Order::Asc);
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));
    assert_decision(&plan, TracePass::OrderPushdown, TraceDecision::ExplicitSort);
    assert_decision(&plan, TracePass::BoundPushdown, TraceDecision::Limit);
    assert_no_decision(&plan, TraceDecision::RangeIndexOrder);
}

#[test]
fn explicit_sort_dynamic_limit_keeps_limit_order_boundary() {
    let plan = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .limit(StreamBound::expr(Expr::param("limit"))),
        PlannerContext::default(),
    );

    let PhysicalOp::Limit { input, count } = run_op(&plan) else {
        panic!("expected dynamic limit: {:?}", run_op(&plan));
    };
    assert!(matches!(count, StreamBoundPlan::Expr(_)));
    let PhysicalOp::Order {
        input,
        plan: OrderPlan::ExplicitSort(keys),
    } = input.as_ref()
    else {
        panic!("expected explicit order under dynamic limit: {input:?}");
    };
    assert_eq!(keys.as_ref()[0].property.as_ref(), "age");
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));
}

#[test]
fn top_n_literal_limits_coalesce_and_dynamic_limits_stay_outer() {
    let literal = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .limit(10usize)
            .limit(3usize),
        PlannerContext::default(),
    );
    let dynamic = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .limit(10usize)
            .limit(StreamBound::expr(Expr::param("limit"))),
        PlannerContext::default(),
    );

    let PhysicalOp::TopN { count, .. } = run_op(&literal) else {
        panic!("expected coalesced top-n: {:?}", run_op(&literal));
    };
    assert_eq!(count.get(), 3);

    let PhysicalOp::Limit { input, count } = run_op(&dynamic) else {
        panic!("expected dynamic limit above top-n: {:?}", run_op(&dynamic));
    };
    assert!(matches!(count, StreamBoundPlan::Expr(_)));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::TopN { count, .. } if count.get() == 10
    ));
}

#[test]
fn explicit_sort_dynamic_limit_literal_limit_pushes_top_n_required_prefix() {
    let plan = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .limit(StreamBound::expr(Expr::param("inner_limit")))
            .limit(5usize),
        PlannerContext::default(),
    );

    let PhysicalOp::Limit { input, count } = run_op(&plan) else {
        panic!("expected outer limit: {:?}", run_op(&plan));
    };
    assert_eq!(count, &StreamBoundPlan::Literal(5));
    let PhysicalOp::Limit { input, count } = input.as_ref() else {
        panic!("expected dynamic inner limit: {input:?}");
    };
    assert!(matches!(count, StreamBoundPlan::Expr(_)));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::TopN { count, .. } if count.get() == 5
    ));
}

#[test]
fn explicit_sort_literal_range_uses_top_n_required_prefix() {
    let plan = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .range(5usize, 12usize),
        PlannerContext::default(),
    );

    let (input, range) = literal_range(&plan);
    assert_eq!(range, StreamLiteralRange::new(5, 12).unwrap());
    let PhysicalOp::TopN { input, keys, count } = input else {
        panic!("expected top-n under range: {input:?}");
    };
    assert_eq!(count.get(), 12);
    assert_eq!(keys.as_ref()[0].property.as_ref(), "age");
    assert_eq!(keys.as_ref()[0].order, Order::Asc);
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));
}

#[test]
fn explicit_sort_skip_limit_uses_top_n_required_prefix() {
    let plan = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .skip(5usize)
            .limit(7usize),
        PlannerContext::default(),
    );

    let (input, range) = literal_range(&plan);
    assert_eq!(range, StreamLiteralRange::new(5, 12).unwrap());
    assert!(matches!(
        input,
        PhysicalOp::TopN { count, .. } if count.get() == 12
    ));
}

#[test]
fn top_n_literal_range_tightens_existing_prefix_and_dynamic_range_stays_outer() {
    let literal = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .limit(100usize)
            .range(10usize, 20usize),
        PlannerContext::default(),
    );
    let dynamic = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .limit(100usize)
            .range(10usize, StreamBound::expr(Expr::param("end"))),
        PlannerContext::default(),
    );

    let (input, range) = literal_range(&literal);
    assert_eq!(range, StreamLiteralRange::new(10, 20).unwrap());
    assert!(matches!(
        input,
        PhysicalOp::TopN { count, .. } if count.get() == 20
    ));

    let PhysicalOp::Range {
        input,
        range: dynamic_range,
    } = run_op(&dynamic)
    else {
        panic!("expected dynamic range over top-n: {:?}", run_op(&dynamic));
    };
    assert!(matches!(dynamic_range, StreamRangePlan::Dynamic(_)));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::TopN { count, .. } if count.get() == 100
    ));
}

#[test]
fn explicit_sort_dynamic_start_literal_end_uses_top_n_required_prefix() {
    let plan = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .range(StreamBound::expr(Expr::param("start")), 12usize),
        PlannerContext::default(),
    );

    let PhysicalOp::Range { input, range } = run_op(&plan) else {
        panic!("expected dynamic range over top-n: {:?}", run_op(&plan));
    };
    assert!(matches!(range, StreamRangePlan::Dynamic(_)));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::TopN { count, .. } if count.get() == 12
    ));
}

#[test]
fn explicit_sort_skip_dynamic_start_literal_end_uses_top_n_required_prefix() {
    let plan = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .skip(5usize)
            .range(StreamBound::expr(Expr::param("start")), 12usize),
        PlannerContext::default(),
    );
    let limited = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .limit(100usize)
            .skip(5usize)
            .range(StreamBound::expr(Expr::param("start")), 12usize),
        PlannerContext::default(),
    );

    for plan in [&plan, &limited] {
        let PhysicalOp::Range { input, range } = run_op(plan) else {
            panic!("expected dynamic range: {:?}", run_op(plan));
        };
        assert!(matches!(range, StreamRangePlan::Dynamic(_)));
        let PhysicalOp::Skip { input, count } = input.as_ref() else {
            panic!("expected skip below range: {input:?}");
        };
        assert_eq!(count, &StreamBoundPlan::Literal(5));
        assert!(matches!(
            input.as_ref(),
            PhysicalOp::TopN { count, .. } if count.get() == 17
        ));
    }
}

#[test]
fn explicit_sort_dynamic_end_literal_limit_uses_top_n_required_prefix() {
    let plan = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .range(10usize, StreamBound::expr(Expr::param("end")))
            .limit(5usize),
        PlannerContext::default(),
    );
    let limited = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .limit(100usize)
            .range(10usize, StreamBound::expr(Expr::param("end")))
            .limit(5usize),
        PlannerContext::default(),
    );

    for plan in [&plan, &limited] {
        let PhysicalOp::Limit { input, count } = run_op(plan) else {
            panic!("expected limit: {:?}", run_op(plan));
        };
        assert_eq!(count, &StreamBoundPlan::Literal(5));
        let PhysicalOp::Range { input, range } = input.as_ref() else {
            panic!("expected range below limit: {input:?}");
        };
        assert!(matches!(range, StreamRangePlan::Dynamic(_)));
        assert!(matches!(
            input.as_ref(),
            PhysicalOp::TopN { count, .. } if count.get() == 15
        ));
    }
}

#[test]
fn explicit_sort_skip_dynamic_end_literal_limit_composes_top_n_required_prefix() {
    let plan = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .skip(5usize)
            .range(10usize, StreamBound::expr(Expr::param("end")))
            .limit(3usize),
        PlannerContext::default(),
    );
    let limited = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .limit(100usize)
            .skip(5usize)
            .range(10usize, StreamBound::expr(Expr::param("end")))
            .limit(3usize),
        PlannerContext::default(),
    );

    for plan in [&plan, &limited] {
        let PhysicalOp::Limit { input, count } = run_op(plan) else {
            panic!("expected limit: {:?}", run_op(plan));
        };
        assert_eq!(count, &StreamBoundPlan::Literal(3));
        let PhysicalOp::Range { input, range } = input.as_ref() else {
            panic!("expected range below limit: {input:?}");
        };
        assert!(matches!(range, StreamRangePlan::Dynamic(_)));
        let PhysicalOp::Skip { input, count } = input.as_ref() else {
            panic!("expected skip below range: {input:?}");
        };
        assert_eq!(count, &StreamBoundPlan::Literal(5));
        assert!(matches!(
            input.as_ref(),
            PhysicalOp::TopN { count, .. } if count.get() == 18
        ));
    }
}

#[test]
fn explicit_sort_stacked_ranges_compose_top_n_required_prefix() {
    let dynamic_start_inner = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .range(2usize, StreamBound::expr(Expr::param("mid")))
            .range(3usize, StreamBound::expr(Expr::param("end")))
            .limit(4usize),
        PlannerContext::default(),
    );
    let literal_inner = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .range(2usize, 12usize)
            .range(3usize, StreamBound::expr(Expr::param("end")))
            .limit(4usize),
        PlannerContext::default(),
    );
    let static_end_inner = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .range(StreamBound::expr(Expr::param("start")), 10usize)
            .range(3usize, StreamBound::expr(Expr::param("end")))
            .limit(4usize),
        PlannerContext::default(),
    );

    for (plan, expected_prefix) in [
        (&dynamic_start_inner, 9),
        (&literal_inner, 9),
        (&static_end_inner, 10),
    ] {
        let PhysicalOp::Limit { input, count } = run_op(plan) else {
            panic!("expected limit: {:?}", run_op(plan));
        };
        assert_eq!(count, &StreamBoundPlan::Literal(4));
        let PhysicalOp::Range { input, .. } = input.as_ref() else {
            panic!("expected outer range below limit: {input:?}");
        };
        let PhysicalOp::Range { input, .. } = input.as_ref() else {
            panic!("expected inner range below outer range: {input:?}");
        };
        assert!(matches!(
            input.as_ref(),
            PhysicalOp::TopN { count, .. } if count.get() == expected_prefix
        ));
    }
}

#[test]
fn explicit_sort_dynamic_limit_static_range_pushes_top_n_required_prefix() {
    let plan = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .limit(StreamBound::expr(Expr::param("limit")))
            .range(2usize, 8usize),
        PlannerContext::default(),
    );

    let PhysicalOp::Range { input, range } = run_op(&plan) else {
        panic!("expected outer range: {:?}", run_op(&plan));
    };
    assert_eq!(
        range,
        &StreamRangePlan::new(StreamBound::Literal(2), StreamBound::Literal(8)).unwrap()
    );
    let PhysicalOp::Limit { input, count } = input.as_ref() else {
        panic!("expected dynamic limit below range: {input:?}");
    };
    assert!(matches!(count, StreamBoundPlan::Expr(_)));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::TopN { count, .. } if count.get() == 8
    ));
}

#[test]
fn explicit_sort_dynamic_limit_dynamic_range_literal_limit_pushes_top_n_required_prefix() {
    let plan = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .limit(StreamBound::expr(Expr::param("inner_limit")))
            .range(10usize, StreamBound::expr(Expr::param("end")))
            .limit(5usize),
        PlannerContext::default(),
    );

    let PhysicalOp::Limit { input, count } = run_op(&plan) else {
        panic!("expected outer limit: {:?}", run_op(&plan));
    };
    assert_eq!(count, &StreamBoundPlan::Literal(5));
    let PhysicalOp::Range { input, range } = input.as_ref() else {
        panic!("expected range below outer limit: {input:?}");
    };
    assert!(matches!(range, StreamRangePlan::Dynamic(_)));
    let PhysicalOp::Limit { input, count } = input.as_ref() else {
        panic!("expected dynamic limit below range: {input:?}");
    };
    assert!(matches!(count, StreamBoundPlan::Expr(_)));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::TopN { count, .. } if count.get() == 15
    ));
}

#[test]
fn required_prefix_pushes_through_literal_limits_when_input_can_be_bounded() {
    let bounded = apply_required_prefix(
        PhysicalOp::Limit {
            input: Box::new(explicit_sort_op()),
            count: StreamBoundPlan::Literal(20),
        },
        NonZeroUsize::new(15).unwrap(),
    );
    let smaller_limit = apply_required_prefix(
        PhysicalOp::Limit {
            input: Box::new(explicit_sort_op()),
            count: StreamBoundPlan::Literal(5),
        },
        NonZeroUsize::new(15).unwrap(),
    );
    let zero_limit = apply_required_prefix(
        PhysicalOp::Limit {
            input: Box::new(explicit_sort_op()),
            count: StreamBoundPlan::Literal(0),
        },
        NonZeroUsize::new(15).unwrap(),
    );

    let PhysicalOp::Limit { input, count } = bounded else {
        panic!("expected limit");
    };
    assert_eq!(count, StreamBoundPlan::Literal(20));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::TopN { count, .. } if count.get() == 15
    ));

    let PhysicalOp::Limit { input, count } = smaller_limit else {
        panic!("expected limit");
    };
    assert_eq!(count, StreamBoundPlan::Literal(5));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::TopN { count, .. } if count.get() == 5
    ));

    let PhysicalOp::Limit { input, count } = zero_limit else {
        panic!("expected limit");
    };
    assert_eq!(count, StreamBoundPlan::Literal(0));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::Order {
            plan: OrderPlan::ExplicitSort(_),
            ..
        }
    ));
}

#[test]
fn required_prefix_pushes_through_row_preserving_projections_only() {
    let row_preserving = apply_required_prefix(
        PhysicalOp::Project {
            input: Box::new(explicit_sort_op()),
            projection: ProjectionPlan::Id,
        },
        NonZeroUsize::new(7).unwrap(),
    );
    let fanout = apply_required_prefix(
        PhysicalOp::Project {
            input: Box::new(explicit_sort_op()),
            projection: ProjectionPlan::Values(
                PropertyNames::new(AtLeast::<_, 1>::from_one(
                    NonEmptyString::new("name").unwrap(),
                ))
                .unwrap(),
            ),
        },
        NonZeroUsize::new(7).unwrap(),
    );

    let PhysicalOp::Project {
        input,
        projection: ProjectionPlan::Id,
    } = row_preserving
    else {
        panic!("expected id projection");
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::TopN { count, .. } if count.get() == 7
    ));

    let PhysicalOp::Project {
        input,
        projection: ProjectionPlan::Values(_),
    } = fanout
    else {
        panic!("expected values projection");
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::Order {
            plan: OrderPlan::ExplicitSort(_),
            ..
        }
    ));
}

#[test]
fn explicit_sort_dynamic_range_keeps_order_boundary() {
    let plan = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .range(5usize, StreamBound::expr(Expr::param("end"))),
        PlannerContext::default(),
    );

    let PhysicalOp::Range { input, range } = run_op(&plan) else {
        panic!("expected dynamic range: {:?}", run_op(&plan));
    };
    assert!(matches!(range, StreamRangePlan::Dynamic(_)));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::Order {
            plan: OrderPlan::ExplicitSort(_),
            ..
        }
    ));
}

#[test]
fn zero_limit_over_non_empty_provable_explicit_sort_keeps_limit_order_boundary() {
    let input = PhysicalOp::Order {
        input: Box::new(PhysicalOp::Variable(VariablePlan::SourceInject {
            variable: NonEmptyString::new("rows").unwrap(),
        })),
        plan: OrderPlan::ExplicitSort(OrderKeys::from(test_order_key("age", Order::Asc))),
    };

    let PhysicalOp::Limit { input, count } = apply_limit(input, StreamBoundPlan::Literal(0)) else {
        panic!("expected zero limit above explicit sort");
    };
    assert_eq!(count, StreamBoundPlan::Literal(0));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::Order {
            plan: OrderPlan::ExplicitSort(_),
            ..
        }
    ));
}

#[test]
fn count_and_exists_strip_top_n_or_limit_only_when_semantics_allow_it() {
    let count_over_top_n = plan_traversal(
        g().n(NodeRef::all())
            .order_by("age", Order::Asc)
            .limit(10usize)
            .count(),
        PlannerContext::default(),
    );
    let count_over_dynamic_limit = plan_traversal(
        g().n(NodeRef::all())
            .limit(StreamBound::expr(Expr::param("limit")))
            .count(),
        PlannerContext::default(),
    );
    let exists_over_dynamic_limit = plan_traversal(
        g().n(NodeRef::all())
            .limit(StreamBound::expr(Expr::param("limit")))
            .exists(),
        PlannerContext::default(),
    );

    let PhysicalOp::Project {
        input,
        projection: ProjectionPlan::Exists,
    } = run_op(&count_over_top_n)
    else {
        panic!("expected count projection: {:?}", run_op(&count_over_top_n));
    };
    let PhysicalOp::Limit { input, count } = input.as_ref() else {
        panic!("expected limit under count after stripping top-n order: {input:?}");
    };
    assert_eq!(count, &StreamBoundPlan::Literal(10));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::Project {
        input,
        projection: ProjectionPlan::Exists,
    } = run_op(&count_over_dynamic_limit)
    else {
        panic!(
            "expected dynamic limit count projection: {:?}",
            run_op(&count_over_dynamic_limit)
        );
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::Limit {
            count: StreamBoundPlan::Expr(_),
            ..
        }
    ));

    let PhysicalOp::Project {
        input,
        projection: ProjectionPlan::Exists,
    } = run_op(&exists_over_dynamic_limit)
    else {
        panic!(
            "expected exists projection over dynamic limit: {:?}",
            run_op(&exists_over_dynamic_limit)
        );
    };
    let PhysicalOp::Limit { input, count } = input.as_ref() else {
        panic!("expected exists limit: {input:?}");
    };
    assert_eq!(count, &StreamBoundPlan::Literal(1));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::Limit {
            count: StreamBoundPlan::Expr(_),
            ..
        }
    ));
}

#[test]
fn dynamic_range_literal_singleton_end_skips_explicit_sort() {
    let plan = plan_traversal(
        g().n(NodeRef::all())
            .range(StreamBound::expr(Expr::param("start")), 1usize)
            .order_by("age", Order::Asc),
        PlannerContext::default(),
    );

    let PhysicalOp::Range { input, range } = run_op(&plan) else {
        panic!("expected range without explicit sort: {:?}", run_op(&plan));
    };
    assert!(matches!(range, StreamRangePlan::Dynamic(_)));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));
    assert_no_decision(&plan, TraceDecision::ExplicitSort);
}

#[test]
fn top_n_order_satisfaction_accepts_equivalent_range_key_requests() {
    let key = test_order_key("age", Order::Asc);
    let input = PhysicalOp::TopN {
        input: Box::new(PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)),
        keys: OrderKeys::from(key.clone()),
        count: NonZeroUsize::new(10).unwrap(),
    };

    assert!(physical_order_satisfies(
        &input,
        &OrderPlan::RangeIndex {
            key: key.clone(),
            index_id: NonEmptyString::new("node_range:User:age:Asc").unwrap(),
        },
    ));

    let distinct = PhysicalOp::Distinct {
        input: Box::new(input),
    };
    assert!(physical_order_satisfies(
        &distinct,
        &OrderPlan::ExplicitSort(OrderKeys::from(key)),
    ));
}

#[test]
fn range_index_order_commutes_bounds_below_order_marker() {
    let indexes = builtin_label_indexes().with_node_range(
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
    );
    let limited = plan_traversal(
        g().n_with_label_where("User", Predicate::gte("age", 21))
            .order_by("age", Order::Asc)
            .limit(10usize),
        ctx(indexes.clone()),
    );
    let skipped = plan_traversal(
        g().n_with_label_where("User", Predicate::gte("age", 21))
            .order_by("age", Order::Asc)
            .skip(2usize),
        ctx(indexes.clone()),
    );
    let ranged = plan_traversal(
        g().n_with_label_where("User", Predicate::gte("age", 21))
            .order_by("age", Order::Asc)
            .range(2usize, 6usize),
        ctx(indexes),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&limited)
    else {
        panic!("expected range-index order: {:?}", run_op(&limited));
    };
    let OrderPlan::RangeIndex { key, index_id } = order_plan else {
        panic!("expected range index order plan: {order_plan:?}");
    };
    assert_eq!(key.property.as_ref(), "age");
    assert_eq!(key.order, Order::Asc);
    assert_eq!(index_id.as_ref(), "node_range:User:age:Asc");
    let PhysicalOp::Limit { input, count } = input.as_ref() else {
        panic!("expected limit under range-index order: {input:?}");
    };
    assert_eq!(count, &StreamBound::Literal(10));
    assert!(matches!(input.as_ref(), PhysicalOp::NodeAccess(_)));
    assert_decision(&limited, TracePass::BoundPushdown, TraceDecision::Limit);
    assert_decision(
        &limited,
        TracePass::OrderPushdown,
        TraceDecision::RangeIndexOrder,
    );
    assert_no_decision(&limited, TraceDecision::ExplicitSort);
    assert_decision(
        &limited,
        TracePass::AccessPath,
        TraceDecision::NodeRangeIndex,
    );

    let PhysicalOp::Order {
        input,
        plan: OrderPlan::RangeIndex { .. },
    } = run_op(&skipped)
    else {
        panic!(
            "expected range-index order over skip: {:?}",
            run_op(&skipped)
        );
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::Skip {
            input,
            count: StreamBoundPlan::Literal(2),
        } if matches!(input.as_ref(), PhysicalOp::NodeAccess(_))
    ));

    let PhysicalOp::Order {
        input,
        plan: OrderPlan::RangeIndex { .. },
    } = run_op(&ranged)
    else {
        panic!(
            "expected range-index order over range: {:?}",
            run_op(&ranged)
        );
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::Range {
            input,
            range,
        } if matches!(input.as_ref(), PhysicalOp::NodeAccess(_))
            && range == &StreamRangePlan::new(
                StreamBound::Literal(2),
                StreamBound::Literal(6),
            )
            .unwrap()
    ));
}

#[test]
fn filters_after_range_index_order_preserve_order_when_access_still_proves_it() {
    let node = plan_traversal(
        g().n_with_label_where("User", Predicate::gte("age", 21))
            .order_by("age", Order::Asc)
            .where_(Predicate::lt("age", 65)),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );
    let edge = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::gte("weight", 10))
            .order_by("weight", Order::Asc)
            .where_(Predicate::lt("weight", 100)),
        ctx(builtin_label_indexes().with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
        )),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&node)
    else {
        panic!("expected node order: {:?}", run_op(&node));
    };
    let OrderPlan::RangeIndex { key, index_id } = order_plan else {
        panic!("expected node range index order: {order_plan:?}");
    };
    assert_eq!(key.property.as_ref(), "age");
    assert_eq!(key.order, Order::Asc);
    assert_eq!(index_id.as_ref(), "node_range:User:age:Asc");
    assert_node_range(
        &node_candidate_sources(match input.as_ref() {
            PhysicalOp::NodeAccess(access) => access,
            other => panic!("expected node access under order: {other:?}"),
        }),
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Between(
            IndexBetweenRange::new(
                IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(21)).unwrap()),
                IndexBound::Exclusive(RangeIndexValue::literal(PropertyValue::from(65)).unwrap()),
            )
            .unwrap(),
        ),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&edge)
    else {
        panic!("expected edge order: {:?}", run_op(&edge));
    };
    let OrderPlan::RangeIndex { key, index_id } = order_plan else {
        panic!("expected edge range index order: {order_plan:?}");
    };
    assert_eq!(key.property.as_ref(), "weight");
    assert_eq!(key.order, Order::Asc);
    assert_eq!(index_id.as_ref(), "edge_range:FOLLOWS:weight:Asc");
    assert_edge_range(
        &edge_candidate_sources(match input.as_ref() {
            PhysicalOp::EdgeAccess(access) => access,
            other => panic!("expected edge access under order: {other:?}"),
        }),
        ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc).unwrap(),
        IndexRange::Between(
            IndexBetweenRange::new(
                IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(10)).unwrap()),
                IndexBound::Exclusive(RangeIndexValue::literal(PropertyValue::from(100)).unwrap()),
            )
            .unwrap(),
        ),
    );

    assert_no_decision(&node, TraceDecision::ResidualFilter);
    assert_no_decision(&edge, TraceDecision::ResidualFilter);
}

#[test]
fn filters_after_range_index_order_sort_after_intersection_pushdown() {
    let node = plan_traversal(
        g().n_with_label_where("User", Predicate::gte("age", 21))
            .order_by("age", Order::Asc)
            .has("tenant_id", "acme"),
        ctx(builtin_label_indexes()
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
            )
            .with_node_eq(ScopedPropertyKey::try_new("User", "tenant_id").unwrap())),
    );
    let edge = plan_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::gte("weight", 10))
            .order_by("weight", Order::Asc)
            .edge_has("status", "active"),
        ctx(builtin_label_indexes()
            .with_edge_range(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                    .unwrap(),
            )
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&node)
    else {
        panic!(
            "expected node explicit sort after pushed filter: {:?}",
            run_op(&node)
        );
    };
    let OrderPlan::ExplicitSort(keys) = order_plan else {
        panic!("expected node explicit sort: {order_plan:?}");
    };
    assert_eq!(keys.as_ref()[0].property.as_ref(), "age");
    assert_eq!(keys.as_ref()[0].order, Order::Asc);
    let PhysicalOp::NodeAccess(access) = input.as_ref() else {
        panic!("expected node access under explicit sort: {input:?}");
    };
    let node_sources = node_candidate_sources(access);
    assert_eq!(node_sources.len(), 2);
    assert_node_eq(
        &node_sources,
        "User",
        "tenant_id",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("acme")).unwrap()),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&edge)
    else {
        panic!(
            "expected edge explicit sort after pushed filter: {:?}",
            run_op(&edge)
        );
    };
    let OrderPlan::ExplicitSort(keys) = order_plan else {
        panic!("expected edge explicit sort: {order_plan:?}");
    };
    assert_eq!(keys.as_ref()[0].property.as_ref(), "weight");
    assert_eq!(keys.as_ref()[0].order, Order::Asc);
    let PhysicalOp::EdgeAccess(access) = input.as_ref() else {
        panic!("expected edge access under explicit sort: {input:?}");
    };
    let edge_sources = edge_candidate_sources(access);
    assert_eq!(edge_sources.len(), 2);
    assert_edge_eq(
        &edge_sources,
        "FOLLOWS",
        "status",
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("active")).unwrap()),
    );

    assert_decision(&node, TracePass::AccessPath, TraceDecision::NodeIntersect);
    assert_decision(&edge, TracePass::AccessPath, TraceDecision::EdgeIntersect);
    assert_decision(&node, TracePass::OrderPushdown, TraceDecision::ExplicitSort);
    assert_decision(&edge, TracePass::OrderPushdown, TraceDecision::ExplicitSort);
    assert_no_decision(&node, TraceDecision::ResidualFilter);
    assert_no_decision(&edge, TraceDecision::ResidualFilter);
}

#[test]
fn filters_after_range_index_order_commute_below_order_without_access_improvement() {
    let plan = plan_traversal(
        g().n_with_label_where("User", Predicate::gte("age", 21))
            .order_by("age", Order::Asc)
            .where_(Predicate::eq("unindexed", "value")),
        ctx(builtin_label_indexes().with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )),
    );

    let PhysicalOp::Order {
        input,
        plan: order_plan,
    } = run_op(&plan)
    else {
        panic!(
            "expected range order above residual filter: {:?}",
            run_op(&plan)
        );
    };
    let OrderPlan::RangeIndex { key, index_id } = order_plan else {
        panic!("expected range index order plan: {order_plan:?}");
    };
    assert_eq!(key.property.as_ref(), "age");
    assert_eq!(key.order, Order::Asc);
    assert_eq!(index_id.as_ref(), "node_range:User:age:Asc");
    let PhysicalOp::Filter {
        input,
        plan: FilterPlan::Residual { predicate },
    } = input.as_ref()
    else {
        panic!("expected residual filter below range order: {input:?}");
    };
    assert_eq!(predicate.as_ref(), &Predicate::eq("unindexed", "value"));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::RangeIndex { key, .. })
            if key.property == "age"
    ));
    assert_decision(
        &plan,
        TracePass::PredicateIndex,
        TraceDecision::ResidualFilter,
    );
}

#[test]
fn adjacent_literal_limits_coalesce_to_the_tightest_bound() {
    let outer_tighter = plan_traversal(
        g().n(NodeRef::all()).limit(10usize).limit(3usize),
        PlannerContext::default(),
    );
    let inner_tighter = plan_traversal(
        g().n(NodeRef::all()).limit(2usize).limit(9usize),
        PlannerContext::default(),
    );
    let dynamic_outer = plan_traversal(
        g().n(NodeRef::all())
            .limit(2usize)
            .limit(StreamBound::expr(Expr::param("limit"))),
        PlannerContext::default(),
    );

    let (input, count) = literal_limit(&outer_tighter);
    assert_eq!(count, 3);
    assert!(matches!(
        input,
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let (input, count) = literal_limit(&inner_tighter);
    assert_eq!(count, 2);
    assert!(matches!(
        input,
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::Limit {
        input,
        count: outer_count,
    } = run_op(&dynamic_outer)
    else {
        panic!("expected outer dynamic limit: {:?}", run_op(&dynamic_outer));
    };
    assert!(matches!(outer_count, StreamBoundPlan::Expr(_)));
    let PhysicalOp::Limit {
        input,
        count: inner_count,
    } = input.as_ref()
    else {
        panic!("expected literal inner limit: {input:?}");
    };
    assert_eq!(inner_count, &StreamBoundPlan::Literal(2));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));
}

#[test]
fn direct_zero_limits_and_empty_ranges_plan_empty_access() {
    let node_zero_limit = plan_traversal(
        g().n(NodeRef::all()).limit(0usize),
        PlannerContext::default(),
    );
    let edge_zero_limit = plan_traversal(g().e([3u64, 4]).limit(0usize), PlannerContext::default());
    let node_empty_range = plan_traversal(
        g().n(NodeRef::all()).range(4usize, 4usize),
        PlannerContext::default(),
    );
    let edge_empty_range = plan_traversal(
        g().e_with_label("FOLLOWS").range(4usize, 4usize),
        PlannerContext::default(),
    );
    let node_wrapped_empty = plan_traversal(
        g().n(NodeRef::all()).limit(0usize).skip(3usize),
        PlannerContext::default(),
    );
    let edge_wrapped_empty = plan_traversal(
        g().e([3u64, 4]).range(0usize, 0usize).limit(3usize),
        PlannerContext::default(),
    );

    assert_node_empty(&node_zero_limit);
    assert_edge_empty(&edge_zero_limit);
    assert_node_empty(&node_empty_range);
    assert_edge_empty(&edge_empty_range);
    assert_node_empty(&node_wrapped_empty);
    assert_edge_empty(&edge_wrapped_empty);
}

#[test]
fn literal_bounds_use_hard_cardinality_upper_bounds() {
    let point_limit_noop = plan_traversal(
        g().n([1u64, 2, 3]).limit(5usize),
        ctx(IndexCatalogSnapshot::default()),
    );
    let point_limit_narrowed = plan_traversal(
        g().n([1u64, 2, 3]).limit(2usize),
        ctx(IndexCatalogSnapshot::default()),
    );
    let point_skip_narrowed = plan_traversal(
        g().n([1u64, 2, 3]).skip(1usize),
        ctx(IndexCatalogSnapshot::default()),
    );
    let point_range_clamped = plan_traversal(
        g().n([1u64, 2, 3]).range(1usize, 9usize),
        ctx(IndexCatalogSnapshot::default()),
    );
    let edge_range_narrowed = plan_traversal(
        g().e([7u64, 9, 11]).range(1usize, 3usize),
        ctx(IndexCatalogSnapshot::default()),
    );
    let filtered_edge_skip_empty = plan_traversal(
        g().e([7u64, 9]).edge_has("active", true).skip(2usize),
        ctx(IndexCatalogSnapshot::default()),
    );
    let variable_filter_skip_empty = plan_traversal(
        g().n([1u64, 2]).within("allowed").skip(2usize),
        ctx(IndexCatalogSnapshot::default()),
    );
    let variable_store_range = plan_traversal(
        g().n([1u64]).store("seed").range(1usize, 2usize),
        ctx(IndexCatalogSnapshot::default()),
    );

    let unique = ScopedPropertyKey::try_new("User", "id").unwrap();
    let mut indexes = builtin_label_indexes();
    indexes.node_eq.insert(
        unique,
        NodeEqualityIndexMeta::try_new("node_eq:User:id")
            .unwrap()
            .with_uniqueness(IndexUniqueness::Unique),
    );
    let unique_skip_empty = plan_traversal(
        g().n_with_label_where("User", Predicate::eq("id", 7))
            .skip(1usize),
        ctx(indexes.clone()),
    );
    let unique_null_skip = plan_traversal(
        g().n_with_label_where("User", Predicate::eq("id", PropertyValue::Null))
            .skip(1usize),
        ctx(indexes),
    );

    assert_node_point_ids(&point_limit_noop, &[1, 2, 3]);
    assert_node_point_ids(&point_limit_narrowed, &[1, 2]);
    assert_node_point_ids(&point_skip_narrowed, &[2, 3]);
    assert_node_point_ids(&point_range_clamped, &[2, 3]);
    assert_edge_point_ids(&edge_range_narrowed, &[9, 11]);

    assert_edge_empty(&filtered_edge_skip_empty);
    assert_node_empty(&variable_filter_skip_empty);
    assert_node_empty(&unique_skip_empty);
    assert!(matches!(run_op(&unique_null_skip), PhysicalOp::Skip { .. }));

    let (input, range) = literal_range(&variable_store_range);
    assert_eq!(range, StreamLiteralRange::new(1, 2).unwrap());
    assert!(matches!(
        input,
        PhysicalOp::Variable(VariablePlan::Stream {
            op: StreamVariableOp::Store(_),
            ..
        })
    ));
}

#[test]
fn zero_result_bounds_skip_side_effect_free_stream_wrappers() {
    let node_zero_limit = plan_traversal(
        g().n(NodeRef::all())
            .has("active", true)
            .order_by("age", Order::Asc)
            .limit(0usize),
        PlannerContext::default(),
    );
    let node_empty_range = plan_traversal(
        g().n(NodeRef::all())
            .dedup()
            .order_by_multiple(vec![("age", Order::Asc), ("name", Order::Desc)])
            .range(8usize, 8usize),
        PlannerContext::default(),
    );
    let edge_zero_limit = plan_traversal(
        g().e([3u64, 4])
            .edge_has("active", true)
            .order_by("since", Order::Desc)
            .limit(0usize),
        PlannerContext::default(),
    );
    let edge_empty_range = plan_traversal(
        g().e([3u64, 4])
            .dedup()
            .order_by("since", Order::Asc)
            .range(3usize, 3usize),
        PlannerContext::default(),
    );
    let node_variable_zero_limit = plan_traversal(
        g().n(NodeRef::all()).within("allowed").limit(0usize),
        PlannerContext::default(),
    );
    let node_variable_empty_range = plan_traversal(
        g().n(NodeRef::all())
            .without("blocked")
            .range(5usize, 5usize),
        PlannerContext::default(),
    );

    assert_node_empty(&node_zero_limit);
    assert_node_empty(&node_empty_range);
    assert_edge_empty(&edge_zero_limit);
    assert_edge_empty(&edge_empty_range);
    assert_node_empty(&node_variable_zero_limit);
    assert_node_empty(&node_variable_empty_range);
}

#[test]
fn zero_result_bounds_do_not_skip_terminal_projection_shapes() {
    let counted_nodes = || AstNode::Count {
        input: Box::new(AstNode::Nodes {
            reference: NodeRef::all(),
        }),
    };
    let zero_limit = plan_ast(
        AstNode::Limit {
            input: Box::new(counted_nodes()),
            count: StreamBound::Literal(0),
        },
        PlannerContext::default(),
    );
    let empty_range = plan_ast(
        AstNode::Range {
            input: Box::new(counted_nodes()),
            start: StreamBound::Literal(0),
            end: StreamBound::Literal(0),
        },
        PlannerContext::default(),
    );

    let PhysicalOp::Limit { input, count } = run_op(&zero_limit) else {
        panic!(
            "expected zero limit above projection: {:?}",
            run_op(&zero_limit)
        );
    };
    assert_eq!(count, &StreamBoundPlan::Literal(0));
    assert!(matches!(input.as_ref(), PhysicalOp::Project { .. }));

    let PhysicalOp::Range { input, range } = run_op(&empty_range) else {
        panic!(
            "expected empty range above projection: {:?}",
            run_op(&empty_range)
        );
    };
    assert_eq!(
        range,
        &StreamRangePlan::new(StreamBound::Literal(0), StreamBound::Literal(0)).unwrap()
    );
    assert!(matches!(input.as_ref(), PhysicalOp::Project { .. }));
}

#[test]
fn one_row_projection_terminals_strip_noop_stream_wrappers() {
    let counted_nodes = || AstNode::Count {
        input: Box::new(AstNode::Nodes {
            reference: NodeRef::all(),
        }),
    };
    let existing_nodes = || AstNode::Exists {
        input: Box::new(AstNode::Nodes {
            reference: NodeRef::all(),
        }),
    };
    let count_limit = plan_ast(
        AstNode::Limit {
            input: Box::new(counted_nodes()),
            count: StreamBound::Literal(10),
        },
        PlannerContext::default(),
    );
    let count_range = plan_ast(
        AstNode::Range {
            input: Box::new(counted_nodes()),
            start: StreamBound::Literal(0),
            end: StreamBound::Literal(1),
        },
        PlannerContext::default(),
    );
    let count_order = plan_ast(
        AstNode::OrderBy {
            input: Box::new(counted_nodes()),
            property: "anything".to_string(),
            order: Order::Asc,
        },
        PlannerContext::default(),
    );
    let exists_limit = plan_ast(
        AstNode::Limit {
            input: Box::new(existing_nodes()),
            count: StreamBound::Literal(1),
        },
        PlannerContext::default(),
    );
    let exists_dedup = plan_ast(
        AstNode::Dedup {
            input: Box::new(existing_nodes()),
        },
        PlannerContext::default(),
    );

    for plan in [count_limit, count_range, count_order] {
        assert!(
            matches!(
                run_op(&plan),
                PhysicalOp::Project {
                    projection: ProjectionPlan::Exists,
                    ..
                }
            ),
            "expected count projection without wrapper: {:?}",
            run_op(&plan)
        );
        assert_no_decision(&plan, TraceDecision::ExplicitSort);
    }
    for plan in [exists_limit, exists_dedup] {
        assert!(
            matches!(
                run_op(&plan),
                PhysicalOp::Project {
                    projection: ProjectionPlan::Exists,
                    ..
                }
            ),
            "expected exists projection without wrapper: {:?}",
            run_op(&plan)
        );
    }
}

#[test]
fn identity_projection_terminals_strip_noop_bounds_for_known_inputs() {
    let node_ids = || AstNode::Id {
        input: Box::new(AstNode::Nodes {
            reference: NodeRef::ids([1u64, 2]),
        }),
    };
    let node_labels = || AstNode::Label {
        input: Box::new(AstNode::Nodes {
            reference: NodeRef::ids([3u64, 4]),
        }),
    };
    let id_limit = plan_ast(
        AstNode::Limit {
            input: Box::new(node_ids()),
            count: StreamBound::Literal(2),
        },
        PlannerContext::default(),
    );
    let label_range = plan_ast(
        AstNode::Range {
            input: Box::new(node_labels()),
            start: StreamBound::Literal(0),
            end: StreamBound::Literal(2),
        },
        PlannerContext::default(),
    );
    let singleton_id_order = plan_ast(
        AstNode::OrderBy {
            input: Box::new(AstNode::Id {
                input: Box::new(AstNode::Nodes {
                    reference: NodeRef::id(7),
                }),
            }),
            property: "anything".to_string(),
            order: Order::Asc,
        },
        PlannerContext::default(),
    );
    let zero_id_limit = plan_ast(
        AstNode::Limit {
            input: Box::new(node_ids()),
            count: StreamBound::Literal(0),
        },
        PlannerContext::default(),
    );

    assert!(
        matches!(
            run_op(&id_limit),
            PhysicalOp::Project {
                projection: ProjectionPlan::Id,
                ..
            }
        ),
        "expected id projection without limit: {:?}",
        run_op(&id_limit)
    );
    assert!(
        matches!(
            run_op(&label_range),
            PhysicalOp::Project {
                projection: ProjectionPlan::Label,
                ..
            }
        ),
        "expected label projection without range: {:?}",
        run_op(&label_range)
    );
    assert!(
        matches!(
            run_op(&singleton_id_order),
            PhysicalOp::Project {
                projection: ProjectionPlan::Id,
                ..
            }
        ),
        "expected singleton id projection without order: {:?}",
        run_op(&singleton_id_order)
    );
    assert_no_decision(&singleton_id_order, TraceDecision::ExplicitSort);

    let PhysicalOp::Project {
        input,
        projection: ProjectionPlan::Id,
    } = run_op(&zero_id_limit)
    else {
        panic!(
            "expected id projection above empty access: {:?}",
            run_op(&zero_id_limit)
        );
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::Empty)
    ));
}

#[test]
fn row_preserving_projection_terminals_strip_noop_bounds_for_known_inputs() {
    let point_nodes = || AstNode::Nodes {
        reference: NodeRef::ids([1u64, 2]),
    };
    let value_map = || AstNode::ValueMap {
        input: Box::new(point_nodes()),
        properties: Some(vec!["name".to_string()]),
    };
    let project = || AstNode::Project {
        input: Box::new(point_nodes()),
        projections: vec![Projection::property("name", "name")],
    };
    let project_bindings = || AstNode::ProjectBindings {
        input: Box::new(AstNode::Nodes {
            reference: NodeRef::id(1),
        }),
        projections: vec![BindingProjection::current("name", "name")],
        distinct: true,
    };
    let edge_properties = || AstNode::EdgeProperties {
        input: Box::new(AstNode::Edges {
            reference: EdgeRef::ids([7u64, 9]),
        }),
    };

    type PlanPredicate = fn(&PhysicalOp) -> bool;
    type ProjectionCase = (PhysicalPlan, PlanPredicate, &'static str);

    let cases: [ProjectionCase; 4] = [
        (
            plan_ast(
                AstNode::Limit {
                    input: Box::new(value_map()),
                    count: StreamBound::Literal(2),
                },
                PlannerContext::default(),
            ),
            |op: &PhysicalOp| {
                matches!(
                    op,
                    PhysicalOp::Project {
                        projection: ProjectionPlan::ValueMap(_),
                        ..
                    }
                )
            },
            "value-map projection without limit",
        ),
        (
            plan_ast(
                AstNode::Range {
                    input: Box::new(project()),
                    start: StreamBound::Literal(0),
                    end: StreamBound::Literal(2),
                },
                PlannerContext::default(),
            ),
            |op: &PhysicalOp| {
                matches!(
                    op,
                    PhysicalOp::Project {
                        projection: ProjectionPlan::Project(_),
                        ..
                    }
                )
            },
            "general projection without range",
        ),
        (
            plan_ast(
                AstNode::OrderBy {
                    input: Box::new(project_bindings()),
                    property: "name".to_string(),
                    order: Order::Asc,
                },
                PlannerContext::default(),
            ),
            |op: &PhysicalOp| {
                matches!(
                    op,
                    PhysicalOp::Project {
                        projection: ProjectionPlan::ProjectBindings { .. },
                        ..
                    }
                )
            },
            "binding projection without order",
        ),
        (
            plan_ast(
                AstNode::Limit {
                    input: Box::new(edge_properties()),
                    count: StreamBound::Literal(2),
                },
                PlannerContext::default(),
            ),
            |op: &PhysicalOp| {
                matches!(
                    op,
                    PhysicalOp::Project {
                        projection: ProjectionPlan::EdgeProperties,
                        ..
                    }
                )
            },
            "edge-properties projection without limit",
        ),
    ];

    for (plan, expected, message) in cases {
        assert!(
            expected(run_op(&plan)),
            "expected {message}: {:?}",
            run_op(&plan)
        );
        assert_no_decision(&plan, TraceDecision::ExplicitSort);
    }

    let values = plan_ast(
        AstNode::Limit {
            input: Box::new(AstNode::Values {
                input: Box::new(point_nodes()),
                properties: vec!["name".to_string()],
            }),
            count: StreamBound::Literal(2),
        },
        PlannerContext::default(),
    );
    assert!(
        matches!(
            run_op(&values),
            PhysicalOp::Limit {
                input,
                count: StreamBoundPlan::Literal(2),
            } if matches!(
                input.as_ref(),
                PhysicalOp::Project {
                    projection: ProjectionPlan::Values(_),
                    ..
                }
            )
        ),
        "expected values projection to keep limit: {:?}",
        run_op(&values)
    );
}

#[test]
fn stream_bounds_push_below_row_preserving_projection_terminals() {
    let all_nodes = || AstNode::Nodes {
        reference: NodeRef::all(),
    };
    let value_map_limit = plan_ast(
        AstNode::Limit {
            input: Box::new(AstNode::ValueMap {
                input: Box::new(all_nodes()),
                properties: Some(vec!["name".to_string()]),
            }),
            count: StreamBound::expr(Expr::param("limit")),
        },
        PlannerContext::default(),
    );
    let id_skip = plan_ast(
        AstNode::Skip {
            input: Box::new(AstNode::Id {
                input: Box::new(all_nodes()),
            }),
            count: StreamBound::Literal(5),
        },
        PlannerContext::default(),
    );
    let project_range = plan_ast(
        AstNode::Range {
            input: Box::new(AstNode::Project {
                input: Box::new(AstNode::Nodes {
                    reference: NodeRef::ids([1u64, 2, 3]),
                }),
                projections: vec![Projection::property("name", "name")],
            }),
            start: StreamBound::Literal(1),
            end: StreamBound::Literal(3),
        },
        PlannerContext::default(),
    );
    let binding_skip = plan_ast(
        AstNode::Skip {
            input: Box::new(AstNode::ProjectBindings {
                input: Box::new(all_nodes()),
                projections: vec![BindingProjection::current("name", "name")],
                distinct: false,
            }),
            count: StreamBound::Literal(2),
        },
        PlannerContext::default(),
    );

    let PhysicalOp::Project {
        input,
        projection: ProjectionPlan::ValueMap(_),
    } = run_op(&value_map_limit)
    else {
        panic!(
            "expected value-map projection above pushed limit: {:?}",
            run_op(&value_map_limit)
        );
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::Limit {
            input,
            count: StreamBoundPlan::Expr(_),
        } if matches!(input.as_ref(), PhysicalOp::NodeAccess(NodeAccessPlan::AllScan))
    ));

    let PhysicalOp::Project {
        input,
        projection: ProjectionPlan::Id,
    } = run_op(&id_skip)
    else {
        panic!(
            "expected id projection above pushed skip: {:?}",
            run_op(&id_skip)
        );
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::Skip {
            input,
            count: StreamBoundPlan::Literal(5),
        } if matches!(input.as_ref(), PhysicalOp::NodeAccess(NodeAccessPlan::AllScan))
    ));

    let PhysicalOp::Project {
        input,
        projection: ProjectionPlan::Project(_),
    } = run_op(&project_range)
    else {
        panic!(
            "expected projection above sliced point access: {:?}",
            run_op(&project_range)
        );
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::PointIds { ids }) if ids.as_ref() == [2, 3]
    ));

    let PhysicalOp::Project {
        input,
        projection:
            ProjectionPlan::ProjectBindings {
                dedup: ProjectionDedupMode::All,
                ..
            },
    } = run_op(&binding_skip)
    else {
        panic!(
            "expected all-binding projection above pushed skip: {:?}",
            run_op(&binding_skip)
        );
    };
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::Skip {
            input,
            count: StreamBoundPlan::Literal(2),
        } if matches!(input.as_ref(), PhysicalOp::NodeAccess(NodeAccessPlan::AllScan))
    ));
}

#[test]
fn stream_bounds_do_not_cross_fanout_or_deduplicating_projection_terminals() {
    let all_nodes = || AstNode::Nodes {
        reference: NodeRef::all(),
    };
    let values_limit = plan_ast(
        AstNode::Limit {
            input: Box::new(AstNode::Values {
                input: Box::new(all_nodes()),
                properties: vec!["name".to_string()],
            }),
            count: StreamBound::Literal(10),
        },
        PlannerContext::default(),
    );
    let values_skip = plan_ast(
        AstNode::Skip {
            input: Box::new(AstNode::Values {
                input: Box::new(all_nodes()),
                properties: vec!["name".to_string()],
            }),
            count: StreamBound::Literal(3),
        },
        PlannerContext::default(),
    );
    let distinct_binding_limit = plan_ast(
        AstNode::Limit {
            input: Box::new(AstNode::ProjectBindings {
                input: Box::new(all_nodes()),
                projections: vec![BindingProjection::current("name", "name")],
                distinct: true,
            }),
            count: StreamBound::Literal(10),
        },
        PlannerContext::default(),
    );

    assert!(
        matches!(
            run_op(&values_limit),
            PhysicalOp::Limit {
                input,
                count: StreamBoundPlan::Literal(10),
            } if matches!(
                input.as_ref(),
                PhysicalOp::Project {
                    projection: ProjectionPlan::Values(_),
                    ..
                }
            )
        ),
        "expected values projection to keep outer limit: {:?}",
        run_op(&values_limit)
    );
    assert!(
        matches!(
            run_op(&values_skip),
            PhysicalOp::Skip {
                input,
                count: StreamBoundPlan::Literal(3),
            } if matches!(
                input.as_ref(),
                PhysicalOp::Project {
                    projection: ProjectionPlan::Values(_),
                    ..
                }
            )
        ),
        "expected values projection to keep outer skip: {:?}",
        run_op(&values_skip)
    );
    assert!(
        matches!(
            run_op(&distinct_binding_limit),
            PhysicalOp::Limit {
                input,
                count: StreamBoundPlan::Literal(10),
            } if matches!(
                input.as_ref(),
                PhysicalOp::Project {
                    projection: ProjectionPlan::ProjectBindings {
                        dedup: ProjectionDedupMode::Distinct,
                        ..
                    },
                    ..
                }
            )
        ),
        "expected distinct binding projection to keep outer limit: {:?}",
        run_op(&distinct_binding_limit)
    );
}

#[test]
fn adjacent_literal_skips_coalesce_and_zero_skip_is_removed() {
    let stacked = plan_traversal(
        g().n(NodeRef::all()).skip(2usize).skip(5usize),
        PlannerContext::default(),
    );
    let zero = plan_traversal(
        g().n(NodeRef::all()).skip(0usize),
        PlannerContext::default(),
    );
    let saturated = plan_traversal(
        g().n(NodeRef::all()).skip(usize::MAX - 1).skip(5usize),
        PlannerContext::default(),
    );
    let dynamic_outer = plan_traversal(
        g().n(NodeRef::all())
            .skip(2usize)
            .skip(StreamBound::expr(Expr::param("offset"))),
        PlannerContext::default(),
    );

    let (input, count) = literal_skip(&stacked);
    assert_eq!(count, 7);
    assert!(matches!(
        input,
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    assert!(matches!(
        run_op(&zero),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let (_, count) = literal_skip(&saturated);
    assert_eq!(count, usize::MAX);

    let PhysicalOp::Skip {
        input,
        count: outer_count,
    } = run_op(&dynamic_outer)
    else {
        panic!("expected outer dynamic skip: {:?}", run_op(&dynamic_outer));
    };
    assert!(matches!(outer_count, StreamBoundPlan::Expr(_)));
    let PhysicalOp::Skip {
        input,
        count: inner_count,
    } = input.as_ref()
    else {
        panic!("expected literal inner skip: {input:?}");
    };
    assert_eq!(inner_count, &StreamBoundPlan::Literal(2));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));
}

#[test]
fn literal_bound_chains_stress_collapse_to_one_effective_slice() {
    let cases = [
        LiteralBoundOp::Limit(0),
        LiteralBoundOp::Limit(4),
        LiteralBoundOp::Skip(0),
        LiteralBoundOp::Skip(3),
        LiteralBoundOp::Range(0, 0),
        LiteralBoundOp::Range(0, 5),
        LiteralBoundOp::Range(2, 7),
    ];

    for first in cases {
        for second in cases {
            for third in cases {
                let ops = [first, second, third];
                let plan = plan_ast(
                    ops.into_iter().fold(
                        AstNode::Nodes {
                            reference: NodeRef::all(),
                        },
                        apply_literal_bound_op,
                    ),
                    PlannerContext::default(),
                );

                assert_eq!(
                    planned_literal_slice(run_op(&plan)),
                    Some(expected_literal_slice(ops)),
                    "expected literal bound chain {ops:?} to collapse, got {:?}",
                    run_op(&plan)
                );
            }
        }
    }
}

#[test]
fn literal_skip_plus_limit_coalesces_to_an_offset_range() {
    let bounded = plan_traversal(
        g().n(NodeRef::all()).skip(4usize).limit(3usize),
        PlannerContext::default(),
    );
    let zero_limit = plan_traversal(
        g().n(NodeRef::all()).skip(4usize).limit(0usize),
        PlannerContext::default(),
    );
    let saturated = plan_traversal(
        g().n(NodeRef::all()).skip(usize::MAX - 1).limit(5usize),
        PlannerContext::default(),
    );
    let dynamic_skip = plan_traversal(
        g().n(NodeRef::all())
            .skip(StreamBound::expr(Expr::param("offset")))
            .limit(3usize),
        PlannerContext::default(),
    );
    let dynamic_limit = plan_traversal(
        g().n(NodeRef::all())
            .skip(4usize)
            .limit(StreamBound::expr(Expr::param("limit"))),
        PlannerContext::default(),
    );

    let (input, range) = literal_range(&bounded);
    assert_eq!(range, StreamLiteralRange::new(4, 7).unwrap());
    assert!(matches!(
        input,
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    assert_node_empty(&zero_limit);

    let (_, range) = literal_range(&saturated);
    assert_eq!(
        range,
        StreamLiteralRange::new(usize::MAX - 1, usize::MAX).unwrap()
    );

    let PhysicalOp::Limit {
        input,
        count: outer_count,
    } = run_op(&dynamic_skip)
    else {
        panic!("expected outer limit: {:?}", run_op(&dynamic_skip));
    };
    assert_eq!(outer_count, &StreamBoundPlan::Literal(3));
    let PhysicalOp::Skip {
        input,
        count: inner_count,
    } = input.as_ref()
    else {
        panic!("expected inner dynamic skip: {input:?}");
    };
    assert!(matches!(inner_count, StreamBoundPlan::Expr(_)));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::Limit {
        input,
        count: outer_count,
    } = run_op(&dynamic_limit)
    else {
        panic!("expected outer dynamic limit: {:?}", run_op(&dynamic_limit));
    };
    assert!(matches!(outer_count, StreamBoundPlan::Expr(_)));
    let PhysicalOp::Skip {
        input,
        count: inner_count,
    } = input.as_ref()
    else {
        panic!("expected inner literal skip: {input:?}");
    };
    assert_eq!(inner_count, &StreamBoundPlan::Literal(4));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));
}

#[test]
fn literal_skip_plus_range_coalesces_to_an_offset_range() {
    let bounded = plan_traversal(
        g().n(NodeRef::all()).skip(4usize).range(2usize, 6usize),
        PlannerContext::default(),
    );
    let zero_range = plan_traversal(
        g().n(NodeRef::all()).skip(4usize).range(0usize, 0usize),
        PlannerContext::default(),
    );
    let saturated = plan_traversal(
        g().n(NodeRef::all())
            .skip(usize::MAX - 1)
            .range(0usize, 5usize),
        PlannerContext::default(),
    );
    let dynamic_skip = plan_traversal(
        g().n(NodeRef::all())
            .skip(StreamBound::expr(Expr::param("offset")))
            .range(2usize, 6usize),
        PlannerContext::default(),
    );
    let dynamic_range = plan_traversal(
        g().n(NodeRef::all())
            .skip(4usize)
            .range(2usize, StreamBound::expr(Expr::param("end"))),
        PlannerContext::default(),
    );

    let (input, range) = literal_range(&bounded);
    assert_eq!(range, StreamLiteralRange::new(6, 10).unwrap());
    assert!(matches!(
        input,
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    assert_node_empty(&zero_range);

    let (_, range) = literal_range(&saturated);
    assert_eq!(
        range,
        StreamLiteralRange::new(usize::MAX - 1, usize::MAX).unwrap()
    );

    let PhysicalOp::Range {
        input,
        range: outer_range,
    } = run_op(&dynamic_skip)
    else {
        panic!("expected outer literal range: {:?}", run_op(&dynamic_skip));
    };
    assert_eq!(
        outer_range,
        &StreamRangePlan::new(StreamBound::Literal(2), StreamBound::Literal(6)).unwrap()
    );
    let PhysicalOp::Skip {
        input,
        count: inner_count,
    } = input.as_ref()
    else {
        panic!("expected inner dynamic skip: {input:?}");
    };
    assert!(matches!(inner_count, StreamBoundPlan::Expr(_)));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::Range {
        input,
        range: outer_range,
    } = run_op(&dynamic_range)
    else {
        panic!("expected outer dynamic range: {:?}", run_op(&dynamic_range));
    };
    assert!(matches!(outer_range, StreamRangePlan::Dynamic(_)));
    let PhysicalOp::Skip {
        input,
        count: inner_count,
    } = input.as_ref()
    else {
        panic!("expected inner literal skip: {input:?}");
    };
    assert_eq!(inner_count, &StreamBoundPlan::Literal(4));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));
}

#[test]
fn literal_limit_plus_skip_coalesces_to_a_clipped_range() {
    let bounded = plan_traversal(
        g().n(NodeRef::all()).limit(10usize).skip(3usize),
        PlannerContext::default(),
    );
    let beyond_limit = plan_traversal(
        g().n(NodeRef::all()).limit(10usize).skip(50usize),
        PlannerContext::default(),
    );
    let saturated = plan_traversal(
        g().n(NodeRef::all()).limit(usize::MAX).skip(usize::MAX - 1),
        PlannerContext::default(),
    );
    let dynamic_limit = plan_traversal(
        g().n(NodeRef::all())
            .limit(StreamBound::expr(Expr::param("limit")))
            .skip(3usize),
        PlannerContext::default(),
    );
    let dynamic_skip = plan_traversal(
        g().n(NodeRef::all())
            .limit(10usize)
            .skip(StreamBound::expr(Expr::param("offset"))),
        PlannerContext::default(),
    );

    let (input, range) = literal_range(&bounded);
    assert_eq!(range, StreamLiteralRange::new(3, 10).unwrap());
    assert!(matches!(
        input,
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    assert_node_empty(&beyond_limit);

    let (_, range) = literal_range(&saturated);
    assert_eq!(
        range,
        StreamLiteralRange::new(usize::MAX - 1, usize::MAX).unwrap()
    );

    let PhysicalOp::Skip {
        input,
        count: outer_count,
    } = run_op(&dynamic_limit)
    else {
        panic!("expected outer skip: {:?}", run_op(&dynamic_limit));
    };
    assert_eq!(outer_count, &StreamBoundPlan::Literal(3));
    let PhysicalOp::Limit {
        input,
        count: inner_count,
    } = input.as_ref()
    else {
        panic!("expected inner dynamic limit: {input:?}");
    };
    assert!(matches!(inner_count, StreamBoundPlan::Expr(_)));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::Skip {
        input,
        count: outer_count,
    } = run_op(&dynamic_skip)
    else {
        panic!("expected outer dynamic skip: {:?}", run_op(&dynamic_skip));
    };
    assert!(matches!(outer_count, StreamBoundPlan::Expr(_)));
    let PhysicalOp::Limit {
        input,
        count: inner_count,
    } = input.as_ref()
    else {
        panic!("expected inner literal limit: {input:?}");
    };
    assert_eq!(inner_count, &StreamBoundPlan::Literal(10));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));
}

#[test]
fn literal_range_plus_skip_coalesces_to_a_clipped_range() {
    let bounded = plan_traversal(
        g().n(NodeRef::all()).range(10usize, 20usize).skip(3usize),
        PlannerContext::default(),
    );
    let beyond_range = plan_traversal(
        g().n(NodeRef::all()).range(10usize, 20usize).skip(50usize),
        PlannerContext::default(),
    );
    let empty_range = plan_traversal(
        g().n(NodeRef::all()).range(10usize, 10usize).skip(3usize),
        PlannerContext::default(),
    );
    let dynamic_range = plan_traversal(
        g().n(NodeRef::all())
            .range(10usize, StreamBound::expr(Expr::param("end")))
            .skip(3usize),
        PlannerContext::default(),
    );
    let dynamic_skip = plan_traversal(
        g().n(NodeRef::all())
            .range(10usize, 20usize)
            .skip(StreamBound::expr(Expr::param("offset"))),
        PlannerContext::default(),
    );

    let (input, range) = literal_range(&bounded);
    assert_eq!(range, StreamLiteralRange::new(13, 20).unwrap());
    assert!(matches!(
        input,
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    assert_node_empty(&beyond_range);

    assert_node_empty(&empty_range);

    let PhysicalOp::Skip {
        input,
        count: outer_count,
    } = run_op(&dynamic_range)
    else {
        panic!("expected outer skip: {:?}", run_op(&dynamic_range));
    };
    assert_eq!(outer_count, &StreamBoundPlan::Literal(3));
    let PhysicalOp::Range {
        input,
        range: inner_range,
    } = input.as_ref()
    else {
        panic!("expected inner dynamic range: {input:?}");
    };
    assert!(matches!(inner_range, StreamRangePlan::Dynamic(_)));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::Skip {
        input,
        count: outer_count,
    } = run_op(&dynamic_skip)
    else {
        panic!("expected outer dynamic skip: {:?}", run_op(&dynamic_skip));
    };
    assert!(matches!(outer_count, StreamBoundPlan::Expr(_)));
    let PhysicalOp::Range {
        input,
        range: inner_range,
    } = input.as_ref()
    else {
        panic!("expected inner literal range: {input:?}");
    };
    assert_eq!(
        inner_range,
        &StreamRangePlan::new(StreamBound::Literal(10), StreamBound::Literal(20)).unwrap()
    );
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));
}

#[test]
fn literal_limit_plus_range_coalesces_to_a_clipped_range() {
    let contained = plan_traversal(
        g().n(NodeRef::all()).limit(10usize).range(3usize, 7usize),
        PlannerContext::default(),
    );
    let clipped_end = plan_traversal(
        g().n(NodeRef::all()).limit(10usize).range(8usize, 50usize),
        PlannerContext::default(),
    );
    let beyond_limit = plan_traversal(
        g().n(NodeRef::all()).limit(10usize).range(15usize, 18usize),
        PlannerContext::default(),
    );
    let empty_outer = plan_traversal(
        g().n(NodeRef::all()).limit(10usize).range(4usize, 4usize),
        PlannerContext::default(),
    );
    let dynamic_limit = plan_traversal(
        g().n(NodeRef::all())
            .limit(StreamBound::expr(Expr::param("limit")))
            .range(3usize, 7usize),
        PlannerContext::default(),
    );
    let dynamic_range = plan_traversal(
        g().n(NodeRef::all())
            .limit(10usize)
            .range(3usize, StreamBound::expr(Expr::param("end"))),
        PlannerContext::default(),
    );

    let (input, range) = literal_range(&contained);
    assert_eq!(range, StreamLiteralRange::new(3, 7).unwrap());
    assert!(matches!(
        input,
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let (_, range) = literal_range(&clipped_end);
    assert_eq!(range, StreamLiteralRange::new(8, 10).unwrap());

    assert_node_empty(&beyond_limit);

    assert_node_empty(&empty_outer);

    let PhysicalOp::Range {
        input,
        range: outer_range,
    } = run_op(&dynamic_limit)
    else {
        panic!("expected outer literal range: {:?}", run_op(&dynamic_limit));
    };
    assert_eq!(
        outer_range,
        &StreamRangePlan::new(StreamBound::Literal(3), StreamBound::Literal(7)).unwrap()
    );
    let PhysicalOp::Limit {
        input,
        count: inner_count,
    } = input.as_ref()
    else {
        panic!("expected inner dynamic limit: {input:?}");
    };
    assert!(matches!(inner_count, StreamBoundPlan::Expr(_)));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::Range {
        input,
        range: outer_range,
    } = run_op(&dynamic_range)
    else {
        panic!("expected outer dynamic range: {:?}", run_op(&dynamic_range));
    };
    assert!(matches!(outer_range, StreamRangePlan::Dynamic(_)));
    let PhysicalOp::Limit {
        input,
        count: inner_count,
    } = input.as_ref()
    else {
        panic!("expected inner literal limit: {input:?}");
    };
    assert_eq!(inner_count, &StreamBoundPlan::Literal(10));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));
}

#[test]
fn adjacent_literal_ranges_coalesce_to_the_composed_slice() {
    let contained = plan_traversal(
        g().n(NodeRef::all())
            .range(10usize, 20usize)
            .range(3usize, 7usize),
        PlannerContext::default(),
    );
    let clipped_to_inner_end = plan_traversal(
        g().n(NodeRef::all())
            .range(10usize, 20usize)
            .range(8usize, 50usize),
        PlannerContext::default(),
    );
    let beyond_inner_end = plan_traversal(
        g().n(NodeRef::all())
            .range(10usize, 20usize)
            .range(15usize, 18usize),
        PlannerContext::default(),
    );
    let empty_inner = plan_traversal(
        g().n(NodeRef::all())
            .range(10usize, 10usize)
            .range(0usize, 5usize),
        PlannerContext::default(),
    );
    let empty_outer = plan_traversal(
        g().n(NodeRef::all())
            .range(10usize, 20usize)
            .range(4usize, 4usize),
        PlannerContext::default(),
    );

    let (input, range) = literal_range(&contained);
    assert_eq!(range, StreamLiteralRange::new(13, 17).unwrap());
    assert!(matches!(
        input,
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let (_, range) = literal_range(&clipped_to_inner_end);
    assert_eq!(range, StreamLiteralRange::new(18, 20).unwrap());

    assert_node_empty(&beyond_inner_end);

    assert_node_empty(&empty_inner);

    assert_node_empty(&empty_outer);
}

#[test]
fn adjacent_ranges_stay_nested_when_either_bound_is_dynamic() {
    let dynamic_inner = plan_traversal(
        g().n(NodeRef::all())
            .range(StreamBound::expr(Expr::param("start")), 20usize)
            .range(3usize, 7usize),
        PlannerContext::default(),
    );
    let dynamic_outer = plan_traversal(
        g().n(NodeRef::all())
            .range(10usize, 20usize)
            .range(3usize, StreamBound::expr(Expr::param("end"))),
        PlannerContext::default(),
    );

    let PhysicalOp::Range {
        input,
        range: outer_range,
    } = run_op(&dynamic_inner)
    else {
        panic!("expected outer literal range: {:?}", run_op(&dynamic_inner));
    };
    assert_eq!(
        outer_range,
        &StreamRangePlan::new(StreamBound::Literal(3), StreamBound::Literal(7)).unwrap()
    );
    let PhysicalOp::Range {
        input,
        range: inner_range,
    } = input.as_ref()
    else {
        panic!("expected inner dynamic range: {input:?}");
    };
    assert!(matches!(inner_range, StreamRangePlan::Dynamic(_)));
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let PhysicalOp::Range {
        input,
        range: outer_range,
    } = run_op(&dynamic_outer)
    else {
        panic!("expected outer dynamic range: {:?}", run_op(&dynamic_outer));
    };
    assert!(matches!(outer_range, StreamRangePlan::Dynamic(_)));
    let PhysicalOp::Range {
        input,
        range: inner_range,
    } = input.as_ref()
    else {
        panic!("expected inner literal range: {input:?}");
    };
    assert_eq!(
        inner_range,
        &StreamRangePlan::new(StreamBound::Literal(10), StreamBound::Literal(20)).unwrap()
    );
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));
}

#[test]
fn literal_range_plus_limit_coalesces_to_the_tightest_range() {
    let shorter_limit = plan_traversal(
        g().n(NodeRef::all()).range(10usize, 20usize).limit(3usize),
        PlannerContext::default(),
    );
    let longer_limit = plan_traversal(
        g().n(NodeRef::all()).range(10usize, 20usize).limit(50usize),
        PlannerContext::default(),
    );
    let zero_limit = plan_traversal(
        g().n(NodeRef::all()).range(10usize, 20usize).limit(0usize),
        PlannerContext::default(),
    );
    let saturated_limit = plan_traversal(
        g().n(NodeRef::all())
            .range(usize::MAX - 1, usize::MAX)
            .limit(5usize),
        PlannerContext::default(),
    );
    let dynamic_limit = plan_traversal(
        g().n(NodeRef::all())
            .range(10usize, 20usize)
            .limit(StreamBound::expr(Expr::param("limit"))),
        PlannerContext::default(),
    );

    let (input, range) = literal_range(&shorter_limit);
    assert_eq!(range, StreamLiteralRange::new(10, 13).unwrap());
    assert!(matches!(
        input,
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));

    let (_, range) = literal_range(&longer_limit);
    assert_eq!(range, StreamLiteralRange::new(10, 20).unwrap());

    assert_node_empty(&zero_limit);

    let (_, range) = literal_range(&saturated_limit);
    assert_eq!(
        range,
        StreamLiteralRange::new(usize::MAX - 1, usize::MAX).unwrap()
    );

    let PhysicalOp::Limit {
        input,
        count: outer_count,
    } = run_op(&dynamic_limit)
    else {
        panic!("expected outer dynamic limit: {:?}", run_op(&dynamic_limit));
    };
    assert!(matches!(outer_count, StreamBoundPlan::Expr(_)));
    let PhysicalOp::Range {
        input,
        range: inner_range,
    } = input.as_ref()
    else {
        panic!("expected inner literal range: {input:?}");
    };
    assert_eq!(
        inner_range,
        &StreamRangePlan::new(StreamBound::Literal(10), StreamBound::Literal(20)).unwrap()
    );
    assert!(matches!(
        input.as_ref(),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)
    ));
}

fn literal_limit(plan: &PhysicalPlan) -> (&PhysicalOp, usize) {
    let PhysicalOp::Limit { input, count } = run_op(plan) else {
        panic!("expected limit: {:?}", run_op(plan));
    };
    let StreamBoundPlan::Literal(count) = count else {
        panic!("expected literal limit count: {count:?}");
    };
    (input.as_ref(), *count)
}

fn literal_skip(plan: &PhysicalPlan) -> (&PhysicalOp, usize) {
    let PhysicalOp::Skip { input, count } = run_op(plan) else {
        panic!("expected skip: {:?}", run_op(plan));
    };
    let StreamBoundPlan::Literal(count) = count else {
        panic!("expected literal skip count: {count:?}");
    };
    (input.as_ref(), *count)
}

fn literal_range(plan: &PhysicalPlan) -> (&PhysicalOp, StreamLiteralRange) {
    let PhysicalOp::Range { input, range } = run_op(plan) else {
        panic!("expected range: {:?}", run_op(plan));
    };
    let StreamRangePlan::Literal(range) = range else {
        panic!("expected literal range: {range:?}");
    };
    (input.as_ref(), *range)
}

#[derive(Debug, Clone, Copy)]
enum LiteralBoundOp {
    Limit(usize),
    Skip(usize),
    Range(usize, usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LiteralSlice {
    Empty,
    Slice { start: usize, end: Option<usize> },
}

fn apply_literal_bound_op(input: AstNode, op: LiteralBoundOp) -> AstNode {
    match op {
        LiteralBoundOp::Limit(count) => AstNode::Limit {
            input: Box::new(input),
            count: StreamBound::from(count),
        },
        LiteralBoundOp::Skip(count) => AstNode::Skip {
            input: Box::new(input),
            count: StreamBound::from(count),
        },
        LiteralBoundOp::Range(start, end) => AstNode::Range {
            input: Box::new(input),
            start: StreamBound::from(start),
            end: StreamBound::from(end),
        },
    }
}

fn expected_literal_slice(ops: [LiteralBoundOp; 3]) -> LiteralSlice {
    let slice = ops.into_iter().fold(
        LiteralSlice::Slice {
            start: 0,
            end: None,
        },
        apply_expected_literal_bound,
    );
    empty_if_degenerate_literal_slice(slice)
}

fn apply_expected_literal_bound(slice: LiteralSlice, op: LiteralBoundOp) -> LiteralSlice {
    let LiteralSlice::Slice { start, end } = slice else {
        return LiteralSlice::Empty;
    };

    match op {
        LiteralBoundOp::Limit(count) => LiteralSlice::Slice {
            start,
            end: Some(end.map_or(start + count, |end| end.min(start + count))),
        },
        LiteralBoundOp::Skip(count) => LiteralSlice::Slice {
            start: end.map_or(start + count, |end| end.min(start + count)),
            end,
        },
        LiteralBoundOp::Range(range_start, range_end) => match end {
            Some(end) => {
                let len = end - start;
                LiteralSlice::Slice {
                    start: start + range_start.min(len),
                    end: Some(start + range_end.min(len)),
                }
            }
            None => LiteralSlice::Slice {
                start: start + range_start,
                end: Some(start + range_end),
            },
        },
    }
}

fn empty_if_degenerate_literal_slice(slice: LiteralSlice) -> LiteralSlice {
    match slice {
        LiteralSlice::Slice {
            start,
            end: Some(end),
        } if start >= end => LiteralSlice::Empty,
        slice => slice,
    }
}

fn planned_literal_slice(op: &PhysicalOp) -> Option<LiteralSlice> {
    match op {
        PhysicalOp::NodeAccess(NodeAccessPlan::Empty) => Some(LiteralSlice::Empty),
        PhysicalOp::NodeAccess(NodeAccessPlan::AllScan) => Some(LiteralSlice::Slice {
            start: 0,
            end: None,
        }),
        PhysicalOp::Limit { input, count } => {
            let StreamBoundPlan::Literal(count) = count else {
                return None;
            };
            match input.as_ref() {
                PhysicalOp::NodeAccess(NodeAccessPlan::AllScan) => Some(LiteralSlice::Slice {
                    start: 0,
                    end: Some(*count),
                }),
                _ => None,
            }
        }
        PhysicalOp::Skip { input, count } => {
            let StreamBoundPlan::Literal(count) = count else {
                return None;
            };
            match input.as_ref() {
                PhysicalOp::NodeAccess(NodeAccessPlan::AllScan) => Some(LiteralSlice::Slice {
                    start: *count,
                    end: None,
                }),
                _ => None,
            }
        }
        PhysicalOp::Range { input, range } => {
            let StreamRangePlan::Literal(range) = range else {
                return None;
            };
            match input.as_ref() {
                PhysicalOp::NodeAccess(NodeAccessPlan::AllScan) => Some(LiteralSlice::Slice {
                    start: range.start(),
                    end: Some(range.end()),
                }),
                _ => None,
            }
        }
        _ => None,
    }
}

fn assert_node_point_ids(plan: &PhysicalPlan, expected: &[u64]) {
    let NodeAccessPlan::PointIds { ids } = node_access(plan) else {
        panic!("expected node point ids: {:?}", run_op(plan));
    };
    assert_eq!(ids.as_ref(), expected);
}

fn assert_edge_point_ids(plan: &PhysicalPlan, expected: &[u64]) {
    let EdgeAccessPlan::PointIds { ids } = edge_access(plan) else {
        panic!("expected edge point ids: {:?}", run_op(plan));
    };
    assert_eq!(ids.as_ref(), expected);
}

fn assert_distinct_over_node_union(plan: &PhysicalPlan) {
    let PhysicalOp::Distinct { input } = run_op(plan) else {
        panic!("expected distinct wrapper: {:?}", run_op(plan));
    };
    assert!(
        matches!(
            input.as_ref(),
            PhysicalOp::NodeAccess(NodeAccessPlan::Union(_))
        ),
        "expected node union under distinct: {input:?}"
    );
}

fn assert_node_empty(plan: &PhysicalPlan) {
    assert!(
        matches!(run_op(plan), PhysicalOp::NodeAccess(NodeAccessPlan::Empty)),
        "expected empty node access: {:?}",
        run_op(plan)
    );
}

fn assert_edge_empty(plan: &PhysicalPlan) {
    assert!(
        matches!(run_op(plan), PhysicalOp::EdgeAccess(EdgeAccessPlan::Empty)),
        "expected empty edge access: {:?}",
        run_op(plan)
    );
}

fn assert_stream_variable<'a>(
    op: &'a PhysicalOp,
    expected_op: &StreamVariableOp,
) -> &'a PhysicalOp {
    let PhysicalOp::Variable(VariablePlan::Stream { input, op }) = op else {
        panic!("expected stream variable wrapper: {op:?}");
    };
    assert_eq!(op, expected_op);
    input.as_ref()
}
