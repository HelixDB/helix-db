use crate::planning::tests::support::*;

#[test]
fn cascades_chosen_access_matrix_proves_index_family_and_trace() {
    let indexes = chosen_plan_indexes();

    let node_equality = executable_traversal(
        g().n_with_label_where("User", Predicate::eq("username", "alice")),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&node_equality, "alternative");
    assert_selected_rule(&node_equality, KnownRuleId::SeedAccessPath);
    assert!(matches!(
        unwrapped_first_exec_access(&node_equality),
        ExecAccessPlan::Node(ExecNodeAccessPlan::EqualityIndex { key, value, .. })
            if key.label == "User"
                && key.property == "username"
                && matches!(value, IndexValue::Literal(_))
    ));
    assert_no_exec_op_family(&node_equality, ExecOpFamily::Filter);
    assert_no_exec_op_family(&node_equality, ExecOpFamily::Order);
    assert_no_exec_window(&node_equality);

    let node_unique = executable_traversal(
        g().n_with_label_where("User", Predicate::eq("email", "a@example.com")),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&node_unique, "alternative");
    assert_selected_rule(&node_unique, KnownRuleId::SeedAccessPath);
    assert!(matches!(
        unwrapped_first_exec_access(&node_unique),
        ExecAccessPlan::Node(ExecNodeAccessPlan::EqualityIndex { key, index, .. })
            if key.label == "User"
                && key.property == "email"
                && index.uniqueness == IndexUniqueness::Unique
    ));
    assert_no_exec_op_family(&node_unique, ExecOpFamily::Filter);
    assert_no_exec_op_family(&node_unique, ExecOpFamily::Order);
    assert_no_exec_window(&node_unique);

    let node_range_desc = executable_traversal(
        g().n_with_label_where("User", Predicate::gte("age", 21))
            .order_by("age", Order::Desc),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&node_range_desc, "alternative");
    assert_selected_rule(&node_range_desc, KnownRuleId::SeedAccessPath);
    assert!(matches!(
        unwrapped_first_exec_access(&node_range_desc),
        ExecAccessPlan::Node(ExecNodeAccessPlan::RangeIndex { key, range, .. })
            if key.label == "User"
                && key.property == "age"
                && key.direction == RangeIndexDirection::Desc
                && matches!(
                    range,
                    IndexRange::Lower {
                        lower: IndexBound::Inclusive(_)
                    }
                )
    ));
    assert_no_exec_op_family(&node_range_desc, ExecOpFamily::Filter);
    assert_no_exec_op_family(&node_range_desc, ExecOpFamily::Order);
    assert_no_exec_window(&node_range_desc);

    let edge_equality = executable_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::eq("status", "active")),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&edge_equality, "alternative");
    assert_selected_rule(&edge_equality, KnownRuleId::SeedAccessPath);
    assert!(matches!(
        unwrapped_first_exec_access(&edge_equality),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::EqualityIndex { key, value, .. })
            if key.label == "FOLLOWS"
                && key.property == "status"
                && matches!(value, IndexValue::Literal(_))
    ));
    assert_no_exec_op_family(&edge_equality, ExecOpFamily::Filter);
    assert_no_exec_op_family(&edge_equality, ExecOpFamily::Order);
    assert_no_exec_window(&edge_equality);

    let edge_range_desc = executable_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::lt("weight", 50))
            .order_by("weight", Order::Desc),
        ctx(indexes),
    );
    assert_selected_root_family(&edge_range_desc, "alternative");
    assert_selected_rule(&edge_range_desc, KnownRuleId::SeedAccessPath);
    assert!(matches!(
        unwrapped_first_exec_access(&edge_range_desc),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::RangeIndex { key, range, .. })
            if key.label == "FOLLOWS"
                && key.property == "weight"
                && key.direction == RangeIndexDirection::Desc
                && matches!(
                    range,
                    IndexRange::Upper {
                        upper: IndexBound::Exclusive(_)
                    }
                )
    ));
    assert_no_exec_op_family(&edge_range_desc, ExecOpFamily::Filter);
    assert_no_exec_op_family(&edge_range_desc, ExecOpFamily::Order);
    assert_no_exec_window(&edge_range_desc);
}

#[test]
fn cascades_chosen_access_matrix_proves_scan_runtime_and_label_sources() {
    let node_all = executable_traversal(g().n(NodeRef::all()), PlannerContext::default());
    assert_selected_root_family(&node_all, "alternative");
    assert_selected_rule(&node_all, KnownRuleId::SeedAccessPath);
    assert!(matches!(
        first_kv_read(&node_all),
        KvReadPlan::RangeScan {
            keyspace: ElementKeyspace::NodeProperty,
            limit: None,
            ..
        }
    ));
    assert_no_exec_op_family(&node_all, ExecOpFamily::Filter);
    assert_no_exec_window(&node_all);

    let edge_all = executable_traversal(g().e(EdgeRef::all()), PlannerContext::default());
    assert_selected_root_family(&edge_all, "alternative");
    assert_selected_rule(&edge_all, KnownRuleId::SeedAccessPath);
    assert!(matches!(
        first_kv_read(&edge_all),
        KvReadPlan::RangeScan {
            keyspace: ElementKeyspace::EdgeEndpoints,
            limit: None,
            ..
        }
    ));
    assert_no_exec_op_family(&edge_all, ExecOpFamily::Filter);
    assert_no_exec_window(&edge_all);

    let node_param =
        executable_traversal(g().n(NodeRef::param("node_ids")), PlannerContext::default());
    let node_var = executable_traversal(
        g().n(NodeRef::var("stored_nodes")),
        PlannerContext::default(),
    );
    let edge_param =
        executable_traversal(g().e(EdgeRef::param("edge_ids")), PlannerContext::default());
    let edge_var = executable_traversal(
        g().e(EdgeRef::var("stored_edges")),
        PlannerContext::default(),
    );

    assert!(matches!(
        first_exec_access(&node_param),
        ExecAccessPlan::Node(ExecNodeAccessPlan::FromParam { param })
            if param.as_ref() == "node_ids"
    ));
    assert!(matches!(
        first_exec_access(&node_var),
        ExecAccessPlan::Node(ExecNodeAccessPlan::FromVar { variable })
            if variable.as_ref() == "stored_nodes"
    ));
    assert!(matches!(
        first_exec_access(&edge_param),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::FromParam { param })
            if param.as_ref() == "edge_ids"
    ));
    assert!(matches!(
        first_exec_access(&edge_var),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::FromVar { variable })
            if variable.as_ref() == "stored_edges"
    ));
    for plan in [&node_param, &node_var, &edge_param, &edge_var] {
        assert_selected_root_family(plan, "alternative");
        assert_selected_rule(plan, KnownRuleId::SeedAccessPath);
        assert_no_exec_op_family(plan, ExecOpFamily::Filter);
        assert_no_exec_window(plan);
    }

    let node_label = executable_traversal(g().n_with_label("User"), PlannerContext::default());
    let edge_label = executable_traversal(g().e_with_label("FOLLOWS"), PlannerContext::default());
    assert!(matches!(
        first_exec_access(&node_label),
        ExecAccessPlan::Node(ExecNodeAccessPlan::LabelScan { label })
            if label.as_ref() == "User"
    ));
    assert!(matches!(
        first_exec_access(&edge_label),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::LabelScan { label })
            if label.as_ref() == "FOLLOWS"
    ));
    for plan in [&node_label, &edge_label] {
        assert_selected_root_family(plan, "alternative");
        assert_selected_rule(plan, KnownRuleId::SeedAccessPath);
        assert_no_exec_op_family(plan, ExecOpFamily::Filter);
        assert_no_exec_window(plan);
    }

    let node_residual = executable_traversal(
        g().n_with_label_where("User", Predicate::eq("username", "alice")),
        PlannerContext::default(),
    );
    let edge_residual = executable_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::eq("status", "active")),
        PlannerContext::default(),
    );
    assert!(matches!(
        first_exec_access(&node_residual),
        ExecAccessPlan::Node(ExecNodeAccessPlan::LabelScan { label })
            if label.as_ref() == "User"
    ));
    assert!(matches!(
        first_exec_access(&edge_residual),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::LabelScan { label })
            if label.as_ref() == "FOLLOWS"
    ));
    for (plan, expected) in [
        (&node_residual, Predicate::eq("username", "alice")),
        (&edge_residual, Predicate::eq("status", "active")),
    ] {
        assert_selected_root_family(plan, "alternative");
        assert_selected_rule(plan, KnownRuleId::SeedAccessFilter);
        assert!(matches!(
            first_exec_op(plan, |op| matches!(op, ExecOp::Filter { .. })),
            ExecOp::Filter { predicate } if predicate == &PredicatePlan::new(expected).unwrap()
        ));
        assert_no_exec_window(plan);
    }
}

#[test]
fn cascades_stream_wrapper_matrix_preserves_indexed_access_and_wrapper_ops() {
    let indexes = chosen_plan_indexes();

    let node_filter = executable_traversal(
        g().n_with_label_where("User", Predicate::eq("username", "alice"))
            .has_key("email"),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&node_filter, "alternative");
    assert_selected_rule(&node_filter, KnownRuleId::SeedAccessPipeline);
    assert!(matches!(
        unwrapped_first_exec_access(&node_filter),
        ExecAccessPlan::Node(ExecNodeAccessPlan::EqualityIndex { key, .. })
            if key.label == "User" && key.property == "username"
    ));
    assert!(matches!(
        first_exec_op(&node_filter, |op| matches!(op, ExecOp::Filter { .. })),
        ExecOp::Filter { predicate }
            if predicate == &PredicatePlan::new(Predicate::has_key("email")).unwrap()
    ));
    assert_no_exec_op_family(&node_filter, ExecOpFamily::Order);
    assert_no_exec_window(&node_filter);

    let edge_filter = executable_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::eq("status", "active"))
            .edge_has("verified", true),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&edge_filter, "alternative");
    assert_selected_rule(&edge_filter, KnownRuleId::SeedAccessPipeline);
    assert!(matches!(
        unwrapped_first_exec_access(&edge_filter),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::EqualityIndex { key, .. })
            if key.label == "FOLLOWS" && key.property == "status"
    ));
    assert!(matches!(
        first_exec_op(&edge_filter, |op| matches!(op, ExecOp::Filter { .. })),
        ExecOp::Filter { predicate }
            if predicate == &PredicatePlan::new(Predicate::eq("verified", true)).unwrap()
    ));
    assert_no_exec_op_family(&edge_filter, ExecOpFamily::Order);
    assert_no_exec_window(&edge_filter);

    let node_label_filter = executable_traversal(
        g().n_with_label_where("User", Predicate::eq("username", "alice"))
            .has_label("User"),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&node_label_filter, "alternative");
    assert_selected_rule(&node_label_filter, KnownRuleId::SeedAccessPath);
    assert!(matches!(
        unwrapped_first_exec_access(&node_label_filter),
        ExecAccessPlan::Node(ExecNodeAccessPlan::EqualityIndex { key, .. })
            if key.label == "User" && key.property == "username"
    ));
    assert_no_exec_op_family(&node_label_filter, ExecOpFamily::Filter);
    assert_no_exec_op_family(&node_label_filter, ExecOpFamily::Order);
    assert_no_exec_window(&node_label_filter);

    let edge_label_filter = executable_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::eq("status", "active"))
            .edge_has_label("FOLLOWS"),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&edge_label_filter, "alternative");
    assert_selected_rule(&edge_label_filter, KnownRuleId::SeedAccessPath);
    assert!(matches!(
        unwrapped_first_exec_access(&edge_label_filter),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::EqualityIndex { key, .. })
            if key.label == "FOLLOWS" && key.property == "status"
    ));
    assert_no_exec_op_family(&edge_label_filter, ExecOpFamily::Filter);
    assert_no_exec_op_family(&edge_label_filter, ExecOpFamily::Order);
    assert_no_exec_window(&edge_label_filter);

    let generic_where = executable_traversal(
        g().n_with_label_where("User", Predicate::eq("username", "alice"))
            .where_(Predicate::eq("active", true)),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&generic_where, "alternative");
    assert_selected_rule(&generic_where, KnownRuleId::SeedAccessPipeline);
    assert!(matches!(
        unwrapped_first_exec_access(&generic_where),
        ExecAccessPlan::Node(ExecNodeAccessPlan::EqualityIndex { key, .. })
            if key.label == "User" && key.property == "username"
    ));
    assert!(matches!(
        first_exec_op(&generic_where, |op| matches!(op, ExecOp::Filter { .. })),
        ExecOp::Filter { predicate }
            if predicate == &PredicatePlan::new(Predicate::eq("active", true)).unwrap()
    ));
    assert_no_exec_op_family(&generic_where, ExecOpFamily::Order);
    assert_no_exec_window(&generic_where);

    let explicit_order = executable_traversal(
        g().n_with_label_where("User", Predicate::eq("username", "alice"))
            .order_by("last_seen", Order::Desc),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&explicit_order, "alternative");
    assert_selected_rule(&explicit_order, KnownRuleId::SeedAccessPipeline);
    assert!(matches!(
        unwrapped_first_exec_access(&explicit_order),
        ExecAccessPlan::Node(ExecNodeAccessPlan::EqualityIndex { key, .. })
            if key.label == "User" && key.property == "username"
    ));
    assert!(matches!(
        first_exec_op(&explicit_order, |op| matches!(op, ExecOp::Order { .. })),
        ExecOp::Order {
            plan: OrderPlan::ExplicitSort(keys),
        } if matches!(
            keys.as_ref(),
            [OrderKey { property, order }]
                if property.as_ref() == "last_seen" && *order == Order::Desc
        )
    ));
    assert_no_exec_op_family(&explicit_order, ExecOpFamily::Filter);
    assert_no_exec_window(&explicit_order);

    let indexed_skip = executable_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::eq("status", "active"))
            .skip(1usize),
        ctx(indexes.clone()),
    );
    assert_access_window_selected(&indexed_skip);
    assert_eq!(first_limited_access_limit(&indexed_skip), None);
    assert!(matches!(
        unwrapped_first_exec_access(&indexed_skip),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::EqualityIndex { key, .. })
            if key.label == "FOLLOWS" && key.property == "status"
    ));
    assert!(matches!(
        first_exec_op(&indexed_skip, |op| matches!(op, ExecOp::Skip { .. })),
        ExecOp::Skip {
            count: StreamBoundPlan::Literal(1),
        }
    ));
    assert_no_exec_op_family(&indexed_skip, ExecOpFamily::Filter);
    assert_no_exec_op_family(&indexed_skip, ExecOpFamily::Order);
    assert_no_exec_op_family(&indexed_skip, ExecOpFamily::Limit);
    assert_no_exec_op_family(&indexed_skip, ExecOpFamily::Range);

    let variables = executable_traversal(
        g().n_with_label_where("User", Predicate::eq("username", "alice"))
            .as_("seen")
            .select("seen")
            .bind("user")
            .without("blocked"),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&variables, "alternative");
    assert_selected_rule(&variables, KnownRuleId::SeedAccessPipeline);
    assert!(matches!(
        unwrapped_first_exec_access(&variables),
        ExecAccessPlan::Node(ExecNodeAccessPlan::EqualityIndex { key, .. })
            if key.label == "User" && key.property == "username"
    ));
    let variable_ops = variables
        .steps()
        .iter()
        .filter_map(|step| match &step.op {
            ExecOp::Variable {
                op: ExecVariableOp::Stream(op),
            } => Some(op),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(matches!(
        variable_ops.as_slice(),
        [
            StreamVariableOp::As(seen_as),
            StreamVariableOp::Select(seen_select),
            StreamVariableOp::Bind(user),
            StreamVariableOp::Without(blocked),
        ] if seen_as.as_ref() == "seen"
            && seen_select.as_ref() == "seen"
            && user.as_ref() == "user"
            && blocked.as_ref() == "blocked"
    ));
    assert_no_exec_op_family(&variables, ExecOpFamily::Filter);
    assert_no_exec_op_family(&variables, ExecOpFamily::Order);
    assert_no_exec_window(&variables);

    let store = executable_traversal(
        g().n_with_label_where("User", Predicate::eq("username", "alice"))
            .store("cached_users"),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&store, "alternative");
    assert_selected_rule(&store, KnownRuleId::SeedAccessPipeline);
    assert!(matches!(
        unwrapped_first_exec_access(&store),
        ExecAccessPlan::Node(ExecNodeAccessPlan::EqualityIndex { key, .. })
            if key.label == "User" && key.property == "username"
    ));
    assert!(matches!(
        first_exec_op(&store, |op| matches!(op, ExecOp::Variable { .. })),
        ExecOp::Variable {
            op: ExecVariableOp::Stream(StreamVariableOp::Store(variable))
        } if variable.as_ref() == "cached_users"
    ));
    assert_no_exec_op_family(&store, ExecOpFamily::Filter);
    assert_no_exec_op_family(&store, ExecOpFamily::Order);
    assert_no_exec_window(&store);

    let stream_inject = executable_ast(
        AstNode::Inject {
            input: Some(Box::new(AstNode::NodesWhere {
                predicate: Predicate::and(vec![
                    Predicate::eq("$label", "User"),
                    Predicate::eq("username", "alice"),
                ]),
            })),
            variable: "cached_users".to_owned(),
        },
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&stream_inject, "alternative");
    assert_selected_rule(&stream_inject, KnownRuleId::SeedAccessPipeline);
    assert!(matches!(
        unwrapped_first_exec_access(&stream_inject),
        ExecAccessPlan::Node(ExecNodeAccessPlan::EqualityIndex { key, .. })
            if key.label == "User" && key.property == "username"
    ));
    assert!(matches!(
        first_exec_op(&stream_inject, |op| matches!(
            op,
            ExecOp::Variable { .. }
        )),
        ExecOp::Variable {
            op: ExecVariableOp::Stream(StreamVariableOp::Inject(variable))
        } if variable.as_ref() == "cached_users"
    ));
    assert_no_exec_op_family(&stream_inject, ExecOpFamily::Filter);
    assert_no_exec_op_family(&stream_inject, ExecOpFamily::Order);
    assert_no_exec_window(&stream_inject);

    let dedup = executable_traversal(
        g().n_with_label_where("User", Predicate::eq("username", "alice"))
            .dedup(),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&dedup, "alternative");
    assert_selected_rule(&dedup, KnownRuleId::SeedAccessPipeline);
    assert!(matches!(
        unwrapped_first_exec_access(&dedup),
        ExecAccessPlan::Node(ExecNodeAccessPlan::EqualityIndex { key, .. })
            if key.label == "User" && key.property == "username"
    ));
    assert!(matches!(
        first_exec_op(&dedup, |op| matches!(op, ExecOp::Distinct)),
        ExecOp::Distinct
    ));
    assert_no_exec_op_family(&dedup, ExecOpFamily::Filter);
    assert_no_exec_op_family(&dedup, ExecOpFamily::Order);
    assert_no_exec_window(&dedup);

    let node_expand = executable_traversal(
        g().n_with_label_where("User", Predicate::eq("username", "alice"))
            .out_e(Some("FOLLOWS")),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&node_expand, "alternative");
    assert_selected_rule(&node_expand, KnownRuleId::SeedAccessPipeline);
    assert!(matches!(
        unwrapped_first_exec_access(&node_expand),
        ExecAccessPlan::Node(ExecNodeAccessPlan::EqualityIndex { key, .. })
            if key.label == "User" && key.property == "username"
    ));
    assert!(matches!(
        first_exec_op(&node_expand, |op| matches!(op, ExecOp::Expand { .. })),
        ExecOp::Expand {
            plan: ExpandPlan {
                direction: ExpandDirection::Out,
                output: ExpandOutput::Edges,
                label: ExpandLabelPlan::Label(label),
            },
        } if label.as_ref() == "FOLLOWS"
    ));
    assert_no_exec_op_family(&node_expand, ExecOpFamily::Filter);
    assert_no_exec_op_family(&node_expand, ExecOpFamily::Order);
    assert_no_exec_window(&node_expand);

    let edge_expand = executable_traversal(
        g().e_with_label_where("FOLLOWS", Predicate::eq("status", "active"))
            .out_n(),
        ctx(indexes),
    );
    assert_selected_root_family(&edge_expand, "alternative");
    assert_selected_rule(&edge_expand, KnownRuleId::SeedAccessPipeline);
    assert!(matches!(
        unwrapped_first_exec_access(&edge_expand),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::EqualityIndex { key, .. })
            if key.label == "FOLLOWS" && key.property == "status"
    ));
    assert!(matches!(
        first_exec_op(&edge_expand, |op| matches!(op, ExecOp::Expand { .. })),
        ExecOp::Expand {
            plan: ExpandPlan {
                direction: ExpandDirection::Out,
                output: ExpandOutput::Nodes,
                label: ExpandLabelPlan::Any,
            },
        }
    ));
    assert_no_exec_op_family(&edge_expand, ExecOpFamily::Filter);
    assert_no_exec_op_family(&edge_expand, ExecOpFamily::Order);
    assert_no_exec_window(&edge_expand);

    let source_inject = executable_traversal(g().inject("cached_users"), PlannerContext::default());
    assert_selected_root_family(&source_inject, "alternative");
    assert_selected_rule(&source_inject, KnownRuleId::SeedVariableSource);
    assert_eq!(source_inject.steps().len(), 1);
    assert!(matches!(
        &source_inject.steps()[0].op,
        ExecOp::Variable {
            op: ExecVariableOp::SourceInject { variable },
        } if variable.as_ref() == "cached_users"
    ));
}

#[test]
fn cascades_chosen_access_matrix_proves_point_and_search_families() {
    let indexes = chosen_plan_indexes();

    let node_points = executable_traversal(g().n([7u64, 9]), PlannerContext::default());
    assert_selected_root_family(&node_points, "alternative");
    assert_selected_rule(&node_points, KnownRuleId::SeedAccessPath);
    assert!(matches!(
        first_kv_read(&node_points),
        KvReadPlan::MultiGet(batch)
            if batch.keyspace() == ElementKeyspace::NodeProperty && batch.len() == 2
    ));
    assert_no_exec_op_family(&node_points, ExecOpFamily::Filter);
    assert_no_exec_op_family(&node_points, ExecOpFamily::Order);
    assert_no_exec_window(&node_points);

    let edge_points = executable_traversal(g().e([3u64, 4, 5]), PlannerContext::default());
    assert_selected_root_family(&edge_points, "alternative");
    assert_selected_rule(&edge_points, KnownRuleId::SeedAccessPath);
    assert!(matches!(
        first_kv_read(&edge_points),
        KvReadPlan::MultiGet(batch)
            if batch.keyspace() == ElementKeyspace::EdgeEndpoints && batch.len() == 3
    ));
    assert_no_exec_op_family(&edge_points, ExecOpFamily::Filter);
    assert_no_exec_op_family(&edge_points, ExecOpFamily::Order);
    assert_no_exec_window(&edge_points);

    let node_vector = executable_traversal(
        g().vector_search_nodes(
            "Doc",
            "embedding",
            vec![0.1f32, 0.2],
            5,
            Some("acme".into()),
        ),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&node_vector, "alternative");
    assert_selected_rule(&node_vector, KnownRuleId::SeedAccessPath);
    assert!(matches!(
        unwrapped_first_exec_access(&node_vector),
        ExecAccessPlan::Node(ExecNodeAccessPlan::VectorSearch {
            key,
            index,
            query_vector,
            ..
        }) if key.label == "Doc"
            && key.property == "embedding"
            && matches!(
                &index.tenant,
                SearchTenantPlan::ScopedValue { property, .. } if property.as_ref() == "tenant_id"
            )
            && matches!(query_vector, VectorQueryInputPlan::Vector(_))
    ));
    assert_eq!(literal_exec_search_k(&node_vector), 5);
    assert_no_exec_op_family(&node_vector, ExecOpFamily::Filter);
    assert_no_exec_op_family(&node_vector, ExecOpFamily::Order);
    assert_no_exec_window(&node_vector);

    let node_text = executable_traversal(
        g().text_search_nodes("Doc", "body", "planner", 6, Some("acme".into())),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&node_text, "alternative");
    assert_selected_rule(&node_text, KnownRuleId::SeedAccessPath);
    assert!(matches!(
        unwrapped_first_exec_access(&node_text),
        ExecAccessPlan::Node(ExecNodeAccessPlan::TextSearch {
            key,
            index,
            query_text,
            ..
        }) if key.label == "Doc"
            && key.property == "body"
            && matches!(
                &index.tenant,
                SearchTenantPlan::ScopedValue { property, .. } if property.as_ref() == "tenant_id"
            )
            && matches!(query_text, TextQueryInputPlan::Text(text) if text.as_ref() == "planner")
    ));
    assert_eq!(literal_exec_search_k(&node_text), 6);
    assert_no_exec_op_family(&node_text, ExecOpFamily::Filter);
    assert_no_exec_op_family(&node_text, ExecOpFamily::Order);
    assert_no_exec_window(&node_text);

    let edge_vector = executable_traversal(
        g().vector_search_edges(
            "MENTIONS",
            "embedding",
            vec![0.3f32, 0.4],
            4,
            Some("acme".into()),
        ),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&edge_vector, "alternative");
    assert_selected_rule(&edge_vector, KnownRuleId::SeedAccessPath);
    assert!(matches!(
        unwrapped_first_exec_access(&edge_vector),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::VectorSearch {
            key,
            index,
            query_vector,
            ..
        }) if key.label == "MENTIONS"
            && key.property == "embedding"
            && matches!(
                &index.tenant,
                SearchTenantPlan::ScopedValue { property, .. } if property.as_ref() == "tenant_id"
            )
            && matches!(query_vector, VectorQueryInputPlan::Vector(_))
    ));
    assert_eq!(literal_exec_search_k(&edge_vector), 4);
    assert_no_exec_op_family(&edge_vector, ExecOpFamily::Filter);
    assert_no_exec_op_family(&edge_vector, ExecOpFamily::Order);
    assert_no_exec_window(&edge_vector);

    let edge_text = executable_traversal(
        g().text_search_edges("MENTIONS", "body", "planner", 7, None),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&edge_text, "alternative");
    assert_selected_rule(&edge_text, KnownRuleId::SeedAccessPath);
    assert!(matches!(
        unwrapped_first_exec_access(&edge_text),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::TextSearch {
            key, index, ..
        }) if key.label == "MENTIONS"
            && key.property == "body"
            && index.tenant == SearchTenantPlan::Unscoped
    ));
    assert_eq!(literal_exec_search_k(&edge_text), 7);
    assert_no_exec_op_family(&edge_text, ExecOpFamily::Filter);
    assert_no_exec_op_family(&edge_text, ExecOpFamily::Order);
    assert_no_exec_window(&edge_text);

    let edge_text_tenant = executable_traversal(
        g().text_search_edges("MENTIONS", "tenant_body", "planner", 8, Some("acme".into())),
        ctx(indexes),
    );
    assert_selected_root_family(&edge_text_tenant, "alternative");
    assert_selected_rule(&edge_text_tenant, KnownRuleId::SeedAccessPath);
    assert!(matches!(
        unwrapped_first_exec_access(&edge_text_tenant),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::TextSearch {
            key,
            index,
            query_text,
            ..
        }) if key.label == "MENTIONS"
            && key.property == "tenant_body"
            && matches!(
                &index.tenant,
                SearchTenantPlan::ScopedValue { property, .. } if property.as_ref() == "tenant_id"
            )
            && matches!(query_text, TextQueryInputPlan::Text(text) if text.as_ref() == "planner")
    ));
    assert_eq!(literal_exec_search_k(&edge_text_tenant), 8);
    assert_no_exec_op_family(&edge_text_tenant, ExecOpFamily::Filter);
    assert_no_exec_op_family(&edge_text_tenant, ExecOpFamily::Order);
    assert_no_exec_window(&edge_text_tenant);
}

#[test]
fn cascades_chosen_access_matrix_proves_index_set_families() {
    let indexes = chosen_plan_indexes();

    let node_union = executable_traversal(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::eq("username", "alice"),
                Predicate::eq("username", "bob"),
            ]),
        ),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&node_union, "alternative");
    assert_selected_rule(&node_union, KnownRuleId::SeedAccessPath);
    assert!(
        node_union.steps().iter().any(
            |step| matches!(&step.op, ExecOp::Merge { mode } if *mode == ExecMergeMode::Union)
        ),
        "missing union merge in plan: {:?}",
        node_union.steps()
    );
    assert_eq!(
        access_steps_matching(&node_union, |access| matches!(
            access,
            ExecAccessPlan::Node(ExecNodeAccessPlan::EqualityIndex { key, .. })
                if key.label == "User" && key.property == "username"
        )),
        2
    );
    assert_no_exec_op_family(&node_union, ExecOpFamily::Filter);
    assert_no_exec_op_family(&node_union, ExecOpFamily::Order);
    assert_no_exec_window(&node_union);

    let edge_intersection = executable_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::eq("status", "active"),
                Predicate::lt("weight", 50),
            ]),
        ),
        ctx(indexes),
    );
    assert_selected_root_family(&edge_intersection, "alternative");
    assert_selected_rule(&edge_intersection, KnownRuleId::SeedAccessPath);
    assert!(
        edge_intersection.steps().iter().any(
            |step| matches!(&step.op, ExecOp::Merge { mode } if *mode == ExecMergeMode::Intersect)
        ),
        "missing intersect merge in plan: {:?}",
        edge_intersection.steps()
    );
    assert_eq!(
        access_steps_matching(&edge_intersection, |access| matches!(
            access,
            ExecAccessPlan::Edge(ExecEdgeAccessPlan::EqualityIndex { key, .. })
                if key.label == "FOLLOWS" && key.property == "status"
        )),
        1
    );
    assert_eq!(
        access_steps_matching(&edge_intersection, |access| matches!(
            access,
            ExecAccessPlan::Edge(ExecEdgeAccessPlan::RangeIndex { key, range, .. })
                if key.label == "FOLLOWS"
                    && key.property == "weight"
                    && matches!(
                        range,
                        IndexRange::Upper {
                            upper: IndexBound::Exclusive(_)
                        }
                    )
        )),
        1
    );
    assert_no_exec_op_family(&edge_intersection, ExecOpFamily::Filter);
    assert_no_exec_op_family(&edge_intersection, ExecOpFamily::Order);
    assert_no_exec_window(&edge_intersection);
}

#[test]
fn cascades_index_set_limits_remain_semantic_after_set_merges() {
    let indexes = chosen_plan_indexes();

    let limited_node_union = executable_traversal(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::eq("username", "alice"),
                Predicate::eq("username", "bob"),
            ]),
        )
        .limit(1usize),
        ctx(indexes.clone()),
    );
    assert_access_window_selected(&limited_node_union);
    assert!(
        limited_node_union.steps().iter().any(
            |step| matches!(&step.op, ExecOp::Merge { mode } if *mode == ExecMergeMode::Union)
        ),
        "missing union merge in plan: {:?}",
        limited_node_union.steps()
    );
    assert_eq!(
        access_steps_matching(&limited_node_union, |access| matches!(
            access,
            ExecAccessPlan::Node(ExecNodeAccessPlan::EqualityIndex { key, .. })
                if key.label == "User" && key.property == "username"
        )),
        2
    );
    assert_no_exec_op_family(&limited_node_union, ExecOpFamily::Filter);
    assert_no_exec_op_family(&limited_node_union, ExecOpFamily::Order);
    assert_retained_static_prefix_window(&limited_node_union, 1);

    let limited_edge_intersection = executable_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::eq("status", "active"),
                Predicate::lt("weight", 50),
            ]),
        )
        .limit(1usize),
        ctx(indexes),
    );
    assert_access_window_selected(&limited_edge_intersection);
    assert!(
        limited_edge_intersection.steps().iter().any(
            |step| matches!(&step.op, ExecOp::Merge { mode } if *mode == ExecMergeMode::Intersect)
        ),
        "missing intersect merge in plan: {:?}",
        limited_edge_intersection.steps()
    );
    assert_eq!(
        access_steps_matching(&limited_edge_intersection, |access| matches!(
            access,
            ExecAccessPlan::Edge(ExecEdgeAccessPlan::EqualityIndex { key, .. })
                if key.label == "FOLLOWS" && key.property == "status"
        )),
        1
    );
    assert_eq!(
        access_steps_matching(&limited_edge_intersection, |access| matches!(
            access,
            ExecAccessPlan::Edge(ExecEdgeAccessPlan::RangeIndex { key, .. })
                if key.label == "FOLLOWS" && key.property == "weight"
        )),
        1
    );
    assert_no_exec_op_family(&limited_edge_intersection, ExecOpFamily::Filter);
    assert_no_exec_op_family(&limited_edge_intersection, ExecOpFamily::Order);
    assert_retained_static_prefix_window(&limited_edge_intersection, 1);
}

#[test]
fn cascades_index_set_ranges_push_end_caps_and_keep_range_suffixes() {
    let indexes = chosen_plan_indexes();

    let ranged_node_union = executable_traversal(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::eq("username", "alice"),
                Predicate::eq("username", "bob"),
            ]),
        )
        .range(1usize, 2usize),
        ctx(indexes.clone()),
    );
    assert_access_window_selected(&ranged_node_union);
    assert!(
        ranged_node_union.steps().iter().any(
            |step| matches!(&step.op, ExecOp::Merge { mode } if *mode == ExecMergeMode::Union)
        ),
        "missing union merge in plan: {:?}",
        ranged_node_union.steps()
    );
    assert!(matches!(
        first_exec_op(&ranged_node_union, |op| matches!(op, ExecOp::Limit { .. })),
        ExecOp::Limit {
            count: StreamBoundPlan::Literal(2),
        }
    ));
    assert_exec_range(&ranged_node_union, 1, 2);
    assert_no_exec_op_family(&ranged_node_union, ExecOpFamily::Filter);
    assert_no_exec_op_family(&ranged_node_union, ExecOpFamily::Order);

    let ranged_edge_intersection = executable_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::eq("status", "active"),
                Predicate::lt("weight", 50),
            ]),
        )
        .range(1usize, 2usize),
        ctx(indexes),
    );
    assert_access_window_selected(&ranged_edge_intersection);
    assert!(
        ranged_edge_intersection.steps().iter().any(
            |step| matches!(&step.op, ExecOp::Merge { mode } if *mode == ExecMergeMode::Intersect)
        ),
        "missing intersect merge in plan: {:?}",
        ranged_edge_intersection.steps()
    );
    assert!(matches!(
        first_exec_op(&ranged_edge_intersection, |op| matches!(
            op,
            ExecOp::Limit { .. }
        )),
        ExecOp::Limit {
            count: StreamBoundPlan::Literal(2),
        }
    ));
    assert_exec_range(&ranged_edge_intersection, 1, 2);
    assert_no_exec_op_family(&ranged_edge_intersection, ExecOpFamily::Filter);
    assert_no_exec_op_family(&ranged_edge_intersection, ExecOpFamily::Order);
}

#[test]
fn cascades_index_set_dynamic_ranges_remain_downstream_runtime_bounds() {
    let indexes = chosen_plan_indexes();

    let dynamic_node_union = executable_traversal(
        g().n_with_label_where(
            "User",
            Predicate::or(vec![
                Predicate::eq("username", "alice"),
                Predicate::eq("username", "bob"),
            ]),
        )
        .range(
            StreamBound::expr(Expr::param("range_start")),
            StreamBound::expr(Expr::param("range_end")),
        ),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&dynamic_node_union, "alternative");
    assert!(
        dynamic_node_union.steps().iter().any(
            |step| matches!(&step.op, ExecOp::Merge { mode } if *mode == ExecMergeMode::Union)
        ),
        "missing union merge in plan: {:?}",
        dynamic_node_union.steps()
    );
    assert!(matches!(
        first_exec_op(&dynamic_node_union, |op| matches!(op, ExecOp::Range { .. })),
        ExecOp::Range {
            range: StreamRangePlan::Dynamic(_),
        }
    ));
    assert_no_exec_op_family(&dynamic_node_union, ExecOpFamily::Limit);
    assert_no_exec_op_family(&dynamic_node_union, ExecOpFamily::Filter);
    assert_no_exec_op_family(&dynamic_node_union, ExecOpFamily::Order);

    let dynamic_edge_intersection = executable_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::eq("status", "active"),
                Predicate::lt("weight", 50),
            ]),
        )
        .range(
            StreamBound::expr(Expr::param("range_start")),
            StreamBound::expr(Expr::param("range_end")),
        ),
        ctx(indexes),
    );
    assert_selected_root_family(&dynamic_edge_intersection, "alternative");
    assert!(
        dynamic_edge_intersection.steps().iter().any(
            |step| matches!(&step.op, ExecOp::Merge { mode } if *mode == ExecMergeMode::Intersect)
        ),
        "missing intersect merge in plan: {:?}",
        dynamic_edge_intersection.steps()
    );
    assert!(matches!(
        first_exec_op(&dynamic_edge_intersection, |op| matches!(
            op,
            ExecOp::Range { .. }
        )),
        ExecOp::Range {
            range: StreamRangePlan::Dynamic(_),
        }
    ));
    assert_no_exec_op_family(&dynamic_edge_intersection, ExecOpFamily::Limit);
    assert_no_exec_op_family(&dynamic_edge_intersection, ExecOpFamily::Filter);
    assert_no_exec_op_family(&dynamic_edge_intersection, ExecOpFamily::Order);
}

#[test]
fn cascades_chosen_predicate_matrix_proves_static_and_residual_filters() {
    let indexes = chosen_plan_indexes();

    let impossible_node = executable_traversal(
        g().n_with_label("User").where_(Predicate::compare(
            Expr::val(1),
            CompareOp::Eq,
            Expr::val(2),
        )),
        PlannerContext::default(),
    );
    assert_selected_root_family(&impossible_node, "alternative");
    assert!(matches!(
        first_exec_access(&impossible_node),
        ExecAccessPlan::Node(ExecNodeAccessPlan::Empty)
    ));
    assert_no_exec_op_family(&impossible_node, ExecOpFamily::Filter);
    assert_no_exec_op_family(&impossible_node, ExecOpFamily::Order);
    assert_no_exec_window(&impossible_node);

    let tautological_edge = executable_traversal(
        g().e_with_label("FOLLOWS").where_(Predicate::compare(
            Expr::val("same"),
            CompareOp::Eq,
            Expr::val("same"),
        )),
        PlannerContext::default(),
    );
    assert_selected_root_family(&tautological_edge, "alternative");
    assert!(matches!(
        unwrapped_first_exec_access(&tautological_edge),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::LabelScan { label })
            if label.as_ref() == "FOLLOWS"
    ));
    assert_no_exec_op_family(&tautological_edge, ExecOpFamily::Filter);
    assert_no_exec_op_family(&tautological_edge, ExecOpFamily::Order);
    assert_no_exec_window(&tautological_edge);

    let node_residual = executable_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::eq("username", "alice"),
                Predicate::eq("active", true),
            ]),
        ),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&node_residual, "alternative");
    assert_selected_rule(&node_residual, KnownRuleId::SeedAccessPipeline);
    assert!(matches!(
        unwrapped_first_exec_access(&node_residual),
        ExecAccessPlan::Node(ExecNodeAccessPlan::EqualityIndex { key, .. })
            if key.label == "User" && key.property == "username"
    ));
    assert_eq!(
        node_residual
            .steps()
            .iter()
            .filter(|step| matches!(&step.op, ExecOp::Filter { .. }))
            .count(),
        1
    );
    assert!(matches!(
        first_exec_op(&node_residual, |op| matches!(op, ExecOp::Filter { .. })),
        ExecOp::Filter { predicate }
            if predicate == &PredicatePlan::new(Predicate::eq("active", true)).unwrap()
    ));
    [
        ExecOpFamily::Order,
        ExecOpFamily::Limit,
        ExecOpFamily::Skip,
        ExecOpFamily::Range,
    ]
    .into_iter()
    .for_each(|family| assert_no_exec_op_family(&node_residual, family));

    let edge_residual = executable_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::eq("status", "active"),
                Predicate::eq("verified", true),
            ]),
        ),
        ctx(indexes),
    );
    assert_selected_root_family(&edge_residual, "alternative");
    assert_selected_rule(&edge_residual, KnownRuleId::SeedAccessPipeline);
    assert!(matches!(
        unwrapped_first_exec_access(&edge_residual),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::EqualityIndex { key, .. })
            if key.label == "FOLLOWS" && key.property == "status"
    ));
    assert_eq!(
        edge_residual
            .steps()
            .iter()
            .filter(|step| matches!(&step.op, ExecOp::Filter { .. }))
            .count(),
        1
    );
    assert!(matches!(
        first_exec_op(&edge_residual, |op| matches!(op, ExecOp::Filter { .. })),
        ExecOp::Filter { predicate }
            if predicate == &PredicatePlan::new(Predicate::eq("verified", true)).unwrap()
    ));
    [
        ExecOpFamily::Order,
        ExecOpFamily::Limit,
        ExecOpFamily::Skip,
        ExecOpFamily::Range,
    ]
    .into_iter()
    .for_each(|family| assert_no_exec_op_family(&edge_residual, family));
}

#[test]
fn cascades_partial_index_residuals_keep_limits_after_filters() {
    let indexes = chosen_plan_indexes();

    let limited_node = executable_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::eq("username", "alice"),
                Predicate::eq("active", true),
            ]),
        )
        .limit(1usize),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&limited_node, "alternative");
    assert_selected_rule(&limited_node, KnownRuleId::SeedAccessPipeline);
    assert_eq!(first_limited_access_limit(&limited_node), None);
    assert!(matches!(
        unwrapped_first_exec_access(&limited_node),
        ExecAccessPlan::Node(ExecNodeAccessPlan::EqualityIndex { key, .. })
            if key.label == "User" && key.property == "username"
    ));
    assert!(matches!(
        first_exec_op(&limited_node, |op| matches!(op, ExecOp::Filter { .. })),
        ExecOp::Filter { predicate }
            if predicate == &PredicatePlan::new(Predicate::eq("active", true)).unwrap()
    ));
    assert_retained_static_prefix_window(&limited_node, 1);

    let limited_edge = executable_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::eq("status", "active"),
                Predicate::eq("verified", true),
            ]),
        )
        .limit(1usize),
        ctx(indexes),
    );
    assert_selected_root_family(&limited_edge, "alternative");
    assert_selected_rule(&limited_edge, KnownRuleId::SeedAccessPipeline);
    assert_eq!(first_limited_access_limit(&limited_edge), None);
    assert!(matches!(
        unwrapped_first_exec_access(&limited_edge),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::EqualityIndex { key, .. })
            if key.label == "FOLLOWS" && key.property == "status"
    ));
    assert!(matches!(
        first_exec_op(&limited_edge, |op| matches!(op, ExecOp::Filter { .. })),
        ExecOp::Filter { predicate }
            if predicate == &PredicatePlan::new(Predicate::eq("verified", true)).unwrap()
    ));
    assert_retained_static_prefix_window(&limited_edge, 1);
}

#[test]
fn cascades_partial_range_index_residuals_keep_ranges_after_filters() {
    let indexes = chosen_plan_indexes();

    let ranged_node = executable_traversal(
        g().n_with_label_where(
            "User",
            Predicate::and(vec![
                Predicate::gte("age", 21),
                Predicate::eq("active", true),
            ]),
        )
        .order_by("age", Order::Asc)
        .range(1usize, 3usize),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&ranged_node, "alternative");
    assert_selected_rule(&ranged_node, KnownRuleId::SeedAccessPipeline);
    assert_eq!(first_limited_access_limit(&ranged_node), None);
    assert!(matches!(
        unwrapped_first_exec_access(&ranged_node),
        ExecAccessPlan::Node(ExecNodeAccessPlan::RangeIndex { key, range, .. })
            if key.label == "User"
                && key.property == "age"
                && key.direction == RangeIndexDirection::Asc
                && matches!(
                    range,
                    IndexRange::Lower {
                        lower: IndexBound::Inclusive(_)
                    }
                )
    ));
    assert!(matches!(
        first_exec_op(&ranged_node, |op| matches!(op, ExecOp::Filter { .. })),
        ExecOp::Filter { predicate }
            if predicate == &PredicatePlan::new(Predicate::eq("active", true)).unwrap()
    ));
    assert_no_exec_op_family(&ranged_node, ExecOpFamily::Order);
    assert_exec_range(&ranged_node, 1, 3);

    let ranged_edge = executable_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            Predicate::and(vec![
                Predicate::lt("weight", 50),
                Predicate::eq("verified", true),
            ]),
        )
        .order_by("weight", Order::Desc)
        .range(1usize, 3usize),
        ctx(indexes),
    );
    assert_selected_root_family(&ranged_edge, "alternative");
    assert_selected_rule(&ranged_edge, KnownRuleId::SeedAccessPipeline);
    assert_eq!(first_limited_access_limit(&ranged_edge), None);
    assert!(matches!(
        unwrapped_first_exec_access(&ranged_edge),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::RangeIndex { key, range, .. })
            if key.label == "FOLLOWS"
                && key.property == "weight"
                && key.direction == RangeIndexDirection::Desc
                && matches!(
                    range,
                    IndexRange::Upper {
                        upper: IndexBound::Exclusive(_)
                    }
                )
    ));
    assert!(matches!(
        first_exec_op(&ranged_edge, |op| matches!(op, ExecOp::Filter { .. })),
        ExecOp::Filter { predicate }
            if predicate == &PredicatePlan::new(Predicate::eq("verified", true)).unwrap()
    ));
    assert_no_exec_op_family(&ranged_edge, ExecOpFamily::Order);
    assert_exec_range(&ranged_edge, 1, 3);
}

#[test]
fn cascades_limit_pushdown_matrix_proves_tight_caps_and_barriers() {
    let indexes = chosen_plan_indexes();

    let all_scan_limit = executable_traversal(
        g().n(NodeRef::all()).limit(3usize),
        PlannerContext::default(),
    );
    assert_access_window_selected(&all_scan_limit);
    assert_eq!(first_kv_read_limit(&all_scan_limit), Some(3));
    assert_no_exec_window(&all_scan_limit);

    let all_scan_range = executable_traversal(
        g().n(NodeRef::all()).range(2usize, 5usize),
        PlannerContext::default(),
    );
    assert_access_window_selected(&all_scan_range);
    assert_eq!(first_kv_read_limit(&all_scan_range), Some(5));
    assert_exec_range(&all_scan_range, 2, 5);
    assert_no_exec_op_family(&all_scan_range, ExecOpFamily::Limit);
    assert_no_exec_op_family(&all_scan_range, ExecOpFamily::Skip);

    let skip_only = executable_traversal(
        g().n(NodeRef::all()).skip(2usize),
        PlannerContext::default(),
    );
    assert_access_window_selected(&skip_only);
    assert_eq!(first_kv_read_limit(&skip_only), None);
    assert!(has_exec_op_family(&skip_only, ExecOpFamily::Skip));
    assert_no_exec_op_family(&skip_only, ExecOpFamily::Limit);
    assert_no_exec_op_family(&skip_only, ExecOpFamily::Range);

    let all_scan_skip_limit = executable_traversal(
        g().n(NodeRef::all()).skip(2usize).limit(3usize),
        PlannerContext::default(),
    );
    assert_access_window_selected(&all_scan_skip_limit);
    assert_eq!(first_kv_read_limit(&all_scan_skip_limit), Some(5));
    assert_exec_range(&all_scan_skip_limit, 2, 5);
    assert_no_exec_op_family(&all_scan_skip_limit, ExecOpFamily::Limit);
    assert_no_exec_op_family(&all_scan_skip_limit, ExecOpFamily::Skip);

    let equality_limit = executable_traversal(
        g().n_with_label_where("User", Predicate::eq("username", "alice"))
            .limit(2usize),
        ctx(indexes.clone()),
    );
    assert_access_window_selected(&equality_limit);
    assert_eq!(first_limited_access_limit(&equality_limit), Some(2));
    assert!(matches!(
        unwrapped_first_exec_access(&equality_limit),
        ExecAccessPlan::Node(ExecNodeAccessPlan::EqualityIndex { key, .. })
            if key.label == "User" && key.property == "username"
    ));
    assert_no_exec_window(&equality_limit);

    let equality_skip_limit = executable_traversal(
        g().n_with_label_where("User", Predicate::eq("username", "alice"))
            .skip(2usize)
            .limit(3usize),
        ctx(indexes.clone()),
    );
    assert_access_window_selected(&equality_skip_limit);
    assert_eq!(first_limited_access_limit(&equality_skip_limit), Some(5));
    assert!(matches!(
        unwrapped_first_exec_access(&equality_skip_limit),
        ExecAccessPlan::Node(ExecNodeAccessPlan::EqualityIndex { key, .. })
            if key.label == "User" && key.property == "username"
    ));
    assert_exec_range(&equality_skip_limit, 2, 5);
    assert_no_exec_op_family(&equality_skip_limit, ExecOpFamily::Limit);
    assert_no_exec_op_family(&equality_skip_limit, ExecOpFamily::Skip);

    let unique_covering_limit = executable_traversal(
        g().n_with_label_where("User", Predicate::eq("email", "a@example.com"))
            .limit(5usize),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&unique_covering_limit, "alternative");
    assert_selected_rule(&unique_covering_limit, KnownRuleId::SeedAccessPipeline);
    assert_eq!(first_limited_access_limit(&unique_covering_limit), None);
    assert!(matches!(
        unwrapped_first_exec_access(&unique_covering_limit),
        ExecAccessPlan::Node(ExecNodeAccessPlan::EqualityIndex { key, index, .. })
            if key.label == "User"
                && key.property == "email"
                && index.uniqueness == IndexUniqueness::Unique
    ));
    assert_no_exec_op_family(&unique_covering_limit, ExecOpFamily::Distinct);
    assert_no_exec_window(&unique_covering_limit);

    let unique_distinct_covering_limit = executable_traversal(
        g().n_with_label_where("User", Predicate::eq("email", "a@example.com"))
            .dedup()
            .limit(5usize),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&unique_distinct_covering_limit, "alternative");
    assert_selected_rule(
        &unique_distinct_covering_limit,
        KnownRuleId::SeedAccessPipeline,
    );
    assert_eq!(
        first_limited_access_limit(&unique_distinct_covering_limit),
        None
    );
    assert!(matches!(
        unwrapped_first_exec_access(&unique_distinct_covering_limit),
        ExecAccessPlan::Node(ExecNodeAccessPlan::EqualityIndex { key, index, .. })
            if key.label == "User"
                && key.property == "email"
                && index.uniqueness == IndexUniqueness::Unique
    ));
    assert_no_exec_op_family(&unique_distinct_covering_limit, ExecOpFamily::Distinct);
    assert_no_exec_window(&unique_distinct_covering_limit);

    let point_ids_covering_limit = executable_traversal(
        g().n([7u64, 9, 11]).limit(5usize),
        PlannerContext::default(),
    );
    assert_selected_root_family(&point_ids_covering_limit, "alternative");
    assert_selected_rule(&point_ids_covering_limit, KnownRuleId::SeedAccessPath);
    assert!(matches!(
        first_kv_read(&point_ids_covering_limit),
        KvReadPlan::MultiGet(batch)
            if batch.keyspace() == ElementKeyspace::NodeProperty && batch.len() == 3
    ));
    assert_no_exec_window(&point_ids_covering_limit);

    let dynamic_equality_limit = executable_traversal(
        g().n_with_label_where("User", Predicate::eq("username", "alice"))
            .limit(StreamBound::expr(Expr::param("limit"))),
        ctx(indexes.clone()),
    );
    assert_access_window_selected(&dynamic_equality_limit);
    assert_eq!(first_limited_access_limit(&dynamic_equality_limit), None);
    assert!(matches!(
        unwrapped_first_exec_access(&dynamic_equality_limit),
        ExecAccessPlan::Node(ExecNodeAccessPlan::EqualityIndex { key, .. })
            if key.label == "User" && key.property == "username"
    ));
    assert!(matches!(
        first_exec_op(&dynamic_equality_limit, |op| matches!(
            op,
            ExecOp::Limit { .. }
        )),
        ExecOp::Limit {
            count: StreamBoundPlan::Expr(_),
        }
    ));
    [
        ExecOpFamily::Filter,
        ExecOpFamily::Order,
        ExecOpFamily::Skip,
        ExecOpFamily::Range,
    ]
    .into_iter()
    .for_each(|family| assert_no_exec_op_family(&dynamic_equality_limit, family));

    let ordered_range_limit = executable_traversal(
        g().n_with_label_where("User", Predicate::gte("age", 21))
            .order_by("age", Order::Desc)
            .limit(2usize),
        ctx(indexes.clone()),
    );
    assert_access_window_selected(&ordered_range_limit);
    assert_eq!(first_limited_access_limit(&ordered_range_limit), Some(2));
    assert!(matches!(
        unwrapped_first_exec_access(&ordered_range_limit),
        ExecAccessPlan::Node(ExecNodeAccessPlan::RangeIndex { key, .. })
            if key.label == "User"
                && key.property == "age"
                && key.direction == RangeIndexDirection::Desc
    ));
    assert_no_exec_op_family(&ordered_range_limit, ExecOpFamily::Filter);
    assert_no_exec_op_family(&ordered_range_limit, ExecOpFamily::Order);
    assert_no_exec_window(&ordered_range_limit);

    let search_limit_tightened = executable_traversal(
        g().vector_search_nodes(
            "Doc",
            "embedding",
            vec![0.1f32, 0.2],
            5,
            Some("acme".into()),
        )
        .limit(2usize),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&search_limit_tightened, "alternative");
    assert_selected_rule(&search_limit_tightened, KnownRuleId::SeedAccessPath);
    assert_eq!(literal_exec_search_k(&search_limit_tightened), 2);
    assert_eq!(first_limited_access_limit(&search_limit_tightened), None);
    assert_no_exec_window(&search_limit_tightened);

    let search_existing_limit_tighter = executable_traversal(
        g().text_search_edges("MENTIONS", "body", "planner", 3, None)
            .limit(7usize),
        ctx(indexes.clone()),
    );
    assert_selected_root_family(&search_existing_limit_tighter, "alternative");
    assert_selected_rule(&search_existing_limit_tighter, KnownRuleId::SeedAccessPath);
    assert_eq!(literal_exec_search_k(&search_existing_limit_tighter), 3);
    assert_eq!(
        first_limited_access_limit(&search_existing_limit_tighter),
        None
    );
    assert_no_exec_window(&search_existing_limit_tighter);

    let distinct_barrier = executable_traversal(
        g().n(NodeRef::all()).dedup().limit(2usize),
        PlannerContext::default(),
    );
    assert_eq!(first_kv_read_limit(&distinct_barrier), None);
    assert!(has_exec_op_family(
        &distinct_barrier,
        ExecOpFamily::Distinct
    ));
    assert_retained_static_prefix_window(&distinct_barrier, 2);

    let duplicate_free_distinct_limit = executable_traversal(
        g().n([7u64, 9, 11]).dedup().limit(2usize),
        PlannerContext::default(),
    );
    assert_access_window_selected(&duplicate_free_distinct_limit);
    assert!(matches!(
        first_kv_read(&duplicate_free_distinct_limit),
        KvReadPlan::MultiGet(batch)
            if batch.keyspace() == ElementKeyspace::NodeProperty && batch.len() == 2
    ));
    assert_no_exec_op_family(&duplicate_free_distinct_limit, ExecOpFamily::Distinct);
    assert_no_exec_window(&duplicate_free_distinct_limit);

    let duplicate_free_edge_distinct_limit = executable_traversal(
        g().e([3u64, 4, 5]).dedup().limit(2usize),
        PlannerContext::default(),
    );
    assert_access_window_selected(&duplicate_free_edge_distinct_limit);
    assert!(matches!(
        first_kv_read(&duplicate_free_edge_distinct_limit),
        KvReadPlan::MultiGet(batch)
            if batch.keyspace() == ElementKeyspace::EdgeEndpoints && batch.len() == 2
    ));
    assert_no_exec_op_family(&duplicate_free_edge_distinct_limit, ExecOpFamily::Distinct);
    assert_no_exec_window(&duplicate_free_edge_distinct_limit);

    let variable_write_barrier = executable_traversal(
        g().n(NodeRef::all()).store("seen").limit(2usize),
        PlannerContext::default(),
    );
    assert_eq!(first_kv_read_limit(&variable_write_barrier), None);
    assert!(has_exec_op_family(
        &variable_write_barrier,
        ExecOpFamily::Variable
    ));
    assert!(matches!(
        first_exec_op(&variable_write_barrier, |op| matches!(
            op,
            ExecOp::Variable { .. }
        )),
        ExecOp::Variable {
            op: ExecVariableOp::Stream(StreamVariableOp::Store(name)),
        } if name.as_ref() == "seen"
    ));
    assert_retained_static_prefix_window(&variable_write_barrier, 2);

    let variable_filter_barrier = executable_traversal(
        g().n(NodeRef::all()).within("allowed").limit(2usize),
        PlannerContext::default(),
    );
    assert_eq!(first_kv_read_limit(&variable_filter_barrier), None);
    assert!(matches!(
        first_exec_op(&variable_filter_barrier, |op| matches!(
            op,
            ExecOp::Variable { .. }
        )),
        ExecOp::Variable {
            op: ExecVariableOp::Stream(StreamVariableOp::Within(name)),
        } if name.as_ref() == "allowed"
    ));
    assert_retained_static_prefix_window(&variable_filter_barrier, 2);

    let optional_barrier = executable_traversal(
        g().n(NodeRef::all())
            .optional(sub().out(Some("FOLLOWS")))
            .limit(2usize),
        PlannerContext::default(),
    );
    assert_eq!(first_kv_read_limit(&optional_barrier), None);
    assert!(has_exec_op_family(&optional_barrier, ExecOpFamily::Branch));
    assert!(matches!(
        first_exec_op(&optional_barrier, |op| matches!(op, ExecOp::Branch { .. })),
        ExecOp::Branch {
            plan: ExecBranchPlan::Optional(_),
        }
    ));
    assert_retained_static_prefix_window(&optional_barrier, 2);

    let union_barrier = executable_traversal(
        g().n(NodeRef::all())
            .union(vec![
                sub().out(Some("FOLLOWS")),
                sub().in_(Some("MENTIONS")),
            ])
            .limit(2usize),
        PlannerContext::default(),
    );
    assert_eq!(first_kv_read_limit(&union_barrier), None);
    assert!(matches!(
        first_exec_op(&union_barrier, |op| matches!(op, ExecOp::Branch { .. })),
        ExecOp::Branch {
            plan: ExecBranchPlan::Union(branches),
        } if branches.as_ref().len() == 2
    ));
    assert_retained_static_prefix_window(&union_barrier, 2);

    let coalesce_barrier = executable_traversal(
        g().n(NodeRef::all())
            .coalesce(vec![
                sub().out(Some("FOLLOWS")),
                sub().in_(Some("MENTIONS")),
            ])
            .limit(2usize),
        PlannerContext::default(),
    );
    assert_eq!(first_kv_read_limit(&coalesce_barrier), None);
    assert!(matches!(
        first_exec_op(&coalesce_barrier, |op| matches!(
            op,
            ExecOp::Branch { .. }
        )),
        ExecOp::Branch {
            plan: ExecBranchPlan::Coalesce(branches),
        } if branches.as_ref().len() == 2
    ));
    assert_retained_static_prefix_window(&coalesce_barrier, 2);

    let choose_barrier = executable_traversal(
        g().n(NodeRef::all())
            .choose(
                Predicate::eq("active", true),
                sub().out(Some("FOLLOWS")),
                Some(sub().in_(Some("MENTIONS"))),
            )
            .limit(2usize),
        PlannerContext::default(),
    );
    assert_eq!(first_kv_read_limit(&choose_barrier), None);
    assert!(matches!(
        first_exec_op(&choose_barrier, |op| matches!(op, ExecOp::Branch { .. })),
        ExecOp::Branch {
            plan: ExecBranchPlan::ChooseElse { .. },
        }
    ));
    assert_retained_static_prefix_window(&choose_barrier, 2);

    let repeat_barrier = executable_traversal(
        g().n(NodeRef::all())
            .repeat(RepeatConfig::new(sub().out(Some("FOLLOWS"))).times(2))
            .limit(2usize),
        PlannerContext::default(),
    );
    assert_eq!(first_kv_read_limit(&repeat_barrier), None);
    assert!(has_exec_op_family(&repeat_barrier, ExecOpFamily::Repeat));
    assert_retained_static_prefix_window(&repeat_barrier, 2);

    let node_expand_barrier = executable_traversal(
        g().n(NodeRef::all()).out_e(Some("FOLLOWS")).limit(2usize),
        PlannerContext::default(),
    );
    assert_selected_root_family(&node_expand_barrier, "alternative");
    assert_selected_rule(&node_expand_barrier, KnownRuleId::SeedAccessPipeline);
    assert!(matches!(
        first_kv_read(&node_expand_barrier),
        KvReadPlan::RangeScan {
            keyspace: ElementKeyspace::NodeProperty,
            limit: None,
            ..
        }
    ));
    assert!(matches!(
        first_exec_op(&node_expand_barrier, |op| matches!(op, ExecOp::Expand { .. })),
        ExecOp::Expand {
            plan: ExpandPlan {
                direction: ExpandDirection::Out,
                output: ExpandOutput::Edges,
                label: ExpandLabelPlan::Label(label),
            },
        } if label.as_ref() == "FOLLOWS"
    ));
    assert_retained_static_prefix_window(&node_expand_barrier, 2);

    let edge_expand_barrier = executable_traversal(
        g().e(EdgeRef::all()).in_n().limit(2usize),
        PlannerContext::default(),
    );
    assert_selected_root_family(&edge_expand_barrier, "alternative");
    assert_selected_rule(&edge_expand_barrier, KnownRuleId::SeedAccessPipeline);
    assert!(matches!(
        first_kv_read(&edge_expand_barrier),
        KvReadPlan::RangeScan {
            keyspace: ElementKeyspace::EdgeEndpoints,
            limit: None,
            ..
        }
    ));
    assert!(matches!(
        first_exec_op(&edge_expand_barrier, |op| matches!(
            op,
            ExecOp::Expand { .. }
        )),
        ExecOp::Expand {
            plan: ExpandPlan {
                direction: ExpandDirection::In,
                output: ExpandOutput::Nodes,
                label: ExpandLabelPlan::Any,
            },
        }
    ));
    assert_retained_static_prefix_window(&edge_expand_barrier, 2);

    let filter_barrier = executable_traversal(
        g().n_with_label_where("User", Predicate::gte("age", 21))
            .where_(Predicate::eq("active", true))
            .limit(2usize),
        ctx(indexes.clone()),
    );
    assert_eq!(first_limited_access_limit(&filter_barrier), None);
    assert!(has_exec_op_family(&filter_barrier, ExecOpFamily::Filter));
    assert_retained_static_prefix_window(&filter_barrier, 2);

    let sort_barrier = executable_traversal(
        g().n_with_label_where("User", Predicate::gte("age", 21))
            .order_by("score", Order::Asc)
            .limit(2usize),
        ctx(indexes),
    );
    assert_eq!(first_limited_access_limit(&sort_barrier), None);
    assert!(has_exec_op_family(&sort_barrier, ExecOpFamily::Order));
    assert_retained_static_prefix_window(&sort_barrier, 2);
}

#[test]
fn cascades_limit_pushdown_matrix_covers_scan_label_and_runtime_sources() {
    let edge_scan_limit = executable_traversal(
        g().e(EdgeRef::all()).limit(4usize),
        PlannerContext::default(),
    );
    assert_access_window_selected(&edge_scan_limit);
    assert!(matches!(
        first_kv_read(&edge_scan_limit),
        KvReadPlan::RangeScan {
            keyspace: ElementKeyspace::EdgeEndpoints,
            limit: Some(limit),
            ..
        } if limit.get() == 4
    ));
    assert_no_exec_window(&edge_scan_limit);

    let node_label_limit = executable_traversal(
        g().n_with_label("User").limit(3usize),
        PlannerContext::default(),
    );
    assert_access_window_selected(&node_label_limit);
    assert_eq!(first_limited_access_limit(&node_label_limit), Some(3));
    assert!(matches!(
        unwrapped_first_exec_access(&node_label_limit),
        ExecAccessPlan::Node(ExecNodeAccessPlan::LabelScan { label })
            if label.as_ref() == "User"
    ));
    assert_no_exec_op_family(&node_label_limit, ExecOpFamily::Filter);
    assert_no_exec_window(&node_label_limit);

    let edge_label_range = executable_traversal(
        g().e_with_label("FOLLOWS").range(2usize, 6usize),
        PlannerContext::default(),
    );
    assert_access_window_selected(&edge_label_range);
    assert_eq!(first_limited_access_limit(&edge_label_range), Some(6));
    assert!(matches!(
        unwrapped_first_exec_access(&edge_label_range),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::LabelScan { label })
            if label.as_ref() == "FOLLOWS"
    ));
    assert_exec_range(&edge_label_range, 2, 6);
    assert_no_exec_op_family(&edge_label_range, ExecOpFamily::Filter);
    assert_no_exec_op_family(&edge_label_range, ExecOpFamily::Limit);
    assert_no_exec_op_family(&edge_label_range, ExecOpFamily::Skip);

    let node_param_limit = executable_traversal(
        g().n(NodeRef::param("node_ids")).limit(2usize),
        PlannerContext::default(),
    );
    assert_access_window_selected(&node_param_limit);
    assert_eq!(first_limited_access_limit(&node_param_limit), Some(2));
    assert!(matches!(
        unwrapped_first_exec_access(&node_param_limit),
        ExecAccessPlan::Node(ExecNodeAccessPlan::FromParam { param })
            if param.as_ref() == "node_ids"
    ));
    assert_no_exec_window(&node_param_limit);

    let edge_var_range = executable_traversal(
        g().e(EdgeRef::var("cached_edges")).range(1usize, 3usize),
        PlannerContext::default(),
    );
    assert_access_window_selected(&edge_var_range);
    assert_eq!(first_limited_access_limit(&edge_var_range), Some(3));
    assert!(matches!(
        unwrapped_first_exec_access(&edge_var_range),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::FromVar { variable })
            if variable.as_ref() == "cached_edges"
    ));
    assert_exec_range(&edge_var_range, 1, 3);
    assert_no_exec_op_family(&edge_var_range, ExecOpFamily::Limit);
    assert_no_exec_op_family(&edge_var_range, ExecOpFamily::Skip);

    let dynamic_runtime_limit = executable_traversal(
        g().n(NodeRef::param("node_ids"))
            .limit(StreamBound::expr(Expr::param("limit"))),
        PlannerContext::default(),
    );
    assert_access_window_selected(&dynamic_runtime_limit);
    assert_eq!(first_limited_access_limit(&dynamic_runtime_limit), None);
    assert!(matches!(
        unwrapped_first_exec_access(&dynamic_runtime_limit),
        ExecAccessPlan::Node(ExecNodeAccessPlan::FromParam { param })
            if param.as_ref() == "node_ids"
    ));
    assert!(matches!(
        first_exec_op(&dynamic_runtime_limit, |op| matches!(
            op,
            ExecOp::Limit { .. }
        )),
        ExecOp::Limit {
            count: StreamBoundPlan::Expr(_),
        }
    ));
    assert_no_exec_op_family(&dynamic_runtime_limit, ExecOpFamily::Filter);
    assert_no_exec_op_family(&dynamic_runtime_limit, ExecOpFamily::Order);
    assert_no_exec_op_family(&dynamic_runtime_limit, ExecOpFamily::Skip);
    assert_no_exec_op_family(&dynamic_runtime_limit, ExecOpFamily::Range);
}

fn chosen_plan_indexes() -> IndexCatalogSnapshot {
    let unique_email = ScopedPropertyKey::try_new("User", "email").unwrap();
    let mut indexes = builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
        .with_node_eq(unique_email.clone())
        .with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        )
        .with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Desc).unwrap(),
        )
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
        .with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
        )
        .with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Desc)
                .unwrap(),
        )
        .with_vector(
            SearchIndexKey::try_new(ElementKind::Node, "Doc", "embedding").unwrap(),
            SearchIndexScope::Tenant {
                property: NonEmptyString::new("tenant_id").unwrap(),
            },
        )
        .with_text(
            SearchIndexKey::try_new(ElementKind::Node, "Doc", "body").unwrap(),
            SearchIndexScope::Tenant {
                property: NonEmptyString::new("tenant_id").unwrap(),
            },
        )
        .with_vector(
            SearchIndexKey::try_new(ElementKind::Edge, "MENTIONS", "embedding").unwrap(),
            SearchIndexScope::Tenant {
                property: NonEmptyString::new("tenant_id").unwrap(),
            },
        )
        .with_text(
            SearchIndexKey::try_new(ElementKind::Edge, "MENTIONS", "body").unwrap(),
            SearchIndexScope::Unscoped,
        )
        .with_text(
            SearchIndexKey::try_new(ElementKind::Edge, "MENTIONS", "tenant_body").unwrap(),
            SearchIndexScope::Tenant {
                property: NonEmptyString::new("tenant_id").unwrap(),
            },
        );
    indexes
        .node_eq
        .get_mut(&unique_email)
        .expect("unique email index was inserted")
        .uniqueness = IndexUniqueness::Unique;
    indexes
}

fn assert_access_window_selected(plan: &ExecutablePlan) {
    assert_selected_root_family(plan, "alternative");
    assert!(
        [
            KnownRuleId::SeedAccessWindow,
            KnownRuleId::SeedAccessPipeline
        ]
        .into_iter()
        .map(RuleId::known)
        .any(|expected| {
            plan.trace().events.iter().any(|event| {
                matches!(
                    &event.reason,
                    TraceReason::SelectedOptimizerRule(rule)
                        if rule.as_ref() == expected.as_ref()
                )
            })
        }),
        "missing selected access-window rule in trace: {:?}",
        plan.trace().events
    );
}

fn assert_retained_static_prefix_window(plan: &ExecutablePlan, end: usize) {
    assert!(
        has_exec_op_family(plan, ExecOpFamily::Limit)
            || has_exec_op_family(plan, ExecOpFamily::Range),
        "expected retained downstream window in plan: {:?}",
        plan.steps()
    );
    if has_exec_op_family(plan, ExecOpFamily::Range) {
        assert_exec_range(plan, 0, end);
    }
}
