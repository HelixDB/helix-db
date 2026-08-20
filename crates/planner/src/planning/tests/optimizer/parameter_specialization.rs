use crate::planning::tests::support::*;

#[test]
fn ordinary_request_membership_parameters_match_literal_index_access() {
    let indexes = builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("Person", "$label").unwrap())
        .with_node_eq(ScopedPropertyKey::try_new("Person", "orbit_id").unwrap())
        .with_edge_eq(ScopedPropertyKey::try_new("MEMBER_OF", "$label").unwrap())
        .with_edge_eq(ScopedPropertyKey::try_new("MEMBER_OF", "orbit_id").unwrap());
    let mut planner_ctx = ctx(indexes.clone());
    planner_ctx.params = ParamBindings::default().with_query_value(
        NonEmptyString::new("orbit_ids").unwrap(),
        QueryValue::Array(vec![
            QueryValue::String("orbit-1".to_owned()),
            QueryValue::String("orbit-2".to_owned()),
        ]),
    );

    let parameterized_node = executable_traversal(
        g().n_with_label("Person")
            .where_(Predicate::is_in_param("orbit_id", "orbit_ids")),
        planner_ctx.clone(),
    );
    let literal_node = executable_traversal(
        g().n_with_label("Person").where_(Predicate::is_in(
            "orbit_id",
            PropertyValue::StringArray(vec!["orbit-1".to_owned(), "orbit-2".to_owned()]),
        )),
        ctx(indexes.clone()),
    );
    assert_eq!(
        unwrapped_first_exec_access(&parameterized_node),
        unwrapped_first_exec_access(&literal_node)
    );
    assert_batched_node_equality_set(&parameterized_node, "Person", "orbit_id", 2);
    assert_no_exec_op_family(&parameterized_node, ExecOpFamily::Filter);

    let parameterized_edge = executable_traversal(
        g().e_with_label("MEMBER_OF")
            .where_(Predicate::is_in_param("orbit_id", "orbit_ids")),
        planner_ctx,
    );
    let literal_edge = executable_traversal(
        g().e_with_label("MEMBER_OF").where_(Predicate::is_in(
            "orbit_id",
            PropertyValue::StringArray(vec!["orbit-1".to_owned(), "orbit-2".to_owned()]),
        )),
        ctx(indexes),
    );
    assert_eq!(
        unwrapped_first_exec_access(&parameterized_edge),
        unwrapped_first_exec_access(&literal_edge)
    );
    assert_batched_edge_equality_set(&parameterized_edge, "MEMBER_OF", "orbit_id", 2);
    assert_no_exec_op_family(&parameterized_edge, ExecOpFamily::Filter);
}

#[test]
fn ordinary_request_label_membership_matches_literal_label_union() {
    let user = NonEmptyString::new("User").unwrap();
    let account = NonEmptyString::new("Account").unwrap();
    let stats = StatsSnapshot::default()
        .with_node_label_cardinality(user, 1)
        .with_node_label_cardinality(account, 1);
    let mut parameterized_ctx = PlannerContext {
        stats: stats.clone(),
        ..PlannerContext::default()
    };
    parameterized_ctx.params = ParamBindings::default().with_query_value(
        NonEmptyString::new("labels").unwrap(),
        QueryValue::Array(vec![
            QueryValue::String("User".to_owned()),
            QueryValue::String("Account".to_owned()),
        ]),
    );

    let parameterized = executable_traversal(
        g().n_where(Predicate::is_in_param("$label", "labels")),
        parameterized_ctx,
    );
    let literal = executable_traversal(
        g().n_where(Predicate::is_in(
            "$label",
            PropertyValue::StringArray(vec!["User".to_owned(), "Account".to_owned()]),
        )),
        PlannerContext {
            stats,
            ..PlannerContext::default()
        },
    );

    assert_eq!(parameterized.steps(), literal.steps());
    assert_eq!(
        access_steps_matching(&parameterized, |access| matches!(
            access,
            ExecAccessPlan::Node(ExecNodeAccessPlan::LabelScan { .. })
        )),
        2
    );
    assert_no_exec_op_family(&parameterized, ExecOpFamily::Filter);
}

#[test]
fn membership_parameter_cardinality_matches_literal_normalization() {
    let indexes = builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("Person", "$label").unwrap())
        .with_node_eq(ScopedPropertyKey::try_new("Person", "orbit_id").unwrap());
    let cases = [
        Vec::new(),
        vec!["orbit-1"],
        vec!["orbit-1", "orbit-1"],
        vec!["orbit-1", "orbit-2"],
    ];

    for values in cases {
        let query_values = values
            .iter()
            .map(|value| QueryValue::String((*value).to_owned()))
            .collect::<Vec<_>>();
        let literal_values = values
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>();
        let mut planner_ctx = ctx(indexes.clone());
        planner_ctx.params = ParamBindings::default().with_query_value(
            NonEmptyString::new("orbit_ids").unwrap(),
            QueryValue::Array(query_values),
        );

        let parameterized = executable_traversal(
            g().n_with_label_where("Person", Predicate::is_in_param("orbit_id", "orbit_ids")),
            planner_ctx,
        );
        let literal = executable_traversal(
            g().n_with_label_where(
                "Person",
                Predicate::is_in("orbit_id", PropertyValue::StringArray(literal_values)),
            ),
            ctx(indexes.clone()),
        );

        assert_eq!(
            unwrapped_first_exec_access(&parameterized),
            unwrapped_first_exec_access(&literal),
            "parameterized and literal membership diverged for {values:?}"
        );
    }
}

#[test]
fn optimizer_proven_empty_membership_preserves_collection_return_shape() {
    let indexes = builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("Person", "$label").unwrap())
        .with_node_eq(ScopedPropertyKey::try_new("Person", "orbit_id").unwrap());
    let mut planner_ctx = ctx(indexes);
    planner_ctx.params = ParamBindings::default().with_query_value(
        NonEmptyString::new("orbit_ids").unwrap(),
        QueryValue::Array(Vec::new()),
    );

    let plan = executable_traversal(
        g().n_with_label("Person")
            .where_(Predicate::is_in_param("orbit_id", "orbit_ids")),
        planner_ctx,
    );

    assert!(matches!(
        unwrapped_first_exec_access(&plan),
        ExecAccessPlan::Node(ExecNodeAccessPlan::Empty)
    ));
    assert_eq!(
        plan.steps().last().unwrap().delivered.cardinality.upper(),
        Some(0)
    );
    assert_eq!(
        plan.steps().last().unwrap().semantic_return_shape,
        Some(ReturnShape::List)
    );
    let ExecutableReturns::Variables(returns) = plan.executable_returns() else {
        panic!("named result should resolve an executable return");
    };
    assert_eq!(returns.as_ref()[0].shape(), ReturnShape::List);

    let serialized = serde_json::to_vec(&plan).unwrap();
    let restored: ExecutablePlan = serde_json::from_slice(&serialized).unwrap();
    assert_eq!(
        restored.steps().last().unwrap().semantic_return_shape,
        Some(ReturnShape::List)
    );
    let ExecutableReturns::Variables(returns) = restored.executable_returns() else {
        panic!("deserialized named result should resolve an executable return");
    };
    assert_eq!(returns.as_ref()[0].shape(), ReturnShape::List);
}

#[test]
fn ordinary_request_range_parameters_enable_literal_constraint_reduction() {
    let indexes = builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("Person", "$label").unwrap())
        .with_node_range(
            ScopedPropertyDirectionKey::try_new("Person", "age", RangeIndexDirection::Asc).unwrap(),
        );
    let mut planner_ctx = ctx(indexes.clone());
    planner_ctx.params = ParamBindings::default().with_query_value(
        NonEmptyString::new("minimum_age").unwrap(),
        QueryValue::I64(20),
    );
    let parameterized_predicate = Predicate::and(vec![
        Predicate::gte_param("age", "minimum_age"),
        Predicate::lt("age", 10),
    ]);
    let literal_predicate =
        Predicate::and(vec![Predicate::gte("age", 20), Predicate::lt("age", 10)]);

    let parameterized = executable_traversal(
        g().n_with_label_where("Person", parameterized_predicate),
        planner_ctx,
    );
    let literal = executable_traversal(
        g().n_with_label_where("Person", literal_predicate),
        ctx(indexes),
    );

    assert_eq!(
        unwrapped_first_exec_access(&parameterized),
        unwrapped_first_exec_access(&literal)
    );
    assert!(matches!(
        unwrapped_first_exec_access(&parameterized),
        ExecAccessPlan::Node(ExecNodeAccessPlan::Empty)
    ));
}

#[test]
fn ordinary_request_parameters_specialize_non_indexable_predicates_recursively() {
    let indexes = builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("Person", "$label").unwrap());
    let mut planner_ctx = ctx(indexes.clone());
    planner_ctx.params = ParamBindings::default()
        .with_query_value(
            NonEmptyString::new("needle").unwrap(),
            QueryValue::String("engineer".to_owned()),
        )
        .with_query_value(
            NonEmptyString::new("excluded_age").unwrap(),
            QueryValue::I64(17),
        );
    let parameterized_predicate = Predicate::and(vec![
        Predicate::contains_param("bio", "needle"),
        Predicate::not(Predicate::eq_param("age", "excluded_age")),
    ]);
    let literal_predicate = Predicate::and(vec![
        Predicate::contains("bio", "engineer"),
        Predicate::not(Predicate::eq("age", 17)),
    ]);

    let parameterized = executable_traversal(
        g().n_with_label("Person").where_(parameterized_predicate),
        planner_ctx,
    );
    let literal = executable_traversal(
        g().n_with_label("Person").where_(literal_predicate),
        ctx(indexes),
    );

    assert_eq!(parameterized.steps(), literal.steps());
}
