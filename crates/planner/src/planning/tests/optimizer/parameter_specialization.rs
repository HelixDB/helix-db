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
