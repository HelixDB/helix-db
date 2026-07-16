use crate::planning::tests::support::*;

#[test]
fn cascades_tightens_zero_based_literal_search_windows_in_executable_plans() {
    let indexes = search_indexes();

    let node_vector = executable_traversal(
        g().vector_search_nodes("Doc", "embedding", vec![0.1f32, 0.2], 10, None)
            .limit(3usize),
        ctx(indexes.clone()),
    );
    let node_text = executable_traversal(
        g().text_search_nodes("Doc", "body", "planner", 8, None)
            .range(0usize, 3usize),
        ctx(indexes.clone()),
    );
    let edge_vector = executable_traversal(
        g().vector_search_edges("MENTIONS", "embedding", vec![0.3f32, 0.4], 7, None)
            .limit(3usize),
        ctx(indexes.clone()),
    );
    let edge_text = executable_traversal(
        g().text_search_edges("MENTIONS", "body", "planner", 6, None)
            .range(0usize, 3usize),
        ctx(indexes),
    );

    for plan in [&node_vector, &node_text, &edge_vector, &edge_text] {
        assert_eq!(literal_exec_search_k(plan), 3);
        assert_no_exec_window(plan);
    }
}

#[test]
fn cascades_tightens_required_search_prefix_and_keeps_remaining_window() {
    let indexes = search_indexes();

    let node_vector = executable_traversal(
        g().vector_search_nodes("Doc", "embedding", vec![0.1f32, 0.2], 10, None)
            .skip(2usize)
            .limit(3usize),
        ctx(indexes.clone()),
    );
    let node_text = executable_traversal(
        g().text_search_nodes("Doc", "body", "planner", 9, None)
            .range(2usize, 5usize),
        ctx(indexes.clone()),
    );
    let edge_vector = executable_traversal(
        g().vector_search_edges("MENTIONS", "embedding", vec![0.3f32, 0.4], 8, None)
            .skip(2usize)
            .range(1usize, 4usize),
        ctx(indexes.clone()),
    );
    let edge_text = executable_traversal(
        g().text_search_edges("MENTIONS", "body", "planner", 7, None)
            .range(2usize, 8usize)
            .skip(3usize),
        ctx(indexes),
    );

    for (plan, k, start, end) in [
        (&node_vector, 5, 2, 5),
        (&node_text, 5, 2, 5),
        (&edge_vector, 6, 3, 6),
        (&edge_text, 7, 5, 8),
    ] {
        assert_eq!(literal_exec_search_k(plan), k);
        assert_exec_range(plan, start, end);
        // The remaining suffix is a single range; separate limit/skip ops would
        // mean the optimizer failed to compose the required read prefix.
        assert_no_exec_op_family(plan, ExecOpFamily::Limit);
        assert_no_exec_op_family(plan, ExecOpFamily::Skip);
    }
}

#[test]
fn cascades_keeps_search_limits_after_distinct_until_duplicate_free_proven() {
    let indexes = search_indexes();

    let node_vector = executable_traversal(
        g().vector_search_nodes("Doc", "embedding", vec![0.1f32, 0.2], 10, None)
            .dedup()
            .limit(3usize),
        ctx(indexes.clone()),
    );
    let edge_text = executable_traversal(
        g().text_search_edges("MENTIONS", "body", "planner", 9, None)
            .dedup()
            .limit(3usize),
        ctx(indexes.clone()),
    );

    for (plan, original_k) in [(&node_vector, 10), (&edge_text, 9)] {
        assert_eq!(literal_exec_search_k(plan), original_k);
        assert!(has_exec_op_family(plan, ExecOpFamily::Distinct));
        if has_exec_op_family(plan, ExecOpFamily::Range) {
            assert_exec_range(plan, 0, 3);
        } else {
            assert!(matches!(
                first_exec_op(plan, |op| matches!(op, ExecOp::Limit { .. })),
                ExecOp::Limit {
                    count: StreamBoundPlan::Literal(3)
                }
            ));
        }
        assert_no_exec_op_family(plan, ExecOpFamily::Skip);
    }

    let singleton_node_text = executable_traversal(
        g().text_search_nodes("Doc", "body", "planner", 1, None)
            .dedup()
            .limit(3usize),
        ctx(indexes.clone()),
    );
    let singleton_edge_vector = executable_traversal(
        g().vector_search_edges("MENTIONS", "embedding", vec![0.3f32, 0.4], 1, None)
            .dedup()
            .limit(3usize),
        ctx(indexes),
    );

    for plan in [&singleton_node_text, &singleton_edge_vector] {
        assert_eq!(literal_exec_search_k(plan), 1);
        assert_no_exec_op_family(plan, ExecOpFamily::Distinct);
        assert_no_exec_window(plan);
    }
}

#[test]
fn cascades_does_not_loosen_or_tighten_runtime_search_limits() {
    let indexes = search_indexes();

    let larger_window = executable_traversal(
        g().text_search_nodes("Doc", "body", "planner", 2, None)
            .limit(9usize),
        ctx(indexes.clone()),
    );
    let dynamic_k = executable_traversal(
        g().vector_search_edges_with(
            "MENTIONS",
            "embedding",
            vec![0.3f32, 0.4],
            StreamBound::expr(Expr::param("k")),
            None,
        )
        .skip(2usize)
        .limit(3usize),
        ctx(indexes.clone()),
    );
    let dynamic_stream_limit = executable_traversal(
        g().text_search_edges("MENTIONS", "body", "planner", 9, None)
            .limit(StreamBound::expr(Expr::param("limit"))),
        ctx(indexes),
    );

    assert_eq!(literal_exec_search_k(&larger_window), 2);
    assert!(matches!(
        exec_search_k(&dynamic_k),
        SearchLimitPlan::Expr(_)
    ));
    assert_exec_range(&dynamic_k, 2, 5);
    assert_eq!(literal_exec_search_k(&dynamic_stream_limit), 9);
    assert!(dynamic_stream_limit.steps().iter().any(|step| matches!(
        &step.op,
        ExecOp::Limit {
            count: StreamBoundPlan::Expr(_)
        }
    )));
}

#[test]
fn cascades_collapses_empty_literal_windows_to_empty_access() {
    let indexes = search_indexes();

    let zero_limit = executable_traversal(
        g().vector_search_nodes("Doc", "embedding", vec![0.1f32, 0.2], 10, None)
            .limit(0usize),
        ctx(indexes.clone()),
    );
    let zero_range = executable_traversal(
        g().text_search_edges("MENTIONS", "body", "planner", 6, None)
            .range(0usize, 0usize),
        ctx(indexes),
    );

    assert!(matches!(
        first_exec_access(&zero_limit),
        ExecAccessPlan::Node(ExecNodeAccessPlan::Empty)
    ));
    assert!(matches!(
        first_exec_access(&zero_range),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::Empty)
    ));
    assert_no_exec_window(&zero_limit);
    assert_no_exec_window(&zero_range);
}

fn search_indexes() -> IndexCatalogSnapshot {
    builtin_label_indexes()
        .with_vector(
            SearchIndexKey::try_new(ElementKind::Node, "Doc", "embedding").unwrap(),
            SearchIndexScope::Unscoped,
        )
        .with_text(
            SearchIndexKey::try_new(ElementKind::Node, "Doc", "body").unwrap(),
            SearchIndexScope::Unscoped,
        )
        .with_vector(
            SearchIndexKey::try_new(ElementKind::Edge, "MENTIONS", "embedding").unwrap(),
            SearchIndexScope::Unscoped,
        )
        .with_text(
            SearchIndexKey::try_new(ElementKind::Edge, "MENTIONS", "body").unwrap(),
            SearchIndexScope::Unscoped,
        )
}
