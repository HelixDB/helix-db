use crate::planning::tests::support::*;

#[test]
fn empty_sources_skip_row_reducing_wrappers() {
    let node_filter = executable_traversal(
        g().n(NodeRef::ids(Vec::<u64>::new())).has("active", true),
        PlannerContext::default(),
    );
    let edge_filter = executable_traversal(
        g().e(EdgeRef::ids(Vec::<u64>::new()))
            .edge_has("active", true),
        PlannerContext::default(),
    );
    let node_distinct = executable_traversal(
        g().n(NodeRef::ids(Vec::<u64>::new())).dedup(),
        PlannerContext::default(),
    );
    let edge_distinct = executable_traversal(
        g().e(EdgeRef::ids(Vec::<u64>::new())).dedup(),
        PlannerContext::default(),
    );
    let node_order = executable_traversal(
        g().n(NodeRef::ids(Vec::<u64>::new()))
            .order_by("age", Order::Asc),
        PlannerContext::default(),
    );
    let edge_order = executable_traversal(
        g().e(EdgeRef::ids(Vec::<u64>::new()))
            .order_by("since", Order::Desc),
        PlannerContext::default(),
    );
    let node_multi_order = executable_traversal(
        g().n(NodeRef::ids(Vec::<u64>::new()))
            .order_by_multiple(vec![("age", Order::Asc), ("name", Order::Desc)]),
        PlannerContext::default(),
    );
    let node_within = executable_traversal(
        g().n(NodeRef::ids(Vec::<u64>::new()))
            .within("allowed_nodes"),
        PlannerContext::default(),
    );
    let node_without = executable_traversal(
        g().n(NodeRef::ids(Vec::<u64>::new()))
            .without("blocked_nodes"),
        PlannerContext::default(),
    );

    assert!(matches!(
        first_exec_access(&node_filter),
        ExecAccessPlan::Node(ExecNodeAccessPlan::Empty)
    ));
    assert_no_exec_op_family(&node_filter, ExecOpFamily::Filter);
    assert!(matches!(
        first_exec_access(&edge_filter),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::Empty)
    ));
    assert_no_exec_op_family(&edge_filter, ExecOpFamily::Filter);
    assert!(matches!(
        first_exec_access(&node_distinct),
        ExecAccessPlan::Node(ExecNodeAccessPlan::Empty)
    ));
    assert_no_exec_op_family(&node_distinct, ExecOpFamily::Distinct);
    assert!(matches!(
        first_exec_access(&edge_distinct),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::Empty)
    ));
    assert_no_exec_op_family(&edge_distinct, ExecOpFamily::Distinct);
    assert!(matches!(
        first_exec_access(&node_order),
        ExecAccessPlan::Node(ExecNodeAccessPlan::Empty)
    ));
    assert!(matches!(
        first_exec_access(&edge_order),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::Empty)
    ));
    assert!(matches!(
        first_exec_access(&node_multi_order),
        ExecAccessPlan::Node(ExecNodeAccessPlan::Empty)
    ));
    assert!(matches!(
        first_exec_access(&node_within),
        ExecAccessPlan::Node(ExecNodeAccessPlan::Empty)
    ));
    assert_no_exec_op_family(&node_within, ExecOpFamily::Variable);
    assert!(matches!(
        first_exec_access(&node_without),
        ExecAccessPlan::Node(ExecNodeAccessPlan::Empty)
    ));
    assert_no_exec_op_family(&node_without, ExecOpFamily::Variable);
}

#[test]
fn empty_expansions_plan_empty_access_for_the_output_kind() {
    let node_to_edges = executable_traversal(
        g().n(NodeRef::ids(Vec::<u64>::new())).out_e(None::<&str>),
        PlannerContext::default(),
    );
    let edge_to_nodes = executable_traversal(
        g().e(EdgeRef::ids(Vec::<u64>::new())).out_n(),
        PlannerContext::default(),
    );

    assert!(matches!(
        first_exec_access(&node_to_edges),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::Empty)
    ));
    assert_no_exec_op_family(&node_to_edges, ExecOpFamily::Expand);
    assert!(matches!(
        first_exec_access(&edge_to_nodes),
        ExecAccessPlan::Node(ExecNodeAccessPlan::Empty)
    ));
    assert_no_exec_op_family(&edge_to_nodes, ExecOpFamily::Expand);
}

#[test]
fn empty_node_repeat_plans_empty_node_access() {
    let repeated = executable_traversal(
        g().n(NodeRef::ids(Vec::<u64>::new()))
            .repeat(RepeatConfig::new(sub().out(Some("FOLLOWS")))),
        PlannerContext::default(),
    );

    assert!(matches!(
        first_exec_access(&repeated),
        ExecAccessPlan::Node(ExecNodeAccessPlan::Empty)
    ));
    assert_no_exec_op_family(&repeated, ExecOpFamily::Repeat);
}

#[test]
fn empty_branch_inputs_plan_empty_access_after_validating_branches() {
    let union = executable_traversal(
        g().n(NodeRef::ids(Vec::<u64>::new())).union(vec![
            sub().out(Some("FOLLOWS")),
            sub().in_(Some("MENTIONS")),
        ]),
        PlannerContext::default(),
    );
    let choose = executable_traversal(
        g().n(NodeRef::ids(Vec::<u64>::new())).choose(
            Predicate::eq("active", true),
            sub().out(Some("FOLLOWS")),
            Some(sub().in_(Some("MENTIONS"))),
        ),
        PlannerContext::default(),
    );
    let choose_without_else = executable_traversal(
        g().n(NodeRef::ids(Vec::<u64>::new())).choose(
            Predicate::eq("verified", true),
            sub().out(Some("FOLLOWS")),
            None,
        ),
        PlannerContext::default(),
    );
    let coalesce = executable_traversal(
        g().n(NodeRef::ids(Vec::<u64>::new())).coalesce(vec![
            sub().out(Some("FOLLOWS")),
            sub().in_(Some("MENTIONS")),
        ]),
        PlannerContext::default(),
    );
    let optional = executable_traversal(
        g().n(NodeRef::ids(Vec::<u64>::new()))
            .optional(sub().both(Some("RELATED"))),
        PlannerContext::default(),
    );
    let edge_union = executable_ast(
        AstNode::Union {
            input: Box::new(AstNode::Edges {
                reference: EdgeRef::ids(Vec::<u64>::new()),
            }),
            traversals: vec![
                sub().edge_has("active", true),
                sub().edge_has_label("FOLLOWS"),
            ],
        },
        PlannerContext::default(),
    );

    for plan in [&union, &choose, &choose_without_else, &coalesce, &optional] {
        assert!(matches!(
            first_exec_access(plan),
            ExecAccessPlan::Node(ExecNodeAccessPlan::Empty)
        ));
        assert_no_exec_op_family(plan, ExecOpFamily::Branch);
    }
    assert!(matches!(
        first_exec_access(&edge_union),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::Empty)
    ));
    assert_no_exec_op_family(&edge_union, ExecOpFamily::Branch);
}
