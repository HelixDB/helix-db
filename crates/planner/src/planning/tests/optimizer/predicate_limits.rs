use crate::{context, planning::tests::support::*};

#[test]
fn cascades_index_union_branch_limit_selects_union_at_limit() {
    let indexes = disjunction_indexes();

    let node_union = executable_traversal(
        g().n_with_label_where("User", literal_disjunction("username", &["alice", "bob"])),
        branch_limited_ctx(indexes.clone(), 2),
    );
    assert_selected_root_family(&node_union, "alternative");
    assert_selected_rule(&node_union, KnownRuleId::SeedAccessPath);
    assert_eq!(
        node_union
            .steps()
            .iter()
            .filter(
                |step| matches!(&step.op, ExecOp::Merge { mode } if *mode == ExecMergeMode::Union)
            )
            .count(),
        1,
        "expected one union merge in plan: {:?}",
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

    let edge_union = executable_traversal(
        g().e_with_label_where(
            "FOLLOWS",
            literal_disjunction("status", &["active", "paused"]),
        ),
        branch_limited_ctx(indexes, 2),
    );
    assert_selected_root_family(&edge_union, "alternative");
    assert_selected_rule(&edge_union, KnownRuleId::SeedAccessPath);
    assert_eq!(
        edge_union
            .steps()
            .iter()
            .filter(
                |step| matches!(&step.op, ExecOp::Merge { mode } if *mode == ExecMergeMode::Union)
            )
            .count(),
        1,
        "expected one union merge in plan: {:?}",
        edge_union.steps()
    );
    assert_eq!(
        access_steps_matching(&edge_union, |access| matches!(
            access,
            ExecAccessPlan::Edge(ExecEdgeAccessPlan::EqualityIndex { key, .. })
                if key.label == "FOLLOWS" && key.property == "status"
        )),
        2
    );
    assert_no_exec_op_family(&edge_union, ExecOpFamily::Filter);
    assert_no_exec_op_family(&edge_union, ExecOpFamily::Order);
    assert_no_exec_window(&edge_union);
}

#[test]
fn cascades_index_union_branch_limit_keeps_residual_filter_above_limit() {
    let predicate = literal_disjunction("username", &["alice", "bob", "carol"]);
    let plan = executable_traversal(
        g().n_with_label_where("User", predicate.clone()),
        branch_limited_ctx(disjunction_indexes(), 2),
    );

    assert_selected_root_family(&plan, "alternative");
    assert_selected_rule(&plan, KnownRuleId::SeedAccessFilter);
    assert_eq!(
        plan.steps()
            .iter()
            .filter(
                |step| matches!(&step.op, ExecOp::Merge { mode } if *mode == ExecMergeMode::Union)
            )
            .count(),
        0,
        "branch limit should reject index union: {:?}",
        plan.steps()
    );
    assert_eq!(
        access_steps_matching(&plan, |access| matches!(
            access,
            ExecAccessPlan::Node(ExecNodeAccessPlan::EqualityIndex { key, .. })
                if key.label == "User" && key.property == "username"
        )),
        0,
        "branch limit should avoid a partial username index plan: {:?}",
        plan.steps()
    );
    assert!(matches!(
        first_exec_access(&plan),
        ExecAccessPlan::Node(ExecNodeAccessPlan::LabelScan { label }) if label.as_ref() == "User"
    ));
    assert!(matches!(
        first_exec_op(&plan, |op| matches!(op, ExecOp::Filter { .. })),
        ExecOp::Filter { predicate: actual }
            if actual == &PredicatePlan::new(predicate).unwrap()
    ));
    assert_no_exec_op_family(&plan, ExecOpFamily::Order);
    assert_no_exec_window(&plan);
}

#[test]
fn cascades_index_union_branch_limit_disabled_keeps_residual_filter() {
    let predicate = literal_disjunction("status", &["active", "paused"]);
    let plan = executable_traversal(
        g().e_with_label_where("FOLLOWS", predicate.clone()),
        PlannerContext {
            indexes: disjunction_indexes(),
            limits: context::PlannerLimits {
                max_index_union_branches: IndexUnionBranchLimit::Disabled,
            },
            ..PlannerContext::default()
        },
    );

    assert_selected_root_family(&plan, "alternative");
    assert_selected_rule(&plan, KnownRuleId::SeedAccessFilter);
    assert_eq!(
        plan.steps()
            .iter()
            .filter(
                |step| matches!(&step.op, ExecOp::Merge { mode } if *mode == ExecMergeMode::Union)
            )
            .count(),
        0,
        "disabled branch limit should reject index union: {:?}",
        plan.steps()
    );
    assert_eq!(
        access_steps_matching(&plan, |access| matches!(
            access,
            ExecAccessPlan::Edge(ExecEdgeAccessPlan::EqualityIndex { key, .. })
                if key.label == "FOLLOWS" && key.property == "status"
        )),
        0,
        "disabled branch limit should avoid partial status index plans: {:?}",
        plan.steps()
    );
    assert!(matches!(
        first_exec_access(&plan),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::LabelScan { label }) if label.as_ref() == "FOLLOWS"
    ));
    assert!(matches!(
        first_exec_op(&plan, |op| matches!(op, ExecOp::Filter { .. })),
        ExecOp::Filter { predicate: actual }
            if actual == &PredicatePlan::new(predicate).unwrap()
    ));
    assert_no_exec_op_family(&plan, ExecOpFamily::Order);
    assert_no_exec_window(&plan);
}

fn disjunction_indexes() -> IndexCatalogSnapshot {
    IndexCatalogSnapshot::default()
        .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
}

fn branch_limited_ctx(indexes: IndexCatalogSnapshot, limit: usize) -> PlannerContext {
    PlannerContext {
        indexes,
        limits: context::PlannerLimits {
            max_index_union_branches: IndexUnionBranchLimit::limited(limit).unwrap(),
        },
        ..PlannerContext::default()
    }
}

fn literal_disjunction(property: &str, values: &[&str]) -> Predicate {
    Predicate::or(
        values
            .iter()
            .copied()
            .map(|value| Predicate::eq(property, value))
            .collect(),
    )
}
