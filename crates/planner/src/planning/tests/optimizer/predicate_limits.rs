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
    assert_batched_node_equality_set(&node_union, "User", "username", 2);
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
    assert_batched_edge_equality_set(&edge_union, "FOLLOWS", "status", 2);
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
            ExecAccessPlan::Node(ExecNodeAccessPlan::Bitmap { bitmap: crate::exec::ExecNodeBitmapExpr::PointRead { key, .. } })
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
            ExecAccessPlan::Edge(ExecEdgeAccessPlan::Bitmap { bitmap: crate::exec::ExecEdgeBitmapExpr::PointRead { key, .. } })
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

#[test]
fn finite_label_membership_uses_bounded_label_scan_unions() {
    let labels = PropertyValue::StringArray(vec!["User".to_owned(), "Account".to_owned()]);
    let node = executable_traversal(
        g().n_where(Predicate::is_in("$label", labels.clone())),
        label_union_ctx(2),
    );
    let edge = executable_traversal(
        g().e_where(Predicate::is_in("$label", labels)),
        label_union_ctx(2),
    );

    for (plan, node) in [(&node, true), (&edge, false)] {
        let labels = plan
            .steps()
            .iter()
            .filter_map(|step| match (&step.op, node) {
                (ExecOp::Access { plan }, true) => match plan.as_ref() {
                    ExecAccessPlan::Node(ExecNodeAccessPlan::LabelScan { label }) => {
                        Some(label.as_ref())
                    }
                    _ => None,
                },
                (ExecOp::Access { plan }, false) => match plan.as_ref() {
                    ExecAccessPlan::Edge(ExecEdgeAccessPlan::LabelScan { label }) => {
                        Some(label.as_ref())
                    }
                    _ => None,
                },
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(labels, vec!["User", "Account"], "plan: {:?}", plan.steps());
        assert_eq!(
            plan.steps()
                .iter()
                .filter(|step| matches!(
                    step.op,
                    ExecOp::Merge {
                        mode: ExecMergeMode::Union
                    }
                ))
                .count(),
            1
        );
        assert_no_exec_op_family(plan, ExecOpFamily::Filter);
    }
}

#[test]
fn label_membership_keeps_residuals_and_respects_the_union_limit() {
    let label_membership = Predicate::is_in(
        "$label",
        PropertyValue::StringArray(vec!["User".to_owned(), "Account".to_owned()]),
    );
    let residual = Predicate::contains("bio", "rust");
    let indexed = executable_traversal(
        g().n_where(Predicate::and(vec![
            label_membership.clone(),
            residual.clone(),
        ])),
        label_union_ctx(2),
    );
    assert_eq!(
        indexed
            .steps()
            .iter()
            .filter(|step| matches!(
                step.op,
                ExecOp::Merge {
                    mode: ExecMergeMode::Union
                }
            ))
            .count(),
        1,
        "plan: {:?}",
        indexed.steps()
    );
    assert!(matches!(
        first_exec_op(&indexed, |op| matches!(op, ExecOp::Filter { .. })),
        ExecOp::Filter { predicate } if predicate == &PredicatePlan::new(residual).unwrap()
    ));

    let over_limit =
        executable_traversal(g().n_where(label_membership.clone()), label_union_ctx(1));
    assert!(matches!(
        first_kv_read(&over_limit),
        KvReadPlan::RangeScan {
            keyspace: ElementKeyspace::NodeProperty,
            ..
        }
    ));
    assert!(matches!(
        first_exec_op(&over_limit, |op| matches!(op, ExecOp::Filter { .. })),
        ExecOp::Filter { predicate } if predicate == &PredicatePlan::new(label_membership).unwrap()
    ));
}

#[test]
fn impossible_label_domains_eliminate_residual_work() {
    let residual = Predicate::contains("bio", "rust");
    let cases = [
        g().n_with_label("User").where_(Predicate::and(vec![
            Predicate::is_in(
                "$label",
                PropertyValue::StringArray(vec!["Account".to_owned()]),
            ),
            residual.clone(),
        ])),
        g().n_where(Predicate::and(vec![
            Predicate::is_in("$label", PropertyValue::StringArray(Vec::new())),
            residual,
        ])),
    ];

    for traversal in cases {
        let plan = executable_traversal(traversal, label_union_ctx(2));
        assert!(matches!(
            unwrapped_first_exec_access(&plan),
            ExecAccessPlan::Node(ExecNodeAccessPlan::Empty)
        ));
        assert_no_exec_op_family(&plan, ExecOpFamily::Filter);
    }
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

fn label_union_ctx(limit: usize) -> PlannerContext {
    let user = NonEmptyString::new("User").unwrap();
    let account = NonEmptyString::new("Account").unwrap();
    PlannerContext {
        stats: context::StatsSnapshot::default()
            .with_node_label_cardinality(user.clone(), 1)
            .with_node_label_cardinality(account.clone(), 1)
            .with_edge_label_cardinality(user, 1)
            .with_edge_label_cardinality(account, 1),
        ..branch_limited_ctx(IndexCatalogSnapshot::default(), limit)
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
