use crate::planning::tests::support::*;

#[test]
fn cascades_control_roots_preserve_indexed_inputs_and_branch_payloads() {
    let indexes = control_indexes();
    let optional = executable_traversal(
        user_by_username().optional(sub().out(Some("FOLLOWS"))),
        ctx(indexes.clone()),
    );
    let union = executable_traversal(
        user_by_username().union(vec![
            sub().out(Some("FOLLOWS")),
            sub().in_(Some("MENTIONS")),
        ]),
        ctx(indexes.clone()),
    );
    let coalesce = executable_traversal(
        user_by_username().coalesce(vec![
            sub().out(Some("FOLLOWS")),
            sub().in_(Some("MENTIONS")),
        ]),
        ctx(indexes.clone()),
    );
    let choose = executable_traversal(
        user_by_username().choose(
            Predicate::eq("active", true),
            sub().out(Some("FOLLOWS")),
            None,
        ),
        ctx(indexes.clone()),
    );
    let choose_else = executable_traversal(
        user_by_username().choose(
            Predicate::eq("active", true),
            sub().out(Some("FOLLOWS")),
            Some(sub().in_(Some("MENTIONS"))),
        ),
        ctx(indexes),
    );

    for plan in [&optional, &union, &coalesce, &choose, &choose_else] {
        assert_indexed_user_access(plan);
        assert_selected_root_family(plan, "branch");
        assert_selected_rule(plan, KnownRuleId::SeedRootBranch);
        assert_selected_rule(plan, KnownRuleId::SeedAccessPath);
        assert_no_exec_op_family(plan, ExecOpFamily::Filter);
        assert_no_exec_op_family(plan, ExecOpFamily::Order);
        assert_no_exec_window(plan);
        assert_control_step_depends_on_indexed_input(plan);
    }

    assert!(matches!(
        first_exec_op(&optional, |op| matches!(op, ExecOp::Branch { .. })),
        ExecOp::Branch {
            plan: ExecBranchPlan::Optional(body),
        } if subplan_has_expand(body, ExpandDirection::Out, "FOLLOWS")
    ));
    assert!(matches!(
        first_exec_op(&union, |op| matches!(op, ExecOp::Branch { .. })),
        ExecOp::Branch {
            plan: ExecBranchPlan::Union(branches),
        } if branches.as_ref().len() == 2
            && subplan_has_expand(&branches.as_ref()[0], ExpandDirection::Out, "FOLLOWS")
            && subplan_has_expand(&branches.as_ref()[1], ExpandDirection::In, "MENTIONS")
    ));
    assert!(matches!(
        first_exec_op(&coalesce, |op| matches!(op, ExecOp::Branch { .. })),
        ExecOp::Branch {
            plan: ExecBranchPlan::Coalesce(branches),
        } if branches.as_ref().len() == 2
            && subplan_has_expand(&branches.as_ref()[0], ExpandDirection::Out, "FOLLOWS")
            && subplan_has_expand(&branches.as_ref()[1], ExpandDirection::In, "MENTIONS")
    ));
    assert!(matches!(
        first_exec_op(&choose, |op| matches!(op, ExecOp::Branch { .. })),
        ExecOp::Branch {
            plan: ExecBranchPlan::Choose {
                condition,
                then_plan,
            },
        } if condition == &PredicatePlan::new(Predicate::eq("active", true)).unwrap()
            && subplan_has_expand(then_plan, ExpandDirection::Out, "FOLLOWS")
    ));
    assert!(matches!(
        first_exec_op(&choose_else, |op| matches!(op, ExecOp::Branch { .. })),
        ExecOp::Branch {
            plan: ExecBranchPlan::ChooseElse {
                condition,
                then_plan,
                else_plan,
            },
        } if condition == &PredicatePlan::new(Predicate::eq("active", true)).unwrap()
            && subplan_has_expand(then_plan, ExpandDirection::Out, "FOLLOWS")
            && subplan_has_expand(else_plan, ExpandDirection::In, "MENTIONS")
    ));
}

#[test]
fn cascades_repeat_roots_preserve_indexed_inputs_and_repeat_contracts() {
    let repeat = executable_traversal(
        user_by_username().repeat(
            RepeatConfig::new(sub().out(Some("FOLLOWS")))
                .times(3)
                .until(Predicate::eq("inactive", true))
                .emit_if(Predicate::eq("active", true))
                .max_depth(12),
        ),
        ctx(control_indexes()),
    );

    assert_indexed_user_access(&repeat);
    assert_selected_root_family(&repeat, "repeat");
    assert_selected_rule(&repeat, KnownRuleId::SeedRootRepeat);
    assert_selected_rule(&repeat, KnownRuleId::SeedAccessPath);
    assert_no_exec_op_family(&repeat, ExecOpFamily::Filter);
    assert_no_exec_op_family(&repeat, ExecOpFamily::Order);
    assert_no_exec_window(&repeat);
    assert_control_step_depends_on_indexed_input(&repeat);

    assert!(matches!(
        first_exec_op(&repeat, |op| matches!(op, ExecOp::Repeat { .. })),
        ExecOp::Repeat { plan }
            if plan.stop == RepeatStopPlan::TimesOrUntil {
                count: NonZeroUsize::new(3).unwrap(),
                predicate: PredicatePlan::new(Predicate::eq("inactive", true)).unwrap(),
            }
                && plan.emit == RepeatEmitPlan::AfterIf {
                    predicate: PredicatePlan::new(Predicate::eq("active", true)).unwrap(),
                }
                && plan.max_depth == NonZeroUsize::new(12).unwrap()
                && subplan_has_expand(&plan.body, ExpandDirection::Out, "FOLLOWS")
    ));
}

fn user_by_username() -> Traversal<helix_ast::traversal::OnNodes, ReadOnly> {
    g().n_with_label_where("User", Predicate::eq("username", "alice"))
}

fn control_indexes() -> IndexCatalogSnapshot {
    builtin_label_indexes().with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
}

fn assert_indexed_user_access(plan: &ExecutablePlan) {
    assert!(matches!(
        unwrapped_first_exec_access(plan),
        ExecAccessPlan::Node(ExecNodeAccessPlan::Bitmap { bitmap: crate::exec::ExecNodeBitmapExpr::PointRead { key, .. } })
            if key.label == "User" && key.property == "username"
    ));
}

fn assert_control_step_depends_on_indexed_input(plan: &ExecutablePlan) {
    let access_step = plan
        .steps()
        .iter()
        .find(|step| matches!(&step.op, ExecOp::Access { .. }))
        .unwrap_or_else(|| panic!("expected indexed access step: {:?}", plan.steps()));
    let control_step = plan
        .steps()
        .iter()
        .find(|step| matches!(&step.op, ExecOp::Branch { .. } | ExecOp::Repeat { .. }))
        .unwrap_or_else(|| panic!("expected control step: {:?}", plan.steps()));
    assert_eq!(control_step.dependencies, vec![access_step.id]);
}

fn subplan_has_expand(
    plan: &crate::exec::ExecutableSubplan,
    direction: ExpandDirection,
    label: &str,
) -> bool {
    plan.steps().iter().any(|step| {
        matches!(
            &step.op,
            ExecOp::Expand {
                plan: ExpandPlan {
                    direction: actual_direction,
                    label: ExpandLabelPlan::Label(actual_label),
                    ..
                },
            } if *actual_direction == direction && actual_label.as_ref() == label
        )
    })
}
