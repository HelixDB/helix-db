use super::*;

#[test]
fn order_repeat_and_branch_operations_preserve_nested_plans() {
    assert!(matches!(
        first_exec_op(
            &executable_ast(
                AstNode::OrderByMultiple {
                    input: boxed(nodes_root()),
                    orderings: vec![
                        ("age".to_string(), Order::Desc),
                        ("name".to_string(), Order::Asc),
                    ],
                },
                PlannerContext::default()
            ),
            |op| matches!(op, ExecOp::Order { .. })
        ),
        ExecOp::Order { plan: OrderPlan::ExplicitSort(keys) }
            if keys.as_ref().len() == 2
                && keys.as_ref()[0].property.as_ref() == "age"
                && keys.as_ref()[0].order == Order::Desc
                && keys.as_ref()[1].property.as_ref() == "name"
                && keys.as_ref()[1].order == Order::Asc
    ));

    let repeat = executable_ast(
        AstNode::Repeat {
            input: boxed(nodes_root()),
            config: RepeatConfig::new(sub().out(Some("FOLLOWS")))
                .times(3)
                .until(Predicate::eq("inactive", true))
                .emit_if(Predicate::eq("active", true))
                .max_depth(12),
        },
        PlannerContext::default(),
    );
    let ExecOp::Repeat { plan } = first_exec_op(&repeat, |op| matches!(op, ExecOp::Repeat { .. }))
    else {
        panic!("expected repeat");
    };
    assert_eq!(
        plan.stop,
        RepeatStopPlan::TimesOrUntil {
            count: NonZeroUsize::new(3).unwrap(),
            predicate: PredicatePlan::new(Predicate::eq("inactive", true)).unwrap(),
        }
    );
    assert_eq!(
        plan.emit,
        RepeatEmitPlan::AfterIf {
            predicate: PredicatePlan::new(Predicate::eq("active", true)).unwrap(),
        }
    );
    assert_eq!(plan.max_depth, NonZeroUsize::new(12).unwrap());
    assert!(plan
        .body
        .steps()
        .iter()
        .any(|step| matches!(&step.op, ExecOp::Expand { .. })));

    let choose = executable_ast(
        AstNode::Choose {
            input: boxed(nodes_root()),
            condition: Predicate::eq("active", true),
            then_traversal: sub().out(Some("FOLLOWS")),
            else_traversal: Some(sub().in_(Some("MENTIONS"))),
        },
        PlannerContext::default(),
    );
    assert!(matches!(
        first_exec_op(&choose, |op| matches!(op, ExecOp::Branch { .. })),
        ExecOp::Branch {
            plan: ExecBranchPlan::ChooseElse {
                condition,
                then_plan,
                else_plan,
            }
        } if *condition == Predicate::eq("active", true)
            && then_plan.steps().iter().any(|step| matches!(&step.op, ExecOp::Expand { .. }))
            && else_plan.steps().iter().any(|step| matches!(&step.op, ExecOp::Expand { .. }))
    ));

    let choose_without_else = executable_ast(
        AstNode::Choose {
            input: boxed(nodes_root()),
            condition: Predicate::eq("verified", true),
            then_traversal: sub().out(Some("FOLLOWS")),
            else_traversal: None,
        },
        PlannerContext::default(),
    );
    assert!(matches!(
        first_exec_op(&choose_without_else, |op| matches!(op, ExecOp::Branch { .. })),
        ExecOp::Branch {
            plan: ExecBranchPlan::Choose {
                condition,
                then_plan,
            }
        } if *condition == Predicate::eq("verified", true)
            && then_plan.steps().iter().any(|step| matches!(&step.op, ExecOp::Expand { .. }))
    ));

    let coalesce = executable_ast(
        AstNode::Coalesce {
            input: boxed(nodes_root()),
            traversals: vec![sub().out(Some("FOLLOWS")), sub().in_(Some("MENTIONS"))],
        },
        PlannerContext::default(),
    );
    assert!(matches!(
        first_exec_op(&coalesce, |op| matches!(op, ExecOp::Branch { .. })),
        ExecOp::Branch {
            plan: ExecBranchPlan::Coalesce(plans),
        } if plans.len() == 2
    ));

    let optional = executable_ast(
        AstNode::Optional {
            input: boxed(nodes_root()),
            traversal: sub().both(Some("RELATED")),
        },
        PlannerContext::default(),
    );
    assert!(matches!(
        first_exec_op(&optional, |op| matches!(op, ExecOp::Branch { .. })),
        ExecOp::Branch {
            plan: ExecBranchPlan::Optional(plan),
        } if plan.steps().iter().any(|step| matches!(&step.op, ExecOp::Expand { .. }))
    ));
}

#[test]
fn repeat_stop_modes_are_encoded_as_disjoint_variants() {
    let cases = [
        (
            RepeatConfig::new(sub().out(Some("FOLLOWS"))),
            RepeatStopPlan::MaxDepthOnly,
        ),
        (
            RepeatConfig::new(sub().out(Some("FOLLOWS"))).times(3),
            RepeatStopPlan::Times {
                count: NonZeroUsize::new(3).unwrap(),
            },
        ),
        (
            RepeatConfig::new(sub().out(Some("FOLLOWS"))).until(Predicate::eq("done", true)),
            RepeatStopPlan::Until {
                predicate: PredicatePlan::new(Predicate::eq("done", true)).unwrap(),
            },
        ),
    ];

    for (config, expected_stop) in cases {
        let repeat = executable_ast(
            AstNode::Repeat {
                input: boxed(nodes_root()),
                config,
            },
            PlannerContext::default(),
        );
        let ExecOp::Repeat { plan } =
            first_exec_op(&repeat, |op| matches!(op, ExecOp::Repeat { .. }))
        else {
            panic!("expected repeat");
        };
        assert_eq!(plan.stop, expected_stop);
    }
}

#[test]
fn repeat_emit_modes_without_predicates_are_preserved() {
    let cases = [
        (
            RepeatConfig::new(sub().out(Some("FOLLOWS"))),
            RepeatEmitPlan::None,
        ),
        (
            RepeatConfig::new(sub().out(Some("FOLLOWS"))).emit_before(),
            RepeatEmitPlan::Before,
        ),
        (
            RepeatConfig::new(sub().out(Some("FOLLOWS"))).emit_after(),
            RepeatEmitPlan::After,
        ),
        (
            RepeatConfig::new(sub().out(Some("FOLLOWS"))).emit_all(),
            RepeatEmitPlan::All,
        ),
    ];

    for (config, expected_emit) in cases {
        let repeat = executable_ast(
            AstNode::Repeat {
                input: boxed(nodes_root()),
                config,
            },
            PlannerContext::default(),
        );
        let ExecOp::Repeat { plan } =
            first_exec_op(&repeat, |op| matches!(op, ExecOp::Repeat { .. }))
        else {
            panic!("expected repeat");
        };
        assert_eq!(plan.emit, expected_emit);
    }
}
