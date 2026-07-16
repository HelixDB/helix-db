use super::super::*;

#[test]
fn single_run_executable_entrypoint_lowers_reserved_operator_family_over_recursive_inputs() {
    let terminal_chain = executable_ast(
        AstNode::Unfold {
            input: boxed(AstNode::Fold {
                input: boxed(nodes_root()),
            }),
        },
        PlannerContext::default(),
    );
    assert_eq!(terminal_chain.steps().len(), 3);
    assert!(matches!(
        &terminal_chain.steps()[1].op,
        crate::exec::ExecOp::Reserved {
            op: ReservedOp::Fold
        }
    ));
    assert!(matches!(
        &terminal_chain.steps()[2].op,
        crate::exec::ExecOp::Reserved {
            op: ReservedOp::Unfold
        }
    ));
    assert_eq!(
        terminal_chain.steps()[2].dependencies,
        vec![terminal_chain.steps()[1].id]
    );

    let control_reserved = executable_ast(
        AstNode::SimplePath {
            input: boxed(AstNode::Optional {
                input: boxed(nodes_root()),
                traversal: sub().out(Some("FOLLOWS")),
            }),
        },
        PlannerContext::default(),
    );
    assert_eq!(control_reserved.steps().len(), 3);
    assert!(matches!(
        &control_reserved.steps()[1].op,
        crate::exec::ExecOp::Branch {
            plan: crate::exec::ExecBranchPlan::Optional(body)
        } if body.steps().len() == 2
    ));
    assert!(matches!(
        &control_reserved.steps()[2].op,
        crate::exec::ExecOp::Reserved {
            op: ReservedOp::SimplePath
        }
    ));
    assert_eq!(
        control_reserved.steps()[2].dependencies,
        vec![control_reserved.steps()[1].id]
    );

    let sack_ops = [
        (
            AstNode::WithSack {
                input: boxed(nodes_root()),
                initial: PropertyValue::from(1),
            },
            "with_sack",
        ),
        (
            AstNode::SackSet {
                input: boxed(nodes_root()),
                property: "score".to_owned(),
            },
            "sack_set",
        ),
        (
            AstNode::SackAdd {
                input: boxed(nodes_root()),
                property: "weight".to_owned(),
            },
            "sack_add",
        ),
        (
            AstNode::SackGet {
                input: boxed(nodes_root()),
            },
            "sack_get",
        ),
    ];

    for (root, expected) in sack_ops {
        let plan = executable_ast(root, PlannerContext::default());
        assert_eq!(plan.steps().len(), 2);
        assert!(matches!(
            &plan.steps()[0].op,
            crate::exec::ExecOp::KvRead(_)
        ));
        assert!(
            matches!(
                (&plan.steps()[1].op, expected),
                (
                    crate::exec::ExecOp::Reserved {
                        op: ReservedOp::WithSack(value)
                    },
                    "with_sack"
                ) if value == &PropertyValue::from(1)
            ) || matches!(
                (&plan.steps()[1].op, expected),
                (
                    crate::exec::ExecOp::Reserved {
                        op: ReservedOp::SackSet(property)
                    },
                    "sack_set"
                ) if property.as_ref() == "score"
            ) || matches!(
                (&plan.steps()[1].op, expected),
                (
                    crate::exec::ExecOp::Reserved {
                        op: ReservedOp::SackAdd(property)
                    },
                    "sack_add"
                ) if property.as_ref() == "weight"
            ) || matches!(
                (&plan.steps()[1].op, expected),
                (
                    crate::exec::ExecOp::Reserved {
                        op: ReservedOp::SackGet
                    },
                    "sack_get"
                )
            )
        );
        assert_eq!(plan.steps()[1].dependencies, vec![plan.steps()[0].id]);
    }
}
