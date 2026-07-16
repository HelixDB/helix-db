use super::*;

#[test]
fn source_predicate_non_literal_index_expressions_remain_residual_filters() {
    let context = ctx(builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
        .with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        ));
    let non_literal_cases = [
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::compare(
                Expr::prop("username"),
                CompareOp::Eq,
                Expr::val("alice").add_expr(Expr::val("example")),
            ),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::compare(
                Expr::val("alice").add_expr(Expr::val("example")),
                CompareOp::Eq,
                Expr::prop("username"),
            ),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::compare(
                Expr::prop("age"),
                CompareOp::Gte,
                Expr::val(20).add_expr(Expr::val(1)),
            ),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::compare(
                Expr::val(20).add_expr(Expr::val(1)),
                CompareOp::Lte,
                Expr::prop("age"),
            ),
        ]),
    ];

    for predicate in non_literal_cases {
        let batch = read_batch().var_as("users", g().n_where(predicate));
        let plan = plan_read_checked(&batch, &context).unwrap();

        assert!(plan
            .steps()
            .iter()
            .any(|step| matches!(&step.op, ExecOp::Filter { .. })));
    }

    let empty_param_cases = [
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::between("age", PropertyInput::param(""), 64),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::between("age", 18, PropertyInput::param("")),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::compare(Expr::param(""), CompareOp::Lte, Expr::prop("age")),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::compare(Expr::param(""), CompareOp::Eq, Expr::prop("username")),
        ]),
    ];

    for predicate in empty_param_cases {
        let batch = read_batch().var_as("users", g().n_where(predicate));
        assert_eq!(
            plan_read_checked(&batch, &context).unwrap_err(),
            PlannerError::InvalidEmptyName {
                field: NameField::Param
            }
        );
    }

    let edge_context = ctx(builtin_label_indexes()
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
        .with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
        ));
    for predicate in [
        Predicate::and(vec![
            Predicate::eq("$label", "FOLLOWS"),
            Predicate::compare(Expr::param(""), CompareOp::Eq, Expr::prop("status")),
        ]),
        Predicate::and(vec![
            Predicate::eq("$label", "FOLLOWS"),
            Predicate::compare(Expr::param(""), CompareOp::Lte, Expr::prop("weight")),
        ]),
    ] {
        let batch = read_batch().var_as("edges", g().e_where(predicate));
        assert_eq!(
            plan_read_checked(&batch, &edge_context).unwrap_err(),
            PlannerError::InvalidEmptyName {
                field: NameField::Param
            }
        );
    }
}

#[test]
fn index_candidate_errors_propagate_through_boolean_planning() {
    let node_context = ctx(builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap()));
    let edge_context = ctx(builtin_label_indexes()
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap()));
    let node_invalid = Predicate::compare(Expr::param(""), CompareOp::Eq, Expr::prop("username"));
    let edge_invalid = Predicate::compare(Expr::param(""), CompareOp::Eq, Expr::prop("status"));

    let cases = [
        (
            raw_read(AstNode::NodesWhere {
                predicate: Predicate::and(vec![
                    Predicate::eq("$label", "User"),
                    Predicate::and(vec![node_invalid.clone()]),
                ]),
            }),
            &node_context,
        ),
        (
            raw_read(AstNode::NodesWhere {
                predicate: Predicate::or(vec![
                    Predicate::and(vec![
                        Predicate::eq("$label", "User"),
                        Predicate::eq("username", "alice"),
                    ]),
                    Predicate::and(vec![Predicate::eq("$label", "User"), node_invalid]),
                ]),
            }),
            &node_context,
        ),
        (
            raw_read(AstNode::EdgesWhere {
                predicate: Predicate::and(vec![
                    Predicate::eq("$label", "FOLLOWS"),
                    Predicate::and(vec![edge_invalid.clone()]),
                ]),
            }),
            &edge_context,
        ),
        (
            raw_read(AstNode::EdgesWhere {
                predicate: Predicate::or(vec![
                    Predicate::and(vec![
                        Predicate::eq("$label", "FOLLOWS"),
                        Predicate::eq("status", "active"),
                    ]),
                    Predicate::and(vec![Predicate::eq("$label", "FOLLOWS"), edge_invalid]),
                ]),
            }),
            &edge_context,
        ),
    ];

    for (batch, context) in cases {
        assert_eq!(
            plan_read_checked(&batch, context).unwrap_err(),
            PlannerError::InvalidEmptyName {
                field: NameField::Param
            }
        );
    }
}

#[test]
fn source_predicate_non_literal_index_expressions_stay_residual_through_boolean_planning() {
    let node_context = ctx(builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
        .with_node_range(
            ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        ));
    let edge_context = ctx(builtin_label_indexes()
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
        .with_edge_range(
            ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
                .unwrap(),
        ));
    let node_invalid_eq = || {
        Predicate::compare(
            Expr::prop("username"),
            CompareOp::Eq,
            Expr::val("alice").add_expr(Expr::val("example")),
        )
    };
    let edge_invalid_eq = || {
        Predicate::compare(
            Expr::prop("status"),
            CompareOp::Eq,
            Expr::val("active").add_expr(Expr::val("suffix")),
        )
    };
    let edge_invalid_range = || {
        Predicate::compare(
            Expr::prop("weight"),
            CompareOp::Gte,
            Expr::val(20).add_expr(Expr::val(1)),
        )
    };

    let cases = [
        (
            raw_read(AstNode::NodesWhere {
                predicate: Predicate::or(vec![
                    Predicate::and(vec![
                        Predicate::eq("$label", "User"),
                        Predicate::eq("username", "alice"),
                    ]),
                    Predicate::and(vec![Predicate::eq("$label", "User"), node_invalid_eq()]),
                ]),
            }),
            &node_context,
        ),
        (
            raw_read(AstNode::NodesWhere {
                predicate: Predicate::and(vec![
                    Predicate::eq("$label", "User"),
                    Predicate::and(vec![node_invalid_eq()]),
                ]),
            }),
            &node_context,
        ),
        (
            raw_read(AstNode::NodesWhere {
                predicate: Predicate::and(vec![
                    Predicate::eq("$label", "User"),
                    Predicate::or(vec![Predicate::eq("username", "alice"), node_invalid_eq()]),
                ]),
            }),
            &node_context,
        ),
        (
            raw_read(AstNode::Where {
                input: Box::new(AstNode::NodesWhere {
                    predicate: Predicate::eq("$label", "User"),
                }),
                predicate: node_invalid_eq(),
            }),
            &node_context,
        ),
        (
            raw_read(AstNode::EdgesWhere {
                predicate: Predicate::and(vec![
                    Predicate::eq("$label", "FOLLOWS"),
                    edge_invalid_eq(),
                ]),
            }),
            &edge_context,
        ),
        (
            raw_read(AstNode::EdgesWhere {
                predicate: Predicate::or(vec![
                    Predicate::and(vec![
                        Predicate::eq("$label", "FOLLOWS"),
                        Predicate::eq("status", "active"),
                    ]),
                    Predicate::and(vec![Predicate::eq("$label", "FOLLOWS"), edge_invalid_eq()]),
                ]),
            }),
            &edge_context,
        ),
        (
            raw_read(AstNode::EdgesWhere {
                predicate: Predicate::and(vec![
                    Predicate::eq("$label", "FOLLOWS"),
                    Predicate::and(vec![edge_invalid_eq()]),
                ]),
            }),
            &edge_context,
        ),
        (
            raw_read(AstNode::EdgesWhere {
                predicate: Predicate::and(vec![
                    Predicate::eq("$label", "FOLLOWS"),
                    Predicate::or(vec![Predicate::eq("status", "active"), edge_invalid_eq()]),
                ]),
            }),
            &edge_context,
        ),
        (
            raw_read(AstNode::Where {
                input: Box::new(AstNode::EdgesWhere {
                    predicate: Predicate::eq("$label", "FOLLOWS"),
                }),
                predicate: edge_invalid_eq(),
            }),
            &edge_context,
        ),
        (
            raw_read(AstNode::EdgesWhere {
                predicate: Predicate::and(vec![
                    Predicate::eq("$label", "FOLLOWS"),
                    edge_invalid_range(),
                ]),
            }),
            &edge_context,
        ),
    ];

    for (batch, context) in cases {
        let plan = plan_read_checked(&batch, context).unwrap();

        assert!(plan
            .steps()
            .iter()
            .any(|step| matches!(&step.op, ExecOp::Filter { .. })));
    }

    let plan = plan_read_checked(
        &raw_read(AstNode::Where {
            input: Box::new(AstNode::EdgesWhere {
                predicate: Predicate::compare(Expr::val(10), CompareOp::Gt, Expr::val(1)),
            }),
            predicate: Predicate::and(vec![Predicate::eq("$label", "FOLLOWS"), edge_invalid_eq()]),
        }),
        &edge_context,
    )
    .unwrap();
    assert!(plan
        .steps()
        .iter()
        .any(|step| matches!(&step.op, ExecOp::Filter { .. })));
}

#[test]
fn scoped_residual_predicates_propagate_validation_errors() {
    for root in [
        AstNode::Where {
            input: Box::new(AstNode::Nodes {
                reference: NodeRef::ids([1u64]),
            }),
            predicate: Predicate::has_key(String::new()),
        },
        AstNode::NodesWhere {
            predicate: Predicate::and(vec![
                Predicate::eq("$label", "User"),
                Predicate::has_key(String::new()),
            ]),
        },
        AstNode::EdgesWhere {
            predicate: Predicate::and(vec![
                Predicate::eq("$label", "FOLLOWS"),
                Predicate::has_key(String::new()),
            ]),
        },
    ] {
        assert_eq!(
            plan_read_checked(&raw_read(root), &PlannerContext::default()).unwrap_err(),
            PlannerError::InvalidEmptyName {
                field: NameField::Property
            }
        );
    }
}
