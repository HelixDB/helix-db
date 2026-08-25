use super::*;

#[test]
fn direct_variable_references_become_variable_access_plans() {
    let node_plan = executable_ast(
        AstNode::Nodes {
            reference: NodeRef::var("cached_nodes"),
        },
        PlannerContext::default(),
    );
    let edge_plan = executable_ast(
        AstNode::Edges {
            reference: EdgeRef::var("cached_edges"),
        },
        PlannerContext::default(),
    );

    assert!(matches!(
        first_exec_access(&node_plan),
        ExecAccessPlan::Node(ExecNodeAccessPlan::FromVar { variable })
            if variable.as_ref() == "cached_nodes"
    ));
    assert!(matches!(
        first_exec_access(&edge_plan),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::FromVar { variable })
            if variable.as_ref() == "cached_edges"
    ));
}

#[test]
fn node_to_edge_and_edge_to_node_expansions_preserve_direction_and_output() {
    let cases = [
        (
            AstNode::OutE {
                input: boxed(nodes_root()),
                label: Some("LIKES".to_string()),
            },
            ExpandDirection::Out,
            ExpandOutput::Edges,
            ExpandLabelPlan::Label(NonEmptyString::new("LIKES").unwrap()),
        ),
        (
            AstNode::InE {
                input: boxed(nodes_root()),
                label: Some("MENTIONS".to_string()),
            },
            ExpandDirection::In,
            ExpandOutput::Edges,
            ExpandLabelPlan::Label(NonEmptyString::new("MENTIONS").unwrap()),
        ),
        (
            AstNode::BothE {
                input: boxed(nodes_root()),
                label: None,
            },
            ExpandDirection::Both,
            ExpandOutput::Edges,
            ExpandLabelPlan::Any,
        ),
        (
            AstNode::InN {
                input: boxed(edges_root()),
            },
            ExpandDirection::In,
            ExpandOutput::Nodes,
            ExpandLabelPlan::Any,
        ),
        (
            AstNode::OtherN {
                input: boxed(edges_root()),
            },
            ExpandDirection::Both,
            ExpandOutput::Nodes,
            ExpandLabelPlan::Any,
        ),
    ];

    for (root, direction, output, label) in cases {
        let executable = executable_ast(root, PlannerContext::default());
        let ExecOp::Expand { plan } =
            first_exec_op(&executable, |op| matches!(op, ExecOp::Expand { .. }))
        else {
            panic!("expected expand");
        };
        assert_eq!(plan.direction, direction);
        assert_eq!(plan.output, output);
        assert_eq!(plan.label, label);
    }
}

#[test]
fn filter_wrappers_emit_residual_filter_contracts() {
    let cases = [
        (
            AstNode::HasLabel {
                input: boxed(AstNode::Nodes {
                    reference: NodeRef::ids([11u64, 13]),
                }),
                label: "User".to_string(),
            },
            Predicate::eq("$label", "User"),
        ),
        (
            AstNode::HasKey {
                input: boxed(nodes_root()),
                property: "email".to_string(),
            },
            Predicate::has_key("email"),
        ),
        (
            AstNode::Where {
                input: boxed(nodes_root()),
                predicate: Predicate::is_not_null("email"),
            },
            Predicate::is_not_null("email"),
        ),
        (
            AstNode::EdgeHas {
                input: boxed(edges_root()),
                property: "weight".to_string(),
                value: PropertyInput::param("wanted_weight"),
            },
            Predicate::eq("weight", 7),
        ),
        (
            AstNode::EdgeHasLabel {
                input: boxed(edges_root()),
                label: "FOLLOWS".to_string(),
            },
            Predicate::eq("$label", "FOLLOWS"),
        ),
    ];

    for (root, expected) in cases {
        let executable = executable_ast(
            root,
            PlannerContext {
                params: ParamBindings::default().with_value(
                    NonEmptyString::new("wanted_weight").unwrap(),
                    PropertyValue::I64(7),
                ),
                ..PlannerContext::default()
            },
        );
        let ExecOp::Filter { predicate } =
            first_exec_op(&executable, |op| matches!(op, ExecOp::Filter { .. }))
        else {
            panic!("expected filter");
        };
        assert_eq!(*predicate, expected);
    }
}

#[test]
fn stream_wrappers_preserve_bounds_and_variable_operations() {
    assert!(matches!(
        first_exec_op(
            &executable_ast(
                AstNode::Dedup {
                    input: boxed(nodes_root())
                },
                PlannerContext::default()
            ),
            |op| matches!(op, ExecOp::Distinct)
        ),
        ExecOp::Distinct
    ));

    assert!(matches!(
        first_exec_op(
            &executable_ast(
                AstNode::Skip {
                    input: boxed(nodes_root()),
                    count: StreamBound::Literal(3),
                },
                PlannerContext::default()
            ),
            |op| matches!(op, ExecOp::Skip { .. })
        ),
        ExecOp::Skip {
            count,
        } if *count == StreamBound::Literal(3)
    ));

    let static_limit = executable_ast(
        AstNode::Limit {
            input: boxed(nodes_root()),
            count: StreamBound::expr(Expr::val(3)),
        },
        PlannerContext::default(),
    );
    assert_eq!(first_kv_read_limit(&static_limit), Some(3));
    assert_no_exec_window(&static_limit);

    assert!(matches!(
        first_exec_op(
            &executable_ast(
                AstNode::Range {
                    input: boxed(nodes_root()),
                    start: StreamBound::Literal(2),
                    end: StreamBound::Literal(8),
                },
                PlannerContext::default()
            ),
            |op| matches!(op, ExecOp::Range { .. })
        ),
        ExecOp::Range {
            range,
        } if *range == StreamRangePlan::new(StreamBound::Literal(2), StreamBound::Literal(8)).unwrap()
    ));

    let variable_cases = [
        (
            AstNode::As {
                input: boxed(nodes_root()),
                name: "a".to_string(),
            },
            StreamVariableOp::As(NonEmptyString::new("a").unwrap()),
        ),
        (
            AstNode::Select {
                input: boxed(nodes_root()),
                name: "a".to_string(),
            },
            StreamVariableOp::Select(NonEmptyString::new("a").unwrap()),
        ),
        (
            AstNode::Bind {
                input: boxed(nodes_root()),
                name: "row".to_string(),
            },
            StreamVariableOp::Bind(NonEmptyString::new("row").unwrap()),
        ),
        (
            AstNode::Without {
                input: boxed(nodes_root()),
                variable: "excluded".to_string(),
            },
            StreamVariableOp::Without(NonEmptyString::new("excluded").unwrap()),
        ),
    ];

    for (root, expected) in variable_cases {
        assert!(matches!(
            first_exec_op(&executable_ast(root, PlannerContext::default()), |op| {
                matches!(op, ExecOp::Variable { .. })
            }),
            ExecOp::Variable {
                op: ExecVariableOp::Stream(op)
            } if *op == expected
        ));
    }
}
