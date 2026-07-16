use super::*;

#[test]
fn variable_operations_reject_empty_names_from_raw_ast() {
    let cases = [
        (
            AstNode::As {
                input: boxed_nodes_root(),
                name: String::new(),
            },
            NameField::Name,
        ),
        (
            AstNode::Store {
                input: boxed_nodes_root(),
                name: String::new(),
            },
            NameField::Name,
        ),
        (
            AstNode::Select {
                input: boxed_nodes_root(),
                name: String::new(),
            },
            NameField::Name,
        ),
        (
            AstNode::Bind {
                input: boxed_nodes_root(),
                name: String::new(),
            },
            NameField::Name,
        ),
        (
            AstNode::Within {
                input: boxed_nodes_root(),
                variable: String::new(),
            },
            NameField::Variable,
        ),
        (
            AstNode::Without {
                input: boxed_nodes_root(),
                variable: String::new(),
            },
            NameField::Variable,
        ),
        (
            AstNode::Inject {
                input: Some(boxed_nodes_root()),
                variable: String::new(),
            },
            NameField::Variable,
        ),
        (
            AstNode::Inject {
                input: None,
                variable: String::new(),
            },
            NameField::Variable,
        ),
    ];

    for (root, field) in cases {
        let batch = raw_read(root);

        assert_eq!(
            plan_read_checked(&batch, &PlannerContext::default()).unwrap_err(),
            PlannerError::InvalidEmptyName { field }
        );
    }
}

#[test]
fn direct_variable_references_reject_empty_names_from_raw_ast() {
    let cases = [
        AstNode::Nodes {
            reference: NodeRef::Var(String::new()),
        },
        AstNode::Edges {
            reference: EdgeRef::Var(String::new()),
        },
        AstNode::AddE {
            input: boxed_nodes_root(),
            label: "FOLLOWS".to_string(),
            to: NodeRef::Var(String::new()),
            properties: Vec::new(),
        },
        AstNode::DropEdge {
            input: boxed_nodes_root(),
            to: NodeRef::Var(String::new()),
        },
        AstNode::DropEdgeLabeled {
            input: boxed_nodes_root(),
            to: NodeRef::Var(String::new()),
            label: "FOLLOWS".to_string(),
        },
        AstNode::DropEdgeById {
            input: None,
            edges: EdgeRef::Var(String::new()),
        },
    ];

    for root in cases {
        let batch = raw_read(root);

        assert_eq!(
            plan_read_checked(&batch, &PlannerContext::default()).unwrap_err(),
            PlannerError::InvalidEmptyName {
                field: NameField::Variable
            }
        );
    }
}

#[test]
fn concrete_id_references_reject_duplicate_ids_from_raw_ast() {
    let cases = [
        (
            AstNode::Nodes {
                reference: NodeRef::ids([7, 7]),
            },
            ElementKind::Node,
            7,
        ),
        (
            AstNode::Edges {
                reference: EdgeRef::ids([3, 3]),
            },
            ElementKind::Edge,
            3,
        ),
        (
            AstNode::DropEdge {
                input: boxed_nodes_root(),
                to: NodeRef::ids([2, 2]),
            },
            ElementKind::Node,
            2,
        ),
        (
            AstNode::DropEdgeById {
                input: None,
                edges: EdgeRef::ids([8, 8]),
            },
            ElementKind::Edge,
            8,
        ),
    ];

    for (root, element, id) in cases {
        let batch = raw_read(root);

        assert_eq!(
            plan_read_checked(&batch, &PlannerContext::default()).unwrap_err(),
            PlannerError::DuplicateElementId { element, id }
        );
    }
}

#[test]
fn parameter_references_reject_empty_names_from_raw_ast() {
    let direct_cases = [
        AstNode::Nodes {
            reference: NodeRef::Param(String::new()),
        },
        AstNode::Edges {
            reference: EdgeRef::Param(String::new()),
        },
        AstNode::AddE {
            input: boxed_nodes_root(),
            label: "FOLLOWS".to_string(),
            to: NodeRef::Param(String::new()),
            properties: Vec::new(),
        },
        AstNode::DropEdge {
            input: boxed_nodes_root(),
            to: NodeRef::Param(String::new()),
        },
        AstNode::DropEdgeLabeled {
            input: boxed_nodes_root(),
            to: NodeRef::Param(String::new()),
            label: "FOLLOWS".to_string(),
        },
        AstNode::DropEdgeById {
            input: None,
            edges: EdgeRef::Param(String::new()),
        },
    ];

    for root in direct_cases {
        let batch = raw_read(root);

        assert_eq!(
            plan_read_checked(&batch, &PlannerContext::default()).unwrap_err(),
            PlannerError::InvalidEmptyName {
                field: NameField::Param
            }
        );
    }

    let indexed_param = read_batch().var_as(
        "invalid",
        g().n_with_label_where("User", Predicate::eq_param("username", "")),
    );

    assert_eq!(
        plan_read_checked(
            &indexed_param,
            &ctx(builtin_label_indexes()
                .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())),
        )
        .unwrap_err(),
        PlannerError::InvalidEmptyName {
            field: NameField::Param
        }
    );
}

#[test]
fn emitted_predicate_fields_reject_empty_names_from_raw_ast() {
    let repeat_until = RepeatConfig {
        traversal: sub().out(Some("FOLLOWS")),
        times: None,
        until: Some(Predicate::has_key(String::new())),
        emit: EmitBehavior::None,
        emit_predicate: None,
        max_depth: 5,
    };
    let repeat_emit = RepeatConfig {
        traversal: sub().out(Some("FOLLOWS")),
        times: None,
        until: None,
        emit: EmitBehavior::After,
        emit_predicate: Some(Predicate::has_key(String::new())),
        max_depth: 5,
    };
    let cases = [
        AstNode::Where {
            input: boxed_nodes_root(),
            predicate: Predicate::has_key(String::new()),
        },
        AstNode::NodesWhere {
            predicate: Predicate::has_key(String::new()),
        },
        AstNode::EdgesWhere {
            predicate: Predicate::has_key(String::new()),
        },
        AstNode::Choose {
            input: boxed_nodes_root(),
            condition: Predicate::has_key(String::new()),
            then_traversal: sub().out(Some("FOLLOWS")),
            else_traversal: None,
        },
        AstNode::Repeat {
            input: boxed_nodes_root(),
            config: repeat_until,
        },
        AstNode::Repeat {
            input: boxed_nodes_root(),
            config: repeat_emit,
        },
    ];

    for root in cases {
        let batch = raw_read(root);

        assert_eq!(
            plan_read_checked(&batch, &PlannerContext::default()).unwrap_err(),
            PlannerError::InvalidEmptyName {
                field: NameField::Property
            }
        );
    }
}
