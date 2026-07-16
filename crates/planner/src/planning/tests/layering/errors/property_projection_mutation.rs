use super::*;

#[test]
fn planner_owned_property_fields_reject_empty_names_from_raw_ast() {
    let cases = [
        AstNode::Values {
            input: boxed_nodes_root(),
            properties: vec![String::new()],
        },
        AstNode::ValueMap {
            input: boxed_nodes_root(),
            properties: Some(vec![String::new()]),
        },
        AstNode::AddN {
            input: None,
            label: "User".to_string(),
            properties: vec![(String::new(), PropertyInput::from("alice"))],
        },
        AstNode::SetProperty {
            input: boxed_nodes_root(),
            name: String::new(),
            value: PropertyInput::from(true),
        },
        AstNode::RemoveProperty {
            input: boxed_nodes_root(),
            name: String::new(),
        },
        AstNode::OrderBy {
            input: boxed_nodes_root(),
            property: String::new(),
            order: Order::Asc,
        },
        AstNode::OrderByMultiple {
            input: boxed_nodes_root(),
            orderings: vec![(String::new(), Order::Asc)],
        },
        AstNode::Group {
            input: boxed_nodes_root(),
            property: String::new(),
        },
        AstNode::GroupCount {
            input: boxed_nodes_root(),
            property: String::new(),
        },
        AstNode::AggregateBy {
            input: boxed_nodes_root(),
            function: AggregateFunction::Count,
            property: String::new(),
        },
        AstNode::SackSet {
            input: boxed_nodes_root(),
            property: String::new(),
        },
        AstNode::SackAdd {
            input: boxed_nodes_root(),
            property: String::new(),
        },
        AstNode::Project {
            input: boxed_nodes_root(),
            projections: vec![Projection::property(String::new(), "alias")],
        },
        AstNode::ProjectBindings {
            input: boxed_nodes_root(),
            projections: vec![BindingProjection::Property {
                target: BindingTarget::Current,
                source: String::new(),
                alias: "alias".to_string(),
            }],
            distinct: false,
        },
        AstNode::ProjectBindings {
            input: boxed_nodes_root(),
            projections: vec![BindingProjection::Coalesce {
                refs: vec![BindingValueRef {
                    target: BindingTarget::Current,
                    source: String::new(),
                }],
                alias: "alias".to_string(),
            }],
            distinct: false,
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

#[test]
fn variable_batch_conditions_reject_zero_min_size_from_raw_batch() {
    let batch = ReadBatch {
        entries: vec![BatchEntry::Query(Box::new(NamedQuery {
            name: Some("valid".to_string()),
            root: AstNode::Nodes {
                reference: NodeRef::all(),
            },
            condition: Some(BatchCondition::VarMinSize("users".to_string(), 0)),
        }))],
        returns: Vec::new(),
    };

    assert_eq!(
        plan_read_checked(&batch, &PlannerContext::default()).unwrap_err(),
        PlannerError::InvalidBatchConditionMinSize { actual: 0 }
    );
}

#[test]
fn projection_fields_reject_empty_aliases_and_bindings_from_raw_ast() {
    let cases = [
        (
            AstNode::Project {
                input: boxed_nodes_root(),
                projections: vec![Projection::property("name", String::new())],
            },
            NameField::Alias,
        ),
        (
            AstNode::Project {
                input: boxed_nodes_root(),
                projections: vec![Projection::expr(String::new(), Expr::val(1))],
            },
            NameField::Alias,
        ),
        (
            AstNode::ProjectBindings {
                input: boxed_nodes_root(),
                projections: vec![BindingProjection::Property {
                    target: BindingTarget::Current,
                    source: "name".to_string(),
                    alias: String::new(),
                }],
                distinct: false,
            },
            NameField::Alias,
        ),
        (
            AstNode::ProjectBindings {
                input: boxed_nodes_root(),
                projections: vec![BindingProjection::Coalesce {
                    refs: vec![BindingValueRef {
                        target: BindingTarget::Current,
                        source: "$id".to_string(),
                    }],
                    alias: String::new(),
                }],
                distinct: false,
            },
            NameField::Alias,
        ),
        (
            AstNode::ProjectBindings {
                input: boxed_nodes_root(),
                projections: vec![BindingProjection::Property {
                    target: BindingTarget::Binding(String::new()),
                    source: "name".to_string(),
                    alias: "alias".to_string(),
                }],
                distinct: false,
            },
            NameField::Binding,
        ),
        (
            AstNode::ProjectBindings {
                input: boxed_nodes_root(),
                projections: vec![BindingProjection::Coalesce {
                    refs: vec![BindingValueRef {
                        target: BindingTarget::Binding(String::new()),
                        source: "name".to_string(),
                    }],
                    alias: "alias".to_string(),
                }],
                distinct: false,
            },
            NameField::Binding,
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
fn projection_lists_reject_duplicate_aliases_from_raw_ast() {
    let cases = [
        AstNode::Project {
            input: boxed_nodes_root(),
            projections: vec![
                Projection::property("name", "display"),
                Projection::expr("display", Expr::val(1)),
            ],
        },
        AstNode::ProjectBindings {
            input: boxed_nodes_root(),
            projections: vec![
                BindingProjection::current("name", "display"),
                BindingProjection::coalesce(vec![BindingValueRef::current("$id")], "display"),
            ],
            distinct: false,
        },
    ];

    for root in cases {
        let batch = raw_read(root);

        assert_eq!(
            plan_read_checked(&batch, &PlannerContext::default()).unwrap_err(),
            PlannerError::DuplicateProjectionAlias {
                alias: NonEmptyString::new("display").unwrap(),
            }
        );
    }
}

#[test]
fn property_selections_reject_duplicate_names_from_raw_ast() {
    let cases = [
        AstNode::Values {
            input: boxed_nodes_root(),
            properties: vec!["name".to_string(), "name".to_string()],
        },
        AstNode::ValueMap {
            input: boxed_nodes_root(),
            properties: Some(vec!["name".to_string(), "name".to_string()]),
        },
    ];

    for root in cases {
        let batch = raw_read(root);

        assert_eq!(
            plan_read_checked(&batch, &PlannerContext::default()).unwrap_err(),
            PlannerError::DuplicatePropertySelection {
                property: NonEmptyString::new("name").unwrap(),
            }
        );
    }
}

#[test]
fn projection_expressions_reject_empty_names_from_raw_ast() {
    let cases = [
        (
            AstNode::Project {
                input: boxed_nodes_root(),
                projections: vec![Projection::expr("computed", Expr::param(String::new()))],
            },
            NameField::Param,
        ),
        (
            AstNode::Project {
                input: boxed_nodes_root(),
                projections: vec![Projection::expr("computed", Expr::prop(String::new()))],
            },
            NameField::Property,
        ),
        (
            AstNode::Project {
                input: boxed_nodes_root(),
                projections: vec![Projection::expr(
                    "computed",
                    Expr::case(
                        vec![(Predicate::has_key(String::new()), Expr::val(1))],
                        None,
                    ),
                )],
            },
            NameField::Property,
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
fn projection_lists_reject_empty_cardinality_from_raw_ast() {
    assert_eq!(
        [
            ProjectionOp::Values,
            ProjectionOp::Project,
            ProjectionOp::ProjectBindings,
            ProjectionOp::Coalesce,
        ]
        .map(|op| op.to_string()),
        ["values", "project", "project_bindings", "coalesce"]
    );

    let cases = [
        (
            AstNode::Values {
                input: boxed_nodes_root(),
                properties: Vec::new(),
            },
            ProjectionOp::Values,
        ),
        (
            AstNode::Project {
                input: boxed_nodes_root(),
                projections: Vec::new(),
            },
            ProjectionOp::Project,
        ),
        (
            AstNode::ProjectBindings {
                input: boxed_nodes_root(),
                projections: Vec::new(),
                distinct: false,
            },
            ProjectionOp::ProjectBindings,
        ),
        (
            AstNode::ProjectBindings {
                input: boxed_nodes_root(),
                projections: vec![BindingProjection::Coalesce {
                    refs: Vec::new(),
                    alias: "alias".to_string(),
                }],
                distinct: false,
            },
            ProjectionOp::Coalesce,
        ),
    ];

    for (root, op) in cases {
        let batch = raw_read(root);

        assert_eq!(
            plan_read_checked(&batch, &PlannerContext::default()).unwrap_err(),
            PlannerError::InvalidProjectionArity {
                op,
                min: 1,
                actual: 0
            }
        );
    }
}

#[test]
fn property_input_expressions_reject_empty_names_from_raw_ast() {
    let cases = [
        (
            AstNode::AddN {
                input: None,
                label: "User".to_string(),
                properties: vec![("name".to_string(), PropertyInput::param(String::new()))],
            },
            NameField::Param,
        ),
        (
            AstNode::AddE {
                input: boxed_nodes_root(),
                label: "FOLLOWS".to_string(),
                to: NodeRef::all(),
                properties: vec![(
                    "since".to_string(),
                    PropertyInput::from(Expr::prop(String::new())),
                )],
            },
            NameField::Property,
        ),
        (
            AstNode::SetProperty {
                input: boxed_nodes_root(),
                name: "active".to_string(),
                value: PropertyInput::param(String::new()),
            },
            NameField::Param,
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
fn mutation_property_assignments_reject_duplicate_names_from_raw_ast() {
    let cases = [
        (
            AstNode::AddN {
                input: None,
                label: "User".to_string(),
                properties: vec![
                    ("name".to_string(), PropertyInput::from("alice")),
                    ("name".to_string(), PropertyInput::from("bob")),
                ],
            },
            "name",
        ),
        (
            AstNode::AddE {
                input: boxed_nodes_root(),
                label: "FOLLOWS".to_string(),
                to: NodeRef::all(),
                properties: vec![
                    ("since".to_string(), PropertyInput::from(2024)),
                    ("since".to_string(), PropertyInput::from(2025)),
                ],
            },
            "since",
        ),
    ];

    for (root, property) in cases {
        let batch = raw_read(root);

        assert_eq!(
            plan_read_checked(&batch, &PlannerContext::default()).unwrap_err(),
            PlannerError::DuplicatePropertyAssignment {
                property: NonEmptyString::new(property).unwrap()
            }
        );
    }
}

#[test]
fn planner_owned_label_fields_reject_empty_names_from_raw_ast() {
    let cases = [
        AstNode::Out {
            input: boxed_nodes_root(),
            label: Some(String::new()),
        },
        AstNode::OutE {
            input: boxed_nodes_root(),
            label: Some(String::new()),
        },
        AstNode::AddN {
            input: None,
            label: String::new(),
            properties: Vec::new(),
        },
        AstNode::AddE {
            input: boxed_nodes_root(),
            label: String::new(),
            to: NodeRef::all(),
            properties: Vec::new(),
        },
        AstNode::DropEdgeLabeled {
            input: boxed_nodes_root(),
            to: NodeRef::all(),
            label: String::new(),
        },
        AstNode::NodesWhere {
            predicate: Predicate::eq("$label", ""),
        },
        AstNode::EdgesWhere {
            predicate: Predicate::eq("$label", ""),
        },
        AstNode::NodesWhere {
            predicate: Predicate::and(vec![
                Predicate::eq("active", true),
                Predicate::eq("$label", ""),
            ]),
        },
        AstNode::NodesWhere {
            predicate: Predicate::or(vec![Predicate::eq("$label", "")]),
        },
        AstNode::EdgesWhere {
            predicate: Predicate::or(vec![
                Predicate::eq("$label", "FOLLOWS"),
                Predicate::eq("$label", ""),
            ]),
        },
    ];

    for root in cases {
        let batch = raw_read(root);

        assert_eq!(
            plan_read_checked(&batch, &PlannerContext::default()).unwrap_err(),
            PlannerError::InvalidEmptyName {
                field: NameField::Label
            }
        );
    }
}
