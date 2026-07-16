use super::*;

#[test]
fn foreach_rejects_empty_parameter_name_from_raw_batch() {
    let batch = ReadBatch {
        entries: vec![BatchEntry::ForEach {
            param: String::new(),
            body: Vec::new(),
        }],
        returns: Vec::new(),
    };

    assert_eq!(
        plan_read_checked(&batch, &PlannerContext::default()).unwrap_err(),
        PlannerError::InvalidEmptyName {
            field: NameField::Param
        }
    );
}

#[test]
fn batches_reject_empty_entry_lists_from_raw_batch() {
    assert_eq!(
        [BatchOp::Batch, BatchOp::ForEach].map(|op| op.to_string()),
        ["batch", "foreach"]
    );

    let empty_read = ReadBatch {
        entries: Vec::new(),
        returns: Vec::new(),
    };
    let empty_write = helix_ast::batch::WriteBatch {
        entries: Vec::new(),
        returns: Vec::new(),
    };
    let empty_foreach = ReadBatch {
        entries: vec![BatchEntry::ForEach {
            param: "items".to_string(),
            body: Vec::new(),
        }],
        returns: Vec::new(),
    };

    assert_eq!(
        plan_read_checked(&empty_read, &PlannerContext::default()).unwrap_err(),
        PlannerError::InvalidBatchArity {
            op: BatchOp::Batch,
            min: 1,
            actual: 0,
        }
    );
    assert_eq!(
        plan_write_checked(&empty_write, &PlannerContext::default()).unwrap_err(),
        PlannerError::InvalidBatchArity {
            op: BatchOp::Batch,
            min: 1,
            actual: 0,
        }
    );
    assert_eq!(
        plan_read_checked(&empty_foreach, &PlannerContext::default()).unwrap_err(),
        PlannerError::InvalidBatchArity {
            op: BatchOp::ForEach,
            min: 1,
            actual: 0,
        }
    );
}

#[test]
fn write_batches_reject_row_binding_operations() {
    let write_batch = |root| helix_ast::batch::WriteBatch {
        entries: vec![BatchEntry::Query(Box::new(NamedQuery {
            name: Some("bad".to_string()),
            root,
            condition: None,
        }))],
        returns: Vec::new(),
    };
    let sub_bind = || sub().bind("row");
    let cases = [
        (
            write_batch(g().n(NodeRef::all()).bind("row").count().into_ast()),
            ReadOnlyWriteOp::Bind,
        ),
        (
            write_batch(
                g().n(NodeRef::all())
                    .project_bindings(vec![BindingProjection::current("$id", "id")])
                    .into_ast(),
            ),
            ReadOnlyWriteOp::ProjectBindings,
        ),
        (
            helix_ast::batch::WriteBatch {
                entries: vec![BatchEntry::ForEach {
                    param: "items".to_string(),
                    body: vec![BatchEntry::Query(Box::new(NamedQuery {
                        name: Some("bad".to_string()),
                        root: g().n(NodeRef::all()).bind("row").into_ast(),
                        condition: None,
                    }))],
                }],
                returns: Vec::new(),
            },
            ReadOnlyWriteOp::Bind,
        ),
        (
            write_batch(g().n(NodeRef::all()).optional(sub_bind()).into_ast()),
            ReadOnlyWriteOp::Bind,
        ),
        (
            write_batch(g().n(NodeRef::all()).union(vec![sub_bind()]).into_ast()),
            ReadOnlyWriteOp::Bind,
        ),
        (
            write_batch(g().n(NodeRef::all()).coalesce(vec![sub_bind()]).into_ast()),
            ReadOnlyWriteOp::Bind,
        ),
        (
            write_batch(
                g().n(NodeRef::all())
                    .choose(Predicate::eq("active", true), sub_bind(), None)
                    .into_ast(),
            ),
            ReadOnlyWriteOp::Bind,
        ),
        (
            write_batch(
                g().n(NodeRef::all())
                    .choose(
                        Predicate::eq("active", true),
                        sub().out(Some("FOLLOWS")),
                        Some(sub_bind()),
                    )
                    .into_ast(),
            ),
            ReadOnlyWriteOp::Bind,
        ),
        (
            write_batch(
                g().n(NodeRef::all())
                    .repeat(RepeatConfig::new(sub_bind()).times(1))
                    .into_ast(),
            ),
            ReadOnlyWriteOp::Bind,
        ),
    ];

    for (batch, op) in cases {
        assert_eq!(
            plan_write_checked(&batch, &PlannerContext::default()).unwrap_err(),
            PlannerError::ReadOnlyTraversalInWriteBatch { op }
        );
    }
}

#[test]
fn stream_bounds_reject_empty_expression_names_from_raw_ast() {
    let search_context = ctx(builtin_label_indexes()
        .with_vector(
            SearchIndexKey::try_new(ElementKind::Node, "Doc", "embedding").unwrap(),
            SearchIndexScope::Unscoped,
        )
        .with_text(
            SearchIndexKey::try_new(ElementKind::Node, "Doc", "body").unwrap(),
            SearchIndexScope::Unscoped,
        )
        .with_vector(
            SearchIndexKey::try_new(ElementKind::Edge, "MENTIONS", "embedding").unwrap(),
            SearchIndexScope::Unscoped,
        )
        .with_text(
            SearchIndexKey::try_new(ElementKind::Edge, "MENTIONS", "body").unwrap(),
            SearchIndexScope::Unscoped,
        ));
    let default_cases = [
        (
            AstNode::Limit {
                input: boxed_nodes_root(),
                count: StreamBound::expr(Expr::param(String::new())),
            },
            NameField::Param,
        ),
        (
            AstNode::Skip {
                input: boxed_nodes_root(),
                count: StreamBound::expr(Expr::param(String::new())),
            },
            NameField::Param,
        ),
        (
            AstNode::Range {
                input: boxed_nodes_root(),
                start: StreamBound::expr(Expr::prop(String::new())),
                end: StreamBound::Literal(10),
            },
            NameField::Property,
        ),
        (
            AstNode::Range {
                input: boxed_nodes_root(),
                start: StreamBound::Literal(0),
                end: StreamBound::expr(Expr::param(String::new())),
            },
            NameField::Param,
        ),
    ];

    for (root, field) in default_cases {
        let batch = raw_read(root);

        assert_eq!(
            plan_read_checked(&batch, &PlannerContext::default()).unwrap_err(),
            PlannerError::InvalidEmptyName { field }
        );
    }

    let search_cases = [
        (
            AstNode::VectorSearchNodes {
                label: "Doc".to_string(),
                property: "embedding".to_string(),
                tenant_value: None,
                query_vector: PropertyInput::from(PropertyValue::F32Array(vec![0.1])),
                k: StreamBound::expr(Expr::param(String::new())),
            },
            NameField::Param,
        ),
        (
            AstNode::TextSearchNodes {
                label: "Doc".to_string(),
                property: "body".to_string(),
                tenant_value: None,
                query_text: PropertyInput::from("query"),
                k: StreamBound::expr(Expr::prop(String::new())),
            },
            NameField::Property,
        ),
        (
            AstNode::VectorSearchEdges {
                label: "MENTIONS".to_string(),
                property: "embedding".to_string(),
                tenant_value: None,
                query_vector: PropertyInput::from(PropertyValue::F32Array(vec![0.1])),
                k: StreamBound::expr(Expr::prop(String::new())),
            },
            NameField::Property,
        ),
        (
            AstNode::TextSearchEdges {
                label: "MENTIONS".to_string(),
                property: "body".to_string(),
                tenant_value: None,
                query_text: PropertyInput::from("query"),
                k: StreamBound::expr(Expr::prop(String::new())),
            },
            NameField::Property,
        ),
    ];

    for (root, field) in search_cases {
        let batch = raw_read(root);

        assert_eq!(
            plan_read_checked(&batch, &search_context).unwrap_err(),
            PlannerError::InvalidEmptyName { field }
        );
    }
}

#[test]
fn stream_bounds_reject_static_invalid_expressions_from_raw_ast() {
    let cases = [
        AstNode::Limit {
            input: boxed_nodes_root(),
            count: StreamBound::expr(Expr::val("many")),
        },
        AstNode::Skip {
            input: boxed_nodes_root(),
            count: StreamBound::expr(Expr::val(-1)),
        },
        AstNode::Range {
            input: boxed_nodes_root(),
            start: StreamBound::expr(Expr::val("start")),
            end: StreamBound::Literal(10),
        },
        AstNode::Range {
            input: boxed_nodes_root(),
            start: StreamBound::Literal(0),
            end: StreamBound::expr(Expr::val("end")),
        },
    ];

    for root in cases {
        let batch = raw_read(root);

        assert_eq!(
            plan_read_checked(&batch, &PlannerContext::default()).unwrap_err(),
            PlannerError::InvalidStreamBoundExpression {
                expected: StreamBoundExpected::NonNegativeInteger
            }
        );
    }
}

#[test]
fn stream_range_rejects_inverted_literal_bounds_from_raw_ast() {
    let batch = raw_read(AstNode::Range {
        input: boxed_nodes_root(),
        start: StreamBound::Literal(8),
        end: StreamBound::Literal(2),
    });

    assert_eq!(
        plan_read_checked(&batch, &PlannerContext::default()).unwrap_err(),
        PlannerError::InvalidStreamRange { start: 8, end: 2 }
    );
}

#[test]
fn query_output_identifiers_reject_empty_names_from_raw_batch() {
    let empty_name = ReadBatch {
        entries: vec![BatchEntry::Query(Box::new(NamedQuery {
            name: Some(String::new()),
            root: AstNode::Nodes {
                reference: NodeRef::all(),
            },
            condition: None,
        }))],
        returns: Vec::new(),
    };
    let empty_return = ReadBatch {
        entries: Vec::new(),
        returns: vec![String::new()],
    };

    assert_eq!(
        plan_read_checked(&empty_name, &PlannerContext::default()).unwrap_err(),
        PlannerError::InvalidEmptyName {
            field: NameField::Name
        }
    );
    assert_eq!(
        plan_read_checked(&empty_return, &PlannerContext::default()).unwrap_err(),
        PlannerError::InvalidEmptyName {
            field: NameField::Return
        }
    );
}

#[test]
fn query_returns_reject_duplicate_names_from_raw_batch() {
    let batch = ReadBatch {
        entries: vec![BatchEntry::Query(Box::new(NamedQuery {
            name: Some("users".to_string()),
            root: AstNode::Nodes {
                reference: NodeRef::all(),
            },
            condition: None,
        }))],
        returns: vec!["users".to_string(), "users".to_string()],
    };

    assert_eq!(
        plan_read_checked(&batch, &PlannerContext::default()).unwrap_err(),
        PlannerError::DuplicateReturnVariable {
            name: NonEmptyString::new("users").unwrap()
        }
    );
}

#[test]
fn variable_batch_conditions_reject_empty_names_from_raw_batch() {
    for condition in [
        BatchCondition::VarNotEmpty(String::new()),
        BatchCondition::VarEmpty(String::new()),
        BatchCondition::VarMinSize(String::new(), 1),
    ] {
        let batch = ReadBatch {
            entries: vec![BatchEntry::Query(Box::new(NamedQuery {
                name: Some("valid".to_string()),
                root: AstNode::Nodes {
                    reference: NodeRef::all(),
                },
                condition: Some(condition),
            }))],
            returns: Vec::new(),
        };

        assert_eq!(
            plan_read_checked(&batch, &PlannerContext::default()).unwrap_err(),
            PlannerError::InvalidEmptyName {
                field: NameField::Variable
            }
        );
    }
}

#[test]
fn previous_result_condition_rejects_first_entry_positions_from_raw_batch() {
    let invalid_query = || {
        BatchEntry::Query(Box::new(NamedQuery {
            name: Some("valid".to_string()),
            root: AstNode::Nodes {
                reference: NodeRef::all(),
            },
            condition: Some(BatchCondition::PrevNotEmpty),
        }))
    };
    let top_level = ReadBatch {
        entries: vec![invalid_query()],
        returns: Vec::new(),
    };
    let nested = ReadBatch {
        entries: vec![BatchEntry::ForEach {
            param: "items".to_string(),
            body: vec![invalid_query()],
        }],
        returns: Vec::new(),
    };

    for batch in [top_level, nested] {
        assert_eq!(
            InitialBatchCondition::PrevNotEmpty.to_string(),
            "prev_not_empty"
        );
        assert_eq!(
            plan_read_checked(&batch, &PlannerContext::default()).unwrap_err(),
            PlannerError::InvalidInitialBatchCondition {
                condition: InitialBatchCondition::PrevNotEmpty
            }
        );
    }
}
