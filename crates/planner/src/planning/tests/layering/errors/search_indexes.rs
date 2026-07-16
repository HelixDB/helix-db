use super::*;

#[test]
fn missing_vector_index_is_explicit_error() {
    let batch = read_batch().var_as(
        "hits",
        g().vector_search_nodes("Doc", "embedding", vec![0.0f32; 3], 10, None),
    );
    let err = plan_read_checked(&batch, &PlannerContext::default()).unwrap_err();
    assert!(matches!(
        err,
        PlannerError::MissingSearchIndex {
            element: ElementKind::Node,
            kind: SearchIndexKind::Vector,
            ..
        }
    ));

    let edge_batch = read_batch().var_as(
        "hits",
        g().vector_search_edges("MENTIONS", "embedding", vec![0.0f32; 3], 10, None),
    );
    assert!(matches!(
        plan_read_checked(&edge_batch, &PlannerContext::default()).unwrap_err(),
        PlannerError::MissingSearchIndex {
            element: ElementKind::Edge,
            kind: SearchIndexKind::Vector,
            ..
        }
    ));
}

#[test]
fn missing_text_index_is_explicit_error() {
    let node_batch =
        read_batch().var_as("hits", g().text_search_nodes("Doc", "body", "q", 10, None));
    assert!(matches!(
        plan_read_checked(&node_batch, &PlannerContext::default()).unwrap_err(),
        PlannerError::MissingSearchIndex {
            element: ElementKind::Node,
            kind: SearchIndexKind::Text,
            ..
        }
    ));

    let edge_batch =
        read_batch().var_as("hits", g().text_search_edges("Doc", "body", "q", 10, None));
    assert!(matches!(
        plan_read_checked(&edge_batch, &PlannerContext::default()).unwrap_err(),
        PlannerError::MissingSearchIndex {
            element: ElementKind::Edge,
            kind: SearchIndexKind::Text,
            ..
        }
    ));
}

#[test]
fn search_tenant_values_require_tenant_scoped_indexes() {
    let vector_context = ctx(builtin_label_indexes().with_vector(
        SearchIndexKey::try_new(ElementKind::Node, "Doc", "embedding").unwrap(),
        SearchIndexScope::Unscoped,
    ));
    let vector_batch = read_batch().var_as(
        "hits",
        g().vector_search_nodes("Doc", "embedding", vec![0.0f32; 3], 10, Some("acme".into())),
    );

    assert_eq!(
        plan_read_checked(&vector_batch, &vector_context).unwrap_err(),
        PlannerError::InvalidSearchTenant {
            kind: SearchIndexKind::Vector,
            index_id: NonEmptyString::new("vector:node:Doc:embedding").unwrap(),
        }
    );

    let text_context = ctx(builtin_label_indexes().with_text(
        SearchIndexKey::try_new(ElementKind::Node, "Doc", "body").unwrap(),
        SearchIndexScope::Unscoped,
    ));
    let text_batch = read_batch().var_as(
        "hits",
        g().text_search_nodes("Doc", "body", "query", 10, Some("acme".into())),
    );

    assert_eq!(
        plan_read_checked(&text_batch, &text_context).unwrap_err(),
        PlannerError::InvalidSearchTenant {
            kind: SearchIndexKind::Text,
            index_id: NonEmptyString::new("text:node:Doc:body").unwrap(),
        }
    );

    let edge_vector_context = ctx(builtin_label_indexes().with_vector(
        SearchIndexKey::try_new(ElementKind::Edge, "MENTIONS", "embedding").unwrap(),
        SearchIndexScope::Unscoped,
    ));
    let edge_vector_batch = read_batch().var_as(
        "hits",
        g().vector_search_edges(
            "MENTIONS",
            "embedding",
            vec![0.0f32; 3],
            10,
            Some("acme".into()),
        ),
    );

    assert_eq!(
        plan_read_checked(&edge_vector_batch, &edge_vector_context).unwrap_err(),
        PlannerError::InvalidSearchTenant {
            kind: SearchIndexKind::Vector,
            index_id: NonEmptyString::new("vector:edge:MENTIONS:embedding").unwrap(),
        }
    );

    let text_context = ctx(builtin_label_indexes().with_text(
        SearchIndexKey::try_new(ElementKind::Edge, "MENTIONS", "body").unwrap(),
        SearchIndexScope::Unscoped,
    ));
    let text_batch = read_batch().var_as(
        "hits",
        g().text_search_edges("MENTIONS", "body", "query", 10, Some("acme".into())),
    );

    assert_eq!(
        plan_read_checked(&text_batch, &text_context).unwrap_err(),
        PlannerError::InvalidSearchTenant {
            kind: SearchIndexKind::Text,
            index_id: NonEmptyString::new("text:edge:MENTIONS:body").unwrap(),
        }
    );
}

#[test]
fn search_tenant_values_reject_null_literals_from_raw_ast() {
    let context = ctx(builtin_label_indexes()
        .with_vector(
            SearchIndexKey::try_new(ElementKind::Node, "Doc", "embedding").unwrap(),
            SearchIndexScope::try_new(Some("tenant_id")).unwrap(),
        )
        .with_text(
            SearchIndexKey::try_new(ElementKind::Node, "Doc", "body").unwrap(),
            SearchIndexScope::try_new(Some("tenant_id")).unwrap(),
        )
        .with_vector(
            SearchIndexKey::try_new(ElementKind::Edge, "MENTIONS", "embedding").unwrap(),
            SearchIndexScope::try_new(Some("tenant_id")).unwrap(),
        )
        .with_text(
            SearchIndexKey::try_new(ElementKind::Edge, "MENTIONS", "body").unwrap(),
            SearchIndexScope::try_new(Some("tenant_id")).unwrap(),
        ));
    let cases = [
        (
            AstNode::VectorSearchNodes {
                label: "Doc".to_string(),
                property: "embedding".to_string(),
                tenant_value: Some(PropertyInput::from(PropertyValue::Null)),
                query_vector: PropertyInput::from(PropertyValue::F32Array(vec![0.1])),
                k: StreamBound::Literal(1),
            },
            SearchIndexKind::Vector,
        ),
        (
            AstNode::VectorSearchNodes {
                label: "Doc".to_string(),
                property: "embedding".to_string(),
                tenant_value: Some(PropertyInput::from(Expr::val(PropertyValue::Null))),
                query_vector: PropertyInput::from(PropertyValue::F32Array(vec![0.1])),
                k: StreamBound::Literal(1),
            },
            SearchIndexKind::Vector,
        ),
        (
            AstNode::TextSearchNodes {
                label: "Doc".to_string(),
                property: "body".to_string(),
                tenant_value: Some(PropertyInput::from(PropertyValue::Null)),
                query_text: PropertyInput::from("query"),
                k: StreamBound::Literal(1),
            },
            SearchIndexKind::Text,
        ),
        (
            AstNode::VectorSearchEdges {
                label: "MENTIONS".to_string(),
                property: "embedding".to_string(),
                tenant_value: Some(PropertyInput::from(PropertyValue::Null)),
                query_vector: PropertyInput::from(PropertyValue::F32Array(vec![0.1])),
                k: StreamBound::Literal(1),
            },
            SearchIndexKind::Vector,
        ),
        (
            AstNode::TextSearchEdges {
                label: "MENTIONS".to_string(),
                property: "body".to_string(),
                tenant_value: Some(PropertyInput::from(PropertyValue::Null)),
                query_text: PropertyInput::from("query"),
                k: StreamBound::Literal(1),
            },
            SearchIndexKind::Text,
        ),
    ];

    for (root, kind) in cases {
        assert_eq!(
            plan_read_checked(&raw_read(root), &context).unwrap_err(),
            PlannerError::InvalidSearchTenantValue {
                kind,
                expected: SearchTenantValueExpected::NonNullPropertyInput,
            }
        );
    }
    assert_eq!(
        SearchTenantValueExpected::NonNullPropertyInput.to_string(),
        "non-null property input"
    );
}

#[test]
fn search_result_counts_reject_zero_literals_from_raw_ast() {
    let context = ctx(builtin_label_indexes()
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
    let cases = [
        (
            AstNode::VectorSearchNodes {
                label: "Doc".to_string(),
                property: "embedding".to_string(),
                tenant_value: None,
                query_vector: PropertyInput::from(PropertyValue::F32Array(vec![0.1])),
                k: StreamBound::expr(Expr::val(0)),
            },
            SearchIndexKind::Vector,
        ),
        (
            AstNode::VectorSearchNodes {
                label: "Doc".to_string(),
                property: "embedding".to_string(),
                tenant_value: None,
                query_vector: PropertyInput::from(PropertyValue::F32Array(vec![0.1])),
                k: StreamBound::Literal(0),
            },
            SearchIndexKind::Vector,
        ),
        (
            AstNode::TextSearchNodes {
                label: "Doc".to_string(),
                property: "body".to_string(),
                tenant_value: None,
                query_text: PropertyInput::from("query"),
                k: StreamBound::Literal(0),
            },
            SearchIndexKind::Text,
        ),
        (
            AstNode::VectorSearchEdges {
                label: "MENTIONS".to_string(),
                property: "embedding".to_string(),
                tenant_value: None,
                query_vector: PropertyInput::from(PropertyValue::F32Array(vec![0.1])),
                k: StreamBound::Literal(0),
            },
            SearchIndexKind::Vector,
        ),
        (
            AstNode::TextSearchEdges {
                label: "MENTIONS".to_string(),
                property: "body".to_string(),
                tenant_value: None,
                query_text: PropertyInput::from("query"),
                k: StreamBound::Literal(0),
            },
            SearchIndexKind::Text,
        ),
    ];

    for (root, kind) in cases {
        let batch = raw_read(root);

        assert_eq!(
            plan_read_checked(&batch, &context).unwrap_err(),
            PlannerError::InvalidSearchResultCount { kind, actual: 0 }
        );
    }
}

#[test]
fn search_result_counts_reject_static_non_integer_expressions_from_raw_ast() {
    let context = ctx(builtin_label_indexes()
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
    let cases = [
        (
            AstNode::VectorSearchNodes {
                label: "Doc".to_string(),
                property: "embedding".to_string(),
                tenant_value: None,
                query_vector: PropertyInput::from(PropertyValue::F32Array(vec![0.1])),
                k: StreamBound::expr(Expr::val("many")),
            },
            SearchIndexKind::Vector,
        ),
        (
            AstNode::TextSearchNodes {
                label: "Doc".to_string(),
                property: "body".to_string(),
                tenant_value: None,
                query_text: PropertyInput::from("query"),
                k: StreamBound::expr(Expr::val("many")),
            },
            SearchIndexKind::Text,
        ),
        (
            AstNode::VectorSearchEdges {
                label: "MENTIONS".to_string(),
                property: "embedding".to_string(),
                tenant_value: None,
                query_vector: PropertyInput::from(PropertyValue::F32Array(vec![0.1])),
                k: StreamBound::expr(Expr::val("many")),
            },
            SearchIndexKind::Vector,
        ),
        (
            AstNode::TextSearchEdges {
                label: "MENTIONS".to_string(),
                property: "body".to_string(),
                tenant_value: None,
                query_text: PropertyInput::from("query"),
                k: StreamBound::expr(Expr::val("many")),
            },
            SearchIndexKind::Text,
        ),
    ];

    for (root, kind) in cases {
        let batch = raw_read(root);

        assert_eq!(
            plan_read_checked(&batch, &context).unwrap_err(),
            PlannerError::InvalidSearchResultCountExpression {
                kind,
                expected: SearchLimitExpected::PositiveInteger,
            }
        );
    }
}
