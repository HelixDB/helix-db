use super::*;

#[test]
fn search_inputs_reject_empty_names_from_raw_ast() {
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
                query_vector: PropertyInput::param(String::new()),
                k: StreamBound::Literal(1),
            },
            NameField::Param,
        ),
        (
            AstNode::TextSearchNodes {
                label: "Doc".to_string(),
                property: "body".to_string(),
                tenant_value: Some(PropertyInput::param(String::new())),
                query_text: PropertyInput::from("query"),
                k: StreamBound::Literal(1),
            },
            NameField::Param,
        ),
        (
            AstNode::TextSearchNodes {
                label: "Doc".to_string(),
                property: "body".to_string(),
                tenant_value: None,
                query_text: PropertyInput::param(String::new()),
                k: StreamBound::Literal(1),
            },
            NameField::Param,
        ),
        (
            AstNode::VectorSearchEdges {
                label: "MENTIONS".to_string(),
                property: "embedding".to_string(),
                tenant_value: None,
                query_vector: PropertyInput::from(Expr::prop(String::new())),
                k: StreamBound::Literal(1),
            },
            NameField::Property,
        ),
        (
            AstNode::TextSearchEdges {
                label: "MENTIONS".to_string(),
                property: "body".to_string(),
                tenant_value: None,
                query_text: PropertyInput::param(String::new()),
                k: StreamBound::Literal(1),
            },
            NameField::Param,
        ),
    ];

    for (root, field) in cases {
        let batch = raw_read(root);

        assert_eq!(
            plan_read_checked(&batch, &context).unwrap_err(),
            PlannerError::InvalidEmptyName { field }
        );
    }
}

#[test]
fn scoped_search_tenant_inputs_reject_empty_names_from_raw_ast() {
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
        AstNode::VectorSearchNodes {
            label: "Doc".to_string(),
            property: "embedding".to_string(),
            tenant_value: Some(PropertyInput::param(String::new())),
            query_vector: PropertyInput::from(PropertyValue::F32Array(vec![0.1])),
            k: StreamBound::Literal(1),
        },
        AstNode::TextSearchNodes {
            label: "Doc".to_string(),
            property: "body".to_string(),
            tenant_value: Some(PropertyInput::param(String::new())),
            query_text: PropertyInput::from("query"),
            k: StreamBound::Literal(1),
        },
        AstNode::VectorSearchEdges {
            label: "MENTIONS".to_string(),
            property: "embedding".to_string(),
            tenant_value: Some(PropertyInput::param(String::new())),
            query_vector: PropertyInput::from(PropertyValue::F32Array(vec![0.1])),
            k: StreamBound::Literal(1),
        },
        AstNode::TextSearchEdges {
            label: "MENTIONS".to_string(),
            property: "body".to_string(),
            tenant_value: Some(PropertyInput::param(String::new())),
            query_text: PropertyInput::from("query"),
            k: StreamBound::Literal(1),
        },
    ];

    for root in cases {
        assert_eq!(
            plan_read_checked(&raw_read(root), &context).unwrap_err(),
            PlannerError::InvalidEmptyName {
                field: NameField::Param
            }
        );
    }
}

#[test]
fn search_inputs_reject_mismatched_literal_types_from_raw_ast() {
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
                query_vector: PropertyInput::from("not a vector"),
                k: StreamBound::Literal(1),
            },
            PlannerError::InvalidSearchInput {
                kind: SearchIndexKind::Vector,
                expected: SearchQueryInputExpected::NonEmptyFiniteF32Array,
            },
        ),
        (
            AstNode::VectorSearchNodes {
                label: "Doc".to_string(),
                property: "embedding".to_string(),
                tenant_value: None,
                query_vector: PropertyInput::from(Expr::val("not a vector")),
                k: StreamBound::Literal(1),
            },
            PlannerError::InvalidSearchInput {
                kind: SearchIndexKind::Vector,
                expected: SearchQueryInputExpected::NonEmptyFiniteF32Array,
            },
        ),
        (
            AstNode::VectorSearchNodes {
                label: "Doc".to_string(),
                property: "embedding".to_string(),
                tenant_value: None,
                query_vector: PropertyInput::from(PropertyValue::F32Array(Vec::new())),
                k: StreamBound::Literal(1),
            },
            PlannerError::InvalidSearchInput {
                kind: SearchIndexKind::Vector,
                expected: SearchQueryInputExpected::NonEmptyFiniteF32Array,
            },
        ),
        (
            AstNode::VectorSearchNodes {
                label: "Doc".to_string(),
                property: "embedding".to_string(),
                tenant_value: None,
                query_vector: PropertyInput::from(PropertyValue::F32Array(vec![f32::INFINITY])),
                k: StreamBound::Literal(1),
            },
            PlannerError::InvalidSearchInput {
                kind: SearchIndexKind::Vector,
                expected: SearchQueryInputExpected::NonEmptyFiniteF32Array,
            },
        ),
        (
            AstNode::TextSearchNodes {
                label: "Doc".to_string(),
                property: "body".to_string(),
                tenant_value: None,
                query_text: PropertyInput::from(PropertyValue::F32Array(vec![0.1])),
                k: StreamBound::Literal(1),
            },
            PlannerError::InvalidSearchInput {
                kind: SearchIndexKind::Text,
                expected: SearchQueryInputExpected::NonEmptyString,
            },
        ),
        (
            AstNode::TextSearchNodes {
                label: "Doc".to_string(),
                property: "body".to_string(),
                tenant_value: None,
                query_text: PropertyInput::from(""),
                k: StreamBound::Literal(1),
            },
            PlannerError::InvalidSearchInput {
                kind: SearchIndexKind::Text,
                expected: SearchQueryInputExpected::NonEmptyString,
            },
        ),
        (
            AstNode::TextSearchNodes {
                label: "Doc".to_string(),
                property: "body".to_string(),
                tenant_value: None,
                query_text: PropertyInput::from(Expr::val("")),
                k: StreamBound::Literal(1),
            },
            PlannerError::InvalidSearchInput {
                kind: SearchIndexKind::Text,
                expected: SearchQueryInputExpected::NonEmptyString,
            },
        ),
        (
            AstNode::VectorSearchEdges {
                label: "MENTIONS".to_string(),
                property: "embedding".to_string(),
                tenant_value: None,
                query_vector: PropertyInput::from("not a vector"),
                k: StreamBound::Literal(1),
            },
            PlannerError::InvalidSearchInput {
                kind: SearchIndexKind::Vector,
                expected: SearchQueryInputExpected::NonEmptyFiniteF32Array,
            },
        ),
        (
            AstNode::TextSearchEdges {
                label: "MENTIONS".to_string(),
                property: "body".to_string(),
                tenant_value: None,
                query_text: PropertyInput::from(PropertyValue::F32Array(vec![0.1])),
                k: StreamBound::Literal(1),
            },
            PlannerError::InvalidSearchInput {
                kind: SearchIndexKind::Text,
                expected: SearchQueryInputExpected::NonEmptyString,
            },
        ),
        (
            AstNode::TextSearchEdges {
                label: "MENTIONS".to_string(),
                property: "body".to_string(),
                tenant_value: None,
                query_text: PropertyInput::from(Expr::val(vec![0.1_f32])),
                k: StreamBound::Literal(1),
            },
            PlannerError::InvalidSearchInput {
                kind: SearchIndexKind::Text,
                expected: SearchQueryInputExpected::NonEmptyString,
            },
        ),
        (
            AstNode::TextSearchEdges {
                label: "MENTIONS".to_string(),
                property: "body".to_string(),
                tenant_value: None,
                query_text: PropertyInput::from(""),
                k: StreamBound::Literal(1),
            },
            PlannerError::InvalidSearchInput {
                kind: SearchIndexKind::Text,
                expected: SearchQueryInputExpected::NonEmptyString,
            },
        ),
        (
            AstNode::TextSearchEdges {
                label: "MENTIONS".to_string(),
                property: "body".to_string(),
                tenant_value: None,
                query_text: PropertyInput::from(Expr::val("")),
                k: StreamBound::Literal(1),
            },
            PlannerError::InvalidSearchInput {
                kind: SearchIndexKind::Text,
                expected: SearchQueryInputExpected::NonEmptyString,
            },
        ),
    ];

    for (root, expected) in cases {
        let batch = raw_read(root);

        assert_eq!(plan_read_checked(&batch, &context).unwrap_err(), expected);
    }
}

#[test]
fn search_index_fields_reject_empty_names_from_raw_ast() {
    let cases = [
        (
            AstNode::VectorSearchNodes {
                label: String::new(),
                property: "embedding".to_string(),
                tenant_value: None,
                query_vector: PropertyInput::from(PropertyValue::F32Array(vec![0.1])),
                k: StreamBound::Literal(1),
            },
            NameField::Label,
        ),
        (
            AstNode::TextSearchNodes {
                label: String::new(),
                property: "body".to_string(),
                tenant_value: None,
                query_text: PropertyInput::from("query"),
                k: StreamBound::Literal(1),
            },
            NameField::Label,
        ),
        (
            AstNode::TextSearchNodes {
                label: "Doc".to_string(),
                property: String::new(),
                tenant_value: None,
                query_text: PropertyInput::from("query"),
                k: StreamBound::Literal(1),
            },
            NameField::Property,
        ),
        (
            AstNode::VectorSearchNodes {
                label: "Doc".to_string(),
                property: String::new(),
                tenant_value: None,
                query_vector: PropertyInput::from(PropertyValue::F32Array(vec![0.1])),
                k: StreamBound::Literal(1),
            },
            NameField::Property,
        ),
        (
            AstNode::VectorSearchEdges {
                label: String::new(),
                property: "embedding".to_string(),
                tenant_value: None,
                query_vector: PropertyInput::from(PropertyValue::F32Array(vec![0.1])),
                k: StreamBound::Literal(1),
            },
            NameField::Label,
        ),
        (
            AstNode::VectorSearchEdges {
                label: "MENTIONS".to_string(),
                property: String::new(),
                tenant_value: None,
                query_vector: PropertyInput::from(PropertyValue::F32Array(vec![0.1])),
                k: StreamBound::Literal(1),
            },
            NameField::Property,
        ),
        (
            AstNode::TextSearchEdges {
                label: String::new(),
                property: "body".to_string(),
                tenant_value: None,
                query_text: PropertyInput::from("query"),
                k: StreamBound::Literal(1),
            },
            NameField::Label,
        ),
        (
            AstNode::TextSearchEdges {
                label: "MENTIONS".to_string(),
                property: String::new(),
                tenant_value: None,
                query_text: PropertyInput::from("query"),
                k: StreamBound::Literal(1),
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
