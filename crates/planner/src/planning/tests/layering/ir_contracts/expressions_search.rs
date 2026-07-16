use super::*;

#[test]
fn expr_plan_validates_all_expression_and_predicate_shapes() {
    let binary_left = Expr::prop("age");
    let binary_right = Expr::val(18);
    let predicates = vec![
        Predicate::Eq {
            left: binary_left.clone(),
            right: binary_right.clone(),
        },
        Predicate::Neq {
            left: binary_left.clone(),
            right: binary_right.clone(),
        },
        Predicate::Gt {
            left: binary_left.clone(),
            right: binary_right.clone(),
        },
        Predicate::Gte {
            left: binary_left.clone(),
            right: binary_right.clone(),
        },
        Predicate::Lt {
            left: binary_left.clone(),
            right: binary_right.clone(),
        },
        Predicate::Lte {
            left: binary_left.clone(),
            right: binary_right.clone(),
        },
        Predicate::Between {
            value: Expr::prop("score"),
            min: Expr::val(1),
            max: Expr::val(10),
        },
        Predicate::HasKey {
            property: "name".to_string(),
        },
        Predicate::IsNull {
            property: "deleted_at".to_string(),
        },
        Predicate::IsNotNull {
            property: "created_at".to_string(),
        },
        Predicate::StartsWith {
            value: Expr::prop("name"),
            prefix: Expr::val("a"),
        },
        Predicate::EndsWith {
            value: Expr::prop("name"),
            suffix: Expr::val("z"),
        },
        Predicate::Contains {
            value: Expr::prop("name"),
            substring: Expr::val("li"),
        },
        Predicate::IsIn {
            value: Expr::prop("status"),
            values: Expr::val(PropertyValue::StringArray(vec![
                "active".to_string(),
                "pending".to_string(),
            ])),
        },
        Predicate::And {
            predicates: vec![Predicate::has_key("name")],
        },
        Predicate::Or {
            predicates: vec![Predicate::is_null("archived_at")],
        },
        Predicate::Not {
            predicate: Box::new(Predicate::is_not_null("email")),
        },
        Predicate::Compare {
            left: Expr::prop("rank"),
            op: CompareOp::Lte,
            right: Expr::val(5),
        },
    ];
    let case_expr = Expr::case(
        predicates
            .into_iter()
            .map(|predicate| (predicate, Expr::val(1)))
            .collect(),
        Some(Expr::param("fallback")),
    );
    let expressions = vec![
        Expr::prop("name"),
        Expr::id(),
        Expr::timestamp(),
        Expr::datetime(),
        Expr::val(1),
        Expr::param("limit"),
        Expr::val(1).add_expr(Expr::val(2)),
        Expr::val(3).sub_expr(Expr::val(2)),
        Expr::val(3).mul_expr(Expr::val(2)),
        Expr::val(4).div_expr(Expr::val(2)),
        Expr::val(5).modulo(Expr::val(2)),
        Expr::val(1).neg_expr(),
        Expr::case(Vec::new(), None),
        case_expr,
    ];

    for expr in expressions {
        let plan = ExprPlan::new(expr.clone()).unwrap();
        assert_eq!(plan, expr);
    }

    let serialized = serde_json::to_string(&ExprPlan::new(Expr::param("limit")).unwrap()).unwrap();
    let parsed: ExprPlan = serde_json::from_str(&serialized).unwrap();
    assert_eq!(parsed, Expr::param("limit"));
}

#[test]
fn expr_plan_rejects_empty_expression_names() {
    assert_eq!(
        [
            NameField::Alias,
            NameField::Binding,
            NameField::Label,
            NameField::Name,
            NameField::Param,
            NameField::Property,
            NameField::Return,
            NameField::TenantProperty,
            NameField::Variable,
        ]
        .map(|field| field.to_string()),
        [
            "alias",
            "binding",
            "label",
            "name",
            "param",
            "property",
            "return",
            "tenant_property",
            "variable",
        ]
    );

    for (expr, field) in [
        (Expr::param(String::new()), NameField::Param),
        (Expr::prop(String::new()), NameField::Property),
        (
            Expr::prop(String::new()).add_expr(Expr::val(1)),
            NameField::Property,
        ),
        (
            Expr::case(
                vec![(Predicate::is_null(String::new()), Expr::val(1))],
                None,
            ),
            NameField::Property,
        ),
    ] {
        let err = ExprPlan::new(expr).unwrap_err();
        assert_eq!(err, ExprPlanError::EmptyName { field });
        assert_eq!(err.to_string(), format!("{field} name must not be empty"));
    }

    assert!(serde_json::from_str::<ExprPlan>(r#"{"param":""}"#).is_err());
    assert!(serde_json::from_str::<ExprPlan>("[]").is_err());

    for predicate in [
        Predicate::compare(Expr::prop(String::new()), CompareOp::Eq, Expr::val(1)),
        Predicate::Between {
            value: Expr::prop(String::new()),
            min: Expr::val(1),
            max: Expr::val(2),
        },
        Predicate::Between {
            value: Expr::prop("age"),
            min: Expr::prop(String::new()),
            max: Expr::val(2),
        },
    ] {
        assert!(matches!(
            PredicatePlan::new(predicate),
            Err(ExprPlanError::EmptyName {
                field: NameField::Property
            })
        ));
    }
}

#[test]
fn property_input_plan_wraps_literals_and_validates_expressions() {
    assert_eq!(
        PropertyInputPlan::new(PropertyInput::from(true)).unwrap(),
        PropertyInputPlan::Value(PropertyValue::Bool(true))
    );
    assert_eq!(
        PropertyInputPlan::new(PropertyInput::from(Expr::val(true))).unwrap(),
        PropertyInputPlan::Value(PropertyValue::Bool(true))
    );

    let expression = PropertyInputPlan::new(PropertyInput::param("needle")).unwrap();
    assert_eq!(
        expression,
        PropertyInputPlan::Expr(PropertyInputExprPlan::new(Expr::param("needle")).unwrap())
    );
    assert_eq!(
        PropertyInputExprPlan::new(Expr::val(true)).unwrap_err(),
        PropertyInputExprPlanError::StaticLiteral
    );
    let input_expr = PropertyInputExprPlan::new(Expr::param("needle")).unwrap();
    assert_eq!(
        serde_json::from_str::<PropertyInputExprPlan>(&serde_json::to_string(&input_expr).unwrap())
            .unwrap(),
        input_expr
    );
    assert!(
        serde_json::from_str::<PropertyInputExprPlan>(r#"{"constant":{"bool":true}}"#).is_err()
    );
    assert!(serde_json::from_str::<PropertyInputExprPlan>(r#"{"param":""}"#).is_err());
    assert!(serde_json::from_str::<PropertyInputExprPlan>("[]").is_err());

    let serialized = serde_json::to_string(&expression).unwrap();
    let parsed: PropertyInputPlan = serde_json::from_str(&serialized).unwrap();
    assert_eq!(parsed, expression);
    assert_eq!(
        serde_json::from_str::<PropertyInputPlan>(r#"{"expr":{"constant":{"bool":true}}}"#)
            .unwrap(),
        PropertyInputPlan::Value(PropertyValue::Bool(true))
    );
    assert!(serde_json::from_str::<PropertyInputPlan>("[]").is_err());

    let err = PropertyInputPlan::new(PropertyInput::param(String::new())).unwrap_err();
    assert_eq!(
        err,
        ExprPlanError::EmptyName {
            field: NameField::Param
        }
    );
}

#[test]
fn search_query_input_plans_restrict_literal_payload_types() {
    assert_eq!(
        VectorQueryInputPlan::new(PropertyInput::from(PropertyValue::F32Array(vec![0.1, 0.2])))
            .unwrap(),
        VectorQueryInputPlan::Vector(SearchVector::new(vec![0.1, 0.2]).unwrap())
    );
    assert_eq!(
        VectorQueryInputPlan::new(PropertyInput::from(Expr::val(vec![0.1_f32, 0.2]))).unwrap(),
        VectorQueryInputPlan::Vector(SearchVector::new(vec![0.1, 0.2]).unwrap())
    );
    let vector = SearchVector::new(vec![0.1, 0.2]).unwrap();
    assert_eq!(
        vector.as_ref(),
        &[
            SearchVectorComponent::new(0.1).unwrap(),
            SearchVectorComponent::new(0.2).unwrap(),
        ]
    );
    assert!(SearchVectorComponent::new(f32::NEG_INFINITY).is_none());
    assert_eq!(SearchVector::new(Vec::new()), Err(SearchVectorError::Empty));
    assert_eq!(
        SearchVector::new(vec![f32::NAN]),
        Err(SearchVectorError::NonFiniteComponent)
    );
    assert_eq!(
        SearchVector::new(vec![f32::INFINITY]),
        Err(SearchVectorError::NonFiniteComponent)
    );
    assert_eq!(
        serde_json::from_str::<SearchVector>(&serde_json::to_string(&vector).unwrap()).unwrap(),
        vector
    );
    assert!(serde_json::from_str::<SearchVector>("[]").is_err());
    assert_eq!(
        VectorQueryInputPlan::new(PropertyInput::param("embedding")).unwrap(),
        VectorQueryInputPlan::Expr(SearchQueryExprPlan::new(Expr::param("embedding")).unwrap())
    );
    assert_eq!(
        SearchQueryExprPlan::new(Expr::val(vec![0.1_f32])).unwrap_err(),
        SearchQueryExprPlanError::StaticLiteral
    );
    let query_expr = SearchQueryExprPlan::new(Expr::param("embedding")).unwrap();
    assert_eq!(
        serde_json::from_str::<SearchQueryExprPlan>(&serde_json::to_string(&query_expr).unwrap())
            .unwrap(),
        query_expr
    );
    assert!(
        serde_json::from_str::<SearchQueryExprPlan>(r#"{"constant":{"string":"needle"}}"#).is_err()
    );
    assert!(serde_json::from_str::<SearchQueryExprPlan>(r#"{"param":""}"#).is_err());
    assert!(serde_json::from_str::<SearchQueryExprPlan>("[]").is_err());
    assert_eq!(
        VectorQueryInputPlan::new(PropertyInput::from("not a vector")).unwrap_err(),
        SearchQueryInputPlanError::InvalidLiteral {
            kind: SearchIndexKind::Vector,
            expected: SearchQueryInputExpected::NonEmptyFiniteF32Array,
        }
    );
    assert_eq!(
        VectorQueryInputPlan::new(PropertyInput::from(Expr::val("not a vector"))).unwrap_err(),
        SearchQueryInputPlanError::InvalidLiteral {
            kind: SearchIndexKind::Vector,
            expected: SearchQueryInputExpected::NonEmptyFiniteF32Array,
        }
    );
    assert_eq!(
        VectorQueryInputPlan::new(PropertyInput::from(PropertyValue::F32Array(Vec::new())))
            .unwrap_err(),
        SearchQueryInputPlanError::InvalidLiteral {
            kind: SearchIndexKind::Vector,
            expected: SearchQueryInputExpected::NonEmptyFiniteF32Array,
        }
    );
    assert!(matches!(
        VectorQueryInputPlan::new(PropertyInput::param(String::new())).unwrap_err(),
        SearchQueryInputPlanError::Expression(ExprPlanError::EmptyName {
            field: NameField::Param
        })
    ));

    assert_eq!(
        TextQueryInputPlan::new(PropertyInput::from("needle")).unwrap(),
        TextQueryInputPlan::Text(NonEmptyString::new("needle").unwrap())
    );
    assert_eq!(
        TextQueryInputPlan::new(PropertyInput::from(Expr::val("needle"))).unwrap(),
        TextQueryInputPlan::Text(NonEmptyString::new("needle").unwrap())
    );
    assert_eq!(
        TextQueryInputPlan::new(PropertyInput::from(Expr::prop("query"))).unwrap(),
        TextQueryInputPlan::Expr(SearchQueryExprPlan::new(Expr::prop("query")).unwrap())
    );
    assert_eq!(
        SearchQueryExprPlan::new(Expr::val("needle")).unwrap_err(),
        SearchQueryExprPlanError::StaticLiteral
    );
    assert_eq!(
        TextQueryInputPlan::new(PropertyInput::from(PropertyValue::F32Array(vec![0.1])))
            .unwrap_err(),
        SearchQueryInputPlanError::InvalidLiteral {
            kind: SearchIndexKind::Text,
            expected: SearchQueryInputExpected::NonEmptyString,
        }
    );
    assert_eq!(
        TextQueryInputPlan::new(PropertyInput::from(Expr::val(vec![0.1_f32]))).unwrap_err(),
        SearchQueryInputPlanError::InvalidLiteral {
            kind: SearchIndexKind::Text,
            expected: SearchQueryInputExpected::NonEmptyString,
        }
    );
    assert_eq!(
        TextQueryInputPlan::new(PropertyInput::from("")).unwrap_err(),
        SearchQueryInputPlanError::InvalidLiteral {
            kind: SearchIndexKind::Text,
            expected: SearchQueryInputExpected::NonEmptyString,
        }
    );
    assert_eq!(
        TextQueryInputPlan::new(PropertyInput::from(Expr::val(""))).unwrap_err(),
        SearchQueryInputPlanError::InvalidLiteral {
            kind: SearchIndexKind::Text,
            expected: SearchQueryInputExpected::NonEmptyString,
        }
    );
    assert!(matches!(
        TextQueryInputPlan::new(PropertyInput::from(Expr::prop(String::new()))).unwrap_err(),
        SearchQueryInputPlanError::Expression(ExprPlanError::EmptyName {
            field: NameField::Property
        })
    ));

    let vector =
        VectorQueryInputPlan::Expr(SearchQueryExprPlan::new(Expr::param("embedding")).unwrap());
    assert_eq!(
        serde_json::from_str::<VectorQueryInputPlan>(&serde_json::to_string(&vector).unwrap())
            .unwrap(),
        vector
    );
    assert_eq!(
        serde_json::from_str::<VectorQueryInputPlan>(r#"{"vector":[0.1]}"#).unwrap(),
        VectorQueryInputPlan::Vector(SearchVector::new(vec![0.1]).unwrap())
    );
    assert_eq!(
        serde_json::from_str::<VectorQueryInputPlan>(
            r#"{"expr":{"constant":{"f32_array":[0.1]}}}"#
        )
        .unwrap(),
        VectorQueryInputPlan::Vector(SearchVector::new(vec![0.1]).unwrap())
    );
    let text = TextQueryInputPlan::Text(NonEmptyString::new("needle").unwrap());
    assert_eq!(
        serde_json::from_str::<TextQueryInputPlan>(&serde_json::to_string(&text).unwrap()).unwrap(),
        text
    );
    assert_eq!(
        serde_json::from_str::<TextQueryInputPlan>(r#"{"expr":{"constant":{"string":"needle"}}}"#)
            .unwrap(),
        text
    );
    assert_eq!(
        [
            SearchQueryInputExpected::NonEmptyFiniteF32Array,
            SearchQueryInputExpected::NonEmptyString,
        ]
        .map(|expected| expected.to_string()),
        ["non-empty finite f32 array", "non-empty string"]
    );
    assert!(serde_json::from_str::<VectorQueryInputPlan>(r#"{"expr":{"param":""}}"#).is_err());
    assert!(serde_json::from_str::<VectorQueryInputPlan>(
        r#"{"expr":{"constant":{"string":"not a vector"}}}"#
    )
    .is_err());
    assert!(serde_json::from_str::<TextQueryInputPlan>(r#"{"expr":{"property":""}}"#).is_err());
    assert!(serde_json::from_str::<TextQueryInputPlan>(
        r#"{"expr":{"constant":{"f32_array":[0.1]}}}"#
    )
    .is_err());
    assert!(serde_json::from_str::<VectorQueryInputPlan>(r#"{"vector":"not a vector"}"#).is_err());
    assert!(serde_json::from_str::<VectorQueryInputPlan>(r#"{"vector":[]}"#).is_err());
    assert!(serde_json::from_str::<TextQueryInputPlan>(r#"{"text":["not text"]}"#).is_err());
    assert!(serde_json::from_str::<TextQueryInputPlan>(r#"{"text":""}"#).is_err());
}
