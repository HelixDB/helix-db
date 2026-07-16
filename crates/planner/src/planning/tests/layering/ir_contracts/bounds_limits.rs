use super::*;

#[test]
fn stream_bound_plan_wraps_literals_and_validates_expressions() {
    let literal = StreamBoundPlan::new(StreamBound::Literal(4)).unwrap();
    assert_eq!(literal, StreamBound::Literal(4));
    assert_ne!(literal, StreamBound::expr(Expr::param("limit")));
    assert_eq!(serde_json::to_string(&literal).unwrap(), r#"{"literal":4}"#);
    assert_eq!(
        StreamBoundPlan::new(StreamBound::expr(Expr::val(4))).unwrap(),
        StreamBoundPlan::Literal(4)
    );

    let expression = StreamBoundPlan::new(StreamBound::expr(Expr::param("limit"))).unwrap();
    assert_eq!(expression, StreamBound::expr(Expr::param("limit")));
    let bound_expr = StreamBoundExprPlan::new(Expr::param("limit")).unwrap();
    assert_eq!(
        serde_json::from_str::<StreamBoundExprPlan>(&serde_json::to_string(&bound_expr).unwrap())
            .unwrap(),
        bound_expr
    );

    let serialized = serde_json::to_string(&expression).unwrap();
    let parsed: StreamBoundPlan = serde_json::from_str(&serialized).unwrap();
    assert_eq!(parsed, expression);

    let err = StreamBoundPlan::new(StreamBound::expr(Expr::param(String::new()))).unwrap_err();
    assert!(matches!(
        err,
        StreamBoundPlanError::Expression(ExprPlanError::EmptyName {
            field: NameField::Param
        })
    ));
    assert_eq!(
        StreamBoundPlan::new(StreamBound::expr(Expr::val(-1))).unwrap_err(),
        StreamBoundPlanError::StaticLiteral {
            expected: StreamBoundExpected::NonNegativeInteger
        }
    );
    assert_eq!(
        StreamBoundPlan::new(StreamBound::expr(Expr::val("many"))).unwrap_err(),
        StreamBoundPlanError::StaticLiteral {
            expected: StreamBoundExpected::NonNegativeInteger
        }
    );
    assert_eq!(
        StreamBoundExprPlan::new(Expr::val(1)).unwrap_err(),
        StreamBoundExprPlanError::StaticLiteral {
            expected: StreamBoundExpected::NonNegativeInteger
        }
    );
    assert_eq!(
        StreamBoundExpected::NonNegativeInteger.to_string(),
        "non-negative integer"
    );
    assert!(serde_json::from_str::<StreamBoundExprPlan>(r#"{"constant":{"i64":1}}"#).is_err());
    assert!(serde_json::from_str::<StreamBoundExprPlan>(r#"{"param":""}"#).is_err());
    assert!(serde_json::from_str::<StreamBoundExprPlan>("[]").is_err());
    assert_eq!(
        serde_json::from_str::<StreamBoundPlan>(r#"{"expr":{"constant":{"i64":4}}}"#).unwrap(),
        StreamBoundPlan::Literal(4)
    );
    assert!(
        serde_json::from_str::<StreamBoundPlan>(r#"{"expr":{"constant":{"string":"many"}}}"#)
            .is_err()
    );
    assert!(serde_json::from_str::<StreamBoundPlan>(r#"{"expr":{"param":""}}"#).is_err());
    assert!(serde_json::from_str::<StreamBoundPlan>("[]").is_err());
}

#[test]
fn stream_range_plan_separates_literal_and_dynamic_bounds() {
    let literal = StreamRangePlan::new(StreamBound::Literal(2), StreamBound::Literal(8)).unwrap();
    let StreamRangePlan::Literal(bounds) = &literal else {
        panic!("expected literal range");
    };
    assert_eq!(bounds, &StreamLiteralRange::new(2, 8).unwrap());
    assert!(StreamLiteralRange::new(8, 2).is_none());
    assert_eq!(
        serde_json::to_string(&literal).unwrap(),
        r#"{"literal":{"start":2,"end":8}}"#
    );
    let parsed_literal: StreamRangePlan =
        serde_json::from_str(&serde_json::to_string(&literal).unwrap()).unwrap();
    assert_eq!(parsed_literal, literal);
    assert!(serde_json::from_str::<StreamRangePlan>(r#"{"literal":{"start":8,"end":2}}"#).is_err());
    assert!(serde_json::from_str::<StreamLiteralRange>(r#"{"start":"bad","end":2}"#).is_err());

    assert_eq!(
        StreamRangePlan::new(StreamBound::Literal(8), StreamBound::Literal(2)).unwrap_err(),
        StreamRangePlanError::InvertedLiteralRange { start: 8, end: 2 }
    );

    let dynamic = StreamRangePlan::new(
        StreamBound::expr(Expr::param("start")),
        StreamBound::Literal(8),
    )
    .unwrap();
    let StreamRangePlan::Dynamic(bounds) = &dynamic else {
        panic!("expected dynamic range");
    };
    assert_eq!(
        bounds,
        &StreamDynamicRange::from_dynamic_start(
            StreamBoundExprPlan::new(Expr::param("start")).unwrap(),
            StreamBoundPlan::Literal(8),
        )
    );
    let serialized = serde_json::to_string(&dynamic).unwrap();
    let parsed_dynamic: StreamRangePlan = serde_json::from_str(&serialized).unwrap();
    assert_eq!(parsed_dynamic, dynamic);

    let dynamic_end = StreamRangePlan::new(
        StreamBound::Literal(2),
        StreamBound::expr(Expr::param("end")),
    )
    .unwrap();
    let StreamRangePlan::Dynamic(bounds) = &dynamic_end else {
        panic!("expected dynamic range");
    };
    assert_eq!(
        bounds,
        &StreamDynamicRange::from_dynamic_end(
            StreamBoundPlan::Literal(2),
            StreamBoundExprPlan::new(Expr::param("end")).unwrap(),
        )
    );
    assert!(
        StreamDynamicRange::new(StreamBoundPlan::Literal(2), StreamBoundPlan::Literal(8)).is_none()
    );

    assert!(serde_json::from_str::<StreamRangePlan>(
        r#"{"dynamic":{"start":{"literal":2},"end":{"literal":8}}}"#
    )
    .is_err());
    assert!(
        serde_json::from_str::<StreamDynamicRange>(r#"{"start":[],"end":{"literal":8}}"#).is_err()
    );

    assert_eq!(
        StreamRangePlan::new(
            StreamBound::Literal(2),
            StreamBound::expr(Expr::param(String::new())),
        )
        .unwrap_err(),
        StreamRangePlanError::Bound(StreamBoundPlanError::Expression(ExprPlanError::EmptyName {
            field: NameField::Param
        },))
    );
}

#[test]
fn search_limit_plan_requires_positive_literals_or_valid_expressions() {
    let literal = SearchLimitPlan::new(StreamBound::Literal(4)).unwrap();
    assert_eq!(literal, StreamBound::Literal(4));
    assert_ne!(literal, StreamBound::expr(Expr::param("limit")));
    assert_eq!(serde_json::to_string(&literal).unwrap(), r#"{"literal":4}"#);
    assert_eq!(
        SearchLimitPlan::new(StreamBound::Literal(0)).unwrap_err(),
        SearchLimitPlanError::NonPositiveLiteral { actual: 0 }
    );
    assert_eq!(
        SearchLimitPlan::new(StreamBound::expr(Expr::val(4))).unwrap(),
        StreamBound::Literal(4)
    );
    assert_eq!(
        SearchLimitPlan::new(StreamBound::expr(Expr::val(0))).unwrap_err(),
        SearchLimitPlanError::NonPositiveLiteral { actual: 0 }
    );
    assert_eq!(
        SearchLimitPlan::new(StreamBound::expr(Expr::val(-1))).unwrap_err(),
        SearchLimitPlanError::StaticLiteral {
            expected: SearchLimitExpected::PositiveInteger
        }
    );
    assert_eq!(
        SearchLimitPlan::new(StreamBound::expr(Expr::val("nope"))).unwrap_err(),
        SearchLimitPlanError::StaticLiteral {
            expected: SearchLimitExpected::PositiveInteger
        }
    );
    assert_eq!(
        SearchLimitExprPlan::new(Expr::val(1)).unwrap_err(),
        SearchLimitExprPlanError::StaticLiteral {
            expected: SearchLimitExpected::PositiveInteger
        }
    );
    assert_eq!(
        SearchLimitExpected::PositiveInteger.to_string(),
        "positive integer"
    );
    assert!(serde_json::from_str::<SearchLimitPlan>(r#"{"literal":0}"#).is_err());
    assert!(serde_json::from_str::<SearchLimitPlan>("[]").is_err());

    let expression = SearchLimitPlan::new(StreamBound::expr(Expr::param("limit"))).unwrap();
    assert_eq!(expression, StreamBound::expr(Expr::param("limit")));
    assert_ne!(expression, StreamBound::Literal(4));

    let serialized = serde_json::to_string(&expression).unwrap();
    let parsed: SearchLimitPlan = serde_json::from_str(&serialized).unwrap();
    assert_eq!(parsed, expression);

    assert_eq!(
        SearchLimitPlan::new(StreamBound::expr(Expr::param(String::new()))).unwrap_err(),
        SearchLimitPlanError::Expression(ExprPlanError::EmptyName {
            field: NameField::Param
        })
    );
    assert!(serde_json::from_str::<SearchLimitPlan>(r#"{"expr":{"constant":{"i64":1}}}"#).is_err());
    assert!(serde_json::from_str::<SearchLimitPlan>(r#"{"expr":{"param":""}}"#).is_err());
    assert!(serde_json::from_str::<SearchLimitExprPlan>("[]").is_err());
}
