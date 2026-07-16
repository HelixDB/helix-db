use super::*;

#[test]
fn stream_bound_eval_accepts_literal_constant_and_runtime_parameters() {
    let literal = ir::StreamBoundPlan::Literal(4);
    let static_param = name("static_limit");
    let dynamic_param = name("dynamic_limit");
    let static_bound =
        ir::StreamBoundPlan::new(StreamBound::expr(Expr::param(static_param.as_ref())))
            .expect("valid static parameter bound");
    let dynamic_bound =
        ir::StreamBoundPlan::new(StreamBound::expr(Expr::param(dynamic_param.as_ref())))
            .expect("valid dynamic parameter bound");
    let constant_bound =
        ir::StreamBoundPlan::new(StreamBound::expr(Expr::val(AstPropertyValue::I64(6))))
            .expect("valid constant bound");
    let params = context::ParamBindings::default()
        .with_value(static_param, AstPropertyValue::I64(5))
        .with_query_value(dynamic_param, QueryValue::I64(7));

    assert_eq!(bound_eval::eval_stream_bound(&literal, &params).unwrap(), 4);
    assert_eq!(
        bound_eval::eval_bound_expr(&Expr::val(AstPropertyValue::I64(8)), &params).unwrap(),
        8
    );
    assert_eq!(
        bound_eval::eval_stream_bound(&static_bound, &params).unwrap(),
        5
    );
    assert_eq!(
        bound_eval::eval_stream_bound(&constant_bound, &params).unwrap(),
        6
    );
    assert_eq!(
        bound_eval::eval_stream_bound(&dynamic_bound, &params).unwrap(),
        7
    );
}

#[test]
fn stream_bound_eval_rejects_invalid_expression_results() {
    let params = context::ParamBindings::default()
        .with_value(name("negative"), AstPropertyValue::I64(-1))
        .with_value(
            name("text"),
            AstPropertyValue::String("not-a-bound".to_string()),
        );
    let negative = ir::StreamBoundPlan::new(StreamBound::expr(Expr::param("negative")))
        .expect("runtime parameter bound expression is syntactically valid");
    let non_i64 = ir::StreamBoundPlan::new(StreamBound::expr(Expr::param("text")))
        .expect("parameter bound expression is syntactically valid");
    let unsupported = ir::StreamBoundPlan::new(StreamBound::expr(Expr::id()))
        .expect("unsupported bound expression is syntactically valid");

    assert!(
        error_message(bound_eval::eval_stream_bound(&negative, &params))
            .contains("stream bound expression returned -1")
    );
    assert!(
        error_message(bound_eval::eval_stream_bound(&non_i64, &params))
            .contains("parameter `text` is not an i64")
    );
    assert!(error_message(bound_eval::eval_bound_expr(
        &Expr::Param(String::new()),
        &params
    ))
    .contains("stream bound parameter name must not be empty"));
    assert!(
        error_message(bound_eval::eval_stream_bound(&unsupported, &params))
            .contains("unsupported stream bound expression")
    );
}
