use super::*;
use crate::execution::interpreter::stream::values;

// Independent reference for the previous native evaluator: resolve the AST
// parameter through the ordinary owned-value conversion before checking i64.
fn legacy_bound(bound: &StreamBound, params: &context::ParamBindings) -> Result<usize> {
    let value = match bound {
        StreamBound::Literal(value) => return Ok(*value),
        StreamBound::Expr(Expr::Constant(AstPropertyValue::I64(value))) => *value,
        StreamBound::Expr(Expr::Param(parameter)) => {
            let parameter = name(parameter);
            values::param_value_from(params, &parameter)?
                .as_i64()
                .ok_or_else(|| {
                    HelixDbError::Query(format!("parameter `{parameter}` is not an i64"))
                })?
        }
        StreamBound::Expr(expression) => {
            return Err(HelixDbError::Query(format!(
                "unsupported stream bound expression {expression:?}"
            )))
        }
    };
    usize::try_from(value)
        .map_err(|_| HelixDbError::Query(format!("stream bound expression returned {value}")))
}

#[test]
fn stream_bounds_preserve_legacy_values_errors_and_parameter_precedence() {
    let mut bindings = vec![context::ParamBindings::default()];
    for value in [i64::MIN, -1, 0, 1, i64::MAX] {
        bindings.extend([
            context::ParamBindings::default().with_value(name("limit"), value),
            context::ParamBindings::default()
                .with_query_value(name("limit"), QueryValue::I64(value)),
        ]);
    }
    for value in [
        AstPropertyValue::Null,
        AstPropertyValue::Bool(true),
        AstPropertyValue::DateTime(3),
        AstPropertyValue::F64(3.0),
        AstPropertyValue::F32(3.0),
        AstPropertyValue::String("3".into()),
        AstPropertyValue::Bytes(vec![3]),
        AstPropertyValue::I64Array(vec![3]),
        AstPropertyValue::F64Array(vec![3.0]),
        AstPropertyValue::F32Array(vec![3.0]),
        AstPropertyValue::StringArray(vec!["3".into()]),
        AstPropertyValue::Array(vec![AstPropertyValue::I64(3)]),
        AstPropertyValue::Object(Default::default()),
    ] {
        // A valid query-value binding must not mask a bad property binding.
        bindings.push(
            context::ParamBindings::default()
                .with_value(name("limit"), value)
                .with_query_value(name("limit"), QueryValue::I64(9)),
        );
    }
    for value in [
        QueryValue::Null,
        QueryValue::Bool(true),
        QueryValue::F64(3.0),
        QueryValue::F32(3.0),
        QueryValue::String("3".into()),
        QueryValue::Array(vec![QueryValue::I64(3)]),
        QueryValue::Object(Default::default()),
    ] {
        bindings.push(context::ParamBindings::default().with_query_value(name("limit"), value));
    }
    bindings.extend([
        context::ParamBindings::default()
            .with_value(name("limit"), 4)
            .with_query_value(name("limit"), QueryValue::I64(9)),
        context::ParamBindings::default()
            .with_value(name("limit"), 4)
            .with_query_value(name("limit"), QueryValue::Null),
    ]);
    for bound in [
        StreamBound::Literal(0),
        StreamBound::Literal(usize::MAX),
        StreamBound::expr(Expr::val(0)),
        StreamBound::expr(Expr::val(17)),
        StreamBound::expr(Expr::param("limit")),
    ] {
        let plan = ir::StreamBoundPlan::new(bound.clone()).unwrap();
        for params in &bindings {
            assert_eq!(
                bound_eval::eval_stream_bound(&plan, params).map_err(|error| error.to_string()),
                legacy_bound(&bound, params).map_err(|error| error.to_string()),
                "bound {bound:?}, bindings {params:?}"
            );
        }
    }
}

#[test]
fn stream_bounds_reject_unavailable_operations_before_evaluating_children() {
    let missing = Expr::param("missing");
    for expression in [
        Expr::prop("limit"),
        Expr::Id,
        Expr::Timestamp,
        Expr::DateTimeNow,
        missing.clone().add_expr(Expr::val(1)),
        missing.clone().sub_expr(Expr::val(1)),
        missing.clone().mul_expr(Expr::val(1)),
        missing.clone().div_expr(Expr::val(0)),
        missing.clone().modulo(Expr::val(0)),
        missing.clone().neg_expr(),
        Expr::case(vec![], Some(missing)),
    ] {
        let raw = StreamBound::expr(expression);
        let plan = ir::StreamBoundPlan::new(raw.clone()).unwrap();
        let params = context::ParamBindings::default();
        assert_eq!(
            error_message(bound_eval::eval_stream_bound(&plan, &params)),
            error_message(legacy_bound(&raw, &params))
        );
    }
}

#[test]
fn stream_bound_parameter_lookup_does_not_copy_names_or_payloads() {
    let bound = ir::StreamBoundPlan::new(StreamBound::expr(Expr::param("limit"))).unwrap();
    let valid = context::ParamBindings::default().with_value(name("limit"), 7);
    let (result, allocations) =
        crate::allocation_testing::observe(|| bound_eval::eval_stream_bound(&bound, &valid));
    assert_eq!(result.unwrap(), 7);
    assert_eq!(allocations.allocations, 0, "borrow the validated parameter");

    for query_parameter in [false, true] {
        let rejection = |len| {
            let values = vec!["payload".repeat(128); len];
            let params = if query_parameter {
                context::ParamBindings::default().with_query_value(
                    name("limit"),
                    QueryValue::Array(values.into_iter().map(QueryValue::String).collect()),
                )
            } else {
                context::ParamBindings::default()
                    .with_value(name("limit"), AstPropertyValue::StringArray(values))
            };
            let (result, allocations) = crate::allocation_testing::observe(|| {
                bound_eval::eval_stream_bound(&bound, &params)
            });
            assert!(error_message(result).contains("parameter `limit` is not an i64"));
            allocations
        };
        let small = rejection(1);
        let large = rejection(1024);
        assert_eq!(large.allocations, small.allocations);
        assert_eq!(
            large.bytes, small.bytes,
            "reject without copying the payload"
        );
    }
}

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
    assert!(matches!(
        ir::StreamBoundPlan::new(StreamBound::expr(Expr::Param(String::new()))),
        Err(ir::StreamBoundPlanError::Expression(
            ir::ExprPlanError::EmptyName { .. }
        ))
    ));
    assert!(
        error_message(bound_eval::eval_stream_bound(&unsupported, &params))
            .contains("unsupported stream bound expression")
    );
}
