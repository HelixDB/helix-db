use helix_planner::relational as r;

#[test]
fn windows_validate_offsets_in_order_and_bound_retention() {
    for (skip, limit, expected) in [
        (None, None, (0, usize::MAX, usize::MAX)),
        (Some(2), None, (2, usize::MAX, usize::MAX)),
        (None, Some(3), (0, 3, 3)),
        (Some(2), Some(3), (2, 3, 5)),
        (Some(100), Some(0), (100, 0, 0)),
        (Some(0), Some(0), (0, 0, 0)),
    ] {
        let skip = skip.map(|n| r::Expression::Literal(r::Value::Integer(n)));
        let limit = limit.map(|n| r::Expression::Literal(r::Value::Integer(n)));
        let mut visited = Vec::new();
        let window = r::Window::evaluate(skip.as_ref(), limit.as_ref(), |expression| {
            visited.push(expression.clone());
            let r::Expression::Literal(value) = expression else {
                unreachable!()
            };
            Ok(value.clone())
        })
        .unwrap();
        assert_eq!(visited, skip.into_iter().chain(limit).collect::<Vec<_>>());
        assert_eq!(
            (window.skip(), window.limit(), window.retained_rows()),
            expected
        );
    }
    // The public expressions carry i64, so adding two maximum valid offsets
    // must neither wrap nor introduce an evaluation overflow.
    let max = i64::try_from(usize::MAX / 2).unwrap();
    let expression = r::Expression::Literal(r::Value::Integer(max));
    let window = r::Window::evaluate(Some(&expression), Some(&expression), |_| {
        Ok(r::Value::Integer(max))
    })
    .unwrap();
    assert_eq!(window.retained_rows(), (max as usize).saturating_mul(2));
}

#[test]
fn invalid_offsets_and_expression_errors_are_not_hidden_by_empty_windows() {
    let skip = r::Expression::Parameter("skip".into());
    let limit = r::Expression::Parameter("limit".into());
    for (invalid, detail) in [
        (r::Value::Null, "InvalidArgumentType"),
        (r::Value::Integer(-1), "NegativeIntegerArgument"),
        (r::Value::Float(1.0), "InvalidArgumentType"),
        (r::Value::Boolean(false), "InvalidArgumentType"),
        (r::Value::String("1".into()), "InvalidArgumentType"),
    ] {
        for invalid_skip in [false, true] {
            let mut visited = Vec::new();
            let error = r::Window::evaluate(Some(&skip), Some(&limit), |expression| {
                visited.push(expression.clone());
                Ok(if (expression == &skip) == invalid_skip {
                    invalid.clone()
                } else {
                    r::Value::Integer(0)
                })
            })
            .unwrap_err();
            assert_eq!(error.category, "SyntaxError");
            assert_eq!(error.detail, detail);
            assert_eq!(error.phase, r::ErrorPhase::Runtime);
            assert_eq!(visited.len(), if invalid_skip { 1 } else { 2 });
        }
    }
    let expected = r::QueryError::runtime("ParameterMissing", "MissingParameter", "missing");
    for fails_first in [false, true] {
        let error = r::Window::evaluate(Some(&skip), Some(&limit), |expression| {
            if (expression == &skip) == fails_first {
                Err(expected.clone())
            } else {
                Ok(r::Value::Integer(0))
            }
        })
        .unwrap_err();
        assert_eq!(error, expected);
    }
}
