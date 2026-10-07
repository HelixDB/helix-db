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

fn pipeline(windows: Vec<(Option<r::Expression>, Option<r::Expression>)>) -> r::RowPipeline {
    let operators = std::iter::once(r::Operator::Unwind {
        expression: r::Expression::Literal(r::Value::List(vec![])),
        slot: r::Slot(0),
    })
    .chain(windows.into_iter().map(|(skip, limit)| {
        r::Operator::Project {
            items: r::ProjectionProgram::new(vec![r::Projection {
                slot: r::Slot(0),
                expression: r::Expression::Slot(r::Slot(0)),
            }])
            .unwrap(),
            distinct: false,
            ordering: vec![],
            predicate: None,
            skip,
            limit,
        }
    }))
    .collect();
    let query = r::Query::new(
        vec![r::Binding {
            name: "x".into(),
            kind: r::BindingType::Scalar,
            nullable: true,
            value_type: r::ValueType::Any,
        }],
        operators,
        vec![("x".into(), r::Slot(0))],
    )
    .unwrap();
    r::RowPipeline::new(std::sync::Arc::new(query), r::RowExecution::Batched)
}

#[test]
fn composed_source_demand_preserves_an_independent_sequence_model() {
    let literal = |value| Some(r::Expression::Literal(r::Value::Integer(value)));
    for skip in [0, 3] {
        for limit in [0, 1, 10, 100] {
            for next_skip in [0, 5] {
                for next_limit in [0, 2, 20] {
                    let plan = pipeline(vec![
                        (literal(skip), literal(limit)),
                        (literal(next_skip), literal(next_limit)),
                    ]);
                    let window = plan.input_window(0).unwrap();
                    assert_eq!((window.projection(), window.last_projection()), (1, 2));
                    let demand = window
                        .demand(|expression| {
                            let r::Expression::Literal(value) = expression else {
                                unreachable!()
                            };
                            Ok(value.clone())
                        })
                        .unwrap();
                    let model = |length| {
                        (0..length)
                            .skip(skip as usize)
                            .take(limit as usize)
                            .skip(next_skip as usize)
                            .take(next_limit as usize)
                            .collect::<Vec<_>>()
                    };
                    let expected = model(256);
                    assert_eq!(model(demand), expected);
                    assert!(demand <= (skip + limit).max(1) as usize);
                    if limit == 0 || next_limit == 0 {
                        // UNWIND still validates its initial source expression.
                        assert_eq!(demand, 1);
                    }
                    if !expected.is_empty() {
                        assert_ne!(model(demand - 1), expected);
                    }
                }
            }
        }
    }
    let full = pipeline(vec![(None, literal(1_000_000))]);
    let limited = pipeline(vec![(None, literal(1_000_000)), (literal(2), literal(3))]);
    let context = helix_planner::context::PlannerContext::default();
    assert!(limited.cost(&context.storage).peak_memory < full.cost(&context.storage).peak_memory);
    let empty = pipeline(vec![(None, literal(0))]).cost(&context.storage);
    for windows in [
        vec![(literal(i64::MAX), literal(0))],
        vec![(literal(i64::MAX), None), (literal(i64::MAX), literal(0))],
        vec![
            (literal(i64::MAX), literal(100)),
            (literal(i64::MAX), literal(0)),
        ],
        vec![
            (literal(i64::MAX), literal(0)),
            (literal(i64::MAX), literal(100)),
        ],
    ] {
        assert_eq!(
            pipeline(windows).cost(&context.storage).peak_memory,
            empty.peak_memory
        );
    }
}

#[test]
fn dynamic_windows_keep_evaluation_order_and_rebuild_derived_proofs() {
    let literal = |value| Some(r::Expression::Literal(r::Value::Integer(value)));
    let context = helix_planner::context::PlannerContext::default();
    let unbounded = pipeline(vec![(None, None)]).cost(&context.storage);
    for dynamic in [
        r::Expression::Parameter("later".into()),
        r::Expression::Literal(r::Value::Integer(-1)),
        r::Expression::Literal(r::Value::Float(1.0)),
    ] {
        // Unknown or invalid offsets must not invent a cheaper source bound
        // during planning. Validation remains an execution responsibility.
        for offsets in [
            (Some(dynamic.clone()), literal(100)),
            (None, Some(dynamic.clone())),
        ] {
            let plan = pipeline(vec![offsets, (None, literal(0))]);
            assert_eq!(
                plan.cost(&context.storage).peak_memory,
                unbounded.peak_memory
            );
            // A later empty window cannot hide an earlier offset error.
            let error = plan
                .input_window(0)
                .unwrap()
                .demand(|expression| match expression {
                    r::Expression::Literal(value) => Ok(value.clone()),
                    r::Expression::Parameter(_) => Err(r::QueryError::runtime(
                        "ParameterMissing",
                        "MissingParameter",
                        "missing offset",
                    )),
                    _ => unreachable!(),
                })
                .unwrap_err();
            assert_eq!(error.phase, r::ErrorPhase::Runtime);
            assert_eq!(
                error.detail,
                match &dynamic {
                    r::Expression::Parameter(_) => "MissingParameter",
                    r::Expression::Literal(r::Value::Integer(_)) => "NegativeIntegerArgument",
                    _ => "InvalidArgumentType",
                }
            );
        }
        let plan = pipeline(vec![
            (literal(2), literal(100)),
            (None, literal(80)),
            (Some(dynamic), literal(50)),
            (None, literal(1)),
        ]);
        let window = plan.input_window(0).unwrap();
        assert_eq!(window.last_projection(), 4);
        let mut visited = Vec::new();
        assert_eq!(
            window
                .demand(|expression| {
                    visited.push(expression.clone());
                    let r::Expression::Literal(value) = expression else {
                        panic!("later window evaluated early")
                    };
                    Ok(value.clone())
                })
                .unwrap(),
            82
        );
        assert_eq!(visited, vec![literal(2).unwrap(), literal(100).unwrap()]);
        let mut encoded = serde_json::to_value(&plan).unwrap();
        encoded["input_windows"]["0"]["last_projection"] = serde_json::json!(999);
        encoded["input_windows"]["0"]["downstream_demand"] = serde_json::json!(0);
        assert_eq!(
            serde_json::from_value::<r::RowPipeline>(encoded).unwrap(),
            plan
        );
    }
    assert!(pipeline(vec![(literal(3), None), (None, None)])
        .input_window(0)
        .is_none());
    let max = i64::try_from(usize::MAX / 2).unwrap();
    let saturated = pipeline(vec![
        (literal(max), None),
        (literal(max), literal(max)),
        (literal(max), literal(max)),
    ]);
    assert_eq!(
        saturated
            .input_window(0)
            .unwrap()
            .demand(|expression| {
                let r::Expression::Literal(value) = expression else {
                    unreachable!()
                };
                Ok(value.clone())
            })
            .unwrap(),
        usize::MAX
    );
}
