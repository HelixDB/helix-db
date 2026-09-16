use super::*;

#[test]
fn extraction_rejects_unbound_group_dependencies_and_invalid_aggregate_inputs() {
    for dependency in [
        r::Expression::Slot(r::Slot(0)),
        r::Expression::HasLabel(r::Slot(0), "N".into()),
    ] {
        let mut bindings = vec![r::Binding {
            name: "result".into(),
            kind: r::BindingType::Scalar,
            nullable: true,
            value_type: r::ValueType::Any,
        }];
        let error = split_projection(
            vec![r::Projection {
                slot: r::Slot(0),
                expression: r::Expression::List(vec![
                    dependency,
                    r::Expression::Aggregate {
                        function: r::Aggregate::Count,
                        argument: None,
                        distinct: false,
                    },
                ]),
            }],
            &mut bindings,
        )
        .unwrap_err();
        assert_eq!(error.category, "SyntaxError");
        assert_eq!(error.detail, "AmbiguousAggregationExpression");
        assert_eq!(bindings.len(), 1);
    }
    let mut bindings = Vec::new();
    let error = split_projection(
        vec![r::Projection {
            slot: r::Slot(0),
            expression: r::Expression::List(vec![r::Expression::Aggregate {
                function: r::Aggregate::Count,
                argument: Some(Box::new(r::Expression::Slot(r::Slot(99)))),
                distinct: false,
            }]),
        }],
        &mut bindings,
    )
    .unwrap_err();
    assert_eq!(error.category, "InternalPlannerError");
    assert_eq!(error.detail, "InvalidSlot");
    assert!(bindings.is_empty());
}

#[test]
fn mixed_projection_splits_into_valid_common_contracts_without_public_columns() {
    let mut bindings = vec![
        r::Binding {
            name: "x".into(),
            kind: r::BindingType::Scalar,
            nullable: false,
            value_type: r::ValueType::Integer,
        },
        r::Binding {
            name: "g".into(),
            kind: r::BindingType::Scalar,
            nullable: true,
            value_type: r::ValueType::Integer,
        },
        r::Binding {
            name: "n".into(),
            kind: r::BindingType::Scalar,
            nullable: true,
            value_type: r::ValueType::Integer,
        },
        r::Binding {
            name: "again".into(),
            kind: r::BindingType::Scalar,
            nullable: true,
            value_type: r::ValueType::Integer,
        },
    ];
    let count = r::Expression::Aggregate {
        function: r::Aggregate::Count,
        argument: None,
        distinct: false,
    };
    let (aggregate, project) = split_projection(
        vec![
            r::Projection {
                slot: r::Slot(1),
                expression: r::Expression::Slot(r::Slot(0)),
            },
            r::Projection {
                slot: r::Slot(2),
                expression: r::Expression::Binary(
                    r::Binary::Add,
                    Box::new(r::Expression::Slot(r::Slot(0))),
                    Box::new(count.clone()),
                ),
            },
            r::Projection {
                slot: r::Slot(3),
                expression: count,
            },
        ],
        &mut bindings,
    )
    .unwrap();
    assert_eq!(bindings.len(), 5);
    assert_eq!(aggregate.len(), 2);
    assert_eq!(project.len(), 3);
    assert_eq!(project[2].expression, r::Expression::Slot(r::Slot(4)));
    assert!(project.iter().all(|item| !item.expression.has_aggregate()));
    let stage = |items| r::Operator::Project {
        items,
        distinct: false,
        ordering: vec![],
        predicate: None,
        skip: None,
        limit: None,
    };
    let query = r::Query::new(
        bindings,
        vec![
            r::Operator::Unwind {
                expression: r::Expression::Literal(r::Value::List(vec![
                    r::Value::Integer(1),
                    r::Value::Integer(1),
                    r::Value::Integer(2),
                ])),
                slot: r::Slot(0),
            },
            stage(aggregate),
            stage(project),
        ],
        vec![
            ("g".into(), r::Slot(1)),
            ("n".into(), r::Slot(2)),
            ("again".into(), r::Slot(3)),
        ],
    )
    .unwrap();
    assert_eq!(query.returns().len(), 3);
    assert_eq!(query.contracts()[2].output().columns().len(), 3);
    assert!(!query.contracts()[2]
        .output()
        .columns()
        .contains_key(&r::Slot(4)));
}

#[test]
fn aggregate_states_preserve_exact_float_literal_identity() {
    let mut bindings = (0..2)
        .map(|index| r::Binding {
            name: format!("n{index}"),
            kind: r::BindingType::Scalar,
            nullable: true,
            value_type: r::ValueType::Float,
        })
        .collect();
    let projections = [0.0, -0.0]
        .into_iter()
        .enumerate()
        .map(|(index, value)| r::Projection {
            slot: r::Slot(index as u32),
            expression: r::Expression::Aggregate {
                function: r::Aggregate::Min,
                argument: Some(Box::new(r::Expression::Literal(r::Value::Float(value)))),
                distinct: false,
            },
        })
        .collect();
    let (aggregate, project) = split_projection(projections, &mut bindings).unwrap();
    assert_eq!(
        aggregate.len(),
        2,
        "distinct planner float bits must not share one aggregate state"
    );
    assert_ne!(project[0].expression, project[1].expression);
}

#[test]
fn forced_digest_collisions_preserve_nested_bits_and_distinct_payloads() {
    use std::collections::BTreeMap;
    let cases = [
        (r::Value::Float(0.0), r::Value::Float(-0.0), 2),
        (
            r::Value::Float(f64::from_bits(0x7ff8_0000_0000_0001)),
            r::Value::Float(f64::from_bits(0x7ff8_0000_0000_0002)),
            2,
        ),
        (
            r::Value::List(vec![r::Value::Float(0.0)]),
            r::Value::List(vec![r::Value::Float(-0.0)]),
            2,
        ),
        (
            r::Value::Map(BTreeMap::from([("x".into(), r::Value::Float(0.0))])),
            r::Value::Map(BTreeMap::from([("x".into(), r::Value::Float(-0.0))])),
            2,
        ),
        (
            r::Value::String("a".into()),
            r::Value::String("b".into()),
            2,
        ),
        (
            r::Value::String("a".into()),
            r::Value::String("a".into()),
            1,
        ),
        (r::Value::Integer(1), r::Value::Boolean(true), 2),
        (r::Value::Null, r::Value::Null, 1),
        (
            r::Value::Entity(r::Entity::Node(1)),
            r::Value::Entity(r::Entity::Node(2)),
            2,
        ),
        (
            r::Value::Path(r::Path::new(vec![1], vec![]).unwrap()),
            r::Value::Path(r::Path::new(vec![2], vec![]).unwrap()),
            2,
        ),
    ];
    for (left, right, expected) in cases {
        let mut bindings = (0..2)
            .map(|index| r::Binding {
                name: format!("n{index}"),
                kind: r::BindingType::Scalar,
                nullable: true,
                value_type: r::ValueType::Any,
            })
            .collect();
        let projections = [left, right]
            .into_iter()
            .enumerate()
            .map(|(index, value)| r::Projection {
                slot: r::Slot(index as u32),
                expression: r::Expression::Aggregate {
                    function: r::Aggregate::Min,
                    argument: Some(Box::new(r::Expression::Literal(value))),
                    distinct: false,
                },
            })
            .collect();
        let (aggregate, _) = split_projection_with(projections, &mut bindings, |expression| {
            let (_, bits) = aggregate_identity(expression)?;
            Ok((helix_planner::digest::PlanDigest::from_u64(0), bits))
        })
        .unwrap();
        assert_eq!(aggregate.len(), expected);
    }
}

#[test]
fn aggregate_indexing_preserves_large_literal_ownership() {
    let payload = "x".repeat(1024 * 1024);
    let pointer = payload.as_ptr();
    let mut bindings = vec![r::Binding {
        name: "n".into(),
        kind: r::BindingType::Scalar,
        nullable: true,
        value_type: r::ValueType::Integer,
    }];
    let projections = vec![r::Projection {
        slot: r::Slot(0),
        expression: r::Expression::Binary(
            r::Binary::Add,
            Box::new(r::Expression::Aggregate {
                function: r::Aggregate::Count,
                argument: Some(Box::new(r::Expression::Literal(r::Value::String(payload)))),
                distinct: false,
            }),
            Box::new(r::Expression::Literal(r::Value::Integer(1))),
        ),
    }];
    let (aggregate, project) = split_projection(projections, &mut bindings).unwrap();
    assert!(!project[0].expression.has_aggregate());
    let r::Expression::Aggregate {
        argument: Some(argument),
        ..
    } = &aggregate[0].expression
    else {
        panic!("aggregate")
    };
    let r::Expression::Literal(r::Value::String(value)) = argument.as_ref() else {
        panic!("literal")
    };
    assert_eq!(value.as_ptr(), pointer);
}

#[test]
fn hidden_aggregation_bindings_respect_the_existing_limit() {
    let mut bindings = vec![
        r::Binding {
            name: String::new(),
            kind: r::BindingType::Scalar,
            nullable: true,
            value_type: r::ValueType::Any
        };
        4096
    ];
    let projections = vec![r::Projection {
        slot: r::Slot(0),
        expression: r::Expression::Binary(
            r::Binary::Add,
            Box::new(r::Expression::Aggregate {
                function: r::Aggregate::Count,
                argument: None,
                distinct: false,
            }),
            Box::new(r::Expression::Literal(r::Value::Integer(1))),
        ),
    }];
    let error = split_projection(projections, &mut bindings).unwrap_err();
    assert_eq!(error.phase, r::ErrorPhase::Compile);
    assert_eq!(error.category, "ResourceLimit");
    assert_eq!(error.detail, "TooManyBindings");
    assert_eq!(bindings.len(), 4096);
}

#[test]
fn forced_collisions_bound_work_and_reuse_the_last_admitted_state() {
    let make = |values: Vec<i64>| {
        let mut bindings = values
            .iter()
            .enumerate()
            .map(|(index, _)| r::Binding {
                name: format!("n{index}"),
                kind: r::BindingType::Scalar,
                nullable: true,
                value_type: r::ValueType::Integer,
            })
            .collect::<Vec<_>>();
        let count = bindings.len();
        let projections = values
            .into_iter()
            .enumerate()
            .map(|(index, value)| r::Projection {
                slot: r::Slot(index as u32),
                expression: r::Expression::Aggregate {
                    function: r::Aggregate::Min,
                    argument: Some(Box::new(r::Expression::Literal(r::Value::Integer(value)))),
                    distinct: false,
                },
            })
            .collect();
        let result = split_projection_with(projections, &mut bindings, |expression| {
            let (_, bits) = aggregate_identity(expression)?;
            Ok((helix_planner::digest::PlanDigest::from_u64(0), bits))
        });
        assert_eq!(bindings.len(), count);
        result
    };
    let (aggregate, post) = make((0..8).chain([7]).collect()).unwrap();
    assert_eq!(aggregate.len(), 8);
    assert_eq!(post.len(), 9);
    assert_eq!(post[7].expression, post[8].expression);
    let error = make((0..9).collect()).unwrap_err();
    assert_eq!(error.phase, r::ErrorPhase::Compile);
    assert_eq!(error.category, "ResourceLimit");
    assert_eq!(error.detail, "AggregateIdentityBudget");
}

#[test]
fn borrowed_index_preserves_payload_identity_and_first_destination() {
    let values = [
        r::Expression::Literal(r::Value::Float(0.0)),
        r::Expression::Literal(r::Value::Float(-0.0)),
        r::Expression::Literal(r::Value::Null),
        r::Expression::Literal(r::Value::Null),
    ];
    let index = ExpressionIndex::new(
        values
            .iter()
            .enumerate()
            .map(|(index, value)| (value, index)),
    )
    .unwrap();
    assert_eq!(index.find(&values[0]).unwrap(), Some(0));
    assert_eq!(index.find(&values[1]).unwrap(), Some(1));
    assert_eq!(index.find(&values[3]).unwrap(), Some(2));
    assert_eq!(
        index
            .find(&r::Expression::Literal(r::Value::String("absent".into())))
            .unwrap(),
        None
    );
}
#[test]
fn forced_collision_work_is_bounded_and_collision_is_not_equality() {
    let values: Vec<_> = (0..9)
        .map(|value| r::Expression::Literal(r::Value::Integer(value)))
        .collect();
    let identity = || (helix_planner::digest::PlanDigest::from_u64(0), vec![]);
    let mut index = ExpressionIndex::new(std::iter::empty()).unwrap();
    for (number, value) in values[..8].iter().enumerate() {
        index.insert(value, number, identity()).unwrap();
    }
    index.insert(&values[7], 99, identity()).unwrap();
    assert_eq!(index.find_identity(&values[7], identity()), Some(7));
    assert_eq!(index.find_identity(&values[8], identity()), None);
    let error = index.insert(&values[8], 8, identity()).unwrap_err();
    assert_eq!(error.category, "ResourceLimit");
    assert_eq!(error.detail, "AggregateIdentityBudget");
    assert_eq!(index.buckets.values().next().unwrap().len(), 8);
}
#[test]
fn indexing_preserves_large_literal_lookup_and_checks_depth() {
    let expression = r::Expression::Literal(r::Value::String("x".repeat(1024 * 1024)));
    {
        let index = ExpressionIndex::new(std::iter::once((&expression, ()))).unwrap();
        assert_eq!(index.find(&expression).unwrap(), Some(()));
    }
    let mut deep = r::Expression::Literal(r::Value::Null);
    for _ in 0..48 {
        deep = r::Expression::List(vec![deep]);
    }
    let error = ExpressionIndex::new(std::iter::once((&deep, ())))
        .err()
        .unwrap();
    assert_eq!(error.category, "ResourceLimit");
    assert_eq!(error.detail, "ExpressionDepth");
}
