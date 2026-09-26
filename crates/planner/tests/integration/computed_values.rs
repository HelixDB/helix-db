use helix_planner::relational as r;
use std::collections::BTreeMap;

struct Graph(r::GraphProperties);
impl r::GraphValues for Graph {
    fn properties(&self, _: r::Entity) -> r::Result<&r::GraphProperties> {
        Ok(&self.0)
    }
    fn label(&self, _: r::Entity) -> r::Result<Option<&str>> {
        Ok(Some("N"))
    }
}

fn nested(depth: usize, map: bool) -> r::Value {
    (0..depth).fold(r::Value::Integer(7), |value, _| {
        if map {
            r::Value::Map(BTreeMap::from([("child".into(), value)]))
        } else {
            r::Value::List(vec![value])
        }
    })
}

fn depth_error(error: r::QueryError) {
    assert_eq!(error.category, "ResourceLimit");
    assert_eq!(error.detail, "ValueDepth");
    assert_eq!(error.phase, r::ErrorPhase::Runtime);
}

#[test]
fn computed_containers_enforce_depth_across_independent_evaluations() {
    use r::{Expression as E, Value as V};
    let graph = Graph(BTreeMap::new());
    let parameters = BTreeMap::new();
    for map in [false, true] {
        let wrapper = if map {
            E::Map(vec![("child".into(), E::Slot(r::Slot(0)))])
        } else {
            E::List(vec![E::Slot(r::Slot(0))])
        };
        let mut row = vec![V::Integer(7)];
        for depth in 1..=r::MAX_EXPRESSION_DEPTH {
            let evaluation = r::Evaluation {
                row: &row,
                parameters: &parameters,
                graph: &graph,
                group: None,
                max_collection_items: usize::MAX,
                memory: r::EvaluationMemory::new(usize::MAX),
            };
            let result = evaluation.eval(&wrapper);
            if depth == r::MAX_EXPRESSION_DEPTH {
                depth_error(result.unwrap_err());
            } else {
                row[0] = result.unwrap();
                assert_eq!(row[0], nested(depth, map));
            }
        }
    }
}

#[test]
fn borrowed_values_are_checked_before_use_and_short_circuits_stay_lazy() {
    use r::{Expression as E, Function as F, Value as V};
    for map in [false, true] {
        for depth in [47, 48, 96] {
            let value = nested(depth, map);
            let row = [value.clone()];
            let parameters = BTreeMap::from([("x".into(), value.clone())]);
            let graph = Graph(BTreeMap::from([("x".into(), Ok(value.clone()))]));
            let evaluation = r::Evaluation {
                row: &row,
                parameters: &parameters,
                graph: &graph,
                group: None,
                max_collection_items: usize::MAX,
                memory: r::EvaluationMemory::new(usize::MAX),
            };
            for expression in [
                E::Literal(value.clone()),
                E::Slot(r::Slot(0)),
                E::Parameter("x".into()),
                E::Property(
                    Box::new(E::Literal(V::Entity(r::Entity::Node(1)))),
                    "x".into(),
                ),
                E::Index(
                    Box::new(E::Literal(V::Entity(r::Entity::Node(1)))),
                    Box::new(E::Literal(V::String("x".into()))),
                ),
            ] {
                let result = evaluation.eval(&expression);
                if depth < 48 {
                    assert_eq!(result.unwrap(), value);
                } else {
                    depth_error(result.unwrap_err());
                }
                for (short_circuit, expected) in [
                    (
                        E::Case {
                            branches: vec![(E::Literal(V::Boolean(false)), expression.clone())],
                            otherwise: Box::new(E::Literal(V::Integer(17))),
                        },
                        V::Integer(17),
                    ),
                    (
                        E::Binary(
                            r::Binary::And,
                            Box::new(E::Literal(V::Boolean(false))),
                            Box::new(expression.clone()),
                        ),
                        V::Boolean(false),
                    ),
                    (
                        E::Binary(
                            r::Binary::Or,
                            Box::new(E::Literal(V::Boolean(true))),
                            Box::new(expression.clone()),
                        ),
                        V::Boolean(true),
                    ),
                    (
                        E::Function(F::Coalesce, vec![E::Literal(V::Integer(17)), expression]),
                        V::Integer(17),
                    ),
                ] {
                    assert_eq!(evaluation.eval(&short_circuit).unwrap(), expected);
                }
            }
            // Materializing a property map adds a level even when each field
            // is valid alone. Keys do not need to inspect field values.
            depth_error(evaluation.properties(r::Entity::Node(1)).unwrap_err());
            depth_error(
                evaluation
                    .eval(&E::Function(
                        F::Properties,
                        vec![E::Literal(V::Entity(r::Entity::Node(1)))],
                    ))
                    .unwrap_err(),
            );
            assert_eq!(
                evaluation
                    .eval(&E::Function(
                        F::Keys,
                        vec![E::Literal(V::Entity(r::Entity::Node(1)))]
                    ))
                    .unwrap(),
                V::List(vec![V::String("x".into())])
            );
        }
    }
}

#[test]
fn collect_rejection_preserves_state_and_grouping_does_not_add_user_depth() {
    use r::{Aggregate as A, Value as V};
    for map in [false, true] {
        let deepest = nested(47, map);
        let key = r::GroupingKey::row(vec![deepest.clone(), V::Null]).unwrap();
        assert_eq!(
            key,
            r::GroupingKey::row(vec![deepest.clone(), V::Null]).unwrap()
        );
        depth_error(r::GroupingKey::new(V::List(vec![deepest.clone()])).unwrap_err());
        depth_error(r::GroupingKey::row(vec![nested(48, map)]).unwrap_err());
        for distinct in [false, true] {
            for function in [A::Count, A::Min, A::Max] {
                let mut accumulator = r::Accumulator::new(function, distinct);
                accumulator.push(deepest.clone(), 10, usize::MAX).unwrap();
                assert_eq!(
                    accumulator.finish().unwrap(),
                    if function == A::Count {
                        V::Integer(1)
                    } else {
                        deepest.clone()
                    }
                );
            }
            let mut collect = r::Accumulator::new(A::Collect, distinct);
            let shallow = nested(46, map);
            collect.push(shallow.clone(), 10, usize::MAX).unwrap();
            let before = collect.allocated_bytes();
            depth_error(collect.push(deepest.clone(), 10, usize::MAX).unwrap_err());
            assert_eq!(before, collect.allocated_bytes());
            collect.push(V::Integer(9), 10, usize::MAX).unwrap();
            assert_eq!(
                collect.finish().unwrap(),
                V::List(vec![shallow, V::Integer(9)])
            );
        }
    }
}

#[test]
fn runtime_depth_does_not_apply_the_literal_cardinality_limit() {
    let wide = r::Value::List(vec![r::Value::Null; 200_000]);
    wide.validate_depth().unwrap();
    let error = wide.validate_shape().unwrap_err();
    assert_eq!(error.phase, r::ErrorPhase::Compile);
    depth_error(r::GroupingKey::new(wide.clone()).unwrap_err());
    depth_error(r::GroupingKey::row(vec![wide.clone()]).unwrap_err());
    depth_error(
        r::Accumulator::new(r::Aggregate::Count, false)
            .push(wide.clone(), usize::MAX, usize::MAX)
            .unwrap_err(),
    );
    let graph = Graph(BTreeMap::new());
    let row = [wide.clone()];
    let parameters = BTreeMap::new();
    let evaluation = r::Evaluation {
        row: &row,
        parameters: &parameters,
        graph: &graph,
        group: None,
        max_collection_items: usize::MAX,
        memory: r::EvaluationMemory::new(usize::MAX),
    };
    assert_eq!(
        evaluation.eval(&r::Expression::Slot(r::Slot(0))).unwrap(),
        wide
    );
    r::GroupingKey::row(vec![r::Value::List(vec![r::Value::Null; 199_998])]).unwrap();
    depth_error(
        r::GroupingKey::row(vec![r::Value::List(vec![r::Value::Null; 199_999])]).unwrap_err(),
    );
}

#[test]
fn averages_use_bounded_numeric_state_without_overflowing_the_sum() {
    use helix_planner::relational as r;
    for distinct in [false, true] {
        for (values, expected) in [
            (
                vec![r::Value::Integer(i64::MAX); 2],
                r::Value::Float(i64::MAX as f64),
            ),
            (
                vec![r::Value::Integer(i64::MIN); 2],
                r::Value::Float(i64::MIN as f64),
            ),
            (vec![r::Value::Float(1e308); 2], r::Value::Float(1e308)),
            (
                vec![
                    r::Value::Integer(i64::MAX),
                    r::Value::Float(1.0),
                    r::Value::Integer(-i64::MAX),
                ],
                r::Value::Float(1.0 / 3.0),
            ),
            (vec![r::Value::Null; 3], r::Value::Null),
        ] {
            let mut accumulator = r::Accumulator::new(r::Aggregate::Avg, distinct);
            for value in values {
                accumulator.push(value, 10, 1024 * 1024).unwrap();
            }
            assert_eq!(accumulator.finish().unwrap(), expected);
        }
    }
    let mut average = r::Accumulator::new(r::Aggregate::Avg, true);
    average.push(r::Value::Integer(1), 10, 1024 * 1024).unwrap();
    let before = average.allocated_bytes();
    let error = average
        .push_with_admission::<r::QueryError>(r::Value::Integer(3), 10, 1024 * 1024, |_| {
            Err(r::QueryError::runtime(
                "ResourceLimit",
                "MemoryLimit",
                "test admission rejection",
            ))
        })
        .unwrap_err();
    assert_eq!(error.detail, "MemoryLimit");
    assert_eq!(average.allocated_bytes(), before);
    // Admission failure must not retain the DISTINCT key or numeric transition.
    average.push(r::Value::Integer(3), 10, 1024 * 1024).unwrap();
    average.push(r::Value::Float(3.0), 10, 1024 * 1024).unwrap();
    average.push(r::Value::Null, 10, 1024 * 1024).unwrap();
    assert_eq!(average.finish().unwrap(), r::Value::Float(2.0));
    let mut sum = r::Accumulator::new(r::Aggregate::Sum, false);
    sum.push(r::Value::Integer(i64::MAX), 10, 1024 * 1024)
        .unwrap();
    assert_eq!(
        sum.push(r::Value::Integer(1), 10, 1024 * 1024)
            .unwrap_err()
            .detail,
        "NumberOutOfRange"
    );
    assert_eq!(sum.finish().unwrap(), r::Value::Integer(i64::MAX));
}

#[test]
fn grouped_scalar_average_shares_numeric_and_null_semantics() {
    let graph = Graph(BTreeMap::new());
    let parameters = BTreeMap::new();
    for (values, expected) in [
        (
            vec![r::Value::Integer(i64::MAX); 2],
            r::Value::Float(i64::MAX as f64),
        ),
        (vec![r::Value::Float(1e308); 2], r::Value::Float(1e308)),
        (vec![r::Value::Null; 3], r::Value::Null),
        (vec![], r::Value::Null),
    ] {
        let rows = values
            .into_iter()
            .map(|value| vec![value])
            .collect::<Vec<_>>();
        let evaluation = r::Evaluation {
            row: &[],
            parameters: &parameters,
            graph: &graph,
            group: Some(&rows),
            max_collection_items: 10,
            memory: r::EvaluationMemory::new(4096),
        };
        let expression = r::Expression::Aggregate {
            function: r::Aggregate::Avg,
            argument: Some(Box::new(r::Expression::Slot(r::Slot(0)))),
            distinct: false,
        };
        assert_eq!(evaluation.eval(&expression).unwrap(), expected);
    }
}
