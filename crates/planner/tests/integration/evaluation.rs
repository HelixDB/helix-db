use helix_planner::relational as r;
use std::collections::BTreeMap;

struct NoGraph;
impl r::GraphValues for NoGraph {
    fn properties(&self, _: r::Entity) -> r::Result<&r::GraphProperties> {
        panic!("scalar expression attempted graph access")
    }
    fn label(&self, _: r::Entity) -> r::Result<Option<&str>> {
        panic!("scalar expression attempted graph access")
    }
}

#[test]
fn owned_expression_temporaries_preserve_inputs_and_function_arguments() {
    use r::{Expression as E, Function as F, Value as V};
    let nested = V::Map(BTreeMap::from([("key".into(), V::String("value".into()))]));
    let list = V::List(vec![V::Integer(1), nested.clone(), V::Integer(3)]);
    let row = vec![list.clone(), nested.clone()];
    let parameters = BTreeMap::from([("list".into(), list.clone())]);
    let evaluation = r::Evaluation {
        row: &row,
        parameters: &parameters,
        graph: &NoGraph,
        group: None,
        max_collection_items: 100,
        max_value_bytes: 16_384,
    };
    let integer = |n| E::Literal(V::Integer(n));
    for (expression, expected) in [
        (
            E::Index(Box::new(E::Slot(r::Slot(0))), Box::new(integer(-2))),
            nested.clone(),
        ),
        (
            E::Property(Box::new(E::Slot(r::Slot(1))), "key".into()),
            V::String("value".into()),
        ),
        (
            E::Index(
                Box::new(E::Slot(r::Slot(1))),
                Box::new(E::Literal(V::String("key".into()))),
            ),
            V::String("value".into()),
        ),
        (
            E::Slice {
                value: Box::new(E::Parameter("list".into())),
                start: Some(Box::new(integer(1))),
                end: Some(Box::new(integer(-1))),
            },
            V::List(vec![nested.clone()]),
        ),
        (
            E::Function(F::Head, vec![E::Slot(r::Slot(0))]),
            V::Integer(1),
        ),
        (
            E::Function(F::Last, vec![E::Slot(r::Slot(0))]),
            V::Integer(3),
        ),
        (
            E::Function(F::Properties, vec![E::Slot(r::Slot(1))]),
            nested.clone(),
        ),
        (
            E::Function(F::Range, vec![integer(3), integer(1), integer(-1)]),
            V::List(vec![V::Integer(3), V::Integer(2), V::Integer(1)]),
        ),
        (
            E::Function(F::Range, vec![E::Literal(V::Null), integer(1)]),
            V::Null,
        ),
        (
            E::Function(
                F::Substring,
                vec![
                    E::Literal(V::String("aλ猫z".into())),
                    integer(1),
                    integer(2),
                ],
            ),
            V::String("λ猫".into()),
        ),
    ] {
        assert_eq!(evaluation.eval(&expression).unwrap(), expected);
        assert_eq!(row, vec![list.clone(), nested.clone()]);
        assert_eq!(parameters["list"], list);
    }
}

#[test]
fn unwind_range_admits_live_arguments_and_keeps_argument_errors_visible() {
    use r::{Expression as E, Function as F, Value as V};
    let parameters = BTreeMap::from([("large".into(), V::String("x".repeat(700)))]);
    let evaluation = r::Evaluation {
        row: &[],
        parameters: &parameters,
        graph: &NoGraph,
        group: None,
        max_collection_items: 100,
        max_value_bytes: 1200,
    };
    let range = E::Function(
        F::Range,
        vec![E::Parameter("large".into()), E::Parameter("large".into())],
    );
    assert!(evaluation.eval(&E::Parameter("large".into())).is_ok());
    assert!(matches!(evaluation.unwind(&range), Err(error) if error.detail == "MemoryLimit"));
    let null_with_missing = E::Function(
        F::Range,
        vec![E::Literal(V::Null), E::Parameter("missing".into())],
    );
    assert!(
        matches!(evaluation.unwind(&null_with_missing), Err(error) if error.detail == "MissingParameter")
    );
    let too_small = r::Evaluation {
        max_value_bytes: 1,
        ..evaluation
    };
    assert!(matches!(too_small.unwind(&range), Err(error) if error.detail == "MemoryLimit"));
}

#[test]
fn overlapping_index_and_slice_operands_are_admitted_together() {
    use r::{Expression as E, Function as F, Value as V};
    let large = V::String("x".repeat(1024));
    let list = V::List(vec![large.clone()]);
    let row = vec![list.clone()];
    let parameters = BTreeMap::from([("large".into(), large.clone())]);
    let evaluation = r::Evaluation {
        row: &row,
        parameters: &parameters,
        graph: &NoGraph,
        group: None,
        max_collection_items: 100,
        max_value_bytes: 1600,
    };
    let index = E::Function(F::Size, vec![E::Parameter("large".into())]);
    // Either operand fits alone. The index expression still needs its large
    // argument while the list operand is live, even though its result is small.
    assert_eq!(evaluation.eval(&E::Slot(r::Slot(0))).unwrap(), list);
    assert_eq!(evaluation.eval(&index).unwrap(), V::Integer(1024));
    for expression in [
        E::Index(Box::new(E::Slot(r::Slot(0))), Box::new(index.clone())),
        E::Slice {
            value: Box::new(E::Slot(r::Slot(0))),
            start: Some(Box::new(index.clone())),
            end: None,
        },
        E::Slice {
            value: Box::new(E::Slot(r::Slot(0))),
            start: None,
            end: Some(Box::new(index)),
        },
    ] {
        let error = evaluation.eval(&expression).unwrap_err();
        assert_eq!(
            (&*error.category, &*error.detail),
            ("ResourceLimit", "MemoryLimit")
        );
    }
    let small = r::Evaluation {
        max_value_bytes: 512,
        ..evaluation
    };
    for expression in [E::Slot(r::Slot(0)), E::Parameter("large".into())] {
        assert_eq!(small.eval(&expression).unwrap_err().detail, "MemoryLimit");
    }
}

#[test]
fn graph_outputs_and_path_expansion_are_admitted_before_materialization() {
    use r::{Entity, Expression as E, Function as F, Value as V};
    struct Graph {
        properties: r::GraphProperties,
        label: String,
    }
    impl r::GraphValues for Graph {
        fn properties(&self, _: Entity) -> r::Result<&r::GraphProperties> {
            Ok(&self.properties)
        }
        fn label(&self, _: Entity) -> r::Result<Option<&str>> {
            Ok(Some(&self.label))
        }
    }
    let graph = Graph {
        properties: BTreeMap::from([
            ("large".into(), Ok(V::String("x".repeat(2048)))),
            ("small".into(), Ok(V::Integer(7))),
            ("k".repeat(2048), Ok(V::Null)),
        ]),
        label: "λ".repeat(1024),
    };
    let parameters = BTreeMap::new();
    let evaluation = r::Evaluation {
        row: &[],
        parameters: &parameters,
        graph: &graph,
        group: None,
        max_collection_items: 100,
        max_value_bytes: 1024,
    };
    let node = E::Literal(V::Entity(Entity::Node(1)));
    assert_eq!(
        evaluation
            .eval(&E::Property(Box::new(node.clone()), "small".into()))
            .unwrap(),
        V::Integer(7),
    );
    for expression in [
        E::Property(Box::new(node.clone()), "large".into()),
        E::Index(
            Box::new(node.clone()),
            Box::new(E::Literal(V::String("large".into()))),
        ),
        E::Function(F::Properties, vec![node.clone()]),
        E::Function(F::Keys, vec![node.clone()]),
        E::Function(F::Labels, vec![node]),
        E::Function(
            F::Type,
            vec![E::Literal(V::Entity(Entity::Relationship(2)))],
        ),
    ] {
        assert_eq!(
            evaluation.eval(&expression).unwrap_err().detail,
            "MemoryLimit"
        );
    }
    let path = E::Literal(V::Path(
        r::Path::new((0..100).collect(), (100..199).collect()).unwrap(),
    ));
    let evaluation = r::Evaluation {
        max_value_bytes: 5000,
        ..evaluation
    };
    assert!(evaluation.eval(&path).is_ok());
    for function in [F::Nodes, F::Relationships] {
        assert_eq!(
            evaluation
                .eval(&E::Function(function, vec![path.clone()]))
                .unwrap_err()
                .detail,
            "MemoryLimit"
        );
    }
}

#[test]
fn expression_sequences_share_live_memory_and_preserve_error_order() {
    use r::{Expression as E, Value as V};
    let row = vec![V::Integer(7)];
    let parameters = BTreeMap::from([("large".into(), V::String("x".repeat(1024)))]);
    let evaluation = r::Evaluation {
        row: &row,
        parameters: &parameters,
        graph: &NoGraph,
        group: None,
        max_collection_items: 100,
        max_value_bytes: 1600,
    };
    let large = E::Parameter("large".into());
    let missing = E::Parameter("missing".into());
    let slot = E::Slot(r::Slot(0));
    assert!(evaluation.eval(&large).is_ok());
    let result = evaluation
        .eval_sequence([&large, &large, &missing].into_iter())
        .unwrap_err();
    assert_eq!(
        (&*result.category, &*result.detail),
        ("ResourceLimit", "MemoryLimit")
    );
    assert_eq!(
        evaluation
            .eval_sequence([&missing, &large].into_iter())
            .unwrap_err()
            .detail,
        "MissingParameter"
    );
    assert_eq!(
        evaluation
            .eval_sequence([&slot, &large].into_iter())
            .unwrap(),
        vec![V::Integer(7), parameters["large"].clone()]
    );
    assert_eq!(row, vec![V::Integer(7)]);
    assert_eq!(parameters["large"], V::String("x".repeat(1024)));
    let no_memory = r::Evaluation {
        max_value_bytes: 0,
        ..evaluation
    };
    assert!(no_memory
        .eval_sequence(std::iter::empty())
        .unwrap()
        .is_empty());
    assert_eq!(
        no_memory
            .eval_sequence([&slot].into_iter())
            .unwrap_err()
            .detail,
        "MemoryLimit"
    );
    let oversized = std::iter::repeat_n(&slot, usize::MAX);
    assert_eq!(
        evaluation.eval_sequence(oversized).unwrap_err().detail,
        "MemoryLimit"
    );
}

#[test]
fn string_transforms_admit_outputs_before_allocating_them() {
    use r::{Expression as E, Function as F, Value as V};
    let source = "aλ猫Z".repeat(8192);
    let parameters = BTreeMap::from([("value".into(), V::String(source.clone()))]);
    let input = parameters["value"].allocated_bytes();
    let evaluation = r::Evaluation {
        row: &[],
        parameters: &parameters,
        graph: &NoGraph,
        group: None,
        max_collection_items: 100,
        max_value_bytes: source.len() * 3 / 2,
    };
    for function in [F::Trim, F::Ltrim, F::Rtrim, F::Reverse, F::Substring] {
        let mut args = vec![E::Parameter("value".into())];
        if function == F::Substring {
            args.push(E::Literal(V::Integer(0)));
        }
        let input_bytes = input + (args.len() - 1) * size_of::<V>();
        let expression = E::Function(function, args);
        let expected = if function == F::Reverse {
            source.chars().rev().collect::<String>()
        } else {
            source.clone()
        };
        let exact = input_bytes + size_of::<V>() + expected.len();
        for allowance in [evaluation.max_value_bytes, exact - 1, exact] {
            let bounded = r::Evaluation {
                max_value_bytes: allowance,
                ..evaluation
            };
            let (result, allocated) = crate::allocations::observe(|| bounded.eval(&expression));
            if allowance < exact {
                let error = result.unwrap_err();
                assert_eq!(
                    (&*error.category, &*error.detail),
                    ("ResourceLimit", "MemoryLimit"),
                    "{function:?}"
                );
                assert!(
                    allocated.bytes < source.len() + 1024,
                    "{function:?}: rejected output was allocated: {allocated:?}"
                );
            } else {
                assert_eq!(result.unwrap(), V::String(expected.clone()), "{function:?}");
                assert!(
                    allocated.bytes <= exact,
                    "{function:?}: {allocated:?} exceeds {exact}"
                );
            }
        }
        assert_eq!(parameters["value"], V::String(source.clone()));
    }
}

#[test]
fn string_selection_preserves_unicode_boundaries_and_argument_errors() {
    use r::{Expression as E, Function as F, Value as V};
    let parameters = BTreeMap::new();
    let evaluation = r::Evaluation {
        row: &[],
        parameters: &parameters,
        graph: &NoGraph,
        group: None,
        max_collection_items: 100,
        max_value_bytes: 16_384,
    };
    let integer = |n| E::Literal(V::Integer(n));
    for (function, source, trailing, expected) in [
        (F::Trim, "\u{2003} λ猫 \t", vec![], "λ猫"),
        (F::Ltrim, "\u{2003} λ猫 \t", vec![], "λ猫 \t"),
        (F::Rtrim, "\u{2003} λ猫 \t", vec![], "\u{2003} λ猫"),
        (F::Trim, " \u{2003}\t", vec![], ""),
        (F::Reverse, "aλ猫😀", vec![], "😀猫λa"),
        (F::Reverse, "e\u{301}", vec![], "\u{301}e"),
        (F::Reverse, "", vec![], ""),
        (
            F::Substring,
            "aλ猫😀z",
            vec![integer(1), integer(3)],
            "λ猫😀",
        ),
        (F::Substring, "aλ猫😀z", vec![integer(2)], "猫😀z"),
        (F::Substring, "aλ猫😀z", vec![integer(1), integer(0)], ""),
        (F::Substring, "aλ猫😀z", vec![integer(5)], ""),
        (F::Substring, "aλ猫😀z", vec![integer(i64::MAX)], ""),
        (
            F::Substring,
            "aλ猫😀z",
            vec![integer(2), integer(i64::MAX)],
            "猫😀z",
        ),
        (F::Substring, "", vec![integer(0)], ""),
    ] {
        let mut args = vec![E::Literal(V::String(source.into()))];
        args.extend(trailing);
        let expression = E::Function(function, args);
        expression.validate_shape().unwrap();
        assert_eq!(
            evaluation.eval(&expression).unwrap(),
            V::String(expected.into()),
            "{expression:?}"
        );
    }
    for function in [
        F::Trim,
        F::Ltrim,
        F::Rtrim,
        F::Reverse,
        F::Substring,
        F::ToLower,
        F::ToUpper,
    ] {
        for first in [V::Null, V::Boolean(true)] {
            let mut args = vec![E::Literal(first.clone())];
            if function == F::Substring {
                args.push(integer(0));
            }
            let result = evaluation.eval(&E::Function(function, args));
            if first == V::Null {
                assert_eq!(result.unwrap(), V::Null);
            } else {
                assert_eq!(result.unwrap_err().category, "TypeError");
            }
        }
    }
    for offset in [V::Integer(-1), V::Float(1.0), V::Null] {
        for position in [1, 2] {
            let mut args = vec![E::Literal(V::String("λ猫".into())), integer(0), integer(1)];
            args[position] = E::Literal(offset.clone());
            let error = evaluation
                .eval(&E::Function(F::Substring, args))
                .unwrap_err();
            assert_eq!(
                error.detail,
                if offset == V::Integer(-1) {
                    "NegativeIntegerArgument"
                } else {
                    "InvalidArgumentType"
                }
            );
        }
    }
}

#[test]
fn string_scalar_conversions_reuse_admitted_arguments() {
    use r::{Expression as E, Function as F, Value as V};
    let source = "λ猫".repeat(8192);
    let parameters = BTreeMap::from([("value".into(), V::String(source.clone()))]);
    let evaluation = r::Evaluation {
        row: &[],
        parameters: &parameters,
        graph: &NoGraph,
        group: None,
        max_collection_items: 100,
        max_value_bytes: source.len() + 2 * size_of::<V>(),
    };
    for (function, expected) in [
        (F::ToString, V::String(source.clone())),
        (F::ToBoolean, V::Null),
    ] {
        let expression = E::Function(function, vec![E::Parameter("value".into())]);
        let (result, allocated) = crate::allocations::observe(|| evaluation.eval(&expression));
        assert_eq!(result.unwrap(), expected);
        assert!(
            allocated.bytes <= evaluation.max_value_bytes,
            "{function:?} copied its owned string: {allocated:?}"
        );
    }
    for (text, expected) in [
        (" true ", V::Boolean(true)),
        ("\u{2003}FaLsE\t", V::Boolean(false)),
        ("TrUe", V::Boolean(true)),
        ("truе", V::Null),
        ("", V::Null),
    ] {
        let expression = E::Function(F::ToBoolean, vec![E::Literal(V::String(text.into()))]);
        assert_eq!(evaluation.eval(&expression).unwrap(), expected);
    }
    assert_eq!(parameters["value"], V::String(source));
}

#[test]
fn decimal_integer_conversion_preserves_precision_and_checks_exact_bounds() {
    let parameters = BTreeMap::new();
    let evaluation = r::Evaluation {
        row: &[],
        parameters: &parameters,
        graph: &NoGraph,
        group: None,
        max_collection_items: 100,
        max_value_bytes: 64 * 1024,
    };
    for (text, expected) in [
        ("9007199254740993.0", Some(9_007_199_254_740_993)),
        ("-9007199254740993.0", Some(-9_007_199_254_740_993)),
        ("9.007199254740993e15", Some(9_007_199_254_740_993)),
        ("9223372036854775807.0", Some(i64::MAX)),
        ("-9223372036854775807.0", Some(i64::MIN + 1)),
        ("9223372036854775806.9", Some(i64::MAX - 1)),
        ("-9223372036854775807.9", Some(i64::MIN + 1)),
        ("-9223372036854775808.0", Some(i64::MIN)),
        ("999999999999999999999999999e-20", Some(9_999_999)),
        ("9223372036854775808", None),
        ("-9223372036854775809", None),
        ("9223372036854775807.00000000000000001", None),
        ("-9223372036854775808.00000000000000001", None),
        ("+0001.99", Some(1)),
        ("-0001.99", Some(-1)),
        ("\t -.9e+1\u{2003}", Some(-9)),
        ("1.", Some(1)),
        ("1.e2", Some(100)),
        (".1", Some(0)),
        ("000.000e100000", Some(0)),
        ("1e100000", None),
        ("-1e100000", None),
        ("1e-100000", Some(0)),
        ("-1e-100000", Some(0)),
        ("1e170141183460469231731687303715884105728", None),
        ("1e-170141183460469231731687303715884105728", Some(0)),
        ("0e170141183460469231731687303715884105728", Some(0)),
    ] {
        let expression = r::Expression::Function(
            r::Function::ToInteger,
            vec![r::Expression::Literal(r::Value::String(text.into()))],
        );
        assert_eq!(
            evaluation.eval(&expression).unwrap(),
            expected.map_or(r::Value::Null, r::Value::Integer),
            "{text}"
        );
    }
    for text in [
        "",
        "+",
        "-",
        ".",
        "e1",
        "1e",
        "1e+",
        "1e-",
        "1e+-2",
        "1e 2",
        "1.2.3",
        "1e2e3",
        "1E2E3",
        "1_0",
        "0x10",
        "1 2",
        "+-1",
        "--1",
        "NaN",
        "Infinity",
        "-inf",
        "١٢٣",
        "１２３",
        "猫",
        "0\0",
    ] {
        let expression = r::Expression::Function(
            r::Function::ToInteger,
            vec![r::Expression::Literal(r::Value::String(text.into()))],
        );
        assert_eq!(
            evaluation.eval(&expression).unwrap(),
            r::Value::Null,
            "{text:?}"
        );
    }
    // Floating values have already been rounded: their existing conversion
    // rule is intentionally distinct from exact decimal-string conversion.
    for (value, expected) in [
        (r::Value::Float(1.9), r::Value::Integer(1)),
        (r::Value::Float(-1.9), r::Value::Integer(-1)),
        (
            r::Value::Float(i64::MIN as f64),
            r::Value::Integer(i64::MIN),
        ),
        (r::Value::Float(i64::MAX as f64), r::Value::Null),
        (r::Value::Float(f64::INFINITY), r::Value::Null),
        (r::Value::Float(f64::NAN), r::Value::Null),
        (r::Value::Integer(i64::MAX), r::Value::Integer(i64::MAX)),
        (r::Value::Null, r::Value::Null),
    ] {
        let expression =
            r::Expression::Function(r::Function::ToInteger, vec![r::Expression::Literal(value)]);
        assert_eq!(evaluation.eval(&expression).unwrap(), expected);
    }
}

#[test]
fn decimal_integer_conversion_matches_scaled_integer_oracles() {
    let parameters = BTreeMap::new();
    let evaluation = r::Evaluation {
        row: &[],
        parameters: &parameters,
        graph: &NoGraph,
        group: None,
        max_collection_items: 100,
        max_value_bytes: 16 * 1024,
    };
    let mut bits = 0x6a09e667f3bcc909_u64;
    for _ in 0..4096 {
        bits ^= bits << 13;
        bits ^= bits >> 7;
        bits ^= bits << 17;
        let integer = bits as i64;
        // Each representation has exactly the same mathematical value, with
        // nonzero trailing integer digits well beyond floating precision.
        for text in [
            format!("{integer}.0"),
            format!("{integer}000000000000000000000e-21"),
            format!("{integer}e+0"),
        ] {
            let expression = r::Expression::Function(
                r::Function::ToInteger,
                vec![r::Expression::Literal(r::Value::String(text))],
            );
            assert_eq!(
                evaluation.eval(&expression).unwrap(),
                r::Value::Integer(integer)
            );
        }
        for scale in [1_u32, 3, 9, 18] {
            let divisor = 10_i64.pow(scale);
            let text = format!("{integer}e-{scale}");
            let expression = r::Expression::Function(
                r::Function::ToInteger,
                vec![r::Expression::Literal(r::Value::String(text))],
            );
            assert_eq!(
                evaluation.eval(&expression).unwrap(),
                r::Value::Integer(integer / divisor)
            );
        }
    }
}

#[test]
fn collection_limits_bound_values_instead_of_function_arity() {
    use r::{Binary as B, Expression as E, Function as F, Value as V};
    let parameters = BTreeMap::new();
    let evaluation = r::Evaluation {
        row: &[],
        parameters: &parameters,
        graph: &NoGraph,
        group: None,
        max_collection_items: 1,
        max_value_bytes: 64 * 1024,
    };
    let integer = |n| E::Literal(V::Integer(n));
    for (expression, expected) in [
        (
            E::Function(
                F::Substring,
                vec![E::Literal(V::String("a猫z".into())), integer(1), integer(1)],
            ),
            V::String("猫".into()),
        ),
        (
            E::Function(F::Range, vec![integer(4), integer(4)]),
            V::List(vec![V::Integer(4)]),
        ),
        (
            E::Function(F::Range, vec![integer(4), integer(4), integer(-1)]),
            V::List(vec![V::Integer(4)]),
        ),
        (
            E::Function(F::Range, vec![integer(4), integer(1)]),
            V::List(vec![]),
        ),
        (
            E::Function(
                F::Coalesce,
                vec![E::Literal(V::Null), E::Literal(V::Null), integer(7)],
            ),
            V::Integer(7),
        ),
    ] {
        assert_eq!(evaluation.eval(&expression).unwrap(), expected);
    }
    for (expression, category, detail) in [
        (
            E::Function(
                F::Substring,
                vec![
                    E::Literal(V::String("abc".into())),
                    E::Binary(B::Divide, Box::new(integer(1)), Box::new(integer(0))),
                    integer(1),
                ],
            ),
            "ArithmeticError",
            "DivisionByZero",
        ),
        (
            E::Function(F::Range, vec![integer(1), integer(2), integer(0)]),
            "ArgumentError",
            "NumberOutOfRange",
        ),
    ] {
        let error = evaluation.eval(&expression).unwrap_err();
        assert_eq!(error.category, category);
        assert_eq!(error.detail, detail);
        assert_eq!(error.phase, r::ErrorPhase::Runtime);
    }
    let singleton = |n| E::List(vec![integer(n)]);
    for (left, right) in [
        (singleton(1), singleton(2)),
        (singleton(1), integer(2)),
        (integer(1), singleton(2)),
    ] {
        let expression = E::Binary(B::Add, Box::new(left), Box::new(right));
        let error = evaluation.eval(&expression).unwrap_err();
        assert_eq!(error.category, "ResourceLimit");
        assert_eq!(error.detail, "CollectionLimit");
        assert_eq!(error.phase, r::ErrorPhase::Runtime);
        assert_eq!(
            r::Evaluation {
                max_collection_items: 2,
                ..evaluation
            }
            .eval(&expression)
            .unwrap(),
            V::List(vec![V::Integer(1), V::Integer(2)])
        );
    }
    // Row demand and range cardinality are independent: the direct UNWIND
    // generator can produce a prefix even when a scalar list could not fit.
    let streamed = evaluation
        .unwind(&E::Function(F::Range, vec![integer(1), integer(100)]))
        .unwrap();
    assert_eq!(
        streamed.take(3).collect::<Vec<_>>(),
        vec![V::Integer(1), V::Integer(2), V::Integer(3)]
    );
}

#[test]
fn collection_limits_validate_nested_borrowed_inputs_before_selected_results() {
    use r::{Expression as E, Function as F, Value as V};
    let nested = V::Map(BTreeMap::from([(
        "values".into(),
        V::List(vec![V::Integer(1), V::Integer(2)]),
    )]));
    let row = vec![nested.clone()];
    let parameters = BTreeMap::from([("input".into(), nested.clone())]);
    let evaluation = r::Evaluation {
        row: &row,
        parameters: &parameters,
        graph: &NoGraph,
        group: None,
        max_collection_items: 1,
        max_value_bytes: 64 * 1024,
    };
    for expression in [
        E::Literal(nested.clone()),
        E::Slot(r::Slot(0)),
        E::Parameter("input".into()),
    ] {
        let error = evaluation.eval(&expression).unwrap_err();
        assert_eq!(error.detail, "CollectionLimit");
        assert_eq!(
            r::Evaluation {
                max_collection_items: 2,
                ..evaluation
            }
            .eval(&expression)
            .unwrap(),
            nested
        );
        for lazy in [
            E::Case {
                branches: vec![(E::Literal(V::Boolean(false)), expression.clone())],
                otherwise: Box::new(E::Literal(V::Integer(7))),
            },
            E::Function(F::Coalesce, vec![E::Literal(V::Integer(7)), expression]),
        ] {
            assert_eq!(evaluation.eval(&lazy).unwrap(), V::Integer(7));
        }
    }
    assert_eq!(row[0], nested);
    assert_eq!(parameters["input"], nested);
    let individually_bounded = V::List(vec![
        V::List(vec![V::Integer(1), V::Integer(2)]),
        V::List(vec![V::Integer(3), V::Integer(4)]),
    ]);
    assert_eq!(
        r::Evaluation {
            max_collection_items: 2,
            ..evaluation
        }
        .eval(&E::Literal(individually_bounded.clone()))
        .unwrap(),
        individually_bounded
    );

    // Map entry count does not consume list cardinality. Its nested lists and
    // allocated bytes remain independently bounded.
    let map = V::Map(BTreeMap::from([
        ("a".into(), V::Integer(1)),
        ("b".into(), V::Integer(2)),
    ]));
    assert_eq!(evaluation.eval(&E::Literal(map.clone())).unwrap(), map);
}

#[test]
fn collection_limits_cover_graph_lists_without_reading_dormant_values() {
    use r::{Expression as E, Function as F, Value as V};
    struct Graph(r::GraphProperties);
    impl r::GraphValues for Graph {
        fn properties(&self, _: r::Entity) -> r::Result<&r::GraphProperties> {
            Ok(&self.0)
        }
        fn label(&self, _: r::Entity) -> r::Result<Option<&str>> {
            Ok(Some("N"))
        }
    }
    let graph = Graph(BTreeMap::from([
        ("a".into(), Ok(V::List(vec![V::Integer(1), V::Integer(2)]))),
        ("b".into(), Err(r::QueryError::unsupported("StoredValue"))),
    ]));
    let parameters = BTreeMap::new();
    let evaluation = r::Evaluation {
        row: &[],
        parameters: &parameters,
        graph: &graph,
        group: None,
        max_collection_items: 1,
        max_value_bytes: 64 * 1024,
    };
    let node = E::Literal(V::Entity(r::Entity::Node(1)));
    let path = E::Literal(V::Path(r::Path::new(vec![1, 2, 3], vec![1, 2]).unwrap()));
    for expression in [
        E::Property(Box::new(node.clone()), "a".into()),
        E::Index(
            Box::new(node.clone()),
            Box::new(E::Literal(V::String("a".into()))),
        ),
        E::Function(F::Keys, vec![node.clone()]),
        E::Function(F::Nodes, vec![path.clone()]),
        E::Function(F::Relationships, vec![path]),
    ] {
        assert_eq!(
            evaluation.eval(&expression).unwrap_err().detail,
            "CollectionLimit"
        );
    }
    assert_eq!(
        evaluation
            .eval(&E::Function(F::Labels, vec![node.clone()]))
            .unwrap(),
        V::List(vec![V::String("N".into())])
    );
    assert_eq!(
        r::Evaluation {
            max_collection_items: 2,
            ..evaluation
        }
        .eval(&E::Function(F::Keys, vec![node]))
        .unwrap(),
        V::List(vec![V::String("a".into()), V::String("b".into())])
    );
    for (function, argument, limit, expected) in [
        (F::Keys, V::Map(BTreeMap::new()), 0, V::List(vec![])),
        (
            F::Nodes,
            V::Path(r::Path::new(vec![1], vec![]).unwrap()),
            1,
            V::List(vec![V::Entity(r::Entity::Node(1))]),
        ),
        (
            F::Relationships,
            V::Path(r::Path::new(vec![1], vec![]).unwrap()),
            0,
            V::List(vec![]),
        ),
        (
            F::Nodes,
            V::Path(r::Path::new(vec![1, 2, 3], vec![1, 2]).unwrap()),
            3,
            V::List(vec![
                V::Entity(r::Entity::Node(1)),
                V::Entity(r::Entity::Node(2)),
                V::Entity(r::Entity::Node(3)),
            ]),
        ),
        (
            F::Relationships,
            V::Path(r::Path::new(vec![1, 2, 3], vec![1, 2]).unwrap()),
            2,
            V::List(vec![
                V::Entity(r::Entity::Relationship(1)),
                V::Entity(r::Entity::Relationship(2)),
            ]),
        ),
    ] {
        let expression = E::Function(function, vec![E::Literal(argument)]);
        assert_eq!(
            r::Evaluation {
                max_collection_items: limit,
                ..evaluation
            }
            .eval(&expression)
            .unwrap(),
            expected
        );
        if limit > 0 {
            assert_eq!(
                r::Evaluation {
                    max_collection_items: limit - 1,
                    ..evaluation
                }
                .eval(&expression)
                .unwrap_err()
                .detail,
                "CollectionLimit"
            );
        }
    }
    assert_eq!(
        r::Evaluation {
            max_collection_items: 0,
            ..evaluation
        }
        .eval(&E::Function(
            F::Labels,
            vec![E::Literal(V::Entity(r::Entity::Node(1)))]
        ))
        .unwrap_err()
        .detail,
        "CollectionLimit"
    );
    // Property-map materialization must validate a nested collection before its
    // copy, while enumerating keys continues to ignore dormant value errors.
    assert_eq!(
        evaluation
            .properties(r::Entity::Node(1))
            .unwrap_err()
            .detail,
        "CollectionLimit"
    );
}

#[test]
fn nested_collection_limits_match_an_independent_stack_walk() {
    use r::Value as V;
    let parameters = BTreeMap::new();
    for seed in 0..256 {
        let mut value = V::Integer(seed);
        for depth in 0..seed % 53 {
            value = if (seed + depth) % 3 == 0 {
                V::Map(BTreeMap::from([
                    ("a".into(), V::Integer(depth)),
                    ("b".into(), value),
                    ("c".into(), V::Boolean(true)),
                ]))
            } else {
                let mut children = vec![V::Null; ((seed + depth) % 4) as usize];
                children.insert(children.len() / 2, value);
                V::List(children)
            };
        }
        for limit in [0, 1, 2, 3, 4, usize::MAX] {
            // The oracle uses an explicit stack, independently of the engine's
            // bounded recursive traversal. Both visit children in value order.
            let mut pending = vec![(&value, 0)];
            let mut expected = None;
            while let Some((value, depth)) = pending.pop() {
                if depth >= r::MAX_EXPRESSION_DEPTH {
                    expected = Some("ValueDepth");
                    break;
                }
                match value {
                    V::List(values) if values.len() > limit => {
                        expected = Some("CollectionLimit");
                        break;
                    }
                    V::List(values) => pending.extend(values.iter().rev().map(|v| (v, depth + 1))),
                    V::Map(values) => pending.extend(values.values().rev().map(|v| (v, depth + 1))),
                    _ => {}
                }
            }
            let evaluation = r::Evaluation {
                row: &[],
                parameters: &parameters,
                graph: &NoGraph,
                group: None,
                max_collection_items: limit,
                max_value_bytes: usize::MAX,
            };
            let result = evaluation.eval(&r::Expression::Literal(value.clone()));
            match expected {
                None => assert_eq!(result.unwrap(), value),
                Some(detail) => {
                    let error = result.unwrap_err();
                    assert_eq!(
                        error.category, "ResourceLimit",
                        "seed={seed}, limit={limit}"
                    );
                    assert_eq!(error.detail, detail, "seed={seed}, limit={limit}");
                    assert_eq!(error.phase, r::ErrorPhase::Runtime);
                }
            }
        }
    }
}

#[test]
fn collection_growth_checks_cardinality_even_with_spare_capacity() {
    use r::{Expression as E, Value as V};
    let parameters = BTreeMap::new();
    let group = vec![vec![V::Integer(1)], vec![V::Integer(2)]];
    let evaluation = r::Evaluation {
        row: &[],
        parameters: &parameters,
        graph: &NoGraph,
        group: Some(&group),
        max_collection_items: 2,
        max_value_bytes: 64 * 1024,
    };
    let collect = E::Aggregate {
        function: r::Aggregate::Collect,
        argument: Some(Box::new(E::Slot(r::Slot(0)))),
        distinct: false,
    };
    let V::List(values) = evaluation.eval(&collect).unwrap() else {
        panic!("collect must return a list")
    };
    assert_eq!(values, vec![V::Integer(1), V::Integer(2)]);
    assert!(
        values.capacity() > values.len(),
        "exercise growth that does not need another allocation"
    );
    let expression = E::Binary(
        r::Binary::Add,
        Box::new(collect),
        Box::new(E::Literal(V::Integer(3))),
    );
    assert_eq!(
        evaluation.eval(&expression).unwrap_err().detail,
        "CollectionLimit"
    );
    assert_eq!(
        r::Evaluation {
            max_collection_items: 3,
            ..evaluation
        }
        .eval(&expression)
        .unwrap(),
        V::List(vec![V::Integer(1), V::Integer(2), V::Integer(3)])
    );
}
