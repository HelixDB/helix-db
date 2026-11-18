//! Datetime values: comparison, ordering, grouping identity, functions and
//! static types, each against the existing semantics of the other types.
use helix_planner::relational as r;
use r::{Expression as E, Function as F, Value as V};
use std::collections::BTreeMap;
use std::hash::{BuildHasher, BuildHasherDefault, DefaultHasher};

struct NoGraph;
impl r::GraphValues for NoGraph {
    fn properties(&self, _: r::Entity) -> r::Result<&r::GraphProperties> {
        panic!("scalar expression attempted graph access")
    }
    fn label(&self, _: r::Entity) -> r::Result<Option<&str>> {
        panic!("scalar expression attempted graph access")
    }
}

const NOW: i64 = 1_700_000_000_123;

fn eval(expression: &E) -> r::Result<V> {
    let parameters = BTreeMap::from([
        ("at".to_owned(), V::DateTime(NOW)),
        (
            "text".to_owned(),
            V::String("2023-11-14T22:13:20.123Z".into()),
        ),
    ]);
    r::Evaluation {
        row: &[],
        parameters: &parameters,
        graph: &NoGraph,
        group: None,
        max_collection_items: 100,
        memory: r::EvaluationMemory::new(16_384),
        statement_time: NOW,
    }
    .eval(expression)
}

fn binary(op: r::Binary, left: V, right: V) -> V {
    eval(&E::Binary(
        op,
        Box::new(E::Literal(left)),
        Box::new(E::Literal(right)),
    ))
    .unwrap()
}

fn call(function: F, arguments: Vec<V>) -> r::Result<V> {
    eval(&E::Function(
        function,
        arguments.into_iter().map(E::Literal).collect(),
    ))
}

#[test]
fn datetimes_compare_chronologically_in_predicates() {
    use r::Binary as B;
    let (early, late) = (V::DateTime(-1), V::DateTime(1));
    for (op, expected) in [
        (B::Less, true),
        (B::LessEqual, true),
        (B::Greater, false),
        (B::GreaterEqual, false),
        (B::Equal, false),
        (B::NotEqual, true),
    ] {
        assert_eq!(
            binary(op, early.clone(), late.clone()),
            V::Boolean(expected),
            "{op:?}"
        );
    }
    assert_eq!(
        binary(B::Equal, late.clone(), V::DateTime(1)),
        V::Boolean(true)
    );
    assert_eq!(
        binary(B::LessEqual, late.clone(), V::DateTime(1)),
        V::Boolean(true)
    );
    assert_eq!(
        binary(
            B::In,
            late.clone(),
            V::List(vec![V::Integer(1), V::DateTime(1)])
        ),
        V::Boolean(true)
    );
    assert_eq!(
        binary(B::In, late.clone(), V::List(vec![V::Integer(1), V::Null])),
        V::Null
    );
    // Lists order element by element, so datetimes inside them do too.
    assert_eq!(
        binary(
            B::Less,
            V::List(vec![early.clone()]),
            V::List(vec![late.clone()])
        ),
        V::Boolean(true)
    );
    assert_eq!(binary(B::Less, late, V::Null), V::Null);
}

#[test]
fn mixed_domain_comparisons_match_string_against_integer() {
    use r::Binary as B;
    // A datetime against any other type but text behaves as a string does
    // against an integer: equality is false, ordering is null, never an error.
    let others = [
        V::Integer(1),
        V::Float(1.0),
        V::Boolean(true),
        V::List(vec![V::DateTime(1)]),
        V::Map(BTreeMap::from([("epochMillis".into(), V::Integer(1))])),
    ];
    for other in others {
        for (op, expected) in [
            (B::Equal, V::Boolean(false)),
            (B::NotEqual, V::Boolean(true)),
            (B::Less, V::Null),
            (B::LessEqual, V::Null),
            (B::Greater, V::Null),
            (B::GreaterEqual, V::Null),
            (B::StartsWith, V::Null),
        ] {
            assert_eq!(
                binary(op, V::DateTime(1), other.clone()),
                expected,
                "{op:?} {other:?}"
            );
            assert_eq!(
                binary(op, other.clone(), V::DateTime(1)),
                expected,
                "{op:?} {other:?}"
            );
            let string_against_integer = binary(op, V::String("1".into()), V::Integer(1));
            assert_eq!(string_against_integer, expected, "{op:?} reference");
        }
    }
    for op in [B::Add, B::Subtract, B::Multiply] {
        let error = eval(&E::Binary(
            op,
            Box::new(E::Literal(V::DateTime(1))),
            Box::new(E::Literal(V::Integer(1))),
        ))
        .unwrap_err();
        assert_eq!(error.category, r::category::TYPE_ERROR, "{op:?}");
    }
}

/// Text is never read as a datetime, so comparing the two is a type error
/// wherever they meet: every comparison and text match, membership, a simple
/// CASE, and inside lists and maps. Null still compares as unknown.
#[test]
fn datetimes_and_text_never_compare() {
    use r::Binary as B;
    let failure = |op: B, left: V, right: V| {
        let error = eval(&E::Binary(
            op,
            Box::new(E::Literal(left.clone())),
            Box::new(E::Literal(right.clone())),
        ))
        .unwrap_err();
        assert_eq!(
            (error.category.as_str(), error.detail.as_str(), error.phase),
            (
                r::category::TYPE_ERROR,
                r::detail::DATETIME_TEXT_COMPARISON,
                r::ErrorPhase::Runtime
            ),
            "{op:?} {left:?} {right:?}"
        );
        // The message names the conversion that would make the comparison.
        assert!(
            error.message.contains("datetime('…')") || error.message.contains("toString("),
            "{}",
            error.message
        );
    };
    let text = || V::String("1970-01-01T00:00:00.001Z".into());
    let unreadable = || V::String("soon".into());
    for op in [
        B::Equal,
        B::NotEqual,
        B::Less,
        B::LessEqual,
        B::Greater,
        B::GreaterEqual,
        B::StartsWith,
        B::EndsWith,
        B::Contains,
    ] {
        for other in [text(), unreadable(), V::String(String::new())] {
            failure(op, V::DateTime(1), other.clone());
            failure(op, other, V::DateTime(1));
        }
    }
    for op in [B::Equal, B::NotEqual, B::Less, B::GreaterEqual] {
        failure(op, V::List(vec![V::DateTime(1)]), V::List(vec![text()]));
        failure(
            op,
            V::List(vec![V::Integer(1), text()]),
            V::List(vec![V::Integer(1), V::DateTime(1)]),
        );
    }
    let map = |value: V| V::Map(BTreeMap::from([("at".to_owned(), value)]));
    failure(B::Equal, map(V::DateTime(1)), map(text()));
    failure(B::In, V::DateTime(1), V::List(vec![V::Integer(1), text()]));
    failure(B::In, text(), V::List(vec![V::DateTime(1)]));
    // A comparison that never reaches the pair does not fail: lists of other
    // lengths are unequal, and an earlier element decides the order.
    assert_eq!(
        binary(
            B::Equal,
            V::List(vec![V::DateTime(1)]),
            V::List(vec![text(), text()])
        ),
        V::Boolean(false)
    );
    assert_eq!(
        binary(
            B::Less,
            V::List(vec![V::Integer(0), V::DateTime(1)]),
            V::List(vec![V::Integer(1), text()])
        ),
        V::Boolean(true)
    );
    assert_eq!(
        binary(B::In, V::DateTime(1), V::List(vec![V::DateTime(1), text()])),
        V::Boolean(true)
    );
    for op in [B::Equal, B::Less, B::StartsWith] {
        assert_eq!(binary(op, V::DateTime(1), V::Null), V::Null, "{op:?}");
        assert_eq!(binary(op, V::Null, text()), V::Null, "{op:?}");
    }
    let case = |operand: V, comparison: V| {
        eval(&E::SimpleCase(Box::new(r::SimpleCase {
            operand: E::Literal(operand),
            branches: helix_planner::ir::AtLeast::from_one((
                E::Literal(comparison),
                E::Literal(V::Integer(1)),
            )),
            otherwise: E::Literal(V::Integer(0)),
        })))
    };
    let error = case(V::DateTime(1), text()).unwrap_err();
    assert_eq!(error.detail, r::detail::DATETIME_TEXT_COMPARISON);
    assert_eq!(case(V::DateTime(1), V::DateTime(1)).unwrap(), V::Integer(1));
    assert_eq!(case(V::DateTime(1), V::Integer(1)).unwrap(), V::Integer(0));
}

#[test]
fn total_order_ranks_datetimes_between_paths_and_strings() {
    let path = V::Path(r::Path::new(vec![1], vec![]).unwrap());
    let ascending = [
        V::Map(BTreeMap::new()),
        V::Entity(r::Entity::Node(1)),
        V::Entity(r::Entity::Relationship(1)),
        V::List(vec![]),
        path,
        V::DateTime(i64::MIN),
        V::DateTime(-1),
        V::DateTime(0),
        V::DateTime(i64::MAX),
        V::String(String::new()),
        V::Boolean(false),
        V::Integer(i64::MIN),
        V::Null,
    ];
    for (index, lower) in ascending.iter().enumerate() {
        assert!(lower.total_cmp(lower).is_eq(), "{lower:?}");
        for higher in &ascending[index + 1..] {
            assert!(lower.total_cmp(higher).is_lt(), "{lower:?} < {higher:?}");
            assert!(higher.total_cmp(lower).is_gt(), "{higher:?} > {lower:?}");
        }
    }
}

#[test]
fn grouping_keys_separate_datetimes_from_their_millis() {
    let hash = |key: &r::GroupingKey| BuildHasherDefault::<DefaultHasher>::default().hash_one(key);
    let key = |value| r::GroupingKey::new(value).unwrap();
    let datetime = key(V::DateTime(1));
    assert_eq!(datetime, key(V::DateTime(1)));
    assert_eq!(hash(&datetime), hash(&key(V::DateTime(1))));
    for other in [
        V::DateTime(2),
        V::Integer(1),
        V::Float(1.0),
        V::String("1".into()),
        V::Null,
    ] {
        assert_ne!(datetime, key(other.clone()), "{other:?}");
    }
    let row = r::GroupingKey::row(vec![V::DateTime(1), V::Null]).unwrap();
    assert_eq!(
        row,
        r::GroupingKey::row(vec![V::DateTime(1), V::Null]).unwrap()
    );
    assert_ne!(
        row,
        r::GroupingKey::row(vec![V::Integer(1), V::Null]).unwrap()
    );
}

#[test]
fn min_and_max_aggregate_datetimes_and_sum_rejects_them() {
    let fold = |aggregate, values: &[V]| {
        let mut accumulator = r::Accumulator::new(aggregate, false);
        values.iter().try_for_each(|value| {
            accumulator.push_with_admission::<r::QueryError>(
                value.clone(),
                100,
                1 << 20,
                |_| Ok(()),
            )
        })?;
        accumulator.finish()
    };
    let values = [V::DateTime(5), V::Null, V::DateTime(-5), V::DateTime(0)];
    assert_eq!(fold(r::Aggregate::Min, &values).unwrap(), V::DateTime(-5));
    assert_eq!(fold(r::Aggregate::Max, &values).unwrap(), V::DateTime(5));
    assert_eq!(fold(r::Aggregate::Count, &values).unwrap(), V::Integer(3));
    for aggregate in [r::Aggregate::Sum, r::Aggregate::Avg] {
        let error = fold(aggregate, &values).unwrap_err();
        assert_eq!(error.category, r::category::TYPE_ERROR, "{aggregate:?}");
    }
    let mut distinct = r::Accumulator::new(r::Aggregate::Count, true);
    for value in [
        V::DateTime(1),
        V::DateTime(1),
        V::Integer(1),
        V::DateTime(2),
    ] {
        distinct
            .push_with_admission::<r::QueryError>(value, 100, 1 << 20, |_| Ok(()))
            .unwrap();
    }
    assert_eq!(distinct.finish().unwrap(), V::Integer(3));
}

#[test]
fn clock_functions_read_the_statement_time() {
    assert_eq!(call(F::DateTime, vec![]).unwrap(), V::DateTime(NOW));
    assert_eq!(call(F::Timestamp, vec![]).unwrap(), V::Integer(NOW));
    // Every call in the statement reads the same instant.
    let twice = E::Binary(
        r::Binary::Equal,
        Box::new(E::Function(F::DateTime, vec![])),
        Box::new(E::Function(F::DateTime, vec![])),
    );
    assert_eq!(eval(&twice).unwrap(), V::Boolean(true));
    let error = call(F::Timestamp, vec![V::Integer(1)]).unwrap_err();
    assert_eq!(error.category, r::category::TYPE_ERROR);
}

#[test]
fn datetime_reads_text_maps_and_datetimes() {
    for (argument, expected) in [
        (
            V::String("2023-11-14T22:13:20.123Z".into()),
            V::DateTime(NOW),
        ),
        (
            V::String("2023-11-15 00:13:20.123+02:00".into()),
            V::DateTime(NOW),
        ),
        (V::String("1970-01-01".into()), V::DateTime(0)),
        (V::DateTime(-7), V::DateTime(-7)),
        (V::DateTime(i64::MAX), V::DateTime(i64::MAX)),
        (
            V::Map(BTreeMap::from([("epochMillis".into(), V::Integer(-1))])),
            V::DateTime(-1),
        ),
        (V::Null, V::Null),
    ] {
        assert_eq!(
            call(F::DateTime, vec![argument.clone()]).unwrap(),
            expected,
            "{argument:?}"
        );
    }
    let parameter = E::Function(F::DateTime, vec![E::Parameter("text".into())]);
    assert_eq!(eval(&parameter).unwrap(), V::DateTime(NOW));
}

#[test]
fn datetime_rejects_unreadable_text_maps_and_types() {
    let code = |argument: V| {
        let error = call(F::DateTime, vec![argument]).unwrap_err();
        assert_eq!(error.phase, r::ErrorPhase::Runtime);
        (error.category, error.detail)
    };
    let invalid = (
        r::category::ARGUMENT_ERROR.to_owned(),
        r::detail::INVALID_DATETIME.to_owned(),
    );
    for text in [
        "",
        "yesterday",
        "2023-11-14T22:13",
        "2023-11-14T22:13:20+0200",
        // Outside the grammar's padding and years, or naming no instant.
        "2023-1-5",
        "+10000-01-01T00:00:00.000Z",
        "2023-02-30",
        "2016-12-31T23:59:60Z",
        // An offset moving the instant outside the four-digit UTC years.
        "9999-12-31T23:59:59-00:01",
        "0000-01-01T00:00:00+00:01",
    ] {
        assert_eq!(code(V::String(text.into())), invalid, "{text}");
    }
    let map = |entries: &[(&str, V)]| {
        V::Map(
            entries
                .iter()
                .map(|(key, value)| ((*key).to_owned(), value.clone()))
                .collect(),
        )
    };
    for argument in [
        map(&[]),
        map(&[("year", V::Integer(2020))]),
        map(&[("epochMillis", V::Float(1.0))]),
        map(&[("epochMillis", V::String("1".into()))]),
        map(&[
            ("epochMillis", V::Integer(1)),
            ("timezone", V::String("Z".into())),
        ]),
    ] {
        assert_eq!(code(argument.clone()), invalid, "{argument:?}");
    }
    let out_of_range = (
        r::category::ARGUMENT_ERROR.to_owned(),
        r::detail::DATETIME_OUT_OF_RANGE.to_owned(),
    );
    for millis in [i64::MIN, i64::MAX] {
        assert_eq!(
            code(map(&[("epochMillis", V::Integer(millis))])),
            out_of_range
        );
    }
    for argument in [
        V::Integer(1),
        V::Float(1.0),
        V::Boolean(true),
        V::List(vec![]),
    ] {
        assert_eq!(
            code(argument.clone()).0,
            r::category::TYPE_ERROR,
            "{argument:?}"
        );
    }
}

/// Each `…OrNull` conversion is its strict conversion, except that a value
/// the strict one fails to convert, with a type or argument error, is null.
#[test]
fn or_null_conversions_are_null_where_strict_conversions_fail() {
    let text = |value: &str| V::String(value.into());
    let map = |value: V| V::Map(BTreeMap::from([("epochMillis".to_owned(), value)]));
    for (strict, or_null, argument, converted) in [
        (
            F::ToInteger,
            F::ToIntegerOrNull,
            text("12"),
            Some(V::Integer(12)),
        ),
        (F::ToInteger, F::ToIntegerOrNull, text("x"), Some(V::Null)),
        (F::ToInteger, F::ToIntegerOrNull, V::Boolean(true), None),
        (F::ToInteger, F::ToIntegerOrNull, V::DateTime(1), None),
        (
            F::ToFloat,
            F::ToFloatOrNull,
            text("1.5"),
            Some(V::Float(1.5)),
        ),
        (F::ToFloat, F::ToFloatOrNull, V::List(vec![]), None),
        (
            F::ToBoolean,
            F::ToBooleanOrNull,
            text("TRUE"),
            Some(V::Boolean(true)),
        ),
        (F::ToBoolean, F::ToBooleanOrNull, V::Integer(1), None),
        (
            F::ToString,
            F::ToStringOrNull,
            V::Integer(7),
            Some(text("7")),
        ),
        (
            F::ToString,
            F::ToStringOrNull,
            V::DateTime(1),
            Some(text("1970-01-01T00:00:00.001Z")),
        ),
        (F::ToString, F::ToStringOrNull, V::DateTime(i64::MAX), None),
        (
            F::ToString,
            F::ToStringOrNull,
            V::List(vec![V::Integer(1)]),
            None,
        ),
        (
            F::DateTime,
            F::DateTimeOrNull,
            text("1970-01-01T00:00:00.001Z"),
            Some(V::DateTime(1)),
        ),
        (
            F::DateTime,
            F::DateTimeOrNull,
            V::DateTime(1),
            Some(V::DateTime(1)),
        ),
        (
            F::DateTime,
            F::DateTimeOrNull,
            map(V::Integer(1)),
            Some(V::DateTime(1)),
        ),
        (F::DateTime, F::DateTimeOrNull, text("soon"), None),
        (
            F::DateTime,
            F::DateTimeOrNull,
            text("9999-12-31T23:59:59-00:01"),
            None,
        ),
        (F::DateTime, F::DateTimeOrNull, map(V::Float(1.5)), None),
        (
            F::DateTime,
            F::DateTimeOrNull,
            map(V::Integer(i64::MAX)),
            None,
        ),
        (F::DateTime, F::DateTimeOrNull, V::Integer(1), None),
    ] {
        let strict_result = call(strict, vec![argument.clone()]);
        match converted {
            Some(value) => {
                assert_eq!(strict_result.unwrap(), value, "{strict:?} {argument:?}");
                assert_eq!(call(or_null, vec![argument.clone()]).unwrap(), value);
            }
            None => {
                let error = strict_result.unwrap_err();
                assert!(
                    [r::category::TYPE_ERROR, r::category::ARGUMENT_ERROR]
                        .contains(&error.category.as_str()),
                    "{strict:?} {argument:?}: {error}"
                );
                assert_eq!(
                    call(or_null, vec![argument.clone()]).unwrap(),
                    V::Null,
                    "{or_null:?} {argument:?}"
                );
            }
        }
        assert_eq!(call(or_null, vec![V::Null]).unwrap(), V::Null);
    }
    // A failure of another kind, such as the memory a conversion's text
    // needs, still fails.
    let error = r::Evaluation {
        row: &[],
        parameters: &BTreeMap::new(),
        graph: &NoGraph,
        group: None,
        max_collection_items: 100,
        memory: r::EvaluationMemory::new(size_of::<V>() + 2),
        statement_time: NOW,
    }
    .eval(&E::Function(
        F::ToStringOrNull,
        vec![E::Literal(V::Integer(1_234_567_890))],
    ))
    .unwrap_err();
    assert_eq!(error.category, r::category::RESOURCE_LIMIT);
}

/// `requireDatetime` passes a datetime and null through and rejects every
/// other value, without reading or converting it.
#[test]
fn require_datetime_admits_only_datetimes() {
    assert_eq!(
        call(F::RequireDateTime, vec![V::DateTime(-1)]).unwrap(),
        V::DateTime(-1)
    );
    assert_eq!(call(F::RequireDateTime, vec![V::Null]).unwrap(), V::Null);
    for argument in [
        V::String("1970-01-01T00:00:00Z".into()),
        V::Integer(0),
        V::Map(BTreeMap::from([("year".to_owned(), V::Integer(2024))])),
        V::List(vec![V::DateTime(0)]),
    ] {
        let error = call(F::RequireDateTime, vec![argument.clone()]).unwrap_err();
        assert_eq!(
            (error.category.as_str(), error.detail.as_str()),
            (r::category::TYPE_ERROR, r::detail::INVALID_ARGUMENT_TYPE),
            "{argument:?}"
        );
    }
}

#[test]
fn to_string_renders_the_native_response_text() {
    assert_eq!(
        call(F::ToString, vec![V::DateTime(NOW)]).unwrap(),
        V::String("2023-11-14T22:13:20.123Z".into())
    );
    assert_eq!(
        call(F::ToString, vec![V::DateTime(-1)]).unwrap(),
        V::String("1969-12-31T23:59:59.999Z".into())
    );
    let error = call(F::ToString, vec![V::DateTime(i64::MAX)]).unwrap_err();
    assert_eq!(error.detail, r::detail::DATETIME_OUT_OF_RANGE);
    // Other conversions keep rejecting a datetime.
    for function in [F::ToInteger, F::ToFloat, F::ToBoolean, F::Size, F::Abs] {
        let error = call(function, vec![V::DateTime(1)]).unwrap_err();
        assert_eq!(error.category, r::category::TYPE_ERROR, "{function:?}");
    }
    // A too-small budget fails before the text is allocated.
    let error = r::Evaluation {
        row: &[],
        parameters: &BTreeMap::new(),
        graph: &NoGraph,
        group: None,
        max_collection_items: 100,
        memory: r::EvaluationMemory::new(size_of::<V>()),
        statement_time: NOW,
    }
    .eval(&E::Function(
        F::ToString,
        vec![E::Literal(V::DateTime(NOW))],
    ))
    .unwrap_err();
    assert_eq!(error.detail, r::detail::MEMORY_LIMIT);
}

#[test]
fn static_types_follow_datetime_values() {
    let infer = |expression: E| expression.value_type(&[]);
    assert_eq!(
        infer(E::Literal(V::DateTime(1))).unwrap(),
        r::ValueType::DateTime
    );
    for arguments in [
        vec![],
        vec![E::Literal(V::String("x".into()))],
        vec![E::Literal(V::DateTime(1))],
        vec![E::Parameter("p".into())],
        vec![E::Literal(V::Null)],
    ] {
        assert_eq!(
            infer(E::Function(F::DateTime, arguments)).unwrap(),
            r::ValueType::DateTime
        );
    }
    assert_eq!(
        infer(E::Function(F::Timestamp, vec![])).unwrap(),
        r::ValueType::Integer
    );
    let error = infer(E::Function(F::DateTime, vec![E::Literal(V::Integer(1))])).unwrap_err();
    assert_eq!(error.category, r::category::TYPE_ERROR);
    let error = infer(E::Unary(
        r::Unary::Negate,
        Box::new(E::Literal(V::DateTime(1))),
    ))
    .unwrap_err();
    assert_eq!(error.phase, r::ErrorPhase::Compile);
    // A datetime against text fails before anything runs when both types
    // are known, and runs, to fail on the values, when either is not.
    for op in [r::Binary::Less, r::Binary::Equal, r::Binary::Contains] {
        for (left, right) in [
            (
                E::Literal(V::DateTime(1)),
                E::Literal(V::String("x".into())),
            ),
            (
                E::Literal(V::String("x".into())),
                E::Function(F::DateTime, vec![]),
            ),
        ] {
            let error = infer(E::Binary(op, Box::new(left), Box::new(right))).unwrap_err();
            assert_eq!(
                (error.category.as_str(), error.detail.as_str(), error.phase),
                (
                    r::category::TYPE_ERROR,
                    r::detail::DATETIME_TEXT_COMPARISON,
                    r::ErrorPhase::Compile
                ),
                "{op:?}"
            );
        }
        assert_eq!(
            infer(E::Binary(
                op,
                Box::new(E::Literal(V::DateTime(1))),
                Box::new(E::Parameter("p".into())),
            ))
            .unwrap(),
            r::ValueType::Boolean
        );
    }
    // Conversions that are null where they cannot convert accept any type.
    for (function, expected) in [
        (F::ToStringOrNull, r::ValueType::String),
        (F::ToIntegerOrNull, r::ValueType::Integer),
        (F::ToFloatOrNull, r::ValueType::Float),
        (F::ToBooleanOrNull, r::ValueType::Boolean),
        (F::DateTimeOrNull, r::ValueType::DateTime),
    ] {
        for argument in [V::Integer(1), V::List(vec![]), V::DateTime(1)] {
            assert_eq!(
                infer(E::Function(function, vec![E::Literal(argument)])).unwrap(),
                expected,
                "{function:?}"
            );
        }
    }
    // requireDatetime admits only a datetime, or a value of unknown type.
    assert_eq!(
        infer(E::Function(
            F::RequireDateTime,
            vec![E::Parameter("p".into())]
        ))
        .unwrap(),
        r::ValueType::DateTime
    );
    for argument in [V::String("2024-01-01".into()), V::Map(BTreeMap::new())] {
        let error = infer(E::Function(F::RequireDateTime, vec![E::Literal(argument)])).unwrap_err();
        assert_eq!(
            (error.category.as_str(), error.phase),
            (r::category::TYPE_ERROR, r::ErrorPhase::Compile)
        );
    }
    // A datetime's fields are integers; a key it does not have fails before
    // anything runs.
    let property = |value: E, key: &str| infer(E::Property(Box::new(value), key.into()));
    assert_eq!(
        property(E::Function(F::DateTime, vec![]), "year").unwrap(),
        r::ValueType::Integer
    );
    let error = property(E::Literal(V::DateTime(1)), "Year").unwrap_err();
    assert_eq!(
        (error.category.as_str(), error.detail.as_str(), error.phase),
        (
            r::category::TYPE_ERROR,
            r::detail::INVALID_ARGUMENT_TYPE,
            r::ErrorPhase::Compile
        )
    );
    assert_eq!(
        property(E::Parameter("p".into()), "Year").unwrap(),
        r::ValueType::Any
    );
    let error = property(E::Literal(V::String("x".into())), "year").unwrap_err();
    assert_eq!(error.category, r::category::TYPE_ERROR);
}

#[test]
fn property_access_reads_datetime_fields() {
    let field = |value: V, key: &str| eval(&E::Property(Box::new(E::Literal(value)), key.into()));
    // NOW is 2023-11-14T22:13:20.123Z, a Tuesday.
    for (key, expected) in [
        ("year", 2023),
        ("quarter", 4),
        ("month", 11),
        ("week", 46),
        ("weekYear", 2023),
        ("day", 14),
        ("ordinalDay", 318),
        ("dayOfWeek", 2),
        ("hour", 22),
        ("minute", 13),
        ("second", 20),
        ("millisecond", 123),
        ("epochSeconds", 1_700_000_000),
        ("epochMillis", NOW),
    ] {
        assert_eq!(
            field(V::DateTime(NOW), key).unwrap(),
            V::Integer(expected),
            "{key}"
        );
    }
    // A parameter or a clock read gives the same fields.
    let year = |value: E| eval(&E::Property(Box::new(value), "year".into())).unwrap();
    assert_eq!(year(E::Parameter("at".into())), V::Integer(2023));
    assert_eq!(year(E::Function(F::DateTime, vec![])), V::Integer(2023));
    assert_eq!(field(V::Null, "year").unwrap(), V::Null);
    // Fields are integers, so they bucket and compute.
    assert_eq!(
        eval(&E::Binary(
            r::Binary::Subtract,
            Box::new(E::Property(
                Box::new(E::Literal(V::DateTime(NOW))),
                "epochMillis".into()
            )),
            Box::new(E::Property(
                Box::new(E::Literal(V::DateTime(NOW - 5))),
                "epochMillis".into()
            )),
        ))
        .unwrap(),
        V::Integer(5)
    );
    // A key a datetime does not have is a type error, not null.
    for key in ["Year", "years", "timezone", "nanosecond", ""] {
        let error = field(V::DateTime(NOW), key).unwrap_err();
        assert_eq!(
            (error.category.as_str(), error.detail.as_str(), error.phase),
            (
                r::category::TYPE_ERROR,
                r::detail::INVALID_ARGUMENT_TYPE,
                r::ErrorPhase::Runtime
            ),
            "{key}"
        );
    }
    // Past chrono's years only the epoch fields exist.
    assert_eq!(
        field(V::DateTime(i64::MAX), "epochMillis").unwrap(),
        V::Integer(i64::MAX)
    );
    assert_eq!(
        field(V::DateTime(i64::MIN), "epochSeconds").unwrap(),
        V::Integer(i64::MIN.div_euclid(1000))
    );
    let error = field(V::DateTime(i64::MAX), "year").unwrap_err();
    assert_eq!(
        (error.category.as_str(), error.detail.as_str()),
        (
            r::category::ARGUMENT_ERROR,
            r::detail::DATETIME_OUT_OF_RANGE
        )
    );
    // Other scalars still reject property access.
    for value in [V::Integer(1), V::String("x".into()), V::Boolean(true)] {
        let error = field(value.clone(), "year").unwrap_err();
        assert_eq!(error.category, r::category::TYPE_ERROR, "{value:?}");
    }
}

#[test]
fn datetimes_have_no_owned_payload_and_keep_memo_identity() {
    assert_eq!(V::DateTime(1).allocated_bytes(), size_of::<V>());
    V::List(vec![V::DateTime(1)]).validate_depth().unwrap();
    // Planner serialization is memo identity: a datetime literal never
    // collides with the integer of its millis.
    let datetime = serde_json::to_string(&V::DateTime(1)).unwrap();
    assert_ne!(datetime, serde_json::to_string(&V::Integer(1)).unwrap());
    assert_eq!(
        serde_json::from_str::<V>(&datetime).unwrap(),
        V::DateTime(1)
    );
}
