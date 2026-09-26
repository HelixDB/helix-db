use super::*;

#[test]
fn integer_average_keeps_exact_total_before_division() {
    assert_eq!(Average::new().finish(), Value::Null);
    for (values, expected) in [
        (vec![i64::MAX; 2], i64::MAX as f64),
        (vec![i64::MIN; 2], i64::MIN as f64),
        (vec![i64::MAX, i64::MAX, -i64::MAX, -i64::MAX], 0.0),
        (vec![i64::MAX, i64::MIN], -0.5),
        (vec![1, 2, 3], 2.0),
    ] {
        let state = values
            .into_iter()
            .try_fold(Average::new(), |state, value| {
                state.next(&Value::Integer(value))
            })
            .unwrap();
        assert_eq!(state.finish(), Value::Float(expected));
    }
}

#[test]
fn repeated_integer_average_retains_the_total_rounding_residue() {
    for value in [
        i64::MAX,
        i64::MIN,
        (1_i64 << 53) - 1,
        (1_i64 << 53) + 1,
        -((1_i64 << 53) - 1),
        -((1_i64 << 53) + 1),
        (1_i64 << 62) - 1,
        -((1_i64 << 62) - 1),
    ] {
        for count in [3, 7, 31, 101, 513, 4097] {
            let state = std::iter::repeat_n(value, count)
                .try_fold(Average::new(), |state, value| {
                    state.next(&Value::Integer(value))
                })
                .unwrap();
            assert_eq!(
                state.finish(),
                Value::Float(value as f64),
                "{value} x {count}"
            );
        }
    }
}

#[test]
fn finite_averages_survive_overflow_and_retain_cancellation_residues() {
    for (values, expected) in [
        (vec![Value::Float(1e308); 2], 1e308),
        (vec![Value::Float(0.1); 3], 0.1),
        (vec![Value::Float(-0.1); 101], -0.1),
        (
            vec![Value::Float(1.5426589869257454e-133); 101],
            1.5426589869257454e-133,
        ),
        (
            vec![
                Value::Float(1e308),
                Value::Float(1e308),
                Value::Float(-1e308),
                Value::Float(-1e308),
            ],
            0.0,
        ),
        (vec![Value::Float(f64::MAX); 101], f64::MAX),
        (vec![Value::Float(-f64::MAX); 101], -f64::MAX),
        (vec![Value::Float(f64::from_bits(1)); 3], f64::from_bits(1)),
        (vec![Value::Float(-0.0), Value::Float(0.0)], 0.0),
        (
            vec![Value::Float(1e16), Value::Float(1.0), Value::Float(-1e16)],
            1.0 / 3.0,
        ),
        (
            vec![
                Value::Integer(i64::MAX),
                Value::Float(1.0),
                Value::Integer(-i64::MAX),
            ],
            1.0 / 3.0,
        ),
        (
            vec![
                Value::Float(1.0),
                Value::Integer(i64::MAX),
                Value::Integer(-i64::MAX),
            ],
            1.0 / 3.0,
        ),
        (
            vec![
                Value::Integer(i64::MAX),
                Value::Integer(i64::MAX),
                Value::Float(-(i64::MAX as f64)),
                Value::Float(-(i64::MAX as f64)),
            ],
            -0.5,
        ),
        (
            vec![Value::Float(1.0), Value::Float(2.0), Value::Float(3.0)],
            2.0,
        ),
    ] {
        let state = values
            .into_iter()
            .try_fold(Average::new(), |state, value| state.next(&value))
            .unwrap();
        assert_eq!(state.finish(), Value::Float(expected));
    }
}

#[test]
fn nonfinite_inputs_preserve_ieee_results() {
    for (values, expected) in [
        (vec![f64::INFINITY, 1.0], f64::INFINITY),
        (vec![1.0, f64::NEG_INFINITY], f64::NEG_INFINITY),
        (vec![f64::INFINITY, f64::NEG_INFINITY], f64::NAN),
        (vec![f64::NAN, 1.0], f64::NAN),
        (vec![1.0, f64::NAN], f64::NAN),
    ] {
        let state = values
            .into_iter()
            .try_fold(Average::new(), |state, value| {
                state.next(&Value::Float(value))
            })
            .unwrap();
        let Value::Float(actual) = state.finish() else {
            panic!("numeric average")
        };
        if expected.is_nan() {
            assert!(actual.is_nan());
        } else {
            assert_eq!(actual, expected);
        }
    }
}

#[test]
fn rejected_types_and_count_overflow_leave_state_unchanged() {
    let state = Average::new().next(&Value::Integer(7)).unwrap();
    for value in [
        Value::Null,
        Value::Boolean(true),
        Value::String("7".into()),
        Value::List(vec![]),
    ] {
        let error = state.next(&value).unwrap_err();
        assert_eq!(
            (error.category.as_str(), error.detail.as_str()),
            ("TypeError", "InvalidArgumentType")
        );
        assert_eq!(state.finish(), Value::Float(7.0));
    }
    let state = Average {
        total: Total::Integer(i128::from(i64::MAX).to_ne_bytes()),
        count: i64::MAX,
    };
    let error = state.next(&Value::Integer(1)).unwrap_err();
    assert_eq!(
        (error.category.as_str(), error.detail.as_str()),
        ("ArithmeticError", "NumberOutOfRange")
    );
    assert_eq!(state.finish(), Value::Float(1.0));
}
