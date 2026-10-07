use super::*;

#[test]
fn scalar_intersections_preserve_legacy_identity_order_and_representatives() {
    let values = vec![
        PropertyValue::Null,
        PropertyValue::Bool(false),
        PropertyValue::Bool(true),
        PropertyValue::I64(i64::MIN),
        PropertyValue::I64(i64::MAX),
        PropertyValue::I64(0),
        PropertyValue::I64(1),
        PropertyValue::I64(9_007_199_254_740_993),
        PropertyValue::F64(9_007_199_254_740_992.0),
        PropertyValue::F64(i64::MAX as f64),
        PropertyValue::F64(-0.0),
        PropertyValue::F64(0.0),
        PropertyValue::F32(-0.0),
        PropertyValue::F64(1.0),
        PropertyValue::F32(1.0),
        PropertyValue::F64(f64::INFINITY),
        PropertyValue::F64(f64::NEG_INFINITY),
        PropertyValue::F64(f64::NAN),
        PropertyValue::F32(f32::NAN),
        PropertyValue::DateTime(1),
        PropertyValue::String("1".into()),
        PropertyValue::String("κλειδί".into()),
        PropertyValue::Bytes(vec![1]),
        PropertyValue::I64Array(vec![1]),
        PropertyValue::F64Array(vec![-0.0]),
        PropertyValue::F64Array(vec![0.0]),
        PropertyValue::F64Array(vec![f64::NAN]),
        PropertyValue::F32Array(vec![1.0]),
        PropertyValue::F32Array(vec![f32::NAN]),
        PropertyValue::StringArray(vec!["1".into()]),
        PropertyValue::array([PropertyValue::I64(1)]),
        PropertyValue::array([PropertyValue::F64(1.0)]),
        PropertyValue::array([PropertyValue::F64(f64::NAN)]),
        PropertyValue::object([("x", PropertyValue::I64(1))]),
        PropertyValue::object([("x", PropertyValue::F64(1.0))]),
        PropertyValue::object([("x", PropertyValue::F64(f64::NAN))]),
    ];
    for left_size in [0, 1, 16, 17, 128] {
        let left: Vec<_> = (0..left_size)
            .rev()
            .map(|index| values[index % values.len()].clone())
            .collect();
        for right_size in [0, 1, 16, 17, 128] {
            for offset in [0, 3, 11] {
                let right: Vec<_> = (0..right_size)
                    .map(|index| values[(index + offset) % values.len()].clone())
                    .collect();
                // Independent previous implementation: keep left payloads in
                // their original order using only the existing equality kernel.
                let expected: Vec<_> = left
                    .iter()
                    .filter(|value| {
                        right
                            .iter()
                            .any(|other| property_values_equal(value, other))
                    })
                    .cloned()
                    .collect();
                let actual = intersect_property_values(&left, &right);
                assert_eq!(actual, expected);
                assert_eq!(
                    serde_json::to_string(&actual).unwrap(),
                    serde_json::to_string(&expected).unwrap()
                );
            }
        }
    }
    let left = vec![
        PropertyValue::F64(-0.0),
        PropertyValue::I64(1),
        PropertyValue::array([PropertyValue::I64(1)]),
        PropertyValue::array([PropertyValue::F64(1.0)]),
    ];
    let right = vec![
        PropertyValue::I64(0),
        PropertyValue::F32(1.0),
        PropertyValue::array([PropertyValue::F64(1.0)]),
    ];
    assert_eq!(
        intersect_property_values(&left, &right),
        vec![left[0].clone(), left[1].clone(), left[3].clone()]
    );
}

#[test]
fn impossible_intersections_preserve_prior_constraint_state() {
    let existing: Vec<_> = (0..4096).rev().map(PropertyValue::I64).collect();
    let mut state = ScalarPropertyConstraint::default();
    assert!(!state.add_allowed_values(existing.clone()));
    assert!(!state.add_equality(PropertyValue::I64(7)));
    // Both empty overlap and overlap incompatible with another constraint must
    // leave the previous finite domain and equality available to the caller.
    for next in [
        Vec::new(),
        (4096..8192).map(PropertyValue::I64).collect(),
        (1024..5120).map(PropertyValue::I64).collect(),
    ] {
        assert!(state.add_allowed_values(next));
        assert_eq!(state.allowed_values.as_ref(), Some(&existing));
        assert_eq!(state.equality, Some(PropertyValue::I64(7)));
    }
    assert!(!state.add_allowed_values(vec![PropertyValue::F32(7.0), PropertyValue::I64(6)]));
    assert_eq!(
        state.allowed_values,
        Some(vec![PropertyValue::I64(7), PropertyValue::I64(6)])
    );
    assert!(!state.add_equality(PropertyValue::F64(7.0)));
    assert!(state.add_equality(PropertyValue::I64(6)));

    let mut nullable = ScalarPropertyConstraint::default();
    let existing = vec![PropertyValue::Null, PropertyValue::I64(1)];
    assert!(!nullable.add_allowed_values(existing.clone()));
    assert!(!nullable.add_nullability(NullabilityConstraint::NonNull));
    assert!(nullable.add_allowed_values(vec![PropertyValue::Null]));
    assert_eq!(nullable.allowed_values, Some(existing));
}
