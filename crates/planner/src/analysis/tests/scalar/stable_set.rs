use super::*;

#[test]
fn bounded_literal_sets_match_legacy_scalar_identity_and_first_representatives() {
    let leaves = vec![
        PropertyValue::Null,
        PropertyValue::Bool(false),
        PropertyValue::I64(0),
        PropertyValue::F64(-0.0),
        PropertyValue::F32(0.0),
        PropertyValue::I64(1),
        PropertyValue::F64(1.0),
        PropertyValue::F32(1.0),
        PropertyValue::I64(9_007_199_254_740_993),
        PropertyValue::F64(9_007_199_254_740_992.0),
        PropertyValue::I64(i64::MIN),
        PropertyValue::I64(i64::MAX),
        PropertyValue::F64(i64::MAX as f64),
        PropertyValue::DateTime(1),
        PropertyValue::F64(f64::NEG_INFINITY),
        PropertyValue::F64(f64::INFINITY),
        PropertyValue::String("1".into()),
        PropertyValue::String("κλειδί".into()),
        PropertyValue::Bytes(vec![1]),
        PropertyValue::I64Array(vec![1]),
        PropertyValue::F64Array(vec![-0.0]),
        PropertyValue::F64Array(vec![0.0]),
        PropertyValue::F32Array(vec![1.0]),
        PropertyValue::StringArray(vec!["1".into()]),
        PropertyValue::array([PropertyValue::I64(1)]),
        PropertyValue::array([PropertyValue::F64(1.0)]),
        PropertyValue::object([("x", PropertyValue::I64(1))]),
        PropertyValue::object([("x", PropertyValue::F64(1.0))]),
    ];
    for offset in 0..leaves.len() {
        for size in [0, 1, 16, 17, leaves.len() * 3] {
            let input: Vec<_> = (0..size)
                .map(|index| leaves[(index + offset) % leaves.len()].clone())
                .collect();
            let expected = input.iter().cloned().fold(Vec::new(), |mut unique, value| {
                if !unique
                    .iter()
                    .any(|previous| property_values_equal(previous, &value))
                {
                    unique.push(value);
                }
                unique
            });
            let actual = literal_collection_values(&PropertyValue::Array(input)).unwrap();
            assert_eq!(actual, expected);
            assert_eq!(
                serde_json::to_string(&actual).unwrap(),
                serde_json::to_string(&expected).unwrap()
            );
        }
    }
    // Existing proof rules retain non-reflexive typed arrays but reject scalar
    // NaN and nested heterogeneous NaN. Deduplication must not broaden proofs.
    let nan_array = PropertyValue::F64Array(vec![f64::NAN]);
    let input = PropertyValue::Array(vec![nan_array; 40]);
    assert_eq!(literal_collection_values(&input).unwrap().len(), 40);
    for invalid in [
        PropertyValue::F64(f64::NAN),
        PropertyValue::F32(f32::NAN),
        PropertyValue::array([PropertyValue::F64(f64::NAN)]),
        PropertyValue::object([("x", PropertyValue::F64(f64::NAN))]),
    ] {
        let mut input = vec![PropertyValue::I64(1); 40];
        input.push(invalid);
        assert!(literal_collection_values(&PropertyValue::Array(input)).is_none());
    }
    for input in [
        PropertyValue::I64Array(vec![1; 4096]),
        PropertyValue::F64Array(vec![1.0; 4096]),
        PropertyValue::F32Array(vec![1.0; 4096]),
        PropertyValue::StringArray(vec!["x".repeat(1024); 4096]),
    ] {
        let values = literal_collection_values(&input).unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(
            values.capacity(),
            1,
            "large duplicate lists retain no spare payload slots"
        );
    }
}
