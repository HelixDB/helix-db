use super::*;

/// Literal domains drop repeats under query equality, keeping each first
/// representative with its own width, in written order, whatever their size.
#[test]
fn large_index_domains_keep_first_representatives_validation_and_order() {
    let leaves = vec![
        PropertyValue::Null,
        PropertyValue::Bool(true),
        PropertyValue::I64(1),
        PropertyValue::F64(1.0),
        PropertyValue::F32(1.0),
        PropertyValue::I64(i64::MAX),
        PropertyValue::F64(i64::MAX as f64),
        PropertyValue::DateTime(1),
        PropertyValue::F64(-0.0),
        PropertyValue::F64(0.0),
        PropertyValue::F32(-0.0),
        PropertyValue::F32(0.0),
        PropertyValue::String("1".into()),
        PropertyValue::Bytes(vec![1, 255]),
        PropertyValue::I64Array(vec![1]),
        PropertyValue::F64Array(vec![-0.0]),
        PropertyValue::F64Array(vec![0.0]),
        PropertyValue::F32Array(vec![1.0]),
        PropertyValue::StringArray(vec!["1".into()]),
        PropertyValue::F64(f64::NAN),
        PropertyValue::F32(f32::NAN),
        PropertyValue::F64Array(vec![f64::NAN]),
        PropertyValue::F32Array(vec![f32::NAN]),
    ];
    for offset in 0..leaves.len() {
        for size in [0, 1, 16, 17, leaves.len() * 4] {
            let input: Vec<_> = (0..size)
                .map(|index| leaves[(index + offset) % leaves.len()].clone())
                .collect();
            let expected = input
                .iter()
                .cloned()
                .map(|value| SecondaryIndexLiteral::new(value).unwrap())
                .fold(Vec::new(), |mut unique, value| {
                    let identity =
                        crate::analysis::scalar::property_value_identity(value.as_property_value());
                    if value.semantics()
                        != crate::ir::LiteralEqualityIndexValueSemantics::NonReflexive
                        && !unique.iter().any(|existing: &SecondaryIndexLiteral| {
                            identity.is_some()
                                && crate::analysis::scalar::property_value_identity(
                                    existing.as_property_value(),
                                ) == identity
                        })
                    {
                        unique.push(value);
                    }
                    unique
                });
            let actual = match literal_equality_set(&PropertyValue::Array(input)).unwrap() {
                EqualityIndexDomain::Empty => Vec::new(),
                EqualityIndexDomain::One(IndexValue::Literal(value)) => vec![value],
                EqualityIndexDomain::Many(values) => values.as_ref().to_vec(),
                _ => panic!("literal domains cannot contain runtime parameters"),
            };
            assert_eq!(actual, expected);
            assert_eq!(
                serde_json::to_string(&actual).unwrap(),
                serde_json::to_string(&expected).unwrap()
            );
        }
    }
    for nested in [PropertyValue::array([1]), PropertyValue::object([("x", 1)])] {
        let mut values = vec![PropertyValue::I64(1); 4096];
        values.push(nested);
        assert_eq!(literal_equality_set(&PropertyValue::Array(values)), None);
    }
}
