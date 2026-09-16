use super::*;
use helix_ast::expr::{Expr, Predicate};

#[test]
fn borrowed_membership_preserves_the_original_owned_truth_proof() {
    let leaves = vec![
        PropertyValue::Null,
        PropertyValue::Bool(false),
        PropertyValue::Bool(true),
        PropertyValue::I64(0),
        PropertyValue::I64(1),
        PropertyValue::I64(i64::MIN),
        PropertyValue::I64(i64::MAX),
        PropertyValue::I64(9_007_199_254_740_993),
        PropertyValue::F64(9_007_199_254_740_992.0),
        PropertyValue::F64(i64::MAX as f64),
        PropertyValue::F64(-0.0),
        PropertyValue::F64(1.0),
        PropertyValue::F64(f64::NEG_INFINITY),
        PropertyValue::F64(f64::INFINITY),
        PropertyValue::F64(f64::NAN),
        PropertyValue::F32(-0.0),
        PropertyValue::F32(1.0),
        PropertyValue::F32(f32::NEG_INFINITY),
        PropertyValue::F32(f32::INFINITY),
        PropertyValue::F32(f32::NAN),
        PropertyValue::DateTime(1),
        PropertyValue::String(String::new()),
        PropertyValue::String("1".into()),
        PropertyValue::String("κλειδί🙂".into()),
        PropertyValue::Bytes(vec![1]),
        PropertyValue::I64Array(vec![1]),
        PropertyValue::F64Array(vec![-0.0]),
        PropertyValue::F64Array(vec![0.0]),
        PropertyValue::F64Array(vec![f64::NAN]),
        PropertyValue::F32Array(vec![1.0]),
        PropertyValue::F32Array(vec![f32::NAN]),
        PropertyValue::StringArray(vec!["1".into()]),
        PropertyValue::Array(vec![PropertyValue::I64(1)]),
        PropertyValue::Array(vec![PropertyValue::F64(1.0)]),
        PropertyValue::Array(vec![PropertyValue::F64(f64::NAN)]),
        PropertyValue::object([("x", PropertyValue::I64(1))]),
        PropertyValue::object([("x", PropertyValue::F64(1.0))]),
        PropertyValue::object([("x", PropertyValue::F64(f64::NAN))]),
    ];
    let eligible: Vec<_> = leaves
        .iter()
        .filter(|value| property_value_has_reflexive_equality(value))
        .cloned()
        .collect();
    let mut collections = leaves.clone(); // Non-collections keep returning None.
    collections.extend([
        PropertyValue::I64Array(vec![]),
        PropertyValue::I64Array(vec![0, 1, i64::MIN, i64::MAX, 9_007_199_254_740_993]),
        PropertyValue::F64Array(vec![]),
        PropertyValue::F64Array(vec![-0.0, 1.0, f64::NEG_INFINITY, f64::INFINITY]),
        PropertyValue::F64Array(vec![1.0, f64::NAN]),
        PropertyValue::F32Array(vec![]),
        PropertyValue::F32Array(vec![-0.0, 1.0, f32::NEG_INFINITY, f32::INFINITY]),
        PropertyValue::F32Array(vec![1.0, f32::NAN]),
        PropertyValue::StringArray(vec![]),
        PropertyValue::StringArray(vec![String::new(), "1".into(), "κλειδί🙂".into()]),
        PropertyValue::Array(vec![]),
        PropertyValue::Array(leaves.clone()),
    ]);
    for offset in 0..eligible.len() {
        for length in [1, 2, 16, 17, eligible.len() * 2] {
            collections.push(PropertyValue::Array(
                (0..length)
                    .map(|index| eligible[(index + offset) % eligible.len()].clone())
                    .collect(),
            ));
        }
    }
    for values in collections {
        for needle in &leaves {
            // Original proof: validate, own and deduplicate, then test equality.
            // It stays separate from the borrowed membership implementation.
            let expected = property_value_has_reflexive_equality(needle)
                .then(|| {
                    literal_collection_values(&values).map(|values| {
                        values
                            .iter()
                            .any(|item| property_values_equal(item, needle))
                    })
                })
                .flatten();
            let predicate = Predicate::IsIn {
                value: Expr::Constant(needle.clone()),
                values: Expr::Constant(values.clone()),
            };
            assert_eq!(
                super::super::truth::static_predicate_value(&predicate),
                expected,
                "needle={needle:?}, values={values:?}"
            );
        }
    }
}

#[test]
fn membership_keeps_native_identity_and_complete_collection_validation() {
    for (needle, values, expected) in [
        (
            PropertyValue::Null,
            PropertyValue::Array(vec![PropertyValue::Null]),
            Some(true),
        ),
        (
            PropertyValue::F64(-0.0),
            PropertyValue::I64Array(vec![0]),
            Some(true),
        ),
        (
            PropertyValue::I64(9_007_199_254_740_993),
            PropertyValue::F64Array(vec![9_007_199_254_740_992.0]),
            Some(false),
        ),
        (
            PropertyValue::DateTime(1),
            PropertyValue::I64Array(vec![1]),
            Some(false),
        ),
        (
            PropertyValue::Array(vec![PropertyValue::I64(1)]),
            PropertyValue::Array(vec![PropertyValue::Array(vec![PropertyValue::F64(1.0)])]),
            Some(false),
        ),
        (
            PropertyValue::I64Array(vec![1]),
            PropertyValue::Array(vec![PropertyValue::F64Array(vec![1.0])]),
            Some(false),
        ),
        (
            PropertyValue::I64(1),
            PropertyValue::Array(vec![PropertyValue::I64(1), PropertyValue::F64(f64::NAN)]),
            None,
        ),
        (
            PropertyValue::I64(1),
            PropertyValue::F64Array(vec![1.0, f64::NAN]),
            None,
        ),
        (
            PropertyValue::I64(1),
            PropertyValue::F32Array(vec![1.0, f32::NAN]),
            None,
        ),
        (
            PropertyValue::I64(1),
            PropertyValue::Array(vec![
                PropertyValue::I64(1),
                PropertyValue::object([("x", PropertyValue::F64(f64::NAN))]),
            ]),
            None,
        ),
        (
            PropertyValue::F64(f64::NAN),
            PropertyValue::Array(vec![]),
            None,
        ),
        (PropertyValue::Null, PropertyValue::Null, None),
        (
            PropertyValue::F64Array(vec![f64::NAN]),
            PropertyValue::Array(vec![PropertyValue::F64Array(vec![f64::NAN])]),
            Some(false),
        ),
    ] {
        assert_eq!(
            super::super::truth::static_predicate_value(&Predicate::IsIn {
                value: Expr::Constant(needle),
                values: Expr::Constant(values)
            }),
            expected
        );
    }
}
