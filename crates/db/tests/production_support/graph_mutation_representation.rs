//! Exact V1 graph-property representation contracts.
//!
//! This harness remains outside the measured production tree while exercising
//! every production representation branch through the feature-gated crate
//! boundary. It deliberately constructs only existing V1 values and bytes.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::encoding::property::property_value::PropertyValue;
use crate::encoding::property::Property;
use crate::encoding::v2::keys::scope::DataScope;
use crate::index_lifecycle::graph_mutation::{
    CanonicalPropertyRow, GraphEntity, GraphMutationTransition, PropertyEdit, PropertyEditOutcome,
};

/// Proves persisted representation identity is recursive and bit exact.
pub fn graph_mutation_representation_contracts() {
    let f64_nan = f64::from_bits(0x7ff8_0000_0000_0042);
    let other_f64_nan = f64::from_bits(f64_nan.to_bits().saturating_add(1));
    let f32_nan = f32::from_bits(0x7fc0_0042);
    let other_f32_nan = f32::from_bits(f32_nan.to_bits().saturating_add(1));

    for (left, right) in [
        (PropertyValue::Null, PropertyValue::Null),
        (PropertyValue::Bool(true), PropertyValue::Bool(true)),
        (PropertyValue::I64(7), PropertyValue::I64(7)),
        (PropertyValue::DateTime(7), PropertyValue::DateTime(7)),
        (PropertyValue::F64(f64_nan), PropertyValue::F64(f64_nan)),
        (PropertyValue::F32(f64_nan), PropertyValue::F32(f64_nan)),
        (
            PropertyValue::String("value".to_owned()),
            PropertyValue::String("value".to_owned()),
        ),
        (
            PropertyValue::Bytes(vec![1, 2]),
            PropertyValue::Bytes(vec![1, 2]),
        ),
        (
            PropertyValue::I64Array(vec![1, 2]),
            PropertyValue::I64Array(vec![1, 2]),
        ),
        (
            PropertyValue::F64Array(vec![-0.0, f64_nan]),
            PropertyValue::F64Array(vec![-0.0, f64_nan]),
        ),
        (
            PropertyValue::F32Array(vec![-0.0, f32_nan]),
            PropertyValue::F32Array(vec![-0.0, f32_nan]),
        ),
        (
            PropertyValue::StringArray(vec!["value".to_owned()]),
            PropertyValue::StringArray(vec!["value".to_owned()]),
        ),
        (
            PropertyValue::Array(vec![PropertyValue::F64(f64_nan)]),
            PropertyValue::Array(vec![PropertyValue::F64(f64_nan)]),
        ),
        (
            PropertyValue::Object(BTreeMap::from([(
                "value".to_owned(),
                PropertyValue::F64(f64_nan),
            )])),
            PropertyValue::Object(BTreeMap::from([(
                "value".to_owned(),
                PropertyValue::F64(f64_nan),
            )])),
        ),
    ] {
        assert!(left.same_v1_representation(&right));
    }

    for (left, right) in [
        (PropertyValue::Bool(true), PropertyValue::Bool(false)),
        (PropertyValue::I64(7), PropertyValue::I64(8)),
        (PropertyValue::DateTime(7), PropertyValue::DateTime(8)),
        (
            PropertyValue::F64(f64_nan),
            PropertyValue::F64(other_f64_nan),
        ),
        (PropertyValue::F32(-0.0), PropertyValue::F32(0.0)),
        (
            PropertyValue::String("left".to_owned()),
            PropertyValue::String("right".to_owned()),
        ),
        (PropertyValue::Bytes(vec![1]), PropertyValue::Bytes(vec![2])),
        (
            PropertyValue::I64Array(vec![1]),
            PropertyValue::I64Array(vec![2]),
        ),
        (
            PropertyValue::F64Array(vec![f64_nan]),
            PropertyValue::F64Array(vec![other_f64_nan]),
        ),
        (
            PropertyValue::F64Array(vec![f64_nan]),
            PropertyValue::F64Array(vec![f64_nan, f64_nan]),
        ),
        (
            PropertyValue::F32Array(vec![f32_nan]),
            PropertyValue::F32Array(vec![other_f32_nan]),
        ),
        (
            PropertyValue::F32Array(vec![f32_nan]),
            PropertyValue::F32Array(vec![f32_nan, f32_nan]),
        ),
        (
            PropertyValue::StringArray(vec!["left".to_owned()]),
            PropertyValue::StringArray(vec!["right".to_owned()]),
        ),
        (
            PropertyValue::Array(vec![PropertyValue::I64(1)]),
            PropertyValue::Array(vec![PropertyValue::I64(2)]),
        ),
        (
            PropertyValue::Array(vec![PropertyValue::I64(1)]),
            PropertyValue::Array(vec![PropertyValue::I64(1), PropertyValue::I64(2)]),
        ),
        (
            PropertyValue::Object(BTreeMap::from([("left".to_owned(), PropertyValue::I64(1))])),
            PropertyValue::Object(BTreeMap::from([(
                "right".to_owned(),
                PropertyValue::I64(1),
            )])),
        ),
        (
            PropertyValue::Object(BTreeMap::from([(
                "value".to_owned(),
                PropertyValue::I64(1),
            )])),
            PropertyValue::Object(BTreeMap::from([(
                "value".to_owned(),
                PropertyValue::I64(2),
            )])),
        ),
        (
            PropertyValue::Object(BTreeMap::from([(
                "value".to_owned(),
                PropertyValue::I64(1),
            )])),
            PropertyValue::Object(BTreeMap::new()),
        ),
        (PropertyValue::Null, PropertyValue::Bool(false)),
    ] {
        assert!(!left.same_v1_representation(&right));
    }

    let exact = Property::new("score", PropertyValue::F64(f64_nan));
    assert!(exact.same_v1_representation(&exact));
    assert!(!exact.same_v1_representation(&Property::new("other", PropertyValue::F64(f64_nan),)));
    assert!(
        !exact.same_v1_representation(&Property::new("score", PropertyValue::F64(other_f64_nan),))
    );

    let scope = DataScope::LegacyUnscoped;
    let entity = GraphEntity::node(1);
    let row = CanonicalPropertyRow::new(vec![exact.clone()]);
    assert!(matches!(
        GraphMutationTransition::edit(scope, entity, row.clone(), PropertyEdit::set(exact.clone()),),
        PropertyEditOutcome::Unchanged(_)
    ));
    assert!(matches!(
        GraphMutationTransition::edit(
            scope,
            entity,
            row,
            PropertyEdit::set(Property::new("score", PropertyValue::F64(other_f64_nan),)),
        ),
        PropertyEditOutcome::Changed(_)
    ));

    query_value_semantics_are_total_and_type_aware();
}

/// Exercises every production query-value domain without compiling unit-test cfg.
fn query_value_semantics_are_total_and_type_aware() {
    let nan = f64::from_bits(0x7ff8_0000_0000_0042);
    let other_nan = f64::from_bits(0x7ff8_0000_0000_0043);

    for (left, right, expected) in [
        (
            PropertyValue::I64(7),
            PropertyValue::F64(7.0),
            Some(Ordering::Equal),
        ),
        (
            PropertyValue::F32(6.0),
            PropertyValue::I64(7),
            Some(Ordering::Less),
        ),
        (
            PropertyValue::F64(8.0),
            PropertyValue::F32(7.0),
            Some(Ordering::Greater),
        ),
        (
            PropertyValue::DateTime(1),
            PropertyValue::DateTime(2),
            Some(Ordering::Less),
        ),
        (
            PropertyValue::String("right".to_owned()),
            PropertyValue::String("left".to_owned()),
            Some(Ordering::Greater),
        ),
        (PropertyValue::F64(nan), PropertyValue::I64(7), None),
        (PropertyValue::Null, PropertyValue::Null, None),
    ] {
        assert_eq!(left.compare(&right), expected);
    }

    let ordered_domains = [
        PropertyValue::Null,
        PropertyValue::Bool(false),
        PropertyValue::I64(0),
        PropertyValue::DateTime(0),
        PropertyValue::String(String::new()),
        PropertyValue::Bytes(Vec::new()),
        PropertyValue::I64Array(Vec::new()),
        PropertyValue::F64Array(Vec::new()),
        PropertyValue::F32Array(Vec::new()),
        PropertyValue::StringArray(Vec::new()),
        PropertyValue::Array(Vec::new()),
        PropertyValue::Object(BTreeMap::new()),
    ];
    for pair in ordered_domains.windows(2) {
        assert_eq!(pair[0].total_order(&pair[1]), Ordering::Less);
        assert_eq!(pair[1].total_order(&pair[0]), Ordering::Greater);
    }

    for (left, right, expected) in [
        (PropertyValue::Null, PropertyValue::Null, Ordering::Equal),
        (
            PropertyValue::Bool(false),
            PropertyValue::Bool(true),
            Ordering::Less,
        ),
        (
            PropertyValue::I64(1),
            PropertyValue::F64(1.0),
            Ordering::Equal,
        ),
        (
            PropertyValue::F64(nan),
            PropertyValue::F64(other_nan),
            Ordering::Less,
        ),
        (
            PropertyValue::F64(nan),
            PropertyValue::I64(1),
            Ordering::Greater,
        ),
        (
            PropertyValue::I64(1),
            PropertyValue::F64(nan),
            Ordering::Less,
        ),
        (
            PropertyValue::DateTime(1),
            PropertyValue::DateTime(2),
            Ordering::Less,
        ),
        (
            PropertyValue::String("a".to_owned()),
            PropertyValue::String("b".to_owned()),
            Ordering::Less,
        ),
        (
            PropertyValue::Bytes(vec![1]),
            PropertyValue::Bytes(vec![2]),
            Ordering::Less,
        ),
        (
            PropertyValue::I64Array(vec![1]),
            PropertyValue::I64Array(vec![2]),
            Ordering::Less,
        ),
        (
            PropertyValue::F64Array(vec![1.0, 2.0]),
            PropertyValue::F64Array(vec![1.0, 3.0]),
            Ordering::Less,
        ),
        (
            PropertyValue::F64Array(vec![1.0]),
            PropertyValue::F64Array(vec![1.0, 2.0]),
            Ordering::Less,
        ),
        (
            PropertyValue::F32Array(vec![1.0, 2.0]),
            PropertyValue::F32Array(vec![1.0, 3.0]),
            Ordering::Less,
        ),
        (
            PropertyValue::F32Array(vec![1.0]),
            PropertyValue::F32Array(vec![1.0, 2.0]),
            Ordering::Less,
        ),
        (
            PropertyValue::StringArray(vec!["a".to_owned()]),
            PropertyValue::StringArray(vec!["b".to_owned()]),
            Ordering::Less,
        ),
        (
            PropertyValue::Array(vec![PropertyValue::I64(1)]),
            PropertyValue::Array(vec![PropertyValue::I64(2)]),
            Ordering::Less,
        ),
        (
            PropertyValue::Array(vec![PropertyValue::I64(1)]),
            PropertyValue::Array(vec![PropertyValue::I64(1), PropertyValue::I64(2)]),
            Ordering::Less,
        ),
        (
            PropertyValue::Object(BTreeMap::from([("a".to_owned(), PropertyValue::I64(1))])),
            PropertyValue::Object(BTreeMap::from([("b".to_owned(), PropertyValue::I64(1))])),
            Ordering::Less,
        ),
        (
            PropertyValue::Object(BTreeMap::from([("a".to_owned(), PropertyValue::I64(1))])),
            PropertyValue::Object(BTreeMap::from([("a".to_owned(), PropertyValue::I64(2))])),
            Ordering::Less,
        ),
        (
            PropertyValue::Object(BTreeMap::new()),
            PropertyValue::Object(BTreeMap::from([("a".to_owned(), PropertyValue::I64(1))])),
            Ordering::Less,
        ),
    ] {
        assert_eq!(left.total_order(&right), expected);
    }

    for (left, right, expected) in [
        (PropertyValue::Null, PropertyValue::Null, true),
        (PropertyValue::Bool(true), PropertyValue::Bool(true), true),
        (PropertyValue::I64(7), PropertyValue::F64(7.0), true),
        (PropertyValue::F32(7.0), PropertyValue::F64(7.0), true),
        (PropertyValue::DateTime(7), PropertyValue::DateTime(7), true),
        (
            PropertyValue::String("value".to_owned()),
            PropertyValue::String("value".to_owned()),
            true,
        ),
        (
            PropertyValue::Bytes(vec![1]),
            PropertyValue::Bytes(vec![1]),
            true,
        ),
        (
            PropertyValue::I64Array(vec![1]),
            PropertyValue::I64Array(vec![1]),
            true,
        ),
        (
            PropertyValue::F64Array(vec![1.0]),
            PropertyValue::F64Array(vec![1.0]),
            true,
        ),
        (
            PropertyValue::F64Array(vec![nan]),
            PropertyValue::F64Array(vec![nan]),
            false,
        ),
        (
            PropertyValue::F64Array(vec![1.0]),
            PropertyValue::F64Array(vec![1.0, 2.0]),
            false,
        ),
        (
            PropertyValue::F32Array(vec![1.0]),
            PropertyValue::F32Array(vec![1.0]),
            true,
        ),
        (
            PropertyValue::F32Array(vec![f32::NAN]),
            PropertyValue::F32Array(vec![f32::NAN]),
            false,
        ),
        (
            PropertyValue::F32Array(vec![1.0]),
            PropertyValue::F32Array(vec![1.0, 2.0]),
            false,
        ),
        (
            PropertyValue::StringArray(vec!["value".to_owned()]),
            PropertyValue::StringArray(vec!["value".to_owned()]),
            true,
        ),
        (
            PropertyValue::Array(vec![PropertyValue::I64(1)]),
            PropertyValue::Array(vec![PropertyValue::I64(1)]),
            true,
        ),
        (
            PropertyValue::Object(BTreeMap::new()),
            PropertyValue::Object(BTreeMap::new()),
            true,
        ),
        (PropertyValue::Null, PropertyValue::Bool(false), false),
    ] {
        assert_eq!(left.eq_value(&right), expected);
    }

    assert_eq!(
        PropertyValue::String("value".to_owned()).as_str(),
        Some("value")
    );
    assert_eq!(PropertyValue::Null.as_str(), None);
    assert_eq!(PropertyValue::I64(7).as_i64(), Some(7));
    assert_eq!(PropertyValue::Null.as_i64(), None);
    assert_eq!(PropertyValue::DateTime(7).as_datetime_millis(), Some(7));
    assert_eq!(PropertyValue::Null.as_datetime_millis(), None);
    assert_eq!(PropertyValue::I64(7).as_f64(), Some(7.0));
    assert_eq!(PropertyValue::F32(7.0).as_f64(), Some(7.0));
    assert_eq!(PropertyValue::F64(7.0).as_f64(), Some(7.0));
    assert_eq!(PropertyValue::Null.as_f64(), None);
    assert_eq!(PropertyValue::Bool(true).as_bool(), Some(true));
    assert_eq!(PropertyValue::Null.as_bool(), None);

    let values = [
        PropertyValue::Null,
        PropertyValue::Bool(true),
        PropertyValue::I64(-7),
        PropertyValue::DateTime(0),
        PropertyValue::F64(1.5),
        PropertyValue::F32(1.5),
        PropertyValue::String("value".to_owned()),
        PropertyValue::Bytes(vec![1, 2]),
        PropertyValue::I64Array(vec![1, 2]),
        PropertyValue::F64Array(vec![1.0, 2.0]),
        PropertyValue::F32Array(vec![1.0, 2.0]),
        PropertyValue::StringArray(vec!["value".to_owned()]),
        PropertyValue::Array(vec![PropertyValue::I64(1)]),
        PropertyValue::Object(BTreeMap::from([(
            "value".to_owned(),
            PropertyValue::I64(1),
        )])),
    ];
    for value in &values {
        assert!(!value.to_index_string().is_empty());
        assert!(!value.to_string().is_empty());
        serde_json::to_value(value).expect("property value serializes");
    }
    assert_eq!(
        PropertyValue::DateTime(i64::MAX).to_string(),
        i64::MAX.to_string()
    );
    assert!(serde_json::to_value(PropertyValue::DateTime(i64::MAX)).is_err());

    let datetime = chrono::DateTime::from_timestamp_millis(0).expect("epoch is representable");
    let from_values = [
        PropertyValue::from("borrowed"),
        PropertyValue::from("owned".to_owned()),
        PropertyValue::from(&"referenced".to_owned()),
        PropertyValue::from(7_i32),
        PropertyValue::from(7_i64),
        PropertyValue::from(7.0_f64),
        PropertyValue::from(true),
        PropertyValue::from(vec![1_u8]),
        PropertyValue::from(vec![1_i64]),
        PropertyValue::from(vec![1.0_f64]),
        PropertyValue::from(vec!["value".to_owned()]),
        PropertyValue::from(vec![PropertyValue::Null]),
        PropertyValue::from(BTreeMap::from([("value".to_owned(), PropertyValue::Null)])),
        PropertyValue::from(datetime),
    ];
    assert_eq!(from_values.len(), 14);
}
