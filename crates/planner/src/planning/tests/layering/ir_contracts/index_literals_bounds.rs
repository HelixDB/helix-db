use super::*;

#[test]
fn secondary_index_literals_reject_null_and_nested_values() {
    let literal = SecondaryIndexLiteral::new(PropertyValue::from("alice")).unwrap();
    assert_eq!(
        literal,
        SecondaryIndexLiteral::new(PropertyValue::from("alice")).unwrap()
    );
    assert_eq!(
        SecondaryIndexLiteral::new(PropertyValue::array([1])),
        Err(SecondaryIndexLiteralError::NestedValue)
    );
    assert_eq!(
        SecondaryIndexLiteral::new(PropertyValue::object([("nested", 1)])),
        Err(SecondaryIndexLiteralError::NestedValue)
    );
    assert_eq!(
        SecondaryIndexLiteral::new(PropertyValue::Null),
        Err(SecondaryIndexLiteralError::NullValue)
    );
    assert_eq!(
        SecondaryIndexLiteral::new(PropertyValue::from("null")),
        Err(SecondaryIndexLiteralError::AmbiguousNullString)
    );

    assert_eq!(
        serde_json::to_string(&IndexValue::Literal(literal)).unwrap(),
        r#"{"literal":{"string":"alice"}}"#
    );
    assert_eq!(
        serde_json::from_str::<IndexValue>(r#"{"literal":{"i64":42}}"#).unwrap(),
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from(42)).unwrap())
    );
    assert!(serde_json::from_str::<IndexValue>(r#"{"literal":{"array":[{"i64":1}]}}"#).is_err());
    assert!(serde_json::from_str::<IndexValue>(r#"{"literal":null}"#).is_err());
    assert!(serde_json::from_str::<IndexValue>(r#"{"literal":{"string":"null"}}"#).is_err());
    assert!(
        serde_json::from_str::<IndexValue>(r#"{"literal":{"object":{"nested":{"i64":1}}}}"#)
            .is_err()
    );
    assert!(serde_json::from_str::<SecondaryIndexLiteral>(r#"{"unknown":1}"#).is_err());
}

#[test]
fn index_bounds_encode_inclusivity_as_variants() {
    assert_eq!(
        RangeIndexValue::literal(PropertyValue::from(18)),
        Some(RangeIndexValue::Literal(RangeIndexLiteral::I64(18)))
    );
    assert_eq!(
        RangeIndexValue::literal(PropertyValue::datetime_millis(1_700_000_000_000)),
        Some(RangeIndexValue::Literal(RangeIndexLiteral::DateTime(
            1_700_000_000_000
        )))
    );
    assert_eq!(
        RangeIndexValue::literal(PropertyValue::from(12.5_f64)),
        Some(RangeIndexValue::Literal(RangeIndexLiteral::F64(
            RangeIndexF64::new(12.5).unwrap()
        )))
    );
    assert_eq!(
        RangeIndexValue::literal(PropertyValue::from(7.25_f32)),
        Some(RangeIndexValue::Literal(RangeIndexLiteral::F32(
            RangeIndexF32::new(7.25).unwrap()
        )))
    );
    assert!(RangeIndexValue::literal(PropertyValue::from(f64::NAN)).is_none());
    assert!(RangeIndexValue::literal(PropertyValue::from(f32::NAN)).is_none());
    assert_eq!(
        serde_json::to_string(&RangeIndexLiteral::F64(RangeIndexF64::new(12.5).unwrap())).unwrap(),
        r#"{"f64":12.5}"#
    );
    assert_eq!(
        serde_json::to_string(&RangeIndexLiteral::F32(RangeIndexF32::new(7.25).unwrap())).unwrap(),
        r#"{"f32":7.25}"#
    );
    let parsed_f64: RangeIndexF64 = serde_json::from_str("12.5").unwrap();
    let parsed_f32: RangeIndexF32 = serde_json::from_str("7.25").unwrap();
    assert_eq!(parsed_f64, RangeIndexF64::new(12.5).unwrap());
    assert_eq!(parsed_f32, RangeIndexF32::new(7.25).unwrap());
    assert!(serde_json::from_str::<RangeIndexF64>(r#""nan""#).is_err());
    assert!(serde_json::from_str::<RangeIndexF32>(r#""nan""#).is_err());
    assert!(<RangeIndexF64 as serde::Deserialize>::deserialize(
        serde::de::value::F64Deserializer::<serde::de::value::Error>::new(f64::NAN),
    )
    .is_err());
    assert!(<RangeIndexF32 as serde::Deserialize>::deserialize(
        serde::de::value::F32Deserializer::<serde::de::value::Error>::new(f32::NAN),
    )
    .is_err());
    assert_eq!(
        RangeIndexValue::literal(PropertyValue::from("alice")),
        Some(RangeIndexValue::Literal(RangeIndexLiteral::String(
            "alice".to_string()
        )))
    );
    assert_eq!(
        RangeIndexValue::param("limit"),
        Some(RangeIndexValue::Param(
            NonEmptyString::new("limit").unwrap()
        ))
    );
    assert!(RangeIndexValue::param("").is_none());

    for value in [
        PropertyValue::Null,
        PropertyValue::from(true),
        PropertyValue::from(vec![1_u8, 2]),
        PropertyValue::from(vec![1_i64, 2]),
        PropertyValue::from(vec![1.0_f64, 2.0]),
        PropertyValue::from(vec![1.0_f32, 2.0]),
        PropertyValue::from(vec!["a".to_string(), "b".to_string()]),
        PropertyValue::array([1, 2]),
        PropertyValue::object([("nested", 1)]),
    ] {
        assert!(RangeIndexValue::literal(value).is_none());
    }

    let inclusive =
        IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(18)).unwrap());
    let exclusive =
        IndexBound::Exclusive(RangeIndexValue::literal(PropertyValue::from(30)).unwrap());

    assert_eq!(
        inclusive,
        IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(18)).unwrap())
    );
    assert_eq!(
        exclusive,
        IndexBound::Exclusive(RangeIndexValue::literal(PropertyValue::from(30)).unwrap())
    );
    let between = IndexBetweenRange::new(inclusive.clone(), exclusive.clone())
        .map(IndexRange::Between)
        .unwrap();
    let IndexRange::Between(bounds) = between else {
        panic!("expected bounded range");
    };
    assert_eq!(
        bounds,
        IndexBetweenRange::new(inclusive.clone(), exclusive).unwrap()
    );
    assert!(IndexBetweenRange::new(
        IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(30)).unwrap()),
        IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(18)).unwrap()),
    )
    .is_none());
    assert!(IndexBetweenRange::new(
        IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::datetime_millis(1)).unwrap()),
        IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::datetime_millis(2)).unwrap()),
    )
    .is_some());
    assert!(IndexBetweenRange::new(
        IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(1.5_f64)).unwrap()),
        IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(2.5_f64)).unwrap()),
    )
    .is_some());
    assert!(IndexBetweenRange::new(
        IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(1.5_f32)).unwrap()),
        IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(2.5_f32)).unwrap()),
    )
    .is_some());
    assert!(IndexBetweenRange::new(
        IndexBound::Exclusive(RangeIndexValue::literal(PropertyValue::from(18)).unwrap()),
        IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(30)).unwrap()),
    )
    .is_some());
    assert!(IndexBetweenRange::new(
        IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from("alice")).unwrap()),
        IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from("bob")).unwrap()),
    )
    .is_some());
    assert!(IndexBetweenRange::new(
        IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(18)).unwrap()),
        IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from("bob")).unwrap()),
    )
    .is_none());
    assert!(IndexBetweenRange::new(
        IndexBound::Inclusive(RangeIndexValue::param("min").unwrap()),
        IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(30)).unwrap()),
    )
    .is_some());
    let parsed_between: IndexBetweenRange = serde_json::from_str(
        r#"{"lower":{"inclusive":{"literal":{"i64":18}}},"upper":{"inclusive":{"literal":{"i64":30}}}}"#,
    )
    .unwrap();
    assert_eq!(
        parsed_between,
        IndexBetweenRange::new(
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(18)).unwrap()),
            IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(30)).unwrap()),
        )
        .unwrap()
    );
    assert!(serde_json::from_str::<IndexBetweenRange>(
        r#"{"lower":{"inclusive":{"literal":{"i64":30}}},"upper":{"inclusive":{"literal":{"i64":18}}}}"#
    )
    .is_err());
    assert!(serde_json::from_str::<IndexBetweenRange>("{}").is_err());

    assert_eq!(
        serde_json::to_string(&IndexBound::Inclusive(
            RangeIndexValue::literal(PropertyValue::from(1)).unwrap()
        ))
        .unwrap(),
        r#"{"inclusive":{"literal":{"i64":1}}}"#
    );
    assert_eq!(
        serde_json::from_str::<IndexBound>(r#"{"exclusive":{"literal":{"i64":2}}}"#).unwrap(),
        IndexBound::Exclusive(RangeIndexValue::literal(PropertyValue::from(2)).unwrap())
    );
    assert!(
        serde_json::from_str::<IndexBound>(r#"{"inclusive":{"literal":{"bool":true}}}"#).is_err()
    );
    assert!(serde_json::from_str::<IndexBound>(r#"{"inclusive":{"param":""}}"#).is_err());
}
