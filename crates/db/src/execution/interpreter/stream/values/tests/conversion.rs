use super::*;

#[test]
fn ast_values_convert_nested_shapes_without_losing_types() {
    let value = AstPropertyValue::Array(vec![
        AstPropertyValue::Null,
        AstPropertyValue::Bool(true),
        AstPropertyValue::I64(7),
        AstPropertyValue::DateTime(123),
        AstPropertyValue::F32(1.5),
        AstPropertyValue::F64(2.5),
        AstPropertyValue::String("value".to_string()),
        AstPropertyValue::Bytes(vec![1, 2, 3]),
        AstPropertyValue::I64Array(vec![4, 5]),
        AstPropertyValue::F32Array(vec![6.5]),
        AstPropertyValue::F64Array(vec![7.5]),
        AstPropertyValue::StringArray(vec!["a".to_string(), "b".to_string()]),
        AstPropertyValue::Object(BTreeMap::from([(
            "nested".to_string(),
            AstPropertyValue::I64(8),
        )])),
    ]);

    assert_eq!(
        value_conversion::ast_to_db_value(value),
        DbPropertyValue::Array(vec![
            DbPropertyValue::Null,
            DbPropertyValue::Bool(true),
            DbPropertyValue::I64(7),
            DbPropertyValue::DateTime(123),
            DbPropertyValue::F32(1.5_f32.into()),
            DbPropertyValue::F64(2.5),
            DbPropertyValue::String("value".to_string()),
            DbPropertyValue::Bytes(vec![1, 2, 3]),
            DbPropertyValue::I64Array(vec![4, 5]),
            DbPropertyValue::F32Array(vec![6.5]),
            DbPropertyValue::F64Array(vec![7.5]),
            DbPropertyValue::StringArray(vec!["a".to_string(), "b".to_string()]),
            DbPropertyValue::Object(BTreeMap::from([(
                "nested".to_string(),
                DbPropertyValue::I64(8),
            )])),
        ])
    );
}

#[test]
fn query_values_convert_nested_shapes_without_losing_types() {
    let value = QueryValue::Object(BTreeMap::from([(
        "items".to_string(),
        QueryValue::Array(vec![
            QueryValue::Null,
            QueryValue::Bool(true),
            QueryValue::I64(1),
            QueryValue::F32(2.5),
            QueryValue::F64(3.5),
            QueryValue::String("four".to_string()),
        ]),
    )]));

    assert_eq!(
        value_conversion::query_value_to_db_value(value),
        DbPropertyValue::Object(BTreeMap::from([(
            "items".to_string(),
            DbPropertyValue::Array(vec![
                DbPropertyValue::Null,
                DbPropertyValue::Bool(true),
                DbPropertyValue::I64(1),
                DbPropertyValue::F32(2.5_f32.into()),
                DbPropertyValue::F64(3.5),
                DbPropertyValue::String("four".to_string()),
            ]),
        )]))
    );
}
