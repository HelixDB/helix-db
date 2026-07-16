use super::*;

#[test]
fn parameter_values_preserve_static_and_dynamic_shapes() {
    let static_name = name("static");
    let dynamic_name = name("dynamic");
    let overlapping_name = name("overlap");
    let params = context::ParamBindings::default()
        .with_value(
            static_name.clone(),
            AstPropertyValue::Object(BTreeMap::from([(
                "score".to_string(),
                AstPropertyValue::I64(7),
            )])),
        )
        .with_query_value(
            dynamic_name.clone(),
            QueryValue::Array(vec![
                QueryValue::I64(1),
                QueryValue::String("two".to_string()),
            ]),
        )
        .with_value(overlapping_name.clone(), AstPropertyValue::I64(7))
        .with_query_value(overlapping_name.clone(), QueryValue::I64(9));

    assert_eq!(
        value_params::param_value_from(&params, &static_name).unwrap(),
        DbPropertyValue::Object(BTreeMap::from([(
            "score".to_string(),
            DbPropertyValue::I64(7),
        )]))
    );
    assert_eq!(
        value_params::param_value_from(&params, &dynamic_name).unwrap(),
        DbPropertyValue::Array(vec![
            DbPropertyValue::I64(1),
            DbPropertyValue::String("two".to_string()),
        ])
    );
    assert_eq!(
        value_params::param_value_from(&params, &overlapping_name).unwrap(),
        DbPropertyValue::I64(7)
    );
    assert!(value_params::param_value_from(&params, &name("missing"))
        .unwrap_err()
        .to_string()
        .contains("not bound"));
}
