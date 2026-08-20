use super::*;

#[tokio::test]
async fn predicates_cover_comparisons_strings_nulls_membership_and_short_circuiting() {
    let db = test_support::open_db("stream-eval-predicates").await;
    let id = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::String("ada lovelace".to_string())),
            ("age", PropertyValue::I64(37)),
            ("nickname", PropertyValue::Null),
        ],
    )
    .await;
    let ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let row = current_node(id);

    let predicate = Predicate::And {
        predicates: vec![
            Predicate::Gte {
                left: Expr::prop("age"),
                right: Expr::val(37),
            },
            Predicate::Lt {
                left: Expr::prop("age"),
                right: Expr::val(40),
            },
            Predicate::Between {
                value: Expr::prop("age"),
                min: Expr::val(30),
                max: Expr::val(40),
            },
            Predicate::StartsWith {
                value: Expr::prop("name"),
                prefix: Expr::val("ada"),
            },
            Predicate::EndsWith {
                value: Expr::prop("name"),
                suffix: Expr::val("lovelace"),
            },
            Predicate::Contains {
                value: Expr::prop("name"),
                substring: Expr::val("love"),
            },
            Predicate::IsNull {
                property: "nickname".to_string(),
            },
            Predicate::IsNull {
                property: "missing".to_string(),
            },
            Predicate::IsNotNull {
                property: "name".to_string(),
            },
            Predicate::HasKey {
                property: "age".to_string(),
            },
            Predicate::IsIn {
                value: Expr::prop("age"),
                values: Expr::val(PropertyValue::I64Array(vec![36, 37])),
            },
            Predicate::IsIn {
                value: Expr::prop("name"),
                values: Expr::val(PropertyValue::StringArray(vec![
                    "ada lovelace".to_string(),
                    "grace hopper".to_string(),
                ])),
            },
            Predicate::IsIn {
                value: Expr::val("ada lovelace"),
                values: Expr::prop("name"),
            },
            Predicate::Not {
                predicate: Box::new(Predicate::Eq {
                    left: Expr::prop("age"),
                    right: Expr::val(38),
                }),
            },
            Predicate::Or {
                predicates: vec![
                    Predicate::Eq {
                        left: Expr::prop("age"),
                        right: Expr::val(0),
                    },
                    Predicate::Compare {
                        left: Expr::prop("age"),
                        op: CompareOp::Eq,
                        right: Expr::val(37),
                    },
                ],
            },
        ],
    };

    assert!(ctx.eval_predicate(&row, &predicate).await.unwrap());
    assert!(!ctx
        .eval_predicate(
            &row,
            &Predicate::Compare {
                left: Expr::prop("age"),
                op: CompareOp::Neq,
                right: Expr::val(37),
            },
        )
        .await
        .unwrap());
}

#[test]
fn membership_semantics_cover_every_typed_array_and_scalar_rhs() {
    assert!(property_value_is_in(
        &DbPropertyValue::F64(2.0),
        &DbPropertyValue::F64Array(vec![1.0, 2.0]),
    ));
    assert!(property_value_is_in(
        &DbPropertyValue::F32(2.0),
        &DbPropertyValue::F32Array(vec![1.0, 2.0]),
    ));
    assert!(property_value_is_in(
        &DbPropertyValue::String("active".to_owned()),
        &DbPropertyValue::String("active".to_owned()),
    ));
    assert!(!property_value_is_in(
        &DbPropertyValue::F64(f64::NAN),
        &DbPropertyValue::F64Array(vec![f64::NAN]),
    ));
}

#[tokio::test]
async fn predicates_cover_alias_comparisons_membership_arrays_and_errors() {
    let db = test_support::open_db("stream-eval-predicate-edges").await;
    let ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let row = ExecutionRow::empty();

    let cases = [
        Predicate::Neq {
            left: Expr::val(1),
            right: Expr::val(2),
        },
        Predicate::Gt {
            left: Expr::val(2),
            right: Expr::val(1),
        },
        Predicate::Lte {
            left: Expr::val(2),
            right: Expr::val(2),
        },
        Predicate::Compare {
            left: Expr::val(2),
            op: CompareOp::Gt,
            right: Expr::val(1),
        },
        Predicate::Compare {
            left: Expr::val(2),
            op: CompareOp::Gte,
            right: Expr::val(2),
        },
        Predicate::Compare {
            left: Expr::val(1),
            op: CompareOp::Lt,
            right: Expr::val(2),
        },
        Predicate::Compare {
            left: Expr::val(2),
            op: CompareOp::Lte,
            right: Expr::val(2),
        },
        Predicate::IsIn {
            value: Expr::val(2),
            values: Expr::val(PropertyValue::Array(vec![
                PropertyValue::I64(1),
                PropertyValue::I64(2),
            ])),
        },
        Predicate::IsIn {
            value: Expr::val(PropertyValue::F64Array(vec![1.5, 2.5])),
            values: Expr::val(PropertyValue::F64Array(vec![1.5, 2.5])),
        },
        Predicate::IsIn {
            value: Expr::val(PropertyValue::F32Array(vec![1.25, 2.25])),
            values: Expr::val(PropertyValue::F32Array(vec![1.25, 2.25])),
        },
    ];
    for predicate in cases {
        assert!(
            ctx.eval_predicate(&row, &predicate).await.unwrap(),
            "{predicate:?}"
        );
    }

    let false_and = Predicate::And {
        predicates: vec![
            Predicate::Eq {
                left: Expr::val(1),
                right: Expr::val(2),
            },
            Predicate::HasKey {
                property: String::new(),
            },
        ],
    };
    assert!(!ctx.eval_predicate(&row, &false_and).await.unwrap());
    let false_or = Predicate::Or {
        predicates: vec![
            Predicate::Eq {
                left: Expr::val(1),
                right: Expr::val(2),
            },
            Predicate::Eq {
                left: Expr::val(3),
                right: Expr::val(4),
            },
        ],
    };
    assert!(!ctx.eval_predicate(&row, &false_or).await.unwrap());
    for predicate in [
        Predicate::Gte {
            left: Expr::val(1),
            right: Expr::val(2),
        },
        Predicate::Lte {
            left: Expr::val(2),
            right: Expr::val(1),
        },
        Predicate::Between {
            value: Expr::val(1),
            min: Expr::val(2),
            max: Expr::val(3),
        },
        Predicate::Between {
            value: Expr::val(4),
            min: Expr::val(2),
            max: Expr::val(3),
        },
    ] {
        assert!(!ctx.eval_predicate(&row, &predicate).await.unwrap());
    }
    assert!(ctx
        .eval_predicate(
            &row,
            &Predicate::HasKey {
                property: String::new(),
            },
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("predicate property name must not be empty"));

    let missing = || Expr::param("missing");
    for predicate in [
        Predicate::Neq {
            left: missing(),
            right: Expr::val(1),
        },
        Predicate::Gt {
            left: missing(),
            right: Expr::val(1),
        },
        Predicate::Gte {
            left: missing(),
            right: Expr::val(1),
        },
        Predicate::Lt {
            left: missing(),
            right: Expr::val(1),
        },
        Predicate::Lte {
            left: missing(),
            right: Expr::val(1),
        },
        Predicate::Compare {
            left: missing(),
            op: CompareOp::Eq,
            right: Expr::val(1),
        },
        Predicate::Compare {
            left: missing(),
            op: CompareOp::Neq,
            right: Expr::val(1),
        },
        Predicate::StartsWith {
            value: missing(),
            prefix: Expr::val("prefix"),
        },
        Predicate::StartsWith {
            value: Expr::val("value"),
            prefix: missing(),
        },
        Predicate::EndsWith {
            value: missing(),
            suffix: Expr::val("suffix"),
        },
        Predicate::EndsWith {
            value: Expr::val("value"),
            suffix: missing(),
        },
        Predicate::Contains {
            value: missing(),
            substring: Expr::val("substring"),
        },
        Predicate::Contains {
            value: Expr::val("value"),
            substring: missing(),
        },
        Predicate::And {
            predicates: vec![Predicate::Eq {
                left: missing(),
                right: Expr::val(1),
            }],
        },
    ] {
        assert_eq!(
            ctx.eval_predicate(&row, &predicate)
                .await
                .expect_err("nested missing predicate parameters are propagated")
                .to_string(),
            "Query error: parameter `missing` is not bound",
            "{predicate:?}"
        );
    }
}

#[tokio::test]
async fn null_predicates_propagate_corrupt_property_blobs() {
    let db = test_support::open_db("stream-eval-predicate-corrupt-property").await;
    let id = 12;
    let key = crate::encoding::keys::Key::Data {
        scope: crate::encoding::keys::tenant::DataScope::LegacyUnscoped,
        kind: crate::encoding::keys::DataKeyKind::NodeProperty(
            crate::encoding::keys::NodePropertyKey::new(id),
        ),
    }
    .to_bytes();
    db.inner_db()
        .put(key, bytes::Bytes::from_static(b"corrupt"))
        .await
        .expect("corrupt property blob writes");
    let ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let row = current_node(id);

    for predicate in [
        Predicate::Eq {
            left: Expr::prop("value"),
            right: Expr::val(1),
        },
        Predicate::HasKey {
            property: "value".to_string(),
        },
        Predicate::IsNull {
            property: "value".to_string(),
        },
        Predicate::IsNotNull {
            property: "value".to_string(),
        },
    ] {
        assert!(matches!(
            ctx.eval_predicate(&row, &predicate).await,
            Err(HelixDbError::Encoding(_))
        ));
    }
}
