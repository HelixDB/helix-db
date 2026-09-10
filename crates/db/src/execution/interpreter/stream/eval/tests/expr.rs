use super::*;

#[tokio::test]
async fn resolved_native_semantics_preserve_values_laziness_and_checked_boundaries() {
    let db = test_support::open_db("native-scalar-domain").await;
    let ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let row = current_node(u64::MAX);
    assert_eq!(
        ctx.eval_expr(&row, &Expr::Id).await.unwrap(),
        DbPropertyValue::I64(i64::MAX)
    );
    for value in [
        PropertyValue::Null,
        PropertyValue::DateTime(17),
        PropertyValue::Bytes(vec![0, 255]),
        PropertyValue::F32(0.5),
        PropertyValue::F32Array(vec![1.0, 2.0]),
        PropertyValue::F64Array(vec![1.5]),
        PropertyValue::I64Array(vec![i64::MIN, i64::MAX]),
        PropertyValue::StringArray(vec!["x".into()]),
        PropertyValue::Object(std::collections::BTreeMap::from([(
            "value".into(),
            PropertyValue::Null,
        )])),
    ] {
        assert_eq!(
            ctx.eval_expr(&row, &Expr::Constant(value.clone()))
                .await
                .unwrap(),
            super::super::super::values::ast_to_db_value(value)
        );
    }
    let missing = Expr::param("missing");
    let short_circuit = Predicate::or(vec![
        Predicate::Compare {
            left: Expr::val(PropertyValue::Null),
            op: CompareOp::Eq,
            right: Expr::val(PropertyValue::Null),
        },
        Predicate::Compare {
            left: missing.clone(),
            op: CompareOp::Eq,
            right: Expr::val(1),
        },
    ]);
    assert!(ctx.eval_predicate(&row, &short_circuit).await.unwrap());
    assert!(!ctx
        .eval_predicate(
            &row,
            &Predicate::Between {
                value: Expr::val(0),
                min: Expr::val(1),
                max: missing.clone()
            }
        )
        .await
        .unwrap());
    for expression in [
        Expr::val(1).modulo(Expr::val(0)),
        Expr::val(i64::MIN).modulo(Expr::val(-1)),
        Expr::val(i64::MIN).neg_expr(),
    ] {
        assert!(matches!(
            ctx.eval_expr(&row, &expression).await,
            Err(HelixDbError::Query(_))
        ));
    }
    assert!(ctx
        .eval_expr(&row, &Expr::val("bad").modulo(missing))
        .await
        .unwrap_err()
        .to_string()
        .contains("mod left expression must be i64"));
    db.close().await.unwrap();
}

#[tokio::test]
async fn expressions_cover_arithmetic_case_parameters_and_errors() {
    let db = test_support::open_db("stream-eval-expressions").await;
    let id =
        test_support::add_node_with_properties(&db, "User", vec![("age", PropertyValue::I64(37))])
            .await;
    let ctx = ExecutionContext::new(
        &db,
        context::ParamBindings::default().with_value(name("bonus"), PropertyValue::I64(5)),
    );
    let row = current_node(id);

    assert_eq!(
        ctx.eval_expr(&row, &Expr::prop("missing")).await.unwrap(),
        DbPropertyValue::Null
    );
    assert_eq!(
        ctx.eval_expr(&row, &Expr::id()).await.unwrap(),
        DbPropertyValue::I64(id as i64)
    );
    assert_eq!(
        ctx.eval_expr(&row, &Expr::param("bonus")).await.unwrap(),
        DbPropertyValue::I64(5)
    );
    assert_eq!(
        ctx.eval_expr(&row, &Expr::val(8).add_expr(Expr::param("bonus")))
            .await
            .unwrap(),
        DbPropertyValue::F64(13.0)
    );
    assert_eq!(
        ctx.eval_expr(&row, &Expr::val(8).sub_expr(Expr::val(3)))
            .await
            .unwrap(),
        DbPropertyValue::F64(5.0)
    );
    assert_eq!(
        ctx.eval_expr(&row, &Expr::val(6).mul_expr(Expr::val(7)))
            .await
            .unwrap(),
        DbPropertyValue::F64(42.0)
    );
    assert_eq!(
        ctx.eval_expr(&row, &Expr::val(9).div_expr(Expr::val(3)))
            .await
            .unwrap(),
        DbPropertyValue::F64(3.0)
    );
    assert_eq!(
        ctx.eval_expr(&row, &Expr::val(9).modulo(Expr::val(4)))
            .await
            .unwrap(),
        DbPropertyValue::I64(1)
    );
    assert_eq!(
        ctx.eval_expr(&row, &Expr::val(4).neg_expr()).await.unwrap(),
        DbPropertyValue::I64(-4)
    );
    assert_eq!(
        ctx.eval_expr(&row, &Expr::val(2.5).neg_expr())
            .await
            .unwrap(),
        DbPropertyValue::F64(-2.5)
    );
    assert_eq!(
        ctx.eval_expr(
            &row,
            &Expr::case(
                vec![(
                    Predicate::Gt {
                        left: Expr::prop("age"),
                        right: Expr::val(30),
                    },
                    Expr::val("senior"),
                )],
                Some(Expr::val("junior")),
            ),
        )
        .await
        .unwrap(),
        DbPropertyValue::String("senior".to_string())
    );
    assert_eq!(
        ctx.eval_expr(&row, &Expr::case(Vec::new(), None))
            .await
            .unwrap(),
        DbPropertyValue::Null
    );

    assert!(ctx
        .eval_expr(&ExecutionRow::empty(), &Expr::id())
        .await
        .unwrap_err()
        .to_string()
        .contains("id expression has no current element"));
    assert!(ctx
        .eval_expr(&row, &Expr::val("nope").add_expr(Expr::val(1)))
        .await
        .unwrap_err()
        .to_string()
        .contains("left expression must be numeric"));
    assert!(ctx
        .eval_expr(&row, &Expr::val("nope").neg_expr())
        .await
        .unwrap_err()
        .to_string()
        .contains("neg expression must be numeric"));
}

#[tokio::test]
async fn expressions_cover_datetime_else_branch_and_type_error_edges() {
    let db = test_support::open_db("stream-eval-expression-edges").await;
    let ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let row = ExecutionRow::empty();

    assert!(matches!(
        ctx.eval_expr(&row, &Expr::Timestamp).await.unwrap(),
        DbPropertyValue::I64(_)
    ));
    assert!(matches!(
        ctx.eval_expr(&row, &Expr::DateTimeNow).await.unwrap(),
        DbPropertyValue::DateTime(_)
    ));
    assert_eq!(
        ctx.eval_expr(
            &row,
            &Expr::case(
                vec![(
                    Predicate::Eq {
                        left: Expr::val(1),
                        right: Expr::val(2),
                    },
                    Expr::val("then"),
                )],
                Some(Expr::val("else")),
            ),
        )
        .await
        .unwrap(),
        DbPropertyValue::String("else".to_string())
    );

    assert!(ctx
        .eval_expr(&row, &Expr::Property(String::new()))
        .await
        .unwrap_err()
        .to_string()
        .contains("expression property name must not be empty"));
    assert!(ctx
        .eval_expr(&row, &Expr::Param(String::new()))
        .await
        .unwrap_err()
        .to_string()
        .contains("expression parameter name must not be empty"));
    assert!(ctx
        .eval_expr(&row, &Expr::val(1).add_expr(Expr::val("nope")))
        .await
        .unwrap_err()
        .to_string()
        .contains("right expression must be numeric"));
    assert!(ctx
        .eval_expr(&row, &Expr::val("nope").modulo(Expr::val(1)))
        .await
        .unwrap_err()
        .to_string()
        .contains("mod left expression must be i64"));
    assert!(ctx
        .eval_expr(&row, &Expr::val(1).modulo(Expr::val("nope")))
        .await
        .unwrap_err()
        .to_string()
        .contains("mod right expression must be i64"));

    for expression in [
        Expr::param("missing").modulo(Expr::val(1)),
        Expr::val(1).modulo(Expr::param("missing")),
        Expr::case(
            vec![(
                Predicate::Eq {
                    left: Expr::param("missing"),
                    right: Expr::val(1),
                },
                Expr::val("then"),
            )],
            Some(Expr::val("else")),
        ),
    ] {
        assert_eq!(
            ctx.eval_expr(&row, &expression)
                .await
                .expect_err("nested missing parameters are propagated")
                .to_string(),
            "Query error: parameter `missing` is not bound"
        );
    }
}
