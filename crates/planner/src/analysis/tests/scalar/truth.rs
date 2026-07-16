use helix_ast::expr::{CompareOp, Expr, Predicate};
use helix_ast::value::PropertyValue;

use super::super::super::scalar::static_predicate_value;
use super::super::support::constant;

#[test]
fn static_predicate_values_cover_constant_boolean_contracts() {
    assert_eq!(
        static_predicate_value(&Predicate::Eq {
            left: constant(7),
            right: constant(7),
        }),
        Some(true)
    );
    assert_eq!(
        static_predicate_value(&Predicate::Neq {
            left: constant(7),
            right: constant(7),
        }),
        Some(false)
    );
    assert_eq!(
        static_predicate_value(&Predicate::Eq {
            left: Expr::Property("age".to_string()),
            right: constant(7),
        }),
        None
    );
    assert_eq!(
        static_predicate_value(&Predicate::Eq {
            left: constant(f64::NAN),
            right: constant(f64::NAN),
        }),
        None
    );

    assert_eq!(
        static_predicate_value(&Predicate::Gt {
            left: constant(9),
            right: constant(3),
        }),
        Some(true)
    );
    assert_eq!(
        static_predicate_value(&Predicate::Gte {
            left: constant(3),
            right: constant(3),
        }),
        Some(true)
    );
    assert_eq!(
        static_predicate_value(&Predicate::Gte {
            left: constant(2),
            right: constant(3),
        }),
        Some(false)
    );
    assert_eq!(
        static_predicate_value(&Predicate::Lt {
            left: constant("alice"),
            right: constant("bob"),
        }),
        Some(true)
    );
    assert_eq!(
        static_predicate_value(&Predicate::Lt {
            left: constant(PropertyValue::datetime_millis(1_000)),
            right: constant(PropertyValue::datetime_millis(2_000)),
        }),
        Some(true)
    );
    assert_eq!(
        static_predicate_value(&Predicate::Lte {
            left: constant(1.5_f64),
            right: constant(1.5_f64),
        }),
        Some(true)
    );
    assert_eq!(
        static_predicate_value(&Predicate::Gt {
            left: constant(2.5_f32),
            right: constant(1.5_f32),
        }),
        Some(true)
    );
    assert_eq!(
        static_predicate_value(&Predicate::Lte {
            left: constant("carol"),
            right: constant("bob"),
        }),
        Some(false)
    );
    assert_eq!(
        static_predicate_value(&Predicate::Gt {
            left: constant(true),
            right: constant(false),
        }),
        None
    );

    assert_eq!(
        static_predicate_value(&Predicate::Between {
            value: constant(5),
            min: constant(3),
            max: constant(8),
        }),
        Some(true)
    );
    assert_eq!(
        static_predicate_value(&Predicate::Between {
            value: constant(2),
            min: constant(3),
            max: constant(8),
        }),
        Some(false)
    );
    assert_eq!(
        static_predicate_value(&Predicate::Between {
            value: constant(9),
            min: constant(3),
            max: constant(8),
        }),
        Some(false)
    );
    assert_eq!(
        static_predicate_value(&Predicate::Between {
            value: constant(2),
            min: constant(3),
            max: constant(false),
        }),
        Some(false)
    );
    assert_eq!(
        static_predicate_value(&Predicate::Between {
            value: constant(2),
            min: constant(false),
            max: constant(8),
        }),
        None
    );
    assert_eq!(
        static_predicate_value(&Predicate::Between {
            value: constant(2),
            min: constant(1),
            max: constant(false),
        }),
        None
    );
    assert_eq!(
        static_predicate_value(&Predicate::Between {
            value: Expr::Property("age".to_string()),
            min: constant(3),
            max: constant(8),
        }),
        None
    );

    assert_eq!(
        static_predicate_value(&Predicate::StartsWith {
            value: constant("planner"),
            prefix: constant("plan"),
        }),
        Some(true)
    );
    assert_eq!(
        static_predicate_value(&Predicate::EndsWith {
            value: constant("planner"),
            suffix: constant("ner"),
        }),
        Some(true)
    );
    assert_eq!(
        static_predicate_value(&Predicate::Contains {
            value: constant("planner"),
            substring: constant("missing"),
        }),
        Some(false)
    );
    assert_eq!(
        static_predicate_value(&Predicate::starts_with("name", "a")),
        None
    );

    assert_eq!(
        static_predicate_value(&Predicate::IsIn {
            value: constant(3),
            values: Expr::Constant(PropertyValue::array([1, 3, 5])),
        }),
        Some(true)
    );
    assert_eq!(
        static_predicate_value(&Predicate::IsIn {
            value: constant(4),
            values: Expr::Constant(PropertyValue::array([1, 3, 5])),
        }),
        Some(false)
    );
    assert_eq!(
        static_predicate_value(&Predicate::IsIn {
            value: constant(4),
            values: constant(4),
        }),
        None
    );
    assert_eq!(
        static_predicate_value(&Predicate::IsIn {
            value: Expr::Property("age".to_string()),
            values: Expr::Constant(PropertyValue::array([1, 3, 5])),
        }),
        None
    );

    assert_eq!(
        static_predicate_value(&Predicate::and(vec![
            Predicate::compare(constant(3), CompareOp::Eq, constant(3)),
            Predicate::compare(constant(4), CompareOp::Gte, constant(3)),
        ])),
        Some(true)
    );
    assert_eq!(
        static_predicate_value(&Predicate::compare(
            constant(2),
            CompareOp::Gte,
            constant(3),
        )),
        Some(false)
    );
    assert_eq!(
        static_predicate_value(&Predicate::and(vec![
            Predicate::compare(constant(3), CompareOp::Eq, constant(3)),
            Predicate::has_key("name"),
        ])),
        None
    );
    assert_eq!(
        static_predicate_value(&Predicate::and(vec![
            Predicate::compare(constant(3), CompareOp::Eq, constant(3)),
            Predicate::compare(constant(4), CompareOp::Lt, constant(3)),
        ])),
        Some(false)
    );
    assert_eq!(static_predicate_value(&Predicate::and(vec![])), None);

    assert_eq!(
        static_predicate_value(&Predicate::or(vec![
            Predicate::has_key("name"),
            Predicate::compare(constant(4), CompareOp::Lt, constant(3)),
        ])),
        None
    );
    assert_eq!(
        static_predicate_value(&Predicate::or(vec![
            Predicate::compare(constant(4), CompareOp::Lt, constant(3)),
            Predicate::compare(constant(4), CompareOp::Gt, constant(3)),
        ])),
        Some(true)
    );
    assert_eq!(
        static_predicate_value(&Predicate::or(vec![
            Predicate::compare(constant(4), CompareOp::Lt, constant(3)),
            Predicate::compare(constant(4), CompareOp::Neq, constant(4)),
        ])),
        Some(false)
    );
    assert_eq!(static_predicate_value(&Predicate::or(vec![])), None);

    assert_eq!(
        static_predicate_value(&Predicate::not(Predicate::compare(
            constant(4),
            CompareOp::Lte,
            constant(3),
        ))),
        Some(true)
    );
    assert_eq!(
        static_predicate_value(&Predicate::is_null("deleted_at")),
        None
    );
    assert_eq!(
        static_predicate_value(&Predicate::is_not_null("deleted_at")),
        None
    );
}
