use helix_ast::expr::Predicate;
use helix_ast::value::PropertyValue;

use crate::analysis::literal_in_values;

#[test]
fn literal_in_values_dedupes_and_rejects_non_reflexive_collections() {
    assert_eq!(
        literal_in_values(&Predicate::is_in(
            "age",
            PropertyValue::I64Array(vec![20, 20, 30])
        )),
        Some((
            "age".to_owned(),
            vec![PropertyValue::from(20), PropertyValue::from(30)]
        ))
    );
    assert_eq!(
        literal_in_values(&Predicate::is_in(
            "name",
            PropertyValue::StringArray(vec!["alice".to_owned(), "alice".to_owned()])
        )),
        Some(("name".to_owned(), vec![PropertyValue::from("alice")]))
    );
    assert_eq!(
        literal_in_values(&Predicate::is_in(
            "score",
            PropertyValue::F32Array(vec![1.5, 1.5, 2.5])
        )),
        Some((
            "score".to_owned(),
            vec![PropertyValue::from(1.5_f32), PropertyValue::from(2.5_f32)]
        ))
    );
    assert_eq!(
        literal_in_values(&Predicate::is_in(
            "score",
            PropertyValue::F64Array(vec![1.5, 1.5, 2.5])
        )),
        Some((
            "score".to_owned(),
            vec![PropertyValue::from(1.5_f64), PropertyValue::from(2.5_f64)]
        ))
    );
    assert_eq!(
        literal_in_values(&Predicate::is_in(
            "mixed",
            PropertyValue::array([
                PropertyValue::from("alice"),
                PropertyValue::from("alice"),
                PropertyValue::from(42),
            ])
        )),
        Some((
            "mixed".to_owned(),
            vec![PropertyValue::from("alice"), PropertyValue::from(42)]
        ))
    );
    assert_eq!(
        literal_in_values(&Predicate::is_in(
            "score",
            PropertyValue::F64Array(vec![1.0, f64::NAN])
        )),
        None
    );
    assert_eq!(
        literal_in_values(&Predicate::is_in(
            "nested",
            PropertyValue::array([PropertyValue::object([(
                "score",
                PropertyValue::from(f64::NAN)
            )])])
        )),
        None
    );
    assert_eq!(
        literal_in_values(&Predicate::is_in_param("age", "ages")),
        None
    );
    assert_eq!(literal_in_values(&Predicate::eq("age", 30)), None);
}
