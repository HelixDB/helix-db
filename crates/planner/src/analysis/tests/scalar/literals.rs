use helix_ast::expr::Predicate;
use helix_ast::value::PropertyValue;

use crate::analysis::literal_in_values;

#[test]
fn literal_in_values_keeps_values_query_equality_tells_apart() {
    // Serialized text writes every infinity as `null`; the allowed values
    // must still tell `+inf` from `-inf`, alone and inside arrays.
    let infinities = PropertyValue::array([
        PropertyValue::F64(f64::INFINITY),
        PropertyValue::F64(f64::NEG_INFINITY),
        PropertyValue::F64(f64::INFINITY),
        PropertyValue::F64Array(vec![f64::INFINITY]),
        PropertyValue::F64Array(vec![f64::NEG_INFINITY]),
        PropertyValue::F32Array(vec![f32::NEG_INFINITY]),
        PropertyValue::F32Array(vec![f32::NEG_INFINITY]),
        PropertyValue::from(1),
        PropertyValue::from(1.0_f64),
    ]);
    assert_eq!(
        literal_in_values(&Predicate::is_in("x", infinities)),
        Some((
            "x".to_owned(),
            vec![
                PropertyValue::F64(f64::INFINITY),
                PropertyValue::F64(f64::NEG_INFINITY),
                PropertyValue::F64Array(vec![f64::INFINITY]),
                PropertyValue::F64Array(vec![f64::NEG_INFINITY]),
                PropertyValue::F32Array(vec![f32::NEG_INFINITY]),
                PropertyValue::from(1),
            ]
        ))
    );
}

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
            "age",
            PropertyValue::array([
                PropertyValue::from(20),
                PropertyValue::from(20.0),
                PropertyValue::from(30),
            ])
        )),
        Some((
            "age".to_owned(),
            vec![PropertyValue::from(20), PropertyValue::from(30)]
        ))
    );
    assert_eq!(
        literal_in_values(&Predicate::is_in(
            "age",
            PropertyValue::array([PropertyValue::from(20), PropertyValue::from("20")])
        )),
        Some((
            "age".to_owned(),
            vec![PropertyValue::from(20), PropertyValue::from("20")]
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
