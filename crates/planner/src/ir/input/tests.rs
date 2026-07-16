use helix_ast::expr::Expr;
use helix_ast::value::{PropertyInput, PropertyValue};

use super::*;
use crate::catalog;

#[test]
fn property_inputs_normalize_static_constant_expressions() {
    let plan = PropertyInputPlan::new(PropertyInput::from(Expr::val("alice"))).unwrap();

    assert_eq!(
        plan,
        PropertyInputPlan::Value(PropertyValue::String("alice".to_owned()))
    );
    assert_eq!(
        PropertyInputExprPlan::new(Expr::val("alice")),
        Err(PropertyInputExprPlanError::StaticLiteral)
    );
}

#[test]
fn search_query_expression_rejects_static_literals() {
    assert_eq!(
        SearchQueryExprPlan::new(Expr::val("needle")),
        Err(SearchQueryExprPlanError::StaticLiteral)
    );
    assert!(SearchQueryExprPlan::new(Expr::param("needle")).is_ok());
}

#[test]
fn vector_query_contract_rejects_bad_literal_shapes() {
    assert_eq!(SearchVector::new(Vec::new()), Err(SearchVectorError::Empty));
    assert_eq!(
        SearchVector::new(vec![f32::NAN]),
        Err(SearchVectorError::NonFiniteComponent)
    );
    assert!(matches!(
        VectorQueryInputPlan::new(PropertyInput::from("needle")),
        Err(SearchQueryInputPlanError::InvalidLiteral {
            kind: catalog::SearchIndexKind::Vector,
            expected: SearchQueryInputExpected::NonEmptyFiniteF32Array,
        })
    ));
}

#[test]
fn text_query_contract_rejects_empty_or_wrong_literal_shapes() {
    assert!(matches!(
        TextQueryInputPlan::new(PropertyInput::from("")),
        Err(SearchQueryInputPlanError::InvalidLiteral {
            kind: catalog::SearchIndexKind::Text,
            expected: SearchQueryInputExpected::NonEmptyString,
        })
    ));
    assert!(matches!(
        TextQueryInputPlan::new(PropertyInput::from(PropertyValue::F32Array(vec![0.25]))),
        Err(SearchQueryInputPlanError::InvalidLiteral {
            kind: catalog::SearchIndexKind::Text,
            expected: SearchQueryInputExpected::NonEmptyString,
        })
    ));
}
