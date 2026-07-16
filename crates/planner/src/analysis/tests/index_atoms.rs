use helix_ast::expr::{CompareOp, Expr, Predicate};
use helix_ast::value::PropertyValue;

use super::support::constant;
use crate::analysis::{equality_atom, range_atom, EqualityIndexAtom, RangeIndexAtom};
use crate::error::PlannerError;
use crate::ir::{IndexValue, SecondaryIndexLiteral};

#[test]
fn equality_atoms_accept_reversed_literal_property_order() {
    let predicate = Predicate::Compare {
        left: Expr::Constant(PropertyValue::from("alice")),
        op: CompareOp::Eq,
        right: Expr::Property("username".to_string()),
    };

    assert_eq!(
        equality_atom(&predicate).unwrap(),
        EqualityIndexAtom::Atom {
            property: "username".to_string(),
            value: IndexValue::Literal(
                SecondaryIndexLiteral::new(PropertyValue::from("alice")).unwrap()
            ),
        }
    );
}

#[test]
fn range_atoms_reject_comparisons_without_property_operands() {
    assert_eq!(
        range_atom(&Predicate::compare(
            constant(10),
            CompareOp::Gt,
            constant(1),
        ))
        .unwrap(),
        RangeIndexAtom::NotIndexable
    );
    assert_eq!(
        range_atom(&Predicate::Between {
            value: Expr::Property("age".to_string()),
            min: constant(false),
            max: constant(10),
        })
        .unwrap(),
        RangeIndexAtom::NotIndexable
    );
    assert_eq!(
        range_atom(&Predicate::Between {
            value: Expr::Property("age".to_string()),
            min: constant(1),
            max: constant(false),
        })
        .unwrap(),
        RangeIndexAtom::NotIndexable
    );
    assert_eq!(
        range_atom(&Predicate::Between {
            value: Expr::Property("age".to_string()),
            min: Expr::Property("min_age".to_string()),
            max: constant(10),
        })
        .unwrap_err(),
        PlannerError::NonLiteralIndexExpression {
            expression: "Property(\"min_age\")".to_string(),
        }
    );
    assert_eq!(
        range_atom(&Predicate::Between {
            value: Expr::Property("age".to_string()),
            min: constant(1),
            max: Expr::Property("max_age".to_string()),
        })
        .unwrap_err(),
        PlannerError::NonLiteralIndexExpression {
            expression: "Property(\"max_age\")".to_string(),
        }
    );
}
