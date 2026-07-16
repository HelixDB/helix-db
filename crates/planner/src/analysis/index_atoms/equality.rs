//! Equality-index atom extraction.

use helix_ast::expr::{CompareOp, Expr, Predicate};

use crate::error::PlannerError;
use crate::ir::{
    IndexValue, NameField, NonEmptyString, SecondaryIndexLiteral, SecondaryIndexLiteralError,
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EqualityIndexAtom {
    Atom { property: String, value: IndexValue },
    NotIndexable,
}

pub(crate) fn equality_atom(predicate: &Predicate) -> Result<EqualityIndexAtom, PlannerError> {
    match predicate {
        Predicate::Eq { left, right }
        | Predicate::Compare {
            left,
            op: CompareOp::Eq,
            right,
        } => equality_from_operands(left, right),
        Predicate::Neq { .. }
        | Predicate::Gt { .. }
        | Predicate::Gte { .. }
        | Predicate::Lt { .. }
        | Predicate::Lte { .. }
        | Predicate::Between { .. }
        | Predicate::HasKey { .. }
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
        | Predicate::StartsWith { .. }
        | Predicate::EndsWith { .. }
        | Predicate::Contains { .. }
        | Predicate::IsIn { .. }
        | Predicate::And { .. }
        | Predicate::Or { .. }
        | Predicate::Not { .. }
        | Predicate::Compare {
            op: CompareOp::Neq | CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte,
            ..
        } => Ok(EqualityIndexAtom::NotIndexable),
    }
}

fn equality_from_operands(left: &Expr, right: &Expr) -> Result<EqualityIndexAtom, PlannerError> {
    match (left, right) {
        (Expr::Property(property), Expr::Constant(value))
        | (Expr::Constant(value), Expr::Property(property)) => {
            match SecondaryIndexLiteral::new(value.clone()) {
                Ok(value) => Ok(EqualityIndexAtom::Atom {
                    property: property.clone(),
                    value: IndexValue::Literal(value),
                }),
                Err(
                    SecondaryIndexLiteralError::NestedValue
                    | SecondaryIndexLiteralError::NullValue
                    | SecondaryIndexLiteralError::AmbiguousNullString,
                ) => Ok(EqualityIndexAtom::NotIndexable),
            }
        }
        (Expr::Property(property), Expr::Param(name))
        | (Expr::Param(name), Expr::Property(property)) => NonEmptyString::new(name.clone())
            .map(|name| EqualityIndexAtom::Atom {
                property: property.clone(),
                value: IndexValue::Param(name),
            })
            .ok_or(PlannerError::InvalidEmptyName {
                field: NameField::Param,
            }),
        (Expr::Property(_), expr) | (expr, Expr::Property(_)) => {
            Err(PlannerError::NonLiteralIndexExpression {
                expression: format!("{expr:?}"),
            })
        }
        _ => Ok(EqualityIndexAtom::NotIndexable),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir;
    use helix_ast::value::PropertyValue;

    #[test]
    fn equality_atom_rejects_empty_param_names_and_non_literal_operands() {
        assert_eq!(
            equality_atom(&Predicate::Eq {
                left: Expr::Property("age".to_owned()),
                right: Expr::Param(String::new()),
            }),
            Err(PlannerError::InvalidEmptyName {
                field: NameField::Param,
            })
        );
        assert_eq!(
            equality_atom(&Predicate::Eq {
                left: Expr::Property("age".to_owned()),
                right: Expr::Id,
            }),
            Err(PlannerError::NonLiteralIndexExpression {
                expression: "Id".to_owned(),
            })
        );
    }

    #[test]
    fn equality_atom_ignores_nested_literals_instead_of_erroring() {
        assert_eq!(
            equality_atom(&Predicate::Eq {
                left: Expr::Property("tags".to_owned()),
                right: Expr::Constant(PropertyValue::array([PropertyValue::from("rust")])),
            })
            .unwrap(),
            EqualityIndexAtom::NotIndexable
        );
    }

    #[test]
    fn equality_atom_ignores_null_literals_instead_of_using_ambiguous_index_keys() {
        for value in [PropertyValue::Null, PropertyValue::from("null")] {
            assert_eq!(
                equality_atom(&Predicate::Eq {
                    left: Expr::Property("deleted_at".to_owned()),
                    right: Expr::Constant(value),
                })
                .unwrap(),
                EqualityIndexAtom::NotIndexable
            );
        }
    }

    #[test]
    fn equality_atom_accepts_parameter_values() {
        assert_eq!(
            equality_atom(&Predicate::Eq {
                left: Expr::Param("age".to_owned()),
                right: Expr::Property("age".to_owned()),
            })
            .unwrap(),
            EqualityIndexAtom::Atom {
                property: "age".to_owned(),
                value: ir::IndexValue::Param(ir::NonEmptyString::new("age").unwrap()),
            }
        );
    }
}
