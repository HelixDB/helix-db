//! Equality-index atom extraction.

use helix_ast::expr::{CompareOp, Expr, Predicate};
use helix_ast::value::PropertyValue;

use crate::error::PlannerError;
use crate::ir::{
    IndexValue, NameField, NonEmptyString, SecondaryIndexLiteral, SecondaryIndexLiteralError,
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EqualityIndexDomain {
    One(IndexValue),
    Many(crate::ir::AtLeast<IndexValue, 2>),
    RuntimeSet(NonEmptyString),
    Empty,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EqualityIndexAtom {
    Atom {
        property: String,
        domain: EqualityIndexDomain,
    },
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
        Predicate::IsIn { value, values } => equality_set_from_operands(value, values),
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
                    domain: EqualityIndexDomain::One(IndexValue::Literal(value)),
                }),
                Err(SecondaryIndexLiteralError::NestedValue) => Ok(EqualityIndexAtom::NotIndexable),
            }
        }
        (Expr::Property(property), Expr::Param(name))
        | (Expr::Param(name), Expr::Property(property)) => NonEmptyString::new(name.clone())
            .map(|name| EqualityIndexAtom::Atom {
                property: property.clone(),
                domain: EqualityIndexDomain::One(IndexValue::Param(name)),
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

fn equality_set_from_operands(
    value: &Expr,
    values: &Expr,
) -> Result<EqualityIndexAtom, PlannerError> {
    let Expr::Property(property) = value else {
        return Ok(EqualityIndexAtom::NotIndexable);
    };
    let domain = match values {
        Expr::Param(name) => EqualityIndexDomain::RuntimeSet(
            NonEmptyString::new(name.clone()).ok_or(PlannerError::InvalidEmptyName {
                field: NameField::Param,
            })?,
        ),
        Expr::Constant(value) => {
            let Some(domain) = literal_equality_set(value) else {
                return Ok(EqualityIndexAtom::NotIndexable);
            };
            domain
        }
        expr @ (Expr::Property(_)
        | Expr::Id
        | Expr::Timestamp
        | Expr::DateTimeNow
        | Expr::Add { .. }
        | Expr::Sub { .. }
        | Expr::Mul { .. }
        | Expr::Div { .. }
        | Expr::Mod { .. }
        | Expr::Neg { .. }
        | Expr::Case { .. }) => {
            return Err(PlannerError::NonLiteralIndexExpression {
                expression: format!("{expr:?}"),
            });
        }
    };
    Ok(EqualityIndexAtom::Atom {
        property: property.clone(),
        domain,
    })
}

fn literal_equality_set(value: &PropertyValue) -> Option<EqualityIndexDomain> {
    let values = match value {
        PropertyValue::I64Array(values) => values
            .iter()
            .copied()
            .map(PropertyValue::I64)
            .collect::<Vec<_>>(),
        PropertyValue::F64Array(values) => values
            .iter()
            .copied()
            .map(PropertyValue::F64)
            .collect::<Vec<_>>(),
        PropertyValue::F32Array(values) => values
            .iter()
            .copied()
            .map(PropertyValue::F32)
            .collect::<Vec<_>>(),
        PropertyValue::StringArray(values) => values
            .iter()
            .cloned()
            .map(PropertyValue::String)
            .collect::<Vec<_>>(),
        PropertyValue::Array(values) => values.clone(),
        value @ (PropertyValue::Null
        | PropertyValue::Bool(_)
        | PropertyValue::I64(_)
        | PropertyValue::DateTime(_)
        | PropertyValue::F64(_)
        | PropertyValue::F32(_)
        | PropertyValue::String(_)
        | PropertyValue::Bytes(_)
        | PropertyValue::Object(_)) => vec![value.clone()],
    };
    let values = values
        .into_iter()
        .map(SecondaryIndexLiteral::new)
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    let mut values = values.into_iter().fold(Vec::new(), |mut unique, value| {
        if value.semantics() != crate::ir::LiteralEqualityIndexValueSemantics::NonReflexive
            && !unique.iter().any(|existing: &SecondaryIndexLiteral| {
                existing.as_property_value() == value.as_property_value()
            })
        {
            unique.push(value);
        }
        unique
    });
    Some(match values.len() {
        0 => EqualityIndexDomain::Empty,
        1 => EqualityIndexDomain::One(IndexValue::Literal(
            values
                .pop()
                .expect("one-value equality domain contains one value"),
        )),
        _ => EqualityIndexDomain::Many(
            crate::ir::AtLeast::try_from_vec(values.into_iter().map(IndexValue::Literal).collect())
                .expect("multi-value equality domain contains at least two values"),
        ),
    })
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
    fn equality_atom_preserves_null_and_string_null_as_distinct_typed_literals() {
        for value in [PropertyValue::Null, PropertyValue::from("null")] {
            assert_eq!(
                equality_atom(&Predicate::Eq {
                    left: Expr::Property("deleted_at".to_owned()),
                    right: Expr::Constant(value.clone()),
                })
                .unwrap(),
                EqualityIndexAtom::Atom {
                    property: "deleted_at".to_owned(),
                    domain: EqualityIndexDomain::One(ir::IndexValue::Literal(
                        ir::SecondaryIndexLiteral::new(value).unwrap(),
                    )),
                }
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
                domain: EqualityIndexDomain::One(ir::IndexValue::Param(
                    ir::NonEmptyString::new("age").unwrap(),
                )),
            }
        );
    }

    #[test]
    fn membership_atoms_share_scalar_finite_and_runtime_equality_domains() {
        assert!(matches!(
            equality_atom(&Predicate::is_in("age", PropertyValue::I64(42))).unwrap(),
            EqualityIndexAtom::Atom {
                domain: EqualityIndexDomain::One(ir::IndexValue::Literal(_)),
                ..
            }
        ));
        assert!(matches!(
            equality_atom(&Predicate::is_in(
                "age",
                PropertyValue::F64Array(vec![f64::NAN, 1.0, 1.0, 2.0]),
            ))
            .unwrap(),
            EqualityIndexAtom::Atom {
                domain: EqualityIndexDomain::Many(values),
                ..
            } if values.len() == 2
        ));
        assert!(matches!(
            equality_atom(&Predicate::is_in_param("age", "ages")).unwrap(),
            EqualityIndexAtom::Atom {
                domain: EqualityIndexDomain::RuntimeSet(param),
                ..
            } if param.as_ref() == "ages"
        ));
        assert!(matches!(
            equality_atom(&Predicate::is_in(
                "age",
                PropertyValue::F32Array(vec![f32::NAN]),
            ))
            .unwrap(),
            EqualityIndexAtom::Atom {
                domain: EqualityIndexDomain::Empty,
                ..
            }
        ));
        assert_eq!(
            equality_atom(&Predicate::is_in(
                "age",
                PropertyValue::array([PropertyValue::object([("nested", 1)])]),
            ))
            .unwrap(),
            EqualityIndexAtom::NotIndexable
        );
    }
}
