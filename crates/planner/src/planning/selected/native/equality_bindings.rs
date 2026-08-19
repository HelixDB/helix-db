//! Immutable request binding specialization for indexable equalities.

use helix_ast::expr::{CompareOp, Expr, Predicate};
use helix_ast::query::QueryValue;
use helix_ast::value::PropertyValue;

use crate::{context, error, ir};

pub(super) fn predicate(
    ctx: &context::PlannerContext,
    predicate: &Predicate,
) -> Result<Predicate, error::PlannerError> {
    Ok(match predicate {
        Predicate::Eq { left, right } => {
            let (left, right) = equality_operands(ctx, left, right)?;
            Predicate::Eq { left, right }
        }
        Predicate::Compare {
            left,
            op: CompareOp::Eq,
            right,
        } => {
            let (left, right) = equality_operands(ctx, left, right)?;
            Predicate::Compare {
                left,
                op: CompareOp::Eq,
                right,
            }
        }
        Predicate::And { predicates } => Predicate::And {
            predicates: predicates
                .iter()
                .map(|child| self::predicate(ctx, child))
                .collect::<Result<_, _>>()?,
        },
        Predicate::Or { predicates } => Predicate::Or {
            predicates: predicates
                .iter()
                .map(|child| self::predicate(ctx, child))
                .collect::<Result<_, _>>()?,
        },
        Predicate::Not { predicate } => Predicate::Not {
            predicate: Box::new(self::predicate(ctx, predicate)?),
        },
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
        | Predicate::Compare { .. } => predicate.clone(),
    })
}

fn equality_operands(
    ctx: &context::PlannerContext,
    left: &Expr,
    right: &Expr,
) -> Result<(Expr, Expr), error::PlannerError> {
    match (left, right) {
        (Expr::Property(_), Expr::Param(param)) => Ok((left.clone(), bound_parameter(ctx, param)?)),
        (Expr::Param(param), Expr::Property(_)) => {
            Ok((bound_parameter(ctx, param)?, right.clone()))
        }
        _ => Ok((left.clone(), right.clone())),
    }
}

fn bound_parameter(
    ctx: &context::PlannerContext,
    param: &str,
) -> Result<Expr, error::PlannerError> {
    let param = ir::NonEmptyString::new(param).ok_or(error::PlannerError::InvalidEmptyName {
        field: ir::NameField::Param,
    })?;
    // A foreach frame expands object fields into the parameter namespace. The
    // AST declares the container parameter, but not the field names, so every
    // parameter referenced from an enclosed body can be shadowed at runtime.
    if !ctx.late_bound_params.is_empty() {
        return Ok(Expr::Param(param.as_ref().to_owned()));
    }
    let value = if let Some(value) = ctx.params.values.get(&param) {
        value.clone()
    } else if let Some(value) = ctx.params.query_values.get(&param) {
        query_property_value(value).ok_or_else(|| {
            error::PlannerError::UnsupportedPlanningEqualityParameter {
                param: param.clone(),
            }
        })?
    } else {
        return Err(error::PlannerError::MissingPlanningEqualityParameter { param });
    };
    ir::SecondaryIndexLiteral::new(value.clone()).map_err(|_| {
        error::PlannerError::UnsupportedPlanningEqualityParameter {
            param: param.clone(),
        }
    })?;
    Ok(Expr::Constant(value))
}

fn query_property_value(value: &QueryValue) -> Option<PropertyValue> {
    match value {
        QueryValue::Null => Some(PropertyValue::Null),
        QueryValue::Bool(value) => Some(PropertyValue::Bool(*value)),
        QueryValue::I64(value) => Some(PropertyValue::I64(*value)),
        QueryValue::F64(value) => Some(PropertyValue::F64(*value)),
        QueryValue::F32(value) => Some(PropertyValue::F32(*value)),
        QueryValue::String(value) => Some(PropertyValue::String(value.clone())),
        QueryValue::Array(_) | QueryValue::Object(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(value: &str) -> ir::NonEmptyString {
        ir::NonEmptyString::new(value).unwrap()
    }

    fn equality(param: &str) -> Predicate {
        Predicate::Eq {
            left: Expr::Property("status".to_owned()),
            right: Expr::Param(param.to_owned()),
        }
    }

    #[test]
    fn immutable_property_and_query_scalars_become_literals() {
        for value in [
            PropertyValue::Null,
            PropertyValue::Bool(true),
            PropertyValue::I64(7),
            PropertyValue::F64(1.5),
            PropertyValue::F32(2.5),
            PropertyValue::String("active".to_owned()),
            PropertyValue::F64Array(vec![1.0, 2.0]),
            PropertyValue::F32Array(vec![3.0, 4.0]),
        ] {
            let ctx = context::PlannerContext {
                params: context::ParamBindings::default().with_value(name("wanted"), value.clone()),
                ..context::PlannerContext::default()
            };
            assert_eq!(
                predicate(&ctx, &equality("wanted")).unwrap(),
                Predicate::Eq {
                    left: Expr::Property("status".to_owned()),
                    right: Expr::Constant(value),
                }
            );
        }

        for (query, property) in [
            (QueryValue::Null, PropertyValue::Null),
            (QueryValue::Bool(true), PropertyValue::Bool(true)),
            (QueryValue::I64(7), PropertyValue::I64(7)),
            (QueryValue::F64(1.5), PropertyValue::F64(1.5)),
            (QueryValue::F32(2.5), PropertyValue::F32(2.5)),
            (
                QueryValue::String("active".to_owned()),
                PropertyValue::String("active".to_owned()),
            ),
        ] {
            let ctx = context::PlannerContext {
                params: context::ParamBindings::default().with_query_value(name("wanted"), query),
                ..context::PlannerContext::default()
            };
            assert!(matches!(
                predicate(&ctx, &equality("wanted")).unwrap(),
                Predicate::Eq {
                    right: Expr::Constant(actual),
                    ..
                } if actual == property
            ));
        }
    }

    #[test]
    fn reversed_compare_recursive_sets_and_late_bound_values_keep_exact_scope() {
        let mut late = context::PlannerContext::default();
        late.late_bound_params.insert(name("late"));
        assert_eq!(
            predicate(&late, &equality("late")).unwrap(),
            equality("late")
        );

        let ctx = context::PlannerContext {
            params: context::ParamBindings::default().with_value(name("wanted"), "active"),
            ..context::PlannerContext::default()
        };
        let reversed = Predicate::Compare {
            left: Expr::Param("wanted".to_owned()),
            op: CompareOp::Eq,
            right: Expr::Property("status".to_owned()),
        };
        assert!(matches!(
            predicate(&ctx, &reversed).unwrap(),
            Predicate::Compare {
                left: Expr::Constant(PropertyValue::String(value)),
                op: CompareOp::Eq,
                ..
            } if value == "active"
        ));

        let recursive = Predicate::And {
            predicates: vec![
                Predicate::Or {
                    predicates: vec![equality("wanted")],
                },
                Predicate::Not {
                    predicate: Box::new(equality("wanted")),
                },
            ],
        };
        let bound = predicate(&ctx, &recursive).unwrap();
        assert!(!format!("{bound:?}").contains("Param"));
    }

    #[test]
    fn missing_invalid_and_nested_values_are_planning_errors() {
        assert!(matches!(
            predicate(&context::PlannerContext::default(), &equality("missing")),
            Err(error::PlannerError::MissingPlanningEqualityParameter { param })
                if param.as_ref() == "missing"
        ));
        assert!(matches!(
            predicate(&context::PlannerContext::default(), &equality("")),
            Err(error::PlannerError::InvalidEmptyName {
                field: ir::NameField::Param
            })
        ));

        for value in [
            QueryValue::Array(vec![QueryValue::I64(1)]),
            QueryValue::Object(std::collections::BTreeMap::from([(
                "nested".to_owned(),
                QueryValue::Bool(true),
            )])),
        ] {
            let ctx = context::PlannerContext {
                params: context::ParamBindings::default().with_query_value(name("nested"), value),
                ..context::PlannerContext::default()
            };
            assert!(matches!(
                predicate(&ctx, &equality("nested")),
                Err(error::PlannerError::UnsupportedPlanningEqualityParameter { param })
                    if param.as_ref() == "nested"
            ));
        }

        let ctx = context::PlannerContext {
            params: context::ParamBindings::default().with_value(
                name("nested"),
                PropertyValue::object([("field", PropertyValue::Bool(true))]),
            ),
            ..context::PlannerContext::default()
        };
        assert!(matches!(
            predicate(&ctx, &equality("nested")),
            Err(error::PlannerError::UnsupportedPlanningEqualityParameter { .. })
        ));
    }

    #[test]
    fn recursive_and_reversed_equalities_propagate_binding_failures() {
        let property = Expr::Property("status".to_owned());
        let missing = Expr::Param("missing".to_owned());
        let cases = [
            Predicate::Compare {
                left: missing.clone(),
                op: CompareOp::Eq,
                right: property.clone(),
            },
            Predicate::And {
                predicates: vec![equality("missing")],
            },
            Predicate::Or {
                predicates: vec![equality("missing")],
            },
            Predicate::Not {
                predicate: Box::new(equality("missing")),
            },
        ];

        for predicate in cases {
            assert!(matches!(
                self::predicate(&context::PlannerContext::default(), &predicate),
                Err(error::PlannerError::MissingPlanningEqualityParameter { param })
                    if param.as_ref() == "missing"
            ));
        }
    }

    #[test]
    fn non_indexable_predicate_families_remain_unchanged() {
        let property = Expr::Property("value".to_owned());
        let constant = Expr::Constant(PropertyValue::I64(1));
        let predicates = vec![
            Predicate::Eq {
                left: property.clone(),
                right: constant.clone(),
            },
            Predicate::Eq {
                left: Expr::Param("unrelated".to_owned()),
                right: constant.clone(),
            },
            Predicate::Neq {
                left: property.clone(),
                right: constant.clone(),
            },
            Predicate::Gt {
                left: property.clone(),
                right: constant.clone(),
            },
            Predicate::Gte {
                left: property.clone(),
                right: constant.clone(),
            },
            Predicate::Lt {
                left: property.clone(),
                right: constant.clone(),
            },
            Predicate::Lte {
                left: property.clone(),
                right: constant.clone(),
            },
            Predicate::Between {
                value: property.clone(),
                min: constant.clone(),
                max: constant.clone(),
            },
            Predicate::HasKey {
                property: "value".to_owned(),
            },
            Predicate::IsNull {
                property: "value".to_owned(),
            },
            Predicate::IsNotNull {
                property: "value".to_owned(),
            },
            Predicate::StartsWith {
                value: property.clone(),
                prefix: constant.clone(),
            },
            Predicate::EndsWith {
                value: property.clone(),
                suffix: constant.clone(),
            },
            Predicate::Contains {
                value: property.clone(),
                substring: constant.clone(),
            },
            Predicate::IsIn {
                value: property.clone(),
                values: constant.clone(),
            },
            Predicate::Compare {
                left: property,
                op: CompareOp::Gte,
                right: constant,
            },
        ];
        for original in predicates {
            assert_eq!(
                predicate(&context::PlannerContext::default(), &original).unwrap(),
                original
            );
        }
    }
}
