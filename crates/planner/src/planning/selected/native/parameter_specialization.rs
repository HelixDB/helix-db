//! Immutable request-parameter specialization for predicate planning.

use helix_ast::expr::{Expr, Predicate, WhenThen};
use helix_ast::value::PropertyValue;

use crate::{context, error, ir};

enum PlanningParameter {
    Literal(PropertyValue),
    Runtime(ir::NonEmptyString),
}

pub(super) fn predicate(
    ctx: &context::PlannerContext,
    predicate: &Predicate,
) -> Result<Predicate, error::PlannerError> {
    Ok(match predicate {
        Predicate::Eq { left, right } => Predicate::Eq {
            left: expression(ctx, left)?,
            right: expression(ctx, right)?,
        },
        Predicate::Neq { left, right } => Predicate::Neq {
            left: expression(ctx, left)?,
            right: expression(ctx, right)?,
        },
        Predicate::Gt { left, right } => Predicate::Gt {
            left: expression(ctx, left)?,
            right: expression(ctx, right)?,
        },
        Predicate::Gte { left, right } => Predicate::Gte {
            left: expression(ctx, left)?,
            right: expression(ctx, right)?,
        },
        Predicate::Lt { left, right } => Predicate::Lt {
            left: expression(ctx, left)?,
            right: expression(ctx, right)?,
        },
        Predicate::Lte { left, right } => Predicate::Lte {
            left: expression(ctx, left)?,
            right: expression(ctx, right)?,
        },
        Predicate::Between { value, min, max } => Predicate::Between {
            value: expression(ctx, value)?,
            min: expression(ctx, min)?,
            max: expression(ctx, max)?,
        },
        Predicate::HasKey { .. } | Predicate::IsNull { .. } | Predicate::IsNotNull { .. } => {
            predicate.clone()
        }
        Predicate::StartsWith { value, prefix } => Predicate::StartsWith {
            value: expression(ctx, value)?,
            prefix: expression(ctx, prefix)?,
        },
        Predicate::EndsWith { value, suffix } => Predicate::EndsWith {
            value: expression(ctx, value)?,
            suffix: expression(ctx, suffix)?,
        },
        Predicate::Contains { value, substring } => Predicate::Contains {
            value: expression(ctx, value)?,
            substring: expression(ctx, substring)?,
        },
        Predicate::IsIn { value, values } => Predicate::IsIn {
            value: expression(ctx, value)?,
            values: expression(ctx, values)?,
        },
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
        Predicate::Compare { left, op, right } => Predicate::Compare {
            left: expression(ctx, left)?,
            op: *op,
            right: expression(ctx, right)?,
        },
    })
}

fn expression(
    ctx: &context::PlannerContext,
    expression: &Expr,
) -> Result<Expr, error::PlannerError> {
    Ok(match expression {
        Expr::Property(_) | Expr::Id | Expr::Timestamp | Expr::DateTimeNow | Expr::Constant(_) => {
            expression.clone()
        }
        Expr::Param(name) => match planning_parameter(ctx, name)? {
            PlanningParameter::Literal(value) => Expr::Constant(value),
            PlanningParameter::Runtime(name) => Expr::Param(name.as_ref().to_owned()),
        },
        Expr::Add { left, right } => Expr::Add {
            left: Box::new(self::expression(ctx, left)?),
            right: Box::new(self::expression(ctx, right)?),
        },
        Expr::Sub { left, right } => Expr::Sub {
            left: Box::new(self::expression(ctx, left)?),
            right: Box::new(self::expression(ctx, right)?),
        },
        Expr::Mul { left, right } => Expr::Mul {
            left: Box::new(self::expression(ctx, left)?),
            right: Box::new(self::expression(ctx, right)?),
        },
        Expr::Div { left, right } => Expr::Div {
            left: Box::new(self::expression(ctx, left)?),
            right: Box::new(self::expression(ctx, right)?),
        },
        Expr::Mod { left, right } => Expr::Mod {
            left: Box::new(self::expression(ctx, left)?),
            right: Box::new(self::expression(ctx, right)?),
        },
        Expr::Neg { expr } => Expr::Neg {
            expr: Box::new(self::expression(ctx, expr)?),
        },
        Expr::Case {
            when_then,
            else_expr,
        } => Expr::Case {
            when_then: when_then
                .iter()
                .map(|branch| {
                    Ok(WhenThen {
                        when: self::predicate(ctx, &branch.when)?,
                        then: self::expression(ctx, &branch.then)?,
                    })
                })
                .collect::<Result<_, error::PlannerError>>()?,
            else_expr: else_expr
                .as_deref()
                .map(|expression| self::expression(ctx, expression).map(Box::new))
                .transpose()?,
        },
    })
}

fn planning_parameter(
    ctx: &context::PlannerContext,
    name: &str,
) -> Result<PlanningParameter, error::PlannerError> {
    let name = ir::NonEmptyString::new(name).ok_or(error::PlannerError::InvalidEmptyName {
        field: ir::NameField::Param,
    })?;
    // A foreach frame expands object fields into the parameter namespace. The
    // AST declares the container parameter, but not the field names, so every
    // parameter referenced from an enclosed body can be shadowed at runtime.
    if !ctx.late_bound_params.is_empty() {
        return Ok(PlanningParameter::Runtime(name));
    }
    if let Some(value) = ctx.params.values.get(&name) {
        return Ok(PlanningParameter::Literal(value.clone()));
    }
    if let Some(value) = ctx.params.query_values.get(&name) {
        return Ok(PlanningParameter::Literal(PropertyValue::from(value)));
    }
    Err(error::PlannerError::MissingPlanningEqualityParameter { param: name })
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_ast::query::QueryValue;

    fn name(value: &str) -> ir::NonEmptyString {
        ir::NonEmptyString::new(value).unwrap()
    }

    #[test]
    fn specialization_recurses_through_predicates_and_expressions() {
        let ctx = context::PlannerContext {
            params: context::ParamBindings::default()
                .with_query_value(name("minimum"), QueryValue::I64(18))
                .with_query_value(name("needle"), QueryValue::String("rust".to_owned())),
            ..context::PlannerContext::default()
        };
        let original = Predicate::and(vec![
            Predicate::gte_param("age", "minimum"),
            Predicate::Contains {
                value: Expr::Property("bio".to_owned()),
                substring: Expr::Case {
                    when_then: vec![WhenThen {
                        when: Predicate::eq_param("age", "minimum"),
                        then: Expr::Param("needle".to_owned()),
                    }],
                    else_expr: Some(Box::new(Expr::Param("needle".to_owned()))),
                },
            },
        ]);

        let specialized = predicate(&ctx, &original).unwrap();

        assert!(!format!("{specialized:?}").contains("Param"));
    }

    #[test]
    fn nested_query_values_specialize_without_index_policy() {
        let value = QueryValue::Array(vec![
            QueryValue::I64(1),
            QueryValue::Object(std::collections::BTreeMap::from([(
                "enabled".to_owned(),
                QueryValue::Bool(true),
            )])),
        ]);
        let ctx = context::PlannerContext {
            params: context::ParamBindings::default()
                .with_query_value(name("values"), value.clone()),
            ..context::PlannerContext::default()
        };

        assert_eq!(
            predicate(&ctx, &Predicate::is_in_param("value", "values")).unwrap(),
            Predicate::is_in("value", PropertyValue::from(&value))
        );
    }

    #[test]
    fn active_runtime_scope_preserves_all_parameters() {
        let mut ctx = context::PlannerContext {
            params: context::ParamBindings::default().with_value(name("status"), "static"),
            ..context::PlannerContext::default()
        };
        ctx.late_bound_params.insert(name("items"));
        let original = Predicate::and(vec![
            Predicate::eq_param("status", "status"),
            Predicate::is_in_param("group", "groups"),
        ]);

        assert_eq!(predicate(&ctx, &original).unwrap(), original);
    }

    #[test]
    fn missing_and_empty_parameter_names_are_rejected() {
        assert!(matches!(
            predicate(
                &context::PlannerContext::default(),
                &Predicate::eq_param("status", "missing")
            ),
            Err(error::PlannerError::MissingPlanningEqualityParameter { param })
                if param.as_ref() == "missing"
        ));
        assert!(matches!(
            predicate(
                &context::PlannerContext::default(),
                &Predicate::eq_param("status", "")
            ),
            Err(error::PlannerError::InvalidEmptyName {
                field: ir::NameField::Param
            })
        ));
    }
}
