//! Recursive expression and predicate validation.

use helix_ast::expr::{Expr, Predicate};

use super::error::{ExprPlanError, NameField, PredicateSetOp};

pub(super) fn validate_expr(expr: &Expr) -> Result<(), ExprPlanError> {
    match expr {
        Expr::Property(property) => validate_expr_name(property, NameField::Property),
        Expr::Param(param) => validate_expr_name(param, NameField::Param),
        Expr::Constant(_) | Expr::Id | Expr::Timestamp | Expr::DateTimeNow => Ok(()),
        Expr::Add { left, right }
        | Expr::Sub { left, right }
        | Expr::Mul { left, right }
        | Expr::Div { left, right }
        | Expr::Mod { left, right } => {
            validate_expr(left)?;
            validate_expr(right)
        }
        Expr::Neg { expr } => validate_expr(expr),
        Expr::Case {
            when_then,
            else_expr,
        } => {
            when_then.iter().try_for_each(|branch| {
                validate_predicate(&branch.when)?;
                validate_expr(&branch.then)
            })?;
            else_expr.as_deref().map_or(Ok(()), validate_expr)
        }
    }
}

pub(super) fn validate_predicate(predicate: &Predicate) -> Result<(), ExprPlanError> {
    match predicate {
        Predicate::Eq { left, right }
        | Predicate::Neq { left, right }
        | Predicate::Gt { left, right }
        | Predicate::Gte { left, right }
        | Predicate::Lt { left, right }
        | Predicate::Lte { left, right }
        | Predicate::StartsWith {
            value: left,
            prefix: right,
        }
        | Predicate::EndsWith {
            value: left,
            suffix: right,
        }
        | Predicate::Contains {
            value: left,
            substring: right,
        }
        | Predicate::IsIn {
            value: left,
            values: right,
        }
        | Predicate::Compare { left, right, .. } => {
            validate_expr(left)?;
            validate_expr(right)
        }
        Predicate::Between { value, min, max } => {
            validate_expr(value)?;
            validate_expr(min)?;
            validate_expr(max)
        }
        Predicate::HasKey { property }
        | Predicate::IsNull { property }
        | Predicate::IsNotNull { property } => validate_expr_name(property, NameField::Property),
        Predicate::And { predicates } => match predicates.as_slice() {
            [] => Err(ExprPlanError::EmptyPredicateSet {
                op: PredicateSetOp::And,
            }),
            predicates => predicates.iter().try_for_each(validate_predicate),
        },
        Predicate::Or { predicates } => match predicates.as_slice() {
            [] => Err(ExprPlanError::EmptyPredicateSet {
                op: PredicateSetOp::Or,
            }),
            predicates => predicates.iter().try_for_each(validate_predicate),
        },
        Predicate::Not { predicate } => validate_predicate(predicate),
    }
}

fn validate_expr_name(value: &str, field: NameField) -> Result<(), ExprPlanError> {
    if value.is_empty() {
        Err(ExprPlanError::EmptyName { field })
    } else {
        Ok(())
    }
}
