//! Range-index value extraction.

use helix_ast::expr::Expr;

use crate::error::PlannerError;
use crate::ir::{NameField, NonEmptyString, RangeIndexValue};

#[derive(Debug, Clone, PartialEq)]
pub(super) enum RangeIndexValueAtom {
    Value(RangeIndexValue),
    NotIndexable,
}

pub(super) fn expr_range_index_value(expr: &Expr) -> Result<RangeIndexValueAtom, PlannerError> {
    match expr {
        Expr::Constant(value) => Ok(RangeIndexValue::literal(value.clone()).map_or(
            RangeIndexValueAtom::NotIndexable,
            RangeIndexValueAtom::Value,
        )),
        Expr::Param(name) => NonEmptyString::new(name.clone())
            .map(RangeIndexValue::Param)
            .map(RangeIndexValueAtom::Value)
            .ok_or(PlannerError::InvalidEmptyName {
                field: NameField::Param,
            }),
        Expr::Property(_)
        | Expr::Id
        | Expr::Timestamp
        | Expr::DateTimeNow
        | Expr::Add { .. }
        | Expr::Sub { .. }
        | Expr::Mul { .. }
        | Expr::Div { .. }
        | Expr::Mod { .. }
        | Expr::Neg { .. }
        | Expr::Case { .. } => Err(PlannerError::NonLiteralIndexExpression {
            expression: format!("{expr:?}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_ast::value::PropertyValue;

    #[test]
    fn range_value_accepts_params_and_rejects_empty_param_names() {
        assert_eq!(
            expr_range_index_value(&Expr::Param("age".to_owned())).unwrap(),
            RangeIndexValueAtom::Value(RangeIndexValue::Param(NonEmptyString::new("age").unwrap()))
        );
        assert_eq!(
            expr_range_index_value(&Expr::Param(String::new())),
            Err(PlannerError::InvalidEmptyName {
                field: NameField::Param,
            })
        );
    }

    #[test]
    fn range_value_distinguishes_non_range_literals_from_non_literals() {
        assert_eq!(
            expr_range_index_value(&Expr::Constant(PropertyValue::Bool(true))).unwrap(),
            RangeIndexValueAtom::NotIndexable
        );
        assert_eq!(
            expr_range_index_value(&Expr::Property("age".to_owned())),
            Err(PlannerError::NonLiteralIndexExpression {
                expression: "Property(\"age\")".to_owned(),
            })
        );
    }
}
