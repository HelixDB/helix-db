use helix_ast::expr::Expr;
use helix_ast::value::{PropertyInput, PropertyValue};
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};

use super::super::{ExprPlan, ExprPlanError};

/// Invalid runtime property-input expression payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropertyInputExprPlanError {
    /// Static literal was supplied through the runtime expression arm.
    StaticLiteral,
    /// Runtime expression failed expression validation.
    Expression(ExprPlanError),
}

/// Runtime property-input expression.
///
/// Static constants are kept out of this arm so literal property inputs cannot
/// bypass the `Value` variant.
///
/// ```
/// use helix_ast::expr::Expr;
/// use helix_planner::ir::{PropertyInputExprPlan, PropertyInputExprPlanError};
///
/// assert!(PropertyInputExprPlan::new(Expr::param("name")).is_ok());
/// assert_eq!(
///     PropertyInputExprPlan::new(Expr::val("alice")).unwrap_err(),
///     PropertyInputExprPlanError::StaticLiteral
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(transparent)]
pub struct PropertyInputExprPlan {
    expr: ExprPlan,
}

impl PropertyInputExprPlan {
    /// Build a runtime property-input expression, rejecting constant expressions.
    pub fn new(expr: Expr) -> Result<Self, PropertyInputExprPlanError> {
        match expr {
            Expr::Constant(_) => Err(PropertyInputExprPlanError::StaticLiteral),
            expr => ExprPlan::new(expr)
                .map(|expr| Self { expr })
                .map_err(PropertyInputExprPlanError::Expression),
        }
    }

    /// Borrow the validated runtime expression.
    pub const fn expr(&self) -> &ExprPlan {
        &self.expr
    }
}

impl<'de> Deserialize<'de> for PropertyInputExprPlan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let expr = Expr::deserialize(deserializer)?;
        Self::new(expr).map_err(|err| match err {
            PropertyInputExprPlanError::StaticLiteral => {
                D::Error::custom("expected non-literal property input expression")
            }
            PropertyInputExprPlanError::Expression(err) => D::Error::custom(err),
        })
    }
}

/// Mutation/search input value with validated runtime expressions.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PropertyInputPlan {
    /// Literal value.
    Value(PropertyValue),
    /// Runtime expression.
    Expr(PropertyInputExprPlan),
}

impl PropertyInputPlan {
    /// Build an input plan after validating embedded expressions.
    pub fn new(input: PropertyInput) -> Result<Self, ExprPlanError> {
        match input {
            PropertyInput::Value(value) => Ok(Self::Value(value)),
            PropertyInput::Expr(Expr::Constant(value)) => Ok(Self::Value(value)),
            PropertyInput::Expr(expr) => {
                ExprPlan::new(expr).map(|expr| Self::Expr(PropertyInputExprPlan { expr }))
            }
        }
    }
}

impl<'de> Deserialize<'de> for PropertyInputPlan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "snake_case")]
        enum Raw {
            Value(PropertyValue),
            Expr(Expr),
        }

        match Raw::deserialize(deserializer)? {
            Raw::Value(value) => Ok(Self::Value(value)),
            Raw::Expr(expr) => Self::new(PropertyInput::Expr(expr)).map_err(D::Error::custom),
        }
    }
}
