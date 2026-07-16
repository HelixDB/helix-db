//! Validated runtime expression wrapper.

use helix_ast::expr::Expr;
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::error::ExprPlanError;
use super::validation::validate_expr;

/// Runtime expression with validated parameter and property names.
#[derive(Debug, Clone, PartialEq)]
pub struct ExprPlan {
    expr: Expr,
}

impl ExprPlan {
    /// Build an expression plan after recursively validating embedded names.
    pub fn new(expr: Expr) -> Result<Self, ExprPlanError> {
        validate_expr(&expr)?;
        Ok(Self { expr })
    }

    /// Borrow the validated expression.
    ///
    /// ```
    /// use helix_ast::expr::Expr;
    /// use helix_planner::ir::ExprPlan;
    ///
    /// let expr = Expr::param("limit");
    /// let plan = ExprPlan::new(expr.clone()).unwrap();
    /// assert_eq!(plan.expr(), &expr);
    /// ```
    pub fn expr(&self) -> &Expr {
        &self.expr
    }
}

impl PartialEq<Expr> for ExprPlan {
    fn eq(&self, other: &Expr) -> bool {
        &self.expr == other
    }
}

impl Serialize for ExprPlan {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.expr.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ExprPlan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let expr = Expr::deserialize(deserializer)?;
        Self::new(expr).map_err(D::Error::custom)
    }
}
