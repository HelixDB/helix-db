//! Validated shared expression with a native serialization compatibility view.

use helix_ast::expr::Expr;
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::error::ExprPlanError;
use super::validation::validate_expr;

/// Runtime expression with validated parameter and property names.
#[derive(Debug, Clone, PartialEq)]
pub struct ExprPlan {
    resolved: std::sync::Arc<super::native::Expression>,
    // Compatibility view for existing native rewrite rules and serialized plans.
    // Execution uses `resolved`; both views are constructed once and immutable.
    expr: std::sync::Arc<Expr>,
}

impl ExprPlan {
    /// Build an expression plan after recursively validating embedded names.
    pub fn new(expr: Expr) -> Result<Self, ExprPlanError> {
        validate_expr(&expr)?;
        Ok(Self {
            resolved: std::sync::Arc::new(super::native::expression(&expr)),
            expr: std::sync::Arc::new(expr),
        })
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

    /// Borrow the shared scalar representation with explicit native semantics.
    pub fn resolved(&self) -> &super::native::Expression {
        &self.resolved
    }
}

impl PartialEq<Expr> for ExprPlan {
    fn eq(&self, other: &Expr) -> bool {
        self.expr.as_ref() == other
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
