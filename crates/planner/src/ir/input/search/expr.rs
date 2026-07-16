use helix_ast::expr::Expr;
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};

use super::super::super::{ExprPlan, ExprPlanError};

/// Invalid runtime search-query expression payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchQueryExprPlanError {
    /// Static literal was supplied through the runtime expression arm.
    StaticLiteral,
    /// Runtime expression failed expression validation.
    Expression(ExprPlanError),
}

/// Runtime search-query expression.
///
/// Static constants are kept out of this arm so literal vector/text payloads
/// cannot bypass the shape-specific contracts on
/// [`crate::ir::VectorQueryInputPlan`] and [`crate::ir::TextQueryInputPlan`].
///
/// ```
/// use helix_ast::expr::Expr;
/// use helix_planner::ir::{SearchQueryExprPlan, SearchQueryExprPlanError};
///
/// assert!(SearchQueryExprPlan::new(Expr::param("query")).is_ok());
/// assert_eq!(
///     SearchQueryExprPlan::new(Expr::val("needle")).unwrap_err(),
///     SearchQueryExprPlanError::StaticLiteral
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(transparent)]
pub struct SearchQueryExprPlan {
    expr: ExprPlan,
}

impl SearchQueryExprPlan {
    /// Build a runtime search-query expression, rejecting constant expressions.
    pub fn new(expr: Expr) -> Result<Self, SearchQueryExprPlanError> {
        match expr {
            Expr::Constant(_) => Err(SearchQueryExprPlanError::StaticLiteral),
            expr => ExprPlan::new(expr)
                .map(|expr| Self { expr })
                .map_err(SearchQueryExprPlanError::Expression),
        }
    }

    /// Borrow the validated runtime expression.
    ///
    /// ```
    /// use helix_ast::expr::Expr;
    /// use helix_planner::ir::SearchQueryExprPlan;
    ///
    /// let plan = SearchQueryExprPlan::new(Expr::param("query")).unwrap();
    /// assert_eq!(plan.expr(), &Expr::param("query"));
    /// ```
    pub fn expr(&self) -> &Expr {
        self.expr.expr()
    }
}

impl<'de> Deserialize<'de> for SearchQueryExprPlan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let expr = Expr::deserialize(deserializer)?;
        Self::new(expr).map_err(|err| match err {
            SearchQueryExprPlanError::StaticLiteral => {
                D::Error::custom("expected non-literal search-query expression")
            }
            SearchQueryExprPlanError::Expression(err) => D::Error::custom(err),
        })
    }
}
