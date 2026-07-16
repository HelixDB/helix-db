//! Positive search result-count limit ADTs.

use std::num::NonZeroUsize;

use helix_ast::expr::{Expr, StreamBound};
use helix_ast::value::PropertyValue;
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};

use super::super::{ExprPlan, ExprPlanError};

/// Expected shape for search result limits.
///
/// # Examples
///
/// ```
/// use helix_planner::ir::SearchLimitExpected;
///
/// assert_eq!(SearchLimitExpected::PositiveInteger.to_string(), "positive integer");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SearchLimitExpected {
    /// Positive integer.
    PositiveInteger,
}

impl std::fmt::Display for SearchLimitExpected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PositiveInteger => f.write_str("positive integer"),
        }
    }
}

/// Invalid search-limit plan payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchLimitPlanError {
    /// Literal result count was not positive.
    NonPositiveLiteral {
        /// Invalid literal value.
        actual: usize,
    },
    /// Static expression result count was not a positive integer literal.
    StaticLiteral {
        /// Expected literal shape.
        expected: SearchLimitExpected,
    },
    /// Runtime expression failed expression validation.
    Expression(ExprPlanError),
}

/// Invalid runtime search-limit expression payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchLimitExprPlanError {
    /// Static literal was supplied through the runtime expression arm.
    StaticLiteral {
        /// Expected literal shape.
        expected: SearchLimitExpected,
    },
    /// Runtime expression failed expression validation.
    Expression(ExprPlanError),
}

/// Runtime search result-count expression.
///
/// Literal constants are kept out of this arm so static search limits cannot
/// bypass the positive-integer literal contract.
///
/// ```
/// use helix_ast::expr::Expr;
/// use helix_planner::ir::{
///     SearchLimitExpected, SearchLimitExprPlan, SearchLimitExprPlanError,
/// };
///
/// assert!(SearchLimitExprPlan::new(Expr::param("limit")).is_ok());
/// assert_eq!(
///     SearchLimitExprPlan::new(Expr::val(1)).unwrap_err(),
///     SearchLimitExprPlanError::StaticLiteral { expected: SearchLimitExpected::PositiveInteger }
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(transparent)]
pub struct SearchLimitExprPlan {
    expr: ExprPlan,
}

impl SearchLimitExprPlan {
    /// Build a runtime search-limit expression, rejecting constant expressions.
    pub fn new(expr: Expr) -> Result<Self, SearchLimitExprPlanError> {
        match expr {
            Expr::Constant(_) => Err(SearchLimitExprPlanError::StaticLiteral {
                expected: SearchLimitExpected::PositiveInteger,
            }),
            expr => ExprPlan::new(expr)
                .map(|expr| Self { expr })
                .map_err(SearchLimitExprPlanError::Expression),
        }
    }

    /// Borrow the validated runtime expression.
    ///
    /// ```
    /// use helix_ast::expr::Expr;
    /// use helix_planner::ir::SearchLimitExprPlan;
    ///
    /// let plan = SearchLimitExprPlan::new(Expr::param("k")).unwrap();
    /// assert_eq!(plan.expr(), &Expr::param("k"));
    /// ```
    pub fn expr(&self) -> &Expr {
        self.expr.expr()
    }
}

impl<'de> Deserialize<'de> for SearchLimitExprPlan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let expr = Expr::deserialize(deserializer)?;
        Self::new(expr).map_err(|err| match err {
            SearchLimitExprPlanError::StaticLiteral { expected } => {
                D::Error::custom(format!("expected non-literal {expected} expression"))
            }
            SearchLimitExprPlanError::Expression(err) => D::Error::custom(err),
        })
    }
}

/// Positive search result count with validated runtime expressions.
///
/// ```
/// use helix_ast::expr::{Expr, StreamBound};
/// use helix_planner::ir::{SearchLimitExpected, SearchLimitPlan, SearchLimitPlanError};
///
/// assert!(matches!(
///     SearchLimitPlan::new(StreamBound::Literal(0)),
///     Err(SearchLimitPlanError::NonPositiveLiteral { actual: 0 })
/// ));
/// assert!(matches!(
///     SearchLimitPlan::new(StreamBound::expr(Expr::val("nope"))),
///     Err(SearchLimitPlanError::StaticLiteral { expected: SearchLimitExpected::PositiveInteger })
/// ));
/// assert!(SearchLimitPlan::new(StreamBound::expr(Expr::param("limit"))).is_ok());
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchLimitPlan {
    /// Positive literal result count.
    Literal(NonZeroUsize),
    /// Runtime expression result count.
    Expr(SearchLimitExprPlan),
}

impl SearchLimitPlan {
    /// Build a search result count plan.
    pub fn new(bound: StreamBound) -> Result<Self, SearchLimitPlanError> {
        match bound {
            StreamBound::Literal(value) => NonZeroUsize::new(value)
                .map(Self::Literal)
                .ok_or(SearchLimitPlanError::NonPositiveLiteral { actual: value }),
            StreamBound::Expr(Expr::Constant(PropertyValue::I64(value))) => {
                match usize::try_from(value) {
                    Ok(value) => NonZeroUsize::new(value)
                        .map(Self::Literal)
                        .ok_or(SearchLimitPlanError::NonPositiveLiteral { actual: value }),
                    Err(_) => Err(SearchLimitPlanError::StaticLiteral {
                        expected: SearchLimitExpected::PositiveInteger,
                    }),
                }
            }
            StreamBound::Expr(expr) => {
                SearchLimitExprPlan::new(expr)
                    .map(Self::Expr)
                    .map_err(|err| match err {
                        SearchLimitExprPlanError::StaticLiteral { expected } => {
                            SearchLimitPlanError::StaticLiteral { expected }
                        }
                        SearchLimitExprPlanError::Expression(err) => {
                            SearchLimitPlanError::Expression(err)
                        }
                    })
            }
        }
    }
}

impl PartialEq<StreamBound> for SearchLimitPlan {
    fn eq(&self, other: &StreamBound) -> bool {
        match (self, other) {
            (Self::Literal(left), StreamBound::Literal(right)) => left.get() == *right,
            (Self::Expr(left), StreamBound::Expr(right)) => &left.expr == right,
            (Self::Literal(_), StreamBound::Expr(_)) | (Self::Expr(_), StreamBound::Literal(_)) => {
                false
            }
        }
    }
}
