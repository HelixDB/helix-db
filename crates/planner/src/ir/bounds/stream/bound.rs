//! Non-negative literal/runtime stream-bound contracts.

use helix_ast::expr::{Expr, StreamBound};
use helix_ast::value::PropertyValue;
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};

use super::super::super::{ExprPlan, ExprPlanError};

/// Expected shape for stream bounds.
///
/// # Examples
///
/// ```
/// use helix_planner::ir::StreamBoundExpected;
///
/// assert_eq!(StreamBoundExpected::NonNegativeInteger.to_string(), "non-negative integer");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamBoundExpected {
    /// Non-negative integer.
    NonNegativeInteger,
}

impl std::fmt::Display for StreamBoundExpected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NonNegativeInteger => f.write_str("non-negative integer"),
        }
    }
}

/// Invalid stream-bound plan payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamBoundPlanError {
    /// Static expression bound was not a non-negative integer literal.
    StaticLiteral {
        /// Expected literal shape.
        expected: StreamBoundExpected,
    },
    /// Runtime bound expression failed expression validation.
    Expression(ExprPlanError),
}

/// Invalid runtime stream-bound expression payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamBoundExprPlanError {
    /// Static literal was supplied through the runtime expression arm.
    StaticLiteral {
        /// Expected literal shape.
        expected: StreamBoundExpected,
    },
    /// Runtime expression failed expression validation.
    Expression(ExprPlanError),
}

/// Runtime stream-bound expression.
///
/// Static constants are kept out of this arm so stream-bound literals cannot
/// bypass the non-negative-integer contract.
///
/// ```
/// use helix_ast::expr::Expr;
/// use helix_planner::ir::{StreamBoundExpected, StreamBoundExprPlan, StreamBoundExprPlanError};
///
/// assert!(StreamBoundExprPlan::new(Expr::param("limit")).is_ok());
/// assert_eq!(
///     StreamBoundExprPlan::new(Expr::val(1)).unwrap_err(),
///     StreamBoundExprPlanError::StaticLiteral {
///         expected: StreamBoundExpected::NonNegativeInteger,
///     }
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(transparent)]
pub struct StreamBoundExprPlan {
    expr: ExprPlan,
}

impl StreamBoundExprPlan {
    /// Build a runtime stream-bound expression, rejecting constant expressions.
    pub fn new(expr: Expr) -> Result<Self, StreamBoundExprPlanError> {
        match expr {
            Expr::Constant(_) => Err(StreamBoundExprPlanError::StaticLiteral {
                expected: StreamBoundExpected::NonNegativeInteger,
            }),
            expr => ExprPlan::new(expr)
                .map(|expr| Self { expr })
                .map_err(StreamBoundExprPlanError::Expression),
        }
    }

    /// Borrow the validated runtime expression.
    ///
    /// ```
    /// use helix_ast::expr::Expr;
    /// use helix_planner::ir::StreamBoundExprPlan;
    ///
    /// let plan = StreamBoundExprPlan::new(Expr::param("limit")).unwrap();
    /// assert_eq!(plan.expr(), &Expr::param("limit"));
    /// ```
    pub fn expr(&self) -> &Expr {
        self.expr.expr()
    }
}

impl<'de> Deserialize<'de> for StreamBoundExprPlan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let expr = Expr::deserialize(deserializer)?;
        Self::new(expr).map_err(|err| match err {
            StreamBoundExprPlanError::StaticLiteral { expected } => {
                D::Error::custom(format!("expected non-literal {expected} expression"))
            }
            StreamBoundExprPlanError::Expression(err) => D::Error::custom(err),
        })
    }
}

/// Non-negative stream bound with validated runtime expressions.
///
/// Static integer expressions are normalized into the literal arm, and other
/// static literals are rejected so runtime expressions cannot hide invalid
/// bound payloads.
///
/// ```
/// use helix_ast::expr::{Expr, StreamBound};
/// use helix_planner::ir::{StreamBoundExpected, StreamBoundPlan, StreamBoundPlanError};
///
/// assert_eq!(
///     StreamBoundPlan::new(StreamBound::expr(Expr::val(4))).unwrap(),
///     StreamBoundPlan::Literal(4)
/// );
/// assert_eq!(
///     StreamBoundPlan::new(StreamBound::expr(Expr::val("many"))).unwrap_err(),
///     StreamBoundPlanError::StaticLiteral {
///         expected: StreamBoundExpected::NonNegativeInteger,
///     }
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamBoundPlan {
    /// Literal bound.
    Literal(usize),
    /// Runtime expression bound.
    Expr(StreamBoundExprPlan),
}

impl StreamBoundPlan {
    /// Build a stream bound plan after validating embedded expressions.
    pub fn new(bound: StreamBound) -> Result<Self, StreamBoundPlanError> {
        match bound {
            StreamBound::Literal(value) => Ok(Self::Literal(value)),
            StreamBound::Expr(Expr::Constant(PropertyValue::I64(value))) => usize::try_from(value)
                .map(Self::Literal)
                .map_err(|_| StreamBoundPlanError::StaticLiteral {
                    expected: StreamBoundExpected::NonNegativeInteger,
                }),
            StreamBound::Expr(expr) => {
                StreamBoundExprPlan::new(expr)
                    .map(Self::Expr)
                    .map_err(|err| match err {
                        StreamBoundExprPlanError::StaticLiteral { expected } => {
                            StreamBoundPlanError::StaticLiteral { expected }
                        }
                        StreamBoundExprPlanError::Expression(err) => {
                            StreamBoundPlanError::Expression(err)
                        }
                    })
            }
        }
    }
}

impl<'de> Deserialize<'de> for StreamBoundPlan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "snake_case")]
        enum Raw {
            Literal(usize),
            Expr(Expr),
        }

        match Raw::deserialize(deserializer)? {
            Raw::Literal(value) => Ok(Self::Literal(value)),
            Raw::Expr(expr) => Self::new(StreamBound::Expr(expr)).map_err(|err| match err {
                StreamBoundPlanError::StaticLiteral { expected } => {
                    D::Error::custom(format!("expected {expected} stream bound"))
                }
                StreamBoundPlanError::Expression(err) => D::Error::custom(err),
            }),
        }
    }
}

impl PartialEq<StreamBound> for StreamBoundPlan {
    fn eq(&self, other: &StreamBound) -> bool {
        match (self, other) {
            (Self::Literal(left), StreamBound::Literal(right)) => left == right,
            (Self::Expr(left), StreamBound::Expr(right)) => &left.expr == right,
            (Self::Literal(_), StreamBound::Expr(_)) | (Self::Expr(_), StreamBound::Literal(_)) => {
                false
            }
        }
    }
}
