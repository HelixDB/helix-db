use helix_ast::expr::Expr;
use helix_ast::value::{PropertyInput, PropertyValue};
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};

use crate::catalog;

use super::{
    SearchQueryExprPlan, SearchQueryExprPlanError, SearchQueryInputExpected,
    SearchQueryInputPlanError,
};
use crate::ir::NonEmptyString;

/// Text-search query input with literal payloads restricted to non-empty strings.
///
/// Runtime expression inputs remain valid here because the planner cannot know
/// their value types before execution. Static constant expressions are
/// normalized through the literal arm so they cannot bypass this contract.
///
/// ```
/// use helix_ast::expr::Expr;
/// use helix_ast::value::{PropertyInput, PropertyValue};
/// use helix_planner::catalog::SearchIndexKind;
/// use helix_planner::ir::{
///     NonEmptyString, SearchQueryInputExpected, SearchQueryInputPlanError, TextQueryInputPlan,
/// };
///
/// assert_eq!(
///     TextQueryInputPlan::new(PropertyInput::from("needle")),
///     Ok(TextQueryInputPlan::Text(NonEmptyString::new("needle").unwrap()))
/// );
/// assert_eq!(
///     TextQueryInputPlan::new(PropertyInput::from(Expr::val("needle"))),
///     Ok(TextQueryInputPlan::Text(NonEmptyString::new("needle").unwrap()))
/// );
/// assert!(matches!(
///     TextQueryInputPlan::new(PropertyInput::from(PropertyValue::F32Array(vec![0.1_f32]))),
///     Err(SearchQueryInputPlanError::InvalidLiteral {
///         kind: SearchIndexKind::Text,
///         expected: SearchQueryInputExpected::NonEmptyString,
///     })
/// ));
/// ```
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TextQueryInputPlan {
    /// Literal query text.
    Text(NonEmptyString),
    /// Runtime query-text expression.
    Expr(SearchQueryExprPlan),
}

impl TextQueryInputPlan {
    /// Build a text-query input plan after validating embedded expressions.
    pub fn new(input: PropertyInput) -> Result<Self, SearchQueryInputPlanError> {
        match input {
            PropertyInput::Value(PropertyValue::String(value))
            | PropertyInput::Expr(Expr::Constant(PropertyValue::String(value))) => {
                NonEmptyString::new(value).map(Self::Text).ok_or(
                    SearchQueryInputPlanError::InvalidLiteral {
                        kind: catalog::SearchIndexKind::Text,
                        expected: SearchQueryInputExpected::NonEmptyString,
                    },
                )
            }
            PropertyInput::Value(_) => Err(SearchQueryInputPlanError::InvalidLiteral {
                kind: catalog::SearchIndexKind::Text,
                expected: SearchQueryInputExpected::NonEmptyString,
            }),
            PropertyInput::Expr(expr) => {
                SearchQueryExprPlan::new(expr)
                    .map(Self::Expr)
                    .map_err(|err| match err {
                        SearchQueryExprPlanError::StaticLiteral => {
                            SearchQueryInputPlanError::InvalidLiteral {
                                kind: catalog::SearchIndexKind::Text,
                                expected: SearchQueryInputExpected::NonEmptyString,
                            }
                        }
                        SearchQueryExprPlanError::Expression(err) => {
                            SearchQueryInputPlanError::Expression(err)
                        }
                    })
            }
        }
    }
}

impl<'de> Deserialize<'de> for TextQueryInputPlan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "snake_case")]
        enum Raw {
            Text(String),
            Expr(Expr),
        }

        match Raw::deserialize(deserializer)? {
            Raw::Text(value) => NonEmptyString::new(value).map(Self::Text).ok_or_else(|| {
                D::Error::custom("text search query input must be non-empty string")
            }),
            Raw::Expr(expr) => Self::new(PropertyInput::Expr(expr)).map_err(|err| match err {
                SearchQueryInputPlanError::InvalidLiteral { kind, expected } => {
                    D::Error::custom(format!("{kind} search query input must be {expected}"))
                }
                SearchQueryInputPlanError::Expression(err) => D::Error::custom(err),
            }),
        }
    }
}
