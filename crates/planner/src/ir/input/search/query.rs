use crate::catalog;

use super::super::super::ExprPlanError;

/// Expected literal shape for search-query input.
///
/// # Examples
///
/// ```
/// use helix_planner::ir::SearchQueryInputExpected;
///
/// assert_eq!(
///     SearchQueryInputExpected::NonEmptyFiniteF32Array.to_string(),
///     "non-empty finite f32 array"
/// );
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SearchQueryInputExpected {
    /// Non-empty finite f32 array.
    NonEmptyFiniteF32Array,
    /// Non-empty string.
    NonEmptyString,
}

impl std::fmt::Display for SearchQueryInputExpected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NonEmptyFiniteF32Array => f.write_str("non-empty finite f32 array"),
            Self::NonEmptyString => f.write_str("non-empty string"),
        }
    }
}

/// Invalid search query input payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchQueryInputPlanError {
    /// Literal payload did not match the required search-query type.
    InvalidLiteral {
        /// Search kind.
        kind: catalog::SearchIndexKind,
        /// Expected literal payload kind.
        expected: SearchQueryInputExpected,
    },
    /// Runtime expression failed expression validation.
    Expression(ExprPlanError),
}
