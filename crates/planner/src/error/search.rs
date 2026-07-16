//! Search-specific diagnostic ADTs.

/// Expected query-time search tenant value shape.
///
/// # Examples
///
/// ```
/// use helix_planner::error::SearchTenantValueExpected;
///
/// assert_eq!(
///     SearchTenantValueExpected::NonNullPropertyInput.to_string(),
///     "non-null property input"
/// );
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SearchTenantValueExpected {
    /// Any property input except literal `Null`.
    NonNullPropertyInput,
}

impl std::fmt::Display for SearchTenantValueExpected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NonNullPropertyInput => f.write_str("non-null property input"),
        }
    }
}
