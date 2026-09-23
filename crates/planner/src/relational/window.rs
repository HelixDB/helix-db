//! Evaluated row windows shared by materialized and batched consumers.
use super::{nonnegative, Expression, Result, Value};

/// Validated nonnegative offsets. Evaluation always checks SKIP before LIMIT,
/// including LIMIT 0; a bound must not hide an invalid offset or expression error.
/// Missing LIMIT means every remaining row.
///
/// ```
/// use helix_planner::relational::{Expression, Value, Window};
/// let skip = Expression::Literal(Value::Integer(2));
/// let limit = Expression::Literal(Value::Integer(3));
/// let window = Window::evaluate(Some(&skip), Some(&limit), |expression| {
///     let Expression::Literal(value) = expression else { unreachable!() };
///     Ok(value.clone())
/// })?;
/// assert_eq!((window.skip(), window.limit(), window.retained_rows()), (2, 3, 5));
/// # Ok::<(), helix_planner::relational::QueryError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    skip: usize,
    limit: usize,
}

impl Window {
    pub fn evaluate(
        skip: Option<&Expression>,
        limit: Option<&Expression>,
        mut evaluate: impl FnMut(&Expression) -> Result<Value>,
    ) -> Result<Self> {
        let skip = skip
            .map(|expression| nonnegative(&evaluate(expression)?))
            .transpose()?
            .unwrap_or(0);
        let limit = limit
            .map(|expression| nonnegative(&evaluate(expression)?))
            .transpose()?
            .unwrap_or(usize::MAX);
        Ok(Self { skip, limit })
    }

    pub fn skip(self) -> usize {
        self.skip
    }

    pub fn limit(self) -> usize {
        self.limit
    }

    /// Maximum candidates needed before applying this window. A zero limit
    /// needs no retained candidates, even with SKIP. Saturation must not create
    /// an arithmetic error for otherwise valid independent clauses.
    pub fn retained_rows(self) -> usize {
        if self.limit == 0 {
            0
        } else {
            self.skip.saturating_add(self.limit)
        }
    }
}
