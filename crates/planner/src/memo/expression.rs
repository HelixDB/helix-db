//! Validated memo-expression contracts.

use super::children::MemoChildGroups;
use crate::logical;

/// Logical expression paired with the ordered memo child groups it references.
///
/// The constructor checks the arity against `LogicalExpr::memo_children`, so a
/// `MemoExpression` cannot carry child groups that selected reconstruction will
/// later be unable to consume.
///
/// ```
/// use helix_planner::logical::{LogicalExpr, PureLogicalOp};
/// use helix_planner::memo::{MemoChildGroups, MemoExpression};
/// use helix_planner::properties::ElementKind;
///
/// let expr = LogicalExpr::Pure(PureLogicalOp::Source {
///     element: ElementKind::Node,
/// });
/// let memo_expr = MemoExpression::new(expr, MemoChildGroups::empty()).unwrap();
///
/// assert!(memo_expr.children().is_empty());
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct MemoExpression {
    expr: logical::LogicalExpr,
    children: MemoChildGroups,
}

impl MemoExpression {
    /// Pair an expression with validated child groups.
    pub fn new(
        expr: logical::LogicalExpr,
        children: MemoChildGroups,
    ) -> Result<Self, MemoExpressionArityError> {
        let expected = expr.memo_children().len();
        let actual = children.len();
        if expected == actual {
            Ok(Self { expr, children })
        } else {
            Err(MemoExpressionArityError { expected, actual })
        }
    }

    /// Build an expression by deriving one memo child group for each logical child.
    ///
    /// This constructor is infallible because it owns child extraction. Callers
    /// provide only the mapping from each extracted child expression to its memo
    /// group, so a mismatched child-group arity is unrepresentable.
    ///
    /// ```
    /// use helix_planner::ir::{AtLeast, NonEmptyString, StreamBoundPlan};
    /// use helix_planner::logical::{
    ///     LogicalExpr, RootPipeline, RootStream, StreamPipelineOp, VariableSource,
    /// };
    /// use helix_planner::memo::{MemoExpression, MemoGroupId};
    ///
    /// let inner = RootPipeline::new(
    ///     RootStream::VariableSource(VariableSource::new(
    ///         NonEmptyString::new("seed").unwrap(),
    ///     )),
    ///     AtLeast::<_, 1>::from_one(StreamPipelineOp::Distinct),
    /// )
    /// .unwrap();
    /// let outer = LogicalExpr::RootPipeline(
    ///     RootPipeline::new(
    ///         RootStream::Pipeline(Box::new(inner)),
    ///         AtLeast::<_, 1>::from_one(StreamPipelineOp::Limit {
    ///             count: StreamBoundPlan::Literal(1),
    ///         }),
    ///     )
    ///     .unwrap(),
    /// );
    /// let child_group = MemoGroupId::new(7).unwrap();
    /// let memo_expr = MemoExpression::with_derived_children(outer, |_| child_group);
    ///
    /// assert_eq!(memo_expr.children().as_slice(), &[child_group]);
    /// ```
    pub fn with_derived_children<F>(expr: logical::LogicalExpr, mut child_group: F) -> Self
    where
        F: FnMut(logical::LogicalExpr) -> super::ids::MemoGroupId,
    {
        let children = expr
            .memo_children()
            .into_iter()
            .map(&mut child_group)
            .collect();
        Self {
            expr,
            children: MemoChildGroups::new(children),
        }
    }

    /// Fallibly build an expression by deriving one memo child group for each
    /// logical child.
    ///
    /// Use this when mapping a child expression can fail, such as during memo
    /// insertion where ID allocation is bounded by `usize`.
    pub fn try_with_derived_children<F, E>(
        expr: logical::LogicalExpr,
        mut child_group: F,
    ) -> Result<Self, E>
    where
        F: FnMut(logical::LogicalExpr) -> Result<super::ids::MemoGroupId, E>,
    {
        let children = expr
            .memo_children()
            .into_iter()
            .map(&mut child_group)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            expr,
            children: MemoChildGroups::new(children),
        })
    }

    /// Borrow the logical expression.
    pub const fn expr(&self) -> &logical::LogicalExpr {
        &self.expr
    }

    /// Borrow ordered child groups.
    pub const fn children(&self) -> &MemoChildGroups {
        &self.children
    }

    /// Consume into the validated expression and child groups.
    pub fn into_parts(self) -> (logical::LogicalExpr, MemoChildGroups) {
        (self.expr, self.children)
    }
}

/// Error returned when memo child groups do not match expression arity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoExpressionArityError {
    expected: usize,
    actual: usize,
}

impl MemoExpressionArityError {
    /// Expected number of memo child groups.
    pub const fn expected(&self) -> usize {
        self.expected
    }

    /// Actual number of memo child groups supplied.
    pub const fn actual(&self) -> usize {
        self.actual
    }
}

impl std::fmt::Display for MemoExpressionArityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "memo child-group arity mismatch: expected {}, got {}",
            self.expected, self.actual
        )
    }
}
