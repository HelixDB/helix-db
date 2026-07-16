//! Stable memo expression identity digests.

use serde::Serialize;

use super::expression::MemoExpression;
use crate::digest;

#[derive(Serialize)]
struct MemoExpressionDigest<'a> {
    expr: &'a crate::logical::LogicalExpr,
    children: &'a super::children::MemoChildGroups,
}

/// Compute the stable identity digest for a memo expression.
pub fn expression_digest(expression: &MemoExpression) -> digest::PlanDigest {
    digest::PlanDigest::for_tagged_value(
        "memo_expr:v2",
        &MemoExpressionDigest {
            expr: expression.expr(),
            children: expression.children(),
        },
    )
}
