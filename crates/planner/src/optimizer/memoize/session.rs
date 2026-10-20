//! Request-local memo expression identity index.

use std::collections::BTreeMap;

use super::contracts::{MemoizedExpr, QueuedMemoExpr};
use crate::{digest, logical, memo};

/// A memo expression this session inserted. The memo holds the expression
/// itself; the session keeps only where to find it.
#[derive(Debug, Clone)]
struct MemoizedIdentity {
    group: memo::MemoGroupId,
    memo_expr: memo::MemoExprId,
    queued_for_exploration: bool,
}

/// Request-local memo expression identity index.
#[derive(Debug, Default)]
pub(in crate::optimizer) struct MemoExpressionMemoizer {
    identities: BTreeMap<digest::PlanDigest, Vec<MemoizedIdentity>>,
    queued: Vec<QueuedMemoExpr>,
}

impl MemoExpressionMemoizer {
    pub(in crate::optimizer) fn insert_root(
        &mut self,
        memo: &mut memo::Memo,
        expr: logical::LogicalExpr,
    ) -> Result<MemoizedExpr, memo::MemoError> {
        self.insert_expr(memo, expr, true)
    }

    pub(in crate::optimizer) fn memo_expression_for_expr(
        &mut self,
        memo: &mut memo::Memo,
        expr: logical::LogicalExpr,
    ) -> Result<memo::MemoExpression, memo::MemoError> {
        memo::MemoExpression::try_with_derived_children(expr, |child| {
            self.insert_expr(memo, child, true)
                .map(|memoized| memoized.group)
        })
    }

    pub(in crate::optimizer) fn drain_queued(&mut self) -> Vec<QueuedMemoExpr> {
        std::mem::take(&mut self.queued)
    }

    fn insert_expr(
        &mut self,
        memo: &mut memo::Memo,
        expr: logical::LogicalExpr,
        queue_for_exploration: bool,
    ) -> Result<MemoizedExpr, memo::MemoError> {
        let expression = self.memo_expression_for_expr(memo, expr)?;
        let digest = memo::expression_digest(&expression);
        if let Some(existing) = self.find_existing_mut(memo, digest, &expression) {
            let queued = if queue_for_exploration && !existing.queued_for_exploration {
                existing.queued_for_exploration = true;
                Some(QueuedMemoExpr {
                    group: existing.group,
                    expr: existing.memo_expr,
                    logical: expression.into_parts().0,
                })
            } else {
                None
            };
            let group = existing.group;
            if let Some(queued) = queued {
                self.queued.push(queued);
            }
            return Ok(MemoizedExpr { group });
        }

        let logical = queue_for_exploration.then(|| expression.expr().clone());
        let inserted = memo.insert_group_with_expr_id(expression)?;
        if let Some(logical) = logical {
            self.queued.push(QueuedMemoExpr {
                group: inserted.group,
                expr: inserted.expr,
                logical,
            });
        }
        self.identities
            .entry(digest)
            .or_default()
            .push(MemoizedIdentity {
                group: inserted.group,
                memo_expr: inserted.expr,
                queued_for_exploration: queue_for_exploration,
            });
        Ok(MemoizedExpr {
            group: inserted.group,
        })
    }

    /// The identity of an inserted expression equal to `expression`,
    /// compared against the copy the memo holds.
    fn find_existing_mut(
        &mut self,
        memo: &memo::Memo,
        digest: digest::PlanDigest,
        expression: &memo::MemoExpression,
    ) -> Option<&mut MemoizedIdentity> {
        self.identities
            .get_mut(&digest)?
            .iter_mut()
            .find(|identity| {
                memo.expression(identity.memo_expr).is_some_and(|known| {
                    &known.expr == expression.expr() && &known.children == expression.children()
                })
            })
    }
}
