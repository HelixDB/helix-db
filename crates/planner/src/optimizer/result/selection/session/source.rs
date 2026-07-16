//! Memo-source validation for selected alternatives.

use super::super::super::PhysicalAlternativeEntry;
use super::super::SelectionError;
use super::SelectionSession;
use crate::memo;

impl<'a> SelectionSession<'a> {
    pub(super) fn ensure_memo_group(&self, group: memo::MemoGroupId) -> Result<(), SelectionError> {
        self.result
            .memo
            .contains_group(group)
            .then_some(())
            .ok_or(SelectionError::MissingMemoGroup { group })
    }

    pub(super) fn source_expr_for_entry(
        &self,
        group: memo::MemoGroupId,
        entry: &PhysicalAlternativeEntry,
    ) -> Result<&'a memo::MemoExpr, SelectionError> {
        self.result.memo.expression(entry.source_expr).ok_or(
            SelectionError::MissingSourceExpression {
                group,
                alternative: entry.id,
                source_expr: entry.source_expr,
            },
        )
    }
}
