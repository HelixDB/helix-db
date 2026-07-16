//! Recursive selected-cost composition.

use super::super::super::PhysicalAlternativeEntry;
use super::super::SelectionError;
use super::SelectionSession;
use crate::{cost, memo};

impl<'a> SelectionSession<'a> {
    pub(super) fn entry_total_cost(
        &mut self,
        group: memo::MemoGroupId,
        entry: &'a PhysicalAlternativeEntry,
    ) -> Result<cost::CostVector, SelectionError> {
        let source_expr = self.source_expr_for_entry(group, entry)?;
        source_expr
            .children
            .iter()
            .try_fold(entry.alternative.cost, |total, child_group| {
                self.best_group_cost(*child_group)
                    .map(|child_cost| total.serial(child_cost))
                    .map_err(|reason| SelectionError::ChildSelectionFailed {
                        parent_group: group,
                        child_group: *child_group,
                        reason: Box::new(reason),
                    })
            })
    }
}
