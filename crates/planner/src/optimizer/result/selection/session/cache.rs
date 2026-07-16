//! Default selected-alternative caching.

use super::super::SelectionError;
use super::{CachedDefaultSelection, SelectionSession};
use crate::{memo, properties};

impl<'a> SelectionSession<'a> {
    pub(super) fn best_default_selection(
        &mut self,
        group: memo::MemoGroupId,
    ) -> Result<CachedDefaultSelection<'a>, SelectionError> {
        if let Some(selected) = self.default_selection_cache.get(&group) {
            return Ok(*selected);
        }
        self.ensure_memo_group(group)?;
        if !self.visiting.insert(group) {
            return Err(SelectionError::RecursiveSelectionCycle { group });
        }
        let selected = self.best_default_selection_in_visiting_group(group);
        self.visiting.remove(&group);
        let selected = selected?;
        self.default_selection_cache.insert(group, selected);
        Ok(selected)
    }

    fn best_default_selection_in_visiting_group(
        &mut self,
        group: memo::MemoGroupId,
    ) -> Result<CachedDefaultSelection<'a>, SelectionError> {
        self.best_alternative_entry_and_cost_in_visiting_group(
            group,
            &properties::RequiredProperties::default(),
        )
        .map(|(entry, selected_cost)| CachedDefaultSelection {
            entry,
            selected_cost,
        })
    }
}
