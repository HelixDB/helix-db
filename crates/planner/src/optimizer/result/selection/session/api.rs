//! Public selection-session API.

use super::super::super::{PhysicalAlternativeEntry, SelectedPhysicalAlternative};
use super::super::SelectionError;
use super::SelectionSession;
use crate::{cost, memo, physical, properties};

impl<'a> SelectionSession<'a> {
    /// Cheapest retained alternative for a group under recursive selected-cost ordering.
    pub fn best_alternative(
        &mut self,
        group: memo::MemoGroupId,
    ) -> Result<&'a physical::PhysicalAlternative, SelectionError> {
        self.best_alternative_entry(group)
            .map(|entry| &entry.alternative)
    }

    /// Cheapest retained alternative entry for a group under recursive selected-cost ordering.
    pub fn best_alternative_entry(
        &mut self,
        group: memo::MemoGroupId,
    ) -> Result<&'a PhysicalAlternativeEntry, SelectionError> {
        self.best_alternative_entry_satisfying(group, &properties::RequiredProperties::default())
    }

    /// Cheapest retained physical plan for a group with source logical provenance
    /// and recursive selected cost.
    pub fn best_plan(
        &mut self,
        group: memo::MemoGroupId,
    ) -> Result<SelectedPhysicalAlternative<'a>, SelectionError> {
        let selected = self.best_default_selection(group)?;
        let source_expr = self.source_expr_for_entry(group, selected.entry)?;
        Ok(SelectedPhysicalAlternative {
            group,
            entry: selected.entry,
            source_expr,
            selected_cost: selected.selected_cost,
        })
    }

    /// Cheapest retained alternative satisfying required physical properties under
    /// recursive selected-cost ordering.
    pub fn best_alternative_satisfying(
        &mut self,
        group: memo::MemoGroupId,
        required: &properties::RequiredProperties,
    ) -> Result<&'a physical::PhysicalAlternative, SelectionError> {
        self.best_alternative_entry_satisfying(group, required)
            .map(|entry| &entry.alternative)
    }

    /// Cheapest retained alternative entry satisfying required physical properties
    /// under recursive selected-cost ordering.
    pub fn best_alternative_entry_satisfying(
        &mut self,
        group: memo::MemoGroupId,
        required: &properties::RequiredProperties,
    ) -> Result<&'a PhysicalAlternativeEntry, SelectionError> {
        self.best_alternative_entry_and_cost_satisfying(group, required)
            .map(|(entry, _)| entry)
    }

    /// Cheapest retained physical plan satisfying required properties, resolved
    /// to the source logical memo expression and recursive selected cost.
    pub fn best_plan_satisfying(
        &mut self,
        group: memo::MemoGroupId,
        required: &properties::RequiredProperties,
    ) -> Result<SelectedPhysicalAlternative<'a>, SelectionError> {
        let (entry, selected_cost) =
            self.best_alternative_entry_and_cost_satisfying(group, required)?;
        let source_expr = self.source_expr_for_entry(group, entry)?;
        Ok(SelectedPhysicalAlternative {
            group,
            entry,
            source_expr,
            selected_cost,
        })
    }

    pub(in crate::optimizer::result::selection) fn best_group_cost(
        &mut self,
        group: memo::MemoGroupId,
    ) -> Result<cost::CostVector, SelectionError> {
        self.best_default_selection(group)
            .map(|selected| selected.selected_cost)
    }
}

#[cfg(test)]
impl<'a> SelectionSession<'a> {
    pub fn cached_default_selection_count(&self) -> usize {
        self.default_selection_cache.len()
    }
}
