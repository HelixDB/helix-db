//! Candidate filtering and deterministic best-alternative selection.

use super::super::super::PhysicalAlternativeEntry;
use super::super::SelectionError;
use super::SelectionSession;
use crate::optimizer::ordering;
use crate::{cost, memo, properties};

impl<'a> SelectionSession<'a> {
    pub(super) fn best_alternative_entry_and_cost_satisfying(
        &mut self,
        group: memo::MemoGroupId,
        required: &properties::RequiredProperties,
    ) -> Result<(&'a PhysicalAlternativeEntry, cost::CostVector), SelectionError> {
        if required == &properties::RequiredProperties::default() {
            return self
                .best_default_selection(group)
                .map(|selected| (selected.entry, selected.selected_cost));
        }
        self.ensure_memo_group(group)?;
        if !self.visiting.insert(group) {
            return Err(SelectionError::RecursiveSelectionCycle { group });
        }
        let best = self.best_alternative_entry_and_cost_in_visiting_group(group, required);
        self.visiting.remove(&group);
        best
    }

    pub(super) fn best_alternative_entry_and_cost_in_visiting_group(
        &mut self,
        group: memo::MemoGroupId,
        required: &properties::RequiredProperties,
    ) -> Result<(&'a PhysicalAlternativeEntry, cost::CostVector), SelectionError> {
        let alternatives = self
            .result
            .physical_for_group(group)
            .ok_or(SelectionError::NoPhysicalAlternatives { group })?;
        if alternatives.alternatives.is_empty() {
            return Err(SelectionError::NoPhysicalAlternatives { group });
        }

        let mut saw_satisfying_properties = false;
        let mut first_failure = None;
        let mut best = None;
        for entry in alternatives
            .alternatives
            .iter()
            .filter(|entry| entry.alternative.delivered.satisfies(required))
        {
            saw_satisfying_properties = true;
            match self.entry_total_cost(group, entry) {
                Ok(cost) => {
                    let candidate_key =
                        ordering::alternative_key_for_cost(&entry.alternative, cost);
                    let replace = best
                        .as_ref()
                        .is_none_or(|(_, _, best_key)| candidate_key < *best_key);
                    if replace {
                        best = Some((entry, cost, candidate_key));
                    }
                }
                Err(error) => {
                    first_failure.get_or_insert(error);
                }
            }
        }

        if let Some((entry, cost, _)) = best {
            return Ok((entry, cost));
        }
        if !saw_satisfying_properties {
            return Err(SelectionError::UnsatisfiedRequiredProperties {
                group,
                required: required.clone(),
            });
        }
        Err(first_failure.unwrap_or(SelectionError::NoPhysicalAlternatives { group }))
    }
}
