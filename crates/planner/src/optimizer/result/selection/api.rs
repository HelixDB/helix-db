//! `OptimizationResult` selection extension API.

use super::{RootSelectionFailure, RootSelectionSummary, SelectionError, SelectionSession};
use crate::{cost, ir, memo, physical, properties};

use super::super::{OptimizationResult, PhysicalAlternativeEntry, SelectedPhysicalAlternative};

impl OptimizationResult {
    /// Start a reusable recursive physical-selection session.
    pub fn selection_session(&self) -> SelectionSession<'_> {
        SelectionSession::new(self)
    }

    /// Selection summary for all independently optimized roots.
    pub fn root_selection_summary(&self) -> RootSelectionSummary {
        let mut session = self.selection_session();
        let mut selected_cost = cost::CostVector::ZERO;
        let mut failures = Vec::new();
        for root in self.roots.iter() {
            match session.best_group_cost(*root) {
                Ok(root_cost) => selected_cost = selected_cost.serial(root_cost),
                Err(error) => failures.push(RootSelectionFailure { root: *root, error }),
            }
        }

        match ir::AtLeast::<_, 1>::try_from_vec(failures) {
            Some(failures) => RootSelectionSummary::Incomplete {
                successful_cost: selected_cost,
                failures,
            },
            None => RootSelectionSummary::Complete { selected_cost },
        }
    }

    /// Cheapest retained alternative for a group under recursive selected-cost ordering.
    pub fn best_alternative(
        &self,
        group: memo::MemoGroupId,
    ) -> Result<&physical::PhysicalAlternative, SelectionError> {
        self.selection_session().best_alternative(group)
    }

    /// Cheapest retained alternative entry for a group under recursive selected-cost ordering.
    pub fn best_alternative_entry(
        &self,
        group: memo::MemoGroupId,
    ) -> Result<&PhysicalAlternativeEntry, SelectionError> {
        self.selection_session().best_alternative_entry(group)
    }

    /// Cheapest retained physical plan for a group with source logical provenance
    /// and recursive selected cost.
    pub fn best_plan(
        &self,
        group: memo::MemoGroupId,
    ) -> Result<SelectedPhysicalAlternative<'_>, SelectionError> {
        self.selection_session().best_plan(group)
    }

    /// Cheapest retained alternative satisfying required physical properties under
    /// recursive selected-cost ordering.
    pub fn best_alternative_satisfying(
        &self,
        group: memo::MemoGroupId,
        required: &properties::RequiredProperties,
    ) -> Result<&physical::PhysicalAlternative, SelectionError> {
        self.selection_session()
            .best_alternative_satisfying(group, required)
    }

    /// Cheapest retained alternative entry satisfying required physical properties
    /// under recursive selected-cost ordering.
    pub fn best_alternative_entry_satisfying(
        &self,
        group: memo::MemoGroupId,
        required: &properties::RequiredProperties,
    ) -> Result<&PhysicalAlternativeEntry, SelectionError> {
        self.selection_session()
            .best_alternative_entry_satisfying(group, required)
    }

    /// Cheapest retained physical plan satisfying required properties, resolved
    /// to the source logical memo expression and recursive selected cost.
    pub fn best_plan_satisfying(
        &self,
        group: memo::MemoGroupId,
        required: &properties::RequiredProperties,
    ) -> Result<SelectedPhysicalAlternative<'_>, SelectionError> {
        self.selection_session()
            .best_plan_satisfying(group, required)
    }
}
