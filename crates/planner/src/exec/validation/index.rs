//! Validated executable step index.

use crate::ir;

use super::{contracts, graph};
use crate::exec::positions::StepPositions;
use crate::exec::{ExecPlanError, ExecStep, ExecStepId};

pub(super) struct ValidatedStepIndex<'a> {
    root: ExecStepId,
    positions: StepPositions,
    /// Steps by position, so in ascending ID order.
    steps: Vec<&'a ExecStep>,
}

impl<'a> ValidatedStepIndex<'a> {
    pub(super) fn new(
        steps: &'a ir::AtLeast<ExecStep, 1>,
        root: ExecStepId,
    ) -> Result<Self, ExecPlanError> {
        let positions = StepPositions::new(steps.iter().map(|step| step.id))
            .map_err(|id| ExecPlanError::DuplicateStepId { id })?;
        let mut by_position = steps.iter().collect::<Vec<_>>();
        by_position.sort_unstable_by_key(|step| step.id);
        let index = Self {
            root,
            positions,
            steps: by_position,
        };
        if index.position(root).is_none() {
            return Err(ExecPlanError::MissingRoot { root });
        }
        contracts::validate_step_contracts(&index)?;
        graph::reject_cycles(&index)?;
        graph::reject_unreachable_steps(&index)?;
        Ok(index)
    }

    pub(super) const fn root(&self) -> ExecStepId {
        self.root
    }

    pub(super) fn len(&self) -> usize {
        self.steps.len()
    }

    pub(super) fn ids(&self) -> impl Iterator<Item = ExecStepId> + '_ {
        self.positions.ids().iter().copied()
    }

    pub(super) fn steps(&self) -> impl Iterator<Item = &'a ExecStep> + '_ {
        self.steps.iter().copied()
    }

    /// The step's position: an index into per-step state, in ID order.
    pub(super) fn position(&self, id: ExecStepId) -> Option<usize> {
        self.positions.position(id)
    }

    /// The step at `position`.
    pub(super) fn at(&self, position: usize) -> &'a ExecStep {
        self.steps[position]
    }

    pub(super) fn get(&self, id: ExecStepId) -> Option<&'a ExecStep> {
        self.position(id).map(|position| self.steps[position])
    }

    pub(super) fn require_dependency(
        &self,
        step: ExecStepId,
        dependency: ExecStepId,
    ) -> Result<&'a ExecStep, ExecPlanError> {
        self.get(dependency)
            .ok_or(ExecPlanError::MissingDependency { step, dependency })
    }
}
