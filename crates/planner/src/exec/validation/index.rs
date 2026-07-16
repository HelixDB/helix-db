//! Validated executable step index.

use std::collections::BTreeMap;

use crate::ir;

use super::{contracts, graph};
use crate::exec::{ExecPlanError, ExecStep, ExecStepId};

pub(super) struct ValidatedStepIndex<'a> {
    root: ExecStepId,
    by_id: BTreeMap<ExecStepId, &'a ExecStep>,
}

impl<'a> ValidatedStepIndex<'a> {
    pub(super) fn new(
        steps: &'a ir::AtLeast<ExecStep, 1>,
        root: ExecStepId,
    ) -> Result<Self, ExecPlanError> {
        let mut by_id = BTreeMap::new();
        for step in steps {
            if by_id.insert(step.id, step).is_some() {
                return Err(ExecPlanError::DuplicateStepId { id: step.id });
            }
        }
        let index = Self { root, by_id };
        if !index.by_id.contains_key(&root) {
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
        self.by_id.len()
    }

    pub(super) fn ids(&self) -> impl Iterator<Item = ExecStepId> + '_ {
        self.by_id.keys().copied()
    }

    pub(super) fn steps(&self) -> impl Iterator<Item = &'a ExecStep> + '_ {
        self.by_id.values().copied()
    }

    pub(super) fn get(&self, id: ExecStepId) -> Option<&'a ExecStep> {
        self.by_id.get(&id).copied()
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
