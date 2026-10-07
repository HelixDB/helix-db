//! Executable DAG dependency graph validation.
//!
//! Traversal state is one entry per step position, visited in the same ID and
//! dependency order as before, so errors name the same steps.

use std::collections::BTreeSet;

use super::index::ValidatedStepIndex;
use crate::exec::{ExecPlanError, ExecStepId};

#[derive(Clone, Copy)]
enum Visit {
    New,
    InProgress,
    Done,
}

pub(super) fn reject_cycles(index: &ValidatedStepIndex<'_>) -> Result<(), ExecPlanError> {
    let mut states = vec![Visit::New; index.len()];
    for id in index.ids() {
        visit(index, id, &mut states)?;
    }
    Ok(())
}

fn visit(
    index: &ValidatedStepIndex<'_>,
    id: ExecStepId,
    states: &mut [Visit],
) -> Result<(), ExecPlanError> {
    let position = index.position(id).ok_or(ExecPlanError::MissingDependency {
        step: id,
        dependency: id,
    })?;
    match states[position] {
        Visit::Done => return Ok(()),
        Visit::InProgress => return Err(ExecPlanError::DependencyCycle { step: id }),
        Visit::New => states[position] = Visit::InProgress,
    }
    let step = index.at(position);
    for dependency in &step.dependencies {
        index.require_dependency(step.id, *dependency)?;
        visit(index, *dependency, states)?;
    }
    states[position] = Visit::Done;
    Ok(())
}

pub(super) fn reject_unreachable_steps(
    index: &ValidatedStepIndex<'_>,
) -> Result<(), ExecPlanError> {
    let mut reachable = vec![false; index.len()];
    collect_reachable(index, index.root(), &mut reachable)?;
    match index
        .ids()
        .zip(&reachable)
        .find_map(|(id, reachable)| (!reachable).then_some(id))
    {
        Some(step) => Err(ExecPlanError::UnreachableStep {
            step,
            root: index.root(),
        }),
        None => Ok(()),
    }
}

fn collect_reachable(
    index: &ValidatedStepIndex<'_>,
    id: ExecStepId,
    reachable: &mut [bool],
) -> Result<(), ExecPlanError> {
    let position = index
        .position(id)
        .ok_or(ExecPlanError::MissingRoot { root: id })?;
    if reachable[position] {
        return Ok(());
    }
    reachable[position] = true;
    let step = index.at(position);
    for dependency in &step.dependencies {
        index.require_dependency(step.id, *dependency)?;
        collect_reachable(index, *dependency, reachable)?;
    }
    Ok(())
}

/// Runs once per conditional step, so its state grows with what it visits
/// rather than with the DAG.
pub(super) fn dependency_reachable(
    index: &ValidatedStepIndex<'_>,
    dependencies: &[ExecStepId],
    target: ExecStepId,
) -> bool {
    let mut seen = BTreeSet::new();
    dependency_reachable_inner(index, dependencies, target, &mut seen)
}

fn dependency_reachable_inner(
    index: &ValidatedStepIndex<'_>,
    dependencies: &[ExecStepId],
    target: ExecStepId,
    seen: &mut BTreeSet<ExecStepId>,
) -> bool {
    dependencies.iter().any(|dependency| {
        *dependency == target
            || (seen.insert(*dependency)
                && index.get(*dependency).is_some_and(|step| {
                    dependency_reachable_inner(index, &step.dependencies, target, seen)
                }))
    })
}
