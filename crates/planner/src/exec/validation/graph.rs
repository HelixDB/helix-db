//! Executable DAG dependency graph validation.

use std::collections::BTreeSet;

use super::index::ValidatedStepIndex;
use crate::exec::{ExecPlanError, ExecStepId};

pub(super) fn reject_cycles(index: &ValidatedStepIndex<'_>) -> Result<(), ExecPlanError> {
    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    for id in index.ids() {
        visit(index, id, &mut visiting, &mut visited)?;
    }
    Ok(())
}

fn visit(
    index: &ValidatedStepIndex<'_>,
    id: ExecStepId,
    visiting: &mut BTreeSet<ExecStepId>,
    visited: &mut BTreeSet<ExecStepId>,
) -> Result<(), ExecPlanError> {
    if visited.contains(&id) {
        return Ok(());
    }
    if !visiting.insert(id) {
        return Err(ExecPlanError::DependencyCycle { step: id });
    }
    let step = index.get(id).ok_or(ExecPlanError::MissingDependency {
        step: id,
        dependency: id,
    })?;
    for dependency in &step.dependencies {
        index.require_dependency(step.id, *dependency)?;
        visit(index, *dependency, visiting, visited)?;
    }
    visiting.remove(&id);
    visited.insert(id);
    Ok(())
}

pub(super) fn reject_unreachable_steps(
    index: &ValidatedStepIndex<'_>,
) -> Result<(), ExecPlanError> {
    let mut reachable = BTreeSet::new();
    collect_reachable(index, index.root(), &mut reachable)?;
    for id in index.ids() {
        if !reachable.contains(&id) {
            return Err(ExecPlanError::UnreachableStep {
                step: id,
                root: index.root(),
            });
        }
    }
    Ok(())
}

fn collect_reachable(
    index: &ValidatedStepIndex<'_>,
    id: ExecStepId,
    reachable: &mut BTreeSet<ExecStepId>,
) -> Result<(), ExecPlanError> {
    if !reachable.insert(id) {
        return Ok(());
    }
    let step = index
        .get(id)
        .ok_or(ExecPlanError::MissingRoot { root: id })?;
    for dependency in &step.dependencies {
        index.require_dependency(step.id, *dependency)?;
        collect_reachable(index, *dependency, reachable)?;
    }
    Ok(())
}

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
