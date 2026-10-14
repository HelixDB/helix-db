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

/// Depth-first, with an explicit stack rather than recursion, so a long
/// dependency chain cannot overflow the thread's stack. Each frame is a step
/// in progress and how many of its dependencies it has visited.
pub(super) fn reject_cycles(index: &ValidatedStepIndex<'_>) -> Result<(), ExecPlanError> {
    let mut states = vec![Visit::New; index.len()];
    let mut stack = Vec::new();
    for start in 0..index.len() {
        if !matches!(states[start], Visit::New) {
            continue;
        }
        states[start] = Visit::InProgress;
        stack.push((start, 0));
        while let Some((position, visited)) = stack.last_mut() {
            let step = index.at(*position);
            let Some(dependency) = step.dependencies.get(*visited) else {
                states[*position] = Visit::Done;
                stack.pop();
                continue;
            };
            *visited += 1;
            index.require_dependency(step.id, *dependency)?;
            let dependency_position =
                index
                    .position(*dependency)
                    .ok_or(ExecPlanError::MissingDependency {
                        step: *dependency,
                        dependency: *dependency,
                    })?;
            match states[dependency_position] {
                Visit::Done => {}
                Visit::InProgress => {
                    return Err(ExecPlanError::DependencyCycle { step: *dependency });
                }
                Visit::New => {
                    states[dependency_position] = Visit::InProgress;
                    stack.push((dependency_position, 0));
                }
            }
        }
    }
    Ok(())
}

pub(super) fn reject_unreachable_steps(
    index: &ValidatedStepIndex<'_>,
) -> Result<(), ExecPlanError> {
    let mut reachable = vec![false; index.len()];
    collect_reachable(index, &mut reachable)?;
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

/// Marks every step the root reaches, depth-first in dependency order with an
/// explicit stack, as [`reject_cycles`] does.
fn collect_reachable(
    index: &ValidatedStepIndex<'_>,
    reachable: &mut [bool],
) -> Result<(), ExecPlanError> {
    let root = index.root();
    let root_position = index
        .position(root)
        .ok_or(ExecPlanError::MissingRoot { root })?;
    reachable[root_position] = true;
    let mut stack = vec![(root_position, 0)];
    while let Some((position, visited)) = stack.last_mut() {
        let step = index.at(*position);
        let Some(dependency) = step.dependencies.get(*visited) else {
            stack.pop();
            continue;
        };
        *visited += 1;
        index.require_dependency(step.id, *dependency)?;
        let dependency_position = index
            .position(*dependency)
            .ok_or(ExecPlanError::MissingRoot { root: *dependency })?;
        if !reachable[dependency_position] {
            reachable[dependency_position] = true;
            stack.push((dependency_position, 0));
        }
    }
    Ok(())
}

/// Runs once per conditional step, so its state grows with what it visits
/// rather than with the DAG. The answer does not depend on visiting order, so
/// a worklist replaces recursion.
pub(super) fn dependency_reachable(
    index: &ValidatedStepIndex<'_>,
    dependencies: &[ExecStepId],
    target: ExecStepId,
) -> bool {
    let mut seen = BTreeSet::new();
    let mut pending = dependencies.to_vec();
    while let Some(dependency) = pending.pop() {
        if dependency == target {
            return true;
        }
        if !seen.insert(dependency) {
            continue;
        }
        let Some(step) = index.get(dependency) else {
            continue;
        };
        pending.extend(&step.dependencies);
    }
    false
}
