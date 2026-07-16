//! Local executable step contract validation.

use super::graph;
use super::index::ValidatedStepIndex;
use crate::exec::{ExecCondition, ExecPlanError, ExecSchedule};

pub(super) fn validate_step_contracts(index: &ValidatedStepIndex<'_>) -> Result<(), ExecPlanError> {
    for step in index.steps() {
        if matches!(step.schedule, ExecSchedule::Parallel { .. }) && step.dependencies.len() < 2 {
            return Err(ExecPlanError::InvalidParallelDependencyCount {
                step: step.id,
                actual: step.dependencies.len(),
            });
        }
        if let ExecCondition::PreviousStepNotEmpty {
            dependency: condition_dependency,
        } = &step.condition
            && !graph::dependency_reachable(index, &step.dependencies, *condition_dependency)
        {
            return Err(ExecPlanError::PreviousConditionMissingDependency {
                step: step.id,
                dependency: *condition_dependency,
            });
        }
        for dependency in &step.dependencies {
            if *dependency == step.id {
                return Err(ExecPlanError::SelfDependency { step: step.id });
            }
            index.require_dependency(step.id, *dependency)?;
        }
    }
    Ok(())
}
