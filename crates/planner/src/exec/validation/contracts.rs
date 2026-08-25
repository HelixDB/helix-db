//! Local executable step contract validation.

use super::graph;
use super::index::ValidatedStepIndex;
use crate::exec::{ExecCondition, ExecCountDependency, ExecOp, ExecPlanError, ExecSchedule};

pub(super) fn validate_step_contracts(index: &ValidatedStepIndex<'_>) -> Result<(), ExecPlanError> {
    for step in index.steps() {
        if let ExecOp::Count { plan } = &step.op {
            plan.validate()
                .map_err(|reason| ExecPlanError::InvalidCountProgram {
                    step: step.id,
                    reason,
                })?;
            let dependency = plan
                .dependency()
                .expect("validated count programs have a dependency contract");
            let dependency_count_is_valid = match dependency {
                // One predecessor is a batch-sequencing edge whose value the
                // direct physical program deliberately ignores.
                ExecCountDependency::Direct => step.dependencies.len() <= 1,
                ExecCountDependency::Rows | ExecCountDependency::Scalars => {
                    step.dependencies.len() == 1
                }
            };
            if !dependency_count_is_valid {
                return Err(ExecPlanError::InvalidCountDependencyCount {
                    step: step.id,
                    dependency,
                    actual: step.dependencies.len(),
                });
            }
        }
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
