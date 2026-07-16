use serde::{Deserialize, Serialize};

use crate::exec::ExecStepId;
use crate::ir;

/// Condition for an executable run step.
///
/// Previous-result conditions carry the dependency they read, so validation can
/// prove the condition cannot reference a step outside the DAG.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecCondition {
    /// Always execute the step once dependencies complete.
    Always,
    /// Execute only when a named variable condition is true.
    Variable(ir::BatchVariableConditionPlan),
    /// Execute only when the dependency step produced a non-empty result.
    PreviousStepNotEmpty {
        /// Dependency whose output is checked.
        dependency: ExecStepId,
    },
}
