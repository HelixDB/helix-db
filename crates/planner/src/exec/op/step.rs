use serde::{Deserialize, Serialize};

use super::{ExecCondition, ExecOp};
use crate::exec::{ExecSchedule, ExecStepId};
use crate::{cost, ir, properties};

/// One executable DAG step.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecStep {
    /// Step ID.
    pub id: ExecStepId,
    /// Dependency step IDs.
    pub dependencies: Vec<ExecStepId>,
    /// Output binding behavior.
    pub output: ir::BatchOutputPlan,
    /// Run condition.
    pub condition: ExecCondition,
    /// Operation.
    pub op: ExecOp,
    /// Schedule contract.
    pub schedule: ExecSchedule,
    /// Delivered properties.
    pub delivered: properties::DeliveredProperties,
    /// Estimated cost.
    pub cost: cost::CostVector,
}
