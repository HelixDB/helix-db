use serde::{Deserialize, Serialize};

use super::{ExecCondition, ExecOp};
use crate::exec::{ExecSchedule, ExecStepId, ReturnShape};
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
    /// Semantic output shape captured before optimizer rewrites.
    ///
    /// Legacy and directly constructed steps may omit this and use executable
    /// inference as a compatibility fallback.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub semantic_return_shape: Option<ReturnShape>,
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
