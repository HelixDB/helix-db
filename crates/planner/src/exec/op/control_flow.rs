use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};

use crate::exec::ExecutableSubplan;
use crate::ir;

/// Native executable branch plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecBranchPlan {
    /// Run all branch plans and union their outputs.
    Union(ir::AtLeast<ExecutableSubplan, 2>),
    /// Run the branch only for rows satisfying the condition.
    Choose {
        /// Branch condition.
        condition: ir::PredicatePlan,
        /// Then plan.
        then_plan: Box<ExecutableSubplan>,
    },
    /// Run one of two branch plans by condition.
    ChooseElse {
        /// Branch condition.
        condition: ir::PredicatePlan,
        /// Then plan.
        then_plan: Box<ExecutableSubplan>,
        /// Else plan.
        else_plan: Box<ExecutableSubplan>,
    },
    /// Run branch plans until one produces output.
    Coalesce(ir::AtLeast<ExecutableSubplan, 1>),
    /// Run an optional branch plan.
    Optional(Box<ExecutableSubplan>),
}

/// Native executable repeat plan with a validated body subplan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecRepeatPlan {
    /// Body executed per iteration.
    pub body: Box<ExecutableSubplan>,
    /// Early stop condition.
    pub stop: ir::RepeatStopPlan,
    /// Emit behavior.
    pub emit: ir::RepeatEmitPlan,
    /// Positive maximum depth.
    pub max_depth: NonZeroUsize,
}
