use serde::{Deserialize, Serialize};

use super::{ExecBranchPlan, ExecMergeMode, ExecMutationPlan, ExecRepeatPlan, ExecVariableOp};
use crate::exec::{ExecAccessPlan, ExecutableSubplan, KvReadPlan};
use crate::ir;

/// Interpreter-facing executable operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecOp {
    /// Native graph access.
    Access {
        /// Access plan.
        plan: Box<ExecAccessPlan>,
    },
    /// KV read.
    KvRead(KvReadPlan),
    /// Graph expansion.
    Expand { plan: ir::ExpandPlan },
    /// Residual predicate filter.
    Filter { predicate: ir::PredicatePlan },
    /// Row limit.
    Limit { count: ir::StreamBoundPlan },
    /// Row skip.
    Skip { count: ir::StreamBoundPlan },
    /// Row range/slice.
    Range { range: ir::StreamRangePlan },
    /// Deduplicate rows.
    Distinct,
    /// Order rows.
    Order { plan: ir::OrderPlan },
    /// Project rows.
    Project { projection: ir::ProjectionPlan },
    /// Aggregate rows.
    Aggregate { aggregate: ir::AggregatePlan },
    /// Variable source or stream operation.
    Variable { op: ExecVariableOp },
    /// Branching control flow.
    Branch { plan: ExecBranchPlan },
    /// Repeat control flow.
    Repeat { plan: ExecRepeatPlan },
    /// Unweighted shortest path.
    ShortestPath { plan: ir::ShortestPathPlan },
    /// Mutation.
    Mutation { plan: ExecMutationPlan },
    /// Index DDL.
    IndexDdl { plan: ir::IndexDdlPlan },
    /// Merge dependency outputs.
    Merge { mode: ExecMergeMode },
    /// Reserved state operation.
    Reserved { op: ir::ReservedOp },
    /// Execute a nested body once per item in a parameter value.
    ForEach {
        /// Parameter name.
        param: ir::NonEmptyString,
        /// Validated executable body.
        body: Box<ExecutableSubplan>,
    },
    /// Side-effect or materialization barrier.
    Barrier { name: ir::NonEmptyString },
    /// Placeholder operation for phase scaffolding and tests.
    Noop,
}
