//! Lowering from Cascades-selected physical alternatives into executable DAG steps.
//!
//! This module owns the selected executable lowering contract. It deliberately
//! depends on the parent lowering kernel only for DAG step allocation and native
//! executable primitives, keeping compatibility-tree adapters out of selected IR.

use super::super::{
    edge_access_cost, edge_access_delivered_properties, expand_delivered_properties,
    filtered_delivered_properties, followup_exec_condition, foreach_subplan_cost,
    initial_exec_condition, limit_delivered_properties, materialized_delivered_properties,
    node_access_cost, node_access_delivered_properties, ordered_delivered_properties,
    predicate_cost_for_rows, project_schedule, range_delivered_properties, reserved_schedule,
    skip_delivered_properties, stream_bound_literal, stream_range_literal_bounds, ExecBranchPlan,
    ExecCondition, ExecMutationPlan, ExecOp, ExecPlanError, ExecRepeatPlan, ExecSchedule,
    ExecStepId, ExecVariableOp, ExecutableDagBuilder, ExecutableSubplan, StepDraft,
};
use super::*;
use crate::{cost, ir, logical, physical, properties};

mod access;
mod alternative;
mod batch;
mod contracts;
mod control_flow;
mod count;
mod entry;
mod finish;
mod index_ddl;
mod mutation;
pub(in crate::exec) mod rejection;
mod root_stream;
mod run_root;
mod shortest_path;
mod stream_root;
mod subplan;

use contracts::*;
pub(in crate::exec) use entry::{
    lower_selected_executable_alternative, lower_selected_executable_batch_entries,
};
#[cfg(test)]
pub(in crate::exec) use rejection::Reason as SelectedExecutableRejectionReason;
use subplan::{lower_selected_branch_plan, lower_selected_run_root_as_subplan};

#[cfg(test)]
pub(in crate::exec) use contracts::{
    selected_stream_reserved_delivered_properties, selected_stream_variable_delivered_properties,
    selected_stream_variable_write_delivered_properties,
};
