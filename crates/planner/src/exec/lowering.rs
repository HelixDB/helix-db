//! Shared executable DAG lowering allocation kernel.
//!
//! This module owns executable step IDs and DAG step allocation. Pure
//! condition, contract, costing, access-leaf, and selected lowering logic lives
//! in sibling modules so selected lowering can depend on narrow contracts
//! instead of physical-tree internals.

use super::*;
use crate::{cost, ir, properties};

mod access_leaf;
mod conditions;
mod contracts;
mod costing;

pub(in crate::exec) use self::access_leaf::{
    edge_exec_access, node_exec_access, SimpleEdgeAccessLeaf, SimpleNodeAccessLeaf,
};
pub(in crate::exec) use self::conditions::{followup_exec_condition, initial_exec_condition};
#[cfg(test)]
pub(in crate::exec) use self::contracts::{
    aggregate_delivered_properties, project_delivered_properties,
};
pub(in crate::exec) use self::contracts::{
    edge_access_delivered_properties, edge_access_hard_upper_bound,
    element_point_delivered_properties, expand_delivered_properties, filtered_delivered_properties,
    limit_delivered_properties, materialized_delivered_properties,
    node_access_delivered_properties, node_access_hard_upper_bound, ordered_delivered_properties,
    preserve_barrier_effect, project_schedule, range_delivered_properties, reserved_schedule,
    skip_delivered_properties, stream_bound_literal, stream_range_literal_bounds,
};
use self::costing::parallel_merge_cost;
pub(in crate::exec) use self::costing::{
    edge_access_cost, foreach_subplan_cost, node_access_cost, predicate_cost_for_rows,
};

pub(in crate::exec) struct ExecutableDagBuilder<'a> {
    pub(in crate::exec) profile: &'a cost::StorageCostProfile,
    pub(in crate::exec) next_id: Option<ExecStepId>,
    pub(in crate::exec) steps: Vec<ExecStep>,
    pub(in crate::exec) previous: Option<ExecStepId>,
}

impl ExecutableDagBuilder<'_> {
    pub(in crate::exec) fn new(profile: &cost::StorageCostProfile) -> ExecutableDagBuilder<'_> {
        ExecutableDagBuilder {
            profile,
            next_id: Some(ExecStepId::first()),
            steps: Vec::new(),
            previous: None,
        }
    }

    #[cfg(test)]
    pub(in crate::exec) fn with_next_id(
        profile: &cost::StorageCostProfile,
        next_id: Option<ExecStepId>,
    ) -> ExecutableDagBuilder<'_> {
        ExecutableDagBuilder {
            profile,
            next_id,
            steps: Vec::new(),
            previous: None,
        }
    }

    pub(in crate::exec) fn push_step(
        &mut self,
        draft: StepDraft,
    ) -> Result<ExecStepId, ExecPlanError> {
        let id = self.next_step_id()?;
        self.steps.push(ExecStep {
            id,
            dependencies: draft.dependencies,
            output: draft.output,
            condition: draft.condition,
            op: draft.op,
            schedule: draft.schedule,
            delivered: draft.delivered,
            cost: draft.cost,
        });
        Ok(id)
    }

    pub(in crate::exec) fn push_native_merge(
        &mut self,
        dependencies: Vec<ExecStepId>,
        mode: ExecMergeMode,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
        delivered: properties::DeliveredProperties,
        preserve_order: bool,
    ) -> Result<ExecStepId, ExecPlanError> {
        let max_concurrency = properties::PositiveUsize::at_least_one(
            dependencies
                .len()
                .min(self.profile.max_parallel_kv_reads.get()),
        );
        let merge_cost = parallel_merge_cost(self.profile, max_concurrency);
        self.push_step(StepDraft {
            dependencies,
            output,
            condition,
            op: ExecOp::Merge { mode },
            schedule: ExecSchedule::Parallel {
                max_concurrency,
                preserve_order,
            },
            delivered,
            cost: merge_cost,
        })
    }

    fn next_step_id(&mut self) -> Result<ExecStepId, ExecPlanError> {
        let id = self.next_id.ok_or(ExecPlanError::StepIdSpaceExhausted)?;
        self.next_id = id.next();
        Ok(id)
    }
}

pub(in crate::exec) struct StepDraft {
    pub(in crate::exec) dependencies: Vec<ExecStepId>,
    pub(in crate::exec) output: ir::BatchOutputPlan,
    pub(in crate::exec) condition: ExecCondition,
    pub(in crate::exec) op: ExecOp,
    pub(in crate::exec) schedule: ExecSchedule,
    pub(in crate::exec) delivered: properties::DeliveredProperties,
    pub(in crate::exec) cost: cost::CostVector,
}
