//! Recursive selected access set-merge allocation.

use super::super::*;
use crate::exec;

impl ExecutableDagBuilder<'_> {
    pub(super) fn push_selected_node_access_merge(
        &mut self,
        plans: &ir::AtLeast<ir::NodeAccessSourcePlan, 2>,
        mode: exec::ExecMergeMode,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
        delivered: properties::DeliveredProperties,
    ) -> Result<ExecStepId, ExecPlanError> {
        let mut child_ids = Vec::with_capacity(plans.len());
        for plan in plans {
            child_ids.push(self.push_selected_node_access_plan(
                plan.as_ref(),
                dependencies.clone(),
                ir::BatchOutputPlan::Discard,
                condition.clone(),
            )?);
        }
        self.push_native_merge(child_ids, mode, output, condition, delivered, false)
    }

    pub(super) fn push_selected_edge_access_merge(
        &mut self,
        plans: &ir::AtLeast<ir::EdgeAccessSourcePlan, 2>,
        mode: exec::ExecMergeMode,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
        delivered: properties::DeliveredProperties,
    ) -> Result<ExecStepId, ExecPlanError> {
        let mut child_ids = Vec::with_capacity(plans.len());
        for plan in plans {
            child_ids.push(self.push_selected_edge_access_plan(
                plan.as_ref(),
                dependencies.clone(),
                ir::BatchOutputPlan::Discard,
                condition.clone(),
            )?);
        }
        self.push_native_merge(child_ids, mode, output, condition, delivered, false)
    }
}
