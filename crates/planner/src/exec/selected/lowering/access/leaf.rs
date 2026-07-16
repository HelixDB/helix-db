use super::*;
use crate::exec::ExecAccessReadLimit;

impl ExecutableDagBuilder<'_> {
    pub(in crate::exec::selected::lowering) fn push_selected_node_access(
        &mut self,
        plan: &ir::NodeAccessPlan,
        access: &physical::PhysicalAccess,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        self.push_selected_node_access_with_read_limit(
            plan,
            access,
            ExecAccessReadLimit::Unbounded,
            dependencies,
            output,
            condition,
        )
    }

    pub(in crate::exec::selected::lowering) fn push_selected_node_access_with_read_limit(
        &mut self,
        plan: &ir::NodeAccessPlan,
        access: &physical::PhysicalAccess,
        read_limit: ExecAccessReadLimit,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        let read_limit = read_limit
            .elide_if_covered_by_hard_upper(crate::exec::node_access_hard_upper_bound(plan));
        match access {
            physical::PhysicalAccess::Kv(read) => self.push_step(StepDraft {
                dependencies,
                output,
                condition,
                op: ExecOp::KvRead(read.clone()),
                schedule: ExecSchedule::Pipeline,
                delivered: access_delivered_with_read_limit(
                    node_access_delivered_properties(plan),
                    read_limit,
                ),
                cost: node_access_cost(plan, self.profile),
            }),
            physical::PhysicalAccess::PointReads { .. } => self
                .push_selected_node_access_plan_with_read_limit(
                    plan,
                    read_limit,
                    dependencies,
                    output,
                    condition,
                ),
            physical::PhysicalAccess::Empty
            | physical::PhysicalAccess::RuntimeInput
            | physical::PhysicalAccess::LabelScan
            | physical::PhysicalAccess::EqualityIndex
            | physical::PhysicalAccess::RangeIndex
            | physical::PhysicalAccess::VectorSearch
            | physical::PhysicalAccess::TextSearch
            | physical::PhysicalAccess::SetIntersection
            | physical::PhysicalAccess::SetUnion
            | physical::PhysicalAccess::Expand => self
                .push_selected_node_access_plan_with_read_limit(
                    plan,
                    read_limit,
                    dependencies,
                    output,
                    condition,
                ),
        }
    }

    pub(in crate::exec::selected::lowering) fn push_selected_edge_access(
        &mut self,
        plan: &ir::EdgeAccessPlan,
        access: &physical::PhysicalAccess,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        self.push_selected_edge_access_with_read_limit(
            plan,
            access,
            ExecAccessReadLimit::Unbounded,
            dependencies,
            output,
            condition,
        )
    }

    pub(in crate::exec::selected::lowering) fn push_selected_edge_access_with_read_limit(
        &mut self,
        plan: &ir::EdgeAccessPlan,
        access: &physical::PhysicalAccess,
        read_limit: ExecAccessReadLimit,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        let read_limit = read_limit
            .elide_if_covered_by_hard_upper(crate::exec::edge_access_hard_upper_bound(plan));
        match access {
            physical::PhysicalAccess::Kv(read) => self.push_step(StepDraft {
                dependencies,
                output,
                condition,
                op: ExecOp::KvRead(read.clone()),
                schedule: ExecSchedule::Pipeline,
                delivered: access_delivered_with_read_limit(
                    edge_access_delivered_properties(plan),
                    read_limit,
                ),
                cost: edge_access_cost(plan, self.profile),
            }),
            physical::PhysicalAccess::PointReads { .. } => self
                .push_selected_edge_access_plan_with_read_limit(
                    plan,
                    read_limit,
                    dependencies,
                    output,
                    condition,
                ),
            physical::PhysicalAccess::Empty
            | physical::PhysicalAccess::RuntimeInput
            | physical::PhysicalAccess::LabelScan
            | physical::PhysicalAccess::EqualityIndex
            | physical::PhysicalAccess::RangeIndex
            | physical::PhysicalAccess::VectorSearch
            | physical::PhysicalAccess::TextSearch
            | physical::PhysicalAccess::SetIntersection
            | physical::PhysicalAccess::SetUnion
            | physical::PhysicalAccess::Expand => self
                .push_selected_edge_access_plan_with_read_limit(
                    plan,
                    read_limit,
                    dependencies,
                    output,
                    condition,
                ),
        }
    }
}

fn access_delivered_with_read_limit(
    delivered: properties::DeliveredProperties,
    read_limit: ExecAccessReadLimit,
) -> properties::DeliveredProperties {
    match read_limit {
        ExecAccessReadLimit::Unbounded => delivered,
        ExecAccessReadLimit::Bounded(limit) => {
            limit_delivered_properties(delivered, Some(limit.get()))
        }
    }
}
