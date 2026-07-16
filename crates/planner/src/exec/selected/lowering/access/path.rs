use super::*;
use crate::exec::ExecAccessReadLimit;

impl ExecutableDagBuilder<'_> {
    pub(in crate::exec::selected::lowering) fn push_selected_access_path(
        &mut self,
        access: &logical::AccessPath,
        physical_access: &physical::PhysicalAccess,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        self.push_selected_access_path_with_read_limit(
            access,
            physical_access,
            ExecAccessReadLimit::Unbounded,
            dependencies,
            output,
            condition,
        )
    }

    pub(in crate::exec::selected::lowering) fn push_selected_access_path_with_read_limit(
        &mut self,
        access: &logical::AccessPath,
        physical_access: &physical::PhysicalAccess,
        read_limit: ExecAccessReadLimit,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        match access {
            logical::AccessPath::Node(path)
                if selected_node_access_matches(path.source().as_ref(), physical_access) =>
            {
                self.push_selected_node_access_with_read_limit(
                    path.source().as_ref(),
                    physical_access,
                    read_limit,
                    dependencies,
                    output,
                    condition,
                )
            }
            logical::AccessPath::Edge(path)
                if selected_edge_access_matches(path.source().as_ref(), physical_access) =>
            {
                self.push_selected_edge_access_with_read_limit(
                    path.source().as_ref(),
                    physical_access,
                    read_limit,
                    dependencies,
                    output,
                    condition,
                )
            }
            logical::AccessPath::Node(_) | logical::AccessPath::Edge(_) => {
                Err(unsupported_selected_alternative(
                    rejection::Reason::AccessPathPhysicalAccessMismatch,
                ))
            }
        }
    }
}
