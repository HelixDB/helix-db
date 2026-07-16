//! Selected shortest-path root lowering.

use super::*;

impl ExecutableDagBuilder<'_> {
    pub(in crate::exec::selected::lowering) fn push_selected_shortest_path_root(
        &mut self,
        path: SelectedRootShortestPath,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        let (alternative, _provenance, plan) = path.into_parts();
        let (delivered, cost) = alternative.clone_contract();
        self.push_step(StepDraft {
            dependencies,
            output,
            condition,
            op: ExecOp::ShortestPath { plan },
            schedule: ExecSchedule::Barrier,
            delivered,
            cost,
        })
    }
}
