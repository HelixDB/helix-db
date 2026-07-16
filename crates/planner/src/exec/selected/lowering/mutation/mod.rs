//! Selected mutation-root lowering.
//!
//! This layer owns mutation roots whose inputs may themselves be selected run
//! roots. Batch orchestration passes dependencies in; this module first
//! classifies the selected payload as a source or input-consuming mutation,
//! then emits the matching executable mutation step.

mod emit;
mod plan;

use self::plan::lower_selected_mutation_plan;
use super::*;

impl ExecutableDagBuilder<'_> {
    pub(in crate::exec::selected::lowering) fn push_selected_mutation_root(
        &mut self,
        mutation: SelectedRootMutation,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        let (alternative, _provenance, plan) = mutation.into_parts();
        let (delivered, cost) = alternative.clone_contract();
        self.push_lowered_selected_mutation(
            lower_selected_mutation_plan(plan),
            dependencies,
            output,
            condition,
            delivered,
            cost,
        )
    }
}
