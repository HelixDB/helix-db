//! Selected index-DDL root lowering.
//!
//! DDL lowering accepts only the selected root barrier contract; generic
//! logical/physical barrier pairs are rejected before they reach this module.

use super::*;

impl ExecutableDagBuilder<'_> {
    pub(in crate::exec::selected::lowering) fn push_selected_index_ddl_root(
        &mut self,
        ddl: SelectedRootIndexDdl,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        let (alternative, _provenance, plan) = ddl.into_parts();
        let (delivered, cost) = alternative.clone_contract();
        self.push_step(StepDraft {
            dependencies,
            output,
            condition,
            op: ExecOp::IndexDdl { plan },
            schedule: ExecSchedule::Barrier,
            delivered,
            cost,
        })
    }
}
