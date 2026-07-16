//! Control-flow selected executable reconstruction.

mod branch;
mod repeat;

use super::super::{rejection, SelectedCascadesPlanner};
use super::memo_children;
use crate::{error, exec};

impl SelectedCascadesPlanner<'_> {
    pub(in crate::planning::selected::lowering) fn selected_flow_children(
        &mut self,
        expected: usize,
        child_plans: memo_children::MemoChildPlanContext<'_, '_>,
        metrics: &mut exec::PlannerMetrics,
    ) -> Result<Vec<exec::SelectedExecutableRunRoot>, error::PlannerError> {
        let mut child_plans = child_plans
            .exactly(expected, rejection::Reason::MemoChildArityMismatch)?
            .cursor();
        (0..expected)
            .map(|_| {
                let child = child_plans.next()?;
                self.selected_run_root_from_memo_child(child, metrics)
            })
            .collect()
    }
}
