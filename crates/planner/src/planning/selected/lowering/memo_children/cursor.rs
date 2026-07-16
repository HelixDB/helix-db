//! Ordered cursor over exact memo-child plans.

use super::context::{MemoChildPlan, MemoChildPlanContext};
use crate::error;

#[derive(Debug)]
pub(in crate::planning::selected::lowering) struct MemoChildPlanCursor<'result, 'selection> {
    context: MemoChildPlanContext<'result, 'selection>,
    next_index: usize,
}

impl<'result, 'selection> MemoChildPlanCursor<'result, 'selection> {
    pub(in crate::planning::selected::lowering::memo_children) fn new(
        context: MemoChildPlanContext<'result, 'selection>,
    ) -> Self {
        Self {
            context,
            next_index: 0,
        }
    }

    pub(in crate::planning::selected::lowering) fn next(
        &mut self,
    ) -> Result<MemoChildPlan<'result, '_>, error::PlannerError> {
        let selected = self.context.selected(self.next_index)?;
        self.next_index += 1;
        Ok(selected)
    }
}
