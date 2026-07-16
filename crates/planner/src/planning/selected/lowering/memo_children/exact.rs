//! Exact-arity proof wrapper for memo child groups.

use super::context::{MemoChildPlan, MemoChildPlanContext};
use super::cursor::MemoChildPlanCursor;
use crate::error;

#[derive(Debug)]
pub(in crate::planning::selected::lowering) struct ExactMemoChildPlanContext<'result, 'selection> {
    context: MemoChildPlanContext<'result, 'selection>,
}

impl<'result, 'selection> ExactMemoChildPlanContext<'result, 'selection> {
    pub(in crate::planning::selected::lowering::memo_children) fn new(
        context: MemoChildPlanContext<'result, 'selection>,
    ) -> Self {
        Self { context }
    }

    pub(in crate::planning::selected::lowering) fn single(
        self,
    ) -> Result<MemoChildPlan<'result, 'selection>, error::PlannerError> {
        self.context.into_selected(0)
    }

    pub(in crate::planning::selected::lowering) fn cursor(
        self,
    ) -> MemoChildPlanCursor<'result, 'selection> {
        MemoChildPlanCursor::new(self.context)
    }

    #[cfg(test)]
    pub(super) fn selected(
        &mut self,
        index: usize,
    ) -> Result<MemoChildPlan<'result, '_>, error::PlannerError> {
        self.context.selected(index)
    }
}
