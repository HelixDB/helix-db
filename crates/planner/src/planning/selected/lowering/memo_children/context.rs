//! Memo-child provenance and optimizer-selection access.

use super::super::super::rejection;
use super::exact::ExactMemoChildPlanContext;
use crate::{error, exec, memo, optimizer};

#[derive(Debug)]
pub(in crate::planning::selected::lowering) struct MemoChildPlanContext<'result, 'selection> {
    selection: &'selection mut optimizer::SelectionSession<'result>,
    children: memo::MemoChildGroups,
}

/// Availability proof for selected memo-child plans.
///
/// Some reconstruction entry points are used only by focused shape tests and
/// therefore have no optimizer selection session. Production selected lowering
/// carries `Available`; callers that need recursive child roots must consume
/// the proof with [`MemoChildPlanAvailability::require`].
#[derive(Debug)]
pub(in crate::planning::selected::lowering) enum MemoChildPlanAvailability<'result, 'selection> {
    /// Recursive memo-child plans can be selected from this optimizer session.
    Available(MemoChildPlanContext<'result, 'selection>),
    /// No recursive child-selection context exists at this boundary.
    Unavailable,
}

#[derive(Debug)]
pub(in crate::planning::selected::lowering) struct MemoChildPlan<'result, 'selection> {
    pub(in crate::planning::selected::lowering) selection:
        &'selection mut optimizer::SelectionSession<'result>,
    pub(in crate::planning::selected::lowering) selected:
        optimizer::SelectedPhysicalAlternative<'result>,
}

impl<'result, 'selection> MemoChildPlanAvailability<'result, 'selection> {
    pub(in crate::planning::selected::lowering) fn from_available_selection(
        selection: Option<&'selection mut optimizer::SelectionSession<'result>>,
        provenance: &exec::SelectedRootProvenance,
    ) -> Self {
        selection.map_or(Self::Unavailable, |selection| {
            Self::Available(MemoChildPlanContext::from_selection_and_provenance(
                selection, provenance,
            ))
        })
    }

    pub(in crate::planning::selected::lowering) fn require(
        self,
    ) -> Result<MemoChildPlanContext<'result, 'selection>, error::PlannerError> {
        match self {
            Self::Available(context) => Ok(context),
            Self::Unavailable => Err(rejection::unsupported(
                rejection::Reason::MemoChildContextMissing,
            )),
        }
    }
}

impl<'result, 'selection> MemoChildPlanContext<'result, 'selection> {
    pub(in crate::planning::selected::lowering) fn from_selection_and_provenance(
        selection: &'selection mut optimizer::SelectionSession<'result>,
        provenance: &exec::SelectedRootProvenance,
    ) -> Self {
        Self {
            selection,
            children: provenance.optimizer().source_child_groups().clone(),
        }
    }

    fn child_group(&self, index: usize) -> Result<memo::MemoGroupId, error::PlannerError> {
        self.children
            .as_slice()
            .get(index)
            .copied()
            .ok_or_else(|| rejection::unsupported(rejection::Reason::MemoChildPlanMissing))
    }

    pub(in crate::planning::selected::lowering::memo_children) fn selected(
        &mut self,
        index: usize,
    ) -> Result<MemoChildPlan<'result, '_>, error::PlannerError> {
        let group = self.child_group(index)?;
        let selection = &mut *self.selection;
        Ok(MemoChildPlan {
            selected: selection
                .best_plan(group)
                .map_err(|_| rejection::unsupported(rejection::Reason::MemoChildPlanMissing))?,
            selection,
        })
    }

    pub(in crate::planning::selected::lowering::memo_children) fn into_selected(
        self,
        index: usize,
    ) -> Result<MemoChildPlan<'result, 'selection>, error::PlannerError> {
        let group = self.child_group(index)?;
        let selection = self.selection;
        Ok(MemoChildPlan {
            selected: selection
                .best_plan(group)
                .map_err(|_| rejection::unsupported(rejection::Reason::MemoChildPlanMissing))?,
            selection,
        })
    }

    /// Convert to an exact-arity child contract for a selected parent shape.
    pub(in crate::planning::selected::lowering) fn exactly(
        self,
        expected: usize,
        mismatch: rejection::Reason,
    ) -> Result<ExactMemoChildPlanContext<'result, 'selection>, error::PlannerError> {
        if self.children.len() != expected {
            return Err(rejection::unsupported(mismatch));
        }
        Ok(ExactMemoChildPlanContext::new(self))
    }

    #[cfg(test)]
    pub(in crate::planning::selected::lowering) fn for_test(
        selection: &'selection mut optimizer::SelectionSession<'result>,
        children: Vec<memo::MemoGroupId>,
    ) -> Self {
        Self {
            selection,
            children: memo::MemoChildGroups::new(children),
        }
    }
}
