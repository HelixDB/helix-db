//! Recursive native batch-selection draft.
//!
//! This module separates batch-shape validation from optimizer execution. It
//! walks complete native batch entries, records every query root that needs
//! Cascades selection, and only materializes selected executable batch entries
//! after the caller has optimized the pending roots in one request-scoped memo.

mod entry;

use helix_ast::batch::BatchEntry;

use super::super::super::{cache, metrics, SelectedCascadesPlanner};
use crate::{cost, error, exec, ir};

use self::entry::{FollowupEntryDraft, InitialEntryDraft};

pub(super) struct SelectedBatchDraft<'a> {
    first: InitialEntryDraft<'a>,
    rest: Vec<FollowupEntryDraft<'a>>,
}

impl<'a> SelectedBatchDraft<'a> {
    pub(super) fn prepare(
        planner: &SelectedCascadesPlanner<'_>,
        entries: &'a [BatchEntry],
        op: error::BatchOp,
        pending: &mut cache::PendingSelectedRunRoots,
    ) -> Result<Self, error::PlannerError> {
        let Some((first, rest)) = entries.split_first() else {
            return Err(error::PlannerError::InvalidBatchArity {
                op,
                min: 1,
                actual: 0,
            });
        };
        let first = InitialEntryDraft::prepare(planner, first, pending)?;
        let rest = rest
            .iter()
            .map(|entry| FollowupEntryDraft::prepare(planner, entry, pending))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { first, rest })
    }

    pub(super) fn materialize(
        self,
        mut optimized: cache::OptimizedSelectedRunRoots,
        profile: &cost::StorageCostProfile,
    ) -> Result<(exec::SelectedExecutableBatchEntries, exec::PlannerMetrics), error::PlannerError>
    {
        self.materialize_with_usage(&mut optimized, profile)
    }

    fn materialize_with_usage(
        self,
        optimized: &mut cache::OptimizedSelectedRunRoots,
        profile: &cost::StorageCostProfile,
    ) -> Result<(exec::SelectedExecutableBatchEntries, exec::PlannerMetrics), error::PlannerError>
    {
        let (first, mut total_metrics) = self.first.materialize(optimized, profile)?;
        let mut selected_rest = Vec::with_capacity(self.rest.len());
        for entry in self.rest {
            let (entry, entry_metrics) = entry.materialize(optimized, profile)?;
            metrics::merge_planner_metrics(&mut total_metrics, entry_metrics);
            selected_rest.push(entry);
        }
        Ok(match ir::AtLeast::<_, 1>::try_from_vec(selected_rest) {
            Some(rest) => (
                exec::SelectedExecutableBatchEntries::WithFollowups { first, rest },
                total_metrics,
            ),
            None => (
                exec::SelectedExecutableBatchEntries::Single(first),
                total_metrics,
            ),
        })
    }
}
