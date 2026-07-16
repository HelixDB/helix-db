//! Native AST batch-entry handoff.
//!
//! This module owns the boundary between complete AST batch entries and selected
//! executable IR batch entries. Traversal-root contracts are delegated to
//! `native::root`; this layer only controls batch sequencing, conditions,
//! nested `ForEach` bodies, and request-scoped multi-root optimization.

use helix_ast::batch::{BatchEntry, BatchQuery};

mod draft;

use super::super::{cache, SelectedCascadesPlanner};
use crate::{context, error, exec};

pub(in crate::planning) fn cascades_batch_entries_from_ast(
    query: &BatchQuery,
    ctx: &context::PlannerContext,
) -> Result<(exec::SelectedExecutableBatchEntries, exec::PlannerMetrics), error::PlannerError> {
    let entries = match query {
        BatchQuery::Read(batch) => batch.entries.as_slice(),
        BatchQuery::Write(batch) => batch.entries.as_slice(),
    };
    cascades_batch_entries_from_ast_entries(entries, ctx, error::BatchOp::Batch)
}

pub(in crate::planning) fn cascades_batch_entries_from_ast_entries(
    entries: &[BatchEntry],
    ctx: &context::PlannerContext,
    op: error::BatchOp,
) -> Result<(exec::SelectedExecutableBatchEntries, exec::PlannerMetrics), error::PlannerError> {
    SelectedCascadesPlanner::new(ctx).selected_ast_batch_entries(entries, op)
}

impl SelectedCascadesPlanner<'_> {
    pub(super) fn selected_ast_batch_entries(
        &mut self,
        entries: &[BatchEntry],
        op: error::BatchOp,
    ) -> Result<(exec::SelectedExecutableBatchEntries, exec::PlannerMetrics), error::PlannerError>
    {
        let mut pending = cache::PendingSelectedRunRoots::default();
        let draft = draft::SelectedBatchDraft::prepare(self, entries, op, &mut pending)?;

        let optimized = self.selected_uncached_logical_run_roots(pending)?;
        draft.materialize(optimized, &self.ctx().storage)
    }
}
