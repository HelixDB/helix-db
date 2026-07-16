//! Position-aware batch-entry draft materialization.
//!
//! `InitialEntryDraft` and `FollowupEntryDraft` are distinct wrapper types so a
//! prepared first entry cannot be accidentally materialized with follow-up
//! conditions, or vice versa. The recursive `EntryDraft` only owns shared query
//! and `ForEach` preparation.

use helix_ast::batch::{BatchEntry, NamedQuery};

use super::super::super::super::{cache, SelectedCascadesPlanner};
use super::super::super::rejection::{self, NativeUnsupportedReason};
use super::super::super::{entries, names};
use super::SelectedBatchDraft;
use crate::{cost, error, exec, ir};

pub(super) struct InitialEntryDraft<'a>(EntryDraft<'a>);

impl<'a> InitialEntryDraft<'a> {
    pub(super) fn prepare(
        planner: &SelectedCascadesPlanner<'_>,
        entry: &'a BatchEntry,
        pending: &mut cache::PendingSelectedRunRoots,
    ) -> Result<Self, error::PlannerError> {
        EntryDraft::prepare(planner, entry, pending).map(Self)
    }

    pub(super) fn materialize(
        self,
        optimized: &mut cache::OptimizedSelectedRunRoots,
        profile: &cost::StorageCostProfile,
    ) -> Result<
        (
            exec::SelectedInitialExecutableBatchEntry,
            exec::PlannerMetrics,
        ),
        error::PlannerError,
    > {
        match self.0 {
            EntryDraft::Query { query, root_use } => {
                let selected = selected_root_for_batch_use(optimized, root_use)?;
                let metrics = selected.metrics.clone();
                Ok((entries::initial_query_entry(query, selected)?, metrics))
            }
            EntryDraft::ForEach { param, body } => {
                let (body, mut metrics) = body.materialize_with_usage(optimized, profile)?;
                charge_foreach_wrapper(&mut metrics, profile);
                Ok((
                    exec::SelectedInitialExecutableBatchEntry::ForEach(
                        exec::SelectedForEachBatch::new(param, body),
                    ),
                    metrics,
                ))
            }
        }
    }
}

pub(super) struct FollowupEntryDraft<'a>(EntryDraft<'a>);

impl<'a> FollowupEntryDraft<'a> {
    pub(super) fn prepare(
        planner: &SelectedCascadesPlanner<'_>,
        entry: &'a BatchEntry,
        pending: &mut cache::PendingSelectedRunRoots,
    ) -> Result<Self, error::PlannerError> {
        EntryDraft::prepare(planner, entry, pending).map(Self)
    }

    pub(super) fn materialize(
        self,
        optimized: &mut cache::OptimizedSelectedRunRoots,
        profile: &cost::StorageCostProfile,
    ) -> Result<
        (
            exec::SelectedFollowupExecutableBatchEntry,
            exec::PlannerMetrics,
        ),
        error::PlannerError,
    > {
        match self.0 {
            EntryDraft::Query { query, root_use } => {
                let selected = selected_root_for_batch_use(optimized, root_use)?;
                let metrics = selected.metrics.clone();
                Ok((entries::followup_query_entry(query, selected)?, metrics))
            }
            EntryDraft::ForEach { param, body } => {
                let (body, mut metrics) = body.materialize_with_usage(optimized, profile)?;
                charge_foreach_wrapper(&mut metrics, profile);
                Ok((
                    exec::SelectedFollowupExecutableBatchEntry::ForEach(
                        exec::SelectedForEachBatch::new(param, body),
                    ),
                    metrics,
                ))
            }
        }
    }
}

fn charge_foreach_wrapper(metrics: &mut exec::PlannerMetrics, profile: &cost::StorageCostProfile) {
    metrics.selected_cost = profile.foreach_wrapper().serial(metrics.selected_cost);
}

fn selected_root_for_batch_use(
    optimized: &mut cache::OptimizedSelectedRunRoots,
    root_use: cache::SelectedRunRootUse,
) -> Result<cache::SelectedRunRoot, error::PlannerError> {
    optimized
        .select(root_use)
        .map_err(|_| rejection::unsupported(NativeUnsupportedReason::BatchRootUseMismatch))
}

enum EntryDraft<'a> {
    Query {
        query: &'a NamedQuery,
        root_use: cache::SelectedRunRootUse,
    },
    ForEach {
        param: ir::NonEmptyString,
        body: Box<SelectedBatchDraft<'a>>,
    },
}

impl<'a> EntryDraft<'a> {
    fn prepare(
        planner: &SelectedCascadesPlanner<'_>,
        entry: &'a BatchEntry,
        pending: &mut cache::PendingSelectedRunRoots,
    ) -> Result<Self, error::PlannerError> {
        match entry {
            BatchEntry::Query(query) => planner
                .prepare_selected_ast_query_root(query, pending)
                .map(|root_use| Self::Query { query, root_use }),
            BatchEntry::ForEach { param, body } => {
                let param = names::non_empty(param, ir::NameField::Param)?;
                let body =
                    SelectedBatchDraft::prepare(planner, body, error::BatchOp::ForEach, pending)?;
                Ok(Self::ForEach {
                    param,
                    body: Box::new(body),
                })
            }
        }
    }
}
