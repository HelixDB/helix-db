//! Request-scoped selected Cascades optimization session.
//!
//! This module owns memo search invocation, selected-root caching, and batched
//! optimization. It intentionally does not construct selected executable IR
//! directly; reconstruction is delegated to the sibling `lowering` contract.

use super::cache;
use super::metrics;
use super::rejection;
use super::root::SelectableRunRoot;
use crate::{context, error, exec, optimizer, rules};

/// Request-scoped selected Cascades lowering state.
///
/// The planner owns optimizer configuration and a selected-root cache for one
/// executable-plan request. The cache is an optimization only: digest buckets
/// are always checked with full logical-expression equality before reuse.
pub(super) struct SelectedCascadesPlanner<'a> {
    ctx: &'a context::PlannerContext,
    rules: rules::SeedRuleSet,
    config: optimizer::OptimizerConfig,
    selected_roots: cache::SelectedRunRootCache,
}

impl<'a> SelectedCascadesPlanner<'a> {
    pub(super) fn new(ctx: &'a context::PlannerContext) -> Self {
        Self {
            ctx,
            rules: rules::SeedRuleSet::default(),
            config: optimizer::OptimizerConfig::from_context(ctx),
            selected_roots: cache::SelectedRunRootCache::default(),
        }
    }

    pub(super) const fn ctx(&self) -> &context::PlannerContext {
        self.ctx
    }

    #[cfg(test)]
    pub(super) fn selected_logical_run_root(
        &mut self,
        logical_root: SelectableRunRoot,
    ) -> Result<cache::SelectedRunRoot, error::PlannerError> {
        if let Some(selected) = self.cached_selected_run_root(&logical_root) {
            return Ok(selected);
        }

        let root_expr = logical_root.expr().clone();
        let result = {
            let optimizer = self.rules.optimizer();
            optimizer.optimize(root_expr.clone(), &self.config)
        }
        .map_err(optimizer_error)?;
        let mut selection = result.selection_session();
        let selected = selection
            .best_plan(result.root())
            .map_err(selection_error)?;
        let mut metrics = result.metrics().clone();
        let root =
            self.selected_run_root_from_optimizer_plan(&mut selection, selected, &mut metrics)?;
        let selected = cache::SelectedRunRoot { root, metrics };
        self.selected_roots.insert(logical_root, selected.clone());
        Ok(selected)
    }

    pub(super) fn cached_selected_run_root(
        &self,
        logical_root: &SelectableRunRoot,
    ) -> Option<cache::SelectedRunRoot> {
        self.selected_roots.get(logical_root)
    }

    pub(super) fn selected_uncached_logical_run_roots(
        &mut self,
        pending: cache::PendingSelectedRunRoots,
    ) -> Result<cache::OptimizedSelectedRunRoots, error::PlannerError> {
        let Some(pending) = pending.into_optimizer_batch() else {
            return Ok(cache::OptimizedSelectedRunRoots::empty());
        };

        let pending_len = pending.len();
        let (root_exprs, pending_entries) = pending.into_parts();
        let result = {
            let optimizer = self.rules.optimizer();
            optimizer.optimize_many(root_exprs, &self.config)
        }
        .map_err(optimizer_error)?;
        if result.roots().len() != pending_len {
            return Err(rejection::unsupported(
                rejection::Reason::OptimizerRootCountMismatch,
            ));
        }
        let mut selected_roots = Vec::with_capacity(pending_len);
        let mut selection = result.selection_session();
        for (index, (root_group, pending)) in result.roots().iter().zip(pending_entries).enumerate()
        {
            let selected = selection.best_plan(*root_group).map_err(selection_error)?;
            let mut metrics = exec::PlannerMetrics {
                selected_cost: selected.selected_cost,
                ..exec::PlannerMetrics::default()
            };
            if index == 0 {
                metrics::merge_planner_work_metrics(&mut metrics, result.metrics());
            }
            let root =
                self.selected_run_root_from_optimizer_plan(&mut selection, selected, &mut metrics)?;
            let selected = cache::SelectedRunRoot { root, metrics };
            self.selected_roots
                .insert(pending.logical_root, selected.clone());
            selected_roots.push(selected);
        }
        cache::OptimizedSelectedRunRoots::new(selected_roots, pending_len)
            .map_err(optimized_root_batch_error)
    }
}

fn optimized_root_batch_error(
    _error: cache::OptimizedSelectedRunRootsError,
) -> error::PlannerError {
    rejection::unsupported(rejection::Reason::OptimizedRootBatchMismatch)
}

fn optimizer_error(error: crate::memo::MemoError) -> error::PlannerError {
    error::PlannerError::OptimizerFailure { memo_error: error }
}

fn selection_error(error: optimizer::SelectionError) -> error::PlannerError {
    match error {
        optimizer::SelectionError::NoPhysicalAlternatives { .. }
        | optimizer::SelectionError::UnsatisfiedRequiredProperties { .. } => {
            rejection::unsupported(rejection::Reason::BestPlanMissing)
        }
        selection_error => error::PlannerError::OptimizerSelectionFailure { selection_error },
    }
}

pub(super) fn selected_root_provenance(
    selected: optimizer::SelectedPhysicalAlternative<'_>,
) -> exec::SelectedRootProvenance {
    exec::SelectedRootProvenance::from_optimizer(exec::SelectedOptimizerProvenance::new(
        selected.entry.provenance.rule_id().clone(),
        selected.group,
        selected.source_expr.id,
        selected.entry.id,
        selected.source_expr.children.clone(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memo;

    #[test]
    fn optimizer_errors_map_to_planner_failures() {
        assert_eq!(
            optimizer_error(memo::MemoError::ExprIdSpaceExhausted),
            error::PlannerError::OptimizerFailure {
                memo_error: memo::MemoError::ExprIdSpaceExhausted
            }
        );
    }

    #[test]
    fn selection_contract_failures_map_to_planner_failures() {
        let group = memo::MemoGroupId::first();
        assert_eq!(
            selection_error(optimizer::SelectionError::NoPhysicalAlternatives { group }),
            rejection::unsupported(rejection::Reason::BestPlanMissing)
        );
        assert_eq!(
            selection_error(optimizer::SelectionError::MissingMemoGroup { group }),
            error::PlannerError::OptimizerSelectionFailure {
                selection_error: optimizer::SelectionError::MissingMemoGroup { group }
            }
        );
    }
}
