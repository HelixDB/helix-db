//! Optimizer-result dispatch for selected executable reconstruction.

use super::super::SelectedCascadesPlanner;
use super::{case, memo_children};
use crate::{error, exec, logical, optimizer, physical};

impl SelectedCascadesPlanner<'_> {
    pub(in crate::planning::selected) fn selected_run_root_from_optimizer_plan<'result>(
        &mut self,
        selection: &mut optimizer::SelectionSession<'result>,
        selected: optimizer::SelectedPhysicalAlternative<'result>,
        metrics: &mut exec::PlannerMetrics,
    ) -> Result<exec::SelectedExecutableRunRoot, error::PlannerError> {
        let source_expr = selected.source_expr.expr.clone();
        let alternative = selected.entry.alternative.clone();
        let provenance = super::super::session::selected_root_provenance(selected);
        self.selected_run_root_from_plan_with_selection(
            source_expr,
            alternative,
            provenance,
            Some(selection),
            metrics,
        )
    }

    #[cfg(test)]
    pub(super) fn selected_run_root_from_plan(
        &mut self,
        source_expr: logical::LogicalExpr,
        alternative: physical::PhysicalAlternative,
        provenance: exec::SelectedRootProvenance,
        metrics: &mut exec::PlannerMetrics,
    ) -> Result<exec::SelectedExecutableRunRoot, error::PlannerError> {
        self.selected_run_root_from_plan_with_selection(
            source_expr,
            alternative,
            provenance,
            None,
            metrics,
        )
    }

    fn selected_run_root_from_plan_with_selection<'result>(
        &mut self,
        source_expr: logical::LogicalExpr,
        alternative: physical::PhysicalAlternative,
        provenance: exec::SelectedRootProvenance,
        selection: Option<&mut optimizer::SelectionSession<'result>>,
        metrics: &mut exec::PlannerMetrics,
    ) -> Result<exec::SelectedExecutableRunRoot, error::PlannerError> {
        match case::SelectedRootPlanCase::classify(&source_expr, &alternative.expr)? {
            case::SelectedRootPlanCase::IndexDdl(ddl) => {
                self.selected_index_ddl_run_root(ddl, alternative, provenance)
            }
            case::SelectedRootPlanCase::Mutation(mutation) => {
                let child_plans =
                    memo_children::MemoChildPlanAvailability::from_available_selection(
                        selection,
                        &provenance,
                    );
                self.selected_mutation_run_root(
                    mutation,
                    alternative,
                    provenance,
                    child_plans,
                    metrics,
                )
            }
            case::SelectedRootPlanCase::Branch(branch) => {
                let child_plans =
                    memo_children::MemoChildPlanAvailability::from_available_selection(
                        selection,
                        &provenance,
                    );
                let child_plans = child_plans.require()?;
                self.selected_branch_run_root(branch, alternative, provenance, child_plans, metrics)
            }
            case::SelectedRootPlanCase::Repeat(repeat) => {
                let child_plans =
                    memo_children::MemoChildPlanAvailability::from_available_selection(
                        selection,
                        &provenance,
                    );
                let child_plans = child_plans.require()?;
                self.selected_repeat_run_root(repeat, alternative, provenance, child_plans, metrics)
            }
            case::SelectedRootPlanCase::ShortestPath(path) => {
                self.selected_shortest_path_run_root(path, alternative, provenance)
            }
            case::SelectedRootPlanCase::Pipeline(pipeline) => {
                let child_plans =
                    memo_children::MemoChildPlanAvailability::from_available_selection(
                        selection,
                        &provenance,
                    );
                self.selected_pipeline_run_root(
                    pipeline,
                    alternative,
                    provenance,
                    child_plans,
                    metrics,
                )
            }
            case::SelectedRootPlanCase::Terminal(payload) => {
                let child_plans =
                    memo_children::MemoChildPlanAvailability::from_available_selection(
                        selection,
                        &provenance,
                    );
                self.selected_terminal_run_root(
                    payload,
                    alternative,
                    provenance,
                    child_plans,
                    metrics,
                )
            }
            case::SelectedRootPlanCase::GenericAlternative(family) => {
                exec::SelectedExecutableRunRoot::classified_alternative_with_provenance(
                    source_expr,
                    alternative,
                    provenance,
                    family,
                )
                .map_err(super::super::rejection::unsupported_alternative_construction)
            }
        }
    }

    pub(super) fn selected_run_root_from_memo_child<'result, 'selection>(
        &mut self,
        child: memo_children::MemoChildPlan<'result, 'selection>,
        metrics: &mut exec::PlannerMetrics,
    ) -> Result<exec::SelectedExecutableRunRoot, error::PlannerError> {
        self.selected_run_root_from_optimizer_plan(child.selection, child.selected, metrics)
    }
}
