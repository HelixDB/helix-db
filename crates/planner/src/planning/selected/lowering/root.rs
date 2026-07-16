//! Selected run-root construction for root-level logical families.

use super::super::rejection;
use super::super::SelectedCascadesPlanner;
use super::memo_children;
use crate::{error, exec, logical, physical};

impl SelectedCascadesPlanner<'_> {
    pub(super) fn selected_index_ddl_run_root(
        &mut self,
        ddl: &logical::RootIndexDdl,
        alternative: physical::PhysicalAlternative,
        provenance: exec::SelectedRootProvenance,
    ) -> Result<exec::SelectedExecutableRunRoot, error::PlannerError> {
        Ok(exec::SelectedExecutableRunRoot::IndexDdl(Box::new(
            exec::SelectedRootIndexDdl::new(alternative.into(), provenance, ddl.plan().clone())
                .map_err(rejection::unsupported_root_construction)?,
        )))
    }

    pub(super) fn selected_mutation_run_root(
        &mut self,
        mutation: &logical::RootMutation,
        alternative: physical::PhysicalAlternative,
        provenance: exec::SelectedRootProvenance,
        child_plans: memo_children::MemoChildPlanAvailability<'_, '_>,
        metrics: &mut exec::PlannerMetrics,
    ) -> Result<exec::SelectedExecutableRunRoot, error::PlannerError> {
        Ok(exec::SelectedExecutableRunRoot::Mutation(Box::new(
            exec::SelectedRootMutation::new(
                alternative.into(),
                provenance,
                self.selected_mutation_plan(mutation.plan(), child_plans, metrics)?,
            )
            .map_err(rejection::unsupported_root_construction)?,
        )))
    }

    pub(super) fn selected_branch_run_root(
        &mut self,
        branch: &logical::RootBranch,
        alternative: physical::PhysicalAlternative,
        provenance: exec::SelectedRootProvenance,
        child_plans: memo_children::MemoChildPlanContext<'_, '_>,
        metrics: &mut exec::PlannerMetrics,
    ) -> Result<exec::SelectedExecutableRunRoot, error::PlannerError> {
        let (input, plan) = self.selected_branch_input_and_plan(branch, child_plans, metrics)?;
        Ok(exec::SelectedExecutableRunRoot::Branch(Box::new(
            exec::SelectedRootBranch::new(alternative.into(), provenance, Box::new(input), plan)
                .map_err(rejection::unsupported_root_construction)?,
        )))
    }

    pub(super) fn selected_repeat_run_root(
        &mut self,
        repeat: &logical::RootRepeat,
        alternative: physical::PhysicalAlternative,
        provenance: exec::SelectedRootProvenance,
        child_plans: memo_children::MemoChildPlanContext<'_, '_>,
        metrics: &mut exec::PlannerMetrics,
    ) -> Result<exec::SelectedExecutableRunRoot, error::PlannerError> {
        let (input, plan) = self.selected_repeat_input_and_plan(repeat, child_plans, metrics)?;
        Ok(exec::SelectedExecutableRunRoot::Repeat(Box::new(
            exec::SelectedRootRepeat::new(alternative.into(), provenance, Box::new(input), plan)
                .map_err(rejection::unsupported_root_construction)?,
        )))
    }

    pub(super) fn selected_shortest_path_run_root(
        &mut self,
        path: &logical::RootShortestPath,
        alternative: physical::PhysicalAlternative,
        provenance: exec::SelectedRootProvenance,
    ) -> Result<exec::SelectedExecutableRunRoot, error::PlannerError> {
        Ok(exec::SelectedExecutableRunRoot::ShortestPath(Box::new(
            exec::SelectedRootShortestPath::new(
                alternative.into(),
                provenance,
                path.plan().clone(),
            )
            .map_err(rejection::unsupported_root_construction)?,
        )))
    }

    pub(super) fn selected_pipeline_run_root(
        &mut self,
        pipeline: &logical::RootPipeline,
        alternative: physical::PhysicalAlternative,
        provenance: exec::SelectedRootProvenance,
        child_plans: memo_children::MemoChildPlanAvailability<'_, '_>,
        metrics: &mut exec::PlannerMetrics,
    ) -> Result<exec::SelectedExecutableRunRoot, error::PlannerError> {
        let input = self.selected_root_stream_input_with_memo_children(
            pipeline.input(),
            child_plans,
            metrics,
        )?;
        Ok(exec::SelectedExecutableRunRoot::Pipeline(Box::new(
            exec::SelectedRootPipeline::new(
                alternative.into(),
                provenance,
                input,
                pipeline.ops_at_least().clone(),
            )
            .map_err(rejection::unsupported_root_construction)?,
        )))
    }
}
