//! Selected run-root dispatch.
//!
//! This module is intentionally small: it routes each selected run-root variant
//! to the contract-specific lowering module that owns its invariants.

use super::*;

impl ExecutableDagBuilder<'_> {
    pub(in crate::exec::selected::lowering) fn push_selected_run_root(
        &mut self,
        root: SelectedExecutableRunRoot,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        match root {
            SelectedExecutableRunRoot::Alternative(root) => {
                let (source_expr, family, _provenance, alternative) = (*root).into_parts();
                let step = self.push_classified_selected_executable_alternative(
                    family,
                    &source_expr,
                    &alternative,
                    dependencies,
                    output,
                    condition,
                )?;
                let (delivered, cost) = alternative.clone_contract();
                self.override_step_contract(step, delivered, cost)?;
                Ok(step)
            }
            SelectedExecutableRunRoot::Mutation(mutation) => {
                self.push_selected_mutation_root(*mutation, dependencies, output, condition)
            }
            SelectedExecutableRunRoot::IndexDdl(ddl) => {
                self.push_selected_index_ddl_root(*ddl, dependencies, output, condition)
            }
            SelectedExecutableRunRoot::Branch(branch) => {
                self.push_selected_branch_root(*branch, dependencies, output, condition)
            }
            SelectedExecutableRunRoot::Repeat(repeat) => {
                self.push_selected_repeat_root(*repeat, dependencies, output, condition)
            }
            SelectedExecutableRunRoot::ShortestPath(path) => {
                self.push_selected_shortest_path_root(*path, dependencies, output, condition)
            }
            SelectedExecutableRunRoot::Pipeline(pipeline) => {
                self.push_selected_pipeline_root(*pipeline, dependencies, output, condition)
            }
            SelectedExecutableRunRoot::Terminal(terminal) => {
                self.push_selected_terminal_root(*terminal, dependencies, output, condition)
            }
        }
    }
}
