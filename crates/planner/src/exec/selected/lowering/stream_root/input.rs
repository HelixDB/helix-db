//! Recursive selected root-stream input lowering.
//!
//! Access and variable-source inputs can consume the localized physical prefix.
//! Recursive selected run roots already own their prefixes, so any parent prefix
//! on those variants is rejected instead of being silently applied twice.

use super::super::*;

impl ExecutableDagBuilder<'_> {
    pub(super) fn push_selected_root_stream_input(
        &mut self,
        input: SelectedRootStreamInput,
        prefix: &[physical::PhysicalPipelineOp],
        dependencies: Vec<ExecStepId>,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        match input {
            SelectedRootStreamInput::Access(access) => self.push_selected_access_stream(
                &access,
                prefix,
                dependencies,
                ir::BatchOutputPlan::Discard,
                condition,
            ),
            SelectedRootStreamInput::VariableSource(source) => self
                .push_selected_variable_source_stream(
                    &source,
                    prefix,
                    dependencies,
                    ir::BatchOutputPlan::Discard,
                    condition,
                ),
            SelectedRootStreamInput::Mutation(mutation) => {
                debug_assert!(
                    prefix.is_empty(),
                    "selected root constructors localize recursive mutation input prefixes"
                );
                self.push_selected_run_root(
                    SelectedExecutableRunRoot::Mutation(mutation),
                    dependencies,
                    ir::BatchOutputPlan::Discard,
                    condition,
                )
            }
            SelectedRootStreamInput::Branch(branch) => {
                debug_assert!(
                    prefix.is_empty(),
                    "selected root constructors localize recursive branch input prefixes"
                );
                self.push_selected_run_root(
                    SelectedExecutableRunRoot::Branch(branch),
                    dependencies,
                    ir::BatchOutputPlan::Discard,
                    condition,
                )
            }
            SelectedRootStreamInput::Repeat(repeat) => {
                debug_assert!(
                    prefix.is_empty(),
                    "selected root constructors localize recursive repeat input prefixes"
                );
                self.push_selected_run_root(
                    SelectedExecutableRunRoot::Repeat(repeat),
                    dependencies,
                    ir::BatchOutputPlan::Discard,
                    condition,
                )
            }
            SelectedRootStreamInput::Pipeline(pipeline) => {
                debug_assert!(
                    prefix.is_empty(),
                    "selected root constructors localize recursive pipeline input prefixes"
                );
                self.push_selected_run_root(
                    SelectedExecutableRunRoot::Pipeline(pipeline),
                    dependencies,
                    ir::BatchOutputPlan::Discard,
                    condition,
                )
            }
            SelectedRootStreamInput::Terminal(terminal) => {
                debug_assert!(
                    prefix.is_empty(),
                    "selected root constructors localize recursive terminal input prefixes"
                );
                self.push_selected_run_root(
                    SelectedExecutableRunRoot::Terminal(terminal),
                    dependencies,
                    ir::BatchOutputPlan::Discard,
                    condition,
                )
            }
        }
    }
}
