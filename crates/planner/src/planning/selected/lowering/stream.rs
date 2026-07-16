//! Root-stream selected executable reconstruction.

use super::super::{rejection, SelectedCascadesPlanner};
use super::memo_children;
use crate::{error, exec, logical};

impl SelectedCascadesPlanner<'_> {
    #[cfg(test)]
    pub(super) fn selected_root_stream_input(
        &mut self,
        input: &logical::RootStream,
        metrics: &mut exec::PlannerMetrics,
    ) -> Result<exec::SelectedRootStreamInput, error::PlannerError> {
        self.selected_root_stream_input_with_memo_children(
            input,
            memo_children::MemoChildPlanAvailability::Unavailable,
            metrics,
        )
    }

    pub(super) fn selected_root_stream_input_with_memo_children(
        &mut self,
        input: &logical::RootStream,
        child_plans: memo_children::MemoChildPlanAvailability<'_, '_>,
        metrics: &mut exec::PlannerMetrics,
    ) -> Result<exec::SelectedRootStreamInput, error::PlannerError> {
        match input {
            logical::RootStream::Access(access) => {
                Ok(exec::SelectedRootStreamInput::Access(access.clone()))
            }
            logical::RootStream::VariableSource(source) => Ok(
                exec::SelectedRootStreamInput::VariableSource(source.clone()),
            ),
            logical::RootStream::Mutation(_) => {
                match self.selected_root_stream_child(child_plans, metrics)? {
                    exec::SelectedExecutableRunRoot::Mutation(mutation) => {
                        Ok(exec::SelectedRootStreamInput::Mutation(mutation))
                    }
                    _ => Err(rejection::unsupported(
                        rejection::Reason::RootStreamChildKindMismatch,
                    )),
                }
            }
            logical::RootStream::Branch(_) => {
                match self.selected_root_stream_child(child_plans, metrics)? {
                    exec::SelectedExecutableRunRoot::Branch(branch) => {
                        Ok(exec::SelectedRootStreamInput::Branch(branch))
                    }
                    _ => Err(rejection::unsupported(
                        rejection::Reason::RootStreamChildKindMismatch,
                    )),
                }
            }
            logical::RootStream::Repeat(_) => {
                match self.selected_root_stream_child(child_plans, metrics)? {
                    exec::SelectedExecutableRunRoot::Repeat(repeat) => {
                        Ok(exec::SelectedRootStreamInput::Repeat(repeat))
                    }
                    _ => Err(rejection::unsupported(
                        rejection::Reason::RootStreamChildKindMismatch,
                    )),
                }
            }
            logical::RootStream::Pipeline(_) => {
                let selected = self.selected_root_stream_child(child_plans, metrics)?;
                match selected {
                    exec::SelectedExecutableRunRoot::Pipeline(pipeline) => {
                        Ok(exec::SelectedRootStreamInput::Pipeline(pipeline))
                    }
                    _ => Err(rejection::unsupported(
                        rejection::Reason::RootStreamChildKindMismatch,
                    )),
                }
            }
            logical::RootStream::Reserved(_)
            | logical::RootStream::Project(_)
            | logical::RootStream::Aggregate(_)
            | logical::RootStream::VariableWrite(_) => {
                self.selected_terminal_root_stream_input(child_plans, metrics)
            }
        }
    }

    fn selected_root_stream_child(
        &mut self,
        child_plans: memo_children::MemoChildPlanAvailability<'_, '_>,
        metrics: &mut exec::PlannerMetrics,
    ) -> Result<exec::SelectedExecutableRunRoot, error::PlannerError> {
        let child_plans = child_plans.require()?;
        let child = child_plans
            .exactly(1, rejection::Reason::RootStreamChildArityMismatch)?
            .single()?;
        self.selected_run_root_from_memo_child(child, metrics)
    }

    fn selected_terminal_root_stream_input(
        &mut self,
        child_plans: memo_children::MemoChildPlanAvailability<'_, '_>,
        metrics: &mut exec::PlannerMetrics,
    ) -> Result<exec::SelectedRootStreamInput, error::PlannerError> {
        match self.selected_root_stream_child(child_plans, metrics)? {
            exec::SelectedExecutableRunRoot::Terminal(terminal) => {
                Ok(exec::SelectedRootStreamInput::Terminal(terminal))
            }
            _ => Err(rejection::unsupported(
                rejection::Reason::TerminalRootStreamChildKindMismatch,
            )),
        }
    }
}
