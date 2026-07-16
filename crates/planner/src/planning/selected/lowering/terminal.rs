//! Selected run-root construction for stream-terminal logical families.

use super::super::rejection;
use super::super::SelectedCascadesPlanner;
use super::memo_children;
use crate::{error, exec, logical, physical};

pub(super) enum TerminalRootPayload<'a> {
    Project(&'a logical::StreamProject),
    Aggregate(&'a logical::StreamAggregate),
    Reserved(&'a logical::StreamReserved),
    VariableWrite(&'a logical::StreamVariableWrite),
}

impl TerminalRootPayload<'_> {
    fn input(&self) -> &logical::RootStream {
        match self {
            Self::Project(project) => project.input(),
            Self::Aggregate(aggregate) => aggregate.input(),
            Self::Reserved(reserved) => reserved.input(),
            Self::VariableWrite(write) => write.input(),
        }
    }

    fn into_terminal(self, input: exec::SelectedRootStreamInput) -> exec::SelectedRootTerminal {
        match self {
            Self::Project(project) => exec::SelectedRootTerminal::Project {
                input,
                projection: project.projection().clone(),
            },
            Self::Aggregate(aggregate) => exec::SelectedRootTerminal::Aggregate {
                input,
                aggregate: aggregate.aggregate().clone(),
            },
            Self::Reserved(reserved) => exec::SelectedRootTerminal::Reserved {
                input,
                op: reserved.op().clone(),
            },
            Self::VariableWrite(write) => exec::SelectedRootTerminal::VariableWrite {
                input,
                op: write.op().clone(),
            },
        }
    }
}

impl SelectedCascadesPlanner<'_> {
    pub(super) fn selected_terminal_run_root(
        &mut self,
        payload: TerminalRootPayload<'_>,
        alternative: physical::PhysicalAlternative,
        provenance: exec::SelectedRootProvenance,
        child_plans: memo_children::MemoChildPlanAvailability<'_, '_>,
        metrics: &mut exec::PlannerMetrics,
    ) -> Result<exec::SelectedExecutableRunRoot, error::PlannerError> {
        let input = self.selected_root_stream_input_with_memo_children(
            payload.input(),
            child_plans,
            metrics,
        )?;
        Ok(exec::SelectedExecutableRunRoot::Terminal(Box::new(
            exec::SelectedRootTerminalPlan::new(
                alternative.into(),
                provenance,
                payload.into_terminal(input),
            )
            .map_err(rejection::unsupported_root_construction)?,
        )))
    }
}
