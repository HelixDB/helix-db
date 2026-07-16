//! Selected batch lowering.
//!
//! This layer owns batch sequencing and follow-up dependencies. Selected
//! run-root dispatch and root-specific contracts live in sibling modules.

use super::*;

impl ExecutableDagBuilder<'_> {
    pub(super) fn push_selected_entries(
        &mut self,
        entries: SelectedExecutableBatchEntries,
    ) -> Result<(), ExecPlanError> {
        match entries {
            SelectedExecutableBatchEntries::Single(first) => {
                self.push_selected_initial_entry(first)
            }
            SelectedExecutableBatchEntries::WithFollowups { first, rest } => {
                self.push_selected_initial_entry(first)?;
                rest.into_iter()
                    .try_for_each(|entry| self.push_selected_followup_entry(entry))
            }
        }
    }

    fn push_selected_initial_entry(
        &mut self,
        entry: SelectedInitialExecutableBatchEntry,
    ) -> Result<(), ExecPlanError> {
        let root = match entry {
            SelectedInitialExecutableBatchEntry::Run(entry) => {
                let entry = *entry;
                let condition = initial_exec_condition(entry.condition);
                self.push_selected_run_root(entry.root, Vec::new(), entry.output, condition)?
            }
            SelectedInitialExecutableBatchEntry::ForEach(batch) => {
                let (param, body) = batch.into_parts();
                self.push_selected_foreach(param, body, Vec::new())?
            }
        };
        self.previous = Some(root);
        Ok(())
    }

    fn push_selected_followup_entry(
        &mut self,
        entry: SelectedFollowupExecutableBatchEntry,
    ) -> Result<(), ExecPlanError> {
        let previous = self.previous.ok_or_else(|| {
            unsupported_selected_alternative(rejection::Reason::FollowupBeforeInitialEntry)
        })?;
        let root = match entry {
            SelectedFollowupExecutableBatchEntry::Run(entry) => {
                let entry = *entry;
                let condition = followup_exec_condition(entry.condition, previous);
                self.push_selected_run_root(entry.root, vec![previous], entry.output, condition)?
            }
            SelectedFollowupExecutableBatchEntry::ForEach(batch) => {
                let (param, body) = batch.into_parts();
                self.push_selected_foreach(param, body, vec![previous])?
            }
        };
        self.previous = Some(root);
        Ok(())
    }

    fn push_selected_foreach(
        &mut self,
        param: ir::NonEmptyString,
        body: SelectedExecutableBatchEntries,
        dependencies: Vec<ExecStepId>,
    ) -> Result<ExecStepId, ExecPlanError> {
        let body = lower_selected_executable_batch_entries(body, self.profile)?;
        let cost = foreach_subplan_cost(&body, self.profile);
        self.push_step(StepDraft {
            dependencies,
            output: ir::BatchOutputPlan::Discard,
            condition: ExecCondition::Always,
            op: ExecOp::ForEach {
                param,
                body: Box::new(body),
            },
            schedule: ExecSchedule::Barrier,
            delivered: properties::DeliveredProperties::default(),
            cost,
        })
    }
}
