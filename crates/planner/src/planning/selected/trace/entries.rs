//! Batch-entry traversal for selected handoff traces.

use crate::{exec, trace};

use super::{event, root};

/// Append selected executable-root provenance events to an existing trace.
pub(in crate::planning) fn append_selected_trace(
    trace: &mut trace::PlanningTrace,
    entries: &exec::SelectedExecutableBatchEntries,
) {
    push_selected_entries("selected.entry", entries, trace);
}

fn push_selected_entries(
    prefix: &str,
    entries: &exec::SelectedExecutableBatchEntries,
    trace: &mut trace::PlanningTrace,
) {
    match entries {
        exec::SelectedExecutableBatchEntries::Single(first) => {
            push_initial_entry(prefix, 0, first, trace);
        }
        exec::SelectedExecutableBatchEntries::WithFollowups { first, rest } => {
            push_initial_entry(prefix, 0, first, trace);
            rest.iter().enumerate().for_each(|(index, entry)| {
                push_followup_entry(prefix, index + 1, entry, trace);
            });
        }
    }
}

fn push_initial_entry(
    prefix: &str,
    index: usize,
    entry: &exec::SelectedInitialExecutableBatchEntry,
    trace: &mut trace::PlanningTrace,
) {
    match entry {
        exec::SelectedInitialExecutableBatchEntry::Run(entry) => {
            root::push_run_root(prefix, index, &entry.root, trace);
        }
        exec::SelectedInitialExecutableBatchEntry::ForEach(batch) => {
            push_foreach(prefix, index, batch.body(), trace);
        }
    }
}

fn push_followup_entry(
    prefix: &str,
    index: usize,
    entry: &exec::SelectedFollowupExecutableBatchEntry,
    trace: &mut trace::PlanningTrace,
) {
    match entry {
        exec::SelectedFollowupExecutableBatchEntry::Run(entry) => {
            root::push_run_root(prefix, index, &entry.root, trace);
        }
        exec::SelectedFollowupExecutableBatchEntry::ForEach(batch) => {
            push_foreach(prefix, index, batch.body(), trace);
        }
    }
}

fn push_foreach(
    prefix: &str,
    index: usize,
    body: &exec::SelectedExecutableBatchEntries,
    trace: &mut trace::PlanningTrace,
) {
    let path = format!("{prefix}[{index}]");
    event::push(
        trace,
        path.as_str(),
        trace::TraceDecision::SelectedForEach,
        trace::TraceReason::SelectedForEachBody,
    );
    push_selected_entries(&format!("{path}.body"), body, trace);
}
