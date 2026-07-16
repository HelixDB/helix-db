//! Event construction helpers for selected handoff traces.

use crate::{ir, trace};

pub(super) fn push(
    trace: &mut trace::PlanningTrace,
    path: impl Into<String>,
    decision: trace::TraceDecision,
    reason: trace::TraceReason,
) {
    if let Some(event) =
        trace::TraceEvent::try_new(trace::TracePass::SelectedHandoff, path, decision, reason)
    {
        trace.events.push(event);
    }
}

pub(super) fn non_empty(value: impl Into<String>) -> Option<ir::NonEmptyString> {
    ir::NonEmptyString::new(value)
}
