use serde::{Deserialize, Serialize};

use crate::ir::NonEmptyString;

use super::{TraceDecision, TracePass, TraceReason};

/// Planning trace.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanningTrace {
    /// Trace events.
    pub events: Vec<TraceEvent>,
}

/// One planning decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceEvent {
    /// Pass name.
    pub pass: TracePass,
    /// AST path.
    pub path: NonEmptyString,
    /// Decision.
    pub decision: TraceDecision,
    /// Reason.
    pub reason: TraceReason,
}

impl TraceEvent {
    /// Build a trace event, returning `None` when the path is empty.
    ///
    /// ```
    /// use helix_planner::trace::{TraceDecision, TraceEvent, TracePass, TraceReason};
    ///
    /// assert!(
    ///     TraceEvent::try_new(
    ///         TracePass::AccessPath,
    ///         "entry[0]",
    ///         TraceDecision::NodeAllScan,
    ///         TraceReason::NodeRefAll,
    ///     )
    ///     .is_some()
    /// );
    /// assert!(
    ///     TraceEvent::try_new(
    ///         TracePass::AccessPath,
    ///         "",
    ///         TraceDecision::NodeAllScan,
    ///         TraceReason::NodeRefAll,
    ///     )
    ///     .is_none()
    /// );
    /// ```
    pub fn try_new(
        pass: TracePass,
        path: impl Into<String>,
        decision: TraceDecision,
        reason: TraceReason,
    ) -> Option<Self> {
        Some(Self {
            pass,
            path: NonEmptyString::new(path)?,
            decision,
            reason,
        })
    }
}
