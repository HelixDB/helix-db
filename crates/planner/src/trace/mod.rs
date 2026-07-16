//! Planner trace contracts.
//!
//! Trace values are interpreter- and diagnostics-facing provenance. The module
//! is split by contract so pass identifiers, decision identifiers, parseable
//! reasons, and event records can evolve independently while the public
//! `trace::*` facade remains stable.

mod decision;
mod event;
mod pass;
mod reason;

pub use decision::TraceDecision;
pub use event::{PlanningTrace, TraceEvent};
pub use pass::TracePass;
pub use reason::TraceReason;

#[cfg(test)]
mod tests;
