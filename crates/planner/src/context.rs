//! Planner input context contracts.
//!
//! Request context, runtime parameters, statistics, and optimizer guardrails
//! live in focused modules behind this stable facade. Keeping these contracts
//! separate makes the planner input surface independently testable without
//! widening call sites beyond `helix_planner::context::*`.

mod limits;
mod params;
mod planner;
mod stats;

pub use limits::{IndexUnionBranchLimit, OptimizerLimits, PlannerLimits};
pub use params::ParamBindings;
pub use planner::PlannerContext;
pub use stats::StatsSnapshot;
