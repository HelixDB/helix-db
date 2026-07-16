//! Native graph access contracts for executable plans.
//!
//! The public `exec::*` surface still exposes the same access ADTs, while this
//! facade keeps element-specific access payloads and limited-access wrappers in
//! focused modules.

mod edge;
mod limited;
mod node;

pub use edge::ExecEdgeAccessPlan;
pub use limited::{ExecAccessPlan, ExecAccessReadLimit, ExecLimitedAccessPlan};
pub use node::ExecNodeAccessPlan;

#[cfg(test)]
mod tests;
