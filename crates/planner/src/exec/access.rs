//! Native graph access contracts for executable plans.
//!
//! The public `exec::*` surface still exposes the same access ADTs, while this
//! facade keeps element-specific access payloads and limited-access wrappers in
//! focused modules.

mod edge;
mod limited;
mod node;

pub(in crate::exec) use edge::{exact_edge_equality, ExecEdgeEqualityAccessPlan};
pub use edge::{ExecEdgeAccessPlan, ExecEdgeSecondaryRangePlan, ExecEdgeSecondarySetPlan};
pub use limited::{ExecAccessPlan, ExecAccessReadLimit, ExecLimitedAccessPlan};
pub(in crate::exec) use node::{exact_node_equality, ExecNodeEqualityAccessPlan};
pub use node::{ExecNodeAccessPlan, ExecNodeSecondaryRangePlan, ExecNodeSecondarySetPlan};

#[cfg(test)]
mod tests;
