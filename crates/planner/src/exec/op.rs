//! Interpreter-facing executable operation facade.
//!
//! The public `exec::*` surface still exposes the same operation ADTs, while
//! this facade keeps conditions, control flow, mutations, variables, and step
//! records in focused modules.

mod condition;
mod control_flow;
mod merge;
mod mutation;
mod operation;
mod step;
mod variable;

pub use condition::ExecCondition;
pub use control_flow::{ExecBranchPlan, ExecRepeatPlan};
pub use merge::ExecMergeMode;
pub use mutation::ExecMutationPlan;
pub use operation::ExecOp;
pub use step::ExecStep;
pub use variable::ExecVariableOp;
