//! Selected control-flow contracts.
//!
//! Branch and repeat roots are selected barriers with selected run-root
//! children. The ADTs mirror the logical branch/repeat algebra while making
//! unselected child payloads impossible to represent.

mod branch;
mod repeat;

#[cfg(test)]
mod tests;

pub use self::branch::{SelectedBranchPlan, SelectedRootBranch};
pub use self::repeat::{SelectedRepeatPlan, SelectedRootRepeat};
