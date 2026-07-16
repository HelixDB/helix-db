//! Root control-flow rule facade.
//!
//! Empty-input rewrites, shared delivered-property contracts, and branch/repeat
//! implementation rules live in narrower modules behind this stable facade.

mod contracts;
mod empty;
mod implementation;

pub use empty::RootControlFlowEmptyRule;
pub use implementation::{RootBranchImplementationRule, RootRepeatImplementationRule};
