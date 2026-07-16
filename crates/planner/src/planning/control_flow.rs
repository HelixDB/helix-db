//! Control-flow payload contract facade.
//!
//! Branch payload and repeat payload validation live in focused modules so
//! branch arity, predicate, emit, stop, and max-depth invariants stay
//! independently testable while preserving the public `control_flow::*` API.

mod branch;
mod repeat;

pub use branch::{choose_plan, coalesce_plan, optional_plan, union_plan};
pub use repeat::{repeat_emit, repeat_plan, repeat_stop};
