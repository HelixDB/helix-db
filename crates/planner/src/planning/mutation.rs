//! Mutation payload contract facade.
//!
//! Property assignments, node/edge target references, and shared raw-name/ID
//! validation live in focused modules. Native selected lowering reuses these
//! invariants without depending on temporary physical shapes.

mod assignments;
mod shared;
mod targets;

pub use assignments::property_assignments;
pub use targets::{edge_target, node_target};
