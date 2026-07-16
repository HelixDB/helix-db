//! Validated executable plan contracts.
//!
//! This module is the interpreter-facing contract boundary for complete plans
//! and nested subplans. Construction always validates the DAG shape before a
//! value can cross the boundary, and serde uses the same constructors so
//! deserialization cannot bypass the invariants.

mod subplan;
mod top_level;

pub use self::subplan::ExecutableSubplan;
pub use self::top_level::ExecutablePlan;
