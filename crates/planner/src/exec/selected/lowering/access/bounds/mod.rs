//! Access scan bound propagation for selected executable lowering.
//!
//! Bound extraction, KV read rewriting, and selected-lowering entrypoints are
//! separate so each contract can be tested without widening the access lowering
//! facade.

mod contracts;
mod kv;
mod window;

pub(in crate::exec::selected::lowering) use window::{WindowAccessReadPlan, WindowSuffix};
