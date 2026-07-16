//! Selected access-window limit pushdown entrypoints.
//!
//! Executable read-limit derivation and physical KV limit pushdown are separate
//! contracts behind this facade.

mod pushdown;
mod read_limit;

#[cfg(test)]
mod tests;

pub(in crate::exec::selected::lowering) use read_limit::{WindowAccessReadPlan, WindowSuffix};
