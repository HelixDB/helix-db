//! Predicate-analysis contract tests split by public proof boundary.

// Test-only allocator observation delegates unchanged operations to System.
// Production planner code continues to deny unsafe code.
#[allow(unsafe_code)]
pub(crate) mod allocations;

mod candidates;
mod index_atoms;
mod prune;
mod scalar;
mod support;
