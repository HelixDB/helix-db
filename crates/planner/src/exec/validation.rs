//! Executable DAG validation facade.
//!
//! Validation is split by contract: `index` owns duplicate/root checks and the
//! validated step lookup, `contracts` owns local step invariants, `graph` owns
//! dependency reachability/cycle proofs, and `order` owns deterministic
//! interpreter-ready execution-stage derivation.

mod contracts;
mod graph;
mod index;
mod order;

pub(in crate::exec) use order::execution_order;

#[cfg(test)]
mod tests;
