//! Shared source access physical-contract facade.
//!
//! Source-family extraction, leaf costing, set aggregation, residual-filter
//! costing, and dispatch are split so each invariant is testable in isolation.

mod dispatch;
mod family;
mod filter;
mod leaf;
mod set;

pub(super) use dispatch::access_contract;
pub(super) use family::{AccessSourceFamily, AccessSourceParts, EqualityIndexKind};

#[cfg(test)]
mod tests;
