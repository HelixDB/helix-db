//! Pure pipeline physical-contract facade.
//!
//! Sequence assembly, individual physical op mapping, stream-op cost mapping,
//! and delivered-property derivation are split so each contract can be tested
//! and tuned independently.

mod contract;
mod delivered;
mod op;
mod stream_op;

pub(in crate::rules) use contract::physical_pipeline_contract;

#[cfg(test)]
mod tests;
