//! Root-stream physical-contract facade.
//!
//! Root-stream implementation rules need three separable contracts: access
//! streams can contribute physical prefix operators, nested root streams are
//! localized behind child memo groups, and terminals append mandatory physical
//! tails. Keeping those contracts split makes the Cascades rule boundary easier
//! to test without routing every case through the full optimizer.

mod access;
mod contract;
mod delivered;

pub(in crate::rules) use contract::{
    root_pipeline_physical_contract, stream_aggregate_pipeline_contract,
    stream_project_pipeline_contract, stream_reserved_pipeline_contract,
    stream_variable_write_pipeline_contract,
};
pub(in crate::rules) use delivered::root_stream_delivered_properties;

#[cfg(test)]
mod tests;
