//! Access physical contracts.
//!
//! This module keeps the rule-facing access contract functions stable while
//! separating the source, pipeline, KV, set, delivered-property, and estimate
//! helpers that evolve independently as more query shapes move into Cascades.

mod contract;
mod delivered;
mod estimates;
mod kv;
mod pipeline;
mod sets;
mod source;

pub(in crate::rules) use pipeline::{
    access_distinct_pipeline_contract, access_filter_pipeline_contract,
    access_order_pipeline_contract, access_pipeline_physical_contract,
    access_window_pipeline_contract,
};
pub(in crate::rules) use source::access_path_contract;

#[cfg(test)]
pub(in crate::rules) use source::edge_access_contract;
#[cfg(test)]
pub(in crate::rules) use source::node_access_contract;
