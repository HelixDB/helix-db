//! Shared physical contract and costing helpers for optimizer rules.
//!
//! This facade keeps rule-facing physical contract functions stable while the
//! implementation is split by optimizer contract boundary. Access costing,
//! root-stream lowering contracts, pure pipeline contracts, individual stream
//! operators, and shared delivered-property helpers live in focused submodules.

mod access;
mod pipeline;
mod root_stream;
mod stream;
mod support;

pub(super) use self::{
    access::{
        access_distinct_pipeline_contract, access_filter_pipeline_contract,
        access_order_pipeline_contract, access_path_contract, access_pipeline_physical_contract,
        access_window_pipeline_contract,
    },
    pipeline::physical_pipeline_contract,
    root_stream::{
        root_pipeline_physical_contract, stream_aggregate_pipeline_contract,
        stream_project_pipeline_contract, stream_reserved_pipeline_contract,
        stream_variable_write_pipeline_contract,
    },
    stream::{stream_physical_contract, StreamPhysicalContract},
    support::{
        access_delivered, barrier_delivered, element_keyspace, empty_delivered, filtered_delivered,
        ordered_delivered,
    },
};

#[cfg(test)]
pub(in crate::rules) use self::access::edge_access_contract;
#[cfg(test)]
pub(in crate::rules) use self::access::node_access_contract;
