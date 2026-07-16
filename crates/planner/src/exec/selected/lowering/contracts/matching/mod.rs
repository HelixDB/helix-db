//! Logical-to-physical shape matching contracts for selected lowering.
//!
//! These helpers do not allocate executable steps and do not derive costs. They
//! only validate that the selected physical shape can implement the logical
//! selected-lowering contract that produced it.

mod access;
mod pipeline;

pub(in crate::exec::selected::lowering) use access::{
    selected_access_filter_pipeline_access, selected_access_pipeline_parts,
    selected_access_window_pipeline_matches, selected_edge_access_matches,
    selected_node_access_matches, SelectedAccessFilterPipelineMatch, SelectedAccessPipelineMatch,
};
pub(in crate::exec::selected::lowering) use pipeline::{
    selected_pipeline_from_ops, selected_stream_pipeline_ops_match,
};
