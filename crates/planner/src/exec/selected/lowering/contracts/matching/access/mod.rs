//! Access logical-to-physical matching contracts.
//!
//! This facade keeps selected access matching decomposed by responsibility:
//! source-family matching, access-prefix pipeline matching, and access window
//! suffix matching.

mod pipeline;
mod source;
mod window;

pub(in crate::exec::selected::lowering) use pipeline::{
    selected_access_filter_pipeline_access, selected_access_pipeline_parts,
    SelectedAccessFilterPipelineMatch, SelectedAccessPipelineMatch,
};
pub(in crate::exec::selected::lowering) use source::{
    selected_edge_access_matches, selected_node_access_matches,
};
pub(in crate::exec::selected::lowering) use window::selected_access_window_pipeline_matches;
