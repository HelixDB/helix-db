//! Selected executable delivered-property inference.
//!
//! Access streams, root-stream inputs, stream-pipeline operators, and variable
//! operators each own their property transform contracts behind this facade.

mod access;
mod pipeline;
mod root;
mod variable;

pub(in crate::exec::selected::lowering) use access::selected_access_path_delivered_properties;
pub(in crate::exec::selected::lowering) use pipeline::selected_stream_pipeline_delivered_properties;
pub(in crate::exec::selected::lowering) use root::selected_root_stream_input_delivered_properties;
#[cfg(test)]
pub(in crate::exec) use root::selected_stream_reserved_delivered_properties;
pub(in crate::exec) use variable::{
    selected_stream_variable_delivered_properties,
    selected_stream_variable_write_delivered_properties,
};
