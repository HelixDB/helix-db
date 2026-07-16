//! Executable lowering contract inference.
//!
//! The parent lowering facade re-exports this module as the executable contract
//! boundary. Submodules keep access cardinality and stream-property inference
//! independently testable and free of DAG allocation state.

mod access;
mod stream;

pub(in crate::exec) use self::access::{
    edge_access_delivered_properties, edge_access_hard_upper_bound,
    element_point_delivered_properties, node_access_delivered_properties,
    node_access_hard_upper_bound,
};
#[cfg(test)]
pub(in crate::exec) use self::stream::{
    aggregate_delivered_properties, project_delivered_properties,
};
pub(in crate::exec) use self::stream::{
    expand_delivered_properties, filtered_delivered_properties, limit_delivered_properties,
    materialized_delivered_properties, ordered_delivered_properties, preserve_barrier_effect,
    project_schedule, range_delivered_properties, reserved_schedule, skip_delivered_properties,
    stream_bound_literal, stream_range_literal_bounds,
};
