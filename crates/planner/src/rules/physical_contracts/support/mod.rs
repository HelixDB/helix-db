//! Shared physical-contract support helpers.
//!
//! These helpers are split by contract family so rule costing code can import
//! a stable facade without mixing unrelated invariants:
//!
//! - `cardinality`: row-count and upper-bound transforms.
//! - `delivered`: delivered-property constructors and transforms.
//! - `window`: access-window physical/operator contracts.
//! - `pipeline`: stream/access pipeline assembly helpers.

mod cardinality;
mod delivered;
mod pipeline;
mod window;

pub(in crate::rules) use self::{
    cardinality::{
        estimated_pipeline_rows, estimated_rows_bounded_by, stream_bound_upper, stream_range_upper,
        with_cardinality,
    },
    delivered::{
        access_delivered, aggregate_output_delivered, barrier_delivered, bounded_delivered,
        element_keyspace, empty_delivered, filtered_delivered, materialized_delivered,
        ordered_delivered, project_output_delivered, required_to_delivered_ordering,
        reserved_output_delivered, stream_variable_write_delivered_properties,
    },
    pipeline::{
        access_pipeline_op, physical_pipeline_from_first_and_rest,
        physical_pipeline_from_prefix_and_required_suffix,
        physical_pipeline_from_prefix_and_required_tail, stream_pipeline_op_contract,
    },
    window::access_window_stream_contract,
};

#[cfg(test)]
mod tests;
