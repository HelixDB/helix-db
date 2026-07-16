//! Stream delivered-property and schedule contracts.
//!
//! These helpers describe how unary stream operators transform properties.
//! They are deliberately cost-free and allocation-free.

mod delivered;
mod literals;
mod schedule;

#[cfg(test)]
pub(in crate::exec) use delivered::{aggregate_delivered_properties, project_delivered_properties};
pub(in crate::exec) use delivered::{
    expand_delivered_properties, filtered_delivered_properties, limit_delivered_properties,
    materialized_delivered_properties, ordered_delivered_properties, preserve_barrier_effect,
    range_delivered_properties, skip_delivered_properties,
};
pub(in crate::exec) use literals::{stream_bound_literal, stream_range_literal_bounds};
pub(in crate::exec) use schedule::{project_schedule, reserved_schedule};
