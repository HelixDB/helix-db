//! Access delivered-property and cardinality contracts.
//!
//! These helpers only inspect access-plan ADTs and catalog metadata. They do not
//! allocate executable steps or consult cost profiles.

mod cardinality;
mod delivered;
mod locality;
mod ordering;

pub(in crate::exec) use cardinality::{edge_access_hard_upper_bound, node_access_hard_upper_bound};
pub(in crate::exec) use delivered::{
    edge_access_delivered_properties, element_point_delivered_properties,
    node_access_delivered_properties,
};

#[cfg(test)]
mod tests;
