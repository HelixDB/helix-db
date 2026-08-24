//! Persisted graph-property rows and canonical index values.

#![allow(clippy::module_inception)]

pub(crate) mod canonical_number;
pub(crate) mod equality_index_value;
pub mod property;
pub(crate) mod property_value;
pub(crate) mod range_index_value;
mod row;

pub use property::Property;
pub use row::decode_properties;
pub(crate) use row::{
    datetime_millis_to_rfc3339, encode_index_partition_value, encode_properties,
    sortable_i64_index_string,
};
