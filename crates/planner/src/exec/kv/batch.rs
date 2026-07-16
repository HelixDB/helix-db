//! Multi-get batch contract facade.
//!
//! The public batch contract stays narrow while proof-carrying implementation
//! details are split by responsibility: indexed key positions, original
//! position validation, prepared same-keyspace keys, executable multi-get plan
//! serde, and LSM-locality coalescing.

mod coalesce;
mod indexed;
mod plan;
mod positions;
mod prepared;

pub use self::{
    coalesce::{coalesce_multi_get_batches, coalesce_non_empty_multi_get_batches},
    plan::KvMultiGetPlan,
    prepared::KvMultiGetKeys,
};
