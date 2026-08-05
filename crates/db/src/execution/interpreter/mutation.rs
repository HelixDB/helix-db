//! Graph mutation execution with transactional index-maintenance ownership.

mod adjacency;
#[cfg(feature = "production-coverage")]
pub(crate) mod benchmark_telemetry;
mod contracts;
mod edge;
mod index_context;
mod node;
mod ops;
mod properties;
pub(in crate::execution::interpreter) mod topology;
mod tx;
pub(super) mod visibility;

use super::*;

pub(super) use index_context::MutationIndexContext;

#[cfg(test)]
mod tests;
