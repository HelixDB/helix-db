//! Graph mutation execution with transactional index-maintenance ownership.

mod adjacency;
mod contracts;
mod edge;
mod footprint;
mod index_context;
mod node;
mod ops;
mod properties;
pub(in crate::execution::interpreter) mod topology;
mod tx;
pub(super) mod visibility;

use super::*;

pub(super) use footprint::{LabelWrites, NodeIndexWrites};
pub(super) use index_context::MutationIndexContext;

#[cfg(test)]
mod tests;
