//! Graph mutation execution with transactional index-maintenance ownership.

mod adjacency;
mod contracts;
mod edge;
mod index_context;
mod node;
mod ops;
mod properties;
mod rows;
pub(in crate::execution::interpreter) mod topology;
mod tx;
pub(super) mod visibility;

use super::*;

pub(super) use index_context::MutationIndexContext;
pub(super) use rows::DeletionTargets;

#[cfg(test)]
mod tests;
