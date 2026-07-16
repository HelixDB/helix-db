//! Graph mutation execution with transactional index-maintenance ownership.

mod active_text;
mod adjacency;
mod contracts;
mod edge;
mod index_context;
mod node;
mod ops;
mod properties;
mod tx;

use super::*;

pub(super) use index_context::MutationIndexContext;

#[cfg(test)]
mod tests;
