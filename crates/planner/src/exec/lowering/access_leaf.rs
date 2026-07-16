//! Simple physical access leaf conversion facade.
//!
//! Selected access lowering uses these helpers for simple leaf conversions so
//! the allocation kernel stays focused on DAG construction instead of
//! source-specific access-shape matching.

mod edge;
mod node;

pub(in crate::exec) use edge::{edge_exec_access, SimpleEdgeAccessLeaf};
pub(in crate::exec) use node::{node_exec_access, SimpleNodeAccessLeaf};

#[cfg(test)]
mod tests;
