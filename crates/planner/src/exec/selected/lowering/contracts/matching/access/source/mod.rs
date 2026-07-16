//! Selected access source-family matching contracts.
//!
//! Shared mismatch ADTs live in `contracts`; node and edge modules own their
//! family-specific physical access matching tables.

mod contracts;
mod edge;
mod node;

#[cfg(test)]
mod tests;

pub(in crate::exec::selected::lowering::contracts::matching::access) use contracts::selected_access_path_match;
pub(in crate::exec::selected::lowering) use contracts::{
    SelectedAccessPathMatch, SelectedAccessPathMismatch, SelectedAccessShapeMatch,
    SelectedAccessShapeMismatch,
};
pub(in crate::exec::selected::lowering) use edge::selected_edge_access_matches;
pub(in crate::exec::selected::lowering) use node::selected_node_access_matches;
