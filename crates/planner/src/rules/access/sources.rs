//! Shared residual-free access source contract facade.
//!
//! Label inference, residual-free path construction, hard-cardinality proofs,
//! digest-based dedupe, and set construction are split so access rules can use
//! one stable helper surface without mixing unrelated invariants.

mod cardinality;
mod dedupe;
mod labels;
mod path;
mod sets;

pub(super) use cardinality::{
    edge_source_hard_cardinality_upper_bound, node_source_hard_cardinality_upper_bound,
};
pub(super) use dedupe::{dedupe_edge_sources, dedupe_node_sources};
pub(super) use labels::{
    access_path_common_label, edge_source_common_label, node_source_common_label,
};
pub(super) use path::{
    access_path_is_direct_empty, edge_access_path_from_plan, empty_access_path_like,
    node_access_path_from_plan, AccessPathFromPlan,
};
pub(super) use sets::{
    edge_intersection_from_sources, edge_union_from_sources, node_intersection_from_sources,
    node_union_from_sources,
};

#[cfg(test)]
mod tests;
