//! Literal equality-union branch removal when covered by range union branches.

mod edge;
mod node;
mod removal;

use super::super::AccessSetRewrite;
use crate::logical;

pub(in crate::rules::access) fn simplify_access_equality_range_union(
    access: &logical::AccessPath,
) -> AccessSetRewrite {
    match access {
        logical::AccessPath::Node(path) => {
            AccessSetRewrite::from_node_plan(node::simplify(path.source().as_ref()))
        }
        logical::AccessPath::Edge(path) => {
            AccessSetRewrite::from_edge_plan(edge::simplify(path.source().as_ref()))
        }
    }
}

pub(in crate::rules) fn access_path_has_equality_range_union_candidate(
    access: &logical::AccessPath,
) -> bool {
    match access {
        logical::AccessPath::Node(path) => node::has_candidate(path.source().as_ref()),
        logical::AccessPath::Edge(path) => edge::has_candidate(path.source().as_ref()),
    }
}
