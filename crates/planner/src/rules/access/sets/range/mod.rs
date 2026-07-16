//! Same-key range-intersection tightening.

mod edge;
mod merge;
mod node;

use super::*;

pub(in crate::rules::access) fn simplify_access_range_intersection(
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

pub(in crate::rules) fn access_path_has_range_intersection_candidate(
    access: &logical::AccessPath,
) -> bool {
    match access {
        logical::AccessPath::Node(path) => node::has_candidate(path.source().as_ref()),
        logical::AccessPath::Edge(path) => edge::has_candidate(path.source().as_ref()),
    }
}
