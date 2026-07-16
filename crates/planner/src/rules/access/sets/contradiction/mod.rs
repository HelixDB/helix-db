//! Static contradiction detection for residual-free access intersections.

mod edge;
mod node;

use super::*;

pub(in crate::rules::access) fn simplify_access_contradiction(
    access: &logical::AccessPath,
) -> AccessSetRewrite {
    match access {
        logical::AccessPath::Node(path)
            if node::has_static_contradiction(path.source().as_ref()) =>
        {
            AccessSetRewrite::rewritten_empty_like(access)
        }
        logical::AccessPath::Edge(path)
            if edge::has_static_contradiction(path.source().as_ref()) =>
        {
            AccessSetRewrite::rewritten_empty_like(access)
        }
        logical::AccessPath::Node(_) | logical::AccessPath::Edge(_) => {
            AccessSetRewrite::NotApplicable
        }
    }
}

pub(in crate::rules) fn access_path_has_contradiction_candidate(
    access: &logical::AccessPath,
) -> bool {
    match access {
        logical::AccessPath::Node(path) => node::has_static_contradiction(path.source().as_ref()),
        logical::AccessPath::Edge(path) => edge::has_static_contradiction(path.source().as_ref()),
    }
}
