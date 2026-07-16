//! Shared selected access-source matching outcomes.

use crate::{logical, physical};

use super::{edge, node};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec::selected::lowering) enum SelectedAccessShapeMatch {
    Matched,
    NotMatched(SelectedAccessShapeMismatch),
}

impl SelectedAccessShapeMatch {
    pub(in crate::exec::selected::lowering) const fn is_matched(self) -> bool {
        matches!(self, Self::Matched)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec::selected::lowering) enum SelectedAccessShapeMismatch {
    PhysicalAccessFamilyMismatch,
    ResidualFilterRequiresPipeline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec::selected::lowering) enum SelectedAccessPathMatch {
    Matched,
    NotMatched(SelectedAccessPathMismatch),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec::selected::lowering) enum SelectedAccessPathMismatch {
    Node(SelectedAccessShapeMismatch),
    Edge(SelectedAccessShapeMismatch),
}

pub(in crate::exec::selected::lowering::contracts::matching::access) fn selected_access_path_match(
    access: &logical::AccessPath,
    physical_access: &physical::PhysicalAccess,
) -> SelectedAccessPathMatch {
    match access {
        logical::AccessPath::Node(path) => {
            match node::selected_node_access_match(path.source().as_ref(), physical_access) {
                SelectedAccessShapeMatch::Matched => SelectedAccessPathMatch::Matched,
                SelectedAccessShapeMatch::NotMatched(reason) => {
                    SelectedAccessPathMatch::NotMatched(SelectedAccessPathMismatch::Node(reason))
                }
            }
        }
        logical::AccessPath::Edge(path) => {
            match edge::selected_edge_access_match(path.source().as_ref(), physical_access) {
                SelectedAccessShapeMatch::Matched => SelectedAccessPathMatch::Matched,
                SelectedAccessShapeMatch::NotMatched(reason) => {
                    SelectedAccessPathMatch::NotMatched(SelectedAccessPathMismatch::Edge(reason))
                }
            }
        }
    }
}
