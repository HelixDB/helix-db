//! Residual-free native access contracts.
//!
//! This module is the only native AST lowering boundary allowed to construct
//! [`logical::AccessPath`] source plans. It converts AST source references into
//! IR source wrappers that reject residual-filtered access plans by type.

mod edge;
mod ids;
mod node;

#[cfg(test)]
mod tests;

use helix_ast::graph::{EdgeRef, NodeRef};

use crate::{error, ir, logical};

/// Native source access that is safe to expose as a Cascades access path.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum NativeAccessPath {
    /// Node-producing access.
    Node(logical::NodeAccessPath),
    /// Edge-producing access.
    Edge(logical::EdgeAccessPath),
}

impl NativeAccessPath {
    /// Lower an AST node source reference into a residual-free native access path.
    pub(super) fn nodes(reference: &NodeRef) -> Result<Self, error::PlannerError> {
        node::node_access_plan(reference).map(Self::node_plan)
    }

    /// Lower an AST edge source reference into a residual-free native access path.
    pub(super) fn edges(reference: &EdgeRef) -> Result<Self, error::PlannerError> {
        edge::edge_access_plan(reference).map(Self::edge_plan)
    }

    /// Full node scan access path.
    pub(super) fn all_nodes() -> Self {
        Self::node_plan(ir::NodeAccessPlan::AllScan)
    }

    /// Full edge scan access path.
    pub(super) fn all_edges() -> Self {
        Self::edge_plan(ir::EdgeAccessPlan::AllScan)
    }

    /// Build from an already validated node access plan.
    pub(super) fn node_plan(plan: ir::NodeAccessPlan) -> Self {
        Self::Node(logical::NodeAccessPath::new(
            ir::NodeAccessSourcePlan::from_unfiltered(plan),
        ))
    }

    /// Build from an already validated edge access plan.
    pub(super) fn edge_plan(plan: ir::EdgeAccessPlan) -> Self {
        Self::Edge(logical::EdgeAccessPath::new(
            ir::EdgeAccessSourcePlan::from_unfiltered(plan),
        ))
    }

    /// Convert into the logical optimizer access-path ADT.
    pub(super) fn into_logical(self) -> logical::AccessPath {
        match self {
            Self::Node(path) => logical::AccessPath::Node(path),
            Self::Edge(path) => logical::AccessPath::Edge(path),
        }
    }
}
