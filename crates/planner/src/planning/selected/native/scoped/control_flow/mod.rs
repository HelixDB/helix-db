//! Scoped branch and repeat AST contracts.
//!
//! This module is the boundary that decides whether a native branch/repeat AST
//! shape can be represented as a Cascades logical root. Child traversals are
//! recursively lowered as logical expressions, making invalid physical-child
//! branch payloads unrepresentable past this point.

mod branch;
mod repeat;
#[cfg(test)]
mod tests;

use helix_ast::traversal::AstNode;

use super::super::scope::NativeAstScope;
use crate::{context, error, logical};

/// Scoped control-flow recognition result.
pub(in crate::planning::selected::native) enum ControlFlowRoot {
    /// The AST root is a branch root with recursively lowered logical children.
    Branch(logical::RootBranch),
    /// The AST root is a repeat root with a recursively lowered logical body.
    Repeat(logical::RootRepeat),
    /// The AST root is not a control-flow family root.
    NotControlFlow,
}

pub(in crate::planning::selected::native) fn control_flow_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
    scope: NativeAstScope,
) -> Result<ControlFlowRoot, error::PlannerError> {
    match root {
        AstNode::Union { .. }
        | AstNode::Choose { .. }
        | AstNode::Coalesce { .. }
        | AstNode::Optional { .. } => match branch::branch_from_ast(ctx, root, scope)? {
            branch::ScopedBranchRoot::Branch(branch) => Ok(ControlFlowRoot::Branch(*branch)),
            branch::ScopedBranchRoot::NotBranch => Ok(ControlFlowRoot::NotControlFlow),
        },
        AstNode::Repeat { .. } => match repeat::repeat_from_ast(ctx, root, scope)? {
            repeat::ScopedRepeatRoot::Repeat(repeat) => Ok(ControlFlowRoot::Repeat(*repeat)),
            repeat::ScopedRepeatRoot::NotRepeat => Ok(ControlFlowRoot::NotControlFlow),
        },
        _ => Ok(ControlFlowRoot::NotControlFlow),
    }
}
