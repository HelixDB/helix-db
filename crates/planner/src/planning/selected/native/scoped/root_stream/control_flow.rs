//! Scoped root-stream recognition for control-flow AST roots.

use helix_ast::traversal::AstNode;

use super::super::control_flow;
use super::ScopedRootStream;
use crate::planning::selected::native::scope::NativeAstScope;
use crate::{context, error, logical};

pub(super) fn control_flow_root_stream_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
    scope: NativeAstScope,
) -> Result<ScopedRootStream, error::PlannerError> {
    match control_flow::control_flow_from_ast(ctx, root, scope)? {
        control_flow::ControlFlowRoot::Branch(branch) => {
            return Ok(ScopedRootStream::Stream(Box::new(
                logical::RootStream::Branch(Box::new(branch)),
            )));
        }
        control_flow::ControlFlowRoot::Repeat(repeat) => {
            return Ok(ScopedRootStream::Stream(Box::new(
                logical::RootStream::Repeat(Box::new(repeat)),
            )));
        }
        control_flow::ControlFlowRoot::NotControlFlow => {}
    }
    Ok(ScopedRootStream::NotRootStream)
}
