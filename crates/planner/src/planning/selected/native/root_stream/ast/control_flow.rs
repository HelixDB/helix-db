//! Root-stream recognition for control-flow AST roots.

use helix_ast::traversal::AstNode;

use super::NativeRootStream;
use crate::planning::selected::native::{control_flow, scoped};
use crate::{context, error, logical};

pub(super) fn control_flow_root_stream_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
) -> Result<NativeRootStream, error::PlannerError> {
    match control_flow::native_control_flow_from_ast(ctx, root)? {
        scoped::ControlFlowRoot::Branch(branch) => {
            return Ok(NativeRootStream::Stream(Box::new(
                logical::RootStream::Branch(Box::new(branch)),
            )));
        }
        scoped::ControlFlowRoot::Repeat(repeat) => {
            return Ok(NativeRootStream::Stream(Box::new(
                logical::RootStream::Repeat(Box::new(repeat)),
            )));
        }
        scoped::ControlFlowRoot::NotControlFlow => {}
    }
    Ok(NativeRootStream::NotRootStream)
}
