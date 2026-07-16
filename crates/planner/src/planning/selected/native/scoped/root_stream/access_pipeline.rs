//! Scoped root-stream recognition for access and pipeline AST roots.

use helix_ast::traversal::AstNode;

use super::super::pipeline;
use super::ScopedRootStream;
use crate::planning::selected::native::scope::NativeAstScope;
use crate::planning::selected::native::{root_stream, shape};
use crate::{context, error};

pub(super) fn access_or_pipeline_root_stream_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
    scope: NativeAstScope,
) -> Result<ScopedRootStream, error::PlannerError> {
    match shape::native_access_stream_from_ast(ctx, root)? {
        shape::NativeAccessStreamRoot::Stream(stream) => {
            return stream
                .into_root_stream()
                .map(Box::new)
                .map(ScopedRootStream::Stream);
        }
        shape::NativeAccessStreamRoot::NotAccessStream => {}
    }
    match pipeline::pipeline_expr_from_ast(ctx, root, scope)? {
        pipeline::ScopedPipelineRoot::Pipeline(expr) => root_stream::root_stream_from_expr(*expr)
            .map(Box::new)
            .map(ScopedRootStream::Stream),
        pipeline::ScopedPipelineRoot::NotPipeline => Ok(ScopedRootStream::NotRootStream),
    }
}
