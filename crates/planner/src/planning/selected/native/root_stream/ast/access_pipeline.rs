//! Root-stream recognition for access and pipeline AST roots.

use helix_ast::traversal::AstNode;

use super::NativeRootStream;
use crate::planning::selected::native::root_stream::root_stream_from_expr;
use crate::planning::selected::native::{pipeline, shape};
use crate::{context, error};

pub(super) fn access_or_pipeline_root_stream_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
) -> Result<NativeRootStream, error::PlannerError> {
    match shape::native_access_stream_from_ast(ctx, root)? {
        shape::NativeAccessStreamRoot::Stream(stream) => {
            return stream
                .into_root_stream()
                .map(Box::new)
                .map(NativeRootStream::Stream);
        }
        shape::NativeAccessStreamRoot::NotAccessStream => {}
    }
    match pipeline::native_pipeline_expr_from_ast(ctx, root)? {
        pipeline::NativePipelineExprRoot::Pipeline(expr) => root_stream_from_expr(*expr)
            .map(Box::new)
            .map(NativeRootStream::Stream),
        pipeline::NativePipelineExprRoot::NotPipeline => Ok(NativeRootStream::NotRootStream),
    }
}
