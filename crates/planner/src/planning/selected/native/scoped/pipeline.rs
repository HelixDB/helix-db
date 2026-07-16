//! Scoped pipeline wrapper lowering.
//!
//! Pipeline wrappers consume a recursively normalized root stream, preserving
//! access-specific pipelines when the input is access-rooted.

use helix_ast::traversal::AstNode;

use super::super::pipeline as native_pipeline;
use super::super::scope::NativeAstScope;
use super::root_stream;
use crate::{context, error, logical};

/// Scoped root-pipeline expression recognition result.
pub(super) enum ScopedPipelineRoot {
    /// The AST root is a validated root-pipeline expression.
    Pipeline(Box<logical::LogicalExpr>),
    /// The AST root is not a root-pipeline expression.
    NotPipeline,
}

pub(super) fn pipeline_expr_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
    scope: NativeAstScope,
) -> Result<ScopedPipelineRoot, error::PlannerError> {
    let pipeline_op = match native_pipeline::pipeline_op_from_ast(root)? {
        native_pipeline::NativePipelineRoot::Pipeline(pipeline_op) => pipeline_op,
        native_pipeline::NativePipelineRoot::NotPipeline => {
            return Ok(ScopedPipelineRoot::NotPipeline);
        }
    };
    let (input, op) = pipeline_op.into_parts();
    root_stream::required_root_stream_from_ast(ctx, input, scope)
        .and_then(|input| native_pipeline::pipeline_expr(input, op))
        .map(|expr| ScopedPipelineRoot::Pipeline(Box::new(expr)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_ast::expr::StreamBound;

    #[test]
    fn scoped_pipeline_reports_pipeline_and_non_pipeline_roots() {
        assert!(matches!(
            pipeline_expr_from_ast(
                &context::PlannerContext::default(),
                &AstNode::Limit {
                    input: Box::new(AstNode::Context),
                    count: StreamBound::Literal(1),
                },
                NativeAstScope::SubTraversal,
            )
            .unwrap(),
            ScopedPipelineRoot::Pipeline(expr)
                if matches!(expr.as_ref(), logical::LogicalExpr::RootPipeline(_))
        ));
        assert!(matches!(
            pipeline_expr_from_ast(
                &context::PlannerContext::default(),
                &AstNode::Context,
                NativeAstScope::SubTraversal,
            )
            .unwrap(),
            ScopedPipelineRoot::NotPipeline
        ));
    }
}
