//! Native AST root-pipeline lowering.
//!
//! Access-rooted stream wrappers stay in `shape` so they can use access-specific
//! logical contracts. This module handles the same stream operators when their
//! input is a terminal or another supported root stream, plus graph expansion
//! roots that are naturally stream-pipeline operators.

mod expr;
mod ops;

use helix_ast::traversal::AstNode;

use super::root_stream;
use crate::{context, error, logical};

pub(super) use expr::pipeline_expr;
pub(super) use ops::{pipeline_op_from_ast, NativePipelineRoot};

/// Native root-pipeline expression recognition result.
pub(super) enum NativePipelineExprRoot {
    /// The AST root is a validated root-pipeline expression.
    Pipeline(Box<logical::LogicalExpr>),
    /// The AST root is not a root-pipeline expression.
    NotPipeline,
}

pub(super) fn native_pipeline_expr_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
) -> Result<NativePipelineExprRoot, error::PlannerError> {
    let pipeline_op = match pipeline_op_from_ast(root)? {
        NativePipelineRoot::Pipeline(pipeline_op) => pipeline_op,
        NativePipelineRoot::NotPipeline => return Ok(NativePipelineExprRoot::NotPipeline),
    };
    let (input, op) = pipeline_op.into_parts();
    root_stream::required_root_stream_from_ast(ctx, input)
        .and_then(|input| expr::pipeline_expr(input, op))
        .map(|expr| NativePipelineExprRoot::Pipeline(Box::new(expr)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_ast::expr::StreamBound;
    use helix_ast::graph::NodeRef;

    fn terminal_source() -> Box<AstNode> {
        Box::new(AstNode::Count {
            input: Box::new(AstNode::Nodes {
                reference: NodeRef::All,
            }),
        })
    }

    #[test]
    fn pipeline_expr_reports_pipeline_and_non_pipeline_roots() {
        assert!(matches!(
            native_pipeline_expr_from_ast(
                &context::PlannerContext::default(),
                &AstNode::Limit {
                    input: terminal_source(),
                    count: StreamBound::Literal(1),
                },
            )
            .unwrap(),
            NativePipelineExprRoot::Pipeline(expr)
                if matches!(expr.as_ref(), logical::LogicalExpr::RootPipeline(_))
        ));
        assert!(matches!(
            native_pipeline_expr_from_ast(&context::PlannerContext::default(), &AstNode::Context)
                .unwrap(),
            NativePipelineExprRoot::NotPipeline
        ));
    }
}
