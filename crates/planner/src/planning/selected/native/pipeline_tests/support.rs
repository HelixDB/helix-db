use helix_ast::graph::{EdgeRef, NodeRef};
use helix_ast::traversal::AstNode;

use crate::{context, error, logical};

#[derive(Debug)]
pub(super) enum LoweredPipeline {
    Native(Box<logical::LogicalExpr>),
    NotPipeline,
}

impl LoweredPipeline {
    pub(super) fn expect_native(self, message: &str) -> logical::LogicalExpr {
        match self {
            Self::Native(expr) => *expr,
            Self::NotPipeline => panic!("{message}"),
        }
    }
}

pub(super) fn node_source() -> Box<AstNode> {
    Box::new(AstNode::Nodes {
        reference: NodeRef::All,
    })
}

pub(super) fn edge_source() -> Box<AstNode> {
    Box::new(AstNode::Edges {
        reference: EdgeRef::All,
    })
}

pub(super) fn count_source() -> Box<AstNode> {
    Box::new(AstNode::Count {
        input: node_source(),
    })
}

pub(super) fn ctx() -> context::PlannerContext {
    context::PlannerContext::default()
}

pub(super) fn lower(root: AstNode) -> Result<LoweredPipeline, error::PlannerError> {
    super::super::pipeline::native_pipeline_expr_from_ast(&ctx(), &root).map(|root| match root {
        super::super::pipeline::NativePipelineExprRoot::Pipeline(expr) => {
            LoweredPipeline::Native(expr)
        }
        super::super::pipeline::NativePipelineExprRoot::NotPipeline => LoweredPipeline::NotPipeline,
    })
}
