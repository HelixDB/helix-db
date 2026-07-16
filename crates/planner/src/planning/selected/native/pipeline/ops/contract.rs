//! Native pipeline-op contract wrapper.

use helix_ast::traversal::AstNode;

use crate::logical;

pub(in crate::planning::selected::native) struct NativePipelineOp<'a> {
    input: &'a AstNode,
    op: logical::StreamPipelineOp,
}

/// Result of probing one native pipeline-op family.
pub(super) enum NativePipelineOpMatch<'a> {
    /// The family owns this AST root and produced a validated operator.
    Op(NativePipelineOp<'a>),
    /// The AST root belongs to another family or is not a pipeline operator.
    NotThisFamily,
}

impl<'a> NativePipelineOp<'a> {
    pub(in crate::planning::selected::native::pipeline::ops) fn new(
        input: &'a AstNode,
        op: logical::StreamPipelineOp,
    ) -> Self {
        Self { input, op }
    }

    pub(in crate::planning::selected::native) fn into_parts(
        self,
    ) -> (&'a AstNode, logical::StreamPipelineOp) {
        (self.input, self.op)
    }
}
