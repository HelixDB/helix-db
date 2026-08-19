//! AST stream-operator recognition facade for native pipeline lowering.
//!
//! The facade preserves the native selected-planning contract while each
//! operator family owns its AST matching and payload validation.

mod bounds;
mod contract;
mod expansion;
mod filter;
mod search;
mod variable;

use helix_ast::traversal::AstNode;

use crate::{context, error};

pub(in crate::planning::selected::native) use contract::NativePipelineOp;
use contract::NativePipelineOpMatch;

/// Native pipeline-op recognition result.
pub(in crate::planning::selected::native) enum NativePipelineRoot<'a> {
    /// The AST root is a validated pipeline wrapper.
    Pipeline(NativePipelineOp<'a>),
    /// The AST root is not a pipeline wrapper.
    NotPipeline,
}

pub(in crate::planning::selected::native) fn pipeline_op_from_ast<'a>(
    ctx: &context::PlannerContext,
    root: &'a AstNode,
) -> Result<NativePipelineRoot<'a>, error::PlannerError> {
    match pipeline_op_family_from_ast(ctx, root)? {
        NativePipelineOpMatch::Op(op) => Ok(NativePipelineRoot::Pipeline(op)),
        NativePipelineOpMatch::NotThisFamily => Ok(NativePipelineRoot::NotPipeline),
    }
}

fn pipeline_op_family_from_ast<'a>(
    ctx: &context::PlannerContext,
    root: &'a AstNode,
) -> Result<NativePipelineOpMatch<'a>, error::PlannerError> {
    match root {
        AstNode::Out { .. }
        | AstNode::In { .. }
        | AstNode::Both { .. }
        | AstNode::OutE { .. }
        | AstNode::InE { .. }
        | AstNode::BothE { .. }
        | AstNode::OutN { .. }
        | AstNode::InN { .. }
        | AstNode::OtherN { .. } => expansion::pipeline_op_from_ast(root),
        AstNode::Has { .. }
        | AstNode::EdgeHas { .. }
        | AstNode::HasLabel { .. }
        | AstNode::EdgeHasLabel { .. }
        | AstNode::HasKey { .. }
        | AstNode::Where { .. } => filter::pipeline_op_from_ast(ctx, root),
        AstNode::Dedup { .. }
        | AstNode::Limit { .. }
        | AstNode::Skip { .. }
        | AstNode::Range { .. }
        | AstNode::OrderBy { .. }
        | AstNode::OrderByMultiple { .. } => bounds::pipeline_op_from_ast(root),
        AstNode::VectorSearchNodesWithin { .. }
        | AstNode::VectorSearchEdgesWithin { .. }
        | AstNode::TextSearchNodesWithin { .. }
        | AstNode::TextSearchEdgesWithin { .. } => search::pipeline_op_from_ast(ctx, root),
        AstNode::Within { .. }
        | AstNode::Without { .. }
        | AstNode::Select { .. }
        | AstNode::Bind { .. }
        | AstNode::Inject { input: Some(_), .. }
        | AstNode::As { .. }
        | AstNode::Store { .. } => variable::pipeline_op_from_ast(root),
        _ => Ok(NativePipelineOpMatch::NotThisFamily),
    }
}

#[cfg(test)]
mod tests;
