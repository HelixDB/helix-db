//! Graph expansion pipeline-op recognition.

use helix_ast::traversal::AstNode;

use super::super::super::expansion as native_expansion;
use super::contract::{NativePipelineOp, NativePipelineOpMatch};
use crate::{error, ir, logical};

pub(super) fn pipeline_op_from_ast(
    root: &AstNode,
) -> Result<NativePipelineOpMatch<'_>, error::PlannerError> {
    Ok(match root {
        AstNode::Out { input, label } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            expand_op(
                ir::ExpandDirection::Out,
                ir::ExpandOutput::Nodes,
                label.as_deref(),
            )?,
        )),
        AstNode::In { input, label } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            expand_op(
                ir::ExpandDirection::In,
                ir::ExpandOutput::Nodes,
                label.as_deref(),
            )?,
        )),
        AstNode::Both { input, label } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            expand_op(
                ir::ExpandDirection::Both,
                ir::ExpandOutput::Nodes,
                label.as_deref(),
            )?,
        )),
        AstNode::OutE { input, label } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            expand_op(
                ir::ExpandDirection::Out,
                ir::ExpandOutput::Edges,
                label.as_deref(),
            )?,
        )),
        AstNode::InE { input, label } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            expand_op(
                ir::ExpandDirection::In,
                ir::ExpandOutput::Edges,
                label.as_deref(),
            )?,
        )),
        AstNode::BothE { input, label } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            expand_op(
                ir::ExpandDirection::Both,
                ir::ExpandOutput::Edges,
                label.as_deref(),
            )?,
        )),
        AstNode::OutN { input } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            expand_op(ir::ExpandDirection::Out, ir::ExpandOutput::Nodes, None)?,
        )),
        AstNode::InN { input } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            expand_op(ir::ExpandDirection::In, ir::ExpandOutput::Nodes, None)?,
        )),
        AstNode::OtherN { input } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            expand_op(ir::ExpandDirection::Both, ir::ExpandOutput::Nodes, None)?,
        )),
        _ => NativePipelineOpMatch::NotThisFamily,
    })
}

fn expand_op(
    direction: ir::ExpandDirection,
    output: ir::ExpandOutput,
    label: Option<&str>,
) -> Result<logical::StreamPipelineOp, error::PlannerError> {
    native_expansion::plan(direction, output, label)
        .map(|plan| logical::StreamPipelineOp::Expand { plan })
}
