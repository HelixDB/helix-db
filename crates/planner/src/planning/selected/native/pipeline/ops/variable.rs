//! Variable read/write pipeline-op recognition.

use helix_ast::traversal::AstNode;

use super::super::super::variables as native_variables;
use super::contract::{NativePipelineOp, NativePipelineOpMatch};
use crate::{error, logical};

pub(super) fn pipeline_op_from_ast(
    root: &AstNode,
) -> Result<NativePipelineOpMatch<'_>, error::PlannerError> {
    Ok(match root {
        AstNode::Within { input, variable } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            logical::StreamPipelineOp::Variable {
                op: native_variables::within(variable)?,
            },
        )),
        AstNode::Without { input, variable } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            logical::StreamPipelineOp::Variable {
                op: native_variables::without(variable)?,
            },
        )),
        AstNode::Select { input, name } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            logical::StreamPipelineOp::Variable {
                op: native_variables::select(name)?,
            },
        )),
        AstNode::Bind { input, name } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            logical::StreamPipelineOp::Variable {
                op: native_variables::bind(name)?,
            },
        )),
        AstNode::Inject {
            input: Some(input),
            variable,
        } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            logical::StreamPipelineOp::Variable {
                op: native_variables::inject(variable)?,
            },
        )),
        AstNode::As { input, name } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            logical::StreamPipelineOp::VariableWrite {
                op: native_variables::as_write(name)?,
            },
        )),
        AstNode::Store { input, name } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            logical::StreamPipelineOp::VariableWrite {
                op: native_variables::store(name)?,
            },
        )),
        _ => NativePipelineOpMatch::NotThisFamily,
    })
}
