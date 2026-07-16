//! Stream bound, distinct, and order pipeline-op recognition.

use helix_ast::traversal::AstNode;

use super::super::super::{ordering, stream};
use super::contract::{NativePipelineOp, NativePipelineOpMatch};
use crate::{error, ir, logical};

pub(super) fn pipeline_op_from_ast(
    root: &AstNode,
) -> Result<NativePipelineOpMatch<'_>, error::PlannerError> {
    Ok(match root {
        AstNode::Dedup { input } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            logical::StreamPipelineOp::Distinct,
        )),
        AstNode::Limit { input, count } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            logical::StreamPipelineOp::Limit {
                count: stream::stream_bound_plan(count.clone())?,
            },
        )),
        AstNode::Skip { input, count } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            logical::StreamPipelineOp::Skip {
                count: stream::stream_bound_plan(count.clone())?,
            },
        )),
        AstNode::Range { input, start, end } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            logical::StreamPipelineOp::Range {
                range: stream::stream_range_plan(start.clone(), end.clone())?,
            },
        )),
        AstNode::OrderBy {
            input,
            property,
            order,
        } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            logical::StreamPipelineOp::Order {
                ordering: ordering::order_key(property, *order).map(ir::OrderKeys::from)?,
            },
        )),
        AstNode::OrderByMultiple { input, orderings } => {
            NativePipelineOpMatch::Op(NativePipelineOp::new(
                input.as_ref(),
                logical::StreamPipelineOp::Order {
                    ordering: ordering::order_keys(orderings)?,
                },
            ))
        }
        _ => NativePipelineOpMatch::NotThisFamily,
    })
}
