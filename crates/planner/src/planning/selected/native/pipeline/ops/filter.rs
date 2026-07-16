//! Filter pipeline-op recognition.

use helix_ast::expr::Predicate;
use helix_ast::traversal::AstNode;

use super::contract::{NativePipelineOp, NativePipelineOpMatch};
use crate::{analysis, error, ir, logical};

pub(super) fn pipeline_op_from_ast(
    root: &AstNode,
) -> Result<NativePipelineOpMatch<'_>, error::PlannerError> {
    Ok(match root {
        AstNode::Has {
            input,
            property,
            value,
        } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            filter_op(&Predicate::eq(property.clone(), value.clone()))?,
        )),
        AstNode::EdgeHas {
            input,
            property,
            value,
        } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            filter_op(&Predicate::eq(property.clone(), value.clone()))?,
        )),
        AstNode::HasLabel { input, label } | AstNode::EdgeHasLabel { input, label } => {
            NativePipelineOpMatch::Op(NativePipelineOp::new(
                input.as_ref(),
                filter_op(&Predicate::eq("$label", label.clone()))?,
            ))
        }
        AstNode::HasKey { input, property } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            filter_op(&Predicate::has_key(property))?,
        )),
        AstNode::Where { input, predicate } => {
            NativePipelineOpMatch::Op(NativePipelineOp::new(input.as_ref(), filter_op(predicate)?))
        }
        _ => NativePipelineOpMatch::NotThisFamily,
    })
}

fn filter_op(predicate: &Predicate) -> Result<logical::StreamPipelineOp, error::PlannerError> {
    let _ = analysis::prune_statically_impossible_branches(predicate)?;
    Ok(logical::StreamPipelineOp::Filter {
        predicate: ir::PredicatePlan::new(predicate.clone())?,
    })
}
