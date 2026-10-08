//! Filter pipeline-op recognition.

use helix_ast::expr::Predicate;
use helix_ast::traversal::AstNode;

use super::contract::{NativePipelineOp, NativePipelineOpMatch};
use crate::planning::selected::native::parameter_specialization;
use crate::{analysis, context, error, ir, logical};

pub(super) fn pipeline_op_from_ast<'a>(
    ctx: &context::PlannerContext,
    root: &'a AstNode,
) -> Result<NativePipelineOpMatch<'a>, error::PlannerError> {
    Ok(match root {
        AstNode::Has {
            input,
            property,
            value,
        } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            filter_op(ctx, &Predicate::eq(property.clone(), value.clone()))?,
        )),
        AstNode::EdgeHas {
            input,
            property,
            value,
        } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            filter_op(ctx, &Predicate::eq(property.clone(), value.clone()))?,
        )),
        AstNode::HasLabel { input, label } | AstNode::EdgeHasLabel { input, label } => {
            NativePipelineOpMatch::Op(NativePipelineOp::new(
                input.as_ref(),
                filter_op(ctx, &Predicate::eq("$label", label.clone()))?,
            ))
        }
        AstNode::HasKey { input, property } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            filter_op(ctx, &Predicate::has_key(property))?,
        )),
        AstNode::Where { input, predicate } => NativePipelineOpMatch::Op(NativePipelineOp::new(
            input.as_ref(),
            filter_op(ctx, predicate)?,
        )),
        _ => NativePipelineOpMatch::NotThisFamily,
    })
}

fn filter_op(
    ctx: &context::PlannerContext,
    predicate: &Predicate,
) -> Result<logical::StreamPipelineOp, error::PlannerError> {
    ir::PredicatePlan::validate(predicate)?;
    let predicate = parameter_specialization::predicate(ctx, predicate)?;
    let _ = analysis::prune_borrowed(&predicate)?;
    let predicate_plan = ir::PredicatePlan::new(predicate)
        .expect("specializing validated parameters preserves predicate validity");
    Ok(logical::StreamPipelineOp::Filter {
        predicate: predicate_plan,
    })
}
