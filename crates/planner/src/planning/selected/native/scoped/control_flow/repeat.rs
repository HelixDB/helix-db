//! Scoped repeat AST lowering.

use helix_ast::traversal::AstNode;

use super::super::super::scope::NativeAstScope;
use super::super::scoped_required_expr_from_ast;
use crate::planning::control_flow as flow_contracts;
use crate::{context, error, ir, logical};

pub(super) enum ScopedRepeatRoot {
    Repeat(Box<logical::RootRepeat>),
    NotRepeat,
}

pub(super) fn repeat_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
    scope: NativeAstScope,
) -> Result<ScopedRepeatRoot, error::PlannerError> {
    Ok(match root {
        AstNode::Repeat { input, config } => {
            let input = scoped_required_expr_from_ast(ctx, input, scope)?;
            let body = scoped_required_expr_from_ast(
                ctx,
                &config.traversal.root,
                NativeAstScope::SubTraversal,
            )?;
            ScopedRepeatRoot::Repeat(Box::new(logical::RootRepeat::new(
                input,
                ir::RepeatPlan {
                    body: Box::new(body),
                    stop: flow_contracts::repeat_stop(config.times, config.until.clone())?,
                    emit: flow_contracts::repeat_emit(config.emit, config.emit_predicate.clone())?,
                    max_depth: std::num::NonZeroUsize::new(config.max_depth).ok_or(
                        error::PlannerError::InvalidRepeatCount {
                            field: error::RepeatCountField::MaxDepth,
                            actual: config.max_depth,
                        },
                    )?,
                },
            )))
        }
        _ => ScopedRepeatRoot::NotRepeat,
    })
}
