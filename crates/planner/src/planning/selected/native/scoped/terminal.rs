//! Scoped terminal wrapper lowering.
//!
//! Terminal wrappers consume a scoped root stream and preserve the validated
//! terminal payload as a logical terminal ADT.

use helix_ast::traversal::AstNode;

use super::super::scope::NativeAstScope;
use super::super::terminal as native_terminal;
use super::root_stream;
use crate::{context, error, logical};

/// Scoped terminal expression recognition result.
pub(super) enum ScopedTerminalRoot {
    /// The AST root is a validated terminal expression.
    Terminal(Box<logical::LogicalExpr>),
    /// The AST root is not a terminal expression.
    NotTerminal,
}

pub(super) fn terminal_expr_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
    scope: NativeAstScope,
) -> Result<ScopedTerminalRoot, error::PlannerError> {
    let terminal_op = match native_terminal::terminal_payload_from_ast(root)? {
        native_terminal::NativeTerminalRoot::Terminal(terminal_op) => terminal_op,
        native_terminal::NativeTerminalRoot::NotTerminal => {
            return Ok(ScopedTerminalRoot::NotTerminal);
        }
    };
    let (input, payload) = terminal_op.into_parts();
    root_stream::required_root_stream_from_ast(ctx, input, scope).map(|input| {
        ScopedTerminalRoot::Terminal(Box::new(native_terminal::terminal_expr_from_payload(
            ctx, input, payload,
        )))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_terminal_reports_terminal_and_non_terminal_roots() {
        assert!(matches!(
            terminal_expr_from_ast(
                &context::PlannerContext::default(),
                &AstNode::Count {
                    input: Box::new(AstNode::Context),
                },
                NativeAstScope::SubTraversal,
            )
            .unwrap(),
            ScopedTerminalRoot::Terminal(expr)
                if matches!(expr.as_ref(), logical::LogicalExpr::StreamCardinality(_))
        ));
        assert!(matches!(
            terminal_expr_from_ast(
                &context::PlannerContext::default(),
                &AstNode::Context,
                NativeAstScope::SubTraversal,
            )
            .unwrap(),
            ScopedTerminalRoot::NotTerminal
        ));
    }
}
