//! Scoped root-stream recognition for terminal AST roots.

use helix_ast::traversal::AstNode;

use super::super::terminal;
use super::ScopedRootStream;
use crate::planning::selected::native::root_stream;
use crate::planning::selected::native::scope::NativeAstScope;
use crate::{context, error};

pub(super) fn terminal_root_stream_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
    scope: NativeAstScope,
) -> Result<ScopedRootStream, error::PlannerError> {
    match terminal::terminal_expr_from_ast(ctx, root, scope)? {
        terminal::ScopedTerminalRoot::Terminal(expr) => {
            return root_stream::root_stream_from_expr(*expr)
                .map(Box::new)
                .map(ScopedRootStream::Stream);
        }
        terminal::ScopedTerminalRoot::NotTerminal => {}
    }
    Ok(ScopedRootStream::NotRootStream)
}
