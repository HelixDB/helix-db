//! Root-stream recognition for terminal AST roots.

use helix_ast::traversal::AstNode;

use super::NativeRootStream;
use crate::planning::selected::native::root_stream::root_stream_from_expr;
use crate::planning::selected::native::terminal;
use crate::{context, error};

pub(super) fn terminal_root_stream_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
) -> Result<NativeRootStream, error::PlannerError> {
    match terminal::native_terminal_expr_from_ast(ctx, root)? {
        terminal::NativeTerminalExprRoot::Terminal(expr) => {
            return root_stream_from_expr(*expr)
                .map(Box::new)
                .map(NativeRootStream::Stream);
        }
        terminal::NativeTerminalExprRoot::NotTerminal => {}
    }
    Ok(NativeRootStream::NotRootStream)
}
