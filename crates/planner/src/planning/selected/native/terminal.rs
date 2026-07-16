//! Native AST terminal-root lowering facade.
//!
//! Terminal roots are split into a parser contract and expression builder so
//! scoped and unscoped lowering consume the same validated terminal payload.

mod expr;
mod payload;
#[cfg(test)]
mod tests;

use helix_ast::traversal::AstNode;

use crate::{error, logical};

pub(super) use expr::{native_terminal_expr_from_ast, NativeTerminalExprRoot};
#[cfg(test)]
pub(in crate::planning::selected::native) use payload::NativeTerminalOp;
pub(in crate::planning::selected::native) use payload::{
    NativeTerminalPayload, NativeTerminalRoot,
};

pub(in crate::planning::selected::native) fn terminal_payload_from_ast(
    root: &AstNode,
) -> Result<NativeTerminalRoot<'_>, error::PlannerError> {
    payload::terminal_payload_from_ast(root)
}

pub(in crate::planning::selected::native) fn terminal_expr_from_payload(
    input: logical::RootStream,
    payload: NativeTerminalPayload,
) -> logical::LogicalExpr {
    expr::terminal_expr_from_payload(input, payload)
}
