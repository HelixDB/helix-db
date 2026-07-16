use super::super::{
    native_terminal_expr_from_ast, terminal_payload_from_ast, NativeTerminalExprRoot,
    NativeTerminalOp, NativeTerminalRoot,
};
use crate::{context, error, logical};
use helix_ast::graph;
use helix_ast::traversal::AstNode;

#[derive(Debug)]
pub(super) enum LoweredTerminal {
    Native(Box<logical::LogicalExpr>),
    NotTerminal,
}

impl LoweredTerminal {
    pub(super) fn expect_native(self, message: &str) -> logical::LogicalExpr {
        match self {
            Self::Native(expr) => *expr,
            Self::NotTerminal => panic!("{message}"),
        }
    }
}

pub(super) fn node_source() -> Box<AstNode> {
    Box::new(AstNode::Nodes {
        reference: graph::NodeRef::All,
    })
}

pub(super) fn ctx() -> context::PlannerContext {
    context::PlannerContext::default()
}

pub(super) fn lower(root: &AstNode) -> Result<LoweredTerminal, error::PlannerError> {
    native_terminal_expr_from_ast(&ctx(), root).map(|root| match root {
        NativeTerminalExprRoot::Terminal(expr) => LoweredTerminal::Native(expr),
        NativeTerminalExprRoot::NotTerminal => LoweredTerminal::NotTerminal,
    })
}

pub(super) fn payload(root: &AstNode) -> Result<NativeTerminalRoot<'_>, error::PlannerError> {
    terminal_payload_from_ast(root)
}

pub(super) fn terminal_payload(
    root: &AstNode,
) -> Result<NativeTerminalOp<'_>, error::PlannerError> {
    match payload(root)? {
        NativeTerminalRoot::Terminal(op) => Ok(op),
        NativeTerminalRoot::NotTerminal => panic!("expected terminal payload"),
    }
}
