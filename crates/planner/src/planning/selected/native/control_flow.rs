//! Complete control-flow root lowering.
//!
//! Branch and repeat AST roots use the same recursive scoped lowering contract
//! as branch/repeat payloads, but query-root scope keeps raw `Context`
//! unbound. This facade lets complete native roots and root-stream inputs admit
//! control-flow contracts without depending on the scoped entry dispatch order.

use helix_ast::traversal::AstNode;

use super::scope::NativeAstScope;
use super::scoped::{self, ControlFlowRoot};
use crate::{context, error};

pub(super) fn native_control_flow_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
) -> Result<ControlFlowRoot, error::PlannerError> {
    scoped::control_flow_from_ast(ctx, root, NativeAstScope::QueryRoot)
}
