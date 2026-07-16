//! Native terminal AST payload parsing.

mod aggregate;
mod projection;
mod reserved;

use helix_ast::traversal::AstNode;

use crate::{error, ir};

/// Validated terminal payload plus the AST input it consumes.
pub(in crate::planning::selected::native) struct NativeTerminalOp<'a> {
    input: &'a AstNode,
    payload: NativeTerminalPayload,
}

impl<'a> NativeTerminalOp<'a> {
    pub(super) fn new(input: &'a AstNode, payload: NativeTerminalPayload) -> Self {
        Self { input, payload }
    }

    pub(in crate::planning::selected::native) fn into_parts(
        self,
    ) -> (&'a AstNode, NativeTerminalPayload) {
        (self.input, self.payload)
    }
}

/// Native terminal root recognition result.
pub(in crate::planning::selected::native) enum NativeTerminalRoot<'a> {
    /// The AST root is a validated terminal wrapper.
    Terminal(NativeTerminalOp<'a>),
    /// The AST root is not a terminal wrapper.
    NotTerminal,
}

/// Validated terminal payload family.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::planning::selected::native) enum NativeTerminalPayload {
    /// Projection terminal.
    Project(ir::ProjectionPlan),
    /// Aggregate terminal.
    Aggregate(ir::AggregatePlan),
    /// Reserved traversal terminal.
    Reserved(ir::ReservedOp),
}

/// Recognize and validate a native terminal payload without selecting its input.
pub(in crate::planning::selected::native) fn terminal_payload_from_ast(
    root: &AstNode,
) -> Result<NativeTerminalRoot<'_>, error::PlannerError> {
    match projection::projection_payload_from_ast(root)? {
        terminal @ NativeTerminalRoot::Terminal(_) => return Ok(terminal),
        NativeTerminalRoot::NotTerminal => {}
    }
    match aggregate::aggregate_payload_from_ast(root)? {
        terminal @ NativeTerminalRoot::Terminal(_) => return Ok(terminal),
        NativeTerminalRoot::NotTerminal => {}
    }
    reserved::reserved_payload_from_ast(root)
}
