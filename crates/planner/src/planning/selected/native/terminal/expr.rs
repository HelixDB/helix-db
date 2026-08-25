//! Native terminal expression construction.

use helix_ast::traversal::AstNode;

use super::payload::{self, NativeTerminalPayload, NativeTerminalRoot};
use crate::{context, error, logical};

use super::super::root_stream;

/// Native terminal expression recognition result.
pub(in crate::planning::selected::native) enum NativeTerminalExprRoot {
    /// The AST root is a validated terminal expression.
    Terminal(Box<logical::LogicalExpr>),
    /// The AST root is not a terminal expression.
    NotTerminal,
}

/// Lower a supported terminal AST shape into a logical root expression.
pub(in crate::planning::selected::native) fn native_terminal_expr_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
) -> Result<NativeTerminalExprRoot, error::PlannerError> {
    let terminal_op = match payload::terminal_payload_from_ast(root)? {
        NativeTerminalRoot::Terminal(terminal_op) => terminal_op,
        NativeTerminalRoot::NotTerminal => return Ok(NativeTerminalExprRoot::NotTerminal),
    };
    let (input, payload) = terminal_op.into_parts();
    root_stream::required_root_stream_from_ast(ctx, input).map(|input| {
        NativeTerminalExprRoot::Terminal(Box::new(terminal_expr_from_payload(ctx, input, payload)))
    })
}

/// Build a logical terminal expression from a selected root-stream input.
pub(in crate::planning::selected::native) fn terminal_expr_from_payload(
    ctx: &context::PlannerContext,
    input: logical::RootStream,
    payload: NativeTerminalPayload,
) -> logical::LogicalExpr {
    match payload {
        NativeTerminalPayload::Cardinality => logical::LogicalExpr::StreamCardinality(
            logical::StreamCardinality::new(input)
                .with_planning_bindings(ctx.params.clone(), ctx.late_bound_params.clone()),
        ),
        NativeTerminalPayload::Project(projection) => {
            logical::LogicalExpr::StreamProject(logical::StreamProject::new(input, projection))
        }
        NativeTerminalPayload::Aggregate(aggregate) => {
            logical::LogicalExpr::StreamAggregate(logical::StreamAggregate::new(input, aggregate))
        }
        NativeTerminalPayload::Reserved(op) => {
            logical::LogicalExpr::StreamReserved(logical::StreamReserved::new(input, op))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_ast::graph::NodeRef;

    fn nodes() -> Box<AstNode> {
        Box::new(AstNode::Nodes {
            reference: NodeRef::All,
        })
    }

    #[test]
    fn terminal_expr_reports_terminal_and_non_terminal_roots() {
        assert!(matches!(
            native_terminal_expr_from_ast(
                &context::PlannerContext::default(),
                &AstNode::Count { input: nodes() },
            )
            .unwrap(),
            NativeTerminalExprRoot::Terminal(expr)
                if matches!(expr.as_ref(), logical::LogicalExpr::StreamCardinality(_))
        ));
        assert!(matches!(
            native_terminal_expr_from_ast(&context::PlannerContext::default(), &AstNode::Context)
                .unwrap(),
            NativeTerminalExprRoot::NotTerminal
        ));
    }
}
