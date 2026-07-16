//! Native reserved-terminal payload parsing.

use helix_ast::traversal::AstNode;

use super::{NativeTerminalOp, NativeTerminalPayload, NativeTerminalRoot};
use crate::error;

pub(super) fn reserved_payload_from_ast(
    root: &AstNode,
) -> Result<NativeTerminalRoot<'_>, error::PlannerError> {
    Ok(match root {
        AstNode::Fold { input } => NativeTerminalRoot::Terminal(NativeTerminalOp::new(
            input.as_ref(),
            NativeTerminalPayload::Reserved(super::super::super::reserved::fold()),
        )),
        AstNode::Unfold { input } => NativeTerminalRoot::Terminal(NativeTerminalOp::new(
            input.as_ref(),
            NativeTerminalPayload::Reserved(super::super::super::reserved::unfold()),
        )),
        AstNode::Path { input } => NativeTerminalRoot::Terminal(NativeTerminalOp::new(
            input.as_ref(),
            NativeTerminalPayload::Reserved(super::super::super::reserved::path()),
        )),
        AstNode::SimplePath { input } => NativeTerminalRoot::Terminal(NativeTerminalOp::new(
            input.as_ref(),
            NativeTerminalPayload::Reserved(super::super::super::reserved::simple_path()),
        )),
        AstNode::WithSack { input, initial } => {
            NativeTerminalRoot::Terminal(NativeTerminalOp::new(
                input.as_ref(),
                NativeTerminalPayload::Reserved(super::super::super::reserved::with_sack(initial)),
            ))
        }
        AstNode::SackSet { input, property } => {
            NativeTerminalRoot::Terminal(NativeTerminalOp::new(
                input.as_ref(),
                NativeTerminalPayload::Reserved(super::super::super::reserved::sack_set(
                    property.as_str(),
                )?),
            ))
        }
        AstNode::SackAdd { input, property } => {
            NativeTerminalRoot::Terminal(NativeTerminalOp::new(
                input.as_ref(),
                NativeTerminalPayload::Reserved(super::super::super::reserved::sack_add(
                    property.as_str(),
                )?),
            ))
        }
        AstNode::SackGet { input } => NativeTerminalRoot::Terminal(NativeTerminalOp::new(
            input.as_ref(),
            NativeTerminalPayload::Reserved(super::super::super::reserved::sack_get()),
        )),
        _ => NativeTerminalRoot::NotTerminal,
    })
}
