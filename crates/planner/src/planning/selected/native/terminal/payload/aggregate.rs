//! Native aggregate-terminal payload parsing.

use helix_ast::traversal::AstNode;

use super::{NativeTerminalOp, NativeTerminalPayload, NativeTerminalRoot};
use crate::{error, ir};

pub(super) fn aggregate_payload_from_ast(
    root: &AstNode,
) -> Result<NativeTerminalRoot<'_>, error::PlannerError> {
    Ok(match root {
        AstNode::Group { input, property } => NativeTerminalRoot::Terminal(NativeTerminalOp::new(
            input.as_ref(),
            NativeTerminalPayload::Aggregate(ir::AggregatePlan::Group(
                super::super::super::names::non_empty(property.as_str(), ir::NameField::Property)?,
            )),
        )),
        AstNode::GroupCount { input, property } => {
            NativeTerminalRoot::Terminal(NativeTerminalOp::new(
                input.as_ref(),
                NativeTerminalPayload::Aggregate(ir::AggregatePlan::GroupCount(
                    super::super::super::names::non_empty(
                        property.as_str(),
                        ir::NameField::Property,
                    )?,
                )),
            ))
        }
        AstNode::AggregateBy {
            input,
            function,
            property,
        } => NativeTerminalRoot::Terminal(NativeTerminalOp::new(
            input.as_ref(),
            NativeTerminalPayload::Aggregate(ir::AggregatePlan::AggregateBy {
                function: function.clone(),
                property: super::super::super::names::non_empty(
                    property.as_str(),
                    ir::NameField::Property,
                )?,
            }),
        )),
        _ => NativeTerminalRoot::NotTerminal,
    })
}
