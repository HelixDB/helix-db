//! Native projection-terminal payload parsing.

use helix_ast::traversal::AstNode;

use super::{NativeTerminalOp, NativeTerminalPayload, NativeTerminalRoot};
use crate::{error, ir};

pub(super) fn projection_payload_from_ast(
    root: &AstNode,
) -> Result<NativeTerminalRoot<'_>, error::PlannerError> {
    Ok(match root {
        AstNode::Count { input } => NativeTerminalRoot::Terminal(NativeTerminalOp::new(
            input.as_ref(),
            NativeTerminalPayload::Project(ir::ProjectionPlan::Count),
        )),
        AstNode::Exists { input } => NativeTerminalRoot::Terminal(NativeTerminalOp::new(
            input.as_ref(),
            NativeTerminalPayload::Project(ir::ProjectionPlan::Exists),
        )),
        AstNode::Id { input } => NativeTerminalRoot::Terminal(NativeTerminalOp::new(
            input.as_ref(),
            NativeTerminalPayload::Project(ir::ProjectionPlan::Id),
        )),
        AstNode::Label { input } => NativeTerminalRoot::Terminal(NativeTerminalOp::new(
            input.as_ref(),
            NativeTerminalPayload::Project(ir::ProjectionPlan::Label),
        )),
        AstNode::Values { input, properties } => {
            NativeTerminalRoot::Terminal(NativeTerminalOp::new(
                input.as_ref(),
                NativeTerminalPayload::Project(ir::ProjectionPlan::Values(
                    super::super::super::projection::values_properties(properties)?,
                )),
            ))
        }
        AstNode::ValueMap { input, properties } => {
            NativeTerminalRoot::Terminal(NativeTerminalOp::new(
                input.as_ref(),
                NativeTerminalPayload::Project(ir::ProjectionPlan::ValueMap(
                    super::super::super::projection::property_selection(properties.as_deref())?,
                )),
            ))
        }
        AstNode::Project { input, projections } => {
            NativeTerminalRoot::Terminal(NativeTerminalOp::new(
                input.as_ref(),
                NativeTerminalPayload::Project(ir::ProjectionPlan::Project(
                    super::super::super::projection::projection_items(projections)?,
                )),
            ))
        }
        AstNode::ProjectBindings {
            input,
            projections,
            distinct,
        } => NativeTerminalRoot::Terminal(NativeTerminalOp::new(
            input.as_ref(),
            NativeTerminalPayload::Project(ir::ProjectionPlan::ProjectBindings {
                projections: super::super::super::projection::binding_projection_items(
                    projections,
                )?,
                dedup: if *distinct {
                    ir::ProjectionDedupMode::Distinct
                } else {
                    ir::ProjectionDedupMode::All
                },
            }),
        )),
        AstNode::EdgeProperties { input } => NativeTerminalRoot::Terminal(NativeTerminalOp::new(
            input.as_ref(),
            NativeTerminalPayload::Project(ir::ProjectionPlan::EdgeProperties),
        )),
        _ => NativeTerminalRoot::NotTerminal,
    })
}
