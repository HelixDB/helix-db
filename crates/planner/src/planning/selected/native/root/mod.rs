//! Native AST root-shape recognition.
//!
//! This facade converts complete AST traversal roots into selected Cascades
//! logical roots. It dispatches by the shared native AST family first, then
//! delegates validation and payload construction to family-specific contract
//! modules.

mod lower;
mod result;

use helix_ast::traversal::AstNode;

use super::family;
use crate::{context, error};

pub(in crate::planning::selected::native) use result::NativeSelectableRoot;

pub(super) fn native_selectable_root_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
) -> Result<NativeSelectableRoot, error::PlannerError> {
    match family::NativeAstFamily::from_ast(root) {
        family::NativeAstFamily::Terminal => lower::terminal_root_from_ast(ctx, root),
        family::NativeAstFamily::VariableSource => lower::variable_source_root_from_ast(root),
        family::NativeAstFamily::IndexDdl => lower::index_ddl_root_from_ast(root),
        family::NativeAstFamily::ShortestPath => lower::shortest_path_root_from_ast(root),
        family::NativeAstFamily::SourceMutation => lower::source_mutation_root_from_ast(root),
        family::NativeAstFamily::ControlFlow => lower::control_flow_root_from_ast(ctx, root),
        family::NativeAstFamily::AccessOrPipeline => {
            lower::access_or_pipeline_root_from_ast(ctx, root)
        }
        family::NativeAstFamily::Context => Ok(NativeSelectableRoot::NotSelectable),
    }
}

#[cfg(test)]
mod tests;
