//! Family-specific native root lowering.

use helix_ast::traversal::AstNode;

use super::super::{
    control_flow, index_ddl, mutation, pipeline, scoped, shape, shortest_path, terminal,
    variable_source,
};
use super::result::{self, NativeSelectableRoot};
use crate::{context, error, logical};

pub(super) fn terminal_root_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
) -> Result<NativeSelectableRoot, error::PlannerError> {
    match terminal::native_terminal_expr_from_ast(ctx, root)? {
        terminal::NativeTerminalExprRoot::Terminal(expr) => Ok(result::selectable_expr(*expr)),
        terminal::NativeTerminalExprRoot::NotTerminal => Ok(NativeSelectableRoot::NotSelectable),
    }
}

pub(super) fn variable_source_root_from_ast(
    root: &AstNode,
) -> Result<NativeSelectableRoot, error::PlannerError> {
    match variable_source::native_variable_source_from_ast(root)? {
        variable_source::NativeVariableSourceRoot::Source(source) => Ok(result::selectable_expr(
            logical::LogicalExpr::VariableSource(source),
        )),
        variable_source::NativeVariableSourceRoot::InputConsuming
        | variable_source::NativeVariableSourceRoot::NotVariableSource => {
            Ok(NativeSelectableRoot::NotSelectable)
        }
    }
}

pub(super) fn index_ddl_root_from_ast(
    root: &AstNode,
) -> Result<NativeSelectableRoot, error::PlannerError> {
    match index_ddl::native_index_ddl_from_ast(root)? {
        index_ddl::NativeIndexDdlRoot::Root(ddl) => Ok(result::selectable_expr(
            logical::LogicalExpr::RootIndexDdl(ddl),
        )),
        index_ddl::NativeIndexDdlRoot::NotIndexDdl => Ok(NativeSelectableRoot::NotSelectable),
    }
}

pub(super) fn source_mutation_root_from_ast(
    root: &AstNode,
) -> Result<NativeSelectableRoot, error::PlannerError> {
    match mutation::native_mutation_from_ast(root)? {
        mutation::NativeMutationRoot::Source(mutation) => Ok(result::selectable_expr(
            logical::LogicalExpr::RootMutation(mutation),
        )),
        mutation::NativeMutationRoot::InputConsuming
        | mutation::NativeMutationRoot::NotMutation => Ok(NativeSelectableRoot::NotSelectable),
    }
}

pub(super) fn shortest_path_root_from_ast(
    root: &AstNode,
) -> Result<NativeSelectableRoot, error::PlannerError> {
    match shortest_path::native_shortest_path_from_ast(root)? {
        shortest_path::NativeShortestPathRoot::Root(path) => Ok(result::selectable_expr(
            logical::LogicalExpr::RootShortestPath(path),
        )),
        shortest_path::NativeShortestPathRoot::NotShortestPath => {
            Ok(NativeSelectableRoot::NotSelectable)
        }
    }
}

pub(super) fn control_flow_root_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
) -> Result<NativeSelectableRoot, error::PlannerError> {
    match control_flow::native_control_flow_from_ast(ctx, root)? {
        scoped::ControlFlowRoot::Branch(branch) => Ok(result::selectable_expr(
            logical::LogicalExpr::RootBranch(branch),
        )),
        scoped::ControlFlowRoot::Repeat(repeat) => Ok(result::selectable_expr(
            logical::LogicalExpr::RootRepeat(repeat),
        )),
        scoped::ControlFlowRoot::NotControlFlow => Ok(NativeSelectableRoot::NotSelectable),
    }
}

pub(super) fn access_or_pipeline_root_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
) -> Result<NativeSelectableRoot, error::PlannerError> {
    match shape::native_access_stream_from_ast(ctx, root)? {
        shape::NativeAccessStreamRoot::Stream(stream) => {
            return stream.into_logical_expr().map(result::selectable_expr);
        }
        shape::NativeAccessStreamRoot::NotAccessStream => {}
    }
    match pipeline::native_pipeline_expr_from_ast(ctx, root)? {
        pipeline::NativePipelineExprRoot::Pipeline(expr) => Ok(result::selectable_expr(*expr)),
        pipeline::NativePipelineExprRoot::NotPipeline => Ok(NativeSelectableRoot::NotSelectable),
    }
}
