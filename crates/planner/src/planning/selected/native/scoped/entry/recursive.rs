//! Recursive scoped selectable-root construction after native root probing.

use helix_ast::traversal::AstNode;

use super::super::{control_flow, input_mutation, pipeline, terminal};
use super::native::try_native_root_from_ast;
use super::ScopedSelectableRoot;
use crate::logical;
use crate::planning::selected::native::scope::NativeAstScope;
use crate::planning::selected::root::SelectableRunRoot;
use crate::{context, error};

pub(super) fn control_flow_root_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
    scope: NativeAstScope,
) -> Result<ScopedSelectableRoot, error::PlannerError> {
    if let ScopedSelectableRoot::Root(root) = try_native_root_from_ast(ctx, root)? {
        return Ok(ScopedSelectableRoot::Root(root));
    }
    match control_flow::control_flow_from_ast(ctx, root, scope)? {
        control_flow::ControlFlowRoot::Branch(branch) => {
            return Ok(ScopedSelectableRoot::Root(Box::new(
                SelectableRunRoot::new(logical::LogicalExpr::RootBranch(branch)),
            )));
        }
        control_flow::ControlFlowRoot::Repeat(repeat) => {
            return Ok(ScopedSelectableRoot::Root(Box::new(
                SelectableRunRoot::new(logical::LogicalExpr::RootRepeat(repeat)),
            )));
        }
        control_flow::ControlFlowRoot::NotControlFlow => {}
    }
    Ok(ScopedSelectableRoot::NotSelectable)
}

pub(super) fn mutation_root_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
    scope: NativeAstScope,
) -> Result<ScopedSelectableRoot, error::PlannerError> {
    if let ScopedSelectableRoot::Root(root) = try_native_root_from_ast(ctx, root)? {
        return Ok(ScopedSelectableRoot::Root(root));
    }
    match input_mutation::input_mutation_from_ast(ctx, root, scope)? {
        input_mutation::InputMutationRoot::Mutation(mutation) => {
            return Ok(ScopedSelectableRoot::Root(Box::new(
                SelectableRunRoot::new(logical::LogicalExpr::RootMutation(mutation)),
            )));
        }
        input_mutation::InputMutationRoot::SourceOnly
        | input_mutation::InputMutationRoot::NotMutation => {}
    }
    Ok(ScopedSelectableRoot::NotSelectable)
}

pub(super) fn terminal_root_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
    scope: NativeAstScope,
) -> Result<ScopedSelectableRoot, error::PlannerError> {
    if let ScopedSelectableRoot::Root(root) = try_native_root_from_ast(ctx, root)? {
        return Ok(ScopedSelectableRoot::Root(root));
    }
    match terminal::terminal_expr_from_ast(ctx, root, scope)? {
        terminal::ScopedTerminalRoot::Terminal(expr) => {
            return Ok(ScopedSelectableRoot::Root(Box::new(
                SelectableRunRoot::new(*expr),
            )));
        }
        terminal::ScopedTerminalRoot::NotTerminal => {}
    }
    Ok(ScopedSelectableRoot::NotSelectable)
}

pub(super) fn pipeline_root_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
    scope: NativeAstScope,
) -> Result<ScopedSelectableRoot, error::PlannerError> {
    if let ScopedSelectableRoot::Root(root) = try_native_root_from_ast(ctx, root)? {
        return Ok(ScopedSelectableRoot::Root(root));
    }
    match pipeline::pipeline_expr_from_ast(ctx, root, scope)? {
        pipeline::ScopedPipelineRoot::Pipeline(expr) => {
            return Ok(ScopedSelectableRoot::Root(Box::new(
                SelectableRunRoot::new(*expr),
            )));
        }
        pipeline::ScopedPipelineRoot::NotPipeline => {}
    }
    Ok(ScopedSelectableRoot::NotSelectable)
}
