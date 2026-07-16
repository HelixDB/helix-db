//! Scoped native lowering entrypoint.
//!
//! The dispatch order is intentionally explicit: complete native roots win
//! first, preserving access-specific stream contracts. If that unscoped path
//! rejects only because a stream wrapper input needs scoped `$context`, scoped
//! wrapper lowering gets a chance to bind it.

mod context_binding;
mod native;
mod recursive;

use helix_ast::traversal::AstNode;

use super::super::family;
use super::super::rejection::{self, NativeUnsupportedReason};
use super::super::scope::NativeAstScope;
use crate::planning::selected::root::SelectableRunRoot;
use crate::{context, error, logical};

/// Scoped selectable-root recognition result.
#[derive(Debug)]
pub(in crate::planning::selected::native) enum ScopedSelectableRoot {
    /// The AST root is a validated selectable Cascades root in this scope.
    Root(Box<SelectableRunRoot>),
    /// The AST root is not selectable in this scope.
    NotSelectable,
}

impl ScopedSelectableRoot {
    #[cfg(test)]
    pub(super) fn expect_selectable(self, message: &str) -> SelectableRunRoot {
        match self {
            Self::Root(root) => *root,
            Self::NotSelectable => panic!("{message}"),
        }
    }
}

/// Lower an AST root to an ordinary selectable Cascades root in the given scope.
pub(in crate::planning::selected::native) fn scoped_selectable_root_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
    scope: NativeAstScope,
) -> Result<ScopedSelectableRoot, error::PlannerError> {
    match family::NativeAstFamily::from_ast(root) {
        family::NativeAstFamily::Terminal => recursive::terminal_root_from_ast(ctx, root, scope),
        family::NativeAstFamily::VariableSource
        | family::NativeAstFamily::IndexDdl
        | family::NativeAstFamily::ShortestPath => native::native_only_root_from_ast(ctx, root),
        family::NativeAstFamily::SourceMutation => {
            recursive::mutation_root_from_ast(ctx, root, scope)
        }
        family::NativeAstFamily::Context => context_binding::context_root(scope),
        family::NativeAstFamily::ControlFlow => {
            recursive::control_flow_root_from_ast(ctx, root, scope)
        }
        family::NativeAstFamily::AccessOrPipeline => {
            recursive::pipeline_root_from_ast(ctx, root, scope)
        }
    }
}

pub(in crate::planning::selected::native::scoped) fn scoped_required_expr_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
    scope: NativeAstScope,
) -> Result<logical::LogicalExpr, error::PlannerError> {
    match scoped_selectable_root_from_ast(ctx, root, scope)? {
        ScopedSelectableRoot::Root(root) => Ok(root.expr().clone()),
        ScopedSelectableRoot::NotSelectable => Err(rejection::unsupported(
            NativeUnsupportedReason::ScopedChildUnsupported,
        )),
    }
}
