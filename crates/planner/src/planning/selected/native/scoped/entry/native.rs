//! Native-first selectable-root probing for scoped lowering.

use helix_ast::traversal::AstNode;

use super::ScopedSelectableRoot;
use crate::planning::selected::native::rejection::{self, NativeUnsupportedReason};
use crate::planning::selected::native::root::{
    native_selectable_root_from_ast, NativeSelectableRoot,
};
use crate::{context, error};

pub(super) fn try_native_root_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
) -> Result<ScopedSelectableRoot, error::PlannerError> {
    match native_selectable_root_from_ast(ctx, root) {
        Ok(NativeSelectableRoot::Root(root)) => Ok(ScopedSelectableRoot::Root(root)),
        Ok(NativeSelectableRoot::NotSelectable) => Ok(ScopedSelectableRoot::NotSelectable),
        Err(error)
            if error
                == rejection::unsupported(NativeUnsupportedReason::RootStreamInputUnsupported) =>
        {
            Ok(ScopedSelectableRoot::NotSelectable)
        }
        Err(error) => Err(error),
    }
}

pub(super) fn native_only_root_from_ast(
    ctx: &context::PlannerContext,
    root: &AstNode,
) -> Result<ScopedSelectableRoot, error::PlannerError> {
    try_native_root_from_ast(ctx, root)
}
