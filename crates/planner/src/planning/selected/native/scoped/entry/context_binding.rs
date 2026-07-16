//! Scoped `$context` selectable-root construction.

use super::super::binding;
use super::ScopedSelectableRoot;
use crate::error;
use crate::logical;
use crate::planning::selected::native::scope::NativeAstScope;
use crate::planning::selected::root::SelectableRunRoot;

pub(super) fn context_root(
    scope: NativeAstScope,
) -> Result<ScopedSelectableRoot, error::PlannerError> {
    if scope.binds_context() {
        Ok(ScopedSelectableRoot::Root(Box::new(
            SelectableRunRoot::new(logical::LogicalExpr::VariableSource(
                binding::context_variable_source(),
            )),
        )))
    } else {
        Ok(ScopedSelectableRoot::NotSelectable)
    }
}
