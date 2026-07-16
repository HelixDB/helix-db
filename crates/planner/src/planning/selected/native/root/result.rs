//! Native selectable-root result contract.

use super::super::super::root::SelectableRunRoot;
use crate::logical;

/// Complete native selectable-root recognition result.
#[derive(Debug)]
pub(in crate::planning::selected::native) enum NativeSelectableRoot {
    /// The AST root is a validated selectable Cascades root.
    Root(Box<SelectableRunRoot>),
    /// The AST root is not selectable by native lowering.
    NotSelectable,
}

pub(super) fn selectable_expr(expr: logical::LogicalExpr) -> NativeSelectableRoot {
    NativeSelectableRoot::Root(Box::new(SelectableRunRoot::new(expr)))
}
